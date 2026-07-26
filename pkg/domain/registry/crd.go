package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	crdPlural           = "kontinuums"
	crdSingular         = "kontinuum"
	crdKind             = "Kontinuum"
	crdListKind         = "KontinuumList"
	crdEstablishTimeout = 10 * time.Second
	crdPollInterval     = 100 * time.Millisecond
	// crdMaxPollInterval caps how large crdBackoff's exponential interval is
	// allowed to grow between retries, however long crdEstablishTimeout is.
	crdMaxPollInterval = 2 * time.Second
	// crdBackoffFactor doubles crdBackoff's interval on every retry.
	crdBackoffFactor = 2
)

// crdBackoff returns a fresh exponential backoff for CRD-readiness polling
// (creation, establishment, discoverability): starts at crdPollInterval,
// doubles each attempt, capped at crdMaxPollInterval. Steps is effectively
// unbounded — the actual ceiling is ctx's own timeout (see EnsureCRD's
// callers, which each derive a crdEstablishTimeout-bounded context), not a
// retry count.
func crdBackoff() wait.Backoff {
	return wait.Backoff{
		Duration: crdPollInterval,
		Factor:   crdBackoffFactor,
		Cap:      crdMaxPollInterval,
		Steps:    math.MaxInt32,
	}
}

// crdName is the kontinuums.kontinuum.sh CustomResourceDefinition's name.
func crdName() string {
	return crdPlural + "." + v1alpha2.GroupName
}

// CustomResourceDefinition builds the kontinuums.kontinuum.sh CRD:
// cluster-scoped, v1alpha2, with a structural schema and a status
// subresource so the heartbeat runnable can update status.lastHeartbeatTime
// and status.role independently of spec.
func CustomResourceDefinition() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName()},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: v1alpha2.GroupName,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   crdPlural,
				Singular: crdSingular,
				Kind:     crdKind,
				ListKind: crdListKind,
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    v1alpha2.APIVersion,
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: kontinuumSchema(),
					},
					// Keep in sync with the +kubebuilder:printcolumn markers
					// on v1alpha2.Kontinuum (see make generate).
					AdditionalPrinterColumns: []apiextensionsv1.CustomResourceColumnDefinition{
						{Name: "Role", Type: "string", JSONPath: ".status.role"},
						{Name: "Region", Type: "string", JSONPath: ".spec.region"},
						{Name: "Zone", Type: "string", JSONPath: ".spec.zone"},
					},
				},
			},
		},
	}
}

// kontinuumSchema is the structural OpenAPIV3 schema for a Kontinuum object.
func kontinuumSchema() *apiextensionsv1.JSONSchemaProps {
	roleEnum := []apiextensionsv1.JSON{
		{Raw: []byte(`"` + v1alpha2.RoleControlPlane + `"`)},
		{Raw: []byte(`"` + v1alpha2.RoleWorker + `"`)},
	}

	return &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"region": {Type: "string"},
					"zone":   {Type: "string"},
				},
			},
			"status": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"role":              {Type: "string", Enum: roleEnum},
					"lastHeartbeatTime": {Type: "string", Format: "date-time"},
				},
			},
		},
	}
}

// EnsureCRD is a libkapi.PostStartHookFunc — see its registration in
// pkg/cli/serve.go via libkapi.WithPostStartHook. It builds an
// apiextensions client and a Kontinuum client (the latter only needed to
// delete stale objects if ApplyCRD hits a stale-stored-version conflict —
// see migrateStaleStoredVersions) from loopbackConfig (the server's own
// privileged identity, only reachable once libkapi's post-start hooks run —
// after ListenAndServe's listener is bound and Serve is already running,
// but before the controller manager starts), creates the CRD and waits for
// it to become Established, then waits for Kontinuum's GVK to actually
// resolve through a RESTMapper built off the same loopbackConfig — the same
// kind of RESTMapper the registry's own Manager (and any other
// controller-runtime client) uses. Established alone doesn't guarantee
// that; since WithPostStartHook registrations run before the controller
// manager starts (see libkapi's own doc on WithPostStartHook and
// WithController), this closes the gap before anything downstream can lose
// the race. logger receives a warning for every retry along the way.
func EnsureCRD(ctx context.Context, loopbackConfig *restclient.Config, logger *slog.Logger) error {
	apiextensionsClient, err := apiextensionsclientset.NewForConfig(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	scheme := runtime.NewScheme()

	err = v1alpha2.AddToScheme(scheme)
	if err != nil {
		return fmt.Errorf("failed to register kontinuum.sh/v1alpha2 scheme: %w", err)
	}

	kontinuums, err := client.New(loopbackConfig, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to build kontinuum client: %w", err)
	}

	err = ensureCRD(ctx, apiextensionsClient, kontinuums, logger)
	if err != nil {
		return err
	}

	return waitForDiscoverable(ctx, loopbackConfig, logger)
}

// waitForDiscoverable blocks until Kontinuum's GVK stops returning
// meta.NoKindMatchError from a RESTMapper built off loopbackConfig. A
// genuine failure to become discoverable (bad manifest, RBAC issue) fails
// this — and therefore ListenAndServe — instead of hanging or silently
// leaving it for a downstream watcher to fail on its own. Retries use
// crdBackoff (exponential, capped at crdMaxPollInterval), bounded overall by
// crdEstablishTimeout; each retry logs a warning.
func waitForDiscoverable(ctx context.Context, loopbackConfig *restclient.Config, logger *slog.Logger) error {
	httpClient, err := restclient.HTTPClientFor(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build discovery http client: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(loopbackConfig, httpClient)
	if err != nil {
		return fmt.Errorf("failed to build rest mapper: %w", err)
	}

	gvk := v1alpha2.GroupVersion().WithKind(crdKind)

	timeoutCtx, cancel := context.WithTimeout(ctx, crdEstablishTimeout)
	defer cancel()

	err = wait.ExponentialBackoffWithContext(timeoutCtx, crdBackoff(),
		func(context.Context) (bool, error) {
			_, mappingErr := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			if mappingErr != nil {
				if meta.IsNoMatchError(mappingErr) {
					logger.Warn("Kontinuum kind not yet resolvable via rest mapper, retrying", "error", mappingErr)

					return false, nil
				}

				return false, fmt.Errorf("failed to resolve %s rest mapping: %w", crdName(), mappingErr)
			}

			return true, nil
		})
	if err != nil {
		return fmt.Errorf("%s was never resolvable via the rest mapper: %w", crdName(), err)
	}

	return nil
}

// ensureCRD creates the kontinuums.kontinuum.sh CRD if it doesn't already
// exist — or updates it in place to match the current
// CustomResourceDefinition() spec if it does, since a stale definition left
// over from a previous run (e.g. one that still only serves a since-removed
// API version) would otherwise never converge — then waits for it to become
// Established. The create-or-update call is inside the same retry loop as
// the Established check — not just a single unretried attempt — since a
// transient failure here (the apiserver still finishing its own startup,
// another kontinuum replica reconciling the same CRD concurrently) isn't
// fatal, only crdEstablishTimeout running out is. Retries use crdBackoff
// (exponential, capped at crdMaxPollInterval); each retry logs a warning.
func ensureCRD(
	ctx context.Context, apiextensionsClient apiextensionsclientset.Interface, kontinuums client.Client,
	logger *slog.Logger,
) error {
	crds := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions()

	timeoutCtx, cancel := context.WithTimeout(ctx, crdEstablishTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, crdBackoff(),
		func(ctx context.Context) (bool, error) {
			err := ApplyCRD(ctx, crds, kontinuums, logger)
			if err != nil {
				logger.Warn("Failed to create or update kontinuums.kontinuum.sh crd, retrying", "error", err)

				return false, nil
			}

			// A failed fetch just means "not ready yet" — the poll keeps
			// retrying until crdEstablishTimeout regardless of why, so a
			// nil crd (transient error, or not yet visible) and a crd
			// that simply isn't Established are treated the same way.
			crd, getErr := crds.Get(ctx, crdName(), metav1.GetOptions{})
			if crd == nil || !crdEstablished(crd) {
				logger.Warn("Waiting for kontinuums.kontinuum.sh crd to become established", "error", getErr)

				return false, nil
			}

			return true, nil
		})
	if err != nil {
		return fmt.Errorf("%s crd was never created and established: %w", crdName(), err)
	}

	return nil
}

// ApplyCRD creates the kontinuums.kontinuum.sh CRD, or — if it already
// exists — updates its spec to match CustomResourceDefinition()'s current
// definition whenever the two differ. Without this, an already-registered
// CRD from a previous run (e.g. one still listing a since-removed
// spec.versions entry like v1alpha1) would keep serving the stale
// definition forever, since Create alone is a no-op once the object exists.
// A no-op Update when the spec already matches is avoided so this doesn't
// churn the CRD's resourceVersion on every retry once it's converged.
//
// An update can still fail with isStaleStoredVersionError: status.storedVersions
// (bookkeeping the apiserver — not this code — maintains) can reference a
// version, like v1alpha1 before the Role-into-status migration, that
// CustomResourceDefinition() no longer lists at all, and apiextensions
// refuses to drop a version still referenced there. kontinuums is only
// needed to recover from that one case — see migrateStaleStoredVersions.
func ApplyCRD(
	ctx context.Context, crds apiextensionsv1client.CustomResourceDefinitionInterface, kontinuums client.Client,
	logger *slog.Logger,
) error {
	desired := CustomResourceDefinition()

	_, err := crds.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %s crd: %w", crdName(), err)
	}

	existing, err := crds.Get(ctx, crdName(), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch existing %s crd: %w", crdName(), err)
	}

	if reflect.DeepEqual(existing.Spec, desired.Spec) {
		return nil
	}

	existing.Spec = desired.Spec

	_, err = crds.Update(ctx, existing, metav1.UpdateOptions{})
	if err == nil {
		return nil
	}

	if !isStaleStoredVersionError(err) {
		return fmt.Errorf("failed to update existing %s crd: %w", crdName(), err)
	}

	err = migrateStaleStoredVersions(ctx, crds, kontinuums, logger)
	if err != nil {
		return fmt.Errorf("failed to migrate %s crd off a removed api version: %w", crdName(), err)
	}

	return nil
}

// isStaleStoredVersionError reports whether err is the specific
// apiextensions validation failure that fires when status.storedVersions
// references an API version CustomResourceDefinition() no longer lists in
// spec.versions — the case migrateStaleStoredVersions handles.
func isStaleStoredVersionError(err error) bool {
	var statusErr *apierrors.StatusError

	if !errors.As(err, &statusErr) || statusErr.Status().Details == nil {
		return false
	}

	for _, cause := range statusErr.Status().Details.Causes {
		if strings.HasPrefix(cause.Field, "status.storedVersions") {
			return true
		}
	}

	return false
}

// migrateStaleStoredVersions handles isStaleStoredVersionError: a version
// this process no longer knows about — e.g. v1alpha1, from before the Role
// moved into status — is still recorded in status.storedVersions from a
// previous run, and apiextensions refuses to drop a version that's still
// referenced there. Kontinuum objects are ephemeral heartbeats, not data
// worth preserving across an API shape change (see Heartbeat and
// TTLReconciler): a live process re-registers its own within one heartbeat
// interval, and anything else was already a TTL-deletion candidate. So this
// deletes every existing Kontinuum, resets status.storedVersions to just
// the current storage version, and updates the spec to match — which by
// then no longer conflicts, since nothing references the removed version
// anymore.
func migrateStaleStoredVersions(
	ctx context.Context, crds apiextensionsv1client.CustomResourceDefinitionInterface, kontinuums client.Client,
	logger *slog.Logger,
) error {
	var list v1alpha2.KontinuumList

	err := kontinuums.List(ctx, &list)
	if err != nil {
		return fmt.Errorf("failed to list existing kontinuums: %w", err)
	}

	for i := range list.Items {
		item := &list.Items[i]

		err := kontinuums.Delete(ctx, item)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete kontinuum %q registered under a removed api version: %w", item.Name, err)
		}

		logger.Warn("Deleted kontinuum registered under a removed api version", "name", item.Name)
	}

	crd, err := crds.Get(ctx, crdName(), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch %s crd: %w", crdName(), err)
	}

	crd.Status.StoredVersions = []string{v1alpha2.APIVersion}

	crd, err = crds.UpdateStatus(ctx, crd, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to reset stored versions: %w", err)
	}

	crd.Spec = CustomResourceDefinition().Spec

	_, err = crds.Update(ctx, crd, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update spec after resetting stored versions: %w", err)
	}

	return nil
}

// crdEstablished reports whether crd's Established condition is true.
func crdEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, cond := range crd.Status.Conditions {
		if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}

	return false
}
