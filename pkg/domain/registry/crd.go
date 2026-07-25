package registry

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
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
	return crdPlural + "." + v1alpha1.GroupName
}

// CustomResourceDefinition builds the kontinuums.kontinuum.sh CRD:
// cluster-scoped, v1alpha1, with a structural schema and a status
// subresource so the heartbeat runnable can update status.lastHeartbeatTime
// independently of spec.
func CustomResourceDefinition() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName()},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: v1alpha1.GroupName,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   crdPlural,
				Singular: crdSingular,
				Kind:     crdKind,
				ListKind: crdListKind,
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    v1alpha1.APIVersion,
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: kontinuumSchema(),
					},
					// Keep in sync with the +kubebuilder:printcolumn markers
					// on v1alpha1.Kontinuum (see make generate).
					AdditionalPrinterColumns: []apiextensionsv1.CustomResourceColumnDefinition{
						{Name: "Role", Type: "string", JSONPath: ".spec.role"},
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
		{Raw: []byte(`"` + v1alpha1.RoleControlPlane + `"`)},
		{Raw: []byte(`"` + v1alpha1.RoleWorker + `"`)},
	}

	return &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type:     "object",
				Required: []string{"role"},
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"role":   {Type: "string", Enum: roleEnum},
					"region": {Type: "string"},
					"zone":   {Type: "string"},
				},
			},
			"status": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"lastHeartbeatTime": {Type: "string", Format: "date-time"},
				},
			},
		},
	}
}

// EnsureCRD is a libkapi.PostStartHookFunc — see its registration in
// pkg/cli/serve.go via libkapi.WithPostStartHook. It builds an
// apiextensions client from loopbackConfig (the server's own privileged
// identity, only reachable once libkapi's post-start hooks run — after
// ListenAndServe's listener is bound and Serve is already running, but
// before the controller manager starts), creates the CRD and waits for it
// to become Established, then waits for Kontinuum's GVK to actually resolve
// through a RESTMapper built off the same loopbackConfig — the same kind of
// RESTMapper the registry's own Manager (and any other controller-runtime
// client) uses. Established alone doesn't guarantee that; since
// WithPostStartHook registrations run before the controller manager starts
// (see libkapi's own doc on WithPostStartHook and WithController), this
// closes the gap before anything downstream can lose the race. logger
// receives a warning for every retry along the way.
func EnsureCRD(ctx context.Context, loopbackConfig *restclient.Config, logger *slog.Logger) error {
	client, err := apiextensionsclientset.NewForConfig(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	err = ensureCRD(ctx, client, logger)
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

	gvk := v1alpha1.GroupVersion().WithKind(crdKind)

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
// exist, then waits for it to become Established. The Create call is inside
// the same retry loop as the Established check — not just a single
// unretried attempt — since a transient failure here (the apiserver still
// finishing its own startup, another kontinuum replica creating the same
// CRD concurrently) isn't fatal, only crdEstablishTimeout running out is.
// Retries use crdBackoff (exponential, capped at crdMaxPollInterval); each
// retry logs a warning.
func ensureCRD(ctx context.Context, client apiextensionsclientset.Interface, logger *slog.Logger) error {
	crds := client.ApiextensionsV1().CustomResourceDefinitions()

	timeoutCtx, cancel := context.WithTimeout(ctx, crdEstablishTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, crdBackoff(),
		func(ctx context.Context) (bool, error) {
			_, err := crds.Create(ctx, CustomResourceDefinition(), metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				logger.Warn("Failed to create kontinuums.kontinuum.sh crd, retrying", "error", err)

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

// crdEstablished reports whether crd's Established condition is true.
func crdEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, cond := range crd.Status.Conditions {
		if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}

	return false
}
