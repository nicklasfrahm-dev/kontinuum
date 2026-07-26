package registry

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"reflect"
	"strconv"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
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

// conversionWebhookPath is where Controller.SetupWithManager registers the
// conversion webhook handler on the manager's webhook server, and where
// CustomResourceDefinition's conversion webhook clientConfig URL points.
//
// This one handler — conversion.NewWebhookHandler(mgr.GetScheme()) — already
// generalizes to any number of resource kinds and versions: it dispatches
// purely by the apiVersion/kind embedded in each ConversionReview request,
// not by path, so a future CRD only needs its own types to implement
// conversion.Convertible/Hub and be registered in the same shared scheme
// (see pkg/cli/serve.go's buildServer) to be served here too — no new path
// or handler required. The one real constraint is that
// webhook.Server.Register panics if the same path is registered twice, so
// if kontinuum ever grows a second Controller, that Controller must NOT
// also call Register on this path — this is the one, shared registration
// for every convertible CRD in the process.
const conversionWebhookPath = "/convert"

// CustomResourceDefinition builds the kontinuums.kontinuum.sh CRD:
// cluster-scoped, with a structural schema and a status subresource so the
// heartbeat runnable can update status.lastHeartbeatTime and status.role
// independently of spec. Lists two versions — v1alpha2 (served, storage)
// and v1alpha1 (served, but no longer storage) — converted between by a
// webhook, since Role moved from spec (v1alpha1) into status (v1alpha2): a
// structural change "None" conversion can't handle (it assumes every
// version is byte-compatible), and in fact apiextensions rejects "None"
// outright once the schemas genuinely diverge like this. caBundle is
// EnsureConversionWebhookCert's result — see its doc for why the cert has
// to exist and be embedded here before the webhook server that will
// actually present it is even built.
func CustomResourceDefinition(caBundle []byte) *apiextensionsv1.CustomResourceDefinition {
	conversionHostPort := net.JoinHostPort(conversionWebhookDNSName, strconv.Itoa(ConversionWebhookPort))
	conversionURL := "https://" + conversionHostPort + conversionWebhookPath

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
			Conversion: &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.WebhookConverter,
				Webhook: &apiextensionsv1.WebhookConversion{
					ClientConfig: &apiextensionsv1.WebhookClientConfig{
						URL:      &conversionURL,
						CABundle: caBundle,
					},
					ConversionReviewVersions: []string{"v1"},
				},
			},
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
				{
					Name:    v1alpha1.APIVersion,
					Served:  true,
					Storage: false,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: kontinuumSchemaV1Alpha1(),
					},
					// Keep in sync with the +kubebuilder:printcolumn markers
					// on v1alpha1.Kontinuum.
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

// kontinuumSchema is the structural OpenAPIV3 schema for a v1alpha2
// Kontinuum object.
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

// kontinuumSchemaV1Alpha1 is the structural OpenAPIV3 schema the removed
// v1alpha1 API version used, before Role moved from spec into status — see
// api/v1alpha1's doc for why the version entry using this still exists at
// all.
func kontinuumSchemaV1Alpha1() *apiextensionsv1.JSONSchemaProps {
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
// closes the gap before anything downstream can lose the race. caBundle
// comes from EnsureConversionWebhookCert, called by the caller before this
// (see its doc for why that ordering matters). logger receives a warning
// for every retry along the way.
func EnsureCRD(ctx context.Context, loopbackConfig *restclient.Config, caBundle []byte, logger *slog.Logger) error {
	apiextensionsClient, err := apiextensionsclientset.NewForConfig(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	err = ensureCRD(ctx, apiextensionsClient, caBundle, logger)
	if err != nil {
		return err
	}

	return waitForDiscoverable(ctx, loopbackConfig, logger)
}

// waitForDiscoverable blocks until Kontinuum's GVK stops returning
// meta.NoKindMatchError from a RESTMapper built off loopbackConfig, for
// every served version — both v1alpha2 and the conversion-webhook-served
// v1alpha1 (see CustomResourceDefinition's doc for why v1alpha1 still has
// to be served at all). Checking only the storage version left a real gap:
// a caller (or, in this codebase, a test) creating a v1alpha1 object right
// after /healthz reports ready could still race the RESTMapper's own
// discovery cache and see "no matches for kind" even though the CRD already
// lists it. A genuine failure to become discoverable (bad manifest, RBAC
// issue) fails this — and therefore ListenAndServe — instead of hanging or
// silently leaving it for a downstream watcher to fail on its own. Retries
// use crdBackoff (exponential, capped at crdMaxPollInterval), bounded
// overall by crdEstablishTimeout per version; each retry logs a warning.
func waitForDiscoverable(ctx context.Context, loopbackConfig *restclient.Config, logger *slog.Logger) error {
	httpClient, err := restclient.HTTPClientFor(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build discovery http client: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(loopbackConfig, httpClient)
	if err != nil {
		return fmt.Errorf("failed to build rest mapper: %w", err)
	}

	gvks := []schema.GroupVersionKind{
		v1alpha2.GroupVersion().WithKind(crdKind),
		v1alpha1.GroupVersion().WithKind(crdKind),
	}

	for _, gvk := range gvks {
		err := waitForGVKDiscoverable(ctx, restMapper, gvk, logger)
		if err != nil {
			return err
		}
	}

	return nil
}

// waitForGVKDiscoverable polls restMapper for gvk until it resolves or
// crdEstablishTimeout elapses — see waitForDiscoverable's doc.
func waitForGVKDiscoverable(
	ctx context.Context, restMapper meta.RESTMapper, gvk schema.GroupVersionKind, logger *slog.Logger,
) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, crdEstablishTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, crdBackoff(),
		func(context.Context) (bool, error) {
			_, mappingErr := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			if mappingErr != nil {
				if meta.IsNoMatchError(mappingErr) {
					logger.Warn("Kontinuum kind not yet resolvable via rest mapper, retrying",
						"version", gvk.Version, "error", mappingErr)

					return false, nil
				}

				return false, fmt.Errorf("failed to resolve %s rest mapping for %s: %w", crdName(), gvk.Version, mappingErr)
			}

			return true, nil
		})
	if err != nil {
		return fmt.Errorf("%s was never resolvable via the rest mapper for %s: %w", crdName(), gvk.Version, err)
	}

	return nil
}

// ensureCRD creates the kontinuums.kontinuum.sh CRD if it doesn't already
// exist — or updates it in place to match the current
// CustomResourceDefinition() spec if it does, since a stale definition left
// over from a previous run (e.g. one predating a schema or printer-column
// change) would otherwise never converge — then waits for it to become
// Established. The create-or-update call is inside the same retry loop as
// the Established check — not just a single unretried attempt — since a
// transient failure here (the apiserver still finishing its own startup,
// another kontinuum replica reconciling the same CRD concurrently) isn't
// fatal, only crdEstablishTimeout running out is. Retries use crdBackoff
// (exponential, capped at crdMaxPollInterval); each retry logs a warning.
func ensureCRD(
	ctx context.Context, apiextensionsClient apiextensionsclientset.Interface, caBundle []byte, logger *slog.Logger,
) error {
	crds := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions()

	timeoutCtx, cancel := context.WithTimeout(ctx, crdEstablishTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, crdBackoff(),
		func(ctx context.Context) (bool, error) {
			err := ApplyCRD(ctx, crds, caBundle)
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
// exists — updates its spec to match CustomResourceDefinition(caBundle)'s
// current definition whenever the two differ. Without this, an
// already-registered CRD from a previous run would keep serving its stale
// definition forever, since Create alone is a no-op once the object exists.
// A no-op Update when the spec already matches is avoided so this doesn't
// churn the CRD's resourceVersion on every retry once it's converged.
func ApplyCRD(
	ctx context.Context, crds apiextensionsv1client.CustomResourceDefinitionInterface, caBundle []byte,
) error {
	desired := CustomResourceDefinition(caBundle)

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
	if err != nil {
		return fmt.Errorf("failed to update existing %s crd: %w", crdName(), err)
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
