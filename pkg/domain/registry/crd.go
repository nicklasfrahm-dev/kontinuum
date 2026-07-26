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
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	crdconfig "github.com/nicklasfrahm/kontinuum/config/crd"
)

const (
	crdPlural = "kontinuums"
	crdKind   = "Kontinuum"
	// crdManifestFile is kontinuums' generated manifest's name within
	// crdconfig.Files — see CustomResourceDefinition.
	crdManifestFile     = "kontinuum.sh_kontinuums.yaml"
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

// CustomResourceDefinition builds the kontinuums.kontinuum.sh CRD by
// reading crdManifestFile out of crdconfig.Files — the config/crd manifests
// controller-gen generates from api/v1alpha1 and api/v1alpha2's kubebuilder
// markers — and patching in the one piece that manifest can't contain: the
// conversion webhook's clientConfig. controller-gen has no marker for a
// webhook's URL or CABundle, and CABundle in particular is only known at
// runtime — EnsureConversionWebhookCert's result — so it can't be baked
// into a generated file at all. Region/zone's CEL rule, the role enum,
// printer columns, and which version is storage are all controller-gen's
// responsibility now (see api/v1alpha2/doc.go); this function no longer
// hand-builds any of that, so schema and markers can't drift apart the way
// they already had once. A missing or malformed manifest can only mean a
// build-time bug (a corrupt `make generate` run), not a condition callers
// could meaningfully recover from — hence the panic instead of a returned
// error, the same contract this function has always had.
func CustomResourceDefinition(caBundle []byte) *apiextensionsv1.CustomResourceDefinition {
	manifest, err := crdconfig.Files.ReadFile(crdManifestFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read embedded %s manifest: %v", crdName(), err))
	}

	var crd apiextensionsv1.CustomResourceDefinition

	err = yaml.Unmarshal(manifest, &crd)
	if err != nil {
		panic(fmt.Sprintf("failed to parse embedded %s manifest: %v", crdName(), err))
	}

	conversionHostPort := net.JoinHostPort(conversionWebhookDNSName, strconv.Itoa(ConversionWebhookPort))
	conversionURL := "https://" + conversionHostPort + conversionWebhookPath

	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.WebhookConverter,
		Webhook: &apiextensionsv1.WebhookConversion{
			ClientConfig: &apiextensionsv1.WebhookClientConfig{
				URL:      &conversionURL,
				CABundle: caBundle,
			},
			ConversionReviewVersions: []string{"v1"},
		},
	}

	return &crd
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
