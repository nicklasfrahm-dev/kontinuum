// Package crd holds the generic "apply a controller-gen-generated CRD
// manifest, wait for it to become Established, then wait for its GVKs to
// resolve through a RESTMapper" lifecycle every CRD this project registers
// needs — originally written once for kontinuums.kontinuum.sh (see
// pkg/domain/registry), generalized here so the zone-join CRDs
// (zones/instances/instancepools/talosclusters.kontinuum.sh) can reuse it
// without a second copy of the same polling/backoff logic.
package crd

import (
	"context"
	"fmt"
	"io/fs"
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
)

const (
	establishTimeout = 10 * time.Second
	pollInterval     = 100 * time.Millisecond
	// maxPollInterval caps how large backoff's exponential interval is
	// allowed to grow between retries, however long establishTimeout is.
	maxPollInterval = 2 * time.Second
	// backoffFactor doubles backoff's interval on every retry.
	backoffFactor = 2
)

// ConversionWebhook configures a CRD's spec.conversion.webhook clientConfig
// — controller-gen has no marker for a webhook's URL or CABundle (CABundle
// in particular is only known at runtime), so it can't be baked into a
// generated manifest at all. A Definition with no prior served version
// (nothing to convert between) leaves this nil.
type ConversionWebhook struct {
	// Path is where this CRD's conversion webhook handler is registered on
	// the manager's webhook server (e.g. "/convert").
	Path string
	// DNSName is the webhook server's DNS name.
	DNSName string
	// Port is the webhook server's port.
	Port int
	// CABundle is the webhook TLS certificate's CA bundle.
	CABundle []byte
}

// Definition describes one CRD's identity and lifecycle: where to read its
// generated manifest from, which GVKs must become discoverable once
// applied, and (optionally) how to patch in a conversion webhook.
type Definition struct {
	// Name is the CRD object's own name, e.g. "instances.kontinuum.sh".
	Name string
	// ManifestFile is this CRD's generated manifest's filename within the
	// fs.FS passed to Build/Ensure/Apply.
	ManifestFile string
	// GVKs lists every GroupVersionKind this CRD serves — each is checked
	// for discoverability once Established. Usually one entry; a CRD with a
	// conversion webhook (like Kontinuum) lists every served version, not
	// just the storage one, since a caller could target any of them.
	GVKs []schema.GroupVersionKind
	// Conversion configures this CRD's conversion webhook clientConfig.
	// Nil when this kind has no prior served version to convert.
	Conversion *ConversionWebhook
}

// backoff returns a fresh exponential backoff for CRD-readiness polling
// (creation, establishment, discoverability): starts at pollInterval,
// doubles each attempt, capped at maxPollInterval. Steps is effectively
// unbounded — the actual ceiling is ctx's own timeout (see Ensure's
// callers, which each derive an establishTimeout-bounded context), not a
// retry count.
func backoff() wait.Backoff {
	return wait.Backoff{
		Duration: pollInterval,
		Factor:   backoffFactor,
		Cap:      maxPollInterval,
		Steps:    math.MaxInt32,
	}
}

// Build reads def.ManifestFile out of manifestFS — the config/crd manifest
// controller-gen generates from this CRD's kubebuilder markers — and
// patches in def.Conversion's webhook clientConfig, if set. A missing or
// malformed manifest can only mean a build-time bug (a corrupt `make
// generate` run), not a condition callers could meaningfully recover
// from — hence the panic instead of a returned error.
func Build(manifestFS fs.FS, def Definition) *apiextensionsv1.CustomResourceDefinition {
	manifest, err := fs.ReadFile(manifestFS, def.ManifestFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read embedded %s manifest: %v", def.Name, err))
	}

	var crdObj apiextensionsv1.CustomResourceDefinition

	err = yaml.Unmarshal(manifest, &crdObj)
	if err != nil {
		panic(fmt.Sprintf("failed to parse embedded %s manifest: %v", def.Name, err))
	}

	if def.Conversion != nil {
		conversionHostPort := net.JoinHostPort(def.Conversion.DNSName, strconv.Itoa(def.Conversion.Port))
		conversionURL := "https://" + conversionHostPort + def.Conversion.Path

		crdObj.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
			Strategy: apiextensionsv1.WebhookConverter,
			Webhook: &apiextensionsv1.WebhookConversion{
				ClientConfig: &apiextensionsv1.WebhookClientConfig{
					URL:      &conversionURL,
					CABundle: def.Conversion.CABundle,
				},
				ConversionReviewVersions: []string{"v1"},
			},
		}
	}

	return &crdObj
}

// Ensure is a libkapi.PostStartHookFunc-shaped helper — see
// pkg/domain/registry.EnsureCRD's own doc for the timing this relies on
// (loopbackConfig is only reachable once libkapi's post-start hooks run,
// before the controller manager starts). It builds an apiextensions client,
// creates def's CRD and waits for it to become Established, then waits for
// every def.GVKs entry to actually resolve through a RESTMapper built off
// the same loopbackConfig — the same kind of RESTMapper any other
// controller-runtime client uses. Established alone doesn't guarantee that.
// logger receives a warning for every retry along the way.
func Ensure(
	ctx context.Context, loopbackConfig *restclient.Config, manifestFS fs.FS, def Definition, logger *slog.Logger,
) error {
	apiextensionsClient, err := apiextensionsclientset.NewForConfig(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	err = ensureEstablished(ctx, apiextensionsClient, manifestFS, def, logger)
	if err != nil {
		return err
	}

	return waitForDiscoverable(ctx, loopbackConfig, def, logger)
}

// waitForDiscoverable blocks until every one of def.GVKs stops returning
// meta.NoKindMatchError from a RESTMapper built off loopbackConfig. A
// genuine failure to become discoverable (bad manifest, RBAC issue) fails
// this instead of hanging or silently leaving it for a downstream watcher
// to fail on its own. Retries use backoff (exponential, capped at
// maxPollInterval), bounded overall by establishTimeout per GVK; each retry
// logs a warning.
func waitForDiscoverable(
	ctx context.Context, loopbackConfig *restclient.Config, def Definition, logger *slog.Logger,
) error {
	httpClient, err := restclient.HTTPClientFor(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build discovery http client: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(loopbackConfig, httpClient)
	if err != nil {
		return fmt.Errorf("failed to build rest mapper: %w", err)
	}

	for _, gvk := range def.GVKs {
		err := waitForGVKDiscoverable(ctx, restMapper, def, gvk, logger)
		if err != nil {
			return err
		}
	}

	return nil
}

// waitForGVKDiscoverable polls restMapper for gvk until it resolves or
// establishTimeout elapses — see waitForDiscoverable's doc.
func waitForGVKDiscoverable(
	ctx context.Context, restMapper meta.RESTMapper, def Definition, gvk schema.GroupVersionKind, logger *slog.Logger,
) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, establishTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, backoff(),
		func(context.Context) (bool, error) {
			_, mappingErr := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			if mappingErr != nil {
				if meta.IsNoMatchError(mappingErr) {
					logger.Warn("kind not yet resolvable via rest mapper, retrying",
						"crd", def.Name, "version", gvk.Version, "error", mappingErr)

					return false, nil
				}

				return false, fmt.Errorf("failed to resolve %s rest mapping for %s: %w", def.Name, gvk.Version, mappingErr)
			}

			return true, nil
		})
	if err != nil {
		return fmt.Errorf("%s was never resolvable via the rest mapper for %s: %w", def.Name, gvk.Version, err)
	}

	return nil
}

// ensureEstablished creates def's CRD if it doesn't already exist — or
// updates it in place to match Build's current spec if it does, since a
// stale definition left over from a previous run (e.g. one predating a
// schema or printer-column change) would otherwise never converge — then
// waits for it to become Established. The create-or-update call is inside
// the same retry loop as the Established check — not just a single
// unretried attempt — since a transient failure here (the apiserver still
// finishing its own startup, another replica reconciling the same CRD
// concurrently) isn't fatal, only establishTimeout running out is. Retries
// use backoff (exponential, capped at maxPollInterval); each retry logs a
// warning.
func ensureEstablished(
	ctx context.Context, apiextensionsClient apiextensionsclientset.Interface,
	manifestFS fs.FS, def Definition, logger *slog.Logger,
) error {
	crds := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions()

	timeoutCtx, cancel := context.WithTimeout(ctx, establishTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, backoff(),
		func(ctx context.Context) (bool, error) {
			err := Apply(ctx, crds, manifestFS, def)
			if err != nil {
				logger.Warn("failed to create or update crd, retrying", "crd", def.Name, "error", err)

				return false, nil
			}

			// A failed fetch just means "not ready yet" — the poll keeps
			// retrying until establishTimeout regardless of why, so a nil
			// crd (transient error, or not yet visible) and a crd that
			// simply isn't Established are treated the same way.
			crdObj, getErr := crds.Get(ctx, def.Name, metav1.GetOptions{})
			if crdObj == nil || !established(crdObj) {
				logger.Warn("waiting for crd to become established", "crd", def.Name, "error", getErr)

				return false, nil
			}

			return true, nil
		})
	if err != nil {
		return fmt.Errorf("%s crd was never created and established: %w", def.Name, err)
	}

	return nil
}

// Apply creates def's CRD, or — if it already exists — updates its spec to
// match Build(manifestFS, def)'s current definition whenever the two
// differ. Without this, an already-registered CRD from a previous run
// would keep serving its stale definition forever, since Create alone is a
// no-op once the object exists. A no-op Update when the spec already
// matches is avoided so this doesn't churn the CRD's resourceVersion on
// every retry once it's converged.
func Apply(
	ctx context.Context, crds apiextensionsv1client.CustomResourceDefinitionInterface, manifestFS fs.FS, def Definition,
) error {
	desired := Build(manifestFS, def)

	_, err := crds.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %s crd: %w", def.Name, err)
	}

	existing, err := crds.Get(ctx, def.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch existing %s crd: %w", def.Name, err)
	}

	if reflect.DeepEqual(existing.Spec, desired.Spec) {
		return nil
	}

	existing.Spec = desired.Spec

	_, err = crds.Update(ctx, existing, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update existing %s crd: %w", def.Name, err)
	}

	return nil
}

// established reports whether crdObj's Established condition is true.
func established(crdObj *apiextensionsv1.CustomResourceDefinition) bool {
	for _, cond := range crdObj.Status.Conditions {
		if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}

	return false
}
