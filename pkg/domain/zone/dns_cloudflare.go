package zone

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// dnsProviderCloudflare is the only KONTINUUM_SERVER_DNS_PROVIDER value
// reconcileExternalDNSAddon auto-wires an external-dns Addon for today —
// see that function's own doc. Any other provider name is passed through
// untouched (see v1alpha2.KontinuumDNSConfigStatus.Provider's own doc) and
// left for the operator's own external-dns Addon to interpret, exactly as
// before this file existed.
const dnsProviderCloudflare = "cloudflare"

// externalDNSReleaseName duplicates pkg/domain/addon's own (unexported)
// externalDNSAddonName — same rationale as this package's other
// cross-package literal duplications (see storageSecretKey's own doc):
// matching it is what lets resolveAddon's embedded values/external-dns.yaml
// defaults (crd-source scaffolding) apply to the Addon
// ensureExternalDNSCloudflareAddonSeed creates below.
const externalDNSReleaseName = "external-dns"

// externalDNSCredentialSecretName and externalDNSCredentialSecretKey are
// ensureExternalDNSCredentialSecret's own upserted Secret's fixed name/key
// — ensureExternalDNSCloudflareAddonSeed's seeded Addon values reference
// them directly via external-dns' own env[].valueFrom.secretKeyRef support.
//
//nolint:gosec // false positive: object/key names, not credential values
const (
	externalDNSCredentialSecretName = "external-dns-cloudflare"
	externalDNSCredentialSecretKey  = "apiToken"
)

// reconcileExternalDNSAddon auto-wires a Cloudflare-backed external-dns
// Addon for cluster once findKontinuumDNSConfig reports the hub has both a
// DNS provider and credential configured — review feedback on issue #51's
// own "automatically wire the credentials, starting with Cloudflare"
// point: an operator who's set KONTINUUM_SERVER_DNS_PROVIDER=cloudflare and
// KONTINUUM_SERVER_DNS_CREDENTIAL (a Cloudflare API token) on the hub gets
// a working external-dns install with no Addon of their own to write. Any
// other provider name is a deliberate no-op here, left entirely to the
// operator's own Addon — same as before this file existed
// (docs/workflows/zone-add.md's own "or point it yourself" path) —
// cloudflare is this package's first supported provider, not a hardcoded
// assumption it's the only one that'll ever be.
func (r *Reconciler) reconcileExternalDNSAddon(
	ctx context.Context, cluster *v1alpha2.TalosCluster, downstream client.Client, provider, credential string,
) error {
	if provider != dnsProviderCloudflare {
		return nil
	}

	err := ensureExternalDNSCredentialSecret(ctx, downstream, downstreamNamespace, credential)
	if err != nil {
		return err
	}

	return ensureExternalDNSCloudflareAddonSeed(ctx, r.Client, cluster)
}

// ensureExternalDNSCredentialSecret upserts the Secret
// ensureExternalDNSCloudflareAddonSeed's own seeded Addon values reference
// — mirrors workload.go's ensureSecret create-then-get-and-update-on-conflict
// upsert idiom, continuously kept in sync (unlike the Addon itself, see
// ensureExternalDNSCloudflareAddonSeed's own doc) so a rotated
// KONTINUUM_SERVER_DNS_CREDENTIAL propagates to the running external-dns
// pod without any operator intervention.
func ensureExternalDNSCredentialSecret(
	ctx context.Context, downstream client.Client, namespace, credential string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: externalDNSCredentialSecretName, Namespace: namespace},
		StringData: map[string]string{externalDNSCredentialSecretKey: credential},
	}

	err := downstream.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Secret

		err = downstream.Get(ctx, client.ObjectKeyFromObject(secret), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q secret: %w", externalDNSCredentialSecretName, err)
		}

		existing.StringData = secret.StringData

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q secret: %w", externalDNSCredentialSecretName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q secret: %w", externalDNSCredentialSecretName, err)
	}

	return nil
}

// deleteExternalDNSCredentialSecret deletes the Secret
// ensureExternalDNSCredentialSecret upserts, tolerating NotFound — see
// teardown.go's own doc for why every deleteX helper is idempotent the
// same way its ensureX counterpart already is. Called unconditionally
// during teardown (not just for provider == cloudflare): idempotent either
// way, and cheaper than threading the hub's own current DNS provider
// through teardownDownstream just to skip a no-op delete.
func deleteExternalDNSCredentialSecret(ctx context.Context, downstream client.Client, namespace string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: externalDNSCredentialSecretName, Namespace: namespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, secret))
	if err != nil {
		return fmt.Errorf("failed to delete %q secret: %w", externalDNSCredentialSecretName, err)
	}

	return nil
}

// ensureExternalDNSCloudflareAddonSeed creates a Cloudflare-configured
// external-dns Addon CR for cluster, namespaced alongside it (mirrors
// pkg/domain/addon.EnsureBuiltinSeeds' own identical convention — see
// addonResourceName's own doc for why), only if one doesn't already exist.
// Deliberately create-only, same rationale as
// pkg/domain/addon.ensureBuiltinAddonSeed's own doc: an operator who's
// since edited this Addon's own spec.values (a different chart version, an
// extra flag) is left alone on every future reconcile here, not fought.
// Only the credential Secret it references (ensureExternalDNSCredentialSecret)
// is kept continuously in sync — Values themselves are seeded once, since
// they never need to change independently of that Secret.
//
// Owned by cluster via a real owner reference, exactly like every other
// Addon a TalosCluster gets seeded (see ensureBuiltinAddonSeed's own doc)
// — deleting the TalosCluster during Zone teardown garbage-collects this
// Addon for free, with no explicit cleanup needed here.
func ensureExternalDNSCloudflareAddonSeed(
	ctx context.Context, hubClient client.Client, cluster *v1alpha2.TalosCluster,
) error {
	name := cluster.Name + "-" + externalDNSReleaseName

	seed := &v1alpha2.Addon{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}

	err := hubClient.Get(ctx, client.ObjectKeyFromObject(seed), seed)
	if err == nil {
		return nil // already exists — hands off, whoever owns it now
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing addon %q: %w", name, err)
	}

	values, err := cloudflareAddonValues()
	if err != nil {
		return fmt.Errorf("failed to build external-dns addon values: %w", err)
	}

	seed.Spec = v1alpha2.AddonSpec{
		TalosClusterRef: v1alpha2.TalosClusterReference{Name: cluster.Name},
		ReleaseName:     externalDNSReleaseName,
		Values:          values,
	}

	err = controllerutil.SetControllerReference(cluster, seed, hubClient.Scheme())
	if err != nil {
		return fmt.Errorf("failed to set owner reference on addon %q: %w", name, err)
	}

	err = hubClient.Create(ctx, seed)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create addon %q: %w", name, err)
	}

	return nil
}

// cloudflareProviderValues, cloudflareEnvVar, cloudflareEnvValueFrom, and
// cloudflareSecretKeyRef are cloudflareAddonValues' own typed mirror of the
// slice of the external-dns chart's own values.yaml shape it needs to set
// (provider.name, env[].valueFrom.secretKeyRef) — typed structs rather than
// map[string]any purely so every "name" field (three different meanings:
// the provider's name, the env var's name, the Secret's name) is a distinct
// Go struct field instead of the same repeated string literal.
type (
	cloudflareProviderValues struct {
		Name string `json:"name"`
	}

	cloudflareEnvVar struct {
		Name      string                 `json:"name"`
		ValueFrom cloudflareEnvValueFrom `json:"valueFrom"`
	}

	cloudflareEnvValueFrom struct {
		SecretKeyRef cloudflareSecretKeyRef `json:"secretKeyRef"`
	}

	cloudflareSecretKeyRef struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	}
)

// cloudflareAddonValues returns the Helm values
// ensureExternalDNSCloudflareAddonSeed seeds a Cloudflare external-dns
// Addon with — merged on top of values/external-dns.yaml's own embedded
// crd-source scaffolding by resolveAddon (see pkg/domain/addon), since this
// Addon's own ReleaseName matches addonNamesWithDefaults' entry for it:
//
//   - provider.name selects external-dns' own Cloudflare provider.
//   - env wires CF_API_TOKEN, the environment variable external-dns'
//     Cloudflare provider reads (see
//     https://github.com/kubernetes-sigs/external-dns/blob/master/docs/tutorials/cloudflare.md),
//     from ensureExternalDNSCredentialSecret's own Secret via
//     valueFrom.secretKeyRef — the credential itself never touches this
//     Addon's own spec.values (stored unencrypted on the hub's Addon
//     object), only a reference to where it actually lives.
func cloudflareAddonValues() (*apiextensionsv1.JSON, error) {
	raw, err := json.Marshal(struct {
		Provider cloudflareProviderValues `json:"provider"`
		Env      []cloudflareEnvVar       `json:"env"`
	}{
		Provider: cloudflareProviderValues{Name: dnsProviderCloudflare},
		Env: []cloudflareEnvVar{{
			Name: "CF_API_TOKEN",
			ValueFrom: cloudflareEnvValueFrom{
				SecretKeyRef: cloudflareSecretKeyRef{
					Name: externalDNSCredentialSecretName,
					Key:  externalDNSCredentialSecretKey,
				},
			},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal external-dns addon values: %w", err)
	}

	return &apiextensionsv1.JSON{Raw: raw}, nil
}
