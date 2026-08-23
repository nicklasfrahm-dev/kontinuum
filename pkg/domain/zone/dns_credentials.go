package zone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// externalDNSReleaseName duplicates pkg/domain/addon's own (unexported)
// externalDNSAddonName — same rationale as this package's other
// cross-package literal duplications (see storageSecretKey's own doc):
// matching it is what lets resolveAddon's embedded values/external-dns.yaml
// defaults (crd-source scaffolding) apply to the Addon
// ensureExternalDNSAddon creates below.
const externalDNSReleaseName = "external-dns"

// externalDNSCredentialSecretName derives the per-cluster credential
// Secret ensureExternalDNSCredentialSecret upserts and
// externalDNSAddonValues' own env references point at — namespaced and
// named the same way ensureExternalDNSAddon's own Addon CR is (see
// addonResourceName's identical convention in pkg/domain/addon), so two
// zones sharing a downstream namespace (not possible today, but not
// assumed against either) would never collide.
func externalDNSCredentialSecretName(clusterName string) string {
	return clusterName + "-external-dns-credentials"
}

// errDNSCredentialEmpty and errDNSCredentialNotFlat are
// parseDNSCredentialKeys' own sentinels — err113 flags a dynamically
// constructed errors.New/fmt.Errorf call without a wrapped static error.
var (
	errDNSCredentialEmpty   = errors.New("dns credential yaml has no top-level keys")
	errDNSCredentialNotFlat = errors.New("dns credential yaml must be a flat mapping of string keys to string values")
)

// parseDNSCredentialKeys parses KONTINUUM_SERVER_DNS_CREDENTIAL as a flat
// YAML mapping — each top-level key/value becomes one key in the Secret
// ensureExternalDNSCredentialSecret upserts, and one env var in the Addon
// externalDNSAddonValues seeds (see that function's own doc for why: the
// external-dns chart has no envFrom/existingSecret support of its own —
// see this package's own doc for the mechanism this builds on top of
// instead). The credential is deliberately never kontinuum's own encoding
// of "the provider's own native format" (see
// v1alpha2.KontinuumDNSConfigStatus.Credential's own doc): whatever keys
// are set here are exactly the environment variable names the operator's
// chosen external-dns provider implementation expects — CF_API_TOKEN for
// Cloudflare, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY for Route53, and so
// on — see docs/workflows/zone-add.md's own examples, including the
// providers (Azure DNS, Google Cloud DNS) whose credential is a mounted
// file rather than plain env vars, which this mechanism doesn't cover. A
// value that isn't a plain string (a nested mapping/list) fails with
// errDNSCredentialNotFlat — "flat" isn't just documentation, yaml.Unmarshal
// into map[string]string enforces it directly.
func parseDNSCredentialKeys(credential string) (map[string]string, error) {
	var keys map[string]string

	err := yaml.Unmarshal([]byte(credential), &keys)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDNSCredentialNotFlat, err)
	}

	if len(keys) == 0 {
		return nil, errDNSCredentialEmpty
	}

	return keys, nil
}

// reconcileExternalDNSAddon auto-wires an external-dns Addon for cluster
// once findKontinuumDNSConfig reports the hub has both a DNS provider and
// credential configured — review feedback on issue #51's own "automatically
// wire the credentials" point. credential is parsed as flat YAML (see
// parseDNSCredentialKeys); each key becomes one entry in a Secret named
// externalDNSCredentialSecretName(cluster.Name) on the zone's own
// downstream cluster, and one env var in an Addon named
// "<cluster>-external-dns" (owned by cluster, so it's garbage-collected on
// teardown like every other Addon a TalosCluster gets — see
// ensureExternalDNSAddon's own doc). provider is passed through untouched
// as that Addon's own provider.name — kontinuum has no per-provider Go
// code of its own to maintain here, so this works for any external-dns
// provider whose credential is expressed purely as environment variables
// (Cloudflare, Route53, ...). A provider needing a mounted file instead of
// env vars (Azure DNS's azure.json, Google Cloud DNS's
// GOOGLE_APPLICATION_CREDENTIALS, notably) isn't covered by this
// mechanism — see docs/workflows/zone-add.md's own Azure/GCP examples for
// the operator-managed alternative.
func (r *Reconciler) reconcileExternalDNSAddon(
	ctx context.Context, cluster *v1alpha2.TalosCluster, downstream client.Client, provider, credential string,
) error {
	keys, err := parseDNSCredentialKeys(credential)
	if err != nil {
		return fmt.Errorf("failed to parse dns credential for zone %q: %w", cluster.Name, err)
	}

	secretName := externalDNSCredentialSecretName(cluster.Name)

	err = ensureExternalDNSCredentialSecret(ctx, downstream, downstreamNamespace, secretName, keys)
	if err != nil {
		return err
	}

	return ensureExternalDNSAddon(ctx, r.Client, cluster, provider, secretName, keys)
}

// ensureExternalDNSCredentialSecret upserts the Secret
// ensureExternalDNSAddon's own seeded Addon values reference — mirrors
// workload.go's ensureSecret create-then-get-and-update-on-conflict upsert
// idiom, continuously kept in sync (unlike ensureSecret, this fully
// replaces both StringData and Data on every call — not just StringData —
// since keys' own key set can shrink between reconciles, e.g. an operator
// removing a now-unused credential field, and a stale leftover Data key
// would otherwise linger forever) so a rotated
// KONTINUUM_SERVER_DNS_CREDENTIAL propagates to the running external-dns
// pod without any operator intervention.
func ensureExternalDNSCredentialSecret(
	ctx context.Context, downstream client.Client, namespace, secretName string, keys map[string]string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		StringData: keys,
	}

	err := downstream.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Secret

		err = downstream.Get(ctx, client.ObjectKeyFromObject(secret), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q secret: %w", secretName, err)
		}

		existing.Data = nil
		existing.StringData = keys

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q secret: %w", secretName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q secret: %w", secretName, err)
	}

	return nil
}

// deleteExternalDNSCredentialSecret deletes the Secret
// ensureExternalDNSCredentialSecret upserts, tolerating NotFound — see
// teardown.go's own doc for why every deleteX helper is idempotent the
// same way its ensureX counterpart already is. Called unconditionally
// during teardown (not just when a DNS provider was ever configured):
// idempotent either way, and cheaper than threading the hub's own current
// DNS configuration state through teardownDownstream just to skip a no-op
// delete.
func deleteExternalDNSCredentialSecret(
	ctx context.Context, downstream client.Client, namespace, clusterName string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: externalDNSCredentialSecretName(clusterName), Namespace: namespace},
	}

	err := client.IgnoreNotFound(downstream.Delete(ctx, secret))
	if err != nil {
		return fmt.Errorf("failed to delete %q secret: %w", secret.Name, err)
	}

	return nil
}

// ensureExternalDNSAddon upserts an external-dns Addon CR for cluster,
// namespaced alongside it (mirrors pkg/domain/addon.EnsureBuiltinSeeds' own
// identical convention — see addonResourceName's own doc for why),
// creating it on the first call and refreshing only its own spec.values on
// every one after — unlike pkg/domain/addon.ensureBuiltinAddonSeed's
// deliberately create-only built-ins (an operator hand-tuning cilium/
// cert-manager's own chart version or extra flags is expected and left
// alone), this Addon's values are entirely kontinuum-generated from
// provider/keys, not something an operator is expected to hand-edit — and
// leaving a stale env var list in place after a credential key set changes
// (a removed key left dangling as a secretKeyRef Kubernetes can't resolve,
// or a newly added key the running pod never picks up) would silently
// break DNS management rather than just look untidy. spec.TalosClusterRef/
// spec.ReleaseName never change after creation, so only spec.Values is
// touched on update — any other field an operator sets directly on this
// Addon (a pinned chart version, spec.namespace) survives untouched.
//
// Owned by cluster via a real owner reference, exactly like every other
// Addon a TalosCluster gets seeded (see ensureBuiltinAddonSeed's own doc)
// — deleting the TalosCluster during Zone teardown garbage-collects this
// Addon for free, with no explicit cleanup needed here.
func ensureExternalDNSAddon(
	ctx context.Context, hubClient client.Client, cluster *v1alpha2.TalosCluster, provider, secretName string,
	keys map[string]string,
) error {
	name := cluster.Name + "-" + externalDNSReleaseName

	values, err := externalDNSAddonValues(provider, secretName, keys)
	if err != nil {
		return fmt.Errorf("failed to build external-dns addon values: %w", err)
	}

	addon := &v1alpha2.Addon{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}

	err = hubClient.Get(ctx, client.ObjectKeyFromObject(addon), addon)
	if apierrors.IsNotFound(err) {
		addon.Spec = v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: cluster.Name},
			ReleaseName:     externalDNSReleaseName,
			Values:          values,
		}

		err = controllerutil.SetControllerReference(cluster, addon, hubClient.Scheme())
		if err != nil {
			return fmt.Errorf("failed to set owner reference on addon %q: %w", name, err)
		}

		err = hubClient.Create(ctx, addon)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create addon %q: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to check for existing addon %q: %w", name, err)
	}

	addon.Spec.Values = values

	err = hubClient.Update(ctx, addon)
	if err != nil {
		return fmt.Errorf("failed to update addon %q: %w", name, err)
	}

	return nil
}

// externalDNSProviderValues, externalDNSEnvVar, externalDNSEnvValueFrom,
// and externalDNSSecretKeyRef are externalDNSAddonValues' own typed mirror
// of the slice of the external-dns chart's own values.yaml shape it needs
// to set (provider.name, env[].valueFrom.secretKeyRef) — typed structs
// rather than map[string]any purely so every "name" field (three different
// meanings: the provider's name, an env var's name, the Secret's name) is
// a distinct Go struct field instead of the same repeated string literal.
type (
	externalDNSProviderValues struct {
		Name string `json:"name"`
	}

	externalDNSEnvVar struct {
		Name      string                  `json:"name"`
		ValueFrom externalDNSEnvValueFrom `json:"valueFrom"`
	}

	externalDNSEnvValueFrom struct {
		SecretKeyRef externalDNSSecretKeyRef `json:"secretKeyRef"`
	}

	externalDNSSecretKeyRef struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	}
)

// externalDNSAddonValues returns the Helm values ensureExternalDNSAddon
// seeds/refreshes an external-dns Addon with — merged on top of
// values/external-dns.yaml's own embedded crd-source scaffolding by
// resolveAddon (see pkg/domain/addon), since this Addon's own ReleaseName
// matches addonNamesWithDefaults' entry for it:
//
//   - provider.name selects external-dns' own <provider> implementation —
//     passed through from KONTINUUM_SERVER_DNS_PROVIDER untouched (see
//     v1alpha2.KontinuumDNSConfigStatus.Provider's own doc).
//   - env carries one entry per key in keys (sorted by name, purely for a
//     deterministic Values encoding — a map iterates in random order in
//     Go), each wiring that provider's own expected env var (e.g.
//     CF_API_TOKEN) from secretName via valueFrom.secretKeyRef — see
//     parseDNSCredentialKeys' own doc for why the credential's own keys
//     are exactly the env var names, not something kontinuum translates.
//     The credential values themselves never touch this Addon's own
//     spec.values (stored unencrypted on the hub's Addon object), only a
//     reference to where they actually live.
func externalDNSAddonValues(provider, secretName string, keys map[string]string) (*apiextensionsv1.JSON, error) {
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}

	sort.Strings(names)

	env := make([]externalDNSEnvVar, 0, len(names))
	for _, name := range names {
		env = append(env, externalDNSEnvVar{
			Name: name,
			ValueFrom: externalDNSEnvValueFrom{
				SecretKeyRef: externalDNSSecretKeyRef{Name: secretName, Key: name},
			},
		})
	}

	raw, err := json.Marshal(struct {
		Provider externalDNSProviderValues `json:"provider"`
		Env      []externalDNSEnvVar       `json:"env"`
	}{
		Provider: externalDNSProviderValues{Name: provider},
		Env:      env,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal external-dns addon values: %w", err)
	}

	return &apiextensionsv1.JSON{Raw: raw}, nil
}
