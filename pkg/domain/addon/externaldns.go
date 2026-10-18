package addon

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

// ErrInvalidDNSCredential is ParseDNSCredentialKeys' sentinel for a
// KONTINUUM_SERVER_DNS_CREDENTIAL that isn't a non-empty flat YAML mapping
// of string keys to string values.
var ErrInvalidDNSCredential = errors.New("dns credential must be a non-empty flat yaml mapping of strings")

// ExternalDNSCredentialSecretName is the Secret holding cluster's DNS
// credential keys — on the hub, where EnsureExternalDNSSeed writes it, and
// on the cluster, where the addon controller copies it (see
// AddonSpec.Secrets).
func ExternalDNSCredentialSecretName(clusterName string) string {
	return clusterName + "-external-dns-credentials"
}

// ParseDNSCredentialKeys parses KONTINUUM_SERVER_DNS_CREDENTIAL as a flat
// YAML mapping. Each key is an environment variable the chosen external-dns
// provider reads (CF_API_TOKEN for Cloudflare, EXTERNAL_DNS_PDNS_SERVER/
// _API_KEY for PowerDNS, ...) — kontinuum never interprets them. "Flat" is
// enforced by unmarshalling into map[string]string: a nested value fails.
func ParseDNSCredentialKeys(credential string) (map[string]string, error) {
	var keys map[string]string

	err := yaml.Unmarshal([]byte(credential), &keys)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDNSCredential, err)
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: no keys", ErrInvalidDNSCredential)
	}

	return keys, nil
}

// EnsureExternalDNSSeed seeds cluster's external-dns Addon from the hub's DNS
// provider and credential, alongside the built-in addons (see
// EnsureBuiltinSeeds) so it installs in the same wave rather than after the
// cluster is already Ready. The credential's keys go into a hub Secret the
// Addon lists under spec.secrets, which the addon controller copies onto the
// cluster before installing; the Addon's values only reference it, so the
// credential never appears in spec.values.
//
// Unlike the create-only built-ins, both objects are kept in sync on every
// call: they're entirely generated from the hub's config, and a stale env
// list after the credential's key set changes would silently break DNS.
// Both are owned by cluster, so they're garbage-collected with it.
func EnsureExternalDNSSeed(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster, provider, credential string,
) error {
	keys, err := ParseDNSCredentialKeys(credential)
	if err != nil {
		return err
	}

	secretName := ExternalDNSCredentialSecretName(cluster.Name)

	err = ensureExternalDNSCredentialSecret(ctx, kubeClient, cluster, secretName, keys)
	if err != nil {
		return err
	}

	values, err := externalDNSAddonValues(provider, secretName, keys)
	if err != nil {
		return err
	}

	return ensureExternalDNSAddon(ctx, kubeClient, cluster, secretName, values)
}

// ensureExternalDNSCredentialSecret upserts the hub Secret holding keys,
// replacing its Data entirely so a key removed from the credential doesn't
// linger.
func ensureExternalDNSCredentialSecret(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster, name string,
	keys map[string]string,
) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, kubeClient, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.StringData = nil
		secret.Data = make(map[string][]byte, len(keys))

		for key, value := range keys {
			secret.Data[key] = []byte(value)
		}

		return controllerutil.SetControllerReference(cluster, secret, kubeClient.Scheme())
	})
	if err != nil {
		return fmt.Errorf("failed to ensure %q secret: %w", name, err)
	}

	return nil
}

// ensureExternalDNSAddon upserts cluster's external-dns Addon. Namespace,
// priority, and provisioning method are baked in from
// values/external-dns.yaml's defaults, the same way ensureBuiltinAddonSeed
// does, so the object shows where and when it actually installs. The
// release must land in the same namespace as the copied credential Secret,
// since its env secretKeyRefs can't cross namespaces.
func ensureExternalDNSAddon(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster, secretName string,
	values *apiextensionsv1.JSON,
) error {
	def, err := loadAddonDefaults(externalDNSAddonName)
	if err != nil {
		return fmt.Errorf("failed to load defaults for addon %q: %w", externalDNSAddonName, err)
	}

	priority := def.Lifecycle.Priority

	method := def.Lifecycle.Provisioning.Method
	if method == "" {
		method = v1alpha2.AddonProvisioningMethodHelmUpgradeInstall
	}

	addon := &v1alpha2.Addon{ObjectMeta: metav1.ObjectMeta{
		Name:      addonResourceName(cluster.Name, externalDNSAddonName),
		Namespace: cluster.Namespace,
	}}

	_, err = controllerutil.CreateOrUpdate(ctx, kubeClient, addon, func() error {
		addon.Spec.TalosClusterRef = v1alpha2.TalosClusterReference{Name: cluster.Name}
		addon.Spec.ReleaseName = externalDNSAddonName
		addon.Spec.Namespace = def.Namespace
		addon.Spec.Lifecycle = v1alpha2.AddonLifecycleSpec{
			Priority:     &priority,
			Provisioning: v1alpha2.AddonProvisioningSpec{Method: method},
		}
		addon.Spec.Secrets = []v1alpha2.AddonSecretReference{{Name: secretName}}
		addon.Spec.Values = values

		return controllerutil.SetControllerReference(cluster, addon, kubeClient.Scheme())
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure addon %q: %w", addon.Name, err)
	}

	return nil
}

// externalDNSProviderValues, externalDNSEnvVar, externalDNSEnvValueFrom, and
// externalDNSSecretKeyRef type the slice of the external-dns chart's values
// externalDNSAddonValues sets (provider.name, env[].valueFrom.secretKeyRef).
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

// externalDNSAddonValues returns the external-dns Addon's values: provider.name
// translated from KONTINUUM_SERVER_DNS_PROVIDER (see ExternalDNSProviderName), and one env entry per credential key
// (sorted, for a deterministic encoding) read from secretName via
// secretKeyRef. The chart has no envFrom/existingSecret support, hence one
// entry per key.
func externalDNSAddonValues(provider, secretName string, keys map[string]string) (*apiextensionsv1.JSON, error) {
	providerName, err := ExternalDNSProviderName(provider)
	if err != nil {
		return nil, err
	}

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
		Provider: externalDNSProviderValues{Name: providerName},
		Env:      env,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal external-dns addon values: %w", err)
	}

	return &apiextensionsv1.JSON{Raw: raw}, nil
}
