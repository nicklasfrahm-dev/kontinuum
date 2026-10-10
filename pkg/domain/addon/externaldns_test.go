package addon_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

const (
	externalDNSTestAddonName  = resourcesTestClusterName + "-external-dns"
	externalDNSTestSecretName = resourcesTestClusterName + "-external-dns-credentials"
	pdnsServerEnv             = "EXTERNAL_DNS_PDNS_SERVER"
	pdnsAuthEnv               = "EXTERNAL_DNS_PDNS_API_KEY"
)

func externalDNSFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// externalDNSTestValues is the slice of the external-dns Addon's values the
// tests below assert on.
type externalDNSTestValues struct {
	Provider struct {
		Name string `json:"name"`
	} `json:"provider"`
	Env []struct {
		Name      string `json:"name"`
		ValueFrom struct {
			SecretKeyRef struct {
				Name string `json:"name"`
				Key  string `json:"key"`
			} `json:"secretKeyRef"`
		} `json:"valueFrom"`
	} `json:"env"`
}

func getExternalDNSSeed(t *testing.T, kubeClient client.Client) (v1alpha2.Addon, corev1.Secret, externalDNSTestValues) {
	t.Helper()

	var seed v1alpha2.Addon
	require.NoError(t, kubeClient.Get(context.Background(),
		client.ObjectKey{Name: externalDNSTestAddonName, Namespace: resourcesTestNamespace}, &seed))

	var secret corev1.Secret
	require.NoError(t, kubeClient.Get(context.Background(),
		client.ObjectKey{Name: externalDNSTestSecretName, Namespace: resourcesTestNamespace}, &secret))

	var values externalDNSTestValues

	require.NotNil(t, seed.Spec.Values)
	require.NoError(t, json.Unmarshal(seed.Spec.Values.Raw, &values))

	return seed, secret, values
}

// TestEnsureExternalDNSSeedWiresCredentialThroughSecret covers Cloudflare's
// one-key credential: the key lands in a hub Secret the Addon lists under
// spec.secrets (so the addon controller copies it onto the cluster), and the
// Addon's values reference it by secretKeyRef rather than containing it.
func TestEnsureExternalDNSSeedWiresCredentialThroughSecret(t *testing.T) {
	t.Parallel()

	cluster := resourcesTestCluster(resourcesTestNamespace)
	kubeClient := externalDNSFakeClient(t, cluster)

	require.NoError(t, addon.EnsureExternalDNSSeed(context.Background(), kubeClient, cluster,
		"cloudflare", "CF_API_TOKEN: example-token\n"))

	seed, secret, values := getExternalDNSSeed(t, kubeClient)

	assert.Equal(t, "example-token", string(secret.Data["CF_API_TOKEN"]))
	assert.True(t, metav1.IsControlledBy(&secret, cluster), "secret must be owned by its TalosCluster")

	assert.Equal(t, resourcesTestClusterName, seed.Spec.TalosClusterRef.Name)
	assert.Equal(t, "external-dns", seed.Spec.ReleaseName)
	assert.Equal(t, v1alpha2.KontinuumSystemNamespace, seed.Spec.Namespace.Name,
		"the release must install next to the copied credential Secret")
	assert.Equal(t, []v1alpha2.AddonSecretReference{{Name: externalDNSTestSecretName}}, seed.Spec.Secrets)
	assert.True(t, metav1.IsControlledBy(&seed, cluster), "addon must be owned by its TalosCluster")
	assert.NotContains(t, string(seed.Spec.Values.Raw), "example-token", "values must only reference the credential")

	assert.Equal(t, "cloudflare", values.Provider.Name)
	require.Len(t, values.Env, 1)
	assert.Equal(t, "CF_API_TOKEN", values.Env[0].Name)
	assert.Equal(t, externalDNSTestSecretName, values.Env[0].ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, "CF_API_TOKEN", values.Env[0].ValueFrom.SecretKeyRef.Key)
}

// TestEnsureExternalDNSSeedTracksCredentialKeys covers PowerDNS's two-key
// credential, then a second call with one key removed: both the Secret and
// the Addon's env list must follow, leaving no stale key behind.
func TestEnsureExternalDNSSeedTracksCredentialKeys(t *testing.T) {
	t.Parallel()

	cluster := resourcesTestCluster(resourcesTestNamespace)
	kubeClient := externalDNSFakeClient(t, cluster)

	require.NoError(t, addon.EnsureExternalDNSSeed(context.Background(), kubeClient, cluster, "pdns",
		pdnsServerEnv+": http://pdns:8081\n"+pdnsAuthEnv+": example-api-key\n"))

	_, secret, values := getExternalDNSSeed(t, kubeClient)
	assert.Equal(t, "http://pdns:8081", string(secret.Data[pdnsServerEnv]))
	assert.Equal(t, "example-api-key", string(secret.Data[pdnsAuthEnv]))
	require.Len(t, values.Env, 2)
	assert.Equal(t, pdnsAuthEnv, values.Env[0].Name, "env is sorted by name")
	assert.Equal(t, pdnsServerEnv, values.Env[1].Name)

	require.NoError(t, addon.EnsureExternalDNSSeed(context.Background(), kubeClient, cluster, "pdns",
		pdnsServerEnv+": http://pdns:8081\n"))

	_, secret, values = getExternalDNSSeed(t, kubeClient)
	assert.Equal(t, map[string][]byte{pdnsServerEnv: []byte("http://pdns:8081")}, secret.Data)
	require.Len(t, values.Env, 1)
	assert.Equal(t, pdnsServerEnv, values.Env[0].Name)
}

// TestEnsureExternalDNSSeedRejectsNonFlatCredential covers a credential that
// isn't a flat YAML mapping: it fails with ErrInvalidDNSCredential (which the
// taloscluster controller logs and skips) and nothing is created.
func TestEnsureExternalDNSSeedRejectsNonFlatCredential(t *testing.T) {
	t.Parallel()

	cluster := resourcesTestCluster(resourcesTestNamespace)
	kubeClient := externalDNSFakeClient(t, cluster)

	err := addon.EnsureExternalDNSSeed(context.Background(), kubeClient, cluster,
		"cloudflare", "CF_API_TOKEN:\n  nested: not-a-string\n")
	require.ErrorIs(t, err, addon.ErrInvalidDNSCredential)

	err = kubeClient.Get(context.Background(),
		client.ObjectKey{Name: externalDNSTestAddonName, Namespace: resourcesTestNamespace}, &v1alpha2.Addon{})
	assert.True(t, apierrors.IsNotFound(err), "no addon must be seeded from an invalid credential")
}
