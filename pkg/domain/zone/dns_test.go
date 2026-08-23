package zone_test

import (
	"encoding/json"
	"log/slog"
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

// testHostname is the hostname every test in this file expects
// ensureDNSEndpoint to have been asked to point at — testZone + "." +
// testRegion + "." + testDomain, matching reconcileInstall's own hostname
// computation.
const testHostname = testZone + "." + testRegion + "." + testDomain

// testGatewayIP is a fake downstream Gateway address, not a real one.
const testGatewayIP = "203.0.113.10"

// testDNSProviderRoute53 is a stand-in for any provider other than
// "cloudflare" — used by every test in this file that only cares about
// reconcileDNS/DNSEndpoint mechanics (provider-agnostic), not
// reconcileExternalDNSAddon's own cloudflare-specific wiring, so those
// tests aren't also on the hook for asserting the Cloudflare Addon/Secret
// never appear.
const testDNSProviderRoute53 = "route53"

// dnsEndpointKey is where reconcileDNS's own ensureDNSEndpoint upserts its
// single DNSEndpoint — "kontinuum" in "kontinuum-system", same fixed naming
// convention as network.go's gatewayName/certificateName/httpRouteName.
func dnsEndpointKey() client.ObjectKey {
	return client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}
}

// TestReconcileInstalledWaitsForDNSConfiguration covers point 2 of the
// PR #73 review: with spec.domain set but no DNS provider/credential
// configured anywhere, installNetwork must never run at all — no
// ClusterIssuer/Gateway/Certificate/HTTPRoute is created, and Installed
// stays False/WaitingForDNSConfiguration rather than reaching True with a
// Certificate that has no way to ever finish issuing.
func TestReconcileInstalledWaitsForDNSConfiguration(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	installed := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, installed)
	assert.Equal(t, metav1.ConditionFalse, installed.Status)
	assert.Equal(t, "WaitingForDNSConfiguration", installed.Reason)

	ready := meta.FindStatusCondition(got.Status.Conditions, zone.ReadyConditionType)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gatewayv1.Gateway{})
	assert.True(t, apierrors.IsNotFound(err), "no Gateway must be created before dns is configured")

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace},
		&certmanagerv1.Certificate{})
	assert.True(t, apierrors.IsNotFound(err), "no Certificate must be created before dns is configured")
}

// TestReconcileDNSWaitsForGatewayAddress covers a DNS provider/credential
// configured but the downstream Gateway not yet assigned an address (its
// own LoadBalancer implementation hasn't finished) — DNSRecordConditionType
// reports that instead of erroring, and no DNSEndpoint is created yet.
func TestReconcileDNSWaitsForGatewayAddress(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS(testDNSProviderRoute53)
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.DNSRecordConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "WaitingForGatewayAddress", cond.Reason)

	err = downstream.Get(t.Context(), dnsEndpointKey(), &zone.DNSEndpoint{})
	assert.True(t, apierrors.IsNotFound(err), "no DNSEndpoint must be created yet")
}

// TestReconcileCreatesDNSEndpointOnceGatewayHasIPAddress covers the full
// happy path: once the downstream Gateway has an IPAddress-typed address,
// reconcileDNS upserts a DNSEndpoint requesting an A record for this zone's
// own hostname pointing at it, and DNSRecordConditionType flips True.
func TestReconcileCreatesDNSEndpointOnceGatewayHasIPAddress(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS(testDNSProviderRoute53)
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	// First pass creates the Gateway (no address yet).
	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gateway))

	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{
		{Type: new(gatewayv1.IPAddressType), Value: testGatewayIP},
	}
	require.NoError(t, downstream.Update(t.Context(), &gateway))

	// Second pass sees the address and upserts the DNSEndpoint.
	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var endpoint zone.DNSEndpoint
	require.NoError(t, downstream.Get(t.Context(), dnsEndpointKey(), &endpoint))
	require.Len(t, endpoint.Spec.Endpoints, 1)
	assert.Equal(t, testHostname, endpoint.Spec.Endpoints[0].DNSName)
	assert.Equal(t, []string{testGatewayIP}, endpoint.Spec.Endpoints[0].Targets)
	assert.Equal(t, "A", endpoint.Spec.Endpoints[0].RecordType)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.DNSRecordConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "DNSRecordCreated", cond.Reason)
}

// TestReconcileCreatesDNSEndpointForHostnameAddress covers a Hostname-typed
// Gateway address (e.g. a cloud load balancer that publishes a DNS name
// rather than a static IP) — ensureDNSEndpoint must request a CNAME, not an
// A record, in that case.
func TestReconcileCreatesDNSEndpointForHostnameAddress(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS(testDNSProviderRoute53)
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gateway))

	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{
		{Type: new(gatewayv1.HostnameAddressType), Value: "lb-1234.us-east-1.elb.amazonaws.com"},
	}
	require.NoError(t, downstream.Update(t.Context(), &gateway))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var endpoint zone.DNSEndpoint
	require.NoError(t, downstream.Get(t.Context(), dnsEndpointKey(), &endpoint))
	require.Len(t, endpoint.Spec.Endpoints, 1)
	assert.Equal(t, []string{"lb-1234.us-east-1.elb.amazonaws.com"}, endpoint.Spec.Endpoints[0].Targets)
	assert.Equal(t, "CNAME", endpoint.Spec.Endpoints[0].RecordType)
}

// TestReconcileTeardownDeletesDNSEndpoint covers teardownDownstream's own
// reverse-of-install ordering (see that function's doc): a DNSEndpoint
// reconcileDNS ever created must be deleted along with every other
// downstream object once the Zone itself is deleted.
func TestReconcileTeardownDeletesDNSEndpoint(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS(testDNSProviderRoute53)
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{client: downstream},
		HubConfig:               testHubConfig(),
		ImageRepo:               testImageRepo,
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Locker:                  zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
		Logger:                  slog.Default(),
	}

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gateway))

	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{
		{Type: new(gatewayv1.IPAddressType), Value: testGatewayIP},
	}
	require.NoError(t, downstream.Update(t.Context(), &gateway))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	require.NoError(t, downstream.Get(t.Context(), dnsEndpointKey(), &zone.DNSEndpoint{}),
		"install pass must have created the DNSEndpoint")

	var toDelete v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &toDelete))
	require.NoError(t, hubClient.Delete(t.Context(), &toDelete))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	err = downstream.Get(t.Context(), dnsEndpointKey(), &zone.DNSEndpoint{})
	assert.True(t, apierrors.IsNotFound(err), "DNSEndpoint must be deleted on teardown")
}

// externalDNSAddonKey and externalDNSCredentialSecretKey are
// reconcileExternalDNSAddon's own object names/namespaces — see
// dns_credentials.go's own externalDNSCredentialSecretName/
// externalDNSReleaseName doc for why these are recomputed here rather than
// exported just for tests.
func externalDNSAddonKey() client.ObjectKey {
	return client.ObjectKey{Name: testZoneName + "-external-dns", Namespace: v1alpha2.KontinuumSystemNamespace}
}

func externalDNSCredentialSecretKey() client.ObjectKey {
	return client.ObjectKey{Name: testZoneName + "-external-dns-credentials", Namespace: testDownstreamNamespace}
}

// addonEnvSecretKeyRef extracts the secretKeyRef one env var name's own
// valueFrom resolves to, out of an Addon's raw spec.values.env — a small
// helper so the tests below can assert against values without hand-rolling
// the same JSON-unmarshal-and-walk more than once.
func addonEnvSecretKeyRef(t *testing.T, addon v1alpha2.Addon, envName string) (string, string) {
	t.Helper()

	var values struct {
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

	require.NoError(t, json.Unmarshal(addon.Spec.Values.Raw, &values))

	for _, env := range values.Env {
		if env.Name == envName {
			return env.ValueFrom.SecretKeyRef.Name, env.ValueFrom.SecretKeyRef.Key
		}
	}

	t.Fatalf("addon %q values has no env entry named %q", addon.Name, envName)

	return "", ""
}

// TestReconcileWiresExternalDNSAddonFromDNSCredential covers point 1 of the
// PR #73 review: a DNS provider/credential configured on the hub gets a
// working external-dns install with no Addon of the operator's own to
// write. testDNSCredential (see controller_test.go) is flat YAML with one
// key, CF_API_TOKEN — Cloudflare's own expected env var name — proving the
// full chain: that key lands in a Secret on the zone's own downstream
// cluster, and the seeded Addon on the hub (owned by the zone's own
// TalosCluster) wires that same key as an env var via secretKeyRef.
func TestReconcileWiresExternalDNSAddonFromDNSCredential(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS("cloudflare")
	cluster := readyTalosCluster()
	hubClient := newHubFakeClient(t, testZoneObject(), cluster, kubeconfigSecret(), kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(), externalDNSCredentialSecretKey(), &secret))
	assert.Equal(t, testDNSCredentialValue, string(secret.Data["CF_API_TOKEN"]))

	var addon v1alpha2.Addon
	require.NoError(t, hubClient.Get(t.Context(), externalDNSAddonKey(), &addon))
	assert.Equal(t, testZoneName, addon.Spec.TalosClusterRef.Name)
	assert.Equal(t, "external-dns", addon.Spec.ReleaseName)
	require.NotNil(t, addon.Spec.Values)

	var values map[string]any
	require.NoError(t, json.Unmarshal(addon.Spec.Values.Raw, &values))
	provider, ok := values["provider"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "cloudflare", provider["name"])

	secretName, key := addonEnvSecretKeyRef(t, addon, "CF_API_TOKEN")
	assert.Equal(t, externalDNSCredentialSecretKey().Name, secretName)
	assert.Equal(t, "CF_API_TOKEN", key)

	require.Len(t, addon.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, addon.OwnerReferences[0].Name)
	assert.Equal(t, "TalosCluster", addon.OwnerReferences[0].Kind)
}

// TestReconcileWiresExternalDNSAddonWithMultipleCredentialKeys covers a
// provider needing more than one credential value — Route53's own
// AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY pair — proving
// parseDNSCredentialKeys' flat-YAML-to-many-keys mapping isn't
// special-cased to Cloudflare's own single key. Not every multi-value
// provider fits this env-var-only mechanism, though — see
// docs/workflows/zone-add.md's own Azure/GCP examples for two that need a
// mounted credentials file instead, which this mechanism doesn't cover.
func TestReconcileWiresExternalDNSAddonWithMultipleCredentialKeys(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	kontinuum.Status.Config.Server.DNS.Provider = "aws"
	kontinuumSecret.Data["KONTINUUM_SERVER_DNS_CREDENTIAL"] = []byte(
		"AWS_ACCESS_KEY_ID: AKIAEXAMPLE\nAWS_SECRET_ACCESS_KEY: example-secret-key\n")
	cluster := readyTalosCluster()
	hubClient := newHubFakeClient(t, testZoneObject(), cluster, kubeconfigSecret(), kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(), externalDNSCredentialSecretKey(), &secret))
	assert.Equal(t, "AKIAEXAMPLE", string(secret.Data["AWS_ACCESS_KEY_ID"]))
	assert.Equal(t, "example-secret-key", string(secret.Data["AWS_SECRET_ACCESS_KEY"]))

	var addon v1alpha2.Addon
	require.NoError(t, hubClient.Get(t.Context(), externalDNSAddonKey(), &addon))

	var values map[string]any
	require.NoError(t, json.Unmarshal(addon.Spec.Values.Raw, &values))
	provider, ok := values["provider"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "aws", provider["name"])

	for _, envName := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		secretName, key := addonEnvSecretKeyRef(t, addon, envName)
		assert.Equal(t, externalDNSCredentialSecretKey().Name, secretName)
		assert.Equal(t, envName, key)
	}
}

// TestReconcileInstalledWaitsForDNSConfigurationWhenCredentialNotFlatYAML
// covers a DNS provider/credential configured, but the credential isn't
// parseable as a flat YAML mapping (see parseDNSCredentialKeys) — a nested
// value, or plain unstructured text left over from before this mechanism
// existed. Surfaced as Installed=False/InstallFailed, same as any other
// downstream API failure — not silently ignored, and not treated as
// "DNS isn't configured" (findKontinuumDNSConfig already succeeded).
func TestReconcileInstalledWaitsForDNSConfigurationWhenCredentialNotFlatYAML(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	kontinuum.Status.Config.Server.DNS.Provider = "cloudflare"
	kontinuumSecret.Data["KONTINUUM_SERVER_DNS_CREDENTIAL"] = []byte("CF_API_TOKEN:\n  nested: not-a-string\n")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	installed := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, installed)
	assert.Equal(t, metav1.ConditionFalse, installed.Status)
	assert.Equal(t, "InstallFailed", installed.Reason)

	err = downstream.Get(t.Context(), externalDNSCredentialSecretKey(), &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err), "no credential secret must be created from an unparseable credential")
}

// TestReconcileTeardownDeletesExternalDNSCredentialSecret covers
// deleteExternalDNSCredentialSecret's own doc: the credential Secret
// reconcileExternalDNSAddon creates must be deleted on teardown too, same
// as every other downstream object teardownDownstream cleans up. The
// Addon CR itself isn't asserted here — it lives on the hub, owned by the
// TalosCluster, and is garbage-collected once that's deleted (see
// ensureExternalDNSAddon's own doc), not explicitly deleted by
// teardownDownstream.
func TestReconcileTeardownDeletesExternalDNSCredentialSecret(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS("cloudflare")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{client: downstream},
		HubConfig:               testHubConfig(),
		ImageRepo:               testImageRepo,
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Locker:                  zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
		Logger:                  slog.Default(),
	}

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	require.NoError(t, downstream.Get(t.Context(), externalDNSCredentialSecretKey(), &corev1.Secret{}),
		"install pass must have created the credential secret")

	var toDelete v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &toDelete))
	require.NoError(t, hubClient.Delete(t.Context(), &toDelete))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	err = downstream.Get(t.Context(), externalDNSCredentialSecretKey(), &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err), "credential secret must be deleted on teardown")
}
