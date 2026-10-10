package zone_test

import (
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

// testDNSProviderOther is the DNS provider testHubConfig configures — any
// name works, since the zone controller only checks one is set; which
// provider external-dns uses is the addon package's concern (see
// addon.EnsureExternalDNSSeed).
const testDNSProviderOther = "other-provider"

// dnsEndpointKey is where reconcileDNS's own ensureDNSEndpoint upserts its
// single DNSEndpoint — "kontinuum" in "kontinuum-system", same fixed naming
// convention as network.go's gatewayName/certificateName/httpRouteName.
func dnsEndpointKey() client.ObjectKey {
	return client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}
}

// TestReconcileInstalledWaitsForDNSConfiguration covers point 2 of the
// PR #73 review: with spec.domain set but no DNS provider/credential
// configured on the hub, installNetwork must never run at all — no
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
	reconciler.HubConfig.Server.DNS = v1alpha2.KontinuumDNSConfigStatus{}

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

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

// externalDNSCredentialSecretKey is where the addon controller copies the
// external-dns credential Secret on the downstream cluster (see
// addon.ExternalDNSCredentialSecretName).
func externalDNSCredentialSecretKey() client.ObjectKey {
	return client.ObjectKey{Name: testZoneName + "-external-dns-credentials", Namespace: testDownstreamNamespace}
}

// TestReconcileInstalledFailsWhenDNSCredentialNotFlatYAML covers a DNS
// provider/credential configured on the hub, but the credential isn't a flat
// YAML mapping (see addon.ParseDNSCredentialKeys) — a nested value, or plain
// text. The taloscluster controller skips seeding external-dns for it, so
// the Zone is where it has to surface: Installed=False/InstallFailed, not
// "DNS isn't configured", and no network layer is installed.
func TestReconcileInstalledFailsWhenDNSCredentialNotFlatYAML(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	reconciler.HubConfig.Server.DNS.Credential = "CF_API_TOKEN:\n  nested: not-a-string\n"

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	installed := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, installed)
	assert.Equal(t, metav1.ConditionFalse, installed.Status)
	assert.Equal(t, "InstallFailed", installed.Reason)

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gatewayv1.Gateway{})
	assert.True(t, apierrors.IsNotFound(err), "no Gateway must be created from an unparseable credential")
}

// TestReconcileTeardownDeletesExternalDNSCredentialSecret covers
// deleteExternalDNSCredentialSecret: the credential Secret the addon
// controller copies onto the downstream cluster must be deleted on teardown
// too, same as every other downstream object teardownDownstream cleans up.
// The hub-side Addon and Secret aren't asserted here — they're owned by the
// TalosCluster and garbage-collected with it.
func TestReconcileTeardownDeletesExternalDNSCredentialSecret(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	key := externalDNSCredentialSecretKey()
	require.NoError(t, downstream.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Data:       map[string][]byte{"CF_API_TOKEN": []byte(testDNSCredentialValue)},
	}))

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

	var toDelete v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &toDelete))
	require.NoError(t, hubClient.Delete(t.Context(), &toDelete))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	err = downstream.Get(t.Context(), key, &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err), "credential secret must be deleted on teardown")
}
