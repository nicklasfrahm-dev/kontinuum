package zone_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// testHostname is the hostname every test in this file expects
// ensureDNSEndpoint to have been asked to point at — testZone + "." +
// testRegion + "." + testDomain, matching reconcileInstall's own hostname
// computation.
const testHostname = testZone + "." + testRegion + "." + testDomain

// testGatewayIP is a fake downstream Gateway address, not a real one.
const testGatewayIP = "203.0.113.10"

// dnsEndpointKey is where reconcileDNS's own ensureDNSEndpoint upserts its
// single DNSEndpoint — "kontinuum" in "kontinuum-system", same fixed naming
// convention as network.go's gatewayName/certificateName/httpRouteName.
func dnsEndpointKey() client.ObjectKey {
	return client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}
}

// TestReconcileDNSSkippedWithoutCredentials covers issue #51's own "must not
// require DNS credentials to reach Ready" requirement: with no
// KONTINUUM_SERVER_DNS_CREDENTIAL configured anywhere, DNSRecordConditionType
// surfaces why, but never creates a DNSEndpoint and never blocks
// InstalledConditionType's own progress.
func TestReconcileDNSSkippedWithoutCredentials(t *testing.T) {
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
	assert.Equal(t, "DNSCredentialsNotConfigured", cond.Reason)

	// InstalledConditionType still progresses to WaitingForCertificate (the
	// same state TestReconcileInstallsDownstreamObjectsAndWaitsForCertificate
	// observes) rather than being blocked by the missing DNS credential.
	installed := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, installed)
	assert.Equal(t, "WaitingForCertificate", installed.Reason)

	err = downstream.Get(t.Context(), dnsEndpointKey(), &zone.DNSEndpoint{})
	assert.True(t, apierrors.IsNotFound(err), "no DNSEndpoint must be created")
}

// TestReconcileDNSWaitsForGatewayAddress covers a DNS credential configured
// but the downstream Gateway not yet assigned an address (its own
// LoadBalancer implementation hasn't finished) — DNSRecordConditionType
// reports that instead of erroring, and no DNSEndpoint is created yet.
func TestReconcileDNSWaitsForGatewayAddress(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS()
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

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS()
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	// First pass creates the Gateway (no address yet).
	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &gateway))

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

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS()
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &gateway))

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

	kontinuum, kontinuumSecret := registeredKontinuumWithDNS()
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{client: downstream},
		ACMEEmail:               "ops@example.com",
		ACMEServer:              "https://acme-v02.api.letsencrypt.org/directory",
		ImageRepo:               testImageRepo,
		GRPCEndpoint:            testGRPCEndpoint,
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Logger:                  slog.Default(),
	}

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &gateway))

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
