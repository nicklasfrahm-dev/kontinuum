package frontend_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/frontend"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	testHost = "kontinuum.example.com"
	// testHub is the holder identity of a hub, which may always take the
	// global lease.
	testHub = "hub"
	// testDownstreamNamespace is where every zone's own kontinuum objects
	// live on its own cluster.
	testDownstreamNamespace = "kontinuum-system"
)

// errUnknownKubeconfig is mapBuilder's failure for a kubeconfig it has no
// client for.
var errUnknownKubeconfig = errors.New("unknown kubeconfig")

// mapBuilder hands out one fake downstream client per zone, keyed by the
// kubeconfig its TalosCluster stores.
type mapBuilder map[string]client.Client

func (b mapBuilder) Build(kubeconfig []byte) (client.Client, error) {
	downstream, ok := b[string(kubeconfig)]
	if !ok {
		return nil, errUnknownKubeconfig
	}

	return downstream, nil
}

func newHubClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// newZoneCluster builds a zone's own cluster whose kontinuum Gateway has
// address, or no address at all when address is empty.
func newZoneCluster(t *testing.T, address string, addressType gatewayv1.AddressType) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, gatewayv1.Install(scheme))
	zone.AddExternalDNSToScheme(scheme)

	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "kontinuum", Namespace: testDownstreamNamespace}}
	if address != "" {
		gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Type: &addressType, Value: address}}
	}

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(gateway).Build()
}

// zoneObjects returns name's Zone, its TalosCluster and the Secret holding
// its kubeconfig (the string name itself, matching mapBuilder's keys).
func zoneObjects(name string, ready bool) []client.Object {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}

	return []client.Object{
		&v1alpha2.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
			Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: name},
			Status: v1alpha2.ZoneStatus{Conditions: []metav1.Condition{
				{Type: zone.ReadyConditionType, Status: status, Reason: "Test"},
			}},
		},
		&v1alpha2.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
			Status: v1alpha2.TalosClusterStatus{
				SecretRef: v1alpha2.SecretReference{Name: name + "-kubeconfig", Namespace: v1alpha2.KontinuumSystemNamespace},
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-kubeconfig", Namespace: v1alpha2.KontinuumSystemNamespace},
			Data:       map[string][]byte{"kubeconfig": []byte(name)},
		},
	}
}

func newRunnable(hubClient client.Client, builder mapBuilder, identity zonelease.Identity) *frontend.Runnable {
	return &frontend.Runnable{
		Client:                  hubClient,
		Host:                    testHost,
		DownstreamClientBuilder: builder,
		Locker:                  zonelease.NewLockerFor(hubClient, hubClient, identity),
		Logger:                  slog.Default(),
	}
}

func getEndpoint(t *testing.T, downstream client.Client) (zone.DNSEndpoint, bool) {
	t.Helper()

	var endpoint zone.DNSEndpoint

	err := downstream.Get(t.Context(),
		client.ObjectKey{Name: frontend.DNSEndpointName, Namespace: testDownstreamNamespace}, &endpoint)
	if apierrors.IsNotFound(err) {
		return endpoint, false
	}

	require.NoError(t, err)

	return endpoint, true
}

func TestReconcilePublishesEveryReadyZoneFromOneHolder(t *testing.T) {
	t.Parallel()

	objects := append(zoneObjects("a", false), zoneObjects("b", true)...)
	objects = append(objects, zoneObjects("c", true)...)
	objects = append(objects, zoneObjects("d", true)...)

	clusterA := newZoneCluster(t, "203.0.113.1", gatewayv1.IPAddressType)
	clusterB := newZoneCluster(t, "203.0.113.2", gatewayv1.IPAddressType)
	clusterC := newZoneCluster(t, "2001:db8::3", gatewayv1.IPAddressType)
	clusterD := newZoneCluster(t, "lb.example.net", gatewayv1.HostnameAddressType)

	// A stale copy in a zone that isn't the holder must be removed.
	require.NoError(t, zone.EnsureDNSEndpoint(t.Context(), clusterC, frontend.DNSEndpointName,
		[]zone.Endpoint{{DNSName: testHost, Targets: []string{"198.51.100.9"}, RecordType: zone.RecordTypeA}}))

	builder := mapBuilder{"a": clusterA, "b": clusterB, "c": clusterC, "d": clusterD}
	runnable := newRunnable(newHubClient(t, objects...), builder, zonelease.Identity{HolderIdentity: testHub})

	runnable.Reconcile(t.Context())

	// a isn't Ready, so b — the first Ready zone by name — holds the record.
	_, found := getEndpoint(t, clusterA)
	assert.False(t, found)

	endpoint, found := getEndpoint(t, clusterB)
	require.True(t, found)
	assert.Equal(t, []zone.Endpoint{
		{DNSName: testHost, Targets: []string{"203.0.113.2"}, RecordType: zone.RecordTypeA},
		{DNSName: testHost, Targets: []string{"2001:db8::3"}, RecordType: zone.RecordTypeAAAA},
	}, endpoint.Spec.Endpoints)

	_, found = getEndpoint(t, clusterC)
	assert.False(t, found, "a non-holder's stale record must be removed")

	_, found = getEndpoint(t, clusterD)
	assert.False(t, found)
}

func TestReconcileSkipsWithoutGlobalLease(t *testing.T) {
	t.Parallel()

	clusterA := newZoneCluster(t, "203.0.113.1", gatewayv1.IPAddressType)

	// A proxy-mode zone peer is refused zonelease.GlobalKey.
	runnable := newRunnable(newHubClient(t, zoneObjects("a", true)...), mapBuilder{"a": clusterA},
		zonelease.Identity{HolderIdentity: "zone-b", SelfZoneKey: "eu-b"})

	runnable.Reconcile(t.Context())

	_, found := getEndpoint(t, clusterA)
	assert.False(t, found)
}

func TestReconcileKeepsRecordWhenNoZoneIsReady(t *testing.T) {
	t.Parallel()

	clusterA := newZoneCluster(t, "203.0.113.1", gatewayv1.IPAddressType)
	require.NoError(t, zone.EnsureDNSEndpoint(t.Context(), clusterA, frontend.DNSEndpointName,
		[]zone.Endpoint{{DNSName: testHost, Targets: []string{"203.0.113.1"}, RecordType: zone.RecordTypeA}}))

	runnable := newRunnable(newHubClient(t, zoneObjects("a", false)...), mapBuilder{"a": clusterA},
		zonelease.Identity{HolderIdentity: testHub})

	runnable.Reconcile(t.Context())

	_, found := getEndpoint(t, clusterA)
	assert.True(t, found, "with nothing Ready, the last published record is left in place")
}

func TestStartReturnsOnCancel(t *testing.T) {
	t.Parallel()

	controller := frontend.NewController(frontend.Config{Host: testHost, Logger: slog.Default()})
	assert.Positive(t, controller.Config.Interval)
	assert.NotNil(t, controller.Config.DownstreamClientBuilder)

	runnable := newRunnable(newHubClient(t), mapBuilder{}, zonelease.Identity{HolderIdentity: testHub})
	runnable.Interval = controller.Config.Interval

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, runnable.Start(ctx))
}
