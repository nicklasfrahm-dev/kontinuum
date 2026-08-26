package fabric_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	talosconfig "github.com/siderolabs/talos/pkg/machinery/config"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/fabric"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	testFabricName    = "eu-fabric"
	testRegion        = "eu"
	testZoneAName     = "eu-a"
	testZoneBName     = "eu-b"
	testCIDR          = "10.0.0.0/16"
	testZonePrefix    = 24
	testRetryInterval = 15 * time.Second
	testImage         = "ghcr.io/nicklasfrahm-dev/kontinuum:dev"
	testGatewayLabel  = "role"
	testGatewayValue  = "gateway"
	// testBlockCIDR0GatewayIP is blockCIDR0's own GatewayIP (broadcast-1
	// of 10.0.0.0/24) — shared across fixtures purely so goconst doesn't
	// flag the repeated literal.
	testBlockCIDR0GatewayIP = "10.0.0.254"
	// testFabricInterface is testGatewayInstance's own free (non-WAN)
	// interface name.
	testFabricInterface = "eth1"
	// testTalosContractVersion is the Talos version testSecretsBundle
	// generates its version contract from — an arbitrary, always-valid
	// pinned version, mirroring
	// pkg/domain/taloscluster's own defaultTalosVersion.
	testTalosContractVersion = "v1.13.0"
	// testAppliedByFabricManagerMessage is testAlreadyKnownZoneFabricObject's
	// own condition message, simulating pkg/cli/fabricmanager's own
	// write-back — shared across its own three conditions purely so
	// goconst doesn't flag the repeated literal.
	testAppliedByFabricManagerMessage = "applied by fabricmanager"
)

func testNamespace() string { return v1alpha2.KontinuumSystemNamespace }

func fabricObjectKey() client.ObjectKey {
	return client.ObjectKey{Name: testFabricName, Namespace: testNamespace()}
}

func testFabricObject() *v1alpha2.Fabric {
	return &v1alpha2.Fabric{
		ObjectMeta: metav1.ObjectMeta{Name: testFabricName, Namespace: testNamespace()},
		Spec: v1alpha2.FabricSpec{
			Region:           testRegion,
			CIDR:             testCIDR,
			ZonePrefixLength: testZonePrefix,
			GatewaySelector:  metav1.LabelSelector{MatchLabels: map[string]string{testGatewayLabel: testGatewayValue}},
		},
	}
}

func testZoneObject(name, zone string) *v1alpha2.Zone {
	return &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace()},
		Spec:       v1alpha2.ZoneSpec{Region: testRegion, Zone: zone},
	}
}

// testSecretsBundle generates a real (but throwaway) Talos secrets bundle —
// pure crypto, no network involved — so fixtures can exercise the real
// fabric.LoadSecretsBundle/BuildTalosConfig code path without a real Talos
// cluster. Mirrors pkg/domain/taloscluster's own bundle-generation
// approach (see its ensureSecretsBundle).
func testSecretsBundle(t *testing.T) *talossecrets.Bundle {
	t.Helper()

	contract, err := talosconfig.ParseContractFromVersion(testTalosContractVersion)
	require.NoError(t, err)

	bundle, err := talossecrets.NewBundle(talossecrets.NewClock(), contract)
	require.NoError(t, err)

	return bundle
}

// talosClusterSecret returns the one Secret a TalosCluster's own
// status.secretRef points to — holding both the secrets bundle
// (fabric.LoadSecretsBundle's own key) and, once bootstrapped, the
// kubeconfig (fabric's own loadClusterKubeconfig) — mirrors
// pkg/domain/taloscluster/secrets.go's own single-Secret-two-keys shape
// (see storeKubeconfig, which adds the kubeconfig key onto the same
// Secret ensureSecretsBundle already created).
func talosClusterSecret(t *testing.T, clusterName string) *corev1.Secret {
	t.Helper()

	data, err := json.Marshal(testSecretsBundle(t))
	require.NoError(t, err)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "taloscluster-" + clusterName, Namespace: testNamespace()},
		Data: map[string][]byte{
			"secrets-bundle": data,
			"kubeconfig":     []byte("fake-kubeconfig"),
		},
	}
}

func testTalosCluster(clusterName string) *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace()},
		Status: v1alpha2.TalosClusterStatus{
			SecretRef: v1alpha2.SecretReference{Name: "taloscluster-" + clusterName, Namespace: testNamespace()},
		},
	}
}

// testGatewayInstance returns a candidate Instance eligible to be zone's
// own NAT gateway node: labeled kontinuum.sh/zone=zone and role=gateway
// (matching testFabricObject's own GatewaySelector), claimed, with two
// discovered interfaces — "eth0", already carrying an address (this
// fixture's own WAN/uplink, per classifyGatewayInterfaces — also what
// dialAddress resolves to reach it on), and "eth1", with none (the one
// free interface every "elects a gateway" test expects the fabric's own
// address to land on).
func testGatewayInstance(name, zone string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace(),
			Labels: map[string]string{
				v1alpha2.LabelZone:      zone,
				v1alpha2.LabelClaimedBy: "pool-" + zone,
				testGatewayLabel:        testGatewayValue,
			},
		},
		Status: v1alpha2.InstanceStatus{
			Interfaces: []v1alpha2.InstanceInterfaceStatus{
				{Name: "eth0", Addresses: []string{"10.0.1.5/24"}},
				{Name: testFabricInterface},
			},
		},
	}
}

// fakeDownstreamClientBuilder is zone.DownstreamClientBuilder's test
// double — it never dials a real kubeconfig, always returning the same
// pre-built fake client so a test can inspect what got created on it.
type fakeDownstreamClientBuilder struct {
	client client.Client
	err    error
	calls  *int
}

func (f fakeDownstreamClientBuilder) Build(_ []byte) (client.Client, error) {
	if f.calls != nil {
		*f.calls++
	}

	if f.err != nil {
		return nil, f.err
	}

	return f.client, nil
}

var errTestDownstreamBuild = errors.New("downstream build failed")

func newHubFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Fabric{}).
		WithObjects(objects...).
		Build()
}

func newDownstreamFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func newReconciler(hubClient client.Client, downstreamBuilder zone.DownstreamClientBuilder) *fabric.Reconciler {
	return &fabric.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: downstreamBuilder,
		RetryInterval:           testRetryInterval,
		Locker:                  zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
		Logger:                  slog.Default(),
	}
}

func reconcileRequest() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: testFabricName, Namespace: testNamespace()}}
}

func TestReconcileIgnoresMissingFabric(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

// TestReconcileDefaultsZonePrefixLengthOnceFromLiveZoneCount exercises
// ensureZonePrefixLengthDefaulted: spec.zonePrefixLength left unset (0)
// gets computed from the live zone count on the first reconcile — testCIDR
// is a /16 and two live zones need only a 2-block split, so the smallest
// valid zonePrefixLength is /17 (see defaultZonePrefixLength's own doc for
// the sizing rule) — and never recomputed again afterward, even once a
// third zone joins and outgrows that /17's own 2-block capacity.
func TestReconcileDefaultsZonePrefixLengthOnceFromLiveZoneCount(t *testing.T) {
	t.Parallel()

	fabricObj := testFabricObject()
	fabricObj.Spec.ZonePrefixLength = 0

	hubClient := newHubFakeClient(t, fabricObj, testZoneObject(testZoneAName, "a"), testZoneObject(testZoneBName, "b"))
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var updated v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &updated))
	assert.EqualValues(t, 17, updated.Spec.ZonePrefixLength)

	require.NoError(t, hubClient.Create(t.Context(), testZoneObject("eu-c", "c")))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &updated))
	assert.EqualValues(t, 17, updated.Spec.ZonePrefixLength,
		"defaulted once, must never be silently recomputed as the live zone count grows")
}

func TestReconcileCarvesSubnetAndSetsValidSpecReady(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testFabricObject(), testZoneObject(testZoneAName, "a"))
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "not settled yet: no gateway candidate exists")

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	assert.True(t, meta.IsStatusConditionTrue(fabricObj.Status.Conditions, fabric.ValidSpecConditionType))
	assert.True(t, meta.IsStatusConditionTrue(fabricObj.Status.Conditions, fabric.ReadyConditionType))

	require.Len(t, fabricObj.Status.Zones, 1)
	assert.Equal(t, "a", fabricObj.Status.Zones[0].Zone)
	assert.Equal(t, "10.0.0.0/24", fabricObj.Status.Zones[0].CIDR)
	assert.Equal(t, testBlockCIDR0GatewayIP, fabricObj.Status.Zones[0].GatewayIP)
	assert.False(t,
		meta.IsStatusConditionTrue(fabricObj.Status.Zones[0].Conditions, fabric.GatewayNodeSelectedConditionType))
}

// TestReconcileWritesStatusOnceWhenZonesAndConditionBothChange is a
// regression test: reconcileFabric used to mutate fabricObj.Status.Zones,
// then call setValidSpecCondition (whose own conditional Status().Update
// already persisted that mutation alongside the condition change), then
// call persistZoneStatuses, which wrote the identical, already-persisted
// object a second time — a redundant Status().Update whenever both zones
// and the ValidSpec/Ready condition changed in the same pass, as they do
// on a Fabric's first successful reconcile (this test's own scenario,
// shared with TestReconcileCarvesSubnetAndSetsValidSpecReady).
func TestReconcileWritesStatusOnceWhenZonesAndConditionBothChange(t *testing.T) {
	t.Parallel()

	var statusUpdates int

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	hubClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Fabric{}).
		WithObjects(testFabricObject(), testZoneObject(testZoneAName, "a")).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(
				ctx context.Context, cli client.Client, subResourceName string,
				obj client.Object, opts ...client.SubResourceUpdateOption,
			) error {
				if subResourceName == "status" {
					statusUpdates++
				}

				return cli.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	assert.Equal(t, 1, statusUpdates,
		"zones and the ValidSpec/Ready condition both changed in this one pass, so this must be a single write")
}

func TestReconcileInvalidSpecBlocksReadyWithNoRequeue(t *testing.T) {
	t.Parallel()

	fabricObj := testFabricObject()
	fabricObj.Spec.CIDR = "not-a-cidr"

	hubClient := newHubFakeClient(t, fabricObj)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result,
		"an invalid spec never requeues on its own — only an edit re-triggers the watch")

	var updated v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &updated))
	assert.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, fabric.ValidSpecConditionType))
	assert.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, fabric.ReadyConditionType))
}

// TestReconcileElectsGatewayAndDeliversTalosCredential covers what this
// controller's own responsibility actually is now: elect a gateway node,
// record its free interfaces (entry.GatewayInterfaces), and deliver the
// Talos credential pkg/cli/fabricmanager needs to apply that config
// itself (see ensureGatewayTalosConfig) — this controller no longer
// pushes interface config or installs any workload directly (see
// reconcileNATForGatewayNode's own doc), so it never claims
// NetworkConfigured/NATInstalled/Ready true on its own.
func TestReconcileElectsGatewayAndDeliversTalosCredential(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"),
		testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"),
	)

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter,
		"ZoneReady is pkg/cli/fabricmanager's own condition to set once it actually applies this state, "+
			"so the hub keeps retrying until a later pass observes it carried forward as true")

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.Len(t, fabricObj.Status.Zones, 1)

	zoneStatus := fabricObj.Status.Zones[0]
	require.NotNil(t, zoneStatus.GatewayNodeRef)
	assert.Equal(t, "node-a1", zoneStatus.GatewayNodeRef.Name)
	assert.Equal(t, []string{testFabricInterface}, zoneStatus.GatewayInterfaces)
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.GatewayNodeSelectedConditionType))
	assert.False(t, meta.IsStatusConditionPresentAndEqual(
		zoneStatus.Conditions, fabric.NetworkConfiguredConditionType, metav1.ConditionFalse),
		"no hub-observable failure occurred, so this must stay unset — pkg/cli/fabricmanager's own to set")

	var secret corev1.Secret

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: fabric.TalosConfigSecretName, Namespace: v1alpha2.KontinuumSystemNamespace}, &secret)
	require.NoError(t, err, "the talos credential fabricmanager needs must be delivered to the downstream cluster")
	assert.NotEmpty(t, secret.Data[fabric.TalosConfigSecretKey])
}

func TestReconcileStickyGatewayNodeSelection(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"),
		testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"), testGatewayInstance("node-a2", "a"),
	)

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.Len(t, fabricObj.Status.Zones, 1)
	require.NotNil(t, fabricObj.Status.Zones[0].GatewayNodeRef)
	assert.Equal(t, "node-a1", fabricObj.Status.Zones[0].GatewayNodeRef.Name,
		"lowest-named candidate wins on first election")

	// Simulate an operator/earlier reconcile having instead picked node-a2 —
	// a second reconcile must keep it sticky, not switch back to node-a1
	// just because it sorts lower.
	fabricObj.Status.Zones[0].GatewayNodeRef.Name = "node-a2"
	require.NoError(t, hubClient.Status().Update(t.Context(), &fabricObj))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.NotNil(t, fabricObj.Status.Zones[0].GatewayNodeRef)
	assert.Equal(t, "node-a2", fabricObj.Status.Zones[0].GatewayNodeRef.Name,
		"sticky: still-eligible previous pick is kept")
}

func TestReconcileSkipsNetworkAndNATWhenDisabled(t *testing.T) {
	t.Parallel()

	fabricObj := testFabricObject()
	fabricObj.Spec.NAT.Disabled = true

	hubClient := newHubFakeClient(t, fabricObj, testZoneObject(testZoneAName, "a"), testGatewayInstance("node-a1", "a"))

	downstreamCalls := 0
	reconciler := newReconciler(hubClient,
		fakeDownstreamClientBuilder{err: errTestDownstreamBuild, calls: &downstreamCalls})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	assert.Zero(t, downstreamCalls, "nat disabled: the downstream client must never even be built")

	var updated v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &updated))
	require.Len(t, updated.Status.Zones, 1)

	zoneStatus := updated.Status.Zones[0]
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.GatewayNodeSelectedConditionType))
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.ZoneReadyConditionType))
	assert.False(t, meta.IsStatusConditionPresentAndEqual(
		zoneStatus.Conditions, fabric.NetworkConfiguredConditionType, metav1.ConditionTrue))
}

// TestReconcileNATDisabledZoneReadyWithNoGatewayCandidate is a regression
// test: the spec.nat.disabled check used to run after recordGatewayNodeSelection
// already returned early on failure, so a zone with NAT disabled and no
// gateway candidate at all could never become Ready — even though NAT is
// the only thing that selector's outcome is used for.
func TestReconcileNATDisabledZoneReadyWithNoGatewayCandidate(t *testing.T) {
	t.Parallel()

	fabricObj := testFabricObject()
	fabricObj.Spec.NAT.Disabled = true

	hubClient := newHubFakeClient(t, fabricObj, testZoneObject(testZoneAName, "a"))

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result, "nat disabled: no gateway candidate is not a reason to keep retrying")

	var updated v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &updated))
	require.Len(t, updated.Status.Zones, 1)

	zoneStatus := updated.Status.Zones[0]
	assert.False(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.GatewayNodeSelectedConditionType),
		"no candidate exists, so this must genuinely stay false")
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.ZoneReadyConditionType),
		"nat disabled means the missing gateway candidate must not block readiness")
}

func TestReconcileNoGatewayCandidateOnlyBlocksThatZone(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"), testZoneObject(testZoneBName, "b"),
		testTalosCluster(testZoneBName), talosClusterSecret(t, testZoneBName),
		testGatewayInstance("node-b1", "b"),
	)

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "zone a never settles, so the fabric keeps retrying")

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	assert.True(t, meta.IsStatusConditionTrue(fabricObj.Status.Conditions, fabric.ReadyConditionType),
		"one zone with no gateway candidate must not block the fabric's own Ready")

	require.Len(t, fabricObj.Status.Zones, 2)

	byZone := map[string]v1alpha2.FabricZoneStatus{}
	for _, z := range fabricObj.Status.Zones {
		byZone[z.Zone] = z
	}

	assert.False(t, meta.IsStatusConditionTrue(byZone["a"].Conditions, fabric.GatewayNodeSelectedConditionType))
	assert.True(t, meta.IsStatusConditionTrue(byZone["b"].Conditions, fabric.GatewayNodeSelectedConditionType),
		"zone a's missing candidate must not block zone b's own election")
	assert.False(t, meta.IsStatusConditionPresentAndEqual(
		byZone["b"].Conditions, fabric.NetworkConfiguredConditionType, metav1.ConditionFalse),
		"zone b hit no hub-observable failure of its own")
}

// TestReconcileDeletionIsUnmanagedNow is a regression test for the
// architecture change itself: this controller used to add FabricFinalizer
// and run its own teardown sequence (tearing down each zone's own nat
// gateway workload before letting deletion proceed). Now that
// pkg/cli/fabricmanager self-manages its own state by watching Fabric
// directly (see Reconciler's own doc) — noticing on its own that it's no
// longer any zone's own gatewayNodeRef, and pruning its own stale
// nftables state accordingly (see PruneStaleMasqueradeTables) — this
// controller has no workload left to tear down, and so no finalizer to
// gate deletion on: a `kubectl delete fabric` must delete the object
// immediately, with nothing left here to block or delay it.
func TestReconcileDeletionIsUnmanagedNow(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testFabricObject())
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	assert.Empty(t, fabricObj.Finalizers, "no finalizer left for this controller to add")

	require.NoError(t, hubClient.Delete(t.Context(), &fabricObj))

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var check v1alpha2.Fabric

	err = hubClient.Get(t.Context(), fabricObjectKey(), &check)
	assert.True(t, apierrors.IsNotFound(err), "with no finalizer, deletion must not be gated on anything")
}

// singleInterfaceInstance returns a candidate Instance with only one
// discovered interface, already carrying an address — a single-NIC node,
// unlike testGatewayInstance's own two-interface fixture. Once elected,
// classifyGatewayInterfaces reports it as an all-WAN, no-free-interface
// node: there's nothing left to advertise the fabric on.
func singleInterfaceInstance(name, zone string) *v1alpha2.Instance {
	inst := testGatewayInstance(name, zone)
	inst.Status.Interfaces = []v1alpha2.InstanceInterfaceStatus{
		{Name: "eth0", Addresses: []string{"10.0.1.5/24"}},
	}

	return inst
}

func TestReconcileSingleInterfaceGatewayNeverConfiguresNetwork(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"),
		testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		singleInterfaceInstance("node-a1", "a"))

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.Len(t, fabricObj.Status.Zones, 1)

	zoneStatus := fabricObj.Status.Zones[0]
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.GatewayNodeSelectedConditionType),
		"the node itself is still a valid gateway candidate — only its own network config fails")

	networkCondition := meta.FindStatusCondition(zoneStatus.Conditions, fabric.NetworkConfiguredConditionType)
	require.NotNil(t, networkCondition)
	assert.Equal(t, metav1.ConditionFalse, networkCondition.Status)
	assert.Contains(t, networkCondition.Message, "no free interface",
		"a single-NIC node must fail on the interface-classification step specifically, not e.g. a missing talos cluster")
	assert.Empty(t, zoneStatus.GatewayInterfaces)
}

func TestReconcileBridgesMultipleFabricInterfaces(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	gatewayNode := testGatewayInstance("node-a1", "a")
	gatewayNode.Status.Interfaces = append(gatewayNode.Status.Interfaces, v1alpha2.InstanceInterfaceStatus{Name: "eth2"})

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"),
		testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName), gatewayNode)

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.Len(t, fabricObj.Status.Zones, 1)
	assert.ElementsMatch(t, []string{testFabricInterface, "eth2"}, fabricObj.Status.Zones[0].GatewayInterfaces,
		"both free interfaces must be published for pkg/cli/fabricmanager to bridge, not just one")
}

// testAlreadyKnownZoneFabricObject returns testFabricObject() with its
// status pre-populated as if this zone had already fully converged in an
// earlier pass: fabric-level ValidSpec/Ready, zone "a"'s GatewayNodeRef/
// CIDR/GatewayIP/GatewayInterfaces set to exactly what a fresh reconcile
// will recompute anyway, GatewayNodeSelectedConditionType true (still this
// controller's own condition to set), and NetworkConfigured/NATInstalled/
// ZoneReady all true — simulating pkg/cli/fabricmanager's own earlier
// write-back onto this same Fabric object once it actually applied that
// state (see reconcileNATForGatewayNode's own doc for why this controller
// itself never sets any of those three true).
func testAlreadyKnownZoneFabricObject() *v1alpha2.Fabric {
	fabricObj := testFabricObject()
	fabricObj.Status.Conditions = []metav1.Condition{
		{
			Type: fabric.ValidSpecConditionType, Status: metav1.ConditionTrue,
			Reason: "ValidSpec", Message: "fabric spec is valid", LastTransitionTime: metav1.Now(),
		},
		{
			Type: fabric.ReadyConditionType, Status: metav1.ConditionTrue,
			Reason: "ValidSpec", Message: "fabric spec is valid", LastTransitionTime: metav1.Now(),
		},
	}

	fabricObj.Status.Zones = []v1alpha2.FabricZoneStatus{
		{
			Zone: "a", CIDR: blockCIDR0, GatewayIP: testBlockCIDR0GatewayIP,
			GatewayNodeRef: &v1alpha2.ObjectReference{
				APIVersion: v1alpha2.GroupVersion().String(), Kind: "Instance", Name: "node-a1",
			},
			GatewayInterfaces: []string{testFabricInterface},
			Conditions: []metav1.Condition{
				{
					Type: fabric.GatewayNodeSelectedConditionType, Status: metav1.ConditionTrue,
					Reason: "GatewayNodeSelected", Message: `instance "node-a1" selected as this zone's nat gateway node`,
					LastTransitionTime: metav1.Now(),
				},
				{
					Type: fabric.NetworkConfiguredConditionType, Status: metav1.ConditionTrue,
					Reason: "NetworkConfigured", Message: testAppliedByFabricManagerMessage, LastTransitionTime: metav1.Now(),
				},
				{
					Type: fabric.NATInstalledConditionType, Status: metav1.ConditionTrue,
					Reason: "NATInstalled", Message: testAppliedByFabricManagerMessage, LastTransitionTime: metav1.Now(),
				},
				{
					Type: fabric.ZoneReadyConditionType, Status: metav1.ConditionTrue,
					Reason: "ZoneReady", Message: testAppliedByFabricManagerMessage, LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	return fabricObj
}

// TestReconcileDoesNotClobberFabricManagerOwnedConditions is a contract
// test for the ownership split itself: NetworkConfigured/NATInstalled/
// ZoneReady are pkg/cli/fabricmanager's own conditions to set true (see
// reconcileNATForGatewayNode's own doc) — a hub reconcile pass that
// re-confirms the same already-known gateway node and interfaces must
// leave all three exactly as fabricmanager left them, never resetting them
// just because this controller itself never sets them true on its own.
func TestReconcileDoesNotClobberFabricManagerOwnedConditions(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)
	fabricObj := testAlreadyKnownZoneFabricObject()

	hubClient := newHubFakeClient(t,
		fabricObj, testZoneObject(testZoneAName, "a"), testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"))

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result, "every zone was already reported ready by fabricmanager, so this pass settles")

	var persisted v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &persisted))
	require.Len(t, persisted.Status.Zones, 1)
	assert.True(t, meta.IsStatusConditionTrue(persisted.Status.Zones[0].Conditions, fabric.NetworkConfiguredConditionType),
		"fabricmanager's own NetworkConfigured=true must survive an unrelated hub reconcile pass")
	assert.True(t, meta.IsStatusConditionTrue(persisted.Status.Zones[0].Conditions, fabric.NATInstalledConditionType),
		"fabricmanager's own NATInstalled=true must survive an unrelated hub reconcile pass")
	assert.True(t, meta.IsStatusConditionTrue(persisted.Status.Zones[0].Conditions, fabric.ZoneReadyConditionType),
		"fabricmanager's own ZoneReady=true must survive an unrelated hub reconcile pass")
}

// TestReconcilePersistsGatewayNodeSelectionTransitionOnAlreadyKnownZone is a
// regression test for the Conditions-slice-aliasing bug: entry.Conditions
// used to be assigned directly from the previous status.zones entry
// (`entry.Conditions = previous.Conditions`), sharing the same backing
// array as fabricObj.Status.Zones[i].Conditions — the "before" snapshot
// equalZoneStatuses compares against. meta.SetStatusCondition mutates an
// existing condition's fields in place through a pointer into that array,
// which silently rewrote the "before" snapshot to already match "after"
// before the comparison ever ran, hiding a real transition from change
// detection and skipping the Status().Update that should have persisted
// it. GatewayNodeSelectedConditionType is still this controller's own
// condition to flip (unlike NetworkConfigured/NATInstalled/ZoneReady,
// which are pkg/cli/fabricmanager's now — see reconcileNATForGatewayNode's
// own doc), so a previously-candidate-less zone gaining one is the
// remaining scenario that still exercises this exact code path end to end.
func TestReconcilePersistsGatewayNodeSelectionTransitionOnAlreadyKnownZone(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	fabricObj := testFabricObject()
	fabricObj.Status.Zones = []v1alpha2.FabricZoneStatus{
		{
			Zone: "a", CIDR: blockCIDR0, GatewayIP: testBlockCIDR0GatewayIP,
			Conditions: []metav1.Condition{
				{
					Type: fabric.GatewayNodeSelectedConditionType, Status: metav1.ConditionFalse,
					Reason: "NoGatewayCandidate", Message: `no instance in zone "a" matches spec.gatewaySelector`,
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	hubClient := newHubFakeClient(t,
		fabricObj, testZoneObject(testZoneAName, "a"), testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"))

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var persisted v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &persisted))
	require.Len(t, persisted.Status.Zones, 1)
	assert.True(t, meta.IsStatusConditionTrue(
		persisted.Status.Zones[0].Conditions, fabric.GatewayNodeSelectedConditionType),
		"the False->True transition must actually be written to the API server, not just computed in memory")
}

// TestReconcileKeepsPreviousGatewayInterfacesWhenGatewayNodeBrieflyUnresolvable
// is a regression test: reconcileZoneStatuses used to start each zone's own
// GatewayInterfaces from zero every reconcile, relying entirely on
// reconcileNATForGatewayNode to repopulate it on success. When a later
// reconcile's own gateway node briefly stops being an eligible candidate
// (e.g. its claimed-by label flaps), reconcileNATForGatewayNode never runs
// at all that pass, which used to leave status.zones[].gatewayInterfaces
// empty even though the gateway address is still actually assigned to
// those interfaces from a prior, successful apply — an incorrect status
// regression, not a reflection of reality. GatewayInterfaces must be
// carried forward from the previous status by default, the same way
// GatewayNodeRef already is.
func TestReconcileKeepsPreviousGatewayInterfacesWhenGatewayNodeBrieflyUnresolvable(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)
	fabricObj := testAlreadyKnownZoneFabricObject()

	flappingNode := testGatewayInstance("node-a1", "a")
	delete(flappingNode.Labels, v1alpha2.LabelClaimedBy)

	hubClient := newHubFakeClient(t,
		fabricObj, testZoneObject(testZoneAName, "a"), testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		flappingNode)

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var persisted v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &persisted))
	require.Len(t, persisted.Status.Zones, 1)
	assert.Equal(t, []string{testFabricInterface}, persisted.Status.Zones[0].GatewayInterfaces,
		"a transient gateway-resolution failure must not wipe out the previously recorded gateway interfaces")
}
