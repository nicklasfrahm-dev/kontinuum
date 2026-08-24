package fabric_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
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

// testFabricManagerName/testFabricManagerNamespace mirror what
// pkg/domain/fabric/workload.go's own unexported
// fabricManagerDeploymentName("eth0")/fabricManagerNamespace resolve to for
// every teardown fixture's own gateway node (see testGatewayInstance,
// always given interface "eth0") — the teardown tests assert against these
// local copies rather than a literal repeated at every call site.
const testFabricManagerName = "kontinuum-fabric-manager-eth0"

func testFabricManagerNamespace() string { return v1alpha2.KontinuumSystemNamespace }

// fabricManagerDeployment returns a stand-in for the Deployment
// ensureFabricManagerWorkload installs — a teardown fixture representing
// "already installed on this zone's downstream cluster," not something the
// tests exercising the install path themselves create.
func fabricManagerDeployment() *appsv1.Deployment {
	labels := map[string]string{"app.kubernetes.io/name": testFabricManagerName}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: testFabricManagerName, Namespace: testFabricManagerNamespace()},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: testFabricManagerName, Image: testImage}},
				},
			},
		},
	}
}

// fakeNetworkConfigurer is fabric.NetworkConfigurer's test double.
type fakeNetworkConfigurer struct {
	err     error
	calls   *[]string
	patches *[][]byte
}

func (f fakeNetworkConfigurer) ApplyInterfaceConfig(
	_ context.Context, addr string, _ *clientconfig.Config, patch []byte,
) error {
	if f.calls != nil {
		*f.calls = append(*f.calls, addr)
	}

	if f.patches != nil {
		*f.patches = append(*f.patches, patch)
	}

	return f.err
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

func newDownstreamFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// testTeardownTimeout is newReconciler's own default Reconciler.TeardownTimeout
// — long enough that TestReconcileTeardown* fixtures whose DeletionTimestamp
// is "now" never spuriously hit it; TestReconcileTeardownGivesUpAfterTimeout
// overrides it directly on the returned *fabric.Reconciler instead.
const testTeardownTimeout = 15 * time.Minute

func newReconciler(
	hubClient client.Client, networkConfigurer fabric.NetworkConfigurer, downstreamBuilder zone.DownstreamClientBuilder,
) *fabric.Reconciler {
	return &fabric.Reconciler{
		Client:                  hubClient,
		NetworkConfigurer:       networkConfigurer,
		DownstreamClientBuilder: downstreamBuilder,
		Image:                   testImage,
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
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
	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestReconcileCarvesSubnetAndSetsValidSpecReady(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testFabricObject(), testZoneObject(testZoneAName, "a"))
	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{})

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

func TestReconcileInvalidSpecBlocksReadyWithNoRequeue(t *testing.T) {
	t.Parallel()

	fabricObj := testFabricObject()
	fabricObj.Spec.CIDR = "not-a-cidr"

	hubClient := newHubFakeClient(t, fabricObj)
	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result,
		"an invalid spec never requeues on its own — only an edit re-triggers the watch")

	var updated v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &updated))
	assert.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, fabric.ValidSpecConditionType))
	assert.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, fabric.ReadyConditionType))
}

func TestReconcileElectsGatewayPushesNetworkConfigAndInstallsNAT(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"),
		testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"),
	)

	var applyCalls []string

	reconciler := newReconciler(hubClient,
		fakeNetworkConfigurer{calls: &applyCalls}, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result, "everything converged — no requeue")

	assert.Equal(t, []string{"10.0.1.5"}, applyCalls, "pushed exactly once, to the elected node's discovered address")

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.Len(t, fabricObj.Status.Zones, 1)

	zoneStatus := fabricObj.Status.Zones[0]
	require.NotNil(t, zoneStatus.GatewayNodeRef)
	assert.Equal(t, "node-a1", zoneStatus.GatewayNodeRef.Name)
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.GatewayNodeSelectedConditionType))
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.NetworkConfiguredConditionType))
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.NATInstalledConditionType))
	assert.True(t, meta.IsStatusConditionTrue(zoneStatus.Conditions, fabric.ZoneReadyConditionType))

	var deployment appsv1.Deployment

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testFabricManagerName, Namespace: v1alpha2.KontinuumSystemNamespace}, &deployment)
	require.NoError(t, err, "nat gateway deployment must be installed on the zone's own downstream cluster")

	assert.Equal(t, "node-a1", deployment.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"])
	assert.True(t, deployment.Spec.Template.Spec.HostNetwork)
	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
	assert.Contains(t, deployment.Spec.Template.Spec.Containers[0].Args, "eth0")
}

func TestReconcileStickyGatewayNodeSelection(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"),
		testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"), testGatewayInstance("node-a2", "a"),
	)

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{client: downstream})

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

	var applyCalls []string

	downstreamCalls := 0
	reconciler := newReconciler(hubClient,
		fakeNetworkConfigurer{calls: &applyCalls},
		fakeDownstreamClientBuilder{err: errTestDownstreamBuild, calls: &downstreamCalls})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	assert.Empty(t, applyCalls, "nat disabled: talos config must never be pushed")
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

func TestReconcileNoGatewayCandidateOnlyBlocksThatZone(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)

	hubClient := newHubFakeClient(t,
		testFabricObject(), testZoneObject(testZoneAName, "a"), testZoneObject(testZoneBName, "b"),
		testTalosCluster(testZoneBName), talosClusterSecret(t, testZoneBName),
		testGatewayInstance("node-b1", "b"),
	)

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{client: downstream})

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
	assert.True(t, meta.IsStatusConditionTrue(byZone["b"].Conditions, fabric.ZoneReadyConditionType))
}

func TestReconcileAddsFinalizer(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testFabricObject())
	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	assert.True(t, controllerutil.ContainsFinalizer(&fabricObj, fabric.FabricFinalizer))
}

// fabricPendingDeletion builds a Fabric already carrying FabricFinalizer
// and a status.zones entry pointing at an elected gateway node — the
// fixture every teardown test starts from, standing in for a Fabric whose
// own reconcileFabric already ran to completion at least once before
// deletion was requested.
func fabricPendingDeletion() *v1alpha2.Fabric {
	fabricObj := testFabricObject()
	controllerutil.AddFinalizer(fabricObj, fabric.FabricFinalizer)
	fabricObj.Status.Zones = []v1alpha2.FabricZoneStatus{
		{
			Zone: "a", CIDR: blockCIDR0, GatewayIP: testBlockCIDR0GatewayIP,
			GatewayNodeRef: &v1alpha2.ObjectReference{
				APIVersion: v1alpha2.GroupVersion().String(), Kind: "Instance", Name: "node-a1",
			},
		},
	}

	return fabricObj
}

func TestReconcileTeardownDeletesFabricManagerWorkloadAndRemovesFinalizer(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t, fabricManagerDeployment())

	fabricObj := fabricPendingDeletion()

	hubClient := newHubFakeClient(t,
		fabricObj, testZoneObject(testZoneAName, "a"), testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"))

	require.NoError(t, hubClient.Delete(t.Context(), fabricObj))

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var deployment appsv1.Deployment

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testFabricManagerName, Namespace: testFabricManagerNamespace()}, &deployment)
	assert.True(t, apierrors.IsNotFound(err), "the zone's own fabric manager workload must be torn down")

	var check v1alpha2.Fabric

	err = hubClient.Get(t.Context(), fabricObjectKey(), &check)
	assert.True(t, apierrors.IsNotFound(err), "the fabric itself must be gone once its finalizer is removed")
}

func TestReconcileTeardownToleratesAlreadyGoneZone(t *testing.T) {
	t.Parallel()

	// No Zone/TalosCluster fixtures at all — the zone this Fabric once
	// carved a subnet for is already gone by the time teardown runs.
	fabricObj := fabricPendingDeletion()

	hubClient := newHubFakeClient(t, fabricObj)

	require.NoError(t, hubClient.Delete(t.Context(), fabricObj))

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{},
		fakeDownstreamClientBuilder{err: errTestDownstreamBuild})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var check v1alpha2.Fabric

	err = hubClient.Get(t.Context(), fabricObjectKey(), &check)
	assert.True(t, apierrors.IsNotFound(err), "teardown must not get stuck on a zone that's already gone")
}

func TestReconcileTeardownGivesUpAfterTimeout(t *testing.T) {
	t.Parallel()

	fabricObj := fabricPendingDeletion()

	hubClient := newHubFakeClient(t,
		fabricObj, testZoneObject(testZoneAName, "a"), testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName))

	require.NoError(t, hubClient.Delete(t.Context(), fabricObj))

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{},
		fakeDownstreamClientBuilder{err: errTestDownstreamBuild})
	// A near-zero timeout guarantees reconcileTeardown sees itself as
	// already past deadline on this very first attempt, even though the
	// downstream build below would otherwise fail teardown forever.
	reconciler.TeardownTimeout = time.Nanosecond

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var check v1alpha2.Fabric

	err = hubClient.Get(t.Context(), fabricObjectKey(), &check)
	assert.True(t, apierrors.IsNotFound(err),
		"the finalizer must be removed once the teardown timeout is exceeded, even though teardown itself never succeeded")
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

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{})

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

	var patches [][]byte

	reconciler := newReconciler(hubClient,
		fakeNetworkConfigurer{patches: &patches}, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	require.Len(t, patches, 1)
	patch := string(patches[0])
	assert.Contains(t, patch, "kind: BridgeConfig",
		"two free interfaces must be bridged, not each assigned the same address")
	assert.Contains(t, patch, testFabricInterface)
	assert.Contains(t, patch, "eth2")

	var fabricObj v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &fabricObj))
	require.Len(t, fabricObj.Status.Zones, 1)
	assert.ElementsMatch(t, []string{testFabricInterface, "eth2"}, fabricObj.Status.Zones[0].GatewayInterfaces)
}

// testAlreadyKnownZoneFabricObject returns testFabricObject() with its
// status pre-populated as if a previous reconcile had already run: fabric-
// level ValidSpec/Ready, and zone "a"'s GatewayNodeRef/CIDR/GatewayIP/
// GatewayInterfaces plus GatewayNodeSelected/NetworkConfigured conditions,
// all set to exactly what TestReconcilePersistsConditionTransitionOnAlreadyKnownZone's
// reconcile will recompute anyway — only NATInstalled/ZoneReady are left
// False, so they're the only conditions that actually change.
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
	networkConfiguredMessage := fmt.Sprintf(
		"assigned gateway address %s on instance %q, interfaces [%s]",
		testBlockCIDR0GatewayIP, "node-a1", testFabricInterface)

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
					Reason: "NetworkConfigured", Message: networkConfiguredMessage, LastTransitionTime: metav1.Now(),
				},
				{
					Type: fabric.NATInstalledConditionType, Status: metav1.ConditionFalse,
					Reason: "NATInstallFailed", Message: "downstream unreachable", LastTransitionTime: metav1.Now(),
				},
				{
					Type: fabric.ZoneReadyConditionType, Status: metav1.ConditionFalse,
					Reason: "ZoneNotReady", Message: "nat gateway workload not installed yet", LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	return fabricObj
}

// TestReconcilePersistsConditionTransitionOnAlreadyKnownZone is a
// regression test for the Conditions-slice-aliasing bug: entry.Conditions
// used to be assigned directly from the previous status.zones entry
// (`entry.Conditions = previous.Conditions`), sharing the same backing
// array as fabricObj.Status.Zones[i].Conditions. meta.SetStatusCondition
// mutates an existing condition's fields in place through a pointer into
// that array, which silently rewrote the "before" snapshot
// equalZoneStatuses compares against to already match "after" — hiding a
// real transition from change detection and skipping the Status().Update
// that should have persisted it.
func TestReconcilePersistsConditionTransitionOnAlreadyKnownZone(t *testing.T) {
	t.Parallel()

	downstream := newDownstreamFakeClient(t)
	fabricObj := testAlreadyKnownZoneFabricObject()

	hubClient := newHubFakeClient(t,
		fabricObj, testZoneObject(testZoneAName, "a"), testTalosCluster(testZoneAName), talosClusterSecret(t, testZoneAName),
		testGatewayInstance("node-a1", "a"))

	reconciler := newReconciler(hubClient, fakeNetworkConfigurer{}, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result, "the zone actually converges this pass, so no more retries are needed")

	var persisted v1alpha2.Fabric

	require.NoError(t, hubClient.Get(t.Context(), fabricObjectKey(), &persisted))
	require.Len(t, persisted.Status.Zones, 1)
	assert.True(t, meta.IsStatusConditionTrue(persisted.Status.Zones[0].Conditions, fabric.NATInstalledConditionType),
		"the False->True transition must actually be written to the API server, not just computed in memory")
	assert.True(t, meta.IsStatusConditionTrue(persisted.Status.Zones[0].Conditions, fabric.ZoneReadyConditionType),
		"the False->True transition must actually be written to the API server, not just computed in memory")
}
