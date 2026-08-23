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
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
// own NAT gateway node: labeled kontinuum.sh/zone=zone and
// role=gateway (matching testFabricObject's own GatewaySelector), claimed,
// with one discovered interface.
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
			Interfaces: []v1alpha2.InstanceInterfaceStatus{{Name: "eth0", Addresses: []string{"10.0.1.5/24"}}},
		},
	}
}

// fakeNetworkConfigurer is fabric.NetworkConfigurer's test double.
type fakeNetworkConfigurer struct {
	err   error
	calls *[]string
}

func (f fakeNetworkConfigurer) ApplyInterfaceConfig(
	_ context.Context, addr string, _ *clientconfig.Config, _ []byte,
) error {
	if f.calls != nil {
		*f.calls = append(*f.calls, addr)
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

func newDownstreamFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func newReconciler(
	hubClient client.Client, networkConfigurer fabric.NetworkConfigurer, downstreamBuilder zone.DownstreamClientBuilder,
) *fabric.Reconciler {
	return &fabric.Reconciler{
		Client:                  hubClient,
		NetworkConfigurer:       networkConfigurer,
		DownstreamClientBuilder: downstreamBuilder,
		Image:                   testImage,
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
	assert.Equal(t, "10.0.0.254", fabricObj.Status.Zones[0].GatewayIP)
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
		client.ObjectKey{Name: "kontinuum-nat-gateway", Namespace: v1alpha2.KontinuumSystemNamespace}, &deployment)
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
