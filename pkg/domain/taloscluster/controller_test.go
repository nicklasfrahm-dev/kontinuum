package taloscluster_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

const (
	testHealthCheckTimeout  = time.Second
	testRetryInterval       = 15 * time.Second
	testHealthCheckInterval = 5 * time.Minute
)

// fakeBootstrapper is taloscluster.ClusterBootstrapper's test double — it
// never dials real gRPC, and records every address it was asked to touch
// so tests can assert on call order/targets (e.g. that a worker is never
// touched until the control plane is ready).
type fakeBootstrapper struct {
	applyConfigCalls []string
	// appliedConfigs mirrors applyConfigCalls, capturing the actual bytes
	// each address's ApplyConfiguration call carried — see
	// TestReconcileGeneratesInstallableConfigs' own use.
	appliedConfigs [][]byte
	bootstrapCalls []string
	bootstrapErr   error
	healthCheckErr error
	kubeconfig     []byte
	kubeconfigErr  error
	versionCalls   []string
	version        string
	versionErr     error
}

func (f *fakeBootstrapper) ApplyConfiguration(_ context.Context, addr string, data []byte) error {
	f.applyConfigCalls = append(f.applyConfigCalls, addr)
	f.appliedConfigs = append(f.appliedConfigs, data)

	return nil
}

func (f *fakeBootstrapper) Bootstrap(_ context.Context, addr string, _ *clientconfig.Config) error {
	f.bootstrapCalls = append(f.bootstrapCalls, addr)

	return f.bootstrapErr
}

func (f *fakeBootstrapper) HealthCheck(
	_ context.Context, _ string, _ *clientconfig.Config, _ []string, _ time.Duration,
) error {
	return f.healthCheckErr
}

func (f *fakeBootstrapper) Kubeconfig(_ context.Context, _ string, _ *clientconfig.Config) ([]byte, error) {
	if f.kubeconfigErr != nil {
		return nil, f.kubeconfigErr
	}

	return f.kubeconfig, nil
}

func (f *fakeBootstrapper) Version(_ context.Context, _, node string, _ *clientconfig.Config) (string, error) {
	f.versionCalls = append(f.versionCalls, node)

	if f.versionErr != nil {
		return "", f.versionErr
	}

	return f.version, nil
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.TalosCluster{}, &v1alpha2.Addon{}, &v1alpha2.Instance{}).
		WithObjects(objects...).
		Build()
}

func claimedDiscoveredInstance(name, poolName, addr string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha2.LabelClaimedBy: poolName}},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{addr}},
		Status: v1alpha2.InstanceStatus{
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
			},
		},
	}
}

// testClusterName is every fixture's own TalosCluster name — kept as one
// constant rather than a parameter every test helper threads through,
// since no test in this file needs more than one cluster.
const testClusterName = "eu-1a"

const (
	controlPlaneInstanceAddress = "10.0.0.1"
	readyConditionReasonHealthy = "Healthy"
	ciliumAddonResourceName     = testClusterName + "-cilium"
	cpNodeName                  = "cp-node-1"
)

func testCluster() *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: "cp-pool"},
			},
			Workers: []v1alpha2.TalosClusterWorkerSpec{
				{Name: "default", PoolRef: v1alpha2.InstancePoolReference{Name: "worker-pool"}},
			},
		},
	}
}

// readyAddon builds an Addon fixture already reporting the given Ready
// status/reason — as if Reconciler (tested separately, in
// pkg/domain/addon) had already reconciled it.
func readyAddon(releaseName string, status metav1.ConditionStatus, reason string) *v1alpha2.Addon {
	return &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-" + releaseName},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     releaseName,
		},
		Status: v1alpha2.AddonStatus{
			Conditions: []metav1.Condition{{Type: "Ready", Status: status, Reason: reason}},
		},
	}
}

func newReconciler(fakeClient client.Client, bootstrapper *fakeBootstrapper) *taloscluster.Reconciler {
	return &taloscluster.Reconciler{
		Client:              fakeClient,
		Bootstrapper:        bootstrapper,
		HealthCheckTimeout:  testHealthCheckTimeout,
		RetryInterval:       testRetryInterval,
		HealthCheckInterval: testHealthCheckInterval,
		Logger:              slog.Default(),
	}
}

func TestReconcileWaitsForControlPlaneInstances(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	fakeClient := newFakeClient(t, cluster)

	bootstrapper := &fakeBootstrapper{}
	reconciler := newReconciler(fakeClient, bootstrapper)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, bootstrapper.applyConfigCalls, "no candidates means no dial attempt at all")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "WaitingForInstances", cond.Reason)
}

func TestReconcileControlPlaneNotYetHealthy(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{healthCheckErr: assert.AnError}
	reconciler := newReconciler(fakeClient, bootstrapper)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.applyConfigCalls)
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.bootstrapCalls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.BootstrappedConditionType),
		"Bootstrap succeeding is independent of ControlPlaneReady — it doesn't wait on HealthCheck")
}

// TestReconcileNeverRepeatsBootstrapOnceSucceeded covers ensureBootstrapped:
// once Bootstrapped is true, a later reconcile — still not ControlPlaneReady,
// e.g. HealthCheck still failing — must not call Bootstrap again.
func TestReconcileNeverRepeatsBootstrapOnceSucceeded(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{healthCheckErr: assert.AnError}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.bootstrapCalls)

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.bootstrapCalls,
		"a second reconcile must not call Bootstrap again once it already succeeded once")
}

// TestReconcileTreatsAlreadyExistsAsBootstrapped covers ensureBootstrapped's
// other success path: a Bootstrap call failing with codes.AlreadyExists
// (etcd's data directory already populated) proves the cluster was already
// bootstrapped, even though this TalosCluster's own status doesn't reflect
// that yet — so it must still mark Bootstrapped true, not just log and
// retry forever.
func TestReconcileTreatsAlreadyExistsAsBootstrapped(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{
		bootstrapErr:   status.Error(codes.AlreadyExists, "etcd data directory is not empty"),
		healthCheckErr: assert.AnError,
	}
	reconciler := newReconciler(fakeClient, bootstrapper)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.BootstrappedConditionType))
}

// TestReconcileSeedsBuiltinAddonsOnlyWhenMissing covers
// addon.EnsureBuiltinSeeds' own create-only contract: on the first
// reconcile, with no Addons at all yet, both cilium's and cert-manager's
// get created — minimal spec (just TalosClusterRef/ReleaseName),
// owned by the TalosCluster. A user edit afterward (here, disabling one)
// must never get clobbered by a later reconcile — TalosCluster never
// touches an Addon's spec once it exists.
func TestReconcileSeedsBuiltinAddonsOnlyWhenMissing(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var cilium v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: ciliumAddonResourceName}, &cilium))
	assert.Equal(t, testClusterName, cilium.Spec.TalosClusterRef.Name)
	assert.Equal(t, "cilium", cilium.Spec.ReleaseName)
	assert.Nil(t, cilium.Spec.Chart, "a built-in seed leaves Chart unset — resolveAddon supplies the fallback")
	require.Len(t, cilium.OwnerReferences, 1)
	assert.Equal(t, testClusterName, cilium.OwnerReferences[0].Name)
	assert.Equal(t, "TalosCluster", cilium.OwnerReferences[0].Kind)

	var certManager v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: "eu-1a-cert-manager"}, &certManager))
	assert.Equal(t, "cert-manager", certManager.Spec.ReleaseName)

	// simulate a user disabling cilium after the fact
	cilium.Spec.Enabled = new(bool)
	require.NoError(t, fakeClient.Update(context.Background(), &cilium))

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var afterSecond v1alpha2.Addon

	afterSecondKey := types.NamespacedName{Name: ciliumAddonResourceName}
	require.NoError(t, fakeClient.Get(context.Background(), afterSecondKey, &afterSecond))
	require.NotNil(t, afterSecond.Spec.Enabled)
	assert.False(t, *afterSecond.Spec.Enabled, "a second reconcile must never re-enable a user-disabled built-in")
}

// TestReconcileAggregatesReadyAcrossAddons covers reconcileAddons'
// aggregation of pre-existing Addon status (installation/health-probing
// itself is pkg/domain/addon's own Reconciler, tested separately):
// both built-ins already Ready=True (so neither gets re-seeded) makes
// TalosCluster's own Ready true; either one reporting False keeps it
// false, with a message naming which.
func TestReconcileAggregatesReadyAcrossAddons(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Status.Conditions = []metav1.Condition{
		{
			Type:   taloscluster.ControlPlaneReadyConditionType,
			Status: metav1.ConditionTrue, Reason: readyConditionReasonHealthy,
		},
	}
	cluster.Spec.Workers = nil

	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	gatewayCRDs := readyAddon("gateway-api-crds", metav1.ConditionTrue, "Healthy")
	cilium := readyAddon("cilium", metav1.ConditionTrue, "Healthy")
	certManager := readyAddon("cert-manager", metav1.ConditionFalse, "NotHealthy")

	fakeClient := newFakeClient(t, cluster, cpInstance, gatewayCRDs, cilium, certManager)

	reconciler := newReconciler(fakeClient, &fakeBootstrapper{})

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonNotHealthy", cond.Reason)
	assert.Contains(t, cond.Message, "cert-manager")

	// flip cert-manager to healthy — Ready must follow on the next reconcile
	certManager.Status.Conditions[0].Status = metav1.ConditionTrue
	require.NoError(t, fakeClient.Status().Update(context.Background(), certManager))

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ReadyConditionType))
}

// TestReconcileDisabledAddonSkippedInAggregation covers a disabled Addon
// (spec.enabled: false) never blocking TalosCluster's own Ready, even
// with no Ready condition of its own at all — Reconciler never
// touches a disabled Addon's status (see its own doc).
func TestReconcileDisabledAddonSkippedInAggregation(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Status.Conditions = []metav1.Condition{
		{
			Type:   taloscluster.ControlPlaneReadyConditionType,
			Status: metav1.ConditionTrue, Reason: readyConditionReasonHealthy,
		},
	}
	cluster.Spec.Workers = nil

	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	gatewayCRDs := readyAddon("gateway-api-crds", metav1.ConditionTrue, "Healthy")
	certManager := readyAddon("cert-manager", metav1.ConditionTrue, "Healthy")
	cilium := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: ciliumAddonResourceName},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     "cilium",
			Enabled:         new(bool),
		},
	}

	fakeClient := newFakeClient(t, cluster, cpInstance, gatewayCRDs, cilium, certManager)

	reconciler := newReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ReadyConditionType),
		"a disabled addon with no Ready condition at all must never block Ready")
}

// TestReconcileCustomAddonCountsTowardReady covers a fully custom
// (non-built-in) Addon counting toward TalosCluster's own Ready
// aggregation exactly like a built-in — no special treatment.
func TestReconcileCustomAddonCountsTowardReady(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Status.Conditions = []metav1.Condition{
		{
			Type:   taloscluster.ControlPlaneReadyConditionType,
			Status: metav1.ConditionTrue, Reason: readyConditionReasonHealthy,
		},
	}
	cluster.Spec.Workers = nil

	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	gatewayCRDs := readyAddon("gateway-api-crds", metav1.ConditionTrue, "Healthy")
	cilium := readyAddon("cilium", metav1.ConditionTrue, "Healthy")
	certManager := readyAddon("cert-manager", metav1.ConditionTrue, "Healthy")
	custom := readyAddon("my-addon", metav1.ConditionFalse, "NotHealthy")

	fakeClient := newFakeClient(t, cluster, cpInstance, gatewayCRDs, cilium, certManager, custom)

	reconciler := newReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "my-addon")
}

// TestReconcileFullSequence is the issue's own milestone: workers are
// never touched until the control plane is ready — checked at the START
// of a reconcile, so becoming ready mid-pass doesn't count and a second
// reconcile is still needed for that. Addon installation/health-probing
// itself is pkg/domain/addon's own asynchronous concern now (see
// Reconciler) — this only confirms TalosCluster's own reconciler
// seeds both built-ins as part of reaching ControlPlaneReady, without
// waiting for them to actually become healthy first.
func TestReconcileFullSequence(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	workerInstance := claimedDiscoveredInstance("worker-node-1", "worker-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	result, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "freshly-seeded addons have no Ready condition yet")
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.applyConfigCalls,
		"only the control-plane member is touched before ControlPlaneReady")

	var cilium, certManager v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: ciliumAddonResourceName}, &cilium))
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: "eu-1a-cert-manager"}, &certManager))

	var afterFirst v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &afterFirst))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.BootstrappedConditionType))
	assert.False(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.ReadyConditionType))
	require.NotEmpty(t, afterFirst.Status.SecretRef.Name)

	var secret corev1.Secret

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: afterFirst.Status.SecretRef.Name, Namespace: afterFirst.Status.SecretRef.Namespace},
		&secret))
	assert.Equal(t, []byte("fake-kubeconfig"), secret.Data["kubeconfig"])

	result, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Equal(t, []string{controlPlaneInstanceAddress, "10.0.0.2"}, bootstrapper.applyConfigCalls,
		"the worker is only touched on the reconcile after ControlPlaneReady")
}

// TestReconcileRecordsTalosVersionOnceMembersAreConfigured covers
// recordTalosVersions: instance.Discoverer's own maintenance-mode Version
// call can no longer succeed on current Talos releases (see its own doc),
// so status.talos.version is populated later instead — for control-plane
// members once HealthCheck first passes, for worker members once their
// config has been applied — dialing through any already-configured member
// and targeting each individually. A member whose version is already known
// is never re-fetched.
func TestReconcileRecordsTalosVersionOnceMembersAreConfigured(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	workerInstance := claimedDiscoveredInstance("worker-node-1", "worker-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig"), version: "v1.9.0"}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.versionCalls,
		"only the now-healthy control-plane member is checked on the first reconcile")

	var afterFirst v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: cpNodeName}, &afterFirst))
	assert.Equal(t, "v1.9.0", afterFirst.Status.Talos.Version)

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, []string{controlPlaneInstanceAddress, "10.0.0.2"}, bootstrapper.versionCalls,
		"the control-plane member's already-known version is never re-fetched; "+
			"the worker is checked once its config is applied")

	var afterSecond v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "worker-node-1"}, &afterSecond))
	assert.Equal(t, "v1.9.0", afterSecond.Status.Talos.Version)
}

// TestReconcileToleratesTalosVersionFetchFailure covers recordTalosVersions'
// own tolerance of a member still rebooting into its new configuration —
// the same best-effort handling as ApplyConfiguration's — leaving
// status.talos.version empty rather than failing the reconcile.
func TestReconcileToleratesTalosVersionFetchFailure(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Workers = nil
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig"), versionErr: assert.AnError}
	reconciler := newReconciler(fakeClient, bootstrapper)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: cpNodeName}, &got))
	assert.Empty(t, got.Status.Talos.Version)
}

// TestReconcileGeneratesInstallableConfigs covers applyAutoInstallDiskSelector's
// own contract, exercised through the full Reconcile path rather than
// calling generateConfigs directly (config.go's own functions are
// unexported, and this file's package is taloscluster_test — see this
// repo's own testpackage lint convention): every config actually applied
// to a real (non-container) candidate must carry a non-nil
// InstallDiskSelector, or Talos's own config validation rejects it
// outright with "either install disk or diskSelector should be defined" —
// this repo has no way to know a candidate's real disk names
// (/dev/sda, /dev/vda, /dev/nvme0n1, ...) at config-generation time, so
// hardcoding install.disk was never an option; an empty selector is
// Talos's own "any disk" mechanism instead (see config.go's own doc).
func TestReconcileGeneratesInstallableConfigs(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	workerInstance := claimedDiscoveredInstance("worker-node-1", "worker-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	require.Len(t, bootstrapper.appliedConfigs, 2, "one control-plane apply, one worker apply")

	for index, data := range bootstrapper.appliedConfigs {
		provider, err := configloader.NewFromBytes(data)
		require.NoError(t, err, "applied config %d", index)

		install := provider.RawV1Alpha1().MachineConfig.MachineInstall
		require.NotNil(t, install, "applied config %d", index)
		assert.Empty(t, install.InstallDisk, "applied config %d", index)
		assert.NotNil(t, install.InstallDiskSelector, "applied config %d", index)
	}
}

// TestReconcileSetsHostnameToInstanceName covers configBytes' own hostname
// contract: each member's applied config carries a hostname set to that
// specific member's own Instance name, not left to Talos's own
// DHCP/mDNS-derived default and not shared verbatim across every member of
// a role — `kubectl get nodes` and this Instance's own name should agree on
// what to call it.
func TestReconcileSetsHostnameToInstanceName(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	workerInstance := claimedDiscoveredInstance("worker-node-1", "worker-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	require.Len(t, bootstrapper.appliedConfigs, 2, "one control-plane apply, one worker apply")

	wantHostname := map[string]string{
		controlPlaneInstanceAddress: cpInstance.Name,
		"10.0.0.2":                  workerInstance.Name,
	}

	for index, data := range bootstrapper.appliedConfigs {
		provider, err := configloader.NewFromBytes(data)
		require.NoError(t, err, "applied config %d", index)

		addr := bootstrapper.applyConfigCalls[index]
		hostnameConfig := provider.NetworkHostnameConfig()
		require.NotNil(t, hostnameConfig, "applied config %d (address %s)", index, addr)
		assert.Equal(t, wantHostname[addr], hostnameConfig.Hostname(), "applied config %d (address %s)", index, addr)
	}
}

// TestReconcileSetsMemberConditionsThroughBootstrapPipeline covers issue
// #62: a claimed, Discovered Instance gets Configured set once its config
// is applied, Joined once it answers a post-config Version RPC, and — for
// a control-plane member only — Ready once the cluster-wide HealthCheck
// that covers it passes. A worker member gets Configured and Joined but
// never Ready, since this package has no per-worker health probe (see
// MemberReadyConditionType's own doc).
func TestReconcileSetsMemberConditionsThroughBootstrapPipeline(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	workerInstance := claimedDiscoveredInstance("worker-node-1", "worker-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig"), version: "v1.9.0"}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var afterFirst v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: cpNodeName}, &afterFirst))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.MemberConfiguredConditionType),
		"control-plane member's config was applied on the first reconcile")
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.MemberJoinedConditionType),
		"control-plane member answered a Version RPC once HealthCheck passed")
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.MemberReadyConditionType),
		"control-plane member was part of the batch HealthCheck just verified healthy")

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var afterSecond v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "worker-node-1"}, &afterSecond))
	assert.True(t, meta.IsStatusConditionTrue(afterSecond.Status.Conditions, taloscluster.MemberConfiguredConditionType),
		"worker member's config was applied once ControlPlaneReady")
	assert.True(t, meta.IsStatusConditionTrue(afterSecond.Status.Conditions, taloscluster.MemberJoinedConditionType),
		"worker member answered a Version RPC once its config was applied")
	assert.False(t, meta.IsStatusConditionTrue(afterSecond.Status.Conditions, taloscluster.MemberReadyConditionType),
		"a worker never gets Ready — no per-worker health probe backs it yet")
}

// convergeFullyReadyCluster drives reconciler through three reconciles —
// bootstrap, addon-Ready aggregation, then recheckControlPlaneHealth — the
// same three-step path TestReconcileFullSequence's own doc explains: each
// condition flip returns immediately, so reaching the state after both
// ControlPlaneReady and Ready are true needs a following reconcile past the
// one that set the second of them. Returns the third reconcile's own
// result, the one that actually reaches recheckControlPlaneHealth.
func convergeFullyReadyCluster(
	t *testing.T, fakeClient client.Client, reconciler *taloscluster.Reconciler, req ctrl.Request,
) ctrl.Result {
	t.Helper()

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var gatewayCRDs, cilium, certManager v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: testClusterName + "-gateway-api-crds"}, &gatewayCRDs))
	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: ciliumAddonResourceName}, &cilium))
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: testClusterName + "-cert-manager"}, &certManager))

	for _, obj := range []*v1alpha2.Addon{&gatewayCRDs, &cilium, &certManager} {
		obj.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy"}}
		require.NoError(t, fakeClient.Status().Update(context.Background(), obj))
	}

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	result, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	return result
}

// TestReconcileRecheckControlPlaneHealthAfterConvergence covers issue #62's
// own follow-up: once ControlPlaneReady and Ready are both true, Reconcile
// must keep re-probing control-plane health on a timer (HealthCheckInterval)
// instead of going silent forever — and a previously Ready member must
// flip to Ready=False if a later recheck fails, while
// ControlPlaneReadyConditionType itself stays untouched (a flaky per-node
// recheck must never re-trigger reconcileControlPlane's own config-apply
// path).
func TestReconcileRecheckControlPlaneHealthAfterConvergence(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Workers = nil
	cpInstance := claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	result := convergeFullyReadyCluster(t, fakeClient, reconciler, req)
	assert.Equal(t, testHealthCheckInterval, result.RequeueAfter,
		"a fully converged cluster must keep reconciling on a timer, not go silent forever")

	var cpAfterConverged v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: cpNodeName}, &cpAfterConverged))
	assert.True(t, meta.IsStatusConditionTrue(cpAfterConverged.Status.Conditions, taloscluster.MemberReadyConditionType))

	// Simulate the control plane later failing a health check — no config
	// changes, no bootstrap needed, just an unhealthy node.
	bootstrapper.healthCheckErr = assert.AnError

	result, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "an unhealthy recheck retries sooner than a healthy one")

	var cpAfterFailure v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: cpNodeName}, &cpAfterFailure))
	assert.False(t, meta.IsStatusConditionTrue(cpAfterFailure.Status.Conditions, taloscluster.MemberReadyConditionType),
		"a previously healthy member must flip to Ready=False once a recheck fails")

	var clusterAfterFailure v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: testClusterName}, &clusterAfterFailure))
	assert.True(t,
		meta.IsStatusConditionTrue(clusterAfterFailure.Status.Conditions, taloscluster.ControlPlaneReadyConditionType),
		"a flaky per-node recheck must never flip the cluster-level ControlPlaneReady back to false")
}

func TestReconcileIgnoresMissingCluster(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	reconciler := newReconciler(fakeClient, &fakeBootstrapper{})

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
}
