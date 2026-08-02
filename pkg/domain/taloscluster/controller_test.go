package taloscluster_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

const (
	testHealthCheckTimeout = time.Second
	testRetryInterval      = 15 * time.Second
)

// fakeBootstrapper is taloscluster.ClusterBootstrapper's test double — it
// never dials real gRPC, and records every address it was asked to touch
// so tests can assert on call order/targets (e.g. that a worker is never
// touched until the control plane is ready).
type fakeBootstrapper struct {
	applyConfigCalls []string
	bootstrapCalls   []string
	healthCheckErr   error
	kubeconfig       []byte
	kubeconfigErr    error
}

func (f *fakeBootstrapper) ApplyConfiguration(_ context.Context, addr string, _ []byte) error {
	f.applyConfigCalls = append(f.applyConfigCalls, addr)

	return nil
}

func (f *fakeBootstrapper) Bootstrap(_ context.Context, addr string, _ *clientconfig.Config) error {
	f.bootstrapCalls = append(f.bootstrapCalls, addr)

	return nil
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

// indexAddonByTalosClusterRef mirrors the addon package's own unexported
// indexer — duplicated here since the real one isn't reachable from an
// external test package, and the manager's own field indexer registration
// (Controller.SetupWithManager) never runs in these fake-client-only
// tests.
func indexAddonByTalosClusterRef(obj client.Object) []string {
	a, ok := obj.(*v1alpha2.Addon)
	if !ok {
		return nil
	}

	return []string{a.Spec.TalosClusterRef.Name}
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.TalosCluster{}, &v1alpha2.Addon{}).
		WithIndex(&v1alpha2.Addon{}, addon.TalosClusterRefField, indexAddonByTalosClusterRef).
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
		Client:             fakeClient,
		Bootstrapper:       bootstrapper,
		HealthCheckTimeout: testHealthCheckTimeout,
		RetryInterval:      testRetryInterval,
		Logger:             slog.Default(),
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
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.BootstrappedConditionType))
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
	cilium := readyAddon("cilium", metav1.ConditionTrue, "Healthy")
	certManager := readyAddon("cert-manager", metav1.ConditionFalse, "NotHealthy")

	fakeClient := newFakeClient(t, cluster, cpInstance, cilium, certManager)

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
	certManager := readyAddon("cert-manager", metav1.ConditionTrue, "Healthy")
	cilium := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: ciliumAddonResourceName},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     "cilium",
			Enabled:         new(bool),
		},
	}

	fakeClient := newFakeClient(t, cluster, cpInstance, cilium, certManager)

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
	cilium := readyAddon("cilium", metav1.ConditionTrue, "Healthy")
	certManager := readyAddon("cert-manager", metav1.ConditionTrue, "Healthy")
	custom := readyAddon("my-addon", metav1.ConditionFalse, "NotHealthy")

	fakeClient := newFakeClient(t, cluster, cpInstance, cilium, certManager, custom)

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
