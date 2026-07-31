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

// addonInstallCall records one AddonInstaller.Install invocation's
// arguments, for asserting the right chart/repo/namespace was used.
type addonInstallCall struct {
	releaseName string
	repoURL     string
	chartName   string
	version     string
	namespace   string
	values      map[string]any
}

// fakeAddonInstaller is taloscluster.AddonInstaller's test double.
type fakeAddonInstaller struct {
	calls []addonInstallCall
	err   error
}

func (f *fakeAddonInstaller) Install(
	_ context.Context, _ []byte, releaseName, repoURL, chartName, version, namespace string, values map[string]any,
) error {
	f.calls = append(f.calls, addonInstallCall{releaseName, repoURL, chartName, version, namespace, values})

	return f.err
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.TalosCluster{}).
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

func testCluster() *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a"},
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

func newReconciler(
	fakeClient client.Client, bootstrapper *fakeBootstrapper, installer *fakeAddonInstaller,
) *taloscluster.Reconciler {
	return &taloscluster.Reconciler{
		Client:             fakeClient,
		Bootstrapper:       bootstrapper,
		AddonInstaller:     installer,
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
	installer := &fakeAddonInstaller{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, bootstrapper.applyConfigCalls, "no candidates means no dial attempt at all")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "WaitingForInstances", cond.Reason)
}

func TestReconcileControlPlaneNotYetHealthy(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{healthCheckErr: assert.AnError}
	installer := &fakeAddonInstaller{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.applyConfigCalls)
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.bootstrapCalls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.BootstrappedConditionType))
}

// TestReconcileCiliumInstallFailureBlocksControlPlaneReady covers
// installCiliumEarly's own failure branch — distinct from
// TestReconcileControlPlaneNotYetHealthy's health-check failure — and
// confirms the health check is never reached (and reconcileWorkers/
// reconcileAddons's cert-manager install never run) when Cilium itself
// fails to install.
func TestReconcileCiliumInstallFailureBlocksControlPlaneReady(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{err: assert.AnError}
	reconciler := newReconciler(fakeClient, bootstrapper, installer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.bootstrapCalls)
	require.Len(t, installer.calls, 1, "only cilium is attempted, never cert-manager")
	assert.Equal(t, "cilium", installer.calls[0].releaseName)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "CiliumInstallFailed", cond.Reason)
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.BootstrappedConditionType))
}

// TestReconcileFullSequence is the issue's own milestone: workers are
// never touched until the control plane is ready. Cilium installs as soon
// as the apiserver is reachable — on the very first reconcile, as part of
// reaching ControlPlaneReady itself, not gated behind the full health
// check (see installCiliumEarly's own doc for why: the health check
// itself needs a working pod network). cert-manager installs afterward,
// on the reconcile after ControlPlaneReady — mirroring how the
// watch-driven controller actually converges across multiple reconciles.
func TestReconcileFullSequence(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")
	workerInstance := claimedDiscoveredInstance("worker-node-1", "worker-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "eu-1a"}}

	result, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Zero(t, result, "control plane becoming ready needs no requeue")
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.applyConfigCalls,
		"only the control-plane member is touched before ControlPlaneReady")

	require.Len(t, installer.calls, 1, "cilium installs as soon as the apiserver is reachable, not after full health")
	assert.Equal(t, "cilium", installer.calls[0].releaseName)
	assert.Equal(t, "https://helm.cilium.io", installer.calls[0].repoURL)
	assert.Equal(t, "kube-system", installer.calls[0].namespace)

	var afterFirst v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &afterFirst))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.BootstrappedConditionType))
	require.NotEmpty(t, afterFirst.Status.SecretRef.Name)

	var secret corev1.Secret

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: afterFirst.Status.SecretRef.Name, Namespace: afterFirst.Status.SecretRef.Namespace},
		&secret))
	assert.Equal(t, []byte("fake-kubeconfig"), secret.Data["kubeconfig"])

	result, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Zero(t, result, "addons installing needs no requeue")
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, bootstrapper.applyConfigCalls,
		"the worker is only touched on the reconcile after ControlPlaneReady")

	require.Len(t, installer.calls, 2)
	assert.Equal(t, "cert-manager", installer.calls[1].releaseName)
	assert.Equal(t, "https://charts.jetstack.io", installer.calls[1].repoURL)
	assert.Equal(t, "cert-manager", installer.calls[1].namespace)

	var afterSecond v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &afterSecond))
	assert.True(t, meta.IsStatusConditionTrue(afterSecond.Status.Conditions, taloscluster.ReadyConditionType))
}

func TestReconcileAddonInstallFailureSetsReadyFalse(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Status.Conditions = []metav1.Condition{
		{Type: taloscluster.ControlPlaneReadyConditionType, Status: metav1.ConditionTrue, Reason: "Healthy"},
	}
	cluster.Status.SecretRef = v1alpha2.SecretReference{
		Name: "taloscluster-eu-1a", Namespace: v1alpha2.DefaultSecretNamespace,
	}
	cluster.Spec.Workers = nil

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "taloscluster-eu-1a", Namespace: v1alpha2.DefaultSecretNamespace},
		Data:       map[string][]byte{"secrets-bundle": []byte("{}"), "kubeconfig": []byte("fake-kubeconfig")},
	}

	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance, secret)

	bootstrapper := &fakeBootstrapper{}
	installer := &fakeAddonInstaller{err: assert.AnError}
	reconciler := newReconciler(fakeClient, bootstrapper, installer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, bootstrapper.applyConfigCalls, "control plane is already ready, so it's never re-touched")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonInstallFailed", cond.Reason)
}

func TestReconcileIgnoresMissingCluster(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	reconciler := newReconciler(fakeClient, &fakeBootstrapper{}, &fakeAddonInstaller{})

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
}
