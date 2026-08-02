package taloscluster_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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

// fakeAddonInstaller is taloscluster.AddonInstaller's test double.
type fakeAddonInstaller struct {
	calls []taloscluster.AddonInstallRequest
	err   error
}

func (f *fakeAddonInstaller) Install(_ context.Context, _ []byte, req taloscluster.AddonInstallRequest) error {
	f.calls = append(f.calls, req)

	return f.err
}

// namespaceHealthResult is one PodProber.NamespaceHealthy fake result.
type namespaceHealthResult struct {
	healthy bool
	reason  string
	err     error
}

// fakePodProber is taloscluster.PodProber's test double. A namespace with
// no configured result defaults to healthy — most tests don't care about
// pod health specifically and just need the addon-install-succeeded path
// to keep progressing.
type fakePodProber struct {
	results map[string]namespaceHealthResult
	calls   []string
}

func (f *fakePodProber) NamespaceHealthy(_ context.Context, _ []byte, namespace string) (bool, string, error) {
	f.calls = append(f.calls, namespace)

	result, ok := f.results[namespace]
	if !ok {
		return true, "", nil
	}

	return result.healthy, result.reason, result.err
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
	fakeClient client.Client, bootstrapper *fakeBootstrapper, installer *fakeAddonInstaller, prober *fakePodProber,
) *taloscluster.Reconciler {
	return &taloscluster.Reconciler{
		Client:             fakeClient,
		Bootstrapper:       bootstrapper,
		AddonInstaller:     installer,
		PodProber:          prober,
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
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

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
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

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

// TestReconcileCiliumNotHealthyBlocksControlPlaneReady covers
// probeAddonHealthy's gating of ControlPlaneReady: even though the Cilium
// Install call itself succeeds (installRelease/upgradeRelease only apply
// manifests, they don't wait for rollout — see that doc), ControlPlaneReady
// must not go true, and the health check must never run, until Cilium's
// own pods actually report healthy.
func TestReconcileCiliumNotHealthyBlocksControlPlaneReady(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{results: map[string]namespaceHealthResult{
		"kube-system": {healthy: false, reason: "kube-system/cilium-abc123 is Running but not Ready"},
	}}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	require.Len(t, installer.calls, 1, "cilium install is still attempted")
	assert.Equal(t, []string{"kube-system"}, prober.calls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonNotHealthy", cond.Reason)
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.BootstrappedConditionType),
		"the health check must never run while cilium isn't healthy yet")
}

// TestReconcileCiliumDisabledSkipsInstall covers AddonSpec.Enabled: once
// explicitly set false, ControlPlaneReady must go true without ever
// calling AddonInstaller.Install or PodProber.NamespaceHealthy for cilium
// — an addon a user has opted out of (e.g. because ArgoCD already owns it)
// must never block the reconciler.
func TestReconcileCiliumDisabledSkipsInstall(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Addons.Cilium = v1alpha2.AddonSpec{Enabled: new(bool)}
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Zero(t, result, "control plane becoming ready needs no requeue")
	assert.Empty(t, installer.calls, "a disabled addon must never be installed")
	assert.Empty(t, prober.calls, "a disabled addon must never be health-probed")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
}

// TestReconcileAddonNamespaceVersionAndValuesOverride covers
// AddonSpec.Namespace/.Version/.Values all being honored together: the
// resulting AddonInstallRequest carries the overridden namespace/version,
// and the request's Values has the user-supplied value winning over the
// package's own default for a conflicting key (kubeProxyReplacement),
// while a default-only key (envoy.enabled) survives untouched — see
// mergeValues' own doc for why.
func TestReconcileAddonNamespaceVersionAndValuesOverride(t *testing.T) {
	t.Parallel()

	userValues, err := json.Marshal(map[string]any{"kubeProxyReplacement": false, "extraFlag": true})
	require.NoError(t, err)

	cluster := testCluster()
	cluster.Spec.Addons.Cilium = v1alpha2.AddonSpec{
		Namespace: "custom-cilium-ns",
		Version:   "1.99.0",
		Values:    &apiextensionsv1.JSON{Raw: userValues},
	}

	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 1)
	req := installer.calls[0]
	assert.Equal(t, "custom-cilium-ns", req.Namespace)
	assert.Equal(t, "1.99.0", req.Version)
	assert.Equal(t, false, req.Values["kubeProxyReplacement"], "user value must win over the package default")
	assert.Equal(t, true, req.Values["extraFlag"], "a user-only key must survive the merge")

	envoy, ok := req.Values["envoy"].(map[string]any)
	require.True(t, ok, "a default-only key must survive the merge untouched")
	assert.Equal(t, true, envoy["enabled"])
}

// TestReconcileCiliumOperatorReplicasScaleWithControlPlaneCount covers
// ciliumValues' control-plane-count-based operator.replicas override: a
// single-control-plane cluster keeps values/cilium.yaml's own default (1,
// since a second replica could never even schedule with hostNetwork: true
// — see multiControlPlaneOperatorReplicas' own doc), while a
// multi-control-plane cluster gets bumped to 2.
func TestReconcileCiliumOperatorReplicasScaleWithControlPlaneCount(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance1 := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")
	cpInstance2 := claimedDiscoveredInstance("cp-node-2", "cp-pool", "10.0.0.2")

	fakeClient := newFakeClient(t, cluster, cpInstance1, cpInstance2)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 1)

	operator, ok := installer.calls[0].Values["operator"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 2, operator["replicas"], "two control-plane nodes must scale cilium-operator to 2 replicas")
}

// TestReconcileCertManagerNotHealthyBlocksReady is
// TestReconcileCiliumNotHealthyBlocksControlPlaneReady's counterpart for
// cert-manager/Ready — control plane already ready, cert-manager's Install
// call succeeds, but its pods aren't healthy yet.
func TestReconcileCertManagerNotHealthyBlocksReady(t *testing.T) {
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
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{results: map[string]namespaceHealthResult{
		"kontinuum-system": {healthy: false, reason: "no pods found yet"},
	}}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Equal(t, []string{"kontinuum-system"}, prober.calls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonNotHealthy", cond.Reason)
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
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.bootstrapCalls)
	require.Len(t, installer.calls, 1, "only cilium is attempted, never cert-manager")
	assert.Equal(t, "cilium", installer.calls[0].ReleaseName)

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
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "eu-1a"}}

	result, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Zero(t, result, "control plane becoming ready needs no requeue")
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.applyConfigCalls,
		"only the control-plane member is touched before ControlPlaneReady")

	require.Len(t, installer.calls, 1, "cilium installs as soon as the apiserver is reachable, not after full health")
	assert.Equal(t, "cilium", installer.calls[0].ReleaseName)
	assert.Equal(t, "https://helm.cilium.io", installer.calls[0].RepoURL)
	assert.Equal(t, "kube-system", installer.calls[0].Namespace)

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
	assert.Equal(t, "cert-manager", installer.calls[1].ReleaseName)
	assert.Equal(t, "https://charts.jetstack.io", installer.calls[1].RepoURL)
	assert.Equal(t, "kontinuum-system", installer.calls[1].Namespace)

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
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

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
	reconciler := newReconciler(fakeClient, &fakeBootstrapper{}, &fakeAddonInstaller{}, &fakePodProber{})

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
}
