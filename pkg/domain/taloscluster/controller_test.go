package taloscluster_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
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

// fakeAddonInstaller is taloscluster.AddonInstaller's test double. Guarded
// by a mutex — addons now install in parallel (see reconcileAddons), so
// concurrent goroutines really do call Install at the same time.
type fakeAddonInstaller struct {
	mu    sync.Mutex
	calls []taloscluster.AddonInstallRequest
	err   error
}

func (f *fakeAddonInstaller) Install(_ context.Context, _ []byte, req taloscluster.AddonInstallRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, req)

	return f.err
}

// findCall returns the first call in calls whose ReleaseName is name —
// parallel installation means call order isn't deterministic, so tests
// look calls up by name rather than by index.
func findCall(calls []taloscluster.AddonInstallRequest, name string) taloscluster.AddonInstallRequest {
	for _, call := range calls {
		if call.ReleaseName == name {
			return call
		}
	}

	return taloscluster.AddonInstallRequest{}
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
// to keep progressing. Guarded by a mutex for the same reason
// fakeAddonInstaller is.
type fakePodProber struct {
	mu      sync.Mutex
	results map[string]namespaceHealthResult
	calls   []string
}

func (f *fakePodProber) NamespaceHealthy(_ context.Context, _ []byte, namespace string) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

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

// TestReconcileAddonNotHealthyBlocksReady covers applyAddonOutcomes'
// gating of Ready: even though the Install call itself succeeds
// (installRelease/upgradeRelease only apply manifests, they don't wait
// for rollout — see that doc), Ready must not go true until every
// enabled addon's own pods actually report healthy. ControlPlaneReady is
// deliberately independent of addon health at the Go level now — in a
// real cluster, Talos's own ClusterHealthCheck already can't pass without
// Cilium applied, so nothing real is lost by not also gating
// ControlPlaneReady here (see bootstrapAndCheckHealth's own doc); this
// fake HealthCheck just never modeled that link either way, so it
// succeeds regardless.
func TestReconcileAddonNotHealthyBlocksReady(t *testing.T) {
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
	assert.Equal(t, testRetryInterval, result.RequeueAfter,
		"a not-yet-healthy addon must still produce a requeue, even though ControlPlaneReady itself needs none")
	require.Len(t, installer.calls, 2, "both built-in addons install in parallel, regardless of each other's health")
	assert.ElementsMatch(t, []string{"kube-system", "kontinuum-system"}, prober.calls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType),
		"control plane health is independent of addon health now")

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonNotHealthy", cond.Reason)
}

// TestReconcileCiliumDisabledSkipsInstall covers AddonSpec.Enabled: once
// explicitly set false, cilium must never be installed or health-probed —
// an addon a user has opted out of (e.g. because ArgoCD already owns it)
// must never block the reconciler. cert-manager, still a built-in
// default not mentioned here, installs normally alongside it.
func TestReconcileCiliumDisabledSkipsInstall(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Addons = []v1alpha2.AddonSpec{{Name: "cilium", Enabled: new(bool)}}
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
	assert.Zero(t, result, "control plane and cert-manager both becoming ready needs no requeue")
	require.Len(t, installer.calls, 1, "a disabled addon must never be installed")
	assert.Equal(t, "cert-manager", installer.calls[0].ReleaseName)
	assert.Equal(t, []string{"kontinuum-system"}, prober.calls, "a disabled addon must never be health-probed")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ReadyConditionType))
}

// TestReconcileAddonNamespaceVersionAndValuesOverride covers
// AddonSpec.Namespace/.Chart.Version/.Values all being honored together
// for a built-in addon, with Chart.Repo left unset falling back to the
// built-in's own default: the resulting AddonInstallRequest carries the
// overridden namespace/version but the built-in's own repo, and the
// request's Values has the user-supplied value winning over the
// package's own default for a conflicting key (kubeProxyReplacement),
// while a default-only key (envoy.enabled) survives untouched — see
// mergeValues' own doc for why.
func TestReconcileAddonNamespaceVersionAndValuesOverride(t *testing.T) {
	t.Parallel()

	userValues, err := json.Marshal(map[string]any{"kubeProxyReplacement": false, "extraFlag": true})
	require.NoError(t, err)

	cluster := testCluster()
	cluster.Spec.Addons = []v1alpha2.AddonSpec{
		{
			Name:      "cilium",
			Namespace: v1alpha2.AddonNamespaceSpec{Name: "custom-cilium-ns"},
			Chart:     &v1alpha2.AddonChartSpec{Version: "1.99.0"},
			Values:    &apiextensionsv1.JSON{Raw: userValues},
		},
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

	require.Len(t, installer.calls, 2, "cert-manager, still a built-in default, installs alongside the override")
	req := findCall(installer.calls, "cilium")
	assert.Equal(t, "custom-cilium-ns", req.Namespace)
	assert.Equal(t, "1.99.0", req.Version)
	assert.Equal(t, "https://helm.cilium.io", req.RepoURL, "unset chart.repo falls back to the built-in default")
	assert.Equal(t, false, req.Values["kubeProxyReplacement"], "user value must win over the package default")
	assert.Equal(t, true, req.Values["extraFlag"], "a user-only key must survive the merge")

	envoy, ok := req.Values["envoy"].(map[string]any)
	require.True(t, ok, "a default-only key must survive the merge untouched")
	assert.Equal(t, true, envoy["enabled"])
}

// TestReconcileCiliumOperatorReplicasScaleWithControlPlaneCount covers
// values/cilium.yaml's $cel-driven operator.replicas rule (see
// celvalues.go and celvalues_test.go for the mechanism itself): a
// single-control-plane cluster keeps the file's own literal default (1,
// since a second replica could never even schedule with hostNetwork:
// true), while a multi-control-plane cluster gets bumped to 2.
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

	require.Len(t, installer.calls, 2)

	req := findCall(installer.calls, "cilium")
	operator, ok := req.Values["operator"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(2), operator["replicas"], "two control-plane nodes must scale cilium-operator to 2 replicas")
}

// TestReconcileCustomAddonInstalls covers a fully user-defined addon —
// not one of the two built-ins, Chart fully specified — installed and
// probed exactly like a built-in, no special treatment.
func TestReconcileCustomAddonInstalls(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Addons = []v1alpha2.AddonSpec{
		{
			Name:      "my-addon",
			Namespace: v1alpha2.AddonNamespaceSpec{Name: "my-addon-ns"},
			Chart:     &v1alpha2.AddonChartSpec{Repo: "https://example.com/charts", Name: "my-chart", Version: "1.0.0"},
		},
	}
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 3, "the two built-ins plus the custom addon")
	req := findCall(installer.calls, "my-addon")
	assert.Equal(t, "https://example.com/charts", req.RepoURL)
	assert.Equal(t, "my-chart", req.ChartName)
	assert.Equal(t, "1.0.0", req.Version)
	assert.Equal(t, "my-addon-ns", req.Namespace)
	assert.Contains(t, prober.calls, "my-addon-ns")
}

// TestReconcileCustomAddonWithoutChartFails covers a non-built-in addon
// with no Chart set: there's no built-in default to fall back to, so
// resolveAddons must fail with a descriptive error, surfaced as Ready's
// own AddonInstallFailed reason — and, since resolution happens before
// any install starts, not even the two built-ins get installed on this
// reconcile.
func TestReconcileCustomAddonWithoutChartFails(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Addons = []v1alpha2.AddonSpec{{Name: "my-addon"}}
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", "10.0.0.1")

	fakeClient := newFakeClient(t, cluster, cpInstance)

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, bootstrapper, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a"},
	})
	require.NoError(t, err)
	assert.Empty(t, installer.calls, "a resolution failure must never attempt any install, not even the built-ins")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonInstallFailed", cond.Reason)
	assert.Contains(t, cond.Message, "my-addon")
}

// TestReconcileCertManagerNotHealthyBlocksReady is
// TestReconcileAddonNotHealthyBlocksReady's counterpart for the
// post-ControlPlaneReady convergence path: cilium's pods are healthy,
// cert-manager's aren't yet — Ready must stay false until both are.
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
	assert.ElementsMatch(t, []string{"kube-system", "kontinuum-system"}, prober.calls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonNotHealthy", cond.Reason)
}

// TestReconcileAddonInstallFailureDuringBootstrap covers
// bootstrapAndCheckHealth combining reconcileAddons' own requeue signal
// with the control-plane health outcome: even though HealthCheck itself
// succeeds (ControlPlaneReady goes true, needing no requeue of its own),
// a failed addon install must still produce a non-zero RequeueAfter,
// rather than that signal getting silently dropped. Install() itself
// failing is distinct from a probe reporting not-yet-healthy — it
// surfaces as AddonInstallFailed, not AddonNotHealthy.
func TestReconcileAddonInstallFailureDuringBootstrap(t *testing.T) {
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
	assert.Equal(t, testRetryInterval, result.RequeueAfter,
		"a failed addon install must still produce a requeue, even though ControlPlaneReady itself needs none")
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.bootstrapCalls)
	require.Len(t, installer.calls, 2, "both built-in addons are attempted in parallel")
	assert.Empty(t, prober.calls, "a failed install is never health-probed")

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &got))

	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType),
		"an addon install failure must never block ControlPlaneReady")
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.BootstrappedConditionType))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AddonInstallFailed", cond.Reason)
}

// TestReconcileFullSequence is the issue's own milestone: workers are
// never touched until the control plane is ready — checked at the START
// of a reconcile, so becoming ready mid-pass doesn't count and a second
// reconcile is still needed for that. Addons install in parallel with no
// ordering between them and can both converge within the very first
// reconcile once their pods report healthy — they don't need to wait for
// the full control-plane health check either (see reconcileAddons' own
// doc for why).
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
	assert.Zero(t, result, "control plane and both addons becoming ready in one pass needs no requeue")
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.applyConfigCalls,
		"only the control-plane member is touched before ControlPlaneReady")

	require.Len(t, installer.calls, 2, "both built-in addons install in parallel, in the same reconcile")
	cilium := findCall(installer.calls, "cilium")
	assert.Equal(t, "https://helm.cilium.io", cilium.RepoURL)
	assert.Equal(t, "kube-system", cilium.Namespace)

	certManager := findCall(installer.calls, "cert-manager")
	assert.Equal(t, "https://charts.jetstack.io", certManager.RepoURL)
	assert.Equal(t, "kontinuum-system", certManager.Namespace)

	var afterFirst v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a"}, &afterFirst))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.BootstrappedConditionType))
	assert.True(t, meta.IsStatusConditionTrue(afterFirst.Status.Conditions, taloscluster.ReadyConditionType))
	require.NotEmpty(t, afterFirst.Status.SecretRef.Name)

	var secret corev1.Secret

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: afterFirst.Status.SecretRef.Name, Namespace: afterFirst.Status.SecretRef.Namespace},
		&secret))
	assert.Equal(t, []byte("fake-kubeconfig"), secret.Data["kubeconfig"])

	result, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Zero(t, result, "the worker joining needs no requeue")
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, bootstrapper.applyConfigCalls,
		"the worker is only touched on the reconcile after ControlPlaneReady")
	assert.Len(t, installer.calls, 2, "addons already ready, so the second reconcile never reinstalls them")
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
	require.Len(t, installer.calls, 2)

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
