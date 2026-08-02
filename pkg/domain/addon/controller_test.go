package addon_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

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
)

const testRetryInterval = 15 * time.Second

// fakeAddonInstaller is addon.Installer's test double.
type fakeAddonInstaller struct {
	calls []addon.InstallRequest
	err   error
}

func (f *fakeAddonInstaller) Install(_ context.Context, _ []byte, req addon.InstallRequest) error {
	f.calls = append(f.calls, req)

	return f.err
}

// namespaceHealthResult is one PodProber.NamespaceHealthy fake result.
type namespaceHealthResult struct {
	healthy bool
	reason  string
	err     error
}

// fakePodProber is addon.PodProber's test double. A namespace with no
// configured result defaults to healthy.
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
		WithStatusSubresource(&v1alpha2.Addon{}).
		WithObjects(objects...).
		Build()
}

func newReconciler(fakeClient client.Client, installer *fakeAddonInstaller, prober *fakePodProber) *addon.Reconciler {
	return &addon.Reconciler{
		Client:        fakeClient,
		Installer:     installer,
		PodProber:     prober,
		RetryInterval: testRetryInterval,
		Logger:        slog.Default(),
	}
}

// testClusterName is every fixture's own TalosCluster name — kept as one
// constant rather than a parameter every test helper threads through,
// since no test in this file needs more than one cluster.
const testClusterName = "eu-1a"

// readyCluster builds a TalosCluster fixture that's already bootstrapped
// far enough to have a stored kubeconfig — what Reconciler needs to
// install anything at all.
func readyCluster() (*v1alpha2.TalosCluster, *corev1.Secret) {
	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: "cp-pool"},
			},
		},
		Status: v1alpha2.TalosClusterStatus{
			SecretRef: v1alpha2.SecretReference{
				Name: testClusterName + "-secrets", Namespace: v1alpha2.DefaultSecretNamespace,
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-secrets", Namespace: v1alpha2.DefaultSecretNamespace},
		Data:       map[string][]byte{"kubeconfig": []byte("fake-kubeconfig")},
	}

	return cluster, secret
}

func claimedDiscoveredInstance(name string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha2.LabelClaimedBy: "cp-pool"}},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1"}},
		Status: v1alpha2.InstanceStatus{
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
			},
		},
	}
}

func builtinAddon() *v1alpha2.Addon {
	return &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-cilium"},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     "cilium",
		},
	}
}

func TestReconcileBuiltinAddonResolvesEmbeddedDefaults(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	require.Len(t, installer.calls, 1)
	req := installer.calls[0]
	assert.Equal(t, "cilium", req.ReleaseName)
	assert.Equal(t, "https://helm.cilium.io", req.RepoURL)
	assert.Equal(t, "kube-system", req.Namespace)
	assert.Equal(t, []string{"kube-system"}, prober.calls)

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a-cilium"}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, "Ready"))
}

func TestReconcileCustomAddonWithChartInstalls(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	custom := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a-my-addon"},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: "eu-1a"},
			ReleaseName:     "my-addon",
			Namespace:       v1alpha2.AddonNamespaceSpec{Name: "my-addon-ns"},
			Chart:           &v1alpha2.AddonChartSpec{Repo: "https://example.com/charts", Name: "my-chart", Version: "1.0.0"},
		},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, custom)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-my-addon"},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 1)
	req := installer.calls[0]
	assert.Equal(t, "https://example.com/charts", req.RepoURL)
	assert.Equal(t, "my-chart", req.ChartName)
	assert.Equal(t, "1.0.0", req.Version)
	assert.Equal(t, "my-addon-ns", req.Namespace)
}

func TestReconcileCustomAddonWithoutChartFails(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	custom := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a-my-addon"},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: "eu-1a"},
			ReleaseName:     "my-addon",
		},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, custom)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-my-addon"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, installer.calls, "resolution must fail before any install is attempted")

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a-my-addon"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "InstallFailed", cond.Reason)
}

func TestReconcileDisabledAddonSkipsInstall(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()
	cilium.Spec.Enabled = new(bool)

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Empty(t, installer.calls)
	assert.Empty(t, prober.calls)
}

func TestReconcileInstallFailureSetsReadyFalse(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium)

	installer := &fakeAddonInstaller{err: assert.AnError}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, prober.calls, "a failed install must never be health-probed")

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a-cilium"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "InstallFailed", cond.Reason)
}

func TestReconcileUnhealthyProbeSetsReadyFalse(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{results: map[string]namespaceHealthResult{
		"kube-system": {healthy: false, reason: "no pods found yet"},
	}}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "eu-1a-cilium"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "NotHealthy", cond.Reason)
	assert.Contains(t, cond.Message, "no pods found yet")
}

func TestReconcileMissingTalosClusterIsNoOp(t *testing.T) {
	t.Parallel()

	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Empty(t, installer.calls)
}

func TestReconcileKubeconfigNotStoredRequeuesWithoutError(t *testing.T) {
	t.Parallel()

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a"},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{PoolRef: v1alpha2.InstancePoolReference{Name: "cp-pool"}},
		},
	}
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, installer.calls)
}

// TestReconcileOperatorReplicasScaleWithControlPlaneCount covers
// values/cilium.yaml's $cel-driven operator.replicas rule, exercised
// through a real Reconciler.Reconcile call: a single-control-plane
// cluster keeps the file's own literal default (1, since a second
// replica could never even schedule with hostNetwork: true), while a
// multi-control-plane cluster gets bumped to 2.
func TestReconcileOperatorReplicasScaleWithControlPlaneCount(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance1 := claimedDiscoveredInstance("cp-node-1")
	cpInstance2 := claimedDiscoveredInstance("cp-node-2")
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, secret, cpInstance1, cpInstance2, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "eu-1a-cilium"},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 1)

	operator, ok := installer.calls[0].Values["operator"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(2), operator["replicas"], "two control-plane nodes must scale cilium-operator to 2 replicas")
}
