package addon_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
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
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	testRetryInterval  = 15 * time.Second
	testResyncInterval = 5 * time.Minute
)

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

func (f *fakePodProber) NamespaceHealthy(_ context.Context, _ []byte, namespace, _, _ string) (bool, string, error) {
	f.calls = append(f.calls, namespace)

	result, ok := f.results[namespace]
	if !ok {
		return true, "", nil
	}

	return result.healthy, result.reason, result.err
}

// fakeCRDChecker is addon.CRDChecker's test double — always reports
// ready unless a test explicitly configures otherwise, matching the
// overwhelmingly common case of a chart with no CRDs to wait on at all.
type fakeCRDChecker struct {
	ready  bool
	reason string
	err    error
}

func (f *fakeCRDChecker) ChartCRDsReady(context.Context, []byte, addon.InstallRequest) (bool, string, error) {
	return f.ready, f.reason, f.err
}

// statusUpdateCountingClient wraps a client.Client, counting every
// Status().Update call made through it — see
// TestReconcileSkipsRedundantStatusUpdate's own doc for what this is used
// to verify.
type statusUpdateCountingClient struct {
	client.Client

	statusUpdates *int
}

//nolint:ireturn // client.Client's own Status() signature dictates this; wrapping it is the point.
func (c statusUpdateCountingClient) Status() client.SubResourceWriter {
	return countingStatusWriter{c.Client.Status(), c.statusUpdates}
}

type countingStatusWriter struct {
	client.SubResourceWriter

	count *int
}

func (w countingStatusWriter) Update(
	ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption,
) error {
	*w.count++

	err := w.SubResourceWriter.Update(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

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
		CRDChecker:    &fakeCRDChecker{ready: true},
		RetryInterval: testRetryInterval,
		Locker:        zonelease.NewLocker(fakeClient, "test-hub", "", 0),
		Logger:        slog.Default(),
	}
}

// newReconcilerWithResync is newReconciler plus a non-zero ResyncInterval —
// a separate helper rather than a newReconciler parameter, since every
// other test relies on ResyncInterval defaulting to zero (see
// TestReconcileBuiltinAddonResolvesEmbeddedDefaults's own assert.Zero on
// its Ready-path result) and only the resync-specific tests below need it
// set.
func newReconcilerWithResync(
	fakeClient client.Client, installer *fakeAddonInstaller, prober *fakePodProber,
) *addon.Reconciler {
	reconciler := newReconciler(fakeClient, installer, prober)
	reconciler.ResyncInterval = testResyncInterval

	return reconciler
}

// testClusterName is every fixture's own TalosCluster name — kept as one
// constant rather than a parameter every test helper threads through,
// since no test in this file needs more than one cluster.
const testClusterName = "eu-1a"

const (
	controlPlanePoolName    = "cp-pool"
	ciliumReleaseName       = "cilium"
	customReleaseName       = "my-addon"
	ciliumAddonResourceName = testClusterName + "-cilium"
	customAddonResourceName = testClusterName + "-" + customReleaseName
)

// readyCluster builds a TalosCluster fixture that's already bootstrapped
// far enough to have a stored kubeconfig — what Reconciler needs to
// install anything at all.
func readyCluster() (*v1alpha2.TalosCluster, *corev1.Secret) {
	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: controlPlanePoolName},
			},
		},
		Status: v1alpha2.TalosClusterStatus{
			SecretRef: v1alpha2.SecretReference{
				Name: testClusterName + "-secrets", Namespace: v1alpha2.KontinuumSystemNamespace,
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-secrets", Namespace: v1alpha2.KontinuumSystemNamespace},
		Data:       map[string][]byte{"kubeconfig": []byte("fake-kubeconfig")},
	}

	return cluster, secret
}

func claimedDiscoveredInstance(name string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: v1alpha2.KontinuumSystemNamespace,
			Labels: map[string]string{v1alpha2.LabelClaimedBy: controlPlanePoolName},
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1"}},
		Status: v1alpha2.InstanceStatus{
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
			},
		},
	}
}

func builtinAddon() *v1alpha2.Addon {
	return &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-cilium", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     ciliumReleaseName,
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
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	require.Len(t, installer.calls, 1)
	req := installer.calls[0]
	assert.Equal(t, ciliumReleaseName, req.ReleaseName)
	assert.Equal(t, "https://helm.cilium.io", req.RepoURL)
	assert.Equal(t, "kube-system", req.Namespace)
	assert.Equal(t, []string{"kube-system"}, prober.calls)

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, "Ready"))
}

// TestReconcileReleaseNameDefaultsToMetadataName covers an Addon that
// never sets spec.releaseName at all — it must resolve as if
// releaseName were its own metadata.name, e.g. so an Addon literally
// named "cilium" is recognized as the built-in without repeating the
// name in spec too.
func TestReconcileReleaseNameDefaultsToMetadataName(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: ciliumReleaseName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.AddonSpec{TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName}},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumReleaseName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 1)
	assert.Equal(t, ciliumReleaseName, installer.calls[0].ReleaseName)
	assert.Equal(t, "https://helm.cilium.io", installer.calls[0].RepoURL)
}

func TestReconcileCustomAddonWithChartInstalls(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	custom := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     customReleaseName,
			Namespace:       v1alpha2.AddonNamespaceSpec{Name: "my-addon-ns"},
			Chart:           &v1alpha2.AddonChartSpec{Repo: "https://example.com/charts", Name: "my-chart", Version: "1.0.0"},
		},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, custom)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
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
		ObjectMeta: metav1.ObjectMeta{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     customReleaseName,
		},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, custom)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, installer.calls, "resolution must fail before any install is attempted")

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))

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
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
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
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, prober.calls, "a failed install must never be health-probed")

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "InstallFailed", cond.Reason)
}

// TestReconcileSkipsRedundantStatusUpdate guards against a reconcile
// storm: this controller's own Addon watch (see SetupWithManager) carries
// no predicate, so any Status().Update — even one that changes nothing —
// re-triggers Reconcile. Reconciling twice in a row against an install
// that keeps failing identically must only write status once.
func TestReconcileSkipsRedundantStatusUpdate(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()

	statusUpdates := 0
	fakeClient := statusUpdateCountingClient{
		newFakeClient(t, cluster, secret, cpInstance, cilium), &statusUpdates,
	}

	installer := &fakeAddonInstaller{err: assert.AnError}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "first reconcile should persist the new Ready=False condition")

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "second reconcile computes the same condition and should not write again")
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
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))

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
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Empty(t, installer.calls)
}

func TestReconcileKubeconfigNotStoredRequeuesWithoutError(t *testing.T) {
	t.Parallel()

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{PoolRef: v1alpha2.InstancePoolReference{Name: controlPlanePoolName}},
		},
	}
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
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
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)

	require.Len(t, installer.calls, 1)

	operator, ok := installer.calls[0].Values["operator"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(2), operator["replicas"], "two control-plane nodes must scale cilium-operator to 2 replicas")
}

// gatewayAPICRDsAddon builds the gateway-api-crds built-in fixture —
// same minimal-seed shape as builtinAddon, just a different release.
func gatewayAPICRDsAddon() *v1alpha2.Addon {
	return &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{
			Name: testClusterName + "-gateway-api-crds", Namespace: v1alpha2.KontinuumSystemNamespace,
		},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     "gateway-api-crds",
		},
	}
}

// TestReconcileWaitsForEarlierPriorityWave covers cilium (priority 100,
// its own built-in default) never installing while gateway-api-crds
// (priority 50) hasn't reached Ready yet — the whole reason priority
// exists: cilium's own Gateway API support assumes those CRDs already
// exist.
func TestReconcileWaitsForEarlierPriorityWave(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()
	gatewayCRDs := gatewayAPICRDsAddon() // no Ready condition yet — still installing

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium, gatewayCRDs)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, installer.calls, "cilium must never install before its lower-priority prerequisite is Ready")
}

// TestReconcileProceedsOnceEarlierWaveReady covers the other half of
// TestReconcileWaitsForEarlierPriorityWave: once gateway-api-crds
// reports Ready, cilium's own reconcile proceeds normally.
func TestReconcileProceedsOnceEarlierWaveReady(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()
	gatewayCRDs := gatewayAPICRDsAddon()
	gatewayCRDs.Status.Conditions = []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Installed"},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium, gatewayCRDs)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Len(t, installer.calls, 1)
}

// TestReconcileSamePriorityAddonsInstallInParallel covers cilium and
// cert-manager — both priority 100, neither Ready yet — never blocking
// each other: only a strictly lower priority number gates.
func TestReconcileSamePriorityAddonsInstallInParallel(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()
	certManager := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-cert-manager", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     "cert-manager",
		},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium, certManager)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Len(t, installer.calls, 1, "same-priority siblings must never gate each other")
}

// TestReconcileNoPodsIsVacuouslyHealthy covers an addon whose namespace
// has no pods at all (e.g. a CRD-only chart) reaching Ready right after
// install — fakePodProber's own doc already says a namespace with no
// configured result defaults to healthy, matching PodProber's real
// production semantics (see podhealth.go's own NamespaceHealthy doc):
// there's nothing unhealthy to find in an empty namespace.
func TestReconcileNoPodsIsVacuouslyHealthy(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	custom := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: testClusterName},
			ReleaseName:     customReleaseName,
			Namespace:       v1alpha2.AddonNamespaceSpec{Name: "my-addon-ns"},
			Chart:           &v1alpha2.AddonChartSpec{Repo: "https://example.com/charts", Name: "my-chart", Version: "1.0.0"},
		},
	}

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, custom)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconciler(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	var got v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: customAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, "Ready"))
}

// TestReconcileReadyAddonRequeuesForResync covers an already-Ready addon
// still getting periodically re-visited rather than going quiet forever —
// see setReady's own doc for why: nothing else would ever pick up a
// built-in chart version bump (values/*.yaml) for an addon that was
// already installed and Ready before a controller carrying that bump was
// deployed.
func TestReconcileReadyAddonRequeuesForResync(t *testing.T) {
	t.Parallel()

	cluster, secret := readyCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1")
	cilium := builtinAddon()

	fakeClient := newFakeClient(t, cluster, secret, cpInstance, cilium)

	installer := &fakeAddonInstaller{}
	prober := &fakePodProber{}
	reconciler := newReconcilerWithResync(fakeClient, installer, prober)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Equal(t, testResyncInterval, result.RequeueAfter)
	assert.Len(t, installer.calls, 1)

	// A second reconcile (simulating that requeue firing) re-resolves and
	// re-installs — idempotent against the fake, but this is what makes a
	// real `helm upgrade --install` pick up a changed chart version
	// against a real cluster.
	result, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ciliumAddonResourceName, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	require.NoError(t, err)
	assert.Equal(t, testResyncInterval, result.RequeueAfter)
	assert.Len(t, installer.calls, 2)
}
