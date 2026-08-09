package zone_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/config"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// testTeardownTimeout is generous enough that no test below except
// TestReconcileTeardownGivesUpAfterTimeout ever hits it.
const testTeardownTimeout = time.Hour

// testControlPlaneAddress is the sole control-plane member's dial address
// in TestReconcileTeardownDeletesDownstreamAndResetsSeedNode.
const testControlPlaneAddress = "10.0.0.5"

// fakeBootstrapper is taloscluster.ClusterBootstrapper's test double for
// this package's own teardown tests — only Reset is ever exercised through
// zone.Reconciler, but every interface method must still be implemented.
type fakeBootstrapper struct {
	resetCalls []string
	resetErr   error
}

func (*fakeBootstrapper) ApplyConfiguration(context.Context, string, []byte) error { return nil }

func (*fakeBootstrapper) Bootstrap(context.Context, string, *clientconfig.Config) error { return nil }

func (*fakeBootstrapper) HealthCheck(
	context.Context, string, *clientconfig.Config, []string, time.Duration,
) error {
	return nil
}

func (*fakeBootstrapper) Kubeconfig(context.Context, string, *clientconfig.Config) ([]byte, error) {
	return []byte("fake-kubeconfig"), nil
}

func (*fakeBootstrapper) Version(context.Context, string, string, *clientconfig.Config) (string, error) {
	return "", nil
}

func (f *fakeBootstrapper) Reset(_ context.Context, _, node string, _ *clientconfig.Config) error {
	f.resetCalls = append(f.resetCalls, node)

	return f.resetErr
}

// deletingZoneObject returns testZoneObject with ZoneFinalizer already set —
// the fake client rejects a Create carrying a DeletionTimestamp directly, so
// every test below Creates this, then Deletes it, to reach the same state a
// real apiserver leaves a Zone in once `kubectl delete zone` is issued
// against one that already carries the finalizer (as every Zone does once
// it's been reconciled at least once — see Reconcile's own AddFinalizer
// call).
func deletingZoneObject() *v1alpha2.Zone {
	zoneObj := testZoneObject()
	zoneObj.Finalizers = []string{zone.ZoneFinalizer}

	return zoneObj
}

// talosSecretsBundleData marshals a real Talos secrets bundle the same way
// pkg/domain/taloscluster's own ensureSecretsBundle does, for the version
// generateConfigs/resolveVersions resolves to when TalosClusterSpec.Talos is
// left unset ("v1.13.0", taloscluster's own pinned default) — needed so
// taloscluster.ResetControlPlane's loadSecretsBundle can actually
// unmarshal a fixture's stored bundle, not because this test package
// depends on that specific version staying pinned forever.
func talosSecretsBundleData(t *testing.T) []byte {
	t.Helper()

	contract, err := talosconfig.ParseContractFromVersion("v1.13.0")
	require.NoError(t, err)

	bundle, err := talossecrets.NewBundle(talossecrets.NewClock(), contract)
	require.NoError(t, err)

	data, err := json.Marshal(bundle)
	require.NoError(t, err)

	return data
}

// bootstrappedClusterSecret extends kubeconfigSecret with a real secrets
// bundle under the "secrets-bundle" key — matching the same Secret
// pkg/domain/taloscluster/secrets.go stores both under, at
// TalosCluster.status.secretRef — so taloscluster.ResetControlPlane can
// load a talosCfg from it exactly as it would in production.
func bootstrappedClusterSecret(t *testing.T) *corev1.Secret {
	t.Helper()

	secret := kubeconfigSecret()
	secret.Data["secrets-bundle"] = talosSecretsBundleData(t)

	return secret
}

// controlPlaneInstance is a discovered Instance claimed by poolName — the
// only kind of member taloscluster.ResetControlPlane's own resolveMembers
// ever resets (see that function's doc).
func controlPlaneInstance(poolName, addr string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cp-node-1", Namespace: v1alpha2.DefaultSecretNamespace,
			Labels: map[string]string{v1alpha2.LabelClaimedBy: poolName},
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{addr}},
		Status: v1alpha2.InstanceStatus{
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
			},
		},
	}
}

// TestReconcileTeardownDeletesDownstreamAndResetsSeedNode covers the full
// happy path end to end: an install pass creates every downstream object,
// then a delete tears every one of them back down (including the
// cluster-scoped ClusterIssuer, which a namespace-cascade alone would never
// reach), resets the control-plane seed node, and finally removes the
// finalizer — letting the Zone actually delete.
func TestReconcileTeardownDeletesDownstreamAndResetsSeedNode(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub", testStorage)
	cluster := readyTalosCluster()
	cluster.Spec.ControlPlane.PoolRef = v1alpha2.InstancePoolReference{Name: "cp-pool"}
	cpInstance := controlPlaneInstance("cp-pool", testControlPlaneAddress)
	secret := bootstrappedClusterSecret(t)

	hubClient := newHubFakeClient(t, testZoneObject(), cluster, secret, cpInstance, kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	bootstrapper := &fakeBootstrapper{}

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{client: downstream},
		ACMEEmail:               "ops@example.com",
		ACMEServer:              "https://acme-v02.api.letsencrypt.org/directory",
		Image:                   testImage,
		RetryInterval:           testRetryInterval,
		Bootstrapper:            bootstrapper,
		TeardownTimeout:         testTeardownTimeout,
		Logger:                  slog.Default(),
	}

	// Install pass — creates every downstream object.
	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: "kontinuum-system"}, &corev1.Namespace{}),
		"install pass must have created the downstream namespace")

	// Delete — sets DeletionTimestamp; the finalizer keeps the Zone around.
	var toDelete v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &toDelete))
	require.NoError(t, hubClient.Delete(t.Context(), &toDelete))

	// Teardown pass.
	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var gone v1alpha2.Zone

	err = hubClient.Get(t.Context(), testZoneKey(), &gone)
	assert.True(t, apierrors.IsNotFound(err), "zone must be fully deleted once the finalizer is removed")

	err = downstream.Get(t.Context(), client.ObjectKey{Name: "kontinuum-system"}, &corev1.Namespace{})
	assert.True(t, apierrors.IsNotFound(err), "downstream namespace must be deleted")

	err = downstream.Get(t.Context(), client.ObjectKey{Name: "kontinuum"}, &certmanagerv1.ClusterIssuer{})
	assert.True(t, apierrors.IsNotFound(err),
		"the cluster-scoped ClusterIssuer must be deleted explicitly, not just cascaded via the namespace")

	assert.Equal(t, []string{testControlPlaneAddress}, bootstrapper.resetCalls)
}

// TestReconcileTeardownSkipsWhenNeverBootstrapped covers a Zone deleted
// before its TalosCluster ever got far enough to persist a kubeconfig or
// secrets bundle: both the downstream teardown and the Talos Reset step
// have nothing to act on, so teardown succeeds immediately.
func TestReconcileTeardownSkipsWhenNeverBootstrapped(t *testing.T) {
	t.Parallel()

	zoneObj := deletingZoneObject()
	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.DefaultSecretNamespace},
	}

	hubClient := newHubFakeClient(t, zoneObj, cluster)
	require.NoError(t, hubClient.Delete(t.Context(), zoneObj))

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{},
		Bootstrapper:            &fakeBootstrapper{},
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Logger:                  slog.Default(),
	}

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var gone v1alpha2.Zone

	err = hubClient.Get(t.Context(), testZoneKey(), &gone)
	assert.True(t, apierrors.IsNotFound(err))
}

// TestReconcileTeardownRemovesFinalizerWhenTalosClusterAlreadyGone covers a
// Zone whose TalosCluster was already deleted separately (or never
// created) — nothing left to tear down or reset, so the finalizer comes
// off on the very first teardown reconcile.
func TestReconcileTeardownRemovesFinalizerWhenTalosClusterAlreadyGone(t *testing.T) {
	t.Parallel()

	zoneObj := deletingZoneObject()
	hubClient := newHubFakeClient(t, zoneObj)
	require.NoError(t, hubClient.Delete(t.Context(), zoneObj))

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{},
		Bootstrapper:            &fakeBootstrapper{},
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Logger:                  slog.Default(),
	}

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var gone v1alpha2.Zone

	err = hubClient.Get(t.Context(), testZoneKey(), &gone)
	assert.True(t, apierrors.IsNotFound(err))
}

// TestReconcileTeardownRetriesWhenDownstreamUnreachable covers issue #49's
// own "downstream cluster already unreachable" scenario: the finalizer
// stays, TeardownConditionType surfaces why, and Reconcile requeues —
// rather than either hanging with no signal or deleting the Zone without
// ever having torn anything down.
func TestReconcileTeardownRetriesWhenDownstreamUnreachable(t *testing.T) {
	t.Parallel()

	zoneObj := deletingZoneObject()
	cluster := readyTalosCluster()

	hubClient := newHubFakeClient(t, zoneObj, cluster, kubeconfigSecret())
	require.NoError(t, hubClient.Delete(t.Context(), zoneObj))

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{err: assert.AnError},
		Bootstrapper:            &fakeBootstrapper{},
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Logger:                  slog.Default(),
	}

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))
	assert.Contains(t, got.Finalizers, zone.ZoneFinalizer, "finalizer must stay while teardown keeps retrying")

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.TeardownConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "DownstreamTeardownFailed", cond.Reason)
	assert.Contains(t, cond.Message, "will keep retrying until")
}

// TestReconcileTeardownGivesUpAfterTimeout covers issue #49's own bounded
// "not a finalizer that blocks deletion forever" requirement: once
// TeardownTimeout has elapsed since DeletionTimestamp, teardown gives up
// and removes the finalizer regardless of whether downstream teardown ever
// succeeded.
func TestReconcileTeardownGivesUpAfterTimeout(t *testing.T) {
	t.Parallel()

	zoneObj := deletingZoneObject()
	hubClient := newHubFakeClient(t, zoneObj)
	require.NoError(t, hubClient.Delete(t.Context(), zoneObj))

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{err: assert.AnError},
		Bootstrapper:            &fakeBootstrapper{},
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         time.Nanosecond,
		Logger:                  slog.Default(),
	}

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var gone v1alpha2.Zone

	err = hubClient.Get(t.Context(), testZoneKey(), &gone)
	assert.True(t, apierrors.IsNotFound(err), "teardown must give up and remove the finalizer once past TeardownTimeout")
}
