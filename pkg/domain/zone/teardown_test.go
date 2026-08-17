package zone_test

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// testTeardownTimeout is generous enough that no test below except
// TestReconcileTeardownGivesUpAfterTimeout ever hits it.
const testTeardownTimeout = time.Hour

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

// TestIgnoreNotFoundOrNoMatch covers every input zone.IgnoreNotFoundOrNoMatch
// branches on — see that function's own doc for why a missing Kind
// (observed for real: a Zone deleted before its downstream cluster ever
// got Cilium/cert-manager/the Gateway API CRDs actually installed, despite
// their own Addon objects claiming Healthy) is tolerated the same way a
// plain NotFound already was, rather than retried until Zone's own
// TeardownTimeout gives up regardless.
func TestIgnoreNotFoundOrNoMatch(t *testing.T) {
	t.Parallel()

	notFoundResource := schema.GroupResource{Group: "gateway.networking.k8s.io", Resource: "httproutes"}
	notFound := apierrors.NewNotFound(notFoundResource, testDownstreamResourceName)
	noResourceMatch := &meta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{
		Group: "gateway.networking.k8s.io", Version: "v1",
	}}
	noKindMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "cert-manager.io", Kind: "ClusterIssuer"}}
	wrapped := fmt.Errorf("unable to retrieve the complete list of server APIs: %w", noResourceMatch)

	assert.NoError(t, zone.IgnoreNotFoundOrNoMatch(nil))
	assert.NoError(t, zone.IgnoreNotFoundOrNoMatch(notFound))
	assert.NoError(t, zone.IgnoreNotFoundOrNoMatch(noResourceMatch))
	assert.NoError(t, zone.IgnoreNotFoundOrNoMatch(noKindMatch))
	assert.NoError(t, zone.IgnoreNotFoundOrNoMatch(wrapped),
		"a NoMatchError wrapped by controller-runtime's own discovery error must still be recognized through it")
	assert.ErrorIs(t, zone.IgnoreNotFoundOrNoMatch(assert.AnError), assert.AnError,
		"a genuine, unrelated failure must still propagate and retry")
}

// TestReconcileTeardownDeletesDownstreamAndTalosCluster covers the full
// happy path end to end: an install pass creates every downstream object,
// then a delete tears every one of them back down (including the
// cluster-scoped ClusterIssuer, which a namespace-cascade alone would never
// reach), deletes the TalosCluster, and finally removes the finalizer —
// letting the Zone actually delete. The actual Talos Reset is no longer
// this package's concern — see reconcileTeardown's own doc — so it isn't
// exercised here; taloscluster.InstanceResetReconciler's own tests cover
// it.
func TestReconcileTeardownDeletesDownstreamAndTalosCluster(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	cluster := readyTalosCluster()
	secret := kubeconfigSecret()

	hubClient := newHubFakeClient(t, testZoneObject(), cluster, secret, kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{client: downstream},
		ACMEEmail:               "ops@example.com",
		ACMEServer:              "https://acme-v02.api.letsencrypt.org/directory",
		ImageRepo:               testImageRepo,
		GRPCEndpoint:            testGRPCEndpoint,
		RetryInterval:           testRetryInterval,
		TeardownTimeout:         testTeardownTimeout,
		Logger:                  slog.Default(),
	}

	// Install pass — creates every downstream object.
	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamNamespace}, &corev1.Namespace{}),
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

	err = downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamNamespace}, &corev1.Namespace{})
	assert.True(t, apierrors.IsNotFound(err), "downstream namespace must be deleted")

	err = downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &certmanagerv1.ClusterIssuer{})
	assert.True(t, apierrors.IsNotFound(err),
		"the cluster-scoped ClusterIssuer must be deleted explicitly, not just cascaded via the namespace")

	// The TalosCluster is deleted explicitly here — see reconcileTeardown's
	// own doc for why (taloscluster's reconciler would otherwise keep
	// actively managing members with no idea the Zone owning them is being
	// torn down).
	err = hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
		&v1alpha2.TalosCluster{})
	assert.True(t, apierrors.IsNotFound(err), "talos cluster must be deleted as part of teardown")
}

// TestReconcileTeardownSkipsWhenNeverBootstrapped covers a Zone deleted
// before its TalosCluster ever got far enough to persist a kubeconfig:
// downstream teardown has nothing to act on, so teardown succeeds
// immediately.
func TestReconcileTeardownSkipsWhenNeverBootstrapped(t *testing.T) {
	t.Parallel()

	zoneObj := deletingZoneObject()
	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
	}

	hubClient := newHubFakeClient(t, zoneObj, cluster)
	require.NoError(t, hubClient.Delete(t.Context(), zoneObj))

	reconciler := &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: fakeDownstreamClientBuilder{},
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
