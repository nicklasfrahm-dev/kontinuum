package zonelease_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	testLeaseDuration = 30 * time.Second
	// testZoneLeaseName is the Lease object name zonelease.Key("eu", "hel01")
	// ("eu-hel01") maps onto.
	testZoneLeaseName = "zonelock-eu-hel01"
	// testZoneKey and otherZoneKey are two zones' own zonelease.Key values.
	testZoneKey  = "eu-hel01"
	otherZoneKey = "eu-hel02"
)

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestKey(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "eu-hel01", zonelease.Key("eu", "hel01"))
	assert.Equal(t, zonelease.GlobalKey, zonelease.Key("", ""))
	assert.Equal(t, zonelease.GlobalKey, zonelease.Key("eu", ""))
	assert.Equal(t, zonelease.GlobalKey, zonelease.Key("", "hel01"))
}

func TestTryAcquire_FreeZoneKeyIsAcquired(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "hub-1", "", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel01")
	require.NoError(t, err)
	assert.True(t, acquired)

	var lease coordinationv1.Lease

	require.NoError(t, fakeClient.Get(context.Background(),
		client.ObjectKey{Name: testZoneLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease))
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, "hub-1", *lease.Spec.HolderIdentity)
}

func TestTryAcquire_FreeGlobalKeyIsAcquired(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "hub-1", "", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), zonelease.GlobalKey)
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestTryAcquire_SelfHeldLeaseIsRenewed(t *testing.T) {
	t.Parallel()

	past := metav1.NewMicroTime(time.Now().Add(-testLeaseDuration / 2))
	duration := int32(testLeaseDuration.Seconds())
	holder := "hub-1"

	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &past, RenewTime: &past,
		},
	}

	fakeClient := newFakeClient(t, existing)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "hub-1", "", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel01")
	require.NoError(t, err)
	assert.True(t, acquired)

	var lease coordinationv1.Lease

	require.NoError(t, fakeClient.Get(context.Background(),
		client.ObjectKey{Name: testZoneLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease))
	assert.True(t, lease.Spec.RenewTime.After(past.Time))
}

func TestTryAcquire_LiveOtherHolderIsRefused(t *testing.T) {
	t.Parallel()

	now := metav1.NowMicro()
	duration := int32(testLeaseDuration.Seconds())
	holder := "hub-2"

	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &now,
		},
	}

	fakeClient := newFakeClient(t, existing)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "hub-1", "", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel01")
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestTryAcquire_ExpiredOtherHolderIsTakenOver(t *testing.T) {
	t.Parallel()

	stale := metav1.NewMicroTime(time.Now().Add(-2 * testLeaseDuration))
	duration := int32(testLeaseDuration.Seconds())
	holder := "hub-2"

	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &stale, RenewTime: &stale,
		},
	}

	fakeClient := newFakeClient(t, existing)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "hub-1", "", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel01")
	require.NoError(t, err)
	assert.True(t, acquired)

	var lease coordinationv1.Lease

	require.NoError(t, fakeClient.Get(context.Background(),
		client.ObjectKey{Name: testZoneLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease))
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, "hub-1", *lease.Spec.HolderIdentity)
	require.NotNil(t, lease.Spec.LeaseTransitions)
	assert.Equal(t, int32(1), *lease.Spec.LeaseTransitions)
}

// TestTryAcquire_OwnZoneKeyIsRefusedWithoutAPICall proves a worker in a
// fleet with a hub never even talks to the API server when asked for its
// own zone's key — the hub reconciles it (see Locker.SelfZoneKey's own
// doc). A nil
// Client would panic if TryAcquire tried to use it, so a nil-panic-free
// return proves no call was attempted.
func TestTryAcquire_OwnZoneKeyIsRefusedWithoutAPICall(t *testing.T) {
	t.Parallel()

	locker := zonelease.NewLocker(nil, nil, "zone-eu-hel01", "eu-hel01", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel01")
	require.NoError(t, err)
	assert.False(t, acquired)
}

// TestTryAcquire_GlobalKeyFromWorkerIsRefusedWithoutAPICall proves a worker
// is refused hub-owned/fleet-wide state (GlobalKey) exactly like its own
// zone's key — see TestTryAcquire_OwnZoneKeyIsRefusedWithoutAPICall's own
// doc for why a nil Client proves no API call was attempted.
func TestTryAcquire_GlobalKeyFromWorkerIsRefusedWithoutAPICall(t *testing.T) {
	t.Parallel()

	locker := zonelease.NewLocker(nil, nil, "zone-eu-hel01", "eu-hel01", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), zonelease.GlobalKey)
	require.NoError(t, err)
	assert.False(t, acquired)
}

// TestTryAcquire_WorkerMayAcquireADifferentZonesKey proves the refusal in
// TestTryAcquire_OwnZoneKeyIsRefusedWithoutAPICall is scoped to the
// worker's own zone, not every zone key — a zone sponsoring further nested
// zones (see pkg/domain/zone's own "further zones it joins" ACME
// propagation) must still be able to reconcile those.
func TestTryAcquire_WorkerMayAcquireADifferentZonesKey(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "zone-eu-hel01", "eu-hel01", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel02")
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestTryAcquire_HubIsNeverRefused(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	locker := zonelease.NewLocker(fakeClient, fakeClient, "hub-1", "", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel01")
	require.NoError(t, err)
	assert.True(t, acquired)

	acquired, err = locker.TryAcquire(context.Background(), zonelease.GlobalKey)
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestJitter(t *testing.T) {
	t.Parallel()

	base := 20 * time.Second

	for range 50 {
		jittered := zonelease.Jitter(base)
		assert.GreaterOrEqual(t, jittered, base/2)
		assert.LessOrEqual(t, jittered, base)
	}

	assert.Equal(t, time.Duration(0), zonelease.Jitter(0))
}

// TestTryAcquire_HubLessMatrix covers every combination of hub/zone peer,
// hub/hub-less and key — see Locker.HubLess's own doc. A nil Client panics
// on use, so a "refused" row also proves no API call was attempted.
func TestTryAcquire_HubLessMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		selfZoneKey string
		hubLess     bool
		key         string
		want        bool
	}{
		{name: "hub, global key", selfZoneKey: "", hubLess: false, key: zonelease.GlobalKey, want: true},
		{name: "hub, zone key", selfZoneKey: "", hubLess: false, key: testZoneKey, want: true},
		{name: "hub-less hub, global key", selfZoneKey: "", hubLess: true, key: zonelease.GlobalKey, want: true},
		{name: "hub-less hub, zone key", selfZoneKey: "", hubLess: true, key: testZoneKey, want: true},
		{name: "worker, global key", selfZoneKey: testZoneKey, hubLess: false, key: zonelease.GlobalKey, want: false},
		{name: "worker, own key", selfZoneKey: testZoneKey, hubLess: false, key: testZoneKey, want: false},
		{name: "worker, other key", selfZoneKey: testZoneKey, hubLess: false, key: otherZoneKey, want: true},
		{name: "peer, global key", selfZoneKey: testZoneKey, hubLess: true, key: zonelease.GlobalKey, want: true},
		{name: "peer, own key", selfZoneKey: testZoneKey, hubLess: true, key: testZoneKey, want: true},
		{name: "peer, other key", selfZoneKey: testZoneKey, hubLess: true, key: otherZoneKey, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var apiClient client.Client
			if test.want {
				apiClient = newFakeClient(t, ownZone(), ownTalosCluster())
			}

			locker := zonelease.NewLockerFor(apiClient, apiClient, zonelease.Identity{
				HolderIdentity: "process-1", SelfZoneKey: test.selfZoneKey, HubLess: test.hubLess,
			})

			acquired, err := locker.TryAcquire(context.Background(), test.key)
			require.NoError(t, err)
			assert.Equal(t, test.want, acquired)
		})
	}
}

// TestTryAcquire_HubLessPeersSerializeGlobalKey proves hub-less peers
// contend for GlobalKey through the Lease like any zone key: the first one
// wins, the second is refused while the first's lease is live.
func TestTryAcquire_HubLessPeersSerializeGlobalKey(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	first := zonelease.NewLockerFor(fakeClient, fakeClient, zonelease.Identity{
		HolderIdentity: "zone-eu-hel01", SelfZoneKey: testZoneKey, HubLess: true,
	})
	second := zonelease.NewLockerFor(fakeClient, fakeClient, zonelease.Identity{
		HolderIdentity: "zone-eu-hel02", SelfZoneKey: otherZoneKey, HubLess: true,
	})

	acquired, err := first.TryAcquire(context.Background(), zonelease.GlobalKey)
	require.NoError(t, err)
	assert.True(t, acquired)

	acquired, err = second.TryAcquire(context.Background(), zonelease.GlobalKey)
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestNewLockerForDefaultsLeaseDuration(t *testing.T) {
	t.Parallel()

	locker := zonelease.NewLockerFor(nil, nil, zonelease.Identity{HolderIdentity: "hub-1", HubLess: true})

	assert.Equal(t, "hub-1", locker.HolderIdentity)
	assert.True(t, locker.HubLess)
	assert.Positive(t, locker.LeaseDuration)
}

// TestTryAcquire_TemporaryPeerIsNeverGrantedGlobalKey covers kontinuum
// up's in-process hub: it reconciles zones, but fleet-wide state is left
// to the fleet's own peers. A nil Client proves no API call is attempted.
func TestTryAcquire_TemporaryPeerIsNeverGrantedGlobalKey(t *testing.T) {
	t.Parallel()

	locker := zonelease.NewLockerFor(nil, nil, zonelease.Identity{
		HolderIdentity: "kontinuum-up", HubLess: true, Temporary: true,
	})

	acquired, err := locker.TryAcquire(context.Background(), zonelease.GlobalKey)
	require.NoError(t, err)
	assert.False(t, acquired)

	fakeClient := newFakeClient(t)
	locker = zonelease.NewLockerFor(fakeClient, fakeClient, zonelease.Identity{
		HolderIdentity: "kontinuum-up", HubLess: true, Temporary: true,
	})

	acquired, err = locker.TryAcquire(context.Background(), testZoneKey)
	require.NoError(t, err)
	assert.True(t, acquired, "zone keys are still contended for")
}

// ownZone and ownTalosCluster are testZoneKey's own Zone and TalosCluster,
// both named after the zone key.
func ownZone() *v1alpha2.Zone {
	return &v1alpha2.Zone{ObjectMeta: metav1.ObjectMeta{Name: testZoneKey, Namespace: v1alpha2.KontinuumSystemNamespace}}
}

func ownTalosCluster() *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneKey, Namespace: v1alpha2.KontinuumSystemNamespace},
	}
}

// removing marks obj as being deleted, held back by a finalizer.
func removing(obj client.Object) {
	now := metav1.Now()
	obj.SetDeletionTimestamp(&now)
	obj.SetFinalizers([]string{"kontinuum.sh/test"})
}

// removingZone and removingTalosCluster are ownZone and ownTalosCluster
// being deleted.
func removingZone() *v1alpha2.Zone {
	zoneObj := ownZone()
	removing(zoneObj)

	return zoneObj
}

func removingTalosCluster() *v1alpha2.TalosCluster {
	cluster := ownTalosCluster()
	removing(cluster)

	return cluster
}

// TestTryAcquire_PeerRefusesOwnKeyWhileItsZoneIsRemoved covers the one
// exception to a hub-less zone reconciling itself: removing it, which
// would delete the Deployment and reset the nodes the peer runs on.
func TestTryAcquire_PeerRefusesOwnKeyWhileItsZoneIsRemoved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		objects []client.Object
	}{
		{name: "zone being deleted", objects: []client.Object{removingZone(), ownTalosCluster()}},
		{name: "talos cluster being deleted", objects: []client.Object{ownZone(), removingTalosCluster()}},
		{name: "zone gone", objects: []client.Object{ownTalosCluster()}},
		{name: "talos cluster gone", objects: []client.Object{ownZone()}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			apiClient := newFakeClient(t, test.objects...)
			locker := zonelease.NewLockerFor(apiClient, apiClient, zonelease.Identity{
				HolderIdentity: "process-1", SelfZoneKey: testZoneKey, HubLess: true,
			})

			acquired, err := locker.TryAcquire(context.Background(), testZoneKey)
			require.NoError(t, err)
			assert.False(t, acquired)

			acquired, err = locker.TryAcquire(context.Background(), otherZoneKey)
			require.NoError(t, err)
			assert.True(t, acquired, "only the peer's own zone is left to others")
		})
	}
}

// shortLeaseDuration keeps KeepRenewed's renewal loop fast enough to test,
// with enough headroom that a loaded -race run still renews in time.
const shortLeaseDuration = 900 * time.Millisecond

func getLease(t *testing.T, apiClient client.Client, name string) *coordinationv1.Lease {
	t.Helper()

	var lease coordinationv1.Lease
	require.NoError(t, apiClient.Get(context.Background(),
		client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease))

	return &lease
}

func TestKeepRenewed_RenewsWhileTheReconcileRuns(t *testing.T) {
	t.Parallel()

	apiClient := newFakeClient(t)
	holder := zonelease.NewLocker(apiClient, apiClient, "process-1", "", shortLeaseDuration)
	contender := zonelease.NewLocker(apiClient, apiClient, "process-2", "", shortLeaseDuration)

	acquired, err := holder.TryAcquire(context.Background(), testZoneKey)
	require.NoError(t, err)
	require.True(t, acquired)

	ctx, release := holder.KeepRenewed(context.Background(), testZoneKey)
	defer release()

	// Run for well past the lease duration: without background renewal
	// the contender would take the Lease over.
	time.Sleep(2 * shortLeaseDuration)

	require.NoError(t, ctx.Err())

	acquired, err = contender.TryAcquire(context.Background(), testZoneKey)
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestKeepRenewed_CancelsWhenTheLeaseIsLost(t *testing.T) {
	t.Parallel()

	apiClient := newFakeClient(t)
	holder := zonelease.NewLocker(apiClient, apiClient, "process-1", "", shortLeaseDuration)

	acquired, err := holder.TryAcquire(context.Background(), testZoneKey)
	require.NoError(t, err)
	require.True(t, acquired)

	ctx, release := holder.KeepRenewed(context.Background(), testZoneKey)
	defer release()

	lease := getLease(t, apiClient, testZoneLeaseName)
	other := "process-2"
	lease.Spec.HolderIdentity = &other
	require.NoError(t, apiClient.Update(context.Background(), lease))

	select {
	case <-ctx.Done():
	case <-time.After(3 * shortLeaseDuration):
		t.Fatal("the reconcile's context outlived the lease")
	}

	require.ErrorIs(t, context.Cause(ctx), zonelease.ErrLeaseLost)
}

func TestKeepRenewed_ReleaseStopsRenewing(t *testing.T) {
	t.Parallel()

	apiClient := newFakeClient(t)
	holder := zonelease.NewLocker(apiClient, apiClient, "process-1", "", shortLeaseDuration)

	acquired, err := holder.TryAcquire(context.Background(), testZoneKey)
	require.NoError(t, err)
	require.True(t, acquired)

	ctx, release := holder.KeepRenewed(context.Background(), testZoneKey)
	release()

	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotErrorIs(t, context.Cause(ctx), zonelease.ErrLeaseLost)
}

func TestReleaseAll_FreesOnlyTheHoldersZoneLeases(t *testing.T) {
	t.Parallel()

	apiClient := newFakeClient(t, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: new("process-1")},
	})
	holder := zonelease.NewLocker(apiClient, apiClient, "process-1", "", testLeaseDuration)
	other := zonelease.NewLocker(apiClient, apiClient, "process-2", "", testLeaseDuration)

	for _, key := range []string{testZoneKey, zonelease.GlobalKey} {
		acquired, err := holder.TryAcquire(context.Background(), key)
		require.NoError(t, err)
		require.True(t, acquired)
	}

	acquired, err := other.TryAcquire(context.Background(), otherZoneKey)
	require.NoError(t, err)
	require.True(t, acquired)

	released, err := zonelease.ReleaseAll(context.Background(), apiClient, "process-1")
	require.NoError(t, err)
	assert.Equal(t, 2, released)

	for _, key := range []string{testZoneKey, zonelease.GlobalKey} {
		acquired, err = other.TryAcquire(context.Background(), key)
		require.NoError(t, err)
		assert.True(t, acquired, "a released lease is free at once, not after the lease duration")
	}

	assert.Equal(t, "process-1", *getLease(t, apiClient, "unrelated").Spec.HolderIdentity,
		"a lease this package didn't create is left alone")
}
