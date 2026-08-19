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
)

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

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
	locker := zonelease.NewLocker(fakeClient, "hub-1", "", testLeaseDuration)

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
	locker := zonelease.NewLocker(fakeClient, "hub-1", "", testLeaseDuration)

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
	locker := zonelease.NewLocker(fakeClient, "hub-1", "", testLeaseDuration)

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
	locker := zonelease.NewLocker(fakeClient, "hub-1", "", testLeaseDuration)

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
	locker := zonelease.NewLocker(fakeClient, "hub-1", "", testLeaseDuration)

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

// TestTryAcquire_OwnZoneKeyIsRefusedWithoutAPICall proves a worker never
// even talks to the API server when asked for its own zone's key — a zone
// must never reconcile its own resources (see Locker's own doc). A nil
// Client would panic if TryAcquire tried to use it, so a nil-panic-free
// return proves no call was attempted.
func TestTryAcquire_OwnZoneKeyIsRefusedWithoutAPICall(t *testing.T) {
	t.Parallel()

	locker := zonelease.NewLocker(nil, "zone-eu-hel01", "eu-hel01", testLeaseDuration)

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

	locker := zonelease.NewLocker(nil, "zone-eu-hel01", "eu-hel01", testLeaseDuration)

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
	locker := zonelease.NewLocker(fakeClient, "zone-eu-hel01", "eu-hel01", testLeaseDuration)

	acquired, err := locker.TryAcquire(context.Background(), "eu-hel02")
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestTryAcquire_HubIsNeverRefused(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	locker := zonelease.NewLocker(fakeClient, "hub-1", "", testLeaseDuration)

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
