package zone //nolint:testpackage // exercises unexported version-watch predicate/mapper directly

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// testHubKontinuumName names the fixture hub Kontinuum shared across this
// file's tests — a hub's own self-registration always has an empty
// Spec.Region/Zone (see mapKontinuumToZone's own doc), which is exactly the
// case mapKontinuumVersionChangeToAllZones must still fan out to every Zone
// for.
const testHubKontinuumName = "hub"

// TestKontinuumVersionChangedPredicate is the regression test for the bug
// this predicate (paired with mapKontinuumVersionChangeToAllZones) closes:
// a hub upgrade writes its own new build version onto its own Kontinuum's
// status.version, but that Kontinuum has no Spec.Region/Zone
// (mapKontinuumToZone's key for it is "", matching no Zone at all), so
// nothing ever woke any already-Ready Zone back up to notice resolveImage's
// target tag moved on. This predicate must let that exact
// status.version-only change through so mapKontinuumVersionChangeToAllZones
// can enqueue every Zone, while filtering out every other Kontinuum event
// (most importantly the routine heartbeat tick that only bumps
// status.lastHeartbeatTime) so that watch doesn't list every Zone in the
// fleet on every single heartbeat interval.
func TestKontinuumVersionChangedPredicate(t *testing.T) {
	t.Parallel()

	hub := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: testHubKontinuumName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status:     v1alpha2.KontinuumStatus{Version: "v1.0.0"},
	}

	t.Run("create with a version reported", func(t *testing.T) {
		t.Parallel()
		assert.True(t, kontinuumVersionChangedPredicate.Create(event.CreateEvent{Object: hub}))
	})

	t.Run("create with no version reported yet", func(t *testing.T) {
		t.Parallel()

		fresh := hub.DeepCopy()
		fresh.Status.Version = ""
		assert.False(t, kontinuumVersionChangedPredicate.Create(event.CreateEvent{Object: fresh}))
	})

	t.Run("update actually changes version — the hub-upgrade case", func(t *testing.T) {
		t.Parallel()

		updated := hub.DeepCopy()
		updated.Status.Version = "v1.1.0"

		assert.True(t, kontinuumVersionChangedPredicate.Update(event.UpdateEvent{ObjectOld: hub, ObjectNew: updated}))
	})

	t.Run("update leaves version unchanged — routine heartbeat tick", func(t *testing.T) {
		t.Parallel()

		heartbeated := hub.DeepCopy()
		heartbeated.Status.LastHeartbeatTime = metav1.Now()

		assert.False(t,
			kontinuumVersionChangedPredicate.Update(event.UpdateEvent{ObjectOld: hub, ObjectNew: heartbeated}))
	})

	t.Run("delete is ignored — mapKontinuumToZone's own concern, not resolveImage's", func(t *testing.T) {
		t.Parallel()
		assert.False(t, kontinuumVersionChangedPredicate.Delete(event.DeleteEvent{Object: hub}))
	})

	t.Run("generic is ignored", func(t *testing.T) {
		t.Parallel()
		assert.False(t, kontinuumVersionChangedPredicate.Generic(event.GenericEvent{Object: hub}))
	})
}

// TestMapKontinuumVersionChangeToAllZonesEnqueuesEveryZone covers
// mapKontinuumVersionChangeToAllZones's own "every Zone, not just the one
// belonging to whichever Kontinuum changed" behavior — the fix itself:
// resolveImage's own anyRegisteredKontinuum can pick any registered
// Kontinuum as the fleet's target tag, so every Zone's own Deployment needs
// a chance to catch up once any one of them changes, not just the Zone (if
// any) the changed Kontinuum happens to belong to.
func TestMapKontinuumVersionChangeToAllZonesEnqueuesEveryZone(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	zoneA := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-eu-1a", Namespace: v1alpha2.KontinuumSystemNamespace},
	}
	zoneB := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "us-us-1a", Namespace: v1alpha2.KontinuumSystemNamespace},
	}

	hubClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zoneA, zoneB).Build()

	reconciler := &Reconciler{Client: hubClient, Logger: slog.Default()}

	// The hub's own Kontinuum — Spec.Region/Zone left zero, exactly like a
	// real hub's self-registration (see mapKontinuumToZone's own doc for why
	// zonelease.Key returns "" for that case) — proving this mapper doesn't
	// key off the changed Kontinuum's own zone the way mapKontinuumToZone
	// does.
	hub := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: testHubKontinuumName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status:     v1alpha2.KontinuumStatus{Version: "v1.1.0"},
	}

	requests := reconciler.mapKontinuumVersionChangeToAllZones(t.Context(), hub)

	assert.ElementsMatch(t, []ctrl.Request{
		{NamespacedName: client.ObjectKeyFromObject(zoneA)},
		{NamespacedName: client.ObjectKeyFromObject(zoneB)},
	}, requests)
}

// TestMapKontinuumVersionChangeToAllZonesReturnsNilOnListError covers the
// mapper's own fail-safe: a List error logs and returns no requests rather
// than panicking or enqueueing a partial/stale set — mirroring how every
// other MapFunc in this package (mapTalosClusterToZone/mapKontinuumToZone)
// has no error return of its own to propagate one through.
func TestMapKontinuumVersionChangeToAllZonesReturnsNilOnListError(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	// v1alpha2 deliberately not registered — Listing v1alpha2.ZoneList
	// against this client fails, standing in for a real API-server error.

	hubClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &Reconciler{Client: hubClient, Logger: slog.Default()}

	requests := reconciler.mapKontinuumVersionChangeToAllZones(t.Context(),
		&v1alpha2.Kontinuum{ObjectMeta: metav1.ObjectMeta{Name: testHubKontinuumName}})

	assert.Nil(t, requests)
}
