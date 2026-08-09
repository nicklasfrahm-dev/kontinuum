package registry_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

var errTestHostnameUnavailable = errors.New("hostname unavailable")

// otherInstanceKey() and selfInstanceKey() are "other-instance"/"self-instance"'s
// own NamespacedName — every Kontinuum fixture in this file registers as
// v1alpha2.DefaultSecretNamespace (see Heartbeat.Start's own doc), so every
// Get/Request below needs both, not just Name.
func otherInstanceKey() types.NamespacedName {
	return types.NamespacedName{Name: "other-instance", Namespace: v1alpha2.DefaultSecretNamespace}
}

func selfInstanceKey() types.NamespacedName {
	return types.NamespacedName{Name: "self-instance", Namespace: v1alpha2.DefaultSecretNamespace}
}

func TestNewControllerDefaultsIntervals(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:   "ControlPlane",
		Logger: slog.Default(),
	})

	assert.Equal(t, time.Minute, controller.Config.HeartbeatInterval)
	assert.Equal(t, 5*time.Minute, controller.Config.StaleThreshold)
}

func TestNewControllerKeepsExplicitIntervals(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:              "ControlPlane",
		Logger:            slog.Default(),
		HeartbeatInterval: time.Second,
		StaleThreshold:    10 * time.Second,
	})

	assert.Equal(t, time.Second, controller.Config.HeartbeatInterval)
	assert.Equal(t, 10*time.Second, controller.Config.StaleThreshold)
}

// TestControllerDeregisterNoOpsBeforeSetupWithManager covers the guard on
// Controller.Deregister's heartbeat == nil case: SetupWithManager (which
// builds the Heartbeat Deregister delegates to) needs a real controller-
// runtime Manager, out of scope for this package's other, fake-client-based
// unit tests — but runServe could in principle call Deregister before
// startup ever reaches SetupWithManager, and that must not panic.
func TestControllerDeregisterNoOpsBeforeSetupWithManager(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:   "ControlPlane",
		Logger: slog.Default(),
	})

	assert.NoError(t, controller.Deregister(context.Background()))
}

func TestInstanceNameLowercasesHostname(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "kontinuum-dev-1", registry.InstanceName("KONTINUUM-Dev-1", nil))
}

func TestInstanceNameFallsBackToUUIDWhenHostnameUnavailable(t *testing.T) {
	t.Parallel()

	name := registry.InstanceName("", errTestHostnameUnavailable)
	instanceID, err := uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname lookup fails")
	assert.Equal(t, uuid.Version(7), instanceID.Version(), "expected a UUIDv7 fallback")

	name = registry.InstanceName("", nil)
	instanceID, err = uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname is empty")
	assert.Equal(t, uuid.Version(7), instanceID.Version(), "expected a UUIDv7 fallback")
}

func TestInstanceNameFallsBackToUUIDWhenHostnameIsLocalhost(t *testing.T) {
	t.Parallel()

	name := registry.InstanceName("localhost", nil)
	instanceID, err := uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname is localhost")
	assert.Equal(t, uuid.Version(7), instanceID.Version(), "expected a UUIDv7 fallback")

	name = registry.InstanceName("LOCALHOST", nil)
	_, err = uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname is localhost, regardless of case")
}

func TestCombinedReconcilerDeletesStaleOtherInstanceWithoutTouchingHeartbeat(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "other-instance", Namespace: v1alpha2.DefaultSecretNamespace},
		Status: v1alpha2.KontinuumStatus{
			Role:              v1alpha2.RoleWorker,
			LastHeartbeatTime: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
	})

	combined := &registry.CombinedReconciler{
		TTL: &registry.TTLReconciler{
			Client:         fakeClient,
			StaleThreshold: time.Minute,
			Logger:         slog.Default(),
		},
		Heartbeat: &registry.Heartbeat{
			Client: fakeClient,
			Name:   "self-instance",
			Logger: slog.Default(),
		},
	}

	req := ctrl.Request{NamespacedName: otherInstanceKey()}

	_, err := combined.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	err = fakeClient.Get(context.Background(), otherInstanceKey(), &server)
	assert.True(t, apierrors.IsNotFound(err), "stale instance belonging to someone else should still be deleted")

	err = fakeClient.Get(context.Background(), selfInstanceKey(), &server)
	assert.True(t, apierrors.IsNotFound(err), "reconciling another instance must not create this instance's own object")
}

func TestCombinedReconcilerReregistersOwnDeletedInstance(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	combined := &registry.CombinedReconciler{
		TTL: &registry.TTLReconciler{
			Client:         fakeClient,
			StaleThreshold: time.Minute,
			Logger:         slog.Default(),
		},
		Heartbeat: &registry.Heartbeat{
			Client: fakeClient,
			Name:   "self-instance",
			Role:   v1alpha2.RoleControlPlane,
			Logger: slog.Default(),
		},
	}

	req := ctrl.Request{NamespacedName: selfInstanceKey()}

	_, err := combined.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	require.NoError(t, fakeClient.Get(context.Background(), selfInstanceKey(), &server))
	assert.Equal(t, v1alpha2.RoleControlPlane, server.Status.Role)
	assert.False(t, server.Status.LastHeartbeatTime.IsZero())
}

func TestCombinedReconcilerPreservesTTLRequeueForOwnFreshInstance(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "self-instance", Namespace: v1alpha2.DefaultSecretNamespace},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleControlPlane, LastHeartbeatTime: metav1.Now()},
	})

	combined := &registry.CombinedReconciler{
		TTL: &registry.TTLReconciler{
			Client:         fakeClient,
			StaleThreshold: time.Minute,
			Logger:         slog.Default(),
		},
		Heartbeat: &registry.Heartbeat{
			Client: fakeClient,
			Name:   "self-instance",
			Role:   v1alpha2.RoleControlPlane,
			Logger: slog.Default(),
		},
	}

	req := ctrl.Request{NamespacedName: selfInstanceKey()}

	result, err := combined.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter, "TTL's own staleness recheck must still be scheduled for self")
}
