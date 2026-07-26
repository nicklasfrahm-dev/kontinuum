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

func TestNewControllerDefaultsIntervals(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:   "controlplane",
		Logger: slog.Default(),
	})

	assert.Equal(t, time.Minute, controller.Config.HeartbeatInterval)
	assert.Equal(t, 5*time.Minute, controller.Config.StaleThreshold)
}

func TestNewControllerKeepsExplicitIntervals(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:              "controlplane",
		Logger:            slog.Default(),
		HeartbeatInterval: time.Second,
		StaleThreshold:    10 * time.Second,
	})

	assert.Equal(t, time.Second, controller.Config.HeartbeatInterval)
	assert.Equal(t, 10*time.Second, controller.Config.StaleThreshold)
}

func TestInstanceNameLowercasesHostname(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "kontinuum-dev-1", registry.InstanceName("KONTINUUM-Dev-1", nil))
}

func TestInstanceNameFallsBackToUUIDWhenHostnameUnavailable(t *testing.T) {
	t.Parallel()

	name := registry.InstanceName("", errTestHostnameUnavailable)
	_, err := uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname lookup fails")

	name = registry.InstanceName("", nil)
	_, err = uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname is empty")
}

func TestCombinedReconcilerDeletesStaleOtherInstanceWithoutTouchingHeartbeat(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "other-instance"},
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

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "other-instance"}}

	_, err := combined.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "other-instance"}, &server)
	assert.True(t, apierrors.IsNotFound(err), "stale instance belonging to someone else should still be deleted")

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "self-instance"}, &server)
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

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "self-instance"}}

	_, err := combined.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "self-instance"}, &server))
	assert.Equal(t, v1alpha2.RoleControlPlane, server.Status.Role)
	assert.False(t, server.Status.LastHeartbeatTime.IsZero())
}

func TestCombinedReconcilerPreservesTTLRequeueForOwnFreshInstance(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "self-instance"},
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

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "self-instance"}}

	result, err := combined.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter, "TTL's own staleness recheck must still be scheduled for self")
}
