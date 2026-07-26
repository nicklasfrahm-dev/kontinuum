package registry_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

const testHeartbeatInterval = 10 * time.Millisecond

func TestHeartbeatRegistersAndUpdatesStatus(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Role:     v1alpha2.RoleWorker,
		Spec:     v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never created with a heartbeat")

	assert.Equal(t, v1alpha2.RoleWorker, server.Status.Role)
	assert.Equal(t, "eu", server.Spec.Region)
	assert.Equal(t, "eu-1a", server.Spec.Zone)

	firstHeartbeat := server.Status.LastHeartbeatTime.Time

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server)

		return err == nil && server.Status.LastHeartbeatTime.After(firstHeartbeat)
	}, time.Second, time.Millisecond, "heartbeat never advanced")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}

	err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server)
	assert.True(t, apierrors.IsNotFound(err), "server should be deregistered after Start returns")
}

func TestHeartbeatStartAdoptsPreexistingRegistration(t *testing.T) {
	t.Parallel()

	// Simulates a hot-reload restart: the previous process's own object
	// (same hostname-derived name) is still around, either because its
	// graceful deregistration lost the race with the new process starting,
	// or the TTL reconciler hasn't expired it yet.
	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server"},
		Spec:       v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker},
	})

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Role:     v1alpha2.RoleWorker,
		Spec:     v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "Start should adopt the pre-existing registration and heartbeat it, not fail")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "Start must not treat an already-registered object as fatal")
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}
}

func TestHeartbeatReregistersIfDeletedExternally(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Role:     v1alpha2.RoleWorker,
		Spec:     v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx := t.Context()

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never created with a heartbeat")

	// Simulate a manual deletion — via the UI's delete button, or kubectl —
	// out from under the running process.
	require.NoError(t, fakeClient.Delete(context.Background(), &server))

	require.Eventually(t, func() bool {
		var recreated v1alpha2.Kontinuum

		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &recreated)

		return err == nil && !recreated.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never re-registered after external deletion")

	var recreated v1alpha2.Kontinuum

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &recreated))
	assert.Equal(t, v1alpha2.RoleWorker, recreated.Status.Role)
	assert.Equal(t, "eu", recreated.Spec.Region)
	assert.Equal(t, "eu-1a", recreated.Spec.Zone)
}

func TestHeartbeatReconcileReregistersOnDelete(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client: fakeClient,
		Name:   "test-server",
		Role:   v1alpha2.RoleControlPlane,
		Logger: slog.Default(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-server"}}

	_, err := heartbeat.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server))
	assert.Equal(t, v1alpha2.RoleControlPlane, server.Status.Role)
	assert.False(t, server.Status.LastHeartbeatTime.IsZero())
}

func TestHeartbeatReconcileNoOpsWhenServerExists(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server"},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker},
	})

	heartbeat := &registry.Heartbeat{
		Client: fakeClient,
		Name:   "test-server",
		Logger: slog.Default(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-server"}}

	result, err := heartbeat.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}
