package registry_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

const testHeartbeatInterval = 10 * time.Millisecond

func TestHeartbeatRegistersAndUpdatesStatus(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Spec:     v1alpha1.KontinuumSpec{Role: v1alpha1.RoleWorker, Region: "eu", Zone: "eu-1a"},
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha1.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-server"}, &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never created with a heartbeat")

	assert.Equal(t, v1alpha1.RoleWorker, server.Spec.Role)
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
