package registry_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

const testStaleThreshold = 5 * time.Minute

func TestStale(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := map[string]struct {
		last      time.Time
		wantStale bool
	}{
		"fresh heartbeat is not stale":       {last: now.Add(-1 * time.Minute), wantStale: false},
		"heartbeat exactly at threshold":     {last: now.Add(-testStaleThreshold), wantStale: true},
		"heartbeat well past threshold":      {last: now.Add(-10 * time.Minute), wantStale: true},
		"heartbeat just under the threshold": {last: now.Add(-testStaleThreshold + time.Second), wantStale: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantStale, registry.Stale(tt.last, now, testStaleThreshold))
		})
	}
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	err := v1alpha2.AddToScheme(scheme)
	require.NoError(t, err)

	// corev1 is needed too: Heartbeat.ensureSecret creates a Namespace and
	// Secret alongside the Kontinuum object itself.
	err = corev1.AddToScheme(scheme)
	require.NoError(t, err)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Kontinuum{}).
		WithObjects(objects...).
		Build()
}

func TestTTLReconcilerDeletesStaleServer(t *testing.T) {
	t.Parallel()

	server := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-server"},
		Status: v1alpha2.KontinuumStatus{
			Role:              v1alpha2.RoleControlPlane,
			LastHeartbeatTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}

	fakeClient := newFakeClient(t, server)

	reconciler := &registry.TTLReconciler{
		Client:         fakeClient,
		StaleThreshold: testStaleThreshold,
		Logger:         slog.Default(),
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "stale-server"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "stale-server"}, &v1alpha2.Kontinuum{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestTTLReconcilerRequeuesFreshServer(t *testing.T) {
	t.Parallel()

	server := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh-server"},
		Status: v1alpha2.KontinuumStatus{
			Role:              v1alpha2.RoleControlPlane,
			LastHeartbeatTime: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
	}

	fakeClient := newFakeClient(t, server)

	reconciler := &registry.TTLReconciler{
		Client:         fakeClient,
		StaleThreshold: testStaleThreshold,
		Logger:         slog.Default(),
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "fresh-server"},
	})
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter)

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "fresh-server"}, &v1alpha2.Kontinuum{})
	assert.NoError(t, err)
}

func TestTTLReconcilerIgnoresMissingServer(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	reconciler := &registry.TTLReconciler{
		Client:         fakeClient,
		StaleThreshold: testStaleThreshold,
		Logger:         slog.Default(),
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing-server"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
}
