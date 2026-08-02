package adminrbac_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/adminrbac"
)

const testInterval = 10 * time.Millisecond

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	err := rbacv1.AddToScheme(scheme)
	require.NoError(t, err)

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// startRunnable starts runnable in a background goroutine and returns a
// cancel func plus the goroutine's completion channel — mirroring
// registry.Heartbeat's own test lifecycle helper.
func startRunnable(runnable *adminrbac.Runnable) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() { done <- runnable.Start(ctx) }()

	return cancel, done
}

// stopRunnable cancels the runnable started by startRunnable and asserts
// Start returns promptly.
func stopRunnable(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}
}

// managedBindingForGroup finds the one managed ClusterRoleBinding annotated
// with group, if any.
func managedBindingForGroup(t *testing.T, fakeClient client.Client, group string) (rbacv1.ClusterRoleBinding, bool) {
	t.Helper()

	var list rbacv1.ClusterRoleBindingList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelManagedBy: adminrbac.ManagedByValue}))

	for _, item := range list.Items {
		if item.Annotations[adminrbac.AdminGroupAnnotation] == group {
			return item, true
		}
	}

	return rbacv1.ClusterRoleBinding{}, false
}

func TestReconcileCreatesRoleAndBindingsForConfiguredGroups(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	runnable := &adminrbac.Runnable{
		Client:      fakeClient,
		AdminGroups: "platform-admins, sre",
		Interval:    testInterval,
		Logger:      slog.Default(),
	}

	cancel, done := startRunnable(runnable)

	var role rbacv1.ClusterRole

	require.Eventually(t, func() bool {
		return fakeClient.Get(context.Background(), types.NamespacedName{Name: adminrbac.RoleName}, &role) == nil
	}, time.Second, time.Millisecond, "admin cluster role was never created")
	assert.Equal(t, adminrbac.ManagedByValue, role.Labels[v1alpha2.LabelManagedBy])

	var platformBinding, sreBinding rbacv1.ClusterRoleBinding

	require.Eventually(t, func() bool {
		var found bool

		platformBinding, found = managedBindingForGroup(t, fakeClient, "platform-admins")
		if !found {
			return false
		}

		sreBinding, found = managedBindingForGroup(t, fakeClient, "sre")

		return found
	}, time.Second, time.Millisecond, "bindings for configured admin groups were never created")

	assert.Equal(t, adminrbac.RoleName, platformBinding.RoleRef.Name)
	assert.Equal(t, "ClusterRole", platformBinding.RoleRef.Kind)
	require.Len(t, platformBinding.Subjects, 1)
	assert.Equal(t, rbacv1.GroupKind, platformBinding.Subjects[0].Kind)
	assert.Equal(t, "platform-admins", platformBinding.Subjects[0].Name)
	assert.Equal(t, adminrbac.ManagedByValue, platformBinding.Labels[v1alpha2.LabelManagedBy])

	assert.Equal(t, "sre", sreBinding.Subjects[0].Name)

	stopRunnable(t, cancel, done)
}

func TestReconcileDeletesBindingForRemovedGroup(t *testing.T) {
	t.Parallel()

	stale := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kontinuum-admin-stale",
			Labels:      map[string]string{v1alpha2.LabelManagedBy: adminrbac.ManagedByValue},
			Annotations: map[string]string{adminrbac.AdminGroupAnnotation: "former-admins"},
		},
		RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: adminrbac.RoleName},
	}

	fakeClient := newFakeClient(t, stale)

	runnable := &adminrbac.Runnable{
		Client:      fakeClient,
		AdminGroups: "sre",
		Interval:    testInterval,
		Logger:      slog.Default(),
	}

	cancel, done := startRunnable(runnable)

	require.Eventually(t, func() bool {
		_, ok := managedBindingForGroup(t, fakeClient, "former-admins")

		return !ok
	}, time.Second, time.Millisecond, "binding for a removed admin group was never deleted")

	require.Eventually(t, func() bool {
		_, ok := managedBindingForGroup(t, fakeClient, "sre")

		return ok
	}, time.Second, time.Millisecond, "binding for the still-configured admin group was never created")

	stopRunnable(t, cancel, done)
}

func TestReconcileLeavesUnmanagedBindingAlone(t *testing.T) {
	t.Parallel()

	unmanaged := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "hand-authored-binding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "view"},
	}

	fakeClient := newFakeClient(t, unmanaged)

	runnable := &adminrbac.Runnable{
		Client:      fakeClient,
		AdminGroups: "",
		Interval:    testInterval,
		Logger:      slog.Default(),
	}

	cancel, done := startRunnable(runnable)

	// No admin groups are configured, so give reconcile a few ticks to run
	// with nothing to converge on, then assert the unmanaged binding — which
	// carries none of v1alpha2.LabelManagedBy — was never touched.
	time.Sleep(5 * testInterval)

	var got rbacv1.ClusterRoleBinding

	err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "hand-authored-binding"}, &got)
	require.NoError(t, err, "unmanaged binding should not have been deleted")
	assert.Equal(t, "view", got.RoleRef.Name)

	stopRunnable(t, cancel, done)
}
