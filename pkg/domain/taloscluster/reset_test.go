package taloscluster_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

func TestResetControlPlaneSkipsWhenNeverBootstrapped(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	fakeClient := newFakeClient(t, cluster)
	bootstrapper := &fakeBootstrapper{}

	err := taloscluster.ResetControlPlane(t.Context(), fakeClient, bootstrapper, cluster)
	require.NoError(t, err)
	assert.Empty(t, bootstrapper.resetCalls, "no secrets bundle ever persisted means no talosCfg to reset with")
}

func TestResetControlPlaneSkipsWhenNoMembers(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	fakeClient := newFakeClient(t, cluster)
	bootstrapper := &fakeBootstrapper{}
	reconciler := newReconciler(fakeClient, bootstrapper)

	// Reconcile once to persist a secrets bundle (see reconcileControlPlane
	// -> ensureSecretsBundle), without ever claiming a control-plane
	// member — mirrors a Zone deleted before its TalosCluster bootstrapped
	// past the "waiting for instances" stage.
	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.TalosCluster
	require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Name: testClusterName}, &got))

	err = taloscluster.ResetControlPlane(t.Context(), fakeClient, bootstrapper, &got)
	require.NoError(t, err)
	assert.Empty(t, bootstrapper.resetCalls, "no discovered control-plane member means nothing to reset")
}

func TestResetControlPlaneResetsEveryControlPlaneMember(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.TalosCluster
	require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Name: testClusterName}, &got))
	require.NotEmpty(t, got.Status.SecretRef.Name, "reconcile must have persisted a secrets bundle")

	err = taloscluster.ResetControlPlane(t.Context(), fakeClient, bootstrapper, &got)
	require.NoError(t, err)
	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.resetCalls)
}

// TestResetControlPlaneReportsPerMemberErrors covers a partial failure
// across more than one control-plane member: ResetControlPlane must still
// attempt every member (not short-circuit on the first failure) and report
// every failure it saw, not just the last one.
func TestResetControlPlaneReportsPerMemberErrors(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance1 := claimedDiscoveredInstance("cp-node-1", "cp-pool", controlPlaneInstanceAddress)
	cpInstance2 := claimedDiscoveredInstance("cp-node-2", "cp-pool", "10.0.0.9")

	fakeClient := newFakeClient(t, cluster, cpInstance1, cpInstance2)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	var got v1alpha2.TalosCluster
	require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Name: testClusterName}, &got))

	bootstrapper.resetErr = assert.AnError

	err = taloscluster.ResetControlPlane(t.Context(), fakeClient, bootstrapper, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, assert.AnError)
	assert.ElementsMatch(t, []string{controlPlaneInstanceAddress, "10.0.0.9"}, bootstrapper.resetCalls,
		"every member must be attempted even once one has already failed")
}
