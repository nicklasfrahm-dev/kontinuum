package taloscluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

// testControlPlanePool is the control-plane pool testCluster references.
const testControlPlanePool = "cp-pool"

// threeReplicaPool is the control-plane pool testCluster references, asking
// for a three-member control plane.
func threeReplicaPool() *v1alpha2.InstancePool {
	return &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: testControlPlanePool},
		Spec:       v1alpha2.InstancePoolSpec{Replicas: 3},
	}
}

// TestReconcileWaitsForEveryControlPlaneReplica proves a three-replica
// control plane is neither configured nor bootstrapped while only some of
// its members are claimed: bootstrapping on the first member would turn
// ControlPlaneReady true on one node and never configure the rest.
func TestReconcileWaitsForEveryControlPlaneReplica(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, testCluster(), threeReplicaPool(),
		claimedDiscoveredInstance("cp-node-1", testControlPlanePool, "10.0.0.1"),
		claimedDiscoveredInstance("cp-node-2", testControlPlanePool, "10.0.0.2"))

	bootstrapper := &fakeBootstrapper{}
	reconciler := newReconciler(fakeClient, bootstrapper)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, bootstrapper.applyConfigCalls)
	assert.Empty(t, bootstrapper.bootstrapCalls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "WaitingForInstances", cond.Reason)
	assert.Equal(t, "2 of 3 control-plane instances claimed and discovered", cond.Message)
}

// TestReconcileBootstrapsOnceAndJoinsEveryReplica proves that once every
// replica is claimed, all of them get their machine config, etcd is
// bootstrapped exactly once — on the first member by name, however the
// list came back — and ControlPlaneReady turns true with all of them.
func TestReconcileBootstrapsOnceAndJoinsEveryReplica(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, testCluster(), threeReplicaPool(),
		claimedDiscoveredInstance("cp-node-3", testControlPlanePool, "10.0.0.3"),
		claimedDiscoveredInstance("cp-node-1", testControlPlanePool, "10.0.0.1"),
		claimedDiscoveredInstance("cp-node-2", testControlPlanePool, "10.0.0.2"))

	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("kubeconfig")}
	reconciler := newReconciler(fakeClient, bootstrapper)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testClusterName},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, bootstrapper.applyConfigCalls)
	assert.Equal(t, []string{"10.0.0.1"}, bootstrapper.bootstrapCalls)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
}
