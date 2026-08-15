package taloscluster_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

// configuredInstance is claimedDiscoveredInstance plus
// MemberConfiguredConditionType — the signal reconcileTeardown checks
// before ever attempting a Reset (see its own doc: a never-configured
// Instance has nothing installed to reset).
func configuredInstance(name, poolName, addr string) *v1alpha2.Instance {
	inst := claimedDiscoveredInstance(name, poolName, addr)
	inst.Status.Conditions = append(inst.Status.Conditions, metav1.Condition{
		Type: taloscluster.MemberConfiguredConditionType, Status: metav1.ConditionTrue, Reason: "ConfigApplied",
	})

	return inst
}

func newInstanceResetReconciler(
	fakeClient client.Client, bootstrapper *fakeBootstrapper,
) *taloscluster.InstanceResetReconciler {
	return &taloscluster.InstanceResetReconciler{
		Client:        fakeClient,
		Bootstrapper:  bootstrapper,
		RetryInterval: testRetryInterval,
		ResetTimeout:  testHealthCheckInterval, // an hour — generous, no test below means to hit it except its own
		Logger:        slog.Default(),
	}
}

func instanceRequest(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Name: name}}
}

// unclaimedInstanceName is the shared fixture name reused across every
// unclaimed-Instance test below.
const unclaimedInstanceName = "node-a"

func TestInstanceResetReconcileAddsFinalizerWhenClaimedByReferencedPool(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)
	reconciler := newInstanceResetReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), instanceRequest(cpNodeName))
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &got))
	assert.Contains(t, got.Finalizers, taloscluster.InstanceResetFinalizer)
}

func TestInstanceResetReconcileSkipsUnclaimedInstance(t *testing.T) {
	t.Parallel()

	unclaimed := &v1alpha2.Instance{ObjectMeta: metav1.ObjectMeta{Name: unclaimedInstanceName}}

	fakeClient := newFakeClient(t, unclaimed)
	reconciler := newInstanceResetReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), instanceRequest(unclaimedInstanceName))
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: unclaimedInstanceName}, &got))
	assert.Empty(t, got.Finalizers, "an unclaimed instance isn't part of any cluster yet")
}

// unclaimedFormerMemberInstance builds an Instance that's no longer claimed
// by any pool but still carries every condition this package's own member
// reconciler ever sets, plus DiscoveredConditionType — as if it had been
// released (or its claiming pool/cluster deleted) after once being
// configured, joined, and healthy in a previous cluster.
func unclaimedFormerMemberInstance(name string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha2.InstanceStatus{
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
				{Type: taloscluster.MemberConfiguredConditionType, Status: metav1.ConditionTrue, Reason: "ConfigApplied"},
				{Type: taloscluster.MemberJoinedConditionType, Status: metav1.ConditionTrue, Reason: "Joined"},
				{Type: taloscluster.MemberReadyConditionType, Status: metav1.ConditionTrue, Reason: "Healthy"},
			},
		},
	}
}

func TestInstanceResetReconcileClearsStaleMemberConditionsForUnclaimedInstance(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, unclaimedFormerMemberInstance(unclaimedInstanceName))
	reconciler := newInstanceResetReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), instanceRequest(unclaimedInstanceName))
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: unclaimedInstanceName}, &got))
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.MemberConfiguredConditionType),
		"no longer claimed by any pool, so this no longer describes anything real")
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.MemberJoinedConditionType))
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.MemberReadyConditionType))
	assert.NotNil(t, meta.FindStatusCondition(got.Status.Conditions, instance.DiscoveredConditionType),
		"instance.Reconciler owns this one, not this package — must be left alone")
}

// TestInstanceResetReconcileRemovesDanglingFinalizerFromDeletingUnclaimedInstance
// covers a real stuck-deletion bug: an Instance owned directly by a Zone
// (see zone/add.go) gets its own deletionTimestamp set by the same GC
// cascade that deletes its owning InstancePool — whose own reconcileTeardown
// races to strip LabelClaimedBy first (see reconcileUnclaimed's own doc).
// Once that label is gone, Reconcile has no pool, and so no owning
// TalosCluster, left to resolve a reset endpoint through — if this
// reconciler didn't remove InstanceResetFinalizer here, the object (and the
// finalizer) would be stuck forever: unclaimed can never transition back to
// claimed, so the branch that would otherwise remove it is never reached
// again.
func TestInstanceResetReconcileRemovesDanglingFinalizerFromDeletingUnclaimedInstance(t *testing.T) {
	t.Parallel()

	deleting := unclaimedFormerMemberInstance(unclaimedInstanceName)
	deleting.Finalizers = []string{taloscluster.InstanceResetFinalizer}
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	fakeClient := newFakeClient(t, deleting)
	reconciler := newInstanceResetReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), instanceRequest(unclaimedInstanceName))
	require.NoError(t, err)

	var got v1alpha2.Instance

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: unclaimedInstanceName}, &got)
	if err == nil {
		assert.NotContains(t, got.Finalizers, taloscluster.InstanceResetFinalizer,
			"no owning pool left to resolve a reset endpoint through — nothing left to block deletion on")

		return
	}

	assert.True(t, apierrors.IsNotFound(err), "with no finalizers left, the fake client should let it go")
}

func TestInstanceResetReconcileSkipsPoolNoClusterReferences(t *testing.T) {
	t.Parallel()

	orphanInstance := claimedDiscoveredInstance(unclaimedInstanceName, "orphan-pool", "10.0.0.9")

	fakeClient := newFakeClient(t, orphanInstance)
	reconciler := newInstanceResetReconciler(fakeClient, &fakeBootstrapper{})

	_, err := reconciler.Reconcile(context.Background(), instanceRequest(unclaimedInstanceName))
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: unclaimedInstanceName}, &got))
	assert.Empty(t, got.Finalizers, "claimed by a pool no TalosCluster references")
}

func TestInstanceResetReconcileTeardownSkipsResetWhenNeverConfigured(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	// Discovered and claimed, but never actually configured.
	cpInstance := claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	cpInstance.Finalizers = []string{taloscluster.InstanceResetFinalizer}

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{}
	reconciler := newInstanceResetReconciler(fakeClient, bootstrapper)

	var toDelete v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	result, err := reconciler.Reconcile(context.Background(), instanceRequest(cpNodeName))
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Empty(t, bootstrapper.resetCalls, "nothing was ever configured, so there's nothing to reset")

	var gone v1alpha2.Instance

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &gone)
	assert.True(t, apierrors.IsNotFound(err))
}

// TestInstanceResetReconcileTeardownResetsSoleControlPlaneMember covers the
// case this whole package's own graceful-flag fix exists for: a
// single-member control-plane cluster can't gracefully leave etcd.
func TestInstanceResetReconcileTeardownResetsSoleControlPlaneMember(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	cpInstance.Finalizers = []string{taloscluster.InstanceResetFinalizer}

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	// Persist a real secrets bundle via the ordinary bootstrap reconciler —
	// same fixture-setup pattern the now-removed reset_test.go used.
	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: testClusterName},
	})
	require.NoError(t, err)

	reconciler := newInstanceResetReconciler(fakeClient, bootstrapper)

	var toDelete v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	result, err := reconciler.Reconcile(context.Background(), instanceRequest(cpNodeName))
	require.NoError(t, err)
	assert.Zero(t, result)

	require.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.resetCalls,
		"the sole member has nowhere else to dial through but itself")
	assert.Equal(t, []bool{false}, bootstrapper.resetGracefulCalls,
		"a single-member cluster must never attempt a graceful reset")

	var gone v1alpha2.Instance

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &gone)
	assert.True(t, apierrors.IsNotFound(err), "instance must be fully deleted once the finalizer is removed")
}

// TestInstanceResetReconcileTeardownResetsControlPlaneMemberGracefully
// covers the HA case: another live control-plane member means both a real
// dial target and a safe graceful reset.
func TestInstanceResetReconcileTeardownResetsControlPlaneMemberGracefully(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance1 := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	cpInstance1.Finalizers = []string{taloscluster.InstanceResetFinalizer}
	cpInstance2 := configuredInstance("cp-node-2", "cp-pool", "10.0.0.9")

	fakeClient := newFakeClient(t, cluster, cpInstance1, cpInstance2)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: testClusterName},
	})
	require.NoError(t, err)

	reconciler := newInstanceResetReconciler(fakeClient, bootstrapper)

	var toDelete v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	_, err = reconciler.Reconcile(context.Background(), instanceRequest(cpNodeName))
	require.NoError(t, err)

	require.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.resetCalls)
	assert.Equal(t, []bool{true}, bootstrapper.resetGracefulCalls,
		"another live control-plane member means a graceful reset is safe")
}

// TestInstanceResetReconcileTeardownResetsWorkerThroughControlPlane covers
// the gap this whole feature closes: a worker was never reset by anything
// before this reconciler existed.
func TestInstanceResetReconcileTeardownResetsWorkerThroughControlPlane(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	workerInstance := configuredInstance(workerNodeName, "worker-pool", workerInstanceAddress)
	workerInstance.Finalizers = []string{taloscluster.InstanceResetFinalizer}

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: testClusterName},
	})
	require.NoError(t, err)

	reconciler := newInstanceResetReconciler(fakeClient, bootstrapper)

	var toDelete v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: workerNodeName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	_, err = reconciler.Reconcile(context.Background(), instanceRequest(workerNodeName))
	require.NoError(t, err)

	require.Equal(t, []string{workerInstanceAddress}, bootstrapper.resetCalls, "the worker itself is the reset target")
	assert.Equal(t, []bool{true}, bootstrapper.resetGracefulCalls,
		"a worker never runs etcd, so graceful is always safe")

	var gone v1alpha2.Instance

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: workerNodeName}, &gone)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestInstanceResetReconcileTeardownRetriesOnFailure(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	cpInstance.Finalizers = []string{taloscluster.InstanceResetFinalizer}

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: testClusterName},
	})
	require.NoError(t, err)

	bootstrapper.resetErr = assert.AnError

	reconciler := newInstanceResetReconciler(fakeClient, bootstrapper)

	var toDelete v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	result, err := reconciler.Reconcile(context.Background(), instanceRequest(cpNodeName))
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &got))
	assert.Contains(t, got.Finalizers, taloscluster.InstanceResetFinalizer,
		"finalizer must stay while reset keeps retrying")
}

func TestInstanceResetReconcileTeardownGivesUpAfterTimeout(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	cpInstance.Finalizers = []string{taloscluster.InstanceResetFinalizer}

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig"), resetErr: assert.AnError}

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: testClusterName},
	})
	require.NoError(t, err)

	reconciler := &taloscluster.InstanceResetReconciler{
		Client:        fakeClient,
		Bootstrapper:  bootstrapper,
		RetryInterval: testRetryInterval,
		ResetTimeout:  0, // effectively already elapsed the instant DeletionTimestamp is set
		Logger:        slog.Default(),
	}

	var toDelete v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	result, err := reconciler.Reconcile(context.Background(), instanceRequest(cpNodeName))
	require.NoError(t, err)
	assert.Zero(t, result)

	var gone v1alpha2.Instance

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &gone)
	assert.True(t, apierrors.IsNotFound(err),
		"teardown must give up and remove the finalizer once past ResetTimeout")
}
