package instancepool_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instancepool"
)

const testRetryInterval = 30 * time.Second

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	err := v1alpha2.AddToScheme(scheme)
	require.NoError(t, err)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.InstancePool{}).
		WithObjects(objects...).
		Build()
}

func newReconciler(fakeClient client.Client) *instancepool.Reconciler {
	return &instancepool.Reconciler{
		Client:        fakeClient,
		RetryInterval: testRetryInterval,
		Logger:        slog.Default(),
	}
}

func candidateInstance(name string, discovered bool) *v1alpha2.Instance {
	inst := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"tier": "worker"}},
	}

	if discovered {
		inst.Status.Conditions = []metav1.Condition{
			{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
		}
	}

	return inst
}

func poolSelector() metav1.LabelSelector {
	return metav1.LabelSelector{MatchLabels: map[string]string{"tier": "worker"}}
}

func TestReconcileClaimsUpToReplicas(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-a"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 2},
	}

	fakeClient := newFakeClient(t, pool,
		candidateInstance("node-a", true), candidateInstance("node-b", true), candidateInstance("node-c", true))

	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-a"},
	})
	require.NoError(t, err)
	assert.Zero(t, result, "meeting replicas needs no requeue")

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "pool-a"}))
	assert.Len(t, list.Items, 2, "only replicas candidates should be claimed")
	assert.Equal(t, "node-a", list.Items[0].Name, "claims proceed in name-sorted order")
	assert.Equal(t, "node-b", list.Items[1].Name)

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-a"}, &got))
	assert.Equal(t, int32(2), got.Status.ReadyReplicas)

	cond := findCondition(got.Status.Conditions, instancepool.InsufficientCapacityConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)

	// Ready mirrors InsufficientCapacity's content but with inverted
	// polarity (True = good) — see instancepool.ReadyConditionType's own
	// doc for why this exists (kstatus/kubectl-tree's generic-CRD fallback
	// only recognizes a condition literally Typed "Ready").
	readyCond := findCondition(got.Status.Conditions, instancepool.ReadyConditionType)
	require.NotNil(t, readyCond)
	assert.Equal(t, metav1.ConditionTrue, readyCond.Status)
}

func TestReconcileReportsInsufficientCapacity(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-b"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 3},
	}

	fakeClient := newFakeClient(t, pool, candidateInstance("node-a", true))

	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-b"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-b"}, &got))

	cond := findCondition(got.Status.Conditions, instancepool.InsufficientCapacityConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	readyCond := findCondition(got.Status.Conditions, instancepool.ReadyConditionType)
	require.NotNil(t, readyCond)
	assert.Equal(t, metav1.ConditionFalse, readyCond.Status, "insufficient capacity means not ready")
}

// TestReconcileClaimingRequiresDiscovery covers the fix for a real
// deadlock: claiming used to be optimistic (an undiscovered candidate was
// still claimable, discovery expected to catch up after), but
// instance.Reconciler stops touching an Instance entirely once claimed —
// so an undiscovered claim left Discovered permanently unset, and
// TalosCluster's control-plane readiness (gated on Discovered) never
// recovered. An undiscovered candidate must now be left unclaimed instead.
func TestReconcileClaimingRequiresDiscovery(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-c"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, pool, candidateInstance("node-a", false))

	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-c"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "an undiscovered candidate leaves the pool short of replicas")

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "pool-c"}))
	assert.Empty(t, list.Items, "an undiscovered candidate must not be claimed")

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-c"}, &got))
	assert.Zero(t, got.Status.ReadyReplicas)

	cond := findCondition(got.Status.Conditions, instancepool.InsufficientCapacityConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status, "no discovered candidates means insufficient capacity")
}

// TestReconcileClaimsOnceDiscovered covers the other half: a candidate left
// unclaimed for being undiscovered becomes claimable the moment Discovered
// flips true, with no other change needed.
func TestReconcileClaimsOnceDiscovered(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-i"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, pool, candidateInstance("node-a", false))
	reconciler := newReconciler(fakeClient)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-i"},
	})
	require.NoError(t, err)

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "pool-i"}))
	assert.Empty(t, list.Items, "not claimable before Discovered flips true")

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &got))
	got.Status.Conditions = []metav1.Condition{
		{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
	}
	require.NoError(t, fakeClient.Update(context.Background(), &got))

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-i"},
	})
	require.NoError(t, err)

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "pool-i"}))
	assert.Len(t, list.Items, 1, "claimable once Discovered is true")
}

func TestReconcileReleasesExcessOnScaleDown(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-d"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	claimedA := candidateInstance("node-a", true)
	claimedA.Labels[v1alpha2.LabelClaimedBy] = "pool-d"

	claimedB := candidateInstance("node-b", true)
	claimedB.Labels[v1alpha2.LabelClaimedBy] = "pool-d"

	fakeClient := newFakeClient(t, pool, claimedA, claimedB)

	reconciler := newReconciler(fakeClient)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-d"},
	})
	require.NoError(t, err)

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "pool-d"}))
	require.Len(t, list.Items, 1, "excess claims are released")
	assert.Equal(t, "node-a", list.Items[0].Name, "release keeps the name-sorted-first claim")

	var released v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-b"}, &released))
	assert.NotContains(t, released.Labels, v1alpha2.LabelClaimedBy)
}

func TestReconcileIgnoresMissingPool(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
}

// TestReconcileNoDoubleClaimUnderRace is the issue's own "no double-claim
// under a race" milestone: two pools with an identical selector both try to
// claim the single available candidate concurrently. The fake client
// enforces resourceVersion checks the same way a real apiserver does, so
// exactly one of the two concurrent Reconcile calls should win the
// candidate.
func TestReconcileNoDoubleClaimUnderRace(t *testing.T) {
	t.Parallel()

	poolA := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "race-pool-a"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}
	poolB := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "race-pool-b"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, poolA, poolB, candidateInstance("node-a", true))

	reconciler := newReconciler(fakeClient)

	var waitGroup sync.WaitGroup

	waitGroup.Add(2)

	for _, name := range []string{"race-pool-a", "race-pool-b"} {
		go func(poolName string) {
			defer waitGroup.Done()

			_, _ = reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: poolName},
			})
		}(name)
	}

	waitGroup.Wait()

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &got))
	require.Contains(t, got.Labels, v1alpha2.LabelClaimedBy)
	assert.Contains(t, []string{"race-pool-a", "race-pool-b"}, got.Labels[v1alpha2.LabelClaimedBy])

	var poolAList, poolBList v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &poolAList,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "race-pool-a"}))
	require.NoError(t, fakeClient.List(context.Background(), &poolBList,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: "race-pool-b"}))
	assert.Equal(t, 1, len(poolAList.Items)+len(poolBList.Items), "exactly one pool must win the single candidate")
}

func TestReconcileAddsFinalizerOnNormalReconcile(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-finalizer"},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, pool)
	reconciler := newReconciler(fakeClient)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-finalizer"},
	})
	require.NoError(t, err)

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-finalizer"}, &got))
	assert.Contains(t, got.Finalizers, instancepool.InstancePoolFinalizer)
}

// TestReconcileTeardownReleasesClaimedInstancesBeforeRemovingFinalizer
// covers the fix for a real gap: without InstancePoolFinalizer, deleting an
// InstancePool left its claimed Instances with a stale
// kontinuum.sh/claimed-by label pointing at a pool that no longer existed,
// never released back for another pool to claim.
func TestReconcileTeardownReleasesClaimedInstancesBeforeRemovingFinalizer(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-teardown", Finalizers: []string{instancepool.InstancePoolFinalizer}},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}
	claimedInstance := candidateInstance("node-a", true)
	claimedInstance.Labels[v1alpha2.LabelClaimedBy] = "pool-teardown"

	fakeClient := newFakeClient(t, pool, claimedInstance)
	reconciler := newReconciler(fakeClient)

	// Delete — the fake client rejects Create with a DeletionTimestamp
	// directly, so Create-then-Delete is how this reaches "finalizer
	// present, DeletionTimestamp set", the same state a real apiserver
	// leaves an InstancePool in once it's deleted (or GC-cascaded — see
	// pkg/domain/zone/add.go's own owner-reference note) while it still
	// carries the finalizer added on its first normal reconcile.
	var toDelete v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-teardown"}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-teardown"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "must requeue to confirm the release actually landed")

	var releasedInstance v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &releasedInstance))
	assert.NotContains(t, releasedInstance.Labels, v1alpha2.LabelClaimedBy,
		"instance must be released, not left claimed")

	// The pool itself is still around — the next reconcile, not finalizer
	// removal in this same pass, is what confirms zero remain claimed.
	var stillPresent v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-teardown"}, &stillPresent))
	assert.Contains(t, stillPresent.Finalizers, instancepool.InstancePoolFinalizer)

	result, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool-teardown"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	var gone v1alpha2.InstancePool

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "pool-teardown"}, &gone)
	assert.True(t, apierrors.IsNotFound(err), "instance pool must be fully deleted once the finalizer is removed")
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}

	return nil
}
