package instancepool_test

import (
	"context"
	"fmt"
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

const (
	nodeAName = "node-a"

	poolAName         = "pool-a"
	poolBName         = "pool-b"
	poolCName         = "pool-c"
	poolDName         = "pool-d"
	poolIName         = "pool-i"
	racePoolAName     = "race-pool-a"
	racePoolBName     = "race-pool-b"
	poolFinalizerName = "pool-finalizer"
	poolTeardownName  = "pool-teardown"
)

// statusUpdateCountingClient wraps a client.Client, counting every
// Status().Update call made through it — see
// TestReconcileSkipsRedundantStatusUpdate's own doc for what this is used
// to verify.
type statusUpdateCountingClient struct {
	client.Client

	statusUpdates *int
}

//nolint:ireturn // client.Client's own Status() signature dictates this; wrapping it is the point.
func (c statusUpdateCountingClient) Status() client.SubResourceWriter {
	return countingStatusWriter{c.Client.Status(), c.statusUpdates}
}

type countingStatusWriter struct {
	client.SubResourceWriter

	count *int
}

func (w countingStatusWriter) Update(
	ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption,
) error {
	*w.count++

	err := w.SubResourceWriter.Update(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

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
		ObjectMeta: metav1.ObjectMeta{Name: poolAName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 2},
	}

	fakeClient := newFakeClient(t, pool,
		candidateInstance(nodeAName, true), candidateInstance("node-b", true), candidateInstance("node-c", true))

	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolAName},
	})
	require.NoError(t, err)
	assert.Zero(t, result, "meeting replicas needs no requeue")

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: poolAName}))
	assert.Len(t, list.Items, 2, "only replicas candidates should be claimed")
	assert.Equal(t, nodeAName, list.Items[0].Name, "claims proceed in name-sorted order")
	assert.Equal(t, "node-b", list.Items[1].Name)

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: poolAName}, &got))
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
		ObjectMeta: metav1.ObjectMeta{Name: poolBName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 3},
	}

	fakeClient := newFakeClient(t, pool, candidateInstance(nodeAName, true))

	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolBName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: poolBName}, &got))

	cond := findCondition(got.Status.Conditions, instancepool.InsufficientCapacityConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	readyCond := findCondition(got.Status.Conditions, instancepool.ReadyConditionType)
	require.NotNil(t, readyCond)
	assert.Equal(t, metav1.ConditionFalse, readyCond.Status, "insufficient capacity means not ready")
}

// TestReconcileSkipsRedundantStatusUpdate guards against a reconcile
// storm: this controller's own InstancePool watch (see SetupWithManager)
// carries no predicate, so any Status().Update — even one that changes
// nothing — re-triggers Reconcile. Reconciling twice in a row against the
// same insufficient-capacity state (node-a stays claimed by pool-b after
// the first pass) must only write status once.
func TestReconcileSkipsRedundantStatusUpdate(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: poolBName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 3},
	}

	statusUpdates := 0
	fakeClient := statusUpdateCountingClient{
		newFakeClient(t, pool, candidateInstance(nodeAName, true)), &statusUpdates,
	}

	reconciler := newReconciler(fakeClient)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: poolBName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "first reconcile should persist the new InsufficientCapacity condition")

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "second reconcile computes the same status and should not write again")
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
		ObjectMeta: metav1.ObjectMeta{Name: poolCName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, pool, candidateInstance(nodeAName, false))

	reconciler := newReconciler(fakeClient)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolCName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "an undiscovered candidate leaves the pool short of replicas")

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: poolCName}))
	assert.Empty(t, list.Items, "an undiscovered candidate must not be claimed")

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: poolCName}, &got))
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
		ObjectMeta: metav1.ObjectMeta{Name: poolIName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, pool, candidateInstance(nodeAName, false))
	reconciler := newReconciler(fakeClient)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolIName},
	})
	require.NoError(t, err)

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: poolIName}))
	assert.Empty(t, list.Items, "not claimable before Discovered flips true")

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeAName}, &got))
	got.Status.Conditions = []metav1.Condition{
		{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
	}
	require.NoError(t, fakeClient.Update(context.Background(), &got))

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolIName},
	})
	require.NoError(t, err)

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: poolIName}))
	assert.Len(t, list.Items, 1, "claimable once Discovered is true")
}

func TestReconcileReleasesExcessOnScaleDown(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: poolDName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	claimedA := candidateInstance(nodeAName, true)
	claimedA.Labels[v1alpha2.LabelClaimedBy] = poolDName

	claimedB := candidateInstance("node-b", true)
	claimedB.Labels[v1alpha2.LabelClaimedBy] = poolDName

	fakeClient := newFakeClient(t, pool, claimedA, claimedB)

	reconciler := newReconciler(fakeClient)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolDName},
	})
	require.NoError(t, err)

	var list v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &list,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: poolDName}))
	require.Len(t, list.Items, 1, "excess claims are released")
	assert.Equal(t, nodeAName, list.Items[0].Name, "release keeps the name-sorted-first claim")

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
		ObjectMeta: metav1.ObjectMeta{Name: racePoolAName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}
	poolB := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: racePoolBName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, poolA, poolB, candidateInstance(nodeAName, true))

	reconciler := newReconciler(fakeClient)

	var waitGroup sync.WaitGroup

	waitGroup.Add(2)

	for _, name := range []string{racePoolAName, racePoolBName} {
		go func(poolName string) {
			defer waitGroup.Done()

			_, _ = reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: poolName},
			})
		}(name)
	}

	waitGroup.Wait()

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeAName}, &got))
	require.Contains(t, got.Labels, v1alpha2.LabelClaimedBy)
	assert.Contains(t, []string{racePoolAName, racePoolBName}, got.Labels[v1alpha2.LabelClaimedBy])

	var poolAList, poolBList v1alpha2.InstanceList

	require.NoError(t, fakeClient.List(context.Background(), &poolAList,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: racePoolAName}))
	require.NoError(t, fakeClient.List(context.Background(), &poolBList,
		client.MatchingLabels{v1alpha2.LabelClaimedBy: racePoolBName}))
	assert.Equal(t, 1, len(poolAList.Items)+len(poolBList.Items), "exactly one pool must win the single candidate")
}

func TestReconcileAddsFinalizerOnNormalReconcile(t *testing.T) {
	t.Parallel()

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: poolFinalizerName},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}

	fakeClient := newFakeClient(t, pool)
	reconciler := newReconciler(fakeClient)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolFinalizerName},
	})
	require.NoError(t, err)

	var got v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: poolFinalizerName}, &got))
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
		ObjectMeta: metav1.ObjectMeta{Name: poolTeardownName, Finalizers: []string{instancepool.InstancePoolFinalizer}},
		Spec:       v1alpha2.InstancePoolSpec{Selector: poolSelector(), Replicas: 1},
	}
	claimedInstance := candidateInstance(nodeAName, true)
	claimedInstance.Labels[v1alpha2.LabelClaimedBy] = poolTeardownName

	fakeClient := newFakeClient(t, pool, claimedInstance)
	reconciler := newReconciler(fakeClient)

	// Delete — the fake client rejects Create with a DeletionTimestamp
	// directly, so Create-then-Delete is how this reaches "finalizer
	// present, DeletionTimestamp set", the same state a real apiserver
	// leaves an InstancePool in once it's deleted (or GC-cascaded — see
	// pkg/domain/zone/add.go's own owner-reference note) while it still
	// carries the finalizer added on its first normal reconcile.
	var toDelete v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: poolTeardownName}, &toDelete))
	require.NoError(t, fakeClient.Delete(context.Background(), &toDelete))

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolTeardownName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "must requeue to confirm the release actually landed")

	var releasedInstance v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeAName}, &releasedInstance))
	assert.NotContains(t, releasedInstance.Labels, v1alpha2.LabelClaimedBy,
		"instance must be released, not left claimed")

	// The pool itself is still around — the next reconcile, not finalizer
	// removal in this same pass, is what confirms zero remain claimed.
	var stillPresent v1alpha2.InstancePool

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: poolTeardownName}, &stillPresent))
	assert.Contains(t, stillPresent.Finalizers, instancepool.InstancePoolFinalizer)

	result, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: poolTeardownName},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	var gone v1alpha2.InstancePool

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: poolTeardownName}, &gone)
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
