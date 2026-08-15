package taloscluster

import (
	"context"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// reconcileTeardown resets, then releases or deletes — per
// cluster.Spec.Teardown.UnregisterInstances, see v1alpha2.TeardownSpec's
// own doc — every Instance still claimed by cluster's own control-plane
// and worker pools, before removing TalosClusterFinalizer. See that
// constant's own doc for why this, not Instance's own
// deletion-triggered InstanceResetFinalizer, is what makes cluster
// teardown reset its members: by design, the default
// (UnregisterInstances false) never deletes the Instance at all, so
// nothing would ever trigger that finalizer's own path. Works identically
// whether cluster is being torn down via its owning Zone or deleted
// directly — this reconciler doesn't know or care which.
//
// Members are processed workers-first, then control-plane (see
// listClaimedMembers), since a worker reset dials through a live
// control-plane member (see resolveResetEndpoint) — control plane must
// still be up to serve that. A member that fails to reset is left claimed
// and retried on the next pass; one already released or deleted in a
// previous pass simply no longer appears in listClaimedMembers, so this
// makes forward progress without needing any separate bookkeeping on
// cluster itself. Past TeardownTimeout since cluster's own
// DeletionTimestamp, this gives up and removes the finalizer anyway —
// mirrors zone.Reconciler's own TeardownTimeout and
// InstanceResetReconciler's own ResetTimeout, and the same "not a
// finalizer that blocks deletion forever" rationale.
func (r *Reconciler) reconcileTeardown(ctx context.Context, cluster *v1alpha2.TalosCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cluster, TalosClusterFinalizer) {
		return ctrl.Result{}, nil
	}

	if time.Since(cluster.DeletionTimestamp.Time) > r.TeardownTimeout {
		r.Logger.Warn(
			"giving up on resetting cluster members after exceeding the teardown timeout — "+
				"still-claimed instances may need a manual reset before being safely rejoined elsewhere",
			"cluster", cluster.Name, "timeout", r.TeardownTimeout)

		return r.removeClusterFinalizer(ctx, cluster)
	}

	members, err := listClaimedMembers(ctx, r.Client, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(members) == 0 {
		return r.removeClusterFinalizer(ctx, cluster)
	}

	anyFailed := false

	for _, member := range members {
		err := r.teardownMember(ctx, cluster, member.inst, member.role)
		if err != nil {
			anyFailed = true

			r.Logger.Warn("failed to reset cluster member, will retry",
				"cluster", cluster.Name, "instance", member.inst.Name, "error", err)
		}
	}

	if anyFailed {
		return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
	}

	return r.removeClusterFinalizer(ctx, cluster)
}

// teardownMember resets member (unless it was never actually configured —
// same gate InstanceResetReconciler's own reconcileTeardown uses, since
// there'd be nothing installed to reset), clears its now-stale
// Configured/Joined/Ready conditions (see clearMemberConditions), then
// either deletes it (cluster.Spec.Teardown.UnregisterInstances) or
// releases its claim. Deleting still trips InstanceResetFinalizer's own
// teardown path on member, but since Configured is already cleared here
// first, that finalizer sees "never configured" and just removes itself —
// no redundant Reset RPC against hardware this already reset.
func (r *Reconciler) teardownMember(
	ctx context.Context, cluster *v1alpha2.TalosCluster, inst v1alpha2.Instance, role memberRole,
) error {
	if meta.IsStatusConditionTrue(inst.Status.Conditions, MemberConfiguredConditionType) {
		err := resetInstance(ctx, r.Client, r.Bootstrapper, &inst, cluster, role)
		if err != nil {
			return err
		}
	}

	clearMemberConditions(ctx, r.Client, r.Logger, &inst)

	if cluster.Spec.Teardown.UnregisterInstances {
		err := r.Client.Delete(ctx, &inst)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete instance %q: %w", inst.Name, err)
		}

		return nil
	}

	return releaseMember(ctx, r.Client, &inst)
}

// releaseMember removes LabelClaimedBy from inst and persists that — the
// same single-item body instancepool.Reconciler's own release loop uses,
// tolerating a conflict (left claimed, retried on reconcileTeardown's next
// pass) rather than treating it as fatal.
func releaseMember(ctx context.Context, kubeClient client.Client, inst *v1alpha2.Instance) error {
	delete(inst.Labels, v1alpha2.LabelClaimedBy)

	err := kubeClient.Update(ctx, inst)
	if err != nil && !apierrors.IsConflict(err) {
		return fmt.Errorf("failed to release instance %q: %w", inst.Name, err)
	}

	return nil
}

// removeClusterFinalizer removes TalosClusterFinalizer from cluster and
// persists that — with cluster.DeletionTimestamp already set (the only way
// reconcileTeardown is ever reached) and no finalizers left, this Update
// is what actually lets the apiserver finish deleting cluster.
func (r *Reconciler) removeClusterFinalizer(ctx context.Context, cluster *v1alpha2.TalosCluster) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(cluster, TalosClusterFinalizer)

	err := r.Client.Update(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from talos cluster %q: %w", cluster.Name, err)
	}

	return ctrl.Result{}, nil
}

// claimedMember pairs an Instance with which role it fills — see
// listClaimedMembers, which returns every one of cluster's own currently
// claimed members already in the order reconcileTeardown must process
// them.
type claimedMember struct {
	inst v1alpha2.Instance
	role memberRole
}

// listClaimedMembers lists every Instance currently claimed by any of
// cluster's own pools — every named worker pool, then control-plane, in
// that order (see reconcileTeardown's own doc for why workers go first) —
// sorted by name within each group for deterministic processing order.
// Unlike resolveMembers, this doesn't filter by Discovered: an unreachable
// member still needs releasing (or deleting) even if resetting it will
// predictably fail and get retried.
func listClaimedMembers(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster,
) ([]claimedMember, error) {
	var members []claimedMember

	for _, worker := range cluster.Spec.Workers {
		claimed, err := listClaimedByPool(ctx, kubeClient, cluster.Namespace, worker.PoolRef.Name)
		if err != nil {
			return nil, err
		}

		sort.Slice(claimed, func(i, j int) bool { return claimed[i].Name < claimed[j].Name })

		for _, inst := range claimed {
			members = append(members, claimedMember{inst: inst, role: roleWorker})
		}
	}

	claimed, err := listClaimedByPool(ctx, kubeClient, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef.Name)
	if err != nil {
		return nil, err
	}

	sort.Slice(claimed, func(i, j int) bool { return claimed[i].Name < claimed[j].Name })

	for _, inst := range claimed {
		members = append(members, claimedMember{inst: inst, role: roleControlPlane})
	}

	return members, nil
}
