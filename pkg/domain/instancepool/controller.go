// Package instancepool implements InstancePool's claim-only reconciler —
// see issue #24's architecture decision 2/5. It claims unclaimed Instance
// objects matching spec.selector up to spec.replicas via a conditional
// (CAS) label update, and releases the excess back to the candidate pool
// when replicas shrinks. The provider-template/create-on-demand path is
// explicitly out of scope for this phase (see InstanceTemplateSpec's own
// doc). instancepool.kontinuum.sh's CRD is already ensured by
// pkg/domain/instance.EnsureCRDs — no separate ensure step lives here.
package instancepool

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	// InsufficientCapacityConditionType is InstancePool.status.conditions'
	// condition set when fewer than spec.replicas candidates could be
	// claimed.
	InsufficientCapacityConditionType = "InsufficientCapacity"
	// ReadyConditionType mirrors InsufficientCapacity's own content but with
	// conventional Ready polarity (True = good) rather than
	// InsufficientCapacity's "problem" polarity (True = bad) — a CRD with no
	// Kind-specific handling only renders a populated READY/REASON/STATUS
	// column in `kubectl tree` if status.conditions carries an entry
	// literally Typed "Ready" (kstatus/kubectl-tree's generic-CRD fallback;
	// mirrors zone.ReadyConditionType's own reasoning). Set in updateStatus,
	// this reconciler's only status writer.
	ReadyConditionType = "Ready"

	// InstancePoolFinalizer keeps a deleted InstancePool around until it
	// has released every Instance it claimed, added to every InstancePool
	// on normal reconcile — without it, deleting a pool (or losing it to
	// GC cascade — see pkg/domain/zone/add.go's own owner-reference note)
	// would leave its claimed Instances with a stale
	// kontinuum.sh/claimed-by label pointing at a pool that no longer
	// exists, never released back for another pool to claim.
	InstancePoolFinalizer = "kontinuum.sh/instancepool-teardown"

	reasonClaimed             = "Claimed"
	reasonCandidatesExhausted = "CandidatesExhausted"

	defaultRetryInterval = 30 * time.Second
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// RetryInterval is how long Reconcile waits before retrying after
	// leaving InsufficientCapacity set. Defaults to thirty seconds when
	// zero.
	RetryInterval time.Duration
	// ZoneLease is this process's own zonelease.Locker identity — see
	// zonelease.Identity's own doc. Reconcile refuses to mutate an
	// InstancePool it doesn't hold its own zone lease for.
	ZoneLease zonelease.Identity
}

// Controller wires the InstancePool claim reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting RetryInterval when
// left zero.
func NewController(cfg Config) *Controller {
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the InstancePool claim reconciler on mgr. It
// also watches Instance — any Instance change (a new candidate appearing,
// one being deleted, a label changing) can affect which pool should claim
// it, so every InstancePool is re-enqueued on any Instance change. The
// expected number of InstancePool objects is small, so this broad
// re-enqueue (rather than narrowing to pools whose selector actually
// matches the changed Instance) stays cheap — the same trade-off this
// repo's registry.CombinedReconciler already accepts for a similarly small
// object count.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:        mgr.GetClient(),
		RetryInterval: c.Config.RetryInterval,
		Locker: zonelease.NewLocker(
			mgr.GetClient(), mgr.GetAPIReader(), c.Config.ZoneLease.HolderIdentity, c.Config.ZoneLease.SelfZoneKey, 0),
		Logger: c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.InstancePool{}).
		Watches(&v1alpha2.Instance{}, handler.EnqueueRequestsFromMapFunc(reconciler.enqueueAllPools)).
		Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register instancepool controller: %w", err)
	}

	return nil
}

// Reconciler claims and releases Instance objects on behalf of an
// InstancePool — see issue #24's architecture decision 2/5.
type Reconciler struct {
	Client        client.Client
	RetryInterval time.Duration
	// Locker gates every write below against zonelease — see
	// Config.ZoneLease's own doc.
	Locker *zonelease.Locker
	Logger *slog.Logger
}

// Reconcile implements reconcile.Reconciler: it releases any claimed
// Instance beyond spec.replicas, then claims unclaimed candidates up to
// spec.replicas, then writes status.readyReplicas and
// InsufficientCapacityConditionType.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool, found, err := r.fetchPool(ctx, req.NamespacedName)
	if !found {
		return ctrl.Result{}, err
	}

	result, acquired, err := r.acquireZoneLease(ctx, &pool)
	if !acquired {
		return result, err
	}

	if !pool.DeletionTimestamp.IsZero() {
		return r.reconcileTeardown(ctx, &pool)
	}

	err = r.ensureFinalizer(ctx, &pool)
	if err != nil {
		return ctrl.Result{}, err
	}

	claimed, err := r.listClaimed(ctx, pool.Namespace, pool.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(claimed) > int(pool.Spec.Replicas) {
		claimed, err = r.release(ctx, &pool, claimed, int(pool.Spec.Replicas))
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	insufficient := false

	if len(claimed) < int(pool.Spec.Replicas) {
		claimed, err = r.claim(ctx, &pool, claimed)
		if err != nil {
			return ctrl.Result{}, err
		}

		insufficient = len(claimed) < int(pool.Spec.Replicas)
	}

	return r.updateStatus(ctx, &pool, claimed, insufficient)
}

// fetchPool fetches the InstancePool named by key, folding NotFound and any
// real Get error into one found=false return — a single decision point in
// Reconcile (mirrors acquireZoneLease's own doc) purely to keep its own
// cyclomatic complexity down. err is always nil alongside a NotFound
// (nothing to reconcile, not a failure); Reconcile doesn't need to tell the
// two apart, only whether to stop.
func (r *Reconciler) fetchPool(ctx context.Context, key client.ObjectKey) (v1alpha2.InstancePool, bool, error) {
	var pool v1alpha2.InstancePool

	err := r.Client.Get(ctx, key, &pool)
	if apierrors.IsNotFound(err) {
		return pool, false, nil
	}

	if err != nil {
		return pool, false, fmt.Errorf("failed to get instance pool %q: %w", key.Name, err)
	}

	return pool, true, nil
}

// acquireZoneLease gates every write below against zonelease — see
// Config.ZoneLease's own doc — factored out purely to keep Reconcile's own
// cyclomatic complexity down. The bool is always false alongside a non-nil
// error, so callers only need to check it, not `err != nil || !acquired`.
func (r *Reconciler) acquireZoneLease(ctx context.Context, pool *v1alpha2.InstancePool) (ctrl.Result, bool, error) {
	acquired, err := r.Locker.TryAcquire(ctx, pool.Name)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("failed to acquire zone lease for instance pool %q: %w", pool.Name, err)
	}

	if !acquired {
		return ctrl.Result{RequeueAfter: zonelease.Jitter(r.RetryInterval)}, false, nil
	}

	return ctrl.Result{}, true, nil
}

// ensureFinalizer adds InstancePoolFinalizer to pool and persists that, if
// not already present.
func (r *Reconciler) ensureFinalizer(ctx context.Context, pool *v1alpha2.InstancePool) error {
	if !controllerutil.AddFinalizer(pool, InstancePoolFinalizer) {
		return nil
	}

	err := r.Client.Update(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to add finalizer to instance pool %q: %w", pool.Name, err)
	}

	return nil
}

// reconcileTeardown releases every Instance still claimed by pool, then
// removes InstancePoolFinalizer once none remain — see that constant's own
// doc. Release is attempted (not just waited for) because nothing else
// unclaims these Instances on a pool's behalf: unlike zone.reconcileTeardown
// driving a real physical side effect (the Talos Reset), an InstancePool has
// none of its own — release's only job here is to stop the label from
// dangling once the pool itself is gone. A conflict releasing one Instance
// (see release's own doc) just means this requeues and tries again, the
// same as ordinary scale-down already tolerates.
func (r *Reconciler) reconcileTeardown(ctx context.Context, pool *v1alpha2.InstancePool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pool, InstancePoolFinalizer) {
		return ctrl.Result{}, nil
	}

	claimed, err := r.listClaimed(ctx, pool.Namespace, pool.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(claimed) > 0 {
		_, err = r.release(ctx, pool, claimed, 0)
		if err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
	}

	controllerutil.RemoveFinalizer(pool, InstancePoolFinalizer)

	err = r.Client.Update(ctx, pool)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from instance pool %q: %w", pool.Name, err)
	}

	return ctrl.Result{}, nil
}

// enqueueAllPools maps any Instance change to a reconcile request for every
// InstancePool — see SetupWithManager's own doc for why this is broad
// rather than selector-filtered.
func (r *Reconciler) enqueueAllPools(ctx context.Context, _ client.Object) []ctrl.Request {
	var pools v1alpha2.InstancePoolList

	err := r.Client.List(ctx, &pools)
	if err != nil {
		r.Logger.Error("failed to list instance pools for instance watch", "error", err)

		return nil
	}

	requests := make([]ctrl.Request, 0, len(pools.Items))
	for _, pool := range pools.Items {
		requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&pool)})
	}

	return requests
}

// listClaimed lists every Instance currently claimed by poolName, scoped to
// namespace — an InstancePool only ever claims Instances in its own
// namespace, the same implicit same-namespace reference convention
// InstancePoolReference itself relies on (see issue #63's architecture).
func (r *Reconciler) listClaimed(ctx context.Context, namespace, poolName string) ([]v1alpha2.Instance, error) {
	var list v1alpha2.InstanceList

	err := r.Client.List(ctx, &list,
		client.InNamespace(namespace), client.MatchingLabels{v1alpha2.LabelClaimedBy: poolName})
	if err != nil {
		return nil, fmt.Errorf("failed to list instances claimed by %q: %w", poolName, err)
	}

	return list.Items, nil
}

// release unclaims the excess of claimed beyond target, name-sorted for
// deterministic behavior, and returns the remaining claimed set. Reconcile
// passes pool.Spec.Replicas as target for ordinary scale-down; reconcileTeardown
// passes zero, releasing every claimed Instance ahead of the pool's own
// deletion. A conflict releasing one Instance is logged and left for the
// next reconcile, not fatal — the candidate stays (harmlessly) over-claimed
// until then.
func (r *Reconciler) release(
	ctx context.Context, pool *v1alpha2.InstancePool, claimed []v1alpha2.Instance, target int,
) ([]v1alpha2.Instance, error) {
	sort.Slice(claimed, func(i, j int) bool { return claimed[i].Name < claimed[j].Name })

	remaining := make([]v1alpha2.Instance, 0, target)

	for i, inst := range claimed {
		if i < target {
			remaining = append(remaining, inst)

			continue
		}

		delete(inst.Labels, v1alpha2.LabelClaimedBy)

		err := r.Client.Update(ctx, &inst)
		if err != nil && !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("failed to release instance %q from pool %q: %w", inst.Name, pool.Name, err)
		}

		if err != nil {
			r.Logger.Warn("conflict releasing instance, will retry next reconcile",
				"instance", inst.Name, "pool", pool.Name, "error", err)

			remaining = append(remaining, inst)
		}
	}

	return remaining, nil
}

// claim lists candidates matching pool.Spec.Selector, filters out anything
// already claimed by any pool or not yet Discovered, and attempts to claim
// each — in name-sorted order, for deterministic behavior — until either
// spec.replicas is met or candidates are exhausted. A conflict claiming one
// candidate means another pool won the race for it; that candidate is
// skipped, not retried, and the next candidate is tried instead.
//
// The Discovered check is load-bearing, not an optimization: claiming used
// to be optimistic (claim first, let discovery catch up), but
// instance.Reconciler stops touching an Instance entirely once claimed (see
// its own doc) — so claiming one before it was ever Discovered would leave
// Discovered permanently unset, and resolveMembers/updateStatus, which both
// gate on Discovered, would then never count it: TalosCluster's
// control-plane readiness would deadlock forever. Requiring Discovered
// here, before the claim, is what closes that race.
func (r *Reconciler) claim(
	ctx context.Context, pool *v1alpha2.InstancePool, claimed []v1alpha2.Instance,
) ([]v1alpha2.Instance, error) {
	selector, err := metav1.LabelSelectorAsSelector(&pool.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse selector for pool %q: %w", pool.Name, err)
	}

	var candidates v1alpha2.InstanceList

	err = r.Client.List(ctx, &candidates,
		client.InNamespace(pool.Namespace), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, fmt.Errorf("failed to list candidate instances for pool %q: %w", pool.Name, err)
	}

	unclaimed := make([]v1alpha2.Instance, 0, len(candidates.Items))

	for _, inst := range candidates.Items {
		_, alreadyClaimed := inst.Labels[v1alpha2.LabelClaimedBy]
		discovered := meta.IsStatusConditionTrue(inst.Status.Conditions, instance.DiscoveredConditionType)

		if !alreadyClaimed && discovered {
			unclaimed = append(unclaimed, inst)
		}
	}

	sort.Slice(unclaimed, func(i, j int) bool { return unclaimed[i].Name < unclaimed[j].Name })

	for _, candidate := range unclaimed {
		if len(claimed) >= int(pool.Spec.Replicas) {
			break
		}

		claimedInst, ok, err := r.tryClaim(ctx, pool, candidate.Name)
		if err != nil {
			return nil, err
		}

		if ok {
			claimed = append(claimed, claimedInst)
		}
	}

	return claimed, nil
}

// tryClaim re-fetches name (a fresh Get, not the possibly-stale copy from
// claim's own List) and attempts the CAS label update. A conflict means
// another pool already claimed it since the List above — reported as
// (zero, false, nil), not an error, so claim's loop moves on to the next
// candidate.
func (r *Reconciler) tryClaim(
	ctx context.Context, pool *v1alpha2.InstancePool, name string,
) (v1alpha2.Instance, bool, error) {
	var inst v1alpha2.Instance

	err := r.Client.Get(ctx, client.ObjectKey{Name: name, Namespace: pool.Namespace}, &inst)
	if apierrors.IsNotFound(err) {
		return v1alpha2.Instance{}, false, nil
	}

	if err != nil {
		return v1alpha2.Instance{}, false, fmt.Errorf("failed to get candidate instance %q: %w", name, err)
	}

	if _, alreadyClaimed := inst.Labels[v1alpha2.LabelClaimedBy]; alreadyClaimed {
		return v1alpha2.Instance{}, false, nil
	}

	if inst.Labels == nil {
		inst.Labels = map[string]string{}
	}

	inst.Labels[v1alpha2.LabelClaimedBy] = pool.Name

	err = r.Client.Update(ctx, &inst)
	if apierrors.IsConflict(err) {
		return v1alpha2.Instance{}, false, nil
	}

	if err != nil {
		return v1alpha2.Instance{}, false, fmt.Errorf("failed to claim instance %q for pool %q: %w", name, pool.Name, err)
	}

	return inst, true, nil
}

// updateStatus writes pool.Status.ReadyReplicas (claimed and Discovered)
// and InsufficientCapacityConditionType, then persists it. insufficient
// requeues after RetryInterval so a since-appeared candidate gets tried
// again; sufficient capacity doesn't — the Instance watch (see
// SetupWithManager) re-triggers on the next relevant change instead.
//
// The Status().Update is skipped when neither ReadyReplicas nor either
// condition actually changed. This controller's own InstancePool watch
// (see SetupWithManager's For(&v1alpha2.InstancePool{}), which carries no
// predicate) re-triggers Reconcile on every Update to an InstancePool,
// including its own status-subresource writes; an unconditional write
// here would self-trigger a reconcile storm the same way pkg/domain/zone's
// identical persistStatus doc describes.
func (r *Reconciler) updateStatus(
	ctx context.Context, pool *v1alpha2.InstancePool, claimed []v1alpha2.Instance, insufficient bool,
) (ctrl.Result, error) {
	ready := int32(0)

	for _, inst := range claimed {
		if meta.IsStatusConditionTrue(inst.Status.Conditions, instance.DiscoveredConditionType) {
			ready++
		}
	}

	changed := pool.Status.ReadyReplicas != ready
	pool.Status.ReadyReplicas = ready

	status := metav1.ConditionFalse

	reason := reasonClaimed

	message := fmt.Sprintf("claimed %d/%d replicas", len(claimed), pool.Spec.Replicas)

	if insufficient {
		status = metav1.ConditionTrue
		reason = reasonCandidatesExhausted
		message = fmt.Sprintf("only claimed %d/%d replicas: no more matching unclaimed candidates",
			len(claimed), pool.Spec.Replicas)
	}

	insufficientChanged := meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:    InsufficientCapacityConditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})

	readyStatus := metav1.ConditionTrue
	if insufficient {
		readyStatus = metav1.ConditionFalse
	}

	readyChanged := meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:    ReadyConditionType,
		Status:  readyStatus,
		Reason:  reason,
		Message: message,
	})

	if changed || insufficientChanged || readyChanged {
		err := r.Client.Status().Update(ctx, pool)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update instance pool %q status: %w", pool.Name, err)
		}
	}

	if insufficient {
		return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
	}

	return ctrl.Result{}, nil
}
