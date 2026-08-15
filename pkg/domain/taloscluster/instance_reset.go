package taloscluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// errNoSecretsBundle and errNoControlPlaneEndpoint are resetInstance's own
// wrapped-static-error cases — see resolveResetEndpoint's own doc for the
// second.
var (
	errNoSecretsBundle        = errors.New("talos cluster has no secrets bundle yet")
	errNoControlPlaneEndpoint = errors.New("no reachable control-plane member left to dial through")
)

const (
	// InstanceResetFinalizer keeps a deleted Instance around until its own
	// physical node has been reset back to Talos maintenance mode — added
	// to every Instance claimed by a pool some TalosCluster references
	// (control-plane or worker), checked/removed by InstanceResetReconciler.
	// This is what makes deleting an Instance always properly offboard its
	// node, not just a full Zone teardown (see reset.go's own former
	// ResetControlPlane, which only ever reset control-plane members and
	// only as part of zone.reconcileTeardown): scaling down a worker pool,
	// releasing an Instance from a pool, or a plain `kubectl delete
	// instance` now all reset the node the same way.
	InstanceResetFinalizer = "kontinuum.sh/talos-reset"

	// defaultInstanceResetTimeout bounds how long InstanceResetReconciler
	// keeps retrying a stuck Reset (e.g. genuinely unreachable hardware)
	// before giving up and removing the finalizer anyway — mirrors
	// zone.Reconciler's own TeardownTimeout default and its identical "not
	// a finalizer that blocks deletion forever" rationale.
	defaultInstanceResetTimeout = 15 * time.Minute
)

// memberRole is which of a TalosCluster's pools an Instance was claimed
// by — control-plane membership is what decides whether a graceful reset is
// even possible (see resetInstance's own doc); worker membership never
// blocks on it.
type memberRole int

const (
	roleControlPlane memberRole = iota
	roleWorker
)

// InstanceResetConfig configures an InstanceResetController.
type InstanceResetConfig struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Bootstrapper issues the actual Reset RPC. Defaults to
	// NewTalosBootstrapper(cfg.Logger) when nil, the same production
	// implementation Controller's own Config defaults to.
	Bootstrapper ClusterBootstrapper
	// RetryInterval is how long Reconcile waits before retrying a Reset
	// that hasn't succeeded yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
	// ResetTimeout bounds how long a deleted Instance's finalizer keeps
	// retrying Reset before giving up and removing itself anyway. Defaults
	// to fifteen minutes when zero.
	ResetTimeout time.Duration
}

// InstanceResetController wires InstanceResetReconciler onto a
// controller-runtime Manager. See NewInstanceResetController.
type InstanceResetController struct {
	Config InstanceResetConfig
}

// NewInstanceResetController builds an InstanceResetController from cfg,
// defaulting Bootstrapper, RetryInterval, and ResetTimeout when left zero.
func NewInstanceResetController(cfg InstanceResetConfig) *InstanceResetController {
	if cfg.Bootstrapper == nil {
		cfg.Bootstrapper = NewTalosBootstrapper(cfg.Logger)
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	if cfg.ResetTimeout == 0 {
		cfg.ResetTimeout = defaultInstanceResetTimeout
	}

	return &InstanceResetController{Config: cfg}
}

// SetupWithManager registers InstanceResetReconciler on mgr — a separate
// controller-runtime Controller from Controller's own TalosCluster
// reconciler above (registered by pkg/cli/serve.go's own
// talosClusterOptions, alongside it), since this one watches Instance, not
// TalosCluster. instances.kontinuum.sh's CRD is already ensured by
// instance.EnsureCRDs — no separate ensure step lives here.
func (c *InstanceResetController) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &InstanceResetReconciler{
		Client:        mgr.GetClient(),
		Bootstrapper:  c.Config.Bootstrapper,
		RetryInterval: c.Config.RetryInterval,
		ResetTimeout:  c.Config.ResetTimeout,
		Logger:        c.Config.Logger,
	}

	// Named explicitly: controller-runtime otherwise derives a controller's
	// name from the Kind it watches ("instance"), which
	// instance.Controller's own discovery reconciler — a completely
	// separate Controller, also watching Instance — already claims;
	// registering two controllers under the same name fails outright.
	err := ctrl.NewControllerManagedBy(mgr).Named("instance-reset").For(&v1alpha2.Instance{}).Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register instance-reset controller: %w", err)
	}

	return nil
}

// InstanceResetReconciler ensures every Instance claimed by a pool some
// TalosCluster references gets reset back to Talos maintenance mode before
// it's allowed to actually delete — see InstanceResetFinalizer's own doc.
type InstanceResetReconciler struct {
	Client        client.Client
	Bootstrapper  ClusterBootstrapper
	RetryInterval time.Duration
	ResetTimeout  time.Duration
	Logger        *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *InstanceResetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var inst v1alpha2.Instance

	err := r.Client.Get(ctx, req.NamespacedName, &inst)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get instance %q: %w", req.Name, err)
	}

	poolName, claimed := inst.Labels[v1alpha2.LabelClaimedBy]
	if !claimed {
		return r.reconcileUnclaimed(ctx, &inst)
	}

	cluster, role, found, err := findOwningCluster(ctx, r.Client, inst.Namespace, poolName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !found {
		// Claimed, but by a pool no TalosCluster references (yet, or ever)
		// — nothing to reset on delete.
		if !inst.DeletionTimestamp.IsZero() {
			return r.removeFinalizer(ctx, &inst)
		}

		return ctrl.Result{}, nil
	}

	if !inst.DeletionTimestamp.IsZero() {
		return r.reconcileTeardown(ctx, &inst, cluster, role)
	}

	if controllerutil.AddFinalizer(&inst, InstanceResetFinalizer) {
		err = r.Client.Update(ctx, &inst)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer to instance %q: %w", inst.Name, err)
		}
	}

	return ctrl.Result{}, nil
}

// reconcileUnclaimed handles inst when it isn't claimed by any pool.
//
// If it's also being deleted, this reconciler has nothing left to reset:
// with no LabelClaimedBy there's no pool, and so no owning TalosCluster, to
// resolve a reset endpoint through (see resolveResetEndpoint) — even if inst
// still carries InstanceResetFinalizer from an earlier claim episode.
// Getting here at all with the finalizer still attached means
// instancepool.Reconciler's own reconcileTeardown released inst (stripping
// the label) in the same GC cascade that set inst's own deletionTimestamp —
// both Instance and InstancePool are owned directly by the same Zone (see
// zone/add.go) — racing ahead of this reconciler ever seeing inst still
// claimed and adding itself to the claimed branch's own teardown path
// above. Removing the finalizer unconditionally here is what lets deletion
// actually finish instead of leaving inst (and the finalizer) stuck
// forever, permanently unreachable by the claimed branch that would
// otherwise have removed it.
//
// Otherwise — not claimed and not being deleted — nothing this reconciler
// manages going forward; it'll be reconciled again once
// instancepool.Reconciler claims it. If it still carries
// Configured/Joined/Ready from a previous claim episode, though, those are
// now stale (see clearMemberConditions's own doc) — clear them.
func (r *InstanceResetReconciler) reconcileUnclaimed(
	ctx context.Context, inst *v1alpha2.Instance,
) (ctrl.Result, error) {
	if !inst.DeletionTimestamp.IsZero() {
		return r.removeFinalizer(ctx, inst)
	}

	clearMemberConditions(ctx, r.Client, r.Logger, inst)

	return ctrl.Result{}, nil
}

// findOwningCluster lists every TalosCluster in namespace and returns the
// first one whose control-plane or worker PoolRef names poolName, plus
// which role that was. TalosClusterMemberSpec/TalosClusterWorkerSpec's own
// PoolRef, like InstancePoolReference generally, is a same-namespace-only
// reference (see issue #63's architecture) — the same convention
// resolveMembers already relies on.
func findOwningCluster(
	ctx context.Context, kubeClient client.Client, namespace, poolName string,
) (*v1alpha2.TalosCluster, memberRole, bool, error) {
	var list v1alpha2.TalosClusterList

	err := kubeClient.List(ctx, &list, client.InNamespace(namespace))
	if err != nil {
		return nil, 0, false, fmt.Errorf("failed to list talos clusters in %q: %w", namespace, err)
	}

	for i := range list.Items {
		cluster := &list.Items[i]

		if cluster.Spec.ControlPlane.PoolRef.Name == poolName {
			return cluster, roleControlPlane, true, nil
		}

		for _, worker := range cluster.Spec.Workers {
			if worker.PoolRef.Name == poolName {
				return cluster, roleWorker, true, nil
			}
		}
	}

	return nil, 0, false, nil
}

// reconcileTeardown resets inst's own node (unless it was never actually
// configured — see resetInstance's own doc), then removes
// InstanceResetFinalizer. A Reset failure retries every RetryInterval until
// ResetTimeout has elapsed since inst's own DeletionTimestamp, after which
// this gives up and removes the finalizer anyway — mirrors
// zone.reconcileTeardown's identical bounded-retry rationale (issue #49's
// "not a finalizer that blocks deletion forever" requirement): hardware
// that's genuinely gone (pulled, powered off) must not block the Instance
// from ever deleting.
func (r *InstanceResetReconciler) reconcileTeardown(
	ctx context.Context, inst *v1alpha2.Instance, cluster *v1alpha2.TalosCluster, role memberRole,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(inst, InstanceResetFinalizer) {
		return ctrl.Result{}, nil
	}

	if !meta.IsStatusConditionTrue(inst.Status.Conditions, MemberConfiguredConditionType) {
		// Never actually configured (e.g. claimed but discovery/bootstrap
		// never got that far) — nothing installed on it to reset.
		return r.removeFinalizer(ctx, inst)
	}

	err := resetInstance(ctx, r.Client, r.Bootstrapper, inst, cluster, role)
	if err == nil {
		return r.removeFinalizer(ctx, inst)
	}

	if time.Since(inst.DeletionTimestamp.Time) > r.ResetTimeout {
		r.Logger.Warn(
			"giving up on resetting instance after exceeding the reset timeout — "+
				"node may still need manual reset before it can be safely rejoined elsewhere",
			"instance", inst.Name, "timeout", r.ResetTimeout, "error", err)

		return r.removeFinalizer(ctx, inst)
	}

	r.Logger.Warn("failed to reset instance, will retry", "instance", inst.Name, "error", err)

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}

// resetInstance issues the actual Reset RPC against inst's own node.
// endpoint (who the admin talosCfg dials to issue the RPC) is any other
// still-live control-plane member of cluster, or inst's own address when
// it's the only control-plane member left — the same "dial one, target
// each via WithNode" pattern the old ResetControlPlane used, just resolved
// per-Instance now instead of once for a whole batch. graceful is only ever
// true for a control-plane target with at least one other live
// control-plane member: Talos's own docs are explicit that a graceful
// reset (leave etcd first) isn't possible without quorum left to leave
// into (see talosBootstrapper.Reset's own doc for the etcd-level hazard
// this avoids); a worker target never runs etcd, so LeaveEtcd's own
// service check already no-ops regardless — graceful is always true there.
// A free function (not a method) since it's shared by two different
// reconcilers: InstanceResetReconciler's own reconcileTeardown above,
// which resets one Instance at a time as each is individually deleted, and
// Reconciler's own cluster-level teardown (see teardown.go), which calls
// this once per still-claimed member as a whole cluster is deleted.
func resetInstance(
	ctx context.Context, kubeClient client.Client, bootstrapper ClusterBootstrapper,
	inst *v1alpha2.Instance, cluster *v1alpha2.TalosCluster, role memberRole,
) error {
	if cluster.Status.SecretRef.Name == "" {
		return fmt.Errorf("%w: %q", errNoSecretsBundle, cluster.Name)
	}

	bundle, err := loadSecretsBundle(ctx, kubeClient, cluster.Status.SecretRef)
	if err != nil {
		return fmt.Errorf("failed to load secrets bundle for %q: %w", cluster.Name, err)
	}

	endpoint, graceful, err := resolveResetEndpoint(ctx, kubeClient, inst, cluster, role)
	if err != nil {
		return err
	}

	_, talosCfg, err := generateConfigs(bundle, cluster, endpoint)
	if err != nil {
		return fmt.Errorf("failed to regenerate talosconfig for %q: %w", cluster.Name, err)
	}

	err = bootstrapper.Reset(ctx, endpoint, dialAddress(*inst), talosCfg, graceful)
	if err != nil {
		return fmt.Errorf("failed to reset %q: %w", inst.Name, err)
	}

	return nil
}

// resolveResetEndpoint picks who resetInstance's admin talosCfg dials to
// issue inst's own Reset RPC (endpoint), and whether that Reset can safely
// be graceful — see resetInstance's own doc for both. endpoint is any
// other still-live control-plane member of cluster, or inst's own address
// when it's the only control-plane member left; a worker with no
// reachable control-plane member at all has nothing to authenticate
// through, which is reported as errNoControlPlaneEndpoint.
func resolveResetEndpoint(
	ctx context.Context, kubeClient client.Client, inst *v1alpha2.Instance, cluster *v1alpha2.TalosCluster,
	role memberRole,
) (string, bool, error) {
	controlPlaneMembers, err := resolveMembers(ctx, kubeClient, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return "", false, err
	}

	var otherControlPlaneMembers []v1alpha2.Instance

	for _, member := range controlPlaneMembers {
		if member.Name != inst.Name {
			otherControlPlaneMembers = append(otherControlPlaneMembers, member)
		}
	}

	if len(otherControlPlaneMembers) > 0 {
		return dialAddress(otherControlPlaneMembers[0]), true, nil
	}

	if role == roleWorker {
		return "", false, fmt.Errorf("%w: %q", errNoControlPlaneEndpoint, cluster.Name)
	}

	return dialAddress(*inst), false, nil
}

// removeFinalizer removes InstanceResetFinalizer from inst and persists
// that.
func (r *InstanceResetReconciler) removeFinalizer(ctx context.Context, inst *v1alpha2.Instance) (ctrl.Result, error) {
	if !controllerutil.RemoveFinalizer(inst, InstanceResetFinalizer) {
		return ctrl.Result{}, nil
	}

	err := r.Client.Update(ctx, inst)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from instance %q: %w", inst.Name, err)
	}

	return ctrl.Result{}, nil
}
