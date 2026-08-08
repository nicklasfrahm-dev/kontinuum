// Package taloscluster implements TalosCluster's bootstrap reconciler —
// see issue #24's architecture decision 3/5. It resolves a cluster's
// control-plane and worker InstancePools' claimed, Discovered members,
// generates and applies Talos machine configs, bootstraps etcd, waits for
// cluster health, and seeds/aggregates its addons (see
// pkg/domain/addon, which owns actually installing and health-probing
// them) — control-plane-first: worker pools are only touched once the
// control plane reports healthy. talosclusters.kontinuum.sh's CRD is
// already ensured by pkg/domain/instance.EnsureCRDs — no separate ensure
// step lives here.
package taloscluster

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

const (
	// ControlPlaneReadyConditionType is set true once the control plane
	// (etcd bootstrap + a passing ClusterHealthCheck) is healthy.
	ControlPlaneReadyConditionType = "ControlPlaneReady"
	// BootstrappedConditionType is set true once etcd bootstrap has
	// succeeded at least once — see TalosClusterSpec's own doc comment
	// ("ControlPlaneReady, Bootstrapped, Ready").
	BootstrappedConditionType = "Bootstrapped"
	// ReadyConditionType is set true once every enabled Addon referencing
	// this cluster is installed and healthy — no addon gets special
	// treatment, see reconcileAddons.
	ReadyConditionType = "Ready"

	// addonReadyConditionType is the condition type Addon's own
	// Reconciler sets — must match addon package's own
	// addonReadyConditionType constant.
	addonReadyConditionType = "Ready"

	reasonWaitingForInstances = "WaitingForInstances"
	reasonBootstrapping       = "Bootstrapping"
	reasonHealthy             = "Healthy"
	reasonAddonsInstalled     = "AddonsInstalled"
	reasonAddonInstallFailed  = "AddonInstallFailed"
	reasonAddonNotHealthy     = "AddonNotHealthy"

	defaultHealthCheckTimeout = 10 * time.Second
	defaultRetryInterval      = 15 * time.Second
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Bootstrapper drives Talos config-apply/bootstrap/health/kubeconfig.
	// Defaults to NewTalosBootstrapper(cfg.Logger) when nil.
	Bootstrapper ClusterBootstrapper
	// HealthCheckTimeout bounds each control-plane health-check attempt.
	// Defaults to ten seconds when zero.
	HealthCheckTimeout time.Duration
	// RetryInterval is how long Reconcile waits before retrying a step
	// that hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
}

// Controller wires the TalosCluster bootstrap reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting Bootstrapper,
// HealthCheckTimeout, and RetryInterval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.Bootstrapper == nil {
		cfg.Bootstrapper = NewTalosBootstrapper(cfg.Logger)
	}

	if cfg.HealthCheckTimeout == 0 {
		cfg.HealthCheckTimeout = defaultHealthCheckTimeout
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the TalosCluster reconciler on mgr. The
// talosclusters.kontinuum.sh CRD itself is ensured separately, via
// instance.EnsureCRDs registered as a libkapi.WithPostStartHook (see
// pkg/cli/serve.go) — not here, for the same reason
// instance.Controller.SetupWithManager's own doc gives. Addon's own
// reconciler is registered separately too, by pkg/domain/addon's own
// Controller.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:             mgr.GetClient(),
		Bootstrapper:       c.Config.Bootstrapper,
		HealthCheckTimeout: c.Config.HealthCheckTimeout,
		RetryInterval:      c.Config.RetryInterval,
		Logger:             c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).For(&v1alpha2.TalosCluster{}).Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register taloscluster controller: %w", err)
	}

	return nil
}

// Reconciler bootstraps a real Talos Kubernetes cluster from a
// TalosCluster's control-plane and worker InstancePools' claimed,
// Discovered members — see issue #24's architecture decision 3/5:
// control-plane-first, workers only reconciled once the control plane is
// healthy.
type Reconciler struct {
	Client             client.Client
	Bootstrapper       ClusterBootstrapper
	HealthCheckTimeout time.Duration
	RetryInterval      time.Duration
	Logger             *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, req.NamespacedName, &cluster)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get talos cluster %q: %w", req.Name, err)
	}

	bundle, err := ensureSecretsBundle(ctx, r.Client, &cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to ensure secrets bundle for %q: %w", cluster.Name, err)
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, ControlPlaneReadyConditionType) {
		return r.reconcileControlPlane(ctx, &cluster, bundle)
	}

	err = r.reconcileWorkers(ctx, &cluster, bundle)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, ReadyConditionType) {
		return r.reconcileAddons(ctx, &cluster)
	}

	return ctrl.Result{}, nil
}

// reconcileControlPlane resolves the control-plane pool's members,
// generates and applies their machine config, bootstraps etcd, and waits
// for cluster health — see this package's own doc for the full sequencing.
// Every step past config generation is best-effort/idempotent: a member
// already past maintenance mode is expected to fail ApplyConfiguration,
// and a Bootstrap call against an already-bootstrapped cluster is expected
// to fail too — both are logged, not fatal, since HealthCheck is the real
// convergence gate.
func (r *Reconciler) reconcileControlPlane(
	ctx context.Context, cluster *v1alpha2.TalosCluster, bundle *talossecrets.Bundle,
) (ctrl.Result, error) {
	members, err := resolveMembers(ctx, r.Client, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(members) == 0 {
		return r.setControlPlaneCondition(ctx, cluster, metav1.ConditionFalse, reasonWaitingForInstances,
			"no discovered control-plane instances claimed yet")
	}

	controlPlaneAddr := dialAddress(members[0])

	cpBytes, _, talosCfg, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to generate control plane config for %q: %w", cluster.Name, err)
	}

	controlPlaneNodes := r.applyControlPlaneConfig(ctx, cluster, members, cpBytes)

	return r.bootstrapAndCheckHealth(ctx, cluster, controlPlaneAddr, controlPlaneNodes, members, talosCfg)
}

// applyControlPlaneConfig best-effort applies cpBytes to every member —
// see reconcileControlPlane's own doc for why a failure here is logged,
// not fatal. Returns every member's dial address, for HealthCheck's
// controlPlaneNodes argument.
func (r *Reconciler) applyControlPlaneConfig(
	ctx context.Context, cluster *v1alpha2.TalosCluster, members []v1alpha2.Instance, cpBytes []byte,
) []string {
	controlPlaneNodes := make([]string, 0, len(members))

	for _, member := range members {
		addr := dialAddress(member)
		controlPlaneNodes = append(controlPlaneNodes, addr)

		err := r.Bootstrapper.ApplyConfiguration(ctx, addr, cpBytes)
		if err != nil {
			r.Logger.Warn("failed to apply control plane configuration, node may already be past maintenance mode",
				"cluster", cluster.Name, "address", addr, "error", err)
		}
	}

	return controlPlaneNodes
}

// bootstrapAndCheckHealth triggers etcd bootstrap, fetches and stores the
// kubeconfig, seeds/aggregates addons (see reconcileAddons — this can't
// wait for the health check below: that check waits on CoreDNS reporting
// ready, which itself needs a working pod network, so installing Cilium
// only once the cluster already reports "healthy" would deadlock — see
// generateConfigs' own doc for why Talos's default flannel CNI is
// disabled instead of racing it against Cilium), then waits for cluster
// health and marks ControlPlaneReady/Bootstrapped true. Addon health
// (Ready) and control-plane health (ControlPlaneReady) are deliberately
// independent conditions — HealthCheck itself already can't pass without
// Cilium already applied, so nothing is lost by not also gating
// ControlPlaneReady on Ready in Go.
func (r *Reconciler) bootstrapAndCheckHealth(
	ctx context.Context, cluster *v1alpha2.TalosCluster, controlPlaneAddr string, controlPlaneNodes []string,
	members []v1alpha2.Instance, talosCfg *clientconfig.Config,
) (ctrl.Result, error) {
	err := r.Bootstrapper.Bootstrap(ctx, controlPlaneAddr, talosCfg)
	if err != nil {
		r.Logger.Warn("bootstrap call failed, cluster may already be bootstrapped",
			"cluster", cluster.Name, "address", controlPlaneAddr, "error", err)
	}

	kubeconfig, err := r.Bootstrapper.Kubeconfig(ctx, controlPlaneAddr, talosCfg)
	if err != nil {
		r.Logger.Warn("kubeconfig not yet available, control plane still starting",
			"cluster", cluster.Name, "error", err)

		return r.setControlPlaneCondition(ctx, cluster, metav1.ConditionFalse, reasonBootstrapping,
			"waiting for the control plane's apiserver to become reachable")
	}

	err = storeKubeconfig(ctx, r.Client, cluster, kubeconfig)
	if err != nil {
		return ctrl.Result{}, err
	}

	addonsResult, err := r.reconcileAddons(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	err = r.Bootstrapper.HealthCheck(ctx, controlPlaneAddr, talosCfg, controlPlaneNodes, r.HealthCheckTimeout)
	if err != nil {
		r.Logger.Warn("control plane not yet healthy", "cluster", cluster.Name, "error", err)

		return r.setControlPlaneCondition(ctx, cluster, metav1.ConditionFalse, reasonBootstrapping,
			"waiting for control plane to become healthy")
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: BootstrappedConditionType, Status: metav1.ConditionTrue, Reason: reasonHealthy,
		Message: "cluster bootstrapped",
	})

	// HealthCheck passing already proves every member in controlPlaneNodes
	// is up and reachable with its real, post-config mTLS identity — the
	// one precondition instance.Discoverer's own maintenance-mode Version
	// call can no longer rely on (see its doc).
	r.recordTalosVersions(ctx, cluster, members, controlPlaneAddr, talosCfg)

	controlPlaneResult, err := r.setControlPlaneCondition(ctx, cluster, metav1.ConditionTrue, reasonHealthy,
		"control plane is healthy")
	if err != nil {
		return controlPlaneResult, err
	}

	// ControlPlaneReady and Ready are independent conditions, each already
	// persisted by its own Status().Update() above — but if addons aren't
	// healthy yet, that shouldn't go unattended just because
	// ControlPlaneReady itself needs no requeue: hand back whichever of
	// the two wants to be checked again sooner, rather than silently
	// dropping addonsResult's own requeue signal.
	if addonsResult.RequeueAfter > controlPlaneResult.RequeueAfter {
		return addonsResult, nil
	}

	return controlPlaneResult, nil
}

// reconcileAddons seeds cluster's two built-in addons (see
// addon.EnsureBuiltinSeeds) and aggregates Ready across every Addon
// referencing it, built-in or user-created custom addon alike — no
// special treatment. Installation/health-probing is pkg/domain/addon's
// own job; a just-created or not-yet-reconciled Addon has no Ready
// condition yet, which counts as pending, not failed. Called both while
// the control plane is still bootstrapping (see bootstrapAndCheckHealth's
// own doc) and afterward, from Reconcile, to keep converging (e.g. a new
// custom Addon a user created, or an addon pod that later crashes).
func (r *Reconciler) reconcileAddons(ctx context.Context, cluster *v1alpha2.TalosCluster) (ctrl.Result, error) {
	err := addon.EnsureBuiltinSeeds(ctx, r.Client, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to seed built-in addons for %q: %w", cluster.Name, err)
	}

	addons, err := addon.ListForCluster(ctx, r.Client, cluster.Name)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list addons for %q: %w", cluster.Name, err)
	}

	var unhealthy []string

	for _, candidate := range addons.Items {
		if !addon.Enabled(candidate.Spec) {
			continue
		}

		cond := meta.FindStatusCondition(candidate.Status.Conditions, addonReadyConditionType)
		if cond == nil || cond.Status != metav1.ConditionTrue {
			reason := "Pending"
			if cond != nil {
				reason = cond.Reason
			}

			unhealthy = append(unhealthy, addon.ReleaseName(&candidate)+": "+reason)
		}
	}

	if len(unhealthy) > 0 {
		return r.setReadyCondition(ctx, cluster, metav1.ConditionFalse, reasonAddonNotHealthy,
			"waiting for addons: "+strings.Join(unhealthy, "; "))
	}

	return r.setReadyCondition(ctx, cluster, metav1.ConditionTrue, reasonAddonsInstalled,
		"all addons installed and healthy")
}

// reconcileWorkers applies each worker pool's machine config to its
// claimed, Discovered members — only ever called once ControlPlaneReady is
// true (see Reconcile). No Bootstrap/HealthCheck call is needed here: a
// worker just joins once its config, carrying the control plane's endpoint
// and cluster CA, is applied — same best-effort/idempotent rationale as
// reconcileControlPlane's own ApplyConfiguration calls.
func (r *Reconciler) reconcileWorkers(
	ctx context.Context, cluster *v1alpha2.TalosCluster, bundle *talossecrets.Bundle,
) error {
	if len(cluster.Spec.Workers) == 0 {
		return nil
	}

	controlPlaneMembers, err := resolveMembers(ctx, r.Client, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return err
	}

	if len(controlPlaneMembers) == 0 {
		return nil
	}

	controlPlaneAddr := dialAddress(controlPlaneMembers[0])

	// Every worker pool shares the same cluster-wide talos/kubernetes
	// version (see KubernetesSpec's own doc), so the machine config is
	// generated once here and reused for every pool below, rather than
	// per-pool as when versions could differ per worker.
	_, workerBytes, talosCfg, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return fmt.Errorf("failed to generate worker config for %q: %w", cluster.Name, err)
	}

	for _, worker := range cluster.Spec.Workers {
		err := r.reconcileWorkerPool(ctx, cluster, worker, workerBytes, controlPlaneAddr, talosCfg)
		if err != nil {
			return err
		}
	}

	return nil
}

// reconcileWorkerPool applies workerBytes to every member claimed by
// worker.PoolRef, then best-effort records their real Talos version — see
// recordTalosVersions' own doc. A worker just joined by config-apply may
// still be mid-reboot, so this frequently fails on its first attempt; that's
// fine, it's retried on every future reconcile pass until Ready (see
// Reconcile), same tolerance recordTalosVersions already documents for
// control-plane members.
func (r *Reconciler) reconcileWorkerPool(
	ctx context.Context, cluster *v1alpha2.TalosCluster, worker v1alpha2.TalosClusterWorkerSpec, workerBytes []byte,
	controlPlaneAddr string, talosCfg *clientconfig.Config,
) error {
	members, err := resolveMembers(ctx, r.Client, worker.PoolRef)
	if err != nil {
		return err
	}

	for _, member := range members {
		addr := dialAddress(member)

		applyErr := r.Bootstrapper.ApplyConfiguration(ctx, addr, workerBytes)
		if applyErr != nil {
			r.Logger.Warn("failed to apply worker configuration, node may already be past maintenance mode",
				"cluster", cluster.Name, "pool", worker.PoolRef.Name, "address", addr, "error", applyErr)
		}
	}

	r.recordTalosVersions(ctx, cluster, members, controlPlaneAddr, talosCfg)

	return nil
}

// recordTalosVersions best-effort fetches and persists each of members' real
// Talos version, skipping any that already have one recorded. Once a
// maintenance-mode member has had its config applied, its own
// maintenance-mode Version RPC becomes permanently unreachable — its apid
// has moved on to the real, non-maintenance-mode server — so dialAddr (any
// already-configured, reachable cluster member) and talosCfg's admin
// identity are used instead, targeting each member individually via
// client.WithNode (see ClusterBootstrapper.Version's own doc). Failures are
// logged, not fatal or returned: a member that hasn't finished rebooting
// into its new config yet just tries again on the next reconcile.
func (r *Reconciler) recordTalosVersions(
	ctx context.Context, cluster *v1alpha2.TalosCluster, members []v1alpha2.Instance, dialAddr string,
	talosCfg *clientconfig.Config,
) {
	for _, member := range members {
		if member.Status.Talos.Version != "" {
			continue
		}

		version, err := r.Bootstrapper.Version(ctx, dialAddr, dialAddress(member), talosCfg)
		if err != nil {
			r.Logger.Warn("failed to fetch talos version for member, may still be rebooting into its new configuration",
				"cluster", cluster.Name, "instance", member.Name, "error", err)

			continue
		}

		member.Status.Talos.Version = version

		err = r.Client.Status().Update(ctx, &member)
		if err != nil {
			r.Logger.Warn("failed to persist discovered talos version",
				"cluster", cluster.Name, "instance", member.Name, "error", err)
		}
	}
}

// setControlPlaneCondition sets ControlPlaneReadyConditionType and
// persists cluster's status. A false status requeues after RetryInterval
// so the next attempt tries again; a true status doesn't — Reconcile's
// controller-runtime watch re-triggers on the Update this Status().Update
// call itself produces, continuing straight into worker/addon
// reconciliation.
func (r *Reconciler) setControlPlaneCondition(
	ctx context.Context, cluster *v1alpha2.TalosCluster, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: ControlPlaneReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	return r.persistStatus(ctx, cluster, status)
}

// setReadyCondition sets ReadyConditionType and persists cluster's status
// — see setControlPlaneCondition's identical requeue rationale.
func (r *Reconciler) setReadyCondition(
	ctx context.Context, cluster *v1alpha2.TalosCluster, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: ReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	return r.persistStatus(ctx, cluster, status)
}

// persistStatus writes cluster's status and decides whether to requeue.
func (r *Reconciler) persistStatus(
	ctx context.Context, cluster *v1alpha2.TalosCluster, status metav1.ConditionStatus,
) (ctrl.Result, error) {
	err := r.Client.Status().Update(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update talos cluster %q status: %w", cluster.Name, err)
	}

	if status == metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}
