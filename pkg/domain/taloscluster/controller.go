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
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	reasonBootstrapped        = "Bootstrapped"
	reasonHealthy             = "Healthy"
	reasonAddonsInstalled     = "AddonsInstalled"
	reasonAddonInstallFailed  = "AddonInstallFailed"
	reasonAddonNotHealthy     = "AddonNotHealthy"

	// defaultHealthCheckTimeout bounds each ClusterHealthCheck attempt —
	// both the client-side context and the WaitTimeout sent to the server
	// (see talosBootstrapper.HealthCheck's own doc). A fresh control plane's
	// etcd/kubelet/static-pod/CoreDNS/kube-proxy checks realistically take
	// on the order of a minute or more to first converge on real hardware —
	// ten seconds (this value before) could never observe a full pass and
	// made HealthCheck fail with DeadlineExceeded on every single attempt,
	// forever, regardless of actual cluster progress.
	defaultHealthCheckTimeout = 60 * time.Second
	defaultRetryInterval      = 15 * time.Second
	// defaultHealthCheckInterval is how often recheckControlPlaneHealth
	// re-probes an already-converged control plane — see its own doc for
	// why this exists at all. Five minutes trades off freshness against
	// not hammering every control-plane node with a full
	// etcd/kubelet/static-pod/CoreDNS/kube-proxy check on a tight loop.
	defaultHealthCheckInterval = 5 * time.Minute
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Bootstrapper drives Talos config-apply/bootstrap/health/kubeconfig.
	// Defaults to NewTalosBootstrapper(cfg.Logger) when nil.
	Bootstrapper ClusterBootstrapper
	// HealthCheckTimeout bounds each control-plane health-check attempt.
	// Defaults to one minute when zero.
	HealthCheckTimeout time.Duration
	// RetryInterval is how long Reconcile waits before retrying a step
	// that hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
	// HealthCheckInterval is how long Reconcile waits between re-probes of
	// an already-converged control plane's health — see
	// recheckControlPlaneHealth's own doc. Defaults to five minutes when
	// zero.
	HealthCheckInterval time.Duration
}

// Controller wires the TalosCluster bootstrap reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting Bootstrapper,
// HealthCheckTimeout, RetryInterval, and HealthCheckInterval when left zero.
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

	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = defaultHealthCheckInterval
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
		Client:              mgr.GetClient(),
		Bootstrapper:        c.Config.Bootstrapper,
		HealthCheckTimeout:  c.Config.HealthCheckTimeout,
		RetryInterval:       c.Config.RetryInterval,
		HealthCheckInterval: c.Config.HealthCheckInterval,
		Logger:              c.Config.Logger,
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
	Client              client.Client
	Bootstrapper        ClusterBootstrapper
	HealthCheckTimeout  time.Duration
	RetryInterval       time.Duration
	HealthCheckInterval time.Duration
	Logger              *slog.Logger
}

// Reconcile implements reconcile.Reconciler. Once the cluster is fully
// converged (ControlPlaneReady and Ready both true), it falls through to
// recheckControlPlaneHealth rather than returning a bare ctrl.Result{} —
// see that method's own doc for why a one-shot health check would
// otherwise leave every control-plane member's MemberReadyConditionType
// stale forever.
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

	return r.recheckControlPlaneHealth(ctx, &cluster, bundle)
}

// reconcileControlPlane resolves the control-plane pool's members,
// generates and applies their machine config, bootstraps etcd, and waits
// for cluster health — see this package's own doc for the full sequencing.
// ApplyConfiguration is best-effort/idempotent: a member already past
// maintenance mode is expected to fail it, logged, not fatal, since
// HealthCheck is the real convergence gate. Bootstrap is different — see
// ensureBootstrapped's own doc — it's only ever attempted until it first
// succeeds, not on every reconcile.
func (r *Reconciler) reconcileControlPlane(
	ctx context.Context, cluster *v1alpha2.TalosCluster, bundle *talossecrets.Bundle,
) (ctrl.Result, error) {
	members, err := resolveMembers(ctx, r.Client, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(members) == 0 {
		return r.setControlPlaneCondition(ctx, cluster, metav1.ConditionFalse, reasonWaitingForInstances,
			"no discovered control-plane instances claimed yet")
	}

	controlPlaneAddr := dialAddress(members[0])

	input, talosCfg, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to generate control plane config for %q: %w", cluster.Name, err)
	}

	controlPlaneNodes := r.applyControlPlaneConfig(ctx, cluster, members, input)

	return r.bootstrapAndCheckHealth(ctx, cluster, controlPlaneAddr, controlPlaneNodes, members, talosCfg)
}

// applyControlPlaneConfig best-effort generates (with hostname set to each
// member's own Instance name — see configBytes' own doc) and applies a
// machine config to every member — see reconcileControlPlane's own doc for
// why a failure here is logged, not fatal. Returns every member's dial
// address, for HealthCheck's controlPlaneNodes argument. Mutates members in
// place (by index, not a range copy) so a Configured member's bumped
// ResourceVersion is visible to bootstrapAndCheckHealth's later
// recordTalosVersions call on the same slice — updating from a stale copy
// there would otherwise conflict.
func (r *Reconciler) applyControlPlaneConfig(
	ctx context.Context, cluster *v1alpha2.TalosCluster, members []v1alpha2.Instance, input *generate.Input,
) []string {
	controlPlaneNodes := make([]string, 0, len(members))

	for i := range members {
		member := &members[i]
		addr := dialAddress(*member)
		controlPlaneNodes = append(controlPlaneNodes, addr)

		cpBytes, err := configBytes(input, machine.TypeControlPlane, member.Name)
		if err != nil {
			r.Logger.Warn("failed to generate control plane configuration",
				"cluster", cluster.Name, "instance", member.Name, "address", addr, "error", err)

			continue
		}

		err = r.Bootstrapper.ApplyConfiguration(ctx, addr, cpBytes)
		if err != nil {
			r.Logger.Warn("failed to apply control plane configuration, node may already be past maintenance mode",
				"cluster", cluster.Name, "address", addr, "error", err)

			continue
		}

		markMemberCondition(ctx, r.Client, r.Logger, member, MemberConfiguredConditionType, reasonMemberConfigured,
			"talos machine configuration applied")
	}

	return controlPlaneNodes
}

// ensureBootstrapped triggers etcd bootstrap on controlPlaneAddr, unless
// cluster's own Bootstrapped condition is already true — once bootstrap has
// succeeded, it never needs to run again, so unlike ApplyConfiguration
// (see reconcileControlPlane's own doc) this isn't just tolerated as a
// best-effort no-op on every reconcile; it's actively skipped, avoiding
// both the repeated RPC and the log noise from its expected failure mode.
// That failure mode is codes.AlreadyExists — Talos itself returns that
// when etcd's data directory is already populated — which is treated the
// same as success here, since it proves bootstrap already happened even
// if this TalosCluster's own status doesn't reflect that yet (e.g. after
// a controller restart mid-convergence, before ControlPlaneReady ever
// went true).
func (r *Reconciler) ensureBootstrapped(
	ctx context.Context, cluster *v1alpha2.TalosCluster, controlPlaneAddr string, talosCfg *clientconfig.Config,
) {
	if meta.IsStatusConditionTrue(cluster.Status.Conditions, BootstrappedConditionType) {
		return
	}

	err := r.Bootstrapper.Bootstrap(ctx, controlPlaneAddr, talosCfg)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		r.Logger.Warn("bootstrap call failed, cluster may already be bootstrapped",
			"cluster", cluster.Name, "address", controlPlaneAddr, "error", err)

		return
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: BootstrappedConditionType, Status: metav1.ConditionTrue, Reason: reasonBootstrapped,
		Message: "etcd bootstrap completed",
	})
}

// bootstrapAndCheckHealth ensures etcd is bootstrapped (see
// ensureBootstrapped), fetches and stores the kubeconfig, seeds/aggregates
// addons (see reconcileAddons — this can't wait for the health check
// below: that check waits on CoreDNS reporting ready, which itself needs a
// working pod network, so installing Cilium only once the cluster already
// reports "healthy" would deadlock — see generateConfigs' own doc for why
// Talos's default flannel CNI is disabled instead of racing it against
// Cilium), then waits for cluster health and marks ControlPlaneReady true.
// Addon health (Ready) and control-plane health (ControlPlaneReady) are
// deliberately independent conditions — HealthCheck itself already can't
// pass without Cilium already applied, so nothing is lost by not also
// gating ControlPlaneReady on Ready in Go.
func (r *Reconciler) bootstrapAndCheckHealth(
	ctx context.Context, cluster *v1alpha2.TalosCluster, controlPlaneAddr string, controlPlaneNodes []string,
	members []v1alpha2.Instance, talosCfg *clientconfig.Config,
) (ctrl.Result, error) {
	r.ensureBootstrapped(ctx, cluster, controlPlaneAddr, talosCfg)

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

	// HealthCheck passing already proves every member in controlPlaneNodes
	// is up and reachable with its real, post-config mTLS identity — the
	// one precondition instance.Discoverer's own maintenance-mode Version
	// call can no longer rely on (see its doc). markReady is true here
	// because that same HealthCheck call already verified this exact batch
	// of members healthy — see MemberReadyConditionType's own doc.
	r.recordTalosVersions(ctx, cluster, members, controlPlaneAddr, talosCfg, true)

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

// recheckControlPlaneHealth keeps each control-plane member's
// MemberReadyConditionType honest after the cluster has already converged
// once — without this, HealthCheck would only ever run while
// ControlPlaneReadyConditionType was still false (see reconcileControlPlane
// and bootstrapAndCheckHealth), then never again: nothing else in this
// reconciler runs on a timer, only in reaction to a watch event on the
// TalosCluster object itself, so a node going unhealthy afterward would
// leave a stale Ready=True forever (see issue #62's own follow-up
// discussion). Deliberately never re-applies config and never flips
// ControlPlaneReadyConditionType itself back to false: doing so would
// re-enter reconcileControlPlane, which reapplies (REBOOT-mode) config to
// every control-plane member — exactly the wrong reaction to what might be
// a single flaky probe. This is a read-only recheck of MemberReadyConditionType
// only, returned via ctrl.Result.RequeueAfter — the same
// no-event-will-tell-me-this-changed mechanism reconcileControlPlane's own
// RetryInterval already relies on, just on a longer, steady-state cadence
// (HealthCheckInterval) once there's nothing left to bootstrap.
func (r *Reconciler) recheckControlPlaneHealth(
	ctx context.Context, cluster *v1alpha2.TalosCluster, bundle *talossecrets.Bundle,
) (ctrl.Result, error) {
	members, err := resolveMembers(ctx, r.Client, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(members) == 0 {
		return ctrl.Result{RequeueAfter: r.HealthCheckInterval}, nil
	}

	controlPlaneAddr := dialAddress(members[0])

	_, talosCfg, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to generate control plane config for %q: %w", cluster.Name, err)
	}

	controlPlaneNodes := make([]string, 0, len(members))
	for _, member := range members {
		controlPlaneNodes = append(controlPlaneNodes, dialAddress(member))
	}

	status, reason, message := metav1.ConditionTrue, reasonMemberHealthy,
		"control plane cluster health check passed with this node included"

	healthErr := r.Bootstrapper.HealthCheck(ctx, controlPlaneAddr, talosCfg, controlPlaneNodes, r.HealthCheckTimeout)
	if healthErr != nil {
		r.Logger.Warn("periodic control plane health recheck failed", "cluster", cluster.Name, "error", healthErr)

		status, reason = metav1.ConditionFalse, reasonMemberUnhealthy
		message = "periodic control plane health recheck failed: " + healthErr.Error()
	}

	for i := range members {
		setMemberCondition(ctx, r.Client, r.Logger, &members[i], MemberReadyConditionType, status, reason, message)
	}

	if healthErr != nil {
		// Same rationale as every other HealthCheck failure branch in this
		// file (e.g. bootstrapAndCheckHealth's own): an unhealthy control
		// plane is an expected, handled outcome — already logged and
		// reflected in MemberReadyConditionType above — not a
		// Reconcile-machinery failure worth returning as an error.
		return ctrl.Result{RequeueAfter: r.RetryInterval}, nil //nolint:nilerr
	}

	return ctrl.Result{RequeueAfter: r.HealthCheckInterval}, nil
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

	controlPlaneMembers, err := resolveMembers(ctx, r.Client, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return err
	}

	if len(controlPlaneMembers) == 0 {
		return nil
	}

	controlPlaneAddr := dialAddress(controlPlaneMembers[0])

	// Every worker pool shares the same cluster-wide talos/kubernetes
	// version (see KubernetesSpec's own doc), so the config generator input
	// is built once here and reused for every pool below, rather than
	// per-pool as when versions could differ per worker — each member still
	// gets its own config bytes generated from it, though, since hostname
	// (see configBytes' own doc) is per-member, not per-pool.
	input, talosCfg, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return fmt.Errorf("failed to generate worker config for %q: %w", cluster.Name, err)
	}

	for _, worker := range cluster.Spec.Workers {
		err := r.reconcileWorkerPool(ctx, cluster, worker, input, controlPlaneAddr, talosCfg)
		if err != nil {
			return err
		}
	}

	return nil
}

// reconcileWorkerPool generates (with hostname set to each member's own
// Instance name — see configBytes' own doc) and applies a machine config to
// every member claimed by worker.PoolRef, then best-effort records their
// real Talos version — see recordTalosVersions' own doc. A worker just
// joined by config-apply may still be mid-reboot, so this frequently fails
// on its first attempt; that's fine, it's retried on every future reconcile
// pass until Ready (see Reconcile), same tolerance recordTalosVersions
// already documents for control-plane members.
func (r *Reconciler) reconcileWorkerPool(
	ctx context.Context, cluster *v1alpha2.TalosCluster, worker v1alpha2.TalosClusterWorkerSpec, input *generate.Input,
	controlPlaneAddr string, talosCfg *clientconfig.Config,
) error {
	members, err := resolveMembers(ctx, r.Client, cluster.Namespace, worker.PoolRef)
	if err != nil {
		return err
	}

	for i := range members {
		member := &members[i]
		addr := dialAddress(*member)

		workerBytes, genErr := configBytes(input, machine.TypeWorker, member.Name)
		if genErr != nil {
			r.Logger.Warn("failed to generate worker configuration",
				"cluster", cluster.Name, "pool", worker.PoolRef.Name, "instance", member.Name, "address", addr,
				"error", genErr)

			continue
		}

		applyErr := r.Bootstrapper.ApplyConfiguration(ctx, addr, workerBytes)
		if applyErr != nil {
			r.Logger.Warn("failed to apply worker configuration, node may already be past maintenance mode",
				"cluster", cluster.Name, "pool", worker.PoolRef.Name, "address", addr, "error", applyErr)

			continue
		}

		markMemberCondition(ctx, r.Client, r.Logger, member, MemberConfiguredConditionType, reasonMemberConfigured,
			"talos machine configuration applied")
	}

	// markReady is false here: unlike the control-plane batch (see
	// bootstrapAndCheckHealth), no HealthCheck has run against these
	// workers — recordTalosVersions' Version RPC succeeding only proves a
	// worker rebooted into its new identity, not that it's healthy (see
	// MemberReadyConditionType's own doc).
	r.recordTalosVersions(ctx, cluster, members, controlPlaneAddr, talosCfg, false)

	return nil
}

// recordTalosVersions best-effort fetches and persists each of members' real
// Talos version and MemberJoinedConditionType, skipping any already marked
// Joined — checked via that condition rather than Status.Talos.Version
// directly so a member upgraded from before this condition existed
// self-heals (re-fetches once, cheaply) instead of staying Joined-less
// forever. Once a maintenance-mode member has had its config applied, its
// own maintenance-mode Version RPC becomes permanently unreachable — its
// apid has moved on to the real, non-maintenance-mode server — so dialAddr
// (any already-configured, reachable cluster member) and talosCfg's admin
// identity are used instead, targeting each member individually via
// client.WithNode (see ClusterBootstrapper.Version's own doc). Failures are
// logged, not fatal or returned: a member that hasn't finished rebooting
// into its new config yet just tries again on the next reconcile. markReady
// additionally sets MemberReadyConditionType — see its own doc for why only
// callers that just health-checked this exact batch of members pass true.
func (r *Reconciler) recordTalosVersions(
	ctx context.Context, cluster *v1alpha2.TalosCluster, members []v1alpha2.Instance, dialAddr string,
	talosCfg *clientconfig.Config, markReady bool,
) {
	for i := range members {
		member := &members[i]

		if meta.IsStatusConditionTrue(member.Status.Conditions, MemberJoinedConditionType) {
			continue
		}

		version, err := r.Bootstrapper.Version(ctx, dialAddr, dialAddress(*member), talosCfg)
		if err != nil {
			r.Logger.Warn("failed to fetch talos version for member, may still be rebooting into its new configuration",
				"cluster", cluster.Name, "instance", member.Name, "error", err)

			continue
		}

		member.Status.Talos.Version = version

		meta.SetStatusCondition(&member.Status.Conditions, metav1.Condition{
			Type: MemberJoinedConditionType, Status: metav1.ConditionTrue, Reason: reasonMemberJoined,
			Message: "node answered a Version RPC with its real, post-config Talos identity",
		})

		if markReady {
			meta.SetStatusCondition(&member.Status.Conditions, metav1.Condition{
				Type: MemberReadyConditionType, Status: metav1.ConditionTrue, Reason: reasonMemberHealthy,
				Message: "control plane cluster health check passed with this node included",
			})
		}

		err = r.Client.Status().Update(ctx, member)
		if err != nil {
			r.Logger.Warn("failed to persist member talos version/conditions",
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
