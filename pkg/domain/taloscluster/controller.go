package taloscluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// ControlPlaneReadyConditionType is set true once the control plane
	// (etcd bootstrap + a passing ClusterHealthCheck) is healthy.
	ControlPlaneReadyConditionType = "ControlPlaneReady"
	// BootstrappedConditionType is set true once etcd bootstrap has
	// succeeded at least once — see TalosClusterSpec's own doc comment
	// ("ControlPlaneReady, Bootstrapped, Ready").
	BootstrappedConditionType = "Bootstrapped"
	// ReadyConditionType is set true once every enabled addon is installed
	// and healthy — no addon gets special ordering, see reconcileAddons.
	ReadyConditionType = "Ready"

	reasonWaitingForInstances = "WaitingForInstances"
	reasonBootstrapping       = "Bootstrapping"
	reasonHealthy             = "Healthy"
	reasonAddonsInstalled     = "AddonsInstalled"
	reasonAddonInstallFailed  = "AddonInstallFailed"
	reasonAddonNotHealthy     = "AddonNotHealthy"

	defaultHealthCheckTimeout = 10 * time.Second
	defaultRetryInterval      = 15 * time.Second

	// addonInstallTimeout bounds every Helm install/upgrade call on the
	// client side. Neither Cilium's nor cert-manager's chart fetch/apply
	// carries any deadline of its own — without wrapping ctx here, a
	// stalled chart download can block the reconcile that invoked it
	// indefinitely (see this package's own bootstrap.go rpcTimeout for the
	// identical rationale applied to the Talos RPCs). The install/upgrade
	// calls themselves are non-blocking (no Wait/WaitForJobs) — they only
	// apply manifests; whether the resulting pods are actually healthy is
	// checked separately, by PodProber, on a later reconcile — so this
	// timeout only needs to cover the apply itself, not a full rollout.
	addonInstallTimeout = 30 * time.Second

	// podProbeTimeout bounds each PodProber.NamespaceHealthy call.
	podProbeTimeout = 15 * time.Second
)

// errKubeconfigNotStored is a static sentinel — err113 flags a dynamically
// constructed errors.New/fmt.Errorf call without a wrapped static error.
var errKubeconfigNotStored = errors.New("secret has no stored kubeconfig yet")

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Bootstrapper drives Talos config-apply/bootstrap/health/kubeconfig.
	// Defaults to NewTalosBootstrapper(cfg.Logger) when nil.
	Bootstrapper ClusterBootstrapper
	// AddonInstaller installs every enabled addon (see reconcileAddons).
	// Defaults to NewHelmInstaller() when nil.
	AddonInstaller AddonInstaller
	// PodProber checks whether an installed addon's pods are actually
	// healthy — installRelease/upgradeRelease only apply manifests, they
	// don't wait for rollout. Defaults to NewPodProber() when nil.
	PodProber PodProber
	// HealthCheckTimeout bounds each control-plane health-check attempt.
	// Defaults to ten seconds when zero.
	HealthCheckTimeout time.Duration
	// RetryInterval is how long Reconcile waits before retrying a step
	// that hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
}

// Controller wires the TalosCluster bootstrap/addons reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting Bootstrapper,
// AddonInstaller, HealthCheckTimeout, and RetryInterval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.Bootstrapper == nil {
		cfg.Bootstrapper = NewTalosBootstrapper(cfg.Logger)
	}

	if cfg.AddonInstaller == nil {
		cfg.AddonInstaller = NewHelmInstaller()
	}

	if cfg.PodProber == nil {
		cfg.PodProber = NewPodProber()
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
// instance.Controller.SetupWithManager's own doc gives.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:             mgr.GetClient(),
		Bootstrapper:       c.Config.Bootstrapper,
		AddonInstaller:     c.Config.AddonInstaller,
		PodProber:          c.Config.PodProber,
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
	AddonInstaller     AddonInstaller
	PodProber          PodProber
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
		kubeconfig, err := r.loadKubeconfig(ctx, &cluster)
		if err != nil {
			return ctrl.Result{}, err
		}

		return r.reconcileAddons(ctx, &cluster, kubeconfig)
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

	return r.bootstrapAndCheckHealth(ctx, cluster, controlPlaneAddr, controlPlaneNodes, talosCfg)
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
// kubeconfig, installs every enabled addon in parallel (see
// reconcileAddons — this can't wait for the health check below: that
// check waits on CoreDNS reporting ready, which itself needs a working
// pod network, so installing Cilium only once the cluster already reports
// "healthy" would deadlock — see generateConfigs' own doc for why Talos's
// default flannel CNI is disabled instead of racing it against Cilium),
// then waits for cluster health and marks ControlPlaneReady/Bootstrapped
// true. Addon health (Ready) and control-plane health (ControlPlaneReady)
// are deliberately independent conditions — HealthCheck itself already
// can't pass without Cilium already applied, so nothing is lost by not
// also gating ControlPlaneReady on Ready in Go.
func (r *Reconciler) bootstrapAndCheckHealth(
	ctx context.Context, cluster *v1alpha2.TalosCluster, controlPlaneAddr string, controlPlaneNodes []string,
	talosCfg *clientconfig.Config,
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

	addonsResult, err := r.reconcileAddons(ctx, cluster, kubeconfig)
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

// reconcileAddons resolves and installs every enabled addon in parallel
// against kubeconfig, then gates ReadyConditionType on all of them being
// healthy — no addon gets special ordering. Called both while the control
// plane is still bootstrapping (see bootstrapAndCheckHealth's own doc)
// and afterward, from Reconcile, to keep converging (e.g. a user changing
// spec.addons[].values, or a pod that later crashes).
func (r *Reconciler) reconcileAddons(
	ctx context.Context, cluster *v1alpha2.TalosCluster, kubeconfig []byte,
) (ctrl.Result, error) {
	controlPlaneCount, err := r.controlPlaneCount(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	celCtx, err := celContext(cluster, controlPlaneCount)
	if err != nil {
		return ctrl.Result{}, err
	}

	requests, err := resolveAddons(cluster, celCtx)
	if err != nil {
		return r.setReadyCondition(ctx, cluster, metav1.ConditionFalse, reasonAddonInstallFailed, err.Error())
	}

	outcomes := make([]addonOutcome, len(requests))

	var waitGroup sync.WaitGroup

	for index, req := range requests {
		waitGroup.Add(1)

		go func(index int, req AddonInstallRequest) {
			defer waitGroup.Done()

			outcomes[index] = r.installAndProbeAddon(ctx, cluster, kubeconfig, req)
		}(index, req)
	}

	waitGroup.Wait()

	return r.applyAddonOutcomes(ctx, cluster, outcomes)
}

// addonOutcome is one addon's install+health-probe result — see
// installAndProbeAddon/applyAddonOutcomes.
type addonOutcome struct {
	name    string
	healthy bool
	reason  string // unhealthy reason, or install error text
	// installFailed distinguishes Install itself failing from a probe
	// simply reporting not-yet-healthy — see applyAddonOutcomes, which
	// picks reasonAddonInstallFailed over reasonAddonNotHealthy whenever
	// any outcome sets this.
	installFailed bool
	err           error // hard error — propagates as a real reconcile error
}

// installAndProbeAddon installs req, then probes its pods' health —
// installRelease/upgradeRelease only apply manifests, they don't wait for
// rollout, so probing is what actually confirms an addon is usable.
func (r *Reconciler) installAndProbeAddon(
	ctx context.Context, cluster *v1alpha2.TalosCluster, kubeconfig []byte, req AddonInstallRequest,
) addonOutcome {
	installCtx, cancel := context.WithTimeout(ctx, addonInstallTimeout)
	defer cancel()

	err := r.AddonInstaller.Install(installCtx, kubeconfig, req)
	if err != nil {
		r.Logger.Warn("failed to install addon", "cluster", cluster.Name, "addon", req.ReleaseName, "error", err)

		return addonOutcome{name: req.ReleaseName, reason: err.Error(), installFailed: true}
	}

	healthy, reason, err := r.probeAddonHealthy(ctx, cluster, kubeconfig, req.ReleaseName, req.Namespace)
	if err != nil {
		return addonOutcome{name: req.ReleaseName, err: err}
	}

	return addonOutcome{name: req.ReleaseName, healthy: healthy, reason: reason}
}

// applyAddonOutcomes aggregates every addon's outcome into one Ready
// condition — true only once every addon is healthy. An Install failure
// takes precedence over a merely-not-yet-healthy probe result when
// picking the aggregate reason, since it's the more actionable of the two.
func (r *Reconciler) applyAddonOutcomes(
	ctx context.Context, cluster *v1alpha2.TalosCluster, outcomes []addonOutcome,
) (ctrl.Result, error) {
	for _, outcome := range outcomes {
		if outcome.err != nil {
			return ctrl.Result{}, outcome.err
		}
	}

	var unhealthy []string

	installFailed := false

	for _, outcome := range outcomes {
		if !outcome.healthy {
			unhealthy = append(unhealthy, outcome.name+": "+outcome.reason)

			if outcome.installFailed {
				installFailed = true
			}
		}
	}

	if len(unhealthy) > 0 {
		reason := reasonAddonNotHealthy
		if installFailed {
			reason = reasonAddonInstallFailed
		}

		return r.setReadyCondition(ctx, cluster, metav1.ConditionFalse, reason,
			"waiting for addons: "+strings.Join(unhealthy, "; "))
	}

	return r.setReadyCondition(ctx, cluster, metav1.ConditionTrue, reasonAddonsInstalled,
		"all addons installed and healthy")
}

// controlPlaneCount reports how many members cluster's control-plane pool
// has currently claimed and discovered — a fact CEL rules in an embedded
// values/*.yaml may need (see celContext) that isn't itself a field
// TalosCluster stores.
func (r *Reconciler) controlPlaneCount(ctx context.Context, cluster *v1alpha2.TalosCluster) (int, error) {
	members, err := resolveMembers(ctx, r.Client, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return 0, err
	}

	return len(members), nil
}

// probeAddonHealthy checks namespace's pods via PodProber — installRelease/
// upgradeRelease only apply manifests, they don't wait for rollout, so
// this is the step that actually confirms an addon is usable, not just
// applied. addon names which chart this is checking (e.g. "cilium",
// "cert-manager"), for the log line only — callers build their own
// condition message from the returned reason.
func (r *Reconciler) probeAddonHealthy(
	ctx context.Context, cluster *v1alpha2.TalosCluster, kubeconfig []byte, addon, namespace string,
) (bool, string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, podProbeTimeout)
	defer cancel()

	healthy, reason, err := r.PodProber.NamespaceHealthy(probeCtx, kubeconfig, namespace)
	if err != nil {
		return false, "", fmt.Errorf("failed to probe pod health for %s in %q: %w", addon, namespace, err)
	}

	if !healthy {
		r.Logger.Warn("Addon pods not yet healthy", "cluster", cluster.Name, "addon", addon, "reason", reason)
	}

	return healthy, reason, nil
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
	_, workerBytes, _, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return fmt.Errorf("failed to generate worker config for %q: %w", cluster.Name, err)
	}

	for _, worker := range cluster.Spec.Workers {
		err := r.reconcileWorkerPool(ctx, cluster, worker, workerBytes)
		if err != nil {
			return err
		}
	}

	return nil
}

// reconcileWorkerPool applies workerBytes to every member claimed by
// worker.PoolRef.
func (r *Reconciler) reconcileWorkerPool(
	ctx context.Context, cluster *v1alpha2.TalosCluster, worker v1alpha2.TalosClusterWorkerSpec, workerBytes []byte,
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

	return nil
}

// loadKubeconfig fetches the kubeconfig reconcileControlPlane already
// stored on cluster.Status.SecretRef.
func (r *Reconciler) loadKubeconfig(ctx context.Context, cluster *v1alpha2.TalosCluster) ([]byte, error) {
	ref := cluster.Status.SecretRef

	var secret corev1.Secret

	err := r.Client.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secret to load kubeconfig: %w", ref.Name, err)
	}

	kubeconfig, ok := secret.Data[kubeconfigKey]
	if !ok {
		return nil, fmt.Errorf("%q %w", ref.Name, errKubeconfigNotStored)
	}

	return kubeconfig, nil
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
