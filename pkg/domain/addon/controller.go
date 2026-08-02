package addon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// addonReadyConditionType is set true once an addon is installed and
	// its pods report healthy.
	addonReadyConditionType = "Ready"

	// defaultRetryInterval is how long Reconcile waits before retrying a
	// step that hasn't converged yet.
	defaultRetryInterval = 15 * time.Second

	// addonInstallTimeout bounds every Helm install/upgrade call on the
	// client side — mirrors pkg/domain/taloscluster's own identical
	// rationale (a stalled chart fetch shouldn't block a reconcile
	// indefinitely). 90s, not 30s: with three addons now installing
	// concurrently (gateway-api-crds, cilium, cert-manager, up from two),
	// real chart-repo fetch + apply latency for a single install can
	// exceed 30s under that combined load — observed via a real e2e run
	// where cert-manager's own install hit exactly this timeout three
	// reconciles in a row before finally landing within it on the fourth.
	addonInstallTimeout = 90 * time.Second

	// podProbeTimeout bounds each PodProber.NamespaceHealthy call.
	podProbeTimeout = 15 * time.Second
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Installer installs an addon's chart. Defaults to
	// NewHelmInstaller() when nil.
	Installer Installer
	// PodProber checks whether an installed addon's pods are actually
	// healthy. Defaults to NewPodProber() when nil.
	PodProber PodProber
	// CRDChecker checks whether an installed addon's own CRDs are
	// Established and discoverable. Defaults to NewCRDChecker() when nil.
	CRDChecker CRDChecker
	// RetryInterval is how long Reconcile waits before retrying a step
	// that hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
}

// Controller wires the Addon reconciler onto a controller-runtime
// Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting Installer/
// PodProber/RetryInterval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.Installer == nil {
		cfg.Installer = NewHelmInstaller()
	}

	if cfg.PodProber == nil {
		cfg.PodProber = NewPodProber()
	}

	if cfg.CRDChecker == nil {
		cfg.CRDChecker = NewCRDChecker()
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the Addon reconciler on mgr. The
// addons.kontinuum.sh CRD is ensured separately, via instance.EnsureCRDs
// (see pkg/cli/serve.go) — not here, for the same reason
// instance.Controller.SetupWithManager's own doc gives: SetupWithManager
// runs before the listener is bound, so ListForCluster deliberately lists
// and filters client-side instead of relying on a cache field index built
// here (which would force a discovery call against that not-yet-listening
// server).
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:        mgr.GetClient(),
		Installer:     c.Config.Installer,
		PodProber:     c.Config.PodProber,
		CRDChecker:    c.Config.CRDChecker,
		RetryInterval: c.Config.RetryInterval,
		Logger:        c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).For(&v1alpha2.Addon{}).Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register addon controller: %w", err)
	}

	return nil
}

// Reconciler installs and health-probes one Addon — resolves its
// spec (built-in fallback + CEL-computed defaults, freshly evaluated
// every reconcile, e.g. so cilium's operator.replicas keeps tracking
// control-plane count even after the Addon itself was created once),
// installs it, and probes its pods.
type Reconciler struct {
	Client        client.Client
	Installer     Installer
	PodProber     PodProber
	CRDChecker    CRDChecker
	RetryInterval time.Duration
	Logger        *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var addon v1alpha2.Addon

	err := r.Client.Get(ctx, req.NamespacedName, &addon)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get addon %q: %w", req.Name, err)
	}

	addon.Spec.ReleaseName = ReleaseName(&addon)

	if !Enabled(addon.Spec) {
		return ctrl.Result{}, nil // disabled — nothing to do; TalosCluster's own aggregation skips it too
	}

	result, ready, err := r.waitForEarlierWaves(ctx, &addon)
	if err != nil || !ready {
		return result, err
	}

	kubeconfig, result, ready, err := r.readyKubeconfig(ctx, addon.Spec.TalosClusterRef.Name)
	if err != nil || !ready {
		return result, err
	}

	installReq, err := r.resolveInstallRequest(ctx, addon.Spec)
	if err != nil {
		return r.setReady(ctx, &addon, metav1.ConditionFalse, "InstallFailed", err.Error())
	}

	return r.installAndVerify(ctx, &addon, kubeconfig, installReq)
}

// installAndVerify installs installReq's chart, then gates addon's own
// Ready condition behind two checks: installReq's own CRDs (if any) are
// Established and discoverable, then its pods are healthy.
func (r *Reconciler) installAndVerify(
	ctx context.Context, addon *v1alpha2.Addon, kubeconfig []byte, installReq InstallRequest,
) (ctrl.Result, error) {
	installCtx, cancel := context.WithTimeout(ctx, addonInstallTimeout)
	defer cancel()

	err := r.Installer.Install(installCtx, kubeconfig, installReq)
	if err != nil {
		r.Logger.Warn("failed to install addon", "addon", addon.Name, "error", err)

		return r.setReady(ctx, addon, metav1.ConditionFalse, "InstallFailed", err.Error())
	}

	crdsOK, crdReason, err := r.CRDChecker.ChartCRDsReady(ctx, kubeconfig, installReq)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check crd readiness for addon %q: %w", addon.Name, err)
	}

	if !crdsOK {
		return r.setReady(ctx, addon, metav1.ConditionFalse, "CRDsNotReady", crdReason)
	}

	healthy, reason, err := r.probeHealthy(ctx, kubeconfig, installReq)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !healthy {
		return r.setReady(ctx, addon, metav1.ConditionFalse, "NotHealthy", reason)
	}

	return r.setReady(ctx, addon, metav1.ConditionTrue, "Healthy", "installed and healthy")
}

// waitForEarlierWaves requeues until every other enabled Addon sharing
// addon's own cluster with a strictly lower EffectivePriority (i.e. an
// earlier install "wave") is already Ready — see
// AddonLifecycleSpec.Priority's own doc. Addons sharing the same
// priority never wait on each other, installing fully in parallel, same
// as if this didn't exist at all.
func (r *Reconciler) waitForEarlierWaves(ctx context.Context, addon *v1alpha2.Addon) (ctrl.Result, bool, error) {
	myPriority, err := EffectivePriority(addon)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	siblings, err := ListForCluster(ctx, r.Client, addon.Spec.TalosClusterRef.Name)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	for _, sibling := range siblings.Items {
		if sibling.Name == addon.Name || !Enabled(sibling.Spec) {
			continue
		}

		siblingPriority, err := EffectivePriority(&sibling)
		if err != nil {
			return ctrl.Result{}, false, err
		}

		if siblingPriority >= myPriority {
			continue
		}

		if !meta.IsStatusConditionTrue(sibling.Status.Conditions, addonReadyConditionType) {
			return ctrl.Result{RequeueAfter: r.RetryInterval}, false, nil
		}
	}

	return ctrl.Result{}, true, nil
}

// readyKubeconfig resolves clusterName's own stored kubeconfig, folding
// clusterKubeconfig's three outcomes into a single ready flag Reconcile
// can check with one branch: not ready with a nil error means "return
// result as Reconcile's own result and stop", not ready with a non-nil
// error means "propagate the error" — see clusterKubeconfig's own doc for
// what each outcome actually means.
func (r *Reconciler) readyKubeconfig(ctx context.Context, clusterName string) ([]byte, ctrl.Result, bool, error) {
	kubeconfig, found, err := r.clusterKubeconfig(ctx, clusterName)
	if err != nil {
		return nil, ctrl.Result{}, false, err
	}

	if !found {
		return nil, ctrl.Result{}, false, nil // orphaned reference — nothing productive to do, relies on GC
	}

	if kubeconfig == nil {
		return nil, ctrl.Result{RequeueAfter: r.RetryInterval}, false, nil // not bootstrapped yet
	}

	return kubeconfig, ctrl.Result{}, true, nil
}

// clusterKubeconfig resolves clusterName's own stored kubeconfig. found is
// false only when the owning TalosCluster itself is missing — an orphaned
// reference (GC hasn't caught up yet), nothing productive to do, not
// worth retrying. found true with a nil kubeconfig means the cluster
// exists but hasn't bootstrapped far enough to have stored one yet —
// callers should retry that case, not treat it the same as "give up".
func (r *Reconciler) clusterKubeconfig(ctx context.Context, clusterName string) ([]byte, bool, error) {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: clusterName}, &cluster)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("failed to get talos cluster %q: %w", clusterName, err)
	}

	kubeconfig, err := loadClusterKubeconfig(ctx, r.Client, &cluster)
	if err != nil {
		return nil, true, nil //nolint:nilerr // not bootstrapped yet, not a hard failure
	}

	return kubeconfig, true, nil
}

// resolveInstallRequest builds spec's own install request, evaluating its
// CEL-computed defaults fresh against cluster's current control-plane
// count — e.g. so cilium's operator.replicas keeps tracking it even after
// the Addon itself was created once.
func (r *Reconciler) resolveInstallRequest(ctx context.Context, spec v1alpha2.AddonSpec) (InstallRequest, error) {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: spec.TalosClusterRef.Name}, &cluster)
	if err != nil {
		return InstallRequest{}, fmt.Errorf("failed to get talos cluster %q: %w", spec.TalosClusterRef.Name, err)
	}

	controlPlaneCount, err := controlPlaneMemberCount(ctx, r.Client, &cluster)
	if err != nil {
		return InstallRequest{}, err
	}

	celCtx, err := CelContext(&cluster, controlPlaneCount)
	if err != nil {
		return InstallRequest{}, err
	}

	return resolveAddon(spec, celCtx)
}

// probeHealthy checks installReq's own pods via PodProber — installRelease/
// upgradeRelease only apply manifests, they don't wait for rollout, so
// this is the step that actually confirms an addon is usable, not just
// applied.
func (r *Reconciler) probeHealthy(
	ctx context.Context, kubeconfig []byte, installReq InstallRequest,
) (bool, string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, podProbeTimeout)
	defer cancel()

	chartLabel := helmChartLabel(installReq.ChartName, installReq.Version)

	healthy, reason, err := r.PodProber.NamespaceHealthy(
		probeCtx, kubeconfig, installReq.Namespace, installReq.ReleaseName, chartLabel)
	if err != nil {
		return false, "", fmt.Errorf("failed to probe pod health in %q: %w", installReq.Namespace, err)
	}

	return healthy, reason, nil
}

// setReady sets addon's Ready condition and persists status — False
// requeues at RetryInterval; True doesn't (stop actively polling once
// healthy — nothing yet forces periodic re-checks of an already-healthy
// addon).
func (r *Reconciler) setReady(
	ctx context.Context, addon *v1alpha2.Addon, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
		Type: addonReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	err := r.Client.Status().Update(ctx, addon)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update addon %q status: %w", addon.Name, err)
	}

	if status == metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}
