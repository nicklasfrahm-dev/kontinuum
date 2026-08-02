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
	// indefinitely).
	addonInstallTimeout = 30 * time.Second

	// podProbeTimeout bounds each PodProber.NamespaceHealthy call.
	podProbeTimeout = 15 * time.Second
)

// TalosClusterRefField is the controller-runtime cache index name for
// Addon.spec.talosClusterRef.name — registered once in
// Controller.SetupWithManager. pkg/domain/taloscluster's own
// reconcileAddons relies on it via ListForCluster (there's no other way
// to discover which Addons belong to a TalosCluster now that
// spec.addons[] is gone), and it's reusable by tests via
// fake.NewClientBuilder().WithIndex(...).
const TalosClusterRefField = "spec.talosClusterRef.name"

func indexByTalosClusterRef(obj client.Object) []string {
	addon, ok := obj.(*v1alpha2.Addon)
	if !ok {
		return nil
	}

	return []string{addon.Spec.TalosClusterRef.Name}
}

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

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the field index Addon's own field selector
// (see api/v1alpha2's own +kubebuilder:selectablefield marker) and this
// package's List calls both rely on, then the Addon reconciler itself.
// The addons.kontinuum.sh CRD is ensured separately, via
// instance.EnsureCRDs (see pkg/cli/serve.go) — not here, for the same
// reason instance.Controller.SetupWithManager's own doc gives.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(
		context.Background(), &v1alpha2.Addon{}, TalosClusterRefField, indexByTalosClusterRef)
	if err != nil {
		return fmt.Errorf("failed to index addons by talosClusterRef: %w", err)
	}

	reconciler := &Reconciler{
		Client:        mgr.GetClient(),
		Installer:     c.Config.Installer,
		PodProber:     c.Config.PodProber,
		RetryInterval: c.Config.RetryInterval,
		Logger:        c.Config.Logger,
	}

	err = ctrl.NewControllerManagedBy(mgr).For(&v1alpha2.Addon{}).Complete(reconciler)
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

	if !Enabled(addon.Spec) {
		return ctrl.Result{}, nil // disabled — nothing to do; TalosCluster's own aggregation skips it too
	}

	kubeconfig, result, ready, err := r.readyKubeconfig(ctx, addon.Spec.TalosClusterRef.Name)
	if err != nil || !ready {
		return result, err
	}

	installReq, err := r.resolveInstallRequest(ctx, addon.Spec)
	if err != nil {
		return r.setReady(ctx, &addon, metav1.ConditionFalse, "InstallFailed", err.Error())
	}

	installCtx, cancel := context.WithTimeout(ctx, addonInstallTimeout)
	defer cancel()

	err = r.Installer.Install(installCtx, kubeconfig, installReq)
	if err != nil {
		r.Logger.Warn("failed to install addon", "addon", addon.Name, "error", err)

		return r.setReady(ctx, &addon, metav1.ConditionFalse, "InstallFailed", err.Error())
	}

	healthy, reason, err := r.probeHealthy(ctx, kubeconfig, installReq.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !healthy {
		return r.setReady(ctx, &addon, metav1.ConditionFalse, "NotHealthy", reason)
	}

	return r.setReady(ctx, &addon, metav1.ConditionTrue, "Healthy", "installed and healthy")
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

	celCtx, err := celContext(&cluster, controlPlaneCount)
	if err != nil {
		return InstallRequest{}, err
	}

	return resolveAddon(spec, celCtx)
}

// probeHealthy checks namespace's pods via PodProber — installRelease/
// upgradeRelease only apply manifests, they don't wait for rollout, so
// this is the step that actually confirms an addon is usable, not just
// applied.
func (r *Reconciler) probeHealthy(ctx context.Context, kubeconfig []byte, namespace string) (bool, string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, podProbeTimeout)
	defer cancel()

	healthy, reason, err := r.PodProber.NamespaceHealthy(probeCtx, kubeconfig, namespace)
	if err != nil {
		return false, "", fmt.Errorf("failed to probe pod health in %q: %w", namespace, err)
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
