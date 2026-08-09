package zone

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

const (
	// ClusterReadyConditionType is set true once the zone's own TalosCluster
	// (found by name — see BuildAddObjects) reports Ready — see
	// ZoneStatus's own doc comment ("ClusterReady, Installed").
	ClusterReadyConditionType = "ClusterReady"
	// InstalledConditionType is set true once every downstream object this
	// package installs is created and, for Certificate specifically, itself
	// reports Ready — a real signal that TLS issuance succeeded, not just
	// that the object was created.
	InstalledConditionType = "Installed"
	// TeardownConditionType is set false while a Zone being deleted is
	// still waiting on downstream cleanup and/or its seed node's Talos
	// Reset — see teardown.go's own doc. Never observed true: the
	// finalizer is removed (deleting the Zone for real) in the same
	// reconcile pass that would otherwise have set it, per issue #49's own
	// "only removes the finalizer once both steps complete" scope.
	TeardownConditionType = "Teardown"

	reasonTalosClusterNotFound   = "TalosClusterNotFound"
	reasonWaitingForTalosCluster = "WaitingForTalosCluster"
	reasonClusterReady           = "ClusterReady"
	reasonDownstreamNotReady     = "DownstreamNotReady"
	reasonNoStorageSecret        = "NoStorageSecretFound"
	reasonInstallFailed          = "InstallFailed"
	reasonWaitingForCertificate  = "WaitingForCertificate"
	reasonInstalled              = "Installed"

	// reasonDownstreamTeardownFailed and reasonTalosResetFailed are
	// teardown.go's own retryable-failure reasons — see reconcileTeardown.
	reasonDownstreamTeardownFailed = "DownstreamTeardownFailed"
	reasonTalosResetFailed         = "TalosResetFailed"

	defaultRetryInterval = 15 * time.Second
	// defaultTeardownTimeout bounds how long a Zone's finalizer keeps
	// retrying downstream teardown/Talos Reset before giving up and
	// removing itself anyway — see teardown.go's own doc for why this
	// exists (issue #49's explicit "not a finalizer that blocks deletion
	// forever" requirement) and docs/workflows/zone-remove.md for the
	// operator escape hatch that forces this sooner.
	defaultTeardownTimeout = 15 * time.Minute

	// ZoneFinalizer is the finalizer teardown.go adds to every Zone this
	// package reconciles, and only ever removes once its downstream
	// footprint is torn down and its seed node reset (or teardown has been
	// abandoned after defaultTeardownTimeout — see reconcileTeardown).
	// Exported so an operator's `kubectl patch` escape hatch (see
	// docs/workflows/zone-remove.md) and this package's own tests can name
	// it without duplicating the literal.
	ZoneFinalizer = "kontinuum.sh/zone-teardown"
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// DownstreamClientBuilder builds a client.Client against a zone's own
	// cluster from its stored kubeconfig. Defaults to
	// NewDownstreamClientBuilder() when nil — the seam tests inject a fake
	// through, the same role addon.Installer plays for Helm installs.
	DownstreamClientBuilder DownstreamClientBuilder
	// ACMEEmail and ACMEServer configure the cert-manager ClusterIssuer this
	// package creates on every joined zone's downstream cluster — see
	// pkg/config's KONTINUUM_ACME_EMAIL/KONTINUUM_ACME_SERVER.
	ACMEEmail  string
	ACMEServer string
	// Image is the kontinuum container image this package deploys onto
	// every joined zone's downstream cluster (e.g.
	// "ghcr.io/nicklasfrahm/kontinuum:v1.2.3") — see pkg/cli/serve.go's
	// zoneOptions, which computes this from this repo's own build version.
	Image string
	// RetryInterval is how long Reconcile waits before retrying a step that
	// hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
	// Bootstrapper resets a deleted zone's seed node back to Talos
	// maintenance mode via taloscluster.ResetControlPlane — see
	// teardown.go. Defaults to taloscluster.NewTalosBootstrapper(cfg.Logger)
	// when nil, the same production implementation
	// pkg/domain/taloscluster's own Controller defaults to.
	Bootstrapper taloscluster.ClusterBootstrapper
	// TeardownTimeout bounds how long a Zone being deleted keeps retrying
	// downstream teardown/Talos Reset before giving up and removing its
	// finalizer anyway — see teardown.go's own doc. Defaults to fifteen
	// minutes when zero.
	TeardownTimeout time.Duration
}

// Controller wires the Zone downstream-install reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting
// DownstreamClientBuilder and RetryInterval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.DownstreamClientBuilder == nil {
		cfg.DownstreamClientBuilder = NewDownstreamClientBuilder()
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	if cfg.Bootstrapper == nil {
		cfg.Bootstrapper = taloscluster.NewTalosBootstrapper(cfg.Logger)
	}

	if cfg.TeardownTimeout == 0 {
		cfg.TeardownTimeout = defaultTeardownTimeout
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the Zone reconciler on mgr. zones.kontinuum.sh's
// CRD itself is ensured separately, via instance.EnsureCRDs registered as a
// libkapi.WithPostStartHook (see pkg/cli/serve.go) — not here, mirroring
// every other domain controller in this repo.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:                  mgr.GetClient(),
		DownstreamClientBuilder: c.Config.DownstreamClientBuilder,
		ACMEEmail:               c.Config.ACMEEmail,
		ACMEServer:              c.Config.ACMEServer,
		Image:                   c.Config.Image,
		RetryInterval:           c.Config.RetryInterval,
		Bootstrapper:            c.Config.Bootstrapper,
		TeardownTimeout:         c.Config.TeardownTimeout,
		Logger:                  c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.Zone{}).
		Watches(&v1alpha2.TalosCluster{}, handler.EnqueueRequestsFromMapFunc(mapTalosClusterToZone)).
		Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register zone controller: %w", err)
	}

	return nil
}

// mapTalosClusterToZone maps a TalosCluster change to exactly one Zone
// reconcile request — the shared <region>-<zone> naming convention (see
// BuildAddObjects) means a TalosCluster named X can only ever be "owned"
// by a Zone also named X, so this is O(1), not a broad "enqueue every Zone"
// scan. Safe even if no such Zone exists: Reconcile's own Get handles
// NotFound.
func mapTalosClusterToZone(_ context.Context, obj client.Object) []ctrl.Request {
	return []ctrl.Request{{
		NamespacedName: types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()},
	}}
}

// Reconciler installs kontinuum's downstream footprint onto a zone's own
// cluster once its TalosCluster reports Ready — see this package's own doc.
type Reconciler struct {
	Client                  client.Client
	DownstreamClientBuilder DownstreamClientBuilder
	ACMEEmail               string
	ACMEServer              string
	Image                   string
	RetryInterval           time.Duration
	Bootstrapper            taloscluster.ClusterBootstrapper
	TeardownTimeout         time.Duration
	Logger                  *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var zoneObj v1alpha2.Zone

	err := r.Client.Get(ctx, req.NamespacedName, &zoneObj)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get zone %q: %w", req.Name, err)
	}

	if !zoneObj.DeletionTimestamp.IsZero() {
		return r.reconcileTeardown(ctx, &zoneObj)
	}

	if controllerutil.AddFinalizer(&zoneObj, ZoneFinalizer) {
		err = r.Client.Update(ctx, &zoneObj)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer to zone %q: %w", zoneObj.Name, err)
		}
	}

	var cluster v1alpha2.TalosCluster

	err = r.Client.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: zoneObj.Namespace}, &cluster)
	if apierrors.IsNotFound(err) {
		return r.setClusterReadyCondition(ctx, &zoneObj, metav1.ConditionFalse, reasonTalosClusterNotFound,
			fmt.Sprintf("no talos cluster named %q found yet", zoneObj.Name))
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get talos cluster %q: %w", zoneObj.Name, err)
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, taloscluster.ReadyConditionType) {
		return r.setClusterReadyCondition(ctx, &zoneObj, metav1.ConditionFalse, reasonWaitingForTalosCluster,
			fmt.Sprintf("waiting for talos cluster %q to become ready", cluster.Name))
	}

	result, err := r.setClusterReadyCondition(ctx, &zoneObj, metav1.ConditionTrue, reasonClusterReady,
		"talos cluster is ready")
	if err != nil {
		return result, err
	}

	return r.reconcileInstall(ctx, &zoneObj, &cluster)
}

// reconcileInstall installs kontinuum's downstream footprint onto zoneObj's own
// cluster — only ever reached once ClusterReady is true (see Reconcile).
// Every step is idempotent create-or-update; the first error short-circuits
// with Installed=False/InstallFailed and a requeue. Installed only flips
// True once the Certificate ensureCertificate creates itself reports Ready
// — a real signal that TLS issuance succeeded, not just that the object was
// created (mirrors how TalosCluster/Addon already aggregate real
// downstream readiness).
func (r *Reconciler) reconcileInstall(
	ctx context.Context, zoneObj *v1alpha2.Zone, cluster *v1alpha2.TalosCluster,
) (ctrl.Result, error) {
	kubeconfig, err := loadClusterKubeconfig(ctx, r.Client, cluster)
	if err != nil {
		r.Logger.Warn("downstream kubeconfig not yet available", "zone", zoneObj.Name, "error", err)

		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonDownstreamNotReady, err.Error())
	}

	downstream, err := r.DownstreamClientBuilder.Build(kubeconfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to build downstream client for %q: %w", zoneObj.Name, err)
	}

	storage, err := findKontinuumStorage(ctx, r.Client)
	if err != nil {
		r.Logger.Warn("no storage credentials to propagate yet", "zone", zoneObj.Name, "error", err)

		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonNoStorageSecret, err.Error())
	}

	hostname := fmt.Sprintf("%s.%s.%s", zoneObj.Spec.Zone, zoneObj.Spec.Region, zoneObj.Spec.Domain)

	err = r.installWorkload(ctx, downstream, zoneObj, storage)
	if err != nil {
		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonInstallFailed, err.Error())
	}

	certReady, err := r.installNetwork(ctx, downstream, hostname)
	if err != nil {
		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonInstallFailed, err.Error())
	}

	if !certReady {
		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonWaitingForCertificate,
			"waiting for cert-manager to issue "+hostname+"'s certificate")
	}

	return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionTrue, reasonInstalled,
		"kontinuum-server installed and serving at "+hostname)
}

// installWorkload ensures the namespace, kontinuum-env Secret/ConfigMap,
// Deployment, and Service — see workload.go.
func (r *Reconciler) installWorkload(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone, storage string,
) error {
	err := ensureNamespace(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	err = ensureSecret(ctx, downstream, downstreamNamespace, storage)
	if err != nil {
		return err
	}

	err = ensureConfigMap(ctx, downstream, downstreamNamespace,
		zoneObj.Spec.Region, zoneObj.Spec.Zone, r.ACMEEmail, r.ACMEServer)
	if err != nil {
		return err
	}

	err = ensureDeployment(ctx, downstream, downstreamNamespace, r.Image)
	if err != nil {
		return err
	}

	return ensureService(ctx, downstream, downstreamNamespace)
}

// setClusterReadyCondition sets ClusterReadyConditionType and persists zoneObj's
// status. A false status requeues after RetryInterval; a true status
// doesn't — Reconcile continues straight into reconcileInstall within the
// same call, and the controller-runtime watch re-triggers on the Update
// this Status().Update call itself produces for any future change.
func (r *Reconciler) setClusterReadyCondition(
	ctx context.Context, zoneObj *v1alpha2.Zone, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: ClusterReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	return r.persistStatus(ctx, zoneObj, status)
}

// setInstalledCondition sets InstalledConditionType and persists zoneObj's status.
func (r *Reconciler) setInstalledCondition(
	ctx context.Context, zoneObj *v1alpha2.Zone, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: InstalledConditionType, Status: status, Reason: reason, Message: message,
	})

	return r.persistStatus(ctx, zoneObj, status)
}

// persistStatus writes zoneObj's status and decides whether to requeue.
func (r *Reconciler) persistStatus(
	ctx context.Context, zoneObj *v1alpha2.Zone, status metav1.ConditionStatus,
) (ctrl.Result, error) {
	err := r.Client.Status().Update(ctx, zoneObj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update zone %q status: %w", zoneObj.Name, err)
	}

	if status == metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}
