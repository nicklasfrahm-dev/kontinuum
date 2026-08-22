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
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	// ClusterReadyConditionType is set true once the zone's own TalosCluster
	// (found by name — see BuildAddObjects) reports Ready — see
	// ZoneStatus's own doc comment ("ClusterReady, Installed").
	ClusterReadyConditionType = "ClusterReady"
	// InstalledConditionType is set true once every downstream object this
	// package installs is created and, for Certificate specifically, itself
	// reports Ready — a real signal that TLS issuance succeeded, not just
	// that the object was created. This is not yet the reconciler's
	// terminal gate — see RegistryJoinedConditionType's own doc — since a
	// zone can be fully installed, with TLS issued and the kontinuum
	// container itself running, while still failing to actually join the
	// hub's registry (e.g. issue #95: a missing authentication env var used
	// to make the deployed process exit immediately on startup, before it
	// ever got as far as heartbeating).
	InstalledConditionType = "Installed"
	// RegistryJoinedConditionType is set true once this zone's own
	// kontinuum-server has actually registered itself as a worker Kontinuum
	// the hub can see (see FindJoinedKontinuum) — checked only once
	// Installed is true, since there's no point looking for a heartbeat
	// from a container that isn't even running yet. This, not Installed, is
	// this reconciler's true terminal gate: "the downstream objects exist
	// and TLS was issued" and "the zone's own kontinuum-server actually
	// joined the registry" are two different failure modes an operator
	// needs to tell apart (see docs/workflows/zone-add.md's own "closing
	// the loop" description of what joining a zone is meant to achieve).
	RegistryJoinedConditionType = "RegistryJoined"
	// TeardownConditionType is set false while a Zone being deleted is
	// still waiting on downstream cleanup and/or its seed node's Talos
	// Reset — see teardown.go's own doc. Never observed true: the
	// finalizer is removed (deleting the Zone for real) in the same
	// reconcile pass that would otherwise have set it, per issue #49's own
	// "only removes the finalizer once both steps complete" scope.
	TeardownConditionType = "Teardown"
	// ReadyConditionType aggregates ClusterReady, Installed, and
	// RegistryJoined into the one Type kstatus/kubectl-tree tooling
	// recognizes generically — a CRD with no Kind-specific handling (like
	// Zone) only ever renders a populated READY/REASON/STATUS column in
	// `kubectl tree` if status.conditions carries an entry literally Typed
	// "Ready" (mirrors TalosCluster's own ReadyConditionType). Set false
	// alongside ClusterReady or Installed whenever either of those is false
	// (see setClusterReadyCondition/setInstalledCondition) — neither being
	// true by itself means the Zone is ready, only that the next step is
	// about to run — and always mirrored from RegistryJoined otherwise (see
	// setRegistryJoinedCondition), which is this reconciler's true terminal
	// gate.
	ReadyConditionType = "Ready"

	reasonTalosClusterNotFound   = "TalosClusterNotFound"
	reasonWaitingForTalosCluster = "WaitingForTalosCluster"
	reasonClusterReady           = "ClusterReady"
	reasonDownstreamNotReady     = "DownstreamNotReady"
	reasonNoStorageSecret        = "NoStorageSecretFound"
	reasonNoVersionFound         = "NoVersionFound"
	reasonInstallFailed          = "InstallFailed"
	reasonWaitingForCertificate  = "WaitingForCertificate"
	reasonInstalled              = "Installed"
	reasonWaitingForRegistry     = "WaitingForRegistry"
	reasonRegistryJoined         = "RegistryJoined"

	// reasonDownstreamTeardownFailed and reasonTalosClusterDeleteFailed are
	// teardown.go's own retryable-failure reasons — see reconcileTeardown.
	reasonDownstreamTeardownFailed = "DownstreamTeardownFailed"
	reasonTalosClusterDeleteFailed = "TalosClusterDeleteFailed"

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
	// HubConfig is this hub's own loaded configuration — ensureEnv (see
	// workload.go) copies every env var it produces (see
	// config.Config.EnvVars) onto every joined zone's own kontinuum-env
	// Secret/ConfigMap, overridden only where a straight copy would be
	// wrong for that zone specifically (see zoneEnvOverrides): its own
	// identity (region/zone), its own storage DSN, and — when it has a
	// domain configured — its own OIDC redirect URL. Without this, the
	// deployed process also fails its own startup check
	// (pkg/config.Config.ValidateAuthentication, which refuses to start
	// unless exactly one of OIDCIssuerURL or InsecureAllowAnonymous is
	// set) and exits immediately, before it ever gets a chance to
	// heartbeat and join the hub's registry.
	HubConfig *config.Config
	// ImageRepo is the kontinuum container image repository this package
	// deploys onto every joined zone's downstream cluster (e.g.
	// "ghcr.io/nicklasfrahm-dev/kontinuum") — see pkg/cli/serve.go's
	// zoneOptions. The tag to deploy is resolved separately, at reconcile
	// time, from whatever version an already-registered Kontinuum reports —
	// see resolveImage's own doc.
	ImageRepo string
	// RetryInterval is how long Reconcile waits before retrying a step that
	// hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
	// TeardownTimeout bounds how long a Zone being deleted keeps retrying
	// downstream teardown before giving up and removing its finalizer
	// anyway — see teardown.go's own doc. Defaults to fifteen minutes when
	// zero.
	TeardownTimeout time.Duration
	// ZoneLease is this process's own zonelease.Locker identity — see
	// zonelease.Identity's own doc. Reconcile refuses to mutate a Zone it
	// doesn't hold this lease for, so the hub and every joined zone sharing
	// the same central storage never reconcile the same Zone at once, and a
	// zone's own process never reconciles its own Zone at all.
	ZoneLease zonelease.Identity
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
		HubConfig:               c.Config.HubConfig,
		ImageRepo:               c.Config.ImageRepo,
		RetryInterval:           c.Config.RetryInterval,
		TeardownTimeout:         c.Config.TeardownTimeout,
		Locker: zonelease.NewLocker(
			mgr.GetClient(), c.Config.ZoneLease.HolderIdentity, c.Config.ZoneLease.SelfZoneKey, 0),
		Logger: c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.Zone{}).
		Watches(&v1alpha2.TalosCluster{}, handler.EnqueueRequestsFromMapFunc(mapTalosClusterToZone)).
		Watches(&v1alpha2.Kontinuum{}, handler.EnqueueRequestsFromMapFunc(mapKontinuumToZone)).
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

// mapKontinuumToZone maps a Kontinuum change to the one Zone whose
// region/zone it identifies, via the same <region>-<zone> naming convention
// mapTalosClusterToZone relies on (see BuildAddObjects) — a Kontinuum's own
// object name is registry.InstanceName(os.Hostname()), unrelated to which
// zone it belongs to, so its Spec.Region/Spec.Zone are what this maps by
// instead. Without this watch, RegistryJoined never gets re-checked once
// true: persistStatus stops requeuing once a condition is True, so a zone's
// kontinuum-server later going stale — TTLReconciler deleting its Kontinuum
// after StaleThreshold, a crash that never re-registers, manual
// deregistration — would otherwise leave the condition stuck reporting
// "registered and heartbeating" forever, with nothing left to notice it
// isn't anymore. Returns no request for a Kontinuum with no region/zone set
// (the hub's own self-registration) — zonelease.Key returns "" for that
// case too, and there's no Zone named "" to enqueue.
func mapKontinuumToZone(_ context.Context, obj client.Object) []ctrl.Request {
	kontinuum, ok := obj.(*v1alpha2.Kontinuum)
	if !ok {
		return nil
	}

	name := zonelease.Key(kontinuum.Spec.Region, kontinuum.Spec.Zone)
	if name == "" {
		return nil
	}

	return []ctrl.Request{{
		NamespacedName: types.NamespacedName{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
	}}
}

// Reconciler installs kontinuum's downstream footprint onto a zone's own
// cluster once its TalosCluster reports Ready — see this package's own doc.
type Reconciler struct {
	Client                  client.Client
	DownstreamClientBuilder DownstreamClientBuilder
	HubConfig               *config.Config
	ImageRepo               string
	RetryInterval           time.Duration
	TeardownTimeout         time.Duration
	// Locker gates every write below against zonelease — see
	// Config.ZoneLease's own doc.
	Locker *zonelease.Locker
	Logger *slog.Logger
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

	acquired, err := r.Locker.TryAcquire(ctx, zonelease.Key(zoneObj.Spec.Region, zoneObj.Spec.Zone))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to acquire zone lease for %q: %w", zoneObj.Name, err)
	}

	if !acquired {
		return ctrl.Result{RequeueAfter: zonelease.Jitter(r.RetryInterval)}, nil
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

	// Checked regardless of install progress — a zone's own etcd proxy
	// identity needs to keep rotating for the whole lifetime of the Zone,
	// not just while it's still being brought up. Its own requeue deadline
	// is folded into whatever the rest of Reconcile decides below (see
	// earliestRequeue), including once everything else is fully Ready and
	// would otherwise stop requeuing altogether.
	identityRequeue, err := r.reconcileIdentityRotationSchedule(ctx, &zoneObj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check identity rotation schedule for zone %q: %w", zoneObj.Name, err)
	}

	result, err := r.reconcileClusterAndInstall(ctx, &zoneObj)

	return earliestRequeue(result, identityRequeue), err
}

// reconcileClusterAndInstall is Reconcile's own former body, factored out
// purely to keep Reconcile's own cyclomatic complexity down — see
// Reconcile's own doc for identityRequeue, folded in by its one caller
// there rather than threaded through every branch here.
func (r *Reconciler) reconcileClusterAndInstall(ctx context.Context, zoneObj *v1alpha2.Zone) (ctrl.Result, error) {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: zoneObj.Namespace}, &cluster)
	if apierrors.IsNotFound(err) {
		return r.setClusterReadyCondition(ctx, zoneObj, metav1.ConditionFalse, reasonTalosClusterNotFound,
			fmt.Sprintf("no talos cluster named %q found yet", zoneObj.Name))
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get talos cluster %q: %w", zoneObj.Name, err)
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, taloscluster.ReadyConditionType) {
		return r.setClusterReadyCondition(ctx, zoneObj, metav1.ConditionFalse, reasonWaitingForTalosCluster,
			fmt.Sprintf("waiting for talos cluster %q to become ready", cluster.Name))
	}

	result, err := r.setClusterReadyCondition(ctx, zoneObj, metav1.ConditionTrue, reasonClusterReady,
		"talos cluster is ready")
	if err != nil {
		return result, err
	}

	return r.reconcileInstall(ctx, zoneObj, &cluster)
}

// earliestRequeue folds identityRequeue (see
// reconcileIdentityRotationSchedule) into result, keeping whichever of the
// two would fire sooner. A zero identityRequeue means "no preference" (no
// identity issued yet, or reconcileIdentityRotationSchedule hit an error
// already surfaced by its own caller) and leaves result untouched.
func earliestRequeue(result ctrl.Result, identityRequeue time.Duration) ctrl.Result {
	if identityRequeue <= 0 {
		return result
	}

	if result.RequeueAfter == 0 || identityRequeue < result.RequeueAfter {
		result.RequeueAfter = identityRequeue
	}

	return result
}

// prepareZoneStorage ensures zoneObj's own etcd proxy identity (see
// ensureEtcdIdentity) and resolves the KONTINUUM_SERVER_STORAGE value that
// depends on it (see zoneStorageDSN) — split out of reconcileInstall
// purely to keep its own cyclomatic complexity down; both steps report
// failure identically to their one caller (Installed=False/
// NoStorageSecretFound), so collapsing them into a single error return
// here removes a duplicated branch there.
func (r *Reconciler) prepareZoneStorage(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone,
) (string, bool, error) {
	rotatedIdentity, err := r.ensureEtcdIdentity(ctx, downstream, zoneObj)
	if err != nil {
		return "", false, err
	}

	storage, err := r.zoneStorageDSN(zoneObj)
	if err != nil {
		return "", false, err
	}

	return storage, rotatedIdentity, nil
}

// reconcileInstall installs kontinuum's downstream footprint onto zoneObj's own
// cluster — only ever reached once ClusterReady is true (see Reconcile).
// Every step is idempotent create-or-update; the first error short-circuits
// with Installed=False/InstallFailed and a requeue. With spec.domain set,
// Installed only flips True once the Certificate ensureCertificate creates
// itself reports Ready — a real signal that TLS issuance succeeded, not
// just that the object was created (mirrors how TalosCluster/Addon already
// aggregate real downstream readiness). With spec.domain unset, there's no
// hostname to issue a certificate or route traffic for, so installNetwork
// is skipped entirely and Installed flips True as soon as the workload
// itself installs. Either way, reconcileRegistryJoin runs as the final step
// once Installed is True, before the Zone can report Ready (see
// RegistryJoinedConditionType's own doc).
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

	storage, rotatedIdentity, err := r.prepareZoneStorage(ctx, downstream, zoneObj)
	if err != nil {
		r.Logger.Warn("no storage credentials to propagate yet", "zone", zoneObj.Name, "error", err)

		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonNoStorageSecret, err.Error())
	}

	image, err := r.resolveImage(ctx)
	if err != nil {
		r.Logger.Warn("no kontinuum version to deploy yet", "zone", zoneObj.Name, "error", err)

		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonNoVersionFound, err.Error())
	}

	// hostname stays empty when spec.domain is unset — installNetwork (and
	// everything it creates: ClusterIssuer, Gateway, Certificate,
	// HTTPRoute) is skipped entirely in that case, not just given a
	// malformed "<zone>.<region>." hostname: none of those resources mean
	// anything without a real domain to issue a certificate and route
	// traffic for. installWorkload still runs regardless — this zone's own
	// kontinuum-server registers with the hub either way (see
	// ensureConfigMap's own doc for hostname's only other use, OIDC's
	// redirect URL, itself skipped unless auth.OIDCIssuerURL is set).
	hasDomain := zoneObj.Spec.Domain != ""

	var hostname string
	if hasDomain {
		hostname = fmt.Sprintf("%s.%s.%s", zoneObj.Spec.Zone, zoneObj.Spec.Region, zoneObj.Spec.Domain)
	}

	err = r.installWorkload(ctx, downstream, zoneObj, storage, image, hostname)
	if err != nil {
		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonInstallFailed, err.Error())
	}

	// No restart to force here: the zone's own kontinuum-server watches its
	// identity Secret directly (see etcdproxy.WatchIdentity) and picks up a
	// rotated key on its own — this is purely an observability log line.
	if rotatedIdentity {
		r.Logger.Info("rotated zone's etcd proxy identity", "zone", zoneObj.Name)
	}

	if !hasDomain {
		return r.finishInstallWithoutDomain(ctx, zoneObj)
	}

	return r.finishInstallWithDomain(ctx, downstream, zoneObj, hostname)
}

// finishInstallWithDomain installs the network layer (ClusterIssuer,
// Gateway, Certificate, HTTPRoute — see installNetwork) for a Zone with
// spec.domain set, and once the Certificate itself reports Ready, flips
// Installed True and proceeds to reconcileRegistryJoin — split out of
// reconcileInstall purely to keep its own cyclomatic complexity down.
func (r *Reconciler) finishInstallWithDomain(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone, hostname string,
) (ctrl.Result, error) {
	certReady, err := r.installNetwork(ctx, downstream, hostname)
	if err != nil {
		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonInstallFailed, err.Error())
	}

	if !certReady {
		return r.setInstalledCondition(ctx, zoneObj, metav1.ConditionFalse, reasonWaitingForCertificate,
			"waiting for cert-manager to issue "+hostname+"'s certificate")
	}

	result, err := r.setInstalledCondition(ctx, zoneObj, metav1.ConditionTrue, reasonInstalled,
		"kontinuum-server installed and serving at "+hostname)
	if err != nil {
		return result, err
	}

	return r.reconcileRegistryJoin(ctx, zoneObj)
}

// finishInstallWithoutDomain flips Installed True and proceeds straight to
// reconcileRegistryJoin for a Zone with no spec.domain configured — split
// out of reconcileInstall purely to keep its own cyclomatic complexity
// down; see that function's own doc for why no domain means installNetwork
// never runs at all.
func (r *Reconciler) finishInstallWithoutDomain(ctx context.Context, zoneObj *v1alpha2.Zone) (ctrl.Result, error) {
	result, err := r.setInstalledCondition(ctx, zoneObj, metav1.ConditionTrue, reasonInstalled,
		"kontinuum-server installed (no spec.domain configured — network exposure skipped)")
	if err != nil {
		return result, err
	}

	return r.reconcileRegistryJoin(ctx, zoneObj)
}

// reconcileRegistryJoin checks whether this zone's own kontinuum-server has
// actually registered itself in the hub's registry yet (see
// FindJoinedKontinuum) — only ever reached once Installed is true (see
// reconcileInstall). This is the reconciler's true terminal gate — see
// RegistryJoinedConditionType's own doc for why Installed alone isn't
// enough.
func (r *Reconciler) reconcileRegistryJoin(ctx context.Context, zoneObj *v1alpha2.Zone) (ctrl.Result, error) {
	_, joined, err := FindJoinedKontinuum(ctx, r.Client, zoneObj.Spec.Region, zoneObj.Spec.Zone)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check whether zone %q joined the registry: %w", zoneObj.Name, err)
	}

	if !joined {
		return r.setRegistryJoinedCondition(ctx, zoneObj, metav1.ConditionFalse, reasonWaitingForRegistry,
			"waiting for this zone's kontinuum-server to register itself in the hub's registry")
	}

	return r.setRegistryJoinedCondition(ctx, zoneObj, metav1.ConditionTrue, reasonRegistryJoined,
		"kontinuum-server for this zone is registered and heartbeating")
}

// resolveImage returns the full container image (r.ImageRepo:tag) to
// deploy onto a newly joined zone's downstream cluster — always read live
// off any already-registered Kontinuum's own status.version (see
// findKontinuumVersion), mirroring findKontinuumStorage/findKontinuumDomain's
// identical "a property of the deployment, not of whichever specific
// process happens to run this reconcile" reasoning: the fleet's actually
// running version self-heals across a rolling upgrade this way, where
// trusting this reconciling process's own build version could deploy a
// stale or ahead-of-the-fleet tag depending on which hub replica's
// reconcile happened to win.
//
// This includes a hub that's itself a local, unreleased build (reporting
// "dev" as its own status.version, from pkg/cli/version.go's default) —
// resolveImage deploys ImageRepo:dev in that case just like any other tag,
// trusting it to be a real, pullable image: CI keeps that tag in sync with
// main on every push, and `make image-push` publishes the working tree's
// own build under it for local zone-join testing (see the Makefile's own
// doc and docs/local-setup.md).
func (r *Reconciler) resolveImage(ctx context.Context) (string, error) {
	tag, err := findKontinuumVersion(ctx, r.Client)
	if err != nil {
		return "", err
	}

	return r.ImageRepo + ":" + tag, nil
}

// installWorkload ensures the namespace, kontinuum-env Secret/ConfigMap,
// identity-watching ServiceAccount/Role/RoleBinding, Deployment, and
// Service — see workload.go. hostname is the zone's own
// <zone>.<region>.<domain>, only used to compute a zone-specific OIDC
// redirect URL (see zoneEnvOverrides) when non-empty — not part of
// storage/region/zone.
func (r *Reconciler) installWorkload(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone, storage, image, hostname string,
) error {
	err := ensureNamespace(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	overrides := zoneEnvOverrides(zoneObj.Spec.Region, zoneObj.Spec.Zone, storage, hostname)

	err = ensureEnv(ctx, downstream, downstreamNamespace, r.HubConfig, overrides)
	if err != nil {
		return err
	}

	// Must run before ensureDeployment — see ensureIdentityRBAC's own doc
	// for why a Pod referencing a not-yet-existing ServiceAccount fails
	// admission.
	err = ensureIdentityRBAC(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	err = ensureDeployment(ctx, downstream, downstreamNamespace, image)
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
	changed := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: ClusterReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	// Only the blocking (False) case propagates to Ready here — see
	// ReadyConditionType's own doc for why a True ClusterReady doesn't.
	if status == metav1.ConditionFalse {
		readyChanged := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
			Type: ReadyConditionType, Status: status, Reason: reason, Message: message,
		})
		changed = changed || readyChanged
	}

	return r.persistStatus(ctx, zoneObj, status, changed)
}

// setInstalledCondition sets InstalledConditionType and persists zoneObj's
// status. Only the blocking (False) case propagates to Ready here — mirrors
// setClusterReadyCondition's own doc for why a True status doesn't: it just
// means reconcileRegistryJoin is about to run, not that the Zone is ready.
func (r *Reconciler) setInstalledCondition(
	ctx context.Context, zoneObj *v1alpha2.Zone, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	changed := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: InstalledConditionType, Status: status, Reason: reason, Message: message,
	})

	if status == metav1.ConditionFalse {
		readyChanged := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
			Type: ReadyConditionType, Status: status, Reason: reason, Message: message,
		})
		changed = changed || readyChanged
	}

	return r.persistStatus(ctx, zoneObj, status, changed)
}

// setRegistryJoinedCondition sets RegistryJoinedConditionType, mirrors it
// onto ReadyConditionType (see that constant's own doc — RegistryJoined is
// this reconciler's true terminal gate, so both directions propagate,
// unlike setClusterReadyCondition/setInstalledCondition above which only
// propagate their False case), and persists zoneObj's status.
func (r *Reconciler) setRegistryJoinedCondition(
	ctx context.Context, zoneObj *v1alpha2.Zone, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	joinedChanged := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: RegistryJoinedConditionType, Status: status, Reason: reason, Message: message,
	})

	readyChanged := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: ReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	return r.persistStatus(ctx, zoneObj, status, joinedChanged || readyChanged)
}

// persistStatus writes zoneObj's status and decides whether to requeue.
// changed is whatever the caller's own meta.SetStatusCondition call(s)
// returned — when every one of them reports false (same
// Type/Status/Reason/Message already stored), the Status().Update call is
// skipped entirely.
//
// This matters beyond avoiding a no-op API call: this controller's own
// Zone watch (see SetupWithManager's For(&v1alpha2.Zone{}), which carries
// no predicate) re-triggers Reconcile on every Update to a Zone, including
// its own status-subresource writes. An unconditional Update here — even
// one that changes nothing — would still bump ResourceVersion and fire a
// new watch event, immediately re-entering Reconcile faster than the
// informer cache can catch up. Two such overlapping reconciles then race:
// the later one's Get reads a ResourceVersion the earlier Update has
// already moved past, and its own Update fails with a 409 conflict. Only
// writing when the conditions actually changed breaks that
// self-sustaining loop.
func (r *Reconciler) persistStatus(
	ctx context.Context, zoneObj *v1alpha2.Zone, status metav1.ConditionStatus, changed bool,
) (ctrl.Result, error) {
	if changed {
		err := r.Client.Status().Update(ctx, zoneObj)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update zone %q status: %w", zoneObj.Name, err)
		}
	}

	if status == metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}
