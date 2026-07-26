package registry

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	ctrl "sigs.k8s.io/controller-runtime"
	conversionwebhook "sigs.k8s.io/controller-runtime/pkg/webhook/conversion"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	defaultHeartbeatInterval = time.Minute
	defaultStaleThreshold    = 5 * time.Minute

	// storageSecretKey is the key Config.Storage is stored under in the
	// Secret status.secretRef points to — matching pkg/config's
	// KONTINUUM_SERVER_STORAGE env var name exactly, so the Secret can be
	// mounted straight into a pod via envFrom with no translation layer.
	// This is a key name, not a credential value — gosec's G101 flags it
	// purely because "SECRET" appears in the string.
	//
	//nolint:gosec // false positive: an env var / secret key name, not a credential value
	storageSecretKey = "KONTINUUM_SERVER_STORAGE"
)

// Config configures a Controller.
type Config struct {
	// Role is this server's registry role — see Role.
	Role string
	// Region is the region this server manages. Empty when Role is v1alpha2.RoleControlPlane.
	Region string
	// Zone is the zone this server manages. Empty when Role is v1alpha2.RoleControlPlane.
	Zone string
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// HeartbeatInterval is how often this server refreshes its own Kontinuum
	// object's status. Defaults to one minute when zero.
	HeartbeatInterval time.Duration
	// StaleThreshold is how long a Kontinuum may go without a heartbeat before
	// the TTL reconciler deletes it. Defaults to five minutes when zero.
	StaleThreshold time.Duration
	// Version is this process's build version, written to status.version on
	// every heartbeat.
	Version string
	// Storage is the storage backend connection string (e.g.
	// "postgres://user:pass@host/db"). It can carry embedded credentials, so
	// it's kept out of status and instead stored in a Secret status.secretRef
	// points to — see Heartbeat.SecretData.
	Storage string
	// DisplayConfig is this process's own non-confidential configuration,
	// written to status.config on every heartbeat — see
	// v1alpha2.KontinuumConfigStatus.
	DisplayConfig v1alpha2.KontinuumConfigStatus
}

// Controller wires kontinuum's server registry — the kontinuums.kontinuum.sh
// CRD, a TTL reconciler that deletes stale Kontinuum objects, and a runnable
// that registers this process and heartbeats it — onto a controller-runtime
// Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting HeartbeatInterval
// and StaleThreshold when left zero.
func NewController(cfg Config) *Controller {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}

	if cfg.StaleThreshold == 0 {
		cfg.StaleThreshold = defaultStaleThreshold
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the TTL reconciler, the heartbeat, and the
// v1alpha1<->v1alpha2 conversion webhook handler on mgr. The
// kontinuums.kontinuum.sh CRD itself is ensured separately, via EnsureCRD
// registered as a libkapi.WithPostStartHook (see pkg/cli/serve.go) — not
// here, and not from the heartbeat, since SetupWithManager runs before the
// listener is bound and a Runnable has no ordering guarantee relative to
// any other registered Runnable.
//
// TTLReconciler and Heartbeat both watch Kontinuum, but controller-runtime
// requires controller names to be unique (for metrics), and
// ctrl.NewControllerManagedBy(mgr).For(&v1alpha2.Kontinuum{}) derives the
// same default name from the GVK regardless of which reconciler it wraps —
// registering each separately collides ("controller with name kontinuum
// already exists"). combinedReconciler merges them onto the one Controller
// both need.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &TTLReconciler{
		Client:         mgr.GetClient(),
		StaleThreshold: c.Config.StaleThreshold,
		Logger:         c.Config.Logger,
	}

	heartbeat := &Heartbeat{
		Client:     mgr.GetClient(),
		Name:       InstanceName(os.Hostname()),
		Role:       c.Config.Role,
		Spec:       v1alpha2.KontinuumSpec{Region: c.Config.Region, Zone: c.Config.Zone},
		Interval:   c.Config.HeartbeatInterval,
		Logger:     c.Config.Logger,
		Version:    c.Config.Version,
		SecretData: map[string]string{storageSecretKey: c.Config.Storage},
		Config:     c.Config.DisplayConfig,
	}

	combined := &CombinedReconciler{TTL: reconciler, Heartbeat: heartbeat}

	err := ctrl.NewControllerManagedBy(mgr).For(&v1alpha2.Kontinuum{}).Complete(combined)
	if err != nil {
		return fmt.Errorf("failed to register server controller: %w", err)
	}

	err = mgr.Add(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to register server heartbeat runnable: %w", err)
	}

	// Answers the CRD's spec.conversion.webhook (see CustomResourceDefinition)
	// — mgr.GetScheme() must already have both api/v1alpha1 and api/v1alpha2
	// registered (see pkg/cli/serve.go's buildServer) for the generic
	// handler to resolve either GVK.
	mgr.GetWebhookServer().Register(conversionWebhookPath, conversionwebhook.NewWebhookHandler(mgr.GetScheme()))

	return nil
}

// CombinedReconciler runs TTLReconciler's staleness-based deletion and
// Heartbeat's self-healing re-registration through the single
// reconcile.Reconciler both are wired onto (see Controller.SetupWithManager's
// doc for why they can't each register their own Controller). Every
// reconcile runs TTL's staleness check unconditionally — it applies to
// every Kontinuum, including this instance's own, same as if it still ran
// alone: a live heartbeat means self is never actually stale, so this is a
// no-op for it in practice, but it's what makes ctrl.Result.RequeueAfter
// keep re-checking self's own expiry too, not just every other instance's.
// Heartbeat's Reconcile additionally runs whenever req names this
// instance's own object, on top of - not instead of - the TTL check.
type CombinedReconciler struct {
	TTL       *TTLReconciler
	Heartbeat *Heartbeat
}

// Reconcile implements reconcile.Reconciler.
func (r *CombinedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.TTL.Reconcile(ctx, req)
	if err != nil {
		return result, err
	}

	if req.Name == r.Heartbeat.Name {
		_, err := r.Heartbeat.Reconcile(ctx, req)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

// InstanceName derives this process's Kontinuum object name from hostname
// and err — the return values of os.Hostname(), passed in rather than
// called here so this stays a pure, easily testable function. hostname is
// lowercased to satisfy Kubernetes' object-name rules (a real-world
// hostname is otherwise already a valid DNS subdomain). Falls back to a
// random UUID if hostname is empty, err is non-nil (os.Hostname can fail in
// a restrictive sandbox), or hostname is "localhost" — GCP Cloud Run's
// sandboxed container runtime doesn't set a real per-instance hostname, so
// os.Hostname() there succeeds but returns "localhost" for every instance,
// which would otherwise make them all collide under the same object name.
func InstanceName(hostname string, err error) string {
	if err != nil || hostname == "" || strings.EqualFold(hostname, "localhost") {
		instanceID, idErr := uuid.NewV7()
		if idErr != nil {
			return uuid.NewString()
		}

		return instanceID.String()
	}

	return strings.ToLower(hostname)
}
