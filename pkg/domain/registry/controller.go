package registry

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
)

const (
	defaultHeartbeatInterval = time.Minute
	defaultStaleThreshold    = 5 * time.Minute
)

// Config configures a Controller.
type Config struct {
	// Role is this server's registry role — see Role.
	Role string
	// Region is the region this server manages. Empty when Role is v1alpha1.RoleControlPlane.
	Region string
	// Zone is the zone this server manages. Empty when Role is v1alpha1.RoleControlPlane.
	Zone string
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// HeartbeatInterval is how often this server refreshes its own Kontinuum
	// object's status. Defaults to one minute when zero.
	HeartbeatInterval time.Duration
	// StaleThreshold is how long a Kontinuum may go without a heartbeat before
	// the TTL reconciler deletes it. Defaults to five minutes when zero.
	StaleThreshold time.Duration
}

// Controller wires kontinuum's server registry — the kontinuums.kontinuum.io
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

// SetupWithManager registers the TTL reconciler and the heartbeat runnable
// on mgr. The kontinuums.kontinuum.io CRD itself is ensured separately, via
// EnsureCRD registered as a libkapi.WithPostStartHook (see pkg/cli/serve.go)
// — not here, and not from the heartbeat runnable, since SetupWithManager
// runs before the listener is bound and a Runnable has no ordering
// guarantee relative to any other registered Runnable.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &TTLReconciler{
		Client:         mgr.GetClient(),
		StaleThreshold: c.Config.StaleThreshold,
		Logger:         c.Config.Logger,
	}

	err := reconciler.SetupWithManager(mgr)
	if err != nil {
		return err
	}

	heartbeatRunnable := &Heartbeat{
		Client:   mgr.GetClient(),
		Name:     InstanceName(os.Hostname()),
		Spec:     v1alpha1.KontinuumSpec{Role: c.Config.Role, Region: c.Config.Region, Zone: c.Config.Zone},
		Interval: c.Config.HeartbeatInterval,
		Logger:   c.Config.Logger,
	}

	err = mgr.Add(heartbeatRunnable)
	if err != nil {
		return fmt.Errorf("failed to register server heartbeat runnable: %w", err)
	}

	return nil
}

// InstanceName derives this process's Kontinuum object name from hostname
// and err — the return values of os.Hostname(), passed in rather than
// called here so this stays a pure, easily testable function. hostname is
// lowercased to satisfy Kubernetes' object-name rules (a real-world
// hostname is otherwise already a valid DNS subdomain). Falls back to a
// random UUID if hostname is empty or err is non-nil (os.Hostname can fail
// in a restrictive sandbox) — matching the previous, always-random naming,
// rather than leaving the registry without a name.
func InstanceName(hostname string, err error) string {
	if err != nil || hostname == "" {
		return uuid.NewString()
	}

	return strings.ToLower(hostname)
}
