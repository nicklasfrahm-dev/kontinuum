package zone

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// ErrLoneHubLessPeer is LonePeerCheck's sentinel for a hub-less zone peer
// that is the only zone in its fleet, with no hub either: no process may
// reconcile a zone's own resources from inside that zone (see
// zonelease.Locker.SelfZoneKey), so a lone peer would present as a zone
// that silently never converges.
var ErrLoneHubLessPeer = errors.New(
	"this zone is the only zone in a hub-less fleet and no other process can reconcile it; " +
		"add a second zone, or run a hub against the same storage")

// defaultLonePeerRetryInterval is how long LonePeerCheck waits between
// attempts while the apiserver can't answer yet.
const defaultLonePeerRetryInterval = 2 * time.Second

// LonePeerCheck is the startup validation that rejects a single-zone
// hub-less deployment — see ErrLoneHubLessPeer. It runs once, as a
// manager.Runnable rather than before the server starts, because it needs
// the apiserver (and the Kontinuum conversion webhook the manager starts
// first) to read the fleet's own Zone and Kontinuum objects. Returning
// ErrLoneHubLessPeer from Start stops the manager, which fails the whole
// serve process with that error.
//
// It only rejects a zone that already exists as the fleet's sole Zone. A
// hub-less fleet's very first peer is deployed out-of-band before any Zone
// object exists at all (see docs/production-deployment.md), and is
// accepted: its own Zone object is created once a second zone joins.
type LonePeerCheck struct {
	// Region and Zone identify this process's own zone.
	Region string
	Zone   string
	// Logger reports retries while the apiserver can't answer yet.
	Logger *slog.Logger
	// RetryInterval defaults to defaultLonePeerRetryInterval when zero.
	RetryInterval time.Duration
}

// SetupWithManager registers the check on mgr. Like every other
// SetupWithManager in this repo it performs no API call itself.
func (c *LonePeerCheck) SetupWithManager(mgr manager.Manager) error {
	err := mgr.Add(NewLonePeerRunnable(c, mgr.GetAPIReader()))
	if err != nil {
		return fmt.Errorf("failed to register lone peer check: %w", err)
	}

	return nil
}

// LonePeerRunnable binds a LonePeerCheck to a reader — the manager's own
// uncached one in production, see LonePeerCheck.SetupWithManager.
type LonePeerRunnable struct {
	check  *LonePeerCheck
	reader client.Reader
}

// NewLonePeerRunnable binds check to reader.
func NewLonePeerRunnable(check *LonePeerCheck, reader client.Reader) *LonePeerRunnable {
	return &LonePeerRunnable{check: check, reader: reader}
}

// Start implements manager.Runnable: it retries ValidateLonePeer until it
// gets an answer, then returns — nil when this peer isn't alone, which
// leaves the manager running, or ErrLoneHubLessPeer, which stops it.
func (r *LonePeerRunnable) Start(ctx context.Context) error {
	interval := r.check.RetryInterval
	if interval <= 0 {
		interval = defaultLonePeerRetryInterval
	}

	for {
		err := ValidateLonePeer(ctx, r.reader, r.check.Region, r.check.Zone)
		if err == nil || errors.Is(err, ErrLoneHubLessPeer) {
			return err
		}

		r.check.Logger.Warn("Failed to check whether this zone is a lone hub-less peer, retrying", "error", err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

// ValidateLonePeer returns ErrLoneHubLessPeer when region/zone's own Zone
// object is the only Zone in v1alpha2.KontinuumSystemNamespace and no hub
// (a v1alpha2.RoleControlPlane Kontinuum) is registered to reconcile it.
// Exported so it can be tested without a manager.
func ValidateLonePeer(ctx context.Context, reader client.Reader, region, zone string) error {
	var zones v1alpha2.ZoneList

	err := reader.List(ctx, &zones, client.InNamespace(v1alpha2.KontinuumSystemNamespace))
	if meta.IsNoMatchError(err) {
		// No Zone CRD yet: a brand-new fleet whose first peer this is.
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to list zones: %w", err)
	}

	if !isSoleZone(zones.Items, region, zone) {
		return nil
	}

	var kontinuums v1alpha2.KontinuumList

	err = reader.List(ctx, &kontinuums)
	if err != nil && !meta.IsNoMatchError(err) {
		return fmt.Errorf("failed to list kontinuums: %w", err)
	}

	for _, kontinuum := range kontinuums.Items {
		if kontinuum.Status.Role == v1alpha2.RoleControlPlane {
			return nil
		}
	}

	return fmt.Errorf("%w: region=%s zone=%s", ErrLoneHubLessPeer, region, zone)
}

// isSoleZone reports whether zones holds exactly one Zone, and it is
// region/zone's own.
func isSoleZone(zones []v1alpha2.Zone, region, zone string) bool {
	if len(zones) != 1 {
		return false
	}

	return zones[0].Spec.Region == region && zones[0].Spec.Zone == zone
}
