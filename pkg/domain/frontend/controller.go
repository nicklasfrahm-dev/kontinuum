// Package frontend keeps the fleet's frontend host resolving to every Ready
// zone — see v1alpha2.KontinuumFrontendConfigStatus.
//
// The record has a single writer. Every zone's external-dns publishes the
// DNSEndpoints in its own cluster, so a per-zone DNSEndpoint for the same
// shared name would have the zones' external-dns instances overwrite each
// other's targets. Instead, one fleet-wide reconciler holding
// zonelease.GlobalKey writes a single DNSEndpoint, listing the Gateway
// address of every Ready zone, into exactly one zone's cluster: the first
// Ready zone by name. Every other zone's copy is deleted, so when that zone
// stops being Ready the record moves to the next one.
package frontend

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

// DNSEndpointName names the frontend host's DNSEndpoint in whichever zone's
// cluster holds it.
const DNSEndpointName = "kontinuum-frontend"

// defaultInterval is how often the record is reconciled.
const defaultInterval = 30 * time.Second

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Host is the fleet's frontend host — the one name this controller
	// publishes.
	Host string
	// DownstreamClientBuilder builds a client against a zone's own cluster.
	// Defaults to zone.NewDownstreamClientBuilder() when nil.
	DownstreamClientBuilder zone.DownstreamClientBuilder
	// Interval defaults to defaultInterval when zero.
	Interval time.Duration
	// ZoneLease is this process's own zonelease.Locker identity: only the
	// holder of zonelease.GlobalKey writes the record.
	ZoneLease zonelease.Identity
}

// Controller wires the frontend DNS reconciler onto a controller-runtime
// Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting
// DownstreamClientBuilder and Interval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.DownstreamClientBuilder == nil {
		cfg.DownstreamClientBuilder = zone.NewDownstreamClientBuilder()
	}

	if cfg.Interval == 0 {
		cfg.Interval = defaultInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the reconciler as a runnable on mgr. Like
// every other SetupWithManager in this repo it performs no API call itself.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.Add(&Runnable{
		Client:                  mgr.GetClient(),
		Host:                    c.Config.Host,
		DownstreamClientBuilder: c.Config.DownstreamClientBuilder,
		Interval:                c.Config.Interval,
		Locker:                  zonelease.NewLockerFor(mgr.GetClient(), mgr.GetAPIReader(), c.Config.ZoneLease),
		Logger:                  c.Config.Logger,
	})
	if err != nil {
		return fmt.Errorf("failed to register frontend dns runnable: %w", err)
	}

	return nil
}

// Runnable implements manager.Runnable, reconciling the frontend host's
// record on every Interval tick — a ticker rather than a watch, since what
// it depends on (every zone's Gateway address) lives in the zones' own
// clusters, not in anything the hub's apiserver can watch.
type Runnable struct {
	Client                  client.Client
	Host                    string
	DownstreamClientBuilder zone.DownstreamClientBuilder
	Interval                time.Duration
	Locker                  *zonelease.Locker
	Logger                  *slog.Logger
}

// Start implements manager.Runnable. It blocks until ctx is canceled,
// reconciling once immediately and then on every tick.
func (r *Runnable) Start(ctx context.Context) error {
	r.Reconcile(ctx)

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.Reconcile(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// zoneCluster is one zone and a client against its own cluster.
type zoneCluster struct {
	zone       v1alpha2.Zone
	downstream client.Client
}

// Reconcile publishes the frontend host's record once — see this package's
// own doc. Failures are logged, not returned: the next tick tries again.
// Skipped entirely while this process doesn't hold zonelease.GlobalKey.
// With no Ready zone that has an address, the existing record is left as
// it is rather than removed, so a fleet-wide outage of the hub's view
// doesn't take the frontend host's DNS down with it.
func (r *Runnable) Reconcile(ctx context.Context) {
	acquired, err := r.Locker.TryAcquire(ctx, zonelease.GlobalKey)
	if err != nil {
		r.Logger.Error("Failed to acquire zone lease for frontend dns", "error", err)

		return
	}

	if !acquired {
		return
	}

	ctx, release := r.Locker.KeepRenewed(ctx, zonelease.GlobalKey)
	defer release()

	clusters, err := r.zoneClusters(ctx)
	if err != nil {
		r.Logger.Error("Failed to list zones for frontend dns", "error", err)

		return
	}

	holder, endpoints := r.records(ctx, clusters)
	if holder == nil {
		r.Logger.Warn("No ready zone with a gateway address to publish the frontend host for", "host", r.Host)

		return
	}

	err = zone.EnsureDNSEndpoint(ctx, holder.downstream, DNSEndpointName, endpoints)
	if err != nil {
		r.Logger.Error("Failed to publish frontend dns record", "zone", holder.zone.Name, "error", err)

		return
	}

	for _, cluster := range clusters {
		if cluster.zone.Name == holder.zone.Name {
			continue
		}

		err = zone.DeleteDNSEndpoint(ctx, cluster.downstream, DNSEndpointName)
		if err != nil {
			r.Logger.Warn("Failed to remove stale frontend dns record", "zone", cluster.zone.Name, "error", err)
		}
	}
}

// zoneClusters lists every zone, sorted by name, with a client against its
// own cluster. A zone whose cluster can't be reached yet is skipped.
func (r *Runnable) zoneClusters(ctx context.Context) ([]zoneCluster, error) {
	var zones v1alpha2.ZoneList

	err := r.Client.List(ctx, &zones, client.InNamespace(v1alpha2.KontinuumSystemNamespace))
	if err != nil {
		return nil, fmt.Errorf("failed to list zones: %w", err)
	}

	slices.SortFunc(zones.Items, func(left, right v1alpha2.Zone) int {
		return strings.Compare(left.Name, right.Name)
	})

	clusters := make([]zoneCluster, 0, len(zones.Items))

	for index := range zones.Items {
		zoneObj := &zones.Items[index]

		downstream, err := zone.DownstreamClient(ctx, r.Client, r.DownstreamClientBuilder, zoneObj)
		if err != nil {
			r.Logger.Debug("Skipping zone for frontend dns", "zone", zoneObj.Name, "error", err)

			continue
		}

		clusters = append(clusters, zoneCluster{zone: *zoneObj, downstream: downstream})
	}

	return clusters, nil
}

// readyAddress returns cluster's Gateway address and record type when the
// zone is Ready and the address can be one of the frontend host's A or AAAA
// targets. A Hostname-type Gateway address can't be one of several targets
// of a CNAME, so it's skipped.
func (r *Runnable) readyAddress(ctx context.Context, cluster *zoneCluster) (string, string, bool) {
	if !meta.IsStatusConditionTrue(cluster.zone.Status.Conditions, zone.ReadyConditionType) {
		return "", "", false
	}

	address, recordType, ready, err := zone.GatewayAddress(ctx, cluster.downstream)
	if err != nil || !ready {
		return "", "", false
	}

	if recordType != zone.RecordTypeA && recordType != zone.RecordTypeAAAA {
		r.Logger.Warn("Skipping zone with a hostname gateway address for the frontend host",
			"zone", cluster.zone.Name, "address", address)

		return "", "", false
	}

	return address, recordType, true
}

// records collects the Gateway address of every Ready zone in clusters into
// the frontend host's A and AAAA records, and picks the record's holder:
// the first Ready zone by name that has an address — see readyAddress.
func (r *Runnable) records(ctx context.Context, clusters []zoneCluster) (*zoneCluster, []zone.Endpoint) {
	var (
		holder     *zoneCluster
		ipv4, ipv6 []string
	)

	for index := range clusters {
		cluster := &clusters[index]

		address, recordType, ok := r.readyAddress(ctx, cluster)
		if !ok {
			continue
		}

		if recordType == zone.RecordTypeA {
			ipv4 = append(ipv4, address)
		} else {
			ipv6 = append(ipv6, address)
		}

		if holder == nil {
			holder = cluster
		}
	}

	var endpoints []zone.Endpoint

	if len(ipv4) > 0 {
		endpoints = append(endpoints, zone.Endpoint{DNSName: r.Host, Targets: ipv4, RecordType: zone.RecordTypeA})
	}

	if len(ipv6) > 0 {
		endpoints = append(endpoints, zone.Endpoint{DNSName: r.Host, Targets: ipv6, RecordType: zone.RecordTypeAAAA})
	}

	return holder, endpoints
}
