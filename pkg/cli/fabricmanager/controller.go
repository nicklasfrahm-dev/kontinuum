package fabricmanager

import (
	"context"
	"fmt"
	"log/slog"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// Reconciler watches every Fabric visible through this node's own
// downstream kontinuum-server (see newInClusterConfig's own doc) and
// converges this node's own NAT masquerade state to match: for every
// Fabric currently naming this node as some zone's own
// status.zones[].gatewayNodeRef, with spec.nat.disabled false, a
// masquerade table exists (see ensureMasquerade) for this node's own
// locally-classified uplink (see classifyLocalInterfaces); every table
// that shouldn't exist anymore — NAT disabled, this node re-elected away,
// or the owning Fabric deleted outright — gets torn down instead (see
// PruneStaleMasqueradeTables).
//
// Reconcile always re-lists and reconciles against every currently-listed
// Fabric, not just whichever one triggered it: narrowing to just the
// changed object would miss the "this node is no longer elected, or the
// Fabric is just gone" cases, since neither one names this node in
// anything left for a narrower Get to find.
//
// A gateway node elected by more than one Fabric (a trunk port with
// several tagged sub-interfaces, each backing a different zone's own
// Fabric) gets one independent table per Fabric here — see NATTableName's
// own doc — even though every one of them currently shares the exact same
// locally-classified wan (there's only one uplink per node); nftables
// tolerates more than one independent table asserting the identical
// masquerade for the same traffic, so this costs nothing and keeps each
// Fabric's own table lifecycle (create/prune) fully independent of any
// other's.
type Reconciler struct {
	// Client reads/writes Fabric through this node's own downstream
	// kontinuum-server — see newInClusterConfig's own doc.
	Client client.Client
	// NodeName is this node's own name, matching exactly what Talos sets
	// as this Kubernetes Node's own metadata.name (see
	// pkg/domain/taloscluster/config.go's own configBytes doc) — and so
	// what a gatewayNodeRef electing this node actually names (see
	// pkg/domain/fabric's own FabricZoneStatus.GatewayNodeRef doc).
	NodeName string
	// Logger receives this reconciler's own log output.
	Logger *slog.Logger
}

// Reconcile implements controller-runtime's reconcile.Reconciler. Never
// returns a hard error for a single Fabric's own ensureMasquerade
// failure — logged and skipped instead, so one Fabric's own transient
// nftables failure doesn't block every other Fabric this node is also
// responsible for in the very same pass; PruneStaleMasqueradeTables
// failing is likewise logged, not propagated, for the identical reason.
func (r *Reconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	var fabrics v1alpha2.FabricList

	err := r.Client.List(ctx, &fabrics)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list fabrics: %w", err)
	}

	wan, _, err := classifyLocalInterfaces()
	if err != nil {
		return ctrl.Result{}, err
	}

	if wan == "" {
		r.Logger.Warn("no local uplink interface found yet, skipping this reconcile pass")

		return ctrl.Result{}, nil
	}

	keep := make(map[string]bool, len(fabrics.Items))

	for _, fabricObj := range fabrics.Items {
		if !r.ElectedWithNATEnabled(fabricObj) {
			continue
		}

		err := ensureMasquerade(fabricObj.Name, wan)
		if err != nil {
			r.Logger.Warn("failed to ensure nat masquerade rule", "fabric", fabricObj.Name, "interface", wan, "error", err)

			continue
		}

		keep[NATTableName(fabricObj.Name, wan)] = true
	}

	err = PruneStaleMasqueradeTables(keep)
	if err != nil {
		r.Logger.Warn("failed to prune stale nat masquerade tables", "error", err)
	}

	return ctrl.Result{}, nil
}

// ElectedWithNATEnabled reports whether fabricObj currently elects this
// node as some zone's own gateway with NAT enabled.
func (r *Reconciler) ElectedWithNATEnabled(fabricObj v1alpha2.Fabric) bool {
	if fabricObj.Spec.NAT.Disabled {
		return false
	}

	for _, zoneStatus := range fabricObj.Status.Zones {
		if zoneStatus.GatewayNodeRef != nil && zoneStatus.GatewayNodeRef.Name == r.NodeName {
			return true
		}
	}

	return false
}

// SetupWithManager registers this Reconciler on mgr, triggered by any
// change to any Fabric — see Reconcile's own doc for why it always
// reconciles against the full list regardless of which one triggered it.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.Fabric{}).
		Complete(r)
	if err != nil {
		return fmt.Errorf("failed to build fabricmanager controller: %w", err)
	}

	return nil
}
