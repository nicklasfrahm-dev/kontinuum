package fabricmanager

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/fabric"
)

// Reconciler watches every Fabric visible through this node's own
// downstream kontinuum-server (see newInClusterConfig's own doc) and
// converges this node's own state to match: for every Fabric currently
// naming this node as some zone's own status.zones[].gatewayNodeRef, with
// spec.nat.disabled false, this node gets a masquerade table for its own
// locally-classified uplink (see ensureMasquerade/classifyLocalInterfaces)
// and, once the hub has published somewhere to put it, this zone's own
// gateway address applied to its free interfaces via Talos (see
// applyElectedZone) — reporting the outcome of both back onto that same
// zone entry's own NetworkConfigured/NATInstalled/Ready conditions (see
// pkg/domain/fabric.reconcileNATForGatewayNode's own doc for why this
// controller, not the hub, is the one that actually sets those three
// true). Every table that shouldn't exist anymore — NAT disabled, this
// node re-elected away, or the owning Fabric deleted outright — gets torn
// down instead (see PruneStaleMasqueradeTables).
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
// returns a hard error for a single Fabric's own ensureMasquerade or
// interface-config failure — logged and recorded as that zone's own False
// condition instead (see applyElectedZone), so one Fabric's own transient
// failure doesn't block every other Fabric this node is also responsible
// for in the very same pass; PruneStaleMasqueradeTables failing is
// likewise logged, not propagated, for the identical reason.
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

	for i := range fabrics.Items {
		fabricObj := &fabrics.Items[i]

		if !r.ElectedWithNATEnabled(*fabricObj) {
			continue
		}

		gatewayZone := electedZoneStatus(fabricObj, r.NodeName)

		changed := r.applyElectedZone(ctx, fabricObj, gatewayZone, wan)
		if changed {
			err := r.Client.Status().Update(ctx, fabricObj)
			if err != nil {
				r.Logger.Warn("failed to persist fabric status", "fabric", fabricObj.Name, "error", err)
			}
		}

		if meta.IsStatusConditionTrue(gatewayZone.Conditions, fabric.NATInstalledConditionType) {
			keep[NATTableName(fabricObj.Name, wan)] = true
		}
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

	return electedZoneStatus(&fabricObj, r.NodeName) != nil
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

// electedZoneStatus returns a pointer into fabricObj's own status.zones
// for the one zone entry naming nodeName as its own gatewayNodeRef, or nil
// if none does. An Instance carries exactly one kontinuum.sh/zone label
// (see pkg/domain/fabric.resolveGatewayNode's own candidate-listing doc),
// so it can only ever be elected for one zone — the first match is always
// the only match. The returned pointer lets applyElectedZone mutate that
// entry's own Conditions in place, ready for a single Status().Update.
func electedZoneStatus(fabricObj *v1alpha2.Fabric, nodeName string) *v1alpha2.FabricZoneStatus {
	for i := range fabricObj.Status.Zones {
		if ref := fabricObj.Status.Zones[i].GatewayNodeRef; ref != nil && ref.Name == nodeName {
			return &fabricObj.Status.Zones[i]
		}
	}

	return nil
}

// applyElectedZone applies gatewayZone's own desired state on this node —
// WAN masquerade (ensureMasquerade) and, once the hub has published
// somewhere to put it (gatewayZone.GatewayInterfaces/GatewayIP/CIDR all
// set), this zone's own gateway address on its free interfaces (see
// applyGatewayNetwork) — and records the outcome back onto gatewayZone's
// own Conditions: NATInstalledConditionType for the masquerade rule,
// NetworkConfiguredConditionType for the interface push, and
// ZoneReadyConditionType once both are true. This is the one place that
// ever sets any of those three conditions true: pkg/domain/fabric's own
// reconcileNATForGatewayNode publishes the desired state onto this same
// zone entry but deliberately never does (see that function's own doc) —
// only the process that actually applies the state is in a position to
// claim it succeeded. Returns whether gatewayZone's own Conditions
// changed, for the caller to decide whether a Status().Update is needed.
func (r *Reconciler) applyElectedZone(
	ctx context.Context, fabricObj *v1alpha2.Fabric, gatewayZone *v1alpha2.FabricZoneStatus, wan string,
) bool {
	natErr := ensureMasquerade(fabricObj.Name, wan)
	if natErr != nil {
		r.Logger.Warn("failed to ensure nat masquerade rule",
			"fabric", fabricObj.Name, "zone", gatewayZone.Zone, "interface", wan, "error", natErr)
	}

	natChanged := meta.SetStatusCondition(&gatewayZone.Conditions, natCondition(natErr))

	networkChanged := false

	if len(gatewayZone.GatewayInterfaces) > 0 && gatewayZone.GatewayIP != "" && gatewayZone.CIDR != "" {
		networkErr := r.applyGatewayNetwork(ctx, fabricObj, gatewayZone)
		if networkErr != nil {
			r.Logger.Warn("failed to apply gateway interface config",
				"fabric", fabricObj.Name, "zone", gatewayZone.Zone, "error", networkErr)
		}

		networkChanged = meta.SetStatusCondition(&gatewayZone.Conditions, networkCondition(networkErr))
	}

	ready := natErr == nil && meta.IsStatusConditionTrue(gatewayZone.Conditions, fabric.NetworkConfiguredConditionType)
	readyChanged := meta.SetStatusCondition(&gatewayZone.Conditions, readyCondition(ready))

	return natChanged || networkChanged || readyChanged
}

// applyGatewayNetwork pushes gatewayZone's own gateway address
// (GatewayIP, combined with CIDR's own prefix length) onto this node's
// free interfaces (GatewayInterfaces) via Talos — see
// readTalosConfig/applyInterfaceConfig (talos.go) for how.
func (r *Reconciler) applyGatewayNetwork(
	ctx context.Context, fabricObj *v1alpha2.Fabric, gatewayZone *v1alpha2.FabricZoneStatus,
) error {
	_, subnet, err := net.ParseCIDR(gatewayZone.CIDR)
	if err != nil {
		return fmt.Errorf("failed to parse zone %q cidr %q: %w", gatewayZone.Zone, gatewayZone.CIDR, err)
	}

	ones, _ := subnet.Mask.Size()
	gatewayPrefix := fmt.Sprintf("%s/%d", gatewayZone.GatewayIP, ones)

	talosCfg, err := readTalosConfig(ctx, r.Client)
	if err != nil {
		return err
	}

	return applyInterfaceConfig(ctx, talosCfg, gatewayZone.GatewayInterfaces, gatewayPrefix, fabricObj.Spec.VLANID)
}

const (
	reasonNATInstalled        = "NATInstalled"
	reasonNATInstallFailed    = "NATInstallFailed"
	reasonNetworkConfigured   = "NetworkConfigured"
	reasonNetworkConfigFailed = "NetworkConfigFailed"
	reasonZoneReady           = "ZoneReady"
	reasonZoneNotReady        = "ZoneNotReady"
)

// natCondition builds gatewayZone's own NATInstalledConditionType from
// ensureMasquerade's own outcome.
func natCondition(err error) metav1.Condition {
	if err != nil {
		return metav1.Condition{
			Type: fabric.NATInstalledConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNATInstallFailed, Message: err.Error(),
		}
	}

	return metav1.Condition{
		Type: fabric.NATInstalledConditionType, Status: metav1.ConditionTrue,
		Reason: reasonNATInstalled, Message: "nat masquerade rule installed",
	}
}

// networkCondition builds gatewayZone's own NetworkConfiguredConditionType
// from applyGatewayNetwork's own outcome.
func networkCondition(err error) metav1.Condition {
	if err != nil {
		return metav1.Condition{
			Type: fabric.NetworkConfiguredConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNetworkConfigFailed, Message: err.Error(),
		}
	}

	return metav1.Condition{
		Type: fabric.NetworkConfiguredConditionType, Status: metav1.ConditionTrue,
		Reason: reasonNetworkConfigured, Message: "gateway address applied to fabric interfaces",
	}
}

// readyCondition builds gatewayZone's own aggregate ZoneReadyConditionType
// — see applyElectedZone's own doc for what ready actually means.
func readyCondition(ready bool) metav1.Condition {
	if !ready {
		return metav1.Condition{
			Type: fabric.ZoneReadyConditionType, Status: metav1.ConditionFalse,
			Reason: reasonZoneNotReady, Message: "nat masquerade or gateway interface config not applied yet",
		}
	}

	return metav1.Condition{
		Type: fabric.ZoneReadyConditionType, Status: metav1.ConditionTrue,
		Reason: reasonZoneReady, Message: "nat masquerade and gateway interface config both applied",
	}
}
