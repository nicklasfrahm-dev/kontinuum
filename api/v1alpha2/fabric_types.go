package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FabricNATSpec configures a Fabric's default per-zone NAT gateway.
type FabricNATSpec struct {
	// Disabled opts a fabric out of its default per-zone NAT gateway. Unset
	// (false) is the default — NAT enabled — following this repo's
	// idiomatic-Go convention of naming boolean fields so their zero value
	// is the default behavior, rather than needing a kubebuilder default
	// marker.
	// +optional
	Disabled bool `json:"disabled,omitempty"`
}

// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/zone_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:validation:XValidation:rule="size(self.region) > 0 && size(self.cidr) > 0",message="region and cidr are both required"

// FabricSpec describes one region-scoped network — modeled like an Azure
// VNet (one address space spanning every AZ/Zone in a region) but,
// unlike a VNet, deliberately carved into independent per-zone subnets and
// NAT gateways: each Zone is already its own separate Talos-bootstrapped
// cluster with no L2 adjacency to any other zone (issue #24's "one cluster
// per zone" decision), so there's no shared broadcast domain to build a
// VRRP/anycast VIP over in the first place, and one AZ's network failing
// never affects another's. VLAN assignment and DHCP are deliberately out of
// scope — a future CRD's concern, the same way Zone's own status doc used
// to forward-reference this one.
type FabricSpec struct {
	// Region is the region this fabric belongs to — every Zone whose own
	// spec.region matches gets a carved-out subnet from this fabric's CIDR.
	Region string `json:"region"`
	// CIDR is the fabric-wide address space every zone's own subnet is
	// carved out of, e.g. "10.0.0.0/16". Validated (net.ParseCIDR) by the
	// controller, not a CEL marker — see this package's own doc for why.
	CIDR string `json:"cidr"`
	// ZonePrefixLength is the prefix length of each zone's own carved
	// subnet, e.g. 24. Must be longer than CIDR's own prefix length —
	// validated by the controller. Left unset, it's computed once, the
	// first time this Fabric is reconciled, from the number of zones
	// already live in spec.region at that moment (see
	// Reconciler.ensureZonePrefixLengthDefaulted) — never recomputed
	// afterward, so scaling past that many zones later needs an explicit
	// edit here instead of an automatic, silent renumbering of every
	// already-carved zone.
	// +optional
	ZonePrefixLength int32 `json:"zonePrefixLength,omitempty"`
	// NAT configures this fabric's default per-zone NAT gateway — see
	// FabricNATSpec's own doc.
	// +optional
	NAT FabricNATSpec `json:"nat,omitempty"`
	// GatewaySelector matches candidate Instance objects (already labeled
	// kontinuum.sh/zone by the same convention InstancePool selectors rely
	// on) eligible to become a zone's NAT gateway node. Required: there is
	// no default/fallback node-selection heuristic, an empty or over-broad
	// selector is the operator's own call, not the controller's.
	GatewaySelector metav1.LabelSelector `json:"gatewaySelector"`
}

// FabricZoneStatus reports one zone's own IPAM allocation and NAT gateway
// selection within a Fabric.
type FabricZoneStatus struct {
	// Zone is this entry's own zone name.
	Zone string `json:"zone"`
	// CIDR is this zone's own carved subnet, e.g. "10.0.1.0/24".
	CIDR string `json:"cidr"`
	// GatewayIP is this zone's own gateway address — the last usable host
	// address (broadcast - 1) of CIDR, computed independently per zone; no
	// VRRP/anycast/BGP needed since no zone shares a broadcast domain with
	// any other (see FabricSpec's own doc).
	GatewayIP string `json:"gatewayIP"`
	// GatewayNodeRef points at the Instance elected to run this zone's own
	// NAT gateway workload — nil until GatewaySelector matches at least one
	// claimed candidate in this zone.
	// +optional
	GatewayNodeRef *ObjectReference `json:"gatewayNodeRef,omitempty"`
	// GatewayInterfaces lists GatewayNodeRef's own interfaces this fabric's
	// GatewayIP was assigned on — every interface Talos discovery reported
	// for that Instance with no address of its own already, i.e. every one
	// except whichever interface is already linked up with an address
	// (that one interface stays this node's own uplink: what
	// GatewayNodeRef is dialed on, and what NAT masquerades outbound
	// traffic through). Empty until GatewayNodeRef is resolved and has at
	// least one such interface.
	// +optional
	GatewayInterfaces []string `json:"gatewayInterfaces,omitempty"`
	// Conditions reports this zone entry's own readiness within the
	// fabric, e.g. GatewayNodeSelected.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions" patchMergeKey:"type" patchStrategy:"merge"`
}

// FabricStatus reports this fabric's readiness and per-zone allocation.
type FabricStatus struct {
	// Conditions reports this fabric's own state, e.g. Ready, ValidSpec.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions" patchMergeKey:"type" patchStrategy:"merge"`
	// Zones reports every zone currently carved out of this fabric — see
	// FabricZoneStatus's own doc. A zone's own entry (and its underlying
	// IPAM block index) is only dropped once its Zone object is actually
	// deleted, not merely unhealthy, so a temporarily-down zone keeps its
	// allocation.
	// +optional
	Zones []FabricZoneStatus `json:"zones,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".spec.region"
// +kubebuilder:printcolumn:name="CIDR",type="string",JSONPath=".spec.cidr"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].reason"

// Fabric represents one region-scoped network — see FabricSpec's own doc.
type Fabric struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec FabricSpec `json:"spec"`
	// +optional
	Status FabricStatus `json:"status"`
}

// +kubebuilder:object:root=true

// FabricList is a list of Fabric.
type FabricList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Fabric `json:"items"`
}
