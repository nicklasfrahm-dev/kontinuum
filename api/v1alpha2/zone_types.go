package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:validation:XValidation:rule="size(self.region) > 0 && size(self.zone) > 0 && size(self.domain) > 0",message="region, zone, and domain are all required"

// ZoneSpec identifies a single availability zone: the region it belongs to,
// and the DNS domain its own kontinuum-server is published under
// (<zone>.<region>.<domain>). Unlike KontinuumSpec's region/zone — where
// both empty means control-plane — a Zone object only ever describes a
// real zone, so all three fields are required together.
type ZoneSpec struct {
	// Region is the region this zone belongs to.
	Region string `json:"region"`
	// Zone is this zone's own name within Region.
	Zone string `json:"zone"`
	// Domain is the DNS domain this zone's kontinuum-server is published
	// under.
	Domain string `json:"domain"`
}

// ZoneStatus reports this zone's readiness. Deliberately minimal — issue
// #24's architecture decision 4/5 considered and rejected aggregate
// capacity, single-node/HA visibility, and a networking/egress address
// here: each already has an authoritative home elsewhere (the zone's own
// InstancePool/TalosCluster status, or a future Fabric-like CRD), and
// duplicating it on Zone would just be a second copy that could drift.
type ZoneStatus struct {
	// Conditions reports this zone's readiness, e.g. ClusterReady, Installed.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions" patchMergeKey:"type" patchStrategy:"merge"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".spec.region"
// +kubebuilder:printcolumn:name="Zone",type="string",JSONPath=".spec.zone"
// +kubebuilder:printcolumn:name="Domain",type="string",JSONPath=".spec.domain"
// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:printcolumn:name="Installed",type="string",JSONPath=".status.conditions[?(@.type==\"Installed\")].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Installed\")].reason"

// Zone represents a single availability zone — a pure identity/DNS anchor
// mapping 1:1 to exactly one TalosCluster (see issue #24's architecture
// decision 1/5: one Kubernetes cluster per zone, not per region).
type Zone struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec ZoneSpec `json:"spec"`
	// +optional
	Status ZoneStatus `json:"status"`
}

// +kubebuilder:object:root=true

// ZoneList is a list of Zone.
type ZoneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Zone `json:"items"`
}
