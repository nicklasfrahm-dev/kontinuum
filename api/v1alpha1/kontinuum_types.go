package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// RoleControlPlane identifies a Kontinuum as the read-write entrypoint —
	// KONTINUUM_SERVER_REGION and KONTINUUM_SERVER_ZONE are both unset.
	RoleControlPlane = "ControlPlane"
	// RoleWorker identifies a Kontinuum as managing a single region and zone.
	RoleWorker = "Worker"
)

// This marker is the CRD's actual, authoritative region/zone invariant —
// see registry.CustomResourceDefinition, which applies config/crd's
// generated manifest rather than hand-building the schema. The rule
// mirrors registry.Role's own check exactly, and the message matches
// registry.ErrRegionZoneRequired, so a rejection at the apiserver reads
// the same as one from this process's own startup config. The line
// exceeds this repo's normal length limit, but splitting a kubebuilder
// marker across lines isn't supported, so it's exempted rather than
// shortened.
//
//nolint:lll
// +kubebuilder:validation:XValidation:rule="(!has(self.region) || self.region == '') == (!has(self.zone) || self.zone == '')",message="region and zone must both be set, or both be empty"

// KontinuumSpec describes a single running kontinuum process.
type KontinuumSpec struct {
	// Role is either RoleControlPlane or RoleWorker.
	// +kubebuilder:validation:Enum=ControlPlane;Worker
	Role string `json:"role"`
	// Region is the region this process manages. Empty when Role is RoleControlPlane.
	Region string `json:"region,omitempty"`
	// Zone is the availability zone this process manages. Empty when Role is RoleControlPlane.
	Zone string `json:"zone,omitempty"`
}

// KontinuumStatus reports the last time a Kontinuum reported in.
// LastHeartbeatTime is +optional despite lacking omitempty: with the
// status subresource enabled (see the +kubebuilder:subresource:status
// marker below), the apiserver always strips whatever status the main
// resource endpoint's Create/Update payload carries before validating it —
// status is only ever populated afterward, via the status subresource.
// Requiring this field would make every Create fail structural-schema
// validation against that always-empty status.
type KontinuumStatus struct {
	// LastHeartbeatTime is when this process last reported in. The server
	// registry deletes a Kontinuum whose LastHeartbeatTime is older than
	// its configured stale threshold (5 minutes by default).
	// +optional
	LastHeartbeatTime metav1.Time `json:"lastHeartbeatTime"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Role",type="string",JSONPath=".spec.role"
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".spec.region"
// +kubebuilder:printcolumn:name="Zone",type="string",JSONPath=".spec.zone"

// Kontinuum represents a single running kontinuum process registered in the
// central server registry.
type Kontinuum struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec KontinuumSpec `json:"spec"`
	// +optional
	Status KontinuumStatus `json:"status"`
}

// +kubebuilder:object:root=true

// KontinuumList is a list of Kontinuum.
type KontinuumList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Kontinuum `json:"items"`
}
