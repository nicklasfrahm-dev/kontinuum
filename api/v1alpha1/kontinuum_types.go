package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// RoleControlPlane identifies a Kontinuum as the read-write entrypoint —
	// KONTINUUM_SERVER_REGION and KONTINUUM_SERVER_ZONE are both unset.
	RoleControlPlane = "controlplane"
	// RoleWorker identifies a Kontinuum as managing a single region and zone.
	RoleWorker = "worker"
)

// This marker's rule is a simplified has()-only check, not the fuller
// (absent-or-empty) one registry.CustomResourceDefinition actually
// enforces (see regionZoneValidationRules) — the marker comment can't wrap
// across lines, and the fuller expression doesn't fit within this repo's
// line-length limit. config/crd's generated manifest is a reference
// artifact only; the real, authoritative CRD built in Go carries the
// stricter rule.
// +kubebuilder:validation:XValidation:rule="has(self.region) == has(self.zone)",message="region/zone: both or neither"

// KontinuumSpec describes a single running kontinuum process.
type KontinuumSpec struct {
	// Role is either RoleControlPlane or RoleWorker.
	Role string `json:"role"`
	// Region is the region this process manages. Empty when Role is RoleControlPlane.
	Region string `json:"region,omitempty"`
	// Zone is the availability zone this process manages. Empty when Role is RoleControlPlane.
	Zone string `json:"zone,omitempty"`
}

// KontinuumStatus reports the last time a Kontinuum reported in.
type KontinuumStatus struct {
	// LastHeartbeatTime is when this process last reported in. The server
	// registry deletes a Kontinuum whose LastHeartbeatTime is older than
	// its configured stale threshold (5 minutes by default).
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

	Spec   KontinuumSpec   `json:"spec"`
	Status KontinuumStatus `json:"status"`
}

// +kubebuilder:object:root=true

// KontinuumList is a list of Kontinuum.
type KontinuumList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Kontinuum `json:"items"`
}
