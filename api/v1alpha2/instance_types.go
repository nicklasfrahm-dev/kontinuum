package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InstanceSpec is this Instance's discovery entrypoint.
type InstanceSpec struct {
	// Interfaces lists candidate addresses (IP or hostname) this Instance's
	// controller dials, in Talos maintenance mode, to discover it — see
	// InstanceStatus.Interfaces. Ignored once ProviderRef is set: discovery
	// then comes from that provider's own controller instead.
	// +optional
	Interfaces []string `json:"interfaces,omitempty"`
	// ProviderRef points at the concrete provider object backing this
	// Instance (e.g. an AWSInstance) — see issue #24's architecture
	// decision 2/5. Unset means bare metal, discovered via Interfaces
	// above; set means a separate provider-specific controller populates
	// Status instead, and deleting this Instance cascades to deprovisioning
	// the real cloud resource via ownerRef.
	// +optional
	ProviderRef *ObjectReference `json:"providerRef,omitempty"`
}

// InstanceInterfaceStatus is one network interface discovered on this
// Instance, via Talos's COSI network resources while probing
// spec.interfaces in maintenance mode.
type InstanceInterfaceStatus struct {
	// Name is the interface's kernel name (e.g. eth0).
	Name string `json:"name"`
	// MACAddress is the interface's hardware address, when available.
	// +optional
	MACAddress string `json:"macAddress"`
	// Addresses lists this interface's discovered IP addresses (CIDR form).
	// +optional
	Addresses []string `json:"addresses"`
}

// InstanceStatus reports this Instance's discovery/probing result. Every
// field is +optional despite lacking omitempty — see KontinuumStatus's own
// doc for why: the status subresource strips whatever the main endpoint's
// Create/Update payload carries before validation runs.
type InstanceStatus struct {
	// Interfaces lists the network interfaces discovered on this Instance —
	// see InstanceInterfaceStatus. Populated by maintenance-mode probing
	// when ProviderRef is unset, or by the provider controller otherwise —
	// identical contract either way.
	// +optional
	Interfaces []InstanceInterfaceStatus `json:"interfaces"`
	// TalosVersion is the Talos version this Instance reported while being
	// probed.
	// +optional
	TalosVersion string `json:"talosVersion"`
	// Conditions reports this Instance's state. Discovered is set true once
	// one of spec.interfaces has been successfully probed.
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
// +kubebuilder:printcolumn:name="TalosVersion",type="string",JSONPath=".status.talosVersion"
// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:printcolumn:name="Discovered",type="string",JSONPath=".status.conditions[?(@.type==\"Discovered\")].status"

// Instance represents a single machine (bare metal or, via ProviderRef, a
// provisioned cloud resource) that can be claimed by an InstancePool — see
// issue #24's architecture decision 2/5. This phase's controller only
// implements discovery/probing; claiming is a later phase.
type Instance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec InstanceSpec `json:"spec"`
	// +optional
	Status InstanceStatus `json:"status"`
}

// +kubebuilder:object:root=true

// InstanceList is a list of Instance.
type InstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Instance `json:"items"`
}
