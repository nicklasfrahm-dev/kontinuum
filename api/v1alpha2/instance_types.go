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

// InstanceTalosStatus groups every Talos-reported status value under
// status.talos — currently just Version, but the natural home for
// anything else Talos itself reports about this Instance later.
type InstanceTalosStatus struct {
	// Version is the Talos version this Instance reported. Left empty by
	// maintenance-mode discovery itself — current Talos releases reject the
	// maintenance-mode Version RPC for any caller without an admin identity,
	// which no not-yet-configured node can present — and instead populated
	// later, once this Instance is claimed by a TalosCluster and its own
	// config-apply gives it a real one (see pkg/domain/taloscluster's
	// recordTalosVersions).
	// +optional
	Version string `json:"version"`
}

// InstanceDiskStatus is one disk discovered on this Instance, via Talos's
// COSI block.Disk resource while probing spec.interfaces in maintenance
// mode — see issue #76. A one-shot snapshot from initial discovery, not
// re-probed afterward (a disk swap after discovery isn't detected).
type InstanceDiskStatus struct {
	// DevPath is the disk's kernel device path (e.g. /dev/sda).
	DevPath string `json:"devPath"`
	// Size is the disk's size in bytes.
	// +optional
	Size uint64 `json:"size"`
	// PrettySize is Size in human-readable form (e.g. "512 GB"), as Talos
	// itself formats it.
	// +optional
	PrettySize string `json:"prettySize"`
	// Model is the disk's reported model name, when available.
	// +optional
	Model string `json:"model"`
	// Serial is the disk's reported serial number, when available.
	// +optional
	Serial string `json:"serial"`
	// Transport is the disk's bus/transport type (e.g. nvme, sata), when
	// available.
	// +optional
	Transport string `json:"transport"`
	// Rotational is true for spinning disks, false for solid-state ones.
	// +optional
	Rotational bool `json:"rotational"`
}

// InstanceCPUStatus is one processor socket discovered on this Instance, via
// Talos's COSI hardware.Processor resource — see InstanceDiskStatus's own
// doc for the one-shot-snapshot caveat, which applies here too.
type InstanceCPUStatus struct {
	// Manufacturer is the processor's reported manufacturer, when available.
	// +optional
	Manufacturer string `json:"manufacturer"`
	// ProductName is the processor's reported model name, when available.
	// +optional
	ProductName string `json:"productName"`
	// Architecture is the node's own instruction-set architecture (e.g.
	// amd64, arm64) — reported once per node via Talos's Version RPC, not
	// per-socket, so every discovered CPU carries the same value. Same
	// best-effort caveat as InstanceTalosStatus.Version: left empty
	// whenever that RPC is unavailable in maintenance mode.
	// +optional
	Architecture string `json:"architecture"`
	// CoreCount is the processor's physical core count.
	// +optional
	CoreCount uint32 `json:"coreCount"`
	// ThreadCount is the processor's logical (hyperthreaded) core count.
	// +optional
	ThreadCount uint32 `json:"threadCount"`
	// MaxSpeedMHz is the processor's maximum clock speed, in megahertz.
	// +optional
	MaxSpeedMHz uint32 `json:"maxSpeedMhz"`
}

// InstanceMemoryStatus is one memory module (DIMM) discovered on this
// Instance, via Talos's COSI hardware.MemoryModule resource — see
// InstanceDiskStatus's own doc for the one-shot-snapshot caveat, which
// applies here too.
type InstanceMemoryStatus struct {
	// SizeMiB is the module's capacity, in mebibytes.
	// +optional
	SizeMiB uint32 `json:"sizeMib"`
	// Manufacturer is the module's reported manufacturer, when available.
	// +optional
	Manufacturer string `json:"manufacturer"`
	// Speed is the module's reported speed in megatransfers per second
	// (MT/s), when available.
	// +optional
	Speed uint32 `json:"speed"`
	// Serial is the module's reported serial number, when available.
	// +optional
	Serial string `json:"serial"`
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
	// Disks lists the disks discovered on this Instance — see
	// InstanceDiskStatus. Populated whenever Interfaces is (re)populated —
	// see pkg/domain/instance's probeCandidates for exactly when that is:
	// on first discovery, and again on any later rediscovery (e.g. after a
	// reboot briefly took this Instance's Discovered condition back to
	// false), but not on a steady-state recheck of an already-Discovered
	// Instance, so a disk swap between reboots isn't detected.
	// +optional
	Disks []InstanceDiskStatus `json:"disks"`
	// CPUs lists the processor sockets discovered on this Instance — see
	// InstanceCPUStatus and Disks' own doc for exactly when this is
	// (re)populated.
	// +optional
	CPUs []InstanceCPUStatus `json:"cpus"`
	// Memory lists the memory modules discovered on this Instance — see
	// InstanceMemoryStatus and Disks' own doc for exactly when this is
	// (re)populated.
	// +optional
	Memory []InstanceMemoryStatus `json:"memory"`
	// SerialNumber is the machine's own chassis/board serial number, read
	// from SMBIOS via Talos's COSI hardware.SystemInformation resource —
	// distinct from InstanceDiskStatus.Serial, which is per-disk. See
	// Disks' own doc for exactly when this is (re)populated.
	// +optional
	SerialNumber string `json:"serialNumber"`
	// Talos groups every Talos-reported status value — see
	// InstanceTalosStatus's own doc.
	// +optional
	Talos InstanceTalosStatus `json:"talos"`
	// LastProbeTime is when this Instance's liveness (see the Live
	// condition below) was last actively checked — distinct from a
	// condition's own LastTransitionTime, which only updates on a status
	// flip. Mirrors corev1.PodCondition.LastProbeTime and
	// KontinuumStatus.LastHeartbeatTime's own precedent.
	// +optional
	LastProbeTime metav1.Time `json:"lastProbeTime"`
	// Conditions reports this Instance's state. Discovered is set true once
	// one of spec.interfaces has been successfully probed — see
	// pkg/domain/instance's Reconciler. Live tracks the same liveness
	// signal for this Instance's entire lifecycle, unclaimed candidate
	// through full cluster member — see pkg/domain/instance's
	// DiscoveredConditionType and pkg/domain/taloscluster's own
	// MemberLiveConditionType docs. Once claimed by an InstancePool and
	// picked up by a TalosCluster's member reconciler, three more get set
	// there, tracking bootstrap progress: Configured (machine config
	// applied), Joined (node rejoined with its real post-config identity),
	// and Ready (control-plane members only — mirrors the TalosCluster's
	// own cluster-wide health check for this specific node; see
	// pkg/domain/taloscluster's own condition docs for why workers don't
	// get one yet).
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions" patchMergeKey:"type" patchStrategy:"merge"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="TalosVersion",type="string",JSONPath=".status.talos.version"
// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:printcolumn:name="Discovered",type="string",JSONPath=".status.conditions[?(@.type==\"Discovered\")].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Discovered\")].reason"
// +kubebuilder:printcolumn:name="Live",type="string",JSONPath=".status.conditions[?(@.type==\"Live\")].status"
// +kubebuilder:printcolumn:name="Configured",type="string",JSONPath=".status.conditions[?(@.type==\"Configured\")].status"
// +kubebuilder:printcolumn:name="Joined",type="string",JSONPath=".status.conditions[?(@.type==\"Joined\")].status"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"

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
