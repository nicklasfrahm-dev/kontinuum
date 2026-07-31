package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TalosVersionSpec pins the Talos and Kubernetes versions a TalosCluster
// member pool runs. Both are optional per member — an unset value simply
// isn't validated against the skew rule below, rather than defaulting to
// the other member's version, since a default silently picked here could
// mask an operator's real intent.
type TalosVersionSpec struct {
	// Talos is the Talos version this pool's members run (e.g. "v1.9.0").
	// MaxLength bounds the CEL cost estimate for TalosClusterSpec's version
	// skew rule below — an unbounded string makes the apiserver reject the
	// rule outright as too expensive to evaluate, regardless of actual
	// input size.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Talos string `json:"talos,omitempty"`
	// Kubernetes is the Kubernetes version this pool's members run (e.g.
	// "v1.32.0"). MaxLength bounds the CEL cost estimate — see Talos's own
	// doc.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Kubernetes string `json:"kubernetes,omitempty"`
}

// TalosClusterMemberSpec is TalosCluster.spec.controlPlane.
type TalosClusterMemberSpec struct {
	// TalosVersionSpec is inlined — see that type's own doc.
	TalosVersionSpec `json:",inline"`

	// PoolRef names the InstancePool this member sizes from — see
	// InstancePoolReference's doc for why TalosCluster never carries its
	// own replicas field.
	PoolRef InstancePoolReference `json:"poolRef"`
}

// TalosClusterWorkerSpec is one entry in TalosCluster.spec.workers — a
// named worker pool, so a cluster can run more than one shape of worker
// (e.g. a gpu pool alongside the default), each independently sized by its
// own InstancePool.
type TalosClusterWorkerSpec struct {
	// TalosVersionSpec optionally overrides the control plane's own
	// versions for this worker pool — see that type's own doc.
	// +optional
	TalosVersionSpec `json:",inline"`

	// Name identifies this worker pool within the cluster (e.g. "default",
	// "gpu").
	Name string `json:"name"`
	// PoolRef names the InstancePool this worker pool sizes from.
	PoolRef InstancePoolReference `json:"poolRef"`
}

// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:validation:XValidation:rule="!has(self.workers) || self.workers.all(w, !has(w.kubernetes) || !has(self.controlPlane.kubernetes) || (w.kubernetes.split('.')[0] == self.controlPlane.kubernetes.split('.')[0] && int(self.controlPlane.kubernetes.split('.')[1]) >= int(w.kubernetes.split('.')[1]) && int(self.controlPlane.kubernetes.split('.')[1]) - int(w.kubernetes.split('.')[1]) <= 3))",message="a worker pool's kubernetes version must not lead the control plane's, and may not trail it by more than 3 minor versions"

// TalosClusterSpec describes the control-plane and worker pools that make
// up one zone's Talos-managed Kubernetes cluster — see issue #24's
// architecture decisions 1/5 (one cluster per zone) and 3/5 (control
// plane/worker split, replica counts owned by InstancePool, not here).
type TalosClusterSpec struct {
	// ControlPlane is this cluster's control-plane member pool. A
	// replicas: 1 InstancePool (no etcd quorum) is a valid, supported
	// configuration — not an edge case to guard against — see decision
	// 3/5.
	ControlPlane TalosClusterMemberSpec `json:"controlPlane"`
	// Workers lists this cluster's worker pools. The controller bootstraps
	// ControlPlane first and only starts reconciling Workers once the
	// control plane reports healthy — a worker attempting to join before
	// etcd exists must block, not race. MaxItems bounds the CEL cost
	// estimate for the version skew rule below, evaluated once per entry —
	// see TalosVersionSpec.Talos's doc for why an unbounded list has the
	// same problem an unbounded string does.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Workers []TalosClusterWorkerSpec `json:"workers,omitempty"`
}

// TalosClusterStatus reports this cluster's bootstrap progress.
type TalosClusterStatus struct {
	// Conditions reports this cluster's state, e.g. ControlPlaneReady,
	// Bootstrapped, Ready.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions" patchMergeKey:"type" patchStrategy:"merge"`
	// SecretRef points to the Secret holding this cluster's generated Talos
	// secrets bundle (key "secrets-bundle") and, once bootstrapped, its
	// kubeconfig (key "kubeconfig") — kept out of status directly for the
	// same reason KontinuumStatus.SecretRef is: a Secret's RBAC can be
	// restricted independently of who can read this broadly-visible
	// TalosCluster object.
	// +optional
	SecretRef SecretReference `json:"secretRef"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="ControlPlanePool",type="string",JSONPath=".spec.controlPlane.poolRef.name"

// TalosCluster represents one zone's real, Talos-bootstrapped Kubernetes
// cluster — see issue #24's architecture decision 1/5. No controller is
// implemented yet; this phase only introduces the type.
type TalosCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec TalosClusterSpec `json:"spec"`
	// +optional
	Status TalosClusterStatus `json:"status"`
}

// +kubebuilder:object:root=true

// TalosClusterList is a list of TalosCluster.
type TalosClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []TalosCluster `json:"items"`
}
