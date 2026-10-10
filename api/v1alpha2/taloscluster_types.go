package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TalosSpec pins the Talos version this cluster's members run — one
// version for the whole cluster, not per-pool. See KubernetesSpec's own
// doc for why per-pool overrides (and the version-skew concept they'd
// imply) aren't supported.
type TalosSpec struct {
	// Version is the Talos version this cluster's members run (e.g.
	// "v1.13.0"). Empty means the reconciler's own pinned default.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Version string `json:"version,omitempty"`
}

// KubernetesSpec pins the Kubernetes version this cluster runs — one
// version for the whole cluster. Per-pool version overrides (e.g. a worker
// pool intentionally trailing the control plane during a rolling upgrade)
// aren't supported by this API: every TalosCluster this reconciler
// bootstraps today is small enough (see issue #24's architecture decision
// 3/5: single-node clusters are first-class, not an edge case) that the
// added complexity of per-pool versions and a version-skew validation rule
// isn't justified yet.
type KubernetesSpec struct {
	// Version is the Kubernetes version this cluster runs (e.g. "v1.32.0").
	// Empty means the reconciler's own pinned default.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Version string `json:"version,omitempty"`
}

// TalosClusterMemberSpec is TalosCluster.spec.controlPlane.
type TalosClusterMemberSpec struct {
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
	// Name identifies this worker pool within the cluster (e.g. "default",
	// "gpu").
	Name string `json:"name"`
	// PoolRef names the InstancePool this worker pool sizes from.
	PoolRef InstancePoolReference `json:"poolRef"`
}

// TeardownSpec configures what happens to this cluster's own claimed
// members once the cluster itself is deleted — see TalosClusterFinalizer's
// own doc for the reset-then-maybe-unregister sequence this drives.
type TeardownSpec struct {
	// UnregisterInstances, when true, deletes each of this cluster's
	// claimed Instances — after resetting them back to Talos maintenance
	// mode — instead of merely releasing them back to the free pool.
	// Defaults to false: instances stay in inventory, reset but claimable
	// again by a future cluster, unless explicitly opted out of.
	// +optional
	UnregisterInstances bool `json:"unregisterInstances,omitempty"`
}

// TalosClusterSpec describes the control-plane and worker pools that make
// up one zone's Talos-managed Kubernetes cluster — see issue #24's
// architecture decisions 1/5 (one cluster per zone) and 3/5 (control
// plane/worker split, replica counts owned by InstancePool, not here).
type TalosClusterSpec struct {
	// Talos pins this cluster's Talos version — one version for the whole
	// cluster, see TalosSpec's own doc.
	// +optional
	Talos TalosSpec `json:"talos,omitempty"`
	// Kubernetes pins this cluster's Kubernetes version — see
	// KubernetesSpec's own doc for why this isn't per-pool.
	// +optional
	Kubernetes KubernetesSpec `json:"kubernetes,omitempty"`
	// ControlPlane is this cluster's control-plane member pool. A
	// replicas: 1 InstancePool (no etcd quorum) is a valid, supported
	// configuration — not an edge case to guard against — see decision
	// 3/5.
	ControlPlane TalosClusterMemberSpec `json:"controlPlane"`
	// Workers lists this cluster's worker pools. The controller bootstraps
	// ControlPlane first and only starts reconciling Workers once the
	// control plane reports healthy — a worker attempting to join before
	// etcd exists must block, not race.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Workers []TalosClusterWorkerSpec `json:"workers,omitempty"`
	// Teardown configures what happens to this cluster's own claimed
	// members once the cluster itself is deleted — see TeardownSpec's own
	// doc.
	// +optional
	Teardown TeardownSpec `json:"teardown,omitempty"`
}

// TalosClusterVersionStatus reports one component's (Talos or Kubernetes)
// converged version across this cluster's members — the observed
// counterpart to TalosSpec/KubernetesSpec's own requested version, and
// what pkg/domain/taloscluster's upgrade reconciler compares against to
// decide whether an upgrade is owed. Deliberately a version every member
// agrees on rather than any single member's own: a rolling upgrade leaves
// the cluster genuinely split across two versions for as long as it runs,
// and reporting the first member's value there would claim the whole
// cluster had already moved.
type TalosClusterVersionStatus struct {
	// Version is the version every one of this cluster's members currently
	// reports. Empty means either nothing has been observed yet, or the
	// members disagree — mid-roll, or a member that hasn't answered a
	// probe yet — in which case the UpToDate condition's own message
	// carries the per-member detail.
	// +optional
	Version string `json:"version"`
}

// TalosClusterStatus reports this cluster's bootstrap progress.
type TalosClusterStatus struct {
	// Talos reports the Talos version this cluster's members are actually
	// running — see TalosClusterVersionStatus's own doc, and TalosSpec for
	// the requested counterpart.
	// +optional
	Talos TalosClusterVersionStatus `json:"talos"`
	// Kubernetes reports the Kubernetes version this cluster's members are
	// actually running, read from each member's own kubelet — see
	// TalosClusterVersionStatus's own doc, and KubernetesSpec for the
	// requested counterpart.
	// +optional
	Kubernetes TalosClusterVersionStatus `json:"kubernetes"`
	// Conditions reports this cluster's state, e.g. ControlPlaneReady,
	// Bootstrapped, Ready, UpToDate.
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
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="ControlPlanePool",type="string",JSONPath=".spec.controlPlane.poolRef.name"
// +kubebuilder:printcolumn:name="Talos",type="string",JSONPath=".status.talos.version"
// +kubebuilder:printcolumn:name="Kubernetes",type="string",JSONPath=".status.kubernetes.version"
// +kubebuilder:printcolumn:name="UpToDate",type="string",JSONPath=".status.conditions[?(@.type==\"UpToDate\")].status"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].reason"

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
