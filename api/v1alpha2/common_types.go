package v1alpha2

// ObjectReference identifies another object by GroupVersionKind and name —
// used wherever a spec points at a same-cluster object without embedding it
// directly (Instance.spec.providerRef, InstancePool.spec.template's
// providerTemplateRef).
type ObjectReference struct {
	// APIVersion is the referenced object's apiVersion (group/version).
	APIVersion string `json:"apiVersion"`
	// Kind is the referenced object's kind.
	Kind string `json:"kind"`
	// Name is the referenced object's name.
	Name string `json:"name"`
}

// InstancePoolReference names an InstancePool by name — TalosCluster never
// carries its own replicas field; sizing is entirely
// InstancePool.spec.replicas's job (see issue #24's architecture decision
// 3/5), so this stays a bare name reference rather than duplicating a count
// that could drift out of sync.
type InstancePoolReference struct {
	// Name is the referenced InstancePool's name.
	Name string `json:"name"`
}

// TalosClusterReference names a TalosCluster by name — mirrors
// InstancePoolReference's identical rationale, just for the parent an
// Addon belongs to.
type TalosClusterReference struct {
	// Name is the referenced TalosCluster's name.
	Name string `json:"name"`
}

// SecretReference points to a Secret holding confidential data owned by
// another object's status — kept as its own type (rather than reusing
// KontinuumSecretReference) so TalosCluster's status doesn't couple to
// Kontinuum's own type for what is otherwise an identical shape.
type SecretReference struct {
	// Name is the Secret's name.
	// +optional
	Name string `json:"name"`
	// Namespace is the Secret's namespace.
	// +optional
	Namespace string `json:"namespace"`
}
