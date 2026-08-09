package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InstanceTemplateSpec is InstancePool.spec.template — optional, and only
// meaningful for provisionable backends. Its absence keeps InstancePool
// claim-only: existing, unclaimed Instance objects matching Selector are
// claimed up to Replicas, and running out surfaces InsufficientCapacity
// rather than conjuring new bare-metal hardware — see issue #24's
// architecture decision 2/5.
type InstanceTemplateSpec struct {
	// ProviderTemplateRef points at the *Template (e.g. AWSInstanceTemplate)
	// this pool clones from when it creates a new Instance rather than
	// claiming an existing one — never a live provider object directly.
	ProviderTemplateRef ObjectReference `json:"providerTemplateRef"`
}

// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:validation:XValidation:rule="!has(self.template) || has(self.template.providerTemplateRef)",message="template.providerTemplateRef is required when template is set"

// InstancePoolSpec selects candidate Instance objects to claim, and
// optionally how to create new ones when there aren't enough — see
// InstanceTemplateSpec's doc.
type InstancePoolSpec struct {
	// Selector matches candidate Instance objects this pool claims.
	Selector metav1.LabelSelector `json:"selector"`
	// Replicas is how many Instance objects this pool tries to keep claimed.
	Replicas int32 `json:"replicas"`
	// Template, when set, lets this pool create new Instance objects (and
	// their backing provider object) once Selector's existing candidates
	// run out. Unset keeps this pool bare-metal-only claim behavior — see
	// this type's own doc.
	// +optional
	Template *InstanceTemplateSpec `json:"template,omitempty"`
}

// InstancePoolStatus reports how many of Spec.Replicas are currently
// claimed and ready.
type InstancePoolStatus struct {
	// ReadyReplicas is how many claimed Instance objects are currently
	// Discovered/ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`
	// Conditions reports this pool's state, e.g. InsufficientCapacity.
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
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened — same convention as api/v1alpha2/kontinuum_types.go's own
// region/zone rule.
//
//nolint:lll
// +kubebuilder:printcolumn:name="InsufficientCapacity",type="string",JSONPath=".status.conditions[?(@.type==\"InsufficientCapacity\")].status"
//
//nolint:lll
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"InsufficientCapacity\")].reason"

// InstancePool claims (and optionally creates) a set of Instance objects to
// satisfy Spec.Replicas — see issue #24's architecture decision 2/5. No
// claiming logic is implemented yet; this phase only introduces the type.
type InstancePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec InstancePoolSpec `json:"spec"`
	// +optional
	Status InstancePoolStatus `json:"status"`
}

// +kubebuilder:object:root=true

// InstancePoolList is a list of InstancePool.
type InstancePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []InstancePool `json:"items"`
}
