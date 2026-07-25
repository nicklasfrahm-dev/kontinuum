// +groupName=kontinuum.sh
// +kubebuilder:object:generate=true

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the API group Kontinuum and KontinuumList belong to.
	GroupName = "kontinuum.sh"
	// APIVersion is the API version Kontinuum and KontinuumList belong to.
	APIVersion = "v1alpha1"
)

// GroupVersion is the API group and version Kontinuum and KontinuumList belong to.
func GroupVersion() schema.GroupVersion {
	return schema.GroupVersion{Group: GroupName, Version: APIVersion}
}

// AddToScheme registers Kontinuum and KontinuumList with s.
func AddToScheme(s *runtime.Scheme) error {
	gv := GroupVersion()

	s.AddKnownTypes(gv, &Kontinuum{}, &KontinuumList{})
	metav1.AddToGroupVersion(s, gv)

	return nil
}
