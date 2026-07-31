// +groupName=kontinuum.sh
// +kubebuilder:object:generate=true

package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the API group Kontinuum and KontinuumList belong to.
	GroupName = "kontinuum.sh"
	// APIVersion is the API version Kontinuum and KontinuumList belong to.
	APIVersion = "v1alpha2"
)

// GroupVersion is the API group and version Kontinuum and KontinuumList belong to.
func GroupVersion() schema.GroupVersion {
	return schema.GroupVersion{Group: GroupName, Version: APIVersion}
}

// AddToScheme registers Kontinuum, Zone, Instance, InstancePool,
// TalosCluster, and their List types with scheme.
func AddToScheme(scheme *runtime.Scheme) error {
	groupVersion := GroupVersion()

	scheme.AddKnownTypes(groupVersion,
		&Kontinuum{}, &KontinuumList{},
		&Zone{}, &ZoneList{},
		&Instance{}, &InstanceList{},
		&InstancePool{}, &InstancePoolList{},
		&TalosCluster{}, &TalosClusterList{},
	)
	metav1.AddToGroupVersion(scheme, groupVersion)

	return nil
}
