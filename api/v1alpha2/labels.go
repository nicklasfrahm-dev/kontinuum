package v1alpha2

const (
	// LabelRegion is the label key InstancePool/TalosCluster selectors match
	// candidate Instance/member objects on — see issue #24's design: Zone,
	// Instance, InstancePool, and TalosCluster are associated via labels and
	// a selector, not direct name references.
	LabelRegion = "kontinuum.sh/region"
	// LabelZone is the zone-scoped counterpart to LabelRegion.
	LabelZone = "kontinuum.sh/zone"
)
