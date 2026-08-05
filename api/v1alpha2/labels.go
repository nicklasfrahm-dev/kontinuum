package v1alpha2

const (
	// LabelRegion is the label key InstancePool/TalosCluster selectors match
	// candidate Instance/member objects on — see issue #24's design: Zone,
	// Instance, InstancePool, and TalosCluster are associated via labels and
	// a selector, not direct name references.
	LabelRegion = "kontinuum.sh/region"
	// LabelZone is the zone-scoped counterpart to LabelRegion.
	LabelZone = "kontinuum.sh/zone"
	// LabelClaimedBy is set on an Instance by the InstancePool that claimed
	// it, to that pool's name — see issue #24's architecture decision 2/5.
	// Claiming is a conditional (CAS) label update: Get, set this label,
	// Update; a resourceVersion conflict means another pool won the race,
	// so that candidate is skipped rather than retried.
	LabelClaimedBy = "kontinuum.sh/claimed-by"
	// LabelManagedBy marks an object as owned and reconciled by a specific
	// kontinuum controller, named by this label's value (e.g.
	// "admin-group-controller" — see pkg/domain/adminrbac.ManagedByValue).
	// Used to scope a controller's List/diff/delete loop to only the
	// objects it created, leaving anything else with the same kind alone —
	// see issue #41's "reconcile by label, not by name" requirement.
	LabelManagedBy = "kontinuum.sh/managed-by"
)
