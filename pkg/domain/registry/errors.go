package registry

import "errors"

// ErrRegionZoneRequired is returned when exactly one of
// KONTINUUM_SERVER_REGION/KONTINUUM_SERVER_ZONE is set. Both or
// neither must be set.
var ErrRegionZoneRequired = errors.New("region and zone must both be set, or both be empty")
