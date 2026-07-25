package registry

import (
	"fmt"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
)

// Role derives this server's registry role from region and zone. Both empty
// means this is the read-write entrypoint (v1alpha1.RoleControlPlane); both
// set means it manages that region and zone (v1alpha1.RoleWorker). Exactly
// one set is a configuration error.
func Role(region, zone string) (string, error) {
	if region == "" && zone == "" {
		return v1alpha1.RoleControlPlane, nil
	}

	if region == "" || zone == "" {
		return "", fmt.Errorf("%w: region=%q zone=%q", ErrRegionZoneRequired, region, zone)
	}

	return v1alpha1.RoleWorker, nil
}
