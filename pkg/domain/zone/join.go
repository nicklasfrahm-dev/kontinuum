package zone

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// JoinOptions is zone-join's fan-out input — pkg/cli/zone's flags and (in a
// later phase) pkg/ui's join form both parse straight onto this.
type JoinOptions struct {
	// Region and Zone identify the new zone — together they name every
	// object BuildJoinObjects creates (see that function's own doc).
	Region string
	Zone   string
	// Domain is this zone's own kontinuum-server's published domain — see
	// ZoneSpec.Domain's own doc for the <zone>.<region>.<domain> format.
	Domain string
	// TalosAddress is the seed Instance's spec.interfaces[0] — the address
	// the instance discovery controller dials in Talos maintenance mode.
	TalosAddress string
	// TalosVersion and KubernetesVersion are optional — left empty, the
	// TalosCluster controller applies its own pinned defaults (see
	// pkg/domain/taloscluster/config.go's resolveVersions).
	TalosVersion      string
	KubernetesVersion string
}

var (
	// errJoinOptionsMissingField is a static sentinel — err113 flags a
	// dynamically constructed errors.New/fmt.Errorf call without a wrapped
	// static error.
	errJoinOptionsMissingField = errors.New("zone join: missing required field")
	// errJoinOptionsInvalidLabel is validateJoinOptions' sentinel for a
	// Region/Zone value that can't be used as a DNS-1123 label component —
	// both become part of an object name (<region>-<zone>) and a label
	// value (v1alpha2.LabelRegion/LabelZone).
	errJoinOptionsInvalidLabel = errors.New("zone join: invalid value")
)

// validateJoinOptions checks that every required field is set and that
// Region/Zone are valid DNS-1123 label components.
func validateJoinOptions(opts JoinOptions) error {
	for name, value := range map[string]string{
		"region": opts.Region, "zone": opts.Zone, "domain": opts.Domain, "talos-address": opts.TalosAddress,
	} {
		if value == "" {
			return fmt.Errorf("%w: %s", errJoinOptionsMissingField, name)
		}
	}

	for name, value := range map[string]string{"region": opts.Region, "zone": opts.Zone} {
		if errs := validation.IsDNS1123Label(value); len(errs) > 0 {
			return fmt.Errorf("%w: %s %q: %s", errJoinOptionsInvalidLabel, name, value, errs[0])
		}
	}

	return nil
}

// joinObjectName is the shared name every one of BuildJoinObjects' four
// objects gets — see that function's own doc.
func joinObjectName(opts JoinOptions) string {
	return opts.Region + "-" + opts.Zone
}

// BuildJoinObjects builds (without creating) the four hub-side objects
// zone-join fans out to — see issue #29's architecture: Zone, the seed
// Instance, a replicas:1 InstancePool selecting it, and a TalosCluster
// whose control plane references that pool. Zone/InstancePool/TalosCluster
// all share one name, <region>-<zone> — this is what lets the Zone
// controller find "its" TalosCluster by name alone (see controller.go's
// mapTalosClusterToZone), and matches the naming convention Addon's own
// resourceName already assumes (see pkg/domain/addon/resources.go). The
// seed Instance gets its own name (suffixed -seed) since Instance is a
// distinct Kind — no collision risk — but is labeled
// v1alpha2.LabelRegion/LabelZone so the InstancePool's selector matches it.
func BuildJoinObjects(
	opts JoinOptions,
) (*v1alpha2.Zone, *v1alpha2.Instance, *v1alpha2.InstancePool, *v1alpha2.TalosCluster) {
	name := joinObjectName(opts)
	labels := map[string]string{v1alpha2.LabelRegion: opts.Region, v1alpha2.LabelZone: opts.Zone}

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha2.ZoneSpec{Region: opts.Region, Zone: opts.Zone, Domain: opts.Domain},
	}

	instance := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-seed", Labels: labels},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{opts.TalosAddress}},
	}

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha2.InstancePoolSpec{
			Selector: metav1.LabelSelector{MatchLabels: labels},
			Replicas: 1,
		},
	}

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha2.TalosClusterSpec{
			Talos:        v1alpha2.TalosSpec{Version: opts.TalosVersion},
			Kubernetes:   v1alpha2.KubernetesSpec{Version: opts.KubernetesVersion},
			ControlPlane: v1alpha2.TalosClusterMemberSpec{PoolRef: v1alpha2.InstancePoolReference{Name: name}},
		},
	}

	return zoneObj, instance, pool, cluster
}

// Apply validates opts and creates all four of BuildJoinObjects' objects on
// hubClient, in dependency order, tolerating AlreadyExists on each — safe
// to re-run zone-join against a zone that's already joining or joined.
// Returns the created (or already-existing) Zone.
func Apply(ctx context.Context, hubClient client.Client, opts JoinOptions) (*v1alpha2.Zone, error) {
	err := validateJoinOptions(opts)
	if err != nil {
		return nil, err
	}

	zoneObj, instance, pool, cluster := BuildJoinObjects(opts)

	for _, obj := range []client.Object{zoneObj, instance, pool, cluster} {
		err := hubClient.Create(ctx, obj)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create %T %q: %w", obj, obj.GetName(), err)
		}
	}

	return zoneObj, nil
}
