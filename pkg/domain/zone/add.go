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

// AddOptions is zone-add's fan-out input — pkg/cli/zone's flags and pkg/ui's
// "Add zone" form both parse straight onto this.
type AddOptions struct {
	// Region and Zone identify the new zone — together they name every
	// object BuildAddObjects creates (see that function's own doc).
	Region string
	Zone   string
	// Domain is this zone's own kontinuum-server's published domain — see
	// ZoneSpec.Domain's own doc for the <zone>.<region>.<domain> format.
	// Optional: left empty, Add infers it from any already-registered
	// Kontinuum's own published KONTINUUM_SERVER_DNS_DOMAIN (see
	// findKontinuumDomain) — exactly mirroring how the zone controller
	// itself infers the downstream storage connection string, rather than
	// requiring every caller to know or supply it.
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
	// errAddOptionsMissingField is a static sentinel — err113 flags a
	// dynamically constructed errors.New/fmt.Errorf call without a wrapped
	// static error.
	errAddOptionsMissingField = errors.New("zone add: missing required field")
	// errAddOptionsInvalidLabel is validateAddOptions' sentinel for a
	// Region/Zone value that can't be used as a DNS-1123 label component —
	// both become part of an object name (<region>-<zone>) and a label
	// value (v1alpha2.LabelRegion/LabelZone).
	errAddOptionsInvalidLabel = errors.New("zone add: invalid value")
)

// validateAddOptions checks that every required field is set and that
// Region/Zone are valid DNS-1123 label components. Domain is deliberately
// not checked here — see its own doc for why it's optional at this point,
// inferred later by Add if still empty.
func validateAddOptions(opts AddOptions) error {
	for name, value := range map[string]string{
		"region": opts.Region, "zone": opts.Zone, "talos-address": opts.TalosAddress,
	} {
		if value == "" {
			return fmt.Errorf("%w: %s", errAddOptionsMissingField, name)
		}
	}

	for name, value := range map[string]string{"region": opts.Region, "zone": opts.Zone} {
		if errs := validation.IsDNS1123Label(value); len(errs) > 0 {
			return fmt.Errorf("%w: %s %q: %s", errAddOptionsInvalidLabel, name, value, errs[0])
		}
	}

	return nil
}

// addObjectName is the shared name every one of BuildAddObjects' four
// objects gets — see that function's own doc.
func addObjectName(opts AddOptions) string {
	return opts.Region + "-" + opts.Zone
}

// BuildAddObjects builds (without creating) the four hub-side objects
// zone-add fans out to — see issue #29's architecture: Zone, the seed
// Instance, a replicas:1 InstancePool selecting it, and a TalosCluster
// whose control plane references that pool. Zone/InstancePool/TalosCluster
// all share one name, <region>-<zone> — this is what lets the Zone
// controller find "its" TalosCluster by name alone (see controller.go's
// mapTalosClusterToZone), and matches the naming convention Addon's own
// resourceName already assumes (see pkg/domain/addon/resources.go). The
// seed Instance gets its own name (suffixed -seed) since Instance is a
// distinct Kind — no collision risk — but is labeled
// v1alpha2.LabelRegion/LabelZone so the InstancePool's selector matches it.
func BuildAddObjects(
	opts AddOptions,
) (*v1alpha2.Zone, *v1alpha2.Instance, *v1alpha2.InstancePool, *v1alpha2.TalosCluster) {
	name := addObjectName(opts)
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

// Add validates opts, infers opts.Domain when left empty, and creates all
// four of BuildAddObjects' objects on hubClient, in dependency order,
// tolerating AlreadyExists on each — safe to re-run zone-add against a
// zone that's already being added or already exists. Returns the created
// (or already-existing) Zone.
func Add(ctx context.Context, hubClient client.Client, opts AddOptions) (*v1alpha2.Zone, error) {
	err := validateAddOptions(opts)
	if err != nil {
		return nil, err
	}

	if opts.Domain == "" {
		domain, err := findKontinuumDomain(ctx, hubClient)
		if err != nil {
			return nil, fmt.Errorf("failed to infer domain: %w", err)
		}

		opts.Domain = domain
	}

	zoneObj, instance, pool, cluster := BuildAddObjects(opts)

	for _, obj := range []client.Object{zoneObj, instance, pool, cluster} {
		err := hubClient.Create(ctx, obj)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create %T %q: %w", obj, obj.GetName(), err)
		}
	}

	return zoneObj, nil
}
