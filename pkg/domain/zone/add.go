package zone

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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
// All four are namespaced into v1alpha2.DefaultSecretNamespace
// ("kontinuum-system") — the admin-driven, system-managed namespace issue
// #63's architecture reserves for zone-join's own objects, as opposed to a
// tenant's own namespace.
func BuildAddObjects(
	opts AddOptions,
) (*v1alpha2.Zone, *v1alpha2.Instance, *v1alpha2.InstancePool, *v1alpha2.TalosCluster) {
	name := addObjectName(opts)
	labels := map[string]string{v1alpha2.LabelRegion: opts.Region, v1alpha2.LabelZone: opts.Zone}

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.DefaultSecretNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: opts.Region, Zone: opts.Zone, Domain: opts.Domain},
	}

	instance := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: name + "-seed", Namespace: v1alpha2.DefaultSecretNamespace, Labels: labels,
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{opts.TalosAddress}},
	}

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.DefaultSecretNamespace},
		Spec: v1alpha2.InstancePoolSpec{
			Selector: metav1.LabelSelector{MatchLabels: labels},
			Replicas: 1,
		},
	}

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.DefaultSecretNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			Talos:        v1alpha2.TalosSpec{Version: opts.TalosVersion},
			Kubernetes:   v1alpha2.KubernetesSpec{Version: opts.KubernetesVersion},
			ControlPlane: v1alpha2.TalosClusterMemberSpec{PoolRef: v1alpha2.InstancePoolReference{Name: name}},
		},
	}

	return zoneObj, instance, pool, cluster
}

// Add validates opts, infers opts.Domain when left empty, and creates all
// four of BuildAddObjects' objects on hubClient, tolerating AlreadyExists on
// each — safe to re-run zone-add against a zone that's already being added
// or already exists. zoneObj is created first (and, if it already existed,
// re-fetched) so its UID is available to set as the other three's
// controller owner reference — matching TalosCluster-owns-Addon and
// TalosCluster/registry's own secrets Secrets (see
// pkg/domain/addon/resources.go and pkg/domain/taloscluster/secrets.go).
// This metadata alone doesn't cascade-delete anything today —
// libkapi.WithGarbageCollector, the mechanism that would act on it, is
// deliberately not enabled (see pkg/cli/serve.go's own doc for why: it
// took the controller manager down entirely, via the Kontinuum
// v1alpha1/v1alpha2 conversion webhook, the one time it was tried) — but
// it's the reference every owning object here is already written to
// expect, ready for whenever that's revisited. Returns the created (or
// already-existing) Zone.
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

	err = ensureNamespace(ctx, hubClient, v1alpha2.DefaultSecretNamespace)
	if err != nil {
		return nil, err
	}

	err = ensureZoneObject(ctx, hubClient, zoneObj)
	if err != nil {
		return nil, err
	}

	err = createOwnedDependents(ctx, hubClient, zoneObj, instance, pool, cluster)
	if err != nil {
		return nil, err
	}

	return zoneObj, nil
}

// ensureZoneObject creates zoneObj, or — if it already exists — re-fetches
// it so its UID (needed by createOwnedDependents' owner references) is
// populated; BuildAddObjects' own local zoneObj never carries one.
func ensureZoneObject(ctx context.Context, hubClient client.Client, zoneObj *v1alpha2.Zone) error {
	err := hubClient.Create(ctx, zoneObj)
	if err == nil {
		return nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %T %q: %w", zoneObj, zoneObj.GetName(), err)
	}

	err = hubClient.Get(ctx, client.ObjectKeyFromObject(zoneObj), zoneObj)
	if err != nil {
		return fmt.Errorf("failed to fetch already-existing zone %q: %w", zoneObj.GetName(), err)
	}

	return nil
}

// createOwnedDependents sets zoneObj as each of dependents' controller
// owner reference and creates it, tolerating AlreadyExists — see Add's own
// doc for why every dependent is owned by the Zone.
func createOwnedDependents(
	ctx context.Context, hubClient client.Client, zoneObj *v1alpha2.Zone, dependents ...client.Object,
) error {
	for _, dependent := range dependents {
		err := controllerutil.SetControllerReference(zoneObj, dependent, hubClient.Scheme())
		if err != nil {
			return fmt.Errorf("failed to set owner reference on %T %q: %w", dependent, dependent.GetName(), err)
		}

		err = hubClient.Create(ctx, dependent)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create %T %q: %w", dependent, dependent.GetName(), err)
		}
	}

	return nil
}
