package zone

import (
	"context"
	"errors"
	"fmt"
	"maps"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	instancedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
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
	// inferDomain) — exactly mirroring how the zone controller itself
	// infers the downstream storage connection string, rather than
	// requiring every caller to know or supply it. Staying empty (nothing
	// registered publishes one) is not an error: this zone's own hostname
	// has no reason to match whatever domain the hub happens to publish,
	// and a Zone with no domain at all is a supported choice — see
	// controller.go's reconcileInstall for what that means at reconcile
	// time.
	Domain string
	// TalosAddress is the seed Instance's candidate address (IP or
	// hostname) — the address the instance discovery controller dials in
	// Talos maintenance mode. A hostname is resolved before it ever reaches
	// spec.interfaces[0] — see instancedomain.ResolveAddress and Add's own
	// doc — exactly like instance.AddOptions.Address's own standalone
	// registration path, so the two converge on one Instance identity for
	// a given address regardless of which one resolves the hostname.
	TalosAddress string
	// Resolver resolves TalosAddress when it isn't already an IP literal —
	// see instancedomain.ResolveAddress. Left nil (always the case for
	// pkg/cli/zone's flag parse and pkg/ui's own form parse), Add uses
	// net.DefaultResolver; tests inject a stub instead of making real DNS
	// queries.
	Resolver instancedomain.Resolver
	// TalosVersion and KubernetesVersion are optional — left empty, the
	// TalosCluster controller applies its own pinned defaults (see
	// pkg/domain/taloscluster/config.go's resolveVersions).
	TalosVersion      string
	KubernetesVersion string
	// UnregisterInstancesOnDelete sets the created TalosCluster's own
	// spec.teardown.unregisterInstances — see v1alpha2.TeardownSpec's own
	// doc. Defaults to false: this zone's instances stay in inventory,
	// reset but claimable again, when the zone is later removed.
	UnregisterInstancesOnDelete bool
	// ExistingInstanceName, when set, names an already-registered, unclaimed
	// Instance in v1alpha2.KontinuumSystemNamespace to adopt as this zone's
	// seed Instance instead of creating a new one from TalosAddress — see
	// issue #81: the UI's "Add zone" modal sets this when its own
	// instance-picker suggestion is chosen, rather than a freshly typed
	// address. TalosAddress is still shown/submitted alongside it purely for
	// display; Add itself only ever reads the adopted Instance's own
	// spec.interfaces once claimed. Left empty (the common case), Add
	// creates a brand-new Instance from TalosAddress exactly as before.
	ExistingInstanceName string
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
	// errInstanceAlreadyClaimed is adoptInstance's sentinel for
	// ExistingInstanceName already carrying a v1alpha2.LabelClaimedBy label
	// — picking an instance-picker suggestion that another zone claimed out
	// from under it between the modal opening and this submission.
	errInstanceAlreadyClaimed = errors.New("zone add: instance already claimed")
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
// resourceName already assumes (see pkg/domain/addon/resources.go). The seed
// Instance instead gets instancedomain.NameFromAddress(opts.TalosAddress) —
// the exact same name pkg/domain/instance.Add's own standalone registration
// derives for that same address (see issue #81) — rather than a name of its
// own scoped under <region>-<zone>, so the two ways of registering an
// Instance always converge on one shared object identity for a given
// address instead of each silently creating its own duplicate. This only
// holds when opts.TalosAddress is already resolved — Add itself is what
// runs it through instancedomain.ResolveAddress before ever calling this,
// so BuildAddObjects itself stays a pure, synchronous helper with no DNS
// lookup of its own; called directly (as this package's own tests do) it
// stores opts.TalosAddress verbatim. It's still labeled
// v1alpha2.LabelRegion/LabelZone so the InstancePool's selector matches it;
// ensureSeedInstance's own doc covers what happens when that name already
// belongs to an Instance created some other way.
// All four are namespaced into v1alpha2.KontinuumSystemNamespace
// ("kontinuum-system") — the admin-driven, system-managed namespace issue
// #63's architecture reserves for zone-join's own objects, as opposed to a
// tenant's own namespace.
func BuildAddObjects(
	opts AddOptions,
) (*v1alpha2.Zone, *v1alpha2.Instance, *v1alpha2.InstancePool, *v1alpha2.TalosCluster) {
	name := addObjectName(opts)
	labels := map[string]string{v1alpha2.LabelRegion: opts.Region, v1alpha2.LabelZone: opts.Zone}

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: opts.Region, Zone: opts.Zone, Domain: opts.Domain},
	}

	instanceSpec := v1alpha2.InstanceSpec{Interfaces: []string{opts.TalosAddress}}

	instance := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instancedomain.NameFromAddress(opts.TalosAddress),
			Namespace: v1alpha2.KontinuumSystemNamespace, Labels: labels,
		},
		Spec: instanceSpec,
	}

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.InstancePoolSpec{
			Selector: metav1.LabelSelector{MatchLabels: labels},
			Replicas: 1,
		},
	}

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			Talos:        v1alpha2.TalosSpec{Version: opts.TalosVersion},
			Kubernetes:   v1alpha2.KubernetesSpec{Version: opts.KubernetesVersion},
			ControlPlane: v1alpha2.TalosClusterMemberSpec{PoolRef: v1alpha2.InstancePoolReference{Name: name}},
			Teardown:     v1alpha2.TeardownSpec{UnregisterInstances: opts.UnregisterInstancesOnDelete},
		},
	}

	return zoneObj, instance, pool, cluster
}

// resolveTalosAddress resolves opts.TalosAddress via
// instancedomain.ResolveAddress and returns opts with TalosAddress replaced
// by the resolved IP, plus the hostname that was typed in (empty when
// TalosAddress was already an IP literal) — split out of Add itself purely
// to keep its own cyclomatic complexity down.
func resolveTalosAddress(ctx context.Context, opts AddOptions) (AddOptions, string, error) {
	resolvedAddress, hostname, err := instancedomain.ResolveAddress(ctx, opts.Resolver, opts.TalosAddress)
	if err != nil {
		return opts, "", fmt.Errorf("failed to resolve talos address: %w", err)
	}

	opts.TalosAddress = resolvedAddress

	return opts, hostname, nil
}

// annotateHostname sets instancedomain.AnnotationHostname on inst when
// hostname is non-empty — a no-op otherwise, so Add can call this
// unconditionally regardless of whether opts.TalosAddress was ever a
// hostname to begin with.
func annotateHostname(inst *v1alpha2.Instance, hostname string) {
	if hostname == "" {
		return
	}

	if inst.Annotations == nil {
		inst.Annotations = map[string]string{}
	}

	inst.Annotations[instancedomain.AnnotationHostname] = hostname
}

// Add validates opts, infers opts.Domain when left empty, resolves
// opts.TalosAddress to an IP if it was typed in as a hostname, and creates
// all four of BuildAddObjects' objects on hubClient, tolerating
// AlreadyExists on each — safe to re-run zone-add against a zone that's
// already being added or already exists. Ownership is a strict chain, not
// four siblings under Zone: Zone owns TalosCluster, TalosCluster owns
// InstancePool, and Instance is owned by nobody — see
// taloscluster.TalosClusterFinalizer's own doc for why. zoneObj (and, in
// turn, cluster) is created first and re-fetched if it already existed, so
// its UID is available to set as the next link's own controller owner
// reference. libkapi.WithGarbageCollector is enabled (see pkg/cli/serve.go's
// own doc), so this ownership is what actually drives cascade deletion —
// not just inert metadata. Returns the created (or already-existing) Zone.
func Add(ctx context.Context, hubClient client.Client, opts AddOptions) (*v1alpha2.Zone, error) {
	err := validateAddOptions(opts)
	if err != nil {
		return nil, err
	}

	if opts.Domain == "" {
		opts.Domain, err = inferDomain(ctx, hubClient)
		if err != nil {
			return nil, err
		}
	}

	opts, hostname, err := resolveTalosAddress(ctx, opts)
	if err != nil {
		return nil, err
	}

	zoneObj, instance, pool, cluster := BuildAddObjects(opts)
	annotateHostname(instance, hostname)

	err = ensureNamespace(ctx, hubClient, v1alpha2.KontinuumSystemNamespace)
	if err != nil {
		return nil, err
	}

	err = ensureObject(ctx, hubClient, zoneObj)
	if err != nil {
		return nil, err
	}

	// Zone owns TalosCluster — see Add's own doc for the full chain.
	// createOwnedDependents itself guarantees cluster's own UID ends up
	// populated (via ensureObject below) either way, ready to use as
	// InstancePool's own owner next.
	err = createOwnedDependents(ctx, hubClient, zoneObj, cluster)
	if err != nil {
		return nil, err
	}

	// TalosCluster owns InstancePool.
	err = createOwnedDependents(ctx, hubClient, cluster, pool)
	if err != nil {
		return nil, err
	}

	// Instance is owned by nobody — see TeardownSpec's own doc for why its
	// fate on cluster teardown is an explicit opt-in, not inferred from
	// ownership. See ensureSeedInstance's own doc for the
	// create-new-vs-adopt-existing branch (opts.ExistingInstanceName).
	err = ensureSeedInstance(ctx, hubClient, opts, instance)
	if err != nil {
		return nil, err
	}

	return zoneObj, nil
}

// ensureSeedInstance ensures newInstance's own identity exists and carries
// this zone's own region/zone labels — either explicitly, via
// opts.ExistingInstanceName naming an already-registered Instance to adopt
// (see adoptInstance's own doc) instead of creating a second one, or
// implicitly: newInstance.Name is now instancedomain.NameFromAddress(opts.TalosAddress)
// (see BuildAddObjects' own doc), the exact same name a standalone "Add
// instance" registration for that same address would also use, so a freshly
// typed address that happens to already be registered that way — or left
// over, unclaimed, from a previous zone (see instancepool.Reconciler's own
// release, which strips only v1alpha2.LabelClaimedBy) — must be adopted the
// same way an explicit pick would be, not just left unlabeled the way a bare
// tolerate-AlreadyExists create used to leave it.
func ensureSeedInstance(
	ctx context.Context, hubClient client.Client, opts AddOptions, newInstance *v1alpha2.Instance,
) error {
	if opts.ExistingInstanceName != "" {
		return adoptInstance(ctx, hubClient, opts.ExistingInstanceName, newInstance.Labels)
	}

	err := hubClient.Create(ctx, newInstance)
	if err == nil {
		return nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %T %q: %w", newInstance, newInstance.GetName(), err)
	}

	return adoptInstance(ctx, hubClient, newInstance.Name, newInstance.Labels)
}

// adoptInstance fetches the already-registered Instance named name (in
// v1alpha2.KontinuumSystemNamespace, where every zone-add Instance lives —
// see BuildAddObjects' own doc), rejects it if some other pool has already
// claimed it out from under this submission (errInstanceAlreadyClaimed), and
// merges labels (region/zone) onto it so the new InstancePool's selector
// matches it — overwriting any stale region/zone pair left over from a
// previous zone that has since released it (see instancepool.Reconciler's
// own release, which strips only v1alpha2.LabelClaimedBy, not region/zone).
func adoptInstance(ctx context.Context, hubClient client.Client, name string, labels map[string]string) error {
	var inst v1alpha2.Instance

	key := client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}

	err := hubClient.Get(ctx, key, &inst)
	if err != nil {
		return fmt.Errorf("failed to fetch existing instance %q: %w", name, err)
	}

	if _, claimed := inst.Labels[v1alpha2.LabelClaimedBy]; claimed {
		return fmt.Errorf("%w: %q", errInstanceAlreadyClaimed, name)
	}

	if inst.Labels == nil {
		inst.Labels = map[string]string{}
	}

	maps.Copy(inst.Labels, labels)

	err = hubClient.Update(ctx, &inst)
	if err != nil {
		return fmt.Errorf("failed to label existing instance %q: %w", name, err)
	}

	return nil
}

// ensureObject creates obj, or — if it already exists — re-fetches it so
// its UID (needed by createOwnedDependents' own owner references, when obj
// is about to be used as one) is populated; BuildAddObjects' own local
// objects never carry one until actually created.
func ensureObject(ctx context.Context, hubClient client.Client, obj client.Object) error {
	err := hubClient.Create(ctx, obj)
	if err == nil {
		return nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %T %q: %w", obj, obj.GetName(), err)
	}

	err = hubClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil {
		return fmt.Errorf("failed to fetch already-existing %T %q: %w", obj, obj.GetName(), err)
	}

	return nil
}

// createOwnedDependents sets owner as each of dependents' controller owner
// reference and ensures it exists (see ensureObject — creating it, or
// re-fetching it so its own UID is populated if it already did) — see
// Add's own doc for the ownership chain this builds one link of at a time.
func createOwnedDependents(
	ctx context.Context, hubClient client.Client, owner client.Object, dependents ...client.Object,
) error {
	for _, dependent := range dependents {
		err := controllerutil.SetControllerReference(owner, dependent, hubClient.Scheme())
		if err != nil {
			return fmt.Errorf("failed to set owner reference on %T %q: %w", dependent, dependent.GetName(), err)
		}

		err = ensureObject(ctx, hubClient, dependent)
		if err != nil {
			return err
		}
	}

	return nil
}
