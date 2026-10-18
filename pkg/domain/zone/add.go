package zone

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

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
	// TalosAddresses are the seed Instances' candidate addresses (IPs or
	// hostnames), one Instance per entry — the addresses the instance
	// discovery controller dials in Talos maintenance mode. Every one of
	// them joins the zone's control plane and runs etcd, so there must be
	// 1, 3 or 5 (see ValidateInstanceCount), each distinct once resolved
	// (see ErrDuplicateInstanceAddress). A hostname is resolved before it
	// ever reaches spec.interfaces[0] — see instancedomain.ResolveAddress
	// and Add's own doc — exactly like instance.AddOptions.Address's own
	// standalone registration path, so the two converge on one Instance
	// identity for a given address regardless of which one resolves the
	// hostname.
	TalosAddresses []string
	// Resolver resolves TalosAddresses when they aren't already IP literals —
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
	// one seed Instance instead of creating a new one from TalosAddresses —
	// see issue #81: the UI's "Add zone" modal sets this when its own
	// instance-picker suggestion is chosen, rather than a freshly typed
	// address. TalosAddresses must then hold exactly that one Instance's
	// address, submitted alongside it purely for display; Add itself only
	// ever reads the adopted Instance's own spec.interfaces once claimed.
	// Adopting several existing Instances isn't supported. Left empty (the
	// common case), Add creates one brand-new Instance per TalosAddresses
	// entry.
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
	// ErrInvalidInstanceCount is ValidateInstanceCount's sentinel for a
	// zone with an instance count other than 1, 3 or 5.
	ErrInvalidInstanceCount = errors.New(
		"a zone needs 1, 3 or 5 instances: every instance joins the control plane and runs etcd, " +
			"and an even count adds a node without adding failure tolerance " +
			"(2 tolerates no failure, like 1, while doubling the chance of one)")
	// ErrDuplicateInstanceAddress is Add's sentinel for two TalosAddresses
	// entries that name the same node once resolved.
	ErrDuplicateInstanceAddress = errors.New("zone add: duplicate instance address")
	// errExistingInstanceNeedsOneAddress is validateAddOptions' sentinel
	// for ExistingInstanceName set alongside more than one address.
	errExistingInstanceNeedsOneAddress = errors.New(
		"zone add: adopting an existing instance takes exactly one talos address")
)

// SplitTalosAddresses splits a comma-separated address list — the "Add
// zone" form's own talos-address field — into its trimmed, non-empty
// entries, the same way kontinuum zone add's own repeatable
// --talos-address flag accepts "a,b,c".
func SplitTalosAddresses(raw string) []string {
	var addresses []string

	for address := range strings.SplitSeq(raw, ",") {
		address = strings.TrimSpace(address)
		if address != "" {
			addresses = append(addresses, address)
		}
	}

	return addresses
}

// maxInstancesPerZone is the largest instance count ValidateInstanceCount
// accepts: beyond five, every extra etcd member slows down writes more
// than it adds availability.
const maxInstancesPerZone = 5

// ValidateInstanceCount enforces a zone's 1/3/5 instance rule — see
// ErrInvalidInstanceCount. Exported so kontinuum up's own validation of
// kontinuum.yml applies exactly the same limit as zone add and the UI.
func ValidateInstanceCount(count int) error {
	if count < 1 || count > maxInstancesPerZone || count%2 == 0 {
		return fmt.Errorf("%w: got %d", ErrInvalidInstanceCount, count)
	}

	return nil
}

// validateAddOptions checks that every required field is set and that
// Region/Zone are valid DNS-1123 label components. Domain is deliberately
// not checked here — see its own doc for why it's optional at this point,
// inferred later by Add if still empty.
func validateAddOptions(opts AddOptions) error {
	for name, value := range map[string]string{"region": opts.Region, "zone": opts.Zone} {
		if value == "" {
			return fmt.Errorf("%w: %s", errAddOptionsMissingField, name)
		}
	}

	err := validateTalosAddresses(opts)
	if err != nil {
		return err
	}

	for name, value := range map[string]string{"region": opts.Region, "zone": opts.Zone} {
		if errs := validation.IsDNS1123Label(value); len(errs) > 0 {
			return fmt.Errorf("%w: %s %q: %s", errAddOptionsInvalidLabel, name, value, errs[0])
		}
	}

	return nil
}

// validateTalosAddresses checks opts.TalosAddresses — see that field's own
// doc — split out of validateAddOptions purely to keep its own cyclomatic
// complexity down.
func validateTalosAddresses(opts AddOptions) error {
	if len(opts.TalosAddresses) == 0 || slices.Contains(opts.TalosAddresses, "") {
		return fmt.Errorf("%w: talos-address", errAddOptionsMissingField)
	}

	if opts.ExistingInstanceName != "" && len(opts.TalosAddresses) != 1 {
		return errExistingInstanceNeedsOneAddress
	}

	return ValidateInstanceCount(len(opts.TalosAddresses))
}

// addObjectName is the shared name every one of BuildAddObjects' four
// objects gets — see that function's own doc.
func addObjectName(opts AddOptions) string {
	return opts.Region + "-" + opts.Zone
}

// BuildAddObjects builds (without creating) the hub-side objects zone-add
// fans out to — see issue #29's architecture: Zone, one seed Instance per
// opts.TalosAddresses entry, an InstancePool selecting them with replicas
// equal to their count, and a TalosCluster whose control plane references
// that pool. Zone/InstancePool/TalosCluster all share one name,
// <region>-<zone> — this is what lets the Zone controller find "its"
// TalosCluster by name alone (see controller.go's mapTalosClusterToZone),
// and matches the naming convention Addon's own resourceName already
// assumes (see pkg/domain/addon/resources.go). Each seed Instance instead
// gets instancedomain.NameFromAddress(address) — the exact same name
// pkg/domain/instance.Add's own standalone registration derives for that
// same address (see issue #81) — rather than a name of its own scoped
// under <region>-<zone>, so the two ways of registering an Instance always
// converge on one shared object identity for a given address instead of
// each silently creating its own duplicate. This only holds when
// opts.TalosAddresses are already resolved — Add itself is what runs them
// through instancedomain.ResolveAddress before ever calling this, so
// BuildAddObjects itself stays a pure, synchronous helper with no DNS
// lookup of its own; called directly (as this package's own tests do) it
// stores each address verbatim. Every Instance is labeled
// v1alpha2.LabelRegion/LabelZone so the InstancePool's selector matches
// it; ensureSeedInstance's own doc covers what happens when that name
// already belongs to an Instance created some other way.
// All of them are namespaced into v1alpha2.KontinuumSystemNamespace
// ("kontinuum-system") — the admin-driven, system-managed namespace issue
// #63's architecture reserves for zone-join's own objects, as opposed to a
// tenant's own namespace.
func BuildAddObjects(
	opts AddOptions,
) (*v1alpha2.Zone, []*v1alpha2.Instance, *v1alpha2.InstancePool, *v1alpha2.TalosCluster) {
	name := addObjectName(opts)
	labels := map[string]string{v1alpha2.LabelRegion: opts.Region, v1alpha2.LabelZone: opts.Zone}

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: opts.Region, Zone: opts.Zone, Domain: opts.Domain},
	}

	instances := make([]*v1alpha2.Instance, 0, len(opts.TalosAddresses))

	// Counted rather than converted from len(instances), so no int to
	// int32 conversion is needed.
	var replicas int32

	for _, address := range opts.TalosAddresses {
		replicas++

		instances = append(instances, &v1alpha2.Instance{
			ObjectMeta: metav1.ObjectMeta{
				Name:      instancedomain.NameFromAddress(address),
				Namespace: v1alpha2.KontinuumSystemNamespace, Labels: maps.Clone(labels),
			},
			Spec: v1alpha2.InstanceSpec{Interfaces: []string{address}},
		})
	}

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.InstancePoolSpec{
			Selector: metav1.LabelSelector{MatchLabels: labels},
			Replicas: replicas,
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

	return zoneObj, instances, pool, cluster
}

// resolvedAddress is one TalosAddresses entry after resolution: ip is what
// lands in spec.interfaces, hostname what was typed in when it wasn't
// already an IP literal (empty otherwise).
type resolvedAddress struct {
	ip       string
	hostname string
}

// ResolveTalosAddresses resolves every one of addresses via
// instancedomain.ResolveAddress — all of them, before anything is
// created, so one unresolvable address fails the whole zone up front — and
// rejects two entries that resolve to the same IP with
// ErrDuplicateInstanceAddress naming both. Exported so kontinuum up can
// check addresses fleet-wide with exactly the resolution zone add uses.
func ResolveTalosAddresses(
	ctx context.Context, resolver instancedomain.Resolver, addresses []string,
) ([]string, []string, error) {
	resolved, err := resolveTalosAddresses(ctx, resolver, addresses)
	if err != nil {
		return nil, nil, err
	}

	ips := make([]string, len(resolved))
	hostnames := make([]string, len(resolved))

	for index, entry := range resolved {
		ips[index], hostnames[index] = entry.ip, entry.hostname
	}

	return ips, hostnames, nil
}

// resolveTalosAddresses is ResolveTalosAddresses' own implementation.
func resolveTalosAddresses(
	ctx context.Context, resolver instancedomain.Resolver, addresses []string,
) ([]resolvedAddress, error) {
	resolved := make([]resolvedAddress, 0, len(addresses))
	seen := map[string]string{}

	for _, address := range addresses {
		resolvedIP, hostname, err := instancedomain.ResolveAddress(ctx, resolver, address)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve talos address: %w", err)
		}

		if first, duplicate := seen[resolvedIP]; duplicate {
			return nil, fmt.Errorf("%w: %q and %q both resolve to %s",
				ErrDuplicateInstanceAddress, first, address, resolvedIP)
		}

		seen[resolvedIP] = address

		resolved = append(resolved, resolvedAddress{ip: resolvedIP, hostname: hostname})
	}

	return resolved, nil
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

// Add validates opts, infers opts.Domain when left empty, resolves every
// one of opts.TalosAddresses to an IP if typed in as a hostname — all of
// them up front, so one unresolvable or duplicate address creates nothing
// at all — and creates BuildAddObjects' objects on hubClient, tolerating
// AlreadyExists on each — safe to re-run zone-add against a zone that's
// already being added or already exists: existing seed Instances are
// adopted, not duplicated. Ownership is a strict chain, not siblings under
// Zone: Zone owns TalosCluster, TalosCluster owns InstancePool, and the
// Instances are owned by nobody — see taloscluster.TalosClusterFinalizer's
// own doc for why. zoneObj (and, in turn, cluster) is created first and
// re-fetched if it already existed, so its UID is available to set as the
// next link's own controller owner reference. libkapi.WithGarbageCollector
// is enabled (see pkg/cli/serve.go's own doc), so this ownership is what
// actually drives cascade deletion — not just inert metadata. Returns the
// created (or already-existing) Zone.
func Add(ctx context.Context, hubClient client.Client, opts AddOptions) (*v1alpha2.Zone, error) {
	err := validateAddOptions(opts)
	if err != nil {
		return nil, err
	}

	resolved, err := resolveTalosAddresses(ctx, opts.Resolver, opts.TalosAddresses)
	if err != nil {
		return nil, err
	}

	if opts.Domain == "" {
		opts.Domain, err = inferDomain(ctx, hubClient)
		if err != nil {
			return nil, err
		}
	}

	opts.TalosAddresses = make([]string, len(resolved))
	for index, entry := range resolved {
		opts.TalosAddresses[index] = entry.ip
	}

	zoneObj, instances, pool, cluster := BuildAddObjects(opts)
	for index, instance := range instances {
		annotateHostname(instance, resolved[index].hostname)
	}

	err = createAddObjects(ctx, hubClient, opts, zoneObj, instances, pool, cluster)
	if err != nil {
		return nil, err
	}

	return zoneObj, nil
}

// createAddObjects creates BuildAddObjects' objects in ownership order —
// see Add's own doc — split out of Add purely to keep its own cyclomatic
// complexity down.
func createAddObjects(
	ctx context.Context, hubClient client.Client, opts AddOptions, zoneObj *v1alpha2.Zone,
	instances []*v1alpha2.Instance, pool *v1alpha2.InstancePool, cluster *v1alpha2.TalosCluster,
) error {
	err := ensureNamespace(ctx, hubClient, v1alpha2.KontinuumSystemNamespace)
	if err != nil {
		return err
	}

	err = ensureObject(ctx, hubClient, zoneObj)
	if err != nil {
		return err
	}

	// Zone owns TalosCluster — see Add's own doc for the full chain.
	// createOwnedDependents itself guarantees cluster's own UID ends up
	// populated (via ensureObject below) either way, ready to use as
	// InstancePool's own owner next.
	err = createOwnedDependents(ctx, hubClient, zoneObj, cluster)
	if err != nil {
		return err
	}

	// TalosCluster owns InstancePool.
	err = createOwnedDependents(ctx, hubClient, cluster, pool)
	if err != nil {
		return err
	}

	// Instances are owned by nobody — see TeardownSpec's own doc for why
	// their fate on cluster teardown is an explicit opt-in, not inferred
	// from ownership. See ensureSeedInstance's own doc for the
	// create-new-vs-adopt-existing branch (opts.ExistingInstanceName).
	for _, instance := range instances {
		err = ensureSeedInstance(ctx, hubClient, opts, pool.Name, instance)
		if err != nil {
			return err
		}
	}

	return nil
}

// ensureSeedInstance ensures newInstance's own identity exists and carries
// this zone's own region/zone labels — either explicitly, via
// opts.ExistingInstanceName naming an already-registered Instance to adopt
// (see adoptInstance's own doc) instead of creating a second one, or
// implicitly: newInstance.Name is instancedomain.NameFromAddress(address)
// (see BuildAddObjects' own doc), the exact same name a standalone "Add
// instance" registration for that same address would also use, so a freshly
// typed address that happens to already be registered that way — or left
// over, unclaimed, from a previous zone (see instancepool.Reconciler's own
// release, which strips only v1alpha2.LabelClaimedBy) — must be adopted the
// same way an explicit pick would be, not just left unlabeled the way a bare
// tolerate-AlreadyExists create used to leave it.
func ensureSeedInstance(
	ctx context.Context, hubClient client.Client, opts AddOptions, poolName string, newInstance *v1alpha2.Instance,
) error {
	if opts.ExistingInstanceName != "" {
		return adoptInstance(ctx, hubClient, opts.ExistingInstanceName, poolName, newInstance.Labels)
	}

	err := hubClient.Create(ctx, newInstance)
	if err == nil {
		return nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %T %q: %w", newInstance, newInstance.GetName(), err)
	}

	return adoptInstance(ctx, hubClient, newInstance.Name, poolName, newInstance.Labels)
}

// adoptInstance fetches the already-registered Instance named name (in
// v1alpha2.KontinuumSystemNamespace, where every zone-add Instance lives —
// see BuildAddObjects' own doc), rejects it if some other pool has already
// claimed it out from under this submission (errInstanceAlreadyClaimed), and
// merges labels (region/zone) onto it so the new InstancePool's selector
// matches it — overwriting any stale region/zone pair left over from a
// previous zone that has since released it (see instancepool.Reconciler's
// own release, which strips only v1alpha2.LabelClaimedBy, not region/zone).
//
// An Instance already claimed by poolName itself is this same zone's own,
// adopted by an earlier or concurrent run of the same zone add — e.g. two
// kontinuum ups creating the same declared zone at once — so it's left as
// is rather than rejected.
func adoptInstance(
	ctx context.Context, hubClient client.Client, name, poolName string, labels map[string]string,
) error {
	var inst v1alpha2.Instance

	key := client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}

	err := hubClient.Get(ctx, key, &inst)
	if err != nil {
		return fmt.Errorf("failed to fetch existing instance %q: %w", name, err)
	}

	claimedBy, claimed := inst.Labels[v1alpha2.LabelClaimedBy]
	if claimed && claimedBy == poolName {
		return nil
	}

	if claimed {
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
