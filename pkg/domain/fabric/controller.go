package fabric

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	// ValidSpecConditionType is set true once spec.cidr/spec.zonePrefixLength
	// parse and the IPAM allocator (see Allocate) successfully carves a
	// subnet for every currently live zone in spec.region — this fabric's
	// own terminal gate, both directions mirrored onto ReadyConditionType
	// (see updateValidSpecCondition's own doc), since nothing else at the
	// fabric level blocks Ready — per-zone gateway/NAT progress is tracked
	// on each entry's own status.zones[].conditions instead (see
	// GatewayNodeSelectedConditionType and friends), deliberately not
	// blocking the fabric-wide Ready the same way one zone's own network
	// failure never affects another's (see this package's own doc).
	ValidSpecConditionType = "ValidSpec"
	// ReadyConditionType mirrors ValidSpecConditionType's own content but
	// with conventional Ready polarity — a CRD with no Kind-specific
	// handling only renders a populated READY/REASON/STATUS column in
	// `kubectl tree` if status.conditions carries an entry literally Typed
	// "Ready" (mirrors zone.ReadyConditionType's own reasoning).
	ReadyConditionType = "Ready"

	// GatewayNodeSelectedConditionType is one zone entry's own
	// (status.zones[].conditions) condition set once spec.gatewaySelector
	// matches at least one claimed Instance in that zone — see
	// resolveGatewayNode.
	GatewayNodeSelectedConditionType = "GatewayNodeSelected"
	// NetworkConfiguredConditionType is one zone entry's own condition —
	// set by pkg/cli/fabricmanager itself, not this controller (see
	// reconcileNATForGatewayNode's own doc): this controller only
	// publishes desired state and delivers the Talos credential needed to
	// apply it, never observing whether that application actually
	// succeeded.
	NetworkConfiguredConditionType = "NetworkConfigured"
	// NATInstalledConditionType is one zone entry's own condition, set by
	// pkg/cli/fabricmanager itself once its own NAT-masquerade rule is
	// actually running — see NetworkConfiguredConditionType's own doc for
	// why this controller never sets it.
	NATInstalledConditionType = "NATInstalled"
	// ZoneReadyConditionType is one zone entry's own aggregate condition —
	// mirrors ReadyConditionType's identical "kubectl-tree needs a literal
	// Ready-Typed condition" reasoning, scoped to this one status.zones[]
	// entry instead of the whole Fabric. Set by this controller only for
	// the paths it fully owns (no gateway candidate, NAT disabled);
	// otherwise pkg/cli/fabricmanager's own write-back owns it, the same
	// as NetworkConfiguredConditionType/NATInstalledConditionType.
	ZoneReadyConditionType = "Ready"

	reasonInvalidSpec         = "InvalidSpec"
	reasonValidSpec           = "ValidSpec"
	reasonNoGatewayCandidate  = "NoGatewayCandidate"
	reasonGatewayNodeSelected = "GatewayNodeSelected"
	reasonNetworkConfigFailed = "NetworkConfigFailed"
	reasonNATDisabled         = "NATDisabled"
	reasonZoneNotReady        = "ZoneNotReady"

	defaultRetryInterval = 15 * time.Second
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// DownstreamClientBuilder builds a client.Client against a zone's own
	// cluster from its stored kubeconfig — reused directly from
	// pkg/domain/zone rather than a second copy of the identical seam.
	// Defaults to zone.NewDownstreamClientBuilder() when nil.
	DownstreamClientBuilder zone.DownstreamClientBuilder
	// RetryInterval is how long Reconcile waits before retrying a step
	// that hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
	// ZoneLease is this process's own zonelease.Locker identity — see
	// zonelease.Identity's own doc. Fabric is region-scoped, hub-owned
	// fleet state (not any one zone's own resource — issue #24's "one
	// cluster per zone" decision has no zone-scoped storage a Fabric could
	// hang off of instead), so Reconcile always acquires
	// zonelease.GlobalKey — mirrors pkg/domain/adminrbac's identical
	// reasoning for its own hub-owned ClusterRoleBindings.
	ZoneLease zonelease.Identity
}

// Controller wires the Fabric IPAM/gateway reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting
// DownstreamClientBuilder and RetryInterval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.DownstreamClientBuilder == nil {
		cfg.DownstreamClientBuilder = zone.NewDownstreamClientBuilder()
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the Fabric reconciler on mgr. fabrics.kontinuum.sh's
// CRD itself is ensured separately, via instance.EnsureCRDs (see
// pkg/cli/serve.go) — not here, mirroring every other domain controller in
// this repo.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:                  mgr.GetClient(),
		DownstreamClientBuilder: c.Config.DownstreamClientBuilder,
		RetryInterval:           c.Config.RetryInterval,
		Locker: zonelease.NewLocker(
			mgr.GetClient(), mgr.GetAPIReader(), c.Config.ZoneLease.HolderIdentity, c.Config.ZoneLease.SelfZoneKey, 0),
		Logger: c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.Fabric{}).
		Watches(&v1alpha2.Zone{}, handler.EnqueueRequestsFromMapFunc(reconciler.mapZoneToFabrics)).
		Watches(&v1alpha2.Instance{}, handler.EnqueueRequestsFromMapFunc(reconciler.mapInstanceToFabrics)).
		Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register fabric controller: %w", err)
	}

	return nil
}

// requestsFor converts fabrics into reconcile Requests.
func requestsFor(fabrics []v1alpha2.Fabric) []ctrl.Request {
	requests := make([]ctrl.Request, 0, len(fabrics))
	for index := range fabrics {
		requests = append(requests, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: fabrics[index].Name, Namespace: fabrics[index].Namespace},
		})
	}

	return requests
}

// Reconciler carves per-zone subnets/gateway IPs out of a Fabric's own CIDR
// and, once NAT is enabled, elects and configures each zone's own NAT
// gateway node — see this package's own doc.
type Reconciler struct {
	Client                  client.Client
	DownstreamClientBuilder zone.DownstreamClientBuilder
	RetryInterval           time.Duration
	// Locker gates every write below against zonelease — see
	// Config.ZoneLease's own doc.
	Locker *zonelease.Locker
	Logger *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var fabricObj v1alpha2.Fabric

	err := r.Client.Get(ctx, req.NamespacedName, &fabricObj)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get fabric %q: %w", req.Name, err)
	}

	acquired, err := r.Locker.TryAcquire(ctx, zonelease.GlobalKey)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to acquire fabric lease for %q: %w", fabricObj.Name, err)
	}

	if !acquired {
		return ctrl.Result{RequeueAfter: zonelease.Jitter(r.RetryInterval)}, nil
	}

	return r.reconcileFabric(ctx, &fabricObj)
}

// mapZoneToFabrics enqueues every Fabric whose own spec.region matches the
// changed Zone's spec.region — a Zone appearing, disappearing, or changing
// region can change which zones a Fabric should carve a subnet for. No
// field indexer: zone/fabric counts are both small (see the issue's own
// design), so a plain List-and-filter stays cheap, the same trade-off
// mapKontinuumVersionChangeToAllZones already accepts.
func (r *Reconciler) mapZoneToFabrics(ctx context.Context, obj client.Object) []ctrl.Request {
	zoneObj, ok := obj.(*v1alpha2.Zone)
	if !ok {
		return nil
	}

	return r.listFabricsForRegion(ctx, zoneObj.Namespace, zoneObj.Spec.Region)
}

// mapInstanceToFabrics enqueues every Fabric in the changed Instance's own
// namespace whenever a kontinuum.sh/zone-labeled Instance changes — a
// gateway candidate appearing, disappearing, or losing its claimed-by label
// can change gateway node selection for whichever Fabric owns that
// Instance's own zone. Broad (every Fabric in the namespace, not narrowed
// to the one whose region actually matches this Instance's own zone) for
// the same "small object count, cheap to over-enqueue" reasoning
// mapZoneToFabrics documents — narrowing further would need to resolve the
// Instance's own zone back to a region via a Zone lookup, for no real
// savings at this scale.
func (r *Reconciler) mapInstanceToFabrics(ctx context.Context, obj client.Object) []ctrl.Request {
	instanceObj, ok := obj.(*v1alpha2.Instance)
	if !ok {
		return nil
	}

	if _, hasZoneLabel := instanceObj.Labels[v1alpha2.LabelZone]; !hasZoneLabel {
		return nil
	}

	var list v1alpha2.FabricList

	err := r.Client.List(ctx, &list, client.InNamespace(instanceObj.Namespace))
	if err != nil {
		r.Logger.Error("failed to list fabrics to propagate instance change", "error", err)

		return nil
	}

	return requestsFor(list.Items)
}

// listFabricsForRegion lists every Fabric in namespace whose spec.region
// equals region, returning them as reconcile Requests.
func (r *Reconciler) listFabricsForRegion(ctx context.Context, namespace, region string) []ctrl.Request {
	var list v1alpha2.FabricList

	err := r.Client.List(ctx, &list, client.InNamespace(namespace))
	if err != nil {
		r.Logger.Error("failed to list fabrics to propagate zone change", "error", err)

		return nil
	}

	matching := make([]v1alpha2.Fabric, 0, len(list.Items))

	for _, fabricObj := range list.Items {
		if fabricObj.Spec.Region == region {
			matching = append(matching, fabricObj)
		}
	}

	return requestsFor(matching)
}

// reconcileFabric runs the IPAM allocator against every zone in
// spec.region currently live, then reconciles each entry's own gateway
// node election and (when NAT is enabled) network config/NAT workload —
// see this package's own doc for the overall design.
func (r *Reconciler) reconcileFabric(ctx context.Context, fabricObj *v1alpha2.Fabric) (ctrl.Result, error) {
	zonesByName, err := r.listZonesForRegion(ctx, fabricObj)
	if err != nil {
		return ctrl.Result{}, err
	}

	liveZones := make([]string, 0, len(zonesByName))
	for zoneName := range zonesByName {
		liveZones = append(liveZones, zoneName)
	}

	err = r.ensureZonePrefixLengthDefaulted(ctx, fabricObj, len(liveZones))
	if err != nil {
		return ctrl.Result{}, err
	}

	previous := make([]PreviousAllocation, 0, len(fabricObj.Status.Zones))
	for _, zoneStatus := range fabricObj.Status.Zones {
		previous = append(previous, PreviousAllocation{Zone: zoneStatus.Zone, CIDR: zoneStatus.CIDR})
	}

	allocations, err := Allocate(fabricObj.Spec.CIDR, fabricObj.Spec.ZonePrefixLength, liveZones, previous)
	if err != nil {
		conditionChanged := r.updateValidSpecCondition(fabricObj, metav1.ConditionFalse, reasonInvalidSpec, err.Error())

		return r.persistZoneStatuses(ctx, fabricObj, conditionChanged, true)
	}

	newZones, zonesSettled := r.reconcileZoneStatuses(ctx, fabricObj, zonesByName, allocations)

	zonesChanged := !equalZoneStatuses(fabricObj.Status.Zones, newZones)
	fabricObj.Status.Zones = newZones

	conditionChanged := r.updateValidSpecCondition(
		fabricObj, metav1.ConditionTrue, reasonValidSpec, "fabric spec is valid")

	return r.persistZoneStatuses(ctx, fabricObj, conditionChanged || zonesChanged, zonesSettled)
}

// ensureZonePrefixLengthDefaulted defaults fabricObj's own
// spec.zonePrefixLength, once, the first time it's reconciled with the
// field left unset (its zero value) — computed from zoneCount, the number
// of zones currently live in this fabric's own region (see
// defaultZonePrefixLength's own package-level doc for the actual sizing
// rule). Persisted back onto spec.zonePrefixLength itself, not just used
// in memory this one pass — the same "mutate then Update, then keep going
// in this same reconcile" shape Reconcile's own AddFinalizer step already
// uses — so every following reconcile finds the field already set and
// skips this entirely, mirroring a one-time admission-time default
// without an actual admission webhook (this repo has none). A CIDR that
// fails to parse is left alone here: Allocate's own identical
// net.ParseCIDR call, reached right after this returns, surfaces that
// same error through the ordinary InvalidSpec path instead.
func (r *Reconciler) ensureZonePrefixLengthDefaulted(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneCount int,
) error {
	if fabricObj.Spec.ZonePrefixLength != 0 {
		return nil
	}

	zonePrefixLength, err := defaultZonePrefixLength(fabricObj.Spec.CIDR, zoneCount)
	if err != nil {
		return nil //nolint:nilerr // invalid CIDR: Allocate's own identical parse surfaces this through InvalidSpec instead
	}

	fabricObj.Spec.ZonePrefixLength = zonePrefixLength

	err = r.Client.Update(ctx, fabricObj)
	if err != nil {
		return fmt.Errorf("failed to default zonePrefixLength for fabric %q: %w", fabricObj.Name, err)
	}

	return nil
}

// listZonesForRegion lists every Zone in fabricObj's own namespace whose
// spec.region matches, keyed by spec.zone (the bare zone identifier this
// package's own status.zones[].zone records — not the Zone object's own
// metadata.name, the <region>-<zone> convention used to look up its
// TalosCluster — see reconcileGatewayWorkload).
func (r *Reconciler) listZonesForRegion(
	ctx context.Context, fabricObj *v1alpha2.Fabric,
) (map[string]v1alpha2.Zone, error) {
	var list v1alpha2.ZoneList

	err := r.Client.List(ctx, &list, client.InNamespace(fabricObj.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to list zones for fabric %q: %w", fabricObj.Name, err)
	}

	zonesByName := make(map[string]v1alpha2.Zone, len(list.Items))

	for _, zoneObj := range list.Items {
		if zoneObj.Spec.Region == fabricObj.Spec.Region {
			zonesByName[zoneObj.Spec.Zone] = zoneObj
		}
	}

	return zonesByName, nil
}

// reconcileZoneStatuses builds the new status.zones slice from allocations,
// carrying forward each entry's own previously recorded GatewayNodeRef and
// Conditions (by zone name) so gateway election and its own conditions stay
// sticky across reconciles — see resolveGatewayNode. Returns the new slice
// (in allocations' own sorted-by-zone-name order — see Allocate's own doc)
// and whether every entry's own ZoneReadyConditionType is currently True.
func (r *Reconciler) reconcileZoneStatuses(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zonesByName map[string]v1alpha2.Zone, allocations []Allocation,
) ([]v1alpha2.FabricZoneStatus, bool) {
	previousByName := make(map[string]v1alpha2.FabricZoneStatus, len(fabricObj.Status.Zones))
	for _, zoneStatus := range fabricObj.Status.Zones {
		previousByName[zoneStatus.Zone] = zoneStatus
	}

	newZones := make([]v1alpha2.FabricZoneStatus, 0, len(allocations))
	settled := true

	for _, alloc := range allocations {
		previous, hadPrevious := previousByName[alloc.Zone]

		entry := v1alpha2.FabricZoneStatus{Zone: alloc.Zone, CIDR: alloc.CIDR, GatewayIP: alloc.GatewayIP}
		if hadPrevious {
			entry.GatewayNodeRef = previous.GatewayNodeRef
			entry.GatewayInterfaces = previous.GatewayInterfaces
			// A plain slice assignment here would alias the same backing
			// array as fabricObj.Status.Zones[i].Conditions — the "before"
			// snapshot equalZoneStatuses compares against below. meta.
			// SetStatusCondition (called throughout this zone's own
			// reconcile below) mutates an existing condition's fields in
			// place through a pointer into that array, which would
			// silently rewrite the "before" snapshot to match "after"
			// before the comparison ever runs, hiding real transitions
			// from change detection. Copying the slice keeps the two
			// arrays independent.
			entry.Conditions = append([]metav1.Condition(nil), previous.Conditions...)
		}

		r.reconcileZoneEntry(ctx, fabricObj, zonesByName[alloc.Zone], &entry)

		if !meta.IsStatusConditionTrue(entry.Conditions, ZoneReadyConditionType) {
			settled = false
		}

		newZones = append(newZones, entry)
	}

	return newZones, settled
}

// reconcileZoneEntry resolves entry's own gateway node and, once NAT is
// enabled and a node is resolved, publishes its desired network config
// and delivers the Talos credential needed to apply it — mutating entry's
// own GatewayNodeRef/Conditions in place. A failure at any step is caught
// and reported as that step's own False condition (logged as a warning),
// never propagated as a hard Reconcile error: every failure mode here (no
// candidate yet, the zone's TalosCluster not bootstrapped yet, its gateway
// node unreachable) is expected, ordinary "not converged yet" state that
// the next requeue retries — see zone.Reconciler.reconcileInstall's
// identical tolerance for the same class of "not ready yet" failure.
//
// A gateway node re-elected away from a previous one needs no explicit
// teardown here: pkg/cli/fabricmanager's own reconcile loop already
// notices, on its own next pass, that it's no longer this zone's own
// gatewayNodeRef for any live Fabric, and prunes its own stale state
// accordingly (see that package's own Reconciler and
// PruneStaleMasqueradeTables) — this controller no longer manages any
// per-node workload lifecycle to tear down in the first place.
func (r *Reconciler) reconcileZoneEntry(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneObj v1alpha2.Zone, entry *v1alpha2.FabricZoneStatus,
) {
	gatewayNode, resolved := r.recordGatewayNodeSelection(ctx, fabricObj, entry, zoneObj.Spec.Zone)

	// Checked before the !resolved return below: NAT is the only thing a
	// resolved gateway node is used for today, so a zone with NAT disabled
	// must not stay stuck NotReady just because no gateway candidate
	// exists — see setZoneReadyCondition already called for the
	// !resolved cases above, which this deliberately overrides.
	if fabricObj.Spec.NAT.Disabled {
		setZoneReadyCondition(entry, true, reasonNATDisabled, "nat is disabled for this fabric")

		return
	}

	if !resolved {
		return
	}

	r.reconcileNATForGatewayNode(ctx, fabricObj, zoneObj, gatewayNode, entry)
}

// recordGatewayNodeSelection resolves entry's own gateway node (see
// resolveGatewayNode), records the outcome as entry's own
// GatewayNodeSelectedConditionType (and GatewayNodeRef), and reports
// whether a node was actually selected — split out of reconcileZoneEntry
// purely to keep its own length in check.
func (r *Reconciler) recordGatewayNodeSelection(
	ctx context.Context, fabricObj *v1alpha2.Fabric, entry *v1alpha2.FabricZoneStatus, zoneName string,
) (*v1alpha2.Instance, bool) {
	gatewayNode, found, err := r.resolveGatewayNode(ctx, fabricObj, entry, zoneName)
	if err != nil {
		r.Logger.Warn("failed to resolve gateway node", "fabric", fabricObj.Name, "zone", entry.Zone, "error", err)
		meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
			Type: GatewayNodeSelectedConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNoGatewayCandidate, Message: err.Error(),
		})
		setZoneReadyCondition(entry, false, reasonZoneNotReady, "failed to resolve gateway node")

		return nil, false
	}

	if !found {
		entry.GatewayNodeRef = nil
		meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
			Type: GatewayNodeSelectedConditionType, Status: metav1.ConditionFalse, Reason: reasonNoGatewayCandidate,
			Message: fmt.Sprintf("no instance in zone %q matches spec.gatewaySelector", entry.Zone),
		})
		setZoneReadyCondition(entry, false, reasonZoneNotReady, "no gateway node candidate found")

		return nil, false
	}

	entry.GatewayNodeRef = &v1alpha2.ObjectReference{
		APIVersion: v1alpha2.GroupVersion().String(), Kind: "Instance", Name: gatewayNode.Name,
	}
	meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
		Type: GatewayNodeSelectedConditionType, Status: metav1.ConditionTrue, Reason: reasonGatewayNodeSelected,
		Message: fmt.Sprintf("instance %q selected as this zone's nat gateway node", gatewayNode.Name),
	})

	return gatewayNode, true
}

// reconcileNATForGatewayNode publishes gatewayNode's own desired network
// config (entry.GatewayInterfaces — see that field's own doc) and
// delivers the Talos credential pkg/cli/fabricmanager needs to apply it
// (see ensureGatewayTalosConfig) — only reached once NAT is enabled and a
// gateway node is resolved (see reconcileZoneEntry). The actual
// interface-address assignment and NAT-masquerade workload are
// pkg/cli/fabricmanager's own responsibility now: it runs as a Pod
// directly on gatewayNode (see pkg/domain/zone.ensureFabricManagerDaemonSet),
// watches this same Fabric object, and reports NetworkConfigured/
// NATInstalled/Ready back onto entry itself once it actually applies
// them. This function still reports its own directly-observable failures
// (talos cluster not found, interfaces not classifiable, credential
// delivery itself failing) as NetworkConfiguredConditionType False — that
// much genuinely blocks pkg/cli/fabricmanager before it can even try —
// but never sets NetworkConfiguredConditionType/NATInstalledConditionType/
// ZoneReadyConditionType True, or NATInstalledConditionType at all: this
// function's own job ends at "desired state published, credential
// delivered", and it has no way to know whether pkg/cli/fabricmanager's
// own application of that state actually succeeded.
func (r *Reconciler) reconcileNATForGatewayNode(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneObj v1alpha2.Zone,
	gatewayNode *v1alpha2.Instance, entry *v1alpha2.FabricZoneStatus,
) {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: fabricObj.Namespace}, &cluster)
	if err != nil {
		r.Logger.Warn("failed to get talos cluster for gateway workload",
			"fabric", fabricObj.Name, "zone", entry.Zone, "cluster", zoneObj.Name, "error", err)
		failNetworkConfig(entry, "talos cluster not found yet", err)

		return
	}

	wan, fabricIfaces := classifyGatewayInterfaces(*gatewayNode)
	if wan == "" || len(fabricIfaces) == 0 {
		err := interfaceClassificationError(gatewayNode.Name, wan)
		r.Logger.Warn("gateway node interfaces not usable yet",
			"fabric", fabricObj.Name, "zone", entry.Zone, "node", gatewayNode.Name, "error", err)
		failNetworkConfig(entry, "gateway node interfaces not usable yet", err)

		return
	}

	entry.GatewayInterfaces = fabricIfaces

	kubeconfig, err := loadClusterKubeconfig(ctx, r.Client, &cluster)
	if err != nil {
		r.Logger.Warn("no downstream kubeconfig available yet, skipping talos credential delivery",
			"fabric", fabricObj.Name, "zone", entry.Zone, "error", err)
		failNetworkConfig(entry, "downstream kubeconfig not stored yet", err)

		return
	}

	downstream, err := r.DownstreamClientBuilder.Build(kubeconfig)
	if err != nil {
		r.Logger.Warn("failed to build downstream client for talos credential delivery",
			"fabric", fabricObj.Name, "zone", entry.Zone, "cluster", cluster.Name, "error", err)
		failNetworkConfig(entry, "downstream cluster not reachable yet", err)

		return
	}

	err = ensureGatewayTalosConfig(ctx, r.Client, downstream, &cluster, gatewayNode)
	if err != nil {
		r.Logger.Warn("failed to deliver talos credential for gateway node",
			"fabric", fabricObj.Name, "zone", entry.Zone, "node", gatewayNode.Name, "error", err)
		failNetworkConfig(entry, "talos credential not delivered yet", err)
	}
}

// failNetworkConfig records err as entry's own NetworkConfiguredConditionType
// False (reasonNetworkConfigFailed) and ZoneReadyConditionType False
// (reasonZoneNotReady, notReadyMessage) — the shared shape every one of
// reconcileNATForGatewayNode's own failure branches reports.
func failNetworkConfig(entry *v1alpha2.FabricZoneStatus, notReadyMessage string, err error) {
	meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
		Type: NetworkConfiguredConditionType, Status: metav1.ConditionFalse,
		Reason: reasonNetworkConfigFailed, Message: err.Error(),
	})
	setZoneReadyCondition(entry, false, reasonZoneNotReady, notReadyMessage)
}

// interfaceClassificationError builds reconcileNATForGatewayNode's own
// error for a gateway node whose interfaces (see classifyGatewayInstances)
// aren't usable yet — wan is "" when no interface has an address at all
// (errNoWANInterface); a non-empty wan reaching here means every
// discovered interface already has one, leaving none free to advertise the
// fabric on (errNoFabricInterface).
func interfaceClassificationError(nodeName, wan string) error {
	if wan == "" {
		return fmt.Errorf("%w: instance %q", errNoWANInterface, nodeName)
	}

	return fmt.Errorf("%w: instance %q", errNoFabricInterface, nodeName)
}

// setZoneReadyCondition sets entry's own ZoneReadyConditionType.
func setZoneReadyCondition(entry *v1alpha2.FabricZoneStatus, ready bool, reason, message string) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}

	meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
		Type: ZoneReadyConditionType, Status: status, Reason: reason, Message: message,
	})
}

// resolveGatewayNode picks the Instance backing entry's own zone's NAT
// gateway — candidates are Instances labeled kontinuum.sh/zone=zoneName
// matching fabricObj.Spec.GatewaySelector and already claimed (proof of
// live cluster membership, not just a labeled spare — see
// v1alpha2.LabelClaimedBy's own doc). Sticky: entry's own previously
// recorded GatewayNodeRef is kept as long as it's still an eligible
// candidate; otherwise the lowest-named eligible candidate is chosen —
// same deterministic tie-break instancepool.Reconciler.claim already uses.
func (r *Reconciler) resolveGatewayNode(
	ctx context.Context, fabricObj *v1alpha2.Fabric, entry *v1alpha2.FabricZoneStatus, zoneName string,
) (*v1alpha2.Instance, bool, error) {
	selector, err := metav1.LabelSelectorAsSelector(&fabricObj.Spec.GatewaySelector)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse gateway selector for fabric %q: %w", fabricObj.Name, err)
	}

	var candidates v1alpha2.InstanceList

	err = r.Client.List(ctx, &candidates, client.InNamespace(fabricObj.Namespace),
		client.MatchingLabelsSelector{Selector: selector}, client.MatchingLabels{v1alpha2.LabelZone: zoneName})
	if err != nil {
		return nil, false, fmt.Errorf("failed to list gateway candidates for zone %q: %w", zoneName, err)
	}

	eligible := make(map[string]v1alpha2.Instance, len(candidates.Items))

	for _, inst := range candidates.Items {
		if _, claimed := inst.Labels[v1alpha2.LabelClaimedBy]; claimed {
			eligible[inst.Name] = inst
		}
	}

	if len(eligible) == 0 {
		return nil, false, nil
	}

	if entry.GatewayNodeRef != nil {
		if inst, stillEligible := eligible[entry.GatewayNodeRef.Name]; stillEligible {
			return &inst, true, nil
		}
	}

	names := make([]string, 0, len(eligible))
	for name := range eligible {
		names = append(names, name)
	}

	sort.Strings(names)

	chosen := eligible[names[0]]

	return &chosen, true, nil
}

// updateValidSpecCondition sets ValidSpecConditionType and mirrors it onto
// ReadyConditionType in both directions — this is Fabric's own only
// blocking gate (see ValidSpecConditionType's own doc), the same "terminal
// gate propagates both ways" reasoning as
// zone.Reconciler.setRegistryJoinedCondition. In-memory only: reports
// whether either condition actually changed, leaving the caller
// (reconcileFabric) to persist it together with any status.zones change in
// the very same pass, via one shared persistZoneStatuses call — rather
// than this and persistZoneStatuses each running their own separate
// Status().Update against the same object.
func (r *Reconciler) updateValidSpecCondition(
	fabricObj *v1alpha2.Fabric, status metav1.ConditionStatus, reason, message string,
) bool {
	specChanged := meta.SetStatusCondition(&fabricObj.Status.Conditions, metav1.Condition{
		Type: ValidSpecConditionType, Status: status, Reason: reason, Message: message,
	})

	readyChanged := meta.SetStatusCondition(&fabricObj.Status.Conditions, metav1.Condition{
		Type: ReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	return specChanged || readyChanged
}

// persistZoneStatuses writes fabricObj's status.zones (already mutated by
// reconcileFabric) when changed is true, and decides whether to requeue:
// no requeue once every zone entry is settled (zonesSettled), otherwise
// requeue after RetryInterval to retry whatever's still converging (a
// gateway candidate not claimed yet, a Talos node unreachable, ...).
func (r *Reconciler) persistZoneStatuses(
	ctx context.Context, fabricObj *v1alpha2.Fabric, changed, zonesSettled bool,
) (ctrl.Result, error) {
	if changed {
		err := r.Client.Status().Update(ctx, fabricObj)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update fabric %q status: %w", fabricObj.Name, err)
		}
	}

	if zonesSettled {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}

// equalZoneStatuses reports whether a and b describe the same zones in the
// same order with the same content — used to decide whether
// reconcileFabric's own status.zones write is a no-op, mirroring
// zone.Reconciler.persistStatus's identical "don't write, don't
// self-trigger another reconcile" reasoning.
func equalZoneStatuses(existing, updated []v1alpha2.FabricZoneStatus) bool {
	if len(existing) != len(updated) {
		return false
	}

	for i := range existing {
		if !equalZoneStatus(existing[i], updated[i]) {
			return false
		}
	}

	return true
}

func equalZoneStatus(existing, updated v1alpha2.FabricZoneStatus) bool {
	if existing.Zone != updated.Zone || existing.CIDR != updated.CIDR || existing.GatewayIP != updated.GatewayIP {
		return false
	}

	if !equalObjectRef(existing.GatewayNodeRef, updated.GatewayNodeRef) {
		return false
	}

	if !slices.Equal(existing.GatewayInterfaces, updated.GatewayInterfaces) {
		return false
	}

	return equalConditions(existing.Conditions, updated.Conditions)
}

// equalConditions reports whether existing and updated carry the same
// condition entries, in the same order — split out of equalZoneStatus
// purely to keep its own cyclomatic complexity down.
func equalConditions(existing, updated []metav1.Condition) bool {
	if len(existing) != len(updated) {
		return false
	}

	for i := range existing {
		left, right := existing[i], updated[i]
		if left.Type != right.Type || left.Status != right.Status ||
			left.Reason != right.Reason || left.Message != right.Message {
			return false
		}
	}

	return true
}

func equalObjectRef(existing, updated *v1alpha2.ObjectReference) bool {
	if existing == nil || updated == nil {
		return existing == updated
	}

	return *existing == *updated
}
