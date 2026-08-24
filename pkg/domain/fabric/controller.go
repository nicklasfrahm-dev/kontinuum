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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
	// (see setValidSpecCondition's own doc), since nothing else at the
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
	// NetworkConfiguredConditionType is one zone entry's own condition set
	// once its elected gateway node's static route has been pushed via
	// Talos — see NetworkConfigurer.
	NetworkConfiguredConditionType = "NetworkConfigured"
	// NATInstalledConditionType is one zone entry's own condition set once
	// its NAT-masquerade workload is running on its elected gateway node —
	// see ensureFabricManagerWorkload.
	NATInstalledConditionType = "NATInstalled"
	// ZoneReadyConditionType is one zone entry's own aggregate condition —
	// mirrors ReadyConditionType's identical "kubectl-tree needs a literal
	// Ready-Typed condition" reasoning, scoped to this one status.zones[]
	// entry instead of the whole Fabric.
	ZoneReadyConditionType = "Ready"

	// TeardownConditionType is set false while a Fabric being deleted is
	// still waiting on a zone's own NAT gateway workload teardown — see
	// teardown.go's own doc. Mirrors zone.TeardownConditionType's identical
	// "never observed true" shape: the finalizer is removed in the same
	// reconcile pass that would otherwise have set it.
	TeardownConditionType = "Teardown"

	reasonInvalidSpec         = "InvalidSpec"
	reasonValidSpec           = "ValidSpec"
	reasonNoGatewayCandidate  = "NoGatewayCandidate"
	reasonGatewayNodeSelected = "GatewayNodeSelected"
	reasonNetworkConfigured   = "NetworkConfigured"
	reasonNetworkConfigFailed = "NetworkConfigFailed"
	reasonNATInstalled        = "NATInstalled"
	reasonNATInstallFailed    = "NATInstallFailed"
	reasonNATDisabled         = "NATDisabled"
	reasonZoneReady           = "ZoneReady"
	reasonZoneNotReady        = "ZoneNotReady"
	// reasonNATTeardownFailed is teardown.go's own retryable-failure reason
	// — see reconcileTeardown.
	reasonNATTeardownFailed = "NATTeardownFailed"

	defaultRetryInterval = 15 * time.Second
	// defaultTeardownTimeout bounds how long a Fabric's finalizer keeps
	// retrying downstream NAT gateway teardown before giving up and
	// removing itself anyway — mirrors zone.defaultTeardownTimeout's
	// identical "not a finalizer that blocks deletion forever" reasoning.
	defaultTeardownTimeout = 15 * time.Minute

	// FabricFinalizer is the finalizer teardown.go adds to every Fabric
	// this package reconciles, and only ever removes once every zone's own
	// NAT gateway workload is torn down (or teardown has been abandoned
	// after defaultTeardownTimeout — see reconcileTeardown).
	FabricFinalizer = "kontinuum.sh/fabric-teardown"
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// NetworkConfigurer pushes a gateway node's static route via Talos —
	// see its own doc. Defaults to NewNetworkConfigurer() when nil; tests
	// inject a fake through, the same role zone.DownstreamClientBuilder
	// plays below.
	NetworkConfigurer NetworkConfigurer
	// DownstreamClientBuilder builds a client.Client against a zone's own
	// cluster from its stored kubeconfig — reused directly from
	// pkg/domain/zone rather than a second copy of the identical seam.
	// Defaults to zone.NewDownstreamClientBuilder() when nil.
	DownstreamClientBuilder zone.DownstreamClientBuilder
	// Image is the full kontinuum container image reference (repo:tag) the
	// NAT gateway workload runs — see pkg/cli/serve.go's fabricOptions.
	// Deployed verbatim, unlike zone.Reconciler.resolveImage's own
	// digest-pinning/floating-tag resolution: the NAT gateway workload is
	// this same process's own image, already known exactly (this process's
	// own build version), with no separate fleet-wide version to resolve.
	Image string
	// RetryInterval is how long Reconcile waits before retrying a step
	// that hasn't converged yet. Defaults to fifteen seconds when zero.
	RetryInterval time.Duration
	// TeardownTimeout bounds how long a Fabric being deleted keeps retrying
	// downstream NAT gateway teardown before giving up and removing its
	// finalizer anyway — see teardown.go's own doc. Defaults to fifteen
	// minutes when zero.
	TeardownTimeout time.Duration
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

// NewController builds a Controller from cfg, defaulting NetworkConfigurer,
// DownstreamClientBuilder, RetryInterval, and TeardownTimeout when left
// zero.
func NewController(cfg Config) *Controller {
	if cfg.NetworkConfigurer == nil {
		cfg.NetworkConfigurer = NewNetworkConfigurer()
	}

	if cfg.DownstreamClientBuilder == nil {
		cfg.DownstreamClientBuilder = zone.NewDownstreamClientBuilder()
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	if cfg.TeardownTimeout == 0 {
		cfg.TeardownTimeout = defaultTeardownTimeout
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
		NetworkConfigurer:       c.Config.NetworkConfigurer,
		DownstreamClientBuilder: c.Config.DownstreamClientBuilder,
		Image:                   c.Config.Image,
		RetryInterval:           c.Config.RetryInterval,
		TeardownTimeout:         c.Config.TeardownTimeout,
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
	NetworkConfigurer       NetworkConfigurer
	DownstreamClientBuilder zone.DownstreamClientBuilder
	Image                   string
	RetryInterval           time.Duration
	TeardownTimeout         time.Duration
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

	if !fabricObj.DeletionTimestamp.IsZero() {
		return r.reconcileTeardown(ctx, &fabricObj)
	}

	if controllerutil.AddFinalizer(&fabricObj, FabricFinalizer) {
		err = r.Client.Update(ctx, &fabricObj)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer to fabric %q: %w", fabricObj.Name, err)
		}
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

	previous := make([]PreviousAllocation, 0, len(fabricObj.Status.Zones))
	for _, zoneStatus := range fabricObj.Status.Zones {
		previous = append(previous, PreviousAllocation{Zone: zoneStatus.Zone, CIDR: zoneStatus.CIDR})
	}

	allocations, err := Allocate(fabricObj.Spec.CIDR, fabricObj.Spec.ZonePrefixLength, liveZones, previous)
	if err != nil {
		return r.setValidSpecCondition(ctx, fabricObj, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
	}

	newZones, zonesSettled := r.reconcileZoneStatuses(ctx, fabricObj, zonesByName, allocations)

	changed := !equalZoneStatuses(fabricObj.Status.Zones, newZones)
	fabricObj.Status.Zones = newZones

	result, err := r.setValidSpecCondition(ctx, fabricObj, metav1.ConditionTrue, reasonValidSpec, "fabric spec is valid")
	if err != nil {
		return result, err
	}

	return r.persistZoneStatuses(ctx, fabricObj, changed, zonesSettled)
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
// enabled and a node is resolved, pushes its static route via Talos and
// ensures its NAT-masquerade workload is running — mutating entry's own
// GatewayNodeRef/Conditions in place. A failure at any step is caught and
// reported as that step's own False condition (logged as a warning), never
// propagated as a hard Reconcile error: every failure mode here (no
// candidate yet, the zone's TalosCluster not bootstrapped yet, its gateway
// node unreachable) is expected, ordinary "not converged yet" state that
// the next requeue retries — see zone.Reconciler.reconcileInstall's
// identical tolerance for the same class of "not ready yet" failure.
func (r *Reconciler) reconcileZoneEntry(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneObj v1alpha2.Zone, entry *v1alpha2.FabricZoneStatus,
) {
	previousGatewayNodeRef := entry.GatewayNodeRef

	gatewayNode, resolved := r.recordGatewayNodeSelection(ctx, fabricObj, entry, zoneObj.Spec.Zone)

	r.teardownStaleGatewayNode(ctx, fabricObj, zoneObj, entry.Zone, previousGatewayNodeRef, entry.GatewayNodeRef)

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

// teardownStaleGatewayNode tears down previousRef's own nat gateway
// workload if this reconcile just replaced it with a different node (or no
// node at all) — otherwise a re-elected gateway node leaves the old one's
// nat-masquerade workload running forever, orphaned and still forwarding
// traffic nobody expects it to. A nil previousRef, or one that still
// matches the newly resolved node, means nothing changed — the common
// case, checked first so the (idempotent, but not free) teardown attempt
// below only runs on an actual re-election.
func (r *Reconciler) teardownStaleGatewayNode(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneObj v1alpha2.Zone,
	zoneName string, previousRef, currentRef *v1alpha2.ObjectReference,
) {
	if previousRef == nil || (currentRef != nil && currentRef.Name == previousRef.Name) {
		return
	}

	staleStatus := v1alpha2.FabricZoneStatus{Zone: zoneName, GatewayNodeRef: previousRef}

	err := r.teardownZoneWorkload(ctx, fabricObj, zoneObj, staleStatus)
	if err != nil {
		r.Logger.Warn("failed to tear down stale gateway node's nat workload",
			"fabric", fabricObj.Name, "zone", zoneName, "node", previousRef.Name, "error", err)
	}
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

// reconcileNATForGatewayNode pushes gatewayNode's own network config and
// NAT workload — only reached once NAT is enabled and a gateway node is
// resolved (see reconcileZoneEntry) — split out purely to keep that
// function's own length in check.
func (r *Reconciler) reconcileNATForGatewayNode(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneObj v1alpha2.Zone,
	gatewayNode *v1alpha2.Instance, entry *v1alpha2.FabricZoneStatus,
) {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: fabricObj.Namespace}, &cluster)
	if err != nil {
		r.Logger.Warn("failed to get talos cluster for gateway workload",
			"fabric", fabricObj.Name, "zone", entry.Zone, "cluster", zoneObj.Name, "error", err)
		meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
			Type: NetworkConfiguredConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNetworkConfigFailed, Message: err.Error(),
		})
		setZoneReadyCondition(entry, false, reasonZoneNotReady, "talos cluster not found yet")

		return
	}

	wan, fabricIfaces := classifyGatewayInterfaces(*gatewayNode)
	if wan == "" || len(fabricIfaces) == 0 {
		err := interfaceClassificationError(gatewayNode.Name, wan)
		r.Logger.Warn("gateway node interfaces not usable yet",
			"fabric", fabricObj.Name, "zone", entry.Zone, "node", gatewayNode.Name, "error", err)
		meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
			Type: NetworkConfiguredConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNetworkConfigFailed, Message: err.Error(),
		})
		setZoneReadyCondition(entry, false, reasonZoneNotReady, "gateway node interfaces not usable yet")

		return
	}

	networkOK := r.reconcileNetworkConfig(ctx, &cluster, gatewayNode, fabricIfaces, entry)

	natOK := false
	if networkOK {
		natOK = r.reconcileNATWorkload(ctx, &cluster, gatewayNode, fabricObj.Name, wan, entry)
	}

	switch {
	case networkOK && natOK:
		setZoneReadyCondition(entry, true, reasonZoneReady, "gateway node network and nat workload configured")
	case networkOK:
		setZoneReadyCondition(entry, false, reasonZoneNotReady, "nat gateway workload not installed yet")
	default:
		setZoneReadyCondition(entry, false, reasonZoneNotReady, "gateway node network not configured yet")
	}
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

// reconcileNetworkConfig assigns entry's own GatewayIP as a real address
// on fabricIfaces (see NetworkConfigurer/BuildGatewayAddressPatch),
// reporting the outcome as entry's own NetworkConfiguredConditionType and
// returning whether it succeeded — every failure here (secrets bundle not
// stored yet, the node itself unreachable) is caught and logged, never
// propagated as a hard Reconcile error, mirroring reconcileZoneEntry's own
// doc. Records entry.GatewayInterfaces on success — see that field's own
// doc.
func (r *Reconciler) reconcileNetworkConfig(
	ctx context.Context, cluster *v1alpha2.TalosCluster, gatewayNode *v1alpha2.Instance,
	fabricIfaces []string, entry *v1alpha2.FabricZoneStatus,
) bool {
	err := r.pushNetworkConfig(ctx, cluster, *entry, gatewayNode, fabricIfaces)
	if err != nil {
		r.Logger.Warn("failed to push gateway node network config",
			"cluster", cluster.Name, "zone", entry.Zone, "node", gatewayNode.Name, "error", err)
		meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
			Type: NetworkConfiguredConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNetworkConfigFailed, Message: err.Error(),
		})

		return false
	}

	entry.GatewayInterfaces = fabricIfaces

	meta.SetStatusCondition(&entry.Conditions, metav1.Condition{
		Type: NetworkConfiguredConditionType, Status: metav1.ConditionTrue, Reason: reasonNetworkConfigured,
		Message: fmt.Sprintf("assigned gateway address %s on instance %q, interfaces %v",
			entry.GatewayIP, gatewayNode.Name, fabricIfaces),
	})

	return true
}

// pushNetworkConfig does the actual work reconcileNetworkConfig reports the
// outcome of.
func (r *Reconciler) pushNetworkConfig(
	ctx context.Context, cluster *v1alpha2.TalosCluster, entry v1alpha2.FabricZoneStatus,
	gatewayNode *v1alpha2.Instance, fabricIfaces []string,
) error {
	gatewayPrefix, err := GatewayPrefix(entry.CIDR, entry.GatewayIP)
	if err != nil {
		return err
	}

	bundle, err := LoadSecretsBundle(ctx, r.Client, cluster.Status.SecretRef)
	if err != nil {
		return err
	}

	addr := dialAddress(*gatewayNode)

	talosCfg, err := BuildTalosConfig(bundle, cluster.Name, []string{addr})
	if err != nil {
		return err
	}

	patch, err := BuildGatewayAddressPatch(fabricIfaces, gatewayPrefix)
	if err != nil {
		return err
	}

	err = r.NetworkConfigurer.ApplyInterfaceConfig(ctx, addr, talosCfg, patch)
	if err != nil {
		return fmt.Errorf("failed to push interface config to gateway node %q: %w", gatewayNode.Name, err)
	}

	return nil
}

// reconcileNATWorkload ensures gatewayNode's own NAT-masquerade workload is
// running on cluster's own downstream cluster, masquerading outbound
// traffic through wanInterface (see classifyGatewayInterfaces), reporting
// the outcome as entry's own NATInstalledConditionType and returning
// whether it succeeded — only reached once reconcileNetworkConfig has
// already succeeded (see reconcileZoneEntry).
func (r *Reconciler) reconcileNATWorkload(
	ctx context.Context, cluster *v1alpha2.TalosCluster, gatewayNode *v1alpha2.Instance,
	fabricID, wanInterface string, target *v1alpha2.FabricZoneStatus,
) bool {
	err := r.installNATWorkload(ctx, cluster, gatewayNode.Name, fabricID, wanInterface)
	if err != nil {
		r.Logger.Warn("failed to install nat gateway workload",
			"cluster", cluster.Name, "zone", target.Zone, "node", gatewayNode.Name, "error", err)
		meta.SetStatusCondition(&target.Conditions, metav1.Condition{
			Type: NATInstalledConditionType, Status: metav1.ConditionFalse,
			Reason: reasonNATInstallFailed, Message: err.Error(),
		})

		return false
	}

	meta.SetStatusCondition(&target.Conditions, metav1.Condition{
		Type: NATInstalledConditionType, Status: metav1.ConditionTrue, Reason: reasonNATInstalled,
		Message: fmt.Sprintf("nat gateway workload running on instance %q", gatewayNode.Name),
	})

	return true
}

// installNATWorkload does the actual work reconcileNATWorkload reports the
// outcome of.
func (r *Reconciler) installNATWorkload(
	ctx context.Context, cluster *v1alpha2.TalosCluster, nodeName, fabricID, interfaceName string,
) error {
	kubeconfig, err := loadClusterKubeconfig(ctx, r.Client, cluster)
	if err != nil {
		return err
	}

	downstream, err := r.DownstreamClientBuilder.Build(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build downstream client for cluster %q: %w", cluster.Name, err)
	}

	err = ensureFabricManagerWorkload(ctx, downstream, r.Image, nodeName, fabricID, interfaceName)
	if err != nil {
		return fmt.Errorf("failed to install nat gateway workload: %w", err)
	}

	return nil
}

// setValidSpecCondition sets ValidSpecConditionType and mirrors it onto
// ReadyConditionType in both directions — this is Fabric's own only
// blocking gate (see ValidSpecConditionType's own doc), the same "terminal
// gate propagates both ways" reasoning as
// zone.Reconciler.setRegistryJoinedCondition.
func (r *Reconciler) setValidSpecCondition(
	ctx context.Context, fabricObj *v1alpha2.Fabric, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	specChanged := meta.SetStatusCondition(&fabricObj.Status.Conditions, metav1.Condition{
		Type: ValidSpecConditionType, Status: status, Reason: reason, Message: message,
	})

	readyChanged := meta.SetStatusCondition(&fabricObj.Status.Conditions, metav1.Condition{
		Type: ReadyConditionType, Status: status, Reason: reason, Message: message,
	})

	changed := specChanged || readyChanged
	if changed {
		err := r.Client.Status().Update(ctx, fabricObj)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update fabric %q status: %w", fabricObj.Name, err)
		}
	}

	if status == metav1.ConditionFalse {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
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
