package taloscluster

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// UpToDateConditionType reports whether every member of this cluster
	// runs the Talos and Kubernetes versions its spec asks for. Unlike
	// BootstrappedConditionType, it isn't append-only: it's kept live for
	// the cluster's whole lifetime, going false the moment either spec
	// version is edited to something the members don't run yet and back
	// true once the roll finishes. An empty spec version is "unmanaged",
	// not "upgrade to the reconciler's own default" (see resolveVersions'
	// defaults, which exist purely so config generation has something to
	// work with) — a cluster with neither pinned is trivially up to date,
	// reported with reasonVersionsUnmanaged rather than left conditionless.
	UpToDateConditionType = "UpToDate"

	reasonUpToDate            = "UpToDate"
	reasonVersionsUnmanaged   = "VersionsUnmanaged"
	reasonUpgradingTalos      = "UpgradingTalos"
	reasonUpgradingKubernetes = "UpgradingKubernetes"
	reasonUpgradeFailed       = "UpgradeFailed"

	// installerImageRepository is the repository the Talos upgrade
	// installer image is built from — <repo>:<version> is exactly what
	// `talosctl upgrade --image` takes, and what Talos's own docs use.
	// Machinery exports no constant for it (the generated machine config's
	// own machine.install.image is left empty by generate.NewInput, letting
	// the running Talos pick its own matching installer at install time),
	// so an upgrade — which by definition targets a *different* version
	// than the one running — has to name it explicitly.
	installerImageRepository = "ghcr.io/siderolabs/installer"
)

// upgradeMember pairs one of this cluster's members with the machine type
// its regenerated config has to be generated as — a worker and a
// control-plane node take different configs, and reconcileUpgrades walks
// one flat, ordered list of both rather than two parallel ones.
type upgradeMember struct {
	// instance is the member itself. A pointer, not a copy: refreshVersions
	// persists onto it and reconcileUpgrades reads those same values back.
	instance *v1alpha2.Instance
	// machineType is machine.TypeControlPlane or machine.TypeWorker — see
	// configBytes, which needs it to generate this member's own config.
	machineType machine.Type
}

// installerImage is the installer image reference for version — see
// installerImageRepository's own doc.
func installerImage(version string) string {
	return installerImageRepository + ":" + normalizeVersion(version)
}

// normalizeVersion returns version with a leading "v", so a Kubernetes
// version written either way in the spec ("1.32.0" and "v1.32.0" are both
// natural, and resolveVersions itself strips the prefix back off for
// Talos's own generator) compares equal to the "v"-prefixed form Talos
// reports back from a Version RPC or a kubelet image tag. An empty version
// stays empty — it means "unmanaged" (see UpToDateConditionType's own
// doc), never "v".
func normalizeVersion(version string) string {
	if version == "" || strings.HasPrefix(version, "v") {
		return version
	}

	return "v" + version
}

// reconcileUpgrades converges every member onto the Talos and Kubernetes
// versions cluster's spec pins, one node at a time, and reports the result
// on UpToDateConditionType. Only ever called from Reconcile's steady-state
// branch — after ControlPlaneReady and Ready are both true *and* the
// periodic health recheck this pass just ran actually passed. That gate is
// the whole rolling mechanism: a node that is mid-upgrade is rebooting and
// therefore unreachable, which fails the next pass's health check, which
// stops this from touching a second node until the first one is back and
// the cluster is healthy again. It's also what makes a freshly added zone
// safe: `kontinuum zone add --talos-version` puts the requested version on
// the TalosCluster at creation time, but nothing here runs until that
// cluster has bootstrapped and converged on whatever version its seed node
// booted — the zone is created first, then upgraded, never both at once.
//
// Talos takes precedence over Kubernetes: the cluster's Talos version
// gates which Kubernetes versions are supported at all, so an edit that
// moves both is rolled out as a complete Talos roll first and only then a
// Kubernetes one. steady is Reconcile's own steady-state result, returned
// unchanged when there's nothing to upgrade, so this never shortens the
// health-recheck cadence just by being on the path.
func (r *Reconciler) reconcileUpgrades(
	ctx context.Context, cluster *v1alpha2.TalosCluster, bundle *talossecrets.Bundle, steady ctrl.Result,
) (ctrl.Result, error) {
	members, err := r.collectUpgradeMembers(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(members) == 0 {
		return steady, nil
	}

	controlPlaneAddr := dialAddress(*members[0].instance)

	input, talosCfg, err := generateConfigs(bundle, cluster, controlPlaneAddr)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to generate upgrade config for %q: %w", cluster.Name, err)
	}

	r.refreshMemberVersions(ctx, cluster, members, controlPlaneAddr, talosCfg)

	return r.applyUpgradePlan(ctx, cluster, members, upgradeContext{
		controlPlaneAddr: controlPlaneAddr,
		input:            input,
		talosCfg:         talosCfg,
		steady:           steady,
	})
}

// upgradeContext carries applyUpgradePlan's per-pass Talos-side inputs —
// grouped into one struct purely to keep that function's own parameter
// list short, not because they mean anything together.
type upgradeContext struct {
	controlPlaneAddr string
	input            *generate.Input
	talosCfg         *config.Config
	steady           ctrl.Result
}

// clusterVersions is the pair of cluster-wide observed versions
// setUpToDateCondition writes onto TalosClusterStatus — see
// v1alpha2.TalosClusterVersionStatus' own doc.
type clusterVersions struct {
	talos      string
	kubernetes string
}

// applyUpgradePlan records the cluster-wide observed versions, then acts on
// the first member (in collectUpgradeMembers' control-plane-first order)
// that still disagrees with a pinned spec version — Talos first, Kubernetes
// only once every member already runs the pinned Talos version. See
// reconcileUpgrades' own doc for the precedence rule and the one-node-at-a-
// time gate.
func (r *Reconciler) applyUpgradePlan(
	ctx context.Context, cluster *v1alpha2.TalosCluster, members []upgradeMember, upgradeCtx upgradeContext,
) (ctrl.Result, error) {
	desiredTalos := normalizeVersion(cluster.Spec.Talos.Version)
	desiredKubernetes := normalizeVersion(cluster.Spec.Kubernetes.Version)

	observed := clusterVersions{
		talos:      agreedVersion(members, talosVersionOf),
		kubernetes: agreedVersion(members, kubernetesVersionOf),
	}

	if desiredTalos == "" && desiredKubernetes == "" {
		return r.setUpToDateCondition(ctx, cluster, observed, metav1.ConditionTrue, reasonVersionsUnmanaged,
			"no talos or kubernetes version pinned, so neither is managed by this controller",
			upgradeCtx.steady)
	}

	if stale := staleMembers(members, desiredTalos, talosVersionOf); len(stale) > 0 {
		return r.upgradeTalosMember(ctx, cluster, observed, stale, desiredTalos, upgradeCtx)
	}

	if stale := staleMembers(members, desiredKubernetes, kubernetesVersionOf); len(stale) > 0 {
		return r.upgradeKubernetesMember(ctx, cluster, observed, stale, desiredKubernetes, upgradeCtx)
	}

	return r.setUpToDateCondition(ctx, cluster, observed, metav1.ConditionTrue, reasonUpToDate,
		upToDateMessage(desiredTalos, desiredKubernetes), upgradeCtx.steady)
}

// upgradeTalosMember starts a Talos upgrade of stale's first member — see
// ClusterBootstrapper.UpgradeTalos' own doc for why the RPC returning
// doesn't mean the upgrade finished, and reconcileUpgrades' for why only
// one member is ever touched per pass.
func (r *Reconciler) upgradeTalosMember(
	ctx context.Context, cluster *v1alpha2.TalosCluster, observed clusterVersions, stale []upgradeMember,
	desired string, upgradeCtx upgradeContext,
) (ctrl.Result, error) {
	target := stale[0].instance
	image := installerImage(desired)

	r.Logger.Info("Upgrading talos on cluster member",
		"cluster", cluster.Name, "instance", target.Name, "image", image, "remaining", len(stale))

	err := r.Bootstrapper.UpgradeTalos(
		ctx, upgradeCtx.controlPlaneAddr, dialAddress(*target), upgradeCtx.talosCfg, image)
	if err != nil {
		r.Logger.Warn("Failed to start talos upgrade",
			"cluster", cluster.Name, "instance", target.Name, "image", image, "error", err)

		return r.setUpToDateCondition(ctx, cluster, observed, metav1.ConditionFalse, reasonUpgradeFailed,
			fmt.Sprintf("failed to start talos upgrade of %s to %s: %s", target.Name, desired, err),
			ctrl.Result{RequeueAfter: r.RetryInterval})
	}

	return r.setUpToDateCondition(ctx, cluster, observed, metav1.ConditionFalse, reasonUpgradingTalos,
		fmt.Sprintf("upgrading %s to talos %s, %d member(s) still to go", target.Name, desired, len(stale)),
		ctrl.Result{RequeueAfter: r.RetryInterval})
}

// upgradeKubernetesMember re-applies stale's first member's own machine
// config, regenerated from the spec's pinned Kubernetes version. Talos's
// own controllers pick the new component image tags up from that config and
// roll the static control-plane pods, the kubelet, and the bootstrap
// manifests — this deliberately doesn't reimplement `talosctl upgrade-k8s`'
// per-component sequencing, which lives in the main talos module this repo
// doesn't depend on (see docs/workflows/cluster-upgrade.md).
func (r *Reconciler) upgradeKubernetesMember(
	ctx context.Context, cluster *v1alpha2.TalosCluster, observed clusterVersions, stale []upgradeMember,
	desired string, upgradeCtx upgradeContext,
) (ctrl.Result, error) {
	target := stale[0]

	data, err := configBytes(upgradeCtx.input, target.machineType, target.instance.Name)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to generate upgrade config for %q: %w", target.instance.Name, err)
	}

	r.Logger.Info("Upgrading kubernetes on cluster member",
		"cluster", cluster.Name, "instance", target.instance.Name, "version", desired, "remaining", len(stale))

	err = r.Bootstrapper.UpgradeConfiguration(
		ctx, upgradeCtx.controlPlaneAddr, dialAddress(*target.instance), upgradeCtx.talosCfg, data)
	if err != nil {
		r.Logger.Warn("Failed to apply kubernetes upgrade configuration",
			"cluster", cluster.Name, "instance", target.instance.Name, "version", desired, "error", err)

		return r.setUpToDateCondition(ctx, cluster, observed, metav1.ConditionFalse, reasonUpgradeFailed,
			fmt.Sprintf("failed to apply kubernetes %s configuration to %s: %s", desired, target.instance.Name, err),
			ctrl.Result{RequeueAfter: r.RetryInterval})
	}

	return r.setUpToDateCondition(ctx, cluster, observed, metav1.ConditionFalse, reasonUpgradingKubernetes,
		fmt.Sprintf("upgrading %s to kubernetes %s, %d member(s) still to go",
			target.instance.Name, desired, len(stale)),
		ctrl.Result{RequeueAfter: r.RetryInterval})
}

// upToDateMessage describes which of the two versions this cluster is
// actually holding itself to — an unpinned one is unmanaged, not converged,
// so claiming both were up to date when only one is pinned would overstate
// what was checked.
func upToDateMessage(desiredTalos, desiredKubernetes string) string {
	var parts []string

	if desiredTalos != "" {
		parts = append(parts, "talos "+desiredTalos)
	}

	if desiredKubernetes != "" {
		parts = append(parts, "kubernetes "+desiredKubernetes)
	}

	return "every member runs " + strings.Join(parts, " and ")
}

// collectUpgradeMembers returns every member of this cluster — the
// control-plane pool's first, then each worker pool's in spec order, each
// pool's own members sorted by name so the roll is deterministic across
// reconciles rather than following whatever order the API server happened
// to list them in. That order is the roll order, and it puts the control
// plane first for the same reason Reconcile itself does: a worker joining
// or restarting against a control plane that hasn't moved yet is fine,
// the reverse is not.
func (r *Reconciler) collectUpgradeMembers(
	ctx context.Context, cluster *v1alpha2.TalosCluster,
) ([]upgradeMember, error) {
	controlPlane, err := resolveMembers(ctx, r.Client, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return nil, err
	}

	members := appendUpgradeMembers(nil, controlPlane, machine.TypeControlPlane)

	for _, worker := range cluster.Spec.Workers {
		workers, err := resolveMembers(ctx, r.Client, cluster.Namespace, worker.PoolRef)
		if err != nil {
			return nil, err
		}

		members = appendUpgradeMembers(members, workers, machine.TypeWorker)
	}

	return members, nil
}

// appendUpgradeMembers sorts pool by name and appends each of its members
// to members, tagged with machineType — see collectUpgradeMembers' own doc
// for why the order matters. pool is indexed, not ranged over by value, so
// every upgradeMember points at the slice's own element rather than a loop
// copy: refreshMemberVersions writes the freshly probed versions onto
// exactly the objects applyUpgradePlan then reads back.
func appendUpgradeMembers(
	members []upgradeMember, pool []v1alpha2.Instance, machineType machine.Type,
) []upgradeMember {
	sort.Slice(pool, func(i, j int) bool { return pool[i].Name < pool[j].Name })

	for i := range pool {
		members = append(members, upgradeMember{instance: &pool[i], machineType: machineType})
	}

	return members
}

// refreshMemberVersions re-probes every member's real Talos and Kubernetes
// version and persists whatever changed onto its own Instance status.
// Unlike recordTalosVersions — which deliberately skips any member already
// marked Joined, since it's only ever establishing that a member first came
// up — this always re-reads: an upgraded node reports a new version without
// ever leaving Joined, so a cached value is exactly what would make a
// finished upgrade look like it never happened. Best-effort throughout: a
// member that's mid-reboot answers nothing, which leaves its last known
// version in place and simply defers the decision to the next pass.
func (r *Reconciler) refreshMemberVersions(
	ctx context.Context, cluster *v1alpha2.TalosCluster, members []upgradeMember, controlPlaneAddr string,
	talosCfg *config.Config,
) {
	for _, member := range members {
		instance := member.instance
		addr := dialAddress(*instance)
		updated := false

		talosVersion, _, err := r.Bootstrapper.Version(ctx, controlPlaneAddr, addr, talosCfg)
		if err != nil {
			r.Logger.Warn("Failed to refresh talos version for member, it may be mid-upgrade",
				"cluster", cluster.Name, "instance", instance.Name, "error", err)
		} else if talosVersion != "" && talosVersion != instance.Status.Talos.Version {
			instance.Status.Talos.Version = talosVersion
			updated = true
		}

		kubernetesVersion, err := r.Bootstrapper.KubeletVersion(ctx, controlPlaneAddr, addr, talosCfg)
		if err != nil {
			r.Logger.Warn("Failed to refresh kubernetes version for member, it may be mid-upgrade",
				"cluster", cluster.Name, "instance", instance.Name, "error", err)
		} else if kubernetesVersion != "" && kubernetesVersion != instance.Status.Kubernetes.Version {
			instance.Status.Kubernetes.Version = kubernetesVersion
			updated = true
		}

		if !updated {
			continue
		}

		err = r.Client.Status().Update(ctx, instance)
		if err != nil {
			r.Logger.Warn("Failed to persist refreshed member versions",
				"cluster", cluster.Name, "instance", instance.Name, "error", err)
		}
	}
}

// talosVersionOf and kubernetesVersionOf are the two version accessors
// agreedVersion and staleMembers are parameterized over, so neither has to
// exist twice over near-identical bodies.
func talosVersionOf(member upgradeMember) string {
	return normalizeVersion(member.instance.Status.Talos.Version)
}

func kubernetesVersionOf(member upgradeMember) string {
	return normalizeVersion(member.instance.Status.Kubernetes.Version)
}

// agreedVersion returns the version every member reports, or an empty
// string if any of them reports nothing or a different one — see
// v1alpha2.TalosClusterVersionStatus' own doc for why a split cluster
// reports no version rather than an arbitrary member's.
func agreedVersion(members []upgradeMember, versionOf func(upgradeMember) string) string {
	agreed := versionOf(members[0])

	for _, member := range members[1:] {
		if versionOf(member) != agreed {
			return ""
		}
	}

	return agreed
}

// staleMembers returns every member whose own version doesn't match
// desired, in members' own roll order. An empty desired means unmanaged
// (see UpToDateConditionType's own doc) and matches nothing, so no member
// is ever upgraded toward a version nobody asked for.
func staleMembers(
	members []upgradeMember, desired string, versionOf func(upgradeMember) string,
) []upgradeMember {
	if desired == "" {
		return nil
	}

	stale := make([]upgradeMember, 0, len(members))

	for _, member := range members {
		if versionOf(member) != desired {
			stale = append(stale, member)
		}
	}

	return stale
}

// setUpToDateCondition records observed on cluster's status, sets
// UpToDateConditionType, persists both if either actually changed, and
// returns result — its own caller's requeue decision, not one derived from
// the condition's status the way persistStatus does: a converged cluster
// still wants the steady-state health-recheck cadence, and an in-flight
// upgrade wants the much shorter retry interval, neither of which is
// expressible as "true means no requeue". The observed versions are part
// of the change check for the same reason the condition is: a member
// finishing its upgrade moves them without necessarily flipping the
// condition, and skipping the write on an unchanged condition alone would
// strand them at a stale value forever.
func (r *Reconciler) setUpToDateCondition(
	ctx context.Context, cluster *v1alpha2.TalosCluster, observed clusterVersions,
	status metav1.ConditionStatus, reason, message string, result ctrl.Result,
) (ctrl.Result, error) {
	versionsChanged := cluster.Status.Talos.Version != observed.talos ||
		cluster.Status.Kubernetes.Version != observed.kubernetes

	cluster.Status.Talos.Version = observed.talos
	cluster.Status.Kubernetes.Version = observed.kubernetes

	conditionChanged := meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: UpToDateConditionType, Status: status, Reason: reason, Message: message,
	})

	if !versionsChanged && !conditionChanged {
		return result, nil
	}

	err := r.Client.Status().Update(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update talos cluster %q status: %w", cluster.Name, err)
	}

	return result, nil
}
