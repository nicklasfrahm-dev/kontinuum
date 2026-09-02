package taloscluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

const (
	// testUpgradeTalosVersion and testUpgradeKubernetesVersion are the
	// targets every upgrade fixture below pins, deliberately different
	// from testTalosVersionFixture/testUpgradeRunningKubernetesVersion —
	// the versions the fake reports the members actually running — so a
	// cluster that pins them starts out genuinely stale.
	testUpgradeTalosVersion      = "v1.13.0"
	testUpgradeKubernetesVersion = "v1.33.0"
	// testUpgradeRunningKubernetesVersion is the Kubernetes version the
	// fake's kubelets report before any upgrade runs.
	testUpgradeRunningKubernetesVersion = "v1.32.0"
	// testUpgradeInstallerImage is the installer reference a Talos upgrade
	// to testUpgradeTalosVersion must target.
	testUpgradeInstallerImage = "ghcr.io/siderolabs/installer:" + testUpgradeTalosVersion

	secondCPNodeName        = "cp-node-2"
	secondCPInstanceAddress = "10.0.0.3"
)

// upgradeHarness is everything an upgrade test needs to drive one more
// reconcile past convergence — see upgradeFixture, which builds it.
type upgradeHarness struct {
	client       client.Client
	bootstrapper *fakeBootstrapper
	reconciler   *taloscluster.Reconciler
	request      ctrl.Request
	// converged is the result of the reconcile that first reached
	// reconcileUpgrades, so a test can assert on the requeue cadence that
	// pass chose without reconciling again.
	converged ctrl.Result
}

// newUpgradeBootstrapper builds the fake every fixture below shares: it
// reports testTalosVersionFixture and testUpgradeRunningKubernetesVersion
// for every member, so whether a member is stale is decided purely by what
// the cluster pins.
func newUpgradeBootstrapper() *fakeBootstrapper {
	return &fakeBootstrapper{
		kubeconfig:     []byte("fake-kubeconfig"),
		version:        testTalosVersionFixture,
		kubeletVersion: testUpgradeRunningKubernetesVersion,
	}
}

// newUpgradeCluster builds a worker-less TalosCluster pinning the given
// Talos/Kubernetes versions — an empty one means unpinned, i.e. unmanaged.
func newUpgradeCluster(talosVersion, kubernetesVersion string) *v1alpha2.TalosCluster {
	cluster := testCluster()
	cluster.Spec.Workers = nil
	cluster.Spec.Talos.Version = talosVersion
	cluster.Spec.Kubernetes.Version = kubernetesVersion

	return cluster
}

// upgradeFixture builds a single-control-plane-node cluster pinning the
// given Talos/Kubernetes versions and converges it all the way to
// ControlPlaneReady + Ready, so the returned harness is already one
// reconcile past the point reconcileUpgrades first runs.
func upgradeFixture(
	t *testing.T, talosVersion, kubernetesVersion string, extraInstances ...*v1alpha2.Instance,
) upgradeHarness {
	t.Helper()

	objects := make([]client.Object, 0, 2+len(extraInstances))
	objects = append(objects, newUpgradeCluster(talosVersion, kubernetesVersion),
		claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress))

	for _, extra := range extraInstances {
		objects = append(objects, extra)
	}

	fakeClient := newFakeClient(t, objects...)

	bootstrapper := newUpgradeBootstrapper()
	reconciler := newReconciler(fakeClient, bootstrapper)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	return upgradeHarness{
		client:       fakeClient,
		bootstrapper: bootstrapper,
		reconciler:   reconciler,
		request:      req,
		converged:    convergeFullyReadyCluster(t, fakeClient, reconciler, req),
	}
}

// upToDateCondition fetches the cluster and returns its UpToDate
// condition, failing the test if it was never set.
func upToDateCondition(t *testing.T, fakeClient client.Client) *metav1.Condition {
	t.Helper()

	var cluster v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName}, &cluster))

	cond := meta.FindStatusCondition(cluster.Status.Conditions, taloscluster.UpToDateConditionType)
	require.NotNil(t, cond, "a converged cluster must always report an UpToDate condition")

	return cond
}

// TestReconcileUpgradesReportsUnmanagedVersions covers the decision that an
// empty spec version means "not managed by this controller", not "upgrade
// to resolveVersions' own pinned default" — without it, merely shipping
// this feature would start an unrequested rolling reboot on every existing
// cluster in the fleet.
func TestReconcileUpgradesReportsUnmanagedVersions(t *testing.T) {
	t.Parallel()

	harness := upgradeFixture(t, "", "")

	assert.Empty(t, harness.bootstrapper.upgradeCalls, "an unpinned talos version must never trigger an upgrade")
	assert.Empty(t, harness.bootstrapper.upgradeConfigCalls,
		"an unpinned kubernetes version must never re-apply config")

	cond := upToDateCondition(t, harness.client)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "VersionsUnmanaged", cond.Reason)
}

// TestReconcileUpgradesRecordsObservedVersions covers the observed-version
// half of the status model: a cluster already running exactly what it pins
// reports both versions and goes UpToDate, and the per-member Instance
// status carries the same values.
func TestReconcileUpgradesRecordsObservedVersions(t *testing.T) {
	t.Parallel()

	harness := upgradeFixture(t, testTalosVersionFixture, testUpgradeRunningKubernetesVersion)

	assert.Empty(t, harness.bootstrapper.upgradeCalls, "a cluster already at its pinned versions has nothing to roll")
	assert.Empty(t, harness.bootstrapper.upgradeConfigCalls)

	cond := upToDateCondition(t, harness.client)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "UpToDate", cond.Reason)

	var cluster v1alpha2.TalosCluster

	require.NoError(t, harness.client.Get(context.Background(),
		types.NamespacedName{Name: testClusterName}, &cluster))
	assert.Equal(t, testTalosVersionFixture, cluster.Status.Talos.Version)
	assert.Equal(t, testUpgradeRunningKubernetesVersion, cluster.Status.Kubernetes.Version)

	var member v1alpha2.Instance

	require.NoError(t, harness.client.Get(context.Background(), types.NamespacedName{Name: cpNodeName}, &member))
	assert.Equal(t, testUpgradeRunningKubernetesVersion, member.Status.Kubernetes.Version,
		"the kubelet version must be recorded on the member itself, not only aggregated")
}

// TestReconcileUpgradesAcceptsUnprefixedKubernetesVersion covers the
// normalization KubernetesSpec's own "v1.32.0" convention makes necessary:
// resolveVersions strips the leading v for Talos's own generator, so a
// spec written either way must compare equal to the v-prefixed tag a
// kubelet image actually carries — otherwise "1.32.0" would look
// permanently stale and re-apply config on every single reconcile.
func TestReconcileUpgradesAcceptsUnprefixedKubernetesVersion(t *testing.T) {
	t.Parallel()

	harness := upgradeFixture(t, "", "1.32.0")

	assert.Empty(t, harness.bootstrapper.upgradeConfigCalls)
	assert.Equal(t, "UpToDate", upToDateCondition(t, harness.client).Reason)
}

// TestReconcileUpgradesTalos covers the Talos half: a pinned version the
// members don't run starts exactly one installer upgrade, reports
// UpgradingTalos, and requeues on the short retry interval rather than the
// steady-state health-recheck cadence.
func TestReconcileUpgradesTalos(t *testing.T) {
	t.Parallel()

	harness := upgradeFixture(t, testUpgradeTalosVersion, "")

	require.Equal(t, []string{controlPlaneInstanceAddress}, harness.bootstrapper.upgradeCalls)
	assert.Equal(t, []string{testUpgradeInstallerImage}, harness.bootstrapper.upgradeImages)
	assert.Equal(t, testRetryInterval, harness.converged.RequeueAfter,
		"an in-flight upgrade must be rechecked on the retry interval, not the steady-state health cadence")

	cond := upToDateCondition(t, harness.client)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "UpgradingTalos", cond.Reason)
}

// TestReconcileUpgradesTalosBeforeKubernetes is the precedence rule: an
// edit that moves both versions at once must roll Talos first and leave
// Kubernetes entirely alone until every member is on the new Talos, since
// the Talos version is what gates which Kubernetes versions are supported
// at all.
func TestReconcileUpgradesTalosBeforeKubernetes(t *testing.T) {
	t.Parallel()

	harness := upgradeFixture(t, testUpgradeTalosVersion, testUpgradeKubernetesVersion)

	assert.Len(t, harness.bootstrapper.upgradeCalls, 1, "the talos roll must start")
	assert.Empty(t, harness.bootstrapper.upgradeConfigCalls,
		"kubernetes must not be touched while any member still runs the old talos version")
}

// TestReconcileUpgradesKubernetesOnceTalosConverged covers the second half
// of the precedence rule: with Talos already where it should be, a stale
// Kubernetes version re-applies that member's regenerated machine config.
func TestReconcileUpgradesKubernetesOnceTalosConverged(t *testing.T) {
	t.Parallel()

	harness := upgradeFixture(t, testTalosVersionFixture, testUpgradeKubernetesVersion)

	assert.Empty(t, harness.bootstrapper.upgradeCalls, "talos is already at its pinned version")
	require.Equal(t, []string{controlPlaneInstanceAddress}, harness.bootstrapper.upgradeConfigCalls)
	assert.NotEmpty(t, harness.bootstrapper.upgradeConfigs[0], "the re-applied config must actually carry bytes")

	cond := upToDateCondition(t, harness.client)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "UpgradingKubernetes", cond.Reason)
}

// TestReconcileUpgradesOneMemberAtATime is the rolling guarantee: two
// stale members must not both be rebooted in the same pass, and the
// cluster-wide observed version must stay empty while they disagree rather
// than claiming the whole cluster had already moved.
func TestReconcileUpgradesOneMemberAtATime(t *testing.T) {
	t.Parallel()

	secondCP := claimedDiscoveredInstance(secondCPNodeName, "cp-pool", secondCPInstanceAddress)

	harness := upgradeFixture(t, testUpgradeTalosVersion, "", secondCP)

	require.Len(t, harness.bootstrapper.upgradeCalls, 1, "only one member may be taken down per pass")
	assert.Equal(t, controlPlaneInstanceAddress, harness.bootstrapper.upgradeCalls[0],
		"the roll order is by name, so cp-node-1 goes before cp-node-2")

	// Model the first member coming back on the new version while the
	// second still runs the old one — a genuinely split cluster.
	harness.bootstrapper.versionForNode = map[string]string{controlPlaneInstanceAddress: testUpgradeTalosVersion}

	_, err := harness.reconciler.Reconcile(context.Background(), harness.request)
	require.NoError(t, err)

	require.Len(t, harness.bootstrapper.upgradeCalls, 2)
	assert.Equal(t, secondCPInstanceAddress, harness.bootstrapper.upgradeCalls[1],
		"the next pass moves on to the member that is still behind")

	var cluster v1alpha2.TalosCluster

	require.NoError(t, harness.client.Get(context.Background(),
		types.NamespacedName{Name: testClusterName}, &cluster))
	assert.Empty(t, cluster.Status.Talos.Version, "a split cluster reports no agreed talos version")
}

// TestReconcileUpgradesSkipUnhealthyControlPlane is the safety gate: a
// failing health recheck — which is exactly what a node still rebooting
// into its new version produces — must stop the reconciler from taking a
// second node down under it.
func TestReconcileUpgradesSkipUnhealthyControlPlane(t *testing.T) {
	t.Parallel()

	secondCP := claimedDiscoveredInstance(secondCPNodeName, "cp-pool", secondCPInstanceAddress)

	harness := upgradeFixture(t, testUpgradeTalosVersion, "", secondCP)

	require.Len(t, harness.bootstrapper.upgradeCalls, 1)

	harness.bootstrapper.healthCheckErr = assert.AnError

	_, err := harness.reconciler.Reconcile(context.Background(), harness.request)
	require.NoError(t, err)

	assert.Len(t, harness.bootstrapper.upgradeCalls, 1,
		"no further member may be upgraded while the control plane is unhealthy")
}

// TestReconcileUpgradesWaitForBootstrap is the sequencing rule zone-add
// depends on: a cluster whose seed node boots a different Talos version
// than the zone asked for must be created and bootstrapped first, and only
// upgraded once it reports ready — never both at once.
func TestReconcileUpgradesWaitForBootstrap(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, newUpgradeCluster(testUpgradeTalosVersion, ""),
		claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress))

	bootstrapper := newUpgradeBootstrapper()
	reconciler := newReconciler(fakeClient, bootstrapper)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	// The first reconcile bootstraps the control plane; addons are still
	// unreconciled, so the cluster is nowhere near Ready.
	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)

	assert.Empty(t, bootstrapper.upgradeCalls, "nothing may be upgraded while the cluster is still bootstrapping")

	var afterBootstrap v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: testClusterName},
		&afterBootstrap))
	assert.Nil(t, meta.FindStatusCondition(afterBootstrap.Status.Conditions, taloscluster.UpToDateConditionType),
		"a cluster that never reached the upgrade step must not claim anything about its versions")
}

// TestReconcileUpgradesReportsFailure covers the failure path: a refused
// upgrade RPC is surfaced on the condition rather than swallowed, and is
// retried on the next pass instead of being treated as terminal.
func TestReconcileUpgradesReportsFailure(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, newUpgradeCluster(testUpgradeTalosVersion, ""),
		claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress))

	bootstrapper := newUpgradeBootstrapper()
	bootstrapper.upgradeErr = assert.AnError

	reconciler := newReconciler(fakeClient, bootstrapper)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: testClusterName}}

	result := convergeFullyReadyCluster(t, fakeClient, reconciler, req)

	cond := upToDateCondition(t, fakeClient)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "UpgradeFailed", cond.Reason)
	assert.Equal(t, testRetryInterval, result.RequeueAfter, "a failed upgrade is retried, not given up on")
}
