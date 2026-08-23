package taloscluster_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

// clusterRequest is testClusterName's own ctrl.Request — shared by every
// test below that reconciles the cluster itself, as opposed to
// instanceRequest's Instance-scoped equivalent in instance_reset_test.go.
func clusterRequest() ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Name: testClusterName}}
}

// bootstrapClusterSecrets runs the ordinary (non-teardown) Reconciler once
// against the cluster fixture already seeded into fakeClient, so it has a
// real secrets bundle (cluster.Status.SecretRef) to reset members
// through — ensureSecretsBundle runs unconditionally early in Reconcile,
// before the ControlPlaneReady gate, so one call is enough regardless of
// how far bootstrap itself progresses. Same fixture-setup pattern
// instance_reset_test.go's own teardown tests use.
func bootstrapClusterSecrets(t *testing.T, fakeClient client.Client, bootstrapper *fakeBootstrapper) {
	t.Helper()

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)
}

// deleteCluster deletes the testClusterName fixture already seeded into
// fakeClient — TalosClusterFinalizer (added by bootstrapClusterSecrets'
// own Reconcile call above) keeps it around with a deletionTimestamp,
// exactly like a real apiserver, ready for reconcileTeardown to pick up.
func deleteCluster(t *testing.T, fakeClient client.Client) {
	t.Helper()

	var cluster v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: testClusterName}, &cluster))
	require.NoError(t, fakeClient.Delete(context.Background(), &cluster))
}

func TestReconcileTeardownNoOpsWhenNoClaimedMembers(t *testing.T) {
	t.Parallel()

	cluster := testCluster()

	fakeClient := newFakeClient(t, cluster)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	bootstrapClusterSecrets(t, fakeClient, bootstrapper)
	deleteCluster(t, fakeClient)

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)

	assert.Empty(t, bootstrapper.resetCalls, "nothing was ever claimed, so there's nothing to reset")

	var gone v1alpha2.TalosCluster

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: testClusterName}, &gone)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestReconcileTeardownSkipsResetForNeverConfiguredMember(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	// TalosClusterFinalizer is normally added by the ordinary (non-teardown)
	// Reconcile pass — set directly here instead, since that same pass would
	// also really configure cpInstance below via the ordinary bootstrap
	// flow, defeating the point of this test.
	cluster.Finalizers = []string{taloscluster.TalosClusterFinalizer}
	// Discovered and claimed, but never actually configured.
	cpInstance := claimedDiscoveredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	deleteCluster(t, fakeClient)

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)

	assert.Empty(t, bootstrapper.resetCalls, "nothing was ever configured, so there's nothing to reset")

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &got))
	assert.NotContains(t, got.Labels, v1alpha2.LabelClaimedBy, "still released, even though nothing needed resetting")
}

// TestReconcileTeardownReleasesInstancesByDefault covers the product
// requirement this whole feature exists for: instances are inventory, not
// discarded when a cluster goes away, unless explicitly told to.
func TestReconcileTeardownReleasesInstancesByDefault(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	bootstrapClusterSecrets(t, fakeClient, bootstrapper)
	deleteCluster(t, fakeClient)

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)

	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.resetCalls)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &got))
	assert.NotContains(t, got.Labels, v1alpha2.LabelClaimedBy, "released, not deleted, by default")
	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, taloscluster.MemberConfiguredConditionType),
		"no longer describes anything real once released")

	var goneCluster v1alpha2.TalosCluster

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: testClusterName}, &goneCluster)
	assert.True(t, apierrors.IsNotFound(err), "no claimed members left, so the finalizer is removed")
}

// TestReconcileTeardownDeletesInstancesWhenUnregisterEnabled covers the
// explicit opt-in — spec.teardown.unregisterInstances — that actually
// removes an instance from inventory instead of merely resetting it.
func TestReconcileTeardownDeletesInstancesWhenUnregisterEnabled(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.Spec.Teardown.UnregisterInstances = true
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	bootstrapClusterSecrets(t, fakeClient, bootstrapper)
	deleteCluster(t, fakeClient)

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)

	assert.Equal(t, []string{controlPlaneInstanceAddress}, bootstrapper.resetCalls)

	var gone v1alpha2.Instance

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &gone)
	assert.True(t, apierrors.IsNotFound(err), "explicitly unregistered — removed from inventory, not just released")
}

// TestReconcileTeardownResetsWorkersBeforeControlPlane covers ordering: a
// worker reset dials through a live control-plane member (see
// resolveResetEndpoint), so control plane must still be up when workers
// are torn down.
func TestReconcileTeardownResetsWorkersBeforeControlPlane(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	workerInstance := configuredInstance(workerNodeName, "worker-pool", workerInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	bootstrapClusterSecrets(t, fakeClient, bootstrapper)
	deleteCluster(t, fakeClient)

	_, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)

	require.Equal(t, []string{workerInstanceAddress, controlPlaneInstanceAddress}, bootstrapper.resetCalls,
		"the worker resets first, while the control-plane member is still up to dial through")
}

// TestReconcileTeardownRetriesFailedMemberWhileReleasingOthers covers that
// one member's reset failure doesn't block progress on the rest: they're
// released in the same pass, and only the failed one is left claimed for
// the next reconcile to retry.
func TestReconcileTeardownRetriesFailedMemberWhileReleasingOthers(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)
	workerInstance := configuredInstance(workerNodeName, "worker-pool", workerInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig")}

	bootstrapClusterSecrets(t, fakeClient, bootstrapper)
	deleteCluster(t, fakeClient)

	// Only the worker's own reset fails — the control-plane member's own
	// address must still succeed.
	bootstrapper.resetErrForNode = workerInstanceAddress

	result, err := newReconciler(fakeClient, bootstrapper).Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter, "the worker's own failure must requeue rather than give up immediately")

	var worker v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: workerNodeName}, &worker))
	assert.Contains(t, worker.Labels, v1alpha2.LabelClaimedBy, "left claimed — its own reset failed, retry next pass")

	var cp v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: cpNodeName}, &cp))
	assert.NotContains(t, cp.Labels, v1alpha2.LabelClaimedBy,
		"released in the same pass despite the worker's own failure alongside it")

	var stillThere v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: testClusterName}, &stillThere),
		"still has a claimed member, so the finalizer must not be removed yet")
}

func TestReconcileTeardownGivesUpAfterTeardownTimeout(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cpInstance := configuredInstance(cpNodeName, "cp-pool", controlPlaneInstanceAddress)

	fakeClient := newFakeClient(t, cluster, cpInstance)
	bootstrapper := &fakeBootstrapper{kubeconfig: []byte("fake-kubeconfig"), resetErr: assert.AnError}

	bootstrapClusterSecrets(t, fakeClient, bootstrapper)
	deleteCluster(t, fakeClient)

	reconciler := &taloscluster.Reconciler{
		Client:          fakeClient,
		Bootstrapper:    bootstrapper,
		RetryInterval:   testRetryInterval,
		TeardownTimeout: 0, // effectively already elapsed the instant DeletionTimestamp is set
		Locker:          zonelease.NewLocker(fakeClient, fakeClient, "test-hub", "", 0),
		Logger:          slog.Default(),
	}

	result, err := reconciler.Reconcile(context.Background(), clusterRequest())
	require.NoError(t, err)
	assert.Zero(t, result)

	var gone v1alpha2.TalosCluster

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: testClusterName}, &gone)
	assert.True(t, apierrors.IsNotFound(err), "gives up and removes the finalizer anyway")
}
