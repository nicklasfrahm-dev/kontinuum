package zone

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// reconcileTeardown runs a Zone's deletion sequence — see issue #49's own
// scope. A cross-cluster owner reference from the hub's Zone to objects
// living on the zone's own downstream cluster is impossible (two different
// apiservers), so Kubernetes GC can't cascade-delete this package's own
// downstream footprint the way it does for same-cluster owner refs (see
// pkg/domain/zone/add.go's identical note about hub-side owner refs); a
// finalizer, driven from here, is the only mechanism that runs at all.
//
// The downstream cluster must be torn down (or safely skipped — see
// teardownDownstream) before the TalosCluster is deleted, since a Zone
// whose downstream is unreachable needs that same kubeconfig-based
// teardown step retried, and a deleted TalosCluster's own Secret (holding
// it) is on borrowed time once GC gets to it.
//
// This no longer issues the Talos Reset itself. Deleting the TalosCluster
// here sets its own DeletionTimestamp, which taloscluster.Reconciler checks
// before touching any member (see TalosClusterFinalizer's own doc) — that
// alone is why it's deleted explicitly rather than left for GC: without it,
// taloscluster.Reconciler is a self-healing reconciler with no way to know
// a member is being intentionally decommissioned, and would keep trying to
// re-apply configuration to it (observed for real: a node reset via
// hack/reset-hetzner-node.sh came back up in maintenance mode and got
// reconfigured back into the cluster within about a minute, because
// nothing had told the still-live TalosCluster reconciler to stop). The
// actual reset now happens per-Instance, via
// taloscluster.InstanceResetReconciler's own finalizer, once GC (see
// pkg/cli/serve.go's WithGarbageCollector) cascades each Instance's
// deletion from here — the same mechanism that resets a worker being
// scaled down or an Instance released from a pool, not just a full Zone
// teardown.
//
// Either step failing (e.g. the downstream cluster is genuinely
// unreachable — hardware pulled, network gone) sets TeardownConditionType
// False with a retry, up to r.TeardownTimeout since zoneObj's own
// DeletionTimestamp — past that, reconcileTeardown gives up and removes the
// finalizer anyway, rather than blocking the Zone's deletion forever
// (issue #49's own explicit requirement). See docs/workflows/zone-remove.md
// for the operator escape hatch that forces this sooner, and for what
// "giving up" leaves behind.
func (r *Reconciler) reconcileTeardown(ctx context.Context, zoneObj *v1alpha2.Zone) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(zoneObj, ZoneFinalizer) {
		return ctrl.Result{}, nil
	}

	if r.teardownTimedOut(zoneObj) {
		r.Logger.Warn(
			"giving up on zone teardown after exceeding the teardown timeout — "+
				"downstream cluster and/or seed node may still need manual cleanup, see docs/workflows/zone-remove.md",
			"zone", zoneObj.Name, "timeout", r.TeardownTimeout)

		return r.removeFinalizer(ctx, zoneObj)
	}

	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: zoneObj.Namespace}, &cluster)
	if apierrors.IsNotFound(err) {
		// No TalosCluster ever existed, or it's already gone — nothing left
		// to tear down downstream, and nothing left for
		// InstanceResetReconciler to have not already handled.
		return r.removeFinalizer(ctx, zoneObj)
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get talos cluster %q for teardown: %w", zoneObj.Name, err)
	}

	err = r.teardownDownstream(ctx, zoneObj, &cluster)
	if err != nil {
		r.Logger.Warn("downstream teardown not yet complete", "zone", zoneObj.Name, "error", err)

		return r.setTeardownCondition(ctx, zoneObj, reasonDownstreamTeardownFailed,
			r.teardownRetryMessage(zoneObj, err))
	}

	// See reconcileTeardown's own doc for why this is deleted explicitly,
	// here, rather than left for GC. Retrying a Delete on an object already
	// mid-deletion (or already gone) is a safe no-op either way.
	err = r.Client.Delete(ctx, &cluster)
	if err != nil && !apierrors.IsNotFound(err) {
		r.Logger.Warn("failed to delete talos cluster", "zone", zoneObj.Name, "error", err)

		return r.setTeardownCondition(ctx, zoneObj, reasonTalosClusterDeleteFailed,
			r.teardownRetryMessage(zoneObj, err))
	}

	return r.removeFinalizer(ctx, zoneObj)
}

// teardownDownstream connects to zoneObj's own downstream cluster and
// deletes everything reconcileInstall ever created there, in exactly the
// reverse of installWorkload/installNetwork's own order: HTTPRoute,
// Certificate, Gateway, ClusterIssuer (network.go), then Deployment,
// Service, the kontinuum-env Secret/ConfigMap, and finally the
// kontinuum-system namespace itself (workload.go) — deleting the namespace
// last cascades away anything not explicitly listed here too (e.g.
// cert-manager's own Certificate-issued TLS Secret, or any ACME challenge
// objects it left behind).
//
// A missing kubeconfig (cluster.Status.SecretRef never populated, or its
// Secret already gone) means the downstream cluster never got far enough to
// have anything installed, or was already destroyed out-of-band — either
// way there's nothing reachable to tear down, so this is treated as
// success, not an error, letting teardown proceed straight to the Talos
// Reset step. Any other failure (most commonly: the downstream cluster is
// genuinely unreachable) is returned as-is, so reconcileTeardown's own
// bounded retry/timeout handles it.
func (r *Reconciler) teardownDownstream(
	ctx context.Context, zoneObj *v1alpha2.Zone, cluster *v1alpha2.TalosCluster,
) error {
	kubeconfig, err := loadClusterKubeconfig(ctx, r.Client, cluster)
	if err != nil {
		r.Logger.Info("no downstream kubeconfig available, skipping downstream teardown",
			"zone", zoneObj.Name, "error", err)

		return nil
	}

	downstream, err := r.DownstreamClientBuilder.Build(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build downstream client for %q: %w", zoneObj.Name, err)
	}

	err = uninstallNetwork(ctx, downstream)
	if err != nil {
		return err
	}

	return uninstallWorkload(ctx, downstream)
}

// uninstallNetwork deletes the ClusterIssuer, Gateway, Certificate, and
// HTTPRoute installNetwork installs — see teardownDownstream's own doc for
// ordering.
func uninstallNetwork(ctx context.Context, downstream client.Client) error {
	err := deleteHTTPRoute(ctx, downstream, downstreamNamespace, httpRouteName)
	if err != nil {
		return err
	}

	err = deleteCertificate(ctx, downstream, downstreamNamespace, certificateName)
	if err != nil {
		return err
	}

	err = deleteGateway(ctx, downstream, downstreamNamespace, gatewayName)
	if err != nil {
		return err
	}

	return deleteClusterIssuer(ctx, downstream, clusterIssuerName)
}

// uninstallWorkload deletes the Deployment, Service, kontinuum-env
// Secret/ConfigMap, and finally the namespace itself, all of which
// installWorkload installs — see teardownDownstream's own doc for ordering.
func uninstallWorkload(ctx context.Context, downstream client.Client) error {
	err := deleteDeployment(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	err = deleteService(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	err = deleteConfigMap(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	err = deleteSecret(ctx, downstream, downstreamNamespace)
	if err != nil {
		return err
	}

	return deleteNamespace(ctx, downstream, downstreamNamespace)
}

// teardownTimedOut reports whether zoneObj has been stuck in teardown for
// longer than r.TeardownTimeout — see reconcileTeardown's own doc.
func (r *Reconciler) teardownTimedOut(zoneObj *v1alpha2.Zone) bool {
	if zoneObj.DeletionTimestamp.IsZero() {
		return false
	}

	return time.Since(zoneObj.DeletionTimestamp.Time) > r.TeardownTimeout
}

// teardownDeadline is the absolute time reconcileTeardown gives up by,
// surfaced in status messages so an operator watching `kubectl get -o yaml`
// knows how long they have before teardown abandons itself automatically.
func (r *Reconciler) teardownDeadline(zoneObj *v1alpha2.Zone) time.Time {
	return zoneObj.DeletionTimestamp.Add(r.TeardownTimeout)
}

// teardownRetryMessage formats a teardown-step failure for
// TeardownConditionType's own Message — see reconcileTeardown's own doc for
// why a bounded timeout, not an indefinite retry, is the point being
// surfaced here.
func (r *Reconciler) teardownRetryMessage(zoneObj *v1alpha2.Zone, err error) string {
	return fmt.Sprintf(
		"%s — will keep retrying until %s, after which the finalizer is removed automatically; "+
			"see docs/workflows/zone-remove.md to force this sooner",
		err, r.teardownDeadline(zoneObj).Format(time.RFC3339))
}

// setTeardownCondition sets TeardownConditionType False and persists
// zoneObj's status, always requeuing after r.RetryInterval — teardown never
// observes this condition True (see TeardownConditionType's own doc), so
// unlike persistStatus this has no True/no-requeue branch to make.
func (r *Reconciler) setTeardownCondition(
	ctx context.Context, zoneObj *v1alpha2.Zone, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: TeardownConditionType, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	})

	err := r.Client.Status().Update(ctx, zoneObj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update zone %q status: %w", zoneObj.Name, err)
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}

// removeFinalizer removes ZoneFinalizer from zoneObj and persists that —
// with zoneObj.DeletionTimestamp already set (the only way reconcileTeardown
// is ever reached) and no finalizers left, this Update is what actually lets
// the apiserver finish deleting zoneObj.
func (r *Reconciler) removeFinalizer(ctx context.Context, zoneObj *v1alpha2.Zone) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(zoneObj, ZoneFinalizer)

	err := r.Client.Update(ctx, zoneObj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from zone %q: %w", zoneObj.Name, err)
	}

	return ctrl.Result{}, nil
}
