package fabric

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

// reconcileTeardown runs a Fabric's deletion sequence: tears down every
// zone's own NAT gateway workload (see teardownZoneWorkload), then removes
// FabricFinalizer — mirrors zone.Reconciler.reconcileTeardown's identical
// shape and bounded-retry reasoning.
//
// This does not revert the static route NetworkConfigurer already pushed
// onto each zone's own elected gateway node — only the actively running,
// privileged workload is torn down. A stale route left on a node that's
// still part of a live TalosCluster is comparatively low-risk (it just
// points at an address on that node's own subnet); if that node itself is
// later decommissioned, zone.Reconciler's own teardown already resets it
// back to Talos maintenance mode, wiping all config including this route.
//
// Either step failing (e.g. a zone's downstream cluster is genuinely
// unreachable) sets TeardownConditionType False with a retry, up to
// r.TeardownTimeout since fabricObj's own DeletionTimestamp — past that,
// reconcileTeardown gives up and removes the finalizer anyway, rather than
// blocking the Fabric's deletion forever.
func (r *Reconciler) reconcileTeardown(ctx context.Context, fabricObj *v1alpha2.Fabric) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(fabricObj, FabricFinalizer) {
		return ctrl.Result{}, nil
	}

	if r.teardownTimedOut(fabricObj) {
		r.Logger.Warn(
			"giving up on fabric teardown after exceeding the teardown timeout — "+
				"one or more zones' nat gateway workloads may still need manual cleanup",
			"fabric", fabricObj.Name, "timeout", r.TeardownTimeout)

		return r.removeFinalizer(ctx, fabricObj)
	}

	err := r.teardownZones(ctx, fabricObj)
	if err != nil {
		r.Logger.Warn("nat gateway teardown not yet complete", "fabric", fabricObj.Name, "error", err)

		return r.setTeardownCondition(ctx, fabricObj, reasonNATTeardownFailed, r.teardownRetryMessage(fabricObj, err))
	}

	return r.removeFinalizer(ctx, fabricObj)
}

// teardownZones tears down every zone's own NAT gateway workload — a zone
// entry with no GatewayNodeRef never had one installed (either NAT was
// disabled, or no candidate was ever resolved), so there's nothing to do
// for it. Zones are listed once up front (see listZonesForRegion) rather
// than once per zone, since teardownZoneWorkload only ever needs a single
// lookup out of that same, unchanging map.
func (r *Reconciler) teardownZones(ctx context.Context, fabricObj *v1alpha2.Fabric) error {
	zonesByName, err := r.listZonesForRegion(ctx, fabricObj)
	if err != nil {
		return err
	}

	for _, zoneStatus := range fabricObj.Status.Zones {
		if zoneStatus.GatewayNodeRef == nil {
			continue
		}

		zoneObj, found := zonesByName[zoneStatus.Zone]
		if !found {
			continue
		}

		err := r.teardownZoneWorkload(ctx, fabricObj, zoneObj, zoneStatus)
		if err != nil {
			return fmt.Errorf("zone %q: %w", zoneStatus.Zone, err)
		}
	}

	return nil
}

// teardownZoneWorkload deletes zoneStatus's own NAT gateway Deployment from
// its downstream cluster. A zone (or its TalosCluster, or its stored
// kubeconfig) that's already gone means there's nothing left reachable to
// tear down — treated as success, not an error, the same tolerance
// zone.Reconciler.teardownDownstream already gives an unreachable
// downstream during its own teardown.
func (r *Reconciler) teardownZoneWorkload(
	ctx context.Context, fabricObj *v1alpha2.Fabric, zoneObj v1alpha2.Zone, zoneStatus v1alpha2.FabricZoneStatus,
) error {
	var cluster v1alpha2.TalosCluster

	err := r.Client.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: fabricObj.Namespace}, &cluster)
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to get talos cluster %q: %w", zoneObj.Name, err)
	}

	kubeconfig, err := loadClusterKubeconfig(ctx, r.Client, &cluster)
	if err != nil {
		r.Logger.Info("no downstream kubeconfig available, skipping nat gateway teardown",
			"fabric", fabricObj.Name, "zone", zoneStatus.Zone, "error", err)

		return nil
	}

	downstream, err := r.DownstreamClientBuilder.Build(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build downstream client for zone %q: %w", zoneStatus.Zone, err)
	}

	// The Deployment's own name is scoped by its own WAN interface (see
	// ManagerDeploymentName's own doc) — reconstructing it needs the
	// same classifyGatewayInterfaces result reconcileNetworkConfig
	// originally used. A gateway Instance already gone by teardown time
	// means there's no way left to know which interface-scoped Deployment
	// name it was — treated the same "nothing left reachable" way a
	// missing kubeconfig is treated above, not as an error.
	var gatewayNode v1alpha2.Instance

	gatewayNodeKey := client.ObjectKey{Name: zoneStatus.GatewayNodeRef.Name, Namespace: fabricObj.Namespace}

	err = r.Client.Get(ctx, gatewayNodeKey, &gatewayNode)
	if apierrors.IsNotFound(err) {
		r.Logger.Info("gateway instance already gone, skipping nat gateway teardown",
			"fabric", fabricObj.Name, "zone", zoneStatus.Zone, "node", zoneStatus.GatewayNodeRef.Name)

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to get gateway instance %q: %w", zoneStatus.GatewayNodeRef.Name, err)
	}

	wan, _ := classifyGatewayInterfaces(gatewayNode)
	if wan == "" {
		return nil
	}

	return deleteFabricManagerWorkload(ctx, downstream, fabricObj.Name, wan)
}

// teardownTimedOut reports whether fabricObj has been stuck in teardown for
// longer than r.TeardownTimeout — see reconcileTeardown's own doc.
func (r *Reconciler) teardownTimedOut(fabricObj *v1alpha2.Fabric) bool {
	if fabricObj.DeletionTimestamp.IsZero() {
		return false
	}

	return time.Since(fabricObj.DeletionTimestamp.Time) > r.TeardownTimeout
}

// teardownDeadline is the absolute time reconcileTeardown gives up by —
// mirrors zone.Reconciler.teardownDeadline's identical doc.
func (r *Reconciler) teardownDeadline(fabricObj *v1alpha2.Fabric) time.Time {
	return fabricObj.DeletionTimestamp.Add(r.TeardownTimeout)
}

// teardownRetryMessage formats a teardown-step failure for
// TeardownConditionType's own Message — mirrors
// zone.Reconciler.teardownRetryMessage's identical doc.
func (r *Reconciler) teardownRetryMessage(fabricObj *v1alpha2.Fabric, err error) string {
	return fmt.Sprintf(
		"%s — will keep retrying until %s, after which the finalizer is removed automatically",
		err, r.teardownDeadline(fabricObj).Format(time.RFC3339))
}

// setTeardownCondition sets TeardownConditionType False and persists
// fabricObj's status, always requeuing after r.RetryInterval — mirrors
// zone.Reconciler.setTeardownCondition's identical doc.
func (r *Reconciler) setTeardownCondition(
	ctx context.Context, fabricObj *v1alpha2.Fabric, reason, message string,
) (ctrl.Result, error) {
	changed := meta.SetStatusCondition(&fabricObj.Status.Conditions, metav1.Condition{
		Type: TeardownConditionType, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	})

	if changed {
		err := r.Client.Status().Update(ctx, fabricObj)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update fabric %q status: %w", fabricObj.Name, err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}

// removeFinalizer removes FabricFinalizer from fabricObj and persists that
// — mirrors zone.Reconciler.removeFinalizer's identical doc.
func (r *Reconciler) removeFinalizer(ctx context.Context, fabricObj *v1alpha2.Fabric) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(fabricObj, FabricFinalizer)

	err := r.Client.Update(ctx, fabricObj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from fabric %q: %w", fabricObj.Name, err)
	}

	return ctrl.Result{}, nil
}
