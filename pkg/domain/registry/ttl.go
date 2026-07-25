package registry

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
)

// TTLReconciler deletes a Kontinuum object once it has gone longer than
// StaleThreshold without a heartbeat. Each Kontinuum reconciles its own expiry
// independently (via ctrl.Result.RequeueAfter) rather than a global sweep,
// so it is safe to run unmodified from every kontinuum instance — all
// sharing the same storage backend — with no leader election.
type TTLReconciler struct {
	Client         client.Client
	StaleThreshold time.Duration
	Logger         *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *TTLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server v1alpha1.Kontinuum

	err := r.Client.Get(ctx, req.NamespacedName, &server)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get server %q: %w", req.Name, err)
	}

	last := server.Status.LastHeartbeatTime.Time
	if last.IsZero() {
		last = server.CreationTimestamp.Time
	}

	now := time.Now()
	if Stale(last, now, r.StaleThreshold) {
		return ctrl.Result{}, r.deleteStale(ctx, &server, last)
	}

	return ctrl.Result{RequeueAfter: r.StaleThreshold - now.Sub(last)}, nil
}

// deleteStale deletes server, which was last seen at lastHeartbeat.
func (r *TTLReconciler) deleteStale(ctx context.Context, server *v1alpha1.Kontinuum, lastHeartbeat time.Time) error {
	err := r.Client.Delete(ctx, server)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete stale server %q: %w", server.Name, err)
	}

	r.Logger.Info("Deleted stale server", "name", server.Name, "lastHeartbeat", lastHeartbeat)

	return nil
}

// Stale reports whether last is more than threshold in the past, relative to now.
func Stale(last, now time.Time, threshold time.Duration) bool {
	return now.Sub(last) >= threshold
}
