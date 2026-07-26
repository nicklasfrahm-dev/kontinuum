package registry

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// TTLReconciler deletes a Kontinuum object once it has gone longer than
// StaleThreshold without a heartbeat. Each Kontinuum reconciles its own expiry
// independently (via ctrl.Result.RequeueAfter) rather than a global sweep,
// so it is safe to run unmodified from every kontinuum instance — all
// sharing the same storage backend — with no leader election.
//
// It also cleans up a Kontinuum's config Secret once the Kontinuum itself
// is gone — see Reconcile's NotFound branch. That Secret already carries an
// owner reference to its Kontinuum (see Heartbeat.ensureSecret), but
// kontinuum's own apiserver doesn't run the Kubernetes garbage collector
// that would normally act on it, so this reconciler deletes it explicitly
// instead. Every deletion path — self-deregistration, this reconciler's own
// staleness sweep below, or a direct UI/kubectl delete — ends the same way:
// the Kontinuum disappears from the watch this reconciler is already
// subscribed to, producing exactly the Reconcile call that observes it
// gone.
type TTLReconciler struct {
	Client         client.Client
	StaleThreshold time.Duration
	Logger         *slog.Logger
}

// Reconcile implements reconcile.Reconciler.
func (r *TTLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server v1alpha2.Kontinuum

	err := r.Client.Get(ctx, req.NamespacedName, &server)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.cleanupSecret(ctx, req.Name)
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
func (r *TTLReconciler) deleteStale(ctx context.Context, server *v1alpha2.Kontinuum, lastHeartbeat time.Time) error {
	err := r.Client.Delete(ctx, server)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete stale server %q: %w", server.Name, err)
	}

	r.Logger.Info("Deleted stale server", "name", server.Name, "lastHeartbeat", lastHeartbeat)

	return nil
}

// cleanupSecret deletes the config Secret belonging to the Kontinuum named
// name — see TTLReconciler's own doc for why this reconciler is the one
// that does it. Called every time Reconcile observes that Kontinuum gone,
// so a missing Secret (already cleaned up by a previous pass, or never
// created) is expected, not an error.
func (r *TTLReconciler) cleanupSecret(ctx context.Context, name string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName(name), Namespace: v1alpha2.DefaultSecretNamespace},
	}

	err := r.Client.Delete(ctx, secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("failed to delete config secret %q: %w", secret.Name, err)
	}

	r.Logger.Info("Deleted orphaned config secret", "name", secret.Name, "kontinuum", name)

	return nil
}

// Stale reports whether last is more than threshold in the past, relative to now.
func Stale(last, now time.Time, threshold time.Duration) bool {
	return now.Sub(last) >= threshold
}
