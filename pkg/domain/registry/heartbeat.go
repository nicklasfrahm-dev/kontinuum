package registry

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// deregisterTimeout bounds the final Delete call Heartbeat.Start makes
// after its ctx is canceled.
const deregisterTimeout = 5 * time.Second

// Heartbeat registers this process as a Kontinuum object, keeps its
// status.lastHeartbeatTime fresh on an interval, deletes it when ctx is
// canceled, and re-registers it immediately if it's deleted out from under
// this process by anything else. It implements both manager.Runnable (the
// heartbeat ticker, added via mgr.Add — see Controller.SetupWithManager)
// and reconcile.Reconciler (Reconcile, invoked for its own object by the
// combinedReconciler both this and TTLReconciler are registered through).
type Heartbeat struct {
	Client client.Client
	Name   string
	// Role is written to status.role on every heartbeat — see registry.Role,
	// which derives it from Spec.Region and Spec.Zone.
	Role     string
	Spec     v1alpha2.KontinuumSpec
	Interval time.Duration
	Logger   *slog.Logger
}

// Reconcile implements reconcile.Reconciler, reacting to its own object's
// deletion the moment combinedReconciler's watch sees it, rather than
// waiting up to Interval for Start's own ticker to notice (see beat's
// doc). A NotFound here means it was deleted — by the UI's delete button,
// kubectl, anything — and Reconcile re-registers it immediately. Any other
// state (the object exists, or a transient fetch error) needs no action:
// Start's own ticker owns keeping it fresh.
func (h *Heartbeat) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server v1alpha2.Kontinuum

	err := h.Client.Get(ctx, req.NamespacedName, &server)
	if apierrors.IsNotFound(err) {
		h.Logger.Warn("Server object deleted, re-registering", "name", h.Name)
		h.reregister(ctx, &server)

		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get server %q: %w", req.Name, err)
	}

	return ctrl.Result{}, nil
}

// Start implements manager.Runnable. It blocks until ctx is canceled. It
// has no separate initial-registration step: beat's own Get-first logic
// already handles both "doesn't exist yet" (creates it) and "already
// exists" (a hot-reload restart racing the previous process's own graceful
// deregistration, or one the TTL reconciler hasn't expired yet — Get just
// succeeds and beat proceeds straight to updating it) uniformly, with
// nothing left over to special-case here.
func (h *Heartbeat) Start(ctx context.Context) error {
	server := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: h.Name},
		Spec:       h.Spec,
	}

	h.beat(ctx, server)

	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.beat(ctx, server)
		case <-ctx.Done():
			h.deregister(ctx, server)

			return nil
		}
	}
}

// beat refreshes server's status.lastHeartbeatTime. It always re-fetches
// server first, rather than trusting whatever resourceVersion/UID it last
// held in memory: if this process's own copy ever falls out of sync with
// what's actually stored — the object was deleted and recreated out from
// under it (manually, via the UI's delete button, or by kubectl), or a
// previous tick's write only partially landed — updating against a stale
// local copy fails with a precondition/conflict error every single tick
// thereafter, not just once. Refreshing first makes each tick self-healing
// regardless of why the local copy drifted. If the object is genuinely
// gone, the fetch itself comes back NotFound, and beat re-registers by
// recreating it (see reregister) rather than leaving this instance
// permanently deregistered until the process restarts. Any other failure
// is logged, not fatal — the next tick tries again.
func (h *Heartbeat) beat(ctx context.Context, server *v1alpha2.Kontinuum) {
	err := h.Client.Get(ctx, client.ObjectKeyFromObject(server), server)
	if apierrors.IsNotFound(err) {
		h.Logger.Warn("Server object missing, re-registering", "name", h.Name)
		h.reregister(ctx, server)

		return
	}

	if err != nil {
		h.Logger.Error("Failed to refresh server before heartbeat", "name", h.Name, "error", err)

		return
	}

	server.Status.Role = h.Role
	server.Status.LastHeartbeatTime = metav1.Now()

	err = h.Client.Status().Update(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to send server heartbeat", "name", h.Name, "error", err)
	}
}

// reregister resets server to a fresh object — clearing any resourceVersion
// or UID left over from the one that was deleted — and recreates it with
// this instance's own Spec, then immediately gives it a fresh heartbeat.
// Reconcile (triggered by the delete event) and beat (self-healing on its
// own next tick) can both reach here for the same deletion; if Create loses
// that race with AlreadyExists, that's not a failure — it just fetches
// whatever the other path already recreated instead.
func (h *Heartbeat) reregister(ctx context.Context, server *v1alpha2.Kontinuum) {
	*server = v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: h.Name},
		Spec:       h.Spec,
	}

	err := h.Client.Create(ctx, server)
	if err != nil && apierrors.IsAlreadyExists(err) {
		err = h.Client.Get(ctx, client.ObjectKeyFromObject(server), server)
	}

	if err != nil {
		h.Logger.Error("Failed to re-register server", "name", h.Name, "error", err)

		return
	}

	server.Status.Role = h.Role
	server.Status.LastHeartbeatTime = metav1.Now()

	err = h.Client.Status().Update(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to send server heartbeat after re-registering", "name", h.Name, "error", err)
	}
}

// deregister deletes server. ctx is already canceled by the time deregister
// is called (that cancellation is what triggers it), so its cancellation is
// stripped and replaced with a fresh, bounded timeout — any request-scoped
// values on ctx are kept. This is what makes graceful shutdown delete the
// Kontinuum object instead of waiting for the TTL reconciler.
func (h *Heartbeat) deregister(ctx context.Context, server *v1alpha2.Kontinuum) {
	deregisterCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deregisterTimeout)
	defer cancel()

	err := h.Client.Delete(deregisterCtx, server)
	if err != nil && !apierrors.IsNotFound(err) {
		h.Logger.Error("Failed to deregister server", "name", h.Name, "error", err)
	}
}
