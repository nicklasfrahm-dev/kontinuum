package registry

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
)

// deregisterTimeout bounds the final Delete call Heartbeat.Start makes
// after its ctx is canceled.
const deregisterTimeout = 5 * time.Second

// Heartbeat registers this process as a Kontinuum object, keeps its
// status.lastHeartbeatTime fresh on an interval, and deletes it when ctx is
// canceled. It implements manager.Runnable.
type Heartbeat struct {
	Client   client.Client
	Name     string
	Spec     v1alpha1.KontinuumSpec
	Interval time.Duration
	Logger   *slog.Logger
}

// Start implements manager.Runnable. It blocks until ctx is canceled.
func (h *Heartbeat) Start(ctx context.Context) error {
	server := &v1alpha1.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: h.Name},
		Spec:       h.Spec,
	}

	err := h.Client.Create(ctx, server)
	if err != nil {
		return fmt.Errorf("failed to create server %q: %w", h.Name, err)
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

// beat refreshes server's status.lastHeartbeatTime. Failures are logged,
// not fatal — the next tick tries again.
func (h *Heartbeat) beat(ctx context.Context, server *v1alpha1.Kontinuum) {
	server.Status.LastHeartbeatTime = metav1.Now()

	err := h.Client.Status().Update(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to send server heartbeat", "name", h.Name, "error", err)
	}
}

// deregister deletes server. ctx is already canceled by the time deregister
// is called (that cancellation is what triggers it), so its cancellation is
// stripped and replaced with a fresh, bounded timeout — any request-scoped
// values on ctx are kept. This is what makes graceful shutdown delete the
// Kontinuum object instead of waiting for the TTL reconciler.
func (h *Heartbeat) deregister(ctx context.Context, server *v1alpha1.Kontinuum) {
	deregisterCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deregisterTimeout)
	defer cancel()

	err := h.Client.Delete(deregisterCtx, server)
	if err != nil && !apierrors.IsNotFound(err) {
		h.Logger.Error("Failed to deregister server", "name", h.Name, "error", err)
	}
}
