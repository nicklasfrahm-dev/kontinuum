package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// secretNamePrefix makes a Kontinuum's config Secret recognizable by name
// alone (e.g. "kontinuum-worker-1"), rather than colliding namespace-wide
// with anything else named after the instance — shared by Heartbeat, which
// creates the Secret, and TTLReconciler, which deletes it once the
// Kontinuum itself is gone. This is a name prefix, not a credential — gosec's
// G101 flags it purely because "SECRET" appears in the identifier.
//
//nolint:gosec // false positive: a name prefix, not a credential value
const secretNamePrefix = "kontinuum-"

// secretName derives the Secret name for a Kontinuum instance named
// instanceName — see secretNamePrefix.
func secretName(instanceName string) string {
	return secretNamePrefix + instanceName
}

// deregisterTimeout bounds a Deregister call's own Delete request — used by
// both Controller.Deregister and Start's ctx.Done() fallback.
const deregisterTimeout = 5 * time.Second

// Heartbeat registers this process as a Kontinuum object, keeps its
// status.lastHeartbeatTime (and status.version, status.secretRef) fresh on
// an interval, deletes it on Deregister (normally called by
// Controller.Deregister, or as a fallback when ctx is canceled — see
// Start's doc), and re-registers it immediately if it's deleted out from
// under this process by anything else before that. It implements both
// manager.Runnable (the heartbeat ticker, added via mgr.Add — see
// Controller.SetupWithManager) and reconcile.Reconciler (Reconcile, invoked
// for its own object by the combinedReconciler both this and TTLReconciler
// are registered through).
type Heartbeat struct {
	Client client.Client
	Name   string
	// Role is written to status.role on every heartbeat — see registry.Role,
	// which derives it from Spec.Region and Spec.Zone.
	Role     string
	Spec     v1alpha2.KontinuumSpec
	Interval time.Duration
	Logger   *slog.Logger
	// Version is this process's build version, written to status.version on
	// every heartbeat.
	Version string
	// SecretData is this process's confidential configuration —
	// KONTINUUM_-prefixed keys matching pkg/config's env var names (e.g.
	// KONTINUUM_SERVER_STORAGE) — kept out of status itself (see
	// KontinuumStatus.SecretRef's doc) and instead upserted into a Secret on
	// every heartbeat via ensureSecret.
	SecretData map[string]string
	// Config is this process's own non-confidential configuration, written
	// to status.config on every heartbeat.
	Config v1alpha2.KontinuumConfigStatus

	// shuttingDown is set by Deregister, before it deletes this instance's
	// own object. Reconcile and beat both treat a NotFound they observe
	// afterward as that same deletion landing, not an external one to
	// self-heal from — without this, either one racing Deregister's DELETE
	// would immediately recreate the object Deregister is in the middle of
	// removing.
	shuttingDown atomic.Bool
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
		if h.shuttingDown.Load() {
			return ctrl.Result{}, nil
		}

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
		ObjectMeta: metav1.ObjectMeta{Name: h.Name, Namespace: v1alpha2.DefaultSecretNamespace},
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
			// ctx is already canceled here, so stripped of that cancellation
			// and given a fresh, bounded timeout — see Deregister's doc for
			// why this is only a fallback: the normal path is
			// Controller.Deregister running before the manager (and this
			// ctx) is ever canceled.
			deregisterCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deregisterTimeout)
			defer cancel()

			err := h.Deregister(deregisterCtx)
			if err != nil {
				h.Logger.Error("Failed to deregister server", "name", h.Name, "error", err)
			}

			return nil
		}
	}
}

// Deregister marks h as shutting down — so Reconcile and beat stop treating
// a NotFound as an external deletion to self-heal from — and deletes h's own
// Kontinuum object. This is what makes graceful shutdown delete the object
// instead of waiting for the TTL reconciler.
//
// Called two ways: by Controller.Deregister, before the controller manager
// (and the webhook server its conversion path depends on — see
// Controller.Deregister's doc) is torn down; and, as a fallback, by Start's
// own ctx.Done() case, in case Controller.Deregister was never called (e.g.
// a test driving Heartbeat.Start directly). Idempotent — a NotFound from an
// already-completed delete is not an error, so calling it from both places
// in the same shutdown is safe.
func (h *Heartbeat) Deregister(ctx context.Context) error {
	h.shuttingDown.Store(true)

	server := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: h.Name, Namespace: v1alpha2.DefaultSecretNamespace},
	}

	err := h.Client.Delete(ctx, server)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to deregister server %q: %w", h.Name, err)
	}

	return nil
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
		if h.shuttingDown.Load() {
			return
		}

		h.Logger.Warn("Server object missing, re-registering", "name", h.Name)
		h.reregister(ctx, server)

		return
	}

	if err != nil {
		h.Logger.Error("Failed to refresh server before heartbeat", "name", h.Name, "error", err)

		return
	}

	secretRef, err := h.ensureSecret(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to ensure server config secret", "name", h.Name, "error", err)

		return
	}

	server.Status.Role = h.Role
	server.Status.LastHeartbeatTime = metav1.Now()
	server.Status.Version = h.Version
	server.Status.SecretRef = secretRef
	server.Status.Config = h.Config

	err = h.Client.Status().Update(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to send server heartbeat", "name", h.Name, "error", err)
	}
}

// ensureNamespace creates namespace if it doesn't already exist — shared by
// ensureSecret and reregister, both of which need it to exist before
// creating a namespaced object inside it.
func ensureNamespace(ctx context.Context, kubeClient client.Client, namespace string) error {
	err := kubeClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q namespace: %w", namespace, err)
	}

	return nil
}

// secretRef is where ensureSecret upserts h.SecretData: a Secret named
// after this instance, in v1alpha2.DefaultSecretNamespace — the same
// namespace this instance's own Kontinuum object lives in (see Start).
func (h *Heartbeat) secretRef() v1alpha2.KontinuumSecretReference {
	return v1alpha2.KontinuumSecretReference{
		Name:      secretName(h.Name),
		Namespace: v1alpha2.DefaultSecretNamespace,
	}
}

// ensureSecret upserts the Secret h.secretRef points to with h.SecretData,
// owned by server (see controllerutil.SetControllerReference) so a real
// Kubernetes garbage collector would clean it up the moment server is
// deleted — kontinuum's own apiserver doesn't run one today, so
// TTLReconciler's Reconcile deletes it explicitly instead the next time it
// observes server gone; the owner reference is set regardless, both because
// it's the correct thing to record and in case that ever changes. It also
// creates the Secret's namespace first if it doesn't already exist. All of
// this runs on every beat/reregister, not just once at startup, so it's
// self-healing the same way the rest of this file is: if the namespace or
// Secret is deleted out from under a running process — manually, or by
// anything else with access — the next tick recreates them rather than
// leaving status.secretRef pointing at nothing.
func (h *Heartbeat) ensureSecret(
	ctx context.Context, server *v1alpha2.Kontinuum,
) (v1alpha2.KontinuumSecretReference, error) {
	ref := h.secretRef()

	err := ensureNamespace(ctx, h.Client, ref.Namespace)
	if err != nil {
		return ref, err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace},
		StringData: h.SecretData,
	}

	err = controllerutil.SetControllerReference(server, secret, h.Client.Scheme())
	if err != nil {
		return ref, fmt.Errorf("failed to set owner reference on %q secret: %w", ref.Name, err)
	}

	err = h.Client.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Secret

		err = h.Client.Get(ctx, client.ObjectKeyFromObject(secret), &existing)
		if err != nil {
			return ref, fmt.Errorf("failed to fetch existing %q secret: %w", ref.Name, err)
		}

		existing.StringData = h.SecretData

		err = controllerutil.SetControllerReference(server, &existing, h.Client.Scheme())
		if err != nil {
			return ref, fmt.Errorf("failed to set owner reference on %q secret: %w", ref.Name, err)
		}

		err = h.Client.Update(ctx, &existing)
	}

	if err != nil {
		return ref, fmt.Errorf("failed to ensure %q secret: %w", ref.Name, err)
	}

	return ref, nil
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
		ObjectMeta: metav1.ObjectMeta{Name: h.Name, Namespace: v1alpha2.DefaultSecretNamespace},
		Spec:       h.Spec,
	}

	err := ensureNamespace(ctx, h.Client, server.Namespace)
	if err != nil {
		h.Logger.Error("Failed to ensure server namespace", "name", h.Name, "error", err)

		return
	}

	err = h.Client.Create(ctx, server)
	if err != nil && apierrors.IsAlreadyExists(err) {
		err = h.Client.Get(ctx, client.ObjectKeyFromObject(server), server)
	}

	if err != nil {
		h.Logger.Error("Failed to re-register server", "name", h.Name, "error", err)

		return
	}

	secretRef, err := h.ensureSecret(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to ensure server config secret after re-registering", "name", h.Name, "error", err)

		return
	}

	server.Status.Role = h.Role
	server.Status.LastHeartbeatTime = metav1.Now()
	server.Status.Version = h.Version
	server.Status.SecretRef = secretRef
	server.Status.Config = h.Config

	err = h.Client.Status().Update(ctx, server)
	if err != nil {
		h.Logger.Error("Failed to send server heartbeat after re-registering", "name", h.Name, "error", err)
	}
}
