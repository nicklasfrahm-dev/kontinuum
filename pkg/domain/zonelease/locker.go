// Package zonelease implements a lightweight, per-zone mutual-exclusion
// primitive backed by coordination.k8s.io/v1 Lease objects. Every
// kontinuum-server process — the hub and every joined zone's own downstream
// deployment alike — runs the identical set of domain controllers (zone,
// addon, taloscluster, instancepool, instance, adminrbac, registry) against
// the same central storage: a joined zone's KONTINUUM_SERVER_STORAGE is an
// etcdproxy relay back to the hub's own storage (see pkg/domain/etcdproxy),
// not a separate database. Without this package, two independent
// controller-runtime managers reconcile the exact same zone-owned objects
// at the same time — the cause of repeated 409 "the object has been
// modified" conflicts and Helm racing itself on the same addon install.
//
// Lease objects are already natively served by libkapi's own apiserver
// (used internally for its own manager-wide leader election — see
// pkg/libkapi/leaderelection.go), so no CRD or RBAC bootstrapping is
// needed here beyond registering coordination.k8s.io/v1 on the client's own
// scheme (see pkg/cli/serve.go's buildScheme).
package zonelease

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// GlobalKey is the zone key for hub-owned/fleet-wide state that isn't
// scoped to any one zone (e.g. registry's TTL sweep, adminrbac's
// ClusterRoleBindings). With a hub, a worker (a Locker with a non-empty
// SelfZoneKey) is refused this key exactly like its own zone's key — that
// state is the hub's to reconcile. Hub-less (see Locker.HubLess), there is
// no hub to leave it to, so every peer contends for it and the Lease
// serializes them, exactly as it already does for zone keys.
const GlobalKey = ""

// leaseNamePrefix names every Lease this package creates, distinguishing
// them from any other Lease that might exist in
// v1alpha2.KontinuumSystemNamespace.
const leaseNamePrefix = "zonelock-"

// globalLeaseName is the Lease name for GlobalKey — not just
// leaseNamePrefix alone, so it can never collide with a real zone key
// (Key already guards against a zone key ever being empty, but this keeps
// the mapping unambiguous regardless).
const globalLeaseName = leaseNamePrefix + "global"

// defaultLeaseDuration is how long a Lease may go without renewal before
// another contender may take it over — long enough that a holder actively
// reconciling (renewing on every call) never loses it to a spurious
// takeover, short enough that a crashed holder's zone isn't stuck for long.
const defaultLeaseDuration = 45 * time.Second

// renewalsPerLease is how many times KeepRenewed renews a held Lease per
// lease duration while a reconcile is still running: often enough that
// one missed renewal never lets the Lease expire under it.
const renewalsPerLease = 3

// ErrLeaseLost is the cause KeepRenewed cancels its context with once the
// Lease it keeps renewed is held by someone else, is gone, or went
// unrenewed for so long that a contender may be about to take it over.
var ErrLeaseLost = errors.New("zone lease lost")

// Key derives the per-zone lock key from region/zone, matching the
// <region>-<zone> naming convention v1alpha2 objects already share (see
// pkg/domain/zone.BuildAddObjects's own doc). Empty when either is empty —
// GlobalKey, not a real zone at all.
func Key(region, zone string) string {
	if region == "" || zone == "" {
		return GlobalKey
	}

	return region + "-" + zone
}

// Identity is a Locker's own identity, threaded from pkg/cli/serve.go into
// every zone-scoped controller's own Config so each package's own
// SetupWithManager can build its own Locker (via NewLocker) once
// mgr.GetClient() exists.
type Identity struct {
	// HolderIdentity names this process — see Locker.HolderIdentity's own
	// doc.
	HolderIdentity string
	// SelfZoneKey is this process's own zone key, empty for the hub — see
	// Locker.SelfZoneKey's own doc.
	SelfZoneKey string
	// HubLess is whether this process runs in a fleet without a hub — see
	// Locker.HubLess's own doc.
	HubLess bool
}

// Locker gates zone-scoped reconciliation across every kontinuum-server
// process sharing one central storage — see this package's own doc.
// Callers build one per controller (via NewLocker, in SetupWithManager) and
// call TryAcquire at the top of every Reconcile/tick that's about to write
// anything, skipping the write entirely when it returns false, and
// KeepRenewed right after a true one.
type Locker struct {
	// Client talks to this process's own apiserver — always mgr.GetClient()
	// in production, a fake client in tests.
	Client client.Client
	// Reader bypasses Client's watch-backed cache for TryAcquire's own Get
	// — always mgr.GetAPIReader() in production, the same fake client as
	// Client in tests (client.Client already satisfies client.Reader). A
	// Get served from a stalled watch's stale cache is exactly wrong here:
	// TryAcquire would judge a Lease's holder/expiry from data that's
	// already out of date, and every Update that follows (renew, takeover)
	// carries that same stale resourceVersion as its optimistic-concurrency
	// precondition — see registry.Heartbeat.Reader's own doc for the
	// identical failure mode this avoids.
	Reader client.Reader
	// HolderIdentity names this process as a Lease's spec.holderIdentity —
	// shared with registry.Heartbeat.Name (see pkg/cli/serve.go) so a
	// process's lease identity and registry identity match.
	HolderIdentity string
	// SelfZoneKey is this process's own zone key
	// (Key(cfg.Server.Region, cfg.Server.Zone)) — empty for the hub. Unless
	// HubLess is set, a non-empty SelfZoneKey makes TryAcquire refuse both
	// this exact key and GlobalKey outright, with no API call: with a hub,
	// the hub reconciles every zone and all fleet-wide state.
	//
	// Hub-less, a zone contends for its own key like any other peer, so a
	// single-zone fleet still converges. The one exception is removal:
	// TryAcquire refuses the zone's own key while its own Zone or
	// TalosCluster is being deleted or is gone, because tearing a zone
	// down deletes the very Deployment and resets the very nodes this
	// process runs on. Another peer, or an outside process such as a hub,
	// finishes the removal.
	SelfZoneKey string
	// HubLess opens GlobalKey and SelfZoneKey to contention by every peer,
	// zones included. It follows from the storage mode (see
	// pkg/config.StorageMode.IsHubLess): in direct and NATS mode every
	// process holds an identical storage DSN, so there may be no hub at
	// all, and refusing those keys to zones would leave fleet-wide state,
	// and a fleet's only zone, silently unreconciled.
	HubLess bool
	// LeaseDuration is how long a Lease may go without renewal before
	// another contender may take it over. Defaults to defaultLeaseDuration
	// when zero — see NewLocker.
	LeaseDuration time.Duration
}

// NewLocker builds a Locker, defaulting LeaseDuration when zero. apiReader
// is typically mgr.GetAPIReader() — see Locker.Reader's own doc.
func NewLocker(
	apiClient client.Client, apiReader client.Reader, holderIdentity, selfZoneKey string, leaseDuration time.Duration,
) *Locker {
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}

	return &Locker{
		Client:         apiClient,
		Reader:         apiReader,
		HolderIdentity: holderIdentity,
		SelfZoneKey:    selfZoneKey,
		LeaseDuration:  leaseDuration,
	}
}

// NewLockerFor builds a Locker for identity with the default lease
// duration — what every controller's own SetupWithManager uses, so a field
// added to Identity reaches every Locker without touching each caller.
func NewLockerFor(apiClient client.Client, apiReader client.Reader, identity Identity) *Locker {
	locker := NewLocker(apiClient, apiReader, identity.HolderIdentity, identity.SelfZoneKey, 0)
	locker.HubLess = identity.HubLess

	return locker
}

// TryAcquire reports whether the caller may perform mutating reconciliation
// for zoneKey (GlobalKey for hub-owned/fleet-wide state) right now — see
// Locker's own doc. A false return with a nil error means "someone else
// holds this right now, or you're structurally not allowed to" — not a
// failure; callers should quietly back off and retry later (see Jitter),
// not log it as an error.
func (l *Locker) TryAcquire(ctx context.Context, zoneKey string) (bool, error) {
	if l.refused(zoneKey) {
		return false, nil
	}

	if l.SelfZoneKey != GlobalKey && zoneKey == l.SelfZoneKey {
		removing, err := l.selfRemoving(ctx)
		if err != nil || removing {
			return false, err
		}
	}

	name := leaseName(zoneKey)

	var lease coordinationv1.Lease

	err := l.Reader.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease)
	if apierrors.IsNotFound(err) {
		return l.create(ctx, name)
	}

	if err != nil {
		return false, fmt.Errorf("failed to get zone lease %q: %w", name, err)
	}

	if holderIdentity(&lease) == l.HolderIdentity {
		return l.renew(ctx, &lease)
	}

	if !expired(&lease, l.LeaseDuration) {
		return false, nil
	}

	return l.takeover(ctx, &lease)
}

// KeepRenewed renews the Lease for zoneKey, which the caller has just
// acquired with TryAcquire, in the background until the returned release
// func is called. TryAcquire only renews at the start of a reconcile, so
// without this a reconcile running longer than the lease duration — a
// Talos health check, a Helm install — would keep writing after a
// contender, such as the next pod of a rolling update, took the Lease
// over. The returned context is canceled with ErrLeaseLost as its cause
// the moment the Lease is lost, or has gone unrenewed for long enough that
// a contender may be about to take it over, so the reconcile's own writes
// stop. Callers do every write of the reconcile with that context, and
// defer release.
func (l *Locker) KeepRenewed(ctx context.Context, zoneKey string) (context.Context, context.CancelFunc) {
	held, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		l.renewUntilDone(held, cancel, zoneKey)
	}()

	release := func() {
		cancel(context.Canceled)
		<-done
	}

	return held, release
}

// refused reports whether this process is structurally not allowed to
// reconcile zoneKey at all — see SelfZoneKey's and HubLess's own docs.
func (l *Locker) refused(zoneKey string) bool {
	if l.SelfZoneKey == "" {
		return false
	}

	if zoneKey == GlobalKey || zoneKey == l.SelfZoneKey {
		return !l.HubLess
	}

	return false
}

// selfRemoving reports whether this process's own zone is being removed —
// its Zone or TalosCluster, both named SelfZoneKey, is being deleted or is
// already gone — see SelfZoneKey's own doc for why TryAcquire then refuses
// the zone's own key.
func (l *Locker) selfRemoving(ctx context.Context) (bool, error) {
	key := client.ObjectKey{Name: l.SelfZoneKey, Namespace: v1alpha2.KontinuumSystemNamespace}

	for _, obj := range []client.Object{&v1alpha2.Zone{}, &v1alpha2.TalosCluster{}} {
		err := l.Reader.Get(ctx, key, obj)
		if apierrors.IsNotFound(err) {
			return true, nil
		}

		if err != nil {
			return false, fmt.Errorf("failed to get own %T %q: %w", obj, l.SelfZoneKey, err)
		}

		if !obj.GetDeletionTimestamp().IsZero() {
			return true, nil
		}
	}

	return false, nil
}

// renewUntilDone is KeepRenewed's own loop: it renews zoneKey's Lease
// renewalsPerLease times per lease duration until ctx is done, canceling
// it with ErrLeaseLost once the Lease is lost or a renewal deadline passes.
// The deadline is one renewal interval short of the lease duration, so the
// reconcile stops before any contender could judge the Lease expired.
func (l *Locker) renewUntilDone(ctx context.Context, cancel context.CancelCauseFunc, zoneKey string) {
	interval := l.LeaseDuration / renewalsPerLease
	deadline := l.LeaseDuration - interval
	lastRenewed := time.Now()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		renewed, err := l.renewHeld(ctx, zoneKey)
		if errors.Is(err, ErrLeaseLost) {
			cancel(err)

			return
		}

		if renewed {
			lastRenewed = time.Now()
		}

		if time.Since(lastRenewed) > deadline {
			cancel(fmt.Errorf("%w: %q not renewed for %s", ErrLeaseLost, leaseName(zoneKey), deadline))

			return
		}
	}
}

// renewHeld renews zoneKey's Lease if l still holds it, returning
// ErrLeaseLost when it's gone or held by someone else. A false return
// with a nil error is a renewal that didn't land this round — see renew —
// not a lost Lease.
func (l *Locker) renewHeld(ctx context.Context, zoneKey string) (bool, error) {
	name := leaseName(zoneKey)

	var lease coordinationv1.Lease

	err := l.Reader.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease)
	if apierrors.IsNotFound(err) {
		return false, fmt.Errorf("%w: %q is gone", ErrLeaseLost, name)
	}

	if err != nil {
		return false, fmt.Errorf("failed to get zone lease %q: %w", name, err)
	}

	if holderIdentity(&lease) != l.HolderIdentity {
		return false, fmt.Errorf("%w: %q is held by %q", ErrLeaseLost, name, holderIdentity(&lease))
	}

	return l.renew(ctx, &lease)
}

// ReleaseAll frees every Lease this package created that holder still
// holds, so a successor — the next pod of a rolling update, another peer —
// takes over at once instead of waiting out the lease duration. It's meant
// for graceful shutdown, once every reconcile that could still write under
// those Leases has stopped. A Lease that changed or vanished in the
// meantime is left alone: it's no longer holder's to release.
func ReleaseAll(ctx context.Context, apiClient client.Client, holder string) (int, error) {
	var leases coordinationv1.LeaseList

	err := apiClient.List(ctx, &leases, client.InNamespace(v1alpha2.KontinuumSystemNamespace))
	if err != nil {
		return 0, fmt.Errorf("failed to list zone leases: %w", err)
	}

	released := 0

	var errs []error

	for index := range leases.Items {
		lease := &leases.Items[index]
		if !strings.HasPrefix(lease.Name, leaseNamePrefix) || holderIdentity(lease) != holder {
			continue
		}

		lease.Spec.HolderIdentity = nil
		lease.Spec.RenewTime = nil

		err = apiClient.Update(ctx, lease)
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			errs = append(errs, fmt.Errorf("failed to release zone lease %q: %w", lease.Name, err))

			continue
		}

		released++
	}

	return released, errors.Join(errs...)
}

// leaseDurationSeconds is l.LeaseDuration as a Lease's
// spec.leaseDurationSeconds, rounded up: truncating would store a
// sub-second duration as 0, which every contender reads as already
// expired.
func (l *Locker) leaseDurationSeconds() int32 {
	return int32(math.Ceil(l.LeaseDuration.Seconds()))
}

// leaseName maps zoneKey onto the Lease object name this package owns —
// see globalLeaseName's own doc for why GlobalKey isn't just
// leaseNamePrefix alone.
func leaseName(zoneKey string) string {
	if zoneKey == GlobalKey {
		return globalLeaseName
	}

	return leaseNamePrefix + zoneKey
}

// create ensures v1alpha2.KontinuumSystemNamespace exists, then creates a
// fresh Lease held by l.HolderIdentity. An AlreadyExists response means a
// racing contender created it first between this call's own Get and this
// Create — not this call's win, so it returns false rather than retrying
// immediately; the caller's own next reconcile tries again.
func (l *Locker) create(ctx context.Context, name string) (bool, error) {
	err := l.ensureNamespace(ctx)
	if err != nil {
		return false, err
	}

	now := metav1.NowMicro()
	durationSeconds := l.leaseDurationSeconds()

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &l.HolderIdentity,
			LeaseDurationSeconds: &durationSeconds,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}

	err = l.Client.Create(ctx, lease)
	if apierrors.IsAlreadyExists(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to create zone lease %q: %w", name, err)
	}

	return true, nil
}

// renew refreshes lease's own renewTime to keep l.HolderIdentity's
// already-held lock alive. A Conflict means another writer touched it
// between this call's own Get and this Update — treated the same as "not
// this round", not an error; the caller's next reconcile retries against a
// fresh Get.
func (l *Locker) renew(ctx context.Context, lease *coordinationv1.Lease) (bool, error) {
	now := metav1.NowMicro()
	durationSeconds := l.leaseDurationSeconds()

	lease.Spec.RenewTime = &now
	lease.Spec.LeaseDurationSeconds = &durationSeconds

	err := l.Client.Update(ctx, lease)
	if apierrors.IsConflict(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to renew zone lease %q: %w", lease.Name, err)
	}

	return true, nil
}

// takeover claims lease — previously held by someone else whose renewal has
// gone stale past its own LeaseDurationSeconds, see expired — for
// l.HolderIdentity instead. A Conflict means another contender already took
// it over (or the previous holder renewed just in time) between this call's
// own Get and this Update — treated the same as "not this round", not an
// error.
func (l *Locker) takeover(ctx context.Context, lease *coordinationv1.Lease) (bool, error) {
	now := metav1.NowMicro()
	durationSeconds := l.leaseDurationSeconds()

	transitions := int32(1)
	if lease.Spec.LeaseTransitions != nil {
		transitions = *lease.Spec.LeaseTransitions + 1
	}

	lease.Spec.HolderIdentity = &l.HolderIdentity
	lease.Spec.LeaseDurationSeconds = &durationSeconds
	lease.Spec.AcquireTime = &now
	lease.Spec.RenewTime = &now
	lease.Spec.LeaseTransitions = &transitions

	err := l.Client.Update(ctx, lease)
	if apierrors.IsConflict(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to take over zone lease %q: %w", lease.Name, err)
	}

	return true, nil
}

// ensureNamespace creates v1alpha2.KontinuumSystemNamespace if it doesn't
// already exist — mirrors the identical private helper duplicated in
// pkg/domain/registry/heartbeat.go and pkg/domain/zone/workload.go.
// TryAcquire needs its own copy since a Lease can be this process's very
// first write into that namespace.
func (l *Locker) ensureNamespace(ctx context.Context) error {
	err := l.Client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha2.KontinuumSystemNamespace}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q namespace: %w", v1alpha2.KontinuumSystemNamespace, err)
	}

	return nil
}

// holderIdentity dereferences lease.Spec.HolderIdentity, treating nil (a
// Lease somehow created with no holder set) as "held by nobody in
// particular" — never equal to any real HolderIdentity, so it always falls
// through to the expired check below rather than being mistaken for
// self-held.
func holderIdentity(lease *coordinationv1.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}

	return *lease.Spec.HolderIdentity
}

// expired reports whether lease's own holder has gone longer than its
// LeaseDurationSeconds without renewing. A Lease with no RenewTime set
// (shouldn't happen for one this package created, but defensive against one
// created some other way) is treated as already expired, free for anyone to
// take over. fallbackDuration covers the same case for
// LeaseDurationSeconds.
func expired(lease *coordinationv1.Lease, fallbackDuration time.Duration) bool {
	if lease.Spec.RenewTime == nil {
		return true
	}

	duration := fallbackDuration
	if lease.Spec.LeaseDurationSeconds != nil {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}

	return time.Since(lease.Spec.RenewTime.Time) > duration
}

// jitterDivisor is Jitter's own "half" in [base/2, base] — named so mnd
// doesn't flag it as a bare magic number.
const jitterDivisor = 2

// Jitter returns a duration uniformly distributed in [base/2, base] —
// callers use it to stagger a refused TryAcquire's own retry, so every
// non-holder contending for the same Lease doesn't wake up and retry in
// lockstep. Not a security-sensitive use of randomness, just retry timing —
// math/rand/v2 is the right tool, not crypto/rand.
func Jitter(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}

	half := base / jitterDivisor

	//nolint:gosec // retry-timing jitter, not a security-sensitive random value
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
