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
	"fmt"
	"math/rand/v2"
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
// ClusterRoleBindings). A worker (a Locker with a non-empty SelfZoneKey) is
// refused this key exactly like its own zone's key — that state isn't its
// own to reconcile either.
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
}

// Locker gates zone-scoped reconciliation across every kontinuum-server
// process sharing one central storage — see this package's own doc.
// Callers build one per controller (via NewLocker, in SetupWithManager) and
// call TryAcquire at the top of every Reconcile/tick that's about to write
// anything, skipping the write entirely when it returns false.
type Locker struct {
	// Client talks to this process's own apiserver — always mgr.GetClient()
	// in production, a fake client in tests.
	Client client.Client
	// HolderIdentity names this process as a Lease's spec.holderIdentity —
	// shared with registry.Heartbeat.Name (see pkg/cli/serve.go) so a
	// process's lease identity and registry identity match.
	HolderIdentity string
	// SelfZoneKey is this process's own zone key
	// (Key(cfg.Server.Region, cfg.Server.Zone)) — empty for the hub. A
	// non-empty SelfZoneKey is what makes TryAcquire refuse both this exact
	// key and GlobalKey outright, with no API call: a zone must never
	// reconcile its own resources, nor anything hub-owned.
	SelfZoneKey string
	// LeaseDuration is how long a Lease may go without renewal before
	// another contender may take it over. Defaults to defaultLeaseDuration
	// when zero — see NewLocker.
	LeaseDuration time.Duration
}

// NewLocker builds a Locker, defaulting LeaseDuration when zero.
func NewLocker(apiClient client.Client, holderIdentity, selfZoneKey string, leaseDuration time.Duration) *Locker {
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}

	return &Locker{
		Client:         apiClient,
		HolderIdentity: holderIdentity,
		SelfZoneKey:    selfZoneKey,
		LeaseDuration:  leaseDuration,
	}
}

// TryAcquire reports whether the caller may perform mutating reconciliation
// for zoneKey (GlobalKey for hub-owned/fleet-wide state) right now — see
// Locker's own doc. A false return with a nil error means "someone else
// holds this right now, or you're structurally not allowed to" — not a
// failure; callers should quietly back off and retry later (see Jitter),
// not log it as an error.
func (l *Locker) TryAcquire(ctx context.Context, zoneKey string) (bool, error) {
	if l.SelfZoneKey != "" && (zoneKey == GlobalKey || zoneKey == l.SelfZoneKey) {
		return false, nil
	}

	name := leaseName(zoneKey)

	var lease coordinationv1.Lease

	err := l.Client.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}, &lease)
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
	durationSeconds := int32(l.LeaseDuration.Seconds())

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
	durationSeconds := int32(l.LeaseDuration.Seconds())

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
	durationSeconds := int32(l.LeaseDuration.Seconds())

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
