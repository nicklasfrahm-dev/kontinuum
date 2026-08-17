package zone

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

// authKeyCheckInterval bounds how long reconcileAuthKeys ever leaves a
// Zone unchecked, even when its current key isn't due for rotation for a
// long while yet — see reconcileAuthKeys' own doc. The key/rotation shape
// itself (length, charset, RotationInterval, OverlapWindow, Secret field
// names) lives in pkg/domain/etcdproxy, shared with the hub-side Verifier
// that has to agree on the exact same contract to accept these credentials.
const authKeyCheckInterval = 5 * time.Minute

// reconcileAuthKeys ensures zoneObj has a current, always-valid etcd gRPC
// proxy bearer credential, and a still-valid previous one during
// etcdproxy.OverlapWindow right after a rotation — see
// pkg/domain/etcdproxy's own doc for how these two keys back the
// "Authorization: Bearer base64(zone:key)" credential a zone's own
// kontinuum-server presents. Both live in one Secret (see
// etcdproxy.AuthSecretName), owned by zoneObj so it's garbage collected the
// moment the Zone itself is deleted, never lingering as an orphaned
// credential.
//
// Exactly two keys exist at all times, never more: on rotation, the current
// key is demoted into the previous slot (keeping its own original
// CreatedAt, so its own ExpiresAt stays fixed regardless of when rotation
// actually happened to notice it was due) and a fresh key takes over as
// current.
//
// Returns how long until this needs checking again: the earlier of "the
// current key is due for rotation" and authKeyCheckInterval, so a
// controller restart or a missed tick can't leave rotation meaningfully
// late, and callers don't need a separate timer of their own.
func (r *Reconciler) reconcileAuthKeys(ctx context.Context, zoneObj *v1alpha2.Zone) (time.Duration, error) {
	var secret corev1.Secret

	key := client.ObjectKey{Name: etcdproxy.AuthSecretName(zoneObj.Name), Namespace: zoneObj.Namespace}

	err := r.Client.Get(ctx, key, &secret)
	if err != nil && !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("failed to get auth secret for zone %q: %w", zoneObj.Name, err)
	}

	exists := err == nil

	existing, parsedOK := etcdproxy.AuthKeyPair{}, false
	if exists {
		existing, parsedOK = etcdproxy.ParseAuthSecret(&secret)
	}

	pair, write, notDueFor, err := desiredAuthKeyPair(existing, parsedOK, time.Now())
	if err != nil {
		return 0, err
	}

	if !write {
		// Current key isn't due yet — nothing to persist, just report back
		// when to check again.
		return minDuration(notDueFor, authKeyCheckInterval), nil
	}

	err = r.persistAuthSecret(ctx, zoneObj, pair, secret.ResourceVersion, exists)
	if err != nil {
		return 0, err
	}

	return minDuration(etcdproxy.RotationInterval, authKeyCheckInterval), nil
}

// desiredAuthKeyPair computes the key pair reconcileAuthKeys should
// persist for a zone whose existing auth Secret parsed to existing —
// parsedOK false means it didn't (missing or malformed, treated
// identically — see reconcileAuthKeys' own doc). write is false only when
// existing's own current key isn't due for rotation yet, in which case
// notDueFor reports how long until it will be and pair is existing,
// unchanged.
func desiredAuthKeyPair(
	existing etcdproxy.AuthKeyPair, parsedOK bool, now time.Time,
) (etcdproxy.AuthKeyPair, bool, time.Duration, error) {
	if parsedOK && now.Before(existing.Current.DueAt()) {
		return existing, false, time.Until(existing.Current.DueAt()), nil
	}

	fresh, err := etcdproxy.GenerateAuthKey()
	if err != nil {
		return etcdproxy.AuthKeyPair{}, false, 0, fmt.Errorf("failed to generate auth key: %w", err)
	}

	if !parsedOK {
		// Secret missing or unparseable — issue a fresh pair, both valid
		// from now. There is no real "previous" key yet, so both start
		// out identical in freshness; the first real rotation (in
		// etcdproxy.RotationInterval) demotes Current into Previous
		// normally.
		return etcdproxy.AuthKeyPair{
			Current:  etcdproxy.AuthKey{Value: fresh, CreatedAt: now},
			Previous: etcdproxy.AuthKey{Value: fresh, CreatedAt: now},
		}, true, 0, nil
	}

	return etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: fresh, CreatedAt: now},
		Previous: existing.Current,
	}, true, 0, nil
}

// persistAuthSecret writes pair as zoneObj's own auth Secret, owned by
// zoneObj. resourceVersion/exists come from reconcileAuthKeys' own earlier
// Get, so the Update path (when exists) round-trips the same object
// instead of racing a blind write against any concurrent change.
func (r *Reconciler) persistAuthSecret(
	ctx context.Context, zoneObj *v1alpha2.Zone, pair etcdproxy.AuthKeyPair, resourceVersion string, exists bool,
) error {
	target := etcdproxy.BuildAuthSecret(zoneObj.Name, zoneObj.Namespace, pair)

	err := controllerutil.SetControllerReference(zoneObj, target, r.Client.Scheme())
	if err != nil {
		return fmt.Errorf("failed to set owner reference on %q secret: %w", target.Name, err)
	}

	if exists {
		target.ResourceVersion = resourceVersion
		err = r.Client.Update(ctx, target)
	} else {
		err = r.Client.Create(ctx, target)
	}

	if err != nil {
		return fmt.Errorf("failed to persist auth secret for zone %q: %w", zoneObj.Name, err)
	}

	return nil
}

// errGRPCEndpointNotConfigured is zoneStorageDSN's sentinel for a hub with
// no KONTINUUM_SERVER_GRPC_ENDPOINT of its own configured — a static
// sentinel for the same err113 reason as inference.go's own
// errNoRegisteredKontinuum.
var errGRPCEndpointNotConfigured = errors.New("this hub has no KONTINUUM_SERVER_GRPC_ENDPOINT configured")

// errAuthSecretNotReady is zoneStorageDSN's own sentinel for the (never
// expected in practice — reconcileAuthKeys always runs first in the same
// Reconcile pass, see that function's own doc) case where zoneObj's own
// auth Secret exists but doesn't parse — a static sentinel for the same
// err113 reason as errGRPCEndpointNotConfigured above.
var errAuthSecretNotReady = errors.New("auth secret for zone is not ready yet")

// zoneStorageDSN builds the KONTINUUM_SERVER_STORAGE value zoneObj's own
// kontinuum-server should run with: an etcdproxy.BuildRelayDSN pointing at
// this hub's own etcd gRPC proxy (r.GRPCEndpoint — see
// KontinuumGRPCConfigStatus's own doc for why this is read off the hub's
// own config rather than inferred from a registered Kontinuum the way
// storage/domain used to be), carrying zoneObj's own current auth key (see
// reconcileAuthKeys, which always runs earlier in the same Reconcile pass
// — see Reconcile's own doc — so that key already exists by the time this
// is called).
//
// This is what actually closes the loop this package exists for: instead
// of every joined zone getting a copy of the hub's own raw database
// connection string (which only the control plane may be able to reach at
// all — see pkg/domain/etcdproxy's own doc), a zone's own kontinuum-server
// only ever talks to the hub's own already-public endpoint, authenticated
// with a credential scoped to that one zone.
func (r *Reconciler) zoneStorageDSN(ctx context.Context, zoneObj *v1alpha2.Zone) (string, error) {
	if r.GRPCEndpoint == "" {
		return "", errGRPCEndpointNotConfigured
	}

	var secret corev1.Secret

	key := client.ObjectKey{Name: etcdproxy.AuthSecretName(zoneObj.Name), Namespace: zoneObj.Namespace}

	err := r.Client.Get(ctx, key, &secret)
	if err != nil {
		return "", fmt.Errorf("failed to get auth secret for zone %q: %w", zoneObj.Name, err)
	}

	pair, ok := etcdproxy.ParseAuthSecret(&secret)
	if !ok {
		return "", fmt.Errorf("%w: %q", errAuthSecretNotReady, zoneObj.Name)
	}

	return etcdproxy.BuildRelayDSN(zoneObj.Name, pair.Current.Value, r.GRPCEndpoint), nil
}

// minDuration returns whichever of a/b is smaller.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
