package zone

import (
	"bytes"
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

// identityCheckInterval bounds how long reconcileIdentityRotationSchedule
// ever leaves a Zone unchecked, even when its current identity isn't due
// for rotation for a long while yet — see that function's own doc.
const identityCheckInterval = 30 * time.Minute

// reconcileIdentityRotationSchedule reports how long until zoneObj's own
// etcd proxy identity is next due for rotation (see
// etcdproxy.IdentityRotationInterval), purely from the hub's own
// already-issued public copy — cheap and hub-only, so it's safe to call
// unconditionally on every Reconcile (see Reconcile's own doc) and fold
// its returned duration into the overall result (see earliestRequeue),
// keeping Reconcile ticking even once a Zone is otherwise fully Ready and
// nothing else would requeue it. The actual rotation happens later, inside
// ensureEtcdIdentity, once a downstream client is available — this only
// ever reads, never writes.
func (r *Reconciler) reconcileIdentityRotationSchedule(
	ctx context.Context, zoneObj *v1alpha2.Zone,
) (time.Duration, error) {
	var hubSecret corev1.Secret

	key := client.ObjectKey{Name: etcdproxy.AuthSecretName(zoneObj.Name), Namespace: zoneObj.Namespace}

	err := r.Client.Get(ctx, key, &hubSecret)
	if apierrors.IsNotFound(err) {
		// Not issued yet — ensureEtcdIdentity issues it once install
		// reaches that point; nothing to schedule until then.
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get identity secret for zone %q: %w", zoneObj.Name, err)
	}

	pair, ok := etcdproxy.ParsePublicSecret(&hubSecret)
	if !ok {
		return identityCheckInterval, nil
	}

	dueIn := time.Until(pair.Current.DueAt())
	if dueIn <= 0 {
		return identityCheckInterval, nil
	}

	return minDuration(dueIn, identityCheckInterval), nil
}

// ensureEtcdIdentity ensures zoneObj has a working, up-to-date etcd gRPC
// proxy identity: an ed25519 keypair (see etcdproxy.GenerateIdentity),
// rotated every etcdproxy.IdentityRotationInterval. Its private half is
// delivered to downstream (this zone's own cluster) as a
// kubernetes.io/tls Secret (see etcdproxy.BuildDownstreamIdentitySecret);
// its public half is kept on the hub as a Current/Previous pair (see
// etcdproxy.BuildPublicSecret), owned by zoneObj so it's garbage collected
// the moment the Zone itself is deleted.
//
// downstream's own identity Secret is the source of truth for which
// private key the zone's own kontinuum-server was actually handed, not the
// hub's own copy: if it's missing (first join, or the downstream
// namespace/Secret was wiped some other way) this issues a fresh keypair;
// if it exists but doesn't match the hub's own idea of "current" (an
// earlier rotation's downstream write landed but its matching hub write
// didn't), this resyncs the hub to match without re-rotating; only once
// both sides agree does it check whether rotation is actually due.
//
// Returns rotated=true only when this pass actually minted and delivered
// a brand-new keypair — the caller (see reconcileInstall) still needs to
// force a rolling restart of the zone's own Deployment so its already-
// running pod (which only ever reads its mounted private key once, at
// startup) picks the new one up, but only *after* installWorkload's own
// ensureDeployment call, which would otherwise overwrite the restart
// annotation this bumps.
func (r *Reconciler) ensureEtcdIdentity(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone,
) (bool, error) {
	var downstreamSecret corev1.Secret

	key := client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: downstreamNamespace}

	err := downstream.Get(ctx, key, &downstreamSecret)

	switch {
	case apierrors.IsNotFound(err):
		return false, r.issueInitialEtcdIdentity(ctx, downstream, zoneObj)

	case err != nil:
		return false, fmt.Errorf("failed to get downstream identity secret for zone %q: %w", zoneObj.Name, err)

	default:
		return r.reconcileExistingEtcdIdentity(ctx, downstream, zoneObj, downstreamSecret)
	}
}

// issueInitialEtcdIdentity mints zoneObj's very first identity — Current
// and Previous start out identical, since there's no real "previous" one
// yet (mirrors the bearer-token scheme this replaced). No restart
// annotation is needed here: installWorkload's own ensureDeployment call,
// later in the same reconcileInstall pass, creates the Deployment fresh
// with this Secret already mounted, rather than restarting an
// already-running pod.
func (r *Reconciler) issueInitialEtcdIdentity(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone,
) error {
	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(zoneObj.Name)
	if err != nil {
		return fmt.Errorf("failed to generate identity for zone %q: %w", zoneObj.Name, err)
	}

	err = r.persistDownstreamIdentity(ctx, downstream, certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("failed to persist downstream identity secret for zone %q: %w", zoneObj.Name, err)
	}

	identity := etcdproxy.Identity{CertPEM: certPEM, IssuedAt: time.Now()}

	return r.persistHubPublicSecret(ctx, zoneObj, etcdproxy.IdentityPair{Current: identity, Previous: identity})
}

// errDownstreamIdentityMissingCert is reconcileExistingEtcdIdentity's own
// sentinel for a downstream identity Secret that exists but carries no
// certificate — never expected in practice (only this package's own
// persistDownstreamIdentity ever writes this Secret), but guards against a
// hand-edited or partially-written one.
var errDownstreamIdentityMissingCert = errors.New("downstream identity secret has no certificate")

// reconcileExistingEtcdIdentity handles every case where downstream
// already has an identity Secret — see ensureEtcdIdentity's own doc for
// the three branches (resync-from-downstream, steady-state no-op, actual
// rotation).
func (r *Reconciler) reconcileExistingEtcdIdentity(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone, downstreamSecret corev1.Secret,
) (bool, error) {
	downstreamCert, ok := downstreamSecret.Data[corev1.TLSCertKey]
	if !ok || len(downstreamCert) == 0 {
		return false, fmt.Errorf("%w: zone %q", errDownstreamIdentityMissingCert, zoneObj.Name)
	}

	var hubSecret corev1.Secret

	hubKey := client.ObjectKey{Name: etcdproxy.AuthSecretName(zoneObj.Name), Namespace: zoneObj.Namespace}

	err := r.Client.Get(ctx, hubKey, &hubSecret)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("failed to get hub identity secret for zone %q: %w", zoneObj.Name, err)
	}

	pair, parsedOK := etcdproxy.IdentityPair{}, false
	if err == nil {
		pair, parsedOK = etcdproxy.ParsePublicSecret(&hubSecret)
	}

	if !parsedOK || !bytes.Equal(pair.Current.CertPEM, downstreamCert) {
		// Either the hub never recorded a public copy at all, or
		// downstream already moved on to a cert the hub doesn't know
		// about yet (an earlier rotation's downstream write landed but
		// its matching hub write didn't) — resync without re-rotating:
		// downstream already holds the right key, nothing to deliver.
		return false, r.resyncHubPublicSecret(ctx, zoneObj, pair, parsedOK, downstreamCert)
	}

	if time.Now().Before(pair.Current.DueAt()) {
		return false, nil
	}

	return true, r.rotateEtcdIdentity(ctx, downstream, zoneObj, pair.Current)
}

// resyncHubPublicSecret rewrites the hub's own public copy to agree with
// downstreamCert — the certificate downstream's own identity Secret
// already carries — without generating anything new. previous carries
// forward whatever the hub's own (possibly malformed or stale) prior
// Current was, best-effort, so a genuinely-still-valid previous identity
// isn't dropped by a resync triggered by something else going wrong.
func (r *Reconciler) resyncHubPublicSecret(
	ctx context.Context, zoneObj *v1alpha2.Zone, existing etcdproxy.IdentityPair, existingOK bool, downstreamCert []byte,
) error {
	current := etcdproxy.Identity{CertPEM: downstreamCert, IssuedAt: time.Now()}

	previous := current
	if existingOK && len(existing.Current.CertPEM) > 0 {
		previous = existing.Current
	}

	return r.persistHubPublicSecret(ctx, zoneObj, etcdproxy.IdentityPair{Current: current, Previous: previous})
}

// rotateEtcdIdentity mints a fresh keypair, delivers its private half to
// downstream, and demotes oldCurrent into the hub's own Previous slot —
// see ensureEtcdIdentity's own doc for why the caller, not this function,
// is responsible for the resulting rolling restart.
func (r *Reconciler) rotateEtcdIdentity(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone, oldCurrent etcdproxy.Identity,
) error {
	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(zoneObj.Name)
	if err != nil {
		return fmt.Errorf("failed to generate identity for zone %q: %w", zoneObj.Name, err)
	}

	err = r.persistDownstreamIdentity(ctx, downstream, certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("failed to persist downstream identity secret for zone %q: %w", zoneObj.Name, err)
	}

	pair := etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: certPEM, IssuedAt: time.Now()},
		Previous: oldCurrent,
	}

	return r.persistHubPublicSecret(ctx, zoneObj, pair)
}

// persistDownstreamIdentity upserts the kubernetes.io/tls Secret
// downstream's own kontinuum-server mounts its identity from — mirrors
// workload.go's own ensureSecret create-then-get-and-update-on-conflict
// idiom, so it works whether downstream never had one (first issuance) or
// already does (rotation).
func (r *Reconciler) persistDownstreamIdentity(
	ctx context.Context, downstream client.Client, certPEM, keyPEM []byte,
) error {
	target := etcdproxy.BuildDownstreamIdentitySecret(downstreamNamespace, certPEM, keyPEM)

	err := downstream.Create(ctx, target)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Secret

		key := client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: downstreamNamespace}

		err = downstream.Get(ctx, key, &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q secret: %w", etcdproxy.IdentitySecretName, err)
		}

		// Rebuilt fresh from certPEM/keyPEM rather than reused from
		// target.StringData: a failed Create is never expected to mutate
		// its own input on a real cluster, but this doesn't lean on that
		// assumption either way.
		existing.StringData = map[string]string{
			corev1.TLSCertKey:       string(certPEM),
			corev1.TLSPrivateKeyKey: string(keyPEM),
		}

		err = downstream.Update(ctx, &existing)
	}

	if err != nil {
		return fmt.Errorf("failed to persist %q secret: %w", etcdproxy.IdentitySecretName, err)
	}

	return nil
}

// persistHubPublicSecret upserts the hub's own trimmed, public-key-only
// copy of zoneObj's identity pair.
func (r *Reconciler) persistHubPublicSecret(
	ctx context.Context, zoneObj *v1alpha2.Zone, pair etcdproxy.IdentityPair,
) error {
	target := etcdproxy.BuildPublicSecret(zoneObj.Name, zoneObj.Namespace, pair)

	err := controllerutil.SetControllerReference(zoneObj, target, r.Client.Scheme())
	if err != nil {
		return fmt.Errorf("failed to set owner reference on %q secret: %w", target.Name, err)
	}

	var existing corev1.Secret

	key := client.ObjectKey{Name: target.Name, Namespace: target.Namespace}

	err = r.Client.Get(ctx, key, &existing)

	switch {
	case apierrors.IsNotFound(err):
		err = r.Client.Create(ctx, target)
	case err != nil:
		return fmt.Errorf("failed to get identity secret for zone %q: %w", zoneObj.Name, err)
	default:
		target.ResourceVersion = existing.ResourceVersion
		err = r.Client.Update(ctx, target)
	}

	if err != nil {
		return fmt.Errorf("failed to persist identity secret for zone %q: %w", zoneObj.Name, err)
	}

	return nil
}

// errGRPCEndpointNotConfigured is zoneStorageDSN's sentinel for a hub with
// no KONTINUUM_SERVER_GRPC_ENDPOINT of its own configured — a static
// sentinel for the same err113 reason as inference.go's own
// errNoRegisteredKontinuum.
var errGRPCEndpointNotConfigured = errors.New("this hub has no KONTINUUM_SERVER_GRPC_ENDPOINT configured")

// zoneStorageDSN builds the KONTINUUM_SERVER_STORAGE value zoneObj's own
// kontinuum-server should run with: an etcdproxy.BuildRelayDSN pointing at
// this hub's own etcd gRPC proxy (r.HubConfig.Server.GRPC.Endpoint — see
// KontinuumGRPCConfigStatus's own doc for why this is read off the hub's
// own config rather than inferred from a registered Kontinuum the way
// storage/domain used to be). Carries no credential of its own — see
// ensureEtcdIdentity, which must have already run in the same Reconcile
// pass to deliver zoneObj's own identity Secret before this DSN is any use
// to it — so, unlike its own former self, this needs no Kubernetes API
// call at all.
//
// This is what actually closes the loop this package exists for: instead
// of every joined zone getting a copy of the hub's own raw database
// connection string (which only the control plane may be able to reach at
// all — see pkg/domain/etcdproxy's own doc), a zone's own kontinuum-server
// only ever talks to the hub's own already-public endpoint, authenticated
// with a credential scoped to that one zone.
func (r *Reconciler) zoneStorageDSN(zoneObj *v1alpha2.Zone) (string, error) {
	endpoint := r.HubConfig.Server.GRPC.Endpoint
	if endpoint == "" {
		return "", errGRPCEndpointNotConfigured
	}

	return etcdproxy.BuildRelayDSN(zoneObj.Name, endpoint), nil
}

// minDuration returns whichever of a/b is smaller.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
