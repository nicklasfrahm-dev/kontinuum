package etcdproxy

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrUnauthenticated is returned by Verifier.Authenticate for any
// credential that doesn't identify a real, currently-trusted zone —
// deliberately the same error regardless of which specific check failed
// (malformed JWT, unknown zone, bad signature, expired token, ...), so a
// caller can't distinguish "this zone doesn't exist" from "this zone's
// token expired" by probing the proxy.
var ErrUnauthenticated = errors.New("invalid or expired etcd proxy credential")

// errIdentitySecretInvalid is lookup's own error for a zone's identity
// Secret existing but not parsing as a valid IdentityPair.
var errIdentitySecretInvalid = errors.New("identity secret is not valid")

// errNoValidIdentityKeys is lookup's own error for a zone whose identity
// Secret parses fine but has no currently-valid (see Identity.Valid) keys
// left in it at all — e.g. a Zone left unreconciled long enough that even
// its own Current identity expired.
var errNoValidIdentityKeys = errors.New("no currently-valid identity keys")

// verifierCacheSize bounds Verifier's own LRU cache — one entry per
// distinct zone identity that has ever presented credentials, not per
// request, so this only needs to be as large as the number of zones ever
// joined, not request volume.
const verifierCacheSize = 256

// verifierCacheTTL bounds how long a cache hit is trusted before Verifier
// re-fetches the zone's own identity Secret — long enough to avoid a
// Kubernetes API round trip on every single proxied etcd RPC (a hot path:
// every Range/Put/Watch event goes through this), short enough that a
// Zone being deleted (which cascades to deleting this Secret via its own
// owner reference) or an identity rotation (see pkg/domain/zone's own
// ensureEtcdIdentity) is picked up within a bounded, short window rather
// than staying trusted for the cache's entire lifetime.
const verifierCacheTTL = 30 * time.Second

// cacheEntry is one Verifier cache slot: zone's own currently-acceptable
// public keys as of fetchedAt (see lookup — one or two, depending on
// whether a Previous identity is still within its own overlap window).
type cacheEntry struct {
	keys      []ed25519.PublicKey
	fetchedAt time.Time
}

// Verifier authenticates a zone-signed JWT (see VerifyToken) against the
// presenting zone's own identity Secret (see AuthSecretName), cached in a
// small LRU to avoid a Kubernetes API round trip on every proxied etcd
// RPC.
type Verifier struct {
	client    client.Client
	namespace string
	cache     *lru.Cache[string, cacheEntry]
	now       func() time.Time
}

// NewVerifier builds a Verifier that reads Zone identity Secrets from
// namespace (always v1alpha2.KontinuumSystemNamespace in practice, the one
// namespace Zone — and so its own identity Secret — ever lives in) via
// hubClient.
func NewVerifier(hubClient client.Client, namespace string) (*Verifier, error) {
	cache, err := lru.New[string, cacheEntry](verifierCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to build auth verifier cache: %w", err)
	}

	return &Verifier{client: hubClient, namespace: namespace, cache: cache, now: time.Now}, nil
}

// Authenticate validates token — a SignToken-shaped
// "Authorization: Bearer" JWT — and returns the zone name it identifies.
func (v *Verifier) Authenticate(ctx context.Context, token string) (string, error) {
	zone, err := VerifyToken(token, func(zone string) ([]ed25519.PublicKey, error) {
		return v.lookup(ctx, zone)
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}

	return zone, nil
}

// lookup returns zone's own currently-acceptable public keys — Current
// always first, Previous appended only while it's still within its own
// overlap window (see Identity.Valid) — from cache when present and still
// within verifierCacheTTL, otherwise fetched fresh from the API and cached
// for next time.
func (v *Verifier) lookup(ctx context.Context, zone string) ([]ed25519.PublicKey, error) {
	if entry, ok := v.cache.Get(zone); ok && v.now().Sub(entry.fetchedAt) < verifierCacheTTL {
		return entry.keys, nil
	}

	var secret corev1.Secret

	err := v.client.Get(ctx, client.ObjectKey{Name: AuthSecretName(zone), Namespace: v.namespace}, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to get identity secret for zone %q: %w", zone, err)
	}

	pair, ok := ParsePublicSecret(&secret)
	if !ok {
		return nil, fmt.Errorf("%w: zone %q", errIdentitySecretInvalid, zone)
	}

	now := v.now()

	var keys []ed25519.PublicKey

	if pair.Current.Valid(now) {
		pub, pubErr := PublicKeyFromCert(pair.Current.CertPEM)
		if pubErr == nil {
			keys = append(keys, pub)
		}
	}

	if pair.Previous.Valid(now) {
		pub, pubErr := PublicKeyFromCert(pair.Previous.CertPEM)
		if pubErr == nil {
			keys = append(keys, pub)
		}
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: zone %q", errNoValidIdentityKeys, zone)
	}

	v.cache.Add(zone, cacheEntry{keys: keys, fetchedAt: now})

	return keys, nil
}
