package etcdproxy

import (
	"context"
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrUnauthenticated is returned by Verifier.Authenticate for any
// credential that doesn't identify a real, currently-valid zone key —
// deliberately the same error regardless of which specific check failed
// (malformed token, unknown zone, expired key, ...), so a caller can't
// distinguish "this zone doesn't exist" from "this zone's key expired" by
// probing the proxy.
var ErrUnauthenticated = errors.New("invalid or expired etcd proxy credential")

// verifierCacheSize bounds Verifier's own LRU cache — one entry per
// distinct zone identity that has ever presented credentials, not per
// request, so this only needs to be as large as the number of zones ever
// joined, not request volume.
const verifierCacheSize = 256

// verifierCacheTTL bounds how long a cache hit is trusted before Verifier
// re-fetches the zone's own auth Secret — long enough to avoid a
// Kubernetes API round trip on every single proxied etcd RPC (a hot path:
// every Range/Put/Watch event goes through this), short enough that a key
// rotation or revocation (e.g. a Zone being deleted, which cascades to
// deleting this Secret via its own owner reference — see
// pkg/domain/zone's reconcileAuthKeys) is picked up within a bounded,
// short window rather than staying trusted for the cache's entire
// lifetime.
const verifierCacheTTL = 30 * time.Second

// cacheEntry is one Verifier cache slot: the zone's own key pair as of
// fetchedAt.
type cacheEntry struct {
	pair      AuthKeyPair
	fetchedAt time.Time
}

// Verifier authenticates a "zone:key" bearer token (see DecodeToken)
// against the presenting zone's own auth Secret (see AuthSecretName),
// cached in a small LRU to avoid a Kubernetes API round trip on every
// proxied etcd RPC.
type Verifier struct {
	client    client.Client
	namespace string
	cache     *lru.Cache[string, cacheEntry]
	now       func() time.Time
}

// NewVerifier builds a Verifier that reads Zone auth Secrets from
// namespace (always v1alpha2.KontinuumSystemNamespace in practice, the one
// namespace Zone — and so its own auth Secret — ever lives in) via
// hubClient.
func NewVerifier(hubClient client.Client, namespace string) (*Verifier, error) {
	cache, err := lru.New[string, cacheEntry](verifierCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to build auth verifier cache: %w", err)
	}

	return &Verifier{client: hubClient, namespace: namespace, cache: cache, now: time.Now}, nil
}

// Authenticate validates token — an EncodeToken-shaped
// "Authorization: Bearer" value — and returns the zone name it identifies.
// Accepts either of the zone's own two currently-valid keys (see
// AuthKey.Valid) — during the overlap window right after a rotation, both
// the just-issued and the just-demoted key work, so an already-running
// zone's own in-flight credential isn't cut off the instant the hub
// rotates.
func (v *Verifier) Authenticate(ctx context.Context, token string) (string, error) {
	zone, key, err := DecodeToken(token)
	if err != nil {
		return "", err
	}

	pair, err := v.lookup(ctx, zone)
	if err != nil {
		return "", err
	}

	now := v.now()

	matches := (key == pair.Current.Value && pair.Current.Valid(now)) ||
		(key == pair.Previous.Value && pair.Previous.Valid(now))
	if !matches {
		return "", ErrUnauthenticated
	}

	return zone, nil
}

// lookup returns zone's own current key pair, from cache when it's both
// present and still within verifierCacheTTL, otherwise fetched fresh from
// the API and cached for next time.
func (v *Verifier) lookup(ctx context.Context, zone string) (AuthKeyPair, error) {
	if entry, ok := v.cache.Get(zone); ok && v.now().Sub(entry.fetchedAt) < verifierCacheTTL {
		return entry.pair, nil
	}

	var secret corev1.Secret

	err := v.client.Get(ctx, client.ObjectKey{Name: AuthSecretName(zone), Namespace: v.namespace}, &secret)
	if err != nil {
		return AuthKeyPair{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}

	pair, ok := ParseAuthSecret(&secret)
	if !ok {
		return AuthKeyPair{}, ErrUnauthenticated
	}

	v.cache.Add(zone, cacheEntry{pair: pair, fetchedAt: v.now()})

	return pair, nil
}
