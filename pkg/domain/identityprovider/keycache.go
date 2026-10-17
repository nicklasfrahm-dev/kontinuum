package identityprovider

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"golang.org/x/sync/singleflight"
)

const (
	// defaultKeysFreshFor is how long fetched signing keys are served
	// without revalidating against the issuer.
	defaultKeysFreshFor = 5 * time.Minute
	// defaultKeysStaleFor is how long past defaultKeysFreshFor cached keys
	// are still served — while a background refresh runs — so an issuer
	// outage doesn't immediately lock every user out.
	defaultKeysStaleFor = 24 * time.Hour
	// defaultMinRefreshInterval bounds how often a token carrying an
	// unknown key ID can force a refetch, so a flood of forged tokens
	// can't turn into a flood of requests against the issuer.
	defaultMinRefreshInterval = 10 * time.Second
	// defaultFetchTimeout bounds a single discovery-plus-JWKS fetch.
	defaultFetchTimeout = 10 * time.Second
	// maxResponseBytes bounds how much of a discovery document or JWKS
	// response is read.
	maxResponseBytes = 1 << 20
	// wellKnownPath is appended to an issuer URL to find its discovery
	// document.
	wellKnownPath = "/.well-known/openid-configuration"
	// keyUseSignature is the JWK "use" value for signing keys.
	keyUseSignature = "sig"
)

// KeySource identifies where an issuer's signing keys are fetched from. Two
// identity providers sharing the same source share one cache entry.
type KeySource struct {
	// IssuerURL is the issuer's URL, which its discovery document's
	// "issuer" field must equal.
	IssuerURL string
	// DiscoveryURL overrides where the discovery document is fetched from.
	// Empty means IssuerURL + "/.well-known/openid-configuration".
	DiscoveryURL string
	// CertificateAuthority is a PEM CA bundle used for TLS verification.
	// Empty means the host's own roots.
	CertificateAuthority string
}

// cacheKey derives a fixed-size map key from every field of s.
func (s KeySource) cacheKey() string {
	sum := sha256.Sum256([]byte(s.IssuerURL + "\x00" + s.DiscoveryURL + "\x00" + s.CertificateAuthority))

	return hex.EncodeToString(sum[:])
}

// discoveryURL returns where s's discovery document lives.
func (s KeySource) discoveryURL() string {
	if s.DiscoveryURL != "" {
		return s.DiscoveryURL
	}

	return strings.TrimSuffix(s.IssuerURL, "/") + wellKnownPath
}

// KeySnapshot is one successful fetch of an issuer's signing keys.
type KeySnapshot struct {
	// Keys are the issuer's public signing keys.
	Keys []jose.JSONWebKey
	// FetchedAt is when Keys were fetched.
	FetchedAt time.Time
}

// KeyCacheConfig configures a KeyCache. Zero values select defaults.
type KeyCacheConfig struct {
	// FreshFor is how long fetched keys are served without revalidating.
	FreshFor time.Duration
	// StaleFor is how long past FreshFor keys are still served while a
	// background refresh runs.
	StaleFor time.Duration
	// MinRefreshInterval bounds how often an unknown key ID can force a
	// refetch.
	MinRefreshInterval time.Duration
	// FetchTimeout bounds a single discovery-plus-JWKS fetch.
	FetchTimeout time.Duration
	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time
	// HTTPClient builds the client used to reach a source. Defaults to a
	// client trusting the source's own CertificateAuthority, or the
	// host's roots when empty.
	HTTPClient func(source KeySource) (*http.Client, error)
	// Logger receives background refresh failures.
	Logger *slog.Logger
}

// KeyCache caches issuers' signing keys with stale-while-revalidate
// semantics. Concurrent fetches of the same source are collapsed into one
// via singleflight, both for callers blocking on a missing or expired
// entry and for background revalidation of a stale one.
type KeyCache struct {
	config KeyCacheConfig
	group  singleflight.Group

	mu      sync.RWMutex
	entries map[string]*keyEntry
}

// keyEntry is one source's cached state.
type keyEntry struct {
	snapshot    KeySnapshot
	hasSnapshot bool
	lastAttempt time.Time
	lastErr     error
}

// NewKeyCache builds a KeyCache from config, filling in defaults.
func NewKeyCache(config KeyCacheConfig) *KeyCache {
	if config.FreshFor <= 0 {
		config.FreshFor = defaultKeysFreshFor
	}

	if config.StaleFor <= 0 {
		config.StaleFor = defaultKeysStaleFor
	}

	if config.MinRefreshInterval <= 0 {
		config.MinRefreshInterval = defaultMinRefreshInterval
	}

	if config.FetchTimeout <= 0 {
		config.FetchTimeout = defaultFetchTimeout
	}

	if config.Now == nil {
		config.Now = time.Now
	}

	if config.HTTPClient == nil {
		config.HTTPClient = DefaultHTTPClient
	}

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &KeyCache{config: config, entries: map[string]*keyEntry{}}
}

// FreshFor returns how long fetched keys are served without revalidating.
func (c *KeyCache) FreshFor() time.Duration {
	return c.config.FreshFor
}

// Keys returns source's signing keys. A fresh entry is returned as-is. A
// stale entry is returned immediately while a deduplicated background
// refresh revalidates it. A missing or fully expired entry blocks on a
// deduplicated fetch. While an issuer is failing, neither path refetches
// more often than MinRefreshInterval: a failed blocking fetch's error is
// returned as-is until then, so an outage doesn't turn every request into
// another request against the issuer.
func (c *KeyCache) Keys(ctx context.Context, source KeySource) ([]jose.JSONWebKey, error) {
	snapshot, ok := c.Snapshot(source)
	if ok {
		age := c.config.Now().Sub(snapshot.FetchedAt)

		if age < c.config.FreshFor {
			return snapshot.Keys, nil
		}

		if age < c.config.FreshFor+c.config.StaleFor {
			if c.refreshDue(source) {
				c.revalidate(ctx, source)
			}

			return snapshot.Keys, nil
		}
	}

	if !c.refreshDue(source) {
		lastErr := c.LastError(source)
		if lastErr != nil {
			return nil, fmt.Errorf("failed to fetch signing keys recently: %w", lastErr)
		}
	}

	snapshot, err := c.Refresh(ctx, source)
	if err != nil {
		return nil, err
	}

	return snapshot.Keys, nil
}

// Refresh fetches source's signing keys now, sharing one in-flight fetch
// with every concurrent caller for the same source. The fetch itself is
// detached from ctx's cancellation — another caller may be waiting on the
// same result — but still bounded by FetchTimeout. A failed fetch keeps any
// previously cached keys, so they continue to be served while stale.
func (c *KeyCache) Refresh(ctx context.Context, source KeySource) (KeySnapshot, error) {
	resultChan := c.group.DoChan(source.cacheKey(), func() (any, error) {
		return c.fetchAndStore(context.WithoutCancel(ctx), source)
	})

	select {
	case <-ctx.Done():
		return KeySnapshot{}, fmt.Errorf("failed to wait for signing keys: %w", ctx.Err())
	case result := <-resultChan:
		if result.Err != nil {
			return KeySnapshot{}, result.Err
		}

		snapshot, _ := result.Val.(KeySnapshot)

		return snapshot, nil
	}
}

// RefreshIfDue refreshes source unless it was already attempted within
// MinRefreshInterval, returning the resulting snapshot and whether a
// refresh actually ran. Used when a token names a key ID the cache doesn't
// know, which usually means the issuer has rotated its keys.
func (c *KeyCache) RefreshIfDue(ctx context.Context, source KeySource) (KeySnapshot, bool, error) {
	if !c.refreshDue(source) {
		return KeySnapshot{}, false, nil
	}

	snapshot, err := c.Refresh(ctx, source)
	if err != nil {
		return KeySnapshot{}, true, err
	}

	return snapshot, true, nil
}

// Snapshot returns source's most recent successful fetch, if any.
func (c *KeyCache) Snapshot(source KeySource) (KeySnapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[source.cacheKey()]
	if !ok || !entry.hasSnapshot {
		return KeySnapshot{}, false
	}

	return entry.snapshot, true
}

// LastError returns the error from source's most recent fetch attempt, or
// nil if it succeeded or was never attempted.
func (c *KeyCache) LastError(source KeySource) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[source.cacheKey()]
	if !ok {
		return nil
	}

	return entry.lastErr
}

// Retain drops every cached entry whose source isn't listed, so keys for a
// deleted identity provider don't linger.
func (c *KeyCache) Retain(sources []KeySource) {
	keep := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		keep[source.cacheKey()] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		_, ok := keep[key]
		if !ok {
			delete(c.entries, key)
		}
	}
}

// refreshDue reports whether source was last attempted at least
// MinRefreshInterval ago, or never.
func (c *KeyCache) refreshDue(source KeySource) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[source.cacheKey()]

	return !ok || c.config.Now().Sub(entry.lastAttempt) >= c.config.MinRefreshInterval
}

// revalidate starts a deduplicated background refresh of source, detached
// from ctx's cancellation so it outlives the request that triggered it.
func (c *KeyCache) revalidate(ctx context.Context, source KeySource) {
	detached := context.WithoutCancel(ctx)

	resultChan := c.group.DoChan(source.cacheKey(), func() (any, error) {
		return c.fetchAndStore(detached, source)
	})

	go func() {
		result := <-resultChan
		if result.Err != nil {
			c.config.Logger.Warn("Failed to revalidate signing keys, serving stale keys",
				"issuer", source.IssuerURL, "error", result.Err)
		}
	}()
}

// fetchAndStore fetches source's keys and records the outcome.
func (c *KeyCache) fetchAndStore(ctx context.Context, source KeySource) (KeySnapshot, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, c.config.FetchTimeout)
	defer cancel()

	snapshot, fetchErr := c.fetch(fetchCtx, source)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[source.cacheKey()]
	if !ok {
		entry = &keyEntry{}
		c.entries[source.cacheKey()] = entry
	}

	entry.lastAttempt = c.config.Now()
	entry.lastErr = fetchErr

	if fetchErr != nil {
		return KeySnapshot{}, fetchErr
	}

	entry.snapshot = snapshot
	entry.hasSnapshot = true

	return snapshot, nil
}

// fetch performs discovery and then fetches the JWKS it points at.
func (c *KeyCache) fetch(ctx context.Context, source KeySource) (KeySnapshot, error) {
	client, err := c.config.HTTPClient(source)
	if err != nil {
		return KeySnapshot{}, fmt.Errorf("failed to build http client: %w", err)
	}

	// A plain map rather than a tagged struct: the discovery document's
	// field names are fixed by the OIDC spec, not this codebase's own JSON
	// naming convention.
	var document map[string]any

	err = getJSON(ctx, client, source.discoveryURL(), &document)
	if err != nil {
		return KeySnapshot{}, fmt.Errorf("failed to fetch discovery document: %w", err)
	}

	issuer, _ := document["issuer"].(string)
	if issuer != source.IssuerURL {
		return KeySnapshot{}, fmt.Errorf("%w: got %q, want %q", ErrIssuerMismatch, issuer, source.IssuerURL)
	}

	jwksURI, _ := document["jwks_uri"].(string)
	if jwksURI == "" {
		return KeySnapshot{}, fmt.Errorf("%w: %s", ErrMissingJWKSURI, source.discoveryURL())
	}

	var keySet jose.JSONWebKeySet

	err = getJSON(ctx, client, jwksURI, &keySet)
	if err != nil {
		return KeySnapshot{}, fmt.Errorf("failed to fetch signing keys: %w", err)
	}

	keys := signingKeys(keySet.Keys)
	if len(keys) == 0 {
		return KeySnapshot{}, fmt.Errorf("%w: %s", ErrNoSigningKeys, jwksURI)
	}

	return KeySnapshot{Keys: keys, FetchedAt: c.config.Now()}, nil
}

// signingKeys keeps only the valid public keys usable for signatures.
func signingKeys(keys []jose.JSONWebKey) []jose.JSONWebKey {
	result := make([]jose.JSONWebKey, 0, len(keys))

	for _, key := range keys {
		if key.Use != "" && key.Use != keyUseSignature {
			continue
		}

		if !key.Valid() || !key.IsPublic() {
			continue
		}

		result = append(result, key)
	}

	return result
}

// getJSON GETs target and decodes its JSON body into out.
func getJSON(ctx context.Context, client *http.Client, target string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	request.Header.Set("Accept", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	defer func() {
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s returned %d", ErrUnexpectedStatus, target, response.StatusCode)
	}

	err = json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes)).Decode(out)
	if err != nil {
		return fmt.Errorf("failed to decode response from %s: %w", target, err)
	}

	return nil
}

// DefaultHTTPClient builds an HTTP client trusting source's own
// CertificateAuthority, or the host's roots when it is empty.
func DefaultHTTPClient(source KeySource) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Client{Timeout: defaultFetchTimeout}, nil
	}

	transport = transport.Clone()

	if source.CertificateAuthority != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(source.CertificateAuthority)) {
			return nil, ErrInvalidCertificateAuthority
		}

		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}

	return &http.Client{Transport: transport, Timeout: defaultFetchTimeout}, nil
}
