package identityprovider_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
)

const (
	testFreshFor           = time.Minute
	testStaleFor           = time.Hour
	testMinRefreshInterval = 10 * time.Second
	concurrentCallers      = 16
	eventuallyTimeout      = 5 * time.Second
	eventuallyTick         = 10 * time.Millisecond
)

func newTestCache(clock *fakeClock) *identityprovider.KeyCache {
	return identityprovider.NewKeyCache(identityprovider.KeyCacheConfig{
		FreshFor:           testFreshFor,
		StaleFor:           testStaleFor,
		MinRefreshInterval: testMinRefreshInterval,
		Now:                clock.Now,
	})
}

func TestKeyCacheFetchesOnceWhileFresh(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	clock := newFakeClock()
	cache := newTestCache(clock)

	keys, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, firstKeyID, keys[0].KeyID)

	clock.Advance(testFreshFor / 2)

	_, err = cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.Equal(t, int32(1), issuer.jwksHits.Load())

	snapshot, ok := cache.Snapshot(issuer.Source())
	require.True(t, ok)
	assert.Equal(t, newFakeClock().Now(), snapshot.FetchedAt)
}

func TestKeyCacheDeduplicatesConcurrentFetches(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	cache := newTestCache(newFakeClock())
	release := issuer.gateJWKS()

	var waitGroup sync.WaitGroup

	errs := make(chan error, concurrentCallers)

	for range concurrentCallers {
		waitGroup.Go(func() {
			_, err := cache.Keys(context.Background(), issuer.Source())
			errs <- err
		})
	}

	require.Eventually(t, func() bool { return issuer.jwksHits.Load() == 1 }, eventuallyTimeout, eventuallyTick)
	release()
	waitGroup.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, int32(1), issuer.jwksHits.Load())
}

func TestKeyCacheServesStaleWhileRevalidating(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	clock := newFakeClock()
	cache := newTestCache(clock)

	_, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)

	issuer.addKey(t, rotatedKeyID)

	release := issuer.gateJWKS()

	clock.Advance(testFreshFor + time.Second)

	keys, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.Len(t, keys, 1, "stale keys must be served without waiting for the refresh")

	release()

	require.Eventually(t, func() bool {
		snapshot, ok := cache.Snapshot(issuer.Source())

		return ok && len(snapshot.Keys) == 2
	}, eventuallyTimeout, eventuallyTick)
	assert.Equal(t, int32(2), issuer.jwksHits.Load())
}

func TestKeyCacheBlocksOnceFullyExpired(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	clock := newFakeClock()
	cache := newTestCache(clock)

	_, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)

	issuer.addKey(t, rotatedKeyID)
	clock.Advance(testFreshFor + testStaleFor + time.Second)

	keys, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.Len(t, keys, 2)
}

func TestKeyCacheKeepsKeysWhenRefreshFails(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	cache := newTestCache(newFakeClock())

	_, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)

	issuer.failing.Store(true)

	_, err = cache.Refresh(context.Background(), issuer.Source())
	require.ErrorIs(t, err, identityprovider.ErrUnexpectedStatus)
	require.ErrorIs(t, cache.LastError(issuer.Source()), identityprovider.ErrUnexpectedStatus)

	snapshot, ok := cache.Snapshot(issuer.Source())
	require.True(t, ok)
	assert.Len(t, snapshot.Keys, 1)
}

func TestKeyCacheBacksOffWhileIssuerIsDown(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	issuer.failing.Store(true)

	clock := newFakeClock()
	cache := newTestCache(clock)

	_, err := cache.Keys(context.Background(), issuer.Source())
	require.ErrorIs(t, err, identityprovider.ErrUnexpectedStatus)

	_, err = cache.Keys(context.Background(), issuer.Source())
	require.ErrorIs(t, err, identityprovider.ErrUnexpectedStatus)
	assert.Equal(t, int32(1), issuer.discoveryHits.Load(), "a recent failure must not be refetched")

	issuer.failing.Store(false)
	clock.Advance(testMinRefreshInterval)

	keys, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestKeyCacheLimitsRevalidationWhileStale(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	clock := newFakeClock()
	cache := newTestCache(clock)

	_, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)

	issuer.failing.Store(true)
	clock.Advance(testFreshFor + time.Second)

	for range concurrentCallers {
		keys, err := cache.Keys(context.Background(), issuer.Source())
		require.NoError(t, err)
		require.Len(t, keys, 1)

		require.Eventually(t, func() bool { return cache.LastError(issuer.Source()) != nil },
			eventuallyTimeout, eventuallyTick)
	}

	assert.Equal(t, int32(2), issuer.discoveryHits.Load(), "a failing stale entry must back off")
}

func TestKeyCacheRejectsIssuerMismatch(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	other := "https://evil.example.com"
	issuer.issuerClaim.Store(&other)

	_, err := newTestCache(newFakeClock()).Keys(context.Background(), issuer.Source())
	require.ErrorIs(t, err, identityprovider.ErrIssuerMismatch)
}

func TestKeyCacheRefreshIfDueIsRateLimited(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	clock := newFakeClock()
	cache := newTestCache(clock)

	_, refreshed, err := cache.RefreshIfDue(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.True(t, refreshed)

	_, refreshed, err = cache.RefreshIfDue(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.False(t, refreshed)

	clock.Advance(testMinRefreshInterval)

	_, refreshed, err = cache.RefreshIfDue(context.Background(), issuer.Source())
	require.NoError(t, err)
	assert.True(t, refreshed)
	assert.Equal(t, int32(2), issuer.jwksHits.Load())
}

func TestKeyCacheRetainDropsUnlistedSources(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	cache := newTestCache(newFakeClock())

	_, err := cache.Keys(context.Background(), issuer.Source())
	require.NoError(t, err)

	cache.Retain([]identityprovider.KeySource{issuer.Source()})

	_, cached := cache.Snapshot(issuer.Source())
	assert.True(t, cached)

	cache.Retain(nil)

	_, cached = cache.Snapshot(issuer.Source())
	assert.False(t, cached)
}

func TestKeyCacheRefreshHonorsCallerCancellation(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	cache := newTestCache(newFakeClock())
	release := issuer.gateJWKS()

	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cache.Refresh(ctx, issuer.Source())
	require.ErrorIs(t, err, context.Canceled)
}

func TestDefaultHTTPClientRejectsInvalidCA(t *testing.T) {
	t.Parallel()

	_, err := identityprovider.DefaultHTTPClient(identityprovider.KeySource{CertificateAuthority: "not pem"})
	require.ErrorIs(t, err, identityprovider.ErrInvalidCertificateAuthority)
}
