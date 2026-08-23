package etcdproxy_test

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

// fakeRefreshableKeySource implements both etcdproxy.KeySource and
// etcdproxy.Refresher, starting on a key the test hub doesn't recognize and
// switching to one it does the moment Refresh is called — standing in for
// IdentityKeySource without needing a real watched Secret.
type fakeRefreshableKeySource struct {
	mu        sync.Mutex
	current   ed25519.PrivateKey
	refreshTo ed25519.PrivateKey
	refreshes int
}

func (f *fakeRefreshableKeySource) Current() ed25519.PrivateKey {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.current
}

func (f *fakeRefreshableKeySource) Refresh(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.current = f.refreshTo
	f.refreshes++

	return nil
}

func (f *fakeRefreshableKeySource) refreshCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.refreshes
}

// TestRelayRefreshesIdentityAfterRepeatedAuthFailures covers authRecovery:
// a Relay connection signing every call with a key the hub doesn't
// currently trust (as if WatchIdentity's own background watch had gone
// stale without closing — the one failure mode its reconnect-on-close logic
// can't detect) should, after enough consecutive Unauthenticated responses,
// force a synchronous Refresh of the identity key rather than retrying the
// same rejected credential forever.
func TestRelayRefreshesIdentityAfterRepeatedAuthFailures(t *testing.T) {
	t.Parallel()

	now := time.Now()
	correct := generateTestIdentity(t)
	wrong := generateTestIdentity(t)
	secret := admittedPublicSecret(verifierTestZone, etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: correct.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: correct.certPEM, IssuedAt: now},
	})

	hubAddr, _ := startTestHub(t, newProxyTestHubClient(t, secret))

	correctKey, err := etcdproxy.LoadPrivateKey(correct.keyPEM)
	require.NoError(t, err)

	wrongKey, err := etcdproxy.LoadPrivateKey(wrong.keyPEM)
	require.NoError(t, err)

	keys := &fakeRefreshableKeySource{current: wrongKey, refreshTo: correctKey}

	socketPath := filepath.Join(t.TempDir(), "relay.sock")

	relay, err := etcdproxy.StartRelay(etcdproxy.RelayConfig{
		SocketPath:  socketPath,
		HubEndpoint: hubAddr,
		Zone:        verifierTestZone,
		Keys:        keys,
		Insecure:    true,
	})
	require.NoError(t, err)
	t.Cleanup(relay.Close)

	localConn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localConn.Close() })

	kvClient := etcdserverpb.NewKVClient(localConn)

	// Keep issuing calls signed with the wrong key until authRecovery's own
	// threshold trips and it refreshes keys — bounded generously so a
	// regression that stops it from ever triggering fails the test instead
	// of hanging.
	const maxAttempts = 20

	refreshed := false

	for range maxAttempts {
		if refreshed {
			break
		}

		_, _ = kvClient.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})
		refreshed = keys.refreshCount() > 0
	}

	require.True(t, refreshed, "authRecovery never refreshed the identity key after repeated auth failures")

	_, err = kvClient.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})
	assert.NoError(t, err, "call should succeed once authRecovery has refreshed onto the hub-trusted key")
}
