package etcdproxy_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

// peerTrackingKV is a minimal etcdserverpb.KVServer that records the
// distinct remote addresses its calls arrive from, so tests can tell how
// many real connections actually reached it — not just how many logical
// callers issued requests.
type peerTrackingKV struct {
	etcdserverpb.UnimplementedKVServer

	mu   sync.Mutex
	seen map[string]struct{}
}

func newPeerTrackingKV() *peerTrackingKV {
	return &peerTrackingKV{seen: map[string]struct{}{}}
}

func (k *peerTrackingKV) Range(
	ctx context.Context, _ *etcdserverpb.RangeRequest,
) (*etcdserverpb.RangeResponse, error) {
	if p, ok := peer.FromContext(ctx); ok {
		k.mu.Lock()
		k.seen[p.Addr.String()] = struct{}{}
		k.mu.Unlock()
	}

	return &etcdserverpb.RangeResponse{}, nil
}

func (k *peerTrackingKV) distinctPeers() int {
	k.mu.Lock()
	defer k.mu.Unlock()

	return len(k.seen)
}

// startPeerTrackingKV starts a peerTrackingKV on a real TCP listener — TCP,
// not the Unix sockets this package's other tests use, because a Unix
// domain socket's client side is unnamed/autobind, so every dialed
// connection would look identical to the server; TCP gives each one a
// distinguishable source port.
func startPeerTrackingKV(t *testing.T) (string, *peerTrackingKV) {
	t.Helper()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	kvServer := newPeerTrackingKV()
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, kvServer)

	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	return listener.Addr().String(), kvServer
}

// TestDialPoolBoundsConnectionCount covers Pool's whole reason for
// existing (see its own doc): however many concurrent callers issue calls
// through it, at most as many real connections as its own size ever reach
// the upstream — never one per caller, and with enough concurrent callers
// to guarantee round-robin visits every pooled connection, exactly that
// many.
func TestDialPoolBoundsConnectionCount(t *testing.T) {
	t.Parallel()

	target, kvServer := startPeerTrackingKV(t)

	const poolSize = 3

	pool, cleanup, err := etcdproxy.DialPool(
		target, poolSize, nil, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	const callers = 50

	var waitGroup sync.WaitGroup

	for range callers {
		waitGroup.Go(func() {
			_, rangeErr := pool.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})
			assert.NoError(t, rangeErr)
		})
	}

	waitGroup.Wait()

	assert.Equal(t, poolSize, kvServer.distinctPeers(),
		"expected exactly %d real connections to reach the upstream across %d callers", poolSize, callers)
}

// TestPoolReconnectsAfterUpstreamGOAWAY covers the scenario Pool's own
// upstream (Kine, on the hub — see RegisterHub) tears down a connection
// with a GOAWAY, including but not limited to the ENHANCE_YOUR_CALM/
// too_many_pings notice grpc-go logs at Error, downgraded to Warn by
// pkg/logging/grpc.go's own bridge: server.Stop sends every connected
// client a GOAWAY before closing, exactly the client-side teardown a real
// ENHANCE_YOUR_CALM GOAWAY triggers, just for a different reason. Nothing
// in this package forces a reconnect — grpc-go's own *grpc.ClientConn
// already redials on its own default backoff once the upstream comes back
// — this proves that promise actually holds for Pool's own dial options
// (the static flow-control windows DialPool sets — see its own doc) rather
// than trusting grpc-go's default behavior unverified.
func TestPoolReconnectsAfterUpstreamGOAWAY(t *testing.T) {
	t.Parallel()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, newPeerTrackingKV())

	go func() { _ = server.Serve(listener) }()

	pool, cleanup, err := etcdproxy.DialPool(
		addr, 1, nil, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	_, err = pool.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})
	require.NoError(t, err, "sanity check: the pool must work before the upstream ever goes away")

	// Stop sends a GOAWAY to every connected client before closing the
	// listener — the connection Pool is holding is torn down exactly as it
	// would be by a real one.
	server.Stop()

	// Re-listen on the exact same address once it's free again, standing in
	// for Kine (or the hub) coming back up after whatever prompted the
	// GOAWAY — retried rather than asserted immediately, since the OS may
	// briefly hold the port after the previous listener closes.
	var listener2 net.Listener

	require.Eventually(t, func() bool {
		relistened, listenErr := new(net.ListenConfig).Listen(t.Context(), "tcp", addr)
		if listenErr != nil {
			return false
		}

		listener2 = relistened

		return true
	}, 5*time.Second, 50*time.Millisecond, "never managed to re-listen on %q", addr)

	server2 := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server2, newPeerTrackingKV())

	go func() { _ = server2.Serve(listener2) }()

	t.Cleanup(server2.Stop)

	// pool's own *grpc.ClientConn is left exactly as DialPool built it — no
	// ResetConnectBackoff, no new Pool, nothing package-specific — only
	// grpc-go's own default reconnect backoff stands between the GOAWAY
	// above and this call succeeding again.
	require.Eventually(t, func() bool {
		_, rangeErr := pool.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})

		return rangeErr == nil
	}, 10*time.Second, 50*time.Millisecond, "pool never reconnected to the upstream after its GOAWAY-driven teardown")
}

// TestDialPoolClosesAllConnectionsOnDialError covers DialPool's own
// documented cleanup guarantee: if any one of size dials fails, every
// connection dialed so far is already closed before it returns — proven
// here by dialing a target that always fails (an address nothing listens
// on isn't enough, since grpc.NewClient never actually dials eagerly; a
// malformed target that fails resolution at NewClient time is what
// exercises the error path).
func TestDialPoolClosesAllConnectionsOnDialError(t *testing.T) {
	t.Parallel()

	pool, cleanup, err := etcdproxy.DialPool("://not-a-valid-target", 3, nil)
	require.Error(t, err)
	assert.Nil(t, pool)
	assert.Nil(t, cleanup)
}
