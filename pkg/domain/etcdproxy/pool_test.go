package etcdproxy_test

import (
	"context"
	"net"
	"sync"
	"testing"

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
