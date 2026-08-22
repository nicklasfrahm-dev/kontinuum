package etcdproxy

import (
	"context"
	"fmt"
	"sync/atomic"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// Pool spreads KV, Watch, and Lease traffic across a small, fixed number of
// real upstream connections to the same etcd3-compatible target, instead of
// either extreme this package exists to avoid:
//
//   - One connection per caller — what k8s.io/apiserver's own etcd3 factory
//     does, dialing a fresh *clientv3.Client per REST storage instance with
//     no caching or reuse — is what originally tripped Kine's per-connection
//     ping-strike enforcement: every connection runs its own independent
//     bandwidth-estimation ping timer, so more connections means more
//     independent chances for one of them to ping faster than Kine's
//     MinTime tolerates.
//   - One connection for every caller (what RegisterHub used to dial: a
//     single *grpc.ClientConn shared by every zone) avoids that, but at
//     enough callers becomes its own problem — a single throughput ceiling
//     and a single point of failure for all of them at once.
//
// Pool is the middle ground: a fixed handful of real connections (see
// DialPool), each wrapped in its own *Proxy, with calls spread across them
// round-robin. Picking happens once per call for unary RPCs, and once per
// stream — not per message — for Watch/LeaseKeepAlive, so a given stream
// stays pinned to the same underlying connection for its whole lifetime
// rather than splitting across several.
//
// DialPool also disables the ping mechanism itself on every connection it
// builds (see poolStreamWindowSize's own doc), not just the connection
// count that used to multiply exposure to it — so the pool size below
// exists purely for throughput headroom and blast-radius isolation now,
// not to keep ping-strike risk low; that risk is zero on these connections
// regardless of how many there are.
type Pool struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	etcdserverpb.UnimplementedLeaseServer

	proxies []*Proxy
	next    atomic.Uint64
}

// NewPool builds a Pool dispatching across upstreams, each authenticated
// identically via auth (nil for a trusted, local-only caller — see Proxy's
// own doc for when that applies). Use DialPool to build upstreams and a
// matching cleanup func in one call.
func NewPool(upstreams []*grpc.ClientConn, auth Authenticator) *Pool {
	proxies := make([]*Proxy, len(upstreams))
	for i, upstream := range upstreams {
		proxies[i] = NewProxy(upstream, auth)
	}

	return &Pool{proxies: proxies}
}

// poolStreamWindowSize and poolConnWindowSize are static HTTP/2
// flow-control window sizes applied to every connection DialPool builds —
// the only way to actually turn off grpc-go's automatic bandwidth-delay-
// product estimator, the thing sending the pings that trip Kine's
// per-connection ping-strike policy in the first place (see Pool's own
// doc). grpc-go's own internal/transport/http2_client.go only builds a
// bdpEst — and only that estimator ever calls for a BDP ping — when
// dialOptions.StaticWindowSize is false; WithInitialWindowSize and
// WithInitialConnWindowSize are the two DialOptions that set it true.
// Static means the window never grows dynamically past this once set, so
// both are sized generously for a busy Watch stream up front rather than
// left to grow on demand: poolStreamWindowSize covers one stream's own
// buffered events, poolConnWindowSize is larger because a single pooled
// connection carries many concurrent streams at once (see zonePoolSize's
// own doc on up to ~1000 zones sharing zonePoolSize connections) and needs
// headroom across all of them, not just one. Starting points, not derived
// from a hard formula — tune based on real Watch payload sizes and fan-in
// once deployed.
const (
	poolStreamWindowSize = 16 << 20 // 16 MiB
	poolConnWindowSize   = 64 << 20 // 64 MiB
)

// DialPool dials size independent connections to target, all using
// dialOpts plus a static flow-control window (see poolStreamWindowSize's
// own doc) that keeps every one of them from ever sending a BDP ping at
// all, and returns a Pool spreading traffic across them plus a cleanup
// func closing every one of them. On error, every connection dialed so far
// is already closed before returning.
func DialPool(
	target string, size int, auth Authenticator, dialOpts ...grpc.DialOption,
) (*Pool, func(), error) {
	opts := append([]grpc.DialOption{
		grpc.WithInitialWindowSize(poolStreamWindowSize),
		grpc.WithInitialConnWindowSize(poolConnWindowSize),
	}, dialOpts...)

	conns := make([]*grpc.ClientConn, 0, size)

	closeAll := func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}

	for range size {
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			closeAll()

			return nil, nil, fmt.Errorf("failed to dial %q: %w", target, err)
		}

		conns = append(conns, conn)
	}

	return NewPool(conns, auth), closeAll, nil
}

// Register registers p's KV, Watch, and Lease services on server — see
// Proxy.Register's own doc.
func (p *Pool) Register(server *grpc.Server) {
	etcdserverpb.RegisterKVServer(server, p)
	etcdserverpb.RegisterWatchServer(server, p)
	etcdserverpb.RegisterLeaseServer(server, p)
}

// KV. Each method picks a proxy round-robin, then delegates to it.

// Range implements etcdserverpb.KVServer.
func (p *Pool) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return p.pick().Range(ctx, req)
}

// Put implements etcdserverpb.KVServer.
func (p *Pool) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return p.pick().Put(ctx, req)
}

// DeleteRange implements etcdserverpb.KVServer.
func (p *Pool) DeleteRange(
	ctx context.Context, req *etcdserverpb.DeleteRangeRequest,
) (*etcdserverpb.DeleteRangeResponse, error) {
	return p.pick().DeleteRange(ctx, req)
}

// Txn implements etcdserverpb.KVServer.
func (p *Pool) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return p.pick().Txn(ctx, req)
}

// Compact implements etcdserverpb.KVServer.
func (p *Pool) Compact(
	ctx context.Context, req *etcdserverpb.CompactionRequest,
) (*etcdserverpb.CompactionResponse, error) {
	return p.pick().Compact(ctx, req)
}

// Lease. Each method picks a proxy round-robin, then delegates to it.

// LeaseGrant implements etcdserverpb.LeaseServer.
func (p *Pool) LeaseGrant(
	ctx context.Context, req *etcdserverpb.LeaseGrantRequest,
) (*etcdserverpb.LeaseGrantResponse, error) {
	return p.pick().LeaseGrant(ctx, req)
}

// LeaseRevoke implements etcdserverpb.LeaseServer.
func (p *Pool) LeaseRevoke(
	ctx context.Context, req *etcdserverpb.LeaseRevokeRequest,
) (*etcdserverpb.LeaseRevokeResponse, error) {
	return p.pick().LeaseRevoke(ctx, req)
}

// LeaseTimeToLive implements etcdserverpb.LeaseServer.
func (p *Pool) LeaseTimeToLive(
	ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest,
) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return p.pick().LeaseTimeToLive(ctx, req)
}

// LeaseLeases implements etcdserverpb.LeaseServer.
func (p *Pool) LeaseLeases(
	ctx context.Context, req *etcdserverpb.LeaseLeasesRequest,
) (*etcdserverpb.LeaseLeasesResponse, error) {
	return p.pick().LeaseLeases(ctx, req)
}

// LeaseKeepAlive picks a proxy round-robin once, for the whole stream — see
// Pool's own doc.
func (p *Pool) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	return p.pick().LeaseKeepAlive(stream)
}

// Watch.

// Watch picks a proxy round-robin once, for the whole stream — see Pool's
// own doc.
func (p *Pool) Watch(stream etcdserverpb.Watch_WatchServer) error {
	return p.pick().Watch(stream)
}

// pick returns the pool's next proxy, round-robin.
func (p *Pool) pick() *Proxy {
	n := p.next.Add(1) - 1

	return p.proxies[n%uint64(len(p.proxies))]
}
