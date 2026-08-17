package etcdproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthorizationMetadataKey is the incoming gRPC metadata key a caller's
// bearer credential arrives on — the gRPC-metadata equivalent of the HTTP
// "Authorization" header (grpc-go lowercases metadata keys automatically,
// so this is already lower-case).
const AuthorizationMetadataKey = "authorization"

// BearerPrefix precedes the token itself in the Authorization value, same
// convention as an HTTP "Authorization: Bearer" header.
const BearerPrefix = "Bearer "

// Authenticator authenticates an incoming call's raw "Authorization"
// metadata value, returning an error for anything that doesn't identify a
// valid, currently-trusted caller. Implemented by *Verifier on the hub
// side; left nil on a zone's own Relay, where the only caller is that same
// zone's own local, already-trusted apiserver process — see Proxy's own
// doc for why that side skips authentication entirely.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (zone string, err error)
}

// Proxy relays KV, Watch, and Lease RPCs verbatim to an upstream
// etcd3-compatible endpoint, without interpreting their contents — so it
// works unchanged regardless of which real backend (postgres/sqlite/mysql/
// nats) a hub's own Kine instance happens to translate to.
//
// The same type backs both directions this package supports: on the hub
// (see RegisterHub), Authenticator is a real *Verifier, since an incoming
// call could be any zone (or nobody at all) presenting a bearer credential
// that needs checking before it's allowed through. On a zone's own local
// Relay, Authenticator is nil: the only caller able to reach that local,
// loopback-only endpoint at all is that same zone's own apiserver process,
// which is trusted by construction — the credential-checking already
// happened one hop further out, on the hub, and Relay's own outbound call
// to it separately attaches the credential itself (see Relay's own doc).
type Proxy struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	etcdserverpb.UnimplementedLeaseServer

	kv    etcdserverpb.KVClient
	watch etcdserverpb.WatchClient
	lease etcdserverpb.LeaseClient
	auth  Authenticator
}

// NewProxy builds a Proxy forwarding to upstream — see Proxy's own doc for
// when auth should (hub) or shouldn't (zone-side Relay) be nil.
func NewProxy(upstream *grpc.ClientConn, auth Authenticator) *Proxy {
	return &Proxy{
		kv:    etcdserverpb.NewKVClient(upstream),
		watch: etcdserverpb.NewWatchClient(upstream),
		lease: etcdserverpb.NewLeaseClient(upstream),
		auth:  auth,
	}
}

// Register registers p's KV, Watch, and Lease services on server — safe to
// call on an already-constructed *grpc.Server (e.g.
// libkapi.Ctx.GRPCServer's own lazily-built one), since RegisterXXXServer
// just calls server.RegisterService under the hood, valid any time before
// the server starts Serve-ing.
func (p *Proxy) Register(server *grpc.Server) {
	etcdserverpb.RegisterKVServer(server, p)
	etcdserverpb.RegisterWatchServer(server, p)
	etcdserverpb.RegisterLeaseServer(server, p)
}

// KV. Each method authenticates, then forwards verbatim to p.kv — see
// Proxy's own doc.

// Range implements etcdserverpb.KVServer by forwarding to the upstream.
func (p *Proxy) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.kv.Range(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream range failed: %w", err)
	}

	return resp, nil
}

// Put implements etcdserverpb.KVServer by forwarding to the upstream.
func (p *Proxy) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.kv.Put(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream put failed: %w", err)
	}

	return resp, nil
}

// DeleteRange implements etcdserverpb.KVServer by forwarding to the
// upstream.
func (p *Proxy) DeleteRange(
	ctx context.Context, req *etcdserverpb.DeleteRangeRequest,
) (*etcdserverpb.DeleteRangeResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.kv.DeleteRange(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream delete-range failed: %w", err)
	}

	return resp, nil
}

// Txn implements etcdserverpb.KVServer by forwarding to the upstream.
func (p *Proxy) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.kv.Txn(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream txn failed: %w", err)
	}

	return resp, nil
}

// Compact implements etcdserverpb.KVServer by forwarding to the upstream.
func (p *Proxy) Compact(
	ctx context.Context, req *etcdserverpb.CompactionRequest,
) (*etcdserverpb.CompactionResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.kv.Compact(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream compact failed: %w", err)
	}

	return resp, nil
}

// Lease. Each method authenticates, then forwards verbatim to p.lease —
// see Proxy's own doc.

// LeaseGrant implements etcdserverpb.LeaseServer by forwarding to the
// upstream.
func (p *Proxy) LeaseGrant(
	ctx context.Context, req *etcdserverpb.LeaseGrantRequest,
) (*etcdserverpb.LeaseGrantResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.lease.LeaseGrant(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream lease-grant failed: %w", err)
	}

	return resp, nil
}

// LeaseRevoke implements etcdserverpb.LeaseServer by forwarding to the
// upstream.
func (p *Proxy) LeaseRevoke(
	ctx context.Context, req *etcdserverpb.LeaseRevokeRequest,
) (*etcdserverpb.LeaseRevokeResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.lease.LeaseRevoke(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream lease-revoke failed: %w", err)
	}

	return resp, nil
}

// LeaseTimeToLive implements etcdserverpb.LeaseServer by forwarding to the
// upstream.
func (p *Proxy) LeaseTimeToLive(
	ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest,
) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.lease.LeaseTimeToLive(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream lease-time-to-live failed: %w", err)
	}

	return resp, nil
}

// LeaseLeases implements etcdserverpb.LeaseServer by forwarding to the
// upstream.
func (p *Proxy) LeaseLeases(
	ctx context.Context, req *etcdserverpb.LeaseLeasesRequest,
) (*etcdserverpb.LeaseLeasesResponse, error) {
	err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.lease.LeaseLeases(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("upstream lease-leases failed: %w", err)
	}

	return resp, nil
}

// LeaseKeepAlive implements etcdserverpb.LeaseServer by opening a matching
// stream against the upstream and pumping both directions between the two
// — see pumpBidi.
func (p *Proxy) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	err := p.authenticate(stream.Context())
	if err != nil {
		return err
	}

	upstream, err := p.lease.LeaseKeepAlive(stream.Context())
	if err != nil {
		return fmt.Errorf("failed to open upstream lease-keep-alive stream: %w", err)
	}

	return pumpBidi[etcdserverpb.LeaseKeepAliveRequest, etcdserverpb.LeaseKeepAliveResponse](stream, upstream)
}

// Watch.

// Watch implements etcdserverpb.WatchServer by opening a matching stream
// against the upstream and pumping both directions between the two — see
// pumpBidi.
func (p *Proxy) Watch(stream etcdserverpb.Watch_WatchServer) error {
	err := p.authenticate(stream.Context())
	if err != nil {
		return err
	}

	upstream, err := p.watch.Watch(stream.Context())
	if err != nil {
		return fmt.Errorf("failed to open upstream watch stream: %w", err)
	}

	return pumpBidi[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse](stream, upstream)
}

// authenticate checks ctx's own incoming "Authorization: Bearer <token>"
// metadata against p.auth — a no-op when p.auth is nil (see Proxy's own
// doc for when that's the case).
func (p *Proxy) authenticate(ctx context.Context) error {
	if p.auth == nil {
		return nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}

	values := md.Get(AuthorizationMetadataKey)
	if len(values) == 0 || !strings.HasPrefix(values[0], BearerPrefix) {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}

	_, err := p.auth.Authenticate(ctx, strings.TrimPrefix(values[0], BearerPrefix))
	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}

	return nil
}

// bidiServerStream and bidiClientStream are the minimal shapes
// Watch_Watch{Server,Client} and Lease_LeaseKeepAlive{Server,Client} both
// already satisfy, letting pumpBidi forward either pair generically rather
// than needing one hand-written copy per streaming RPC.
type bidiServerStream[Req, Resp any] interface {
	Send(resp *Resp) error
	Recv() (*Req, error)
}

type bidiClientStream[Req, Resp any] interface {
	Send(req *Req) error
	Recv() (*Resp, error)
	CloseSend() error
}

// pumpBidi relays server's own incoming requests to client, and client's
// own responses back to server, concurrently in both directions, until
// both sides have cleanly reached io.EOF or one of them errors. Returns
// the first non-nil error from either direction, if any — a genuine
// failure on one side is surfaced rather than masked by the other
// direction's own clean close.
func pumpBidi[Req, Resp any](server bidiServerStream[Req, Resp], client bidiClientStream[Req, Resp]) error {
	// pumpDirections is one per direction (server->client, client->server)
	// — see pumpRequests/pumpResponses below.
	const pumpDirections = 2

	errCh := make(chan error, pumpDirections)

	go pumpRequests(server, client, errCh)
	go pumpResponses(server, client, errCh)

	var firstErr error

	for range pumpDirections {
		err := <-errCh
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// pumpRequests relays server's own incoming requests to client until it
// cleanly reaches io.EOF (in which case it half-closes client's own send
// side and reports that result, nil or not, as this direction's outcome)
// or errors.
func pumpRequests[Req, Resp any](
	server bidiServerStream[Req, Resp], client bidiClientStream[Req, Resp], errCh chan<- error,
) {
	for {
		req, err := server.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				errCh <- client.CloseSend()

				return
			}

			errCh <- err

			return
		}

		err = client.Send(req)
		if err != nil {
			errCh <- err

			return
		}
	}
}

// pumpResponses relays client's own responses back to server until it
// cleanly reaches io.EOF or errors — the mirror of pumpRequests, for the
// opposite direction.
func pumpResponses[Req, Resp any](
	server bidiServerStream[Req, Resp], client bidiClientStream[Req, Resp], errCh chan<- error,
) {
	for {
		resp, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				errCh <- nil

				return
			}

			errCh <- err

			return
		}

		err = server.Send(resp)
		if err != nil {
			errCh <- err

			return
		}
	}
}
