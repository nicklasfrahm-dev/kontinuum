package etcdproxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// jwtCredentials attaches "Authorization: Bearer <jwt>" to every outbound
// RPC over the connection it's installed on, minting a fresh, short-lived
// JWT (see SignToken) on every single call — grpc-go's PerRPCCredentials
// hook is already invoked per-RPC, and ed25519 signing is cheap enough
// that there's no need to cache and refresh a token instead. keys.Current
// is called fresh on every call too, rather than once at dial time — see
// KeySource's own doc for why that's what actually lets a rotated identity
// take effect without restarting the process holding this connection.
type jwtCredentials struct {
	zone                     string
	keys                     KeySource
	requireTransportSecurity bool
}

func (c jwtCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, err := SignToken(c.zone, c.keys.Current())
	if err != nil {
		return nil, fmt.Errorf("failed to sign etcd proxy token: %w", err)
	}

	return map[string]string{AuthorizationMetadataKey: BearerPrefix + token}, nil
}

func (c jwtCredentials) RequireTransportSecurity() bool {
	return c.requireTransportSecurity
}

// Relay is a zone's own local, authenticated bridge onto the hub's etcd
// gRPC proxy (see RegisterHub) — see this package's own doc for why a zone
// needs one at all, rather than dialing storage directly. It listens on a
// local Unix socket (matching Kine's own convention — see
// libkapi/storage.startKine) that only this same process's own apiserver
// ever dials, via libkapi's already-supported "unix://" storage scheme
// (see storage.Resolve) — from the apiserver's own point of view, this is
// indistinguishable from talking to a local Kine instance. Every call
// Relay receives there is forwarded straight through to the hub, with a
// fresh JWT signed by the zone's own identity key (see SignToken) attached
// on every outbound RPC. Relay's own local listener needs no authentication of its
// own — see Proxy's own doc for why Authenticator is left nil here: the
// only caller able to reach a loopback Unix socket inside this same
// container is this same process's own apiserver.
type Relay struct {
	listener net.Listener
	server   *grpc.Server
	upstream *grpc.ClientConn
}

// RelayConfig configures StartRelay.
type RelayConfig struct {
	// SocketPath is where Relay listens.
	SocketPath string
	// HubEndpoint is the hub's own etcd gRPC proxy address (host:port) —
	// see ParseRelayDSN for how a zone's own KONTINUUM_SERVER_STORAGE
	// value carries this.
	HubEndpoint string
	// Zone and Keys identify this zone to the hub (see SignToken) — Keys
	// supplies this zone's own ed25519 identity key (see GenerateIdentity
	// and pkg/domain/zone's ensureEtcdIdentity) on every signed RPC,
	// typically an *IdentityKeySource kept live by WatchIdentity so a
	// rotated key takes effect immediately, with no restart of the process
	// running this Relay.
	Zone string
	Keys KeySource
	// Insecure skips TLS entirely on the connection to HubEndpoint — for
	// local development only; a real deployment's HubEndpoint is expected
	// to terminate TLS, the same as every other kind of traffic the hub
	// serves.
	Insecure bool
	// InsecureSkipVerify keeps TLS but skips certificate verification —
	// for local development against a HubEndpoint terminating TLS with a
	// self-signed certificate (see compose.yaml's own proxy service), where
	// Insecure above would be a step too far: the connection still needs
	// real TLS framing (HTTP/2 over plaintext isn't what a TLS-terminating
	// proxy speaks), just not certificate validation against this
	// process's own root CA set. Ignored when Insecure is true.
	InsecureSkipVerify bool
	// Logger receives authRecovery's own recovery-triggered messages —
	// falls back to slog.Default() when nil, so existing callers (chiefly
	// tests) that never hit that path need not supply one.
	Logger *slog.Logger
}

// authFailureThreshold is how many consecutive Unauthenticated responses
// from HubEndpoint a Relay connection tolerates before treating its own
// cached identity key and hub connection as possibly stale and forcing
// recovery — see authRecovery. One alone is expected background noise (a
// rotation's own brief propagation delay, see IdentityOverlapWindow's own
// doc); a run this long left alone would otherwise retry forever against
// the same rejected credential, since a per-RPC signing failure never
// surfaces to WatchIdentity's own watch-based refresh path on its own —
// that path only ever reacts to the identity Secret itself changing, not to
// what the hub does with whatever's currently signed with it.
const authFailureThreshold = 5

// authRecovery watches every RPC Relay's own connection to HubEndpoint
// makes, tracking consecutive Unauthenticated responses. Reaching
// authFailureThreshold triggers recovery exactly once, then resets the
// counter to require another full run before triggering again: keys.Refresh
// (if Keys implements Refresher) forces a synchronous re-fetch of the
// zone's identity key, bypassing whatever state WatchIdentity's own
// background watch is stuck in, and conn.ResetConnectBackoff forces the
// underlying *grpc.ClientConn to retry its transport immediately rather
// than waiting out whatever backoff it's currently in — covering the
// separate, less likely case that the connection itself, not just the
// credential, is what's actually stuck. Safe for concurrent use: every RPC
// Relay forwards observes it, potentially from many goroutines at once.
type authRecovery struct {
	zone   string
	keys   KeySource
	logger *slog.Logger

	mu       sync.Mutex
	failures int
	conn     *grpc.ClientConn // set once, right after grpc.NewClient returns — see StartRelay
}

// recordResult updates r from a single RPC's outcome — nil or any non-
// Unauthenticated error resets the streak, an Unauthenticated error extends
// it and, upon reaching authFailureThreshold, triggers recover(), which
// deliberately uses its own context.Background() rather than any ctx
// threaded through here — see recover's own doc for why.
func (r *authRecovery) recordResult(err error) {
	if status.Code(err) != codes.Unauthenticated {
		r.mu.Lock()
		r.failures = 0
		r.mu.Unlock()

		return
	}

	r.mu.Lock()
	r.failures++

	trigger := r.failures >= authFailureThreshold
	if trigger {
		r.failures = 0
	}
	r.mu.Unlock()

	if trigger {
		r.recover()
	}
}

// recover runs authRecovery's two independent recovery actions — see
// authRecovery's own doc. Uses context.Background(), not the ctx of
// whichever RPC's own failure tripped the threshold — that ctx is on its
// way out (its RPC has already returned to its caller by the time recover
// runs) and may already be canceled before this refresh even starts.
func (r *authRecovery) recover() {
	r.logger.Warn("repeated invalid bearer token responses from hub, forcing recovery",
		"zone", r.zone, "threshold", authFailureThreshold)

	if refresher, ok := r.keys.(Refresher); ok {
		refreshErr := refresher.Refresh(context.Background())
		if refreshErr != nil {
			r.logger.Error("failed to refresh etcd proxy identity after repeated auth failures",
				"zone", r.zone, "error", refreshErr)
		}
	}

	r.conn.ResetConnectBackoff()
}

// unaryInterceptor implements grpc.UnaryClientInterceptor, feeding every
// unary call's outcome through recordResult.
func (r *authRecovery) unaryInterceptor(
	ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	r.recordResult(err) //nolint:contextcheck // recover() deliberately uses context.Background(), see its own doc

	return err
}

// streamInterceptor implements grpc.StreamClientInterceptor. Opening the
// stream itself rarely fails on auth — Proxy's own Watch/LeaseKeepAlive
// handlers authenticate first thing, but that rejection only ever surfaces
// as a stream-level error on the first Recv, not from streamer here — so a
// successfully opened stream is wrapped in authRecoveryClientStream to keep
// watching it.
//
//nolint:ireturn // grpc.StreamClientInterceptor is grpc-go's own seam, mirrors grpc.Streamer's own return
func (r *authRecovery) streamInterceptor(
	ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
	streamer grpc.Streamer, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		r.recordResult(err) //nolint:contextcheck // recover() deliberately uses context.Background(), see its own doc

		return nil, err
	}

	return authRecoveryClientStream{ClientStream: stream, recovery: r}, nil
}

// authRecoveryClientStream wraps a grpc.ClientStream, feeding every RecvMsg
// outcome through authRecovery.recordResult — see streamInterceptor's own
// doc for why that's where a streaming RPC's auth rejection actually shows
// up.
type authRecoveryClientStream struct {
	grpc.ClientStream

	recovery *authRecovery
}

// RecvMsg must return the upstream error unwrapped, not implement it — a
// caller further up (e.g. pumpResponses) checks it with errors.Is(io.EOF)
// and status.Code, both of which need the original error as-is.
//
//nolint:wrapcheck // see doc above
func (s authRecoveryClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	s.recovery.recordResult(err)

	return err
}

// StartRelay starts a Relay per cfg, listening immediately — a returned
// nil error means the local socket is already accepting connections. Call
// Close when done.
func StartRelay(cfg RelayConfig) (*Relay, error) {
	// A stale socket file from a previous, uncleanly-terminated process
	// would otherwise make Listen fail with "address already in use" —
	// harmless to remove, since a Unix socket file carries no state of its
	// own once nothing is listening on it.
	_ = os.Remove(cfg.SocketPath)

	// context.Background(), not a context this call was ever handed one of
	// (StartRelay has no ctx parameter of its own) — the listener's own
	// lifetime is tied to Relay.Close, not to any request/setup context
	// that might be canceled before this process is done with it, the same
	// reasoning buildServer's own libkapi.New(context.Background(), ...)
	// call documents.
	listener, err := new(net.ListenConfig).Listen(context.Background(), "unix", cfg.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %q: %w", cfg.SocketPath, err)
	}

	//nolint:gosec // opt-in, dev-only — see RelayConfig.InsecureSkipVerify's own doc
	transportCreds := credentials.NewTLS(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	})
	if cfg.Insecure {
		transportCreds = insecure.NewCredentials()
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	recovery := &authRecovery{zone: cfg.Zone, keys: cfg.Keys, logger: logger}

	upstream, err := grpc.NewClient(cfg.HubEndpoint,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithPerRPCCredentials(jwtCredentials{
			zone:                     cfg.Zone,
			keys:                     cfg.Keys,
			requireTransportSecurity: !cfg.Insecure,
		}),
		// Static flow-control windows disable grpc-go's automatic BDP ping
		// estimator on this connection, same as DialPool's own identical
		// options and for the same reason (see poolStreamWindowSize's own
		// doc) — this is the one leg of the package that crosses the real
		// network to reach HubEndpoint rather than staying on localhost, so
		// it's the one most exposed to a receiving proxy's own ping-rate
		// enforcement.
		grpc.WithInitialWindowSize(poolStreamWindowSize),
		grpc.WithInitialConnWindowSize(poolConnWindowSize),
		grpc.WithChainUnaryInterceptor(recovery.unaryInterceptor),
		grpc.WithChainStreamInterceptor(recovery.streamInterceptor),
	)
	if err != nil {
		_ = listener.Close()

		return nil, fmt.Errorf("failed to dial hub etcd proxy %q: %w", cfg.HubEndpoint, err)
	}

	// Set once the connection recover() would reset actually exists — safe
	// before Serve starts below, since nothing can reach this Relay's own
	// listener (and so trigger an RPC through recovery's interceptors) until
	// then.
	recovery.conn = upstream

	server := grpc.NewServer()
	NewProxy(upstream, nil).Register(server)

	go func() {
		// Serve only returns once Close (see below) calls server.Stop, or
		// the listener itself fails — either way, nothing left for this
		// goroutine to do but exit; the caller learns about a failed dial
		// to HubEndpoint itself from the RPC errors that follow, not from
		// here.
		_ = server.Serve(listener)
	}()

	return &Relay{listener: listener, server: server, upstream: upstream}, nil
}

// Close stops accepting new local connections, waits for in-flight ones to
// finish, and tears down the connection to HubEndpoint.
func (r *Relay) Close() {
	r.server.GracefulStop()
	_ = r.upstream.Close()
}
