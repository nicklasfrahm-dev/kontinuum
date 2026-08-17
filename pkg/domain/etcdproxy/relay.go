package etcdproxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// bearerCredentials attaches "Authorization: Bearer <token>" to every
// outbound RPC over the connection it's installed on — grpc-go's
// PerRPCCredentials hook, the client-side counterpart of
// Proxy.authenticate's own incoming-metadata check.
type bearerCredentials struct {
	token                    string
	requireTransportSecurity bool
}

func (c bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{AuthorizationMetadataKey: BearerPrefix + c.token}, nil
}

func (c bearerCredentials) RequireTransportSecurity() bool {
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
// Relay receives there is forwarded straight through to the hub, with the
// zone's own bearer credential (see EncodeToken) attached on every
// outbound RPC. Relay's own local listener needs no authentication of its
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
	// Zone and Key identify this zone to the hub (see EncodeToken) — Key
	// is one of the zone's own two currently-valid bearer keys (see
	// pkg/domain/zone's reconcileAuthKeys).
	Zone string
	Key  string
	// Insecure skips TLS on the connection to HubEndpoint — for local
	// development only; a real deployment's HubEndpoint is expected to
	// terminate TLS, the same as every other kind of traffic the hub
	// serves.
	Insecure bool
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

	transportCreds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if cfg.Insecure {
		transportCreds = insecure.NewCredentials()
	}

	upstream, err := grpc.NewClient(cfg.HubEndpoint,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithPerRPCCredentials(bearerCredentials{
			token:                    EncodeToken(cfg.Zone, cfg.Key),
			requireTransportSecurity: !cfg.Insecure,
		}),
	)
	if err != nil {
		_ = listener.Close()

		return nil, fmt.Errorf("failed to dial hub etcd proxy %q: %w", cfg.HubEndpoint, err)
	}

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
