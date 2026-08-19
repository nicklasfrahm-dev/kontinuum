package etcdproxy

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// jwtCredentials attaches "Authorization: Bearer <jwt>" to every outbound
// RPC over the connection it's installed on, minting a fresh, short-lived
// JWT (see SignToken) on every single call — grpc-go's PerRPCCredentials
// hook is already invoked per-RPC, and ed25519 signing is cheap enough
// that there's no need to cache and refresh a token instead.
type jwtCredentials struct {
	zone                     string
	key                      ed25519.PrivateKey
	requireTransportSecurity bool
}

func (c jwtCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, err := SignToken(c.zone, c.key)
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
	// Zone and PrivateKey identify this zone to the hub (see SignToken) —
	// PrivateKey is this zone's own ed25519 identity key (see
	// GenerateIdentity and pkg/domain/zone's ensureEtcdIdentity), loaded
	// from its own mounted kubernetes.io/tls identity Secret.
	Zone       string
	PrivateKey ed25519.PrivateKey
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

	upstream, err := grpc.NewClient(cfg.HubEndpoint,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithPerRPCCredentials(jwtCredentials{
			zone:                     cfg.Zone,
			key:                      cfg.PrivateKey,
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
