package etcdproxy

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kommodity-io/kommodity/pkg/libkapi/storage"
)

// localPoolSize is deliberately small: LocalPool sits in front of a single
// process's own Kine instance, serving only that same process's own
// apiserver storage instances (see LocalPool's own doc) — a handful of
// registered resource types, not the up-to-thousands-of-zones fan-in
// RegisterHub's own Pool has to absorb (see zonePoolSize). Just enough real
// connections that no single one becomes a throughput bottleneck for every
// resource type at once, while staying far below the connection count that
// originally tripped Kine's per-connection ping-strike enforcement.
const localPoolSize = 4

// IsPoolableBackend reports whether connStr is a storage connection string
// LocalPool knows how to front — the Kine DSN schemes (postgres://,
// sqlite://, mysql://, nats://) that would otherwise make libkapi spawn its
// own embedded Kine endpoint and hand its socket directly to every REST
// storage instance's own client. An already-running "etcd://" or "unix://"
// endpoint is left alone: pooling only helps when this same process is the
// one about to create the many-connections-to-one-socket topology in the
// first place.
func IsPoolableBackend(connStr string) bool {
	parsed, err := url.Parse(connStr)
	if err != nil {
		return false
	}

	return storage.IsKineDSNScheme(parsed.Scheme)
}

// LocalPool is a hub's (or any single kontinuum-server process's) own
// local stand-in for talking to Kine directly. It resolves backend via
// libkapi/storage.Resolve — the same resolution libkapi's own WithStorage
// would have performed, spawning an embedded Kine endpoint exactly the
// same way — but instead of handing the apiserver's many independent
// RESTOptionsGetter-built clients direct access to Kine's socket (the
// shape that originally tripped Kine's per-connection ping enforcement,
// since k8s.io/apiserver's own etcd3 factory dials a fresh client per
// resource type with no caching or reuse), LocalPool interposes a small
// Pool between them and the real backend. The apiserver still dials once
// per resource type, exactly as before; those dials just land on
// LocalPool's own local socket instead of Kine's, and LocalPool spreads
// them across localPoolSize real connections to Kine rather than letting
// every dial become its own.
//
// No Authenticator is used here, for the same reason Relay's own local
// listener needs none — see Proxy's own doc: the only caller able to reach
// a loopback Unix socket inside this same process is this same process's
// own apiserver.
type LocalPool struct {
	listener  net.Listener
	server    *grpc.Server
	handle    *storage.Handle
	closePool func()
}

// LocalPoolConfig configures StartLocalPool.
type LocalPoolConfig struct {
	// SocketPath is where LocalPool listens — handed to libkapi.WithStorage
	// as a "unix://" DSN in place of Backend (see pkg/cli/serve.go's
	// resolveStorageDSN).
	SocketPath string
	// Backend is the real storage connection string that would otherwise
	// have gone straight to libkapi.WithStorage — see IsPoolableBackend.
	Backend string
}

// StartLocalPool resolves cfg.Backend (spawning an embedded Kine endpoint
// exactly as libkapi's own WithStorage would have), then starts a Pool of
// localPoolSize real connections to it, reachable at cfg.SocketPath. Call
// Close when done — it also tears down the resolved backend, since
// LocalPool now owns that lifecycle instead of libkapi.
func StartLocalPool(ctx context.Context, cfg LocalPoolConfig) (*LocalPool, error) {
	handle, err := storage.Resolve(ctx, cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve storage backend %q: %w", cfg.Backend, err)
	}

	// A stale socket file from a previous, uncleanly-terminated process
	// would otherwise make Listen fail with "address already in use" —
	// harmless to remove, same reasoning as StartRelay's own identical
	// line.
	_ = os.Remove(cfg.SocketPath)

	// context.Background(), not ctx: the listener's own lifetime is tied to
	// LocalPool.Close, not to whatever setup context resolved the backend —
	// same reasoning as StartRelay's own identical call.
	listener, err := new(net.ListenConfig).Listen(context.Background(), "unix", cfg.SocketPath) //nolint:contextcheck
	if err != nil {
		handle.Close()

		return nil, fmt.Errorf("failed to listen on %q: %w", cfg.SocketPath, err)
	}

	// handle.Endpoints()[0] is Kine's own local unix socket — a loopback,
	// single-process-local endpoint, so a plaintext connection is correct
	// here, same reasoning as RegisterHub's own outbound hop to Kine.
	pool, closePool, err := DialPool(
		handle.Endpoints()[0], localPoolSize, nil, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		_ = listener.Close()
		handle.Close()

		return nil, fmt.Errorf("failed to dial local storage pool: %w", err)
	}

	// No keepalive.EnforcementPolicy set on this server, so it falls back
	// to grpc-go's own generous default (5 minutes) rather than Kine's own
	// tightened one — deliberately: this listener's whole job is absorbing
	// the apiserver's many independent, bursty local dials, the exact
	// traffic pattern that tripped Kine's stricter policy in the first
	// place, before it ever reaches Kine as only localPoolSize connections.
	server := grpc.NewServer()
	pool.Register(server)

	go func() {
		_ = server.Serve(listener)
	}()

	return &LocalPool{listener: listener, server: server, handle: handle, closePool: closePool}, nil
}

// Close stops accepting new local connections, waits for in-flight ones to
// finish, closes every pooled connection to the real backend, and tears
// that backend down.
func (l *LocalPool) Close() {
	l.server.GracefulStop()
	l.closePool()
	l.handle.Close()
}
