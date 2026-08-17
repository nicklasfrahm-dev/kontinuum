package etcdproxy

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RegisterHub wires the hub side of this package onto server: it dials
// kineTarget (the hub's own local Kine gRPC endpoint — the first of
// libkapi.Ctx.StorageEndpoints()'s own values, which this expects to
// already be running), builds a Verifier reading zone auth Secrets from
// namespace via hubClient, and registers an authenticated Proxy in front
// of it. Meant to be called once, from a libkapi.ServerFactory (see
// pkg/cli/serve.go's customHandlers) — server is the same shared
// *grpc.Server every other ServerFactory-registered gRPC service is
// registered on (see libkapi.Ctx.GRPCServer), multiplexed onto the same
// port as everything else this process serves.
//
// Returns a cleanup func closing the dialed connection to kineTarget —
// callers aren't required to call it (the connection is otherwise
// harmless to leak for a process's whole lifetime), but tests that build
// many Reconcile-scoped proxies in a row should.
func RegisterHub(
	server *grpc.Server, kineTarget string, hubClient client.Client, namespace string,
) (func(), error) {
	verifier, err := NewVerifier(hubClient, namespace)
	if err != nil {
		return nil, err
	}

	// kineTarget is Kine's own local unix socket (see
	// libkapi/storage.startKine) — a loopback, single-process-local
	// endpoint, never a networked one, so a plaintext connection is
	// correct here; the TLS/authentication boundary this package exists
	// to add is the *inbound* side (see Proxy.authenticate), not this
	// outbound hop to Kine itself.
	conn, err := grpc.NewClient(kineTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial local storage endpoint %q: %w", kineTarget, err)
	}

	proxy := NewProxy(conn, verifier)
	proxy.Register(server)

	return func() { _ = conn.Close() }, nil
}
