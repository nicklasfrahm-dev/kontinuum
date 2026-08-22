package etcdproxy

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// zonePoolSize bounds how many real connections RegisterHub's own Pool
// dials to Kine, regardless of how many zones actually connect. A single
// shared connection (the previous design) is fine for a handful of zones,
// but scaling to hundreds or thousands turns it into both a throughput
// ceiling and a single point of failure for every zone at once — a dropped
// connection or reconnect stalls all of them simultaneously. One connection
// per zone would avoid that but reintroduce the per-connection ping-strike
// problem this package exists to avoid in the first place (see Pool's own
// doc). zonePoolSize is a fixed, conservative starting point, not derived
// from a hard formula — tune based on real fleet size once deployed.
const zonePoolSize = 8

// RegisterHub wires the hub side of this package onto server: it dials
// kineTarget (the hub's own local Kine gRPC endpoint — the first of
// libkapi.Ctx.StorageEndpoints()'s own values, which this expects to
// already be running) zonePoolSize times, builds a Verifier reading zone
// auth Secrets from namespace via hubClient, and registers an
// authenticated Pool in front of those connections. Meant to be called
// once, from a libkapi.ServerFactory (see pkg/cli/serve.go's
// customHandlers) — server is the same shared *grpc.Server every other
// ServerFactory-registered gRPC service is registered on (see
// libkapi.Ctx.GRPCServer), multiplexed onto the same port as everything
// else this process serves.
//
// Returns a cleanup func closing every dialed connection to kineTarget —
// callers aren't required to call it (the connections are otherwise
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
	// endpoint, never a networked one, so plaintext connections are
	// correct here; the TLS/authentication boundary this package exists
	// to add is the *inbound* side (see Proxy.authenticate), not this
	// outbound hop to Kine itself.
	pool, cleanup, err := DialPool(
		kineTarget, zonePoolSize, verifier, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial local storage endpoint %q: %w", kineTarget, err)
	}

	pool.Register(server)

	return cleanup, nil
}
