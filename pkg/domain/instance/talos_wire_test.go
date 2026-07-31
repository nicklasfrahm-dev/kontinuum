package instance_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	cosiv1alpha1 "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	cosiserver "github.com/cosi-project/runtime/pkg/state/protobuf/server"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/nethelpers"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"

	certutil "k8s.io/client-go/util/cert"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

// wireTestPort is the same fixed port real Talos maintenance mode always
// listens on (see instance's own maintenanceModePort) — the discoverer has
// no way to target a different port, since that constant matches Talos's
// own fixed behavior, not a testability gap. Binding this fake server to
// the same literal port is therefore the only way to exercise the real
// dial path end to end.
const wireTestPort = 50000

// fakeMachineServer implements just enough of Talos's maintenance-mode
// MachineService for the wire-compat test below: Version.
type fakeMachineServer struct {
	machineapi.UnimplementedMachineServiceServer

	tag string
}

func (s *fakeMachineServer) Version(
	context.Context, *emptypb.Empty,
) (*machineapi.VersionResponse, error) {
	return &machineapi.VersionResponse{
		Messages: []*machineapi.Version{
			{Version: &machineapi.VersionInfo{Tag: s.tag}},
		},
	}, nil
}

// seedNetworkState populates coreState with one LinkStatus and its matching
// AddressStatus, the same COSI resources a real Talos node's network
// controller would publish — see instance.discoverInterfaces's own doc for
// why these two resource types are what gets read.
func seedNetworkState(t *testing.T, coreState state.CoreState) {
	t.Helper()

	ctx := context.Background()

	link := network.NewLinkStatus(network.NamespaceName, "eth0")
	link.TypedSpec().HardwareAddr = nethelpers.HardwareAddr{0xde, 0xad, 0xbe, 0xef, 0x00, 0x01}
	require.NoError(t, coreState.Create(ctx, link))

	addr := network.NewAddressStatus(network.NamespaceName, "eth0/192.168.1.10/24")
	addr.TypedSpec().Address = netip.MustParsePrefix("192.168.1.10/24")
	addr.TypedSpec().LinkName = "eth0"
	require.NoError(t, coreState.Create(ctx, addr))
}

// TestTalosDiscovererWireCompat dials a fake Talos maintenance-mode gRPC
// server — MachineService.Version plus a real in-memory COSI state serving
// network.LinkStatus/AddressStatus resources — with the real
// github.com/siderolabs/talos/pkg/machinery/client, the same client
// instance.NewTalosDiscoverer wraps. This proves wire-compatibility with
// actual Talos gRPC contracts, not just compatibility against a hand-rolled
// interface — mirroring pkg/domain/kms/grpc_test.go's own
// round-trip-through-the-real-client-stub shape.
//
//nolint:paralleltest // binds the fixed maintenance-mode port (wireTestPort); a parallel run would race it
func TestTalosDiscovererWireCompat(t *testing.T) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(wireTestPort))

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		t.Skipf("port %d unavailable in this environment: %v", wireTestPort, err)
	}

	certPEM, keyPEM, err := certutil.GenerateSelfSignedCertKey("127.0.0.1", []net.IP{net.ParseIP("127.0.0.1")}, nil)
	require.NoError(t, err)

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	inmemBuilder := inmem.NewStateWithOptions()
	coreState := namespaced.NewState(func(ns resource.Namespace) state.CoreState { return inmemBuilder(ns) })
	seedNetworkState(t, coreState)

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))

	machineapi.RegisterMachineServiceServer(grpcServer, &fakeMachineServer{tag: "v1.9.0"})
	cosiv1alpha1.RegisterStateServer(grpcServer, cosiserver.NewState(coreState))

	serveErr := make(chan error, 1)

	go func() { serveErr <- grpcServer.Serve(listener) }()

	t.Cleanup(func() {
		grpcServer.Stop()
		<-serveErr
	})

	discoverer := instance.NewTalosDiscoverer()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	talosVersion, interfaces, err := discoverer.Discover(ctx, "127.0.0.1")
	require.NoError(t, err)
	assert.Equal(t, "v1.9.0", talosVersion)
	require.Len(t, interfaces, 1)
	assert.Equal(t, "eth0", interfaces[0].Name)
	assert.Equal(t, "de:ad:be:ef:00:01", interfaces[0].MACAddress)
	assert.Equal(t, []string{"192.168.1.10/24"}, interfaces[0].Addresses)
}
