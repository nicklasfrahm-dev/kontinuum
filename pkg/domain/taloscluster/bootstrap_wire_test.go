package taloscluster_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/constants"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

// bootstrapWireTestPort is the same fixed apid port every real,
// post-maintenance-mode Talos node listens on (constants.ApidPort) — the
// only port ClusterBootstrapper's own "real identity" methods (Version,
// CPUTopology, ...) ever dial, via talosBootstrapper.dial's own
// talosclient.WithEndpoints(addr), same as
// pkg/domain/instance/talos_wire_test.go's identical fixed-port
// constraint for maintenance mode's own ApidPort-numbered port.
const bootstrapWireTestPort = constants.ApidPort

// testPKI is a self-signed CA plus a server and client leaf certificate
// pair issued from it — everything talosBootstrapper's own "real identity"
// dial path (unlike ApplyConfiguration's maintenance-mode
// InsecureSkipVerify dial, which pkg/domain/instance's own wire-compat
// test already covers) needs: mutual TLS, the same trust relationship a
// real cluster's own apid/talosconfig share.
type testPKI struct {
	caPEM         []byte
	serverCertPEM []byte
	serverKeyPEM  []byte
	clientCertPEM []byte
	clientKeyPEM  []byte
}

// generateTestPKI builds testPKI with plain crypto/x509 — not Talos's own
// internal CA-issuance flow, which involves trustd/CSR machinery this test
// has no need to replicate: talosclient.BuildTLSConfig (what
// talosBootstrapper.dial's talosclient.WithConfig ultimately drives) only
// cares that Context.CA/Crt/Key are valid PEM, not how they were minted.
func generateTestPKI(t *testing.T) testPKI {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kontinuum-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	issueLeaf := func(serial int64, commonName string, ips []net.IP, extKeyUsage x509.ExtKeyUsage) ([]byte, []byte) {
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, keyErr)

		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: commonName},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{extKeyUsage},
			IPAddresses:  ips,
		}

		der, certErr := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		require.NoError(t, certErr)

		keyBytes, keyMarshalErr := x509.MarshalECPrivateKey(key)
		require.NoError(t, keyMarshalErr)

		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

		return certPEM, keyPEM
	}

	serverCertPEM, serverKeyPEM := issueLeaf(2, "apid", []net.IP{net.ParseIP("127.0.0.1")}, x509.ExtKeyUsageServerAuth)
	clientCertPEM, clientKeyPEM := issueLeaf(3, "talosconfig", nil, x509.ExtKeyUsageClientAuth)

	return testPKI{
		caPEM: caPEM, serverCertPEM: serverCertPEM, serverKeyPEM: serverKeyPEM,
		clientCertPEM: clientCertPEM, clientKeyPEM: clientKeyPEM,
	}
}

// talosConfig builds the *clientconfig.Config talosBootstrapper.dial
// expects — the same shape a real talosconfig file has, with pki's client
// leaf certificate as the admin identity and pki's CA as what it trusts
// endpoint's own server certificate against.
func (pki testPKI) talosConfig(endpoint string) *clientconfig.Config {
	return &clientconfig.Config{
		Context: "test",
		Contexts: map[string]*clientconfig.Context{
			"test": {
				Endpoints: []string{endpoint},
				CA:        base64.StdEncoding.EncodeToString(pki.caPEM),
				Crt:       base64.StdEncoding.EncodeToString(pki.clientCertPEM),
				Key:       base64.StdEncoding.EncodeToString(pki.clientKeyPEM),
			},
		},
	}
}

// serverTLSConfig builds the fake apid server's own TLS config: pki's
// server leaf certificate, requiring and verifying a client certificate
// against pki's own CA — the same mutual-TLS posture a real apid holds.
func (pki testPKI) serverTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	cert, err := tls.X509KeyPair(pki.serverCertPEM, pki.serverKeyPEM)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(pki.caPEM))

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}
}

// fakeBootstrapMachineServer implements just enough of Talos's real
// (post-maintenance-mode) MachineService for the wire-compat test below:
// Version, Read, and Kubeconfig — the RPCs talosBootstrapper.Version/
// CPUTopology/Kubeconfig actually call.
type fakeBootstrapMachineServer struct {
	machineapi.UnimplementedMachineServiceServer

	tag, arch      string
	sysfs          map[string]string
	kubeconfigYAML string
}

func (s *fakeBootstrapMachineServer) Version(
	context.Context, *emptypb.Empty,
) (*machineapi.VersionResponse, error) {
	return &machineapi.VersionResponse{
		Messages: []*machineapi.Version{
			{Version: &machineapi.VersionInfo{Tag: s.tag, Arch: s.arch}},
		},
	}, nil
}

func (s *fakeBootstrapMachineServer) Read(
	req *machineapi.ReadRequest, stream machineapi.MachineService_ReadServer,
) error {
	content, ok := s.sysfs[req.GetPath()]
	if !ok {
		return status.Errorf(codes.NotFound, "no such file %s", req.GetPath())
	}

	err := stream.Send(&common.Data{Bytes: []byte(content)})
	if err != nil {
		return fmt.Errorf("failed to send read response: %w", err)
	}

	return nil
}

// Kubeconfig implements MachineService's own Kubeconfig RPC — Client.
// Kubeconfig (what talosBootstrapper.Kubeconfig calls) unwraps a
// single-file tar.gz stream, the same archive format a real apid sends
// back, into the raw kubeconfig bytes.
func (s *fakeBootstrapMachineServer) Kubeconfig(
	_ *emptypb.Empty, stream machineapi.MachineService_KubeconfigServer,
) error {
	var buf bytes.Buffer

	gzWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzWriter)

	kubeconfigBytes := []byte(s.kubeconfigYAML)

	header := &tar.Header{Name: "kubeconfig", Size: int64(len(kubeconfigBytes)), Mode: 0o600}

	err := tarWriter.WriteHeader(header)
	if err != nil {
		return fmt.Errorf("failed to write tar header: %w", err)
	}

	_, err = tarWriter.Write(kubeconfigBytes)
	if err != nil {
		return fmt.Errorf("failed to write tar content: %w", err)
	}

	err = tarWriter.Close()
	if err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	err = gzWriter.Close()
	if err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	err = stream.Send(&common.Data{Bytes: buf.Bytes()})
	if err != nil {
		return fmt.Errorf("failed to send kubeconfig response: %w", err)
	}

	return nil
}

// testKubeconfigYAML is a placeholder kubeconfig's worth of bytes — its
// contents don't matter to this test, only that they round-trip intact
// through the real tar.gz unwrapping talosBootstrapper.Kubeconfig relies
// on Client.Kubeconfig to do.
const testKubeconfigYAML = "apiVersion: v1\nkind: Config\n"

// TestTalosBootstrapperWireCompat dials a fake, real-identity (mutual-TLS,
// not maintenance-mode-insecure) Talos gRPC server with the real
// github.com/siderolabs/talos/pkg/machinery/client, the same client
// talosBootstrapper wraps — proving wire-compatibility with actual Talos
// gRPC contracts for the RPCs behind Version, CPUTopology, and Kubeconfig,
// mirroring pkg/domain/instance/talos_wire_test.go's identical real-client,
// real-server, fake-only-at-the-network-boundary shape for the
// maintenance-mode Discoverer.
//
//nolint:paralleltest // binds the fixed apid port (bootstrapWireTestPort); a parallel run would race it
func TestTalosBootstrapperWireCompat(t *testing.T) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrapWireTestPort))

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		t.Skipf("port %d unavailable in this environment: %v", bootstrapWireTestPort, err)
	}

	pki := generateTestPKI(t)

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(pki.serverTLSConfig(t))))
	machineapi.RegisterMachineServiceServer(grpcServer, &fakeBootstrapMachineServer{
		tag: testTalosVersionFixture, arch: testTalosArchFixture, kubeconfigYAML: testKubeconfigYAML,
		sysfs: map[string]string{
			"/sys/devices/system/cpu/possible":                      "0-3",
			"/sys/devices/system/cpu/cpu0/topology/core_id":         "0",
			"/sys/devices/system/cpu/cpu1/topology/core_id":         "0",
			"/sys/devices/system/cpu/cpu2/topology/core_id":         "1",
			"/sys/devices/system/cpu/cpu3/topology/core_id":         "1",
			"/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq": "1500000",
		},
	})

	serveErr := make(chan error, 1)

	go func() { serveErr <- grpcServer.Serve(listener) }()

	t.Cleanup(func() {
		grpcServer.Stop()
		<-serveErr
	})

	bootstrapper := taloscluster.NewTalosBootstrapper(slog.Default())
	talosCfg := pki.talosConfig("127.0.0.1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	version, arch, err := bootstrapper.Version(ctx, "127.0.0.1", "test-node", talosCfg)
	require.NoError(t, err)
	assert.Equal(t, testTalosVersionFixture, version)
	assert.Equal(t, testTalosArchFixture, arch)

	coreCount, threadCount, maxSpeedMHz, err := bootstrapper.CPUTopology(ctx, "127.0.0.1", "test-node", talosCfg)
	require.NoError(t, err)
	assert.Equal(t, uint32(4), threadCount, "4 possible cpus (0-3)")
	assert.Equal(t, uint32(2), coreCount, "cpus 0/1 and 2/3 each share a core_id — 2 distinct cores (SMT)")
	assert.Equal(t, uint32(1500), maxSpeedMHz, "1500000 kHz from cpu0's own cpufreq file")

	kubeconfig, err := bootstrapper.Kubeconfig(ctx, "127.0.0.1", talosCfg)
	require.NoError(t, err)
	assert.YAMLEq(t, testKubeconfigYAML, string(kubeconfig))
}
