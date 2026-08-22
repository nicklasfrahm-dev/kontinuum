package etcdproxy_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

// fakeKV is a minimal etcdserverpb.KVServer standing in for a hub's own
// local Kine instance — enough to prove a call issued through
// Relay -> hub Proxy -> this actually reaches the "real" backend and its
// response comes back unmodified, without needing to stand up real Kine.
type fakeKV struct {
	etcdserverpb.UnimplementedKVServer

	store map[string][]byte
}

func (f *fakeKV) Put(_ context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	f.store[string(req.GetKey())] = req.GetValue()

	return &etcdserverpb.PutResponse{}, nil
}

func (f *fakeKV) Range(_ context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	value, ok := f.store[string(req.GetKey())]
	if !ok {
		return &etcdserverpb.RangeResponse{}, nil
	}

	return &etcdserverpb.RangeResponse{
		Kvs: []*mvccpb.KeyValue{{Key: req.GetKey(), Value: value}},
	}, nil
}

// startFakeKine starts fakeKV on a real Unix socket, standing in for
// libkapi's own local Kine gRPC endpoint (see libkapi/storage's own
// startKine) — RegisterHub expects exactly this shape (a "unix://<path>"
// target).
func startFakeKine(t *testing.T) (string, map[string][]byte) {
	t.Helper()

	socketPath := filepath.Join(t.TempDir(), "kine.sock")

	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socketPath)
	require.NoError(t, err)

	store := map[string][]byte{}
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, &fakeKV{store: store})

	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	return "unix://" + socketPath, store
}

func newProxyTestHubClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// admittedPublicSecret builds a hub-side identity Secret and simulates the
// real apiserver's own StringData->Data admission conversion, which the
// fake client doesn't replicate — see TestBuildAndParsePublicSecretRoundTrip's
// identical note.
func admittedPublicSecret(zone string, pair etcdproxy.IdentityPair) *corev1.Secret {
	secret := etcdproxy.BuildPublicSecret(zone, "kontinuum-system", pair)
	secret.Data = map[string][]byte{}

	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	secret.StringData = nil

	return secret
}

// startTestHub starts a real hub-side grpc.Server (playing the role of
// libkapi.Ctx.GRPCServer's own shared server) with RegisterHub's proxy
// registered on it, forwarding to a fresh fake Kine instance.
func startTestHub(t *testing.T, hubClient client.Client) (string, map[string][]byte) {
	t.Helper()

	kineTarget, store := startFakeKine(t)

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	cleanup, err := etcdproxy.RegisterHub(server, kineTarget, hubClient, "kontinuum-system")
	require.NoError(t, err)
	t.Cleanup(cleanup)

	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	return listener.Addr().String(), store
}

// TestRelayForwardsAuthenticatedCallsToHub covers the full round trip a
// real zone would take: its own apiserver dials Relay's local socket
// (exactly as libkapi's "unix://" storage scheme would), Relay forwards
// authenticated over the wire to the hub's own Proxy, which forwards again
// to the hub's own (here, fake) Kine — and the response makes it all the
// way back unmodified.
func TestRelayForwardsAuthenticatedCallsToHub(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)
	previous := generateTestIdentity(t)
	secret := admittedPublicSecret("eu-eu-1a", etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: previous.certPEM, IssuedAt: now},
	})

	hubAddr, store := startTestHub(t, newProxyTestHubClient(t, secret))

	socketPath := filepath.Join(t.TempDir(), "relay.sock")

	currentKey, err := etcdproxy.LoadPrivateKey(current.keyPEM)
	require.NoError(t, err)

	relay, err := etcdproxy.StartRelay(etcdproxy.RelayConfig{
		SocketPath:  socketPath,
		HubEndpoint: hubAddr,
		Zone:        "eu-eu-1a",
		Keys:        etcdproxy.StaticKey(currentKey),
		Insecure:    true,
	})
	require.NoError(t, err)
	t.Cleanup(relay.Close)

	localConn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localConn.Close() })

	kvClient := etcdserverpb.NewKVClient(localConn)

	_, err = kvClient.Put(t.Context(), &etcdserverpb.PutRequest{Key: []byte("/registry/foo"), Value: []byte("bar")})
	require.NoError(t, err)
	assert.Equal(t, []byte("bar"), store["/registry/foo"])

	resp, err := kvClient.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/registry/foo")})
	require.NoError(t, err)
	require.Len(t, resp.GetKvs(), 1)
	assert.Equal(t, []byte("bar"), resp.GetKvs()[0].Value)
}

// TestRelayAcceptsPreviousIdentityDuringOverlap covers Relay presenting
// the zone's *previous* identity (the shape it would still hold, briefly,
// right after a hub-side rotation but before its own watch on the
// downstream identity Secret observes the new one — see
// pkg/domain/zone's ensureEtcdIdentity) — the hub must still accept it.
func TestRelayAcceptsPreviousIdentityDuringOverlap(t *testing.T) {
	t.Parallel()

	now := time.Now()
	newCurrent := generateTestIdentity(t)
	stillGoodPrevious := generateTestIdentity(t)
	secret := admittedPublicSecret("eu-eu-1a", etcdproxy.IdentityPair{
		Current: etcdproxy.Identity{CertPEM: newCurrent.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{
			CertPEM: stillGoodPrevious.certPEM, IssuedAt: now.Add(-etcdproxy.IdentityRotationInterval),
		},
	})

	hubAddr, _ := startTestHub(t, newProxyTestHubClient(t, secret))

	socketPath := filepath.Join(t.TempDir(), "relay.sock")

	previousKey, err := etcdproxy.LoadPrivateKey(stillGoodPrevious.keyPEM)
	require.NoError(t, err)

	relay, err := etcdproxy.StartRelay(etcdproxy.RelayConfig{
		SocketPath:  socketPath,
		HubEndpoint: hubAddr,
		Zone:        "eu-eu-1a",
		Keys:        etcdproxy.StaticKey(previousKey),
		Insecure:    true,
	})
	require.NoError(t, err)
	t.Cleanup(relay.Close)

	localConn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localConn.Close() })

	kvClient := etcdserverpb.NewKVClient(localConn)

	_, err = kvClient.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})
	require.NoError(t, err)
}

// TestHubRejectsWrongBearerKey covers a call reaching the hub's own Proxy
// directly (bypassing Relay) with a credential signed by a key that isn't
// either of the zone's two currently-valid identities.
func TestHubRejectsWrongBearerKey(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)
	previous := generateTestIdentity(t)
	secret := admittedPublicSecret("eu-eu-1a", etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: previous.certPEM, IssuedAt: now},
	})

	hubAddr, _ := startTestHub(t, newProxyTestHubClient(t, secret))

	conn, err := grpc.NewClient(hubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	kvClient := etcdserverpb.NewKVClient(conn)

	unrelated := generateTestIdentity(t)
	bearer := "Bearer " + signTestToken(t, unrelated)
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", bearer)

	_, err = kvClient.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/anything")})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

// TestHubRejectsMissingBearerToken covers a call with no Authorization
// metadata at all.
func TestHubRejectsMissingBearerToken(t *testing.T) {
	t.Parallel()

	hubAddr, _ := startTestHub(t, newProxyTestHubClient(t))

	conn, err := grpc.NewClient(hubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	kvClient := etcdserverpb.NewKVClient(conn)

	_, err = kvClient.Range(t.Context(), &etcdserverpb.RangeRequest{Key: []byte("/anything")})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
