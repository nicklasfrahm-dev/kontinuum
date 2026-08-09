package instance_test

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/kommodity-io/kommodity/pkg/libkapi"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

const (
	e2eShutdownTimeout   = 10 * time.Second
	e2eHealthzTimeout    = 10 * time.Second
	e2eHealthzInterval   = 50 * time.Millisecond
	e2eEventuallyTimeout = 5 * time.Second
)

// TestZoneJoinCRDsApplyAndRoundTrip is the testable milestone issue #27
// itself names: "CRDs apply cleanly and round-trip empty objects." Unlike
// pkg/domain/registry's e2e tests, none of these four kinds have a
// conversion webhook or a controller registered on them yet (Instance's
// discovery reconciler is covered separately by controller_test.go/
// talos_wire_test.go, against a fake client and a fake gRPC server,
// respectively) — this test only needs a real apiserver to exercise CRD
// establishment and schema/CEL validation, which a fake clientset never
// runs at all.
func TestZoneJoinCRDsApplyAndRoundTrip(t *testing.T) {
	t.Parallel()

	if raceEnabled {
		// Mirrors pkg/domain/registry's identical e2e skips: booting a real
		// backing store trips a pre-existing, unrelated data race inside
		// the vendored github.com/k3s-io/kine.
		t.Skip("triggers a pre-existing, unrelated data race in github.com/k3s-io/kine under -race")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client := startTestServer(ctx, t)

	// Warms up client's RESTMapper before the Create calls below — a
	// brand-new client's RESTMapper resolves lazily, with no retry of its
	// own, and can race the apiserver's discovery-document propagation on
	// its very first request even after EnsureCRDs' own (separate) loopback
	// client has already confirmed every kind discoverable — see
	// pkg/domain/registry/region_zone_validation_e2e_test.go's identical
	// warm-up for why this isn't optional.
	for _, list := range []ctrlclient.ObjectList{
		&v1alpha2.ZoneList{}, &v1alpha2.InstanceList{}, &v1alpha2.InstancePoolList{}, &v1alpha2.TalosClusterList{},
	} {
		require.Eventually(t, func() bool {
			return client.List(ctx, list) == nil
		}, e2eEventuallyTimeout, e2eHealthzInterval, "client never became ready")
	}

	zone := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a", Namespace: v1alpha2.DefaultSecretNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: "eu-1a", Domain: "example.com"},
	}
	require.NoError(t, client.Create(ctx, zone))

	inst := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", Namespace: v1alpha2.DefaultSecretNamespace},
	}
	require.NoError(t, client.Create(ctx, inst))

	pool := &v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-pool-a", Namespace: v1alpha2.DefaultSecretNamespace},
	}
	require.NoError(t, client.Create(ctx, pool))

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a", Namespace: v1alpha2.DefaultSecretNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: "cp-pool"},
			},
		},
	}
	require.NoError(t, client.Create(ctx, cluster))

	require.NoError(t, client.Get(ctx, ctrlclient.ObjectKeyFromObject(zone), &v1alpha2.Zone{}))
	require.NoError(t, client.Get(ctx, ctrlclient.ObjectKeyFromObject(inst), &v1alpha2.Instance{}))
	require.NoError(t, client.Get(ctx, ctrlclient.ObjectKeyFromObject(pool), &v1alpha2.InstancePool{}))
	require.NoError(t, client.Get(ctx, ctrlclient.ObjectKeyFromObject(cluster), &v1alpha2.TalosCluster{}))
}

// startTestServer boots a real libkapi.Server (sqlite-backed, per
// pkg/domain/registry/conversion_e2e_test.go's own pattern) with the four
// zone-join CRDs ensured, and returns a real controller-runtime client
// against it. No Controller is registered — see this test's own doc for
// why that's unnecessary here.
func startTestServer(ctx context.Context, t *testing.T) ctrlclient.Client {
	t.Helper()

	logger := slog.Default()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	ensureCRDs := func(ctx context.Context, loopbackConfig *restclient.Config) error {
		return instance.EnsureCRDs(ctx, loopbackConfig, logger)
	}

	addr := freeAddr(ctx, t)
	dbPath := filepath.Join(t.TempDir(), "instance-e2e.db")

	server, err := libkapi.New(ctx,
		libkapi.WithAddr(addr),
		libkapi.WithStorage("sqlite://"+dbPath),
		libkapi.WithLogger(logger),
		libkapi.WithScheme(scheme),
		libkapi.WithPostStartHook(ensureCRDs),
	)
	require.NoError(t, err)

	go func() { _ = server.ListenAndServe(ctx) }()

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), e2eShutdownTimeout)
		defer shutdownCancel()

		_ = server.Shutdown(shutdownCtx)
	})

	baseURL := "http://" + addr

	waitForHealthz(ctx, t, baseURL+"/healthz")

	client, err := ctrlclient.New(&restclient.Config{Host: baseURL}, ctrlclient.Options{Scheme: scheme})
	require.NoError(t, err)

	return client
}

// freeAddr picks an available loopback address by binding to port 0 and
// immediately releasing it — mirrors
// pkg/domain/registry/conversion_e2e_test.go's identical helper.
func freeAddr(ctx context.Context, t *testing.T) string {
	t.Helper()

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	require.NoError(t, listener.Close())

	return addr
}

// waitForHealthz polls url until it returns 200 OK or ctx/the timeout
// expires — mirrors pkg/domain/registry/conversion_e2e_test.go's identical
// helper.
func waitForHealthz(ctx context.Context, t *testing.T, url string) {
	t.Helper()

	httpClient := &http.Client{Timeout: e2eHealthzInterval}

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return false
		}

		defer func() { _ = resp.Body.Close() }()

		return resp.StatusCode == http.StatusOK
	}, e2eHealthzTimeout, e2eHealthzInterval, "server never became healthy")
}
