package registry_test

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/kommodity-io/kommodity/pkg/libkapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

const (
	e2eShutdownTimeout   = 10 * time.Second
	e2eHealthzTimeout    = 10 * time.Second
	e2eHealthzInterval   = 50 * time.Millisecond
	e2eHeartbeatInterval = 10 * time.Millisecond
	e2eEventuallyTimeout = 5 * time.Second

	e2eLegacyInstanceName = "legacy-instance"
	e2eRegion             = "eu"
	e2eZone               = "eu-1a"
)

// TestConversionWebhookBridgesLegacyRegistration is the regression test for
// the exact scenario that broke a previous release: an existing Kontinuum
// object registered under the removed v1alpha1 shape (Role in spec) has to
// keep working once a server running the current (Role-in-status,
// conversion-webhook) code takes over — readable correctly through both API
// versions, and safely picked up by a live Heartbeat.
//
// Unlike the rest of this package's tests, this runs against a real
// libkapi.Server — a real apiserver performing a real TLS conversion webhook
// handshake — rather than a fake client: a fake client never invokes CRD
// conversion at all, which is exactly the machinery this test exists to
// exercise. t.Parallel() is safe today since no other test in this suite
// touches registry.ConversionWebhookPort, the fixed port the conversion
// webhook binds — but if a second real-server test is ever added here, only
// one of them can run t.Parallel() against that same port without a
// collision.
func TestConversionWebhookBridgesLegacyRegistration(t *testing.T) {
	t.Parallel()

	if raceEnabled {
		// Skipped only under -race: booting a real backing store trips a
		// pre-existing data race inside the vendored github.com/k3s-io/kine
		// dependency itself — SQLLog.compactor() and SQLLog.poll() (sql.go
		// lines 125/471) read/write a shared revision field with no
		// synchronization. Nothing else in this suite starts a real
		// backing store long enough to hit it; every other test uses a
		// fake client. Not a kontinuum bug — tracked as a known issue in
		// kine, not something fixable from here.
		t.Skip("triggers a pre-existing, unrelated data race in github.com/k3s-io/kine under -race")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	kontinuums, logger := startTestServer(ctx, t)

	legacy := createLegacyRegistration(ctx, t, kontinuums)
	runHeartbeatAndAssertBridge(ctx, t, kontinuums, logger, legacy)
}

// createLegacyRegistration creates a Kontinuum directly through the
// v1alpha1 endpoint — simulating what the previous, v1alpha1-only release
// would have left behind: an object with role in spec, no status.role at
// all — then asserts the conversion webhook already bridges it correctly
// when read back as v1alpha2.
func createLegacyRegistration(ctx context.Context, t *testing.T, kontinuums ctrlclient.Client) *v1alpha1.Kontinuum {
	t.Helper()

	legacy := &v1alpha1.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: e2eLegacyInstanceName},
		Spec:       v1alpha1.KontinuumSpec{Role: v1alpha1.RoleWorker, Region: e2eRegion, Zone: e2eZone},
	}

	// kontinuums' RESTMapper resolves lazily, with no retry of its own, on
	// this very first request — unlike EnsureCRD's own mapper (already
	// confirmed both versions discoverable before /healthz reported ready),
	// this one can still race the apiserver's discovery-document
	// propagation. Retried here the same way any real client reasonably
	// would on a transient "no matches for kind".
	require.Eventually(t, func() bool {
		return kontinuums.Create(ctx, legacy) == nil
	}, e2eEventuallyTimeout, e2eHealthzInterval, "failed to create the legacy v1alpha1 registration")

	var viaV2 v1alpha2.Kontinuum

	require.NoError(t, kontinuums.Get(ctx, ctrlclient.ObjectKeyFromObject(legacy), &viaV2))
	assert.Equal(t, e2eRegion, viaV2.Spec.Region)
	assert.Equal(t, e2eZone, viaV2.Spec.Zone)
	assert.Equal(t, v1alpha1.RoleWorker, viaV2.Status.Role,
		"the conversion webhook must move the legacy spec.role into status.role")

	return legacy
}

// runHeartbeatAndAssertBridge starts a real Heartbeat pointed at legacy's
// name — simulating the new release's own process taking over an existing
// registration — and confirms: it successfully picks up and refreshes the
// legacy object; the object still reads correctly through both API
// versions afterward; and it's deregistered (deleted) on shutdown, same as
// any other Heartbeat-owned Kontinuum.
func runHeartbeatAndAssertBridge(
	ctx context.Context, t *testing.T, kontinuums ctrlclient.Client, logger *slog.Logger, legacy *v1alpha1.Kontinuum,
) {
	t.Helper()

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeat := &registry.Heartbeat{
		Client:   kontinuums,
		Name:     e2eLegacyInstanceName,
		Role:     v1alpha2.RoleWorker,
		Spec:     v1alpha2.KontinuumSpec{Region: e2eRegion, Zone: e2eZone},
		Interval: e2eHeartbeatInterval,
		Logger:   logger,
	}

	heartbeatDone := make(chan error, 1)

	go func() { heartbeatDone <- heartbeat.Start(heartbeatCtx) }()

	// The new release's own heartbeat taking over the legacy registration:
	// once it writes status.role via v1alpha2 (the storage version), that
	// write needs no conversion at all and lands directly.
	require.Eventually(t, func() bool {
		var current v1alpha2.Kontinuum

		err := kontinuums.Get(ctx, ctrlclient.ObjectKeyFromObject(legacy), &current)

		return err == nil && !current.Status.LastHeartbeatTime.IsZero()
	}, e2eEventuallyTimeout, e2eHealthzInterval, "the new release's heartbeat never picked up the legacy registration")

	// Bidirectional proof: fetching the now-heartbeating object back
	// through the legacy v1alpha1 view must still show the same role,
	// derived from status.role via ConvertFrom.
	var viaV1 v1alpha1.Kontinuum

	require.NoError(t, kontinuums.Get(ctx, ctrlclient.ObjectKeyFromObject(legacy), &viaV1))
	assert.Equal(t, v1alpha2.RoleWorker, viaV1.Spec.Role)
	assert.Equal(t, e2eRegion, viaV1.Spec.Region)
	assert.Equal(t, e2eZone, viaV1.Spec.Zone)

	cancelHeartbeat()

	select {
	case err := <-heartbeatDone:
		require.NoError(t, err)
	case <-time.After(e2eEventuallyTimeout):
		t.Fatal("Heartbeat.Start did not return after its context was canceled")
	}

	err := kontinuums.Get(ctx, ctrlclient.ObjectKeyFromObject(legacy), &v1alpha2.Kontinuum{})
	assert.True(t, apierrors.IsNotFound(err), "heartbeat deregisters on shutdown, so the object should be gone")
}

// startTestServer builds and starts a real libkapi.Server wired exactly
// like pkg/cli/serve.go's buildServer/registryOptions — the same scheme,
// CRD post-start hook, conversion webhook certificate and server, and
// Controller — and returns a client scoped to it (recognizing both
// api/v1alpha1 and api/v1alpha2) once the server reports healthy. Reaching
// /healthz also proves EnsureCRD already ran to completion: libkapi's own
// post-start-hook healthz check only reports healthy once every registered
// hook, including ours, has finished (see EnsureCRD's own doc).
func startTestServer(ctx context.Context, t *testing.T) (ctrlclient.Client, *slog.Logger) {
	t.Helper()

	logger := slog.Default()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	caBundle, err := registry.EnsureConversionWebhookCert()
	require.NoError(t, err)

	controller := registry.NewController(registry.Config{
		Role:   v1alpha2.RoleControlPlane,
		Logger: logger,
	})

	ensureCRD := func(ctx context.Context, loopbackConfig *restclient.Config) error {
		return registry.EnsureCRD(ctx, loopbackConfig, caBundle, logger)
	}

	addr := freeAddr(ctx, t)
	dbPath := filepath.Join(t.TempDir(), "registry-e2e.db")

	server, err := libkapi.New(ctx,
		libkapi.WithAddr(addr),
		libkapi.WithStorage("sqlite://"+dbPath),
		libkapi.WithLogger(logger),
		libkapi.WithScheme(scheme),
		libkapi.WithPostStartHook(ensureCRD),
		libkapi.WithController(controller),
		libkapi.WithWebhookServer(libkapi.WebhookConfig{Port: registry.ConversionWebhookPort}),
	)
	require.NoError(t, err)

	go func() { _ = server.ListenAndServe(ctx) }()

	t.Cleanup(func() {
		// Deliberately not derived from ctx via context.WithTimeout alone —
		// context.WithoutCancel matches heartbeat.go's deregister, which
		// bounds cleanup the same way: ctx may already be canceled by the
		// time this runs, but shutdown should still get its own full
		// e2eShutdownTimeout rather than being aborted immediately.
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), e2eShutdownTimeout)
		defer shutdownCancel()

		_ = server.Shutdown(shutdownCtx)
	})

	baseURL := "http://" + addr

	waitForHealthz(ctx, t, baseURL+"/healthz")

	kontinuums, err := ctrlclient.New(&restclient.Config{Host: baseURL}, ctrlclient.Options{Scheme: scheme})
	require.NoError(t, err)

	return kontinuums, logger
}

// freeAddr picks an available loopback address by binding to port 0 and
// immediately releasing it — the same technique libkapi's own tests use,
// unavailable to us directly since it lives in one of libkapi's _test.go
// files, which Go never exports across module boundaries.
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
// expires.
func waitForHealthz(ctx context.Context, t *testing.T, url string) {
	t.Helper()

	client := &http.Client{Timeout: e2eHealthzInterval}

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false
		}

		resp, err := client.Do(req)
		if err != nil {
			return false
		}

		defer func() { _ = resp.Body.Close() }()

		return resp.StatusCode == http.StatusOK
	}, e2eHealthzTimeout, e2eHealthzInterval, "server never became healthy")
}
