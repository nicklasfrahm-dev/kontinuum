package identityprovider_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kommodity-io/kommodity/pkg/libkapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/request/bearertoken"
	restclient "k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	e2eShutdownTimeout   = 10 * time.Second
	e2eReadyTimeout      = 30 * time.Second
	e2ePollInterval      = 100 * time.Millisecond
	e2eAdminGroup        = "admins"
	e2eNamespacesPath    = "/api/v1/namespaces"
	e2eHTTPClientTimeout = 5 * time.Second
)

// apiServer is a running libkapi server with this package's authenticator
// and controller wired exactly like pkg/cli's serve command does.
type apiServer struct {
	baseURL string
	client  ctrlclient.Client
}

// TestIdentityProvidersAuthenticateAgainstAPIServer drives the whole chain
// through a real apiserver: CRDs established, providers created through the
// API, hot-swapped into libkapi.WithAuthenticator by the controller, and
// tokens authorized by the real RBAC authorizer — including a tenant's
// provider being unable to mint an admin identity, and a deleted provider
// taking effect without a restart.
func TestIdentityProvidersAuthenticateAgainstAPIServer(t *testing.T) {
	t.Parallel()

	if raceEnabled {
		// Mirrors pkg/domain/instance's identical skip: booting a real
		// backing store trips a pre-existing, unrelated data race inside
		// the vendored github.com/k3s-io/kine.
		t.Skip("triggers a pre-existing, unrelated data race in github.com/k3s-io/kine under -race")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	corporate := newTestIssuer(t)
	tenantIssuer := newTestIssuer(t)
	server := startAPIServer(ctx, t)

	adminToken := corporate.sign(t, firstKeyID, map[string]any{groupsClaim: []string{e2eAdminGroup}})
	tenantToken := tenantIssuer.sign(t, firstKeyID, map[string]any{groupsClaim: []string{e2eAdminGroup}})

	clusterProvider := &v1alpha2.ClusterIdentityProvider{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName},
		Spec:       celSpec(corporate),
	}
	require.NoError(t, server.client.Create(ctx, clusterProvider))

	require.NoError(t, server.client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenantName}}))
	require.NoError(t, server.client.Create(ctx, &v1alpha2.IdentityProvider{
		ObjectMeta: metav1.ObjectMeta{Namespace: tenantName, Name: tenantProviderName},
		Spec:       celSpec(tenantIssuer),
	}))

	server.eventuallyResponds(ctx, t, adminToken, http.StatusOK, "")
	server.eventuallyResponds(ctx, t, tenantToken, http.StatusForbidden, "tenant:acme:"+aliceEmail)

	require.Eventually(t, func() bool {
		var got v1alpha2.ClusterIdentityProvider

		err := server.client.Get(ctx, ctrlclient.ObjectKeyFromObject(clusterProvider), &got)

		return err == nil && meta.IsStatusConditionTrue(got.Status.Conditions, identityprovider.ReadyConditionType) &&
			got.Status.SigningKeys == 1
	}, e2eReadyTimeout, e2ePollInterval, "cluster identity provider never reported Ready")

	require.NoError(t, server.client.Delete(ctx, clusterProvider))

	server.eventuallyResponds(ctx, t, adminToken, http.StatusForbidden, "system:anonymous")
}

// eventuallyResponds polls a namespace listing with token until it returns
// status and, when bodyContains is set, a body containing it.
func (s apiServer) eventuallyResponds(
	ctx context.Context, t *testing.T, token string, status int, bodyContains string,
) {
	t.Helper()

	httpClient := &http.Client{Timeout: e2eHTTPClientTimeout}

	var lastStatus int

	var lastBody string

	matched := assert.Eventually(t, func() bool {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+e2eNamespacesPath, nil)
		if err != nil {
			return false
		}

		request.Header.Set("Authorization", "Bearer "+token)

		response, err := httpClient.Do(request)
		if err != nil {
			return false
		}

		defer func() { _ = response.Body.Close() }()

		body, err := io.ReadAll(response.Body)
		if err != nil {
			return false
		}

		lastStatus, lastBody = response.StatusCode, string(body)

		return lastStatus == status && strings.Contains(lastBody, bodyContains)
	}, e2eReadyTimeout, e2ePollInterval, "want %d containing %q", status, bodyContains)

	require.True(t, matched, "last response: %d %s", lastStatus, lastBody)
}

// startAPIServer boots a sqlite-backed libkapi server with the identity
// provider CRDs, controller, and authenticator, behind the real RBAC
// authorizer, and returns a client acting as the server's own privileged
// loopback identity.
func startAPIServer(ctx context.Context, t *testing.T) apiServer {
	t.Helper()

	logger := slog.Default()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	authn, controller := newE2EController(logger)

	loopback := make(chan *restclient.Config, 1)

	addr := freeAddr(ctx, t)

	server, err := libkapi.New(ctx,
		libkapi.WithAddr(addr),
		libkapi.WithStorage("sqlite://"+filepath.Join(t.TempDir(), "identityprovider-e2e.db")),
		libkapi.WithLogger(logger),
		libkapi.WithScheme(scheme),
		libkapi.WithSystemNamespace(v1alpha2.KontinuumSystemNamespace),
		libkapi.WithAuthenticator(bearertoken.New(authn)),
		libkapi.WithRBACAuthorizer(libkapi.RBACAuthorizerConfig{AdminGroups: e2eAdminGroup}),
		libkapi.WithServerFactory(func(serverCtx *libkapi.Ctx) error {
			loopback <- serverCtx.LoopbackConfig()

			return nil
		}),
		libkapi.WithPostStartHook(func(ctx context.Context, loopbackConfig *restclient.Config) error {
			return identityprovider.EnsureCRDs(ctx, loopbackConfig, logger)
		}),
		libkapi.WithController(controller),
	)
	require.NoError(t, err)

	go func() { _ = server.ListenAndServe(ctx) }()

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), e2eShutdownTimeout)
		defer shutdownCancel()

		_ = server.Shutdown(shutdownCtx)
	})

	client, err := ctrlclient.New(<-loopback, ctrlclient.Options{Scheme: scheme})
	require.NoError(t, err)

	// Waits for the CRDs to become discoverable to this client too — a
	// fresh client's RESTMapper resolves lazily, with no retry of its own.
	require.Eventually(t, func() bool {
		return client.List(ctx, &v1alpha2.ClusterIdentityProviderList{}) == nil &&
			client.List(ctx, &v1alpha2.IdentityProviderList{}) == nil
	}, e2eReadyTimeout, e2ePollInterval, "identity provider CRDs never became discoverable")

	return apiServer{baseURL: "http://" + addr, client: client}
}

// newE2EController wires an Authenticator and the Controller feeding it,
// the same way pkg/cli's identityProviderOptions does.
func newE2EController(logger *slog.Logger) (*identityprovider.Authenticator, *identityprovider.Controller) {
	cache := identityprovider.NewKeyCache(identityprovider.KeyCacheConfig{Logger: logger})
	authn := identityprovider.NewAuthenticator()

	return authn, identityprovider.NewController(identityprovider.Config{
		Logger: logger,
		Registry: identityprovider.NewRegistry(identityprovider.RegistryConfig{
			Cache: cache, Authenticator: authn,
		}),
		Cache:     cache,
		ZoneLease: zonelease.Identity{HolderIdentity: "e2e"},
	})
}

// freeAddr picks an available loopback address — mirrors
// pkg/domain/instance's identical helper.
func freeAddr(ctx context.Context, t *testing.T) string {
	t.Helper()

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	require.NoError(t, listener.Close())

	return addr
}
