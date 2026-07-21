package ui_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

// errFactory is returned by a stub NamespaceListerFactory to exercise
// handleHome's error path.
var errFactory = errors.New("factory failed")

// Shared OIDC test fixture values, reused across handleSettings tests.
const (
	testOIDCIssuerURL = "https://auth.example.com"
	testOIDCClientID  = "kontinuum"
)

// stubNamespaceLister is a fixed-response ui.NamespaceLister for tests.
type stubNamespaceLister struct {
	list *corev1.NamespaceList
	err  error
}

func (s stubNamespaceLister) List(context.Context, metav1.ListOptions) (*corev1.NamespaceList, error) {
	return s.list, s.err
}

func newTestRequest(t *testing.T, target string) *http.Request {
	t.Helper()

	return httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
}

func TestHandleHomeRendersTenants(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{
			Items: []corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}},
		}}, nil
	}

	router := ui.NewRouter(factory, "test-version", config.Config{}, false)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/home"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "demo")
}

func TestHandleHomeReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return nil, errFactory
	}

	router := ui.NewRouter(factory, "test-version", config.Config{}, false)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/home"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestRegisterRoutesUsesCustomAppRootAndProtect(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	router := ui.NewRouter(factory, "test-version", config.Config{}, false)

	appRootCalled := false
	appRoot := func(http.ResponseWriter, *http.Request) { appRootCalled = true }

	protectCalls := 0
	protect := func(next http.HandlerFunc) http.HandlerFunc {
		return func(writer http.ResponseWriter, request *http.Request) {
			protectCalls++

			next(writer, request)
		}
	}

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, appRoot, protect)

	mux.ServeHTTP(httptest.NewRecorder(), newTestRequest(t, "/app"))
	assert.True(t, appRootCalled, "RegisterRoutes should mount the supplied appRoot at GET /app")

	mux.ServeHTTP(httptest.NewRecorder(), newTestRequest(t, "/app/home"))
	assert.Equal(t, 1, protectCalls)

	mux.ServeHTTP(httptest.NewRecorder(), newTestRequest(t, "/app/settings"))
	assert.Equal(t, 2, protectCalls)
}

func TestHandleHomeShowsLogoutLinkOnlyWhenAuthEnabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	for _, authEnabled := range []bool{true, false} {
		router := ui.NewRouter(factory, "test-version", config.Config{}, authEnabled)

		mux := http.NewServeMux()
		router.RegisterRoutes(mux, nil, nil)

		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, newTestRequest(t, "/app/home"))

		resp := recorder.Result()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		if authEnabled {
			assert.Contains(t, string(body), `href="/app/logout"`)
		} else {
			assert.NotContains(t, string(body), `href="/app/logout"`)
		}
	}
}

func TestHandleSettingsShowsOIDCDetailsOnlyWhenAuthEnabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID
	cfg.OIDC.AdminGroups = "platform-team"

	for _, authEnabled := range []bool{true, false} {
		router := ui.NewRouter(factory, "test-version", cfg, authEnabled)

		mux := http.NewServeMux()
		router.RegisterRoutes(mux, nil, nil)

		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, newTestRequest(t, "/app/settings"))

		resp := recorder.Result()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		if authEnabled {
			assert.Contains(t, string(body), testOIDCIssuerURL)
			assert.Contains(t, string(body), "platform-team")
		} else {
			assert.NotContains(t, string(body), testOIDCIssuerURL)
			assert.NotContains(t, string(body), "platform-team")
		}
	}
}

func TestHandleSettingsShowsKubeconfigOnlyWhenAuthEnabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID

	for _, authEnabled := range []bool{true, false} {
		router := ui.NewRouter(factory, "test-version", cfg, authEnabled)

		mux := http.NewServeMux()
		router.RegisterRoutes(mux, nil, nil)

		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, newTestRequest(t, "/app/settings"))

		resp := recorder.Result()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		if authEnabled {
			assert.Contains(t, string(body), "kubectl access")
			assert.Contains(t, string(body), "server: http://example.com")
			assert.Contains(t, string(body), "insecure-skip-tls-verify: true")
			assert.Contains(t, string(body), "name: example.com\n    cluster:")
			assert.Contains(t, string(body), "cluster: example.com")
			assert.Contains(t, string(body), "name: oidc@example.com")
			assert.Contains(t, string(body), "current-context: oidc@example.com")
			assert.Contains(t, string(body), "--oidc-issuer-url="+testOIDCIssuerURL)
			assert.Contains(t, string(body), "--oidc-client-id="+testOIDCClientID)
			assert.Contains(t, string(body), "downloadKubeconfig()")
		} else {
			assert.NotContains(t, string(body), "kubectl access")
			assert.NotContains(t, string(body), "oidc-login")
		}
	}
}

func TestHandleSettingsStripsPortFromKubeconfigClusterName(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID

	router := ui.NewRouter(factory, "test-version", cfg, true)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	request := newTestRequest(t, "/app/settings")
	request.Host = "example.com:8443"

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Contains(t, string(body), "server: http://example.com:8443")
	assert.Contains(t, string(body), "name: example.com\n    cluster:")
	assert.Contains(t, string(body), "cluster: example.com")
	assert.Contains(t, string(body), "name: oidc@example.com")
	assert.NotContains(t, string(body), "example.com:8443\n    cluster:")
	assert.NotContains(t, string(body), "cluster: example.com:8443")
	assert.NotContains(t, string(body), "oidc@example.com:8443")
}

func TestHandleSettingsUsesForwardedProtoForKubeconfigOrigin(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID

	router := ui.NewRouter(factory, "test-version", cfg, true)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	request := newTestRequest(t, "/app/settings")
	request.Header.Set("X-Forwarded-Proto", "https")

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Contains(t, string(body), "server: https://example.com")
	assert.Contains(t, string(body), "name: oidc@example.com")
	assert.NotContains(t, string(body), "insecure-skip-tls-verify")
}

func TestRegisterRoutesDefaultsToUnconditionalAppRedirect(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	router := ui.NewRouter(factory, "test-version", config.Config{}, false)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/app/home", resp.Header.Get("Location"))
}
