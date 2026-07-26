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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

// errFactory is returned by a stub NamespaceListerFactory to exercise
// handleHome's error path.
var errFactory = errors.New("factory failed")

// errTestForbidden is the wrapped reason on a forbidden test fixture — see
// TestHandleRegistryInvalidatesSessionOnForbidden.
var errTestForbidden = errors.New("forbidden: user is not in admin group")

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

// stubKontinuumLister is a fixed-response ui.KontinuumClient for tests.
type stubKontinuumLister struct {
	items     []v1alpha2.Kontinuum
	err       error
	getErr    error
	deleteErr error
}

// Get looks up a Kontinuum by name in items, matching a real client's
// NotFound behavior when no item matches — see
// TestHandleInstanceDetailReturnsNotFoundForUnknownInstance.
func (s stubKontinuumLister) Get(
	_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption,
) error {
	if s.getErr != nil {
		return s.getErr
	}

	for _, item := range s.items {
		if item.Name == key.Name {
			// obj is always *v1alpha2.Kontinuum in this package's tests.
			*obj.(*v1alpha2.Kontinuum) = item //nolint:forcetypeassert // see comment above

			return nil
		}
	}

	return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha2.GroupName, Resource: "kontinuums"}, key.Name)
}

func (s stubKontinuumLister) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if s.err != nil {
		return s.err
	}

	if kontinuumList, ok := list.(*v1alpha2.KontinuumList); ok {
		kontinuumList.Items = s.items
	}

	return nil
}

func (s stubKontinuumLister) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return s.deleteErr
}

func newTestRequest(t *testing.T, target string) *http.Request {
	t.Helper()

	return httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
}

func newTestDeleteRequest(t *testing.T, target string) *http.Request {
	t.Helper()

	return httptest.NewRequestWithContext(context.Background(), http.MethodDelete, target, nil)
}

func TestHandleHomeRendersTenants(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{
			Items: []corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}},
		}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

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

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

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

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

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

func TestRegisterRoutesServesVendoredStaticAssets(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	for _, asset := range []string{
		"tailwindcss.js", "htmx.min.js",
		"prism-core.min.js", "prism-yaml.min.js", "prism-bash.min.js",
		"jetbrains-mono.css", "jetbrains-mono-latin.woff2",
	} {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, newTestRequest(t, "/static/vendor/"+asset))

		resp := recorder.Result()

		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "expected %s to be served", asset)
	}
}

func TestHandleHomeShowsLogoutLinkOnlyWhenAuthEnabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	for _, authEnabled := range []bool{true, false} {
		kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
			return stubKontinuumLister{}, nil
		}

		router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, authEnabled, nil)

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

// instanceWithConfig builds a Kontinuum fixture carrying the given
// status.config — shared by the handleInstanceDetail tests below.
func instanceWithConfig(name string, cfg v1alpha2.KontinuumConfigStatus) v1alpha2.Kontinuum {
	return v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha2.KontinuumStatus{
			Role:      v1alpha2.RoleWorker,
			SecretRef: v1alpha2.KontinuumSecretReference{Name: name, Namespace: v1alpha2.DefaultSecretNamespace},
			Config:    cfg,
		},
	}
}

func TestHandleInstanceDetailRendersInstanceSettings(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := instanceWithConfig("worker-1", v1alpha2.KontinuumConfigStatus{
		Server: v1alpha2.KontinuumServerConfigStatus{Addr: ":8080", Storage: "postgres://db.internal:5432/kontinuum"},
		Log:    v1alpha2.KontinuumLogConfigStatus{Level: "info", Format: "json"},
		OIDC: v1alpha2.KontinuumOIDCConfigStatus{
			Enabled: true, IssuerURL: testOIDCIssuerURL, ClientID: testOIDCClientID, AdminGroups: "platform-team",
		},
	})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "worker-1")
	assert.Contains(t, string(body), ":8080")
	assert.Contains(t, string(body), "postgres://db.internal:5432/kontinuum")
	assert.Contains(t, string(body), "kontinuum-system/worker-1")
	assert.Contains(t, string(body), testOIDCIssuerURL)
	assert.Contains(t, string(body), "platform-team")
}

func TestHandleInstanceDetailHidesOIDCDetailsWhenInstanceOIDCDisabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := instanceWithConfig("worker-1", v1alpha2.KontinuumConfigStatus{
		OIDC: v1alpha2.KontinuumOIDCConfigStatus{Enabled: false, IssuerURL: testOIDCIssuerURL},
	})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), testOIDCIssuerURL)
}

func TestHandleInstanceDetailReturnsNotFoundForUnknownInstance(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/missing"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandleInstanceDetailReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return nil, errFactory
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleInstanceDetailInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: "kontinuums"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{getErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t, newTestRequest(t, "/app/kontinuums/worker-1"), kontinuumFactory)
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
		kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
			return stubKontinuumLister{}, nil
		}

		router := ui.NewRouter(factory, kontinuumFactory, "test-version", cfg, authEnabled, nil)

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
			assert.NotContains(t, string(body), "insecure-skip-tls-verify")
			assert.Contains(t, string(body), "name: kontinuum-example.com\n    cluster:")
			assert.Contains(t, string(body), "cluster: kontinuum-example.com")
			assert.Contains(t, string(body), "name: oidc@kontinuum-example.com")
			assert.Contains(t, string(body), "current-context: oidc@kontinuum-example.com")
			assert.Contains(t, string(body), "user: kontinuum-example.com")
			assert.Contains(t, string(body), "name: kontinuum-example.com\n    user:")
			assert.NotContains(t, string(body), "user: oidc\n")
			assert.NotContains(t, string(body), "name: oidc\n")
			assert.Contains(t, string(body), "--oidc-issuer-url="+testOIDCIssuerURL)
			assert.Contains(t, string(body), "--oidc-client-id="+testOIDCClientID)
			assert.Contains(t, string(body), "downloadKubeconfig()")
			assert.Contains(t, string(body), "kontinuum config import")
			assert.Contains(t, string(body), "copyImportSnippet(this)")
			assert.Contains(t, string(body), "KUBECONFIG")
		} else {
			assert.NotContains(t, string(body), "kubectl access")
			assert.NotContains(t, string(body), "oidc-login")
			assert.NotContains(t, string(body), "kontinuum config import")
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

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", cfg, true, nil)

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
	assert.Contains(t, string(body), "name: kontinuum-example.com\n    cluster:")
	assert.Contains(t, string(body), "cluster: kontinuum-example.com")
	assert.Contains(t, string(body), "name: oidc@kontinuum-example.com")
	assert.NotContains(t, string(body), "example.com:8443\n    cluster:")
	assert.NotContains(t, string(body), "cluster: example.com:8443")
	assert.NotContains(t, string(body), "oidc@example.com:8443")
}

func TestHandleSettingsSetsInsecureSkipTLSVerifyForLocalHosts(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID

	for _, host := range []string{"localhost:8443", "127.0.0.1:8443", "[::1]:8443"} {
		kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
			return stubKontinuumLister{}, nil
		}

		router := ui.NewRouter(factory, kontinuumFactory, "test-version", cfg, true, nil)

		mux := http.NewServeMux()
		router.RegisterRoutes(mux, nil, nil)

		request := newTestRequest(t, "/app/settings")
		request.Host = host

		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)

		resp := recorder.Result()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		assert.Contains(t, string(body), "insecure-skip-tls-verify: true", "host %q", host)
	}
}

func TestHandleSettingsUsesForwardedProtoForKubeconfigOrigin(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", cfg, true, nil)

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
	assert.Contains(t, string(body), "name: oidc@kontinuum-example.com")
	assert.NotContains(t, string(body), "insecure-skip-tls-verify")
}

func TestRegisterRoutesDefaultsToUnconditionalAppRedirect(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/app/home", resp.Header.Get("Location"))
}

func TestHandleRegistryRendersInstances(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{
			{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Status: v1alpha2.KontinuumStatus{Role: v1alpha2.RoleControlPlane}},
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "demo")
}

func TestHandleRegistryReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return nil, errFactory
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// assertForbiddenInvalidatesSession drives request through a router backed
// by kontinuumFactory and asserts the Forbidden response redirected via
// invalidateSession — shared by the list and delete handlers' equivalent
// tests, which differ only in which request they send and which
// stubKontinuumLister field carries the Forbidden error.
func assertForbiddenInvalidatesSession(
	t *testing.T, request *http.Request, kontinuumFactory ui.KontinuumClientFactory,
) {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var invalidatedWith string

	invalidateSession := func(writer http.ResponseWriter, _ *http.Request, message string) {
		invalidatedWith = message

		writer.WriteHeader(http.StatusFound)
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, invalidateSession)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

func TestHandleRegistryInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: "kontinuums"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{err: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t, newTestRequest(t, "/app/kontinuums"), kontinuumFactory)
}

func TestHandleDeleteInstanceRemovesInstanceAndRerendersRegistry(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t, "/app/kontinuums/demo"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "No instances found")
}

func TestHandleDeleteInstanceReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t, "/app/kontinuums/demo"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteInstanceInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: "kontinuums"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t, newTestDeleteRequest(t, "/app/kontinuums/demo"), kontinuumFactory)
}
