package ui_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/adminrbac"
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

// secretsResource is the corev1 Secret GroupResource used to build fake
// NotFound/Forbidden errors for the config-secret reveal tests below.
const secretsResource = "secrets"

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
	// secret and secretGetErr back Get calls for a *corev1.Secret — used by
	// the config-secret reveal panel on the instance page (see
	// fetchSecretDataYAML). Kept separate from items/getErr, which only ever
	// answer for *v1alpha2.Kontinuum, since a single handler request can
	// issue both kinds of Get and needs to control them independently.
	secret       *corev1.Secret
	secretGetErr error
	// bindings backs List calls for a *rbacv1.ClusterRoleBindingList — used
	// by the IAM page (see handleIAM). listErr, when set, is returned
	// instead — separate from err (which handleRegistry/handleDeleteInstance
	// use for *v1alpha2.KontinuumList) so a single test can control each
	// list kind independently.
	bindings []rbacv1.ClusterRoleBinding
	listErr  error
}

// Get looks up either a Kontinuum by name in items or, for a *corev1.Secret,
// the fixed secret field — dispatching on obj's concrete type the same way a
// real controller-runtime client would. Matches a real client's NotFound
// behavior when no item/secret matches — see
// TestHandleInstanceDetailReturnsNotFoundForUnknownInstance.
func (s stubKontinuumLister) Get(
	_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption,
) error {
	switch target := obj.(type) {
	case *corev1.Secret:
		if s.secretGetErr != nil {
			return s.secretGetErr
		}

		if s.secret != nil && s.secret.Name == key.Name && s.secret.Namespace == key.Namespace {
			*target = *s.secret

			return nil
		}

		return apierrors.NewNotFound(schema.GroupResource{Resource: secretsResource}, key.Name)
	default:
		if s.getErr != nil {
			return s.getErr
		}

		for _, item := range s.items {
			if item.Name == key.Name {
				// obj is always *v1alpha2.Kontinuum here — anything else
				// falls through to the corev1.Secret case above.
				*obj.(*v1alpha2.Kontinuum) = item //nolint:forcetypeassert // see comment above

				return nil
			}
		}

		return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha2.GroupName, Resource: "kontinuums"}, key.Name)
	}
}

func (s stubKontinuumLister) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	switch target := list.(type) {
	case *v1alpha2.KontinuumList:
		if s.err != nil {
			return s.err
		}

		target.Items = s.items
	case *rbacv1.ClusterRoleBindingList:
		if s.listErr != nil {
			return s.listErr
		}

		target.Items = s.bindings
	}

	return nil
}

func (s stubKontinuumLister) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return s.deleteErr
}

// zoneFactory is a fixed ui.ZoneClientFactory for tests that don't exercise
// the "Add zone" form or the registry page's own zones table — an empty
// (but scheme-registered, so renderRegistry's own Zone List call succeeds
// with zero results rather than erroring) fake client is enough to satisfy
// ui.NewRouter's constructor.
func zoneFactory(context.Context) (client.Client, error) {
	scheme := apiruntime.NewScheme()
	_ = v1alpha2.AddToScheme(scheme)

	return fake.NewClientBuilder().WithScheme(scheme).Build(), nil
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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

		router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
			config.Config{}, authEnabled, nil)

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
func instanceWithConfig(cfg v1alpha2.KontinuumConfigStatus) v1alpha2.Kontinuum {
	const name = "worker-1"

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

	item := instanceWithConfig(v1alpha2.KontinuumConfigStatus{
		Server: v1alpha2.KontinuumServerConfigStatus{Addr: ":8080", Storage: "postgres://db.internal:5432/kontinuum"},
		Log:    v1alpha2.KontinuumLogConfigStatus{Level: "info", Format: "json"},
		OIDC: v1alpha2.KontinuumOIDCConfigStatus{
			Enabled: true, IssuerURL: testOIDCIssuerURL, ClientID: testOIDCClientID, AdminGroups: "platform-team",
		},
	})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	item := instanceWithConfig(v1alpha2.KontinuumConfigStatus{
		OIDC: v1alpha2.KontinuumOIDCConfigStatus{Enabled: false, IssuerURL: testOIDCIssuerURL},
	})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

func TestHandleInstanceDetailShowsConfigSecretDataReveal(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := instanceWithConfig(v1alpha2.KontinuumConfigStatus{})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: v1alpha2.DefaultSecretNamespace},
		Data:       map[string][]byte{"password": []byte("s3cr3t")},
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}, secret: secret}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Secrets")
	assert.Contains(t, string(body), "toggleSecretData")
	assert.Contains(t, string(body), "password: s3cr3t")
}

func TestHandleInstanceDetailHidesConfigSecretRevealWhenSecretRefEmpty(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := v1alpha2.Kontinuum{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "toggleSecretData")
}

func TestHandleInstanceDetailHidesConfigSecretRevealWhenSecretNotFound(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := instanceWithConfig(v1alpha2.KontinuumConfigStatus{})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			items: []v1alpha2.Kontinuum{item},
			secretGetErr: apierrors.NewNotFound(
				schema.GroupResource{Resource: secretsResource}, "worker-1"),
		}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "kontinuum-system/worker-1")
	assert.NotContains(t, string(body), "toggleSecretData")
}

func TestHandleInstanceDetailReturnsBadGatewayWhenSecretGetFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := instanceWithConfig(v1alpha2.KontinuumConfigStatus{})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}, secretGetErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleInstanceDetailInvalidatesSessionOnForbiddenSecretGet(t *testing.T) {
	t.Parallel()

	item := instanceWithConfig(v1alpha2.KontinuumConfigStatus{})
	forbiddenReason := schema.GroupResource{Resource: secretsResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			items:        []v1alpha2.Kontinuum{item},
			secretGetErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden),
		}, nil
	}

	assertForbiddenInvalidatesSession(t, newTestRequest(t, "/app/kontinuums/worker-1"), kontinuumFactory)
}

func TestHandleSettingsShowsOIDCKubeconfigWhenAuthEnabled(t *testing.T) {
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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/settings"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

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
}

func TestHandleSettingsShowsNoAuthKubeconfigWhenOIDCDisabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/settings"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// Kontinuum's default is no authentication at all, not no access — the
	// kubectl access section (and a working kubeconfig) must still show.
	assert.Contains(t, string(body), "kubectl access")
	assert.Contains(t, string(body), "No authentication is required")
	assert.Contains(t, string(body), "server: http://example.com")
	assert.Contains(t, string(body), "name: kontinuum-example.com\n    cluster:")
	assert.Contains(t, string(body), "cluster: kontinuum-example.com")
	assert.Contains(t, string(body), "current-context: kontinuum-example.com")
	assert.NotContains(t, string(body), "oidc-login")
	assert.NotContains(t, string(body), "users:")
	assert.Contains(t, string(body), "downloadKubeconfig()")
	assert.Contains(t, string(body), "kontinuum config import")
	assert.Contains(t, string(body), "copyImportSnippet(this)")
	assert.Contains(t, string(body), "KUBECONFIG")
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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		cfg, true, nil)

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

func TestHandleSettingsSetsInsecureSkipTLSVerifyForLocalHostsOverHTTPS(t *testing.T) {
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

		router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
			cfg, true, nil)

		mux := http.NewServeMux()
		router.RegisterRoutes(mux, nil, nil)

		request := newTestRequest(t, "/app/settings")
		request.Host = host
		// A TLS-terminating reverse proxy is what would actually front a
		// local deployment reached over https — see requestOrigin.
		request.Header.Set("X-Forwarded-Proto", "https")

		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)

		resp := recorder.Result()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		assert.Contains(t, string(body), "insecure-skip-tls-verify: true", "host %q", host)
	}
}

func TestHandleSettingsOmitsInsecureSkipTLSVerifyForPlainHTTP(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL
	cfg.OIDC.ClientID = testOIDCClientID

	// Plain http has no certificate to skip verifying at all, so even a
	// local host that would otherwise look self-signed (see
	// probablySelfSigned) must not get the line — see kubeconfig's doc.
	for _, host := range []string{"localhost:8080", "127.0.0.1:8080", "[::1]:8080"} {
		kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
			return stubKontinuumLister{}, nil
		}

		router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
			cfg, true, nil)

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

		assert.NotContains(t, string(body), "insecure-skip-tls-verify", "host %q", host)
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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		cfg, true, nil)

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

// adminGroupBinding builds a fixture rbacv1.ClusterRoleBinding shaped the
// way pkg/domain/adminrbac's controller creates one — labeled as managed
// and annotated with the OIDC group it grants — for handleIAM's tests.
func adminGroupBinding(name, group string) rbacv1.ClusterRoleBinding {
	return rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{v1alpha2.LabelManagedBy: adminrbac.ManagedByValue},
			Annotations: map[string]string{adminrbac.AdminGroupAnnotation: group},
		},
		RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: adminrbac.RoleName},
	}
}

func TestHandleIAMShowsAdminGroupBindings(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{bindings: []rbacv1.ClusterRoleBinding{
			adminGroupBinding("kontinuum-admin-aaaaaaaaaaaa", "platform-admins"),
			adminGroupBinding("kontinuum-admin-bbbbbbbbbbbb", "sre"),
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/iam"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "Role bindings")
	assert.Contains(t, string(body), "platform-admins")
	assert.Contains(t, string(body), "sre")
	assert.Contains(t, string(body), adminrbac.RoleName)
	assert.Contains(t, string(body), "kontinuum-admin-aaaaaaaaaaaa")
	assert.NotContains(t, string(body), "No admin groups are configured")
}

func TestHandleIAMInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	forbiddenReason := schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterrolebindings"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{listErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	var invalidatedWith string

	invalidateSession := func(writer http.ResponseWriter, _ *http.Request, message string) {
		invalidatedWith = message

		writer.WriteHeader(http.StatusFound)
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		cfg, true, invalidateSession)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/iam"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

func TestHandleIAMShowsNoticeWhenOIDCDisabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/iam"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "OIDC is not configured")
	assert.NotContains(t, string(body), "Role bindings")
}

func TestHandleIAMShowsNoBindingsMessageWhenNoneExist(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/iam"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "Role bindings")
	assert.Contains(t, string(body), "No admin groups are configured")
}

func TestRegisterRoutesDefaultsToUnconditionalAppRedirect(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

// zoneWithCondition builds a Zone fixture with a single Installed condition
// — see TestHandleRegistryRendersZones' own use for exercising both the
// True (green badge) and False (blue badge) rendering paths.
func zoneWithCondition(
	name, region, zoneName string, status metav1.ConditionStatus, reason, message string,
) *v1alpha2.Zone {
	return &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha2.ZoneSpec{Region: region, Zone: zoneName, Domain: "example.com"},
		Status: v1alpha2.ZoneStatus{
			Conditions: []metav1.Condition{
				{
					Type: "Installed", Status: status, Reason: reason,
					Message: message, LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
}

func TestHandleRegistryRendersZones(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	notReadyZone := zoneWithCondition("eu-eu-1a", "eu", "eu-1a", metav1.ConditionFalse, "WaitingForCertificate",
		"waiting for cert-manager to issue eu-1a.eu.example.com's certificate")
	readyZone := zoneWithCondition("eu-eu-1b", "eu", "eu-1b", metav1.ConditionTrue, "Installed",
		"kontinuum-server installed")
	zoneClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(notReadyZone, readyZone).Build()
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a")
	assert.Contains(t, string(body), ">eu<")
	assert.Contains(t, string(body), "Installed=False")
	assert.Contains(t, string(body), "Waiting for cert-manager to issue",
		"the condition message's first letter is capitalized")
	assert.Contains(t, string(body), "bg-blue-900/40", "a False condition renders a blue badge")
	assert.Contains(t, string(body), "bg-green-900/40", "a True condition renders a green badge")
}

func TestHandleRegistryReturnsBadGatewayWhenZoneListFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return failingListZoneClient{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

// failingListZoneClient is a client.Client test double whose List always
// fails with a plain (non-Forbidden) error — every other method falls
// through to the embedded nil client.Client, which is fine: renderRegistry
// only ever calls List through this factory.
type failingListZoneClient struct{ client.Client }

func (failingListZoneClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errFactory
}

func TestHandleRegistryReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return nil, errFactory
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, invalidateSession)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

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

// newTestZoneClient builds a real fake controller-runtime client with
// kontinuum.sh/v1alpha2 registered, for tests that need handleZoneAdd's
// pkg/domain/zone.Add call to actually create objects.
func newTestZoneClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// registeredKontinuumWithDomain is a Kontinuum that's already published a
// DNS domain on its own status.config.server.dns.domain — seeded into a
// zone client's fake scheme so pkg/domain/zone.Add's own domain inference
// (see AddOptions.Domain's doc) has something to find, the same way a real
// hub's self-registration would provide it.
func registeredKontinuumWithDomain(name, domain string) *v1alpha2.Kontinuum {
	return &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha2.KontinuumStatus{
			Config: v1alpha2.KontinuumConfigStatus{
				Server: v1alpha2.KontinuumServerConfigStatus{
					DNS: v1alpha2.KontinuumDNSConfigStatus{Domain: domain},
				},
			},
		},
	}
}

// forbiddenZoneClient is a client.Client test double whose Create always
// returns Forbidden — every other method falls through to the embedded nil
// client.Client, which is fine: handleZoneAdd's only call through
// pkg/domain/zone.Add is a sequence of Create calls, and Apply stops at
// the first error.
type forbiddenZoneClient struct{ client.Client }

func (forbiddenZoneClient) Create(context.Context, client.Object, ...client.CreateOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: v1alpha2.GroupName, Resource: "zones"}, "",
		errTestForbidden)
}

// List is also overridden: Add's own domain inference (see
// AddOptions.Domain's doc) lists Kontinuums before ever reaching Create,
// so that's the first call a Forbidden hub actually rejects.
func (forbiddenZoneClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: v1alpha2.GroupName, Resource: "kontinuums"}, "",
		errTestForbidden)
}

func newTestZoneAddRequest(t *testing.T, form url.Values) *http.Request {
	t.Helper()

	request := httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/app/zones/add", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return request
}

func TestRegistryPageEmbedsAddZoneButtonAndEmptyModal(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// The button that opens the modal, and the (always-empty on a plain
	// page load) form itself, both embedded directly in the registry page
	// — not a separate /app/zones/add page.
	assert.Contains(t, string(body), "openZoneAddModal()")
	assert.Contains(t, string(body), `id="zone-add-modal"`)
	assert.Contains(t, string(body), `name="talos-address"`)
	assert.NotContains(t, string(body), "Cluster provisioning is now underway")
}

func TestHandleZoneAddCreatesZoneAndReturnsSuccessFragment(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneClient := newTestZoneClient(t, registeredKontinuumWithDomain("hub", "example.com"))
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{"region": {"eu"}, "zone": {"eu-1a"}, "talos-address": {"10.0.0.5"}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a")
	// The form itself is gone from a success response — nothing left to
	// resubmit.
	assert.NotContains(t, string(body), `name="talos-address"`)

	var got v1alpha2.Zone
	require.NoError(t, zoneClient.Get(context.Background(), client.ObjectKey{Name: "eu-eu-1a"}, &got))
	assert.Equal(t, "example.com", got.Spec.Domain)
}

func TestHandleZoneAddRerendersFormOnValidationError(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zonesFactory := func(context.Context) (client.Client, error) { return newTestZoneClient(t), nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	// talos-address deliberately omitted — Add's own validation rejects it.
	form := url.Values{"region": {"eu"}, "zone": {"eu-1a"}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "talos-address")
	// The submitted region/zone are preserved so the user doesn't retype them.
	assert.Contains(t, string(body), `value="eu"`)
}

func TestHandleZoneAddReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return nil, errFactory }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{"region": {"eu"}, "zone": {"eu-1a"}, "talos-address": {"10.0.0.5"}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleZoneAddInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return forbiddenZoneClient{}, nil }

	var invalidatedWith string

	invalidateSession := func(writer http.ResponseWriter, _ *http.Request, message string) {
		invalidatedWith = message

		writer.WriteHeader(http.StatusFound)
	}

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, invalidateSession)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{"region": {"eu"}, "zone": {"eu-1a"}, "talos-address": {"10.0.0.5"}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}
