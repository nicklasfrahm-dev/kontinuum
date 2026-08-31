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
	"time"

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
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

// errFactory is returned by a stub NamespaceListerFactory to exercise
// handleHome's error path.
var errFactory = errors.New("factory failed")

// errTestForbidden is the wrapped reason on a forbidden test fixture — see
// TestHandleRegistryInvalidatesSessionOnForbidden.
var errTestForbidden = errors.New("forbidden: user is not in admin group")

// Shared OIDC test fixture values, reused across handleRegistryKubeconfigDownload tests.
const (
	testOIDCIssuerURL = "https://auth.example.com"
	testOIDCClientID  = "kontinuum"
)

// secretsResource is the corev1 Secret GroupResource used to build fake
// NotFound/Forbidden errors for the config-secret reveal tests below.
const secretsResource = "secrets"

// testTalosClusterName is the shared fixture name reused across the
// clusters list/detail/kubeconfig-download tests below (see
// talosClusterFixture) and TestHandleZoneAddCreatesZoneAndReturnsSuccessFragment,
// where it coincidentally matches zone-add's own <region>-<zone> naming
// (region "eu", zone testZoneValue — see addObjectName's doc).
const testTalosClusterName = "eu-eu-1a"

// talosClustersResource is the GroupResource "resource" name used to build
// fake NotFound/Forbidden errors for the clusters pages' tests below — see
// secretsResource for the equivalent used by the config-secret tests.
const talosClustersResource = "talosclusters"

// kontinuumsResource, instancesResource, and zonesResource are the same
// pattern as talosClustersResource/secretsResource above, for the
// registry/instances/zones pages' own NotFound/Forbidden fixtures.
const (
	kontinuumsResource = "kontinuums"
	instancesResource  = "instances"
	zonesResource      = "zones"
)

// testTalosAddress is the shared --talos-address fixture value reused
// across every "Add zone" form-submission test below.
const testTalosAddress = "10.0.0.5"

// testZoneValue is the shared zone fixture value ("eu", testZoneValue) reused
// across zone-add/zone-delete tests below — see testTalosClusterName's own
// doc for why this coincidentally matches its own <region>-<zone> naming.
const testZoneValue = "eu-1a"

// testExampleDomain is the shared DNS domain fixture value reused across
// zone-add/zone-detail tests below.
const testExampleDomain = "example.com"

// testReadyConditionType mirrors zone.ReadyConditionType/
// taloscluster.ReadyConditionType's own literal ("Ready") — kept as a local
// copy rather than importing either domain package just for this one
// string, same rationale as this file's other test-only literal constants.
const testReadyConditionType = "Ready"

// zoneAddFormRegionKey/zoneAddFormZoneKey/zoneAddFormTalosAddressKey are the
// "Add zone" form's own field names (see zone_add_modal.html), reused across
// every url.Values fixture below that submits that form.
const (
	zoneAddFormRegionKey       = "region"
	zoneAddFormZoneKey         = "zone"
	zoneAddFormTalosAddressKey = "talos-address"
)

// instanceAddFormAddressKey is the "Add instance" form's own field name
// (see instance_add_modal.html), reused across every url.Values fixture
// below that submits that form.
const instanceAddFormAddressKey = "address"

// testExistingInstanceAddress is the shared spec.interfaces[0] fixture value
// for an already-registered Instance reused across the "Add zone" modal's
// own instance-picker tests below.
const testExistingInstanceAddress = "10.0.0.9"

// testNamespace is the shared tenant-namespace fixture value reused across
// the namespaced Role/RoleBinding tests below.
const testNamespace = "demo"

// testRoleName/testRoleBindingName are the shared Role/RoleBinding fixture
// names reused across the namespaced IAM tests below.
const (
	testRoleName        = "pod-reader"
	testRoleBindingName = "pod-reader-binding"
)

// testClusterRoleName is the shared ClusterRole fixture name reused across
// the cluster-scoped IAM tests below.
const testClusterRoleName = "custom-role"

// testRoleRuleResource is the shared rule-builder "resources" fixture value
// reused across the role-rule-building tests below.
const testRoleRuleResource = "pods"

// testSubjectName is the shared RoleBinding/ClusterRoleBinding Group-subject
// fixture value reused across the IAM tests below.
const testSubjectName = "sre"

// roleRuleFormVerbGet/roleRuleFormVerbList are two of the fixed verb
// checkboxes role_add_modal.html renders (see verb-checkbox) — reused
// across the role-rule-building tests below wherever a rule needs at least
// one real verb.
const (
	roleRuleFormVerbGet  = "get"
	roleRuleFormVerbList = "list"
)

// roleAddFormNameKey is the "Add role"/"Add role binding"/"Add cluster
// role"/"Add cluster role binding" forms' own shared "name" field (see
// role_add_modal.html/rolebinding_add_modal.html) — kept as one constant
// since every one of those forms uses the identical field name.
const roleAddFormNameKey = "name"

// roleAddFormRuleResourcesKey is the rule builder's own "resources" field
// name (see role_add_modal.html's role-add-rule-row), reused across the
// role-rule-building tests below.
const roleAddFormRuleResourcesKey = "rules[0][resources]"

// rolebindingAddFormSubjectKindKey/rolebindingAddFormSubjectNameKey are the
// "Add role binding" form's own field names (see rolebinding_add_modal.html),
// reused across every url.Values fixture below that submits that form.
const (
	rolebindingAddFormSubjectKindKey = "subject-kind"
	rolebindingAddFormSubjectNameKey = "subject-name"
)

// zoneAddForm builds the "Add zone" form's own minimal valid submission
// (region/zone/talos-address only) — reused by every test below that
// doesn't need to vary those three fields.
func zoneAddForm() url.Values {
	return url.Values{
		zoneAddFormRegionKey: {"eu"}, zoneAddFormZoneKey: {testZoneValue}, zoneAddFormTalosAddressKey: {testTalosAddress},
	}
}

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
	// instances and instanceErr back List calls for a *v1alpha2.InstanceList —
	// used by the /app/instances page (see handleInstances). Separate from
	// items/err, which List uses for *v1alpha2.KontinuumList, so a single
	// test can control each list kind independently.
	instances   []v1alpha2.Instance
	instanceErr error
	// instanceGetErr backs Get calls for a *v1alpha2.Instance — used by the
	// instance detail page (see handleInstanceDetail). Separate from getErr,
	// which Get uses for *v1alpha2.Kontinuum.
	instanceGetErr error
	// talosClusters backs List calls for a *v1alpha2.TalosClusterList and
	// Get calls for a *v1alpha2.TalosCluster — used by the clusters pages
	// (see handleTalosClusters/handleTalosClusterDetail). talosClustersErr,
	// when set, is returned by List instead; talosClusterGetErr, when set,
	// is returned by Get instead — kept separate since a detail-page test
	// only ever needs one or the other.
	talosClusters      []v1alpha2.TalosCluster
	talosClustersErr   error
	talosClusterGetErr error
	// zones backs Get calls for a *v1alpha2.Zone — used by
	// handleTalosClusterDetail's owning-Zone lookup (see fetchOwningZone).
	// Kept separate from zoneFactory's own fake client, which only ever
	// backs the "Add zone" form/registry page's own zones table.
	zones []v1alpha2.Zone
	// pools backs Get calls for a *v1alpha2.InstancePool — used by
	// handleTalosClusterDetail's pool breakdown (see fetchPoolRow).
	pools []v1alpha2.InstancePool
	// roles/roleBindings/clusterRoles back List calls for the IAM roles
	// pages (see handleIAMNamespaceRoles/handleIAMNamespaceRoleBindings/
	// handleIAMClusterRoles). listErr, reused from the
	// *rbacv1.ClusterRoleBindingList case above, is returned for any of
	// these list kinds too — no test needs to distinguish between them.
	roles        []rbacv1.Role
	roleBindings []rbacv1.RoleBinding
	clusterRoles []rbacv1.ClusterRole
	// created, when non-nil, receives a copy of every object passed to
	// Create — a pointer so appends survive stubKontinuumLister being
	// copied by value on every kontinuumsFor(ctx) call (see the "add"
	// handler tests, which pass in a *[]client.Object they inspect after
	// the request completes). createErr, when set, is returned instead of
	// recording anything.
	created   *[]client.Object
	createErr error
}

// Get looks up either a Kontinuum by name in items or, for a *corev1.Secret,
// the fixed secret field — dispatching on obj's concrete type the same way a
// real controller-runtime client would. Matches a real client's NotFound
// behavior when no item/secret matches — see
// TestHandleKontinuumDetailReturnsNotFoundForUnknownInstance.
func (s stubKontinuumLister) Get(
	_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption,
) error {
	switch target := obj.(type) {
	case *corev1.Secret:
		return s.getSecret(key, target)
	case *v1alpha2.Instance:
		return s.getInstance(key, target)
	case *v1alpha2.TalosCluster:
		return s.getTalosCluster(key, target)
	case *v1alpha2.Zone:
		return s.getZone(key, target)
	case *v1alpha2.InstancePool:
		return s.getInstancePool(key, target)
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

		return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha2.GroupName, Resource: kontinuumsResource}, key.Name)
	}
}

func (s stubKontinuumLister) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	switch target := list.(type) {
	case *v1alpha2.KontinuumList:
		if s.err != nil {
			return s.err
		}

		target.Items = s.items
	case *v1alpha2.InstanceList:
		if s.instanceErr != nil {
			return s.instanceErr
		}

		target.Items = s.instances
	case *v1alpha2.TalosClusterList:
		if s.talosClustersErr != nil {
			return s.talosClustersErr
		}

		target.Items = s.talosClusters
	default:
		return s.listRBAC(list)
	}

	return nil
}

func (s stubKontinuumLister) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return s.deleteErr
}

// Create records obj into s.created (see that field's own doc) or returns
// s.createErr.
func (s stubKontinuumLister) Create(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
	if s.createErr != nil {
		return s.createErr
	}

	if s.created != nil {
		*s.created = append(*s.created, obj)
	}

	return nil
}

// listRBAC backs List's RBAC-related list kinds (ClusterRoleBindingList,
// RoleList, RoleBindingList, ClusterRoleList) — split out of List purely to
// keep that function's cyclomatic complexity down.
func (s stubKontinuumLister) listRBAC(list client.ObjectList) error {
	switch target := list.(type) {
	case *rbacv1.ClusterRoleBindingList:
		if s.listErr != nil {
			return s.listErr
		}

		target.Items = s.bindings
	case *rbacv1.RoleList:
		if s.listErr != nil {
			return s.listErr
		}

		target.Items = s.roles
	case *rbacv1.RoleBindingList:
		if s.listErr != nil {
			return s.listErr
		}

		target.Items = s.roleBindings
	case *rbacv1.ClusterRoleList:
		if s.listErr != nil {
			return s.listErr
		}

		target.Items = s.clusterRoles
	}

	return nil
}

// getSecret looks up the fixed s.secret field by name/namespace — factored
// out of Get purely to keep that function's cyclomatic complexity down, same
// as getInstance below.
func (s stubKontinuumLister) getSecret(key client.ObjectKey, target *corev1.Secret) error {
	if s.secretGetErr != nil {
		return s.secretGetErr
	}

	if s.secret != nil && s.secret.Name == key.Name && s.secret.Namespace == key.Namespace {
		*target = *s.secret

		return nil
	}

	return apierrors.NewNotFound(schema.GroupResource{Resource: secretsResource}, key.Name)
}

// getInstance looks up an Instance by name in s.instances — factored out of
// Get purely to keep that function's cyclomatic complexity down.
func (s stubKontinuumLister) getInstance(key client.ObjectKey, target *v1alpha2.Instance) error {
	if s.instanceGetErr != nil {
		return s.instanceGetErr
	}

	for _, item := range s.instances {
		if item.Name == key.Name {
			*target = item

			return nil
		}
	}

	return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha2.GroupName, Resource: instancesResource}, key.Name)
}

// getTalosCluster backs Get's *v1alpha2.TalosCluster case.
func (s stubKontinuumLister) getTalosCluster(key client.ObjectKey, target *v1alpha2.TalosCluster) error {
	if s.talosClusterGetErr != nil {
		return s.talosClusterGetErr
	}

	for _, item := range s.talosClusters {
		if item.Name == key.Name {
			*target = item

			return nil
		}
	}

	notFoundResource := schema.GroupResource{Group: v1alpha2.GroupName, Resource: talosClustersResource}

	return apierrors.NewNotFound(notFoundResource, key.Name)
}

// getZone backs Get's *v1alpha2.Zone case — used by
// handleTalosClusterDetail's owning-Zone lookup (see fetchOwningZone).
func (s stubKontinuumLister) getZone(key client.ObjectKey, target *v1alpha2.Zone) error {
	for _, item := range s.zones {
		if item.Name == key.Name {
			*target = item

			return nil
		}
	}

	return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha2.GroupName, Resource: zonesResource}, key.Name)
}

// getInstancePool backs Get's *v1alpha2.InstancePool case — used by
// handleTalosClusterDetail's pool breakdown (see fetchPoolRow).
func (s stubKontinuumLister) getInstancePool(key client.ObjectKey, target *v1alpha2.InstancePool) error {
	for _, item := range s.pools {
		if item.Name == key.Name {
			*target = item

			return nil
		}
	}

	return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha2.GroupName, Resource: "instancepools"}, key.Name)
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

// newTestFormRequest builds a form-encoded POST request against target —
// used by the IAM "add" handler tests below, which (unlike
// newTestZoneAddRequest) need more than one fixed URL.
func newTestFormRequest(t *testing.T, target string, form url.Values) *http.Request {
	t.Helper()

	request := httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, target, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return request
}

// TestHandleHomeRedirectsToDefaultTenantInstances covers GET /app/home
// specifically — /app/home has no page of its own anymore (see issue #63's
// UI comment: the tenants list it used to render is gone, replaced by
// nav.html's own tenant switcher). It's kept as a real route purely as
// pkg/auth's own post-login redirect target, and just forwards straight to
// the default tenant's instances page — the same target GET /app's own
// unconditional redirect uses (see
// TestRegisterRoutesDefaultsToUnconditionalAppRedirect), but reached via a
// different route.
func TestHandleHomeRedirectsToDefaultTenantInstances(t *testing.T) {
	t.Parallel()

	mux := newRedirectTestMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/home"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances", resp.Header.Get("Location"))
}

// newRedirectTestMux builds a fully-wired *http.ServeMux for the two
// GET-/app.../redirect tests above and below — see
// TestHandleHomeRedirectsToDefaultTenantInstances and
// TestRegisterRoutesDefaultsToUnconditionalAppRedirect, which differ only in
// which path they request.
func newRedirectTestMux(t *testing.T) *http.ServeMux {
	t.Helper()

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

	return mux
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

	mux.ServeHTTP(httptest.NewRecorder(), newTestRequest(t, "/app/registry/kubeconfig"))
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

// TestRegisterRoutesRedirectsUnmatchedAppPathToParent covers the catch-all
// "/app/" route notFoundFallback wraps in RegisterRoutes: a URL that never
// matched any registered pattern at all (a typo, a stale deep link into
// something that never existed) gets the same walk-up-to-parent treatment
// as an object lookup's own 404 — not a bare "404 page not found" with
// nowhere obvious to go from here.
func TestRegisterRoutesRedirectsUnmatchedAppPathToParent(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/bogus/path",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/bogus")
}

// TestRegisterRoutesRedirectsBareAppSlashUpToAppRoot covers the catch-all
// "/app/" route's own boundary: hit with no further path segments at all
// (as opposed to TestRegisterRoutesRedirectsUnmatchedAppPathToParent's
// deeper bogus path), its parent is "/app" itself — GET /app always
// succeeds (see appRoot's own unconditional redirect to
// defaultInstancesPath), so this is where the walk-up chain ends rather
// than looping.
func TestRegisterRoutesRedirectsBareAppSlashUpToAppRoot(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t, "/app/", "/app")
}

func TestHandleMachinesShowsLogoutLinkOnlyWhenAuthEnabled(t *testing.T) {
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
		mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances"))

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

// kontinuumWithConfig builds a Kontinuum fixture carrying the given
// status.config — shared by the handleKontinuumDetail tests below.
func kontinuumWithConfig(cfg v1alpha2.KontinuumConfigStatus) v1alpha2.Kontinuum {
	const name = "worker-1"

	return v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha2.KontinuumStatus{
			Role:      v1alpha2.RoleWorker,
			SecretRef: v1alpha2.KontinuumSecretReference{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
			Config:    cfg,
		},
	}
}

func TestHandleKontinuumDetailRendersInstanceSettings(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{
		Server: v1alpha2.KontinuumServerConfigStatus{
			Addr: ":8080", Storage: "postgres://db.internal:5432/kontinuum",
			DNS:  v1alpha2.KontinuumDNSConfigStatus{Domain: "kontinuum.example.com"},
			GRPC: v1alpha2.KontinuumGRPCConfigStatus{Endpoint: "proxy:8443", InsecureTLSSkipVerify: "true"},
		},
		Log: v1alpha2.KontinuumLogConfigStatus{Level: "info", Format: "json"},
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
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "worker-1")
	assert.Contains(t, string(body), ":8080")
	assert.Contains(t, string(body), "postgres://db.internal:5432/kontinuum")
	assert.Contains(t, string(body), "kontinuum-system/worker-1")
	assert.Contains(t, string(body), testOIDCIssuerURL)
	assert.Contains(t, string(body), "platform-team")
	assert.Contains(t, string(body), "kontinuum.example.com")
	assert.Contains(t, string(body), "proxy:8443")
	assert.Contains(t, string(body), "Insecure TLS skip verify")
}

// TestHandleKontinuumDetailShowsNotConfiguredWhenDNSAndGRPCUnset covers
// issue #98's own case: a Kontinuum with no KONTINUUM_SERVER_DNS_DOMAIN or
// KONTINUUM_SERVER_GRPC_ENDPOINT configured (the default for a local Talos
// dev zone — see docs/local-setup.md) must show "Not configured" in both
// the DNS and gRPC sections, not a blank value, and gRPC's own insecure
// TLS skip verify must read "Disabled".
func TestHandleKontinuumDetailShowsNotConfiguredWhenDNSAndGRPCUnset(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{
		Server: v1alpha2.KontinuumServerConfigStatus{Addr: ":8080", Storage: "postgres://db.internal:5432/kontinuum"},
	})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(string(body), "Not configured"),
		"both DNS domain and gRPC endpoint must show their own \"Not configured\" state")
	assert.Contains(t, string(body), "text-neutral-400\">Disabled<")
}

func TestHandleKontinuumDetailHidesOIDCDetailsWhenInstanceOIDCDisabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{
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
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), testOIDCIssuerURL)
}

func TestHandleKontinuumDetailRedirectsToRegistryForUnknownInstance(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/missing",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums")
}

func TestHandleKontinuumDetailReturnsServerErrorWhenFactoryFails(t *testing.T) {
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
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleKontinuumDetailInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: kontinuumsResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{getErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"), kontinuumFactory)
}

func TestHandleKontinuumDetailShowsConfigSecretDataReveal(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: v1alpha2.KontinuumSystemNamespace},
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
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Secrets")
	assert.Contains(t, string(body), `id="secret-data-toggle"`)
	assert.NotContains(t, string(body), "password: s3cr3t",
		"the secret's own contents must never be rendered into the page")

	secretRecorder := httptest.NewRecorder()
	mux.ServeHTTP(secretRecorder,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1/secret"))

	secretResp := secretRecorder.Result()

	defer func() { _ = secretResp.Body.Close() }()

	require.Equal(t, http.StatusOK, secretResp.StatusCode)

	secretBody, err := io.ReadAll(secretResp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(secretBody), "password: s3cr3t")
}

func TestHandleKontinuumDetailHidesConfigSecretRevealWhenSecretRefEmpty(t *testing.T) {
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
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), `id="secret-data-toggle"`)
}

// TestHandleKontinuumDetailShowsGRPCThumbprintForWorker covers a Worker
// Kontinuum's gRPC card showing the same etcd gRPC proxy identity
// thumbprint the zone detail page's own "Etcd proxy identity" card shows
// (see fetchZoneIdentity) — keyed by the "<region>-<zone>" Zone name
// zoneNameForKontinuum derives from item's own Spec.Region/Spec.Zone.
func TestHandleKontinuumDetailShowsGRPCThumbprintForWorker(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-2"},
		Spec:       v1alpha2.KontinuumSpec{Region: "us-east", Zone: "a"},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker},
	}

	certPEM, _, err := etcdproxy.GenerateIdentity("us-east-a")
	require.NoError(t, err)

	previousCertPEM, _, err := etcdproxy.GenerateIdentity("us-east-a")
	require.NoError(t, err)

	// ParsePublicSecret requires both Current and Previous to be non-empty
	// (see parseIdentity) — a freshly rotated identity still carries its
	// predecessor for IdentityOverlapWindow, so a real Secret always has
	// both.
	pair := etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: certPEM, IssuedAt: time.Now()},
		Previous: etcdproxy.Identity{CertPEM: previousCertPEM, IssuedAt: time.Now().Add(-etcdproxy.IdentityRotationInterval)},
	}
	wantThumbprint, err := etcdproxy.Thumbprint(certPEM)
	require.NoError(t, err)

	// ParsePublicSecret reads secret.Data, not secret.StringData — a real
	// apiserver merges the two on write, but this test builds the Secret
	// in-process, so it must do that merge itself.
	secret := etcdproxy.BuildPublicSecret("us-east-a", v1alpha2.KontinuumSystemNamespace, pair)
	secret.Data = make(map[string][]byte, len(secret.StringData))

	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}, secret: secret}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-2"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), wantThumbprint)
}

// TestHandleKontinuumDetailHidesGRPCThumbprintForControlPlane covers the
// hub's own Kontinuum (RoleControlPlane, empty Spec.Region/Spec.Zone) never
// showing a thumbprint row — it has no corresponding Zone, so
// zoneNameForKontinuum reports ok=false and fetchZoneIdentity is never even
// called.
func TestHandleKontinuumDetailHidesGRPCThumbprintForControlPlane(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "hub-1"},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleControlPlane},
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/hub-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "Thumbprint")
}

func TestHandleKontinuumDetailHidesConfigSecretRevealWhenSecretNotFound(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{})

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
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "kontinuum-system/worker-1")
	assert.NotContains(t, string(body), `id="secret-data-toggle"`)
}

func TestHandleKontinuumDetailReturnsBadGatewayWhenSecretGetFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{})

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{items: []v1alpha2.Kontinuum{item}, secretGetErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleKontinuumDetailInvalidatesSessionOnForbiddenSecretGet(t *testing.T) {
	t.Parallel()

	item := kontinuumWithConfig(v1alpha2.KontinuumConfigStatus{})
	forbiddenReason := schema.GroupResource{Resource: secretsResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			items:        []v1alpha2.Kontinuum{item},
			secretGetErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden),
		}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/worker-1"), kontinuumFactory)
}

// registryKubeconfigBody issues GET /app/registry/kubeconfig through mux,
// optionally overriding Host and X-Forwarded-Proto to match whatever the
// page-level request used (pass "" to leave either at its default). Since
// registry_content.html no longer embeds the kubeconfig directly (see
// handleRegistryKubeconfigDownload's own doc), tests that need to assert on
// its actual contents fetch it separately from the page itself.
func registryKubeconfigBody(t *testing.T, mux *http.ServeMux, host, forwardedProto string) string {
	t.Helper()

	request := newTestRequest(t, "/app/registry/kubeconfig")
	if host != "" {
		request.Host = host
	}

	if forwardedProto != "" {
		request.Header.Set("X-Forwarded-Proto", forwardedProto)
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}

func TestHandleConnectShowsOIDCKubeconfigWhenAuthEnabled(t *testing.T) {
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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/connect"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Contains(t, string(body), ">kubectl<")
	assert.Contains(t, string(body), "KUBECONFIG")
	assert.Contains(t, string(body), "kontinuum config import")
	assert.Contains(t, string(body), `href="/app/registry/kubeconfig"`)
	assert.NotContains(t, string(body), "server: http://example.com",
		"the kubeconfig's own contents must never be rendered into the initial page")

	kubeconfig := registryKubeconfigBody(t, mux, "", "")
	assert.Contains(t, kubeconfig, "server: http://example.com")
	assert.NotContains(t, kubeconfig, "insecure-skip-tls-verify")
	assert.Contains(t, kubeconfig, "name: kontinuum-example.com\n    cluster:")
	assert.Contains(t, kubeconfig, "cluster: kontinuum-example.com")
	assert.Contains(t, kubeconfig, "name: oidc@kontinuum-example.com")
	assert.Contains(t, kubeconfig, "current-context: oidc@kontinuum-example.com")
	assert.Contains(t, kubeconfig, "user: kontinuum-example.com")
	assert.Contains(t, kubeconfig, "name: kontinuum-example.com\n    user:")
	assert.NotContains(t, kubeconfig, "user: oidc\n")
	assert.NotContains(t, kubeconfig, "name: oidc\n")
	assert.Contains(t, kubeconfig, "--oidc-issuer-url="+testOIDCIssuerURL)
	assert.Contains(t, kubeconfig, "--oidc-client-id="+testOIDCClientID)
}

func TestHandleConnectShowsNoAuthKubeconfigWhenOIDCDisabled(t *testing.T) {
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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/connect"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// Kontinuum's default is no authentication at all, not no access — the
	// kubectl access section (and a working kubeconfig) must still show.
	assert.Contains(t, string(body), ">kubectl<")
	assert.Contains(t, string(body), "No authentication is required")
	assert.Contains(t, string(body), "kontinuum config import")
	assert.Contains(t, string(body), `href="/app/registry/kubeconfig"`)
	assert.Contains(t, string(body), "KUBECONFIG")

	kubeconfig := registryKubeconfigBody(t, mux, "", "")
	assert.Contains(t, kubeconfig, "server: http://example.com")
	assert.Contains(t, kubeconfig, "name: kontinuum-example.com\n    cluster:")
	assert.Contains(t, kubeconfig, "cluster: kontinuum-example.com")
	assert.Contains(t, kubeconfig, "current-context: kontinuum-example.com")
	assert.NotContains(t, kubeconfig, "oidc-login")
	assert.NotContains(t, kubeconfig, "users:")
}

// TestHandleConnectHighlightsConnectNavItem guards issue #89's nav item: a
// new "Connect" link, using the Unplug icon, sitting above Logout — and
// confirms the kubectl access card it now owns no longer renders on the
// registry page it used to live on (see TestHandleRegistryRendersInstances
// for that page's own remaining content).
func TestHandleConnectHighlightsConnectNavItem(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	connectRecorder := httptest.NewRecorder()
	mux.ServeHTTP(connectRecorder, newTestRequest(t, "/app/connect"))

	connectResp := connectRecorder.Result()

	connectBody, err := io.ReadAll(connectResp.Body)
	require.NoError(t, err)
	require.NoError(t, connectResp.Body.Close())

	assert.Equal(t, http.StatusOK, connectResp.StatusCode)
	assert.Contains(t, string(connectBody), `href="/app/connect"`)
	assert.Contains(t, string(connectBody), "text-accent bg-neutral-800",
		"the Connect nav link renders active/highlighted on its own page")

	registryRecorder := httptest.NewRecorder()
	mux.ServeHTTP(registryRecorder,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

	registryResp := registryRecorder.Result()

	registryBody, err := io.ReadAll(registryResp.Body)
	require.NoError(t, err)
	require.NoError(t, registryResp.Body.Close())

	assert.Equal(t, http.StatusOK, registryResp.StatusCode)
	assert.NotContains(t, string(registryBody), "kubectl access",
		"the kubectl access card moved to /app/connect and must not remain on the registry page")
	assert.Contains(t, string(registryBody), `href="/app/connect"`,
		"the registry page's own nav must still link to Connect")
}

func TestHandleRegistryStripsPortFromKubeconfigClusterName(t *testing.T) {
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

	kubeconfig := registryKubeconfigBody(t, mux, "example.com:8443", "")
	assert.Contains(t, kubeconfig, "server: http://example.com:8443")
	assert.Contains(t, kubeconfig, "name: kontinuum-example.com\n    cluster:")
	assert.Contains(t, kubeconfig, "cluster: kontinuum-example.com")
	assert.Contains(t, kubeconfig, "name: oidc@kontinuum-example.com")
	assert.NotContains(t, kubeconfig, "example.com:8443\n    cluster:")
	assert.NotContains(t, kubeconfig, "cluster: example.com:8443")
	assert.NotContains(t, kubeconfig, "oidc@example.com:8443")
}

func TestHandleRegistrySetsInsecureSkipTLSVerifyForLocalHostsOverHTTPS(t *testing.T) {
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

		// A TLS-terminating reverse proxy is what would actually front a
		// local deployment reached over https — see requestOrigin.
		kubeconfig := registryKubeconfigBody(t, mux, host, "https")
		assert.Contains(t, kubeconfig, "insecure-skip-tls-verify: true", "host %q", host)
	}
}

func TestHandleRegistryOmitsInsecureSkipTLSVerifyForPlainHTTP(t *testing.T) {
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

		kubeconfig := registryKubeconfigBody(t, mux, host, "")
		assert.NotContains(t, kubeconfig, "insecure-skip-tls-verify", "host %q", host)
	}
}

func TestHandleRegistryUsesForwardedProtoForKubeconfigOrigin(t *testing.T) {
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

	kubeconfig := registryKubeconfigBody(t, mux, "", "https")
	assert.Contains(t, kubeconfig, "server: https://example.com")
	assert.Contains(t, kubeconfig, "name: oidc@kontinuum-example.com")
	assert.NotContains(t, kubeconfig, "insecure-skip-tls-verify")
}

// adminGroupBinding builds a fixture rbacv1.ClusterRoleBinding shaped the
// way pkg/domain/adminrbac's controller creates one — labeled as managed
// and annotated with the OIDC group it grants — for the IAM cluster
// bindings tests.
func adminGroupBinding(name, group string) rbacv1.ClusterRoleBinding {
	return rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{v1alpha2.LabelManagedBy: adminrbac.ManagedByValue},
			Annotations: map[string]string{adminrbac.AdminGroupAnnotation: group},
		},
		RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: adminrbac.RoleName},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: group},
		},
	}
}

func TestHandleIAMNamespaceRolesShowsRoles(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{roles: []rbacv1.Role{
			{
				ObjectMeta: metav1.ObjectMeta{Name: testRoleName, Namespace: testNamespace},
				Rules: []rbacv1.PolicyRule{
					{
						APIGroups: []string{""}, Resources: []string{testRoleRuleResource},
						Verbs: []string{roleRuleFormVerbGet, roleRuleFormVerbList},
					},
				},
			},
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), testRoleName)
	assert.Contains(t, string(body), testRoleRuleResource)
}

func TestHandleIAMNamespaceRoleBindingsShowsBindings(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{roleBindings: []rbacv1.RoleBinding{
			{
				ObjectMeta: metav1.ObjectMeta{Name: testRoleBindingName, Namespace: testNamespace},
				Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, Name: testSubjectName}},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: testRoleName},
			},
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), testRoleBindingName)
	assert.Contains(t, string(body), "Group: sre")
	assert.Contains(t, string(body), testRoleName)
}

func TestHandleIAMNamespaceInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	forbiddenReason := schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"}

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

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, invalidateSession)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

func TestHandleIAMNamespaceShowsNoticeWhenOIDCDisabled(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "OIDC is not configured")
	assert.NotContains(t, string(body), "No roles found")
}

func TestHandleIAMClusterRoleBindingsSeparatesManagedFromOther(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{bindings: []rbacv1.ClusterRoleBinding{
			adminGroupBinding("kontinuum-admin-aaaaaaaaaaaa", "platform-admins"),
			{
				ObjectMeta: metav1.ObjectMeta{Name: "custom-binding"},
				Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "jane"}},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: testClusterRoleName},
			},
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterrolebindings"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "Managed by OIDC admin group config")
	assert.Contains(t, string(body), "platform-admins")
	assert.Contains(t, string(body), "custom-binding")
	assert.Contains(t, string(body), "jane")
}

// TestHandleIAMClusterRolesHidesDeleteButtonForManagedRole guards against
// the "kontinuum-admin" ClusterRole being deletable from the UI —
// pkg/domain/adminrbac's own reconcile loop just recreates it within its
// own interval (default 30s) if it's gone, so offering a delete button for
// it would only produce a confusing "it deleted, then came back" result
// rather than actually removing anything, the same reasoning
// TestHandleIAMClusterRoleBindingsSeparatesManagedFromOther's own managed
// binding is exempted from a delete button for.
func TestHandleIAMClusterRolesHidesDeleteButtonForManagedRole(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{clusterRoles: []rbacv1.ClusterRole{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:   adminrbac.RoleName,
					Labels: map[string]string{v1alpha2.LabelManagedBy: adminrbac.ManagedByValue},
				},
			},
			{ObjectMeta: metav1.ObjectMeta{Name: testClusterRoleName}},
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterroles"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), adminrbac.RoleName)
	assert.NotContains(t, string(body), `aria-label="Delete `+adminrbac.RoleName+`"`)
	assert.Contains(t, string(body), `aria-label="Delete custom-role"`)
}

func TestHandleRoleAddCreatesRoleAndReturnsSuccessFragment(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var created []client.Object

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{created: &created}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{
		roleAddFormNameKey:          {testRoleName},
		"rules[0][apiGroups]":       {""},
		roleAddFormRuleResourcesKey: {testRoleRuleResource},
		"rules[0][verbs]":           {roleRuleFormVerbGet, roleRuleFormVerbList},
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestFormRequest(t,
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles", form))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "Created role")
	assert.Contains(t, string(body), testRoleName)

	require.Len(t, created, 1)

	role, ok := created[0].(*rbacv1.Role)
	require.True(t, ok)
	assert.Equal(t, testRoleName, role.Name)
	assert.Equal(t, testNamespace, role.Namespace)
	require.Len(t, role.Rules, 1)
	assert.Equal(t, []string{""}, role.Rules[0].APIGroups)
	assert.Equal(t, []string{testRoleRuleResource}, role.Rules[0].Resources)
	assert.ElementsMatch(t, []string{roleRuleFormVerbGet, roleRuleFormVerbList}, role.Rules[0].Verbs)
}

func TestHandleRoleAddRequiresName(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var created []client.Object

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{created: &created}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{roleAddFormRuleResourcesKey: {testRoleRuleResource}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestFormRequest(t,
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles", form))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "Name is required")
	assert.Empty(t, created)
}

func TestHandleClusterRoleAddCreatesClusterRole(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var created []client.Object

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{created: &created}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{
		roleAddFormNameKey:          {testClusterRoleName},
		roleAddFormRuleResourcesKey: {"nodes"},
		"rules[0][verbs]":           {"*"},
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestFormRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterroles", form))

	resp := recorder.Result()

	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Len(t, created, 1)

	clusterRole, ok := created[0].(*rbacv1.ClusterRole)
	require.True(t, ok)
	assert.Equal(t, testClusterRoleName, clusterRole.Name)
	assert.Empty(t, clusterRole.Namespace)
	require.Len(t, clusterRole.Rules, 1)
	assert.Equal(t, []string{"nodes"}, clusterRole.Rules[0].Resources)
}

func TestHandleRoleBindingAddCreatesRoleBinding(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var created []client.Object

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			created: &created,
			roles:   []rbacv1.Role{{ObjectMeta: metav1.ObjectMeta{Name: testRoleName, Namespace: testNamespace}}},
		}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{
		roleAddFormNameKey:               {testRoleBindingName},
		rolebindingAddFormSubjectKindKey: {"Group"},
		rolebindingAddFormSubjectNameKey: {testSubjectName},
		"role-ref":                       {testRoleName},
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestFormRequest(t,
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings", form))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "Created role binding")

	require.Len(t, created, 1)

	binding, ok := created[0].(*rbacv1.RoleBinding)
	require.True(t, ok)
	assert.Equal(t, testRoleBindingName, binding.Name)
	assert.Equal(t, testNamespace, binding.Namespace)
	require.Len(t, binding.Subjects, 1)
	assert.Equal(t, rbacv1.GroupKind, binding.Subjects[0].Kind)
	assert.Equal(t, testSubjectName, binding.Subjects[0].Name)
	assert.Equal(t, "Role", binding.RoleRef.Kind)
	assert.Equal(t, testRoleName, binding.RoleRef.Name)
}

func TestHandleRoleBindingAddRequiresRoleRef(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var created []client.Object

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			created: &created,
			roles:   []rbacv1.Role{{ObjectMeta: metav1.ObjectMeta{Name: testRoleName, Namespace: testNamespace}}},
		}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{
		roleAddFormNameKey:               {testRoleBindingName},
		rolebindingAddFormSubjectKindKey: {"Group"},
		rolebindingAddFormSubjectNameKey: {testSubjectName},
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestFormRequest(t,
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings", form))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "A role is required")
	assert.Empty(t, created)
}

func TestHandleClusterRoleBindingAddCreatesClusterRoleBinding(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	var created []client.Object

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			created:      &created,
			clusterRoles: []rbacv1.ClusterRole{{ObjectMeta: metav1.ObjectMeta{Name: testClusterRoleName}}},
		}, nil
	}

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{
		roleAddFormNameKey:               {"custom-binding"},
		rolebindingAddFormSubjectKindKey: {"User"},
		rolebindingAddFormSubjectNameKey: {"jane"},
		"role-ref":                       {testClusterRoleName},
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestFormRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterrolebindings", form))

	resp := recorder.Result()

	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Len(t, created, 1)

	binding, ok := created[0].(*rbacv1.ClusterRoleBinding)
	require.True(t, ok)
	assert.Equal(t, "custom-binding", binding.Name)
	require.Len(t, binding.Subjects, 1)
	assert.Equal(t, rbacv1.UserKind, binding.Subjects[0].Kind)
	assert.Equal(t, "jane", binding.Subjects[0].Name)
	assert.Equal(t, "ClusterRole", binding.RoleRef.Kind)
	assert.Equal(t, testClusterRoleName, binding.RoleRef.Name)
}

func TestHandleDeleteRoleRemovesRoleAndRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t,
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles/pod-reader",
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles")
}

func TestHandleDeleteRoleReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles/pod-reader"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteRoleInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/roles/pod-reader"), kontinuumFactory)
}

func TestHandleDeleteRoleBindingRemovesBindingAndRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t,
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings/pod-reader-binding",
		"/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings")
}

func TestHandleDeleteRoleBindingReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder,
		newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings/pod-reader-binding"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteRoleBindingInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: rbacv1.GroupName, Resource: "rolebindings"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings/pod-reader-binding"),
		kontinuumFactory)
}

func TestHandleDeleteClusterRoleRemovesRoleAndRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t,
		"/app/rbac.authorization.k8s.io/v1/clusterroles/custom-role",
		"/app/rbac.authorization.k8s.io/v1/clusterroles")
}

func TestHandleDeleteClusterRoleReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterroles/custom-role"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteClusterRoleInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterroles"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterroles/custom-role"), kontinuumFactory)
}

func TestHandleDeleteClusterRoleBindingRemovesBindingAndRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t,
		"/app/rbac.authorization.k8s.io/v1/clusterrolebindings/custom-binding",
		"/app/rbac.authorization.k8s.io/v1/clusterrolebindings")
}

func TestHandleDeleteClusterRoleBindingReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t,
		"/app/rbac.authorization.k8s.io/v1/clusterrolebindings/custom-binding"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteClusterRoleBindingInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterrolebindings"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/rbac.authorization.k8s.io/v1/clusterrolebindings/custom-binding"), kontinuumFactory)
}

func TestRegisterRoutesDefaultsToUnconditionalAppRedirect(t *testing.T) {
	t.Parallel()

	mux := newRedirectTestMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances", resp.Header.Get("Location"))
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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kontinuum-system"},
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

	notReadyZone := zoneWithCondition("eu-eu-1a", "eu", testZoneValue, metav1.ConditionFalse, "WaitingForCertificate",
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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a")
	assert.Contains(t, string(body), ">eu<")
	assert.Contains(t, string(body), ">Installed<")
	assert.Contains(t, string(body), "Waiting for cert-manager to issue",
		"the condition message's first letter is capitalized")
	assert.Contains(t, string(body), "bg-neutral-800", "a False condition renders a grey badge")
	assert.Contains(t, string(body), "bg-green-900/40", "a True condition renders a green badge")
}

// TestHandleRegistryRendersDeletingForZoneWithDeletionTimestamp covers a
// Zone mid-teardown: its own last-transitioned condition (e.g.
// "Installed=True") is left over from before deletion started and would
// otherwise read as "everything's fine" — the row must show "Deleting"
// instead, not that stale condition.
func TestHandleRegistryRendersDeletingForZoneWithDeletionTimestamp(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	deletingZone := zoneWithCondition("eu-eu-1a", "eu", testZoneValue, metav1.ConditionTrue, "Installed",
		"kontinuum-server installed")
	now := metav1.Now()
	deletingZone.DeletionTimestamp = &now
	deletingZone.Finalizers = []string{"kontinuum.sh/zone-teardown"}

	zoneClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deletingZone).Build()
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Deleting")
	assert.NotContains(t, string(body), "Installed=True")
}

// TestHandleRegistryOmitsZonesFromOtherTenants guards against the registry
// page leaking another tenant's Zone objects: Zone is namespace-scoped (see
// api/v1alpha2/zone_types.go), so listZoneRows must filter by the request's
// own {ns} path value the same way renderRegistry already does for
// Kontinuum instances.
func TestHandleRegistryOmitsZonesFromOtherTenants(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	ownZone := zoneWithCondition("eu-eu-1a", "eu", "eu-1a", metav1.ConditionTrue, "Installed",
		"kontinuum-server installed")
	ownZone.Namespace = "tenant-a"
	otherZone := zoneWithCondition("us-us-1a", "us", "us-1a", metav1.ConditionTrue, "Installed",
		"kontinuum-server installed")
	otherZone.Namespace = "tenant-b"

	zoneClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ownZone, otherZone).Build()
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/tenant-a/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a", "tenant-a's own zone must still render")
	assert.NotContains(t, string(body), "us-us-1a", "tenant-b's zone must never render on tenant-a's page")
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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

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

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: kontinuumsResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{err: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"), kontinuumFactory)
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
	mux.ServeHTTP(recorder, newTestDeleteRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/demo"))

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
	mux.ServeHTTP(recorder, newTestDeleteRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/demo"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteInstanceInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: kontinuumsResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/demo"), kontinuumFactory)
}

// newTestZoneClient builds a real fake controller-runtime client with
// kontinuum.sh/v1alpha2 registered, for tests that need handleZoneAdd's
// pkg/domain/zone.Add call to actually create objects.
func newTestZoneClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

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
	return apierrors.NewForbidden(schema.GroupResource{Group: v1alpha2.GroupName, Resource: zonesResource}, "",
		errTestForbidden)
}

// List is also overridden: Add's own domain inference (see
// AddOptions.Domain's doc) lists Kontinuums before ever reaching Create,
// so that's the first call a Forbidden hub actually rejects.
func (forbiddenZoneClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: v1alpha2.GroupName, Resource: kontinuumsResource}, "",
		errTestForbidden)
}

// Delete is overridden for handleDeleteZone's own forbidden test — the only
// call that handler makes through client.Client.
func (forbiddenZoneClient) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: v1alpha2.GroupName, Resource: zonesResource}, "",
		errTestForbidden)
}

// errorZoneClient is a client.Client test double whose Delete always
// returns err — used to exercise handleDeleteZone's own bad-gateway path on
// a non-Forbidden, non-NotFound failure.
type errorZoneClient struct {
	client.Client

	err error
}

func (e errorZoneClient) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return e.err
}

// getErrorZoneClient is a client.Client test double whose Get always
// returns err — used to exercise handleZoneDetail's own bad-gateway/
// forbidden paths, mirroring errorZoneClient's identical role for Delete.
type getErrorZoneClient struct {
	client.Client

	err error
}

func (g getErrorZoneClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return g.err
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
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

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
	// No suggestions (zoneFactory's own empty fake client) means no
	// listbox is rendered at all — the address input must not declare
	// combobox semantics pointing at an element that doesn't exist.
	assert.NotContains(t, string(body), `role="combobox"`)
	assert.NotContains(t, string(body), `id="zone-add-instance-list"`)
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

	form := zoneAddForm()

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), testTalosClusterName)
	// The form itself is gone from a success response — nothing left to
	// resubmit.
	assert.NotContains(t, string(body), `name="talos-address"`)
	// The toast marker's kubectl command is split into its own attribute
	// (see zone_add_modal.html) so showToast can render it as a
	// click-to-copy chip rather than plain text — not folded into one
	// combined message string.
	assert.Contains(t, string(body), `data-toast-command="kubectl get zone `+testTalosClusterName+`"`)
	assert.Contains(t, string(body), `data-toast-prefix="Created zone `+testTalosClusterName+`.`)

	var got v1alpha2.Zone

	zoneKey := client.ObjectKey{Name: testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace}
	require.NoError(t, zoneClient.Get(context.Background(), zoneKey, &got))
	assert.Equal(t, "example.com", got.Spec.Domain)
}

// TestHandleZoneAddThreadsUnregisterInstancesCheckboxToClusterSpec covers
// the "Unregister instances on decommissioning" checkbox's own path all
// the way onto the created TalosCluster's spec.teardown.unregisterInstances
// — the field TalosClusterFinalizer's own teardown actually reads.
func TestHandleZoneAddThreadsUnregisterInstancesCheckboxToClusterSpec(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneClient := newTestZoneClient(t, registeredKontinuumWithDomain("hub", "example.com"))
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{
		zoneAddFormRegionKey: {"eu"}, zoneAddFormZoneKey: {testZoneValue}, zoneAddFormTalosAddressKey: {testTalosAddress},
		"unregister-instances": {"on"},
	}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got v1alpha2.TalosCluster

	clusterKey := client.ObjectKey{Name: testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace}
	require.NoError(t, zoneClient.Get(context.Background(), clusterKey, &got))
	assert.True(t, got.Spec.Teardown.UnregisterInstances)
}

// TestHandleZoneAddPreservesUnregisterInstancesCheckboxOnValidationError
// covers the same "don't make the user retype everything" guarantee
// TestHandleZoneAddRerendersFormOnValidationError already covers for the
// text fields, extended to the checkbox: a validation failure re-renders
// the form with it still checked, not silently reset to unticked.
func TestHandleZoneAddPreservesUnregisterInstancesCheckboxOnValidationError(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zonesFactory := func(context.Context) (client.Client, error) { return newTestZoneClient(t), nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	// talos-address deliberately omitted — Add's own validation rejects it.
	form := url.Values{zoneAddFormRegionKey: {"eu"}, zoneAddFormZoneKey: {testZoneValue}, "unregister-instances": {"on"}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Regexp(t, `name="unregister-instances"[^>]*checked`, string(body))
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
	form := url.Values{zoneAddFormRegionKey: {"eu"}, zoneAddFormZoneKey: {testZoneValue}}

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

	form := zoneAddForm()

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

	form := zoneAddForm()

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

func newTestZoneDeleteRequest(t *testing.T, namespace, name string) *http.Request {
	t.Helper()

	return newTestDeleteRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/"+namespace+"/zones/"+name)
}

func TestHandleDeleteZoneRemovesZoneAndRerendersRegistry(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneClient := newTestZoneClient(t, &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneValue, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneDeleteRequest(t, "kontinuum-system", testZoneValue))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "No zones found")

	var got v1alpha2.Zone

	zoneKey := client.ObjectKey{Name: testZoneValue, Namespace: v1alpha2.KontinuumSystemNamespace}
	assert.True(t, apierrors.IsNotFound(zoneClient.Get(context.Background(), zoneKey, &got)))
}

func TestHandleDeleteZoneReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return errorZoneClient{err: errFactory}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneDeleteRequest(t, "kontinuum-system", testZoneValue))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteZoneInvalidatesSessionOnForbidden(t *testing.T) {
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

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneDeleteRequest(t, "kontinuum-system", testZoneValue))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

// TestHandleZoneDetailRendersOverviewClusterAndRegistry covers the happy
// path: a Zone whose owning TalosCluster exists and is Ready, and whose own
// kontinuum-server has actually joined the hub's registry (see
// zonedomain.FindJoinedKontinuum) — the state issue #95 asked this page to
// surface directly.
func TestHandleZoneDetailRendersOverviewClusterAndRegistry(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: testZoneValue, Domain: testExampleDomain},
		Status: v1alpha2.ZoneStatus{
			Conditions: []metav1.Condition{
				{
					Type: testReadyConditionType, Status: metav1.ConditionTrue, Reason: "RegistryJoined",
					Message: "kontinuum-server for this zone is registered and heartbeating",
				},
			},
		},
	}
	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status: v1alpha2.TalosClusterStatus{
			Conditions: []metav1.Condition{
				{Type: testReadyConditionType, Status: metav1.ConditionTrue, Reason: "AddonsInstalled"},
			},
		},
	}
	kontinuum := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a-worker", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.KontinuumSpec{Region: "eu", Zone: testZoneValue},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker, LastHeartbeatTime: metav1.Now()},
	}

	zoneClient := newTestZoneClient(t, zoneObj, cluster, kontinuum)
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a")
	assert.Contains(t, string(body), "example.com")
	assert.Contains(t, string(body),
		`href="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"`)
	assert.Contains(t, string(body),
		`href="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums/eu-1a-worker"`)
	assert.Contains(t, string(body), "Worker")
	assert.NotContains(t, string(body), "Not yet joined")
	assert.NotContains(t, string(body), "Not provisioned yet")
	assert.Contains(t, string(body), "RegistryJoined", "the Ready condition's own reason, rendered by conditions-table")
}

// TestHandleZoneDetailShowsNotProvisionedAndNotJoinedWhenMissing covers a
// freshly created Zone: zone-add's own fan-out hasn't finished yet (no
// TalosCluster sharing its name), and this zone's own kontinuum-server
// hasn't joined the registry (or, per issue #95, may never even manage to
// start — see zone.AuthConfig's own doc) yet either.
func TestHandleZoneDetailShowsNotProvisionedAndNotJoinedWhenMissing(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: testZoneValue, Domain: testExampleDomain},
	}

	zoneClient := newTestZoneClient(t, zoneObj)
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Not provisioned yet")
	assert.Contains(t, string(body), "Not yet joined")
	assert.Contains(t, string(body), "No conditions reported yet.")
}

// TestHandleZoneDetailRedirectsToParentForUnknownZone: Zone has no
// dedicated list page of its own (unlike TalosCluster/Instance), so the one
// notFoundFallback hop this asserts lands on
// .../namespaces/{ns}/zones — itself unmapped, walked past by a real
// browser's own next redirect (see notFoundFallback's own doc), but this is
// the correct first hop regardless.
func TestHandleZoneDetailRedirectsToParentForUnknownZone(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/missing",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones")
}

// TestHandleZoneDetailPollSendsHxRedirectForDeletedZone is
// TestHandleTalosClusterDetailPollSendsHxRedirectForDeletedCluster's own
// counterpart for the zone detail page's identical 15s poll.
func TestHandleZoneDetailPollSendsHxRedirectForDeletedZone(t *testing.T) {
	t.Parallel()

	assertHTMXGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/missing",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones")
}

func TestHandleZoneDetailInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: zonesResource}
	zonesFactory := func(context.Context) (client.Client, error) {
		return getErrorZoneClient{err: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	var invalidatedWith string

	invalidateSession := func(writer http.ResponseWriter, _ *http.Request, message string) {
		invalidatedWith = message

		writer.WriteHeader(http.StatusFound)
	}

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, invalidateSession)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

func TestHandleZoneDetailReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return getErrorZoneClient{err: errFactory}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

// TestRegistryPageLinksZoneRowToDetailPage covers the registry page's own
// zones table — see registry_content.html's own zone-name cell.
func TestRegistryPageLinksZoneRowToDetailPage(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneClient := newTestZoneClient(t, &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneValue, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body),
		`href="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/zones/`+testZoneValue+`"`)
}

func TestRegistryPageEmbedsLeaveZoneButtonAndModal(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	zoneClient := newTestZoneClient(t, &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneValue, Namespace: v1alpha2.KontinuumSystemNamespace},
	})
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), `onclick="openZoneLeaveModal('eu-1a', 'kontinuum-system')"`)
	assert.Contains(t, string(body), `id="zone-leave-modal"`)
	assert.Contains(t, string(body), `id="zone-leave-modal-confirm" disabled`)
}

// TestRegistryPageRendersInstanceSuggestionsInDropdown covers the "Add
// zone" modal's own instance-picker (see instanceSuggestion's own doc):
// an unclaimed Instance in v1alpha2.KontinuumSystemNamespace is offered as
// a suggestion, but one already claimed by some other pool is not.
func TestRegistryPageRendersInstanceSuggestionsInDropdown(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	unclaimed := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "instance-unclaimed", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{testExistingInstanceAddress}},
	}
	claimed := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "instance-claimed", Namespace: v1alpha2.KontinuumSystemNamespace,
			Labels: map[string]string{v1alpha2.LabelClaimedBy: "some-pool"},
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.10"}},
	}

	zoneClient := newTestZoneClient(t, unclaimed, claimed)
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/kontinuums"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), `data-instance-name="instance-unclaimed"`)
	assert.Contains(t, string(body), `data-instance-address="`+testExistingInstanceAddress+`"`)
	assert.NotContains(t, string(body), `data-instance-name="instance-claimed"`)
	// The instance-picker's own accessibility wiring (see zone_add_modal.html's
	// own doc: DOM focus never leaves the address input — options are
	// tabindex="-1" so Tab moves to the next form field instead of
	// stepping through each suggestion — arrow keys highlight a "virtual"
	// active option via aria-selected/aria-activedescendant instead).
	assert.Contains(t, string(body), `role="combobox"`)
	assert.Contains(t, string(body), `aria-controls="zone-add-instance-list"`)
	assert.Contains(t, string(body), `role="listbox"`)
	assert.Contains(t, string(body), `role="option"`)
	assert.Contains(t, string(body), `tabindex="-1"`)
}

// TestHandleZoneAddAdoptsExistingInstanceInstead covers the "Add zone"
// modal's own instance-picker end to end through the router: submitting
// "existing-instance" alongside the usual form fields must adopt that
// already-registered Instance (relabeling it into the new zone) instead of
// creating a second, brand-new one from talos-address — see
// zone.AddOptions.ExistingInstanceName's own doc.
func TestHandleZoneAddAdoptsExistingInstanceInstead(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	existing := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "instance-preexisting", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{testExistingInstanceAddress}},
	}

	zoneClient := newTestZoneClient(t, existing, registeredKontinuumWithDomain("hub", "example.com"))
	zonesFactory := func(context.Context) (client.Client, error) { return zoneClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := zoneAddForm()
	form.Set("existing-instance", existing.Name)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestZoneAddRequest(t, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var list v1alpha2.InstanceList
	require.NoError(t,
		zoneClient.List(context.Background(), &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace)))
	require.Len(t, list.Items, 1, "adopting an existing instance must not also create a new one")
	assert.Equal(t, existing.Name, list.Items[0].Name)
	assert.Equal(t, "eu", list.Items[0].Labels[v1alpha2.LabelRegion])
	assert.Equal(t, testZoneValue, list.Items[0].Labels[v1alpha2.LabelZone])
	assert.Equal(t, []string{testExistingInstanceAddress}, list.Items[0].Spec.Interfaces)
}

// newTestInstanceAddRequest builds a POST
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/add request — the "Add
// instance" form's own submission target (see instance_add_modal.html).
func newTestInstanceAddRequest(t *testing.T, namespace string, form url.Values) *http.Request {
	t.Helper()

	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/app/kontinuum.sh/v1alpha2/namespaces/"+namespace+"/instances/add", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return request
}

func TestInstancesPageEmbedsAddInstanceButtonAndEmptyModal(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "openInstanceAddModal()")
	assert.Contains(t, string(body), `id="instance-add-modal"`)
	assert.Contains(t, string(body), `name="address"`)
	// A successful submission refreshes #instances-content right away (see
	// instances_content.html's own htmx.trigger call) rather than leaving
	// the new row invisible until the next 15s poll — this is the trigger
	// that wires the two together.
	assert.Contains(t, string(body), `hx-trigger="every 15s, instance-added from:body"`)
	// The submit button's own loading state is declarative (see AGENTS.md's
	// UI policy) rather than a hand-rolled htmx:beforeRequest/afterRequest
	// listener: hx-disabled-elt disables it for the request's own round
	// trip, and the spinner span is keyed off the "htmx-request" class htmx
	// adds to the button itself while in flight.
	assert.Contains(t, string(body), `hx-disabled-elt="this"`)
	assert.Contains(t, string(body), `[.htmx-request_&]:inline-flex`)
}

// TestHandleInstanceAddCreatesInstanceAndReturnsSuccessFragment also covers
// namespace-scoping (see instance.AddOptions.Namespace's own doc: a tenant
// can bring their own hardware into their own namespace) by submitting
// against a tenant namespace other than kontinuum-system.
func TestHandleInstanceAddCreatesInstanceAndReturnsSuccessFragment(t *testing.T) {
	t.Parallel()

	const tenantNamespace = "acme"

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	instClient := newTestZoneClient(t)
	zonesFactory := func(context.Context) (client.Client, error) { return instClient, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{instanceAddFormAddressKey: {testTalosAddress}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestInstanceAddRequest(t, tenantNamespace, form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Registered instance")
	// The form itself is gone from a success response — nothing left to
	// resubmit.
	assert.NotContains(t, string(body), `name="address"`)

	var list v1alpha2.InstanceList
	require.NoError(t, instClient.List(context.Background(), &list, client.InNamespace(tenantNamespace)))
	require.Len(t, list.Items, 1)
	assert.Equal(t, []string{testTalosAddress}, list.Items[0].Spec.Interfaces)
	assert.Empty(t, list.Items[0].Labels[v1alpha2.LabelClaimedBy])
}

func TestHandleInstanceAddRerendersFormOnValidationError(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return newTestZoneClient(t), nil }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	// address deliberately omitted — Add's own validation rejects it.
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestInstanceAddRequest(t, "kontinuum-system", url.Values{}))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), `name="address"`)
}

func TestHandleInstanceAddReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }
	zonesFactory := func(context.Context) (client.Client, error) { return nil, errFactory }

	router := ui.NewRouter(factory, kontinuumFactory, zonesFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	form := url.Values{instanceAddFormAddressKey: {testTalosAddress}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestInstanceAddRequest(t, "kontinuum-system", form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleInstanceAddInvalidatesSessionOnForbidden(t *testing.T) {
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

	form := url.Values{instanceAddFormAddressKey: {testTalosAddress}}

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestInstanceAddRequest(t, "kontinuum-system", form))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotEmpty(t, invalidatedWith)
}

// discoveredInstance builds a v1alpha2.Instance with the Discovered
// condition set true, name and talosVersion as given — the shape
// TestHandleMachinesRendersInstances/TestHandleMachineDetail* both need.
func discoveredInstance(name, talosVersion string) v1alpha2.Instance {
	return v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha2.InstanceStatus{
			Talos: v1alpha2.InstanceTalosStatus{Version: talosVersion},
			Interfaces: []v1alpha2.InstanceInterfaceStatus{
				{Name: "eth0", MACAddress: "aa:bb:cc:dd:ee:ff", Addresses: []string{"10.0.0.5/24"}},
			},
			Disks: []v1alpha2.InstanceDiskStatus{
				{DevPath: "/dev/sda", PrettySize: "512 GB", Model: "Samsung SSD", Transport: "nvme"},
			},
			CPUs: []v1alpha2.InstanceCPUStatus{
				{ProductName: "AMD EPYC 7302P", Architecture: "amd64", CoreCount: 16, ThreadCount: 32, MaxSpeedMHz: 3000},
			},
			Memory: []v1alpha2.InstanceMemoryStatus{
				{SizeMiB: 32768, Manufacturer: "Micron", Speed: 3200},
			},
			SerialNumber: "SN-1234567890",
			Conditions: []metav1.Condition{
				{
					Type: "Discovered", Status: metav1.ConditionTrue,
					Reason: "Discovered", Message: "discovered via 10.0.0.5",
				},
			},
		},
	}
}

func TestHandleMachinesRendersInstances(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }

	claimed := discoveredInstance("node-a", "v1.9.0")
	claimed.Labels = map[string]string{v1alpha2.LabelClaimedBy: "control-plane"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{instances: []v1alpha2.Instance{
			claimed,
			{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "node-a")
	assert.Contains(t, string(body), "v1.9.0")
	assert.Contains(t, string(body), "control-plane")
	assert.Contains(t, string(body), "node-b")
	assert.Contains(t, string(body), "Unclaimed")
}

// deletingInstanceMux builds a router+mux serving a single Instance named
// node-a with its own DeletionTimestamp set (as if InstanceResetFinalizer
// were still resetting it back to maintenance mode) but its stale
// Discovered=True condition from before deletion started left untouched —
// shared by the list/detail "Deleting" tests below, the same
// one-fixture-two-routes shape as newTalosClusterKubeconfigMux.
func deletingInstanceMux(t *testing.T) *http.ServeMux {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }

	deleting := discoveredInstance("node-a", "v1.9.0")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"kontinuum.sh/talos-reset"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{instances: []v1alpha2.Instance{deleting}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	return mux
}

// assertRendersDeletingNotDiscovered asserts body renders the amber
// "Deleting" badge instead of a green "Discovered" one in that same
// title/list-row spot — shared by the Instance list/detail "Deleting"
// tests below. wantConditionPill is how many *other* green "Discovered"
// pills body is still allowed to carry: the detail page's own conditions
// table (see "conditions-table") always shows the raw, unfiltered
// status.conditions list — same as the TalosCluster detail page's own
// conditions table — so it keeps rendering this stale Discovered=True
// condition as a green pill even once deletion has started; the list page
// has no such table, so it allows none.
func assertRendersDeletingNotDiscovered(t *testing.T, body string, wantConditionPill int) {
	t.Helper()

	assert.Contains(t, body, "bg-amber-900/40", "Deleting must render as the amber badge")
	assert.Contains(t, body, ">Deleting<")
	assert.Equal(t, wantConditionPill, strings.Count(body, "text-green-300\">Discovered<"),
		"the title/list-row spot must show Deleting, not a green Discovered pill")
}

// TestHandleMachinesRendersDeletingForInstanceWithDeletionTimestamp covers
// an Instance mid-reset (InstanceResetFinalizer still resetting it back to
// maintenance mode): its own Discovered=True condition is left over from
// before deletion started — the row must show "Deleting" instead of
// "Discovered".
func TestHandleMachinesRendersDeletingForInstanceWithDeletionTimestamp(t *testing.T) {
	t.Parallel()

	mux := deletingInstanceMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assertRendersDeletingNotDiscovered(t, string(body), 0)
}

func TestHandleMachinesShowsEmptyState(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "No instances found")
}

func TestHandleMachinesReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return nil, errFactory }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleMachinesInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: instancesResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{instanceErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	request := newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances")
	assertForbiddenInvalidatesSession(t, request, kontinuumFactory)
}

func TestHandleMachineDetailRendersInstance(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }

	claimed := discoveredInstance("node-a", "v1.9.0")
	claimed.Labels = map[string]string{v1alpha2.LabelClaimedBy: "control-plane", "kontinuum.sh/zone": "zone-a"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{instances: []v1alpha2.Instance{claimed}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "node-a")
	assert.Contains(t, string(body), "v1.9.0")
	assert.Contains(t, string(body), "eth0")
	assert.Contains(t, string(body), "aa:bb:cc:dd:ee:ff")
	assert.Contains(t, string(body), "10.0.0.5/24")
	assert.Contains(t, string(body), "control-plane")
	assert.Contains(t, string(body), "Discovered via 10.0.0.5")
	assert.Contains(t, string(body), "kontinuum.sh/zone")
	assert.Contains(t, string(body), "zone-a")
	assert.Contains(t, string(body), "/dev/sda")
	assert.Contains(t, string(body), "512 GB")
	assert.Contains(t, string(body), "AMD EPYC 7302P")
	assert.Contains(t, string(body), "amd64")
	assert.Contains(t, string(body), "32768 MiB")
	assert.Contains(t, string(body), "SN-1234567890")
}

// TestHandleMachineDetailRendersDeletingForInstanceWithDeletionTimestamp
// covers the same "Deleting" override as the list-page test above, on the
// detail page's own title-bar badge — its conditions table still shows the
// stale Discovered=True condition as its own green pill (see
// assertRendersDeletingNotDiscovered's own doc), which is why this expects
// exactly one, not zero, occurrences.
func TestHandleMachineDetailRendersDeletingForInstanceWithDeletionTimestamp(t *testing.T) {
	t.Parallel()

	mux := deletingInstanceMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a"))

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assertRendersDeletingNotDiscovered(t, string(body), 1)
}

func TestHandleMachineDetailRedirectsToListForUnknownInstance(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/does-not-exist",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances")
}

// TestHandleMachineDetailPollSendsHxRedirectForDeletedInstance covers a bug
// a plain http.Redirect can't fix: a still-open instance detail page's own
// 15s poll (see instance_detail_content.html) for an Instance deleted out
// from under it must send the browser to the instances list via
// Hx-Redirect, which htmx itself turns into a real navigation — not a 3xx
// the poll's own XHR/fetch layer would just follow transparently in place,
// landing on content with no matching hx-select target and leaving the
// caller stuck looking at a stale page for an object that's already gone.
func TestHandleMachineDetailPollSendsHxRedirectForDeletedInstance(t *testing.T) {
	t.Parallel()

	assertHTMXGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/does-not-exist",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances")
}

func TestHandleMachineDetailReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return nil, errFactory }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleMachineDetailInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: instancesResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			instanceGetErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden),
		}, nil
	}

	request := newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a")
	assertForbiddenInvalidatesSession(t, request, kontinuumFactory)
}

// assertGetRedirectsTo issues a GET to path against a router with an empty
// KontinuumClient (so every object lookup 404s) and asserts the response is
// a plain (non-htmx) redirect to wantLocation — shared by every
// notFoundFallback test below that otherwise only differs in which path it
// starts from and where that's expected to land.
func assertGetRedirectsTo(t *testing.T, path, wantLocation string) {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, path))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, wantLocation, resp.Header.Get("Location"))
}

// assertHTMXGetRedirectsTo issues an htmx-flavored GET (HX-Request: true —
// the header every htmx-driven request, including a page's own "every 15s"
// poll, carries) to path against a router with an empty KontinuumClient (so
// every object lookup 404s), and asserts the response answers with
// Hx-Redirect to wantLocation instead of a classic 3xx — see
// notFoundFallback's own doc for why a plain redirect can't steer an AJAX
// poll the way it steers a real browser navigation.
func assertHTMXGetRedirectsTo(t *testing.T, path, wantLocation string) {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	request := newTestRequest(t, path)
	request.Header.Set("Hx-Request", "true")

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, wantLocation, resp.Header.Get("Hx-Redirect"))
	assert.Empty(t, resp.Header.Get("Location"), "an htmx-driven redirect must not also be a classic 3xx")
}

// assertDeleteRedirectsToList issues a DELETE to path against a router whose
// KontinuumClient always succeeds, and asserts the response redirects the
// browser to wantRedirect via Hx-Redirect — shared by
// TestHandleDeleteMachineRemovesInstanceAndRedirectsToList and
// TestHandleDeleteTalosClusterRemovesClusterAndRedirectsToList below, which
// are otherwise identical apart from which object kind's delete route they
// hit.
func assertDeleteRedirectsToList(t *testing.T, path, wantRedirect string) {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t, path))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, wantRedirect, resp.Header.Get("Hx-Redirect"))
}

func TestHandleDeleteMachineRemovesInstanceAndRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances")
}

func TestHandleDeleteMachineReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestDeleteRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteMachineInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: instancesResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/instances/node-a"), kontinuumFactory)
}

// talosClusterFixture builds a TalosCluster fixture named
// testTalosClusterName, with a control-plane pool, one named worker pool,
// and a Ready condition — shared by the clusters list/detail tests below.
func talosClusterFixture(ready metav1.ConditionStatus) v1alpha2.TalosCluster {
	return v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			Talos:      v1alpha2.TalosSpec{Version: "v1.13.0"},
			Kubernetes: v1alpha2.KubernetesSpec{Version: "v1.32.0"},
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: testTalosClusterName},
			},
			Workers: []v1alpha2.TalosClusterWorkerSpec{
				{Name: "default", PoolRef: v1alpha2.InstancePoolReference{Name: testTalosClusterName + "-default"}},
			},
		},
		Status: v1alpha2.TalosClusterStatus{
			Conditions: []metav1.Condition{
				{
					Type: testReadyConditionType, Status: ready, Reason: "ClusterReady", Message: "cluster is ready",
					LastTransitionTime: metav1.Now(),
				},
			},
			SecretRef: v1alpha2.SecretReference{
				Name: "taloscluster-" + testTalosClusterName, Namespace: v1alpha2.KontinuumSystemNamespace,
			},
		},
	}
}

func TestHandleTalosClustersRendersList(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusters: []v1alpha2.TalosCluster{
			talosClusterFixture(metav1.ConditionTrue),
		}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a")
	assert.Contains(t, string(body), "v1.13.0")
	assert.Contains(t, string(body), "v1.32.0")
	assert.Contains(t, string(body), "text-green-300\">Ready<")
	assert.Contains(t, string(body),
		`href="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"`)
	assert.Contains(t, string(body), `href="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters"`,
		"the nav's own Clusters link")
}

// deletingTalosClusterMux builds a router+mux serving a single TalosCluster
// named testTalosClusterName with its own DeletionTimestamp set (as if
// TalosClusterFinalizer were still tearing it down) but its stale Ready
// condition from before deletion started left untouched — shared by the
// list/detail "Deleting" tests below, the same one-fixture-two-routes
// shape as newTalosClusterKubeconfigMux.
func deletingTalosClusterMux(t *testing.T) *http.ServeMux {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cluster := talosClusterFixture(metav1.ConditionTrue)
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	cluster.Finalizers = []string{"kontinuum.sh/taloscluster-teardown"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusters: []v1alpha2.TalosCluster{cluster}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	return mux
}

// TestHandleTalosClustersRendersDeletingForClusterWithDeletionTimestamp
// covers a TalosCluster mid-teardown: its own Ready condition is left over
// from before deletion started and would otherwise read as "everything's
// fine" — the row must show "Deleting" instead.
func TestHandleTalosClustersRendersDeletingForClusterWithDeletionTimestamp(t *testing.T) {
	t.Parallel()

	mux := deletingTalosClusterMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Deleting")
	assert.NotContains(t, string(body), "text-green-300\">Ready<")
}

func TestHandleTalosClustersShowsEmptyState(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stubKontinuumLister{}, nil }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "No clusters found")
}

func TestHandleTalosClustersReturnsServerErrorWhenFactoryFails(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return nil, errFactory }

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandleTalosClustersInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: talosClustersResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClustersErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters"), kontinuumFactory)
}

// newTalosClusterKubeconfigMux builds a router+mux serving a single ready
// TalosCluster ("eu-eu-1a") with a control-plane pool and a kubeconfig
// Secret — shared by the reveal-panel tests below so each test's own body
// stays focused on the behavior it actually asserts, rather than repeating
// this fixture setup.
func newTalosClusterKubeconfigMux(t *testing.T) *http.ServeMux {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cluster := talosClusterFixture(metav1.ConditionTrue)
	cluster.Labels = map[string]string{"kontinuum.sh/region": "eu"}
	zone := v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-eu-1a"},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: testZoneValue, Domain: "example.com"},
	}
	controlPlanePool := v1alpha2.InstancePool{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-eu-1a"},
		Spec:       v1alpha2.InstancePoolSpec{Replicas: 1},
		Status:     v1alpha2.InstancePoolStatus{ReadyReplicas: 1},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "taloscluster-eu-eu-1a", Namespace: v1alpha2.KontinuumSystemNamespace},
		Data:       map[string][]byte{"kubeconfig": []byte("apiVersion: v1\nkind: Config\n")},
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{
			talosClusters: []v1alpha2.TalosCluster{cluster},
			zones:         []v1alpha2.Zone{zone},
			pools:         []v1alpha2.InstancePool{controlPlanePool},
			secret:        secret,
		}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	return mux
}

func TestHandleTalosClusterDetailRendersOverviewPoolsAndConditions(t *testing.T) {
	t.Parallel()

	mux := newTalosClusterKubeconfigMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "eu-eu-1a")
	assert.Contains(t, string(body), "v1.13.0")
	assert.Contains(t, string(body), "v1.32.0")
	assert.Contains(t, string(body), "eu/eu-1a")
	assert.Contains(t, string(body), "1/1", "the control-plane pool's ready/replicas count")
	assert.Contains(t, string(body), "Pending", "the worker pool's InstancePool doesn't exist yet")
	assert.Contains(t, string(body), "Cluster is ready", "the Ready condition's own message, capitalized")
	assert.Contains(t, string(body), "kontinuum.sh/region")
	assert.Contains(t, string(body), "eu")
	assert.Contains(t, string(body),
		`href="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a/kubeconfig"`)
	assert.NotContains(t, string(body), "apiVersion: v1\nkind: Config",
		"the kubeconfig's own contents must never be rendered into the page")
	assert.Contains(t, string(body),
		`hx-get="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"`,
		"hx-get is always the plain URL — hx-vals carries ?reveal on every poll instead, see hx-vals assertion below")

	wantHxVals := `hx-vals="js:{reveal: (new URLSearchParams(location.search).get('reveal') === 'true')}"`
	assert.Contains(t, string(body), wantHxVals,
		"the auto-refresh must re-derive ?reveal from the browser's current URL on every poll, "+
			"not a value baked in at render time")
	assert.Regexp(t, `id="taloscluster-kubeconfig-masked"\s+class="relative`, string(body),
		"the masked panel starts visible when the page loads without ?reveal=true")
	assert.Regexp(t, `id="taloscluster-kubeconfig-content"\s+class="hidden relative`, string(body))
}

// TestHandleTalosClusterDetailSortsConditionsNewestFirst covers issue #98:
// the conditions table must show the most recently transitioned condition
// first, regardless of the order status.conditions happens to store them
// in (Kubernetes gives no ordering guarantee there) — here the older
// "ClusterReady" condition is listed before the newer "AddonsHealthy" one
// in the fixture itself, so a naive render-in-storage-order would get this
// backwards.
func TestHandleTalosClusterDetailSortsConditionsNewestFirst(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	older := metav1.NewTime(time.Now().Add(-time.Hour))
	newer := metav1.NewTime(time.Now())

	cluster := talosClusterFixture(metav1.ConditionTrue)
	cluster.Status.Conditions = []metav1.Condition{
		{
			Type: testReadyConditionType, Status: metav1.ConditionTrue,
			Reason: "ClusterReady", Message: "cluster is ready", LastTransitionTime: older,
		},
		{
			Type: "AddonsHealthy", Status: metav1.ConditionTrue,
			Reason: "AddonsInstalled", Message: "addons are healthy", LastTransitionTime: newer,
		},
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusters: []v1alpha2.TalosCluster{cluster}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	newerIndex := strings.Index(string(body), "AddonsHealthy")
	olderIndex := strings.Index(string(body), "ClusterReady")

	require.NotEqual(t, -1, newerIndex, "AddonsHealthy condition must be rendered")
	require.NotEqual(t, -1, olderIndex, "ClusterReady condition must be rendered")
	assert.Less(t, newerIndex, olderIndex,
		"the newer AddonsHealthy condition must render before the older ClusterReady one")
}

func TestHandleTalosClusterDetailRevealsKubeconfigPanelViaQueryParam(t *testing.T) {
	t.Parallel()

	mux := newTalosClusterKubeconfigMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a?reveal=true"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body),
		`hx-get="/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"`,
		"hx-get stays the plain URL even when the page itself loaded with ?reveal=true — "+
			"see hx-vals in the sibling default-state test")
	assert.Regexp(t, `id="taloscluster-kubeconfig-masked"\s+class="hidden relative`, string(body),
		"?reveal=true renders the masked panel already hidden")
	assert.Regexp(t, `id="taloscluster-kubeconfig-content"\s+class="relative`, string(body),
		"?reveal=true renders the revealed panel already visible")
	assert.NotRegexp(t, `id="taloscluster-kubeconfig-content"\s+class="hidden`, string(body))
	assert.NotContains(t, string(body), "apiVersion: v1\nkind: Config",
		"the kubeconfig's own contents must never be rendered into the page, even when the panel starts open")
}

func TestHandleTalosClusterDetailShowsNoKubeconfigMessageWhenNotReady(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cluster := talosClusterFixture(metav1.ConditionFalse)
	cluster.Status.SecretRef = v1alpha2.SecretReference{}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusters: []v1alpha2.TalosCluster{cluster}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "No kubeconfig is available yet")
	assert.NotContains(t, string(body), "Download kubeconfig")
	assert.NotContains(t, string(body), "eu/eu-1a", "no Zone shares this cluster's name")
}

// TestHandleTalosClusterDetailRendersDeletingForClusterWithDeletionTimestamp
// covers the same "Deleting" override as the list-page test above, on the
// detail page's own title-bar badge — its conditions table still shows the
// stale Ready=True condition as its own green pill (same as
// assertRendersDeletingNotDiscovered's own doc for the Instance detail
// page), which is why this expects exactly one, not zero, occurrences.
func TestHandleTalosClusterDetailRendersDeletingForClusterWithDeletionTimestamp(t *testing.T) {
	t.Parallel()

	mux := deletingTalosClusterMux(t)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, newTestRequest(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Deleting")
	assert.Equal(t, 1, strings.Count(string(body), "text-green-300\">Ready<"),
		"the title-bar spot must show Deleting, not a green Ready pill — "+
			"the conditions table's own pill is the only allowed occurrence")
}

func TestHandleTalosClusterDetailRedirectsToListForUnknownCluster(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/missing",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters")
}

// TestHandleTalosClusterDetailPollSendsHxRedirectForDeletedCluster is
// TestHandleMachineDetailPollSendsHxRedirectForDeletedInstance's own
// counterpart for a TalosCluster's detail page (see taloscluster_content.html's
// identical 15s poll) — notFoundFallback's Hx-Redirect branch isn't specific
// to instances, so this covers a second page it applies to.
func TestHandleTalosClusterDetailPollSendsHxRedirectForDeletedCluster(t *testing.T) {
	t.Parallel()

	assertHTMXGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/missing",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters")
}

func TestHandleTalosClusterDetailInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: talosClustersResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusterGetErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"), kontinuumFactory)
}

func TestHandleTalosClusterKubeconfigDownloadServesFileWithHeaders(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cluster := talosClusterFixture(metav1.ConditionTrue)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "taloscluster-eu-eu-1a", Namespace: v1alpha2.KontinuumSystemNamespace},
		Data:       map[string][]byte{"kubeconfig": []byte("apiVersion: v1\nkind: Config\n")},
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusters: []v1alpha2.TalosCluster{cluster}, secret: secret}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a/kubeconfig"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `attachment; filename="eu-eu-1a-kubeconfig.yaml"`, resp.Header.Get("Content-Disposition"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "apiVersion: v1\nkind: Config\n", string(body))
}

func TestHandleTalosClusterKubeconfigDownloadRedirectsToClusterWhenNotReady(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	cluster := talosClusterFixture(metav1.ConditionFalse)
	cluster.Status.SecretRef = v1alpha2.SecretReference{}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusters: []v1alpha2.TalosCluster{cluster}}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a/kubeconfig"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a",
		resp.Header.Get("Location"), "no kubeconfig yet, but the cluster itself exists — back to its own detail page")
}

// TestHandleTalosClusterKubeconfigDownloadRedirectsOneHopUpForUnknownCluster
// covers notFoundFallback's own one-redirect-per-request contract: this
// request's immediate parent is the (also missing) cluster's own detail
// page, not the clusters list two levels up — the list only gets reached
// once a browser follows this redirect and hits that detail page's own
// identical fallback in turn (see TestHandleTalosClusterDetailRedirectsToListForUnknownCluster).
func TestHandleTalosClusterKubeconfigDownloadRedirectsOneHopUpForUnknownCluster(t *testing.T) {
	t.Parallel()

	assertGetRedirectsTo(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/missing/kubeconfig",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/missing")
}

func TestHandleTalosClusterKubeconfigDownloadInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: talosClustersResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{talosClusterGetErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a/kubeconfig"),
		kontinuumFactory)
}

func TestHandleDeleteTalosClusterRemovesClusterAndRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t,
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a",
		"/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters")
}

func TestHandleDeleteTalosClusterReturnsBadGatewayOnFailure(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (ui.NamespaceLister, error) { return stubNamespaceLister{}, nil }
	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: errFactory}, nil
	}

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version",
		config.Config{}, false, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder,
		newTestDeleteRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestHandleDeleteTalosClusterInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: talosClustersResource}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, "/app/kontinuum.sh/v1alpha2/namespaces/kontinuum-system/talosclusters/eu-eu-1a"),
		kontinuumFactory)
}
