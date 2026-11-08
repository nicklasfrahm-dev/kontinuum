package ui_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

const (
	testIdentityProviderName   = "corporate-sso"
	testIdentityProviderIssuer = "https://login.example.com"
	namespacedProvidersPath    = "/app/kontinuum.sh/v1alpha2/namespaces/demo/identityproviders"
	clusterProvidersPath       = "/app/kontinuum.sh/v1alpha2/clusteridentityproviders"
	usernameExpressionKey      = "usernameExpression"
	issuerURLKey               = "issuerURL"
	audiencesKey               = "audiences"
)

// serveIdentityProviders runs request against a Router whose Kubernetes
// client is stub, with OIDC enabled, and returns the response status and
// body.
func serveIdentityProviders(t *testing.T, stub stubKontinuumLister, request *http.Request) (int, string) {
	t.Helper()

	factory := func(context.Context) (ui.NamespaceLister, error) {
		return stubNamespaceLister{list: &corev1.NamespaceList{}}, nil
	}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) { return stub, nil }

	cfg := config.Config{}
	cfg.OIDC.IssuerURL = testOIDCIssuerURL

	router := ui.NewRouter(factory, kontinuumFactory, zoneFactory, "test-version", cfg, true, nil)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux, nil, nil)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	resp := recorder.Result()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode, string(body)
}

func testIdentityProviderSpec() v1alpha2.IdentityProviderSpec {
	return v1alpha2.IdentityProviderSpec{
		Issuer: v1alpha2.IdentityProviderIssuer{URL: testIdentityProviderIssuer, Audiences: []string{testOIDCClientID}},
		ClaimMappings: v1alpha2.ClaimMappings{
			Username: v1alpha2.PrefixedClaimOrExpression{Expression: "claims.email"},
		},
	}
}

func readyStatus(keys int32) v1alpha2.IdentityProviderStatus {
	return v1alpha2.IdentityProviderStatus{
		SigningKeys: keys,
		Conditions: []metav1.Condition{{
			Type: identityprovider.ReadyConditionType, Status: metav1.ConditionTrue, Reason: identityprovider.ReasonActive,
		}},
	}
}

func TestHandleIAMNamespaceIdentityProvidersShowsProviders(t *testing.T) {
	t.Parallel()

	stub := stubKontinuumLister{identityProviders: []v1alpha2.IdentityProvider{{
		ObjectMeta: metav1.ObjectMeta{Name: testIdentityProviderName, Namespace: testNamespace},
		Spec:       testIdentityProviderSpec(),
		Status:     readyStatus(2),
	}}}

	status, body := serveIdentityProviders(t, stub, newTestRequest(t, namespacedProvidersPath))

	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, testIdentityProviderName)
	assert.Contains(t, body, testIdentityProviderIssuer)
	assert.Contains(t, body, "tenant:demo:")
	assert.Contains(t, body, "claims.email")
	assert.Contains(t, body, "Add identity provider")
}

func TestHandleIAMClusterIdentityProvidersShowsConflict(t *testing.T) {
	t.Parallel()

	stub := stubKontinuumLister{clusterIdentityProviders: []v1alpha2.ClusterIdentityProvider{{
		ObjectMeta: metav1.ObjectMeta{Name: testIdentityProviderName},
		Spec:       testIdentityProviderSpec(),
		Status: v1alpha2.IdentityProviderStatus{Conditions: []metav1.Condition{{
			Type: identityprovider.ReadyConditionType, Status: metav1.ConditionFalse,
			Reason: identityprovider.ReasonConflict, Message: "already served by static config",
		}}},
	}}}

	status, body := serveIdentityProviders(t, stub, newTestRequest(t, clusterProvidersPath))

	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, testIdentityProviderName)
	assert.Contains(t, body, identityprovider.ReasonConflict)
	assert.Contains(t, body, "already served by static config")
	assert.NotContains(t, body, "tenant:")
	assert.Contains(t, body, "Add cluster identity provider")
}

func TestHandleIdentityProviderAddCreatesIdentityProvider(t *testing.T) {
	t.Parallel()

	var created []client.Object

	form := url.Values{
		roleAddFormNameKey:              {testIdentityProviderName},
		issuerURLKey:                    {testIdentityProviderIssuer},
		audiencesKey:                    {testOIDCClientID + ", cli"},
		usernameExpressionKey:           {"claims.email"},
		"groupsExpression":              {"has(claims.groups) ? claims.groups : []"},
		"claimRules[0][expression]":     {"claims.?email_verified.orValue(true) == true"},
		"claimRules[1][expression]":     {""},
		"userRules[0][expression]":      {"!user.username.startsWith('system:')"},
		"userRules[0][message]":         {"no system users"},
		"certificateAuthority":          {""},
		"unrelatedRules[0][expression]": {"ignored"},
	}

	status, body := serveIdentityProviders(t, stubKontinuumLister{created: &created},
		newTestFormRequest(t, namespacedProvidersPath, form))

	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "Created identity provider "+testIdentityProviderName)
	require.Len(t, created, 1)

	provider, ok := created[0].(*v1alpha2.IdentityProvider)
	require.True(t, ok)
	assert.Equal(t, testNamespace, provider.Namespace)
	assert.Equal(t, []string{testOIDCClientID, "cli"}, provider.Spec.Issuer.Audiences)
	assert.Equal(t, v1alpha2.AudienceMatchPolicyMatchAny, provider.Spec.Issuer.AudienceMatchPolicy)
	require.Len(t, provider.Spec.ClaimValidationRules, 1)
	require.Len(t, provider.Spec.UserValidationRules, 1)
	assert.Equal(t, "no system users", provider.Spec.UserValidationRules[0].Message)
}

func TestHandleIdentityProviderAddRejectsInvalidCEL(t *testing.T) {
	t.Parallel()

	var created []client.Object

	form := url.Values{
		roleAddFormNameKey:    {testIdentityProviderName},
		issuerURLKey:          {testIdentityProviderIssuer},
		audiencesKey:          {testOIDCClientID},
		usernameExpressionKey: {"claims.sub +"},
	}

	status, body := serveIdentityProviders(t, stubKontinuumLister{created: &created},
		newTestFormRequest(t, namespacedProvidersPath, form))

	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "invalid identity provider configuration")
	assert.Contains(t, body, `value="claims.sub &#43;"`, "the submitted value must be redisplayed")
	assert.Empty(t, created)
}

func TestHandleIdentityProviderAddRequiresFields(t *testing.T) {
	t.Parallel()

	var created []client.Object

	status, body := serveIdentityProviders(t, stubKontinuumLister{created: &created},
		newTestFormRequest(t, namespacedProvidersPath, url.Values{roleAddFormNameKey: {testIdentityProviderName}}))

	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "Issuer URL is required.")
	assert.Empty(t, created)
}

func TestHandleClusterIdentityProviderAddCreatesClusterIdentityProvider(t *testing.T) {
	t.Parallel()

	var created []client.Object

	form := url.Values{
		roleAddFormNameKey:          {testIdentityProviderName},
		issuerURLKey:                {testIdentityProviderIssuer},
		audiencesKey:                {testOIDCClientID},
		usernameExpressionKey:       {"'oidc:' + claims.sub"},
		"uidExpression":             {"claims.sub"},
		"claimRules[0][expression]": {"has(claims.sub)"},
	}

	status, body := serveIdentityProviders(t, stubKontinuumLister{created: &created},
		newTestFormRequest(t, clusterProvidersPath, form))

	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "Created cluster identity provider")
	require.Len(t, created, 1)

	provider, ok := created[0].(*v1alpha2.ClusterIdentityProvider)
	require.True(t, ok)
	assert.Empty(t, provider.Namespace)
	assert.Empty(t, provider.Spec.Issuer.AudienceMatchPolicy)
	assert.Equal(t, "claims.sub", provider.Spec.ClaimMappings.UID.Expression)
}

func TestHandleDeleteIdentityProviderRedirectsToList(t *testing.T) {
	t.Parallel()

	assertDeleteRedirectsToList(t, namespacedProvidersPath+"/"+testIdentityProviderName, namespacedProvidersPath)
	assertDeleteRedirectsToList(t, clusterProvidersPath+"/"+testIdentityProviderName, clusterProvidersPath)
}

func TestHandleDeleteIdentityProviderInvalidatesSessionOnForbidden(t *testing.T) {
	t.Parallel()

	forbiddenReason := schema.GroupResource{Group: v1alpha2.GroupName, Resource: "identityproviders"}

	kontinuumFactory := func(context.Context) (ui.KontinuumClient, error) {
		return stubKontinuumLister{deleteErr: apierrors.NewForbidden(forbiddenReason, "", errTestForbidden)}, nil
	}

	assertForbiddenInvalidatesSession(t,
		newTestDeleteRequest(t, namespacedProvidersPath+"/"+testIdentityProviderName), kontinuumFactory)
}

func TestHandleIAMIdentityProvidersReportsListFailure(t *testing.T) {
	t.Parallel()

	status, _ := serveIdentityProviders(t, stubKontinuumLister{listErr: errFactory},
		newTestRequest(t, clusterProvidersPath))

	assert.Equal(t, http.StatusBadGateway, status)
}
