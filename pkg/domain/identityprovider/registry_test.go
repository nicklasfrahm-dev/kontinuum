package identityprovider_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/authenticator"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
)

// emailVerifiedRule is the claim validation rule upstream requires
// whenever a username expression reads claims.email.
const emailVerifiedRule = "claims.?email_verified.orValue(true) == true"

type testRegistry struct {
	registry      *identityprovider.Registry
	authenticator *identityprovider.Authenticator
	cache         *identityprovider.KeyCache
}

func newTestRegistry(reserved ...identityprovider.ReservedIssuer) testRegistry {
	cache := identityprovider.NewKeyCache(identityprovider.KeyCacheConfig{MinRefreshInterval: time.Nanosecond})
	authn := identityprovider.NewAuthenticator()

	return testRegistry{
		registry: identityprovider.NewRegistry(identityprovider.RegistryConfig{
			Cache: cache, Authenticator: authn, Reserved: reserved,
		}),
		authenticator: authn,
		cache:         cache,
	}
}

// sync runs a Registry.Sync with providers as the complete desired set.
func (s testRegistry) sync(
	providers ...identityprovider.Provider,
) map[identityprovider.ProviderRef]identityprovider.SyncResult {
	return s.registry.Sync(context.Background(), providers)
}

func celSpec(issuer *testIssuer) v1alpha2.IdentityProviderSpec {
	return v1alpha2.IdentityProviderSpec{
		Issuer: v1alpha2.IdentityProviderIssuer{
			URL:                  issuer.URL(),
			Audiences:            []string{testAudience},
			CertificateAuthority: issuer.CA(),
		},
		ClaimValidationRules: []v1alpha2.ClaimValidationRule{{Expression: emailVerifiedRule}},
		ClaimMappings: v1alpha2.ClaimMappings{
			Username: v1alpha2.PrefixedClaimOrExpression{Expression: "claims.email"},
			Groups:   v1alpha2.PrefixedClaimOrExpression{Expression: "has(claims.groups) ? claims.groups : []"},
			UID:      v1alpha2.ClaimOrExpression{Expression: "claims.sub"},
		},
	}
}

func provider(namespace, name string, spec v1alpha2.IdentityProviderSpec, created time.Time) identityprovider.Provider {
	return identityprovider.Provider{
		Ref:               identityprovider.ProviderRef{Namespace: namespace, Name: name},
		UID:               types.UID(namespace + "/" + name),
		Generation:        1,
		CreationTimestamp: created,
		Spec:              spec,
	}
}

func authenticate(t *testing.T, authn *identityprovider.Authenticator, token string) *authenticator.Response {
	t.Helper()

	response, ok, err := authn.AuthenticateToken(context.Background(), token)
	require.NoError(t, err)
	require.True(t, ok)

	return response
}

func TestClusterIdentityProviderMapsClaimsWithCEL(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()

	results := setup.sync(provider("", clusterName, celSpec(issuer), time.Now()))
	require.True(t, results[identityprovider.ProviderRef{Name: clusterName}].Active)

	response := authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, nil))
	assert.Equal(t, aliceEmail, response.User.GetName())
	assert.Equal(t, "user-123", response.User.GetUID())
	assert.Equal(t, []string{"engineering"}, response.User.GetGroups())
	assert.Equal(t, []string{clusterName}, response.User.GetExtra()[identityprovider.ProviderExtraKey])
}

func TestIdentityProviderScopesIdentitiesToTenant(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()

	setup.sync(provider(tenantName, tenantProviderName, celSpec(issuer), time.Now()))

	response := authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, map[string]any{
		"email":     "system:masters",
		groupsClaim: []string{"system:masters", "admins"},
	}))
	assert.Equal(t, "tenant:acme:system:masters", response.User.GetName())
	assert.Equal(t, []string{"tenant:acme:system:masters", "tenant:acme:admins"}, response.User.GetGroups())
	assert.Equal(t, []string{"acme/sso"}, response.User.GetExtra()[identityprovider.ProviderExtraKey])
}

func TestClaimValidationRuleRejectsToken(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()

	spec := celSpec(issuer)
	spec.ClaimValidationRules = append(spec.ClaimValidationRules, v1alpha2.ClaimValidationRule{
		Expression: "'engineering' in claims.groups", Message: "must be an engineer",
	})
	setup.sync(provider("", clusterName, spec, time.Now()))

	_, ok, err := setup.authenticator.AuthenticateToken(context.Background(),
		issuer.sign(t, firstKeyID, map[string]any{groupsClaim: []string{"sales"}}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be an engineer")
	assert.False(t, ok)
}

func TestAuthenticatorIgnoresForeignTokens(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	other := newTestIssuer(t)
	setup := newTestRegistry()

	setup.sync(provider("", clusterName, celSpec(issuer), time.Now()))

	for _, token := range []string{"not-a-jwt", other.sign(t, firstKeyID, nil)} {
		response, ok, err := setup.authenticator.AuthenticateToken(context.Background(), token)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, response)
	}
}

func TestSyncHotSwapsProviders(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()
	token := issuer.sign(t, firstKeyID, nil)

	setup.sync(provider("", clusterName, celSpec(issuer), time.Now()))
	authenticate(t, setup.authenticator, token)

	setup.sync()

	_, ok, err := setup.authenticator.AuthenticateToken(context.Background(), token)
	require.NoError(t, err)
	assert.False(t, ok)

	_, cached := setup.cache.Snapshot(issuer.Source())
	assert.False(t, cached, "keys of a deleted provider must be dropped")
}

func TestSyncRecompilesChangedGeneration(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()
	created := time.Now()

	setup.sync(provider("", clusterName, celSpec(issuer), created))

	updated := provider("", clusterName, celSpec(issuer), created)
	updated.Generation = 2
	updated.Spec.ClaimMappings.Username = v1alpha2.PrefixedClaimOrExpression{Expression: "'oidc:' + claims.sub"}
	setup.sync(updated)

	response := authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, nil))
	assert.Equal(t, "oidc:user-123", response.User.GetName())
}

func TestSyncResolvesConflictsByPrecedence(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()
	now := time.Now()

	results := setup.sync(
		provider("tenant-a", "older", celSpec(issuer), now.Add(-time.Hour)),
		provider("tenant-b", "newer", celSpec(issuer), now),
		provider("", clusterName, celSpec(issuer), now),
	)

	assert.True(t, results[identityprovider.ProviderRef{Name: clusterName}].Active)

	for _, ref := range []identityprovider.ProviderRef{
		{Namespace: "tenant-a", Name: "older"}, {Namespace: "tenant-b", Name: "newer"},
	} {
		assert.False(t, results[ref].Active)
		assert.Equal(t, identityprovider.ReasonConflict, results[ref].Reason)
		assert.Contains(t, results[ref].Message, "ClusterIdentityProvider corp")
	}

	response := authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, nil))
	assert.Equal(t, aliceEmail, response.User.GetName())
}

func TestSyncRejectsReservedIssuer(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry(identityprovider.ReservedIssuer{
		URL: issuer.URL(), Audiences: []string{testAudience}, Owner: "static config",
	})

	results := setup.sync(provider("", clusterName, celSpec(issuer), time.Now()))

	result := results[identityprovider.ProviderRef{Name: clusterName}]
	assert.False(t, result.Active)
	assert.Equal(t, identityprovider.ReasonConflict, result.Reason)
	assert.Contains(t, result.Message, "static config")
}

func TestSyncReportsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()

	spec := celSpec(issuer)
	spec.ClaimMappings.Username = v1alpha2.PrefixedClaimOrExpression{Expression: "claims.email +"}

	results := setup.sync(provider("", clusterName, spec, time.Now()))

	result := results[identityprovider.ProviderRef{Name: clusterName}]
	assert.False(t, result.Active)
	assert.Equal(t, identityprovider.ReasonInvalidConfiguration, result.Reason)

	stored, ok := setup.registry.Result(identityprovider.ProviderRef{Name: clusterName})
	require.True(t, ok)
	assert.Equal(t, result, stored)
}

func TestKeyRotationIsPickedUpOnUnknownKeyID(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	setup := newTestRegistry()

	setup.sync(provider("", clusterName, celSpec(issuer), time.Now()))
	authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, nil))

	issuer.addKey(t, rotatedKeyID)

	response := authenticate(t, setup.authenticator, issuer.sign(t, rotatedKeyID, nil))
	assert.Equal(t, aliceEmail, response.User.GetName())
	assert.Equal(t, int32(2), issuer.jwksHits.Load())
}

func TestIdentityPrefix(t *testing.T) {
	t.Parallel()

	assert.Empty(t, identityprovider.IdentityPrefix(""))
	assert.Equal(t, "tenant:acme:", identityprovider.IdentityPrefix(tenantName))
}

func TestProviderRef(t *testing.T) {
	t.Parallel()

	cluster := identityprovider.ProviderRef{Name: clusterName}
	assert.Equal(t, clusterName, cluster.String())
	assert.Equal(t, "ClusterIdentityProvider", cluster.Kind())

	namespaced := identityprovider.ProviderRef{Namespace: tenantName, Name: tenantProviderName}
	assert.Equal(t, "acme/sso", namespaced.String())
	assert.Equal(t, "IdentityProvider", namespaced.Kind())
}

func TestValidateSpec(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)

	require.NoError(t, identityprovider.ValidateSpec(celSpec(issuer)))

	invalid := celSpec(issuer)
	invalid.Issuer.URL = "http://insecure.example.com"
	invalid.ClaimMappings.Groups = v1alpha2.PrefixedClaimOrExpression{Expression: "claims.groups +"}

	err := identityprovider.ValidateSpec(invalid)
	require.ErrorIs(t, err, identityprovider.ErrInvalidConfiguration)
	assert.Contains(t, err.Error(), "https")
}
