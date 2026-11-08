package identityprovider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	// TenantPrefix starts the username and group prefix forced onto every
	// identity a namespaced IdentityProvider maps — see
	// IdentityPrefix.
	TenantPrefix = "tenant:"
	// ProviderExtraKey is the user "extra" key every identity mapped by an
	// identity provider carries, naming the provider that authenticated
	// it — "<name>" for a ClusterIdentityProvider, "<namespace>/<name>"
	// for an IdentityProvider. Useful in audit logs.
	ProviderExtraKey = "kontinuum.sh/identity-provider"
	// jwtSegments is the number of dot-separated segments in a compact JWS.
	jwtSegments = 3
)

// IdentityPrefix returns the prefix forced onto every username and group
// mapped by an identity provider in namespace: "tenant:<namespace>:" for a
// namespaced IdentityProvider, and "" for a ClusterIdentityProvider (empty
// namespace). The prefix keeps a tenant's issuer from ever minting a
// cluster-wide identity, a configured admin group, or another tenant's
// identity.
func IdentityPrefix(namespace string) string {
	if namespace == "" {
		return ""
	}

	return TenantPrefix + namespace + ":"
}

// TokenAuthenticator is the subset of k8s.io/apiserver's
// authenticator.Token every compiled identity provider satisfies.
type TokenAuthenticator interface {
	AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error)
}

// activeProvider is one compiled identity provider in an Authenticator's
// current set.
type activeProvider struct {
	ref    ProviderRef
	issuer string
	token  TokenAuthenticator
}

// providerSet is an immutable, issuer-indexed snapshot of the active
// identity providers, swapped atomically by Authenticator.Set.
type providerSet struct {
	byIssuer map[string][]activeProvider
}

// Authenticator authenticates bearer tokens against the active set of
// identity providers. The set is swapped atomically at runtime — see
// Registry.Sync — so creating, changing, or deleting an IdentityProvider
// or ClusterIdentityProvider takes effect on the very next request, with
// no apiserver restart.
type Authenticator struct {
	providers atomic.Pointer[providerSet]
}

// NewAuthenticator builds an Authenticator with no active providers.
func NewAuthenticator() *Authenticator {
	authn := &Authenticator{}
	authn.providers.Store(&providerSet{byIssuer: map[string][]activeProvider{}})

	return authn
}

// AuthenticateToken implements authenticator.Token. Only providers whose
// issuer matches the token's (unverified) "iss" claim are tried; a token
// from any other issuer, or one that isn't a JWT at all, is left for the
// next authenticator in the chain. The first provider that verifies the
// token wins, and its identity is scoped by IdentityPrefix.
func (a *Authenticator) AuthenticateToken(
	ctx context.Context, token string,
) (*authenticator.Response, bool, error) {
	issuer, ok := untrustedIssuer(token)
	if !ok {
		return nil, false, nil
	}

	candidates := a.providers.Load().byIssuer[issuer]
	if len(candidates) == 0 {
		return nil, false, nil
	}

	var errs []error

	for _, candidate := range candidates {
		response, authenticated, err := candidate.token.AuthenticateToken(ctx, token)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", candidate.ref, err))

			continue
		}

		if authenticated {
			return scopeResponse(response, candidate.ref), true, nil
		}
	}

	if len(errs) == 0 {
		return nil, false, nil
	}

	return nil, false, fmt.Errorf("failed to authenticate token: %w", utilerrors.NewAggregate(errs))
}

// set replaces the active providers. Within one issuer, providers are tried
// in the order given.
func (a *Authenticator) set(providers []activeProvider) {
	byIssuer := make(map[string][]activeProvider, len(providers))
	for _, provider := range providers {
		byIssuer[provider.issuer] = append(byIssuer[provider.issuer], provider)
	}

	a.providers.Store(&providerSet{byIssuer: byIssuer})
}

// scopeResponse applies ref's IdentityPrefix to response's username and
// groups and records ref under ProviderExtraKey.
func scopeResponse(response *authenticator.Response, ref ProviderRef) *authenticator.Response {
	prefix := IdentityPrefix(ref.Namespace)

	groups := make([]string, 0, len(response.User.GetGroups()))
	for _, group := range response.User.GetGroups() {
		groups = append(groups, prefix+group)
	}

	extra := make(map[string][]string, len(response.User.GetExtra())+1)
	maps.Copy(extra, response.User.GetExtra())

	extra[ProviderExtraKey] = []string{ref.String()}

	return &authenticator.Response{
		Audiences: response.Audiences,
		User: &user.DefaultInfo{
			Name:   prefix + response.User.GetName(),
			UID:    response.User.GetUID(),
			Groups: groups,
			Extra:  extra,
		},
	}
}

// untrustedIssuer extracts a compact JWS's "iss" claim without verifying
// anything — only to pick which providers to try.
func untrustedIssuer(token string) (string, bool) {
	segments := strings.Split(token, ".")
	if len(segments) != jwtSegments {
		return "", false
	}

	payload, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		return "", false
	}

	var claims struct {
		Issuer string `json:"iss"`
	}

	err = json.Unmarshal(payload, &claims)
	if err != nil || claims.Issuer == "" {
		return "", false
	}

	return claims.Issuer, true
}
