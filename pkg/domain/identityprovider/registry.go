package identityprovider

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiservervalidation "k8s.io/apiserver/pkg/apis/apiserver/validation"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
	"k8s.io/apiserver/plugin/pkg/authenticator/token/oidc"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// ReasonActive is the Ready condition reason of a provider in use.
	ReasonActive = "Active"
	// ReasonInvalidConfiguration is the Ready condition reason of a
	// provider whose spec failed validation or CEL compilation.
	ReasonInvalidConfiguration = "InvalidConfiguration"
	// ReasonConflict is the Ready condition reason of a provider whose
	// issuer and audience are already served by another authenticator.
	ReasonConflict = "Conflict"
)

// ProviderRef names an identity provider. Namespace is empty for a
// ClusterIdentityProvider.
type ProviderRef struct {
	Namespace string
	Name      string
}

// String renders ref as "<name>" or "<namespace>/<name>".
func (r ProviderRef) String() string {
	if r.Namespace == "" {
		return r.Name
	}

	return r.Namespace + "/" + r.Name
}

// Kind returns ref's resource kind.
func (r ProviderRef) Kind() string {
	if r.Namespace == "" {
		return "ClusterIdentityProvider"
	}

	return "IdentityProvider"
}

// Provider is the kind-agnostic view of an IdentityProvider or
// ClusterIdentityProvider that Registry.Sync consumes.
type Provider struct {
	Ref               ProviderRef
	UID               types.UID
	Generation        int64
	CreationTimestamp time.Time
	Spec              v1alpha2.IdentityProviderSpec
}

// FromIdentityProvider converts a namespaced IdentityProvider.
func FromIdentityProvider(obj *v1alpha2.IdentityProvider) Provider {
	return Provider{
		Ref:               ProviderRef{Namespace: obj.Namespace, Name: obj.Name},
		UID:               obj.UID,
		Generation:        obj.Generation,
		CreationTimestamp: obj.CreationTimestamp.Time,
		Spec:              obj.Spec,
	}
}

// FromClusterIdentityProvider converts a ClusterIdentityProvider.
func FromClusterIdentityProvider(obj *v1alpha2.ClusterIdentityProvider) Provider {
	return Provider{
		Ref:               ProviderRef{Name: obj.Name},
		UID:               obj.UID,
		Generation:        obj.Generation,
		CreationTimestamp: obj.CreationTimestamp.Time,
		Spec:              obj.Spec,
	}
}

// Source returns where p's signing keys are fetched from.
func (p Provider) Source() KeySource {
	return KeySource{
		IssuerURL:            p.Spec.Issuer.URL,
		DiscoveryURL:         p.Spec.Issuer.DiscoveryURL,
		CertificateAuthority: p.Spec.Issuer.CertificateAuthority,
	}
}

// ReservedIssuer is an issuer and audiences already served by another
// authenticator in the chain — e.g. kontinuum's own static OIDC config —
// which no identity provider may claim, since the earlier authenticator
// would always win.
type ReservedIssuer struct {
	URL       string
	Audiences []string
	// Owner describes the reserving authenticator in conflict messages.
	Owner string
}

// SyncResult reports whether one provider is in use after Registry.Sync.
type SyncResult struct {
	// Active is true when the provider authenticates requests.
	Active bool
	// Reason is ReasonActive, ReasonInvalidConfiguration, or
	// ReasonConflict.
	Reason string
	// Message explains Reason.
	Message string
}

// RegistryConfig configures a Registry.
type RegistryConfig struct {
	// Cache holds every provider's signing keys.
	Cache *KeyCache
	// Authenticator receives the active provider set on every Sync.
	Authenticator *Authenticator
	// Reserved lists issuers no provider may claim.
	Reserved []ReservedIssuer
}

// compiledProvider caches one provider's compilation, keyed by its UID and
// generation so an unchanged provider isn't recompiled on every Sync.
type compiledProvider struct {
	uid        types.UID
	generation int64
	token      TokenAuthenticator
	err        error
	cancel     context.CancelFunc
}

// Registry compiles identity providers into token authenticators and
// swaps the active set into an Authenticator. It is safe for concurrent
// use; Sync calls are serialized.
type Registry struct {
	config   RegistryConfig
	compiler authenticationcel.Compiler

	mu       sync.Mutex
	compiled map[ProviderRef]*compiledProvider
	results  map[ProviderRef]SyncResult
}

// NewRegistry builds a Registry from config.
func NewRegistry(config RegistryConfig) *Registry {
	return &Registry{
		config:   config,
		compiler: authenticationcel.NewDefaultCompiler(),
		compiled: map[ProviderRef]*compiledProvider{},
		results:  map[ProviderRef]SyncResult{},
	}
}

// Sync makes providers the complete desired set: new or changed providers
// are compiled, removed ones are dropped, conflicts are resolved, and the
// resulting active set is swapped into the Authenticator atomically. On a
// conflict between providers sharing an issuer URL and any audience, a
// ClusterIdentityProvider beats an IdentityProvider and, within a kind,
// the oldest wins — so a tenant can never shadow an existing provider.
//
// Compiled providers keep running past ctx — they are only stopped when
// replaced or removed by a later Sync — so only ctx's values are used.
func (r *Registry) Sync(ctx context.Context, providers []Provider) map[ProviderRef]SyncResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.compileChanged(context.WithoutCancel(ctx), providers)

	ordered := slices.Clone(providers)
	slices.SortFunc(ordered, compareProviders)

	claims := slices.Clone(r.config.Reserved)
	active := make([]activeProvider, 0, len(ordered))
	sources := make([]KeySource, 0, len(ordered))
	results := make(map[ProviderRef]SyncResult, len(ordered))

	for _, provider := range ordered {
		result, claim := r.resolve(provider, claims)
		results[provider.Ref] = result

		if !result.Active {
			continue
		}

		claims = append(claims, claim)
		sources = append(sources, provider.Source())
		active = append(active, activeProvider{
			ref:    provider.Ref,
			issuer: provider.Spec.Issuer.URL,
			token:  r.compiled[provider.Ref].token,
		})
	}

	r.config.Authenticator.set(active)
	r.config.Cache.Retain(sources)
	r.results = results

	return results
}

// Result returns ref's outcome from the most recent Sync.
func (r *Registry) Result(ref ProviderRef) (SyncResult, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result, ok := r.results[ref]

	return result, ok
}

// resolve decides whether provider can be active given the claims of every
// higher-precedence provider, returning its own claim if so.
func (r *Registry) resolve(provider Provider, claims []ReservedIssuer) (SyncResult, ReservedIssuer) {
	compiled := r.compiled[provider.Ref]
	if compiled.err != nil {
		return SyncResult{Reason: ReasonInvalidConfiguration, Message: compiled.err.Error()}, ReservedIssuer{}
	}

	issuer := provider.Spec.Issuer

	for _, claim := range claims {
		if claim.URL != issuer.URL {
			continue
		}

		for _, audience := range issuer.Audiences {
			if slices.Contains(claim.Audiences, audience) {
				return SyncResult{
					Reason: ReasonConflict,
					Message: fmt.Sprintf("Issuer %q with audience %q is already served by %s",
						issuer.URL, audience, claim.Owner),
				}, ReservedIssuer{}
			}
		}
	}

	claim := ReservedIssuer{
		URL:       issuer.URL,
		Audiences: issuer.Audiences,
		Owner:     provider.Ref.Kind() + " " + provider.Ref.String(),
	}

	return SyncResult{Active: true, Reason: ReasonActive, Message: "Identity provider is authenticating requests"}, claim
}

// compileChanged compiles every provider that is new or whose UID or
// generation changed, and forgets every provider no longer listed.
func (r *Registry) compileChanged(ctx context.Context, providers []Provider) {
	seen := make(map[ProviderRef]struct{}, len(providers))

	for _, provider := range providers {
		seen[provider.Ref] = struct{}{}

		existing, ok := r.compiled[provider.Ref]
		if ok && existing.uid == provider.UID && existing.generation == provider.Generation {
			continue
		}

		if ok && existing.cancel != nil {
			existing.cancel()
		}

		r.compiled[provider.Ref] = r.compile(ctx, provider)
	}

	for ref, existing := range r.compiled {
		_, ok := seen[ref]
		if ok {
			continue
		}

		if existing.cancel != nil {
			existing.cancel()
		}

		delete(r.compiled, ref)
	}
}

// compile validates provider's spec exactly as kube-apiserver validates an
// AuthenticationConfiguration jwt[] entry, then builds upstream's own JWT
// authenticator around a KeySet backed by the shared KeyCache — so CEL
// claim validation, claim mappings, and user validation behave exactly as
// they do in Kubernetes, while discovery and key fetching go through the
// stale-while-revalidate cache.
func (r *Registry) compile(ctx context.Context, provider Provider) *compiledProvider {
	result := &compiledProvider{uid: provider.UID, generation: provider.Generation}

	err := validateSpec(r.compiler, provider.Spec)
	if err != nil {
		result.err = err

		return result
	}

	jwtAuthenticator := toJWTAuthenticator(provider.Spec)

	source := provider.Source()

	client, err := DefaultHTTPClient(source)
	if err != nil {
		result.err = fmt.Errorf("%w: %w", ErrInvalidConfiguration, err)

		return result
	}

	// Discovery is the KeySet's job: upstream refuses a DiscoveryURL
	// alongside a custom KeySet.
	jwtAuthenticator.Issuer.DiscoveryURL = ""

	lifecycleCtx, cancel := context.WithCancel(ctx)

	token, err := oidc.New(lifecycleCtx, oidc.Options{
		JWTAuthenticator:     jwtAuthenticator,
		KeySet:               &KeySet{Cache: r.config.Cache, Source: source},
		Client:               client,
		Compiler:             r.compiler,
		SupportedSigningAlgs: SupportedSigningAlgs(),
	})
	if err != nil {
		cancel()

		result.err = fmt.Errorf("%w: %w", ErrInvalidConfiguration, err)

		return result
	}

	result.token = token
	result.cancel = cancel

	return result
}

// ValidateSpec validates spec exactly as a Registry does before compiling
// it — the same rules kube-apiserver applies to an AuthenticationConfiguration
// jwt[] entry, including CEL compilation — without starting anything, so a
// caller such as the UI can reject an invalid provider before creating it.
func ValidateSpec(spec v1alpha2.IdentityProviderSpec) error {
	return validateSpec(authenticationcel.NewDefaultCompiler(), spec)
}

// validateSpec is ValidateSpec with a caller-supplied CEL compiler.
func validateSpec(compiler authenticationcel.Compiler, spec v1alpha2.IdentityProviderSpec) error {
	_, fieldErrs := apiservervalidation.CompileAndValidateJWTAuthenticator(compiler, toJWTAuthenticator(spec), nil)

	aggregate := fieldErrs.ToAggregate()
	if aggregate != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfiguration, aggregate)
	}

	return nil
}

// compareProviders orders providers by precedence: cluster-scoped first,
// then oldest first, then by name.
func compareProviders(left, right Provider) int {
	leftCluster := left.Ref.Namespace == ""
	rightCluster := right.Ref.Namespace == ""

	if leftCluster != rightCluster {
		if leftCluster {
			return -1
		}

		return 1
	}

	return cmp.Or(
		left.CreationTimestamp.Compare(right.CreationTimestamp),
		cmp.Compare(left.Ref.Namespace, right.Ref.Namespace),
		cmp.Compare(left.Ref.Name, right.Ref.Name),
	)
}

// toJWTAuthenticator converts spec into k8s.io/apiserver's internal
// AuthenticationConfiguration jwt[] type.
func toJWTAuthenticator(spec v1alpha2.IdentityProviderSpec) apiserver.JWTAuthenticator {
	claimValidationRules := make([]apiserver.ClaimValidationRule, 0, len(spec.ClaimValidationRules))
	for _, rule := range spec.ClaimValidationRules {
		claimValidationRules = append(claimValidationRules, apiserver.ClaimValidationRule{
			Claim:         rule.Claim,
			RequiredValue: rule.RequiredValue,
			Expression:    rule.Expression,
			Message:       rule.Message,
		})
	}

	extra := make([]apiserver.ExtraMapping, 0, len(spec.ClaimMappings.Extra))
	for _, mapping := range spec.ClaimMappings.Extra {
		extra = append(extra, apiserver.ExtraMapping{Key: mapping.Key, ValueExpression: mapping.ValueExpression})
	}

	userValidationRules := make([]apiserver.UserValidationRule, 0, len(spec.UserValidationRules))
	for _, rule := range spec.UserValidationRules {
		userValidationRules = append(userValidationRules, apiserver.UserValidationRule{
			Expression: rule.Expression,
			Message:    rule.Message,
		})
	}

	return apiserver.JWTAuthenticator{
		Issuer: apiserver.Issuer{
			URL:                  spec.Issuer.URL,
			DiscoveryURL:         spec.Issuer.DiscoveryURL,
			CertificateAuthority: spec.Issuer.CertificateAuthority,
			Audiences:            spec.Issuer.Audiences,
			AudienceMatchPolicy:  apiserver.AudienceMatchPolicyType(spec.Issuer.AudienceMatchPolicy),
		},
		ClaimValidationRules: claimValidationRules,
		ClaimMappings: apiserver.ClaimMappings{
			Username: toPrefixedClaimOrExpression(spec.ClaimMappings.Username),
			Groups:   toPrefixedClaimOrExpression(spec.ClaimMappings.Groups),
			UID: apiserver.ClaimOrExpression{
				Claim:      spec.ClaimMappings.UID.Claim,
				Expression: spec.ClaimMappings.UID.Expression,
			},
			Extra: extra,
		},
		UserValidationRules: userValidationRules,
	}
}

// toPrefixedClaimOrExpression converts one claim mapping.
func toPrefixedClaimOrExpression(mapping v1alpha2.PrefixedClaimOrExpression) apiserver.PrefixedClaimOrExpression {
	return apiserver.PrefixedClaimOrExpression{
		Claim:      mapping.Claim,
		Prefix:     mapping.Prefix,
		Expression: mapping.Expression,
	}
}
