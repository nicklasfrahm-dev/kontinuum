package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AudienceMatchPolicy selects how a token's "aud" claim is matched against
// IdentityProviderIssuer.Audiences — mirrors Kubernetes'
// AuthenticationConfiguration's own audienceMatchPolicy.
// +kubebuilder:validation:Enum=MatchAny
type AudienceMatchPolicy string

const (
	// AudienceMatchPolicyMatchAny accepts a token whose "aud" claim
	// contains at least one of the configured audiences.
	AudienceMatchPolicyMatchAny AudienceMatchPolicy = "MatchAny"
)

// IdentityProviderIssuer identifies the OIDC issuer whose tokens an
// identity provider accepts, and how to reach its signing keys.
type IdentityProviderIssuer struct {
	// URL is the issuer URL. It must use https and match the token's "iss"
	// claim exactly. Unless DiscoveryURL is set, the discovery document is
	// fetched from {URL}/.well-known/openid-configuration.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`
	// DiscoveryURL overrides where the discovery document is fetched from,
	// e.g. an in-cluster address for an issuer whose public URL isn't
	// reachable from kontinuum itself. The document's own "issuer" field
	// must still equal URL.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	DiscoveryURL string `json:"discoveryUrl,omitempty"`
	// Audiences lists the acceptable "aud" claim values. At least one must
	// match.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +listType=set
	Audiences []string `json:"audiences"`
	// AudienceMatchPolicy selects how Audiences is matched. Only MatchAny
	// is supported, and it is required whenever more than one audience is
	// listed.
	// +optional
	AudienceMatchPolicy AudienceMatchPolicy `json:"audienceMatchPolicy,omitempty"`
	// CertificateAuthority is a PEM-encoded CA bundle used to verify the
	// TLS certificates of the discovery and JWKS endpoints. Empty means
	// the host's own root CAs.
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	CertificateAuthority string `json:"certificateAuthority,omitempty"`
}

// ClaimValidationRule rejects tokens whose claims don't satisfy it. Exactly
// one of Claim or Expression must be set.
type ClaimValidationRule struct {
	// Claim is a top-level claim that must be present and equal
	// RequiredValue.
	// +optional
	Claim string `json:"claim,omitempty"`
	// RequiredValue is the value Claim must equal. Only valid with Claim.
	// +optional
	RequiredValue string `json:"requiredValue,omitempty"`
	// Expression is a CEL expression over the token's claims (exposed as
	// "claims") that must evaluate to true, e.g.
	// "claims.email_verified == true".
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	Expression string `json:"expression,omitempty"`
	// Message is returned when Expression evaluates to false. Only valid
	// with Expression.
	// +optional
	Message string `json:"message,omitempty"`
}

// PrefixedClaimOrExpression maps a username or group list from the token's
// claims. Exactly one of Claim or Expression must be set.
type PrefixedClaimOrExpression struct {
	// Claim is the top-level claim to read the value from.
	// +optional
	Claim string `json:"claim,omitempty"`
	// Prefix is prepended to the claim's value. Required, but may be empty,
	// whenever Claim is set; not allowed with Expression, which can build
	// its own prefix.
	// +optional
	Prefix *string `json:"prefix,omitempty"`
	// Expression is a CEL expression over the token's claims (exposed as
	// "claims"), e.g. "claims.email" or
	// "has(claims.groups) ? claims.groups : []".
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	Expression string `json:"expression,omitempty"`
}

// ClaimOrExpression maps a single value from the token's claims. Exactly one
// of Claim or Expression must be set.
type ClaimOrExpression struct {
	// Claim is the top-level claim to read the value from.
	// +optional
	Claim string `json:"claim,omitempty"`
	// Expression is a CEL expression over the token's claims (exposed as
	// "claims").
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	Expression string `json:"expression,omitempty"`
}

// ExtraMapping maps one key of the user's "extra" attributes.
type ExtraMapping struct {
	// Key is a domain-prefixed path, e.g. "example.org/department".
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
	// ValueExpression is a CEL expression over the token's claims (exposed
	// as "claims") returning a string or a list of strings.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	ValueExpression string `json:"valueExpression"`
}

// ClaimMappings maps a verified token's claims onto the Kubernetes user's
// identity attributes.
type ClaimMappings struct {
	// Username maps the user's name. Required.
	Username PrefixedClaimOrExpression `json:"username"`
	// Groups maps the user's group memberships.
	// +optional
	Groups PrefixedClaimOrExpression `json:"groups,omitempty"`
	// UID maps the user's unique identifier.
	// +optional
	UID ClaimOrExpression `json:"uid,omitempty"`
	// Extra maps additional user attributes.
	// +optional
	// +listType=map
	// +listMapKey=key
	Extra []ExtraMapping `json:"extra,omitempty"`
}

// UserValidationRule rejects a mapped user that doesn't satisfy it.
type UserValidationRule struct {
	// Expression is a CEL expression over the mapped user (exposed as
	// "user", with "username", "uid", "groups" and "extra" fields) that
	// must evaluate to true, e.g. "!user.username.startsWith('system:')".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	Expression string `json:"expression"`
	// Message is returned when Expression evaluates to false.
	// +optional
	Message string `json:"message,omitempty"`
}

// IdentityProviderSpec configures how users are discovered from an OIDC
// issuer's JWTs, mirroring one jwt[] entry of Kubernetes'
// AuthenticationConfiguration (apiserver.config.k8s.io).
type IdentityProviderSpec struct {
	// Issuer identifies the token issuer and its acceptable audiences.
	Issuer IdentityProviderIssuer `json:"issuer"`
	// ClaimValidationRules reject tokens whose claims don't satisfy them.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	ClaimValidationRules []ClaimValidationRule `json:"claimValidationRules,omitempty"`
	// ClaimMappings maps claims onto the user's identity.
	ClaimMappings ClaimMappings `json:"claimMappings"`
	// UserValidationRules reject mapped users that don't satisfy them.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	UserValidationRules []UserValidationRule `json:"userValidationRules,omitempty"`
}

// IdentityProviderStatus reports whether an identity provider is in use and
// the state of its signing key cache.
type IdentityProviderStatus struct {
	// ObservedGeneration is the metadata.generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions reports this identity provider's state, e.g. Ready.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchMergeKey:"type" patchStrategy:"merge"`
	// SigningKeys is the number of signing keys currently cached for the
	// issuer.
	// +optional
	SigningKeys int32 `json:"signingKeys,omitempty"`
	// LastKeyRefreshTime is when the signing keys were last fetched
	// successfully.
	// +optional
	LastKeyRefreshTime *metav1.Time `json:"lastKeyRefreshTime,omitempty"`
	// UsernamePrefix is the prefix kontinuum forces onto every username
	// this provider maps. Always empty for a ClusterIdentityProvider;
	// "tenant:<namespace>:" for a namespaced IdentityProvider, so a
	// tenant's issuer can never mint another tenant's or a cluster-wide
	// identity.
	// +optional
	UsernamePrefix string `json:"usernamePrefix,omitempty"`
	// GroupsPrefix is the prefix kontinuum forces onto every group this
	// provider maps — see UsernamePrefix.
	// +optional
	GroupsPrefix string `json:"groupsPrefix,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=idp
// +kubebuilder:printcolumn:name="Issuer",type="string",JSONPath=".spec.issuer.url"
// +kubebuilder:printcolumn:name="Keys",type="integer",JSONPath=".status.signingKeys"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// IdentityProvider authenticates a tenant's users against an OIDC issuer,
// using CEL to map token claims onto Kubernetes users. Every identity it
// produces is confined to its own namespace's tenant: kontinuum prefixes
// usernames and groups with "tenant:<namespace>:" (see
// IdentityProviderStatus.UsernamePrefix), so access is only ever granted
// through RoleBindings that name those prefixed subjects.
type IdentityProvider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec IdentityProviderSpec `json:"spec"`
	// +optional
	Status IdentityProviderStatus `json:"status"`
}

// +kubebuilder:object:root=true

// IdentityProviderList is a list of IdentityProvider.
type IdentityProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []IdentityProvider `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=cidp
// +kubebuilder:printcolumn:name="Issuer",type="string",JSONPath=".spec.issuer.url"
// +kubebuilder:printcolumn:name="Keys",type="integer",JSONPath=".status.signingKeys"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ClusterIdentityProvider authenticates users against an OIDC issuer
// cluster-wide, using CEL to map token claims onto Kubernetes users. Unlike
// IdentityProvider, the identities it maps are used as-is, so it can grant
// cluster-scoped access (e.g. via ClusterRoleBindings or the configured
// admin groups) — only cluster administrators should be allowed to create
// one.
type ClusterIdentityProvider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec IdentityProviderSpec `json:"spec"`
	// +optional
	Status IdentityProviderStatus `json:"status"`
}

// +kubebuilder:object:root=true

// ClusterIdentityProviderList is a list of ClusterIdentityProvider.
type ClusterIdentityProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ClusterIdentityProvider `json:"items"`
}
