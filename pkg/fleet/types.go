// Package fleet decodes, merges, expands and validates kontinuum.yml — the
// declarative Fleet kontinuum up converges the world to. A Fleet is a
// client-side file, never served by the apiserver, so it has its own API
// group, up.kontinuum.sh, and kubectl users never go looking for it.
package fleet

const (
	// APIVersion is the only apiVersion a kontinuum.yml may declare.
	APIVersion = "up.kontinuum.sh/v1alpha1"
	// Kind is the only kind a kontinuum.yml may declare.
	Kind = "Fleet"
)

// Fleet is kontinuum.yml's own shape. Field names mirror the KONTINUUM_*
// configuration they map onto (see ToConfig), so a Fleet translates
// one-to-one onto each zone's kontinuum-env.
type Fleet struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
}

// Metadata names a Fleet.
type Metadata struct {
	// Name identifies the fleet in up's own output.
	Name string `json:"name"`
}

// Spec is everything a Fleet declares.
type Spec struct {
	// Version must equal the CLI's own version: up runs the CLI's own
	// controllers, and zones deploy the image of the version they're
	// reconciled with.
	Version string `json:"version"`
	// Frontend is where the fleet's UI is served.
	Frontend Frontend `json:"frontend"`
	// Storage is the shared state every zone reconciles against.
	Storage Storage `json:"storage"`
	// Talos pins the Talos version zones are bootstrapped with. Optional.
	Talos Versioned `json:"talos"`
	// Kubernetes pins the Kubernetes version zones run. Optional.
	Kubernetes Versioned `json:"kubernetes"`
	// DNS is where zone records and the frontend host are published.
	DNS DNS `json:"dns"`
	// ACME is where every zone's certificates are ordered from.
	ACME ACME `json:"acme"`
	// Auth is how users and API clients authenticate.
	Auth Auth `json:"auth"`
	// Zones is the complete list of zones in the fleet.
	Zones []Zone `json:"zones"`
}

// Frontend maps onto KONTINUUM_SERVER_FRONTEND_HOST.
type Frontend struct {
	// Host is the one hostname the UI is served on.
	Host string `json:"host"`
}

// Storage maps onto KONTINUUM_SERVER_STORAGE and
// KONTINUUM_SERVER_STORAGE_MODE.
type Storage struct {
	// Mode overrides storage mode inference, e.g. behind split-horizon DNS.
	// Optional: one of Auto (the default), Direct or NATS.
	Mode string `json:"mode,omitempty"`
	// URI is the storage connection string — a secret field, so it must be
	// exactly one ${env:} or ${file:} reference.
	URI string `json:"uri"`
}

// Versioned pins a component's version.
type Versioned struct {
	Version string `json:"version,omitempty"`
}

// DNS maps onto KONTINUUM_SERVER_DNS_*.
type DNS struct {
	// Domain is the base domain zones are published under, as
	// <zone>.<region>.<domain>.
	Domain string `json:"domain"`
	// Provider is a PascalCase DNS provider, e.g. Cloudflare or AWS.
	Provider string `json:"provider"`
	// Credential is the flat mapping of environment variables the provider
	// reads. Every value is a secret field.
	Credential map[string]string `json:"credential,omitempty"`
}

// ACME selects one of several declared ACME providers.
type ACME struct {
	// Provider names the key of Providers every zone's certificates are
	// ordered from.
	Provider string `json:"provider"`
	// Providers is keyed by an operator-chosen DNS-1123 label.
	Providers map[string]ACMEProvider `json:"providers,omitempty"`
}

// ACMEProvider maps onto KONTINUUM_ACME_* when selected.
type ACMEProvider struct {
	// Server is the ACME directory URL.
	Server string `json:"server"`
	// Email is the ACME account email.
	Email string `json:"email"`
	// ExternalAccountBinding binds to an existing CA account. Optional.
	ExternalAccountBinding *ExternalAccountBinding `json:"externalAccountBinding,omitempty"`
	// InsecureSkipVerify is for a Pebble test server only.
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
}

// ExternalAccountBinding maps onto
// KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_*.
type ExternalAccountBinding struct {
	KeyID string `json:"keyID"`
	// HMACKey is a secret field.
	HMACKey string `json:"hmacKey"`
}

// Auth maps onto KONTINUUM_OIDC_* and KONTINUUM_INSECURE_ALLOW_ANONYMOUS.
type Auth struct {
	// OIDC configures login through a public PKCE client. Mutually
	// exclusive with InsecureAllowAnonymous.
	OIDC *OIDC `json:"oidc,omitempty"`
	// InsecureAllowAnonymous allows anonymous, always-allowed access.
	InsecureAllowAnonymous bool `json:"insecureAllowAnonymous,omitempty"`
}

// OIDC maps onto KONTINUUM_OIDC_*. There is deliberately no client
// secret: the login flow is PKCE against a public client.
type OIDC struct {
	IssuerURL string `json:"issuerURL"`
	ClientID  string `json:"clientID,omitempty"`
	// AdminGroups is joined into KONTINUUM_OIDC_ADMIN_GROUPS.
	AdminGroups []string `json:"adminGroups,omitempty"`
}

// Zone is one zone and the Talos maintenance-mode nodes it bootstraps from.
type Zone struct {
	Region string `json:"region"`
	Zone   string `json:"zone"`
	// Instances are the zone's control-plane nodes: 1, 3 or 5.
	Instances []Instance `json:"instances"`
}

// Instance is one maintenance-mode Talos node. An object rather than a
// bare address so per-instance fields can be added later.
type Instance struct {
	// Address is an IP or a hostname.
	Address string `json:"address"`
}

// Name is the zone's object name, <region>-<zone>.
func (z Zone) Name() string {
	return z.Region + "-" + z.Zone
}

// Addresses returns the zone's instance addresses, in order.
func (z Zone) Addresses() []string {
	addresses := make([]string, 0, len(z.Instances))
	for _, instance := range z.Instances {
		addresses = append(addresses, instance.Address)
	}

	return addresses
}
