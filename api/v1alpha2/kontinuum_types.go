package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// RoleControlPlane identifies a Kontinuum as the read-write entrypoint —
	// KONTINUUM_SERVER_REGION and KONTINUUM_SERVER_ZONE are both unset.
	RoleControlPlane = "controlplane"
	// RoleWorker identifies a Kontinuum as managing a single region and zone.
	RoleWorker = "worker"

	// DefaultSecretNamespace is where status.secretRef.namespace points by
	// default — created automatically if it doesn't already exist, since
	// Kontinuum is cluster-scoped and has no namespace of its own to fall
	// back to. This is a namespace name, not a credential — gosec's G101
	// flags it purely because "Secret" appears in the identifier.
	//
	//nolint:gosec // false positive: a namespace name, not a credential
	DefaultSecretNamespace = "kontinuum-system"
)

// This marker is the CRD's actual, authoritative region/zone invariant —
// see registry.CustomResourceDefinition, which applies config/crd's
// generated manifest (see api/v1alpha2/doc.go) rather than hand-building
// the schema. The rule mirrors registry.Role's own check exactly, and the
// message matches registry.ErrRegionZoneRequired, so a rejection at the
// apiserver reads the same as one from this process's own startup config.
// The line exceeds this repo's normal length limit, but splitting a
// kubebuilder marker across lines isn't supported, so it's exempted rather
// than shortened.
//
//nolint:lll
// +kubebuilder:validation:XValidation:rule="(!has(self.region) || self.region == '') == (!has(self.zone) || self.zone == '')",message="region and zone must both be set, or both be empty"

// KontinuumSpec describes a single running kontinuum process.
type KontinuumSpec struct {
	// Region is the region this process manages. Empty when both Region and
	// Zone are empty, in which case the process is the control-plane entrypoint.
	Region string `json:"region,omitempty"`
	// Zone is the availability zone this process manages. Empty when both
	// Region and Zone are empty, in which case the process is the
	// control-plane entrypoint.
	Zone string `json:"zone,omitempty"`
}

// KontinuumStatus reports the last time a Kontinuum reported in. Every
// field is +optional despite lacking omitempty: with the status subresource
// enabled (see the +kubebuilder:subresource:status marker below), the
// apiserver always strips whatever status the main resource endpoint's
// Create/Update payload carries before validating it — status is only ever
// populated afterward, via the status subresource. Requiring these fields
// would make every Create fail structural-schema validation against that
// always-empty status.
type KontinuumStatus struct {
	// Role is either RoleControlPlane or RoleWorker, derived from
	// spec.region and spec.zone — see registry.Role.
	// +optional
	// +kubebuilder:validation:Enum=controlplane;worker
	Role string `json:"role"`
	// LastHeartbeatTime is when this process last reported in. The server
	// registry deletes a Kontinuum whose LastHeartbeatTime is older than
	// its configured stale threshold (5 minutes by default).
	// +optional
	LastHeartbeatTime metav1.Time `json:"lastHeartbeatTime"`
	// Version is this process's build version.
	// +optional
	Version string `json:"version"`
	// SecretRef points to the Secret holding this process's confidential
	// configuration (storage connection string and any other credentials) —
	// see KontinuumSecretReference. It is never inlined into status
	// directly: unlike spec/status, a Secret's RBAC can be restricted
	// independently of who can read this broadly-visible Kontinuum object.
	// +optional
	SecretRef KontinuumSecretReference `json:"secretRef"`
	// Config is this process's own non-confidential configuration, shown on
	// its per-instance settings page (/app/kontinuums/{name}). It is in fact
	// pkg/config.Config itself: Server/Log/OIDC below are the very types
	// pkg/config.Config's own fields are declared with (see that package),
	// so there is exactly one definition of each shape, not two that could
	// drift apart, and a value read here maps 1:1 onto the env vars that
	// produced it. The one accepted duplication is Region/Zone, which also
	// live on KontinuumSpec — harmless, and it's what keeps the mapping
	// exact rather than Server needing an asterisk. Storage here is always
	// the credential-stripped display copy (see pkg/config.RedactStorage) —
	// never the raw, connectable value pkg/config.Config.Server.Storage
	// holds before that redaction — see pkg/cli/serve.go's displayConfig,
	// the one place that redaction happens.
	// +optional
	Config KontinuumConfigStatus `json:"config"`
}

// KontinuumConfigStatus is pkg/config.Config's own shape — see
// KontinuumStatus.Config's doc.
type KontinuumConfigStatus struct {
	// +optional
	Server KontinuumServerConfigStatus `json:"server"`
	// +optional
	Log KontinuumLogConfigStatus `json:"log"`
	// +optional
	OIDC KontinuumOIDCConfigStatus `json:"oidc"`
}

// KontinuumServerConfigStatus is pkg/config.ServerConfig, referenced
// directly by that package rather than redefined — see
// pkg/config.Config.Server. Region/Zone duplicate KontinuumSpec's own
// fields — see KontinuumStatus.Config's doc for why that's accepted.
type KontinuumServerConfigStatus struct {
	// Addr is the listener address this process is serving on.
	// +optional
	Addr string `default:":8080" json:"addr"`
	// Storage is the storage connection string with any embedded
	// credentials stripped (see pkg/config.RedactStorage). The
	// credential-bearing original lives in the Secret
	// KontinuumStatus.SecretRef points to.
	// +optional
	Storage string `default:"sqlite://kontinuum.db" json:"storage"`
	// Region duplicates spec.region — see this type's own doc.
	// +optional
	Region string `default:"" json:"region"`
	// Zone duplicates spec.zone — see this type's own doc.
	// +optional
	Zone string `default:"" json:"zone"`
}

// KontinuumLogConfigStatus is pkg/config.LogConfig, referenced directly by
// that package rather than redefined — see pkg/config.Config.Log.
type KontinuumLogConfigStatus struct {
	// Level is one of: debug, info, warn, error.
	// +optional
	Level string `default:"warn" json:"level"`
	// Format is one of: console, text, json.
	// +optional
	Format string `default:"json" json:"format"`
}

// KontinuumOIDCConfigStatus is pkg/config.OIDCConfig, referenced directly
// by that package rather than redefined — see pkg/config.Config.OIDC.
// Enabled has no `default` tag: pkg/config.Load never sets it directly
// (its string-only field walk skips bool fields entirely), it's always
// derived as IssuerURL != "" — see pkg/cli/serve.go's displayConfig.
type KontinuumOIDCConfigStatus struct {
	// Enabled is true when this process has OIDC authentication configured.
	// +optional
	Enabled bool `json:"enabled"`
	// IssuerURL is the OIDC issuer URL. Empty when Enabled is false.
	// +optional
	//nolint:tagliatelle // "issuerURL" (acronym kept uppercase) is the established Kubernetes API convention
	IssuerURL string `default:"" json:"issuerURL"`
	// ClientID is the OAuth 2.0 public client ID registered with the
	// issuer.
	// +optional
	//nolint:tagliatelle // "clientID" (acronym kept uppercase) is the established Kubernetes API convention
	ClientID string `default:"kontinuum" json:"clientID"`
	// RedirectURL is the browser login flow's callback URL.
	// +optional
	//nolint:tagliatelle // "redirectURL" (acronym kept uppercase) is the established Kubernetes API convention
	RedirectURL string `default:"http://localhost:8080/app" json:"redirectURL"`
	// AdminGroups is a comma-delimited list of OIDC groups granted full
	// access. Empty when Enabled is false.
	// +optional
	AdminGroups string `default:"" json:"adminGroups"`
}

// KontinuumSecretReference points to the Secret holding a Kontinuum's
// confidential configuration. The Secret's keys match pkg/config's
// KONTINUUM_-prefixed env var names (e.g. KONTINUUM_SERVER_STORAGE), so it
// can be mounted straight into a pod via envFrom with no translation layer.
type KontinuumSecretReference struct {
	// Name is the Secret's name.
	// +optional
	Name string `json:"name"`
	// Namespace is the Secret's namespace. Defaults to
	// DefaultSecretNamespace.
	// +optional
	Namespace string `json:"namespace"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Role",type="string",JSONPath=".status.role"
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".spec.region"
// +kubebuilder:printcolumn:name="Zone",type="string",JSONPath=".spec.zone"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".status.version"

// Kontinuum represents a single running kontinuum process registered in the
// central server registry.
type Kontinuum struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec KontinuumSpec `json:"spec"`
	// +optional
	Status KontinuumStatus `json:"status"`
}

// +kubebuilder:object:root=true

// KontinuumList is a list of Kontinuum.
type KontinuumList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Kontinuum `json:"items"`
}
