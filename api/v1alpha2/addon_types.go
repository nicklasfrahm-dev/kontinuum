package v1alpha2

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AddonProvisioningMethod selects how an addon's manifests are applied —
// see AddonProvisioningSpec.
// +kubebuilder:validation:Enum=HelmUpgradeInstall;KubectlApply
type AddonProvisioningMethod string

const (
	// AddonProvisioningMethodHelmUpgradeInstall installs/upgrades the
	// addon as a real Helm release (the `helm upgrade --install`
	// equivalent) — a Helm release record is created, same as installing
	// the chart by hand.
	AddonProvisioningMethodHelmUpgradeInstall AddonProvisioningMethod = "HelmUpgradeInstall"
	// AddonProvisioningMethodKubectlApply renders the chart client-side
	// (the `helm template` equivalent) and applies the resulting
	// manifests directly via server-side apply (the `kubectl apply -f -`
	// equivalent) — no Helm release record is left behind, so the same
	// manifests can later be adopted by a GitOps tool like ArgoCD without
	// it needing to understand a pre-existing Helm release.
	AddonProvisioningMethodKubectlApply AddonProvisioningMethod = "KubectlApply"
)

// AddonProvisioningSpec configures how an addon's manifests are applied.
type AddonProvisioningSpec struct {
	// Method selects HelmUpgradeInstall (the default) or KubectlApply.
	// +optional
	// +kubebuilder:default=HelmUpgradeInstall
	Method AddonProvisioningMethod `json:"method,omitempty"`
}

// AddonLifecycleSpec configures how an addon is provisioned and, in the
// future, kept up to date.
type AddonLifecycleSpec struct {
	// +optional
	Provisioning AddonProvisioningSpec `json:"provisioning,omitempty"`
}

// AddonChartSpec identifies the Helm chart an addon installs from.
// Optional for the built-in addons (cilium, cert-manager — see
// AddonSpec.ReleaseName's own doc), whose own chart identity/version is
// baked into pkg/domain/addon and only needs a field here to override one
// piece of it; required for any other ReleaseName, since there's no
// built-in to fall back on.
type AddonChartSpec struct {
	// Repo is the Helm chart repository URL.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Repo string `json:"repo,omitempty"`
	// Name is the chart's own name within Repo.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name,omitempty"`
	// Version pins the chart version to install.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Version string `json:"version,omitempty"`
}

// AddonNamespaceSpec configures the namespace an addon installs into. A
// struct rather than a bare string so it can later grow labels (e.g. the
// pod-security-standard labels Cilium's own namespace needs to run
// privileged) without another breaking field-shape change.
type AddonNamespaceSpec struct {
	// Name is the namespace's own name. Empty means the built-in's own
	// default (for cilium/cert-manager) or an error (for any other
	// ReleaseName).
	// +optional
	Name string `json:"name,omitempty"`
}

// AddonSpec configures one addon. Enabled is a pointer, not a plain bool,
// so a hand-built Go value (e.g. a unit test's fake-client object, which
// bypasses CRD admission defaulting) can still distinguish "unset" from
// "explicitly false" — nil is treated as enabled, matching the effective
// behavior +kubebuilder:default=true gives any real Create/Update through
// the apiserver.
type AddonSpec struct {
	// TalosClusterRef names the TalosCluster this addon belongs to.
	TalosClusterRef TalosClusterReference `json:"talosClusterRef"`
	// ReleaseName identifies this addon — also its Helm release name.
	// Defaults to this Addon's own metadata.name when unset (see
	// addon.ReleaseName in pkg/domain/addon). "cilium" and "cert-manager"
	// are built-in: leaving Chart/Namespace/Values unset falls back to
	// this addon's own embedded defaults — see resolveAddon in
	// pkg/domain/addon. Any other ReleaseName has no built-in fallback —
	// Chart must be set.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	ReleaseName string `json:"releaseName,omitempty"`
	// Enabled controls whether this addon is installed at all. Defaults to
	// true; set to false when something else (e.g. ArgoCD) already owns
	// this addon's lifecycle.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
	// Chart identifies the Helm chart to install. Optional for the
	// built-in addons, whose own default applies unless overridden here;
	// required for any other ReleaseName.
	// +optional
	Chart *AddonChartSpec `json:"chart,omitempty"`
	// +optional
	Lifecycle AddonLifecycleSpec `json:"lifecycle,omitempty"`
	// Namespace configures the namespace this addon installs into.
	// +optional
	Namespace AddonNamespaceSpec `json:"namespace,omitempty"`
	// Values are user-provided Helm values, merged on top of Kontinuum's
	// own required values for this addon — user values win on conflict.
	// Free-form (not a typed struct) since chart values vary per addon and
	// per chart version.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Values *apiextensionsv1.JSON `json:"values,omitempty"`
}

// AddonStatus reports one addon's install/health state.
type AddonStatus struct {
	// Conditions reports this addon's state, e.g. Ready.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions" patchMergeKey:"type" patchStrategy:"merge"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:selectablefield:JSONPath=".spec.talosClusterRef.name"
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.talosClusterRef.name"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].reason"

// Addon represents one addon belonging to a TalosCluster, installed and
// health-probed by pkg/domain/addon's own AddonReconciler — an
// independent, owned resource: TalosCluster's own reconciler only seeds
// the two built-ins (see pkg/domain/addon's EnsureBuiltinSeeds) and
// aggregates Ready across whatever Addons reference it, built-in or
// fully custom alike.
type Addon struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec AddonSpec `json:"spec"`
	// +optional
	Status AddonStatus `json:"status"`
}

// +kubebuilder:object:root=true

// AddonList is a list of Addon.
type AddonList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Addon `json:"items"`
}
