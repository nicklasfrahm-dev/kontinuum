package v1alpha2

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
// AddonSpec.Name's own doc), whose own chart identity/version is baked
// into this controller and only needs a field here to override one piece
// of it; required for any other Name, since there's no built-in to fall
// back on.
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

// AddonSpec configures one addon this TalosCluster installs. Enabled is a
// pointer, not a plain bool, so a hand-built Go value (e.g. a unit test's
// fake-client object, which bypasses CRD admission defaulting) can still
// distinguish "unset" from "explicitly false" — nil is treated as enabled,
// matching the effective behavior +kubebuilder:default=true gives any real
// Create/Update through the apiserver.
type AddonSpec struct {
	// Name identifies this addon — also its Helm release name. "cilium"
	// and "cert-manager" are built-in: TalosCluster installs both by
	// default even with no entry here at all, using the chart/namespace/
	// values baked into this controller. An entry with one of those names
	// overrides that built-in's own fields one at a time — an unset field
	// keeps the built-in's own value. Any other Name is a fully
	// user-defined addon with no built-in default — Chart must be set.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// Enabled controls whether this addon is installed at all. Defaults to
	// true; set to false when something else (e.g. ArgoCD) already owns
	// this addon's lifecycle.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
	// Chart identifies the Helm chart to install. Optional for the
	// built-in addons, whose own default applies unless overridden here;
	// required for any other Name.
	// +optional
	Chart *AddonChartSpec `json:"chart,omitempty"`
	// +optional
	Lifecycle AddonLifecycleSpec `json:"lifecycle,omitempty"`
	// Namespace this addon installs into. Empty means the built-in's own
	// default (for cilium/cert-manager) or an error (for any other Name).
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Values are user-provided Helm values, merged on top of Kontinuum's
	// own required values for this addon — user values win on conflict.
	// Free-form (not a typed struct) since chart values vary per addon and
	// per chart version.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Values *apiextensionsv1.JSON `json:"values,omitempty"`
}
