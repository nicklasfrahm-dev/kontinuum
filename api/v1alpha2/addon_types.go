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

// AddonSpec configures one addon this TalosCluster installs. Enabled is a
// pointer, not a plain bool, so a hand-built Go value (e.g. a unit test's
// fake-client object, which bypasses CRD admission defaulting) can still
// distinguish "unset" from "explicitly false" — nil is treated as enabled,
// matching the effective behavior +kubebuilder:default=true gives any real
// Create/Update through the apiserver.
type AddonSpec struct {
	// Enabled controls whether this addon is installed at all. Defaults to
	// true; set to false when something else (e.g. ArgoCD) already owns
	// this addon's lifecycle.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
	// +optional
	Lifecycle AddonLifecycleSpec `json:"lifecycle,omitempty"`
	// Namespace this addon installs into. Empty means the reconciler's own
	// per-addon default.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Version pins the chart version to install. Empty means the
	// reconciler's own pinned default for this addon.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Version string `json:"version,omitempty"`
	// Values are user-provided Helm values, merged on top of Kontinuum's
	// own required values for this addon — user values win on conflict.
	// Free-form (not a typed struct) since chart values vary per addon and
	// per chart version.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Values *apiextensionsv1.JSON `json:"values,omitempty"`
}

// AddonsSpec configures the addons a TalosCluster installs once its
// control plane is healthy — see this package's own pkg/domain/taloscluster
// for the reconciler that acts on it.
type AddonsSpec struct {
	// Cilium is this cluster's CNI — envoy and node LB IPAM enabled,
	// kube-proxy replaced.
	// +optional
	Cilium AddonSpec `json:"cilium,omitempty"`
	// CertManager provisions and renews the cluster's TLS certificates.
	// The hyphenated JSON key matches the project's own established name
	// (its chart/release/CLI are all "cert-manager", never "certManager").
	// +optional
	//nolint:tagliatelle // "cert-manager" is the tool's own established name, not a stray abbreviation
	CertManager AddonSpec `json:"cert-manager,omitempty"`
}
