// Package v1alpha2 contains the kontinuum.sh/v1alpha2 API: the Kontinuum
// custom resource kontinuum's server registry (pkg/registry) uses to track
// every running kontinuum process.
//
// The crd generator's two paths (this package, plus ../v1alpha1) are
// deliberate: without both, the generated config/crd manifest would only
// ever show one version, when the CRD registry.CustomResourceDefinition
// actually applies lists both v1alpha2 (storage, marked
// +kubebuilder:storageversion below) and the conversion-webhook-served
// v1alpha1. That manifest is a reference artifact only, never read by this
// app — it doesn't include the conversion webhook stanza, which stays
// hand-rolled in crd.go.
//
//go:generate go tool controller-gen object paths=.
//go:generate go tool controller-gen crd paths=. paths=../v1alpha1 output:crd:artifacts:config=../../config/crd
package v1alpha2
