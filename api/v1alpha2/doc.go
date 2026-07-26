// Package v1alpha2 contains the kontinuum.sh/v1alpha2 API: the Kontinuum
// custom resource kontinuum's server registry (pkg/registry) uses to track
// every running kontinuum process.
//
// The crd generator's two paths (this package, plus ../v1alpha1) are
// deliberate: without both, the generated config/crd manifest would only
// ever show one version, when the CRD registry.CustomResourceDefinition
// actually applies lists both v1alpha2 (storage, marked
// +kubebuilder:storageversion below) and the conversion-webhook-served
// v1alpha1. That manifest is go:embedded and parsed by
// registry.CustomResourceDefinition (see config/crd's embed) — it's the
// one source of truth for the schema, required fields, enums, printer
// columns, and the region/zone CEL rule, so `make generate` must be rerun
// after any change to these markers. The one thing it doesn't include is
// the conversion webhook stanza, since the CABundle and URL are only known
// at runtime — that's patched in by registry.CustomResourceDefinition
// after loading.
//
//go:generate go tool controller-gen object paths=.
//go:generate go tool controller-gen crd paths=. paths=../v1alpha1 output:crd:artifacts:config=../../config/crd
package v1alpha2
