// Package v1alpha2 contains the kontinuum.sh/v1alpha2 API: the Kontinuum
// custom resource kontinuum's server registry (pkg/registry) uses to track
// every running kontinuum process.
//
//go:generate go tool controller-gen object paths=.
//go:generate go tool controller-gen crd paths=. output:crd:artifacts:config=../../config/crd
package v1alpha2
