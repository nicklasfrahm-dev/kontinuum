// Package v1alpha1 is kontinuum.sh's original, now-frozen API version,
// superseded by api/v1alpha2 (which moved Role from spec into status). It is
// no longer served — see registry.CustomResourceDefinition, which lists it
// only so status.storedVersions on a CRD registered before that migration
// still validates. This package exists purely so that entry has a schema to
// point at; nothing in kontinuum constructs these types anymore.
//
//go:generate go tool controller-gen object paths=.
package v1alpha1
