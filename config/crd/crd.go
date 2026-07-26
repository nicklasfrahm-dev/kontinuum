// Package crd embeds every CustomResourceDefinition manifest controller-gen
// generates in this directory from kubebuilder markers (see
// api/v1alpha2/doc.go's go:generate directive) — currently just kontinuums,
// but the wildcard picks up any future CRD's generated yaml here too, with
// no change needed in this file. It exists purely so
// pkg/domain/registry.CustomResourceDefinition — the only consumer — can
// embed these: an embed pattern can't reach outside the embedding file's
// own directory tree, and that package lives well outside config/crd.
package crd

import "embed"

// Files holds every generated *.yaml manifest in this directory, still
// missing each CRD's runtime-only conversion webhook clientConfig (URL and
// CABundle) where applicable — see registry.CustomResourceDefinition, which
// reads the one it needs by filename and patches that in.
//
//go:embed *.yaml
var Files embed.FS
