package addon //nolint:testpackage // exercises unexported parseCRDRefs directly

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCRDManifest is one CRD with two versions — v1 served, v1alpha1 not
// — plus an unrelated Deployment that parseCRDRefs must ignore.
const testCRDManifest = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names:
    kind: Widget
  versions:
    - name: v1
      served: true
      storage: true
    - name: v1alpha1
      served: false
      storage: false
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: not-a-crd
`

// TestParseCRDRefsSkipsUnservedVersions covers the actual bug found via
// the real e2e run against Gateway API's own BackendTLSPolicy CRD: its
// v1alpha3 version is listed in spec.versions but served: false — an
// apiserver never serves an unserved version, so a RESTMapper check
// against it would report "not discoverable" forever, not just until
// the apiserver catches up. parseCRDRefs must exclude unserved versions
// from the GVKs crdsReady checks, or a chart that keeps an old version
// around for schema history would never converge.
func TestParseCRDRefsSkipsUnservedVersions(t *testing.T) {
	t.Parallel()

	refs, err := parseCRDRefs(testCRDManifest)
	require.NoError(t, err)
	require.Len(t, refs, 1, "the Deployment must be ignored, only the CRD counted")

	widget := refs[0]
	assert.Equal(t, "widgets.example.com", widget.name)
	require.Len(t, widget.gvks, 1, "only the served version must be checked")
	assert.Equal(t, "v1", widget.gvks[0].Version)
}
