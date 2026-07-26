package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fakeapiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestApplyCRDCreatesWhenMissing covers the common case: no CRD registered
// yet, so applyCRD's Create succeeds outright.
func TestApplyCRDCreatesWhenMissing(t *testing.T) {
	t.Parallel()

	crds := fakeapiextensions.NewSimpleClientset().ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, applyCRD(context.Background(), crds))

	crd, err := crds.Get(context.Background(), crdName(), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, CustomResourceDefinition().Spec, crd.Spec)
}

// TestApplyCRDUpdatesStaleDefinition is the regression case: a CRD left
// over from a previous run — e.g. one still only serving a since-removed
// API version like v1alpha1 — must be reconciled to the current spec
// rather than left in place, which is what Create alone did.
func TestApplyCRDUpdatesStaleDefinition(t *testing.T) {
	t.Parallel()

	stale := CustomResourceDefinition()
	stale.Spec.Versions[0].Name = "v1alpha1"

	crds := fakeapiextensions.NewSimpleClientset(stale).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, applyCRD(context.Background(), crds))

	crd, err := crds.Get(context.Background(), crdName(), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, CustomResourceDefinition().Spec, crd.Spec)
}

// TestApplyCRDNoopWhenAlreadyCurrent asserts applyCRD skips the Update call
// once the stored spec already matches — otherwise every retry in
// ensureCRD's poll loop would churn the CRD's resourceVersion forever.
func TestApplyCRDNoopWhenAlreadyCurrent(t *testing.T) {
	t.Parallel()

	current := CustomResourceDefinition()

	crds := fakeapiextensions.NewSimpleClientset(current).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, applyCRD(context.Background(), crds))

	crd, err := crds.Get(context.Background(), crdName(), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, current.ResourceVersion, crd.ResourceVersion,
		"spec already matched the desired definition, so no update should have been issued")
}
