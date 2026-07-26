package registry_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	fakeapiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clienttesting "k8s.io/client-go/testing"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

func TestCustomResourceDefinition(t *testing.T) {
	t.Parallel()

	crd := registry.CustomResourceDefinition()

	assert.Equal(t, "kontinuums.kontinuum.sh", crd.Name)
	assert.Equal(t, v1alpha2.GroupName, crd.Spec.Group)
	assert.Equal(t, apiextensionsv1.ClusterScoped, crd.Spec.Scope)
	assert.Equal(t, "Kontinuum", crd.Spec.Names.Kind)
	assert.Equal(t, "KontinuumList", crd.Spec.Names.ListKind)
	assert.Equal(t, "kontinuums", crd.Spec.Names.Plural)
	assert.Equal(t, "kontinuum", crd.Spec.Names.Singular)

	require.Len(t, crd.Spec.Versions, 1)

	version := crd.Spec.Versions[0]

	assert.Equal(t, v1alpha2.APIVersion, version.Name)
	assert.True(t, version.Served)
	assert.True(t, version.Storage)
	require.NotNil(t, version.Subresources)
	assert.NotNil(t, version.Subresources.Status)

	require.NotNil(t, version.Schema)
	require.NotNil(t, version.Schema.OpenAPIV3Schema)

	specSchema, hasSpec := version.Schema.OpenAPIV3Schema.Properties["spec"]
	require.True(t, hasSpec)
	assert.Contains(t, specSchema.Properties, "region")
	assert.Contains(t, specSchema.Properties, "zone")

	statusSchema, hasStatus := version.Schema.OpenAPIV3Schema.Properties["status"]
	require.True(t, hasStatus)
	assert.Contains(t, statusSchema.Properties, "role")
	assert.Contains(t, statusSchema.Properties, "lastHeartbeatTime")

	wantColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Role", Type: "string", JSONPath: ".status.role"},
		{Name: "Region", Type: "string", JSONPath: ".spec.region"},
		{Name: "Zone", Type: "string", JSONPath: ".spec.zone"},
	}
	assert.Equal(t, wantColumns, version.AdditionalPrinterColumns)
}

// TestApplyCRDCreatesWhenMissing covers the common case: no CRD registered
// yet, so ApplyCRD's Create succeeds outright.
func TestApplyCRDCreatesWhenMissing(t *testing.T) {
	t.Parallel()

	crds := fakeapiextensions.NewSimpleClientset().ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, newFakeClient(t), slog.Default()))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition().Spec, crd.Spec)
}

// TestApplyCRDUpdatesStaleDefinition is the regression case: a CRD left
// over from a previous run — e.g. one still only serving a since-removed
// API version like v1alpha1 — must be reconciled to the current spec
// rather than left in place, which is what Create alone did.
func TestApplyCRDUpdatesStaleDefinition(t *testing.T) {
	t.Parallel()

	stale := registry.CustomResourceDefinition()
	stale.Spec.Versions[0].Name = "v1alpha1"

	crds := fakeapiextensions.NewSimpleClientset(stale).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, newFakeClient(t), slog.Default()))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition().Spec, crd.Spec)
}

// TestApplyCRDNoopWhenAlreadyCurrent asserts ApplyCRD skips the Update call
// once the stored spec already matches — otherwise every retry in
// ensureCRD's poll loop would churn the CRD's resourceVersion forever.
func TestApplyCRDNoopWhenAlreadyCurrent(t *testing.T) {
	t.Parallel()

	current := registry.CustomResourceDefinition()

	crds := fakeapiextensions.NewSimpleClientset(current).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, newFakeClient(t), slog.Default()))

	crd, err := crds.Get(context.Background(), current.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, current.ResourceVersion, crd.ResourceVersion,
		"spec already matched the desired definition, so no update should have been issued")
}

// newStaleStoredVersionsError reproduces the real apiextensions validation
// failure that fires when a CRD update's spec.versions drops a version
// still referenced by status.storedVersions — the shape
// TestApplyCRDMigratesOffStaleStoredVersion's injected failure has to match
// for ApplyCRD's isStaleStoredVersionError check to recognize it.
func newStaleStoredVersionsError() error {
	gk := schema.GroupKind{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition"}
	errs := field.ErrorList{
		field.Invalid(field.NewPath("status", "storedVersions").Index(0), "v1alpha1", "must appear in spec.versions"),
	}

	return apierrors.NewInvalid(gk, "kontinuums.kontinuum.sh", errs)
}

// TestApplyCRDMigratesOffStaleStoredVersion is the regression case for a
// CRD registered before the Role-into-status migration whose
// status.storedVersions still referenced the removed v1alpha1 version, so
// the ordinary spec update ApplyCRD attempts first was rejected forever. It
// simulates that rejection via a reactor (the fake clientset itself never
// validates status.storedVersions), and asserts ApplyCRD recovers by
// deleting the stale Kontinuum and resetting status.storedVersions, rather
// than keeping the removed version around permanently.
func TestApplyCRDMigratesOffStaleStoredVersion(t *testing.T) {
	t.Parallel()

	existing := registry.CustomResourceDefinition()
	existing.Spec.Versions[0].Name = "v1alpha1"
	existing.Status.StoredVersions = []string{"v1alpha1"}

	clientset := fakeapiextensions.NewSimpleClientset(existing)

	failuresLeft := 1

	clientset.PrependReactor("update", "customresourcedefinitions",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			if failuresLeft == 0 {
				return false, nil, nil
			}

			failuresLeft--

			return true, nil, newStaleStoredVersionsError()
		})

	crds := clientset.ApiextensionsV1().CustomResourceDefinitions()
	kontinuums := newFakeClient(t, &v1alpha2.Kontinuum{ObjectMeta: metav1.ObjectMeta{Name: "leftover"}})

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, kontinuums, slog.Default()))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition().Spec, crd.Spec)
	assert.Equal(t, []string{v1alpha2.APIVersion}, crd.Status.StoredVersions)

	var list v1alpha2.KontinuumList

	require.NoError(t, kontinuums.List(context.Background(), &list))
	assert.Empty(t, list.Items, "the leftover kontinuum registered under the removed api version should have been deleted")
}
