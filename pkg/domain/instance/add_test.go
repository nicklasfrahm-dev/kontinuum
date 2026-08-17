package instance_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

const (
	testNamespace = "acme"
	testAddress   = "10.0.0.5"
	testHostname  = "node1.example.com"
)

// stubResolver is a test-only instance.Resolver that never touches the
// network — LookupHost either returns addrs or, when addrs is empty,
// errLookupFailed.
type stubResolver struct {
	addrs []string
}

var errLookupFailed = errors.New("stub resolver: lookup failed")

func (s stubResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	if len(s.addrs) == 0 {
		return nil, errLookupFailed
	}

	return s.addrs, nil
}

func TestAddCreatesUnclaimedInstance(t *testing.T) {
	t.Parallel()

	hubClient := newFakeClient(t)

	got, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	assert.Equal(t, testNamespace, got.Namespace)
	assert.Equal(t, []string{testAddress}, got.Spec.Interfaces)
	assert.Empty(t, got.Labels[v1alpha2.LabelClaimedBy])

	var fetched v1alpha2.Instance
	require.NoError(t, hubClient.Get(t.Context(), client.ObjectKeyFromObject(got), &fetched))
}

func TestAddIsIdempotentForSameAddress(t *testing.T) {
	t.Parallel()

	hubClient := newFakeClient(t)

	first, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	second, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	assert.Equal(t, first.Name, second.Name, "re-submitting the same address must adopt the same object")

	var list v1alpha2.InstanceList
	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(testNamespace)))
	assert.Len(t, list.Items, 1, "re-submitting the same address must not create a duplicate")
}

func TestAddDifferentAddressesGetDifferentNames(t *testing.T) {
	t.Parallel()

	hubClient := newFakeClient(t)

	first, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	second, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: "10.0.0.6",
	})
	require.NoError(t, err)

	assert.NotEqual(t, first.Name, second.Name)
}

func TestAddTrimsWhitespaceFromAddress(t *testing.T) {
	t.Parallel()

	hubClient := newFakeClient(t)

	untrimmed, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: "  " + testAddress + "  ",
	})
	require.NoError(t, err)

	trimmed, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	assert.Equal(t, trimmed.Name, untrimmed.Name)
	assert.Equal(t, []string{testAddress}, untrimmed.Spec.Interfaces)
}

func TestAddRejectsMissingNamespace(t *testing.T) {
	t.Parallel()

	_, err := instance.Add(t.Context(), newFakeClient(t), instance.AddOptions{Address: testAddress})
	require.Error(t, err)
}

func TestAddRejectsMissingAddress(t *testing.T) {
	t.Parallel()

	_, err := instance.Add(t.Context(), newFakeClient(t), instance.AddOptions{Namespace: testNamespace})
	require.Error(t, err)
}

func TestAddRejectsBlankAddress(t *testing.T) {
	t.Parallel()

	_, err := instance.Add(t.Context(), newFakeClient(t), instance.AddOptions{
		Namespace: testNamespace, Address: "   ",
	})
	require.Error(t, err)
}

func TestAddResolvesHostnameToIP(t *testing.T) {
	t.Parallel()

	hubClient := newFakeClient(t)

	got, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testHostname,
		Resolver: stubResolver{addrs: []string{testAddress}},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{testAddress}, got.Spec.Interfaces, "spec must carry the resolved IP, not the hostname")
	assert.Equal(t, testHostname, got.Annotations[instance.AnnotationHostname])
}

func TestAddByHostnameMatchesAddByResolvedIP(t *testing.T) {
	t.Parallel()

	hubClient := newFakeClient(t)

	byHostname, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testHostname,
		Resolver: stubResolver{addrs: []string{testAddress}},
	})
	require.NoError(t, err)

	byIP, err := instance.Add(t.Context(), hubClient, instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	assert.Equal(t, byIP.Name, byHostname.Name, "hostname and its resolved IP must converge on one Instance")

	var list v1alpha2.InstanceList
	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(testNamespace)))
	assert.Len(t, list.Items, 1, "hostname and its resolved IP must not create two separate Instances")
}

func TestAddDoesNotAnnotateIPLiteral(t *testing.T) {
	t.Parallel()

	got, err := instance.Add(t.Context(), newFakeClient(t), instance.AddOptions{
		Namespace: testNamespace, Address: testAddress,
	})
	require.NoError(t, err)

	assert.NotContains(t, got.Annotations, instance.AnnotationHostname)
}

func TestAddSurfacesResolveFailure(t *testing.T) {
	t.Parallel()

	_, err := instance.Add(t.Context(), newFakeClient(t), instance.AddOptions{
		Namespace: testNamespace, Address: testHostname,
		Resolver: stubResolver{},
	})
	require.Error(t, err)
}
