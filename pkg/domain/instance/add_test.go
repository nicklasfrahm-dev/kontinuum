package instance_test

import (
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
)

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
