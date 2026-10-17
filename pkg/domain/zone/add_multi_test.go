package zone_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	instancedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const (
	// secondTalosAddress and thirdTalosAddress complete a three-node
	// control plane alongside testTalosAddress.
	secondTalosAddress = "10.0.1.6"
	thirdTalosAddress  = "10.0.1.7"
	// resolvedHostnameAddress is what tableResolver resolves hostnames to.
	resolvedHostnameAddress = "10.0.1.9"
)

// threeTalosAddresses returns a three-node control plane's seed addresses.
func threeTalosAddresses() []string {
	return []string{testTalosAddress, secondTalosAddress, thirdTalosAddress}
}

// tableResolver resolves hostnames from a fixed table, failing any other.
type tableResolver map[string][]string

func (r tableResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	addrs, ok := r[host]
	if !ok {
		return nil, errStubLookupFailed
	}

	return addrs, nil
}

func listZoneInstances(t *testing.T, hubClient client.Client) []v1alpha2.Instance {
	t.Helper()

	var list v1alpha2.InstanceList

	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace),
		client.MatchingLabels{v1alpha2.LabelRegion: testRegion, v1alpha2.LabelZone: testZone}))

	return list.Items
}

func TestValidateInstanceCount(t *testing.T) {
	t.Parallel()

	for _, count := range []int{1, 3, 5} {
		require.NoError(t, zone.ValidateInstanceCount(count), count)
	}

	for _, count := range []int{0, 2, 4, 6, 7, -1} {
		require.ErrorIs(t, zone.ValidateInstanceCount(count), zone.ErrInvalidInstanceCount, count)
	}
}

func TestBuildAddObjectsCreatesOneInstancePerAddress(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.TalosAddresses = threeTalosAddresses()

	_, instances, pool, _ := zone.BuildAddObjects(opts)

	require.Len(t, instances, len(threeTalosAddresses()))

	for index, address := range threeTalosAddresses() {
		assert.Equal(t, instancedomain.NameFromAddress(address), instances[index].Name)
		assert.Equal(t, []string{address}, instances[index].Spec.Interfaces)
		assert.Equal(t, pool.Spec.Selector.MatchLabels, instances[index].Labels)
	}

	assert.Equal(t, int32(3), pool.Spec.Replicas)
}

func TestAddCreatesOneSeedInstancePerAddress(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	opts := testAddOptions()
	opts.TalosAddresses = threeTalosAddresses()

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)

	assert.Len(t, listZoneInstances(t, hubClient), len(threeTalosAddresses()))

	var pool v1alpha2.InstancePool
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &pool))
	assert.Equal(t, int32(3), pool.Spec.Replicas)

	// Re-running adopts the existing seeds instead of duplicating them.
	_, err = zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)
	assert.Len(t, listZoneInstances(t, hubClient), len(threeTalosAddresses()))
}

func TestAddRejectsEvenAndOversizedInstanceCounts(t *testing.T) {
	t.Parallel()

	for _, addresses := range [][]string{
		{testTalosAddress, secondTalosAddress},
		{testTalosAddress, secondTalosAddress, thirdTalosAddress, "10.0.1.8"},
		{"10.0.1.1", "10.0.1.2", "10.0.1.3", "10.0.1.4", testTalosAddress, secondTalosAddress},
	} {
		hubClient := newHubFakeClient(t)

		opts := testAddOptions()
		opts.TalosAddresses = addresses

		_, err := zone.Add(t.Context(), hubClient, opts)
		require.ErrorIs(t, err, zone.ErrInvalidInstanceCount, addresses)
		assert.Empty(t, listZoneInstances(t, hubClient))
	}
}

func TestAddRejectsDuplicateAddressWithinZone(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.TalosAddresses = []string{testTalosAddress, secondTalosAddress, testTalosAddress}

	_, err := zone.Add(t.Context(), newHubFakeClient(t), opts)
	require.ErrorIs(t, err, zone.ErrDuplicateInstanceAddress)
}

// TestAddRejectsTwoHostnamesResolvingToOneNode covers the duplicate check
// running after resolution: two different hostnames naming the same IP are
// the same node.
func TestAddRejectsTwoHostnamesResolvingToOneNode(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	opts := testAddOptions()
	opts.TalosAddresses = []string{"a.example.com", "b.example.com", thirdTalosAddress}
	opts.Resolver = tableResolver{"a.example.com": {resolvedHostnameAddress}, "b.example.com": {resolvedHostnameAddress}}

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.ErrorIs(t, err, zone.ErrDuplicateInstanceAddress)
	assert.Contains(t, err.Error(), `"a.example.com" and "b.example.com"`)
	assert.Empty(t, listZoneInstances(t, hubClient))
}

// TestAddCreatesNothingWhenOneAddressFailsToResolve covers resolving every
// address up front: one bad entry must not leave a partial zone behind.
func TestAddCreatesNothingWhenOneAddressFailsToResolve(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	opts := testAddOptions()
	opts.TalosAddresses = []string{testTalosAddress, "missing.example.com", thirdTalosAddress}
	opts.Resolver = tableResolver{}

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.Error(t, err)

	var zones v1alpha2.ZoneList
	require.NoError(t, hubClient.List(t.Context(), &zones))
	assert.Empty(t, zones.Items)
	assert.Empty(t, listZoneInstances(t, hubClient))
}

func TestAddRejectsExistingInstanceWithSeveralAddresses(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.TalosAddresses = threeTalosAddresses()
	opts.ExistingInstanceName = "already-registered"

	_, err := zone.Add(t.Context(), newHubFakeClient(t), opts)
	require.Error(t, err)
}

func TestResolveTalosAddresses(t *testing.T) {
	t.Parallel()

	const hostname = "node.example.com"

	ips, hostnames, err := zone.ResolveTalosAddresses(t.Context(),
		tableResolver{hostname: {resolvedHostnameAddress}}, []string{testTalosAddress, hostname})
	require.NoError(t, err)
	assert.Equal(t, []string{testTalosAddress, resolvedHostnameAddress}, ips)
	assert.Equal(t, []string{"", hostname}, hostnames)
}

func TestSplitTalosAddresses(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{testTalosAddress}, zone.SplitTalosAddresses(testTalosAddress))
	assert.Equal(t, []string{testTalosAddress, secondTalosAddress, thirdTalosAddress},
		zone.SplitTalosAddresses(" "+testTalosAddress+", "+secondTalosAddress+",,"+thirdTalosAddress+" "))
	assert.Empty(t, zone.SplitTalosAddresses(" , "))
}
