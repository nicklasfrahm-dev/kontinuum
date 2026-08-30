package fabric_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/fabric"
)

// testFabricCIDR/testZonePrefixLength are every test's own default
// fabric.Allocate(testFabricCIDR, testZonePrefixLength, ...) inputs;
// blockCIDR0/1/2 are the first three /24 blocks it carves out — shared
// across tests purely so goconst doesn't flag the repeated literal.
const (
	testFabricCIDR       = "10.0.0.0/16"
	testZonePrefixLength = 24

	blockCIDR0 = "10.0.0.0/24"
	blockCIDR1 = "10.0.1.0/24"
	blockCIDR2 = "10.0.2.0/24"
)

func TestAllocateFreshCarvesLowestBlocksInSortedOrder(t *testing.T) {
	t.Parallel()

	allocations, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, []string{"c", "a", "b"}, nil)
	require.NoError(t, err)
	require.Len(t, allocations, 3)

	assert.Equal(t, fabric.Allocation{Zone: "a", CIDR: blockCIDR0, GatewayIP: testBlockCIDR0GatewayIP}, allocations[0])
	assert.Equal(t, fabric.Allocation{Zone: "b", CIDR: blockCIDR1, GatewayIP: "10.0.1.254"}, allocations[1])
	assert.Equal(t, fabric.Allocation{Zone: "c", CIDR: blockCIDR2, GatewayIP: "10.0.2.254"}, allocations[2])
}

func TestAllocateIsStickyForAlreadyAllocatedZones(t *testing.T) {
	t.Parallel()

	previous := []fabric.PreviousAllocation{
		{Zone: "a", CIDR: blockCIDR0},
		{Zone: "b", CIDR: blockCIDR1},
		{Zone: "c", CIDR: blockCIDR2},
	}

	allocations, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, []string{"a", "b", "c"}, previous)
	require.NoError(t, err)
	require.Len(t, allocations, 3)

	assert.Equal(t, blockCIDR0, allocations[0].CIDR)
	assert.Equal(t, blockCIDR1, allocations[1].CIDR)
	assert.Equal(t, blockCIDR2, allocations[2].CIDR)
}

func TestAllocateReusesFreedBlockBeforeCarvingNew(t *testing.T) {
	t.Parallel()

	previous := []fabric.PreviousAllocation{
		{Zone: "a", CIDR: blockCIDR0},
		{Zone: "b", CIDR: blockCIDR1},
		{Zone: "c", CIDR: blockCIDR2},
	}

	// b is gone (no longer live), d is new — d must reuse b's freed block
	// (index 1, blockCIDR1), not carve a brand-new one past c.
	allocations, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, []string{"a", "c", "d"}, previous)
	require.NoError(t, err)

	byZone := map[string]fabric.Allocation{}
	for _, alloc := range allocations {
		byZone[alloc.Zone] = alloc
	}

	assert.Equal(t, blockCIDR0, byZone["a"].CIDR)
	assert.Equal(t, blockCIDR2, byZone["c"].CIDR)
	assert.Equal(t, blockCIDR1, byZone["d"].CIDR)
}

func TestAllocateRemovingThenReaddingSameZoneDoesNotReshuffleOthers(t *testing.T) {
	t.Parallel()

	previous := []fabric.PreviousAllocation{
		{Zone: "a", CIDR: blockCIDR0},
		{Zone: "b", CIDR: blockCIDR1},
		{Zone: "c", CIDR: blockCIDR2},
	}

	// b removed.
	afterRemoval, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, []string{"a", "c"}, previous)
	require.NoError(t, err)

	statusAfterRemoval := toPrevious(afterRemoval)

	// b re-added — must land back on its old block (index 1), not
	// wherever the next free index happens to be.
	afterReadd, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, []string{"a", "b", "c"}, statusAfterRemoval)
	require.NoError(t, err)

	byZone := map[string]fabric.Allocation{}
	for _, alloc := range afterReadd {
		byZone[alloc.Zone] = alloc
	}

	assert.Equal(t, blockCIDR0, byZone["a"].CIDR)
	assert.Equal(t, blockCIDR1, byZone["b"].CIDR)
	assert.Equal(t, blockCIDR2, byZone["c"].CIDR)
}

func TestAllocateGatewayIPIsBroadcastMinusOne(t *testing.T) {
	t.Parallel()

	allocations, err := fabric.Allocate("192.168.0.0/24", 30, []string{"z"}, nil)
	require.NoError(t, err)
	require.Len(t, allocations, 1)

	assert.Equal(t, "192.168.0.0/30", allocations[0].CIDR)
	// A /30 has 4 addresses: .0 (network), .1, .2 (gateway, broadcast-1), .3 (broadcast).
	assert.Equal(t, "192.168.0.2", allocations[0].GatewayIP)
}

func TestAllocateInvalidCIDRErrors(t *testing.T) {
	t.Parallel()

	_, err := fabric.Allocate("not-a-cidr", 24, []string{"a"}, nil)
	require.Error(t, err)
}

func TestAllocateZonePrefixNotLongerThanFabricErrors(t *testing.T) {
	t.Parallel()

	_, err := fabric.Allocate(testFabricCIDR, 16, []string{"a"}, nil)
	require.Error(t, err)

	_, err = fabric.Allocate(testFabricCIDR, 8, []string{"a"}, nil)
	require.Error(t, err)
}

func TestAllocateZonePrefixLeavesNoGatewayRoomErrors(t *testing.T) {
	t.Parallel()

	_, err := fabric.Allocate(blockCIDR0, 32, []string{"a"}, nil)
	require.Error(t, err)
}

// TestAllocateZonePrefixOneBitShortOfFabricLeavesNoGatewayRoomErrors is a
// regression test for an off-by-one: a /31 zone prefix out of a /24 fabric
// carves a 2-address block whose gateway IP (blockAddresses' own
// broadcastInt-1) collides with the block's own network address — not a
// distinct, usable gateway address at all. This used to be accepted
// (validateZonePrefixLength's own bound was totalBits-1, not totalBits-2).
func TestAllocateZonePrefixOneBitShortOfFabricLeavesNoGatewayRoomErrors(t *testing.T) {
	t.Parallel()

	_, err := fabric.Allocate(blockCIDR0, 31, []string{"a"}, nil)
	require.ErrorContains(t, err, "leaves no room for a gateway address")
}

func TestAllocateExhaustedBlocksErrors(t *testing.T) {
	t.Parallel()

	_, err := fabric.Allocate("10.0.0.0/30", 31, []string{"a", "b", "c"}, nil)
	require.Error(t, err)
}

func TestAllocateExcessiveBlockCountErrors(t *testing.T) {
	t.Parallel()

	_, err := fabric.Allocate("0.0.0.0/0", 30, []string{"a"}, nil)
	require.Error(t, err)
}

func TestAllocateNoLiveZonesReturnsEmpty(t *testing.T) {
	t.Parallel()

	allocations, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, allocations)
}

func TestAllocateStaleStatusForNoLongerLiveZoneIsIgnored(t *testing.T) {
	t.Parallel()

	previous := []fabric.PreviousAllocation{
		{Zone: "gone", CIDR: blockCIDR0},
	}

	allocations, err := fabric.Allocate(testFabricCIDR, testZonePrefixLength, []string{"fresh"}, previous)
	require.NoError(t, err)
	require.Len(t, allocations, 1)
	// fresh reuses gone's freed block, since it's the lowest free index.
	assert.Equal(t, blockCIDR0, allocations[0].CIDR)
}

func toPrevious(allocations []fabric.Allocation) []fabric.PreviousAllocation {
	previous := make([]fabric.PreviousAllocation, 0, len(allocations))
	for _, alloc := range allocations {
		previous = append(previous, fabric.PreviousAllocation{Zone: alloc.Zone, CIDR: alloc.CIDR})
	}

	return previous
}
