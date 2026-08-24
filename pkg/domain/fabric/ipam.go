package fabric

import (
	"errors"
	"fmt"
	"math/big"
	"net"
	"sort"
	"strconv"
)

// maxZonePrefixDiff bounds ZonePrefixLength - the fabric CIDR's own prefix
// length — i.e. how many per-zone blocks a fabric CIDR may ever be carved
// into (1<<24, ~16.7M). Without this, a spec like cidr "0.0.0.0/0" plus
// zonePrefixLength 32 would ask Allocate to reason about four billion
// blocks, hanging the reconcile that runs it. No real fabric needs anywhere
// near this many zones — this is a sanity ceiling, not a realistic limit.
const maxZonePrefixDiff = 24

var (
	// errInvalidFabricCIDR is Allocate's sentinel for a spec.cidr that
	// doesn't parse — see the err113 wrapping convention in AGENTS.md.
	errInvalidFabricCIDR = errors.New("invalid fabric CIDR")
	// errZonePrefixTooShort is Allocate's sentinel for a
	// spec.zonePrefixLength that isn't strictly longer than the fabric
	// CIDR's own prefix length — carving a zone subnet no smaller than the
	// fabric itself makes no sense.
	errZonePrefixTooShort = errors.New("zone prefix length must be longer than the fabric CIDR's own prefix length")
	// errZonePrefixNoGatewayRoom is Allocate's sentinel for a
	// spec.zonePrefixLength that leaves a carved zone subnet with fewer
	// than two usable addresses — no room for a gateway IP distinct from
	// the subnet's own network address.
	errZonePrefixNoGatewayRoom = errors.New("zone prefix length leaves no room for a gateway address")
	// errZonePrefixDiffTooLarge is Allocate's sentinel for a
	// spec.zonePrefixLength/cidr combination that would carve the fabric
	// CIDR into more than maxZonePrefixDiff blocks.
	errZonePrefixDiffTooLarge = errors.New("zone prefix length carves the fabric cidr into too many blocks")
	// errBlocksExhausted is Allocate's sentinel for more live zones than
	// the fabric CIDR has room to carve blocks for.
	errBlocksExhausted = errors.New("fabric cidr has no more room for another zone subnet")
	// errPreviousCIDRNotInFabric is blockIndex's sentinel for a previously
	// recorded zone CIDR that no longer falls inside the fabric's own
	// CIDR — e.g. spec.cidr was edited out from under an already-allocated
	// zone. Allocate treats this the same as "no previous allocation for
	// this zone", carving a fresh block instead of failing outright: the
	// controller's own validation already rejects a cidr edit that would
	// orphan every zone's own status.Zones this way, but tolerating it here
	// keeps Allocate itself a pure, always-terminating function.
	errPreviousCIDRNotInFabric = errors.New("previous zone cidr does not fall inside the fabric cidr")
)

// PreviousAllocation is one zone's own previously recorded carved subnet —
// reduced from the controller's own status.Zones entries to just what
// Allocate needs to recompute each zone's own block index and honor
// stickiness (see Allocate's own doc).
type PreviousAllocation struct {
	// Zone is this entry's own zone name.
	Zone string
	// CIDR is this zone's own previously carved subnet.
	CIDR string
}

// Allocation is one zone's own carved subnet and gateway IP, computed by
// Allocate.
type Allocation struct {
	// Zone is this entry's own zone name.
	Zone string
	// CIDR is this zone's own carved subnet, e.g. "10.0.1.0/24".
	CIDR string
	// GatewayIP is this zone's own gateway address — the last usable host
	// address (broadcast - 1) of CIDR.
	GatewayIP string
}

// GatewayPrefix combines gatewayIP with cidr's own prefix length into a
// single "address/prefixLength" string (e.g. "10.0.0.254/24") — the shape
// BuildGatewayAddressPatch needs to assign the gateway address as a real
// interface (or bridge) address, not just a bare IP with no mask of its
// own.
func GatewayPrefix(cidr, gatewayIP string) (string, error) {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", fmt.Errorf("%w: %q", errInvalidFabricCIDR, cidr)
	}

	ones, _ := ipNet.Mask.Size()

	return gatewayIP + "/" + strconv.Itoa(ones), nil
}

// Allocate carves one subnet per entry in liveZones out of fabricCIDR,
// each zonePrefixLength long, deterministically and reproducibly:
//
//   - A zone already present in previous (and still live) keeps its exact
//     block, in place — Allocate never reshuffles an existing allocation
//     just because another zone came or went.
//   - A live zone with no previous entry is assigned the fabric's own
//     lowest not-currently-live block index. Since every past allocation
//     this function has ever produced only ever used lowest-available
//     indices the same way, any index freed by a zone that's no longer
//     live is always lower than (or equal to) any index that has never
//     been assigned at all — so this single rule already implements "reuse
//     a freed block before carving a new one," with no separate freed-list
//     bookkeeping needed.
//
// liveZones is deduplicated and processed in sorted order, so Allocate's
// own output order (and which block a batch of simultaneously-new zones
// each land on) is stable across calls with the same input, not dependent
// on liveZones' own incoming order (e.g. a List's own return order, which
// Kubernetes does not guarantee).
func Allocate(
	fabricCIDR string, zonePrefixLength int32, liveZones []string, previous []PreviousAllocation,
) ([]Allocation, error) {
	_, fabricNet, err := net.ParseCIDR(fabricCIDR)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w", errInvalidFabricCIDR, fabricCIDR, err)
	}

	fabricPrefixLen, totalBits := fabricNet.Mask.Size()

	err = validateZonePrefixLength(fabricPrefixLen, totalBits, zonePrefixLength)
	if err != nil {
		return nil, err
	}

	diff := uint(zonePrefixLength) - uint(fabricPrefixLen) //nolint:gosec // validated non-negative above
	blockCount := new(big.Int).Lsh(big.NewInt(1), diff)
	blockSize := new(big.Int).Lsh(big.NewInt(1), uint(totalBits)-uint(zonePrefixLength)) //nolint:gosec // validated above

	liveSet := dedupeSorted(liveZones)
	if int64(len(liveSet)) > blockCount.Int64() {
		return nil, fmt.Errorf("%w: %d zones, %s can carve at most %s",
			errBlocksExhausted, len(liveSet), fabricCIDR, blockCount.String())
	}

	reserved := map[string]int64{}
	sticky := map[string]int64{}

	for _, prev := range previous {
		if !liveSet[prev.Zone] {
			continue
		}

		index, indexErr := blockIndex(fabricNet, zonePrefixLength, prev.CIDR)
		if indexErr != nil {
			continue
		}

		sticky[prev.Zone] = index
		reserved[prev.Zone] = index
	}

	usedIndices := make(map[int64]bool, len(reserved))
	for _, index := range reserved {
		usedIndices[index] = true
	}

	allocations := make([]Allocation, 0, len(liveSet))

	for _, zone := range sortedKeys(liveSet) {
		index, ok := sticky[zone]
		if !ok {
			index = lowestFreeIndex(usedIndices, blockCount.Int64())
			usedIndices[index] = true
		}

		subnetCIDR, gatewayIP := blockAddresses(fabricNet, zonePrefixLength, blockSize, index)

		allocations = append(allocations, Allocation{Zone: zone, CIDR: subnetCIDR, GatewayIP: gatewayIP})
	}

	return allocations, nil
}

// validateZonePrefixLength checks that zonePrefixLength both carves a
// strictly smaller block than the fabric CIDR itself and leaves at least
// two usable addresses (network + gateway, distinct) per block, without
// carving more than maxZonePrefixDiff blocks.
func validateZonePrefixLength(fabricPrefixLen, totalBits int, zonePrefixLength int32) error {
	if int(zonePrefixLength) <= fabricPrefixLen {
		return fmt.Errorf("%w: fabric is /%d, zone prefix is /%d", errZonePrefixTooShort, fabricPrefixLen, zonePrefixLength)
	}

	if int(zonePrefixLength) > totalBits-1 {
		return fmt.Errorf("%w: /%d", errZonePrefixNoGatewayRoom, zonePrefixLength)
	}

	if int(zonePrefixLength)-fabricPrefixLen > maxZonePrefixDiff {
		return fmt.Errorf("%w: fabric /%d, zone /%d exceeds /%d blocks",
			errZonePrefixDiffTooLarge, fabricPrefixLen, zonePrefixLength, maxZonePrefixDiff)
	}

	return nil
}

// dedupeSorted returns zones as a set, silently collapsing duplicates —
// Allocate's own caller (listing Zone objects by spec.region) can't produce
// one, but Allocate stays a total function regardless of what it's handed.
func dedupeSorted(zones []string) map[string]bool {
	set := make(map[string]bool, len(zones))
	for _, zone := range zones {
		set[zone] = true
	}

	return set
}

// sortedKeys returns set's own keys in ascending order — Go's own map
// iteration order is randomized, and Allocate's own doc requires
// deterministic output.
func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// lowestFreeIndex returns the smallest index in [0, blockCount) not already
// present in used.
func lowestFreeIndex(used map[int64]bool, blockCount int64) int64 {
	for index := range blockCount {
		if !used[index] {
			return index
		}
	}

	// Unreachable: Allocate's own caller already checked len(liveSet) <=
	// blockCount before calling this for every live zone, so there is
	// always at least one free index left by the time this runs.
	return blockCount
}

// blockIndex recomputes which block index previousCIDR (a zone's own
// previously recorded subnet) corresponds to within fabricNet, by
// subtracting fabricNet's own network address from previousCIDR's and
// dividing by the block size implied by zonePrefixLength. Returns
// errPreviousCIDRNotInFabric if previousCIDR doesn't parse, isn't exactly
// zonePrefixLength long, or falls outside fabricNet's own range — see this
// package's own doc for why that's tolerated rather than surfaced as a hard
// failure.
func blockIndex(fabricNet *net.IPNet, zonePrefixLength int32, previousCIDR string) (int64, error) {
	parsedIP, ipNet, err := net.ParseCIDR(previousCIDR)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", errPreviousCIDRNotInFabric, previousCIDR)
	}

	ones, bits := ipNet.Mask.Size()
	if ones != int(zonePrefixLength) {
		return 0, fmt.Errorf("%w: %q", errPreviousCIDRNotInFabric, previousCIDR)
	}

	// ones/bits both come from net.IPMask.Size(), always small (<=128) and
	// non-negative — never a real overflow risk despite the signed-to-
	// unsigned conversion below.
	//nolint:gosec
	blockSize := new(big.Int).Lsh(big.NewInt(1), uint(bits)-uint(zonePrefixLength))

	fabricStart := ipToInt(fabricNet.IP)
	zoneStart := ipToInt(parsedIP.Mask(ipNet.Mask))

	offset := new(big.Int).Sub(zoneStart, fabricStart)
	if offset.Sign() < 0 {
		return 0, fmt.Errorf("%w: %q", errPreviousCIDRNotInFabric, previousCIDR)
	}

	fabricOnes, _ := fabricNet.Mask.Size()

	fabricSize := new(big.Int).Lsh(big.NewInt(1), uint(bits)-uint(fabricOnes))
	if offset.Cmp(fabricSize) >= 0 {
		return 0, fmt.Errorf("%w: %q", errPreviousCIDRNotInFabric, previousCIDR)
	}

	index := new(big.Int).Div(offset, blockSize)
	if !index.IsInt64() {
		return 0, fmt.Errorf("%w: %q", errPreviousCIDRNotInFabric, previousCIDR)
	}

	return index.Int64(), nil
}

// blockAddresses computes index's own subnet CIDR and gateway IP (the last
// usable address, broadcast - 1) within fabricNet, each block blockSize
// addresses long.
func blockAddresses(fabricNet *net.IPNet, zonePrefixLength int32, blockSize *big.Int, index int64) (string, string) {
	fabricStart := ipToInt(fabricNet.IP)
	offset := new(big.Int).Mul(blockSize, big.NewInt(index))
	networkInt := new(big.Int).Add(fabricStart, offset)

	broadcastInt := new(big.Int).Add(networkInt, blockSize)
	broadcastInt.Sub(broadcastInt, big.NewInt(1))

	gatewayInt := new(big.Int).Sub(broadcastInt, big.NewInt(1))

	isIPv4 := fabricNet.IP.To4() != nil

	networkIP := intToIP(networkInt, isIPv4)
	gatewayIP := intToIP(gatewayInt, isIPv4)

	cidr := networkIP.String() + "/" + strconv.Itoa(int(zonePrefixLength))

	return cidr, gatewayIP.String()
}

// ipToInt encodes ip as an unsigned big.Int, always over its 16-byte form —
// so IPv4 and IPv6 addresses compare and arithmetic the same way regardless
// of which 4-byte/16-byte representation net.ParseCIDR happened to return.
func ipToInt(ip net.IP) *big.Int {
	return new(big.Int).SetBytes(ip.To16())
}

// intToIP decodes i back into a net.IP, returning its 4-byte form when
// ipv4 is true (mirrors ipToInt's own always-16-byte encoding).
func intToIP(i *big.Int, ipv4 bool) net.IP {
	buf := make([]byte, net.IPv6len)

	b := i.Bytes()
	copy(buf[net.IPv6len-len(b):], b)

	ip := net.IP(buf)
	if ipv4 {
		return ip.To4()
	}

	return ip
}
