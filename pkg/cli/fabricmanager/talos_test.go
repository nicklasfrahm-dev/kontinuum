package fabricmanager_test

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/pkg/machinery/config/types/network"

	"github.com/nicklasfrahm/kontinuum/pkg/cli/fabricmanager"
)

const (
	testFabricIface1  = "eth1"
	testFabricIface2  = "eth2"
	testGatewayPrefix = "10.0.0.254/24"
)

// testGatewayAddress is the AddressConfig BuildInterfaceConfigDocuments
// assigns for testGatewayPrefix — a func, not a const, since
// netip.MustParsePrefix isn't a constant expression.
func testGatewayAddress() network.AddressConfig {
	return network.AddressConfig{AddressAddress: netip.MustParsePrefix(testGatewayPrefix)}
}

func TestBuildInterfaceConfigDocumentsSingleInterfaceNoVLAN(t *testing.T) {
	t.Parallel()

	docs, err := fabricmanager.BuildInterfaceConfigDocuments([]string{testFabricIface1}, testGatewayPrefix, 0)
	require.NoError(t, err)
	require.Len(t, docs, 1)

	link, ok := docs[0].(*network.LinkConfigV1Alpha1)
	require.True(t, ok, "a single, untagged interface must be addressed directly, not bridged or VLAN-tagged")
	assert.Equal(t, testFabricIface1, link.Name())
	assert.Equal(t, []network.AddressConfig{testGatewayAddress()}, link.LinkAddresses)
}

func TestBuildInterfaceConfigDocumentsSingleInterfaceWithVLANTagsSubInterfaceNotParent(t *testing.T) {
	t.Parallel()

	docs, err := fabricmanager.BuildInterfaceConfigDocuments([]string{testFabricIface1}, testGatewayPrefix, 100)
	require.NoError(t, err)
	require.Len(t, docs, 1, "the address goes on the VLAN sub-interface itself, no separate LinkConfig needed")

	vlan, ok := docs[0].(*network.VLANConfigV1Alpha1)
	require.True(t, ok, "a single VLAN-tagged interface must stay a pure trunk, addressed via its own VLAN sub-interface")
	assert.Equal(t, "eth1.100", vlan.Name())
	assert.Equal(t, testFabricIface1, vlan.ParentLink())
	assert.EqualValues(t, 100, vlan.VLANID())
	assert.Equal(t, []network.AddressConfig{testGatewayAddress()}, vlan.LinkAddresses)
}

func TestBuildInterfaceConfigDocumentsBridgesMultipleInterfacesNoVLAN(t *testing.T) {
	t.Parallel()

	docs, err := fabricmanager.BuildInterfaceConfigDocuments(
		[]string{testFabricIface1, testFabricIface2}, testGatewayPrefix, 0)
	require.NoError(t, err)
	require.Len(t, docs, 1, "no VLAN sub-interfaces needed, just the one bridge")

	bridge, ok := docs[0].(*network.BridgeConfigV1Alpha1)
	require.True(t, ok, "two free interfaces must be bridged, not each independently assigned the same address")
	assert.ElementsMatch(t, []string{testFabricIface1, testFabricIface2}, bridge.Links())
	assert.Equal(t, []network.AddressConfig{testGatewayAddress()}, bridge.LinkAddresses)
	assert.True(t, *bridge.BridgeSTP.BridgeSTPEnabled, "stp must be on as a loop-prevention safety net")
}

func TestBuildInterfaceConfigDocumentsBridgesVLANSubInterfacesNotRawParents(t *testing.T) {
	t.Parallel()

	docs, err := fabricmanager.BuildInterfaceConfigDocuments(
		[]string{testFabricIface1, testFabricIface2}, testGatewayPrefix, 100)
	require.NoError(t, err)
	require.Len(t, docs, 3, "one VLANConfig per parent, plus the one bridge enslaving both sub-interfaces")

	vlan0, isVLAN0 := docs[0].(*network.VLANConfigV1Alpha1)
	require.True(t, isVLAN0)
	assert.Equal(t, "eth1.100", vlan0.Name())
	assert.Equal(t, testFabricIface1, vlan0.ParentLink())
	assert.Empty(t, vlan0.LinkAddresses, "the address belongs on the bridge, not the individual VLAN sub-interfaces")

	vlan1, isVLAN1 := docs[1].(*network.VLANConfigV1Alpha1)
	require.True(t, isVLAN1)
	assert.Equal(t, "eth2.100", vlan1.Name())
	assert.Equal(t, testFabricIface2, vlan1.ParentLink())

	bridge, isBridge := docs[2].(*network.BridgeConfigV1Alpha1)
	require.True(t, isBridge)
	assert.Equal(t, []string{"eth1.100", "eth2.100"}, bridge.Links(),
		"the bridge must enslave the VLAN sub-interfaces, never the raw trunk parents directly")
	assert.Equal(t, []network.AddressConfig{testGatewayAddress()}, bridge.LinkAddresses)
}

func TestBuildInterfaceConfigDocumentsInvalidGatewayPrefixErrors(t *testing.T) {
	t.Parallel()

	_, err := fabricmanager.BuildInterfaceConfigDocuments([]string{testFabricIface1}, "not-a-prefix", 0)
	assert.Error(t, err)
}
