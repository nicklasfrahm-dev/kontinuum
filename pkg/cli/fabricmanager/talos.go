package fabricmanager

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/container"
	"github.com/siderolabs/talos/pkg/machinery/config/types/network"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/fabric"
)

// talosSelfAddr is the address applyInterfaceConfig dials to reach this
// same node's own Talos API — this process runs hostNetwork directly on
// the gateway node it's configuring (see
// pkg/domain/zone.buildFabricManagerDaemonSet's own doc), so its own
// loopback interface reaches Talos's own apid the same way any other
// localhost-bound client would, with no need to know this node's own
// externally-reachable address the way the hub's own (now-retired) dial
// code once did.
const talosSelfAddr = "127.0.0.1"

// rpcTimeout bounds every single-shot Talos RPC this package makes —
// mirrors pkg/domain/taloscluster's own identical constant, and what
// pkg/domain/fabric's own now-retired talos.go used before this push
// moved here.
const rpcTimeout = 30 * time.Second

// errNoMachineConfig is applyInterfaceConfig's sentinel for a COSI Get
// that returned something other than a *configresource.MachineConfig —
// should never happen against a real Talos node, but keeps that code a
// checked, non-panicking path regardless.
var errNoMachineConfig = errors.New("talos did not return a machine config resource")

// readTalosConfig fetches and parses fabric.TalosConfigSecretName's own
// current payload — the hub re-issues and upserts this Secret on every
// one of its own reconcile passes (see fabric.ensureGatewayTalosConfig's
// own doc), so a fresh Get here always sees the latest credential, no
// separate rotation-watch needed.
func readTalosConfig(ctx context.Context, hubClient client.Client) (*clientconfig.Config, error) {
	var secret corev1.Secret

	key := client.ObjectKey{Name: fabric.TalosConfigSecretName, Namespace: v1alpha2.KontinuumSystemNamespace}

	err := hubClient.Get(ctx, key, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secret: %w", fabric.TalosConfigSecretName, err)
	}

	talosCfg, err := clientconfig.FromBytes(secret.Data[fabric.TalosConfigSecretKey])
	if err != nil {
		return nil, fmt.Errorf("failed to parse %q secret: %w", fabric.TalosConfigSecretName, err)
	}

	return talosCfg, nil
}

// dialTalos connects to addr (this node's own address — see dialSelf)
// with talosCfg's real (non-maintenance-mode) admin identity — mirrors
// pkg/domain/taloscluster.talosBootstrapper.dial's identical rationale
// (talosCfg already carries a CA and admin client cert both signed by
// the cluster's own secrets bundle, so no InsecureSkipVerify is needed
// here).
func dialTalos(ctx context.Context, addr string, talosCfg *clientconfig.Config) (*talosclient.Client, error) {
	talosClient, err := talosclient.New(ctx,
		talosclient.WithConfig(talosCfg),
		talosclient.WithEndpoints(addr),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	return talosClient, nil
}

// fabricBridgeName names the bridge device BuildInterfaceConfigDocuments
// creates when a gateway node has more than one interface to advertise
// the fabric on — see that function's own doc for why bridging is what
// actually makes assigning one shared gateway address to more than one
// physical interface valid in the first place.
const fabricBridgeName = "fabric0"

// vlanInterfaceName names the VLAN sub-interface BuildInterfaceConfigDocuments
// creates over parent when vlanID is set — mirrors the naming convention
// Talos's own VLANConfig example itself documents (e.g. "enp0s3.34").
func vlanInterfaceName(parent string, vlanID int32) string {
	return fmt.Sprintf("%s.%d", parent, vlanID)
}

// BuildInterfaceConfigDocuments returns the LinkConfig/BridgeConfig/
// VLANConfig documents applyInterfaceConfig strategic-merges onto a
// gateway node's own running config: gatewayPrefix (e.g. "10.0.0.254/24"
// — the zone's own carved-subnet gateway IP combined with that subnet's
// own prefix length) assigned as a real address, making this node an
// actual gateway on that subnet — holding the address itself, not just a
// route to it.
//
// fabricIfaces is every discovered interface with no address of its own
// yet (see classifyLocalInterfaces's own doc for why this node's own
// already-addressed uplink is deliberately excluded). Assigning the exact
// same address independently to more than one raw interface is invalid —
// Linux has no notion of "this one address lives on either of these two
// links," and each interface would fight over it. So a single interface
// gets the address directly; more than one are first bridged together
// (fabricBridgeName, enslaving every one of them via BridgeConfig's own
// BridgeLinks) and the address goes on the bridge device instead — the
// standard way Linux (and Talos) represents "one logical L3 presence
// reachable over several physical links." STP is enabled on that bridge
// purely as a loop-prevention safety net, since there's no way to verify
// those links' own physical topology doesn't already put them on the
// same L2 segment somewhere upstream.
//
// vlanID, when non-zero (see FabricSpec.VLANID's own doc), tags the
// gateway address onto an 802.1q VLAN sub-interface (vlanInterfaceName)
// created over each fabricIface, rather than assigning it to that raw
// interface directly — one VLANConfig document per fabricIface, always,
// even for a single interface, so the raw physical link itself is never
// addressed and stays a pure 802.1q trunk. Bridging (when there's more
// than one) then enslaves those VLAN sub-interfaces, not the raw parents.
func BuildInterfaceConfigDocuments(
	fabricIfaces []string, gatewayPrefix string, vlanID int32,
) ([]config.Document, error) {
	prefix, err := netip.ParsePrefix(gatewayPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gateway prefix %q: %w", gatewayPrefix, err)
	}

	addressedIfaces := fabricIfaces
	docs := []config.Document{}

	if vlanID != 0 {
		addressedIfaces = make([]string, len(fabricIfaces))

		for index, parent := range fabricIfaces {
			vlanDoc := network.NewVLANConfigV1Alpha1(vlanInterfaceName(parent, vlanID))
			vlanDoc.VLANIDConfig = uint16(vlanID) //nolint:gosec // validated 1-4094 by the Fabric CRD's own schema
			vlanDoc.ParentLinkConfig = parent

			docs = append(docs, vlanDoc)
			addressedIfaces[index] = vlanDoc.Name()
		}
	}

	if len(addressedIfaces) == 1 {
		if vlanID != 0 {
			//nolint:forcetypeassert // docs[0] is always the one VLANConfig this function itself just appended above
			vlanDoc := docs[0].(*network.VLANConfigV1Alpha1)
			vlanDoc.LinkAddresses = []network.AddressConfig{{AddressAddress: prefix}}

			return docs, nil
		}

		doc := network.NewLinkConfigV1Alpha1(addressedIfaces[0])
		doc.LinkAddresses = []network.AddressConfig{{AddressAddress: prefix}}

		return append(docs, doc), nil
	}

	enableSTP := true

	bridgeDoc := network.NewBridgeConfigV1Alpha1(fabricBridgeName)
	bridgeDoc.BridgeLinks = addressedIfaces
	bridgeDoc.BridgeSTP = network.BridgeSTPConfig{BridgeSTPEnabled: &enableSTP}
	bridgeDoc.LinkAddresses = []network.AddressConfig{{AddressAddress: prefix}}

	return append(docs, bridgeDoc), nil
}

// applyInterfaceConfig reads this node's own currently running machine
// config (dialed at talosSelfAddr) via its COSI state (the same resource,
// configresource.MachineConfigType at configresource.ActiveID, talosctl's
// own `edit machineconfig`/`patch machineconfig` commands read before
// merging a patch client-side and reapplying — which this mirrors),
// strategic-merges BuildInterfaceConfigDocuments' own documents onto it,
// and re-applies the merged whole with Mode:
// machineapi.ApplyConfigurationRequest_NO_REBOOT — a network-only patch
// that never reboots the node.
func applyInterfaceConfig(
	ctx context.Context, talosCfg *clientconfig.Config, fabricIfaces []string, gatewayPrefix string, vlanID int32,
) error {
	patch, err := buildInterfaceConfigPatch(fabricIfaces, gatewayPrefix, vlanID)
	if err != nil {
		return err
	}

	talosClient, err := dialTalos(ctx, talosSelfAddr, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived apply connection

	mergedBytes, err := mergeInterfaceConfigPatch(ctx, talosClient, patch)
	if err != nil {
		return err
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	_, err = talosClient.ApplyConfiguration(rpcCtx, &machineapi.ApplyConfigurationRequest{
		Data: mergedBytes,
		Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT,
	})
	if err != nil {
		return fmt.Errorf("failed to apply interface config: %w", err)
	}

	return nil
}

// buildInterfaceConfigPatch bundles BuildInterfaceConfigDocuments' own
// documents into one strategic-merge patch's own raw bytes.
func buildInterfaceConfigPatch(fabricIfaces []string, gatewayPrefix string, vlanID int32) ([]byte, error) {
	docs, err := BuildInterfaceConfigDocuments(fabricIfaces, gatewayPrefix, vlanID)
	if err != nil {
		return nil, err
	}

	bundle, err := container.New(docs...)
	if err != nil {
		return nil, fmt.Errorf("failed to bundle interface config documents: %w", err)
	}

	patch, err := bundle.Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to encode interface config patch: %w", err)
	}

	return patch, nil
}

// mergeInterfaceConfigPatch fetches talosClient's own currently running
// machine config (see applyInterfaceConfig's own doc for the COSI resource
// this reads) and strategic-merges patch onto it, returning the merged
// whole's own encoded bytes.
func mergeInterfaceConfigPatch(ctx context.Context, talosClient *talosclient.Client, patch []byte) ([]byte, error) {
	res, err := talosClient.COSI.Get(ctx, resource.NewMetadata(
		configresource.NamespaceName, configresource.MachineConfigType, configresource.ActiveID, resource.VersionUndefined))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch running machine config: %w", err)
	}

	machineConfig, ok := res.(*configresource.MachineConfig)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errNoMachineConfig, res)
	}

	patchProvider, err := configloader.NewFromBytes(patch)
	if err != nil {
		return nil, fmt.Errorf("failed to parse interface config patch: %w", err)
	}

	merged, err := configpatcher.StrategicMerge(
		machineConfig.Provider(), configpatcher.NewStrategicMergePatch(patchProvider))
	if err != nil {
		return nil, fmt.Errorf("failed to merge interface config patch: %w", err)
	}

	mergedBytes, err := merged.Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to encode merged machine config: %w", err)
	}

	return mergedBytes, nil
}
