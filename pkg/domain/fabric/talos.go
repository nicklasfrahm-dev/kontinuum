package fabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/types/network"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	"github.com/siderolabs/talos/pkg/machinery/role"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// rpcTimeout bounds every single-shot RPC this package makes on the client
// side — same rationale, and same value, as taloscluster's own identical
// constant.
const rpcTimeout = 30 * time.Second

// secretsBundleKey is the key a TalosCluster's own secrets bundle is stored
// under in the Secret its status.secretRef points to — must match
// pkg/domain/taloscluster/secrets.go's own secretsBundleKey. Duplicated
// rather than imported, mirroring pkg/domain/zone's identical
// kubeconfigSecretKey duplication (see its own doc for the import-cycle-
// avoidance rationale — taloscluster already imports this package's future
// siblings, so importing taloscluster from here risks the same cycle).
const secretsBundleKey = "secrets-bundle"

// errNoMachineConfig is ApplyInterfaceConfig's sentinel for a COSI Get that
// returned something other than a *configresource.MachineConfig — should
// never happen against a real Talos node, but keeps that code a checked,
// non-panicking path regardless.
var errNoMachineConfig = errors.New("talos did not return a machine config resource")

// NetworkConfigurer pushes a static route/gateway config patch onto a
// zone's own elected NAT gateway node, dialing it with a TalosCluster's
// real (non-maintenance-mode) admin identity — same seam pattern as
// taloscluster.ClusterBootstrapper. talosNetworkConfigurer is the
// production implementation; tests inject a fake to avoid a real gRPC
// dial.
type NetworkConfigurer interface {
	// ApplyInterfaceConfig applies patch — one or more LinkConfig
	// documents (see BuildInterfaceAddressPatch) — onto addr's own
	// currently running machine config, strategic-merged so every other
	// already-applied document (every other interface, install config,
	// the cluster's own etcd/token secrets, ...) is left untouched, then
	// re-applies the merged whole with Mode:
	// machineapi.ApplyConfigurationRequest_NO_REBOOT — a network-only
	// patch that never reboots the node.
	ApplyInterfaceConfig(ctx context.Context, addr string, talosCfg *clientconfig.Config, patch []byte) error
}

// talosNetworkConfigurer is NetworkConfigurer's production implementation.
type talosNetworkConfigurer struct{}

// NewNetworkConfigurer returns the production NetworkConfigurer, which
// dials real Talos nodes. NetworkConfigurer is this package's own seam for
// injecting a fake in tests.
//
//nolint:ireturn // see doc above
func NewNetworkConfigurer() NetworkConfigurer {
	return talosNetworkConfigurer{}
}

// ApplyInterfaceConfig implements NetworkConfigurer. It reads addr's own
// currently running machine config via its COSI state (the same resource,
// configresource.MachineConfigType at configresource.ActiveID, talosctl's
// own `edit machineconfig`/`patch machineconfig` commands read before
// merging a patch client-side and reapplying — which this mirrors),
// strategic-merges patch onto it, and re-applies the merged whole.
func (talosNetworkConfigurer) ApplyInterfaceConfig(
	ctx context.Context, addr string, talosCfg *clientconfig.Config, patch []byte,
) error {
	talosClient, err := dial(ctx, addr, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived apply connection

	res, err := talosClient.COSI.Get(ctx, resource.NewMetadata(
		configresource.NamespaceName, configresource.MachineConfigType, configresource.ActiveID, resource.VersionUndefined))
	if err != nil {
		return fmt.Errorf("failed to fetch running machine config from %s: %w", addr, err)
	}

	machineConfig, ok := res.(*configresource.MachineConfig)
	if !ok {
		return fmt.Errorf("%w: got %T from %s", errNoMachineConfig, res, addr)
	}

	patchProvider, err := configloader.NewFromBytes(patch)
	if err != nil {
		return fmt.Errorf("failed to parse interface config patch: %w", err)
	}

	merged, err := configpatcher.StrategicMerge(
		machineConfig.Provider(), configpatcher.NewStrategicMergePatch(patchProvider))
	if err != nil {
		return fmt.Errorf("failed to merge interface config patch for %s: %w", addr, err)
	}

	mergedBytes, err := merged.Bytes()
	if err != nil {
		return fmt.Errorf("failed to encode merged machine config for %s: %w", addr, err)
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	_, err = talosClient.ApplyConfiguration(rpcCtx, &machineapi.ApplyConfigurationRequest{
		Data: mergedBytes,
		Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT,
	})
	if err != nil {
		return fmt.Errorf("failed to apply interface config to %s: %w", addr, err)
	}

	return nil
}

// dial connects to addr with talosCfg's real (non-maintenance-mode) admin
// identity — mirrors taloscluster.talosBootstrapper.dial's identical
// rationale (talosCfg already carries a CA and admin client cert both
// signed by the cluster's own secrets bundle, so no InsecureSkipVerify is
// needed here).
func dial(ctx context.Context, addr string, talosCfg *clientconfig.Config) (*talosclient.Client, error) {
	talosClient, err := talosclient.New(ctx,
		talosclient.WithConfig(talosCfg),
		talosclient.WithEndpoints(addr),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	return talosClient, nil
}

// fabricBridgeName names the bridge device BuildGatewayAddressPatch
// creates when a gateway node has more than one interface to advertise
// the fabric on — see that function's own doc for why bridging is what
// actually makes assigning one shared gateway address to more than one
// physical interface valid in the first place.
const fabricBridgeName = "fabric0"

// BuildGatewayAddressPatch returns the LinkConfig (or, for more than one
// interface, BridgeConfig) document NetworkConfigurer.ApplyInterfaceConfig
// strategic-merges onto a gateway node's own running config: gatewayPrefix
// (e.g. "10.0.0.254/24" — the zone's own carved-subnet gateway IP, see
// ipam.go's Allocation.GatewayIP, combined with that same subnet's own
// prefix length) assigned as a real address, making the node an actual
// gateway on that subnet — holding the address itself, not just a route to
// it.
//
// fabricIfaces is every discovered interface with no address of its own
// yet (see classifyGatewayInterfaces's own doc for why the node's own
// already-addressed uplink is deliberately excluded). Assigning the exact
// same address independently to more than one raw interface is invalid —
// Linux has no notion of "this one address lives on either of these two
// links," and each interface would fight over it. So a single interface
// gets the address directly (a plain LinkConfig document); more than one
// are first bridged together (fabricBridgeName, enslaving every one of
// them via BridgeConfig's own BridgeLinks) and the address goes on the
// bridge device instead — the standard way Linux (and Talos) represents
// "one logical L3 presence reachable over several physical links." STP is
// enabled on that bridge purely as a loop-prevention safety net, since
// this controller has no way to verify those links' own physical topology
// doesn't already put them on the same L2 segment somewhere upstream.
//
// LinkConfig/BridgeConfig (config/types/network) are the modern,
// per-document replacement for the legacy, deprecated
// machine.network.interfaces[] field — a strategic-merge patch carrying
// one of these adds/replaces cleanly alongside whatever else the node's
// own machine config already applies, without touching any of it.
func BuildGatewayAddressPatch(fabricIfaces []string, gatewayPrefix string) ([]byte, error) {
	prefix, err := netip.ParsePrefix(gatewayPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gateway prefix %q: %w", gatewayPrefix, err)
	}

	if len(fabricIfaces) == 1 {
		doc := network.NewLinkConfigV1Alpha1(fabricIfaces[0])
		doc.LinkAddresses = []network.AddressConfig{{AddressAddress: prefix}}

		data, err := yaml.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal gateway address patch for %q: %w", fabricIfaces[0], err)
		}

		return data, nil
	}

	enableSTP := true

	doc := network.NewBridgeConfigV1Alpha1(fabricBridgeName)
	doc.BridgeLinks = fabricIfaces
	doc.BridgeSTP = network.BridgeSTPConfig{BridgeSTPEnabled: &enableSTP}
	doc.LinkAddresses = []network.AddressConfig{{AddressAddress: prefix}}

	data, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal gateway bridge patch for %v: %w", fabricIfaces, err)
	}

	return data, nil
}

// LoadSecretsBundle fetches and unmarshals cluster's own stored Talos
// secrets bundle — mirrors pkg/domain/taloscluster's identical unexported
// loadSecretsBundle (see secretsBundleKey's own doc for why this is
// duplicated rather than imported).
func LoadSecretsBundle(
	ctx context.Context, hubClient client.Client, ref v1alpha2.SecretReference,
) (*talossecrets.Bundle, error) {
	var secret corev1.Secret

	err := hubClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%q secret not found: %w", ref.Name, err)
		}

		return nil, fmt.Errorf("failed to fetch %q secrets bundle secret: %w", ref.Name, err)
	}

	bundle := &talossecrets.Bundle{Clock: talossecrets.NewClock()}

	err = json.Unmarshal(secret.Data[secretsBundleKey], bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %q secrets bundle: %w", ref.Name, err)
	}

	return bundle, nil
}

// BuildTalosConfig derives a real (non-maintenance-mode) admin
// clientconfig.Config from bundle — the same OS CA and a freshly generated
// os:admin client certificate, both signed by the cluster's own secrets
// bundle. clusterName becomes the config's own context name; endpoints is
// carried for completeness but every dial in this package overrides it
// explicitly via talosclient.WithEndpoints (see dial), matching
// taloscluster's own identical convention.
func BuildTalosConfig(
	bundle *talossecrets.Bundle, clusterName string, endpoints []string,
) (*clientconfig.Config, error) {
	clientCert, err := bundle.GenerateTalosAPIClientCertificate(role.MakeSet(role.Admin))
	if err != nil {
		return nil, fmt.Errorf("failed to generate talos admin client certificate: %w", err)
	}

	return clientconfig.NewConfig(clusterName, endpoints, bundle.Certs.OS.Crt, clientCert), nil
}
