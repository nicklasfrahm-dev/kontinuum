package instance

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"

	"github.com/cosi-project/runtime/pkg/resource"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// maintenanceModePort is the gRPC port apid listens on while a Talos node
// is unconfigured and in maintenance mode — see talosctl's own default
// dial target for a maintenance-mode address.
const maintenanceModePort = 50000

// Discoverer probes a single candidate address for Talos maintenance-mode
// discovery info. talosDiscoverer is the production implementation, dialing
// a real node with github.com/siderolabs/talos/pkg/machinery/client; tests
// inject a fake to avoid a real gRPC dial — see controller_test.go.
type Discoverer interface {
	// Discover connects to addr (host or IP, no port) in Talos maintenance
	// mode and returns the node's Talos version and discovered network
	// interfaces.
	Discover(ctx context.Context, addr string) (string, []v1alpha2.InstanceInterfaceStatus, error)
}

// talosDiscoverer is Discoverer's production implementation.
type talosDiscoverer struct{}

// NewTalosDiscoverer returns the production Discoverer, which dials real
// Talos maintenance-mode nodes. Discoverer is this package's own seam for
// injecting a fake in tests (see controller_test.go/talos_wire_test.go) —
// the whole point of this constructor is to hide talosDiscoverer behind it.
//
//nolint:ireturn // see doc above
func NewTalosDiscoverer() Discoverer {
	return talosDiscoverer{}
}

// Discover implements Discoverer. A maintenance-mode node serves gRPC over
// a self-signed certificate with no CA yet issued — InsecureSkipVerify
// mirrors talosctl's own dial behavior against a node in this state, not a
// general relaxation of this codebase's TLS posture.
func (talosDiscoverer) Discover(
	ctx context.Context, addr string,
) (string, []v1alpha2.InstanceInterfaceStatus, error) {
	endpoint := net.JoinHostPort(addr, strconv.Itoa(maintenanceModePort))

	talosClient, err := talosclient.New(ctx,
		//nolint:gosec // maintenance mode has no issued CA yet — see this func's doc
		talosclient.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		talosclient.WithEndpoints(endpoint),
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to dial %s: %w", endpoint, err)
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived discovery connection

	// Version is best-effort: recent Talos releases gate the maintenance-mode
	// Version RPC behind an os:admin role check (see
	// internal/app/maintenance/server.go's assertAdminRole in siderolabs/talos),
	// which no maintenance-mode caller can ever satisfy — there's no CA yet to
	// issue that role's client cert from. Every other maintenance-mode caller,
	// including talosctl itself, hits the same "API is not implemented in
	// maintenance mode" error, so failing discovery over it would make
	// candidates on affected Talos versions permanently undiscoverable. The
	// version becomes known once the node is actually provisioned instead.
	talosVersion := ""

	versionResp, err := talosClient.Version(ctx)
	if err == nil {
		if messages := versionResp.GetMessages(); len(messages) > 0 {
			talosVersion = messages[0].GetVersion().GetTag()
		}
	}

	interfaces, err := discoverInterfaces(ctx, talosClient)
	if err != nil {
		return "", nil, fmt.Errorf("failed to discover interfaces from %s: %w", endpoint, err)
	}

	return talosVersion, interfaces, nil
}

// discoverInterfaces reads talosClient's COSI network.LinkStatus/AddressStatus
// resources — the same resources `talosctl get links`/`get addresses`
// read against a maintenance-mode node — since the machinery client has no
// flat "list interfaces" RPC of its own.
func discoverInterfaces(
	ctx context.Context, talosClient *talosclient.Client,
) ([]v1alpha2.InstanceInterfaceStatus, error) {
	linkMetadata := resource.NewMetadata(network.NamespaceName, network.LinkStatusType, "", resource.VersionUndefined)

	links, err := talosClient.COSI.List(ctx, linkMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to list link status resources: %w", err)
	}

	addrMetadata := resource.NewMetadata(network.NamespaceName, network.AddressStatusType, "", resource.VersionUndefined)

	addrs, err := talosClient.COSI.List(ctx, addrMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to list address status resources: %w", err)
	}

	addressesByLink := map[string][]string{}

	for _, item := range addrs.Items {
		addrStatus, ok := item.(*network.AddressStatus)
		if !ok {
			continue
		}

		spec := addrStatus.TypedSpec()
		addressesByLink[spec.LinkName] = append(addressesByLink[spec.LinkName], spec.Address.String())
	}

	interfaces := make([]v1alpha2.InstanceInterfaceStatus, 0, len(links.Items))

	for _, item := range links.Items {
		linkStatus, ok := item.(*network.LinkStatus)
		if !ok {
			continue
		}

		spec := linkStatus.TypedSpec()
		name := item.Metadata().ID()

		interfaces = append(interfaces, v1alpha2.InstanceInterfaceStatus{
			Name:       name,
			MACAddress: spec.HardwareAddr.String(),
			Addresses:  addressesByLink[name],
		})
	}

	return interfaces, nil
}
