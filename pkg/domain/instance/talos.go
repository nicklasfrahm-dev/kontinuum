package instance

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"

	"github.com/cosi-project/runtime/pkg/resource"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/hardware"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// maintenanceModePort is the gRPC port apid listens on while a Talos node
// is unconfigured and in maintenance mode — see talosctl's own default
// dial target for a maintenance-mode address.
const maintenanceModePort = 50000

// DiscoveryResult is everything a successful Discover call learns about a
// candidate node — see Discoverer's own doc.
type DiscoveryResult struct {
	// TalosVersion is the node's reported Talos version, best-effort — see
	// Discover's own doc for why it's frequently left empty.
	TalosVersion string
	// Interfaces lists the node's discovered network interfaces.
	Interfaces []v1alpha2.InstanceInterfaceStatus
	// Disks lists the node's discovered disks — see issue #76.
	Disks []v1alpha2.InstanceDiskStatus
	// CPUs lists the node's discovered processor sockets — see issue #76.
	CPUs []v1alpha2.InstanceCPUStatus
	// Memory lists the node's discovered memory modules — see issue #76.
	Memory []v1alpha2.InstanceMemoryStatus
}

// Discoverer probes a single candidate address for Talos maintenance-mode
// discovery info. talosDiscoverer is the production implementation, dialing
// a real node with github.com/siderolabs/talos/pkg/machinery/client; tests
// inject a fake to avoid a real gRPC dial — see controller_test.go.
type Discoverer interface {
	// Discover connects to addr (host or IP, no port) in Talos maintenance
	// mode and returns everything learned about the node — see
	// DiscoveryResult.
	Discover(ctx context.Context, addr string) (DiscoveryResult, error)
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
func (talosDiscoverer) Discover(ctx context.Context, addr string) (DiscoveryResult, error) {
	endpoint := net.JoinHostPort(addr, strconv.Itoa(maintenanceModePort))

	talosClient, err := talosclient.New(ctx,
		//nolint:gosec // maintenance mode has no issued CA yet — see this func's doc
		talosclient.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		talosclient.WithEndpoints(endpoint),
	)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to dial %s: %w", endpoint, err)
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
		return DiscoveryResult{}, fmt.Errorf("failed to discover interfaces from %s: %w", endpoint, err)
	}

	disks, err := discoverDisks(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover disks from %s: %w", endpoint, err)
	}

	cpus, err := discoverCPUs(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover cpus from %s: %w", endpoint, err)
	}

	memory, err := discoverMemory(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover memory from %s: %w", endpoint, err)
	}

	return DiscoveryResult{
		TalosVersion: talosVersion,
		Interfaces:   interfaces,
		Disks:        disks,
		CPUs:         cpus,
		Memory:       memory,
	}, nil
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

// discoverDisks reads talosClient's COSI block.Disk resources — the same
// resource `talosctl get disks` reads against a maintenance-mode node —
// see issue #76.
func discoverDisks(ctx context.Context, talosClient *talosclient.Client) ([]v1alpha2.InstanceDiskStatus, error) {
	diskMetadata := resource.NewMetadata(block.NamespaceName, block.DiskType, "", resource.VersionUndefined)

	disks, err := talosClient.COSI.List(ctx, diskMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to list disk resources: %w", err)
	}

	result := make([]v1alpha2.InstanceDiskStatus, 0, len(disks.Items))

	for _, item := range disks.Items {
		disk, ok := item.(*block.Disk)
		if !ok {
			continue
		}

		spec := disk.TypedSpec()

		result = append(result, v1alpha2.InstanceDiskStatus{
			DevPath:    spec.DevPath,
			Size:       spec.Size,
			PrettySize: spec.PrettySize,
			Model:      spec.Model,
			Serial:     spec.Serial,
			Transport:  spec.Transport,
			Rotational: spec.Rotational,
		})
	}

	return result, nil
}

// discoverCPUs reads talosClient's COSI hardware.Processor resources — the
// same resource `talosctl get cpus` reads against a maintenance-mode node
// — see issue #76.
func discoverCPUs(ctx context.Context, talosClient *talosclient.Client) ([]v1alpha2.InstanceCPUStatus, error) {
	cpuMetadata := resource.NewMetadata(hardware.NamespaceName, hardware.ProcessorType, "", resource.VersionUndefined)

	cpus, err := talosClient.COSI.List(ctx, cpuMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to list processor resources: %w", err)
	}

	result := make([]v1alpha2.InstanceCPUStatus, 0, len(cpus.Items))

	for _, item := range cpus.Items {
		processor, ok := item.(*hardware.Processor)
		if !ok {
			continue
		}

		spec := processor.TypedSpec()

		result = append(result, v1alpha2.InstanceCPUStatus{
			Manufacturer: spec.Manufacturer,
			ProductName:  spec.ProductName,
			CoreCount:    spec.CoreCount,
			ThreadCount:  spec.ThreadCount,
			MaxSpeedMHz:  spec.MaxSpeed,
		})
	}

	return result, nil
}

// discoverMemory reads talosClient's COSI hardware.MemoryModule resources —
// the same resource `talosctl get memorymodules` reads against a
// maintenance-mode node — see issue #76. Empty modules (no DIMM installed
// in that slot, Size 0) are skipped, mirroring `talosctl get memorymodules`'
// own display, which reports every physical slot regardless of occupancy.
func discoverMemory(ctx context.Context, talosClient *talosclient.Client) ([]v1alpha2.InstanceMemoryStatus, error) {
	memMetadata := resource.NewMetadata(hardware.NamespaceName, hardware.MemoryModuleType, "", resource.VersionUndefined)

	modules, err := talosClient.COSI.List(ctx, memMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to list memory module resources: %w", err)
	}

	result := make([]v1alpha2.InstanceMemoryStatus, 0, len(modules.Items))

	for _, item := range modules.Items {
		module, ok := item.(*hardware.MemoryModule)
		if !ok {
			continue
		}

		spec := module.TypedSpec()
		if spec.Size == 0 {
			continue
		}

		result = append(result, v1alpha2.InstanceMemoryStatus{
			SizeMiB:      spec.Size,
			Manufacturer: spec.Manufacturer,
			Speed:        spec.Speed,
			Serial:       spec.SerialNumber,
		})
	}

	return result, nil
}
