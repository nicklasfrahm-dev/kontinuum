package instance

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
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
	// Disks lists the node's discovered disks, when Discover was called
	// with includeHardware — see issue #76 and v1alpha2.InstanceStatus.
	// Disks' own doc.
	Disks []v1alpha2.InstanceDiskStatus
	// CPUs lists the node's discovered processor sockets, when Discover
	// was called with includeHardware — see Disks' own doc.
	CPUs []v1alpha2.InstanceCPUStatus
	// Memory lists the node's discovered memory modules, when Discover was
	// called with includeHardware — see Disks' own doc.
	Memory []v1alpha2.InstanceMemoryStatus
	// SerialNumber is the node's chassis/board serial number, from SMBIOS,
	// when Discover was called with includeHardware — see Disks' own doc
	// and v1alpha2.InstanceStatus.SerialNumber's own doc.
	SerialNumber string
}

// Discoverer probes a single candidate address for Talos maintenance-mode
// discovery info. talosDiscoverer is the production implementation, dialing
// a real node with github.com/siderolabs/talos/pkg/machinery/client; tests
// inject a fake to avoid a real gRPC dial — see controller_test.go.
type Discoverer interface {
	// Discover connects to addr (host or IP, no port) in Talos maintenance
	// mode and returns everything learned about the node — see
	// DiscoveryResult. includeHardware controls whether Disks/CPUs/Memory/
	// SerialNumber are also enumerated: probeCandidates only asks for it
	// while the Instance isn't already Discovered — see its own doc for
	// why a steady-state recheck of an already-Discovered candidate skips
	// it instead.
	Discover(ctx context.Context, addr string, includeHardware bool) (DiscoveryResult, error)
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
func (talosDiscoverer) Discover(ctx context.Context, addr string, includeHardware bool) (DiscoveryResult, error) {
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
	// Arch (e.g. amd64, arm64) rides along on the same best-effort response
	// — it's the node's own architecture, not a per-socket property, but
	// Talos has no other maintenance-mode source for it, so every
	// discovered CPU socket gets stamped with the same value.
	talosVersion := ""

	var arch string

	versionResp, err := talosClient.Version(ctx)
	if err == nil {
		if messages := versionResp.GetMessages(); len(messages) > 0 {
			talosVersion = messages[0].GetVersion().GetTag()
			arch = messages[0].GetVersion().GetArch()
		}
	}

	interfaces, err := discoverInterfaces(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover interfaces from %s: %w", endpoint, err)
	}

	result := DiscoveryResult{TalosVersion: talosVersion, Interfaces: interfaces}

	if !includeHardware {
		return result, nil
	}

	result.Disks, err = discoverDisks(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover disks from %s: %w", endpoint, err)
	}

	result.CPUs, err = discoverCPUs(ctx, talosClient, arch)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover cpus from %s: %w", endpoint, err)
	}

	result.Memory, err = discoverMemory(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover memory from %s: %w", endpoint, err)
	}

	result.SerialNumber, err = discoverSerialNumber(ctx, talosClient)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("failed to discover serial number from %s: %w", endpoint, err)
	}

	return result, nil
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
// — see issue #76. arch (e.g. amd64, arm64) has no COSI source of its own —
// see Discover's own doc — so it's stamped onto every returned socket as-is.
func discoverCPUs(
	ctx context.Context, talosClient *talosclient.Client, arch string,
) ([]v1alpha2.InstanceCPUStatus, error) {
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
			Architecture: arch,
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

// discoverSerialNumber reads talosClient's COSI hardware.SystemInformation
// resource — the same resource `talosctl get systeminformation` reads
// against a maintenance-mode node — for the machine's own chassis/board
// serial number (from SMBIOS). Unlike the List-based discover* functions
// above, this is a singleton resource fetched by its fixed ID. Missing
// SMBIOS data (e.g. some VM configurations) isn't treated as a discovery
// failure — it's reported as best-effort, same as Version above, just
// left empty.
func discoverSerialNumber(ctx context.Context, talosClient *talosclient.Client) (string, error) {
	sysInfoMetadata := resource.NewMetadata(
		hardware.NamespaceName, hardware.SystemInformationType, hardware.SystemInformationID, resource.VersionUndefined)

	item, err := talosClient.COSI.Get(ctx, sysInfoMetadata)
	if err != nil {
		if state.IsNotFoundError(err) {
			return "", nil
		}

		return "", fmt.Errorf("failed to get system information resource: %w", err)
	}

	sysInfo, ok := item.(*hardware.SystemInformation)
	if !ok {
		return "", nil
	}

	return sysInfo.TypedSpec().SerialNumber, nil
}
