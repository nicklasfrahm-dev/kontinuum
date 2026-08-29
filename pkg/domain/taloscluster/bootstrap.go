package taloscluster

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"

	clusterapi "github.com/siderolabs/talos/pkg/machinery/api/cluster"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/constants"
)

// maintenanceModePort is the gRPC port apid listens on while a Talos node
// is unconfigured and in maintenance mode — same constant, and same
// rationale, as instance's own maintenanceModePort (see
// pkg/domain/instance/talos.go).
const maintenanceModePort = 50000

// rpcTimeout bounds every single-shot (non-streaming) RPC this package
// makes on the client side — ApplyConfiguration, Bootstrap, Kubeconfig.
// None of these calls carry their own server-side timeout field the way
// ClusterHealthCheck's WaitTimeout does, but without a client-side
// deadline a wedged or unreachable node could still block the calling
// reconcile indefinitely.
const rpcTimeout = 30 * time.Second

// errNoCPUsReported is CPUTopology's own sentinel — see its own doc.
var errNoCPUsReported = errors.New("no possible cpus reported")

// ClusterBootstrapper is the Discoverer-style seam this package's
// controller dials Talos through — see instance.Discoverer's own doc for
// why this pattern exists. talosBootstrapper is the production
// implementation, dialing real nodes; tests inject a fake to avoid a real
// gRPC dial.
type ClusterBootstrapper interface {
	// ApplyConfiguration applies data (a generated machine config) to addr
	// in maintenance mode — talosctl apply-config's programmatic
	// equivalent. addr is expected to move out of maintenance mode as a
	// result (the node installs and reboots), so a failure here is often
	// just "already applied and rebooted," not a real problem — see this
	// interface's callers for how that's handled.
	ApplyConfiguration(ctx context.Context, addr string, data []byte) error
	// Bootstrap triggers etcd bootstrap on addr — talosctl bootstrap's
	// programmatic equivalent — dialing with the real (non-maintenance-mode)
	// admin identity in talosCfg.
	Bootstrap(ctx context.Context, addr string, talosCfg *clientconfig.Config) error
	// HealthCheck blocks (bounded by timeout) until the cluster reachable
	// via addr reports healthy, or returns an error if it doesn't in time.
	HealthCheck(
		ctx context.Context, addr string, talosCfg *clientconfig.Config, controlPlaneNodes []string, timeout time.Duration,
	) error
	// Kubeconfig fetches the cluster's kubeconfig via addr.
	Kubeconfig(ctx context.Context, addr string, talosCfg *clientconfig.Config) ([]byte, error)
	// Version fetches the Talos version and architecture reported by node,
	// dialing endpoint with the real (non-maintenance-mode) admin identity
	// in talosCfg and routing the request to node via client.WithNode —
	// see this interface's own Version implementation doc for why
	// maintenance-mode discovery (pkg/domain/instance's own Discoverer)
	// can no longer learn either of these on current Talos releases,
	// making endpoint/node's post-config identity the only place left to
	// fetch them.
	Version(ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config) (version, arch string, err error)
	// CPUTopology reads node's own /sys/devices/system/cpu directly via the
	// Talos API's Read RPC — the same endpoint/node/talosCfg targeting
	// Version uses, since maintenance mode has no Read access either — as a
	// workaround for boards whose SMBIOS-derived hardware.Processor data is
	// incomplete (see this interface's own CPUTopology implementation doc,
	// and nicklasfrahm-dev/kontinuum#130 / siderolabs/talos#14171).
	CPUTopology(
		ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config,
	) (coreCount, threadCount, maxSpeedMHz uint32, err error)
	// Reset wipes node back to maintenance mode — talosctl reset's
	// programmatic equivalent — dialing endpoint with the real
	// (non-maintenance-mode) admin identity in talosCfg and routing the
	// request to node via client.WithNode, the same targeting Version
	// already uses (a reset seed node can no longer serve its own dial, so
	// this always goes through some other still-reachable member's
	// endpoint, or node's own address when it's the only member — see
	// pkg/domain/zone's teardown, this interface's only caller). graceful
	// must be false when node is (or is about to become, mid-reset) etcd's
	// only member — see graceful's doc on Reset's implementation for why
	// leaving etcd isn't safe to attempt in that case.
	Reset(ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config, graceful bool) error
}

// talosBootstrapper is ClusterBootstrapper's production implementation.
type talosBootstrapper struct {
	// Logger receives each HealthCheckProgress message Talos streams back
	// — the same per-check detail talosctl health -v prints (e.g. "waiting
	// for all k8s nodes to report ready"), otherwise silently discarded and
	// leaving only a generic context-deadline-exceeded error once
	// HealthCheck's own timeout elapses.
	Logger *slog.Logger
}

// NewTalosBootstrapper returns the production ClusterBootstrapper, which
// dials real Talos nodes. ClusterBootstrapper is this package's own seam
// for injecting a fake in tests — the whole point of this constructor is
// to hide talosBootstrapper behind it (mirrors
// instance.NewTalosDiscoverer's own doc).
//
//nolint:ireturn // see doc above
func NewTalosBootstrapper(logger *slog.Logger) ClusterBootstrapper {
	return talosBootstrapper{Logger: logger}
}

// ApplyConfiguration implements ClusterBootstrapper. A maintenance-mode
// node serves gRPC over a self-signed certificate with no CA yet issued —
// InsecureSkipVerify mirrors talosctl's own dial behavior against a node in
// this state, same as instance.talosDiscoverer.Discover's identical
// rationale.
func (talosBootstrapper) ApplyConfiguration(ctx context.Context, addr string, data []byte) error {
	endpoint := net.JoinHostPort(addr, strconv.Itoa(maintenanceModePort))

	talosClient, err := talosclient.New(ctx,
		//nolint:gosec // maintenance mode has no issued CA yet — see this func's doc
		talosclient.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		talosclient.WithEndpoints(endpoint),
	)
	if err != nil {
		return fmt.Errorf("failed to dial %s in maintenance mode: %w", endpoint, err)
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived apply-config connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	_, err = talosClient.ApplyConfiguration(rpcCtx, &machineapi.ApplyConfigurationRequest{
		Data: data,
		Mode: machineapi.ApplyConfigurationRequest_REBOOT,
	})
	if err != nil {
		return fmt.Errorf("failed to apply configuration to %s: %w", endpoint, err)
	}

	return nil
}

// Bootstrap implements ClusterBootstrapper.
func (t talosBootstrapper) Bootstrap(ctx context.Context, addr string, talosCfg *clientconfig.Config) error {
	talosClient, err := t.dial(ctx, addr, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived bootstrap connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	err = talosClient.Bootstrap(rpcCtx, &machineapi.BootstrapRequest{})
	if err != nil {
		return fmt.Errorf("failed to bootstrap %s: %w", addr, err)
	}

	return nil
}

// HealthCheck implements ClusterBootstrapper. timeout bounds this call on
// the client side (via ctx) as well as being sent to the server as
// HealthCheckRequest.WaitTimeout — that field only governs how long the
// server keeps retrying internally, it is not a client-side deadline, so
// without wrapping ctx here a slow or wedged server could block this call
// (and the reconcile that invoked it) indefinitely.
func (t talosBootstrapper) HealthCheck(
	ctx context.Context, addr string, talosCfg *clientconfig.Config, controlPlaneNodes []string, timeout time.Duration,
) error {
	talosClient, err := t.dial(ctx, addr, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived health-check connection

	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	clusterInfo := &clusterapi.ClusterInfo{ControlPlaneNodes: controlPlaneNodes}

	stream, err := talosClient.ClusterHealthCheck(checkCtx, timeout, clusterInfo)
	if err != nil {
		return fmt.Errorf("failed to start health check against %s: %w", addr, err)
	}

	for {
		progress, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}

		if recvErr != nil {
			return fmt.Errorf("cluster health check against %s failed: %w", addr, recvErr)
		}

		t.Logger.Debug("health check progress", "address", addr, "message", progress.GetMessage())
	}
}

// Kubeconfig implements ClusterBootstrapper.
func (t talosBootstrapper) Kubeconfig(ctx context.Context, addr string, talosCfg *clientconfig.Config) ([]byte, error) {
	talosClient, err := t.dial(ctx, addr, talosCfg)
	if err != nil {
		return nil, err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived kubeconfig-fetch connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	kubeconfig, err := talosClient.Kubeconfig(rpcCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch kubeconfig from %s: %w", addr, err)
	}

	return kubeconfig, nil
}

// Version implements ClusterBootstrapper. Unlike ApplyConfiguration, this
// deliberately doesn't dial node directly in maintenance mode: recent Talos
// releases gate the maintenance-mode Version RPC behind an os:admin role
// check that no maintenance-mode caller — kontinuum's discoverer or
// talosctl alike — can ever satisfy, since there's no CA yet to issue that
// role's client cert from (see pkg/domain/instance/talos.go's own Discover
// doc). Once node has moved past maintenance mode, though, talosCfg's real
// admin identity satisfies that check, so this dials endpoint (any
// reachable, already-configured cluster member) and targets node via
// client.WithNode, the same way talosctl -n routes a single dial to any
// cluster member.
func (t talosBootstrapper) Version(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config,
) (string, string, error) {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return "", "", err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived version-fetch connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	versionResp, err := talosClient.Version(talosclient.WithNode(rpcCtx, node))
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch talos version for %s via %s: %w", node, endpoint, err)
	}

	if messages := versionResp.GetMessages(); len(messages) > 0 {
		return messages[0].GetVersion().GetTag(), messages[0].GetVersion().GetArch(), nil
	}

	return "", "", nil
}

// cpuPossiblePath, cpuCoreIDPathf, and cpuMaxFreqPathf are the sysfs paths
// CPUTopology reads — see its own doc.
const (
	cpuPossiblePath = "/sys/devices/system/cpu/possible"
	cpuCoreIDPathf  = "/sys/devices/system/cpu/cpu%d/topology/core_id"
	cpuMaxFreqPathf = "/sys/devices/system/cpu/cpu%d/cpufreq/cpuinfo_max_freq"
)

// khzPerMHz converts cpuinfo_max_freq's kHz reading to the MHz
// v1alpha2.InstanceCPUStatus.MaxSpeedMHz itself uses.
const khzPerMHz = 1000

// CPUTopology implements ClusterBootstrapper. It's a workaround, not a
// long-term fix — see nicklasfrahm-dev/kontinuum#130, which links
// siderolabs/talos#14171: on boards with thin/incomplete SMBIOS data (e.g.
// Raspberry Pi CM4), Talos's own hardware.Processor resource comes back
// with CoreCount/ThreadCount/MaxSpeed all zero, because it's built purely
// from SMBIOS with no fallback. The Linux kernel knows all three
// regardless of firmware, so this reads them straight from node's own
// sysfs via the Talos API's Read RPC — dialed the same way, and gated by
// the same maintenance-mode limitation, as Version above (maintenance mode
// has no Read access either).
//
// CoreCount is the number of distinct topology/core_id values across every
// possible CPU — deliberately not cross-referenced with
// physical_package_id to also split multi-socket systems apart, since
// every board this workaround actually targets is a single-SoC ARM
// board with no such thing; see this repo's own tracking issue for why
// that's an acceptable simplification for a workaround, not the real fix.
// MaxSpeedMHz comes from CPU 0's own cpufreq driver and is left zero, not
// an error, when that driver doesn't exist.
func (t talosBootstrapper) CPUTopology(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config,
) (uint32, uint32, uint32, error) {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return 0, 0, 0, err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived sysfs-read connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	rpcCtx = talosclient.WithNode(rpcCtx, node)

	possible, err := readSysfsFile(rpcCtx, talosClient, cpuPossiblePath)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read cpu list for %s via %s: %w", node, endpoint, err)
	}

	cpus, err := parseCPUList(possible)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to parse cpu list for %s: %w", node, err)
	}

	if len(cpus) == 0 {
		return 0, 0, 0, fmt.Errorf("%w: %s", errNoCPUsReported, node)
	}

	threadCount := countCPUs(cpus)

	coreCount := countDistinctCores(rpcCtx, talosClient, cpus)
	if coreCount == 0 {
		// topology/core_id was unreadable for every cpu — assume no SMT
		// rather than reporting zero cores for a machine we just proved has
		// at least threadCount of them.
		coreCount = threadCount
	}

	maxSpeedMHz := readMaxSpeedMHz(rpcCtx, talosClient, cpus[0])

	return coreCount, threadCount, maxSpeedMHz, nil
}

// countCPUs is len(cpus) as a uint32 — cpus comes from parseCPUList, whose
// own bitSize-32 parsing already keeps every individual value in range, and
// no real machine has anywhere near 1<<32 CPUs, so len() itself can't
// overflow here either.
func countCPUs(cpus []uint32) uint32 {
	return uint32(len(cpus)) //nolint:gosec // see doc above
}

// countDistinctCores reads topology/core_id for every entry in cpus and
// returns how many distinct values came back — see CPUTopology's own doc
// for why physical_package_id isn't also factored in. A cpu whose core_id
// can't be read or parsed is skipped, not fatal — see CPUTopology's own
// best-effort framing.
func countDistinctCores(rpcCtx context.Context, talosClient *talosclient.Client, cpus []uint32) uint32 {
	coreIDs := make(map[uint32]struct{}, len(cpus))

	for _, cpu := range cpus {
		coreIDRaw, err := readSysfsFile(rpcCtx, talosClient, fmt.Sprintf(cpuCoreIDPathf, cpu))
		if err != nil {
			continue
		}

		coreID, err := parseSysfsUint(coreIDRaw)
		if err != nil {
			continue
		}

		coreIDs[coreID] = struct{}{}
	}

	return uint32(len(coreIDs)) //nolint:gosec // bounded by len(cpus) above, see countCPUs' own doc
}

// readMaxSpeedMHz reads cpu's own cpufreq-reported max frequency and
// converts it to MHz, or returns 0 when the cpufreq driver doesn't exist
// for this platform — genuinely best-effort, not an error CPUTopology's
// caller needs to see.
func readMaxSpeedMHz(rpcCtx context.Context, talosClient *talosclient.Client, cpu uint32) uint32 {
	maxFreqRaw, err := readSysfsFile(rpcCtx, talosClient, fmt.Sprintf(cpuMaxFreqPathf, cpu))
	if err != nil {
		return 0
	}

	maxFreqKHz, err := parseSysfsUint(maxFreqRaw)
	if err != nil {
		return 0
	}

	return maxFreqKHz / khzPerMHz
}

// readSysfsFile reads path from talosClient's already-node-targeted rpcCtx
// (see talosclient.WithNode) and returns its trimmed contents — every
// sysfs file CPUTopology reads is a single short line.
func readSysfsFile(rpcCtx context.Context, talosClient *talosclient.Client, path string) (string, error) {
	reader, err := talosClient.Read(rpcCtx, path)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", path, err)
	}
	defer reader.Close() //nolint:errcheck // best-effort close after a fully-buffered read below

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", path, err)
	}

	return string(data), nil
}

// Reset implements ClusterBootstrapper. graceful is passed straight through
// as Talos's own Graceful flag — it is NOT safe to just always pass true.
// Talos's own docs are explicit about this: a graceful reset only works
// when the cluster is in a good HA state; a single-member cluster can't
// gracefully "leave" etcd at all (LeaveCluster's own etcd MemberRemove call
// against yourself, as the only member, is the documented hazard —
// https://docs.siderolabs.com/talos/v1.13/configure-your-talos-cluster/lifecycle-management/resetting-a-machine
// recommends --graceful=false for exactly this case). ResetControlPlane
// decides graceful per call based on how many members it resolved — this
// method has no cluster-topology awareness of its own and trusts whatever
// the caller passes. Reboot is always true, so the node comes back up into
// maintenance mode (wiped, discoverable again) rather than halting.
//
// Only the STATE and EPHEMERAL partitions are wiped — not the default
// whole-disk wipe talosClient.Reset's own convenience method would issue.
// kontinuum's seed nodes are bare-metal, pre-provisioned with Talos already
// installed to disk (see docs/workflows/zone-add.md: discovery dials an
// address the operator already booted into maintenance mode), not
// network/PXE-booted on every boot — a whole-disk wipe would also erase the
// boot/EFI partition Talos's own installer wrote there, leaving the machine
// unable to boot back into maintenance mode on its own and defeating
// ResetControlPlane's entire "rejoinable with no manual intervention"
// purpose (see docs/workflows/zone-remove.md). See
// https://docs.siderolabs.com/talos/v1.13/configure-your-talos-cluster/lifecycle-management/resetting-a-machine
// for the same caveat Talos's own docs give for cloud VMs.
func (t talosBootstrapper) Reset(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config, graceful bool,
) error {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived reset connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	err = talosClient.ResetGeneric(talosclient.WithNode(rpcCtx, node), &machineapi.ResetRequest{
		Graceful: graceful,
		Reboot:   true,
		SystemPartitionsToWipe: []*machineapi.ResetPartitionSpec{
			{Label: constants.StatePartitionLabel, Wipe: true},
			{Label: constants.EphemeralPartitionLabel, Wipe: true},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to reset %s via %s: %w", node, endpoint, err)
	}

	return nil
}

// dial connects to addr with talosCfg's real (non-maintenance-mode) admin
// identity — talosCfg carries the OS CA and an admin client cert both
// signed by the cluster's own secrets bundle (see generateConfigs), so no
// InsecureSkipVerify is needed here, unlike ApplyConfiguration's
// maintenance-mode dial.
func (talosBootstrapper) dial(
	ctx context.Context, addr string, talosCfg *clientconfig.Config,
) (*talosclient.Client, error) {
	talosClient, err := talosclient.New(ctx,
		talosclient.WithConfig(talosCfg),
		talosclient.WithEndpoints(addr),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	return talosClient, nil
}
