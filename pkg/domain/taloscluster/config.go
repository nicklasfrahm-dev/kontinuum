package taloscluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/container"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/config/types/network"
	"github.com/siderolabs/talos/pkg/machinery/config/types/v1alpha1"
	"github.com/siderolabs/talos/pkg/machinery/constants"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// defaultTalosVersion and defaultKubernetesVersion are used whenever a
	// TalosVersionSpec leaves Talos/Kubernetes unset — see that type's own
	// doc for why an unset value isn't defaulted to another member's
	// version instead. Pinned to the same Talos version already used
	// elsewhere in this repo (go.mod's machinery requirement, and the
	// discovery e2e test's own pinned image).
	defaultTalosVersion      = "v1.13.0"
	defaultKubernetesVersion = "v1.32.0"

	// kubernetesAPIPort is the Kubernetes API server's fixed port, used to
	// build the cluster endpoint embedded in every generated machine
	// config.
	kubernetesAPIPort = 6443
)

// resolveVersions fills in defaultTalosVersion/defaultKubernetesVersion for
// whichever of cluster's talos/kubernetes version fields are left empty,
// and strips a leading "v" from the Kubernetes version: KubernetesSpec's
// own doc uses the conventional "v1.32.0" form, but generate.NewInput's
// kubernetesVersion argument is used bare (Talos machinery prepends its
// own "v" when building image references like the kubelet image tag —
// passing an already-prefixed value here doubles it into an invalid
// "vv1.32.0" tag that fails to pull).
func resolveVersions(cluster *v1alpha2.TalosCluster) (string, string) {
	talosVersion := cluster.Spec.Talos.Version
	if talosVersion == "" {
		talosVersion = defaultTalosVersion
	}

	kubernetesVersion := cluster.Spec.Kubernetes.Version
	if kubernetesVersion == "" {
		kubernetesVersion = defaultKubernetesVersion
	}

	kubernetesVersion = strings.TrimPrefix(kubernetesVersion, "v")

	return talosVersion, kubernetesVersion
}

// endpointFor builds the https://<addr>:6443 cluster endpoint embedded in
// generated machine configs. This phase points it straight at a single
// control-plane member's own address — see the implementation plan's
// "Control-plane endpoint" decision for why a multi-replica control plane
// has no load-balanced endpoint yet.
func endpointFor(addr string) string {
	return "https://" + net.JoinHostPort(addr, strconv.Itoa(kubernetesAPIPort))
}

// generateConfigs builds the shared config generator input (talosctl gen
// config's programmatic equivalent) for clusterName, signed by bundle and
// targeting controlPlaneAddr's own address as both the dial/apid endpoint
// and (via endpointFor) the cluster's Kubernetes API endpoint, plus the
// admin Talosconfig used to dial the real (non-maintenance-mode) cluster
// once bootstrapped. It does not itself produce per-node machine config
// bytes — see configBytes, which every member needs its own call to
// (passing its own hostname), rather than one shared config reused
// verbatim across every member of a role.
//
// CNI is disabled (constants.NoneCNI) rather than left at Talos's own
// flannel default: this controller installs Cilium itself (see the issue's
// own scope), and Talos's ClusterHealthCheck waits on CoreDNS reporting
// ready, which itself needs a working pod network — leaving flannel
// enabled races two CNIs against each other for nothing, and either one
// alone is sufficient to unblock CoreDNS, so only the one this controller
// actually manages is installed. Scheduling is allowed on control-plane
// nodes (talosctl's own --allow-scheduling-on-control-planes) since a
// single-node cluster (this phase's common case — see decision 3/5) has
// nowhere else for Cilium/CoreDNS/cert-manager to run.
func generateConfigs(
	bundle *talossecrets.Bundle, cluster *v1alpha2.TalosCluster, controlPlaneAddr string,
) (*generate.Input, *clientconfig.Config, error) {
	clusterName := cluster.Name

	talosVersion, kubernetesVersion := resolveVersions(cluster)

	contract, err := talosconfig.ParseContractFromVersion(talosVersion)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse talos version contract %q: %w", talosVersion, err)
	}

	input, err := generate.NewInput(clusterName, endpointFor(controlPlaneAddr), kubernetesVersion,
		generate.WithSecretsBundle(bundle),
		generate.WithVersionContract(contract),
		generate.WithEndpointList([]string{controlPlaneAddr}),
		generate.WithClusterCNIConfig(&v1alpha1.CNIConfig{CNIName: constants.NoneCNI}),
		generate.WithAllowSchedulingOnControlPlanes(true),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build config generator input for %q: %w", clusterName, err)
	}

	talosCfg, err := input.Talosconfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate talosconfig for %q: %w", clusterName, err)
	}

	return input, talosCfg, nil
}

// configBytes generates and encodes machineType's machine config from
// input for one specific member, with its install disk left to Talos's own
// auto-selection (see applyAutoInstallDiskSelector's own doc) and its
// hostname set to hostname — the member's own Instance name — rather than
// left to Talos's own DHCP/mDNS-derived default, so `kubectl get nodes` and
// this Instance's own name agree on what to call it. The hostname is
// carried as a separate HostnameConfig multi-document (bundled alongside
// the main v1alpha1 config via container.New), not the legacy
// machine.network.hostname field directly — Talos's newer versions phased
// that field out in favor of this, and the two are mutually exclusive
// (Talos's own config validation rejects setting both).
func configBytes(input *generate.Input, machineType machine.Type, hostname string) ([]byte, error) {
	provider, err := input.Config(machineType)
	if err != nil {
		return nil, fmt.Errorf("failed to generate %s config: %w", machineType, err)
	}

	applyAutoInstallDiskSelector(provider)

	hostnameDoc := network.NewHostnameConfigV1Alpha1()
	hostnameDoc.ConfigHostname = hostname

	bundle, err := container.New(provider.RawV1Alpha1(), hostnameDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to bundle %s hostname config: %w", machineType, err)
	}

	data, err := bundle.Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to encode %s config: %w", machineType, err)
	}

	return data, nil
}

// applyAutoInstallDiskSelector sets an empty InstallDiskSelector on
// provider's generated config, letting Talos pick the install disk itself
// instead of requiring one hardcoded up front — this repo has no idea what
// a given bare-metal/VM candidate's disks are named (/dev/sda, /dev/vda,
// /dev/nvme0n1, ...) at config-generation time, and generate.NewInput's own
// options only cover a fixed InstallDisk path (generate.WithInstallDisk),
// never a selector. Left unset entirely (generate.NewInput's own default),
// Talos's own config validation rejects the result outright the moment a
// real, non-container node (mode.RequiresInstall() — see
// v1alpha1_validation.go) tries to apply it: "either install disk or
// diskSelector should be defined". An InstallDiskSelector with every field
// left zero isn't a no-op, either — Talos's own DiskMatchExpression (see
// siderolabs/talos's InstallConfig) always excludes read-only disks and
// CD-ROMs regardless, then installs to the first disk still matching; this
// is Talos's own built-in "any disk" mechanism, not a kontinuum-specific
// workaround.
func applyAutoInstallDiskSelector(provider talosconfig.Provider) {
	provider.RawV1Alpha1().MachineConfig.MachineInstall.InstallDiskSelector = &v1alpha1.InstallDiskSelector{}
}
