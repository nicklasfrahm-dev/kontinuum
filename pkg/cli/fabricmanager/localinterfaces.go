package fabricmanager

import (
	"fmt"
	"net"
)

// classifyLocalInterfaces splits this node's own currently-discovered
// network interfaces into wan — the one already carrying a real
// (non-loopback) address, this node's own pre-existing uplink, what NAT
// masquerades outbound traffic through — and fabricIfaces, every other
// interface with no address of its own yet. Mirrors
// pkg/domain/fabric.classifyGatewayInterfaces' own identical policy, but
// reads this node's own live interface state directly (net.Interfaces),
// rather than a hub-side Instance snapshot from Talos maintenance-mode
// discovery: this process already runs hostNetwork on the actual gateway
// node (see the DaemonSet pkg/domain/zone.ensureFabricManagerDaemonSet
// installs), so it can see current reality without that extra hop, and
// without depending on a status field the hub might not have refreshed
// recently. wan is "" if no interface has a real address yet.
func classifyLocalInterfaces() (string, []string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", nil, fmt.Errorf("failed to list local interfaces: %w", err)
	}

	var wan string

	var fabricIfaces []string

	for _, iface := range ifaces {
		if iface.Name == "lo" {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return "", nil, fmt.Errorf("failed to list addresses for interface %q: %w", iface.Name, err)
		}

		if hasUsableLocalAddress(addrs) {
			if wan == "" {
				wan = iface.Name
			}

			continue
		}

		fabricIfaces = append(fabricIfaces, iface.Name)
	}

	return wan, fabricIfaces, nil
}

// hasUsableLocalAddress reports whether addrs (an interface's own current
// addresses, as net.Interface.Addrs returns them — CIDR-form strings like
// "10.0.1.5/24") includes at least one real, non-loopback address.
func hasUsableLocalAddress(addrs []net.Addr) bool {
	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err == nil && !ip.IsLoopback() {
			return true
		}
	}

	return false
}
