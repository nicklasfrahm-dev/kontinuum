package fabric

import (
	"context"
	"errors"
	"fmt"
	"net"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// errNoWANInterface is reconcileNATForGatewayNode's sentinel for an
// elected gateway Instance with no interface carrying an address yet —
// nothing reachable to dial the node on, and nothing to masquerade
// outbound traffic through. Expected right after election, before
// maintenance-mode (or, once claimed, cluster-joined) probing has
// populated status.interfaces; the next requeue retries.
var errNoWANInterface = errors.New("gateway instance has no interface with an assigned address yet")

// errNoFabricInterface is reconcileNATForGatewayNode's sentinel for an
// elected gateway Instance whose every discovered interface already
// carries an address — see classifyGatewayInterfaces's own doc for why
// that leaves nothing to advertise the fabric's own gateway address on. A
// single-NIC node can never satisfy this: advertising a fabric needs a
// gateway node with at least one interface beyond its own already
// addressed uplink.
var errNoFabricInterface = errors.New("gateway instance has no free interface to advertise the fabric on")

// classifyGatewayInterfaces splits inst's own discovered interfaces into
// wan — the one already carrying a real (non-loopback) address, this
// node's own pre-existing uplink, both what it's dialed on (see
// dialAddress) and what NAT masquerades outbound traffic through — and
// fabricIfaces, every other interface with no address of its own yet, the
// candidates this zone's own gateway address gets assigned to (recorded
// as entry.GatewayInterfaces — see reconcileNATForGatewayNode — for
// pkg/cli/fabricmanager to actually apply). This is the concrete rule
// behind "advertise the fabric on every interface except the one already
// linked up to a public IP, or otherwise already carrying an address": an
// interface Talos discovery already reports holding an address is always
// treated as this node's own pre-existing uplink, never as a fabric
// candidate. wan is "" if no interface has one yet; fabricIfaces is empty
// if every discovered interface already does (e.g. a single-NIC node).
func classifyGatewayInterfaces(inst v1alpha2.Instance) (string, []string) {
	var wan string

	var fabricIfaces []string

	for _, iface := range inst.Status.Interfaces {
		if iface.Name == "lo" {
			continue
		}

		if hasUsableAddress(iface) {
			if wan == "" {
				wan = iface.Name
			}

			continue
		}

		fabricIfaces = append(fabricIfaces, iface.Name)
	}

	return wan, fabricIfaces
}

// hasUsableAddress reports whether iface already carries at least one
// real (non-loopback) address.
func hasUsableAddress(iface v1alpha2.InstanceInterfaceStatus) bool {
	for _, addr := range iface.Addresses {
		ip, _, err := net.ParseCIDR(addr)
		if err == nil && !ip.IsLoopback() {
			return true
		}
	}

	return false
}

// kubeconfigSecretKey is the key a TalosCluster's own kubeconfig is stored
// under in the Secret its status.secretRef points to — must match
// pkg/domain/taloscluster/secrets.go's own kubeconfigKey. Duplicated rather
// than imported, mirroring pkg/domain/zone's identical
// kubeconfigSecretKey — see that constant's own doc for the import-cycle-
// avoidance rationale (taloscluster already imports pkg/domain/addon, and
// would cycle back through fabric too once fabric depends on taloscluster's
// own types, the same shape as zone/addon's existing cycle).
const kubeconfigSecretKey = "kubeconfig"

// errKubeconfigNotStored is loadClusterKubeconfig's sentinel — mirrors
// pkg/domain/zone's identical errKubeconfigNotStored.
var errKubeconfigNotStored = errors.New("secret has no stored kubeconfig yet")

// loadClusterKubeconfig fetches cluster's own stored kubeconfig — mirrors
// pkg/domain/zone's identical helper (see kubeconfigSecretKey's own doc for
// why it's duplicated rather than imported).
func loadClusterKubeconfig(
	ctx context.Context, hubClient client.Client, cluster *v1alpha2.TalosCluster,
) ([]byte, error) {
	ref := cluster.Status.SecretRef

	var secret corev1.Secret

	err := hubClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secret to load kubeconfig: %w", ref.Name, err)
	}

	kubeconfig, ok := secret.Data[kubeconfigSecretKey]
	if !ok {
		return nil, fmt.Errorf("%q %w", ref.Name, errKubeconfigNotStored)
	}

	return kubeconfig, nil
}

// dialAddress returns the address used to reach inst — mirrors
// pkg/domain/taloscluster/members.go's identical dialAddress (see that
// function's own doc for why status.interfaces is preferred over
// spec.interfaces[0] verbatim). Duplicated rather than imported: it's
// unexported there, and this package already avoids importing taloscluster
// directly for the same cycle-avoidance reason kubeconfigSecretKey's own
// doc explains.
func dialAddress(inst v1alpha2.Instance) string {
	for _, iface := range inst.Status.Interfaces {
		for _, addr := range iface.Addresses {
			ip, _, err := net.ParseCIDR(addr)
			if err != nil || ip.IsLoopback() {
				continue
			}

			return ip.String()
		}
	}

	if len(inst.Spec.Interfaces) > 0 {
		return inst.Spec.Interfaces[0]
	}

	return ""
}
