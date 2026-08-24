package fabric

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"golang.org/x/mod/semver"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// candidates this zone's own gateway address gets assigned to (see
// BuildInterfaceAddressPatch). This is the concrete rule behind "advertise
// the fabric on every interface except the one already linked up to a
// public IP, or otherwise already carrying an address": an interface
// Talos discovery already reports holding an address is always treated as
// this node's own pre-existing uplink, never as a fabric candidate. wan is
// "" if no interface has one yet; fabricIfaces is empty if every
// discovered interface already does (e.g. a single-NIC node).
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

// fabricManagerNamespace is the namespace ensureFabricManagerWorkload
// installs the fabric manager workload into on a zone's own downstream
// cluster — reuses v1alpha2.KontinuumSystemNamespace's own value directly,
// matching pkg/domain/zone/workload.go's identical downstreamNamespace
// convention: this is the same namespace that zone's own kontinuum-server
// Deployment already lives in on this same downstream cluster.
const fabricManagerNamespace = v1alpha2.KontinuumSystemNamespace

// fabricManagerBaseName is the prefix every Deployment
// ensureFabricManagerWorkload upserts is named from — see
// fabricManagerDeploymentName's own doc for why the full name is scoped
// per interface, not this bare prefix alone.
const fabricManagerBaseName = "kontinuum-fabric-manager"

// fabricManagerDeploymentName derives the interface-scoped Deployment name
// ensureFabricManagerWorkload upserts. A gateway node terminating more than
// one VLAN (a trunk port with several tagged sub-interfaces, each backing
// a different zone's own Fabric) runs one fabricmanager process — and so
// one Deployment — per interface, matching the identical per-interface
// scoping pkg/cli/fabricmanager/nftables.go's own natTableName already
// uses for the nftables side. Without this, a second Fabric electing the
// very same node for a different interface would silently overwrite the
// first Fabric's own Deployment (same static name, same Selector), taking
// down that interface's own NAT gateway.
func fabricManagerDeploymentName(interfaceName string) string {
	return fabricManagerBaseName + "-" + SanitizeForK8sName(interfaceName)
}

// SanitizeForK8sName maps s onto a valid Kubernetes object name segment: a
// VLAN sub-interface's own kernel name (e.g. "eth0.100") contains a "."
// and uppercase letters aren't valid in a DNS-1123 label either (case is
// folded first, since real kernel interface names are lowercase already —
// no information lost there), so any remaining byte other than a
// lowercase ASCII letter or digit is escaped as "-" followed by its two
// lowercase hex digits (e.g. "." becomes "-2e") — including a literal "-"
// itself (escaped as "-2d"), so every "-" in the output unambiguously
// starts an escape sequence. Collapsing every disallowed byte to the same
// literal "-" (as an earlier version of this function did) let two
// different interfaces collide onto the same name (e.g. "eth0.1" and
// "eth0-1" both became "eth0-1"); this escaping is injective, so distinct
// inputs always produce distinct outputs.
func SanitizeForK8sName(s string) string {
	var sanitized strings.Builder

	lowered := strings.ToLower(s)

	for i := range len(lowered) {
		charByte := lowered[i]

		switch {
		case charByte >= 'a' && charByte <= 'z', charByte >= '0' && charByte <= '9':
			sanitized.WriteByte(charByte)
		default:
			fmt.Fprintf(&sanitized, "-%02x", charByte)
		}
	}

	return sanitized.String()
}

// fabricManagerNodeLabel is the well-known Kubernetes node label
// ensureFabricManagerWorkload pins the workload's own nodeSelector to —
// Talos sets a node's own kubectl-visible hostname to its owning
// Instance's own name (see pkg/domain/taloscluster/config.go's configBytes
// doc), so this always resolves to exactly the elected gateway Instance.
const fabricManagerNodeLabel = "kubernetes.io/hostname"

// ensureFabricManagerWorkload upserts the fabric manager Deployment on
// downstream: a single replica, pinned via nodeSelector to nodeName,
// running `kontinuum fabricmanager run --interface interfaceName` (see
// pkg/cli/fabricmanager — named for the node agent's own growing scope,
// not just NAT: DHCP and other per-zone network duties are expected to
// land as further fabricmanager subcommands/flags later) — a small,
// privileged (CAP_NET_ADMIN only, every other Linux capability dropped),
// host-network Pod, since programming the kernel's nftables ruleset and
// toggling ipv4 forwarding both require direct access to the node's own
// real network namespace, not this Pod's isolated one.
func ensureFabricManagerWorkload(
	ctx context.Context, downstream client.Client, image, nodeName, interfaceName string,
) error {
	err := ensureFabricManagerNamespace(ctx, downstream)
	if err != nil {
		return err
	}

	deployment := buildFabricManagerDeployment(image, nodeName, interfaceName)
	name := deployment.Name

	err = downstream.Create(ctx, deployment)
	if apierrors.IsAlreadyExists(err) {
		var existing appsv1.Deployment

		err = downstream.Get(ctx, client.ObjectKeyFromObject(deployment), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q deployment: %w", name, err)
		}

		existing.Spec = deployment.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q deployment: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q deployment: %w", name, err)
	}

	return nil
}

// deleteFabricManagerWorkload deletes interfaceName's own Deployment (see
// fabricManagerDeploymentName), tolerating NotFound — used by teardown.go
// when a Fabric is deleted. Deliberately leaves fabricManagerNamespace
// itself (and any earlier interface/route config Talos already applied to
// the node) alone: unlike zone.uninstallWorkload's own last step, this
// namespace is shared with zone's own kontinuum-server Deployment (and
// potentially a sibling interface's own fabricmanager Deployment too — see
// fabricManagerDeploymentName's own doc), so deleting it here could take
// those down too.
func deleteFabricManagerWorkload(ctx context.Context, downstream client.Client, interfaceName string) error {
	name := fabricManagerDeploymentName(interfaceName)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fabricManagerNamespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, deployment))
	if err != nil {
		return fmt.Errorf("failed to delete %q deployment: %w", name, err)
	}

	return nil
}

// ensureFabricManagerNamespace creates fabricManagerNamespace on downstream
// if it doesn't already exist — mirrors pkg/domain/zone/workload.go's
// identical ensureNamespace; tolerated running twice (zone's own
// installWorkload likely already created this same namespace) since both
// calls are idempotent.
func ensureFabricManagerNamespace(ctx context.Context, downstream client.Client) error {
	err := downstream.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fabricManagerNamespace}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q namespace: %w", fabricManagerNamespace, err)
	}

	return nil
}

// imagePullPolicy mirrors pkg/domain/zone/workload.go's identical helper —
// see its own doc for why a digest-pinned or real-semver image is safe to
// cache (PullIfNotPresent) while a bare floating tag ("dev"/"latest") is
// re-pulled every time (PullAlways).
func imagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@sha256:") {
		return corev1.PullIfNotPresent
	}

	tag := image[strings.LastIndex(image, ":")+1:]

	if semver.IsValid(tag) {
		return corev1.PullIfNotPresent
	}

	return corev1.PullAlways
}

// fabricManagerLabels is name's own Deployment's pod-template labels —
// name already carries the interface (see fabricManagerDeploymentName),
// so two Deployments for different interfaces on the same node never
// share a Selector (which would otherwise let one Deployment's controller
// adopt the other's Pods).
func fabricManagerLabels(name string) map[string]string {
	return map[string]string{"app.kubernetes.io/name": name}
}

// buildFabricManagerDeployment returns the desired fabric manager
// Deployment — see ensureFabricManagerWorkload's own doc for the full
// rationale behind its shape.
func buildFabricManagerDeployment(image, nodeName, interfaceName string) *appsv1.Deployment {
	name := fabricManagerDeploymentName(interfaceName)
	labels := fabricManagerLabels(name)
	replicas := int32(1)
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fabricManagerNamespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					HostNetwork:  true,
					DNSPolicy:    corev1.DNSClusterFirstWithHostNet,
					NodeSelector: map[string]string{fabricManagerNodeLabel: nodeName},
					Containers: []corev1.Container{{
						Name:            fabricManagerBaseName,
						Image:           image,
						ImagePullPolicy: imagePullPolicy(image),
						Args:            []string{"fabricmanager", "run", "--interface", interfaceName},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
							Capabilities: &corev1.Capabilities{
								Add:  []corev1.Capability{"NET_ADMIN"},
								Drop: []corev1.Capability{"ALL"},
							},
						},
					}},
				},
			},
		},
	}
}
