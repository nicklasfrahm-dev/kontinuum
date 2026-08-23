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

// errNoGatewayInterface is reconcileNetworkConfig/reconcileNATWorkload's
// sentinel for an elected gateway Instance with no status.interfaces
// discovered yet — expected right after election, before maintenance-mode
// (or, once claimed, cluster-joined) probing has populated it; the next
// requeue retries.
var errNoGatewayInterface = errors.New("gateway instance has no discovered interfaces")

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

// natGatewayNamespace is the namespace ensureNATGatewayWorkload installs
// the NAT gateway workload into on a zone's own downstream cluster —
// reuses v1alpha2.KontinuumSystemNamespace's own value directly, matching
// pkg/domain/zone/workload.go's identical downstreamNamespace convention:
// this is the same namespace that zone's own kontinuum-server Deployment
// already lives in on this same downstream cluster.
const natGatewayNamespace = v1alpha2.KontinuumSystemNamespace

// natGatewayName names the Deployment/ServiceAccount ensureNATGatewayWorkload
// upserts.
const natGatewayName = "kontinuum-nat-gateway"

// natGatewayNodeLabel is the well-known Kubernetes node label
// ensureNATGatewayWorkload pins the workload's own nodeSelector to —
// Talos sets a node's own kubectl-visible hostname to its owning
// Instance's own name (see pkg/domain/taloscluster/config.go's configBytes
// doc), so this always resolves to exactly the elected gateway Instance.
const natGatewayNodeLabel = "kubernetes.io/hostname"

// ensureNATGatewayWorkload upserts the NAT gateway Deployment on
// downstream: a single replica, pinned via nodeSelector to nodeName,
// running `kontinuum nat-gateway run --interface interfaceName` (see
// pkg/cli/natgateway) — a small, privileged (CAP_NET_ADMIN only, every
// other Linux capability dropped), host-network Pod, since programming the
// kernel's nftables ruleset and toggling ipv4 forwarding both require
// direct access to the node's own real network namespace, not this Pod's
// isolated one.
func ensureNATGatewayWorkload(
	ctx context.Context, downstream client.Client, image, nodeName, interfaceName string,
) error {
	err := ensureNATGatewayNamespace(ctx, downstream)
	if err != nil {
		return err
	}

	deployment := buildNATGatewayDeployment(image, nodeName, interfaceName)

	err = downstream.Create(ctx, deployment)
	if apierrors.IsAlreadyExists(err) {
		var existing appsv1.Deployment

		err = downstream.Get(ctx, client.ObjectKeyFromObject(deployment), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q deployment: %w", natGatewayName, err)
		}

		existing.Spec = deployment.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q deployment: %w", natGatewayName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q deployment: %w", natGatewayName, err)
	}

	return nil
}

// ensureNATGatewayNamespace creates natGatewayNamespace on downstream if it
// doesn't already exist — mirrors pkg/domain/zone/workload.go's identical
// ensureNamespace; tolerated running twice (zone's own installWorkload
// likely already created this same namespace) since both calls are
// idempotent.
func ensureNATGatewayNamespace(ctx context.Context, downstream client.Client) error {
	err := downstream.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: natGatewayNamespace}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q namespace: %w", natGatewayNamespace, err)
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

// natGatewayLabels is the NAT gateway Deployment's own pod-template labels.
func natGatewayLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": natGatewayName}
}

// buildNATGatewayDeployment returns the desired NAT gateway Deployment —
// see ensureNATGatewayWorkload's own doc for the full rationale behind its
// shape.
func buildNATGatewayDeployment(image, nodeName, interfaceName string) *appsv1.Deployment {
	labels := natGatewayLabels()
	replicas := int32(1)
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: natGatewayName, Namespace: natGatewayNamespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					HostNetwork:  true,
					DNSPolicy:    corev1.DNSClusterFirstWithHostNet,
					NodeSelector: map[string]string{natGatewayNodeLabel: nodeName},
					Containers: []corev1.Container{{
						Name:            natGatewayName,
						Image:           image,
						ImagePullPolicy: imagePullPolicy(image),
						Args:            []string{"nat-gateway", "run", "--interface", interfaceName},
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
