package taloscluster

import (
	"context"
	"fmt"
	"net"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

// resolveMembers lists the Instance objects claimed by poolRef's
// InstancePool that are also Discovered — TalosCluster only ever touches
// Instances instancepool.Reconciler has already claimed and
// instance.Reconciler has already probed successfully, since only those
// have a known-reachable dialAddress. namespace is the owning TalosCluster's
// own namespace — InstancePoolReference is a same-namespace-only reference
// (see issue #63's architecture), the same convention a Deployment already
// uses for its ConfigMap/Secret refs.
func resolveMembers(
	ctx context.Context, kubeClient client.Client, namespace string, poolRef v1alpha2.InstancePoolReference,
) ([]v1alpha2.Instance, error) {
	claimed, err := listClaimedByPool(ctx, kubeClient, namespace, poolRef.Name)
	if err != nil {
		return nil, err
	}

	members := make([]v1alpha2.Instance, 0, len(claimed))

	for _, inst := range claimed {
		if meta.IsStatusConditionTrue(inst.Status.Conditions, instance.DiscoveredConditionType) {
			members = append(members, inst)
		}
	}

	return members, nil
}

// listClaimedByPool lists every Instance claimed by poolName within
// namespace — the same label query resolveMembers itself narrows further
// (to Discovered members only) above, and teardown.go's own
// listClaimedMembers reuses unfiltered: an unreachable/undiscovered member
// still needs releasing (or deleting) during teardown even though it
// can't be reset.
func listClaimedByPool(
	ctx context.Context, kubeClient client.Client, namespace, poolName string,
) ([]v1alpha2.Instance, error) {
	var list v1alpha2.InstanceList

	err := kubeClient.List(ctx, &list,
		client.InNamespace(namespace), client.MatchingLabels{v1alpha2.LabelClaimedBy: poolName})
	if err != nil {
		return nil, fmt.Errorf("failed to list instances claimed by pool %q: %w", poolName, err)
	}

	return list.Items, nil
}

// dialAddress returns the address used to reach inst in maintenance mode and
// (once configured) via its real mTLS identity. Talos's own server
// certificate, once past maintenance mode, is scoped to the node's own
// detected hostname/addresses — not whatever string spec.interfaces[0]
// happened to be typed in as (which can be a DNS hostname, e.g. a Docker
// Compose service name that resolves fine but isn't what the certificate
// covers) — so this always prefers a real discovered IP from
// status.interfaces (populated by maintenance-mode probing once Discovered
// is true — see pkg/domain/instance's discoverInterfaces) over
// spec.interfaces[0] verbatim, skipping loopback addresses. Falls back to
// spec.interfaces[0] only if status.interfaces has no usable address yet.
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

	return inst.Spec.Interfaces[0]
}
