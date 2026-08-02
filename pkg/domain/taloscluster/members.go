package taloscluster

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

// resolveMembers lists the Instance objects claimed by poolRef's
// InstancePool that are also Discovered — TalosCluster only ever touches
// Instances instancepool.Reconciler has already claimed and
// instance.Reconciler has already probed successfully, since only those
// have a known-reachable dialAddress.
func resolveMembers(
	ctx context.Context, kubeClient client.Client, poolRef v1alpha2.InstancePoolReference,
) ([]v1alpha2.Instance, error) {
	var list v1alpha2.InstanceList

	err := kubeClient.List(ctx, &list, client.MatchingLabels{v1alpha2.LabelClaimedBy: poolRef.Name})
	if err != nil {
		return nil, fmt.Errorf("failed to list instances claimed by pool %q: %w", poolRef.Name, err)
	}

	members := make([]v1alpha2.Instance, 0, len(list.Items))

	for _, inst := range list.Items {
		if meta.IsStatusConditionTrue(inst.Status.Conditions, instance.DiscoveredConditionType) {
			members = append(members, inst)
		}
	}

	return members, nil
}

// dialAddress returns the address used to reach inst in maintenance mode
// and (once configured) via its real mTLS identity — the same candidate
// address inst's own Discovered condition already proved reachable.
func dialAddress(inst v1alpha2.Instance) string {
	return inst.Spec.Interfaces[0]
}
