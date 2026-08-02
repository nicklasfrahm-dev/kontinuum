package addon

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

// kubeconfigSecretKey is the key a TalosCluster's own kubeconfig is
// stored under in the Secret its status.secretRef points to — must match
// pkg/domain/taloscluster/secrets.go's own kubeconfigKey. Duplicated
// rather than imported: pkg/domain/taloscluster already imports this
// package (to seed built-in Addons and aggregate their status), so the
// reverse import would cycle — see this package's own doc for why a
// handful of small, stable facts like this one are duplicated instead of
// shared through a new package.
const kubeconfigSecretKey = "kubeconfig"

// errKubeconfigNotStored is a static sentinel — err113 flags a
// dynamically constructed errors.New/fmt.Errorf call without a wrapped
// static error.
var errKubeconfigNotStored = errors.New("secret has no stored kubeconfig yet")

// loadClusterKubeconfig fetches cluster's own stored kubeconfig — empty
// until its owning TalosCluster reconciler has bootstrapped far enough to
// store one (see pkg/domain/taloscluster's bootstrapAndCheckHealth),
// which callers should treat as "not ready yet", not a hard failure.
func loadClusterKubeconfig(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster,
) ([]byte, error) {
	ref := cluster.Status.SecretRef

	var secret corev1.Secret

	err := kubeClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secret to load kubeconfig: %w", ref.Name, err)
	}

	kubeconfig, ok := secret.Data[kubeconfigSecretKey]
	if !ok {
		return nil, fmt.Errorf("%q %w", ref.Name, errKubeconfigNotStored)
	}

	return kubeconfig, nil
}

// controlPlaneMemberCount counts cluster's control-plane pool's claimed,
// Discovered Instances — the same "claimed AND Discovered" filter
// pkg/domain/taloscluster's own resolveMembers uses, duplicated for the
// same import-cycle reason kubeconfigSecretKey is. Only ever used to
// populate the CEL ctx.taloscluster.status.controlPlane.replicas value
// (see celContext) — not a correctness-critical count, just an input to
// addon values resolution.
func controlPlaneMemberCount(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster,
) (int, error) {
	var list v1alpha2.InstanceList

	poolName := cluster.Spec.ControlPlane.PoolRef.Name

	err := kubeClient.List(ctx, &list, client.MatchingLabels{v1alpha2.LabelClaimedBy: poolName})
	if err != nil {
		return 0, fmt.Errorf("failed to list control-plane instances for %q: %w", cluster.Name, err)
	}

	count := 0

	for _, inst := range list.Items {
		if meta.IsStatusConditionTrue(inst.Status.Conditions, instance.DiscoveredConditionType) {
			count++
		}
	}

	return count, nil
}
