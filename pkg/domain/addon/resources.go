package addon

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// addonResourceName derives an Addon CR's own name from its owning
// cluster and release name — mirrors taloscluster's own secretNamePrefix
// convention (Addon is cluster-scoped like TalosCluster itself, so two
// clusters' same-named addons would otherwise collide).
func addonResourceName(clusterName, releaseName string) string {
	return clusterName + "-" + releaseName
}

// EnsureBuiltinSeeds creates cilium's and cert-manager's Addon CRs for
// cluster with a minimal spec (TalosClusterRef + ReleaseName only —
// Chart/Namespace/Values left unset, so resolveAddon's own built-in
// fallback applies), one per name, only if each doesn't already exist.
// Deliberately create-only — see this package's own doc for why
// continuously re-asserting a desired spec here would fight a user's own
// edits (enabled: false, a customized values, a pinned chart version).
func EnsureBuiltinSeeds(ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster) error {
	for _, releaseName := range builtinAddonNames() {
		err := ensureBuiltinAddonSeed(ctx, kubeClient, cluster, releaseName)
		if err != nil {
			return err
		}
	}

	return nil
}

// ensureBuiltinAddonSeed is EnsureBuiltinSeeds' own per-addon body.
func ensureBuiltinAddonSeed(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster, releaseName string,
) error {
	seed := &v1alpha2.Addon{ObjectMeta: metav1.ObjectMeta{Name: addonResourceName(cluster.Name, releaseName)}}

	err := kubeClient.Get(ctx, client.ObjectKeyFromObject(seed), seed)
	if err == nil {
		return nil // already exists — hands off, whoever owns it now
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing addon %q: %w", seed.Name, err)
	}

	seed.Spec = v1alpha2.AddonSpec{
		TalosClusterRef: v1alpha2.TalosClusterReference{Name: cluster.Name},
		ReleaseName:     releaseName,
	}

	err = controllerutil.SetControllerReference(cluster, seed, kubeClient.Scheme())
	if err != nil {
		return fmt.Errorf("failed to set owner reference on addon %q: %w", seed.Name, err)
	}

	err = kubeClient.Create(ctx, seed)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create addon %q: %w", seed.Name, err)
	}

	return nil
}

// ListForCluster returns every Addon referencing clusterName — the only
// way to discover a TalosCluster's own addons now that TalosCluster.spec
// carries no addon list of its own. Relies on the field index
// Controller.SetupWithManager registers on the manager's cached client; a
// caller using a client without that index registered (e.g. a hand-built
// fake in a test lacking WithIndex) gets a clear error, not silent empty
// results.
func ListForCluster(ctx context.Context, kubeClient client.Client, clusterName string) (v1alpha2.AddonList, error) {
	var addons v1alpha2.AddonList

	err := kubeClient.List(ctx, &addons, client.MatchingFields{TalosClusterRefField: clusterName})
	if err != nil {
		return v1alpha2.AddonList{}, fmt.Errorf("failed to list addons for %q: %w", clusterName, err)
	}

	return addons, nil
}
