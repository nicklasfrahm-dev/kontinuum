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

// EnsureBuiltinSeeds creates every built-in's own Addon CR for cluster,
// one per name, only if it doesn't already exist. Deliberately
// create-only — see this package's own doc for why continuously
// re-asserting a desired spec here would fight a user's own edits
// (enabled: false, a customized values, a pinned chart version).
func EnsureBuiltinSeeds(ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster) error {
	for _, releaseName := range builtinAddonNames() {
		err := ensureBuiltinAddonSeed(ctx, kubeClient, cluster, releaseName)
		if err != nil {
			return err
		}
	}

	return nil
}

// ensureBuiltinAddonSeed is EnsureBuiltinSeeds' own per-addon body. The
// seeded spec bakes in this built-in's own default namespace/priority/
// provisioning method explicitly (read once, at creation time, from its
// embedded values/<name>.yaml) rather than leaving spec.namespace/
// spec.lifecycle empty and relying on resolveAddon's own invisible
// fallback — so `kubectl get addon -o yaml` is self-documenting instead
// of requiring a reader to know the built-in fallback exists at all.
// Provisioning.Method in particular MUST be baked in here, not left
// empty: AddonProvisioningSpec.Method carries a
// +kubebuilder:default=HelmUpgradeInstall marker, so the apiserver's own
// CRD defaulting fills the empty field in at admission time, before
// addonMethod's own "empty means fall back to this built-in's own
// default" logic ever runs — leaving it unset here would silently and
// permanently lock every built-in (e.g. gateway-api-crds, whose own
// default is KubectlApply because its CRDs are too large for a Helm
// release Secret) onto HelmUpgradeInstall instead. Chart/Values are
// still left unset: unlike namespace/priority/provisioning, they're not
// something a reader benefits from seeing spelled out redundantly, and
// leaving them unset is what lets resolveAddon's own chart-version
// fallback keep tracking a bumped default (e.g. a chart version bump in
// values/cilium.yaml) after this seed was first created — namespace,
// priority, and provisioning method essentially never change
// post-creation, so baking those in costs nothing.
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

	def, err := loadAddonDefaults(releaseName)
	if err != nil {
		return fmt.Errorf("failed to load built-in defaults for addon %q: %w", releaseName, err)
	}

	priority := def.Lifecycle.Priority

	method := def.Lifecycle.Provisioning.Method
	if method == "" {
		method = v1alpha2.AddonProvisioningMethodHelmUpgradeInstall
	}

	seed.Spec = v1alpha2.AddonSpec{
		TalosClusterRef: v1alpha2.TalosClusterReference{Name: cluster.Name},
		ReleaseName:     releaseName,
		Namespace:       def.Namespace,
		Lifecycle: v1alpha2.AddonLifecycleSpec{
			Priority:     &priority,
			Provisioning: v1alpha2.AddonProvisioningSpec{Method: method},
		},
	}

	// Kubernetes itself refuses an owner reference from a namespaced owner
	// onto a cluster-scoped object ("cluster-scoped resource must not have
	// a namespace-scoped owner") — and cluster is namespaced (see issue
	// #63's architecture) while Addon stays cluster-scoped (see
	// addonResourceName's own doc), so native GC-based cleanup can only be
	// wired up once Addon itself moves to namespaced scope too. Until
	// then, skip it rather than fail seeding entirely: TalosClusterRef.Name
	// (see ListForCluster) is already how every reconciler finds an addon's
	// owning cluster, so nothing here actually depends on the owner
	// reference besides GC.
	if cluster.Namespace == "" {
		err = controllerutil.SetControllerReference(cluster, seed, kubeClient.Scheme())
		if err != nil {
			return fmt.Errorf("failed to set owner reference on addon %q: %w", seed.Name, err)
		}
	}

	err = kubeClient.Create(ctx, seed)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create addon %q: %w", seed.Name, err)
	}

	return nil
}

// ListForCluster returns every Addon referencing clusterName — the only
// way to discover a TalosCluster's own addons now that TalosCluster.spec
// carries no addon list of its own. Lists every Addon and filters
// client-side rather than through a cache field index: Controller.
// SetupWithManager runs before the server's own listener is bound (see its
// own doc), and registering a field index there would force an immediate
// discovery call against that not-yet-listening server. Addon counts per
// cluster are small, so the extra client-side pass costs nothing that
// matters.
func ListForCluster(ctx context.Context, kubeClient client.Client, clusterName string) (v1alpha2.AddonList, error) {
	var all v1alpha2.AddonList

	err := kubeClient.List(ctx, &all)
	if err != nil {
		return v1alpha2.AddonList{}, fmt.Errorf("failed to list addons for %q: %w", clusterName, err)
	}

	matched := v1alpha2.AddonList{}

	for _, item := range all.Items {
		if item.Spec.TalosClusterRef.Name == clusterName {
			matched.Items = append(matched.Items, item)
		}
	}

	return matched, nil
}
