package taloscluster

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// ResetControlPlane resets every discovered, claimed member of cluster's own
// control-plane pool back to Talos maintenance mode — see pkg/domain/zone's
// teardown, this function's only caller, for why that lives outside this
// package: Zone owns the finalizer sequencing a zone's deletion, but only
// this package can reach cluster's own admin Talos identity (see
// generateConfigs/ensureSecretsBundle).
//
// A cluster that never got far enough to persist a secrets bundle, or whose
// control-plane pool has no discovered members left, has nothing to reset —
// both are reported as success, not an error, since there's no seed node
// left to wipe either way (e.g. a Zone deleted before its TalosCluster ever
// bootstrapped).
func ResetControlPlane(
	ctx context.Context, kubeClient client.Client, bootstrapper ClusterBootstrapper, cluster *v1alpha2.TalosCluster,
) error {
	if cluster.Status.SecretRef.Name == "" {
		return nil
	}

	bundle, err := loadSecretsBundle(ctx, kubeClient, cluster.Status.SecretRef)
	if err != nil {
		return fmt.Errorf("failed to load secrets bundle for %q: %w", cluster.Name, err)
	}

	members, err := resolveMembers(ctx, kubeClient, cluster.Namespace, cluster.Spec.ControlPlane.PoolRef)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		return nil
	}

	// dialAddr is any already-configured, reachable control-plane member —
	// the same "dial one, target each via WithNode" pattern
	// recordTalosVersions already uses, and for the same reason: a member
	// mid-reset can no longer serve its own dial once wiped, but every
	// member shares one admin identity/endpoint set, so the first member
	// still reachable is enough to route every Reset call.
	dialAddr := dialAddress(members[0])

	_, _, talosCfg, err := generateConfigs(bundle, cluster, dialAddr)
	if err != nil {
		return fmt.Errorf("failed to regenerate talosconfig for %q: %w", cluster.Name, err)
	}

	var errs []error

	for _, member := range members {
		err := bootstrapper.Reset(ctx, dialAddr, dialAddress(member), talosCfg)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to reset %q: %w", member.Name, err))
		}
	}

	return errors.Join(errs...)
}
