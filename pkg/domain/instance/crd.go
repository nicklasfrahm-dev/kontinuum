// Package instance is the zone-join build-out's first domain package (see
// issue #24's architecture, phase 1 in issue #27): it ensures the four new
// CRDs (Zone, Instance, InstancePool, TalosCluster) exist on the hub
// apiserver, and reconciles Instance's maintenance-mode discovery. None of
// the other three kinds have a controller yet — later phases split their
// own CRD-ensure and reconciler out of here once they get one, the same way
// pkg/domain/registry owns Kontinuum's.
package instance

import (
	"context"
	"fmt"
	"log/slog"

	"k8s.io/apimachinery/pkg/runtime/schema"
	restclient "k8s.io/client-go/rest"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	crdconfig "github.com/nicklasfrahm/kontinuum/config/crd"
	"github.com/nicklasfrahm/kontinuum/pkg/crd"
)

// definitions lists every CRD this package ensures. None carry a
// crd.ConversionWebhook — unlike Kontinuum (see pkg/domain/registry), these
// four kinds have no prior released version to convert from.
func definitions() []crd.Definition {
	return []crd.Definition{
		{
			Name:         "zones.kontinuum.sh",
			ManifestFile: "kontinuum.sh_zones.yaml",
			GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("Zone")},
		},
		{
			Name:         "instances.kontinuum.sh",
			ManifestFile: "kontinuum.sh_instances.yaml",
			GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("Instance")},
		},
		{
			Name:         "instancepools.kontinuum.sh",
			ManifestFile: "kontinuum.sh_instancepools.yaml",
			GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("InstancePool")},
		},
		{
			Name:         "talosclusters.kontinuum.sh",
			ManifestFile: "kontinuum.sh_talosclusters.yaml",
			GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("TalosCluster")},
		},
	}
}

// EnsureCRDs is a libkapi.PostStartHookFunc — see its registration in
// pkg/cli/serve.go, and registry.EnsureCRD's doc for the timing this relies
// on (loopbackConfig is only reachable once libkapi's post-start hooks run,
// before the controller manager starts). It applies all four CRDs and
// waits for each to become Established and discoverable, in the order
// listed by definitions — Zone before the kinds that will eventually
// reference it, so a partial failure fails on the first, most foundational
// kind rather than a downstream one.
func EnsureCRDs(ctx context.Context, loopbackConfig *restclient.Config, logger *slog.Logger) error {
	for _, def := range definitions() {
		err := crd.Ensure(ctx, loopbackConfig, crdconfig.Files, def, logger)
		if err != nil {
			return fmt.Errorf("failed to ensure %s crd: %w", def.Name, err)
		}
	}

	return nil
}
