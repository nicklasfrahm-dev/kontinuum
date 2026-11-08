package identityprovider

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

// Definitions lists the CRDs this package ensures, cluster-scoped kind
// first.
func Definitions() []crd.Definition {
	return []crd.Definition{
		{
			Name:         "clusteridentityproviders.kontinuum.sh",
			ManifestFile: "kontinuum.sh_clusteridentityproviders.yaml",
			GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("ClusterIdentityProvider")},
		},
		{
			Name:         "identityproviders.kontinuum.sh",
			ManifestFile: "kontinuum.sh_identityproviders.yaml",
			GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("IdentityProvider")},
		},
	}
}

// EnsureCRDs is a libkapi.PostStartHookFunc that applies both identity
// provider CRDs and waits for each to become Established and discoverable,
// before the controller manager — and so this package's Controller —
// starts.
func EnsureCRDs(ctx context.Context, loopbackConfig *restclient.Config, logger *slog.Logger) error {
	for _, def := range Definitions() {
		err := crd.Ensure(ctx, loopbackConfig, crdconfig.Files, def, logger)
		if err != nil {
			return fmt.Errorf("failed to ensure %s crd: %w", def.Name, err)
		}
	}

	return nil
}
