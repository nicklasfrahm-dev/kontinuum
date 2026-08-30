package fabricmanager

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// newInClusterConfig builds a *rest.Config against the cluster this
// process is itself running in (see rest.InClusterConfig) — the zone's
// own downstream kontinuum-server, whose apiserver is backed by the same
// shared storage the hub's own Fabric objects live in (see
// pkg/domain/etcdproxy's own relay, and architecture.md's own Storage
// section), so Fabric objects the hub writes are visible here without any
// direct network path back to the hub itself. Authenticated with
// whatever ServiceAccount token Kubernetes projects into this pod, scoped
// via that ServiceAccount's own Role/RoleBinding — see
// pkg/domain/zone.ensureFabricManagerRBAC's own doc — to exactly Fabric
// get/list/watch and fabrics/status update. Mirrors
// pkg/domain/etcdproxy.NewInClusterIdentityWatcher's identical
// rest.InClusterConfig call.
func newInClusterConfig() (*rest.Config, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build in-cluster config: %w", err)
	}

	return restCfg, nil
}

// fabricScheme registers exactly the one Kind this package's own
// controller-runtime manager ever needs — kontinuum.sh/v1alpha2's Fabric —
// mirroring pkg/domain/etcdproxy.NewInClusterIdentityWatcher's own
// single-Kind scheme.
func fabricScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	err := v1alpha2.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register kontinuum.sh/v1alpha2 scheme: %w", err)
	}

	return scheme, nil
}
