package cli_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instancepool"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

// unreachableHost is a loopback address nothing listens on. Port 1
// ("tcpmux") is essentially never bound, so connecting to it fails
// immediately with "connection refused" — the same failure shape a real
// SetupWithManager sees when it runs, as buildManager does, before
// kontinuum's own embedded apiserver listener is bound.
const unreachableHost = "http://127.0.0.1:1"

// setupWithManagerController is the subset of libkapi.Controller each
// domain package's own Controller implements. Mirrors the five
// registrations pkg/cli/serve.go's own *Options functions wire onto the
// real server.
type setupWithManagerController interface {
	SetupWithManager(mgr ctrl.Manager) error
}

// TestSetupWithManagerDoesNotRequireLiveAPIServer guards the invariant
// every domain controller's own SetupWithManager doc already claims:
// SetupWithManager must not talk to the API server, because libkapi's
// buildManager calls it synchronously while building the server — before
// ListenAndServe ever binds kontinuum's own embedded apiserver listener
// (see that function's own doc, and pkg/cli/serve.go's buildServer,
// which runs entirely before the "Kontinuum starting"/ListenAndServe
// step). A controller that violates this (addon.Controller once did, by
// calling mgr.GetFieldIndexer().IndexField(...) directly instead of
// deferring to a lazy watch like ctrl.NewControllerManagedBy(...).For(...)
// does) breaks `make dev` with a "connection refused" error before the
// process ever starts listening. Pointing every controller at a host
// nothing listens on reproduces that failure mode for any of the five,
// without needing a running apiserver at all.
func TestSetupWithManagerDoesNotRequireLiveAPIServer(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	logger := slog.Default()

	controllers := map[string]setupWithManagerController{
		"registry":     registry.NewController(registry.Config{Role: v1alpha2.RoleControlPlane, Logger: logger}),
		"instance":     instance.NewController(instance.Config{Logger: logger}),
		"instancepool": instancepool.NewController(instancepool.Config{Logger: logger}),
		"taloscluster": taloscluster.NewController(taloscluster.Config{Logger: logger}),
		"addon":        addon.NewController(addon.Config{Logger: logger}),
	}

	for name, controller := range controllers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mgr, err := ctrl.NewManager(&rest.Config{Host: unreachableHost}, ctrl.Options{
				Scheme:                 scheme,
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
			})
			require.NoError(t, err)

			done := make(chan error, 1)

			go func() { done <- controller.SetupWithManager(mgr) }()

			select {
			case err := <-done:
				require.NoErrorf(t, err, "%s.SetupWithManager must not fail against an unreachable API "+
					"server — it must defer any API calls until the manager actually starts, not perform "+
					"them eagerly during setup", name)
			case <-time.After(5 * time.Second):
				t.Fatalf("%s.SetupWithManager did not return within 5s — it may be blocking on a live API "+
					"call instead of deferring it", name)
			}
		})
	}
}
