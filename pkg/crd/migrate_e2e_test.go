package crd_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/kommodity-io/kommodity/pkg/libkapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	crdconfig "github.com/nicklasfrahm/kontinuum/config/crd"
	"github.com/nicklasfrahm/kontinuum/pkg/crd"
)

const (
	e2eShutdownTimeout   = 10 * time.Second
	e2eHealthzTimeout    = 10 * time.Second
	e2eHealthzInterval   = 50 * time.Millisecond
	e2eEstablishTimeout  = 10 * time.Second
	e2eEstablishInterval = 50 * time.Millisecond
)

// zoneDefinition is the real, current (Namespaced) zones.kontinuum.sh
// definition — the same one pkg/domain/instance.EnsureCRDs applies.
func zoneDefinition() crd.Definition {
	return crd.Definition{
		Name:         "zones.kontinuum.sh",
		ManifestFile: "kontinuum.sh_zones.yaml",
		GVKs:         []schema.GroupVersionKind{v1alpha2.GroupVersion().WithKind("Zone")},
	}
}

// TestMigrateScopeRecreatesExistingObjectsNamespaced is the regression test
// for issue #63's CRD-scope migration: it simulates an existing install by
// applying zones.kontinuum.sh as Cluster-scoped (what every kontinuum
// release before this one shipped) and creating a cluster-scoped Zone
// object directly, then drives the exact same migrate-ensure-restore
// sequence pkg/domain/instance.EnsureCRDs now runs on every startup, and
// asserts the CRD ends up Namespaced with the object recreated, spec and
// status intact, in kontinuum-system.
func TestMigrateScopeRecreatesExistingObjectsNamespaced(t *testing.T) {
	t.Parallel()

	if raceEnabled {
		// Mirrors pkg/domain/registry and pkg/domain/instance's identical
		// e2e skips: booting a real backing store trips a pre-existing,
		// unrelated data race inside the vendored github.com/k3s-io/kine.
		t.Skip("triggers a pre-existing, unrelated data race in github.com/k3s-io/kine under -race")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := slog.Default()
	def := zoneDefinition()

	const zoneName = "eu-eu-1a"

	// hookDone carries migrate's own outcome across goroutines — a channel,
	// not a plain var, since ListenAndServe runs migrate on its own
	// goroutine and the assertions below run on this test's goroutine, with
	// no other synchronization between the two.
	hookDone := make(chan error, 1)
	migrate := newMigrateHook(def, zoneName, logger, hookDone)

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	addr := freeAddr(ctx, t)
	dbPath := filepath.Join(t.TempDir(), "migrate-e2e.db")

	server, err := libkapi.New(ctx,
		libkapi.WithAddr(addr),
		libkapi.WithStorage("sqlite://"+dbPath),
		libkapi.WithLogger(logger),
		libkapi.WithScheme(scheme),
		libkapi.WithPostStartHook(migrate),
	)
	require.NoError(t, err)

	go func() { _ = server.ListenAndServe(ctx) }()

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), e2eShutdownTimeout)
		defer shutdownCancel()

		_ = server.Shutdown(shutdownCtx)
	})

	baseURL := "http://" + addr

	// /healthz becomes ready as soon as Serve's Accept loop is running —
	// before, not after, ListenAndServe's own synchronous WithPostStartHook
	// run (see libkapi.Server.ListenAndServe's doc: internal hooks start
	// first so a post-start hook can depend on them, then post-start hooks
	// run, then the manager). So healthz alone doesn't prove migrate has
	// finished — only that it's safe to start polling for its result.
	waitForHealthz(ctx, t, baseURL+"/healthz")

	select {
	case hookErr := <-hookDone:
		require.NoError(t, hookErr)
	case <-time.After(e2eHealthzTimeout):
		t.Fatal("the post-start hook never finished")
	}

	assertMigrated(ctx, t, baseURL, scheme, def, zoneName)
}

// newMigrateHook builds the libkapi.PostStartHookFunc
// TestMigrateScopeRecreatesExistingObjectsNamespaced runs: seed a legacy
// cluster-scoped zoneName, then drive the exact migrate-ensure-restore
// sequence pkg/domain/instance.EnsureCRDs runs on every real startup. Its
// own outcome is reported on hookDone — see that test's own doc for why a
// channel, not a plain var.
func newMigrateHook(
	def crd.Definition, zoneName string, logger *slog.Logger, hookDone chan<- error,
) libkapi.PostStartHookFunc {
	return func(ctx context.Context, loopbackConfig *restclient.Config) error {
		err := seedClusterScopedZone(ctx, loopbackConfig, def, zoneName, logger)
		if err != nil {
			err = fmt.Errorf("seed: %w", err)
			hookDone <- err

			return err
		}

		migrated, err := crd.MigrateScope(ctx, loopbackConfig, crdconfig.Files, def, logger)
		if err != nil {
			err = fmt.Errorf("migrate: %w", err)
			hookDone <- err

			return err
		}

		err = crd.Ensure(ctx, loopbackConfig, crdconfig.Files, def, logger)
		if err != nil {
			err = fmt.Errorf("ensure: %w", err)
			hookDone <- err

			return err
		}

		err = crd.RestoreMigrated(ctx, loopbackConfig, migrated, v1alpha2.DefaultSecretNamespace, logger)
		if err != nil {
			err = fmt.Errorf("restore: %w", err)
		}

		hookDone <- err

		return err
	}
}

// assertMigrated asserts def's crd ended up Namespaced and zoneName was
// recreated in kontinuum-system with its spec and status intact — the tail
// of TestMigrateScopeRecreatesExistingObjectsNamespaced, factored out
// purely to keep that function under this repo's statement-count limit.
func assertMigrated(
	ctx context.Context, t *testing.T, baseURL string, scheme *runtime.Scheme, def crd.Definition, zoneName string,
) {
	t.Helper()

	kubeClient, err := ctrlclient.New(&restclient.Config{Host: baseURL}, ctrlclient.Options{Scheme: scheme})
	require.NoError(t, err)

	apiextensionsClient, err := apiextensionsclientset.NewForConfig(&restclient.Config{Host: baseURL})
	require.NoError(t, err)

	var migratedZone v1alpha2.Zone

	require.Eventually(t, func() bool {
		key := ctrlclient.ObjectKey{Name: zoneName, Namespace: v1alpha2.DefaultSecretNamespace}

		return kubeClient.Get(ctx, key, &migratedZone) == nil
	}, e2eHealthzTimeout, e2eHealthzInterval, "migrated zone was never recreated in kontinuum-system")

	crdObj, err := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions().
		Get(ctx, def.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, apiextensionsv1.NamespaceScoped, crdObj.Spec.Scope,
		"the crd must end up Namespaced, not still (or again) Cluster-scoped")

	assert.Equal(t, "eu", migratedZone.Spec.Region)
	assert.Equal(t, "eu-1a", migratedZone.Spec.Zone)
	assert.Equal(t, "kontinuum.example.com", migratedZone.Spec.Domain)
	require.Len(t, migratedZone.Status.Conditions, 1)
	assert.Equal(t, "ClusterReady", migratedZone.Status.Conditions[0].Type,
		"migration must restore status, not just spec")
}

// seedClusterScopedZone applies def as Cluster-scoped — deliberately
// diverging from its own generated (Namespaced) manifest, to simulate
// exactly what an existing install's already-applied CRD looks like before
// this release ever ran — waits for it to establish, then creates one
// cluster-scoped Zone with a status already set, standing in for real
// pre-migration data.
func seedClusterScopedZone(
	ctx context.Context, loopbackConfig *restclient.Config, def crd.Definition, zoneName string, logger *slog.Logger,
) error {
	legacy := crd.Build(crdconfig.Files, def)
	legacy.Spec.Scope = apiextensionsv1.ClusterScoped

	apiextensionsClient, err := apiextensionsclientset.NewForConfig(loopbackConfig)
	if err != nil {
		return fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	crds := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions()

	_, err = crds.Create(ctx, legacy, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create legacy cluster-scoped crd: %w", err)
	}

	err = waitEstablished(ctx, crds, def.Name, logger)
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()

	err = v1alpha2.AddToScheme(scheme)
	if err != nil {
		return fmt.Errorf("failed to build scheme: %w", err)
	}

	kubeClient, err := ctrlclient.New(loopbackConfig, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to build client: %w", err)
	}

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: zoneName},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: "eu-1a", Domain: "kontinuum.example.com"},
	}

	err = kubeClient.Create(ctx, zoneObj)
	if err != nil {
		return fmt.Errorf("failed to create legacy cluster-scoped zone: %w", err)
	}

	zoneObj.Status.Conditions = []metav1.Condition{{
		Type: "ClusterReady", Status: metav1.ConditionTrue, Reason: "ClusterReady",
		Message: "talos cluster is ready", LastTransitionTime: metav1.Now(),
	}}

	err = kubeClient.Status().Update(ctx, zoneObj)
	if err != nil {
		return fmt.Errorf("failed to set legacy zone status: %w", err)
	}

	return nil
}

// waitEstablished polls until name's CRD reports Established — a bespoke,
// unexported-free copy of what pkg/crd's own ensureEstablished does
// internally, needed here since this test seeds a CRD pkg/crd's own Ensure
// deliberately doesn't own yet (see seedClusterScopedZone's doc).
func waitEstablished(
	ctx context.Context, crds interface {
		Get(ctx context.Context, name string, opts metav1.GetOptions) (*apiextensionsv1.CustomResourceDefinition, error)
	}, name string, logger *slog.Logger,
) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, e2eEstablishTimeout)
	defer cancel()

	err := wait.PollUntilContextTimeout(timeoutCtx, e2eEstablishInterval, e2eEstablishTimeout, true,
		func(ctx context.Context) (bool, error) {
			crdObj, err := crds.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				logger.Warn("waiting for legacy crd to establish", "crd", name, "error", err)

				return false, nil
			}

			for _, cond := range crdObj.Status.Conditions {
				if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
					return true, nil
				}
			}

			return false, nil
		})
	if err != nil {
		return fmt.Errorf("legacy crd never became established: %w", err)
	}

	return nil
}

// freeAddr picks an available loopback address by binding to port 0 and
// immediately releasing it — mirrors pkg/domain/instance's identical
// helper.
func freeAddr(ctx context.Context, t *testing.T) string {
	t.Helper()

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	require.NoError(t, listener.Close())

	return addr
}

// waitForHealthz polls url until it returns 200 OK or ctx/the timeout
// expires — mirrors pkg/domain/instance's identical helper.
func waitForHealthz(ctx context.Context, t *testing.T, url string) {
	t.Helper()

	httpClient := &http.Client{Timeout: e2eHealthzInterval}

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return false
		}

		defer func() { _ = resp.Body.Close() }()

		return resp.StatusCode == http.StatusOK
	}, e2eHealthzTimeout, e2eHealthzInterval, "server never became healthy")
}
