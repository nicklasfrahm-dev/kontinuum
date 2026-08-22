package instance_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

// e2eTalosImage is pinned to a version verified (empirically, against a
// real container run in this repo's own development environment) to
// support the maintenance-mode Version RPC — older Talos releases (e.g.
// v1.9.0) run maintenance mode too, but reply "Unimplemented" to Version
// there; discoverInterfaces' COSI-based reads work on every version tried.
// Bump this alongside github.com/siderolabs/talos/pkg/machinery when
// updating that dependency.
const e2eTalosImage = "ghcr.io/siderolabs/talos:v1.13.0"

const e2eContainerStartTimeout = 60 * time.Second

// TestE2EInstanceDiscoversRealTalosContainer is the gated, opt-in
// end-to-end test the testable milestone in issue #27 describes: pointing
// a real Instance at a real (here, container-mode) maintenance-mode Talos
// node and confirming status.interfaces/status.talosVersion populate and
// Discovered flips true. It's skipped by default — set KONTINUUM_TEST_E2E=1
// to run it — since it needs Docker and pulls/boots a real Talos image,
// unlike every other test in this package. The TestE2E prefix is this
// repo's convention (see .github/workflows/ci.yml's "E2E" job) for
// selecting every gated, Docker-requiring test via `go test -run '^TestE2E'`
// without CI needing to name each one individually as they're added.
//
// The container is started with the same docker run shape
// `talosctl cluster create docker`'s own provisioner uses for an
// unconfigured (pre-machineconfig) node: privileged, host cgroup
// namespace, tmpfs for /run|/system|/tmp, and anonymous volumes for the
// directories Talos's container-mode init expects to be writable. No port
// mapping is used — the container's own bridge-network IP is dialed
// directly on the fixed maintenance-mode port (see instance's
// maintenanceModePort), the same way Docker's default bridge network makes
// any container reachable from the host.
func TestE2EInstanceDiscoversRealTalosContainer(t *testing.T) {
	t.Parallel()

	if os.Getenv("KONTINUUM_TEST_E2E") == "" {
		t.Skip("set KONTINUUM_TEST_E2E=1 to run this test — it needs Docker and boots a real Talos container")
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eContainerStartTimeout)
	defer cancel()

	containerIP := startTalosContainer(ctx, t)

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "real-talos-node"},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{containerIP}},
	}

	fakeClient := newFakeClient(t, obj)

	reconciler := &instance.Reconciler{
		Client:        fakeClient,
		Discoverer:    instance.NewTalosDiscoverer(),
		DialTimeout:   10 * time.Second,
		RetryInterval: testRetryInterval,
		Locker:        zonelease.NewLocker(fakeClient, "test-hub", "", 0),
		Logger:        slog.Default(),
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "real-talos-node"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(obj), &got))
	assert.NotEmpty(t, got.Status.Talos.Version)
	assert.NotEmpty(t, got.Status.Interfaces)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType))
}

// startTalosContainer boots e2eTalosImage with the same docker run shape
// `talosctl cluster create docker`'s own provisioner uses for an
// unconfigured (pre-machineconfig) node: privileged, host cgroup
// namespace, tmpfs for /run|/system|/tmp, and anonymous volumes for the
// directories Talos's container-mode init expects to be writable. No port
// mapping is used — the container's own bridge-network IP (returned here)
// is dialed directly on the fixed maintenance-mode port (see instance's
// maintenanceModePort), the same way Docker's default bridge network makes
// any container reachable from the host. The container is torn down via
// t.Cleanup.
func startTalosContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	talosContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e2eTalosImage,
			Tmpfs: map[string]string{"/run": "", "/system": "", "/tmp": ""},
			// apid — the maintenance-mode gRPC listener the discoverer
			// dials — starts a moment after "entering maintenance service"
			// logs; its own health check is what actually confirms the
			// listener is up.
			WaitingFor: wait.ForLog("service[apid](Running): Health check successful"),
			HostConfigModifier: func(hostConfig *container.HostConfig) {
				hostConfig.Privileged = true
				hostConfig.CgroupnsMode = container.CgroupnsModeHost
				hostConfig.SecurityOpt = []string{"seccomp=unconfined", "apparmor=unconfined"}
				hostConfig.Mounts = []mount.Mount{
					{Type: mount.TypeVolume, Target: "/system/state"},
					{Type: mount.TypeVolume, Target: "/var"},
					{Type: mount.TypeVolume, Target: "/etc/cni"},
					{Type: mount.TypeVolume, Target: "/etc/kubernetes"},
					{Type: mount.TypeVolume, Target: "/usr/libexec/kubernetes"},
					{Type: mount.TypeVolume, Target: "/opt"},
				}
			},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, talosContainer.Terminate(context.WithoutCancel(ctx)))
	})

	containerIP, err := talosContainer.ContainerIP(ctx)
	require.NoError(t, err)

	return containerIP
}
