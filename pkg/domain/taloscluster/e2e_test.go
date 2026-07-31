package taloscluster_test

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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

// e2eTalosImage is pinned for the same reason
// pkg/domain/instance/talos_e2e_test.go's own e2eTalosImage is — see that
// constant's doc.
const e2eTalosImage = "ghcr.io/siderolabs/talos:v1.13.0"

const (
	e2eContainerStartTimeout = 90 * time.Second
	e2eOverallTimeout        = 12 * time.Minute
	e2eReconcileTick         = 15 * time.Second
	e2eHealthCheckTimeout    = 60 * time.Second
	e2eRetryInterval         = 15 * time.Second
	e2eWorkerJoinTimeout     = 5 * time.Minute
)

// TestE2ETalosClusterBootstrapsAndWorkerJoins is the gated, opt-in
// end-to-end test issue #28's own testable milestone describes: a real
// control-plane node is bootstrapped to healthy, Cilium and cert-manager
// are installed for real against its kubeconfig, and a worker only joins
// once the control plane is healthy. Skipped by default — set
// KONTINUUM_TEST_E2E=1 to run it — since it needs Docker, boots two real
// Talos containers, and pulls real Helm charts and container images from
// the network, unlike every other test in this package. The TestE2E
// prefix is this repo's convention (see .github/workflows/ci.yml's "E2E"
// job) for selecting every gated, Docker-requiring test.
func TestE2ETalosClusterBootstrapsAndWorkerJoins(t *testing.T) {
	t.Parallel()

	if os.Getenv("KONTINUUM_TEST_E2E") == "" {
		t.Skip("set KONTINUUM_TEST_E2E=1 to run this test — it needs Docker and boots real Talos containers")
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eOverallTimeout)
	defer cancel()

	fakeClient, reconciler, req := setupE2ECluster(ctx, t)

	reconcileUntilReady(ctx, t, fakeClient, reconciler, req, e2eOverallTimeout-2*time.Minute)

	var got v1alpha2.TalosCluster

	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ControlPlaneReadyConditionType))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.BootstrappedConditionType))

	kubeconfig := fetchKubeconfig(ctx, t, fakeClient, got.Status.SecretRef)

	assertWorkerJoins(ctx, t, kubeconfig)
}

// reconcileUntilReady calls reconciler.Reconcile sequentially — waiting for
// each call to return before deciding whether to retry — until
// ReadyConditionType is true or timeout elapses. This is deliberately not
// require.Eventually: that helper fires its condition function in a new
// goroutine on every tick regardless of whether the previous one has
// returned yet, and Reconcile here can legitimately block for minutes at a
// time (a real Helm install) — far longer than e2eReconcileTick. Against a
// blocking condition function like that, Eventually ends up running
// multiple overlapping Reconcile calls concurrently: redundant Helm
// installs racing each other, and concurrent access to fakeClient, which
// isn't guaranteed goroutine-safe. A real controller-runtime manager never
// runs two reconciles for the same object concurrently either — this
// loop's strictly sequential shape matches that guarantee.
func reconcileUntilReady(
	ctx context.Context, t *testing.T, fakeClient client.Client, reconciler *taloscluster.Reconciler,
	req ctrl.Request, timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		_, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Logf("reconcile error (may be transient while bootstrapping): %v", err)
		}

		var got v1alpha2.TalosCluster

		getErr := fakeClient.Get(ctx, req.NamespacedName, &got)
		if getErr == nil && meta.IsStatusConditionTrue(got.Status.Conditions, taloscluster.ReadyConditionType) {
			return
		}

		if time.Now().After(deadline) {
			require.Fail(t, "cluster never became ready")

			return
		}

		select {
		case <-ctx.Done():
			require.Fail(t, "context canceled while waiting for cluster to become ready")

			return
		case <-time.After(e2eReconcileTick):
		}
	}
}

// setupE2ECluster boots the two real Talos containers and seeds a fake
// client with the TalosCluster/Instance objects a production Reconciler
// needs to bootstrap them for real.
func setupE2ECluster(ctx context.Context, t *testing.T) (client.Client, *taloscluster.Reconciler, ctrl.Request) {
	t.Helper()

	controlPlaneIP := startTalosContainer(ctx, t)
	workerIP := startTalosContainer(ctx, t)

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-cluster"},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: "cp-pool"},
			},
			Workers: []v1alpha2.TalosClusterWorkerSpec{
				{Name: "default", PoolRef: v1alpha2.InstancePoolReference{Name: "worker-pool"}},
			},
		},
	}

	cpInstance := claimedDiscoveredInstance("cp-node", "cp-pool", controlPlaneIP)
	workerInstance := claimedDiscoveredInstance("worker-node", "worker-pool", workerIP)

	fakeClient := newFakeClient(t, cluster, cpInstance, workerInstance)

	// Debug level so ClusterBootstrapper's HealthCheck logs every
	// intermediate HealthCheckProgress message Talos streams back — the
	// only way to see which specific check (etcd, apiserver, all nodes
	// Ready, ...) is still blocking convergence, instead of just a final
	// context-deadline-exceeded once HealthCheckTimeout elapses.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reconciler := &taloscluster.Reconciler{
		Client:             fakeClient,
		Bootstrapper:       taloscluster.NewTalosBootstrapper(logger),
		AddonInstaller:     taloscluster.NewHelmInstaller(),
		HealthCheckTimeout: e2eHealthCheckTimeout,
		RetryInterval:      e2eRetryInterval,
		Logger:             logger,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "e2e-cluster"}}

	return fakeClient, reconciler, req
}

// fetchKubeconfig reads the kubeconfig reconciler.Reconcile stored on
// ref's Secret.
func fetchKubeconfig(
	ctx context.Context, t *testing.T, fakeClient client.Client, ref v1alpha2.SecretReference,
) []byte {
	t.Helper()

	var secret corev1.Secret

	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}, &secret))

	kubeconfig := secret.Data["kubeconfig"]
	require.NotEmpty(t, kubeconfig)

	return kubeconfig
}

// assertWorkerJoins waits for the worker Node to register against the
// real cluster reachable via kubeconfig — the worker joins on its own once
// Talos applies its config, independent of any further reconciler calls.
func assertWorkerJoins(ctx context.Context, t *testing.T, kubeconfig []byte) {
	t.Helper()

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	require.NoError(t, err)

	clientset, err := kubernetes.NewForConfig(restConfig)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		nodes, listErr := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if listErr != nil {
			return false
		}

		return len(nodes.Items) >= 2
	}, e2eWorkerJoinTimeout, e2eReconcileTick, "worker never joined the cluster")
}

// startTalosContainer boots e2eTalosImage with the same docker run shape
// pkg/domain/instance/talos_e2e_test.go's own startTalosContainer uses —
// duplicated rather than shared/exported, see the implementation plan's
// "Shared test helper" note: both are test-only, gated-by-env-var files,
// and this repo has no shared e2e test-support package today.
func startTalosContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	talosContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e2eTalosImage,
			Tmpfs: map[string]string{"/run": "", "/system": "", "/tmp": ""},
			// apid — the maintenance-mode gRPC listener this test's
			// bootstrapper dials — starts a moment after "entering
			// maintenance service" logs; its own health check is what
			// actually confirms the listener is up.
			WaitingFor: wait.ForLog("service[apid](Running): Health check successful").
				WithStartupTimeout(e2eContainerStartTimeout),
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
