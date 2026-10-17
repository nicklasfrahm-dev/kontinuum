package cli_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/cli"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/fleet"
)

const (
	upVersion = "v0.37.0"
	// upPeerIdentity holds the fleet-wide lease in every test fleet: a
	// zone peer, already running.
	upPeerIdentity = "zone-peer"
	// zoneA and addressA are upFleet's first zone and its one instance.
	zoneA    = "eu-1a"
	addressA = "203.0.113.10"
	upFleet  = `apiVersion: up.kontinuum.sh/v1alpha1
kind: Fleet
metadata:
  name: test
spec:
  version: v0.37.0
  frontend:
    host: kontinuum.example.com
  storage:
    uri: ${env:STORAGE_URI}
  dns:
    domain: example.com
    provider: Cloudflare
    credential:
      CF_API_TOKEN: ${env:CF_TOKEN}
  acme:
    provider: letsencrypt
    providers:
      letsencrypt:
        server: https://acme-v02.api.letsencrypt.org/directory
        email: ops@example.com
  auth:
    oidc:
      issuerURL: https://id.example.com
      adminGroups: [admins]
  zones:
    - region: eu
      zone: 1a
      instances:
        - address: 203.0.113.10
    - region: eu
      zone: 1b
      instances:
        - address: 203.0.113.20
`
)

// errNotMaintenance is fakeProbe's failure.
var errNotMaintenance = errors.New("tls: handshake failure")

// stubLookup resolves hostnames from a table.
type stubLookup map[string][]string

func (s stubLookup) LookupHost(_ context.Context, host string) ([]string, error) {
	addrs, ok := s[host]
	if !ok {
		return nil, errNotMaintenance
	}

	return addrs, nil
}

// fakeHub is an in-process hub backed by a fake client.
type fakeHub struct {
	client client.Client
}

func (h *fakeHub) Client() client.Client { return h.client }
func (h *fakeHub) Identity() string      { return "kontinuum-up-test" }
func (h *fakeHub) Stop() error           { return nil }

// upHarness records how RunUp used its seams.
type upHarness struct {
	mutex       sync.Mutex
	client      client.Client
	controllers []bool
	probed      []string
	failProbe   map[string]bool
}

// newUpHarness builds a fleet whose zones turn Ready as soon as they're
// created, with a zone peer already holding the fleet-wide lease.
func newUpHarness(t *testing.T, objects ...client.Object) *upHarness {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	holder := upPeerIdentity
	renewed := metav1.NewMicroTime(time.Now().Add(time.Hour))
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "zonelock-global", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &renewed},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Zone{}).
		WithObjects(append(objects, lease)...).
		WithInterceptorFuncs(interceptor.Funcs{Create: markZonesReady}).
		Build()

	return &upHarness{client: fakeClient, failProbe: map[string]bool{}}
}

// markZonesReady creates obj, and reports a Zone Ready right away, as if
// the fleet reconciled it.
func markZonesReady(
	ctx context.Context, apiClient client.WithWatch, obj client.Object, opts ...client.CreateOption,
) error {
	err := apiClient.Create(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to create: %w", err)
	}

	zoneObj, isZone := obj.(*v1alpha2.Zone)
	if !isZone {
		return nil
	}

	meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "RegistryJoined",
	})

	err = apiClient.Status().Update(ctx, zoneObj)
	if err != nil {
		return fmt.Errorf("failed to mark zone ready: %w", err)
	}

	return nil
}

func (h *upHarness) deps() cli.UpDeps {
	return cli.UpDeps{
		StartHub: func(_ context.Context, _ *config.Config, controllers bool) (cli.UpHub, error) {
			h.controllers = append(h.controllers, controllers)

			return &fakeHub{client: h.client}, nil
		},
		Probe: func(_ context.Context, address string) error {
			h.mutex.Lock()
			defer h.mutex.Unlock()

			h.probed = append(h.probed, address)
			if h.failProbe[address] {
				return errNotMaintenance
			}

			return nil
		},
		StorageResolver: stubLookup{"db.example.com": {"203.0.113.99"}},
		Sources: fleet.Sources{LookupEnv: func(name string) (string, bool) {
			value, ok := map[string]string{
				"STORAGE_URI": "postgres://kontinuum@db.example.com/kontinuum", "CF_TOKEN": "token",
			}[name]

			return value, ok
		}},
		Version:      upVersion,
		PollInterval: time.Millisecond,
	}
}

// upFlags writes content as the fleet file and returns flags using it.
func upFlags(t *testing.T, content string) cli.UpFlags {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "kontinuum.yml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return cli.UpFlags{
		Files: []string{path}, KubeconfigOut: filepath.Join(dir, "kubeconfig.yaml"), Timeout: 10 * time.Second,
	}
}

func listZoneNames(t *testing.T, apiClient client.Client) []string {
	t.Helper()

	var zones v1alpha2.ZoneList
	require.NoError(t, apiClient.List(t.Context(), &zones))

	names := make([]string, 0, len(zones.Items))
	for _, zoneObj := range zones.Items {
		names = append(names, zoneObj.Name)
	}

	return names
}

func TestUpCreatesANewFleet(t *testing.T) {
	t.Parallel()

	harness := newUpHarness(t)
	flags := upFlags(t, upFleet)

	var out bytes.Buffer
	require.NoError(t, cli.RunUp(t.Context(), &out, flags, harness.deps()))

	assert.ElementsMatch(t, []string{zoneA, "eu-1b"}, listZoneNames(t, harness.client))
	assert.Equal(t, []bool{false, true}, harness.controllers,
		"planned with a read-only hub, converged with one running every controller")
	assert.ElementsMatch(t, []string{addressA, "203.0.113.20"}, harness.probed)

	info, err := os.Stat(flags.KubeconfigOut)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	kubeconfig, err := clientcmd.LoadFromFile(flags.KubeconfigOut)
	require.NoError(t, err)
	assert.Contains(t, kubeconfig.Contexts, "1a.eu.example.com")
	assert.Contains(t, kubeconfig.Contexts, "1b.eu.example.com")
	assert.Equal(t, "https://1a.eu.example.com", kubeconfig.Clusters["1a.eu.example.com"].Server)
	assert.Contains(t, kubeconfig.AuthInfos["oidc"].Exec.Args, "--oidc-issuer-url=https://id.example.com")
}

func TestUpIsIdempotent(t *testing.T) {
	t.Parallel()

	harness := newUpHarness(t)
	flags := upFlags(t, upFleet)

	require.NoError(t, cli.RunUp(t.Context(), &bytes.Buffer{}, flags, harness.deps()))
	require.NoError(t, os.Remove(flags.KubeconfigOut))

	harness.controllers = nil

	var out bytes.Buffer
	require.NoError(t, cli.RunUp(t.Context(), &out, flags, harness.deps()))

	assert.Equal(t, []bool{false}, harness.controllers, "nothing to do starts no controllers")
	assert.Contains(t, out.String(), "Fleet is up to date.")

	_, err := os.Stat(flags.KubeconfigOut)
	assert.True(t, os.IsNotExist(err), "a kubeconfig is only written for a fleet up created")
}

func TestUpDryRunExitCodes(t *testing.T) {
	t.Parallel()

	harness := newUpHarness(t)
	flags := upFlags(t, upFleet)
	flags.DryRun = true

	var out bytes.Buffer

	err := cli.RunUp(t.Context(), &out, flags, harness.deps())

	var exitErr *cli.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 2, exitErr.Code)
	assert.Empty(t, listZoneNames(t, harness.client), "a dry run changes nothing")
	assert.Contains(t, out.String(), "${env:STORAGE_URI}", "the merged fleet is printed with references unexpanded")
	assert.Contains(t, out.String(), "+ create zone eu-1a")

	flags.DryRun = false
	require.NoError(t, cli.RunUp(t.Context(), &bytes.Buffer{}, flags, harness.deps()))

	flags.DryRun = true
	require.NoError(t, cli.RunUp(t.Context(), &bytes.Buffer{}, flags, harness.deps()),
		"a dry run against an up-to-date fleet exits 0")
}

func TestUpRefusesNodesNotInMaintenanceMode(t *testing.T) {
	t.Parallel()

	harness := newUpHarness(t)
	harness.failProbe["203.0.113.20"] = true

	err := cli.RunUp(t.Context(), &bytes.Buffer{}, upFlags(t, upFleet), harness.deps())
	require.ErrorIs(t, err, cli.ErrNodeNotInMaintenance)
	assert.Contains(t, err.Error(), "203.0.113.20")
	assert.Empty(t, listZoneNames(t, harness.client), "one failing check aborts before anything is created")
}

func TestUpRejectsANodeDeclaredTwice(t *testing.T) {
	t.Parallel()

	content := strings.Replace(upFleet, "address: 203.0.113.10", "address: a.example.com", 1)
	content = strings.Replace(content, "address: 203.0.113.20", "address: b.example.com", 1)

	harness := newUpHarness(t)
	deps := harness.deps()
	deps.Resolver = stubLookup{"a.example.com": {addressA}, "b.example.com": {addressA}}

	err := cli.RunUp(t.Context(), &bytes.Buffer{}, upFlags(t, content), deps)
	require.ErrorIs(t, err, cli.ErrDuplicateAddress)
	assert.Contains(t, err.Error(), "zone eu-1a instances[0] (a.example.com)")
	assert.Contains(t, err.Error(), "zone eu-1b instances[0] (b.example.com)")
}

func TestUpRejectsStorageNotInferringDirect(t *testing.T) {
	t.Parallel()

	harness := newUpHarness(t)
	deps := harness.deps()
	deps.StorageResolver = stubLookup{"db.example.com": {"10.0.0.5"}}

	err := cli.RunUp(t.Context(), &bytes.Buffer{}, upFlags(t, upFleet), deps)
	require.ErrorIs(t, err, cli.ErrStorageNotDirect)
	assert.Contains(t, err.Error(), "spec.storage.mode: Direct", "the error names the split-horizon override")
	assert.Empty(t, harness.controllers, "nothing is started")
}

func TestUpPrunesOnlyWithPrune(t *testing.T) {
	t.Parallel()

	extra := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1z", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: "1z"},
	}

	harness := newUpHarness(t, extra)
	flags := upFlags(t, upFleet)

	var out bytes.Buffer
	require.NoError(t, cli.RunUp(t.Context(), &out, flags, harness.deps()))
	assert.Contains(t, listZoneNames(t, harness.client), "eu-1z", "never deleted because a line is missing")
	assert.Contains(t, out.String(), "zone eu-1z runs in the fleet but isn't in kontinuum.yml")

	flags.Prune = true
	require.NoError(t, cli.RunUp(t.Context(), &bytes.Buffer{}, flags, harness.deps()))

	err := harness.client.Get(t.Context(),
		client.ObjectKey{Name: "eu-1z", Namespace: v1alpha2.KontinuumSystemNamespace}, &v1alpha2.Zone{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestUpAgainstAnotherFleetVersionOnlyAppliesZones(t *testing.T) {
	t.Parallel()

	older := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: zoneA, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status:     v1alpha2.KontinuumStatus{Version: "v0.36.0"},
	}

	harness := newUpHarness(t, older)

	var out bytes.Buffer
	require.NoError(t, cli.RunUp(t.Context(), &out, upFlags(t, upFleet), harness.deps()))

	assert.Equal(t, []bool{false}, harness.controllers, "this CLI's controllers never run against another version")
	assert.ElementsMatch(t, []string{zoneA, "eu-1b"}, listZoneNames(t, harness.client))
	assert.Contains(t, out.String(), "fleet runs v0.36.0")
}

func TestUpFileFlagAcceptsCommaSeparatedAndRepeated(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{{"-f", "a.yml,b.yml"}, {"-f", "a.yml", "-f", "b.yml"}, {"--file", "a.yml,b.yml"}} {
		cmd := cli.NewUpCmd()
		require.NoError(t, cmd.ParseFlags(args))

		files, err := cmd.Flags().GetStringSlice("file")
		require.NoError(t, err)
		assert.Equal(t, []string{"a.yml", "b.yml"}, files, args)
	}

	files, err := cli.NewUpCmd().Flags().GetStringSlice("file")
	require.NoError(t, err)
	assert.Equal(t, []string{"kontinuum.yml"}, files, "kontinuum.yaml is never discovered")
}
