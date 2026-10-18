package cli_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/e2e"
)

// The kontinuum up end-to-end test. Zones run their own kontinuum-server
// from upE2EImage, so that image must hold the code under test: push the
// working tree's image first with `VERSION=dev make image-push`. The test
// skips, rather than test stale code, unless the pushed image was built
// from the commit checked out — see requireImageOfHead.
//
// No apiserver runs inside this test binary: up's in-process hub embeds
// Kine, whose SQL backend has data races of its own that -race would
// report. Every up runs from upE2EImage in a container instead, and the
// test reads the fleet through its zone peers' own API.

const (
	upE2EImage    = "ghcr.io/nicklasfrahm-dev/kontinuum:dev"
	upE2EDomain   = "e2e.test"
	upE2EFrontend = "app." + upE2EDomain
	upE2EKeyName  = "kontinuum."
	// upE2EKey is a throwaway TSIG key, valid only for this test's BIND.
	upE2EKey = "a29udGludXVtLXVwLWUyZS10c2lnLWtleS10ZXN0cw=="
	// upE2ETimeout bounds the whole test.
	upE2ETimeout = 55 * time.Minute
	// upE2EConvergeTimeout bounds one converging up run.
	upE2EConvergeTimeout = 30 * time.Minute
	// upE2EPollInterval is how often assertions re-check the fleet.
	upE2EPollInterval = 10 * time.Second
	// upE2ERevisionLabel is the label make image records the commit in.
	upE2ERevisionLabel = "org.opencontainers.image.revision"
	// upE2EStopTimeout is how long a stopped container may take to exit.
	upE2EStopTimeout = 30 * time.Second
	// upE2EKubeconfigPath is where an up container writes its kubeconfig.
	upE2EKubeconfigPath = "/tmp/kubeconfig.yaml"
)

// upE2EZone is one zone of the test fleet and its Talos nodes.
type upE2EZone struct {
	zone      string
	addresses []string
}

// host is the zone's own hostname.
func (z upE2EZone) host() string {
	return z.zone + ".eu." + upE2EDomain
}

// upE2EInfra is the test's shared infrastructure.
type upE2EInfra struct {
	storageURI string
	bindIP     string
	pebbleIP   string
}

// TestE2EUpConvergesADirectModeFleet runs kontinuum up end to end:
//
//   - an up killed midway, after creating the zones, is resumed by the next
//     run, which converges every zone to Ready on a shared PostgreSQL and
//     waits for a zone peer to take over fleet-wide work;
//   - running up again is a no-op with exit code 0;
//   - every zone's certificate covers its own hostname and the frontend
//     host, issued via DNS-01, the frontend host resolves to every zone, and
//     a zone hostname's UI redirects to it;
//   - adding a zone with three instances to the file and running two ups at
//     once adds it, with a three-member control plane.
func TestE2EUpConvergesADirectModeFleet(t *testing.T) {
	t.Parallel()
	e2e.Skip(t)
	requireImageOfHead(t)

	ctx, cancel := context.WithTimeout(t.Context(), upE2ETimeout)
	defer cancel()

	infra := startUpE2EInfra(ctx, t)

	zones := make([]upE2EZone, 0, 3)
	zones = append(zones,
		upE2EZone{zone: "1a", addresses: []string{e2e.StartTalos(ctx, t)}},
		upE2EZone{zone: "1b", addresses: []string{e2e.StartTalos(ctx, t)}})

	fleetFile := writeUpE2EFleet(t, t.TempDir(), infra, zones)

	seedThroughServe(ctx, t, infra, zones)

	killed := startUp(ctx, t, fleetFile, infra, wait.ForLog("zone eu-1a: ").WithStartupTimeout(upE2EConvergeTimeout))
	kill := time.Duration(0)
	require.NoError(t, killed.Stop(ctx, &kill), "up is killed once it waits on the zones it created")

	resumed := startUp(ctx, t, fleetFile, infra, untilExit())
	output := requireSuccess(ctx, t, resumed)
	assert.Contains(t, output, "isn't Ready yet: resuming", "the killed run is resumed")

	kubeconfig := copyKubeconfig(ctx, t, resumed)
	assert.Len(t, kubeconfig.Contexts, len(zones), "the resumed creation writes the kubeconfig")

	output = requireSuccess(ctx, t, startUp(ctx, t, fleetFile, infra, untilExit()))
	assert.Contains(t, output, "Fleet is up to date.", "a second run is a no-op")

	requireSuccess(ctx, t, startUp(ctx, t, fleetFile, infra, untilExit(), "--dry-run"))

	peer := peerClient(t, zones[0])
	assertFrontendHost(ctx, t, peer, infra, zones)

	zones = append(zones, upE2EZone{zone: "1c", addresses: []string{
		e2e.StartTalos(ctx, t), e2e.StartTalos(ctx, t), e2e.StartTalos(ctx, t),
	}})
	fleetFile = writeUpE2EFleet(t, t.TempDir(), infra, zones)
	seedCertManager(ctx, t, peer, infra, zones[2:])

	first := startUp(ctx, t, fleetFile, infra, wait.ForLog("+ create zone eu-1c"))
	second := startUp(ctx, t, fleetFile, infra, untilExit())
	requireSuccess(ctx, t, second)
	requireSuccess(ctx, t, first)

	assertControlPlaneMembers(ctx, t, peer, "eu-1c", 3)
}

// requireImageOfHead skips unless upE2EImage was built from the commit
// checked out.
func requireImageOfHead(t *testing.T) {
	t.Helper()

	head, err := exec.CommandContext(t.Context(), "git", "rev-parse", "HEAD").Output()
	require.NoError(t, err)

	pull := exec.CommandContext(t.Context(), "docker", "pull", "--quiet", upE2EImage)
	pull.Stdout, pull.Stderr = io.Discard, io.Discard
	require.NoError(t, pull.Run())

	revision, err := exec.CommandContext(t.Context(), "docker", "image", "inspect", "--format",
		`{{index .Config.Labels "`+upE2ERevisionLabel+`"}}`, upE2EImage).Output()
	require.NoError(t, err)

	if strings.TrimSpace(string(revision)) != strings.TrimSpace(string(head)) {
		t.Skipf("%s was built from %q, not HEAD %s: push this commit's image with `VERSION=dev make image-push`",
			upE2EImage, strings.TrimSpace(string(revision)), strings.TrimSpace(string(head)))
	}
}

// startUpE2EInfra starts the shared database, BIND and Pebble.
func startUpE2EInfra(ctx context.Context, t *testing.T) upE2EInfra {
	t.Helper()

	infra := upE2EInfra{storageURI: e2e.StartPostgres(ctx, t)}
	infra.bindIP = e2e.StartBind(ctx, t, upE2EDomain, upE2EKeyName, upE2EKey)
	infra.pebbleIP = e2e.StartPebble(ctx, t, infra.bindIP)

	return infra
}

// upE2EEnv is what the fleet's references expand to.
func upE2EEnv(infra upE2EInfra) map[string]string {
	return map[string]string{
		"E2E_STORAGE_URI": infra.storageURI,
		"E2E_BIND_HOST":   infra.bindIP,
		"E2E_BIND_PORT":   "53",
		"E2E_DNS_ZONE":    upE2EDomain,
		"E2E_TSIG_NAME":   upE2EKeyName,
		"E2E_TSIG_KEY":    upE2EKey,
		"E2E_TSIG_ALG":    "hmac-sha256",
		"E2E_TSIG_AXFR":   "true",
	}
}

// writeUpE2EFleet writes the test fleet's kontinuum.yml into dir.
func writeUpE2EFleet(t *testing.T, dir string, infra upE2EInfra, zones []upE2EZone) string {
	t.Helper()

	var zoneList strings.Builder

	for _, zone := range zones {
		fmt.Fprintf(&zoneList, "    - region: eu\n      zone: %s\n      instances:\n", zone.zone)

		for _, address := range zone.addresses {
			fmt.Fprintf(&zoneList, "        - address: %s\n", address)
		}
	}

	content := fmt.Sprintf(`apiVersion: up.kontinuum.sh/v1alpha1
kind: Fleet
metadata:
  name: e2e
spec:
  version: dev
  frontend:
    host: %[1]s
  storage:
    mode: Direct
    uri: ${env:E2E_STORAGE_URI}
  dns:
    domain: %[2]s
    provider: RFC2136
    credential:
      EXTERNAL_DNS_RFC2136_HOST: ${env:E2E_BIND_HOST}
      EXTERNAL_DNS_RFC2136_PORT: ${env:E2E_BIND_PORT}
      EXTERNAL_DNS_RFC2136_ZONE: ${env:E2E_DNS_ZONE}
      EXTERNAL_DNS_RFC2136_TSIG_KEYNAME: ${env:E2E_TSIG_NAME}
      EXTERNAL_DNS_RFC2136_TSIG_SECRET: ${env:E2E_TSIG_KEY}
      EXTERNAL_DNS_RFC2136_TSIG_SECRET_ALG: ${env:E2E_TSIG_ALG}
      EXTERNAL_DNS_RFC2136_TSIG_AXFR: ${env:E2E_TSIG_AXFR}
  acme:
    provider: pebble
    providers:
      pebble:
        server: https://%[3]s:14000/dir
        email: ops@%[2]s
        insecureSkipVerify: true
  auth:
    insecureAllowAnonymous: true
  zones:
%[4]s`, upE2EFrontend, upE2EDomain, infra.pebbleIP, zoneList.String())

	path := filepath.Join(dir, "kontinuum.yml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

// untilExit waits for an up container to exit, however long converging
// takes.
func untilExit() *wait.ExitStrategy {
	return wait.ForExit().WithExitTimeout(upE2EConvergeTimeout)
}

// startUp runs kontinuum up, with args, from upE2EImage in a container of
// its own — a separate process with its own network namespace, like an up
// on another machine — against fleetFile, returning once waitFor is met.
func startUp(
	ctx context.Context, t *testing.T, fleetFile string, infra upE2EInfra, waitFor wait.Strategy, args ...string,
) *testcontainers.DockerContainer {
	t.Helper()

	content, err := os.ReadFile(filepath.Clean(fleetFile))
	require.NoError(t, err)

	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      upE2EImage,
			Entrypoint: []string{"/app/kontinuum"},
			Cmd: append([]string{
				"up", "-f", "/fleet/kontinuum.yml", "--kubeconfig-out", upE2EKubeconfigPath,
				"--timeout", upE2EConvergeTimeout.String(),
			}, args...),
			Env: upE2EEnv(infra),
			Files: []testcontainers.ContainerFile{
				{Reader: bytes.NewReader(content), ContainerFilePath: "/fleet/kontinuum.yml", FileMode: 0o644},
			},
			WaitingFor: waitFor,
		},
	})
	if started != nil {
		t.Cleanup(func() { e2e.Terminate(ctx, t, started) })
	}

	require.NoError(t, err, "up failed to start:\n%s", containerLogs(ctx, started))

	docker, isDocker := started.(*testcontainers.DockerContainer)
	require.True(t, isDocker)

	return docker
}

// requireSuccess requires an exited up container to have exited 0,
// returning its output.
func requireSuccess(ctx context.Context, t *testing.T, started testcontainers.Container) string {
	t.Helper()

	state, err := started.State(ctx)
	require.NoError(t, err)

	output := containerLogs(ctx, started)
	require.Equal(t, 0, state.ExitCode, "up exited with %d:\n%s", state.ExitCode, output)

	return output
}

// containerLogs returns started's output, or a note why it can't.
func containerLogs(ctx context.Context, started testcontainers.Container) string {
	if started == nil {
		return "(no container)"
	}

	logs, err := started.Logs(ctx)
	if err != nil {
		return "(no logs: " + err.Error() + ")"
	}

	output, err := io.ReadAll(logs)
	if err != nil {
		return "(no logs: " + err.Error() + ")"
	}

	return string(output)
}

// copyKubeconfig loads the kubeconfig an exited up container wrote.
func copyKubeconfig(ctx context.Context, t *testing.T, started testcontainers.Container) *clientcmdapi.Config {
	t.Helper()

	reader, err := started.CopyFileFromContainer(ctx, upE2EKubeconfigPath)
	require.NoError(t, err)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	kubeconfig, err := clientcmd.Load(data)
	require.NoError(t, err)

	return kubeconfig
}

// seedThroughServe creates the fleet's very first objects before any zone
// peer exists to serve its API: a short-lived kontinuum serve against the
// shared storage, stopped gracefully so it deregisters itself again.
func seedThroughServe(ctx context.Context, t *testing.T, infra upE2EInfra, zones []upE2EZone) {
	t.Helper()

	serve, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image: upE2EImage,
			Env: map[string]string{
				"KONTINUUM_SERVER_STORAGE":           infra.storageURI,
				"KONTINUUM_SERVER_STORAGE_MODE":      "Direct",
				"KONTINUUM_INSECURE_ALLOW_ANONYMOUS": "true",
				// Info, so the startup line WaitingFor matches is logged.
				"KONTINUUM_LOG_LEVEL": "info",
			},
			WaitingFor: wait.ForLog("Kontinuum starting").WithStartupTimeout(upE2EPollInterval * 6),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() { e2e.Terminate(ctx, t, serve) })

	address, err := serve.ContainerIP(ctx)
	require.NoError(t, err)

	hubClient := newE2EClient(t, &rest.Config{Host: "http://" + net.JoinHostPort(address, "8080")})

	require.Eventually(t, func() bool {
		return hubClient.List(ctx, &v1alpha2.ZoneList{}) == nil
	}, 2*time.Minute, time.Second, "kontinuum serve never served the fleet's API")

	seedCertManager(ctx, t, hubClient, infra, zones)

	stopTimeout := upE2EStopTimeout
	require.NoError(t, serve.Stop(ctx, &stopTimeout))
}

// seedCertManager points each zone's cert-manager DNS-01 propagation check
// at BIND, the only server e2e.test exists on, by creating its cert-manager
// Addon ahead of up: the taloscluster controller never replaces an Addon
// that already exists.
func seedCertManager(ctx context.Context, t *testing.T, hubClient client.Client, infra upE2EInfra, zones []upE2EZone) {
	t.Helper()

	values, err := json.Marshal(map[string]any{"extraArgs": []string{
		"--dns01-recursive-nameservers-only", "--dns01-recursive-nameservers=" + infra.bindIP + ":53",
	}})
	require.NoError(t, err)

	require.NoError(t, client.IgnoreAlreadyExists(hubClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha2.KontinuumSystemNamespace},
	})))

	for _, zone := range zones {
		name := "eu-" + zone.zone

		require.NoError(t, client.IgnoreAlreadyExists(hubClient.Create(ctx, &v1alpha2.Addon{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-cert-manager", Namespace: v1alpha2.KontinuumSystemNamespace},
			Spec: v1alpha2.AddonSpec{
				TalosClusterRef: v1alpha2.TalosClusterReference{Name: name},
				ReleaseName:     "cert-manager",
				Values:          &apiextensionsv1.JSON{Raw: values},
			},
		})))
	}
}

// newE2EClient builds a client for the fleet's and the zones' own kinds.
func newE2EClient(t *testing.T, restConfig *rest.Config) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, certmanagerv1.AddToScheme(scheme))
	require.NoError(t, gatewayv1.Install(scheme))
	zonedomain.AddExternalDNSToScheme(scheme)

	apiClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	require.NoError(t, err)

	return apiClient
}

// peerClient talks to the fleet's API on zone's own kontinuum-server,
// through its Gateway — the address up's kubeconfig context for it points
// at. Pebble's test certificate isn't verified here; assertFrontendHost
// checks the served chain separately.
func peerClient(t *testing.T, zone upE2EZone) client.Client {
	t.Helper()

	// The Gateway routes by hostname, so requests go to the zone's own
	// hostname — dialed at its node, since e2e.test only exists on BIND.
	return newE2EClient(t, &rest.Config{
		Host:            "https://" + zone.host(),
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, network, net.JoinHostPort(zone.addresses[0], "443"))
		},
	})
}

// zoneClient builds a client against name's own cluster.
func zoneClient(ctx context.Context, t *testing.T, peer client.Client, name string) client.Client {
	t.Helper()

	var cluster v1alpha2.TalosCluster
	require.NoError(t, peer.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		&cluster))

	var secret corev1.Secret
	require.NoError(t, peer.Get(ctx, client.ObjectKey{
		Name: cluster.Status.SecretRef.Name, Namespace: cluster.Status.SecretRef.Namespace,
	}, &secret))

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["kubeconfig"])
	require.NoError(t, err)

	return newE2EClient(t, restConfig)
}

// assertFrontendHost checks every zone's certificate covers its own
// hostname and the frontend host, the frontend host resolves to every
// zone, and a zone hostname's UI redirects to it.
func assertFrontendHost(ctx context.Context, t *testing.T, peer client.Client, infra upE2EInfra, zones []upE2EZone) {
	t.Helper()

	addresses := make([]string, 0, len(zones))
	roots := x509.NewCertPool()

	for _, zone := range zones {
		downstream := zoneClient(ctx, t, peer, "eu-"+zone.zone)

		var cert certmanagerv1.Certificate
		require.NoError(t, downstream.Get(ctx, kontinuumObject("kontinuum"), &cert))
		assert.Equal(t, []string{zone.host(), upE2EFrontend}, cert.Spec.DNSNames)

		addIssuers(ctx, t, downstream, roots)

		addresses = append(addresses, zone.addresses[0])
	}

	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return new(net.Dialer).DialContext(ctx, network, net.JoinHostPort(infra.bindIP, "53"))
	}}

	slices.Sort(addresses)

	var (
		resolved  []string
		lookupErr error
	)

	resolvedAll := assert.Eventually(t, func() bool {
		resolved, lookupErr = resolver.LookupHost(ctx, upE2EFrontend)
		slices.Sort(resolved)

		return lookupErr == nil && slices.Equal(resolved, addresses)
	}, 10*time.Minute, upE2EPollInterval)
	if !resolvedAll {
		t.Fatalf("the frontend host resolved to %v (%v), not every zone %v:\n%s",
			resolved, lookupErr, addresses, frontendDiagnostics(ctx, t, peer, zones))
	}

	status, location := getVia(ctx, t, zones[0].addresses[0], zones[0].host(), "/app", roots)
	assert.Equal(t, http.StatusPermanentRedirect, status)
	assert.Equal(t, "https://"+upE2EFrontend+"/app", location)

	status, _ = getVia(ctx, t, zones[0].addresses[0], upE2EFrontend, "/app", roots)
	assert.NotEqual(t, http.StatusPermanentRedirect, status, "the frontend host serves the UI itself")
}

// kontinuumObject names an object in a zone cluster's kontinuum-system.
func kontinuumObject(name string) client.ObjectKey {
	return client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}
}

// frontendDiagnostics describes, for a failed frontend host lookup, every
// zone's Gateway addresses and the DNSEndpoints in its cluster.
func frontendDiagnostics(ctx context.Context, t *testing.T, peer client.Client, zones []upE2EZone) string {
	t.Helper()

	var out strings.Builder

	for _, zone := range zones {
		downstream := zoneClient(ctx, t, peer, "eu-"+zone.zone)

		var gateway gatewayv1.Gateway

		err := downstream.Get(ctx, kontinuumObject("kontinuum"), &gateway)
		fmt.Fprintf(&out, "zone %s: gateway addresses %v (%v)\n", zone.zone, gateway.Status.Addresses, err)

		var endpoints zonedomain.DNSEndpointList

		err = downstream.List(ctx, &endpoints)
		fmt.Fprintf(&out, "zone %s: dnsendpoints (%v):\n", zone.zone, err)

		for _, endpoint := range endpoints.Items {
			fmt.Fprintf(&out, "  %s/%s: %+v\n", endpoint.Namespace, endpoint.Name, endpoint.Spec.Endpoints)
		}
	}

	return out.String()
}

// addIssuers adds the issuing certificates of a zone's own TLS Secret —
// Pebble's intermediate — to roots: Pebble's test root is never handed
// out, but Go accepts any certificate in RootCAs as a trust anchor.
func addIssuers(ctx context.Context, t *testing.T, downstream client.Client, roots *x509.CertPool) {
	t.Helper()

	var secret corev1.Secret
	require.NoError(t, downstream.Get(ctx, kontinuumObject("kontinuum-tls"), &secret))

	rest := secret.Data[corev1.TLSCertKey]

	for index := 0; ; index++ {
		var block *pem.Block

		block, rest = pem.Decode(rest)
		if block == nil {
			return
		}

		if index == 0 {
			continue
		}

		issuer, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		roots.AddCert(issuer)
	}
}

// getVia requests https://host/path from address, verifying the served
// certificate against roots, without following redirects, and returns the
// response's status and Location.
func getVia(ctx context.Context, t *testing.T, address, host, path string, roots *x509.CertPool) (int, string) {
	t.Helper()

	httpClient := &http.Client{
		Timeout: upE2EPollInterval,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{ServerName: host, RootCAs: roots, MinVersion: tls.VersionTLS12},
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return new(net.Dialer).DialContext(ctx, network, net.JoinHostPort(address, "443"))
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+path, nil)
	require.NoError(t, err)

	response, err := httpClient.Do(request)
	require.NoError(t, err)

	defer func() { _ = response.Body.Close() }()

	return response.StatusCode, response.Header.Get("Location")
}

// assertControlPlaneMembers checks name's cluster runs want control-plane
// nodes.
func assertControlPlaneMembers(ctx context.Context, t *testing.T, peer client.Client, name string, want int) {
	t.Helper()

	var nodes corev1.NodeList
	require.NoError(t, zoneClient(ctx, t, peer, name).List(ctx, &nodes,
		client.HasLabels{"node-role.kubernetes.io/control-plane"}))
	assert.Len(t, nodes.Items, want)
}
