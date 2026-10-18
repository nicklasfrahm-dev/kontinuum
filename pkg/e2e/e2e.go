// Package e2e holds the containers kontinuum's gated end-to-end tests run
// against — Talos nodes, PostgreSQL, an authoritative DNS server and an
// ACME test CA — so every package's E2E tests start them the same way. It
// is only ever imported by _test.go files.
package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// TalosImage is pinned to the Talos release every E2E test runs.
	TalosImage = "ghcr.io/siderolabs/talos:v1.13.0"
	// BindImage is the pinned authoritative DNS server image.
	BindImage = "internetsystemsconsortium/bind9:9.20"
	// PebbleImage is the pinned ACME test CA image.
	PebbleImage = "ghcr.io/letsencrypt/pebble:2.10.1"
	// PostgresImage is the pinned shared database image.
	PostgresImage = "postgres:17-alpine"
	// startTimeout bounds how long one container may take to start.
	startTimeout = 90 * time.Second
	// postgresUser, postgresLogin and postgresDB are a throwaway test
	// database's own user, login and database.
	postgresUser  = "kontinuum"
	postgresLogin = "kontinuum-e2e"
	postgresDB    = "kontinuum"
	// postgresReadyLogs is how often PostgreSQL's image logs being ready:
	// once for its temporary bootstrap server, once for the real one.
	postgresReadyLogs = 2
	// configFileMode and zoneFileMode are BIND's files' modes: the zone
	// file must be writable for dynamic updates.
	configFileMode = 0o644
	zoneFileMode   = 0o666
)

// Skip skips t unless KONTINUUM_TEST_E2E is set: every E2E test needs
// Docker and boots real containers.
func Skip(t *testing.T) {
	t.Helper()

	if os.Getenv("KONTINUUM_TEST_E2E") == "" {
		t.Skip("set KONTINUUM_TEST_E2E=1 to run this test — it needs Docker and boots real containers")
	}
}

// StartTalos boots a Talos node in maintenance mode and returns its
// address. It's stopped when t ends.
func StartTalos(ctx context.Context, t *testing.T) string {
	t.Helper()

	return start(ctx, t, testcontainers.ContainerRequest{
		Image: TalosImage,
		Tmpfs: map[string]string{"/run": "", "/system": "", "/tmp": ""},
		// apid — the maintenance-mode gRPC listener — starts a moment
		// after "entering maintenance service" logs; its own health check
		// is what confirms the listener is up.
		WaitingFor: wait.ForLog("service[apid](Running): Health check successful").WithStartupTimeout(startTimeout),
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
	})
}

// StartPostgres starts a PostgreSQL server and returns a connection string
// every container on the Docker network can reach it with.
func StartPostgres(ctx context.Context, t *testing.T) string {
	t.Helper()

	address := start(ctx, t, testcontainers.ContainerRequest{
		Image: PostgresImage,
		Env: map[string]string{
			"POSTGRES_USER": postgresUser, "POSTGRES_PASSWORD": postgresLogin, "POSTGRES_DB": postgresDB,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(postgresReadyLogs).
			WithStartupTimeout(startTimeout),
	})

	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		postgresUser, postgresLogin, net.JoinHostPort(address, "5432"), postgresDB)
}

// BindConfig returns the named.conf and zone file for an authoritative
// server of zone accepting RFC2136 updates, and zone transfers (which
// external-dns's RFC2136 provider lists records with), signed with the
// TSIG key keyName/key (hmac-sha256).
func BindConfig(zone, keyName, key string) (string, string) {
	namedConf := fmt.Sprintf(`key %q { algorithm hmac-sha256; secret %q; };
options {
	directory "/var/cache/bind";
	listen-on { any; };
	listen-on-v6 { none; };
	allow-query { any; };
	recursion no;
	dnssec-validation no;
};
zone %q {
	type primary;
	file "/var/lib/bind/db.zone";
	update-policy { grant %s zonesub ANY; };
	allow-transfer { key %q; };
};
`, keyName, key, zone, keyName, keyName)

	zoneFile := fmt.Sprintf(`$TTL 30
@ IN SOA ns.%[1]s. admin.%[1]s. 1 30 30 300 30
@ IN NS ns.%[1]s.
ns IN A 127.0.0.1
`, zone)

	return namedConf, zoneFile
}

// StartBind runs BIND as the authoritative server for zone — see
// BindConfig — and returns its address.
func StartBind(ctx context.Context, t *testing.T, zone, keyName, key string) string {
	t.Helper()

	namedConf, zoneFile := BindConfig(zone, keyName, key)

	return start(ctx, t, testcontainers.ContainerRequest{
		Image: BindImage,
		Cmd:   []string{"-g", "-c", "/etc/bind/named.conf"},
		Files: []testcontainers.ContainerFile{
			{Reader: strings.NewReader(namedConf), ContainerFilePath: "/etc/bind/named.conf", FileMode: configFileMode},
			{Reader: strings.NewReader(zoneFile), ContainerFilePath: "/var/lib/bind/db.zone", FileMode: zoneFileMode},
		},
		WaitingFor: wait.ForLog("running").WithStartupTimeout(startTimeout),
	})
}

// StartPebble runs Pebble, validating challenges against the DNS server
// at dnsServer, and returns its address. Its ACME directory is
// https://<address>:14000/dir, behind a certificate from its own test CA.
func StartPebble(ctx context.Context, t *testing.T, dnsServer string) string {
	t.Helper()

	return start(ctx, t, testcontainers.ContainerRequest{
		Image:      PebbleImage,
		Cmd:        []string{"-config", "test/config/pebble-config.json", "-dnsserver", dnsServer + ":53"},
		Env:        map[string]string{"PEBBLE_VA_NOSLEEP": "1", "PEBBLE_WFE_NONCEREJECT": "0"},
		WaitingFor: wait.ForLog("ACME directory available").WithStartupTimeout(startTimeout),
	})
}

// start starts request, stops it when t ends, and returns its address on
// the Docker network.
func start(ctx context.Context, t *testing.T, request testcontainers.ContainerRequest) string {
	t.Helper()

	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true, ContainerRequest: request,
	})
	require.NoError(t, err)

	t.Cleanup(func() { Terminate(ctx, t, started) })

	address, err := started.ContainerIP(ctx)
	require.NoError(t, err)

	return address
}

// Terminate stops container, tolerating the benign teardown races
// IsBenignTerminateError lists.
func Terminate(ctx context.Context, t *testing.T, started testcontainers.Container) {
	t.Helper()

	err := started.Terminate(context.WithoutCancel(ctx))
	if err != nil && !IsBenignTerminateError(err) {
		require.NoError(t, err)
	}
}

// IsBenignTerminateError reports whether err is one of Docker's benign
// teardown races rather than a real cleanup failure: "removal of container
// ... is already in progress" (Ryuk reaping the same container
// concurrently) or "is zombie and can not be killed" (a privileged Talos
// container whose init is already exiting). By the time either fires,
// every real assertion has already passed.
func IsBenignTerminateError(err error) bool {
	return strings.Contains(err.Error(), "is already in progress") || strings.Contains(err.Error(), "is zombie")
}
