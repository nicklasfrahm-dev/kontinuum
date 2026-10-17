package config_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

// publicIP is a documentation-range (RFC 5737) address standing in for a
// globally routable one.
const publicIP = "203.0.113.10"

// errNoSuchHost is stubResolver's own failure for a hostname it has no
// records for.
var errNoSuchHost = errors.New("no such host")

// stubResolver resolves hostnames from a fixed table instead of making real
// DNS queries.
type stubResolver map[string][]string

func (s stubResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	addrs, ok := s[host]
	if !ok {
		return nil, errNoSuchHost
	}

	return addrs, nil
}

func TestIsGloballyRoutable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr     string
		routable bool
	}{
		{addr: publicIP, routable: true},
		{addr: "8.8.8.8", routable: true},
		{addr: "2001:4860:4860::8888", routable: true},
		// RFC 1918.
		{addr: "10.0.0.1", routable: false},
		{addr: "10.255.255.255", routable: false},
		{addr: "172.16.0.1", routable: false},
		{addr: "172.31.255.255", routable: false},
		{addr: "172.32.0.1", routable: true},
		{addr: "192.168.1.1", routable: false},
		// RFC 6598 CGNAT.
		{addr: "100.64.0.1", routable: false},
		{addr: "100.127.255.255", routable: false},
		{addr: "100.128.0.1", routable: true},
		// Loopback.
		{addr: "127.0.0.1", routable: false},
		{addr: "127.255.255.254", routable: false},
		{addr: "::1", routable: false},
		// Link-local.
		{addr: "169.254.1.1", routable: false},
		{addr: "fe80::1", routable: false},
		// IPv6 ULA.
		{addr: "fc00::1", routable: false},
		{addr: "fd12:3456:789a::1", routable: false},
		// Unspecified and multicast.
		{addr: "0.0.0.0", routable: false},
		{addr: "::", routable: false},
		{addr: "224.0.0.1", routable: false},
		{addr: "ff02::1", routable: false},
		// IPv4-mapped IPv6 is classified by the address it maps.
		{addr: "::ffff:10.0.0.1", routable: false},
		{addr: "::ffff:203.0.113.10", routable: true},
	}

	for _, test := range tests {
		t.Run(test.addr, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.routable, config.IsGloballyRoutable(netip.MustParseAddr(test.addr)))
		})
	}
}

func TestIsGloballyRoutableRejectsInvalidAddress(t *testing.T) {
	t.Parallel()

	assert.False(t, config.IsGloballyRoutable(netip.Addr{}))
}

func TestResolveStorageModeInference(t *testing.T) {
	t.Parallel()

	resolver := stubResolver{
		"db.example.com":      {publicIP, "2001:4860::10"},
		"public.example.com":  {publicIP},
		"private.example.com": {"10.0.0.5"},
		"mixed.example.com":   {publicIP, "10.0.0.5"},
		"cgnat.example.com":   {"100.64.1.1"},
		"empty.example.com":   {},
	}

	tests := []struct {
		name    string
		storage string
		want    config.StorageMode
	}{
		{name: "nats", storage: "nats://nats.example.com:4222", want: config.StorageModeNATS},
		{name: "nats with private host", storage: "nats://10.0.0.5:4222", want: config.StorageModeNATS},
		{name: "postgres public ip", storage: "postgres://203.0.113.10:5432/db", want: config.StorageModeDirect},
		{name: "postgresql public host", storage: "postgresql://public.example.com/db", want: config.StorageModeDirect},
		{name: "postgres public ipv6", storage: "postgres://[2001:4860::1]:5432/db", want: config.StorageModeDirect},
		{name: "postgres private ip", storage: "postgres://10.0.0.5:5432/db", want: config.StorageModeProxy},
		{name: "postgres private host", storage: "postgres://private.example.com/db", want: config.StorageModeProxy},
		{name: "postgres mixed records", storage: "postgres://mixed.example.com/db", want: config.StorageModeProxy},
		{name: "postgres cgnat host", storage: "postgres://cgnat.example.com/db", want: config.StorageModeProxy},
		{name: "postgres unresolvable host", storage: "postgres://missing.example.com/db", want: config.StorageModeProxy},
		{name: "postgres host without records", storage: "postgres://empty.example.com/db", want: config.StorageModeProxy},
		{name: "postgres loopback", storage: "postgres://127.0.0.1:5432/db", want: config.StorageModeProxy},
		{name: "postgres dual stack public", storage: "postgres://db.example.com/db", want: config.StorageModeDirect},
		{name: "mysql tcp form public", storage: "mysql://kine@tcp(203.0.113.10:3306)/db", want: config.StorageModeDirect},
		{name: "mysql tcp form private", storage: "mysql://kine@tcp(192.168.1.5:3306)/db", want: config.StorageModeProxy},
		{name: "mysql plain form public", storage: "mysql://kine@public.example.com:3306/db", want: config.StorageModeDirect},
		{name: "sqlite", storage: "sqlite://kontinuum.db", want: config.StorageModeProxy},
		{name: "relay dsn", storage: "etcdproxy://eu-1a@hub.example.com:443", want: config.StorageModeProxy},
		{name: "etcd endpoint", storage: "etcd://203.0.113.10:2379", want: config.StorageModeProxy},
		{name: "unix socket", storage: "unix:///tmp/kine.sock", want: config.StorageModeProxy},
		{name: "unparseable", storage: "postgres://[::1", want: config.StorageModeProxy},
		{name: "empty", storage: "", want: config.StorageModeProxy},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			mode, err := config.ResolveStorageMode(context.Background(), resolver, test.storage, config.StorageModeAuto)
			require.NoError(t, err)
			assert.Equal(t, test.want, mode)
		})
	}
}

func TestResolveStorageModeOverride(t *testing.T) {
	t.Parallel()

	// Every override short-circuits inference: a private postgres URI
	// infers to Proxy, a public one to Direct, yet each override wins
	// regardless of which one it's paired with.
	storages := []string{"postgres://10.0.0.5/db", "postgres://" + publicIP + "/db", "sqlite://kontinuum.db"}

	overrides := []config.StorageMode{config.StorageModeProxy, config.StorageModeDirect, config.StorageModeNATS}

	for _, storage := range storages {
		for _, override := range overrides {
			t.Run(storage+"/"+string(override), func(t *testing.T) {
				t.Parallel()

				mode, err := config.ResolveStorageMode(context.Background(), stubResolver{}, storage, override)
				require.NoError(t, err)
				assert.Equal(t, override, mode)
			})
		}
	}
}

func TestResolveStorageModeEmptyOverrideInfers(t *testing.T) {
	t.Parallel()

	mode, err := config.ResolveStorageMode(context.Background(), stubResolver{}, "postgres://"+publicIP+"/db", "")
	require.NoError(t, err)
	assert.Equal(t, config.StorageModeDirect, mode)
}

func TestResolveStorageModeRejectsInvalidOverride(t *testing.T) {
	t.Parallel()

	_, err := config.ResolveStorageMode(context.Background(), stubResolver{}, "postgres://"+publicIP+"/db", "direct")
	require.ErrorIs(t, err, config.ErrInvalidStorageMode)
}

func TestParseStorageMode(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]config.StorageMode{
		"":       config.StorageModeAuto,
		"Auto":   config.StorageModeAuto,
		"Proxy":  config.StorageModeProxy,
		"Direct": config.StorageModeDirect,
		"NATS":   config.StorageModeNATS,
	} {
		mode, err := config.ParseStorageMode(raw)
		require.NoError(t, err, raw)
		assert.Equal(t, want, mode, raw)
	}

	for _, raw := range []string{"auto", "proxy", "Nats", "nats", "Hub"} {
		_, err := config.ParseStorageMode(raw)
		require.ErrorIs(t, err, config.ErrInvalidStorageMode, raw)
	}
}

func TestDefaultZoneStorageModeIsAuto(t *testing.T) {
	t.Parallel()

	defaults := &config.Config{}
	defaults.Defaults()

	assert.Equal(t, string(config.StorageModeAuto), defaults.Server.ZoneStorageMode)
	require.NoError(t, defaults.ValidateStorageMode())
}

func TestLoadReadsZoneStorageModeEnvVar(t *testing.T) {
	t.Setenv("KONTINUUM_SERVER_ZONE_STORAGE_MODE", "Direct")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "Direct", cfg.Server.ZoneStorageMode)
	require.NoError(t, cfg.ValidateStorageMode())
}

func TestValidateStorageModeRejectsInvalidValue(t *testing.T) {
	t.Setenv("KONTINUUM_SERVER_ZONE_STORAGE_MODE", "direct")

	cfg, err := config.Load()
	require.NoError(t, err)

	require.ErrorIs(t, cfg.ValidateStorageMode(), config.ErrInvalidStorageMode)
}

func TestZoneStorageModeReachesEnvVars(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.Defaults()
	cfg.Server.ZoneStorageMode = string(config.StorageModeDirect)

	var found bool

	for _, envVar := range cfg.EnvVars() {
		if envVar.Name == "KONTINUUM_SERVER_ZONE_STORAGE_MODE" {
			found = true

			assert.Equal(t, "Direct", envVar.Value)
			assert.False(t, envVar.Secret)
		}
	}

	assert.True(t, found)
}
