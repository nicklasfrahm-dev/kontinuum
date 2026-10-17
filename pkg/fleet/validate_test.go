package fleet_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/fleet"
)

// testCLIVersion is the version baseFleet declares.
const testCLIVersion = "v0.37.0"

// validFleet loads baseFleet with replacements applied in order.
func validFleet(t *testing.T, replacements ...string) *fleet.Fleet {
	t.Helper()

	content := baseFleet
	for index := 0; index+1 < len(replacements); index += 2 {
		require.Contains(t, content, replacements[index])
		content = strings.Replace(content, replacements[index], replacements[index+1], 1)
	}

	parsed, err := load(t, writeFile(t, t.TempDir(), "kontinuum.yml", content))
	require.NoError(t, err)

	return parsed
}

func TestValidateAcceptsBaseFleet(t *testing.T) {
	t.Parallel()

	require.NoError(t, fleet.Validate(validFleet(t), testCLIVersion))
}

// rejectionCase is one TestValidateRejections row: baseFleet with
// replacements applied fails validation with an error containing want.
type rejectionCase struct {
	name         string
	replacements []string
	want         string
}

// rejectionCases returns TestValidateRejections' rows.
func rejectionCases() []rejectionCase {
	return append(settingsRejectionCases(), topologyRejectionCases()...)
}

// settingsRejectionCases covers versions, DNS and ACME.
func settingsRejectionCases() []rejectionCase {
	return []rejectionCase{
		{
			name: "version mismatch", replacements: []string{"version: v0.37.0", "version: v0.36.0"},
			want: "spec.version must equal this CLI's version",
		},
		{
			name: "unknown dns provider", replacements: []string{"provider: Cloudflare", "provider: cloudflare"},
			want: "unknown dns provider",
		},
		{
			name:         "frontend outside domain",
			replacements: []string{"host: kontinuum.example.com", "host: kontinuum.example.org"},
			want:         "must equal KONTINUUM_SERVER_DNS_DOMAIN or be a subdomain",
		},
		{
			name: "powerdns with frontend", replacements: []string{"provider: Cloudflare", "provider: PowerDNS"},
			want: "DNS-01 solver",
		},
		{
			name:         "missing solver credential key",
			replacements: []string{"CF_API_TOKEN: ${env:CF_TOKEN}", "OTHER_KEY: ${env:CF_TOKEN}"},
			want:         "CF_API_TOKEN",
		},
		{
			name:         "unknown acme provider",
			replacements: []string{"provider: letsencrypt", "provider: letsencrypt-staging"},
			want:         "declared: letsencrypt, zerossl",
		},
		{
			name:         "acme provider name not a label",
			replacements: []string{"      zerossl:", "      ZeroSSL:"}, want: "DNS-1123",
		},
		{
			name: "half-set eab", replacements: []string{"          hmacKey: ${env:EAB_HMAC}\n", ""},
			want: "keyID and hmacKey must both be set",
		},
	}
}

// topologyRejectionCases covers storage, auth and zones.
func topologyRejectionCases() []rejectionCase {
	return []rejectionCase{
		{
			name:         "proxy storage mode",
			replacements: []string{"    uri: ${env:STORAGE_URI}", "    mode: Proxy\n    uri: ${env:STORAGE_URI}"},
			want:         "Proxy is not supported",
		},
		{
			name:         "nats storage mode",
			replacements: []string{"    uri: ${env:STORAGE_URI}", "    mode: NATS\n    uri: ${env:STORAGE_URI}"},
			want:         "NATS is not supported",
		},
		{
			name:         "oidc and anonymous",
			replacements: []string{"  auth:\n", "  auth:\n    insecureAllowAnonymous: true\n"},
			want:         "mutually exclusive",
		},
		{
			name: "no admin groups", replacements: []string{"      adminGroups:\n        - platform-admins\n", ""},
			want: "adminGroups",
		},
		{
			name: "even instance count",
			replacements: []string{
				"        - address: 198.51.100.20\n",
				"        - address: 198.51.100.20\n        - address: 198.51.100.21\n",
			},
			want: "1, 3 or 5 instances",
		},
		{name: "duplicate zone", replacements: []string{"zone: eu-1b", "zone: eu-1a"}, want: "already declared"},
		{
			name: "single zone",
			replacements: []string{
				"    - region: eu\n      zone: eu-1b\n      instances:\n        - address: 198.51.100.20\n",
				"",
			},
			want: "at least 2 zones",
		},
	}
}

func TestValidateRejections(t *testing.T) {
	t.Parallel()

	for _, test := range rejectionCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := fleet.Validate(validFleet(t, test.replacements...), testCLIVersion)
			require.ErrorIs(t, err, fleet.ErrInvalidFleet)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestValidateRejectsChallengeAsUnknownField(t *testing.T) {
	t.Parallel()

	content := strings.Replace(baseFleet, "        email: ops@example.com\n      zerossl:",
		"        email: ops@example.com\n        challenge: HTTP01\n      zerossl:", 1)

	_, err := load(t, writeFile(t, t.TempDir(), "kontinuum.yml", content))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "challenge")
}

func TestToConfigMapsOnlyTheSelectedACMEProvider(t *testing.T) {
	t.Parallel()

	parsed := validFleet(t, "provider: letsencrypt", "provider: zerossl")

	cfg, err := parsed.ToConfig()
	require.NoError(t, err)

	assert.Equal(t, "https://acme.zerossl.com/v2/DV90", cfg.ACME.Server)
	assert.Equal(t, "kid", cfg.ACME.ExternalAccountBinding.KeyID)
	assert.Equal(t, "hmac", cfg.ACME.ExternalAccountBinding.HMACKey)
	assert.Equal(t, "false", cfg.ACME.InsecureSkipVerify)

	for _, envVar := range cfg.EnvVars() {
		assert.NotContains(t, envVar.Value, "letsencrypt.org", "unselected providers never leave the CLI")
	}
}

func TestToConfigMapsEveryField(t *testing.T) {
	t.Parallel()

	cfg, err := validFleet(t).ToConfig()
	require.NoError(t, err)

	assert.Equal(t, "postgres://kontinuum@db.example.com/kontinuum", cfg.Server.Storage)
	assert.Equal(t, string(config.StorageModeAuto), cfg.Server.ZoneStorageMode)
	assert.Equal(t, "kontinuum.example.com", cfg.Server.Frontend.Host)
	assert.Equal(t, "example.com", cfg.Server.DNS.Domain)
	assert.Equal(t, "Cloudflare", cfg.Server.DNS.Provider)
	assert.Equal(t, "CF_API_TOKEN: token\n", cfg.Server.DNS.Credential)
	assert.Equal(t, "https://id.example.com", cfg.OIDC.IssuerURL)
	assert.Equal(t, "platform-admins", cfg.OIDC.AdminGroups)
	assert.Equal(t, "https://kontinuum.example.com/app", cfg.OIDC.RedirectURL)
	assert.Equal(t, "false", cfg.InsecureAllowAnonymous)
	_, err = cfg.ValidateAuthentication()
	require.NoError(t, err)
	require.NoError(t, cfg.ValidateACME())
}

func TestOverlaySwitchesACMEProvider(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", baseFleet)
	staging := writeFile(t, dir, "staging.yml", header+`spec:
  acme:
    provider: letsencrypt-staging
    providers:
      letsencrypt-staging:
        server: https://acme-staging-v02.api.letsencrypt.org/directory
        email: staging@example.com
`)

	parsed, err := load(t, base, staging)
	require.NoError(t, err)
	require.NoError(t, fleet.Validate(parsed, testCLIVersion))
	assert.Len(t, parsed.Spec.ACME.Providers, 3, "providers merge key by key")

	cfg, err := parsed.ToConfig()
	require.NoError(t, err)
	assert.Equal(t, "staging@example.com", cfg.ACME.Email)
}
