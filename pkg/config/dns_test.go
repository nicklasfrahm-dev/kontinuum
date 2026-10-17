package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

const (
	// cloudflare is a valid DNS provider with a DNS-01 solver.
	cloudflare = string(config.DNSProviderCloudflare)
	// testDomain is the DNS domain every case manages.
	testDomain = "example.com"
	// testFrontendHost is a frontend host within testDomain.
	testFrontendHost = "kontinuum." + testDomain
)

func TestParseDNSProvider(t *testing.T) {
	t.Parallel()

	for _, provider := range config.DNSProviders() {
		parsed, err := config.ParseDNSProvider(string(provider))
		require.NoError(t, err)
		assert.Equal(t, provider, parsed)
	}

	empty, err := config.ParseDNSProvider("")
	require.NoError(t, err)
	assert.Empty(t, empty)

	for _, raw := range []string{"cloudflare", "route53", "pdns", "aws", "Route53"} {
		_, err := config.ParseDNSProvider(raw)
		require.ErrorIs(t, err, config.ErrUnknownDNSProvider, raw)
		assert.Contains(t, err.Error(), "Cloudflare, AWS", "the error lists the accepted values")
	}
}

func TestHasDNS01Solver(t *testing.T) {
	t.Parallel()

	assert.False(t, config.DNSProviderPowerDNS.HasDNS01Solver())
	assert.False(t, config.DNSProvider("").HasDNS01Solver())

	for _, provider := range config.DNSProviders() {
		if provider != config.DNSProviderPowerDNS {
			assert.True(t, provider.HasDNS01Solver(), provider)
		}
	}
}

// validateDNSCase is one TestValidateDNS row.
type validateDNSCase struct {
	name     string
	provider string
	domain   string
	host     string
	wantErr  error
}

// validateDNSCases returns TestValidateDNS's rows.
func validateDNSCases() []validateDNSCase {
	return []validateDNSCase{
		{name: "nothing configured"},
		{name: "provider only", provider: cloudflare},
		{name: "powerdns without frontend", provider: "PowerDNS"},
		{name: "lowercase provider", provider: "cloudflare", wantErr: config.ErrUnknownDNSProvider},
		{name: "frontend on subdomain", provider: cloudflare, domain: testDomain, host: testFrontendHost},
		{name: "frontend on apex", provider: "AWS", domain: testDomain, host: testDomain},
		{
			name: "frontend without provider", domain: testDomain, host: testFrontendHost,
			wantErr: config.ErrFrontendHostNeedsDNS01,
		},
		{
			name: "frontend with powerdns", provider: "PowerDNS", domain: testDomain, host: testFrontendHost,
			wantErr: config.ErrFrontendHostNeedsDNS01,
		},
		{
			name: "frontend outside domain", provider: cloudflare, domain: testDomain, host: "kontinuum.example.org",
			wantErr: config.ErrFrontendHostOutsideDomain,
		},
		{
			name: "frontend sharing a suffix", provider: cloudflare, domain: testDomain, host: "notexample.com",
			wantErr: config.ErrFrontendHostOutsideDomain,
		},
		{
			name: "frontend without domain", provider: cloudflare, host: testFrontendHost,
			wantErr: config.ErrFrontendHostOutsideDomain,
		},
		{
			name: "invalid frontend", provider: cloudflare, domain: testDomain, host: "Not A Host.example.com",
			wantErr: config.ErrInvalidFrontendHost,
		},
	}
}

func TestValidateDNS(t *testing.T) {
	t.Parallel()

	for _, test := range validateDNSCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{}
			cfg.Defaults()
			cfg.Server.DNS.Provider = test.provider
			cfg.Server.DNS.Domain = test.domain
			cfg.Server.Frontend.Host = test.host

			err := cfg.ValidateDNS()
			if test.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

func TestLoadReadsFrontendHostEnvVar(t *testing.T) {
	t.Setenv("KONTINUUM_SERVER_FRONTEND_HOST", testFrontendHost)

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, testFrontendHost, cfg.Server.Frontend.Host)
}

func TestDefaultFrontendHostIsEmpty(t *testing.T) {
	t.Parallel()

	defaults := &config.Config{}
	defaults.Defaults()

	assert.Empty(t, defaults.Server.Frontend.Host)
}
