package addon_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

const (
	// testSolverStore is the Secret every DNS-01 solver test references.
	testSolverStore = "dns01-keys"
	// testAPIValue is a fake provider API value, not a real one.
	testAPIValue = "example"
	// rfc2136KeyEnv is the credential key holding the RFC2136 TSIG key.
	rfc2136KeyEnv = "EXTERNAL_DNS_RFC2136_TSIG_" + "SECRET"
)

func TestExternalDNSProviderName(t *testing.T) {
	t.Parallel()

	for provider, want := range map[config.DNSProvider]string{
		config.DNSProviderCloudflare:   "cloudflare",
		config.DNSProviderAWS:          "aws",
		config.DNSProviderAzure:        "azure",
		config.DNSProviderGoogle:       "google",
		config.DNSProviderDigitalOcean: "digitalocean",
		config.DNSProviderRFC2136:      "rfc2136",
		config.DNSProviderPowerDNS:     "pdns",
	} {
		got, err := addon.ExternalDNSProviderName(string(provider))
		require.NoError(t, err, provider)
		assert.Equal(t, want, got, provider)
	}

	_, err := addon.ExternalDNSProviderName("cloudflare")
	require.ErrorIs(t, err, config.ErrUnknownDNSProvider)
}

func TestDNS01SolverCloudflare(t *testing.T) {
	t.Parallel()

	solver, err := addon.DNS01Solver("Cloudflare", testSolverStore, map[string]string{"CF_API_TOKEN": testAPIValue})
	require.NoError(t, err)
	require.NotNil(t, solver.Cloudflare)
	assert.Equal(t, testSolverStore, solver.Cloudflare.APIToken.Name)
	assert.Equal(t, "CF_API_TOKEN", solver.Cloudflare.APIToken.Key)
}

func TestDNS01SolverAWS(t *testing.T) {
	t.Parallel()

	solver, err := addon.DNS01Solver("AWS", testSolverStore, map[string]string{
		"AWS_ACCESS_KEY_ID": "id", "AWS_SECRET_ACCESS_KEY": "key", "AWS_REGION": "eu-central-1",
	})
	require.NoError(t, err)
	require.NotNil(t, solver.Route53)
	assert.Equal(t, "eu-central-1", solver.Route53.Region)
	assert.Equal(t, "AWS_ACCESS_KEY_ID", solver.Route53.SecretAccessKeyID.Key)
	assert.Equal(t, "AWS_SECRET_ACCESS_KEY", solver.Route53.SecretAccessKey.Key)
}

func TestDNS01SolverAzure(t *testing.T) {
	t.Parallel()

	solver, err := addon.DNS01Solver("Azure", testSolverStore, map[string]string{
		"AZURE_CLIENT_ID": "client", "AZURE_CLIENT_SECRET": "secret", "AZURE_TENANT_ID": "tenant",
		"AZURE_SUBSCRIPTION_ID": "subscription", "AZURE_RESOURCE_GROUP": "dns",
	})
	require.NoError(t, err)
	require.NotNil(t, solver.AzureDNS)
	assert.Equal(t, "client", solver.AzureDNS.ClientID)
	assert.Equal(t, "AZURE_CLIENT_SECRET", solver.AzureDNS.ClientSecret.Key)
	assert.Equal(t, "dns", solver.AzureDNS.ResourceGroupName)
}

func TestDNS01SolverGoogle(t *testing.T) {
	t.Parallel()

	solver, err := addon.DNS01Solver("Google", testSolverStore, map[string]string{
		"EXTERNAL_DNS_GOOGLE_PROJECT": "project", "GOOGLE_SERVICE_ACCOUNT_KEY": "{}",
	})
	require.NoError(t, err)
	require.NotNil(t, solver.CloudDNS)
	assert.Equal(t, "project", solver.CloudDNS.Project)
	assert.Equal(t, "GOOGLE_SERVICE_ACCOUNT_KEY", solver.CloudDNS.ServiceAccount.Key)
}

func TestDNS01SolverDigitalOcean(t *testing.T) {
	t.Parallel()

	solver, err := addon.DNS01Solver("DigitalOcean", testSolverStore, map[string]string{"DO_TOKEN": testAPIValue})
	require.NoError(t, err)
	require.NotNil(t, solver.DigitalOcean)
	assert.Equal(t, "DO_TOKEN", solver.DigitalOcean.Token.Key)
}

func TestDNS01SolverRFC2136(t *testing.T) {
	t.Parallel()

	keys := map[string]string{
		"EXTERNAL_DNS_RFC2136_HOST": "ns.example.com", "EXTERNAL_DNS_RFC2136_TSIG_KEYNAME": "kontinuum.",
	}
	keys[rfc2136KeyEnv] = testAPIValue

	// external-dns has no default port or algorithm, so the solver requires
	// both.
	_, err := addon.DNS01Solver("RFC2136", testSolverStore, keys)
	require.ErrorIs(t, err, addon.ErrMissingDNSCredentialKey)
	assert.Contains(t, err.Error(), "EXTERNAL_DNS_RFC2136_PORT, EXTERNAL_DNS_RFC2136_TSIG_SECRET_ALG")

	keys["EXTERNAL_DNS_RFC2136_PORT"] = "53"
	keys["EXTERNAL_DNS_RFC2136_TSIG_SECRET_ALG"] = "hmac-sha256"

	solver, err := addon.DNS01Solver("RFC2136", testSolverStore, keys)
	require.NoError(t, err)
	require.NotNil(t, solver.RFC2136)
	assert.Equal(t, "ns.example.com:53", solver.RFC2136.Nameserver)
	assert.Equal(t, "kontinuum.", solver.RFC2136.TSIGKeyName)
	assert.Equal(t, "HMACSHA256", solver.RFC2136.TSIGAlgorithm)
	assert.Equal(t, rfc2136KeyEnv, solver.RFC2136.TSIGSecret.Key)

	keys["EXTERNAL_DNS_RFC2136_PORT"] = "5353"
	keys["EXTERNAL_DNS_RFC2136_TSIG_SECRET_ALG"] = "hmac-sha512"

	solver, err = addon.DNS01Solver("RFC2136", testSolverStore, keys)
	require.NoError(t, err)
	assert.Equal(t, "ns.example.com:5353", solver.RFC2136.Nameserver)
	assert.Equal(t, "HMACSHA512", solver.RFC2136.TSIGAlgorithm)

	keys["EXTERNAL_DNS_RFC2136_TSIG_SECRET_ALG"] = "hmac-sha3"

	_, err = addon.DNS01Solver("RFC2136", testSolverStore, keys)
	require.ErrorIs(t, err, addon.ErrUnknownTSIGAlgorithm)
}

func TestDNS01SolverNamesMissingKeys(t *testing.T) {
	t.Parallel()

	_, err := addon.DNS01Solver("AWS", testSolverStore, map[string]string{"AWS_REGION": "eu-central-1"})
	require.ErrorIs(t, err, addon.ErrMissingDNSCredentialKey)
	assert.Contains(t, err.Error(), "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
}

func TestDNS01SolverRejectsPowerDNS(t *testing.T) {
	t.Parallel()

	_, err := addon.DNS01Solver("PowerDNS", testSolverStore, map[string]string{"EXTERNAL_DNS_PDNS_SERVER": "x"})
	require.ErrorIs(t, err, addon.ErrNoDNS01Solver)
}

func TestDNS01SolverRejectsUnknownProvider(t *testing.T) {
	t.Parallel()

	_, err := addon.DNS01Solver("cloudflare", testSolverStore, map[string]string{"CF_API_TOKEN": testAPIValue})
	require.ErrorIs(t, err, config.ErrUnknownDNSProvider)
}
