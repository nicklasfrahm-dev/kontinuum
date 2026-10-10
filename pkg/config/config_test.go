package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

func TestDefaults(t *testing.T) {
	t.Parallel()

	defaults := &config.Config{}
	defaults.Defaults()

	assert.Equal(t, ":8080", defaults.Server.Addr)
	assert.Equal(t, "sqlite://kontinuum.db", defaults.Server.Storage)
	assert.Equal(t, "warn", defaults.Log.Level)
	assert.Equal(t, "json", defaults.Log.Format)
	assert.Empty(t, defaults.OIDC.IssuerURL)
	assert.Equal(t, "kontinuum", defaults.OIDC.ClientID)
	assert.Equal(t, "http://localhost:8080/app", defaults.OIDC.RedirectURL)
	assert.Empty(t, defaults.OIDC.AdminGroups)
	assert.Equal(t, "false", defaults.InsecureAllowAnonymous)
	assert.Empty(t, defaults.ACME.Email)
	assert.Equal(t, "https://acme-v02.api.letsencrypt.org/directory", defaults.ACME.Server)
}

func TestLoadReadsInsecureAllowAnonymousEnvVar(t *testing.T) {
	t.Setenv("KONTINUUM_INSECURE_ALLOW_ANONYMOUS", "true")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "true", cfg.InsecureAllowAnonymous)
}

func TestLoadReadsOIDCEnvVars(t *testing.T) {
	t.Setenv("KONTINUUM_OIDC_ISSUER_URL", "https://auth.example.com")
	t.Setenv("KONTINUUM_OIDC_CLIENT_ID", "example-client")
	t.Setenv("KONTINUUM_OIDC_REDIRECT_URL", "https://console.example.com/app")
	t.Setenv("KONTINUUM_OIDC_ADMIN_GROUPS", "platform-team,sre")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "https://auth.example.com", cfg.OIDC.IssuerURL)
	assert.Equal(t, "example-client", cfg.OIDC.ClientID)
	assert.Equal(t, "https://console.example.com/app", cfg.OIDC.RedirectURL)
	assert.Equal(t, "platform-team,sre", cfg.OIDC.AdminGroups)
}

func TestLoadFallsBackToOIDCDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Empty(t, cfg.OIDC.IssuerURL)
	assert.Equal(t, "kontinuum", cfg.OIDC.ClientID)
	assert.Equal(t, "http://localhost:8080/app", cfg.OIDC.RedirectURL)
	assert.Empty(t, cfg.OIDC.AdminGroups)
}

func TestLoadReadsACMEEnvVars(t *testing.T) {
	t.Setenv("KONTINUUM_ACME_EMAIL", "ops@example.com")
	t.Setenv("KONTINUUM_ACME_SERVER", "https://acme-staging-v02.api.letsencrypt.org/directory")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "ops@example.com", cfg.ACME.Email)
	assert.Equal(t, "https://acme-staging-v02.api.letsencrypt.org/directory", cfg.ACME.Server)
}

func TestLoadFallsBackToACMEDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Empty(t, cfg.ACME.Email)
	assert.Equal(t, "https://acme-v02.api.letsencrypt.org/directory", cfg.ACME.Server)
}

func TestLoadReadsDNSEnvVars(t *testing.T) {
	t.Setenv("KONTINUUM_SERVER_DNS_DOMAIN", "example.com")
	t.Setenv("KONTINUUM_SERVER_DNS_PROVIDER", "route53")
	t.Setenv("KONTINUUM_SERVER_DNS_CREDENTIAL", "AKIAEXAMPLE:secret")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "example.com", cfg.Server.DNS.Domain)
	assert.Equal(t, "route53", cfg.Server.DNS.Provider)
	assert.Equal(t, "AKIAEXAMPLE:secret", cfg.Server.DNS.Credential)
}

func TestRedactStripsDNSCredential(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.Server.DNS.Domain = "example.com"
	cfg.Server.DNS.Provider = "route53"
	cfg.Server.DNS.Credential = "AKIAEXAMPLE:secret"

	redacted := cfg.Redact()

	assert.Equal(t, "example.com", redacted.Server.DNS.Domain)
	assert.Equal(t, "route53", redacted.Server.DNS.Provider)
	assert.Empty(t, redacted.Server.DNS.Credential)
}
