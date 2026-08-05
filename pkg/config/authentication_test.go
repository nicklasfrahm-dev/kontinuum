package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

func TestValidateAuthenticationStartsNormallyWithOIDC(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.InsecureAllowAnonymous = "false"
	cfg.OIDC.IssuerURL = "https://auth.example.com"

	anonymous, err := cfg.ValidateAuthentication()
	require.NoError(t, err)
	assert.False(t, anonymous)
}

func TestValidateAuthenticationFailsWithNeitherOIDCNorAnonymous(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.InsecureAllowAnonymous = "false"

	_, err := cfg.ValidateAuthentication()
	require.ErrorIs(t, err, config.ErrAuthenticationNotConfigured)
}

func TestValidateAuthenticationWarnsWithAnonymousOnly(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.InsecureAllowAnonymous = "true"

	anonymous, err := cfg.ValidateAuthentication()
	require.NoError(t, err)
	assert.True(t, anonymous)
}

func TestValidateAuthenticationFailsWithBothOIDCAndAnonymous(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.InsecureAllowAnonymous = "true"
	cfg.OIDC.IssuerURL = "https://auth.example.com"

	_, err := cfg.ValidateAuthentication()
	require.ErrorIs(t, err, config.ErrAnonymousAccessWithOIDC)
}

func TestValidateAuthenticationFailsWithInvalidBoolValue(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.InsecureAllowAnonymous = "yes-please"

	_, err := cfg.ValidateAuthentication()
	require.ErrorIs(t, err, config.ErrInvalidInsecureAllowAnonymous)
}
