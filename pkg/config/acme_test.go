package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

const (
	// testKeyID and testHMACKey are a fake external account binding, not
	// a real one.
	testKeyID   = "kid-1"
	testHMACKey = "example-hmac-key"
)

func TestValidateACMEExternalAccountBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyID   string
		hmacKey string
		valid   bool
	}{
		{name: "neither", valid: true},
		{name: "both", keyID: testKeyID, hmacKey: testHMACKey, valid: true},
		{name: "key id only", keyID: testKeyID, valid: false},
		{name: "hmac key only", hmacKey: testHMACKey, valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{}
			cfg.Defaults()
			cfg.ACME.ExternalAccountBinding.KeyID = test.keyID
			cfg.ACME.ExternalAccountBinding.HMACKey = test.hmacKey

			err := cfg.ValidateACME()
			if test.valid {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, config.ErrIncompleteExternalAccountBinding)
		})
	}
}

func TestLoadReadsExternalAccountBindingEnvVars(t *testing.T) {
	t.Setenv("KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_KEY_ID", testKeyID)
	t.Setenv("KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_HMAC_KEY", testHMACKey)

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, testKeyID, cfg.ACME.ExternalAccountBinding.KeyID)
	assert.Equal(t, testHMACKey, cfg.ACME.ExternalAccountBinding.HMACKey)
}

func TestExternalAccountBindingHMACKeyIsSecretAndRedacted(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.Defaults()
	cfg.ACME.ExternalAccountBinding.KeyID = testKeyID
	cfg.ACME.ExternalAccountBinding.HMACKey = testHMACKey

	secrets := map[string]bool{}
	for _, envVar := range cfg.EnvVars() {
		secrets[envVar.Name] = envVar.Secret
	}

	assert.True(t, secrets["KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_HMAC_KEY"])
	assert.False(t, secrets["KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_KEY_ID"])

	redacted := cfg.Redact()
	assert.Empty(t, redacted.ACME.ExternalAccountBinding.HMACKey)
	assert.Equal(t, testKeyID, redacted.ACME.ExternalAccountBinding.KeyID)
}
