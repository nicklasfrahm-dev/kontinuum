package config

import "errors"

// ErrIncompleteExternalAccountBinding is ValidateACME's sentinel for an
// ACME external account binding with only one of its key ID and MAC key
// set.
var ErrIncompleteExternalAccountBinding = errors.New(
	"KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_KEY_ID and KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_HMAC_KEY " +
		"must both be set, or both be empty")

// ValidateACME checks that the ACME external account binding is either
// fully configured or not at all, so a half-set binding fails at startup
// rather than as a rejected ACME registration on every zone.
func (c *Config) ValidateACME() error {
	binding := c.ACME.ExternalAccountBinding
	if (binding.KeyID == "") != (binding.HMACKey == "") {
		return ErrIncompleteExternalAccountBinding
	}

	return nil
}
