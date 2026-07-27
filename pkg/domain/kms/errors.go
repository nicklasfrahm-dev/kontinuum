package kms

import "errors"

var (
	// ErrInvalidPassphraseSize is returned by Seal when the request data is
	// not exactly constants.PassphraseSize bytes, matching what a real
	// Talos-compatible KMS server validates before sealing.
	ErrInvalidPassphraseSize = errors.New("data must be exactly the passphrase size")
	// ErrSealedDataTooShort is returned by Unseal when the request data is
	// too short to contain a nonce, and therefore couldn't have come from
	// Seal.
	ErrSealedDataTooShort = errors.New("sealed data is shorter than the nonce")
	// ErrAuthenticationFailed is returned by Unseal when the request data
	// fails AES-GCM authentication — either it wasn't sealed by this node's
	// key, or this Server was restarted (losing its keys) since it was
	// sealed.
	ErrAuthenticationFailed = errors.New("failed to authenticate sealed data")
)
