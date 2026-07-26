package registry

import (
	"fmt"
	"os"
	"path/filepath"

	certutil "k8s.io/client-go/util/cert"
)

// ConversionWebhookPort is the port the conversion webhook server listens
// on — wired into libkapi.WithWebhookServer by pkg/cli/serve.go, and used
// here to build CustomResourceDefinition's conversion webhook clientConfig
// URL. Both must agree, since the same process serves and applies both.
const ConversionWebhookPort = 9443

// conversionWebhookDNSName is the only hostname the apiserver ever dials
// the conversion webhook on — see EnsureCRD's doc for why "localhost" (not
// a Service) is correct here.
const conversionWebhookDNSName = "localhost"

const (
	webhookCertFileName = "tls.crt"
	webhookKeyFileName  = "tls.key"
	// webhookCertPerm/webhookKeyPerm: the key is private (owner read/write
	// only); the cert is the public half, safe to be world-readable like
	// any other cert file.
	webhookCertPerm = 0o644
	webhookKeyPerm  = 0o600
	webhookDirPerm  = 0o700
)

// webhookCertDir matches controller-runtime's own webhook.Options default
// CertDir exactly (<temp-dir>/k8s-webhook-server/serving-certs) — not
// configurable, so the certificate EnsureConversionWebhookCert provisions
// here is the same one the webhook server started later, by
// libkapi.WithWebhookServer, ends up serving: its own cert provisioning
// (which this code can't call directly — it's libkapi-internal) checks the
// same well-known path first and reuses whatever it finds there. A
// function, not a package var, purely to avoid a mutable global — the
// path itself is fixed for the life of the process.
func webhookCertDir() string {
	return filepath.Join(os.TempDir(), "k8s-webhook-server", "serving-certs")
}

// EnsureConversionWebhookCert writes a self-signed serving certificate for
// "localhost" to controller-runtime's default webhook CertDir if one
// doesn't already exist there, and returns its PEM bytes — embedded as
// CustomResourceDefinition's conversion webhook CABundle, so the apiserver
// trusts whatever cert the webhook server presents.
//
// This has to run — and the CRD has to be applied with its result — before
// the controller manager (and therefore the webhook server itself) is even
// built: EnsureCRD is a PostStartHook, and WithPostStartHook registrations
// run before the controller manager starts (see EnsureCRD's own doc). So
// the CRD's conversion config must be correct from its very first apply,
// not patched in afterward once a cert happens to exist. Provisioning it
// here first also means libkapi's own (internal, unexported) cert
// provisioning — which runs later, when the manager is built — finds one
// already in place and reuses it instead of generating a second, different
// cert the CRD's CABundle would no longer match.
func EnsureConversionWebhookCert() ([]byte, error) {
	certDir := webhookCertDir()
	certPath := filepath.Join(certDir, webhookCertFileName)
	keyPath := filepath.Join(certDir, webhookKeyFileName)

	existingCert, certErr := os.ReadFile(filepath.Clean(certPath))

	_, keyErr := os.Stat(keyPath)
	if certErr == nil && keyErr == nil {
		return existingCert, nil
	}

	err := os.MkdirAll(certDir, webhookDirPerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook certificate directory %q: %w", certDir, err)
	}

	dnsNames := []string{conversionWebhookDNSName}

	certPEM, keyPEM, err := certutil.GenerateSelfSignedCertKey(dnsNames[0], nil, dnsNames)
	if err != nil {
		return nil, fmt.Errorf("failed to generate self-signed webhook certificate: %w", err)
	}

	err = os.WriteFile(certPath, certPEM, webhookCertPerm)
	if err != nil {
		return nil, fmt.Errorf("failed to write webhook certificate %q: %w", certPath, err)
	}

	err = os.WriteFile(keyPath, keyPEM, webhookKeyPerm)
	if err != nil {
		return nil, fmt.Errorf("failed to write webhook key %q: %w", keyPath, err)
	}

	return certPEM, nil
}
