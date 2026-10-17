package config

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// DNSProvider is the DNS provider a fleet's records are managed through —
// KONTINUUM_SERVER_DNS_PROVIDER (Server.DNS.Provider). pkg/domain/addon
// translates it into external-dns's own provider name and cert-manager's
// DNS-01 solver.
type DNSProvider string

const (
	// DNSProviderCloudflare is Cloudflare DNS.
	DNSProviderCloudflare DNSProvider = "Cloudflare"
	// DNSProviderAWS is AWS Route 53.
	DNSProviderAWS DNSProvider = "AWS"
	// DNSProviderAzure is Azure DNS.
	DNSProviderAzure DNSProvider = "Azure"
	// DNSProviderGoogle is Google Cloud DNS.
	DNSProviderGoogle DNSProvider = "Google"
	// DNSProviderDigitalOcean is DigitalOcean DNS.
	DNSProviderDigitalOcean DNSProvider = "DigitalOcean"
	// DNSProviderRFC2136 is any authoritative server accepting RFC 2136
	// dynamic updates signed with TSIG.
	DNSProviderRFC2136 DNSProvider = "RFC2136"
	// DNSProviderPowerDNS is PowerDNS. cert-manager has no built-in DNS-01
	// solver for it, so it can't be combined with a frontend host.
	DNSProviderPowerDNS DNSProvider = "PowerDNS"
)

var (
	// ErrUnknownDNSProvider is ParseDNSProvider's sentinel for a value that
	// isn't one of the DNSProvider constants.
	ErrUnknownDNSProvider = errors.New("unknown dns provider")
	// ErrFrontendHostNeedsDNS01 is ValidateDNS's sentinel for a frontend
	// host without a DNS provider cert-manager can solve DNS-01 with: a
	// name several zones serve at once can't be validated over HTTP-01.
	ErrFrontendHostNeedsDNS01 = errors.New(
		"KONTINUUM_SERVER_FRONTEND_HOST requires a KONTINUUM_SERVER_DNS_PROVIDER with a DNS-01 solver")
	// ErrFrontendHostOutsideDomain is ValidateDNS's sentinel for a frontend
	// host that isn't KONTINUUM_SERVER_DNS_DOMAIN or one of its subdomains,
	// the only names the DNS provider manages.
	ErrFrontendHostOutsideDomain = errors.New(
		"KONTINUUM_SERVER_FRONTEND_HOST must equal KONTINUUM_SERVER_DNS_DOMAIN or be a subdomain of it")
	// ErrInvalidFrontendHost is ValidateDNS's sentinel for a frontend host
	// that isn't a valid DNS name.
	ErrInvalidFrontendHost = errors.New("KONTINUUM_SERVER_FRONTEND_HOST is not a valid dns name")
)

// DNSProviders returns every DNSProvider, in the order the docs list them.
func DNSProviders() []DNSProvider {
	return []DNSProvider{
		DNSProviderCloudflare, DNSProviderAWS, DNSProviderAzure, DNSProviderGoogle,
		DNSProviderDigitalOcean, DNSProviderRFC2136, DNSProviderPowerDNS,
	}
}

// ParseDNSProvider parses raw as a DNSProvider. Matching is exact, so the
// lowercase external-dns names this field used to take (e.g. "cloudflare")
// are rejected with the list of accepted values. An empty raw means no
// provider is configured and parses as "".
func ParseDNSProvider(raw string) (DNSProvider, error) {
	if raw == "" {
		return "", nil
	}

	for _, provider := range DNSProviders() {
		if string(provider) == raw {
			return provider, nil
		}
	}

	names := make([]string, 0, len(DNSProviders()))
	for _, provider := range DNSProviders() {
		names = append(names, string(provider))
	}

	return "", fmt.Errorf("%w %q, must be one of %s", ErrUnknownDNSProvider, raw, strings.Join(names, ", "))
}

// HasDNS01Solver reports whether cert-manager can solve DNS-01 challenges
// through p with a built-in solver.
func (p DNSProvider) HasDNS01Solver() bool {
	return p != "" && p != DNSProviderPowerDNS
}

// ValidateDNS checks Server.DNS.Provider and Server.Frontend.Host: the
// provider must be a known one, and a frontend host requires a provider
// with a DNS-01 solver and must lie within Server.DNS.Domain. serve and
// kontinuum up reject exactly the same configurations.
func (c *Config) ValidateDNS() error {
	provider, err := ParseDNSProvider(c.Server.DNS.Provider)
	if err != nil {
		return err
	}

	host := c.Server.Frontend.Host
	if host == "" {
		return nil
	}

	if errs := validation.IsDNS1123Subdomain(host); len(errs) > 0 {
		return fmt.Errorf("%w: %q: %s", ErrInvalidFrontendHost, host, errs[0])
	}

	if !provider.HasDNS01Solver() {
		return fmt.Errorf("%w, got %q", ErrFrontendHostNeedsDNS01, provider)
	}

	if !WithinDomain(host, c.Server.DNS.Domain) {
		return fmt.Errorf("%w: %q is outside %q", ErrFrontendHostOutsideDomain, host, c.Server.DNS.Domain)
	}

	return nil
}

// WithinDomain reports whether host equals domain or is one of its
// subdomains. An empty domain contains nothing.
func WithinDomain(host, domain string) bool {
	if domain == "" {
		return false
	}

	return host == domain || strings.HasSuffix(host, "."+domain)
}
