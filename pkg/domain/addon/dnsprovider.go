package addon

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"

	acmev1 "github.com/cert-manager/cert-manager/pkg/apis/acme/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

var (
	// ErrNoDNS01Solver is DNS01Solver's sentinel for a provider
	// cert-manager has no built-in DNS-01 solver for.
	ErrNoDNS01Solver = errors.New("dns provider has no built-in cert-manager dns-01 solver")
	// ErrMissingDNSCredentialKey is DNS01Solver's sentinel for a
	// KONTINUUM_SERVER_DNS_CREDENTIAL lacking a key its provider's solver
	// reads.
	ErrMissingDNSCredentialKey = errors.New("dns credential is missing keys")
	// ErrUnknownTSIGAlgorithm is DNS01Solver's sentinel for an RFC2136
	// TSIG algorithm cert-manager doesn't support.
	ErrUnknownTSIGAlgorithm = errors.New("unknown rfc2136 tsig algorithm")
)

// externalDNSProviderNames maps each config.DNSProvider onto external-dns's
// own provider.name.
func externalDNSProviderNames() map[config.DNSProvider]string {
	return map[config.DNSProvider]string{
		config.DNSProviderCloudflare:   "cloudflare",
		config.DNSProviderAWS:          "aws",
		config.DNSProviderAzure:        "azure",
		config.DNSProviderGoogle:       "google",
		config.DNSProviderDigitalOcean: "digitalocean",
		config.DNSProviderRFC2136:      "rfc2136",
		config.DNSProviderPowerDNS:     "pdns",
	}
}

// ExternalDNSProviderName translates KONTINUUM_SERVER_DNS_PROVIDER's
// PascalCase value into external-dns's own provider.name, rejecting an
// unknown one (see config.ParseDNSProvider).
func ExternalDNSProviderName(raw string) (string, error) {
	provider, err := config.ParseDNSProvider(raw)
	if err != nil {
		return "", fmt.Errorf("failed to parse dns provider: %w", err)
	}

	name, ok := externalDNSProviderNames()[provider]
	if !ok {
		return "", fmt.Errorf("%w %q", config.ErrUnknownDNSProvider, raw)
	}

	return name, nil
}

// The KONTINUUM_SERVER_DNS_CREDENTIAL keys each DNS-01 solver below reads.
// They're the same environment variables external-dns itself reads for
// that provider where it reads any, so one credential serves both.
const (
	// Cloudflare: an API token with Zone:DNS:Edit.
	envCloudflareAPI = "CF_API_TOKEN"
	// AWS: an access key for Route 53; the region is optional.
	envAWSAccessKeyID = "AWS_ACCESS_KEY_ID"
	envAWSAccessKey   = "AWS_SECRET_ACCESS_KEY"
	envAWSRegion      = "AWS_REGION"
	// Azure: a service principal and the DNS zone's resource group.
	envAzureClientID       = "AZURE_CLIENT_ID"
	envAzureClientKey      = "AZURE_CLIENT_SECRET"
	envAzureTenantID       = "AZURE_TENANT_ID"
	envAzureSubscriptionID = "AZURE_SUBSCRIPTION_ID"
	envAzureResourceGroup  = "AZURE_RESOURCE_GROUP"
	// Google: the project and a service account key in JSON.
	envGoogleProject        = "EXTERNAL_DNS_GOOGLE_PROJECT"
	envGoogleServiceAccount = "GOOGLE_SERVICE_ACCOUNT_KEY"
	// DigitalOcean: an API token.
	envDigitalOceanAPI = "DO_TOKEN"
	// RFC2136: the nameserver and a TSIG key with its algorithm; the port
	// is optional, defaulting to 53. external-dns exits on startup without
	// an algorithm, so unlike cert-manager it has no default to fall back on.
	envRFC2136Host         = "EXTERNAL_DNS_RFC2136_HOST"
	envRFC2136Port         = "EXTERNAL_DNS_RFC2136_PORT"
	envRFC2136KeyName      = "EXTERNAL_DNS_RFC2136_TSIG_KEYNAME"
	envRFC2136Key          = "EXTERNAL_DNS_RFC2136_TSIG_SECRET"
	envRFC2136KeyAlgorithm = "EXTERNAL_DNS_RFC2136_TSIG_SECRET_ALG"
	defaultRFC2136Port     = "53"
)

// DNS01Solver builds cert-manager's DNS-01 solver for raw (a
// KONTINUUM_SERVER_DNS_PROVIDER value) from keys, the parsed
// KONTINUUM_SERVER_DNS_CREDENTIAL. Confidential keys are referenced from
// secretName — a Secret in cert-manager's own namespace holding exactly
// keys — rather than inlined; non-confidential ones such as a project or
// region are read from keys directly. Fails with ErrMissingDNSCredentialKey
// naming every key the provider needs that keys lacks, and with
// ErrNoDNS01Solver for PowerDNS.
func DNS01Solver(raw, secretName string, keys map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	provider, err := config.ParseDNSProvider(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dns provider: %w", err)
	}

	builder, ok := dns01SolverBuilders()[provider]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoDNS01Solver, raw)
	}

	err = requireEnvKeys(provider, keys, builder.required...)
	if err != nil {
		return nil, err
	}

	return builder.build(secretRefs(secretName), keys)
}

// dns01SolverBuilder is one provider's required credential keys and solver
// builder — see DNS01Solver.
type dns01SolverBuilder struct {
	required []string
	build    func(ref secretRefs, keys map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error)
}

// secretRefs builds cmmeta.SecretKeySelectors into one Secret.
type secretRefs string

// key references key in the Secret.
func (s secretRefs) key(key string) cmmeta.SecretKeySelector {
	return cmmeta.SecretKeySelector{LocalObjectReference: cmmeta.LocalObjectReference{Name: string(s)}, Key: key}
}

// keyPtr is key, as a pointer, for the solver fields that take one.
func (s secretRefs) keyPtr(key string) *cmmeta.SecretKeySelector {
	ref := s.key(key)

	return &ref
}

// dns01SolverBuilders returns every provider's DNS-01 solver builder.
// PowerDNS has none.
func dns01SolverBuilders() map[config.DNSProvider]dns01SolverBuilder {
	return map[config.DNSProvider]dns01SolverBuilder{
		config.DNSProviderCloudflare: {required: []string{envCloudflareAPI}, build: cloudflareSolver},
		config.DNSProviderAWS: {
			required: []string{envAWSAccessKeyID, envAWSAccessKey}, build: route53Solver,
		},
		config.DNSProviderAzure: {required: azureEnvKeys(), build: azureSolver},
		config.DNSProviderGoogle: {
			required: []string{envGoogleProject, envGoogleServiceAccount}, build: cloudDNSSolver,
		},
		config.DNSProviderDigitalOcean: {required: []string{envDigitalOceanAPI}, build: digitalOceanSolver},
		config.DNSProviderRFC2136: {
			required: []string{envRFC2136Host, envRFC2136KeyName, envRFC2136Key, envRFC2136KeyAlgorithm},
			build:    rfc2136Solver,
		},
	}
}

// azureEnvKeys is every key the Azure DNS solver needs.
func azureEnvKeys() []string {
	return []string{
		envAzureClientID, envAzureClientKey, envAzureTenantID,
		envAzureSubscriptionID, envAzureResourceGroup,
	}
}

func cloudflareSolver(ref secretRefs, _ map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	return &acmev1.ACMEChallengeSolverDNS01{
		Cloudflare: &acmev1.ACMEIssuerDNS01ProviderCloudflare{APIToken: ref.keyPtr(envCloudflareAPI)},
	}, nil
}

func route53Solver(ref secretRefs, keys map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	return &acmev1.ACMEChallengeSolverDNS01{
		Route53: &acmev1.ACMEIssuerDNS01ProviderRoute53{
			Region:            keys[envAWSRegion],
			SecretAccessKeyID: ref.keyPtr(envAWSAccessKeyID),
			SecretAccessKey:   ref.key(envAWSAccessKey),
		},
	}, nil
}

func azureSolver(ref secretRefs, keys map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	return &acmev1.ACMEChallengeSolverDNS01{
		AzureDNS: &acmev1.ACMEIssuerDNS01ProviderAzureDNS{
			ClientID:          keys[envAzureClientID],
			ClientSecret:      ref.keyPtr(envAzureClientKey),
			SubscriptionID:    keys[envAzureSubscriptionID],
			TenantID:          keys[envAzureTenantID],
			ResourceGroupName: keys[envAzureResourceGroup],
		},
	}, nil
}

func cloudDNSSolver(ref secretRefs, keys map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	return &acmev1.ACMEChallengeSolverDNS01{
		CloudDNS: &acmev1.ACMEIssuerDNS01ProviderCloudDNS{
			Project:        keys[envGoogleProject],
			ServiceAccount: ref.keyPtr(envGoogleServiceAccount),
		},
	}, nil
}

func digitalOceanSolver(ref secretRefs, _ map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	return &acmev1.ACMEChallengeSolverDNS01{
		DigitalOcean: &acmev1.ACMEIssuerDNS01ProviderDigitalOcean{Token: ref.key(envDigitalOceanAPI)},
	}, nil
}

func rfc2136Solver(ref secretRefs, keys map[string]string) (*acmev1.ACMEChallengeSolverDNS01, error) {
	port := keys[envRFC2136Port]
	if port == "" {
		port = defaultRFC2136Port
	}

	algorithm, err := tsigAlgorithm(keys[envRFC2136KeyAlgorithm])
	if err != nil {
		return nil, err
	}

	return &acmev1.ACMEChallengeSolverDNS01{
		RFC2136: &acmev1.ACMEIssuerDNS01ProviderRFC2136{
			Nameserver:    net.JoinHostPort(keys[envRFC2136Host], port),
			TSIGKeyName:   keys[envRFC2136KeyName],
			TSIGAlgorithm: algorithm,
			TSIGSecret:    ref.key(envRFC2136Key),
		},
	}, nil
}

// tsigAlgorithm translates external-dns's TSIG algorithm name (e.g.
// hmac-sha256) into cert-manager's (HMACSHA256).
func tsigAlgorithm(raw string) (string, error) {
	algorithms := map[string]string{
		"hmac-md5":    "HMACMD5",
		"hmac-sha1":   "HMACSHA1",
		"hmac-sha256": "HMACSHA256",
		"hmac-sha512": "HMACSHA512",
	}

	algorithm, ok := algorithms[strings.ToLower(raw)]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownTSIGAlgorithm, raw)
	}

	return algorithm, nil
}

// requireEnvKeys fails with ErrMissingDNSCredentialKey naming every
// one of required that keys lacks, or holds empty.
func requireEnvKeys(provider config.DNSProvider, keys map[string]string, required ...string) error {
	var missing []string

	for _, key := range required {
		if keys[key] == "" {
			missing = append(missing, key)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	sort.Strings(missing)

	return fmt.Errorf("%w: %s needs %s", ErrMissingDNSCredentialKey, provider, strings.Join(missing, ", "))
}
