package fleet

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

var (
	// ErrInvalidFleet wraps every validation failure Validate reports.
	ErrInvalidFleet = errors.New("invalid fleet")
	// ErrVersionMismatch is Validate's sentinel for a spec.version other
	// than the CLI's own.
	ErrVersionMismatch = errors.New("spec.version must equal this CLI's version")
	// ErrUnknownACMEProvider is Validate's sentinel for an acme.provider
	// that isn't a key of acme.providers.
	ErrUnknownACMEProvider = errors.New("acme.provider is not one of acme.providers")
)

// Validate checks everything Load leaves to the merged result: required
// fields, the version check against cliVersion, the ACME provider
// selection, auth, the zone list and its instances. Every failure is
// reported, not just the first, each wrapping ErrInvalidFleet. Checks
// that need the network — the storage mode, and instance addresses being
// unique once resolved — run in kontinuum up itself.
func Validate(fleet *Fleet, cliVersion string) error {
	var errs []error

	report := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidFleet, fmt.Sprintf(format, args...)))
	}

	validateMetadata(fleet, cliVersion, report)
	validateStorage(fleet.Spec.Storage, report)
	validateACME(fleet.Spec.ACME, report)
	validateAuth(fleet.Spec.Auth, report)
	validateZones(fleet.Spec.Zones, report)

	validateDNS(fleet, report)

	if fleet.Spec.Frontend.Host == "" {
		report("spec.frontend.host is required")
	}

	if fleet.Spec.DNS.Domain == "" {
		report("spec.dns.domain is required")
	}

	if fleet.Spec.DNS.Provider == "" {
		report("spec.dns.provider is required, with a DNS-01 solver: the frontend certificate is issued via DNS-01")
	}

	return errors.Join(errs...)
}

// validateDNS runs the same DNS checks serve runs at startup on the
// configuration the fleet maps onto, plus the DNS-01 solver's own
// credential keys, so a missing key fails up before any zone is created
// rather than on every zone's certificate.
func validateDNS(fleet *Fleet, report reporter) {
	cfg, err := fleet.ToConfig()
	if err != nil {
		report("%s", err.Error())

		return
	}

	err = cfg.ValidateDNS()
	if err != nil {
		report("%s", err.Error())

		return
	}

	if fleet.Spec.DNS.Provider == "" {
		return
	}

	_, err = addon.DNS01Solver(fleet.Spec.DNS.Provider, "", fleet.Spec.DNS.Credential)
	if err != nil {
		report("spec.dns.credential: %s", err.Error())
	}
}

// reporter records one validation failure.
type reporter func(format string, args ...any)

func validateMetadata(fleet *Fleet, cliVersion string, report reporter) {
	if fleet.Metadata.Name == "" {
		report("metadata.name is required")
	}

	if fleet.Spec.Version == "" {
		report("spec.version is required")
	} else if fleet.Spec.Version != cliVersion {
		report("%s: spec.version is %q, this CLI is %q; run the CLI matching the fleet's version",
			ErrVersionMismatch, fleet.Spec.Version, cliVersion)
	}
}

func validateStorage(storage Storage, report reporter) {
	if storage.URI == "" {
		report("spec.storage.uri is required")
	}

	mode, err := config.ParseStorageMode(storage.Mode)
	if err != nil {
		report("spec.storage.mode: %s", err.Error())

		return
	}

	switch mode {
	case config.StorageModeProxy:
		report("spec.storage.mode Proxy is not supported by kontinuum up, which needs storage every zone can reach")
	case config.StorageModeNATS:
		report("spec.storage.mode NATS is not supported by kontinuum up yet")
	case config.StorageModeAuto, config.StorageModeDirect:
	}
}

func validateACME(acme ACME, report reporter) {
	if acme.Provider == "" {
		report("spec.acme.provider is required")
	} else if _, ok := acme.Providers[acme.Provider]; !ok {
		report("%s: %q, declared: %s", ErrUnknownACMEProvider, acme.Provider, declaredProviders(acme.Providers))
	}

	for _, name := range declaredProviderNames(acme.Providers) {
		provider := acme.Providers[name]
		path := "spec.acme.providers." + name

		if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
			report("%s: provider names must be DNS-1123 labels: %s", path, errs[0])
		}

		if provider.Server == "" {
			report("%s.server is required", path)
		}

		if provider.Email == "" {
			report("%s.email is required", path)
		}

		binding := provider.ExternalAccountBinding
		if binding != nil && (binding.KeyID == "") != (binding.HMACKey == "") {
			report("%s.externalAccountBinding: keyID and hmacKey must both be set, or both be absent", path)
		}
	}
}

func validateAuth(auth Auth, report reporter) {
	switch {
	case auth.OIDC != nil && auth.InsecureAllowAnonymous:
		report("spec.auth.oidc and spec.auth.insecureAllowAnonymous are mutually exclusive")
	case auth.OIDC == nil && !auth.InsecureAllowAnonymous:
		report("spec.auth needs either oidc or insecureAllowAnonymous: true")
	case auth.OIDC != nil:
		if auth.OIDC.IssuerURL == "" {
			report("spec.auth.oidc.issuerURL is required")
		}

		if len(auth.OIDC.AdminGroups) == 0 {
			report("spec.auth.oidc.adminGroups needs at least one group, or nobody could administer the fleet")
		}
	}
}

func validateZones(zones []Zone, report reporter) {
	if len(zones) == 0 {
		report("spec.zones needs at least one zone")
	}

	seen := map[string]int{}

	for index, zone := range zones {
		path := "spec.zones[" + strconv.Itoa(index) + "]"

		for field, value := range map[string]string{"region": zone.Region, "zone": zone.Zone} {
			if errs := validation.IsDNS1123Label(value); len(errs) > 0 {
				report("%s.%s %q: %s", path, field, value, errs[0])
			}
		}

		if first, duplicate := seen[zone.Name()]; duplicate {
			report("%s: zone %s is already declared by spec.zones[%d]", path, zone.Name(), first)
		}

		seen[zone.Name()] = index

		err := zonedomain.ValidateInstanceCount(len(zone.Instances))
		if err != nil {
			report("%s.instances: %s", path, err.Error())
		}

		for instanceIndex, instance := range zone.Instances {
			if instance.Address == "" {
				report("%s.instances[%d].address is required", path, instanceIndex)
			}
		}
	}
}

// declaredProviderNames returns providers' keys, sorted.
func declaredProviderNames(providers map[string]ACMEProvider) []string {
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// declaredProviders lists providers' keys for an error message.
func declaredProviders(providers map[string]ACMEProvider) string {
	names := declaredProviderNames(providers)
	if len(names) == 0 {
		return "none"
	}

	return strings.Join(names, ", ")
}

// ToConfig maps the fleet onto the configuration every zone's
// kontinuum-env is written from — one-to-one, see Fleet. Only the selected
// ACME provider is carried over; the others never leave the CLI.
func (f *Fleet) ToConfig() (*config.Config, error) {
	cfg := &config.Config{}
	cfg.Defaults()

	spec := f.Spec

	cfg.Server.Storage = spec.Storage.URI
	cfg.Server.StorageMode = string(config.StorageModeAuto)

	if spec.Storage.Mode != "" {
		cfg.Server.StorageMode = spec.Storage.Mode
	}

	cfg.Server.Frontend.Host = spec.Frontend.Host
	cfg.Server.DNS.Domain = spec.DNS.Domain
	cfg.Server.DNS.Provider = spec.DNS.Provider

	if len(spec.DNS.Credential) > 0 {
		credential, err := yaml.Marshal(spec.DNS.Credential)
		if err != nil {
			return nil, fmt.Errorf("failed to encode spec.dns.credential: %w", err)
		}

		cfg.Server.DNS.Credential = string(credential)
	}

	if provider, ok := spec.ACME.Providers[spec.ACME.Provider]; ok {
		cfg.ACME.Server = provider.Server
		cfg.ACME.Email = provider.Email
		cfg.ACME.InsecureSkipVerify = strconv.FormatBool(provider.InsecureSkipVerify)

		if provider.ExternalAccountBinding != nil {
			cfg.ACME.ExternalAccountBinding.KeyID = provider.ExternalAccountBinding.KeyID
			cfg.ACME.ExternalAccountBinding.HMACKey = provider.ExternalAccountBinding.HMACKey
		}
	}

	cfg.InsecureAllowAnonymous = strconv.FormatBool(spec.Auth.InsecureAllowAnonymous)

	if oidc := spec.Auth.OIDC; oidc != nil {
		cfg.OIDC.IssuerURL = oidc.IssuerURL
		cfg.OIDC.AdminGroups = strings.Join(oidc.AdminGroups, ",")

		if oidc.ClientID != "" {
			cfg.OIDC.ClientID = oidc.ClientID
		}

		if spec.Frontend.Host != "" {
			cfg.OIDC.RedirectURL = "https://" + spec.Frontend.Host + "/app"
		}
	}

	return cfg, nil
}
