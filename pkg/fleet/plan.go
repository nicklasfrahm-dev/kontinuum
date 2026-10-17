package fleet

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"golang.org/x/mod/semver"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

// DesiredZone is one zone of a Fleet with its instance addresses resolved
// to IPs, the way they're stored on its seed Instances.
type DesiredZone struct {
	Zone

	// IPs are Zone.Instances' addresses, resolved, in order.
	IPs []string
}

// ObservedZone is one zone found in the fleet's shared storage.
type ObservedZone struct {
	Name   string
	Region string
	Zone   string
	// IPs are the zone's seed Instances' addresses.
	IPs []string
	// Ready is whether the zone reports Ready.
	Ready bool
}

// Observed is what kontinuum up found in the fleet's shared storage.
type Observed struct {
	Zones []ObservedZone
	// Kontinuums are the fleet's registered processes, whose versions and
	// configuration settings drift is compared against.
	Kontinuums []v1alpha2.Kontinuum
}

// Plan is what kontinuum up will do, and what it will only report — see
// NewPlan.
type Plan struct {
	// Create are the declared zones not in the fleet yet.
	Create []DesiredZone
	// Prune are the zones in the fleet but not in the file that up removes
	// because --prune was given.
	Prune []string
	// Unmanaged are the zones in the fleet but not in the file that up
	// leaves alone, without --prune.
	Unmanaged []string
	// Unready are declared zones already in the fleet but not Ready yet —
	// e.g. created by an earlier, interrupted run. up runs its controllers
	// and waits for them again: in a new hub-less fleet nothing else may
	// be able to reconcile them yet.
	Unready []string
	// Drift describes every declared difference up doesn't apply: changed
	// zone instances, and changed fleet-wide settings.
	Drift []string
	// FleetVersion is the version the fleet's registered processes run,
	// empty when none is registered.
	FleetVersion string
	// NewFleet is true when the fleet had no zones at all.
	NewFleet bool
}

// NewPlan compares the desired zones and configuration with what's
// observed. Zones in the file but not the fleet are created; zones in the
// fleet but not the file are pruned with prune, reported otherwise, never
// deleted because a line was dropped from a file. Changed instances on an
// existing zone — growing or shrinking a live etcd cluster — and changed
// fleet-wide settings are reported as drift, never applied: there is no
// single process whose environment is the fleet's configuration.
func NewPlan(desired []DesiredZone, cfg *config.Config, version string, observed Observed, prune bool) Plan {
	plan := Plan{NewFleet: len(observed.Zones) == 0, FleetVersion: fleetVersion(observed.Kontinuums)}

	existing := map[string]ObservedZone{}
	for _, zone := range observed.Zones {
		existing[zone.Name] = zone
	}

	declared := map[string]bool{}

	for _, zone := range desired {
		declared[zone.Name()] = true

		current, found := existing[zone.Name()]
		if !found {
			plan.Create = append(plan.Create, zone)

			continue
		}

		if !current.Ready {
			plan.Unready = append(plan.Unready, zone.Name())
		}

		plan.Drift = append(plan.Drift, instanceDrift(zone, current)...)
	}

	for _, zone := range observed.Zones {
		if declared[zone.Name] {
			continue
		}

		if prune {
			plan.Prune = append(plan.Prune, zone.Name)
		} else {
			plan.Unmanaged = append(plan.Unmanaged, zone.Name)
		}
	}

	sort.Strings(plan.Prune)
	sort.Strings(plan.Unmanaged)

	if plan.FleetVersion != "" && plan.FleetVersion != version {
		plan.Drift = append(plan.Drift, fmt.Sprintf(
			"fleet runs %s, kontinuum.yml declares %s: upgrading a fleet is not supported by up", plan.FleetVersion, version))
	}

	plan.Drift = append(plan.Drift, settingsDrift(cfg, observed.Kontinuums)...)

	return plan
}

// Upgrade reports whether the fleet runs a different version than the one
// declared: up then only applies zone objects, without running its own
// controllers against a fleet running different controller code.
func (p Plan) Upgrade(version string) bool {
	return p.FleetVersion != "" && p.FleetVersion != version
}

// Pending reports whether anything is to be done or reported: zones to
// create, prune or wait for, unmanaged zones, or drift.
func (p Plan) Pending() bool {
	return p.Changes() || len(p.Unmanaged) > 0 || len(p.Drift) > 0
}

// Changes reports whether up has anything to apply or converge: zones to
// create or prune, or declared zones not Ready yet.
func (p Plan) Changes() bool {
	return len(p.Create) > 0 || len(p.Prune) > 0 || len(p.Unready) > 0
}

// Print writes the plan for a human to out.
func (p Plan) Print(out io.Writer) {
	if !p.Pending() {
		_, _ = fmt.Fprintln(out, "Fleet is up to date.")

		return
	}

	for _, zone := range p.Create {
		_, _ = fmt.Fprintf(out, "+ create zone %s with instances %v\n", zone.Name(), zone.Addresses())
	}

	for _, name := range p.Prune {
		_, _ = fmt.Fprintf(out, "- remove zone %s\n", name)
	}

	for _, name := range p.Unready {
		_, _ = fmt.Fprintf(out, "~ zone %s isn't Ready yet: resuming\n", name)
	}

	for _, name := range p.Unmanaged {
		_, _ = fmt.Fprintf(out, "! zone %s runs in the fleet but isn't in kontinuum.yml; pass --prune to remove it\n", name)
	}

	for _, drift := range p.Drift {
		_, _ = fmt.Fprintf(out, "~ drift: %s\n", drift)
	}
}

// instanceDrift describes how zone's declared instances differ from the
// fleet's.
func instanceDrift(zone DesiredZone, current ObservedZone) []string {
	var added, missing []string

	for _, ip := range zone.IPs {
		if !slices.Contains(current.IPs, ip) {
			added = append(added, ip)
		}
	}

	for _, ip := range current.IPs {
		if !slices.Contains(zone.IPs, ip) {
			missing = append(missing, ip)
		}
	}

	if len(added) == 0 && len(missing) == 0 {
		return nil
	}

	return []string{fmt.Sprintf(
		"zone %s instances differ (declared but not in the fleet: %v; in the fleet but not declared: %v); "+
			"growing or shrinking a running zone is not supported by up",
		zone.Name(), added, missing)}
}

// fleetVersion is the highest version any registered process reports.
func fleetVersion(kontinuums []v1alpha2.Kontinuum) string {
	versions := make([]string, 0, len(kontinuums))

	for _, kontinuum := range kontinuums {
		if kontinuum.Status.Version != "" {
			versions = append(versions, kontinuum.Status.Version)
		}
	}

	if len(versions) == 0 {
		return ""
	}

	semver.Sort(versions)

	return versions[len(versions)-1]
}

// settingsDrift compares cfg's fleet-wide, non-confidential settings with
// what the first registered process by name reports running with.
// Confidential values (credentials, the storage URI) are redacted from
// every Kontinuum's status, so they can't be compared.
func settingsDrift(cfg *config.Config, kontinuums []v1alpha2.Kontinuum) []string {
	if len(kontinuums) == 0 {
		return nil
	}

	sorted := slices.Clone(kontinuums)
	slices.SortFunc(sorted, func(left, right v1alpha2.Kontinuum) int {
		return strings.Compare(left.Name, right.Name)
	})

	running := sorted[0].Status.Config

	settings := []struct {
		name            string
		declared, fleet string
	}{
		{"KONTINUUM_OIDC_ISSUER_URL", cfg.OIDC.IssuerURL, running.OIDC.IssuerURL},
		{"KONTINUUM_OIDC_CLIENT_ID", cfg.OIDC.ClientID, running.OIDC.ClientID},
		{"KONTINUUM_OIDC_ADMIN_GROUPS", cfg.OIDC.AdminGroups, running.OIDC.AdminGroups},
		{"KONTINUUM_INSECURE_ALLOW_ANONYMOUS", cfg.InsecureAllowAnonymous, running.InsecureAllowAnonymous},
		{"KONTINUUM_ACME_SERVER", cfg.ACME.Server, running.ACME.Server},
		{"KONTINUUM_ACME_EMAIL", cfg.ACME.Email, running.ACME.Email},
		{"KONTINUUM_ACME_INSECURE_SKIP_VERIFY", cfg.ACME.InsecureSkipVerify, running.ACME.InsecureSkipVerify},
		{
			"KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_KEY_ID",
			cfg.ACME.ExternalAccountBinding.KeyID, running.ACME.ExternalAccountBinding.KeyID,
		},
		{"KONTINUUM_SERVER_DNS_DOMAIN", cfg.Server.DNS.Domain, running.Server.DNS.Domain},
		{"KONTINUUM_SERVER_DNS_PROVIDER", cfg.Server.DNS.Provider, running.Server.DNS.Provider},
		{"KONTINUUM_SERVER_FRONTEND_HOST", cfg.Server.Frontend.Host, running.Server.Frontend.Host},
	}

	var drift []string

	for _, setting := range settings {
		if setting.declared != setting.fleet {
			drift = append(drift, fmt.Sprintf("%s is %q in the fleet but %q in kontinuum.yml; "+
				"changing fleet-wide settings is not supported by up", setting.name, setting.fleet, setting.declared))
		}
	}

	return drift
}
