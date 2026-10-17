package fleet_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/fleet"
)

const (
	// firstIP is the first zone's instance address.
	firstIP = "10.0.0.1"
	// zoneB is the second zone's name within region eu.
	zoneB = "1b"
)

func desiredZone(zone string, ips ...string) fleet.DesiredZone {
	instances := make([]fleet.Instance, 0, len(ips))
	for _, ip := range ips {
		instances = append(instances, fleet.Instance{Address: ip})
	}

	return fleet.DesiredZone{Zone: fleet.Zone{Region: "eu", Zone: zone, Instances: instances}, IPs: ips}
}

func observedZone(zone string, ips ...string) fleet.ObservedZone {
	return fleet.ObservedZone{Name: "eu-" + zone, Region: "eu", Zone: zone, IPs: ips}
}

// planConfig is the configuration the desired fleet maps onto, and a
// Kontinuum reporting it as running — no settings drift between them.
func planConfig(t *testing.T) (*config.Config, v1alpha2.Kontinuum) {
	t.Helper()

	cfg := &config.Config{}
	cfg.Defaults()
	cfg.Server.DNS.Domain = "example.com"

	return cfg, v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a"},
		Status:     v1alpha2.KontinuumStatus{Version: testCLIVersion, Config: v1alpha2.KontinuumConfigStatus(cfg.Redact())},
	}
}

func TestPlanCreatesEveryZoneOfANewFleet(t *testing.T) {
	t.Parallel()

	cfg, _ := planConfig(t)

	plan := fleet.NewPlan([]fleet.DesiredZone{desiredZone("1a", firstIP), desiredZone("1b", "10.0.0.2")},
		cfg, testCLIVersion, fleet.Observed{}, false)

	assert.True(t, plan.NewFleet)
	assert.Len(t, plan.Create, 2)
	assert.True(t, plan.Changes())
	assert.False(t, plan.Upgrade(testCLIVersion))
}

func TestPlanIsANoOpForAnUpToDateFleet(t *testing.T) {
	t.Parallel()

	cfg, kontinuum := planConfig(t)

	plan := fleet.NewPlan([]fleet.DesiredZone{desiredZone("1a", firstIP), desiredZone("1b", "10.0.0.2")},
		cfg, testCLIVersion, fleet.Observed{
			Zones:      []fleet.ObservedZone{observedZone("1a", firstIP), observedZone("1b", "10.0.0.2")},
			Kontinuums: []v1alpha2.Kontinuum{kontinuum},
		}, false)

	assert.False(t, plan.Pending())
	assert.False(t, plan.Changes())

	var out bytes.Buffer
	plan.Print(&out)
	assert.Equal(t, "Fleet is up to date.\n", out.String())
}

func TestPlanAddsMissingZonesAndReportsUnmanagedOnes(t *testing.T) {
	t.Parallel()

	cfg, kontinuum := planConfig(t)
	desired := []fleet.DesiredZone{desiredZone("1a", firstIP), desiredZone("1c", "10.0.0.3")}
	observed := fleet.Observed{
		Zones:      []fleet.ObservedZone{observedZone("1a", firstIP), observedZone("1b", "10.0.0.2")},
		Kontinuums: []v1alpha2.Kontinuum{kontinuum},
	}

	plan := fleet.NewPlan(desired, cfg, testCLIVersion, observed, false)

	require.Len(t, plan.Create, 1)
	assert.Equal(t, "eu-1c", plan.Create[0].Name())
	assert.Equal(t, []string{"eu-" + zoneB}, plan.Unmanaged, "a zone missing from the file is reported, never deleted")
	assert.Empty(t, plan.Prune)
	assert.False(t, plan.NewFleet)

	pruning := fleet.NewPlan(desired, cfg, testCLIVersion, observed, true)
	assert.Equal(t, []string{"eu-" + zoneB}, pruning.Prune)
	assert.Empty(t, pruning.Unmanaged)
}

func TestPlanReportsInstanceDrift(t *testing.T) {
	t.Parallel()

	cfg, kontinuum := planConfig(t)

	tests := []struct {
		name     string
		desired  []string
		observed []string
		drift    string
	}{
		{
			name: "added", desired: []string{firstIP, "10.0.0.2", "10.0.0.3"}, observed: []string{firstIP},
			drift: "declared but not in the fleet: [10.0.0.2 10.0.0.3]",
		},
		{
			name: "removed", desired: []string{firstIP}, observed: []string{firstIP, "10.0.0.2", "10.0.0.3"},
			drift: "in the fleet but not declared: [10.0.0.2 10.0.0.3]",
		},
		{
			name: "changed", desired: []string{"10.0.0.9"}, observed: []string{firstIP},
			drift: "declared but not in the fleet: [10.0.0.9]; in the fleet but not declared: [10.0.0.1]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plan := fleet.NewPlan([]fleet.DesiredZone{desiredZone("1a", test.desired...)}, cfg, testCLIVersion,
				fleet.Observed{
					Zones:      []fleet.ObservedZone{observedZone("1a", test.observed...)},
					Kontinuums: []v1alpha2.Kontinuum{kontinuum},
				}, false)

			assert.Empty(t, plan.Create, "a changed zone is never recreated")
			require.Len(t, plan.Drift, 1)
			assert.Contains(t, plan.Drift[0], test.drift)
			assert.True(t, plan.Pending())
			assert.False(t, plan.Changes())
		})
	}
}

func TestPlanReportsSettingsDriftAndUpgrade(t *testing.T) {
	t.Parallel()

	cfg, kontinuum := planConfig(t)
	kontinuum.Status.Version = "v0.36.0"
	kontinuum.Status.Config.Server.DNS.Domain = "example.org"

	plan := fleet.NewPlan([]fleet.DesiredZone{desiredZone("1a", firstIP)}, cfg, testCLIVersion,
		fleet.Observed{
			Zones:      []fleet.ObservedZone{observedZone("1a", firstIP)},
			Kontinuums: []v1alpha2.Kontinuum{kontinuum},
		}, false)

	assert.True(t, plan.Upgrade(testCLIVersion))
	assert.Equal(t, "v0.36.0", plan.FleetVersion)
	require.Len(t, plan.Drift, 2)
	assert.Contains(t, plan.Drift[0], "fleet runs v0.36.0")
	assert.Contains(t, plan.Drift[1], "KONTINUUM_SERVER_DNS_DOMAIN")

	var out bytes.Buffer
	plan.Print(&out)
	assert.Contains(t, out.String(), "~ drift: KONTINUUM_SERVER_DNS_DOMAIN")
}

func TestPlanFleetVersionIsTheHighest(t *testing.T) {
	t.Parallel()

	cfg, kontinuum := planConfig(t)
	older := kontinuum
	older.Name = "eu-" + zoneB
	older.Status.Version = "v0.9.0"

	plan := fleet.NewPlan(nil, cfg, testCLIVersion, fleet.Observed{Kontinuums: []v1alpha2.Kontinuum{older, kontinuum}},
		false)
	assert.Equal(t, testCLIVersion, plan.FleetVersion, "v0.37.0 sorts above v0.9.0 as semver, not as a string")
}
