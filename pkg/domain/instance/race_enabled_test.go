//go:build race

package instance_test

// raceEnabled is true when this test binary was built with -race — see
// TestZoneJoinCRDsApplyAndRoundTrip's skip. Mirrors
// pkg/domain/registry's identical helper, duplicated here since Go test
// helpers don't cross package boundaries.
const raceEnabled = true
