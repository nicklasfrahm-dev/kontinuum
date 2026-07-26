//go:build race

package registry_test

// raceEnabled is true when this test binary was built with -race — see
// TestConversionWebhookBridgesLegacyRegistration's skip.
const raceEnabled = true
