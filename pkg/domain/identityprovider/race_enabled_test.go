//go:build race

package identityprovider_test

// raceEnabled is true when this test binary was built with -race — see
// TestIdentityProvidersAuthenticateAgainstAPIServer's skip. Mirrors
// pkg/domain/instance's identical helper, duplicated here since Go test
// helpers don't cross package boundaries.
const raceEnabled = true
