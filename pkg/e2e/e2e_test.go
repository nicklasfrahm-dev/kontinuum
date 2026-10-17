package e2e_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/pkg/e2e"
)

var (
	errRemoving = errors.New("removal of container abc is already in progress")
	errZombie   = errors.New("container abc PID 1 is zombie and can not be killed")
	errMissing  = errors.New("no such container")
)

func TestIsBenignTerminateError(t *testing.T) {
	t.Parallel()

	assert.True(t, e2e.IsBenignTerminateError(errRemoving))
	assert.True(t, e2e.IsBenignTerminateError(errZombie))
	assert.False(t, e2e.IsBenignTerminateError(errMissing))
}

func TestBindConfig(t *testing.T) {
	t.Parallel()

	namedConf, zoneFile := e2e.BindConfig("e2e.test", "kontinuum.", "c2VjcmV0")

	assert.Contains(t, namedConf, `key "kontinuum." { algorithm hmac-sha256; secret "c2VjcmV0"; };`)
	assert.Contains(t, namedConf, `zone "e2e.test"`)
	assert.Contains(t, namedConf, "update-policy { grant kontinuum. zonesub ANY; };")
	assert.Contains(t, namedConf, `allow-transfer { key "kontinuum."; };`)
	assert.Contains(t, zoneFile, "@ IN SOA ns.e2e.test. admin.e2e.test.")
}

func TestSkipWithoutOptIn(t *testing.T) {
	t.Setenv("KONTINUUM_TEST_E2E", "")

	skipped := t.Run("gated", func(t *testing.T) {
		e2e.Skip(t)
		t.Fatal("Skip should have skipped this test")
	})
	assert.True(t, skipped, "a skipped subtest counts as passed")
}
