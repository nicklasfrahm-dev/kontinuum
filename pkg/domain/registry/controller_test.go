package registry_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

var errTestHostnameUnavailable = errors.New("hostname unavailable")

func TestNewControllerDefaultsIntervals(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:   "controlplane",
		Logger: slog.Default(),
	})

	assert.Equal(t, time.Minute, controller.Config.HeartbeatInterval)
	assert.Equal(t, 5*time.Minute, controller.Config.StaleThreshold)
}

func TestNewControllerKeepsExplicitIntervals(t *testing.T) {
	t.Parallel()

	controller := registry.NewController(registry.Config{
		Role:              "controlplane",
		Logger:            slog.Default(),
		HeartbeatInterval: time.Second,
		StaleThreshold:    10 * time.Second,
	})

	assert.Equal(t, time.Second, controller.Config.HeartbeatInterval)
	assert.Equal(t, 10*time.Second, controller.Config.StaleThreshold)
}

func TestInstanceNameLowercasesHostname(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "kontinuum-dev-1", registry.InstanceName("KONTINUUM-Dev-1", nil))
}

func TestInstanceNameFallsBackToUUIDWhenHostnameUnavailable(t *testing.T) {
	t.Parallel()

	name := registry.InstanceName("", errTestHostnameUnavailable)
	_, err := uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname lookup fails")

	name = registry.InstanceName("", nil)
	_, err = uuid.Parse(name)
	require.NoError(t, err, "expected a UUID fallback when hostname is empty")
}
