package cli_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/pkg/cli"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

func TestLonePeerOptions(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.Defaults()
	cfg.Server.Region = "eu"
	cfg.Server.Zone = "1a"

	tests := []struct {
		name     string
		identity zonelease.Identity
		wired    bool
	}{
		{name: "hub-less zone peer", identity: zonelease.Identity{SelfZoneKey: zoneA, HubLess: true}, wired: true},
		{name: "proxy-mode worker", identity: zonelease.Identity{SelfZoneKey: zoneA}, wired: false},
		{name: "hub-less hub", identity: zonelease.Identity{HubLess: true}, wired: false},
		{name: "hub", identity: zonelease.Identity{}, wired: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			opts := cli.LonePeerOptions(cfg, slog.Default(), test.identity)
			assert.Equal(t, test.wired, len(opts) == 1)
		})
	}
}
