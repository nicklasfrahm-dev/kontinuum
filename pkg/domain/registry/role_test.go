package registry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

func TestRole(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		region      string
		zone        string
		wantRole    string
		wantErrIs   error
		expectError bool
	}{
		"both empty is controlplane": {
			region:   "",
			zone:     "",
			wantRole: v1alpha2.RoleControlPlane,
		},
		"both set is worker": {
			region:   "eu",
			zone:     "eu-1a",
			wantRole: v1alpha2.RoleWorker,
		},
		"only region set is an error": {
			region:      "eu",
			zone:        "",
			expectError: true,
			wantErrIs:   registry.ErrRegionZoneRequired,
		},
		"only zone set is an error": {
			region:      "",
			zone:        "eu-1a",
			expectError: true,
			wantErrIs:   registry.ErrRegionZoneRequired,
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			role, err := registry.Role(testCase.region, testCase.zone)

			if testCase.expectError {
				require.Error(t, err)
				assert.ErrorIs(t, err, testCase.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.wantRole, role)
		})
	}
}
