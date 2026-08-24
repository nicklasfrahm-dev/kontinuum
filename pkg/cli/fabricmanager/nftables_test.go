package fabricmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/pkg/cli/fabricmanager"
)

// TestSanitizeForTableNameDoesNotCollide is a regression test: an earlier
// version of this function mapped every byte outside [a-zA-Z0-9_] onto the
// same literal "_", so "eth0.1" and "eth0_1" both sanitized to "eth0_1" —
// two distinct interfaces silently sharing one nftables table.
func TestSanitizeForTableNameDoesNotCollide(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, fabricmanager.SanitizeForTableName("eth0.1"), fabricmanager.SanitizeForTableName("eth0_1"))
}

func TestSanitizeForTableNameEscapesDisallowedBytes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "eth0", fabricmanager.SanitizeForTableName("eth0"))
	assert.Equal(t, "eth0_2e1", fabricmanager.SanitizeForTableName("eth0.1"))
	assert.Equal(t, "eth0_5f1", fabricmanager.SanitizeForTableName("eth0_1"))
}
