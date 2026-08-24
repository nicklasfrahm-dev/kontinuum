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

// TestNATTableNameDoesNotCollideAcrossFabrics is a regression test: an
// earlier version scoped the table name by interface alone, so two
// different Fabric objects (nothing enforces uniqueness on spec.region)
// electing the same gateway node for the same interface would race to own
// the identical table.
func TestNATTableNameDoesNotCollideAcrossFabrics(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t,
		fabricmanager.NATTableName("fabric-a", "eth0"), fabricmanager.NATTableName("fabric-b", "eth0"))
}

// TestNATTableNameDoesNotCollideOnSegmentBoundary is a regression test for
// the length-prefix scheme itself: without it, plain
// sanitize(fabricID)+"_"+sanitize(iface) concatenation is not injective —
// SanitizeForTableName(".") is "_2e" and SanitizeForTableName("abcd") is
// unchanged ("abcd", since 'a'-'d' are all valid hex digits too), so
// fabricID="." + iface="abcd" and fabricID="" + iface="2e\xabcd" both
// naively concatenate to the identical "__2e_abcd": the "_2e" prefix of
// the first pair's escaped fabricID is byte-for-byte indistinguishable
// from the "_2e" that opens the second pair's escaped iface, and "ab"
// right after a "_" reads as a valid two-hex-digit escape either way.
func TestNATTableNameDoesNotCollideOnSegmentBoundary(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t,
		fabricmanager.NATTableName(".", "abcd"), fabricmanager.NATTableName("", "2e\xabcd"))
}
