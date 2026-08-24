package fabric_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/fabric"
)

// TestSanitizeForK8sNameDoesNotCollide is a regression test: an earlier
// version of this function mapped every byte outside [a-z0-9] onto the same
// literal "-", so "eth0.1" and "eth0-1" both sanitized to "eth0-1" —
// two distinct interfaces silently sharing one Deployment name.
func TestSanitizeForK8sNameDoesNotCollide(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, fabric.SanitizeForK8sName("eth0.1"), fabric.SanitizeForK8sName("eth0-1"))
}

func TestSanitizeForK8sNameLowersCaseAndEscapesDisallowedBytes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "eth0", fabric.SanitizeForK8sName("eth0"))
	assert.Equal(t, "eth0", fabric.SanitizeForK8sName("ETH0"))
	assert.Equal(t, "eth0-2e1", fabric.SanitizeForK8sName("eth0.1"))
	assert.Equal(t, "eth0-2d1", fabric.SanitizeForK8sName("eth0-1"))
}

// TestManagerDeploymentNameDoesNotCollideAcrossFabrics is a
// regression test: an earlier version scoped the Deployment name by
// interface alone, so two different Fabric objects (nothing enforces
// uniqueness on spec.region) electing the same gateway node for the same
// interface would race to own the identical Deployment.
func TestManagerDeploymentNameDoesNotCollideAcrossFabrics(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t,
		fabric.ManagerDeploymentName("fabric-a", "eth0"), fabric.ManagerDeploymentName("fabric-b", "eth0"))
}
