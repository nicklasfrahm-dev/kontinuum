package fabricmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/cli/fabricmanager"
)

const testNodeName = "node-a1"

func testFabricWithGatewayNode(natDisabled bool, gatewayNodeName string) v1alpha2.Fabric {
	var gatewayNodeRef *v1alpha2.ObjectReference

	if gatewayNodeName != "" {
		gatewayNodeRef = &v1alpha2.ObjectReference{
			APIVersion: v1alpha2.GroupVersion().String(), Kind: "Instance", Name: gatewayNodeName,
		}
	}

	return v1alpha2.Fabric{
		Spec: v1alpha2.FabricSpec{NAT: v1alpha2.FabricNATSpec{Disabled: natDisabled}},
		Status: v1alpha2.FabricStatus{
			Zones: []v1alpha2.FabricZoneStatus{{Zone: "a", GatewayNodeRef: gatewayNodeRef}},
		},
	}
}

func TestElectedWithNATEnabledTrueWhenThisNodeIsGatewayAndNATEnabled(t *testing.T) {
	t.Parallel()

	reconciler := &fabricmanager.Reconciler{NodeName: testNodeName}
	fabricObj := testFabricWithGatewayNode(false, testNodeName)

	assert.True(t, reconciler.ElectedWithNATEnabled(fabricObj))
}

func TestElectedWithNATEnabledFalseWhenNATDisabled(t *testing.T) {
	t.Parallel()

	reconciler := &fabricmanager.Reconciler{NodeName: testNodeName}
	fabricObj := testFabricWithGatewayNode(true, testNodeName)

	assert.False(t, reconciler.ElectedWithNATEnabled(fabricObj))
}

func TestElectedWithNATEnabledFalseWhenAnotherNodeIsGateway(t *testing.T) {
	t.Parallel()

	reconciler := &fabricmanager.Reconciler{NodeName: testNodeName}
	fabricObj := testFabricWithGatewayNode(false, "node-a2")

	assert.False(t, reconciler.ElectedWithNATEnabled(fabricObj))
}

func TestElectedWithNATEnabledFalseWhenNoGatewayElectedYet(t *testing.T) {
	t.Parallel()

	reconciler := &fabricmanager.Reconciler{NodeName: testNodeName}
	fabricObj := testFabricWithGatewayNode(false, "")

	assert.False(t, reconciler.ElectedWithNATEnabled(fabricObj))
}
