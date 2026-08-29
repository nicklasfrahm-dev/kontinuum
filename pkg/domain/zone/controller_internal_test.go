package zone

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const testZoneNameFixture = "eu-1a"

func TestMapTalosClusterToZoneEnqueuesSameNamedZone(t *testing.T) {
	t.Parallel()

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneNameFixture, Namespace: v1alpha2.KontinuumSystemNamespace},
	}

	got := mapTalosClusterToZone(context.Background(), cluster)

	assert.Equal(t, []ctrl.Request{{
		NamespacedName: types.NamespacedName{Name: testZoneNameFixture, Namespace: v1alpha2.KontinuumSystemNamespace},
	}}, got)
}

func TestMapKontinuumToZoneEnqueuesOwningZone(t *testing.T) {
	t.Parallel()

	kontinuum := &v1alpha2.Kontinuum{
		Spec: v1alpha2.KontinuumSpec{Region: "eu", Zone: "1a"},
	}

	got := mapKontinuumToZone(context.Background(), kontinuum)

	require.Len(t, got, 1)
	assert.Equal(t, testZoneNameFixture, got[0].Name)
	assert.Equal(t, v1alpha2.KontinuumSystemNamespace, got[0].Namespace)
}

func TestMapKontinuumToZoneSkipsHubsOwnSelfRegistration(t *testing.T) {
	t.Parallel()

	hub := &v1alpha2.Kontinuum{Spec: v1alpha2.KontinuumSpec{}}

	assert.Nil(t, mapKontinuumToZone(context.Background(), hub),
		"a Kontinuum with no region/zone set is the hub's own self-registration — zonelease.Key "+
			"returns GlobalKey for it, and there's no Zone named that to enqueue")
}

func TestMapKontinuumToZoneIgnoresNonKontinuumObjects(t *testing.T) {
	t.Parallel()

	other := &v1alpha2.TalosCluster{ObjectMeta: metav1.ObjectMeta{Name: testZoneNameFixture}}

	assert.Nil(t, mapKontinuumToZone(context.Background(), other))
}
