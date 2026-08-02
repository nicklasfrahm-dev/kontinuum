package addon_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

func testCelCtx(t *testing.T, controlPlaneCount int) map[string]any {
	t.Helper()

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a"},
		Spec: v1alpha2.TalosClusterSpec{
			Talos: v1alpha2.TalosSpec{Version: "v1.13.0"},
		},
	}

	celCtx, err := addon.CelContext(cluster, controlPlaneCount)
	require.NoError(t, err)

	return celCtx
}

func TestEvaluateComputedValuesLeavesLiteralsUntouched(t *testing.T) {
	t.Parallel()

	values := map[string]any{
		"a": "literal",
		"b": map[string]any{"c": true},
		"d": []any{"x", "y"},
	}

	resolved, err := addon.EvaluateComputedValues(values, testCelCtx(t, 1))
	require.NoError(t, err)
	assert.Equal(t, values, resolved)
}

func TestEvaluateComputedValuesEvaluatesCelField(t *testing.T) {
	t.Parallel()

	values := map[string]any{
		"replicas": map[string]any{addon.CelFieldKey: "ctx.taloscluster.status.controlPlane.replicas > 1 ? 2 : 1"},
	}

	single, err := addon.EvaluateComputedValues(values, testCelCtx(t, 1))
	require.NoError(t, err)
	assert.Equal(t, int64(1), single["replicas"])

	multi, err := addon.EvaluateComputedValues(values, testCelCtx(t, 3))
	require.NoError(t, err)
	assert.Equal(t, int64(2), multi["replicas"])
}

func TestEvaluateComputedValuesAccessesTalosClusterResource(t *testing.T) {
	t.Parallel()

	values := map[string]any{
		"clusterName": map[string]any{addon.CelFieldKey: "ctx.taloscluster.metadata.name"},
	}

	resolved, err := addon.EvaluateComputedValues(values, testCelCtx(t, 1))
	require.NoError(t, err)
	assert.Equal(t, "eu-1a", resolved["clusterName"])
}

func TestEvaluateComputedValuesAccessesTalosNamespace(t *testing.T) {
	t.Parallel()

	values := map[string]any{
		"port": map[string]any{addon.CelFieldKey: "ctx.talos.kubePrism.port"},
	}

	resolved, err := addon.EvaluateComputedValues(values, testCelCtx(t, 1))
	require.NoError(t, err)
	assert.Equal(t, int64(7445), resolved["port"])
}

func TestEvaluateComputedValuesPreservesNestedStructure(t *testing.T) {
	t.Parallel()

	values := map[string]any{
		"outer": map[string]any{
			"inner": []any{
				map[string]any{addon.CelFieldKey: "ctx.talos.kubePrism.port"},
				"literal",
			},
		},
	}

	resolved, err := addon.EvaluateComputedValues(values, testCelCtx(t, 1))
	require.NoError(t, err)

	outer, ok := resolved["outer"].(map[string]any)
	require.True(t, ok)

	inner, ok := outer["inner"].([]any)
	require.True(t, ok)
	require.Len(t, inner, 2)
	assert.Equal(t, int64(7445), inner[0])
	assert.Equal(t, "literal", inner[1])
}

func TestEvaluateComputedValuesReturnsErrorOnMalformedExpression(t *testing.T) {
	t.Parallel()

	values := map[string]any{
		"broken": map[string]any{addon.CelFieldKey: "ctx.taloscluster.status.controlPlane.replicas +"},
	}

	_, err := addon.EvaluateComputedValues(values, testCelCtx(t, 1))
	require.Error(t, err)
}

func TestEvaluateComputedValuesNilSafe(t *testing.T) {
	t.Parallel()

	resolved, err := addon.EvaluateComputedValues(nil, testCelCtx(t, 1))
	require.NoError(t, err)
	assert.Nil(t, resolved)
}
