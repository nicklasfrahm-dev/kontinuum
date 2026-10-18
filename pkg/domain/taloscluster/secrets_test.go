package taloscluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
)

// testClusterUID is the UID every secrets-bundle fixture's TalosCluster is
// given, so owner references can be compared.
const testClusterUID = types.UID("cluster-uid")

// TestEnsureSecretsBundleAdoptsAnUnrecordedBundle covers a reconcile that
// stored the bundle but stopped before recording status.secretRef — a
// restart or a lost zone lease: the next one adopts it instead of failing
// on AlreadyExists or minting a second CA.
func TestEnsureSecretsBundleAdoptsAnUnrecordedBundle(t *testing.T) {
	t.Parallel()

	cluster := testCluster()
	cluster.UID = testClusterUID
	apiClient := newFakeClient(t)

	first, err := taloscluster.EnsureSecretsBundle(context.Background(), apiClient, cluster)
	require.NoError(t, err)

	cluster.Status.SecretRef = v1alpha2.SecretReference{}

	second, err := taloscluster.EnsureSecretsBundle(context.Background(), apiClient, cluster)
	require.NoError(t, err)

	assert.Equal(t, first.Certs.OS.Crt, second.Certs.OS.Crt, "the stored CA wins")
	assert.NotEmpty(t, cluster.Status.SecretRef.Name)
}

// TestEnsureSecretsBundleRejectsAnotherClustersBundle covers a Secret left
// behind by an earlier TalosCluster of the same name: its CA isn't this
// cluster's, so it's never adopted.
func TestEnsureSecretsBundleRejectsAnotherClustersBundle(t *testing.T) {
	t.Parallel()

	earlier := testCluster()
	earlier.UID = "earlier-uid"
	apiClient := newFakeClient(t)

	_, err := taloscluster.EnsureSecretsBundle(context.Background(), apiClient, earlier)
	require.NoError(t, err)

	cluster := testCluster()
	cluster.UID = testClusterUID

	_, err = taloscluster.EnsureSecretsBundle(context.Background(), apiClient, cluster)
	require.ErrorIs(t, err, taloscluster.ErrForeignSecretsBundle)
}
