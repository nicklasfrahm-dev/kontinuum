package zone_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// TestHelmDigestResolverRejectsMalformedReference covers helmDigestResolver
// (DigestResolver's production implementation) without a real registry: an
// empty/malformed reference fails helm's own local reference parsing before
// helmDigestResolver.ResolveDigest ever reaches the network, so this stays
// offline and deterministic — see resolveImage's own tests for the
// network-free fake-DigestResolver path every other caller uses instead.
func TestHelmDigestResolverRejectsMalformedReference(t *testing.T) {
	t.Parallel()

	resolver := zone.NewHelmDigestResolver()
	require.NotNil(t, resolver)

	_, err := resolver.ResolveDigest("")
	assert.Error(t, err)
}
