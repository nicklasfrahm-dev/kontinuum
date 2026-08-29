package zone_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// TestRestDownstreamClientBuilderRejectsMalformedKubeconfig covers
// restDownstreamClientBuilder (DownstreamClientBuilder's production
// implementation, and downstreamScheme it builds from) without a real
// cluster: malformed kubeconfig bytes fail clientcmd's own local YAML
// parsing before Build ever reaches the network, so this stays offline and
// deterministic — see zone.Add's own tests for the network-free
// fake-DownstreamClientBuilder path every other caller uses instead.
func TestRestDownstreamClientBuilderRejectsMalformedKubeconfig(t *testing.T) {
	t.Parallel()

	builder := zone.NewDownstreamClientBuilder()
	require.NotNil(t, builder)

	_, err := builder.Build([]byte("not: valid: kubeconfig: yaml"))
	assert.Error(t, err)
}
