package etcdproxy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

// TestIsPoolableBackend covers the scheme dispatch LocalPool's own callers
// (see pkg/cli/serve.go's resolveStorageDSN) rely on: only the Kine DSN
// schemes that would otherwise make libkapi spawn its own embedded Kine —
// and so create the many-connections-to-one-socket topology LocalPool
// exists to avoid — are poolable. An already-running "etcd://" or
// "unix://" endpoint, and the zone-only "grpc://" RelayScheme, are not.
//
// LocalPool's actual pooling guarantee — that many local dialers only ever
// produce a small, fixed number of real upstream connections — is covered
// against a fake upstream by TestDialPoolBoundsConnectionCount in
// pool_test.go instead of here against a real embedded Kine: as of
// k3s-io/kine v1.14.2 (pinned transitively via
// github.com/kommodity-io/kommodity), simply resolving and closing an
// embedded Kine backend — nothing to do with LocalPool or this package —
// trips a genuine, pre-existing data race between Kine's own internal
// compactor and poll goroutines under `go test -race`, which this repo's
// CI runs. Verified independently of any code in this package: a bare
// storage.Resolve("sqlite://...") followed by Close() reproduces it every
// time. Filed as a follow-up rather than worked around here, since fixing
// it belongs upstream.
func TestIsPoolableBackend(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"postgres://user:pass@host/db": true,
		"sqlite://kontinuum.db":        true,
		"mysql://host/db":              true,
		"nats://host":                  true,
		"etcd://host:2379":             false,
		"unix:///tmp/kine.sock":        false,
		"grpc://zone:key@hub:443":      false,
	}

	for dsn, want := range cases {
		assert.Equal(t, want, etcdproxy.IsPoolableBackend(dsn), dsn)
	}
}
