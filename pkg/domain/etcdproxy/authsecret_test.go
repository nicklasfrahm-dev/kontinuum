package etcdproxy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

func TestGenerateAuthKeyLengthAndCharset(t *testing.T) {
	t.Parallel()

	key, err := etcdproxy.GenerateAuthKey()
	require.NoError(t, err)
	assert.Len(t, key, etcdproxy.KeyLength)

	for _, r := range key {
		assert.Contains(t, etcdproxy.KeyCharset, string(r))
	}
}

func TestGenerateAuthKeyProducesDistinctValues(t *testing.T) {
	t.Parallel()

	first, err := etcdproxy.GenerateAuthKey()
	require.NoError(t, err)

	second, err := etcdproxy.GenerateAuthKey()
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
}

func TestAuthKeyDueAtExpiresAtAndValid(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	key := etcdproxy.AuthKey{Value: "test", CreatedAt: now}

	dueAt := now.Add(etcdproxy.RotationInterval)
	expiresAt := dueAt.Add(etcdproxy.OverlapWindow)

	assert.Equal(t, dueAt, key.DueAt())
	assert.Equal(t, expiresAt, key.ExpiresAt())

	assert.True(t, key.Valid(dueAt), "still valid right at its own rotation-due time — demoted, not expired, then")
	assert.True(t, key.Valid(expiresAt.Add(-time.Second)))
	assert.False(t, key.Valid(expiresAt))
	assert.False(t, key.Valid(expiresAt.Add(time.Second)))
}

func TestBuildAndParseAuthSecretRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pair := etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: "current-value", CreatedAt: now},
		Previous: etcdproxy.AuthKey{Value: "previous-value", CreatedAt: now.Add(-etcdproxy.RotationInterval)},
	}

	secret := etcdproxy.BuildAuthSecret("eu-eu-1a", "kontinuum-system", pair)
	assert.Equal(t, etcdproxy.AuthSecretName("eu-eu-1a"), secret.Name)
	assert.Equal(t, "kontinuum-system", secret.Namespace)

	// A real apiserver converts StringData into the base64-encoded Data via
	// admission logic a fake client wouldn't replicate — ParseAuthSecret
	// reads Data, so mirror that conversion here directly rather than
	// going through a fake client just to round-trip this.
	secret.Data = map[string][]byte{}
	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	got, ok := etcdproxy.ParseAuthSecret(secret)
	require.True(t, ok)
	assert.Equal(t, pair.Current.Value, got.Current.Value)
	assert.WithinDuration(t, pair.Current.CreatedAt, got.Current.CreatedAt, 0)
	assert.Equal(t, pair.Previous.Value, got.Previous.Value)
	assert.WithinDuration(t, pair.Previous.CreatedAt, got.Previous.CreatedAt, 0)
}

func TestParseAuthSecretRejectsMissingOrMalformedFields(t *testing.T) {
	t.Parallel()

	const (
		keyField          = "key"
		keyCreatedAtField = "key-created-at"
		previousKeyField  = "previous-key"
	)

	valid := map[string][]byte{
		keyField:          []byte("v"),
		keyCreatedAtField: []byte(time.Now().Format(time.RFC3339Nano)),
		previousKeyField:  []byte("v"),
	}

	cases := map[string]map[string][]byte{
		"empty":                 {},
		"missing previous":      {keyField: valid[keyField], keyCreatedAtField: valid[keyCreatedAtField]},
		"unparseable timestamp": {keyField: valid[keyField], keyCreatedAtField: []byte("not-a-time")},
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			secret := &corev1.Secret{Data: data}
			_, ok := etcdproxy.ParseAuthSecret(secret)
			assert.False(t, ok)
		})
	}
}
