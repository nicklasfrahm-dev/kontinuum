package fleet_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/fleet"
)

// withVersion returns baseFleet with spec.version set to version.
func withVersion(version string) string {
	return strings.Replace(baseFleet, "  version: v0.37.0\n", "  version: "+version+"\n", 1)
}

func TestExpandEmbeddedReference(t *testing.T) {
	t.Parallel()

	env := testEnv()
	env["MINOR"] = "37"

	doc, err := fleet.Load([]string{writeFile(t, t.TempDir(), "kontinuum.yml", withVersion("v0.${env:MINOR}.0"))})
	require.NoError(t, err)

	parsed, err := doc.Expand(sources(env))
	require.NoError(t, err)
	assert.Equal(t, "v0.37.0", parsed.Spec.Version)
}

func TestExpandEscapesDollarBrace(t *testing.T) {
	t.Parallel()

	parsed, err := load(t, writeFile(t, t.TempDir(), "kontinuum.yml", withVersion("$${env:LITERAL}")))
	require.NoError(t, err)
	assert.Equal(t, "${env:LITERAL}", parsed.Spec.Version)
}

func TestExpandRejectsUnsetAndEmptyEnv(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"unset", ""} {
		env := testEnv()
		if value == "" {
			env["CF_TOKEN"] = ""
		} else {
			delete(env, "CF_TOKEN")
		}

		doc, err := fleet.Load([]string{writeFile(t, t.TempDir(), "kontinuum.yml", baseFleet)})
		require.NoError(t, err)

		_, err = doc.Expand(sources(env))
		require.ErrorIs(t, err, fleet.ErrUnsetEnv)
		assert.Contains(t, err.Error(), "CF_TOKEN")
	}
}

func TestExpandRejectsBareAndUnknownReferences(t *testing.T) {
	t.Parallel()

	_, err := load(t, writeFile(t, t.TempDir(), "kontinuum.yml", withVersion("${KONTINUUM_VERSION}")))
	require.ErrorIs(t, err, fleet.ErrBareReference)
	assert.Contains(t, err.Error(), "${env:KONTINUUM_VERSION}", "the error suggests the env form")

	_, err = load(t, writeFile(t, t.TempDir(), "kontinuum.yml", withVersion("${vault:secret/version}")))
	require.ErrorIs(t, err, fleet.ErrUnknownSource)

	_, err = load(t, writeFile(t, t.TempDir(), "kontinuum.yml", withVersion("${env:OPEN")))
	require.ErrorIs(t, err, fleet.ErrUnterminatedReference)

	_, err = load(t, writeFile(t, t.TempDir(), "kontinuum.yml", withVersion("${env:}")))
	require.ErrorIs(t, err, fleet.ErrEmptyReference)
}

func TestExpandFileReferenceRelativeToDeclaringFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", baseFleet)
	writeFile(t, dir, "prod/secrets/storage-uri", "postgres://prod@db.example.com/kontinuum\n")
	overlay := writeFile(t, dir, "prod/kontinuum.yml", header+"spec:\n  storage:\n    uri: ${file:secrets/storage-uri}\n")

	parsed, err := load(t, base, overlay)
	require.NoError(t, err)
	assert.Equal(t, "postgres://prod@db.example.com/kontinuum", parsed.Spec.Storage.URI,
		"resolved against prod/, the overlay's own directory, with one trailing newline stripped")
}

func TestExpandFileStripsOnlyOneTrailingNewline(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "uri", "postgres://kontinuum@db.example.com/kontinuum\n\n")

	only := strings.Replace(baseFleet, "${env:STORAGE_URI}", "${file:uri}", 1)

	parsed, err := load(t, writeFile(t, dir, "kontinuum.yml", only))
	require.NoError(t, err)
	assert.Equal(t, "postgres://kontinuum@db.example.com/kontinuum\n", parsed.Spec.Storage.URI)
}

// TestExpandedValueStaysAPlainString covers expansion running after YAML
// decoding: a value containing YAML syntax can't change the structure.
func TestExpandedValueStaysAPlainString(t *testing.T) {
	t.Parallel()

	env := testEnv()
	env["CF_TOKEN"] = "line one\nkey: value\n- item"

	doc, err := fleet.Load([]string{writeFile(t, t.TempDir(), "kontinuum.yml", baseFleet)})
	require.NoError(t, err)

	parsed, err := doc.Expand(sources(env))
	require.NoError(t, err)
	assert.Equal(t, "line one\nkey: value\n- item", parsed.Spec.DNS.Credential["CF_API_TOKEN"])
}

func TestOverriddenReferenceIsNeverRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", strings.Replace(baseFleet, "${env:STORAGE_URI}", "${env:BASE_URI}", 1))
	overlay := writeFile(t, dir, "prod.yml", header+"spec:\n  storage:\n    uri: ${env:STORAGE_URI}\n")

	// BASE_URI is unset: reading it would fail.
	parsed, err := load(t, base, overlay)
	require.NoError(t, err)
	assert.Equal(t, "postgres://kontinuum@db.example.com/kontinuum", parsed.Spec.Storage.URI)
}

func TestExpandRejectsInlineSecrets(t *testing.T) {
	t.Parallel()

	inline := []struct {
		name, from, to string
	}{
		{name: "storage uri", from: "${env:STORAGE_URI}", to: "postgres://kontinuum@db.example.com/db"},
		{name: "embedded storage uri", from: "${env:STORAGE_URI}", to: "postgres://${env:CF_TOKEN}@db/db"},
		{name: "dns credential", from: "CF_API_TOKEN: ${env:CF_TOKEN}", to: "CF_API_TOKEN: literal"},
		{name: "eab hmac", from: "hmacKey: ${env:EAB_HMAC}", to: "hmacKey: literal"},
	}

	for _, test := range inline {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			content := strings.Replace(baseFleet, test.from, test.to, 1)

			_, err := load(t, writeFile(t, t.TempDir(), "kontinuum.yml", content))
			require.ErrorIs(t, err, fleet.ErrInlineSecret)
		})
	}
}
