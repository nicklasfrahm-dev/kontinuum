package fleet_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/fleet"
)

// header is every test file's apiVersion and kind.
const header = "apiVersion: up.kontinuum.sh/v1alpha1\nkind: Fleet\n"

// baseFleet is a complete, valid kontinuum.yml.
const baseFleet = header + `metadata:
  name: example
spec:
  version: v0.37.0
  frontend:
    host: kontinuum.example.com
  storage:
    uri: ${env:STORAGE_URI}
  talos:
    version: v1.13.0
  dns:
    domain: example.com
    provider: Cloudflare
    credential:
      CF_API_TOKEN: ${env:CF_TOKEN}
  acme:
    provider: letsencrypt
    providers:
      letsencrypt:
        server: https://acme-v02.api.letsencrypt.org/directory
        email: ops@example.com
      zerossl:
        server: https://acme.zerossl.com/v2/DV90
        email: ops@example.com
        externalAccountBinding:
          keyID: ${env:EAB_KID}
          hmacKey: ${env:EAB_HMAC}
  auth:
    oidc:
      issuerURL: https://id.example.com
      clientID: kontinuum
      adminGroups:
        - platform-admins
  zones:
    - region: eu
      zone: eu-1a
      instances:
        - address: 203.0.113.10
    - region: eu
      zone: eu-1b
      instances:
        - address: 198.51.100.20
`

// testToken is the fake Cloudflare token CF_TOKEN expands to.
const testToken = "token"

// testEnv is the environment references in baseFleet expand against.
func testEnv() map[string]string {
	return map[string]string{
		"STORAGE_URI": "postgres://kontinuum@db.example.com/kontinuum",
		"CF_TOKEN":    testToken,
		"EAB_KID":     "kid",
		"EAB_HMAC":    "hmac",
	}
}

// sources expands references against env, reading files for real.
func sources(env map[string]string) fleet.Sources {
	return fleet.Sources{LookupEnv: func(name string) (string, bool) {
		value, ok := env[name]

		return value, ok
	}}
}

// writeFile writes content to name in dir, creating parent directories.
func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

// load loads and expands files with testEnv.
func load(t *testing.T, paths ...string) (*fleet.Fleet, error) {
	t.Helper()

	doc, err := fleet.Load(paths)
	if err != nil {
		return nil, fmt.Errorf("failed to load: %w", err)
	}

	parsed, err := doc.Expand(sources(testEnv()))
	if err != nil {
		return nil, fmt.Errorf("failed to expand: %w", err)
	}

	return parsed, nil
}

func TestLoadDecodesAndExpandsAFleet(t *testing.T) {
	t.Parallel()

	parsed, err := load(t, writeFile(t, t.TempDir(), "kontinuum.yml", baseFleet))
	require.NoError(t, err)

	assert.Equal(t, "example", parsed.Metadata.Name)
	assert.Equal(t, "postgres://kontinuum@db.example.com/kontinuum", parsed.Spec.Storage.URI)
	assert.Equal(t, testToken, parsed.Spec.DNS.Credential["CF_API_TOKEN"])
	assert.Equal(t, "hmac", parsed.Spec.ACME.Providers["zerossl"].ExternalAccountBinding.HMACKey)
	require.Len(t, parsed.Spec.Zones, 2)
	assert.Equal(t, "eu-eu-1a", parsed.Spec.Zones[0].Name())
	assert.Equal(t, []string{"203.0.113.10"}, parsed.Spec.Zones[0].Addresses())
}

func TestLoadRejectsUnknownFieldsNamingTheFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", baseFleet)
	overlay := writeFile(t, dir, "prod/kontinuum.yml", header+"spec:\n  acme:\n    challenge: dns-01\n")

	_, err := fleet.Load([]string{base, overlay})
	require.Error(t, err)
	assert.Contains(t, err.Error(), overlay)
	assert.Contains(t, err.Error(), "challenge")
}

func TestLoadRejectsAKubeconfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kubeconfig := writeFile(t, dir, "kontinuum.yaml", "apiVersion: v1\nkind: Config\nclusters: []\n")

	_, err := fleet.Load([]string{writeFile(t, dir, "kontinuum.yml", baseFleet), kubeconfig})
	require.ErrorIs(t, err, fleet.ErrKubeconfig)
	assert.Contains(t, err.Error(), kubeconfig)
}

func TestLoadRejectsWrongAPIVersionAndKind(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	_, err := fleet.Load([]string{writeFile(t, dir, "a.yml", "apiVersion: v1\nkind: Fleet\n")})
	require.ErrorIs(t, err, fleet.ErrWrongAPIVersion)

	_, err = fleet.Load([]string{writeFile(t, dir, "b.yml", "apiVersion: up.kontinuum.sh/v1alpha1\nkind: Zone\n")})
	require.ErrorIs(t, err, fleet.ErrWrongKind)

	_, err = fleet.Load([]string{writeFile(t, dir, "c.yml", "")})
	require.ErrorIs(t, err, fleet.ErrNotAMapping)

	_, err = fleet.Load(nil)
	require.ErrorIs(t, err, fleet.ErrNoFiles)
}

func TestOverlayMergesMappingsAndReplacesScalars(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", baseFleet)
	overlay := writeFile(t, dir, "prod/kontinuum.yml", header+`metadata:
  name: prod
spec:
  dns:
    domain: example.org
    credential:
      EXTRA_KEY: ${env:CF_TOKEN}
  frontend:
    host: kontinuum.example.org
`)

	parsed, err := load(t, base, overlay)
	require.NoError(t, err)

	assert.Equal(t, "prod", parsed.Metadata.Name)
	assert.Equal(t, "example.org", parsed.Spec.DNS.Domain)
	assert.Equal(t, "Cloudflare", parsed.Spec.DNS.Provider, "a key the overlay doesn't set survives")
	assert.Equal(t, map[string]string{"CF_API_TOKEN": testToken, "EXTRA_KEY": testToken}, parsed.Spec.DNS.Credential,
		"dns.credential merges key by key like every mapping")
}

func TestOverlaySequencesReplaceWhole(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", baseFleet)

	withZones := writeFile(t, dir, "zones.yml", header+`spec:
  zones:
    - region: us
      zone: us-1a
      instances:
        - address: 192.0.2.1
`)

	parsed, err := load(t, base, withZones)
	require.NoError(t, err)
	require.Len(t, parsed.Spec.Zones, 1, "an overlay's zones replace the base's")
	assert.Equal(t, "us-us-1a", parsed.Spec.Zones[0].Name())

	withoutZones := writeFile(t, dir, "nozones.yml", header+"spec:\n  version: v0.37.0\n")

	parsed, err = load(t, base, withoutZones)
	require.NoError(t, err)
	assert.Len(t, parsed.Spec.Zones, 2, "a base's zones survive an overlay without any")
}

func TestOverlayNullRemovesFields(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := writeFile(t, dir, "kontinuum.yml", baseFleet)
	overlay := writeFile(t, dir, "prod.yml", header+`spec:
  talos: null
  acme:
    providers:
      zerossl: null
  zones: null
  storage:
    mode: null
`)

	doc, err := fleet.Load([]string{base, overlay})
	require.NoError(t, err)

	parsed, err := doc.Expand(sources(testEnv()))
	require.NoError(t, err)

	assert.Empty(t, parsed.Spec.Talos.Version, "a removed mapping falls back to its default")
	assert.NotContains(t, parsed.Spec.ACME.Providers, "zerossl")
	assert.Contains(t, parsed.Spec.ACME.Providers, "letsencrypt")
	assert.Empty(t, parsed.Spec.Zones)

	rendered, err := doc.Render()
	require.NoError(t, err)
	assert.Contains(t, string(rendered), "# spec.talos (removed by "+overlay+")")
	assert.Contains(t, string(rendered), "# spec.acme.providers.zerossl (removed by "+overlay+")")
	assert.NotContains(t, string(rendered), "spec.storage.mode (removed", "null for an unset key is a no-op")

	require.ErrorIs(t, fleet.Validate(parsed, "v0.37.0"), fleet.ErrInvalidFleet,
		"a removed required field is reported by validation")
}

func TestNullInLeftMostFileLeavesFieldUnset(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	only := writeFile(t, dir, "kontinuum.yml", strings.Replace(baseFleet, "  talos:\n    version: v1.13.0\n",
		"  talos: null\n", 1))

	parsed, err := load(t, only)
	require.NoError(t, err)
	assert.Empty(t, parsed.Spec.Talos.Version)
}

func TestNullSequenceElementIsRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	_, err := fleet.Load([]string{writeFile(t, dir, "kontinuum.yml", header+"spec:\n  zones: [null]\n")})
	require.ErrorIs(t, err, fleet.ErrNullSequenceElement)
}

func TestIncompleteBaseMadeValidByOverlay(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	baseOnly, _, found := strings.Cut(baseFleet, "  zones:\n")
	require.True(t, found)

	base := writeFile(t, dir, "kontinuum.yml", baseOnly)

	parsed, err := load(t, base)
	require.NoError(t, err)
	require.Error(t, fleet.Validate(parsed, "v0.37.0"), "the base alone has no zones")

	overlay := writeFile(t, dir, "prod.yml", header+`spec:
  zones:
    - region: eu
      zone: eu-1a
      instances:
        - address: 203.0.113.10
    - region: eu
      zone: eu-1b
      instances:
        - address: 198.51.100.20
`)

	parsed, err = load(t, base, overlay)
	require.NoError(t, err)
	require.NoError(t, fleet.Validate(parsed, "v0.37.0"))
}

func TestRenderLeavesReferencesUnexpanded(t *testing.T) {
	t.Parallel()

	doc, err := fleet.Load([]string{writeFile(t, t.TempDir(), "kontinuum.yml", baseFleet)})
	require.NoError(t, err)

	rendered, err := doc.Render()
	require.NoError(t, err)
	assert.Contains(t, string(rendered), "${env:STORAGE_URI}")
	assert.NotContains(t, string(rendered), "db.example.com")
}
