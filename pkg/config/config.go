// Package config loads kontinuum's configuration from KONTINUUM_-prefixed
// environment variables, with defaults declared via `default` struct tags.
// Env-var names are derived from the field path (e.g. Server.Addr →
// KONTINUUM_SERVER_ADDR), so adding a field is a one-line tag change — no
// manual env plumbing.
package config

import (
	"net/url"
	"os"
	"reflect"
	"strings"
	"unicode"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const envPrefix = "KONTINUUM_"

// Config holds all kontinuum configuration. Each leaf string field carries a
// `default` struct tag; the env-var name is auto-derived from its path.
//
// It is defined directly in terms of v1alpha2.KontinuumConfigStatus — the
// very type a Kontinuum's status.config reports on its per-instance
// settings page (/app/kontinuums/{name}) — rather than a separately
// declared struct of the same shape, so there is exactly one definition to
// maintain and a value read from status.config maps 1:1 onto the env vars
// that produced it (see v1alpha2.KontinuumStatus.Config's doc for the one
// accepted duplication, Region/Zone, that makes that mapping exact). It's a
// distinct named type rather than a plain alias (`type Config =
// v1alpha2.KontinuumConfigStatus`) only so Redact below can live here,
// where the redaction logic belongs, rather than on an API schema type —
// Go only allows attaching methods to a type from the package that defines
// it, and a plain alias would still count as v1alpha2's own type for that
// purpose. Converting between the two where they cross (see
// pkg/cli/serve.go's displayConfig) is a plain type conversion: identical
// underlying struct, so it can't fail or lose data. Server specifically
// holds the raw, connectable Storage value here — see Redact.
type Config v1alpha2.KontinuumConfigStatus

// Load reads configuration from KONTINUUM_-prefixed environment variables,
// falling back to the `default` struct tag when an env var is unset or empty.
// Env-var names are derived from each field's path (Server.Addr →
// KONTINUUM_SERVER_ADDR).
func Load() (*Config, error) {
	cfg := &Config{}
	loadStruct(reflect.ValueOf(cfg).Elem(), nil, true)

	return cfg, nil
}

// Defaults populates c with only the `default` struct tag values, ignoring
// environment variables and discarding whatever c already held — useful for
// cobra flag defaults. Load overlays environment variables on top of this
// same set of defaults; call it on a fresh &Config{} for the same effect
// Defaults gives standalone.
func (c *Config) Defaults() {
	loadStruct(reflect.ValueOf(c).Elem(), nil, false)
}

// EnvVar is one leaf field's derived KONTINUUM_-prefixed env-var name (see
// envName), its current value, and whether the underlying
// v1alpha2.KontinuumConfigStatus field is tagged `secret:"true"` — see
// Config.EnvVars.
type EnvVar struct {
	Name   string
	Value  string
	Secret bool
}

// EnvVars returns every leaf string field as the KONTINUUM_-prefixed env
// var that produced it, walking the exact same field paths Load itself
// reads from (see walkStringFields) — so this can never miss a field Load
// knows about, or disagree with it on a field's name.
//
// Built for pkg/domain/zone, which copies a hub's own configuration onto a
// newly joined zone's kontinuum-server: hand-maintaining a separate list of
// which env vars to forward there fell behind this struct more than once —
// Log.Level/Format were never forwarded at all, and a hand-written
// zone-specific override for KONTINUUM_OIDC_REDIRECT_URL produced a
// malformed URL for any zone with no domain configured, instead of falling
// back to this same hub value the way every other field already did.
// Every field this struct gains from now on reaches a joined zone
// automatically, with no change needed in that package at all — unless it
// needs its own zone-specific override, or (see Secret above) needs
// routing into a Secret instead of a broadly-readable ConfigMap.
func (c *Config) EnvVars() []EnvVar {
	var vars []EnvVar

	walkStringFields(reflect.ValueOf(c).Elem(), nil,
		func(fieldPath []string, field reflect.Value, structField reflect.StructField) {
			vars = append(vars, EnvVar{
				Name:   envName(fieldPath),
				Value:  field.String(),
				Secret: structField.Tag.Get("secret") == "true",
			})
		})

	return vars
}

// Redact returns a copy of c with sensitive fields stripped, safe to log,
// display, or copy onto a Kontinuum's broadly-readable status.config —
// currently, any username/password embedded in Server.Storage (e.g.
// "postgres://user:pass@host/db"). The unredacted original is what stays
// confidential, in the Secret status.secretRef points to — see
// pkg/domain/registry.Heartbeat.SecretData. Pointer receiver purely to
// match Defaults below (which must take one to populate c in place) —
// Redact itself never modifies c.
func (c *Config) Redact() Config {
	redacted := *c
	redacted.Server.Storage = RedactStorage(c.Server.Storage)
	redacted.Server.DNS.Credential = ""

	return redacted
}

// RedactStorage strips an embedded username/password from a storage
// connection string, leaving the scheme, host, path, and query intact.
// Exported (not just used via Config.Redact) so callers that only have the
// raw connection string — not a whole Config — can still redact it.
func RedactStorage(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	parsed.User = nil

	return parsed.String()
}

// ParseAdminGroups splits OIDC.AdminGroups's comma-delimited value into
// trimmed, non-empty group names — the single source of truth for exactly
// which groups get admin access, used by pkg/domain/adminrbac's
// ClusterRoleBinding reconciler so it can never drift from the groups
// libkapi.WithRBACAuthorizer itself resolves from the same raw string.
// Mirrors github.com/kommodity-io/kommodity/pkg/libkapi/auth's own
// unexported parseAdminGroups.
func ParseAdminGroups(raw string) []string {
	var groups []string

	for group := range strings.SplitSeq(raw, ",") {
		group = strings.TrimSpace(group)
		if group != "" {
			groups = append(groups, group)
		}
	}

	return groups
}

// loadStruct walks structVal recursively. For each string field, it sets the
// field from the KONTINUUM_-prefixed env var derived from path (when useEnv
// is true and the var is non-empty) or the field's `default` tag.
func loadStruct(structVal reflect.Value, path []string, useEnv bool) {
	walkStringFields(structVal, path, func(fieldPath []string, field reflect.Value, structField reflect.StructField) {
		val := structField.Tag.Get("default")

		if useEnv {
			if env := os.Getenv(envName(fieldPath)); env != "" {
				val = env
			}
		}

		field.SetString(val)
	})
}

// walkStringFields recursively visits every leaf string field of
// structVal, depth-first, passing each one's full field path alongside its
// reflect.Value and reflect.StructField (for reading its struct tags) to
// visit — the one definition of "which fields count" (string leaves only;
// anything else, e.g. KontinuumOIDCConfigStatus.Enabled's bool, is
// skipped) that loadStruct and Config.EnvVars both build on, rather than
// two separate traversals that could disagree about it.
func walkStringFields(
	structVal reflect.Value, path []string,
	visit func(fieldPath []string, field reflect.Value, structField reflect.StructField),
) {
	for fieldIndex := range structVal.NumField() {
		field := structVal.Field(fieldIndex)
		structField := structVal.Type().Field(fieldIndex)

		fieldPath := make([]string, len(path)+1)
		copy(fieldPath, path)
		fieldPath[len(path)] = structField.Name

		if field.Kind() == reflect.Struct {
			walkStringFields(field, fieldPath, visit)

			continue
		}

		if field.Kind() != reflect.String {
			continue
		}

		visit(fieldPath, field, structField)
	}
}

// envName derives the full env-var name from a field path:
// ["Server", "Addr"] → KONTINUUM_SERVER_ADDR.
func envName(path []string) string {
	parts := make([]string, len(path))
	for index, part := range path {
		parts[index] = toSnakeUpper(part)
	}

	return envPrefix + strings.Join(parts, "_")
}

// toSnakeUpper converts a CamelCase Go field name to UPPER_SNAKE_CASE:
// "Addr" → "ADDR", "ServerPort" → "SERVER_PORT". Acronym runs are kept
// together rather than split per letter: "IssuerURL" → "ISSUER_URL", not
// "ISSUER_U_R_L". A boundary is placed before an uppercase rune when the
// previous rune is lowercase (start of a new word), or when the previous
// rune is uppercase but the next one is lowercase (end of an acronym run,
// e.g. the "U" in "URLPath" → "URL_PATH").
func toSnakeUpper(source string) string {
	runes := []rune(source)

	var builder strings.Builder

	for index, runeVal := range runes {
		if index > 0 && unicode.IsUpper(runeVal) && isWordBoundary(runes, index) {
			builder.WriteByte('_')
		}

		builder.WriteRune(unicode.ToUpper(runeVal))
	}

	return builder.String()
}

// isWordBoundary reports whether runes[index] starts a new word, given that
// it is already known to be uppercase.
func isWordBoundary(runes []rune, index int) bool {
	if unicode.IsLower(runes[index-1]) {
		return true
	}

	nextIndex := index + 1

	return unicode.IsUpper(runes[index-1]) && nextIndex < len(runes) && unicode.IsLower(runes[nextIndex])
}
