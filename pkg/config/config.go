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
)

const envPrefix = "KONTINUUM_"

// Config holds all kontinuum configuration. Each leaf string field carries a
// `default` struct tag; the env-var name is auto-derived from its path.
type Config struct {
	Server ServerConfig
	Log    LogConfig
	OIDC   OIDCConfig
}

// ServerConfig holds the API server listener and storage configuration.
type ServerConfig struct {
	// Addr is the listener address. Defaults to ":8080".
	Addr string `default:":8080"`
	// Storage is the connection string for the storage backend.
	// See pkg/libkapi for supported schemes (sqlite, postgres, mysql, etcd, ...).
	Storage string `default:"sqlite://kontinuum.db"`
}

// LogConfig holds logging configuration. Level and Format are stored as
// strings and parsed by pkg/logging, keeping this package dependency-free.
type LogConfig struct {
	// Level is one of: debug, info, warn, error. Defaults to "warn".
	Level string `default:"warn"`
	// Format is one of: console, text, json. Defaults to "json".
	// console and text are equivalent (colorful, human-readable).
	Format string `default:"json"`
}

// OIDCConfig configures OIDC authentication: bearer-token validation on the
// Kubernetes-style API and the PKCE browser login flow for the /app UI. An
// empty IssuerURL disables OIDC entirely, matching kontinuum's default of no
// authentication.
type OIDCConfig struct {
	// IssuerURL is the OIDC issuer URL (e.g. Dex). The discovery document is
	// fetched from {IssuerURL}/.well-known/openid-configuration at startup.
	IssuerURL string `default:""`
	// ClientID is the OAuth 2.0 public client ID registered with the issuer.
	// No client secret is used — authentication relies entirely on PKCE.
	ClientID string `default:"kontinuum"`
	// RedirectURL is the browser login flow's callback URL. It must exactly
	// match one of the redirect URIs registered with the issuer, and is
	// reused as both the login-initiation and callback endpoint since the
	// registered URI has no dedicated /callback path.
	RedirectURL string `default:"http://localhost:8080/app"`
	// AdminGroups is a comma-delimited list of OIDC groups granted full
	// (system:masters-equivalent) access. Members of system:masters are
	// always allowed; every other group has no access by default.
	AdminGroups string `default:""`
}

// Load reads configuration from KONTINUUM_-prefixed environment variables,
// falling back to the `default` struct tag when an env var is unset or empty.
// Env-var names are derived from each field's path (Server.Addr →
// KONTINUUM_SERVER_ADDR).
func Load() (*Config, error) {
	cfg := &Config{}
	loadStruct(reflect.ValueOf(cfg).Elem(), nil, true)

	return cfg, nil
}

// Defaults returns a Config populated with only the `default` struct tag
// values, ignoring environment variables. Useful for cobra flag defaults.
func Defaults() *Config {
	cfg := &Config{}
	loadStruct(reflect.ValueOf(cfg).Elem(), nil, false)

	return cfg
}

// Redact returns a copy of cfg with sensitive fields stripped, safe to log
// or display — currently, any username/password embedded in
// Server.Storage (e.g. "postgres://user:pass@host/db").
func Redact(cfg Config) Config {
	redacted := cfg
	redacted.Server.Storage = redactStorage(cfg.Server.Storage)

	return redacted
}

// redactStorage strips an embedded username/password from a storage
// connection string, leaving the scheme, host, path, and query intact.
func redactStorage(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	parsed.User = nil

	return parsed.String()
}

// loadStruct walks structVal recursively. For each string field, it sets the
// field from the KONTINUUM_-prefixed env var derived from path (when useEnv
// is true and the var is non-empty) or the field's `default` tag.
func loadStruct(structVal reflect.Value, path []string, useEnv bool) {
	for fieldIndex := range structVal.NumField() {
		field := structVal.Field(fieldIndex)
		fieldPath := make([]string, len(path)+1)
		copy(fieldPath, path)
		fieldPath[len(path)] = structVal.Type().Field(fieldIndex).Name

		if field.Kind() == reflect.Struct {
			loadStruct(field, fieldPath, useEnv)

			continue
		}

		if field.Kind() != reflect.String {
			continue
		}

		val := structVal.Type().Field(fieldIndex).Tag.Get("default")

		if useEnv {
			if env := os.Getenv(envName(fieldPath)); env != "" {
				val = env
			}
		}

		field.SetString(val)
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
