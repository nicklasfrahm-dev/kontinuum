package ui

import (
	"fmt"
	"net/url"
	"time"
)

const hoursPerDay = 24

// storageBackendName returns the friendly backend name for a storage
// connection string's URL scheme (e.g. "postgres://..." -> "PostgreSQL"),
// shown on the settings page. target is expected to already be redacted
// (see config.Config.Redact) — this only inspects the scheme.
func storageBackendName(target string) string {
	parsed, err := url.Parse(target)
	if err != nil {
		return target
	}

	switch parsed.Scheme {
	case "sqlite":
		return "SQLite"
	case "postgres", "postgresql":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	case "etcd":
		return "etcd"
	case "unix":
		return "Kine (Unix socket)"
	case "nats":
		return "NATS"
	default:
		return parsed.Scheme
	}
}

// formatAge renders t as a short relative age string (e.g. "5m", "3h", "2d").
func formatAge(t time.Time) string {
	if t.IsZero() {
		return "-"
	}

	elapsed := time.Since(t)

	switch {
	case elapsed < time.Minute:
		return fmt.Sprintf("%ds", int(elapsed.Seconds()))
	case elapsed < time.Hour:
		return fmt.Sprintf("%dm", int(elapsed.Minutes()))
	case elapsed < hoursPerDay*time.Hour:
		return fmt.Sprintf("%dh", int(elapsed.Hours()))
	default:
		return fmt.Sprintf("%dd", int(elapsed.Hours()/hoursPerDay))
	}
}
