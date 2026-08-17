package etcdproxy

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidToken is returned by DecodeToken when the raw value isn't a
// base64("<zone>:<key>") credential — either malformed base64, or missing
// the ':' separator once decoded.
var ErrInvalidToken = errors.New("invalid bearer token")

// EncodeToken builds the credential a zone presents as
// "Authorization: Bearer <token>" against the hub's etcd gRPC proxy:
// base64(zone + ":" + key), where key is one of the zone's own two
// currently-valid auth keys (see pkg/domain/zone's reconcileAuthKeys).
func EncodeToken(zone, key string) string {
	return base64.StdEncoding.EncodeToString([]byte(zone + ":" + key))
}

// DecodeToken reverses EncodeToken, returning the zone name and its key.
// zone names are valid DNS labels (never containing ':'), so splitting on
// the first ':' unambiguously separates zone from key even though key
// itself could, in principle, contain one.
func DecodeToken(token string) (string, string, error) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	zone, key, ok := strings.Cut(string(raw), ":")
	if !ok || zone == "" || key == "" {
		return "", "", ErrInvalidToken
	}

	return zone, key, nil
}
