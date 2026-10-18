package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

// StorageMode is how a joined zone's own kontinuum-server reaches the shared
// state every process in a fleet reconciles against — see
// KONTINUUM_SERVER_STORAGE_MODE (Server.StorageMode) and
// ResolveStorageMode.
type StorageMode string

const (
	// StorageModeAuto infers the mode from the storage connection string —
	// see ResolveStorageMode. Only ever a configured value, never a
	// resolved one.
	StorageModeAuto StorageMode = "Auto"
	// StorageModeProxy hands every zone an etcdproxy relay DSN back to the
	// hub's own etcd gRPC proxy, so only the hub ever holds the database
	// credential — the right choice when the database is only reachable
	// from the hub (a private address, or a local sqlite file).
	StorageModeProxy StorageMode = "Proxy"
	// StorageModeDirect hands every zone the shared database's own
	// connection string, so every process connects to it directly. Requires
	// a database every zone can reach, and every zone then holds its full
	// credential.
	StorageModeDirect StorageMode = "Direct"
	// StorageModeNATS runs the shared state on a NATS JetStream cluster.
	StorageModeNATS StorageMode = "NATS"
)

// ErrInvalidStorageMode is ParseStorageMode's sentinel for a value that
// isn't one of the StorageMode constants.
var ErrInvalidStorageMode = errors.New("invalid storage mode, must be one of Auto, Proxy, Direct or NATS")

// ParseStorageMode parses raw as a configured StorageMode. Matching is
// exact: like every other enum in this repo, the values are PascalCase, and
// a near-miss such as "direct" is rejected rather than silently accepted.
// An empty raw parses as StorageModeAuto, the field's own default.
func ParseStorageMode(raw string) (StorageMode, error) {
	switch mode := StorageMode(raw); mode {
	case "":
		return StorageModeAuto, nil
	case StorageModeAuto, StorageModeProxy, StorageModeDirect, StorageModeNATS:
		return mode, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidStorageMode, raw)
	}
}

// StorageResolver resolves a hostname to its IP addresses — the LookupHost
// subset of *net.Resolver that ResolveStorageMode needs. net.DefaultResolver
// satisfies it; tests inject a stub instead of making real DNS queries.
type StorageResolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// natsScheme is the storage connection string scheme that selects
// StorageModeNATS.
const natsScheme = "nats"

// ResolveStorageMode resolves the StorageMode a fleet runs in from its
// storage connection string. A configured override other than
// StorageModeAuto short-circuits inference entirely — the escape hatch for
// split-horizon DNS, where the address this process resolves isn't the one
// every zone would. Otherwise:
//
//   - a nats:// connection string is StorageModeNATS;
//   - a Kine SQL connection string (postgres://, postgresql://, mysql://)
//     whose host resolves only to globally routable addresses (see
//     IsGloballyRoutable) is StorageModeDirect;
//   - everything else is StorageModeProxy: a sqlite:// file, a host with
//     any non-routable address, a host that doesn't resolve at all, and
//     anything that isn't a Kine connection string.
//
// resolver defaults to net.DefaultResolver when nil.
func ResolveStorageMode(
	ctx context.Context, resolver StorageResolver, storage string, override StorageMode,
) (StorageMode, error) {
	if override == "" {
		override = StorageModeAuto
	}

	_, err := ParseStorageMode(string(override))
	if err != nil {
		return "", err
	}

	if override != StorageModeAuto {
		return override, nil
	}

	scheme, host, ok := storageHost(storage)
	if !ok {
		return StorageModeProxy, nil
	}

	if scheme == natsScheme {
		return StorageModeNATS, nil
	}

	if host == "" {
		return StorageModeProxy, nil
	}

	if resolveRoutable(ctx, resolver, host) {
		return StorageModeDirect, nil
	}

	return StorageModeProxy, nil
}

// storageHost extracts the scheme and bare host (no port, no brackets) of a
// storage connection string. ok is false for anything that doesn't parse or
// isn't a scheme ResolveStorageMode infers anything from. The host is empty
// for a connection string with none, such as sqlite://kontinuum.db's
// relative path parsed as a host-less URL.
func storageHost(storage string) (string, string, bool) {
	if host, found := mysqlTCPHost(storage); found {
		return "mysql", host, true
	}

	parsed, err := url.Parse(storage)
	if err != nil {
		return "", "", false
	}

	switch scheme := strings.ToLower(parsed.Scheme); scheme {
	case natsScheme, "postgres", "postgresql", "mysql":
		return scheme, parsed.Hostname(), true
	default:
		return "", "", false
	}
}

// mysqlTCPHost returns the bare host of a mysql:// connection string in
// Kine's go-sql-driver form, "mysql://user:pass@tcp(host:port)/db", which
// url.Parse rejects outright because of the parentheses. found is false
// for any other connection string, which url.Parse then handles.
func mysqlTCPHost(storage string) (string, bool) {
	rest, isMySQL := strings.CutPrefix(storage, "mysql://")
	if !isMySQL {
		return "", false
	}

	_, address, hasTCP := strings.Cut(rest, "tcp(")
	if !hasTCP {
		return "", false
	}

	address, _, _ = strings.Cut(address, ")")

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}

	return strings.Trim(host, "[]"), true
}

// resolveRoutable reports whether host — an IP literal or a hostname — is
// globally routable: an IP literal is classified directly; a hostname must
// resolve, and every address it resolves to must be globally routable. A
// single non-routable record (a hostname with mixed public and private
// records) or a failed lookup counts as not routable, since some zone could
// then end up with an address only the hub can reach.
func resolveRoutable(ctx context.Context, resolver StorageResolver, host string) bool {
	literal, err := netip.ParseAddr(host)
	if err == nil {
		return IsGloballyRoutable(literal)
	}

	if resolver == nil {
		resolver = net.DefaultResolver
	}

	addrs, err := resolver.LookupHost(ctx, host)
	if err != nil || len(addrs) == 0 {
		return false
	}

	for _, raw := range addrs {
		addr, err := netip.ParseAddr(raw)
		if err != nil || !IsGloballyRoutable(addr) {
			return false
		}
	}

	return true
}

// cgnatPrefixBits is the prefix length of RFC 6598's 100.64.0.0/10.
const cgnatPrefixBits = 10

// cgnatPrefix returns RFC 6598's shared address space, used for
// carrier-grade NAT — not covered by netip.Addr.IsPrivate, which only knows
// RFC 1918 and IPv6 ULA.
func cgnatPrefix() netip.Prefix {
	return netip.PrefixFrom(netip.AddrFrom4([4]byte{100, 64, 0, 0}), cgnatPrefixBits)
}

// IsGloballyRoutable reports whether addr is reachable from the public
// internet, i.e. not one of: RFC 1918 private (10/8, 172.16/12,
// 192.168/16), RFC 6598 CGNAT (100.64/10), loopback (127/8, ::1),
// link-local (169.254/16, fe80::/10), IPv6 ULA (fc00::/7), unspecified,
// multicast, or an invalid address. An IPv4-mapped IPv6 address is
// classified by the IPv4 address it maps.
func IsGloballyRoutable(addr netip.Addr) bool {
	addr = addr.Unmap()

	switch {
	case !addr.IsValid(),
		addr.IsUnspecified(),
		addr.IsPrivate(),
		addr.IsLoopback(),
		addr.IsLinkLocalUnicast(),
		addr.IsLinkLocalMulticast(),
		addr.IsInterfaceLocalMulticast(),
		addr.IsMulticast(),
		cgnatPrefix().Contains(addr):
		return false
	default:
		return true
	}
}

// ValidateStorageMode checks that Server.StorageMode is one of the
// StorageMode values, so a typo fails at startup rather than surfacing the
// first time a zone is reconciled.
func (c *Config) ValidateStorageMode() error {
	_, err := ParseStorageMode(c.Server.StorageMode)

	return err
}
