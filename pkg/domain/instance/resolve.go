package instance

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// AnnotationHostname records the DNS hostname ResolveAddress resolved to
// reach spec.interfaces[0]'s IP — see ResolveAddress's own doc for why the
// spec itself only ever stores the resolved IP, never the hostname that was
// typed in.
const AnnotationHostname = "kontinuum.sh/hostname"

// errHostnameNoAddresses is ResolveAddress's own sentinel — err113 flags a
// dynamically constructed errors.New/fmt.Errorf call without a wrapped
// static error, same as Add's own errAddOptionsMissingField.
var errHostnameNoAddresses = errors.New("hostname did not resolve to any address")

// Resolver resolves a hostname to its IP addresses — the LookupHost subset
// of *net.Resolver that ResolveAddress needs. AddOptions.Resolver injects
// one; net.DefaultResolver satisfies this out of the box, and is what Add
// uses when AddOptions.Resolver is left nil.
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// ResolveAddress returns address unchanged, with an empty hostname, when
// address is already an IP literal (net.ParseIP succeeds) — the common case,
// requiring no lookup. Otherwise it treats address as a DNS hostname,
// resolves it via resolver (net.DefaultResolver when nil), and returns the
// first address the lookup gives back, alongside address itself as hostname
// — so a caller can store the resolved IP in spec.interfaces while
// annotating the Instance with AnnotationHostname, preserving a record of
// what was actually typed in. This is what keeps an Instance registered by
// hostname byte-for-byte identical — same Name (from NameFromAddress), same
// Spec — to one registered directly by that same resolved IP. Exported for
// zone.Add to reuse: BuildAddObjects' seed Instance must converge on the
// exact same identity Add below gives that same address, hostname or not.
func ResolveAddress(ctx context.Context, resolver Resolver, address string) (string, string, error) {
	if net.ParseIP(address) != nil {
		return address, "", nil
	}

	if resolver == nil {
		resolver = net.DefaultResolver
	}

	addrs, err := resolver.LookupHost(ctx, address)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve hostname %q: %w", address, err)
	}

	if len(addrs) == 0 {
		return "", "", fmt.Errorf("%w: %q", errHostnameNoAddresses, address)
	}

	return addrs[0], address, nil
}
