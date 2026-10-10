package zone

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"golang.org/x/mod/semver"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// storageSecretKey is the key a Kontinuum's own confidential storage
// connection string is stored under in the Secret its status.secretRef
// points to — must match pkg/config's own env-var name for
// Server.Storage (KONTINUUM_SERVER_STORAGE) and
// pkg/domain/registry.Heartbeat's SecretData, since that Secret's keys are
// meant to be used directly via envFrom with no translation layer.
// Duplicated rather than imported — pkg/domain/registry's own
// storageSecretKey is unexported, and this package's downstream-cluster
// Secret ensureSecret writes reuses the exact same key name so the value
// can be used directly via envFrom, same rationale as
// downstreamclient.go's kubeconfigSecretKey duplication.
//
//nolint:gosec // false positive: an env var / secret key name, not a credential value
const storageSecretKey = "KONTINUUM_SERVER_STORAGE"

// errNoRegisteredKontinuum is a static sentinel — err113 flags a
// dynamically constructed errors.New/fmt.Errorf call without a wrapped
// static error.
var errNoRegisteredKontinuum = errors.New("no registered kontinuum found")

// errNoKontinuumDNSDomain is findKontinuumDomain's sentinel for "a
// Kontinuum was found, but it (and by extension every other one, since
// they're all meant to share this same config) has no DNS domain
// configured" — a static sentinel for the same err113 reason as
// errNoRegisteredKontinuum above.
var errNoKontinuumDNSDomain = errors.New("no registered kontinuum publishes a DNS domain")

// anyRegisteredKontinuum returns any one registered Kontinuum —
// deterministically (name-sorted), not just "first in whatever order List
// returned", mirroring pkg/domain/instancepool's own claim/release
// determinism. Every registered Kontinuum, hub or worker alike, is assumed
// to share the same deployment-wide config (storage backend, DNS domain —
// see issue #24's architecture: these are "a property of the deployment,
// not of Role"), so which one gets picked doesn't matter. Zero Kontinuums
// should basically never happen — the hub always self-registers — but is
// handled as a retryable condition, not a panic.
func anyRegisteredKontinuum(ctx context.Context, hubClient client.Client) (*v1alpha2.Kontinuum, error) {
	var list v1alpha2.KontinuumList

	err := hubClient.List(ctx, &list)
	if err != nil {
		return nil, fmt.Errorf("failed to list kontinuums: %w", err)
	}

	if len(list.Items) == 0 {
		return nil, errNoRegisteredKontinuum
	}

	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })

	return &list.Items[0], nil
}

// FindJoinedKontinuum returns the Kontinuum registered for the given
// region/zone — the one ensureConfigMap's own KONTINUUM_SERVER_REGION/_ZONE
// env vars cause that zone's own kontinuum-server to register itself as,
// once it heartbeats (see pkg/domain/registry.Heartbeat) — or ok=false if
// none has joined yet. Matched by spec, not by name: a Kontinuum's own
// object name is registry.InstanceName(os.Hostname()), arbitrary and with
// no fixed relationship to the zone's own <region>-<zone> name. A
// Kontinuum that exists but has never actually heartbeated
// (status.lastHeartbeatTime still zero — possible for the brief window
// between Create and its first beat) doesn't count as joined, mirroring
// what a human staring at the registry page would consider "actually
// there." Exported so both this package's own Reconciler (the Zone
// RegistryJoined condition — see controller.go) and pkg/ui's zone detail
// page (showing whether/which Kontinuum joined) share one definition of
// "has this zone's kontinuum-server joined the registry," rather than two
// that could drift apart.
func FindJoinedKontinuum(
	ctx context.Context, hubClient client.Client, region, zoneName string,
) (*v1alpha2.Kontinuum, bool, error) {
	var list v1alpha2.KontinuumList

	err := hubClient.List(ctx, &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace))
	if err != nil {
		return nil, false, fmt.Errorf("failed to list kontinuums: %w", err)
	}

	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })

	for index := range list.Items {
		item := &list.Items[index]

		if item.Spec.Region == region && item.Spec.Zone == zoneName && !item.Status.LastHeartbeatTime.IsZero() {
			return item, true, nil
		}
	}

	return nil, false, nil
}

// findKontinuumDomain returns the DNS domain any registered Kontinuum
// publishes on its own status.config.server.dns.domain — set from
// KONTINUUM_SERVER_DNS_DOMAIN, the same env var every kontinuum serve
// process (hub or worker) reads at startup (see pkg/config). Unlike
// storage, this is non-confidential, so it's read directly off the
// Kontinuum object's own status — no Secret fetch needed. Add calls this
// when the caller (kontinuum zone add, or the UI's "Add zone" form) leaves
// JoinOptions.Domain empty, exactly mirroring how the zone controller
// itself infers storage during downstream install (see
// controller.go's reconcileInstall) — an operator configures the domain
// once, on the hub, rather than exporting it before every zone add.
func findKontinuumDomain(ctx context.Context, hubClient client.Client) (string, error) {
	kontinuum, err := anyRegisteredKontinuum(ctx, hubClient)
	if err != nil {
		return "", err
	}

	domain := kontinuum.Status.Config.Server.DNS.Domain
	if domain == "" {
		return "", fmt.Errorf("%w: %q has none configured", errNoKontinuumDNSDomain, kontinuum.Name)
	}

	return domain, nil
}

// inferDomain returns findKontinuumDomain's result, but treats there being
// nothing to infer from (no registered Kontinuum at all, or one exists but
// publishes no domain) as a soft "" rather than an error: a zone's own
// hostname has no reason to match whatever domain the hub happens to
// publish, and a Zone with no domain configured at all is a legitimate,
// supported choice (see controller.go's reconcileInstall — it just skips
// installing a network layer for one). Any other error — the List call
// itself failing — still surfaces, since that's a real problem distinct
// from having nothing to infer.
func inferDomain(ctx context.Context, hubClient client.Client) (string, error) {
	domain, err := findKontinuumDomain(ctx, hubClient)

	switch {
	case err == nil:
		return domain, nil
	case errors.Is(err, errNoRegisteredKontinuum), errors.Is(err, errNoKontinuumDNSDomain):
		return "", nil
	default:
		return "", fmt.Errorf("failed to infer domain: %w", err)
	}
}

// errNoKontinuumVersion is findKontinuumVersion's own sentinel — a static
// error for the same err113 reason as errNoRegisteredKontinuum/
// errNoKontinuumDNSDomain above.
var errNoKontinuumVersion = errors.New("no registered kontinuum reports a version yet")

// findKontinuumVersion returns the highest build version any registered
// Kontinuum reports on its own status.version, written on every heartbeat
// (see pkg/domain/registry/heartbeat.go's beat/reregister) — used by
// resolveImage to pick which tag of ImageRepo to deploy onto a newly
// joined zone.
//
// Unlike anyRegisteredKontinuum (fine for storage/domain, which really are
// one shared deployment-wide value), a fleet's reported version is only
// uniform outside of a rollout: mid hub-upgrade, some replicas — and the
// worker Kontinuums that haven't yet been bumped — still report the old
// version while others already report the new one. zonelease can hand this
// Zone's reconcile to whichever hub replica currently holds the lease,
// including a not-yet-upgraded one, so picking "any" registered Kontinuum
// risks deploying a stale tag even after the fleet has mostly moved on.
// Scanning every registered Kontinuum and taking the highest instead means
// the outcome only ever depends on what the fleet has collectively
// achieved, not on which replica's reconcile happened to run or which
// Kontinuum a name sort happened to land on first.
//
// semver.Compare treats an invalid version (a floating "dev"/"latest" tag)
// as less than any valid one, and all invalid versions as equal to each
// other — so a real semver release always outranks a floating tag, and a
// tie (every registered Kontinuum floating, or genuinely equal versions)
// falls back to whichever sorts first by name, mirroring
// anyRegisteredKontinuum's own tie-break.
func findKontinuumVersion(ctx context.Context, hubClient client.Client) (string, error) {
	var list v1alpha2.KontinuumList

	err := hubClient.List(ctx, &list)
	if err != nil {
		return "", fmt.Errorf("failed to list kontinuums: %w", err)
	}

	if len(list.Items) == 0 {
		return "", errNoRegisteredKontinuum
	}

	// One sort, ordered by version descending, does the whole job: the
	// highest version is always list.Items[0] afterward, so there's no
	// separate scan-and-compare pass needed on top of it.
	sort.Slice(list.Items, func(i, j int) bool {
		left, right := list.Items[i], list.Items[j]

		if cmp := semver.Compare(left.Status.Version, right.Status.Version); cmp != 0 {
			return cmp > 0
		}

		return left.Name < right.Name
	})

	highest := list.Items[0].Status.Version
	if highest == "" {
		return "", fmt.Errorf("%w: none of the %d registered kontinuums reported one", errNoKontinuumVersion, len(list.Items))
	}

	return highest, nil
}
