package zone

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
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

// errNoKontinuumDNSCredential is findKontinuumDNSCredential's sentinel for
// "a Kontinuum was found, but its own Secret has no DNS credential stored" —
// the expected, common case for an operator who hasn't configured
// KONTINUUM_SERVER_DNS_CREDENTIAL, not a real error (see reconcileDNS's own
// doc for how this is used to skip DNS record management entirely).
var errNoKontinuumDNSCredential = errors.New("no registered kontinuum has a DNS credential configured")

// dnsCredentialSecretKey is the key a Kontinuum's own DNS provider
// credential is stored under in the Secret its status.secretRef points to —
// must match pkg/config's own env-var name for Server.DNS.Credential
// (KONTINUUM_SERVER_DNS_CREDENTIAL) and
// pkg/domain/registry.Controller.Config's own dnsCredentialSecretKey.
// Duplicated rather than imported — same rationale as storageSecretKey's
// own doc above.
//
//nolint:gosec // false positive: an env var / secret key name, not a credential value
const dnsCredentialSecretKey = "KONTINUUM_SERVER_DNS_CREDENTIAL"

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

// findKontinuumVersion returns the build version any registered Kontinuum
// reports on its own status.version, written on every heartbeat (see
// pkg/domain/registry/heartbeat.go's beat/reregister) — used by
// resolveImage to pick which tag of ImageRepo to deploy onto a newly
// joined zone. Every registered Kontinuum is assumed to run the same
// version, same rationale as anyRegisteredKontinuum's own doc.
func findKontinuumVersion(ctx context.Context, hubClient client.Client) (string, error) {
	kontinuum, err := anyRegisteredKontinuum(ctx, hubClient)
	if err != nil {
		return "", err
	}

	if kontinuum.Status.Version == "" {
		return "", fmt.Errorf("%w: %q has none reported", errNoKontinuumVersion, kontinuum.Name)
	}

	return kontinuum.Status.Version, nil
}

// findKontinuumDNSCredential returns the raw, credential-bearing DNS
// provider credential from any registered Kontinuum's own Secret — set from
// KONTINUUM_SERVER_DNS_CREDENTIAL (see
// v1alpha2.KontinuumDNSConfigStatus.Credential's own doc) and stored
// alongside storage in the same Secret (see registry.Controller's own
// dnsCredentialSecretKey). Mirrors findKontinuumStorage exactly; a missing
// key (the expected case when an operator hasn't configured DNS
// credentials) returns errNoKontinuumDNSCredential, which reconcileDNS
// treats as "skip DNS record management for this Zone", not a retryable
// error.
func findKontinuumDNSCredential(ctx context.Context, hubClient client.Client) (string, error) {
	kontinuum, err := anyRegisteredKontinuum(ctx, hubClient)
	if err != nil {
		return "", err
	}

	ref := kontinuum.Status.SecretRef

	var secret corev1.Secret

	err = hubClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return "", fmt.Errorf("failed to fetch %q secret to load dns credentials: %w", ref.Name, err)
	}

	credential, ok := secret.Data[dnsCredentialSecretKey]
	if !ok || len(credential) == 0 {
		return "", fmt.Errorf("%w: %q has no stored dns credential", errNoKontinuumDNSCredential, ref.Name)
	}

	return string(credential), nil
}
