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
var errNoRegisteredKontinuum = errors.New("no registered kontinuum found to source storage credentials from")

// findKontinuumStorage returns the raw, credential-bearing storage
// connection string from any registered Kontinuum's own Secret — every
// Kontinuum, hub or worker alike, upserts this on every heartbeat (see
// pkg/domain/registry/heartbeat.go's ensureSecret), and every one of them
// necessarily shares the same storage backend (see issue #24's
// architecture: "storage is a property of the deployment, not of Role").
// Zero Kontinuums should basically never happen — the hub always
// self-registers — but is handled as a retryable condition, not a panic.
// Deterministic (name-sorted), not just "first in whatever order List
// returned", mirroring pkg/domain/instancepool's own claim/release
// determinism.
func findKontinuumStorage(ctx context.Context, hubClient client.Client) (string, error) {
	var list v1alpha2.KontinuumList

	err := hubClient.List(ctx, &list)
	if err != nil {
		return "", fmt.Errorf("failed to list kontinuums: %w", err)
	}

	if len(list.Items) == 0 {
		return "", errNoRegisteredKontinuum
	}

	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })

	ref := list.Items[0].Status.SecretRef

	var secret corev1.Secret

	err = hubClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return "", fmt.Errorf("failed to fetch %q secret to load storage credentials: %w", ref.Name, err)
	}

	storage, ok := secret.Data[storageSecretKey]
	if !ok {
		return "", fmt.Errorf("%w: %q has no stored storage key yet", errNoRegisteredKontinuum, ref.Name)
	}

	return string(storage), nil
}
