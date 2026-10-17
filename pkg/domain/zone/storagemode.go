package zone

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

var (
	// ErrStorageModeChanged is resolveZoneStorageMode's sentinel for a zone
	// whose recorded storage mode differs from the one the fleet resolves
	// now. A mode is fixed at install time: switching it means handing the
	// zone a different kind of storage DSN, which would rewrite its
	// kontinuum-env and restart it against storage it may not be able to
	// reach, so it's refused rather than applied.
	ErrStorageModeChanged = errors.New("storage mode changed since the zone was installed")
	// errStorageModeNotSupported is zoneStorageDSN's sentinel for a
	// resolved mode the zone controller can't install a zone in yet.
	errStorageModeNotSupported = errors.New("storage mode is not supported by the zone controller yet")
)

// resolveZoneStorageMode returns the storage mode zoneObj runs in, and
// records it on zoneObj.Status.StorageMode the first time — see that
// field's own doc. It is, in order:
//
//   - zoneObj.Status.StorageMode, once recorded;
//   - otherwise, for a zone installed before this field existed, the mode
//     its downstream kontinuum-env Secret's own KONTINUUM_SERVER_STORAGE
//     already shows (see storageModeOfDSN), so upgrading never silently
//     moves an existing zone onto a different mode;
//   - otherwise, for a fresh zone, r.StorageMode — the mode this process
//     resolved for the fleet at startup.
//
// Whichever it is, it's recorded, so the zone's detail page shows the mode
// it really runs in. It must also equal r.StorageMode, or the zone fails
// with ErrStorageModeChanged and nothing downstream is touched. The returned
// bool reports whether zoneObj.Status.StorageMode was changed in memory, so
// the caller persists it either way.
func (r *Reconciler) resolveZoneStorageMode(
	ctx context.Context, downstream client.Client, zoneObj *v1alpha2.Zone,
) (config.StorageMode, bool, error) {
	fleetMode := r.StorageMode
	if fleetMode == "" {
		fleetMode = config.StorageModeProxy
	}

	recorded := config.StorageMode(zoneObj.Status.StorageMode)

	if recorded == "" {
		installed, err := installedStorageMode(ctx, downstream)
		if err != nil {
			return "", false, err
		}

		recorded = installed
	}

	if recorded == "" {
		recorded = fleetMode
	}

	changed := zoneObj.Status.StorageMode != string(recorded)
	zoneObj.Status.StorageMode = string(recorded)

	if recorded != fleetMode {
		return "", changed, fmt.Errorf(
			"%w: zone %q runs in %s mode but this process resolves %s; "+
				"set KONTINUUM_SERVER_STORAGE_MODE=%s to keep the zone on its mode",
			ErrStorageModeChanged, zoneObj.Name, recorded, fleetMode, recorded)
	}

	return recorded, changed, nil
}

// installedStorageMode reads the storage mode a zone was already installed
// with off its downstream kontinuum-env Secret — empty when there is no
// such Secret yet, i.e. the zone has never been installed.
func installedStorageMode(ctx context.Context, downstream client.Client) (config.StorageMode, error) {
	var secret corev1.Secret

	err := downstream.Get(ctx, client.ObjectKey{Name: envSecretName, Namespace: downstreamNamespace}, &secret)
	if apierrors.IsNotFound(err) {
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("failed to get downstream %q secret: %w", envSecretName, err)
	}

	dsn, ok := secret.Data[storageSecretKey]
	if !ok || len(dsn) == 0 {
		return "", nil
	}

	return StorageModeOfDSN(string(dsn)), nil
}

// StorageModeOfDSN classifies a storage DSN the zone controller itself
// handed a zone: an etcdproxy relay DSN is StorageModeProxy, a nats:// one
// StorageModeNATS, and anything else the shared database's own connection
// string, StorageModeDirect. Unlike config.ResolveStorageMode this never
// resolves a host: it reads what was already decided, not what would be.
func StorageModeOfDSN(dsn string) config.StorageMode {
	if _, _, ok := etcdproxy.ParseRelayDSN(dsn); ok {
		return config.StorageModeProxy
	}

	parsed, err := url.Parse(dsn)
	if err == nil && parsed.Scheme == "nats" {
		return config.StorageModeNATS
	}

	return config.StorageModeDirect
}
