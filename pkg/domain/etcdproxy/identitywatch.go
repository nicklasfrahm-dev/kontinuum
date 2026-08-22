package etcdproxy

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KeySource supplies the ed25519 private key jwtCredentials signs every
// outbound RPC with (see SignToken) — Relay reads it fresh on every call
// rather than capturing one at startup, so a caller backed by
// IdentityKeySource can rotate the key underneath an already-running Relay
// with no restart of any kind.
type KeySource interface {
	Current() ed25519.PrivateKey
}

// staticKeySource implements KeySource over a key that never changes — see
// StaticKey.
type staticKeySource ed25519.PrivateKey

func (s staticKeySource) Current() ed25519.PrivateKey {
	return ed25519.PrivateKey(s)
}

// StaticKey wraps a fixed key as a KeySource, for callers (chiefly tests)
// that already hold the one key they'll ever use and have no need for
// IdentityKeySource's own live rotation.
//
//nolint:ireturn // KeySource is this package's own seam — see its own doc
func StaticKey(key ed25519.PrivateKey) KeySource {
	return staticKeySource(key)
}

// IdentityKeySource is a KeySource kept up to date by WatchIdentity for as
// long as its watch goroutine keeps running — see that function's own doc.
// Safe for concurrent use: Current is called on every signed RPC (see
// jwtCredentials.GetRequestMetadata) while the watch goroutine updates it
// in the background.
type IdentityKeySource struct {
	mu  sync.RWMutex
	key ed25519.PrivateKey
}

// Current implements KeySource.
func (s *IdentityKeySource) Current() ed25519.PrivateKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.key
}

func (s *IdentityKeySource) set(key ed25519.PrivateKey) {
	s.mu.Lock()
	s.key = key
	s.mu.Unlock()
}

// NewInClusterIdentityWatcher builds a client.WithWatch against the
// cluster this process is itself running in (see rest.InClusterConfig) —
// authenticated with whatever ServiceAccount token Kubernetes projects
// into this pod (see pkg/domain/zone/workload.go's
// etcdIdentityServiceAccountName), and scoped, via that ServiceAccount's
// own Role/RoleBinding, to exactly the one Secret WatchIdentity reads.
// Only core/v1 is registered on its scheme — the one Kind this client is
// ever used for.
//
//nolint:ireturn // client.WithWatch is controller-runtime's own seam, mirrors client.New's client.Client return
func NewInClusterIdentityWatcher() (client.WithWatch, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build in-cluster config: %w", err)
	}

	scheme := runtime.NewScheme()

	err = corev1.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register core/v1 scheme: %w", err)
	}

	watchClient, err := client.NewWithWatch(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to build in-cluster client: %w", err)
	}

	return watchClient, nil
}

// identityWatchRetryInterval is how long run waits before reconnecting
// after a failed or closed watch — long enough not to hammer the
// apiserver through a real outage, short enough that a transient
// disconnect doesn't leave IdentityKeySource stale for long.
const identityWatchRetryInterval = 5 * time.Second

// WatchIdentity loads namespace's own IdentitySecretName Secret through
// watchClient, then keeps the returned IdentityKeySource up to date in the
// background for as long as ctx stays alive. This replaces the former
// approach of reading the private key off a mounted Secret volume once at
// process startup and relying on a rolling restart of the zone's own
// kontinuum Deployment to pick up a rotated one (see
// pkg/domain/zone/ensureEtcdIdentity's own doc) — watchClient is expected
// to come from NewInClusterIdentityWatcher, scoped by RBAC to read and
// watch exactly this one Secret.
//
// The initial load is synchronous, so a returned nil error means the
// caller already has a usable key in hand; every later rotation lands in
// the background, observed by the next call to Current. The watch is
// itself established before that initial load, not after — otherwise a
// rotation landing in the gap between the two would be silently missed
// (nothing left to notice it: the watch wasn't listening yet, and the one
// Get already happened).
func WatchIdentity(
	ctx context.Context, watchClient client.WithWatch, namespace string, logger *slog.Logger,
) (*IdentityKeySource, error) {
	source := &IdentityKeySource{}

	watcher, err := startIdentityWatch(ctx, watchClient, namespace)
	if err != nil {
		return nil, err
	}

	key, err := getIdentityKey(ctx, watchClient, namespace)
	if err != nil {
		watcher.Stop()

		return nil, err
	}

	source.set(key)

	go source.run(ctx, watchClient, namespace, watcher, logger)

	return source, nil
}

// startIdentityWatch opens a watch over every Secret in namespace — not
// just IdentitySecretName, since a field selector scoped to one object
// name isn't something every client.WithWatch implementation (including
// the fake one this package's own tests use) is guaranteed to honor;
// run's own handleEvent filters down to the one Secret that matters.
//
//nolint:ireturn // watch.Interface is client.WithWatch's own return type
func startIdentityWatch(ctx context.Context, watchClient client.WithWatch, namespace string) (watch.Interface, error) {
	var list corev1.SecretList

	watcher, err := watchClient.Watch(ctx, &list, client.InNamespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to watch %q secret: %w", IdentitySecretName, err)
	}

	return watcher, nil
}

// getIdentityKey fetches and parses namespace's own IdentitySecretName
// Secret.
func getIdentityKey(ctx context.Context, reader client.Reader, namespace string) (ed25519.PrivateKey, error) {
	var secret corev1.Secret

	key := client.ObjectKey{Name: IdentitySecretName, Namespace: namespace}

	err := reader.Get(ctx, key, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secret: %w", IdentitySecretName, err)
	}

	privateKey, err := LoadPrivateKey(secret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return nil, fmt.Errorf("failed to parse %q secret: %w", IdentitySecretName, err)
	}

	return privateKey, nil
}

// run consumes watcher — already established by WatchIdentity, see its
// own doc for why that matters — until it closes, then reconnects
// (calling startIdentityWatch again) and keeps going, until ctx is done.
// This is what makes a transient apiserver disconnect (or the watch
// simply timing out server-side, as every long-poll watch eventually
// does) never permanently strand source on a stale key.
func (s *IdentityKeySource) run(
	ctx context.Context, watchClient client.WithWatch, namespace string, watcher watch.Interface, logger *slog.Logger,
) {
	for {
		s.consume(ctx, watcher, logger)
		watcher.Stop()

		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(identityWatchRetryInterval):
		}

		next, err := startIdentityWatch(ctx, watchClient, namespace)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error("failed to reconnect etcd proxy identity watch", "error", err)
			}
			// watch.NewEmptyWatch's own ResultChan is already closed, so
			// consume returns immediately on the next iteration — this loop
			// simply retries again after another identityWatchRetryInterval,
			// the same backoff as any other reconnect.
			next = watch.NewEmptyWatch()
		}

		watcher = next
	}
}

// consume reads watcher's own ResultChan until it closes or ctx is done —
// checked explicitly, rather than trusting every client.WithWatch
// implementation to close ResultChan on its own once ctx is canceled (the
// fake one this package's own tests use never does) — updating s on every
// rotation of IdentitySecretName it observes. A delete is deliberately
// ignored rather than clearing the key: the zone's own ensureEtcdIdentity
// never deletes this Secret, only ever replaces its contents.
func (s *IdentityKeySource) consume(ctx context.Context, watcher watch.Interface, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return
			}

			s.handleEvent(event, logger)
		}
	}
}

// handleEvent updates s from event when it's an Added/Modified for
// IdentitySecretName — every other event (a different Secret in the same
// namespace, a Deleted, an Error) is ignored, see consume's own doc for
// Deleted specifically.
func (s *IdentityKeySource) handleEvent(event watch.Event, logger *slog.Logger) {
	if event.Type != watch.Added && event.Type != watch.Modified {
		return
	}

	secret, ok := event.Object.(*corev1.Secret)
	if !ok || secret.Name != IdentitySecretName {
		return
	}

	key, err := LoadPrivateKey(secret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		logger.Error("failed to parse rotated etcd proxy identity secret", "error", err)

		return
	}

	s.set(key)

	logger.Info("etcd proxy identity rotated")
}
