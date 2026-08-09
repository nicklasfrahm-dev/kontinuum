package crd

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// migrateDeleteTimeout bounds how long MigrateScope waits for a CRD it just
// deleted to actually disappear.
const migrateDeleteTimeout = 30 * time.Second

// MigrateScope handles the one CRD-scope transition Kubernetes can't apply
// in place: an already-applied CRD that's still Cluster-scoped, while def's
// generated manifest (see Build) now says Namespaced — exactly the change
// issue #63's architecture makes to Zone/Instance/InstancePool/TalosCluster/
// Kontinuum. Ensure's own create-or-update (see ensureEstablished/Apply)
// would otherwise just fail trying to Update an immutable field.
//
// When that exact transition is detected, MigrateScope lists every existing
// object of def's kind (via def.GVKs[0], the storage version), deletes the
// CRD — which cascades to delete all of them, since that's the only way
// Kubernetes allows a scope change — and waits for the deletion to finish.
// It returns the drained objects so the caller can recreate them, namespaced,
// once its own call to Ensure has (re)established the CRD fresh — see
// RestoreMigrated.
//
// Returns (nil, nil), doing nothing, in every other case: a fresh install
// with no existing CRD, a CRD that's already Namespaced (already migrated,
// or a kind that was never Cluster-scoped to begin with, like Addon), or one
// whose manifest still says Cluster (nothing this repo does today, but not
// this function's job to migrate).
func MigrateScope(
	ctx context.Context, loopbackConfig *restclient.Config, manifestFS fs.FS, def Definition, logger *slog.Logger,
) ([]unstructured.Unstructured, error) {
	apiextensionsClient, err := apiextensionsclientset.NewForConfig(loopbackConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	crds := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions()

	existing, err := crds.Get(ctx, def.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch existing %s crd: %w", def.Name, err)
	}

	desired := Build(manifestFS, def)
	if existing.Spec.Scope != apiextensionsv1.ClusterScoped || desired.Spec.Scope != apiextensionsv1.NamespaceScoped {
		return nil, nil
	}

	logger.Warn("crd scope changed from Cluster to Namespaced, migrating existing objects — "+
		"every existing object will be recreated with a new resourceVersion/UID",
		"crd", def.Name)

	items, err := drainExisting(ctx, loopbackConfig, def, logger)
	if err != nil {
		return nil, err
	}

	err = crds.Delete(ctx, def.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to delete %s crd to migrate its scope: %w", def.Name, err)
	}

	err = waitForDeleted(ctx, crds, def.Name, logger)
	if err != nil {
		return nil, err
	}

	return items, nil
}

// drainExisting waits for def's already-Established CRD to actually resolve
// through a RESTMapper here, then lists every existing object of its kind —
// MigrateScope's own doc explains why the wait is needed: Established alone,
// on a fresh process start, doesn't mean the discovery document has already
// propagated to this process's own RESTMapper — the same cold-start gap
// Ensure's own waitForDiscoverable closes for a CRD this process just
// created. Skipping it made the List below race that propagation and fail
// outright on a real process restart against already-populated storage,
// even though the otherwise-identical scenario inside a single long-lived
// test process (see pkg/crd's own e2e test) never hit it.
func drainExisting(
	ctx context.Context, loopbackConfig *restclient.Config, def Definition, logger *slog.Logger,
) ([]unstructured.Unstructured, error) {
	err := waitForDiscoverable(ctx, loopbackConfig, def, logger)
	if err != nil {
		return nil, fmt.Errorf("failed waiting for existing %s to become discoverable before migrating: %w", def.Name, err)
	}

	kind := def.GVKs[0]
	listKind := schema.GroupVersionKind{Group: kind.Group, Version: kind.Version, Kind: kind.Kind + "List"}

	items, err := listExisting(ctx, loopbackConfig, listKind)
	if err != nil {
		return nil, fmt.Errorf("failed to list existing %s objects before migrating: %w", def.Name, err)
	}

	return items, nil
}

// RestoreMigrated recreates every one of items (drained by an earlier
// MigrateScope call) in namespace — called once the caller's own Ensure has
// (re)established the CRD fresh. Each item is stripped of its old
// resourceVersion/UID/generation (neither is valid anymore: the CRD itself,
// and so every object of its kind, was deleted and recreated) and given
// namespace, preserving name, labels, annotations, and spec; status, which
// Create always strips regardless of what's in the payload, is restored
// afterward via a separate Status().Update. A create racing something else
// that already recreated the same name (AlreadyExists) is tolerated, not an
// error — this makes re-running a startup that got interrupted mid-restore
// safe.
func RestoreMigrated(
	ctx context.Context, loopbackConfig *restclient.Config, items []unstructured.Unstructured, namespace string,
	logger *slog.Logger,
) error {
	if len(items) == 0 {
		return nil
	}

	kubeClient, err := newUnstructuredClient(loopbackConfig)
	if err != nil {
		return err
	}

	for index := range items {
		err := restoreOne(ctx, kubeClient, &items[index], namespace, logger)
		if err != nil {
			return err
		}
	}

	return nil
}

// restoreOne is RestoreMigrated's own per-object body.
func restoreOne(
	ctx context.Context, kubeClient client.Client, item *unstructured.Unstructured, namespace string,
	logger *slog.Logger,
) error {
	status, hasStatus, err := unstructured.NestedMap(item.Object, "status")
	if err != nil {
		return fmt.Errorf("failed to read status off migrated %s %q: %w", item.GetKind(), item.GetName(), err)
	}

	obj := item.DeepCopy()
	obj.SetNamespace(namespace)
	obj.SetResourceVersion("")
	obj.SetUID("")
	obj.SetGeneration(0)
	obj.SetManagedFields(nil)
	unstructured.RemoveNestedField(obj.Object, "status")

	err = kubeClient.Create(ctx, obj)
	if apierrors.IsAlreadyExists(err) {
		logger.Warn("migrated object already recreated, leaving it as-is",
			"kind", obj.GetKind(), "name", obj.GetName(), "namespace", namespace)

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to recreate migrated %s %q in %q: %w", obj.GetKind(), obj.GetName(), namespace, err)
	}

	if hasStatus {
		err = unstructured.SetNestedMap(obj.Object, status, "status")
		if err != nil {
			return fmt.Errorf("failed to restore status for %s %q: %w", obj.GetKind(), obj.GetName(), err)
		}

		err = kubeClient.Status().Update(ctx, obj)
		if err != nil {
			return fmt.Errorf("failed to restore status for %s %q: %w", obj.GetKind(), obj.GetName(), err)
		}
	}

	logger.Info("migrated object to namespaced scope",
		"kind", obj.GetKind(), "name", obj.GetName(), "namespace", namespace)

	return nil
}

// newUnstructuredClient builds a client.Client straight off loopbackConfig,
// with a fresh RESTMapper of its own — deliberately not this process's usual
// scheme-typed client, since MigrateScope/RestoreMigrated both need to read
// or write objects around a CRD (re)establishment they're driving themselves,
// before/after the exact moment any other client's own RESTMapper would
// resolve the same kind.
func newUnstructuredClient(loopbackConfig *restclient.Config) (client.Client, error) {
	httpClient, err := restclient.HTTPClientFor(loopbackConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build http client: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(loopbackConfig, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to build rest mapper: %w", err)
	}

	kubeClient, err := client.New(loopbackConfig, client.Options{Scheme: runtime.NewScheme(), Mapper: restMapper})
	if err != nil {
		return nil, fmt.Errorf("failed to build client: %w", err)
	}

	return kubeClient, nil
}

// listExisting lists every object of listKind (a List GroupVersionKind, e.g.
// "ZoneList") through a fresh, unstructured client.
func listExisting(
	ctx context.Context, loopbackConfig *restclient.Config, listKind schema.GroupVersionKind,
) ([]unstructured.Unstructured, error) {
	kubeClient, err := newUnstructuredClient(loopbackConfig)
	if err != nil {
		return nil, err
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(listKind)

	err = kubeClient.List(ctx, list)
	if err != nil {
		return nil, fmt.Errorf("failed to list %s: %w", listKind.Kind, err)
	}

	return list.Items, nil
}

// waitForDeleted blocks until name's CRD is fully gone (its own finalizers,
// if any, have run) — deleting a CRD isn't synchronous, and Ensure's own
// Create right after would otherwise race an apiserver that still thinks
// the old (Cluster-scoped) definition exists.
func waitForDeleted(
	ctx context.Context, crds apiextensionsv1client.CustomResourceDefinitionInterface, name string, logger *slog.Logger,
) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, migrateDeleteTimeout)
	defer cancel()

	err := wait.ExponentialBackoffWithContext(timeoutCtx, backoff(), func(ctx context.Context) (bool, error) {
		_, getErr := crds.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return true, nil
		}

		if getErr != nil {
			return false, fmt.Errorf("failed to check %s crd deletion: %w", name, getErr)
		}

		logger.Warn("waiting for crd to finish deleting before migrating its scope", "crd", name)

		return false, nil
	})
	if err != nil {
		return fmt.Errorf("%s crd was never fully deleted while migrating its scope: %w", name, err)
	}

	return nil
}
