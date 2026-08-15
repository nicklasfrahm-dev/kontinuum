package taloscluster

import (
	"context"
	"encoding/json"
	"fmt"

	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	talosconfig "github.com/siderolabs/talos/pkg/machinery/config"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// secretsBundleKey and kubeconfigKey are the two keys stored under the
	// Secret cluster.Status.SecretRef points to — see that field's own
	// doc.
	secretsBundleKey = "secrets-bundle"
	kubeconfigKey    = "kubeconfig"

	// secretNamePrefix makes a TalosCluster's secret recognizable by name
	// alone — mirrors registry's own secretNamePrefix convention.
	secretNamePrefix = "taloscluster-"
)

// secretRefFor derives the Secret reference a TalosCluster named
// clusterName's secrets/kubeconfig are stored under.
func secretRefFor(clusterName string) v1alpha2.SecretReference {
	return v1alpha2.SecretReference{
		Name:      secretNamePrefix + clusterName,
		Namespace: v1alpha2.KontinuumSystemNamespace,
	}
}

// ensureSecretsBundle returns cluster's persisted Talos secrets bundle —
// the CA/keys/tokens every generated machine config and admin Talosconfig
// is (re-)derived from every reconcile, see this package's own doc for why
// only the bundle itself is persisted. On first reconcile
// (cluster.Status.SecretRef unset) it generates a fresh bundle and stores
// it; once set, the ref must resolve — a missing Secret after the ref was
// already recorded is reported as an error rather than silently
// regenerating a new CA, which would orphan any node that already trusts
// the old one.
func ensureSecretsBundle(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster,
) (*talossecrets.Bundle, error) {
	if cluster.Status.SecretRef.Name != "" {
		return loadSecretsBundle(ctx, kubeClient, cluster.Status.SecretRef)
	}

	talosVersion, _ := resolveVersions(cluster)

	contract, err := talosconfig.ParseContractFromVersion(talosVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to parse talos version contract %q: %w", talosVersion, err)
	}

	bundle, err := talossecrets.NewBundle(talossecrets.NewClock(), contract)
	if err != nil {
		return nil, fmt.Errorf("failed to generate secrets bundle for %q: %w", cluster.Name, err)
	}

	data, err := json.Marshal(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal secrets bundle for %q: %w", cluster.Name, err)
	}

	ref := secretRefFor(cluster.Name)

	err = ensureNamespace(ctx, kubeClient, ref.Namespace)
	if err != nil {
		return nil, err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace},
		Data:       map[string][]byte{secretsBundleKey: data},
	}

	err = controllerutil.SetControllerReference(cluster, secret, kubeClient.Scheme())
	if err != nil {
		return nil, fmt.Errorf("failed to set owner reference on %q secret: %w", ref.Name, err)
	}

	err = kubeClient.Create(ctx, secret)
	if err != nil {
		return nil, fmt.Errorf("failed to create %q secret: %w", ref.Name, err)
	}

	cluster.Status.SecretRef = ref

	return bundle, nil
}

// loadSecretsBundle fetches and unmarshals the secrets bundle stored at
// ref.
func loadSecretsBundle(
	ctx context.Context, kubeClient client.Client, ref v1alpha2.SecretReference,
) (*talossecrets.Bundle, error) {
	var secret corev1.Secret

	err := kubeClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secrets bundle secret: %w", ref.Name, err)
	}

	bundle := &talossecrets.Bundle{Clock: talossecrets.NewClock()}

	err = json.Unmarshal(secret.Data[secretsBundleKey], bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %q secrets bundle: %w", ref.Name, err)
	}

	return bundle, nil
}

// storeKubeconfig upserts kubeconfig onto cluster.Status.SecretRef's
// Secret.
func storeKubeconfig(
	ctx context.Context, kubeClient client.Client, cluster *v1alpha2.TalosCluster, kubeconfig []byte,
) error {
	ref := cluster.Status.SecretRef

	var secret corev1.Secret

	err := kubeClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return fmt.Errorf("failed to fetch %q secret to store kubeconfig: %w", ref.Name, err)
	}

	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}

	secret.Data[kubeconfigKey] = kubeconfig

	err = kubeClient.Update(ctx, &secret)
	if err != nil {
		return fmt.Errorf("failed to store kubeconfig on %q secret: %w", ref.Name, err)
	}

	return nil
}

// ensureNamespace creates namespace if it doesn't already exist — mirrors
// registry.Heartbeat.ensureSecret's identical namespace-first pattern.
func ensureNamespace(ctx context.Context, kubeClient client.Client, namespace string) error {
	err := kubeClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q namespace: %w", namespace, err)
	}

	return nil
}
