package addon

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// errAddonSecretNotFound is fetchAddonSecrets' sentinel for a spec.secrets
// entry with no matching Secret on the hub.
var errAddonSecretNotFound = errors.New("addon secret not found on the hub")

// SecretSyncer copies an addon's Secrets (see AddonSpec.Secrets) onto the
// cluster kubeconfig points at, into namespace.
type SecretSyncer interface {
	SyncSecrets(ctx context.Context, kubeconfig []byte, namespace string, secrets []corev1.Secret) error
}

// ClusterSecretSyncer is the SecretSyncer the addon controller uses by
// default, writing to the cluster through client-go.
type ClusterSecretSyncer struct{}

// SyncSecrets creates namespace if it doesn't exist yet — Secrets are copied
// before the first Helm install, which would otherwise be what creates it —
// then upserts each Secret there, replacing its Data entirely.
func (ClusterSecretSyncer) SyncSecrets(
	ctx context.Context, kubeconfig []byte, namespace string, secrets []corev1.Secret,
) error {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to parse cluster kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to build cluster client: %w", err)
	}

	_, err = clientset.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create namespace %q: %w", namespace, err)
	}

	for index := range secrets {
		err = upsertSecret(ctx, clientset, namespace, &secrets[index])
		if err != nil {
			return err
		}
	}

	return nil
}

func upsertSecret(ctx context.Context, clientset kubernetes.Interface, namespace string, source *corev1.Secret) error {
	secrets := clientset.CoreV1().Secrets(namespace)

	existing, err := secrets.Get(ctx, source.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = secrets.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: namespace},
			Type:       source.Type,
			Data:       source.Data,
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create secret %q: %w", source.Name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to get secret %q: %w", source.Name, err)
	}

	existing.Data = source.Data

	_, err = secrets.Update(ctx, existing, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update secret %q: %w", source.Name, err)
	}

	return nil
}

// fetchAddonSecrets reads every Secret addon lists under spec.secrets from
// addon's own namespace on the hub.
func fetchAddonSecrets(ctx context.Context, kubeClient client.Client, addon *v1alpha2.Addon) ([]corev1.Secret, error) {
	secrets := make([]corev1.Secret, 0, len(addon.Spec.Secrets))

	for _, ref := range addon.Spec.Secrets {
		var secret corev1.Secret

		err := kubeClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: addon.Namespace}, &secret)
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", errAddonSecretNotFound, addon.Namespace, ref.Name)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to get addon secret %q: %w", ref.Name, err)
		}

		secrets = append(secrets, secret)
	}

	return secrets, nil
}
