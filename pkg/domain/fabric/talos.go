package fabric

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	talossecrets "github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/role"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// secretsBundleKey is the key a TalosCluster's own secrets bundle is stored
// under in the Secret its status.secretRef points to — must match
// pkg/domain/taloscluster/secrets.go's own secretsBundleKey. Duplicated
// rather than imported, mirroring pkg/domain/zone's identical
// kubeconfigSecretKey duplication (see its own doc for the import-cycle-
// avoidance rationale — taloscluster already imports this package's future
// siblings, so importing taloscluster from here risks the same cycle).
const secretsBundleKey = "secrets-bundle"

// LoadSecretsBundle fetches and unmarshals cluster's own stored Talos
// secrets bundle — mirrors pkg/domain/taloscluster's identical unexported
// loadSecretsBundle (see secretsBundleKey's own doc for why this is
// duplicated rather than imported).
func LoadSecretsBundle(
	ctx context.Context, hubClient client.Client, ref v1alpha2.SecretReference,
) (*talossecrets.Bundle, error) {
	var secret corev1.Secret

	err := hubClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%q secret not found: %w", ref.Name, err)
		}

		return nil, fmt.Errorf("failed to fetch %q secrets bundle secret: %w", ref.Name, err)
	}

	bundle := &talossecrets.Bundle{Clock: talossecrets.NewClock()}

	err = json.Unmarshal(secret.Data[secretsBundleKey], bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %q secrets bundle: %w", ref.Name, err)
	}

	return bundle, nil
}

// BuildTalosConfig derives a real (non-maintenance-mode) admin
// clientconfig.Config from bundle — the same OS CA and a freshly generated
// os:admin client certificate, both signed by the cluster's own secrets
// bundle. clusterName becomes the config's own context name; endpoints is
// carried for completeness but every real dial against it overrides it
// explicitly (see pkg/cli/fabricmanager's own dial code), matching
// taloscluster's own identical convention.
func BuildTalosConfig(
	bundle *talossecrets.Bundle, clusterName string, endpoints []string,
) (*clientconfig.Config, error) {
	clientCert, err := bundle.GenerateTalosAPIClientCertificate(role.MakeSet(role.Admin))
	if err != nil {
		return nil, fmt.Errorf("failed to generate talos admin client certificate: %w", err)
	}

	return clientconfig.NewConfig(clusterName, endpoints, bundle.Certs.OS.Crt, clientCert), nil
}

// TalosConfigSecretName is the Secret name pkg/cli/fabricmanager reads its
// own Talos admin client credential from — see ensureGatewayTalosConfig's
// own doc for what's actually inside it, and why. Exported (not
// duplicated the way secretsBundleKey is) since pkg/cli/fabricmanager
// already imports this package for v1alpha2 types, and this is the one
// name both sides need to agree on exactly, the same shared-constant
// shape pkg/domain/etcdproxy.IdentitySecretName already uses between
// pkg/domain/zone (writer) and pkg/domain/etcdproxy's own watcher
// (reader).
//
//nolint:gosec // object name, not a credential
const TalosConfigSecretName = "kontinuum-fabricmanager-talosconfig"

// TalosConfigSecretKey is the one data key ensureGatewayTalosConfig writes
// TalosConfigSecretName's own payload under.
const TalosConfigSecretKey = "talosconfig"

// ensureGatewayTalosConfig delivers a Talos admin client credential for
// gatewayNode onto downstream (the same downstream cluster gatewayNode
// itself is a member of) as a Secret, so pkg/cli/fabricmanager — running
// as a Pod directly on that node — can push its own interface config
// without the hub dialing in from outside (see that package's own
// Reconciler). Only a fresh os:admin client certificate (via
// BuildTalosConfig) and the cluster's own OS CA are delivered, marshaled
// in talosctl's own config file format (clientconfig.Config.Bytes, loaded
// back via clientconfig.FromBytes) — not the cluster's own raw secrets
// bundle: everything else in that bundle (etcd certs, the Kubernetes CA,
// bootstrap tokens) is a far bigger blast radius than any one gateway
// node's own interface-config push ever needs, and none of it is
// required to drive the Talos API. The certificate carries a year-long
// validity (Talos's own TalosAPIDefaultCertificateValidityDuration
// default) — this Secret is re-issued and upserted on every reconcile
// pass regardless, so a nearing-expiry credential is always refreshed
// well ahead of that, with no separate rotation schedule to maintain.
func ensureGatewayTalosConfig(
	ctx context.Context, hubClient, downstream client.Client,
	cluster *v1alpha2.TalosCluster, gatewayNode *v1alpha2.Instance,
) error {
	bundle, err := LoadSecretsBundle(ctx, hubClient, cluster.Status.SecretRef)
	if err != nil {
		return err
	}

	addr := dialAddress(*gatewayNode)

	talosCfg, err := BuildTalosConfig(bundle, cluster.Name, []string{addr})
	if err != nil {
		return err
	}

	data, err := talosCfg.Bytes()
	if err != nil {
		return fmt.Errorf("failed to encode talosconfig for %q: %w", gatewayNode.Name, err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: TalosConfigSecretName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Data:       map[string][]byte{TalosConfigSecretKey: data},
	}

	err = downstream.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Secret

		err = downstream.Get(ctx, client.ObjectKeyFromObject(secret), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q secret: %w", TalosConfigSecretName, err)
		}

		existing.Data = secret.Data

		err = downstream.Update(ctx, &existing)
	}

	if err != nil {
		return fmt.Errorf("failed to ensure %q secret: %w", TalosConfigSecretName, err)
	}

	return nil
}
