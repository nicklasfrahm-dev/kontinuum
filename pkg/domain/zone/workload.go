package zone

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/mod/semver"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

// downstreamNamespace is the namespace this package installs everything
// into on a zone's own downstream cluster — matches
// v1alpha2.KontinuumSystemNamespace's own value ("kontinuum-system"), which
// this package reuses directly rather than redeclaring the same string.
const downstreamNamespace = "kontinuum-system"

// envSecretName and envConfigMapName are the kontinuum-env Secret/ConfigMap
// ensureDeployment wires into the kontinuum container via envFrom.
//
//nolint:gosec // false positive: object names, not credentials
const (
	envSecretName    = "kontinuum-env"
	envConfigMapName = "kontinuum-env"
	deploymentName   = "kontinuum"
	serviceName      = "kontinuum"
	portName         = "http"
	containerPort    = 8080
	servicePort      = 80
)

// ensureNamespace creates namespace on the downstream cluster if it
// doesn't already exist — mirrors
// pkg/domain/taloscluster/secrets.go's identical pattern.
func ensureNamespace(ctx context.Context, downstream client.Client, namespace string) error {
	err := downstream.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q namespace: %w", namespace, err)
	}

	return nil
}

// zoneEnvOverrides computes the env vars a joined zone's own env can never
// just copy from the hub's own HubConfig — either because the value is
// inherently specific to this zone (region/zone identify this zone, never
// the hub's own — always empty there; storage is this zone's own DSN, see
// zoneStorageDSN; addr must stay the container's own listen address
// regardless of whatever the hub itself happens to be configured with),
// or because a straight copy would be wrong for this zone specifically:
// KONTINUUM_OIDC_REDIRECT_URL, registered with the issuer for the hub's
// own host, would never match a browser completing a login against this
// zone's own domain. hostname is the zone's own <zone>.<region>.<domain>
// — left empty (network layer skipped — see installNetwork's own doc)
// when the zone has no domain configured, in which case this simply
// doesn't override KONTINUUM_OIDC_REDIRECT_URL at all, falling back to
// the hub's own value rather than producing a malformed "https:///app"
// the way computing one from an empty hostname unconditionally used to.
//
// Everything else copies straight off hubConfig.EnvVars() — see
// ensureEnv — including KONTINUUM_ACME_EMAIL/_SERVER (so this zone's own
// kontinuum-server can, in turn, create a cert-manager ClusterIssuer for
// any further zone it joins) and KONTINUUM_SERVER_GRPC_ENDPOINT/
// _INSECURE_TLS_SKIP_VERIFY (so that same further-nested Zone controller
// can dial this same hub's etcd proxy the exact way this one does — see
// zoneStorageDSN).
func zoneEnvOverrides(region, zoneName, storage, hostname string) map[string]string {
	overrides := map[string]string{
		"KONTINUUM_SERVER_ADDR":   ":8080",
		"KONTINUUM_SERVER_REGION": region,
		"KONTINUUM_SERVER_ZONE":   zoneName,
		storageSecretKey:          storage,
	}

	if hostname != "" {
		overrides["KONTINUUM_OIDC_REDIRECT_URL"] = "https://" + hostname + "/app"
	}

	return overrides
}

// ensureEnv upserts the kontinuum-env Secret and ConfigMap with every env
// var hubConfig.EnvVars produces, replaced by overrides's own value where
// present (see zoneEnvOverrides) — routed into the Secret or ConfigMap by
// whichever api/v1alpha2.KontinuumConfigStatus field produced it is tagged
// `secret:"true"` (currently just Server.Storage). Without
// KONTINUUM_INSECURE_ALLOW_ANONYMOUS/OIDC_ISSUER_URL — both copied
// straight from the hub, like everything else not in overrides — the
// deployed process refuses to even start (see
// pkg/config.Config.ValidateAuthentication), so it never gets as far as
// registering itself at all.
//
// This replaces what used to be a hand-maintained, field-by-field env var
// list here: that list silently fell behind pkg/config.Config more than
// once as fields were added there — KONTINUUM_LOG_LEVEL/_FORMAT and
// KONTINUUM_SERVER_DNS_DOMAIN were never forwarded to a joined zone at
// all. Every field pkg/config.Config gains from now on reaches a joined
// zone automatically, with no change needed here — unless it needs its
// own zoneEnvOverrides entry, or (via the `secret` struct tag) needs
// routing into the Secret instead.
func ensureEnv(
	ctx context.Context, downstream client.Client, namespace string, hubConfig *config.Config, overrides map[string]string,
) error {
	secretData := map[string]string{}
	configData := map[string]string{}

	for _, envVar := range hubConfig.EnvVars() {
		value := envVar.Value
		if override, ok := overrides[envVar.Name]; ok {
			value = override
		}

		if envVar.Secret {
			secretData[envVar.Name] = value
		} else {
			configData[envVar.Name] = value
		}
	}

	err := ensureSecret(ctx, downstream, namespace, secretData)
	if err != nil {
		return err
	}

	return ensureConfigMap(ctx, downstream, namespace, configData)
}

// ensureSecret upserts the kontinuum-env Secret with data. Mirrors
// pkg/domain/registry/heartbeat.go's own
// create-then-get-and-update-on-conflict upsert idiom.
func ensureSecret(ctx context.Context, downstream client.Client, namespace string, data map[string]string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: envSecretName, Namespace: namespace},
		StringData: data,
	}

	err := downstream.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Secret

		err = downstream.Get(ctx, client.ObjectKeyFromObject(secret), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q secret: %w", envSecretName, err)
		}

		existing.StringData = secret.StringData

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q secret: %w", envSecretName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q secret: %w", envSecretName, err)
	}

	return nil
}

// ensureConfigMap upserts the kontinuum-env ConfigMap with data.
func ensureConfigMap(ctx context.Context, downstream client.Client, namespace string, data map[string]string) error {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: envConfigMapName, Namespace: namespace},
		Data:       data,
	}

	err := downstream.Create(ctx, configMap)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.ConfigMap

		err = downstream.Get(ctx, client.ObjectKeyFromObject(configMap), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q configmap: %w", envConfigMapName, err)
		}

		existing.Data = data

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q configmap: %w", envConfigMapName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q configmap: %w", envConfigMapName, err)
	}

	return nil
}

// deploymentLabels is both the kontinuum Deployment's pod-template labels
// and its Service's selector.
func deploymentLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": deploymentName}
}

// nonRootUID is the distroless "nonroot" user's own UID/GID — baked into
// every distroless/static-debian13 image regardless of tag (see
// Containerfile), so setting it explicitly here works even though that
// image's default tag still runs as root. Used for both RunAsUser and
// FSGroup: FSGroup makes the tmpVolume emptyDir (see tmpVolumeMount)
// group-writable by this same UID, since a Pod's fsGroup only takes
// effect for volumes, never the container's own already-baked-in
// filesystem.
const nonRootUID = 65532

// tmpVolumeName and tmpVolumeMountPath back the one writable path this
// container gets under its own readOnlyRootFilesystem — see
// podSecurityContext's own doc for why a fully read-only root still needs
// this.
const (
	tmpVolumeName      = "tmp"
	tmpVolumeMountPath = "/tmp"
)

// etcdIdentityVolumeName names the read-only volume buildDeployment mounts
// etcdproxy.IdentitySecretName's own kubernetes.io/tls Secret through, at
// etcdproxy.IdentityMountPath — see that constant's own doc for why the
// mount path itself lives in pkg/domain/etcdproxy rather than here.
const etcdIdentityVolumeName = "etcd-identity"

// podSecurityContext and containerSecurityContext satisfy the
// "restricted" Pod Security Standard: no privilege escalation, every
// Linux capability dropped, a non-root user, the runtime's default
// seccomp profile, and a read-only root filesystem. The root filesystem
// being read-only means this container has nowhere left to write — except
// tmpVolumeMountPath's own emptyDir (see ensureDeployment), needed because
// this same binary's own addon installer (pkg/domain/addon/installer.go's
// writeTempKubeconfig) calls os.CreateTemp("", ...) when this downstream
// instance goes on to run its own Zone controller for a further zone.
func podSecurityContext() *corev1.PodSecurityContext {
	runAsNonRoot := true
	runAsUser := int64(nonRootUID)
	runAsGroup := int64(nonRootUID)
	fsGroup := int64(nonRootUID)

	return &corev1.PodSecurityContext{
		RunAsNonRoot: &runAsNonRoot,
		RunAsUser:    &runAsUser,
		RunAsGroup:   &runAsGroup,
		FSGroup:      &fsGroup,
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

func containerSecurityContext() *corev1.SecurityContext {
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true

	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivilegeEscalation,
		ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// imagePullPolicy returns corev1.PullAlways for image when its tag isn't
// valid semver — "dev" or "latest", the two mutable, floating tags CI and
// `make image-push` (see the Makefile) both keep overwriting in place —
// and corev1.PullIfNotPresent otherwise, since a real semver-tagged
// release is immutable once published and safe to cache. Kubernetes'
// own default pull policy only special-cases the literal tag "latest"
// this way; that's no longer enough now that resolveImage deploys
// ImageRepo:dev just as often as a real version (see that function's own
// doc) — without this, a node that already cached an older :dev layer
// would never re-pull a newer one pushed under the same tag.
func imagePullPolicy(image string) corev1.PullPolicy {
	tag := image[strings.LastIndex(image, ":")+1:]

	if semver.IsValid(tag) {
		return corev1.PullIfNotPresent
	}

	return corev1.PullAlways
}

// buildDeployment returns the desired kontinuum Deployment — a single
// replica running image, with no command/args override (Containerfile's own
// ENTRYPOINT already runs `serve`), sourcing all of its configuration from
// the kontinuum-env Secret/ConfigMap ensureSecret/ensureConfigMap maintain.
// Hardened to the "restricted" Pod Security Standard — see
// podSecurityContext/containerSecurityContext's own doc.
func buildDeployment(namespace, image string) *appsv1.Deployment {
	labels := deploymentLabels()
	replicas := int32(1)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					SecurityContext: podSecurityContext(),
					Containers: []corev1.Container{{
						Name:            deploymentName,
						Image:           image,
						ImagePullPolicy: imagePullPolicy(image),
						Ports:           []corev1.ContainerPort{{Name: portName, ContainerPort: containerPort}},
						SecurityContext: containerSecurityContext(),
						EnvFrom: []corev1.EnvFromSource{
							{SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: envSecretName},
							}},
							{ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: envConfigMapName},
							}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: tmpVolumeName, MountPath: tmpVolumeMountPath},
							{Name: etcdIdentityVolumeName, MountPath: etcdproxy.IdentityMountPath, ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: tmpVolumeName, VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						}},
						{Name: etcdIdentityVolumeName, VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: etcdproxy.IdentitySecretName},
						}},
					},
				},
			},
		},
	}
}

// ensureDeployment upserts the Deployment buildDeployment describes.
func ensureDeployment(ctx context.Context, downstream client.Client, namespace, image string) error {
	deployment := buildDeployment(namespace, image)

	err := downstream.Create(ctx, deployment)
	if apierrors.IsAlreadyExists(err) {
		var existing appsv1.Deployment

		err = downstream.Get(ctx, client.ObjectKeyFromObject(deployment), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q deployment: %w", deploymentName, err)
		}

		existing.Spec = deployment.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q deployment: %w", deploymentName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q deployment: %w", deploymentName, err)
	}

	return nil
}

// etcdIdentityRestartAnnotation is bumped on the kontinuum Deployment's own
// pod template whenever ensureEtcdIdentity rotates a zone's identity — the
// zone's own kontinuum-server caches its private key in memory at startup
// (see pkg/cli/serve.go's resolveStorageDSN), so a rotated key only takes
// effect once the pod restarts; bumping this annotation is what forces
// that rolling restart. Must be applied *after* ensureDeployment in any
// given reconcile pass — ensureDeployment's own update path replaces
// Spec.Template wholesale (see its own Update call above), which would
// otherwise silently discard this annotation.
const etcdIdentityRestartAnnotation = "kontinuum.sh/etcd-identity-restarted-at"

// bumpEtcdIdentityRestartAnnotation forces a rolling restart of the
// kontinuum Deployment by stamping its own pod template with restartedAt
// — see etcdIdentityRestartAnnotation's own doc.
func bumpEtcdIdentityRestartAnnotation(ctx context.Context, downstream client.Client, restartedAt time.Time) error {
	var deployment appsv1.Deployment

	key := client.ObjectKey{Name: deploymentName, Namespace: downstreamNamespace}

	err := downstream.Get(ctx, key, &deployment)
	if err != nil {
		return fmt.Errorf("failed to get %q deployment: %w", deploymentName, err)
	}

	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}

	deployment.Spec.Template.Annotations[etcdIdentityRestartAnnotation] = restartedAt.Format(time.RFC3339Nano)

	err = downstream.Update(ctx, &deployment)
	if err != nil {
		return fmt.Errorf("failed to bump restart annotation on %q deployment: %w", deploymentName, err)
	}

	return nil
}

// deleteDeployment deletes the Deployment ensureDeployment upserts,
// tolerating NotFound — see teardown.go's own doc for why every deleteX
// helper is idempotent the same way its ensureX counterpart already is.
func deleteDeployment(ctx context.Context, downstream client.Client, namespace string) error {
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: namespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, deployment))
	if err != nil {
		return fmt.Errorf("failed to delete %q deployment: %w", deploymentName, err)
	}

	return nil
}

// deleteService deletes the Service ensureService upserts, tolerating
// NotFound.
func deleteService(ctx context.Context, downstream client.Client, namespace string) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, service))
	if err != nil {
		return fmt.Errorf("failed to delete %q service: %w", serviceName, err)
	}

	return nil
}

// deleteConfigMap deletes the ConfigMap ensureConfigMap upserts, tolerating
// NotFound.
func deleteConfigMap(ctx context.Context, downstream client.Client, namespace string) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: envConfigMapName, Namespace: namespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, configMap))
	if err != nil {
		return fmt.Errorf("failed to delete %q configmap: %w", envConfigMapName, err)
	}

	return nil
}

// deleteSecret deletes the Secret ensureSecret upserts, tolerating NotFound.
func deleteSecret(ctx context.Context, downstream client.Client, namespace string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envSecretName, Namespace: namespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, secret))
	if err != nil {
		return fmt.Errorf("failed to delete %q secret: %w", envSecretName, err)
	}

	return nil
}

// deleteNamespace deletes namespace itself, tolerating NotFound — the last
// step of teardown.go's own uninstallWorkload, cascading away anything this
// package (or cert-manager, e.g. the Certificate's own TLS Secret) ever
// created inside it that isn't explicitly deleted above.
func deleteNamespace(ctx context.Context, downstream client.Client, namespace string) error {
	err := client.IgnoreNotFound(downstream.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	if err != nil {
		return fmt.Errorf("failed to delete %q namespace: %w", namespace, err)
	}

	return nil
}

// ensureService upserts the ClusterIP Service ensureHTTPRoute's HTTPRoute
// backendRef targets.
func ensureService(ctx context.Context, downstream client.Client, namespace string) error {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: deploymentLabels(),
			Ports: []corev1.ServicePort{{
				Name:       portName,
				Port:       servicePort,
				TargetPort: intstr.FromString(portName),
			}},
		},
	}

	err := downstream.Create(ctx, service)
	if apierrors.IsAlreadyExists(err) {
		var existing corev1.Service

		err = downstream.Get(ctx, client.ObjectKeyFromObject(service), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q service: %w", serviceName, err)
		}

		// ClusterIP is immutable and Kubernetes-assigned — preserve it
		// rather than round-tripping the zero value back through Update.
		service.Spec.ClusterIP = existing.Spec.ClusterIP
		existing.Spec = service.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q service: %w", serviceName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q service: %w", serviceName, err)
	}

	return nil
}
