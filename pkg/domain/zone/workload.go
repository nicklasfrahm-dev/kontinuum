package zone

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// downstreamNamespace is the namespace this package installs everything
// into on a zone's own downstream cluster — matches
// v1alpha2.DefaultSecretNamespace's own value ("kontinuum-system"), which
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

// ensureSecret upserts the kontinuum-env Secret with this zone's copy of
// the hub's own storage connection string — see findKontinuumStorage.
// Mirrors pkg/domain/registry/heartbeat.go's own
// create-then-get-and-update-on-conflict upsert idiom.
func ensureSecret(ctx context.Context, downstream client.Client, namespace, storage string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: envSecretName, Namespace: namespace},
		StringData: map[string]string{storageSecretKey: storage},
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

// ensureConfigMap upserts the kontinuum-env ConfigMap with this zone's
// non-confidential config — KONTINUUM_SERVER_REGION/_ZONE (so this zone's
// own kontinuum serve process registers itself as a Worker — see
// pkg/domain/registry.Role) and KONTINUUM_ACME_EMAIL/_SERVER (so it can, in
// turn, run this same Zone controller for any further zones it joins).
func ensureConfigMap(
	ctx context.Context, downstream client.Client, namespace, region, zoneName, acmeEmail, acmeServer string,
) error {
	data := map[string]string{
		"KONTINUUM_SERVER_ADDR":   ":8080",
		"KONTINUUM_SERVER_REGION": region,
		"KONTINUUM_SERVER_ZONE":   zoneName,
		"KONTINUUM_ACME_EMAIL":    acmeEmail,
		"KONTINUUM_ACME_SERVER":   acmeServer,
	}

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

// ensureDeployment upserts the kontinuum Deployment — a single replica
// running image, with no command/args override (Containerfile's own
// ENTRYPOINT already runs `serve`), sourcing all of its configuration from
// the kontinuum-env Secret/ConfigMap ensureSecret/ensureConfigMap maintain.
// Hardened to the "restricted" Pod Security Standard — see
// podSecurityContext/containerSecurityContext's own doc.
func ensureDeployment(ctx context.Context, downstream client.Client, namespace, image string) error {
	labels := deploymentLabels()
	replicas := int32(1)

	deployment := &appsv1.Deployment{
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
						},
					}},
					Volumes: []corev1.Volume{
						{Name: tmpVolumeName, VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						}},
					},
				},
			},
		},
	}

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
