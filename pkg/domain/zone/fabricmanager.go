package zone

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fabricManagerServiceAccountName names the ServiceAccount
// buildFabricManagerDaemonSet's own pod template runs as — narrowly
// scoped, via ensureFabricManagerRBAC's own ClusterRole/ClusterRoleBinding,
// to read/watch Fabric. A ClusterRole/ClusterRoleBinding, not a
// namespace-scoped Role/RoleBinding like etcdIdentityServiceAccountName's
// own: Fabric objects live in whichever tenant namespace owns their own
// region, which this cluster-wide daemon has no way to already know up
// front, unlike the one, fixed downstreamNamespace every other piece of
// this package's own workload lives in.
const fabricManagerServiceAccountName = "kontinuum-fabricmanager"

// ensureFabricManagerServiceAccount upserts the ServiceAccount
// fabricManagerServiceAccountName names, in namespace (downstreamNamespace
// — see buildFabricManagerDaemonSet's own doc for why its Pods still run
// there even though the ServiceAccount's own RBAC reaches every
// namespace).
func ensureFabricManagerServiceAccount(ctx context.Context, downstream client.Client, namespace string) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: fabricManagerServiceAccountName, Namespace: namespace},
	}

	err := downstream.Create(ctx, sa)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q service account: %w", fabricManagerServiceAccountName, err)
	}

	return nil
}

// ensureFabricManagerClusterRole upserts the ClusterRole granting
// fabricManagerServiceAccountName's own ServiceAccount read/watch access
// to every Fabric, cluster-wide — see that constant's own doc for why this
// can't be scoped to one namespace — plus update on fabrics/status, for
// pkg/cli/fabricmanager's own write-back of NetworkConfigured/NATInstalled/
// Ready once it actually applies a zone's own desired state (see that
// package's own Reconciler.applyElectedZone).
func ensureFabricManagerClusterRole(ctx context.Context, downstream client.Client) error {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: fabricManagerServiceAccountName},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"kontinuum.sh"},
				Resources: []string{"fabrics"},
				Verbs:     []string{rbacVerbGet, "list", "watch"},
			},
			{
				APIGroups: []string{"kontinuum.sh"},
				Resources: []string{"fabrics/status"},
				Verbs:     []string{"update"},
			},
		},
	}

	err := downstream.Create(ctx, role)
	if apierrors.IsAlreadyExists(err) {
		var existing rbacv1.ClusterRole

		err = downstream.Get(ctx, client.ObjectKeyFromObject(role), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q cluster role: %w", fabricManagerServiceAccountName, err)
		}

		existing.Rules = role.Rules

		err = downstream.Update(ctx, &existing)
	}

	if err != nil {
		return fmt.Errorf("failed to ensure %q cluster role: %w", fabricManagerServiceAccountName, err)
	}

	return nil
}

// fabricManagerTalosConfigSecretName mirrors pkg/domain/fabric's own
// exported TalosConfigSecretName exactly (duplicated rather than imported
// — this package already avoids importing pkg/domain/fabric, which itself
// imports this package, the same cycle-avoidance nodeNameEnvVar's own doc
// explains).
//
//nolint:gosec // object name, not a credential
const fabricManagerTalosConfigSecretName = "kontinuum-fabricmanager-talosconfig"

// ensureFabricManagerSecretRole upserts the Role granting
// fabricManagerServiceAccountName's own ServiceAccount get access to
// fabricManagerTalosConfigSecretName, in namespace — the Talos admin
// credential ensureGatewayTalosConfig delivers there for
// pkg/cli/fabricmanager's own interface-config push (see that function's
// own doc). Scoped by ResourceNames to exactly that one Secret — see
// ensureIdentityRole's own doc for why get (unlike list/watch) supports
// this, and why this Pod has no need for either of those two here in the
// first place: it always knows the one name it wants, unlike
// etcdproxy.startIdentityWatch's own watch-and-filter approach.
func ensureFabricManagerSecretRole(ctx context.Context, downstream client.Client, namespace string) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: fabricManagerServiceAccountName, Namespace: namespace},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{""},
				Resources:     []string{secretsResource},
				ResourceNames: []string{fabricManagerTalosConfigSecretName},
				Verbs:         []string{rbacVerbGet},
			},
		},
	}

	err := downstream.Create(ctx, role)
	if apierrors.IsAlreadyExists(err) {
		var existing rbacv1.Role

		err = downstream.Get(ctx, client.ObjectKeyFromObject(role), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q role: %w", fabricManagerServiceAccountName, err)
		}

		existing.Rules = role.Rules

		err = downstream.Update(ctx, &existing)
	}

	if err != nil {
		return fmt.Errorf("failed to ensure %q role: %w", fabricManagerServiceAccountName, err)
	}

	return nil
}

// ensureFabricManagerSecretRoleBinding upserts the RoleBinding tying
// fabricManagerServiceAccountName's own ServiceAccount to the Role
// ensureFabricManagerSecretRole grants — its own RoleRef/Subjects never
// change once created, so unlike its sibling ensure funcs this has nothing
// to update on an already-exists conflict.
func ensureFabricManagerSecretRoleBinding(ctx context.Context, downstream client.Client, namespace string) error {
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: fabricManagerServiceAccountName, Namespace: namespace},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     fabricManagerServiceAccountName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      fabricManagerServiceAccountName,
			Namespace: namespace,
		}},
	}

	err := downstream.Create(ctx, binding)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q role binding: %w", fabricManagerServiceAccountName, err)
	}

	return nil
}

// ensureFabricManagerClusterRoleBinding upserts the ClusterRoleBinding
// tying fabricManagerServiceAccountName's own ServiceAccount (in
// namespace) to the ClusterRole ensureFabricManagerClusterRole grants —
// its own RoleRef/Subjects never change once created, so unlike its
// sibling ensure funcs this has nothing to update on an already-exists
// conflict.
func ensureFabricManagerClusterRoleBinding(ctx context.Context, downstream client.Client, namespace string) error {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: fabricManagerServiceAccountName},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     fabricManagerServiceAccountName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      fabricManagerServiceAccountName,
			Namespace: namespace,
		}},
	}

	err := downstream.Create(ctx, binding)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to ensure %q cluster role binding: %w", fabricManagerServiceAccountName, err)
	}

	return nil
}

// ensureFabricManagerRBAC upserts the ServiceAccount, ClusterRole, and
// ClusterRoleBinding buildFabricManagerDaemonSet's own pod template needs
// to watch Fabric directly — see fabricManagerServiceAccountName's own
// doc. Must run before ensureFabricManagerDaemonSet: a Pod referencing a
// ServiceAccount that doesn't exist yet fails admission, mirroring
// ensureIdentityRBAC's own identical ordering requirement.
func ensureFabricManagerRBAC(ctx context.Context, downstream client.Client, namespace string) error {
	err := ensureFabricManagerServiceAccount(ctx, downstream, namespace)
	if err != nil {
		return err
	}

	err = ensureFabricManagerClusterRole(ctx, downstream)
	if err != nil {
		return err
	}

	err = ensureFabricManagerClusterRoleBinding(ctx, downstream, namespace)
	if err != nil {
		return err
	}

	err = ensureFabricManagerSecretRole(ctx, downstream, namespace)
	if err != nil {
		return err
	}

	return ensureFabricManagerSecretRoleBinding(ctx, downstream, namespace)
}

// fabricManagerDaemonSetName names both the DaemonSet
// buildFabricManagerDaemonSet describes and its own pod template's
// container.
const fabricManagerDaemonSetName = "kontinuum-fabricmanager"

// nodeNameEnvVar mirrors pkg/cli/fabricmanager's own identical constant
// (unexported there too, so duplicated rather than imported — this
// package already avoids importing pkg/cli/* the same way
// kubeconfigSecretKey's own doc explains for pkg/domain/fabric).
const nodeNameEnvVar = "NODE_NAME"

// fabricManagerLabels is the DaemonSet's own pod-template labels and
// Selector.
func fabricManagerLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": fabricManagerDaemonSetName}
}

// buildFabricManagerDaemonSet returns the desired fabricmanager
// DaemonSet: one pod per node in this zone's own downstream cluster (no
// nodeSelector — see pkg/cli/fabricmanager's own package doc for why
// every node runs this, self-discovering which Fabric(s), if any, it's
// actually responsible for, rather than this controller managing a
// separate Deployment per elected gateway node the way it used to), each
// running `kontinuum fabricmanager run` (see pkg/cli/fabricmanager — named
// for the node agent's own growing scope, not just NAT: DHCP and other
// per-zone network duties are expected to land as further fabricmanager
// reconcile logic later) — a small, privileged (CAP_NET_ADMIN only, every
// other Linux capability dropped), host-network Pod, since programming
// the kernel's nftables ruleset and toggling ipv4 forwarding both require
// direct access to the node's own real network namespace, not this Pod's
// isolated one. NODE_NAME is the Downward API's own spec.nodeName field,
// matching exactly what a gatewayNodeRef electing this node names — see
// pkg/cli/fabricmanager.Reconciler.NodeName's own doc.
func buildFabricManagerDaemonSet(namespace, image string) *appsv1.DaemonSet {
	labels := fabricManagerLabels()
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: fabricManagerDaemonSetName, Namespace: namespace},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: fabricManagerServiceAccountName,
					HostNetwork:        true,
					DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
					Containers: []corev1.Container{{
						Name:            fabricManagerDaemonSetName,
						Image:           image,
						ImagePullPolicy: imagePullPolicy(image),
						Args:            []string{"fabricmanager", "run"},
						Env: []corev1.EnvVar{{
							Name: nodeNameEnvVar,
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
							},
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
							Capabilities: &corev1.Capabilities{
								Add:  []corev1.Capability{"NET_ADMIN"},
								Drop: []corev1.Capability{"ALL"},
							},
						},
					}},
				},
			},
		},
	}
}

// ensureFabricManagerDaemonSet upserts the DaemonSet
// buildFabricManagerDaemonSet describes.
func ensureFabricManagerDaemonSet(ctx context.Context, downstream client.Client, namespace, image string) error {
	daemonSet := buildFabricManagerDaemonSet(namespace, image)

	err := downstream.Create(ctx, daemonSet)
	if apierrors.IsAlreadyExists(err) {
		var existing appsv1.DaemonSet

		err = downstream.Get(ctx, client.ObjectKeyFromObject(daemonSet), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q daemonset: %w", fabricManagerDaemonSetName, err)
		}

		existing.Spec = daemonSet.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q daemonset: %w", fabricManagerDaemonSetName, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q daemonset: %w", fabricManagerDaemonSetName, err)
	}

	return nil
}
