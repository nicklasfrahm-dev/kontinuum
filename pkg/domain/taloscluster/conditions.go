package taloscluster

import (
	"context"
	"log/slog"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// MemberConfiguredConditionType is set true on a claimed, Discovered
	// Instance once this package's member reconciler has successfully
	// applied its Talos machine config (see applyControlPlaneConfig and
	// reconcileWorkerPool) — the first step of the bootstrap pipeline that
	// isn't otherwise visible on the Instance object itself (see issue
	// #62). Like BootstrappedConditionType, it's only ever set true, never
	// reset: ApplyConfiguration succeeding a second time against the same
	// member isn't expected — see ClusterBootstrapper.ApplyConfiguration's
	// own doc — so a later failure there is never proof the earlier
	// success didn't happen.
	MemberConfiguredConditionType = "Configured"
	// MemberJoinedConditionType is set true on a member once its real,
	// post-config Talos identity first answers a Version RPC (see
	// recordTalosVersions) — proof it survived its install/reboot and
	// rejoined the cluster (etcd for a control-plane member, kubelet
	// registration for a worker).
	MemberJoinedConditionType = "Joined"
	// MemberReadyConditionType mirrors, on a control-plane member, the
	// cluster-wide HealthCheck this package already runs against exactly
	// that batch of nodes (see bootstrapAndCheckHealth) — a pass proves
	// each of them individually healthy, not just the aggregate. Workers
	// don't get this condition yet: this package has no per-worker health
	// probe to honestly back it with (see reconcileWorkerPool's own doc).
	MemberReadyConditionType = "Ready"

	reasonMemberConfigured = "ConfigApplied"
	reasonMemberJoined     = "Joined"
	reasonMemberHealthy    = "Healthy"
)

// markMemberCondition sets a true conditionType on member and persists it,
// skipping the write entirely when it wouldn't change anything — mirrors
// recordTalosVersions' own already-known skip, so a member already past a
// pipeline stage doesn't cost a Status().Update on every future reconcile.
func markMemberCondition(
	ctx context.Context, kubeClient client.Client, logger *slog.Logger, member *v1alpha2.Instance,
	conditionType, reason, message string,
) {
	changed := meta.SetStatusCondition(&member.Status.Conditions, metav1.Condition{
		Type: conditionType, Status: metav1.ConditionTrue, Reason: reason, Message: message,
	})
	if !changed {
		return
	}

	err := kubeClient.Status().Update(ctx, member)
	if err != nil {
		logger.Warn("failed to persist member condition",
			"instance", member.Name, "condition", conditionType, "error", err)
	}
}
