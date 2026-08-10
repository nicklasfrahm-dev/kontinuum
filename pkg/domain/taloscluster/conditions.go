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
	// that batch of nodes — a pass proves each of them individually
	// healthy, not just the aggregate. Unlike MemberConfiguredConditionType
	// and MemberJoinedConditionType, this one isn't append-only: it's kept
	// live for as long as the cluster exists, first by
	// bootstrapAndCheckHealth while the control plane is still converging,
	// then by recheckControlPlaneHealth's own periodic recheck once it has
	// — so it can also flip back to false if a previously healthy node
	// later fails a recheck. Workers don't get this condition yet: this
	// package has no per-worker health probe to honestly back it with (see
	// reconcileWorkerPool's own doc).
	MemberReadyConditionType = "Ready"
	// MemberLiveConditionType is this package's own contribution to a
	// member's Live condition (see api/v1alpha2.InstanceStatus's own doc)
	// — the narrower gap issue #76 actually identified: control-plane
	// members already get a continuous per-node health signal via
	// MemberReadyConditionType above, but workers get none at all once
	// joined. Set true the moment recordTalosVersions first dials a member
	// with its real post-config identity (same proof-of-life Joined
	// itself rests on), then kept fresh by recheckMemberLiveness's own
	// periodic recheck for every member — control-plane and worker alike —
	// once the cluster has converged, mirroring MemberReadyConditionType's
	// own live-for-the-cluster's-lifetime shape but scoped to reachability
	// rather than cluster health.
	MemberLiveConditionType = "Live"

	reasonMemberConfigured  = "ConfigApplied"
	reasonMemberJoined      = "Joined"
	reasonMemberHealthy     = "Healthy"
	reasonMemberUnhealthy   = "Unhealthy"
	reasonMemberLive        = "Live"
	reasonMemberUnreachable = "Unreachable"

	// messageMemberAnsweredVersion is MemberJoinedConditionType's and
	// MemberLiveConditionType's shared success message — a successful
	// Version RPC is simultaneously proof the member rejoined (Joined) and
	// proof it's currently reachable (Live), so every place that records
	// one on that basis records the other with the same message.
	messageMemberAnsweredVersion = "node answered a Version RPC with its real, post-config Talos identity"
)

// setMemberCondition sets conditionType on member to status and persists
// it, skipping the write entirely when it wouldn't change anything — a
// member already past a pipeline stage, or whose live health hasn't
// changed since the last recheck, doesn't cost a Status().Update on every
// future reconcile.
func setMemberCondition(
	ctx context.Context, kubeClient client.Client, logger *slog.Logger, member *v1alpha2.Instance,
	conditionType string, status metav1.ConditionStatus, reason, message string,
) {
	changed := meta.SetStatusCondition(&member.Status.Conditions, metav1.Condition{
		Type: conditionType, Status: status, Reason: reason, Message: message,
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

// markMemberCondition sets a true conditionType on member — see
// setMemberCondition's own doc. Every one of MemberConfiguredConditionType
// and MemberJoinedConditionType's own callers only ever mark true (both
// are append-only, see their own docs), so this thin wrapper is what they
// use instead of spelling out metav1.ConditionTrue at every call site.
func markMemberCondition(
	ctx context.Context, kubeClient client.Client, logger *slog.Logger, member *v1alpha2.Instance,
	conditionType, reason, message string,
) {
	setMemberCondition(ctx, kubeClient, logger, member, conditionType, metav1.ConditionTrue, reason, message)
}

// clearMemberConditions removes every condition type this package's own
// member reconciler ever sets (MemberConfiguredConditionType,
// MemberJoinedConditionType, MemberReadyConditionType) from member's own
// status.conditions and persists that, if any were actually present —
// called by InstanceResetReconciler once an Instance goes back to
// unclaimed. Unlike setMemberCondition/markMemberCondition above, which are
// deliberately append-only while a member is still part of a cluster (see
// MemberConfiguredConditionType's own doc), none of these still describe
// anything real once the Instance is no longer claimed by any pool: the
// node's prior config, if any, is specific to that claim episode, not a
// property of the Instance itself, so leaving it behind would misreport an
// unclaimed, freshly-rediscovered node as still configured for a cluster
// it's no longer part of.
func clearMemberConditions(
	ctx context.Context, kubeClient client.Client, logger *slog.Logger, member *v1alpha2.Instance,
) {
	conditionTypes := []string{MemberConfiguredConditionType, MemberJoinedConditionType, MemberReadyConditionType}

	changed := false

	for _, conditionType := range conditionTypes {
		if meta.RemoveStatusCondition(&member.Status.Conditions, conditionType) {
			changed = true
		}
	}

	if !changed {
		return
	}

	err := kubeClient.Status().Update(ctx, member)
	if err != nil {
		logger.Warn("failed to clear stale member conditions", "instance", member.Name, "error", err)
	}
}

// persistMemberProbe sets every one of conds on member, bumps
// Status.LastProbeTime to probeTime, and unconditionally persists both —
// unlike setMemberCondition's own skip-if-unchanged optimization (correct
// for the one-shot Configured/Joined conditions it otherwise serves,
// where an unchanged status really does mean nothing worth writing), a
// probe's whole point is proving *when* it last ran (see
// api/v1alpha2.InstanceStatus.LastProbeTime's own doc), which is a real
// change on every single call here even when a condition's own status
// doesn't flip.
func persistMemberProbe(
	ctx context.Context, kubeClient client.Client, logger *slog.Logger, member *v1alpha2.Instance,
	probeTime metav1.Time, conds ...metav1.Condition,
) {
	for _, cond := range conds {
		meta.SetStatusCondition(&member.Status.Conditions, cond)
	}

	member.Status.LastProbeTime = probeTime

	err := kubeClient.Status().Update(ctx, member)
	if err != nil {
		logger.Warn("failed to persist member liveness probe", "instance", member.Name, "error", err)
	}
}
