package taloscluster

import (
	"context"
	"fmt"

	"github.com/siderolabs/talos/pkg/machinery/config/machine"
)

// k8sUpgradeStep is a single (component, member) patch the Kubernetes plan
// has decided to make on this pass.
type k8sUpgradeStep struct {
	// component is the component to move — see kubernetesComponents' own
	// doc for the order they move in.
	component kubernetesComponent
	// member is the member to move it on.
	member upgradeMember
	// image is the reference to move it to, already carrying the kubelet's
	// own flavour suffix where that applies (see kubernetesComponent.image).
	image string
}

// k8sUpgradePlan is what one pass of the Kubernetes plan decided.
//
// Exactly one of its fields is meaningful at a time, which is the point:
// either there is a step to take, or the plan is parked waiting for a
// component it already patched, or every component on every member is
// where it should be.
type k8sUpgradePlan struct {
	// step is the patch to make, or nil when there is none to make.
	step *k8sUpgradeStep
	// waitingFor names the "<component> on <member>" the plan is parked
	// on, empty unless it is parked. A parked plan is the normal state for
	// most of an upgrade: a component takes a while to come back Ready,
	// and nothing else may move until it does.
	waitingFor string
}

// converged reports whether every component on every member already runs
// the pinned version.
func (p k8sUpgradePlan) converged() bool {
	return p.step == nil && p.waitingFor == ""
}

// planKubernetesUpgrade walks the cluster's members component by component,
// in the order talosctl upgrade-k8s walks them, and returns the first thing
// that still needs doing.
//
// The walk is the whole rollout: it visits a component on every member
// before moving to the next component, and within a component it visits
// members in collectUpgradeMembers' own control-plane-first order. A member
// already at the target image is not re-patched, but — for the components
// whose rollout is observable — it *is* checked, and a member that has not
// finished rolling parks the plan then and there. So the next member's
// patch cannot happen until this one is Ready, and the next component's
// first patch cannot happen until every member finished the previous one.
// That is the same sequencing talosctl performs inside one blocking call,
// spread across reconcile passes instead.
//
// Every probe is a live read of the member's own Talos API rather than
// anything cached on the cluster: a node that rebooted, rolled back, or was
// replaced underneath us re-enters the plan at whatever it actually runs.
func (r *Reconciler) planKubernetesUpgrade(
	ctx context.Context, members []upgradeMember, desired string, upgradeCtx upgradeContext,
) (k8sUpgradePlan, error) {
	for _, component := range kubernetesComponents() {
		for _, member := range members {
			if !componentRunsOn(component, member) {
				continue
			}

			plan, err := r.planComponentOnMember(ctx, component, member, desired, upgradeCtx)
			if err != nil {
				return k8sUpgradePlan{}, err
			}

			if !plan.converged() {
				return plan, nil
			}
		}
	}

	return k8sUpgradePlan{}, nil
}

// planComponentOnMember decides what, if anything, one component on one
// member needs: a patch, a wait, or nothing.
func (r *Reconciler) planComponentOnMember(
	ctx context.Context, component kubernetesComponent, member upgradeMember,
	desired string, upgradeCtx upgradeContext,
) (k8sUpgradePlan, error) {
	node := dialAddress(*member.instance)

	current, err := r.Bootstrapper.KubernetesComponentImage(
		ctx, upgradeCtx.controlPlaneAddr, node, upgradeCtx.talosCfg, component.name)
	if err != nil {
		return k8sUpgradePlan{}, fmt.Errorf("failed to read %s image on %q: %w",
			component.name, member.instance.Name, err)
	}

	// The kubelet's flavour is read off whatever it runs today rather than
	// chosen here: Talos publishes plain, -slim and -fat kubelet images,
	// and an upgrade has no business moving a node between them.
	target := component.image(desired, imageFlavourSuffix(current))

	if current != target {
		return k8sUpgradePlan{
			step: &k8sUpgradeStep{component: component, member: member, image: target},
		}, nil
	}

	if !component.awaitsRollout {
		return k8sUpgradePlan{}, nil
	}

	rolledOut, err := r.Bootstrapper.KubernetesComponentRolledOut(
		ctx, upgradeCtx.controlPlaneAddr, node, upgradeCtx.talosCfg, component.name, target)
	if err != nil {
		return k8sUpgradePlan{}, fmt.Errorf("failed to check %s rollout on %q: %w",
			component.name, member.instance.Name, err)
	}

	if !rolledOut {
		return k8sUpgradePlan{waitingFor: component.name + " on " + member.instance.Name}, nil
	}

	return k8sUpgradePlan{}, nil
}

// componentRunsOn reports whether member runs component at all. Only the
// kubelet runs everywhere; the rest are control-plane-only, so a worker
// pool's members are skipped for them entirely rather than probed and
// found wanting.
func componentRunsOn(component kubernetesComponent, member upgradeMember) bool {
	return component.onWorkers || member.machineType == machine.TypeControlPlane
}
