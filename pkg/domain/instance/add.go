package instance

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// errAddOptionsMissingField is Add's own sentinel — err113 flags a
// dynamically constructed errors.New/fmt.Errorf call without a wrapped
// static error, same as zone.errAddOptionsMissingField.
var errAddOptionsMissingField = errors.New("instance add: missing required field")

// AddOptions is Add's own input — pkg/ui's "Add instance" form parses
// straight onto this. See issue #81: this is the standalone counterpart to
// zone.AddOptions' own seed Instance — a bare-metal candidate registered on
// its own, with no Zone/InstancePool/TalosCluster fanned out around it, left
// unclaimed until something (a zone-add's own instance-picker, or a future
// InstancePool selector match) claims it.
type AddOptions struct {
	// Namespace is which tenant's own namespace the new Instance is created
	// in — see issue #63's architecture: Instance became namespaced
	// specifically so a tenant can bring their own hardware into their own
	// namespace, the same namespace handleInstances already lists it from.
	Namespace string
	// Address is the candidate's maintenance-mode address (IP or hostname)
	// — becomes spec.interfaces[0], the same field instance.Reconciler's own
	// discovery probing dials (see Reconciler.Reconcile).
	Address string
}

// NameFromAddress derives an Instance's name deterministically from address
// via Hash — shared by Add below and by zone.BuildAddObjects' own seed
// Instance (see issue #81), so the exact same address always names the exact
// same Instance object regardless of which of the two ever creates it
// first: registering an address here that a zone later types in freehand
// (or the reverse) resolves to one object, not two independent duplicates.
// Re-submitting the same address is then a safe, idempotent no-op (see
// Add's own doc) rather than creating a duplicate candidate every time.
func NameFromAddress(address string) string {
	return "instance-" + Hash(v1alpha2.InstanceSpec{Interfaces: []string{address}})
}

// Add creates a standalone, unclaimed Instance object for opts.Address in
// opts.Namespace — discovered asynchronously by Reconciler's own
// maintenance-mode probing, exactly like zone.Add's own seed Instance, but
// fanning out no other objects around it. Tolerates AlreadyExists —
// re-submitting the same address is a safe no-op, returning the
// already-registered Instance rather than erroring or creating a duplicate.
func Add(ctx context.Context, hubClient client.Client, opts AddOptions) (*v1alpha2.Instance, error) {
	address := strings.TrimSpace(opts.Address)

	if opts.Namespace == "" || address == "" {
		return nil, fmt.Errorf("%w: namespace and address are required", errAddOptionsMissingField)
	}

	inst := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: NameFromAddress(address), Namespace: opts.Namespace},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{address}},
	}

	err := hubClient.Create(ctx, inst)
	if err == nil {
		return inst, nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("failed to create instance %q: %w", inst.Name, err)
	}

	err = hubClient.Get(ctx, client.ObjectKeyFromObject(inst), inst)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch already-existing instance %q: %w", inst.Name, err)
	}

	return inst, nil
}
