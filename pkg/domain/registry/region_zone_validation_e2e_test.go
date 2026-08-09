package registry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// TestRegionZoneValidationRejectsInconsistentValues is the regression test
// for the CEL rule registry.CustomResourceDefinition's schema attaches to
// spec (see the +kubebuilder:validation:XValidation marker on
// v1alpha2.KontinuumSpec, which config/crd's generated manifest carries):
// only a real apiserver evaluates x-kubernetes-validations — the fake
// clientset used by this package's other CRD tests never runs any
// admission validation, CEL included — so, like
// TestConversionWebhookBridgesLegacyRegistration, this
// needs a real server. Unlike that test, it passes withController=false:
// this test only ever touches v1alpha2 objects directly, so it needs
// neither the conversion webhook nor the heartbeat/TTL reconciler — and
// skipping the Controller means it's also safe to run t.Parallel() against
// TestConversionWebhookBridgesLegacyRegistration (see startTestServer's
// doc for why building two Controllers in the same test binary isn't).
func TestRegionZoneValidationRejectsInconsistentValues(t *testing.T) {
	t.Parallel()

	if raceEnabled {
		// See TestConversionWebhookBridgesLegacyRegistration's identical
		// skip: booting a real backing store trips a pre-existing,
		// unrelated data race inside github.com/k3s-io/kine.
		t.Skip("triggers a pre-existing, unrelated data race in github.com/k3s-io/kine under -race")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	kontinuums, _ := startTestServer(ctx, t, false)

	// Warms up kontinuums' RESTMapper before the table below — a brand-new
	// client's RESTMapper resolves lazily, with no retry of its own, and
	// can race the apiserver's discovery-document propagation on its very
	// first request (see createLegacyRegistration's identical comment).
	// Doing that here means every Create attempt below is an unambiguous
	// signal — success or a genuine CEL rejection — never a coin flip with
	// a transient resolution failure.
	require.Eventually(t, func() bool {
		return kontinuums.List(ctx, &v1alpha2.KontinuumList{}) == nil
	}, e2eEventuallyTimeout, e2eHealthzInterval, "kontinuums client never became ready")

	tests := []struct {
		testName    string
		objName     string
		region      string
		zone        string
		expectError bool
	}{
		{testName: "both empty is valid", objName: "rz-empty", region: "", zone: "", expectError: false},
		{testName: "both set is valid", objName: "rz-both-set", region: e2eRegion, zone: e2eZone, expectError: false},
		{testName: "only region set is invalid", objName: "rz-region-only", region: e2eRegion, zone: "", expectError: true},
		{testName: "only zone set is invalid", objName: "rz-zone-only", region: "", zone: e2eZone, expectError: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.testName, func(t *testing.T) {
			t.Parallel()

			obj := &v1alpha2.Kontinuum{
				ObjectMeta: metav1.ObjectMeta{Name: testCase.objName, Namespace: v1alpha2.DefaultSecretNamespace},
				Spec:       v1alpha2.KontinuumSpec{Region: testCase.region, Zone: testCase.zone},
			}

			err := kontinuums.Create(ctx, obj)

			if testCase.expectError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
