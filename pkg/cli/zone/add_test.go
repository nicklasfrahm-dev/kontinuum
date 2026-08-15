package zone_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/cli/zone"
	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const (
	testRegion       = "eu"
	testZone         = "eu-1a"
	testDomain       = "kontinuum.example.com"
	testTalosAddress = "10.0.0.5"
)

func newFakeHubClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Zone{}).
		WithObjects(objects...).
		Build()
}

// registeredKontinuumWithDomain is a Kontinuum that's already published a
// DNS domain on its own status.config.server.dns.domain — seeded into a
// fake hub client so pkg/domain/zone.Add's own domain inference (see
// AddOptions.Domain's doc) has something to find, the same way a real
// hub's self-registration would provide it.
func registeredKontinuumWithDomain(name, domain string) *v1alpha2.Kontinuum {
	return &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha2.KontinuumStatus{
			Config: v1alpha2.KontinuumConfigStatus{
				Server: v1alpha2.KontinuumServerConfigStatus{
					DNS: v1alpha2.KontinuumDNSConfigStatus{Domain: domain},
				},
			},
		},
	}
}

func testCmd(out *bytes.Buffer) *cobra.Command {
	cmd := &cobra.Command{Use: "add"}
	cmd.SetOut(out)
	cmd.SetContext(context.Background())

	return cmd
}

func TestRunZoneAddCreatesZoneObjects(t *testing.T) {
	t.Parallel()

	hubClient := newFakeHubClient(t, registeredKontinuumWithDomain("hub", testDomain))
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneAdd(cmd, zone.AddFlags{Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress},
		func(string, string) (client.Client, error) { return hubClient, nil })
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "Created zone")

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(cmd.Context(),
		client.ObjectKey{Name: testRegion + "-" + testZone, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))
	assert.Equal(t, testDomain, got.Spec.Domain)
}

// TestRunZoneAddThreadsUnregisterInstancesOnDeleteFlag covers
// --unregister-instances-on-delete's own path onto the created
// TalosCluster's spec.teardown.unregisterInstances — the field
// TalosClusterFinalizer's own teardown actually reads.
func TestRunZoneAddThreadsUnregisterInstancesOnDeleteFlag(t *testing.T) {
	t.Parallel()

	hubClient := newFakeHubClient(t, registeredKontinuumWithDomain("hub", testDomain))
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneAdd(cmd, zone.AddFlags{
		Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress,
		UnregisterInstancesOnDelete: true,
	}, func(string, string) (client.Client, error) { return hubClient, nil })
	require.NoError(t, err)

	var got v1alpha2.TalosCluster
	require.NoError(t, hubClient.Get(cmd.Context(),
		client.ObjectKey{Name: testRegion + "-" + testZone, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))
	assert.True(t, got.Spec.Teardown.UnregisterInstances)
}

func TestRunZoneAddPropagatesHubClientBuildError(t *testing.T) {
	t.Parallel()

	cmd := testCmd(&bytes.Buffer{})

	err := zone.RunZoneAdd(cmd, zone.AddFlags{Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress},
		func(string, string) (client.Client, error) { return nil, assert.AnError })
	require.ErrorIs(t, err, assert.AnError)
}

func TestRunZoneAddPropagatesDomainInferenceError(t *testing.T) {
	t.Parallel()

	// No registered Kontinuum at all — Add has nothing to infer a domain
	// from.
	hubClient := newFakeHubClient(t)
	cmd := testCmd(&bytes.Buffer{})

	err := zone.RunZoneAdd(cmd, zone.AddFlags{Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress},
		func(string, string) (client.Client, error) { return hubClient, nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "infer domain")
}

func TestRunZoneAddWaitReturnsOnceInstalled(t *testing.T) {
	t.Parallel()

	name := testRegion + "-" + testZone
	// Pre-seeded already-Installed Zone: RunZoneAdd's own Add call is a
	// no-op AlreadyExists, and --wait's first poll (before it would ever
	// need to wait on pollInterval's ticker) already observes Installed.
	existing := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: testRegion, Zone: testZone, Domain: testDomain},
		Status: v1alpha2.ZoneStatus{
			Conditions: []metav1.Condition{{
				Type: zonedomain.InstalledConditionType, Status: metav1.ConditionTrue, Reason: "Installed",
				Message: "kontinuum-server installed", LastTransitionTime: metav1.Now(),
			}},
		},
	}

	hubClient := newFakeHubClient(t, existing, registeredKontinuumWithDomain("hub", testDomain))
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneAdd(cmd, zone.AddFlags{
		Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress,
		Wait: true, Timeout: time.Minute,
	}, func(string, string) (client.Client, error) { return hubClient, nil })
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "Installed")
}

func TestRunZoneAddWaitTimesOut(t *testing.T) {
	t.Parallel()

	hubClient := newFakeHubClient(t, registeredKontinuumWithDomain("hub", testDomain))
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneAdd(cmd, zone.AddFlags{
		Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress,
		Wait: true, Timeout: time.Millisecond,
	}, func(string, string) (client.Client, error) { return hubClient, nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}
