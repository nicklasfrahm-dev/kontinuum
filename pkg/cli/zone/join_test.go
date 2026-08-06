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

func testCmd(out *bytes.Buffer) *cobra.Command {
	cmd := &cobra.Command{Use: "join"}
	cmd.SetOut(out)
	cmd.SetContext(context.Background())

	return cmd
}

func TestRunZoneJoinRequiresDomainEnv(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneJoin(cmd, zone.JoinFlags{Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress},
		func(string, string) (client.Client, error) {
			t.Fatal("hub client should not be built when KONTINUUM_DOMAIN is unset")

			return nil, assert.AnError
		})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KONTINUUM_DOMAIN")
}

func TestRunZoneJoinCreatesZoneObjects(t *testing.T) {
	t.Setenv("KONTINUUM_DOMAIN", testDomain)

	hubClient := newFakeHubClient(t)
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneJoin(cmd, zone.JoinFlags{Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress},
		func(string, string) (client.Client, error) { return hubClient, nil })
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "Created zone")

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(cmd.Context(), client.ObjectKey{Name: testRegion + "-" + testZone}, &got))
	assert.Equal(t, testDomain, got.Spec.Domain)
}

func TestRunZoneJoinPropagatesHubClientBuildError(t *testing.T) {
	t.Setenv("KONTINUUM_DOMAIN", testDomain)

	cmd := testCmd(&bytes.Buffer{})

	err := zone.RunZoneJoin(cmd, zone.JoinFlags{Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress},
		func(string, string) (client.Client, error) { return nil, assert.AnError })
	require.ErrorIs(t, err, assert.AnError)
}

func TestRunZoneJoinWaitReturnsOnceInstalled(t *testing.T) {
	t.Setenv("KONTINUUM_DOMAIN", testDomain)

	name := testRegion + "-" + testZone
	// Pre-seeded already-Installed Zone: RunZoneJoin's own Apply call is a
	// no-op AlreadyExists, and --wait's first poll (before it would ever
	// need to wait on pollInterval's ticker) already observes Installed.
	existing := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha2.ZoneSpec{Region: testRegion, Zone: testZone, Domain: testDomain},
		Status: v1alpha2.ZoneStatus{
			Conditions: []metav1.Condition{{
				Type: zonedomain.InstalledConditionType, Status: metav1.ConditionTrue, Reason: "Installed",
				Message: "kontinuum-server installed", LastTransitionTime: metav1.Now(),
			}},
		},
	}

	hubClient := newFakeHubClient(t, existing)
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneJoin(cmd, zone.JoinFlags{
		Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress,
		Wait: true, Timeout: time.Minute,
	}, func(string, string) (client.Client, error) { return hubClient, nil })
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "Installed")
}

func TestRunZoneJoinWaitTimesOut(t *testing.T) {
	t.Setenv("KONTINUUM_DOMAIN", testDomain)

	hubClient := newFakeHubClient(t)
	buf := &bytes.Buffer{}
	cmd := testCmd(buf)

	err := zone.RunZoneJoin(cmd, zone.JoinFlags{
		Region: testRegion, Zone: testZone, TalosAddress: testTalosAddress,
		Wait: true, Timeout: time.Millisecond,
	}, func(string, string) (client.Client, error) { return hubClient, nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}
