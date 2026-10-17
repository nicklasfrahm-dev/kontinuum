package zone_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// errAPIUnavailable stands in for an apiserver that can't answer yet.
var errAPIUnavailable = errors.New("api unavailable")

// lonePeerZone builds region "eu"'s Zone object for zoneName.
func lonePeerZone(zoneName string) *v1alpha2.Zone {
	return &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-" + zoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: zoneName},
	}
}

func lonePeerKontinuum(name, role string) *v1alpha2.Kontinuum {
	return &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status:     v1alpha2.KontinuumStatus{Role: role},
	}
}

func TestValidateLonePeer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		objects []client.Object
		lone    bool
	}{
		{name: "no zones yet", objects: nil, lone: false},
		{name: "own zone alone", objects: []client.Object{lonePeerZone("1a")}, lone: true},
		{
			name:    "own zone alone with only workers registered",
			objects: []client.Object{lonePeerZone("1a"), lonePeerKontinuum("eu-1a", v1alpha2.RoleWorker)},
			lone:    true,
		},
		{
			name:    "own zone and another",
			objects: []client.Object{lonePeerZone("1a"), lonePeerZone("1b")},
			lone:    false,
		},
		{name: "only another zone", objects: []client.Object{lonePeerZone("1b")}, lone: false},
		{
			name:    "own zone alone with a hub registered",
			objects: []client.Object{lonePeerZone("1a"), lonePeerKontinuum("hub", v1alpha2.RoleControlPlane)},
			lone:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			hubClient := newHubFakeClient(t, test.objects...)

			err := zone.ValidateLonePeer(context.Background(), hubClient, "eu", "1a")
			if test.lone {
				require.ErrorIs(t, err, zone.ErrLoneHubLessPeer)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateLonePeerAcceptsMissingZoneCRD(t *testing.T) {
	t.Parallel()

	noMatch := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: v1alpha2.GroupVersion().Group, Kind: "Zone"}}
		},
	}).Build()

	require.NoError(t, zone.ValidateLonePeer(context.Background(), noMatch, "eu", "1a"))
}

func TestValidateLonePeerSurfacesListErrors(t *testing.T) {
	t.Parallel()

	failing := failingListClient(t, nil)

	err := zone.ValidateLonePeer(context.Background(), failing, "eu", "1a")
	require.ErrorIs(t, err, errAPIUnavailable)
}

// failingListClient fails every List with errAPIUnavailable, counting the
// calls in calls when it's non-nil.
func failingListClient(t *testing.T, calls *atomic.Int32) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			if calls != nil {
				calls.Add(1)
			}

			return errAPIUnavailable
		},
	}).Build()
}

func TestLonePeerCheckStartRejectsLonePeer(t *testing.T) {
	t.Parallel()

	runnable := zone.NewLonePeerRunnable(&zone.LonePeerCheck{
		Region: "eu", Zone: "1a", Logger: slog.Default(), RetryInterval: time.Millisecond,
	}, newHubFakeClient(t, lonePeerZone("1a")))

	require.ErrorIs(t, runnable.Start(context.Background()), zone.ErrLoneHubLessPeer)
}

func TestLonePeerCheckStartAcceptsFleet(t *testing.T) {
	t.Parallel()

	runnable := zone.NewLonePeerRunnable(&zone.LonePeerCheck{
		Region: "eu", Zone: "1a", Logger: slog.Default(), RetryInterval: time.Millisecond,
	}, newHubFakeClient(t, lonePeerZone("1a"), lonePeerZone("1b")))

	require.NoError(t, runnable.Start(context.Background()))
}

func TestLonePeerCheckStartRetriesUntilCanceled(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	runnable := zone.NewLonePeerRunnable(&zone.LonePeerCheck{
		Region: "eu", Zone: "1a", Logger: slog.Default(), RetryInterval: time.Millisecond,
	}, failingListClient(t, &calls))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() { done <- runnable.Start(ctx) }()

	require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, time.Millisecond)

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}
}
