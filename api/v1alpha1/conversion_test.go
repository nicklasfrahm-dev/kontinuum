package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	conversionTestName = "test"
	conversionTestZone = "eu-1a"
)

func TestKontinuumConvertToMovesRoleIntoStatus(t *testing.T) {
	t.Parallel()

	heartbeat := metav1.Now()
	src := &v1alpha1.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: conversionTestName},
		Spec:       v1alpha1.KontinuumSpec{Role: v1alpha1.RoleWorker, Region: "eu", Zone: conversionTestZone},
		Status:     v1alpha1.KontinuumStatus{LastHeartbeatTime: heartbeat},
	}

	dst := &v1alpha2.Kontinuum{}

	require.NoError(t, src.ConvertTo(dst))

	assert.Equal(t, conversionTestName, dst.Name)
	assert.Equal(t, "eu", dst.Spec.Region)
	assert.Equal(t, conversionTestZone, dst.Spec.Zone)
	assert.Equal(t, v1alpha1.RoleWorker, dst.Status.Role)
	assert.Equal(t, heartbeat, dst.Status.LastHeartbeatTime)
}

func TestKontinuumConvertFromMovesRoleIntoSpec(t *testing.T) {
	t.Parallel()

	heartbeat := metav1.Now()
	src := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: conversionTestName},
		Spec:       v1alpha2.KontinuumSpec{Region: "eu", Zone: conversionTestZone},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker, LastHeartbeatTime: heartbeat},
	}

	dst := &v1alpha1.Kontinuum{}

	require.NoError(t, dst.ConvertFrom(src))

	assert.Equal(t, conversionTestName, dst.Name)
	assert.Equal(t, "eu", dst.Spec.Region)
	assert.Equal(t, conversionTestZone, dst.Spec.Zone)
	assert.Equal(t, v1alpha2.RoleWorker, dst.Spec.Role)
	assert.Equal(t, heartbeat, dst.Status.LastHeartbeatTime)
}

// fakeHub is a minimal conversion.Hub that isn't *v1alpha2.Kontinuum, to
// exercise ConvertTo/ConvertFrom's defensive type-assertion failure path —
// unreachable in practice (the webhook machinery only ever calls these with
// the real hub type) but worth covering since it's a real branch.
type fakeHub struct {
	metav1.TypeMeta
	metav1.ObjectMeta
}

func (f *fakeHub) DeepCopyObject() runtime.Object {
	return &fakeHub{TypeMeta: f.TypeMeta, ObjectMeta: *f.DeepCopy()}
}

func (*fakeHub) Hub() {}

func TestKontinuumConvertToRejectsUnsupportedHub(t *testing.T) {
	t.Parallel()

	src := &v1alpha1.Kontinuum{}

	require.Error(t, src.ConvertTo(&fakeHub{}))
}

func TestKontinuumConvertFromRejectsUnsupportedHub(t *testing.T) {
	t.Parallel()

	dst := &v1alpha1.Kontinuum{}

	require.Error(t, dst.ConvertFrom(&fakeHub{}))
}
