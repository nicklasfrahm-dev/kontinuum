package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
)

func TestGroupVersion(t *testing.T) {
	t.Parallel()

	gv := v1alpha1.GroupVersion()

	assert.Equal(t, "kontinuum.sh", gv.Group)
	assert.Equal(t, "v1alpha1", gv.Version)
}

func TestAddToScheme(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()

	err := v1alpha1.AddToScheme(scheme)
	require.NoError(t, err)

	assert.True(t, scheme.Recognizes(v1alpha1.GroupVersion().WithKind("Kontinuum")))
	assert.True(t, scheme.Recognizes(v1alpha1.GroupVersion().WithKind("KontinuumList")))
}

func TestKontinuumDeepCopyIsIndependent(t *testing.T) {
	t.Parallel()

	original := &v1alpha1.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
		Spec:       v1alpha1.KontinuumSpec{Role: v1alpha1.RoleWorker, Region: "eu", Zone: "eu-1a"},
		Status:     v1alpha1.KontinuumStatus{LastHeartbeatTime: metav1.Now()},
	}

	copied := original.DeepCopy()
	copied.Name = "changed"
	copied.Spec.Region = "us"

	assert.Equal(t, "test", original.Name)
	assert.Equal(t, "eu", original.Spec.Region)
	assert.Equal(t, "changed", copied.Name)
	assert.Equal(t, "us", copied.Spec.Region)

	_ = original.DeepCopyObject()
}

func TestKontinuumListDeepCopyIsIndependent(t *testing.T) {
	t.Parallel()

	original := &v1alpha1.KontinuumList{
		Items: []v1alpha1.Kontinuum{
			{ObjectMeta: metav1.ObjectMeta{Name: "a"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "b"}},
		},
	}

	copied := original.DeepCopy()
	copied.Items[0].Name = "changed"

	assert.Equal(t, "a", original.Items[0].Name)
	assert.Equal(t, "changed", copied.Items[0].Name)

	_ = original.DeepCopyObject()
}
