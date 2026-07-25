package auth_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/nicklasfrahm/kontinuum/pkg/auth"
)

var (
	errTestForbiddenReason = errors.New(
		"forbidden: user is not in admin group, system:masters group, or a service account")
	errTestContext = errors.New("context")
	errTestBoom    = errors.New("boom")
)

func TestMapError(t *testing.T) {
	t.Parallel()

	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "", errTestForbiddenReason)

	tests := map[string]struct {
		err  error
		want string
	}{
		"nil error returns empty string": {
			err:  nil,
			want: "",
		},
		"known sentinel error is mapped": {
			err:  auth.ErrLoginExpired,
			want: "Your login session has expired. Please try signing in again.",
		},
		"wrapped known sentinel error is mapped": {
			err:  errors.Join(errTestContext, auth.ErrStateMismatch),
			want: "Security validation failed (state mismatch). Please try signing in again.",
		},
		"forbidden kubernetes error is mapped": {
			err: forbidden,
			want: "You're signed in, but your account isn't authorized to access this. " +
				"Ask an administrator to grant you the necessary permissions.",
		},
		"unrecognized error falls back to its own message": {
			err:  errTestBoom,
			want: "boom",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, auth.MapError(tc.err))
		})
	}
}
