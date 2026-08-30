package taloscluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCPUList(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		input   string
		want    []uint32
		wantErr bool
	}{
		"single range": {
			input: "0-3",
			want:  []uint32{0, 1, 2, 3},
		},
		"single cpu": {
			input: "0",
			want:  []uint32{0},
		},
		"multiple groups": {
			input: "0-3,8-11",
			want:  []uint32{0, 1, 2, 3, 8, 9, 10, 11},
		},
		"trailing newline": {
			input: "0-3\n",
			want:  []uint32{0, 1, 2, 3},
		},
		"empty": {
			input: "",
			want:  nil,
		},
		"garbage": {
			input:   "not-a-cpu-list",
			wantErr: true,
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := parseCPUList(testCase.input)
			if testCase.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.want, got)
		})
	}
}

func TestParseSysfsUint(t *testing.T) {
	t.Parallel()

	got, err := parseSysfsUint("1500000\n")
	require.NoError(t, err)
	assert.Equal(t, uint32(1500000), got)

	_, err = parseSysfsUint("not-a-number")
	require.Error(t, err)
}
