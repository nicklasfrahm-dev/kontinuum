package config_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/charmbracelet/huh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/nicklasfrahm/kontinuum/pkg/cli/config"
)

// Shared test fixture names, reused across MergeKubeconfig test cases.
const (
	testClusterName = "demo"
	testKeepName    = "keep"
)

// writeFile writes content to path, failing the test on error.
func writeFile(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

// stubPrompter answers Confirm calls from a fixed queue, in call order, and
// fails the test if either method is called more times than expected —
// e.g. if entries that compare equal are prompted for anyway. Text is not
// exercised by these tests, since MergeKubeconfig never calls it.
type stubPrompter struct {
	t        *testing.T
	confirms []bool
}

func (s *stubPrompter) Confirm(_ string, _ bool) (bool, error) {
	s.t.Helper()

	require.NotEmpty(s.t, s.confirms, "unexpected Confirm call")

	value := s.confirms[0]
	s.confirms = s.confirms[1:]

	return value, nil
}

func (s *stubPrompter) Text(_ string) (string, error) {
	s.t.Helper()

	s.t.Fatal("unexpected Text call")

	return "", nil
}

// abortingPrompter simulates a user cancelling the interactive paste prompt
// (e.g. Ctrl+C/Esc), which huh reports via the huh.ErrUserAborted sentinel.
type abortingPrompter struct{}

func (abortingPrompter) Confirm(string, bool) (bool, error) {
	return false, huh.ErrUserAborted
}

func (abortingPrompter) Text(string) (string, error) {
	return "", huh.ErrUserAborted
}

func newCluster(server string) *clientcmdapi.Cluster {
	return &clientcmdapi.Cluster{Server: server}
}

func TestMergeKubeconfigAddsNewEntriesWithoutPrompting(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()

	imported := clientcmdapi.NewConfig()
	imported.Clusters[testClusterName] = newCluster("https://demo.example.com")
	imported.AuthInfos[testClusterName] = &clientcmdapi.AuthInfo{Token: "demo-token"}
	imported.Contexts[testClusterName] = &clientcmdapi.Context{Cluster: testClusterName, AuthInfo: testClusterName}
	imported.CurrentContext = testClusterName

	prompt := &stubPrompter{t: t}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)

	assert.Equal(t, "https://demo.example.com", merged.Clusters[testClusterName].Server)
	assert.Equal(t, "demo-token", merged.AuthInfos[testClusterName].Token)
	assert.Equal(t, testClusterName, merged.Contexts[testClusterName].Cluster)
	assert.Equal(t, testClusterName, merged.CurrentContext)
}

func TestMergeKubeconfigSkipsPromptWhenEntryIsIdentical(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()
	existing.Clusters[testClusterName] = newCluster("https://demo.example.com")
	existing.Clusters[testClusterName].LocationOfOrigin = "/home/user/.kube/config"

	imported := clientcmdapi.NewConfig()
	imported.Clusters[testClusterName] = newCluster("https://demo.example.com")

	// A stubPrompter with an empty confirms queue fails the test the moment
	// Confirm is called, so this asserts no prompt was raised for the
	// byte-identical cluster (LocationOfOrigin aside).
	prompt := &stubPrompter{t: t}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, "https://demo.example.com", merged.Clusters[testClusterName].Server)
}

func TestMergeKubeconfigPromptsOnConflictAndOverwrites(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()
	existing.Clusters[testClusterName] = newCluster("https://old.example.com")

	imported := clientcmdapi.NewConfig()
	imported.Clusters[testClusterName] = newCluster("https://new.example.com")

	prompt := &stubPrompter{t: t, confirms: []bool{true}}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, "https://new.example.com", merged.Clusters[testClusterName].Server)
}

func TestMergeKubeconfigPromptsOnConflictAndKeepsExisting(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()
	existing.Clusters[testClusterName] = newCluster("https://old.example.com")

	imported := clientcmdapi.NewConfig()
	imported.Clusters[testClusterName] = newCluster("https://new.example.com")

	prompt := &stubPrompter{t: t, confirms: []bool{false}}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, "https://old.example.com", merged.Clusters[testClusterName].Server)
}

func TestMergeKubeconfigLeavesUnrelatedExistingEntriesUntouched(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()
	existing.Clusters[testKeepName] = newCluster("https://keep.example.com")
	existing.CurrentContext = testKeepName

	imported := clientcmdapi.NewConfig()
	imported.Clusters[testClusterName] = newCluster("https://demo.example.com")

	prompt := &stubPrompter{t: t}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, "https://keep.example.com", merged.Clusters[testKeepName].Server)
	assert.Equal(t, "https://demo.example.com", merged.Clusters[testClusterName].Server)
	assert.Equal(t, testKeepName, merged.CurrentContext)
}

func TestMergeKubeconfigCurrentContextAdoptedWithoutPromptWhenNoneSetYet(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()

	imported := clientcmdapi.NewConfig()
	imported.CurrentContext = testClusterName

	// No confirms queued: adopting a current-context into an empty
	// kubeconfig must not prompt.
	prompt := &stubPrompter{t: t}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, testClusterName, merged.CurrentContext)
}

func TestMergeKubeconfigCurrentContextKeepsExistingWhenDeclined(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()
	existing.CurrentContext = testKeepName

	imported := clientcmdapi.NewConfig()
	imported.CurrentContext = testClusterName

	prompt := &stubPrompter{t: t, confirms: []bool{false}}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, testKeepName, merged.CurrentContext)
}

func TestMergeKubeconfigCurrentContextUnchangedWhenImportedEmpty(t *testing.T) {
	t.Parallel()

	existing := clientcmdapi.NewConfig()
	existing.CurrentContext = testKeepName

	imported := clientcmdapi.NewConfig()

	// No confirms queued: an empty imported.CurrentContext must not prompt.
	prompt := &stubPrompter{t: t}

	merged, err := config.MergeKubeconfig(existing, imported, prompt)
	require.NoError(t, err)
	assert.Equal(t, testKeepName, merged.CurrentContext)
}

func TestNewCmdRegistersImportSubcommand(t *testing.T) {
	t.Parallel()

	cmd := config.NewCmd()

	importCmd, _, err := cmd.Find([]string{"import"})
	require.NoError(t, err)
	assert.Equal(t, "import [path]", importCmd.Use)
}

func TestNewImportCmdRejectsExtraArgs(t *testing.T) {
	t.Parallel()

	cmd := config.NewImportCmd()
	cmd.SetArgs([]string{"one", "two"})
	cmd.SilenceErrors = true

	err := cmd.Execute()
	assert.Error(t, err)
}

func TestNewImportCmdReadsKubeconfigFromPath(t *testing.T) {
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "config"))

	kubeconfigPath := filepath.Join(t.TempDir(), "imported.yaml")
	writeFile(t, kubeconfigPath, `apiVersion: v1
kind: Config
clusters:
  - name: demo
    cluster:
      server: https://demo.example.com
contexts:
  - name: demo
    context:
      cluster: demo
      user: demo
current-context: demo
users:
  - name: demo
    user:
      token: demo-token
`)

	cmd := config.NewImportCmd()
	cmd.SetArgs([]string{kubeconfigPath})

	err := cmd.Execute()
	require.NoError(t, err)
}

func TestRunConfigImportPrintsPlainNoticeWhenPromptAborted(t *testing.T) {
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "config"))

	cmd := config.NewImportCmd()

	var stderr bytes.Buffer

	cmd.SetErr(&stderr)

	err := config.RunConfigImport(cmd, nil, abortingPrompter{})

	require.Error(t, err)
	require.ErrorIs(t, err, huh.ErrUserAborted)
	assert.True(t, cmd.SilenceUsage, "aborting should silence cobra's usage output")
	assert.True(t, cmd.SilenceErrors, "aborting should silence cobra's default error output")
	assert.Equal(t, "Aborted.\n", stderr.String())
}
