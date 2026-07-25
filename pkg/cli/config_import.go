package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// errEmptyKubeconfig is returned when no kubeconfig content was read from a
// file or pasted interactively.
var errEmptyKubeconfig = errors.New("no kubeconfig content provided")

// pasteInputLines and pasteInputCharLimit size the interactive textarea used
// to paste a kubeconfig: tall and roomy enough for any realistic
// clusters/users/contexts document.
const (
	pasteInputLines     = 15
	pasteInputCharLimit = 1 << 20
)

// Prompter asks the user yes/no and free-text questions during
// "config import". HuhPrompter is the production implementation, backed by
// interactive huh forms; tests substitute a stub that returns canned answers
// without needing a terminal.
type Prompter interface {
	// Confirm asks a yes/no question, pre-selected to defaultValue.
	Confirm(title string, defaultValue bool) (bool, error)
	// Text asks for free-form, potentially multi-line input.
	Text(title string) (string, error)
}

// HuhPrompter is the interactive Prompter, backed by charmbracelet/huh.
type HuhPrompter struct{}

// Confirm asks a yes/no question, pre-selected to defaultValue, via an
// interactive huh confirm field.
func (HuhPrompter) Confirm(title string, defaultValue bool) (bool, error) {
	value := defaultValue

	err := huh.NewConfirm().
		Title(title).
		Affirmative("Yes").
		Negative("No").
		Value(&value).
		Run()
	if err != nil {
		return false, fmt.Errorf("failed to read confirmation: %w", err)
	}

	return value, nil
}

// Text asks for free-form, potentially multi-line input via an interactive
// huh text field.
func (HuhPrompter) Text(title string) (string, error) {
	var value string

	err := huh.NewText().
		Title(title).
		Lines(pasteInputLines).
		CharLimit(pasteInputCharLimit).
		Value(&value).
		Run()
	if err != nil {
		return "", fmt.Errorf("failed to read input: %w", err)
	}

	return value, nil
}

// NewConfigImportCmd builds the "config import" command. It merges a
// kubeconfig — read from path, or pasted interactively when path is omitted —
// into the kubeconfig resolved from $KUBECONFIG (or ~/.kube/config when
// unset). Clusters, users, and contexts that already exist under the same
// name are only overwritten after the user confirms.
func NewConfigImportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "import [path]",
		Short: "Merge a kubeconfig into the existing kubeconfig",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConfigImport(cmd, args, HuhPrompter{})
		},
	}
}

// runConfigImport reads the kubeconfig to import, merges it into the
// existing kubeconfig on disk, and writes the result back to the same file.
func runConfigImport(cmd *cobra.Command, args []string, prompt Prompter) error {
	content, err := readKubeconfigInput(args, prompt)
	if err != nil {
		return err
	}

	imported, err := clientcmd.Load(content)
	if err != nil {
		return fmt.Errorf("failed to parse kubeconfig: %w", err)
	}

	targetPath := clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()

	existing, err := loadExistingKubeconfig(targetPath)
	if err != nil {
		return err
	}

	merged, err := MergeKubeconfig(existing, imported, prompt)
	if err != nil {
		return err
	}

	writeErr := clientcmd.WriteToFile(*merged, targetPath)
	if writeErr != nil {
		return fmt.Errorf("failed to write kubeconfig: %w", writeErr)
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Imported kubeconfig into %s\n", targetPath)
	if err != nil {
		return fmt.Errorf("failed to print import result: %w", err)
	}

	return nil
}

// readKubeconfigInput returns the raw kubeconfig content to import: the
// contents of args[0] when a path was given, otherwise content pasted
// interactively via prompt.
func readKubeconfigInput(args []string, prompt Prompter) ([]byte, error) {
	if len(args) > 0 {
		content, err := os.ReadFile(args[0])
		if err != nil {
			return nil, fmt.Errorf("failed to read kubeconfig file: %w", err)
		}

		return content, nil
	}

	pasted, err := prompt.Text("Paste kubeconfig content")
	if err != nil {
		return nil, fmt.Errorf("failed to read pasted kubeconfig: %w", err)
	}

	if strings.TrimSpace(pasted) == "" {
		return nil, errEmptyKubeconfig
	}

	return []byte(pasted), nil
}

// loadExistingKubeconfig loads the kubeconfig at path, or an empty one if
// the file does not exist yet.
func loadExistingKubeconfig(path string) (*clientcmdapi.Config, error) {
	_, statErr := os.Stat(path)
	if os.IsNotExist(statErr) {
		return clientcmdapi.NewConfig(), nil
	}

	existing, err := clientcmd.LoadFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing kubeconfig: %w", err)
	}

	return existing, nil
}

// MergeKubeconfig merges imported's clusters, users, and contexts into
// existing, prompting before any name collision is overwritten (see
// mergeEntries). The result keeps existing's preferences and current
// context, except current-context switches to imported's when the user
// confirms it (see promptCurrentContext).
func MergeKubeconfig(existing, imported *clientcmdapi.Config, prompt Prompter) (*clientcmdapi.Config, error) {
	clusters, err := mergeEntries("cluster", existing.Clusters, imported.Clusters, clustersEqual, prompt)
	if err != nil {
		return nil, err
	}

	authInfos, err := mergeEntries("user", existing.AuthInfos, imported.AuthInfos, authInfosEqual, prompt)
	if err != nil {
		return nil, err
	}

	contexts, err := mergeEntries("context", existing.Contexts, imported.Contexts, contextsEqual, prompt)
	if err != nil {
		return nil, err
	}

	merged := clientcmdapi.NewConfig()
	merged.Preferences = existing.Preferences
	merged.Extensions = existing.Extensions
	merged.Clusters = clusters
	merged.AuthInfos = authInfos
	merged.Contexts = contexts
	merged.CurrentContext = existing.CurrentContext

	currentContext, err := promptCurrentContext(existing.CurrentContext, imported.CurrentContext, prompt)
	if err != nil {
		return nil, err
	}

	merged.CurrentContext = currentContext

	return merged, nil
}

// promptCurrentContext decides the merged kubeconfig's current-context.
// imported is adopted outright when it names a different, non-empty context
// and existing has none of its own yet — the common case of importing into
// a fresh or empty kubeconfig. Otherwise, switching away from an existing
// current-context requires confirmation.
func promptCurrentContext(existing, imported string, prompt Prompter) (string, error) {
	if imported == "" || imported == existing {
		return existing, nil
	}

	if existing == "" {
		return imported, nil
	}

	title := fmt.Sprintf("Set %q as the current context?", imported)

	switchContext, err := prompt.Confirm(title, false)
	if err != nil {
		return "", fmt.Errorf("failed to confirm current-context switch: %w", err)
	}

	if !switchContext {
		return existing, nil
	}

	return imported, nil
}

// mergeEntries merges imported into a copy of existing. When a name exists
// in both maps with unequal values (per equal), the user is prompted before
// imported's value replaces existing's; declining keeps existing's value.
// kind names the entry type in the prompt text (e.g. "cluster").
func mergeEntries[T any](
	kind string, existing, imported map[string]T, equal func(existing, imported T) bool, prompt Prompter,
) (map[string]T, error) {
	merged := make(map[string]T, len(existing)+len(imported))
	maps.Copy(merged, existing)

	for name, value := range imported {
		current, ok := merged[name]
		if ok && !equal(current, value) {
			title := fmt.Sprintf("Overwrite existing %s %q?", kind, name)

			overwrite, err := prompt.Confirm(title, false)
			if err != nil {
				return nil, fmt.Errorf("failed to confirm %s overwrite: %w", kind, err)
			}

			if !overwrite {
				continue
			}
		}

		merged[name] = value
	}

	return merged, nil
}

// clustersEqual reports whether existing and imported describe the same
// cluster. They're compared via their clientcmd wire representation, which
// tags LocationOfOrigin json:"-" — so the field clientcmd.LoadFromFile
// stamps with the source file's path (but clientcmd.Load leaves empty)
// never factors into the comparison.
func clustersEqual(existing, imported *clientcmdapi.Cluster) bool {
	return entriesEqual(existing, imported)
}

// authInfosEqual reports whether existing and imported describe the same
// user — see clustersEqual.
func authInfosEqual(existing, imported *clientcmdapi.AuthInfo) bool {
	return entriesEqual(existing, imported)
}

// contextsEqual reports whether existing and imported describe the same
// context — see clustersEqual.
func contextsEqual(existing, imported *clientcmdapi.Context) bool {
	return entriesEqual(existing, imported)
}

// entriesEqual reports whether existing and imported serialize to the same
// JSON. Both are clientcmd API types (*clientcmdapi.Cluster, *AuthInfo, or
// *Context), so a marshal error here would mean the type itself can no
// longer be serialized — treated as inequality rather than surfaced, since
// clientcmd.WriteToFile would fail identically and far more informatively
// right after.
func entriesEqual(existing, imported any) bool {
	existingJSON, existingErr := json.Marshal(existing)
	importedJSON, importedErr := json.Marshal(imported)

	return existingErr == nil && importedErr == nil && bytes.Equal(existingJSON, importedJSON)
}
