package fleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"sigs.k8s.io/yaml"
)

var (
	// ErrNotAMapping is Load's sentinel for a file that isn't a YAML
	// mapping at all (empty, a list, a scalar).
	ErrNotAMapping = errors.New("kontinuum.yml must be a yaml mapping")
	// ErrWrongAPIVersion is Load's sentinel for a file declaring any other
	// apiVersion than APIVersion.
	ErrWrongAPIVersion = errors.New("unsupported apiVersion, must be " + APIVersion)
	// ErrWrongKind is Load's sentinel for a file declaring any other kind
	// than Kind.
	ErrWrongKind = errors.New("unsupported kind, must be " + Kind)
	// ErrKubeconfig is Load's sentinel for a file whose kind is Config: a
	// kubeconfig handed to up by mistake, e.g. the kontinuum.yaml this
	// repo used to ship.
	ErrKubeconfig = errors.New("this is a kubeconfig (kind: Config), not a kontinuum.yml (kind: Fleet)")
	// ErrNullSequenceElement is Load's sentinel for a null inside a list:
	// it removes nothing and is almost certainly a typo.
	ErrNullSequenceElement = errors.New("null is not allowed as a list element")
	// ErrNoFiles is Load's sentinel for an empty list of paths.
	ErrNoFiles = errors.New("no kontinuum.yml given")
)

// sourced is a string value of a loaded file, remembering which file it
// came from: a ${file:} reference resolves relative to that file, and an
// error names it.
type sourced struct {
	text string
	file string
}

// Document is one or more kontinuum.yml files merged left to right — see
// Load. It holds references unexpanded: Render shows it as written, and
// Expand turns it into a Fleet.
type Document struct {
	tree    map[string]any
	removed []string
}

// Load reads paths and merges them left to right, the right-most file
// winning, following plain YAML structure with no per-field special cases:
// mappings merge key by key, recursively; scalars and sequences (zones
// included) are replaced whole; and null removes a key, subtree included,
// as if no file had set it. Every file is checked on its own for its
// apiVersion and kind, for unknown fields and for null list elements,
// with errors naming the file. Everything else — required fields, the
// secret-field rule — is checked on the merged result by Expand and
// Validate, so neither a base nor an overlay needs to be a valid Fleet by
// itself. Merging happens before reference expansion, so a reference an
// overlay overrides is never read.
func Load(paths []string) (*Document, error) {
	if len(paths) == 0 {
		return nil, ErrNoFiles
	}

	doc := &Document{tree: map[string]any{}}

	for _, path := range paths {
		tree, err := loadFile(path)
		if err != nil {
			return nil, err
		}

		doc.removed = append(doc.removed, mergeTree(doc.tree, tree, "", path)...)
	}

	return doc, nil
}

// loadFile reads and checks one file — see Load — returning its tree with
// every string replaced by a sourced naming path.
func loadFile(path string) (map[string]any, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}

	tree, err := parseFile(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	sourceMapping(tree, path)

	return tree, nil
}

// parseFile parses and checks one file's contents — see Load.
func parseFile(data []byte) (map[string]any, error) {
	var tree map[string]any

	err := yaml.Unmarshal(data, &tree)
	if err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	if tree == nil {
		return nil, ErrNotAMapping
	}

	err = checkKind(tree)
	if err != nil {
		return nil, err
	}

	err = yaml.UnmarshalStrict(data, &Fleet{})
	if err != nil {
		return nil, fmt.Errorf("invalid fleet: %w", err)
	}

	return tree, checkNullElements(tree, "")
}

// checkKind checks tree's apiVersion and kind, with a pointed error for a
// kubeconfig.
func checkKind(tree map[string]any) error {
	if tree["kind"] == "Config" {
		return fmt.Errorf("%w; pass a kontinuum.yml with -f, and note up writes kubeconfigs to kubeconfig.yaml",
			ErrKubeconfig)
	}

	if tree["apiVersion"] != APIVersion {
		return fmt.Errorf("%w, got %v", ErrWrongAPIVersion, tree["apiVersion"])
	}

	if tree["kind"] != Kind {
		return fmt.Errorf("%w, got %v", ErrWrongKind, tree["kind"])
	}

	return nil
}

// checkNullElements rejects a null anywhere inside a sequence of node.
func checkNullElements(node any, path string) error {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			err := checkNullElements(value, joinPath(path, key))
			if err != nil {
				return err
			}
		}
	case []any:
		for index, value := range typed {
			if value == nil {
				return fmt.Errorf("%w: %s[%d]", ErrNullSequenceElement, path, index)
			}

			err := checkNullElements(value, fmt.Sprintf("%s[%d]", path, index))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// sourceMapping replaces every string in tree, in place, with a sourced
// naming file.
func sourceMapping(tree map[string]any, file string) {
	for key, value := range tree {
		tree[key] = sourceTree(value, file)
	}
}

// sourceTree replaces every string in node with a sourced naming file.
func sourceTree(node any, file string) any {
	switch typed := node.(type) {
	case map[string]any:
		sourceMapping(typed, file)

		return typed
	case []any:
		for index, value := range typed {
			typed[index] = sourceTree(value, file)
		}

		return typed
	case string:
		return sourced{text: typed, file: file}
	default:
		return node
	}
}

// mergeTree merges src into dst in place — see Load for the rules —
// returning a description of every key a null removed.
func mergeTree(dst, src map[string]any, path, file string) []string {
	var removed []string

	for _, key := range sortedKeys(src) {
		value := src[key]
		keyPath := joinPath(path, key)

		if value == nil {
			if _, present := dst[key]; present {
				delete(dst, key)

				removed = append(removed, keyPath+" (removed by "+file+")")
			}

			continue
		}

		srcMap, srcIsMap := value.(map[string]any)
		dstMap, dstIsMap := dst[key].(map[string]any)

		if srcIsMap && dstIsMap {
			removed = append(removed, mergeTree(dstMap, srcMap, keyPath, file)...)

			continue
		}

		if srcIsMap {
			// A mapping that didn't exist yet (or replaces a scalar) is
			// still merged into an empty one, so its own nulls are dropped
			// rather than kept as values.
			fresh := map[string]any{}
			mergeTree(fresh, srcMap, keyPath, file)
			dst[key] = fresh

			continue
		}

		dst[key] = value
	}

	return removed
}

// sortedKeys returns tree's keys in order, for deterministic output.
func sortedKeys(tree map[string]any) []string {
	keys := make([]string, 0, len(tree))
	for key := range tree {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// joinPath appends key to a dotted path.
func joinPath(path, key string) string {
	if path == "" {
		return key
	}

	return path + "." + key
}

// Render returns the merged document as YAML with every reference left
// unexpanded, so the effective configuration can be reviewed without
// revealing a secret. Keys an overlay removed with null are listed in a
// comment above it, so a field falling back to its default stays visible.
func (d *Document) Render() ([]byte, error) {
	data, err := yaml.Marshal(unsource(d.tree, func(value sourced) string { return value.text }))
	if err != nil {
		return nil, fmt.Errorf("failed to render merged fleet: %w", err)
	}

	var out bytes.Buffer

	for _, removed := range d.removed {
		out.WriteString("# ")
		out.WriteString(removed)
		out.WriteString("\n")
	}

	out.Write(data)

	return out.Bytes(), nil
}

// unsource rebuilds node with every sourced replaced by text(value).
func unsource(node any, text func(sourced) string) any {
	switch typed := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = unsource(value, text)
		}

		return out
	case []any:
		out := make([]any, len(typed))
		for index, value := range typed {
			out[index] = unsource(value, text)
		}

		return out
	case sourced:
		return text(typed)
	default:
		return node
	}
}

// decodeStrict decodes tree, already expanded, into a Fleet, rejecting
// unknown fields.
func decodeStrict(tree any) (*Fleet, error) {
	data, err := json.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to encode merged fleet: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var fleet Fleet

	err = decoder.Decode(&fleet)
	if err != nil {
		return nil, fmt.Errorf("invalid merged fleet: %w", err)
	}

	return &fleet, nil
}
