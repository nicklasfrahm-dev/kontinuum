package fleet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// ErrUnsetEnv is Expand's sentinel for an ${env:NAME} whose variable
	// is unset or empty — never silently an empty string.
	ErrUnsetEnv = errors.New("environment variable is unset or empty")
	// ErrBareReference is Expand's sentinel for ${NAME} without a source
	// prefix, which would otherwise silently stay literal.
	ErrBareReference = errors.New("reference has no source")
	// ErrUnknownSource is Expand's sentinel for a reference with a source
	// other than env or file.
	ErrUnknownSource = errors.New("unknown reference source, must be env or file")
	// ErrUnterminatedReference is Expand's sentinel for a ${ without a
	// closing }.
	ErrUnterminatedReference = errors.New("unterminated reference")
	// ErrEmptyReference is Expand's sentinel for ${env:} or ${file:}.
	ErrEmptyReference = errors.New("reference names nothing")
	// ErrInlineSecret is Expand's sentinel for a secret field holding
	// anything other than exactly one reference, so a committed
	// kontinuum.yml can never carry a secret itself.
	ErrInlineSecret = errors.New("secret fields must be exactly one ${env:NAME} or ${file:path} reference")
)

// Sources resolves references — the environment and the filesystem in
// production, stubs in tests.
type Sources struct {
	// LookupEnv defaults to os.LookupEnv when nil.
	LookupEnv func(name string) (string, bool)
	// ReadFile defaults to os.ReadFile when nil.
	ReadFile func(path string) ([]byte, error)
}

// singleReferencePattern matches a value that is exactly one reference.
const singleReferencePattern = `^\$\{(env|file):[^}]+\}$`

// secretPaths are the merged document's secret fields, as dotted paths
// where "*" matches any one key.
func secretPaths() []string {
	return []string{
		"spec.storage.uri",
		"spec.dns.credential.*",
		"spec.acme.providers.*.externalAccountBinding.hmacKey",
	}
}

// Expand checks the secret-field rule on the merged document, then expands
// every reference in every string value — after YAML decoding, never on
// the raw text, so an expanded value containing newlines or ": " can't
// change the document's structure — and decodes the result into a Fleet.
// ${env:NAME} is the environment variable NAME, which must be set and
// non-empty; ${file:path} is the file's contents with one trailing newline
// stripped, path resolving relative to the file the value came from; $${
// is a literal ${. References may be embedded in a longer string.
func (d *Document) Expand(sources Sources) (*Fleet, error) {
	err := checkSecrets(d.tree, "")
	if err != nil {
		return nil, err
	}

	if sources.LookupEnv == nil {
		sources.LookupEnv = os.LookupEnv
	}

	if sources.ReadFile == nil {
		sources.ReadFile = os.ReadFile
	}

	var expandErr error

	tree := unsource(d.tree, func(value sourced) string {
		expanded, err := expandString(value, sources)
		if err != nil && expandErr == nil {
			expandErr = fmt.Errorf("%s: %w", value.file, err)
		}

		return expanded
	})

	if expandErr != nil {
		return nil, expandErr
	}

	return decodeStrict(tree)
}

// checkSecrets enforces the secret-field rule on node — see
// ErrInlineSecret.
func checkSecrets(node any, path string) error {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			err := checkSecrets(value, joinPath(path, key))
			if err != nil {
				return err
			}
		}
	case sourced:
		if isSecretPath(path) && !regexp.MustCompile(singleReferencePattern).MatchString(typed.text) {
			return fmt.Errorf("%s: %w: %s", typed.file, ErrInlineSecret, path)
		}
	}

	return nil
}

// isSecretPath reports whether path is one of secretPaths.
func isSecretPath(path string) bool {
	parts := strings.Split(path, ".")

	for _, pattern := range secretPaths() {
		patternParts := strings.Split(pattern, ".")
		if len(patternParts) != len(parts) {
			continue
		}

		matched := true

		for index, part := range patternParts {
			if part != "*" && part != parts[index] {
				matched = false

				break
			}
		}

		if matched {
			return true
		}
	}

	return false
}

// expandString expands every reference in value — see Expand.
func expandString(value sourced, sources Sources) (string, error) {
	text := value.text

	var out strings.Builder

	for {
		start := strings.Index(text, "$")
		if start < 0 {
			out.WriteString(text)

			return out.String(), nil
		}

		out.WriteString(text[:start])
		text = text[start:]

		var (
			written  string
			consumed int
		)

		switch {
		case strings.HasPrefix(text, "$${"):
			written, consumed = "${", len("$${")
		case strings.HasPrefix(text, "${"):
			end := strings.Index(text, "}")
			if end < 0 {
				return "", fmt.Errorf("%w: %q", ErrUnterminatedReference, value.text)
			}

			resolved, err := resolveReference(text[len("${"):end], value.file, sources)
			if err != nil {
				return "", err
			}

			written, consumed = resolved, end+1
		default:
			written, consumed = "$", 1
		}

		out.WriteString(written)

		text = text[consumed:]
	}
}

// resolveReference resolves one reference's body (between ${ and }).
func resolveReference(body, file string, sources Sources) (string, error) {
	source, target, hasSource := strings.Cut(body, ":")
	if !hasSource {
		return "", fmt.Errorf("%w: ${%s}, use ${env:%s} for an environment variable", ErrBareReference, body, body)
	}

	if target == "" {
		return "", fmt.Errorf("%w: ${%s}", ErrEmptyReference, body)
	}

	switch source {
	case "env":
		value, ok := sources.LookupEnv(target)
		if !ok || value == "" {
			return "", fmt.Errorf("%w: %s", ErrUnsetEnv, target)
		}

		return value, nil
	case "file":
		return readReferencedFile(target, file, sources)
	default:
		return "", fmt.Errorf("%w: ${%s}", ErrUnknownSource, body)
	}
}

// readReferencedFile reads a ${file:target} reference declared in file —
// target resolving relative to file's own directory — with one trailing
// newline stripped.
func readReferencedFile(target, file string, sources Sources) (string, error) {
	path := target
	if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(file), path)
	}

	data, err := sources.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read referenced file: %w", err)
	}

	content := string(data)
	if trimmed, found := strings.CutSuffix(content, "\r\n"); found {
		return trimmed, nil
	}

	return strings.TrimSuffix(content, "\n"), nil
}
