// Command vendor-web-assets downloads pinned copies of the third-party
// browser assets the UI depends on — Tailwind CSS's Play CDN build, HTMX,
// PrismJS, and the JetBrains Mono webfont — into pkg/ui/assets/vendor,
// where pkg/ui embeds them into the kontinuum binary via go:embed (see
// pkg/ui/assets.go). Run via `go generate ./...` (see the go:generate
// directive in pkg/ui/assets.go) whenever a version below is bumped, so
// kontinuum ships these assets itself and can run fully offline / air-gapped
// instead of depending on a CDN at runtime.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

// asset is one file to fetch and vendor. checksum pins the exact bytes
// expected from url, so a compromised or unexpectedly changed CDN response
// fails the generate step loudly instead of silently vendoring different
// content than what was reviewed. Bump both url and checksum together when
// upgrading a library.
type asset struct {
	url      string
	dest     string
	checksum string
}

// vendoredAssets lists the exact files this tool fetches. Kept as a
// function (rather than a package-level var) so the pinned versions live
// with the code that consumes them, not as mutable shared state.
func vendoredAssets() []asset {
	return []asset{
		{
			url:      "https://cdn.tailwindcss.com/3.4.17",
			dest:     "tailwindcss.js",
			checksum: "176e894661aa9cdc9a5cba6c720044cbbf7b8bd80d1c9a142a7c24b1b6c50d15",
		},
		{
			url:      "https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js",
			dest:     "htmx.min.js",
			checksum: "e209dda5c8235479f3166defc7750e1dbcd5a5c1808b7792fc2e6733768fb447",
		},
		{
			url:      "https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-core.min.js",
			dest:     "prism-core.min.js",
			checksum: "e2624d4f66cc5f171cd460896b106630f7666a1e638b42dd9ddefd0ca7758683",
		},
		{
			url:      "https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-yaml.min.js",
			dest:     "prism-yaml.min.js",
			checksum: "719c8e8b8c344dc9de510c729f65ba840b1502a0a8e7e25e2ad19ee715f65c02",
		},
		{
			url:      "https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-bash.min.js",
			dest:     "prism-bash.min.js",
			checksum: "6260814110e5182f2956e3bd257429548d9dbf2a9b66a63719b26cf9fac966a7",
		},
	}
}

// fontWeights are the JetBrains Mono weights templates/layout.html and
// pkg/auth/templates/login.html use (see their font-face declarations).
func fontWeights() []int {
	return []int{400, 500, 600, 700}
}

// fontSubset is one Unicode-range slice of the JetBrains Mono webfont.
// Google Fonts ships JetBrains Mono as a variable font: a single woff2 per
// subset covers every weight in fontWeights, so unlike the CDN assets
// above, one downloaded file backs several generated @font-face rules —
// see fontCSS.
type fontSubset struct {
	name         string
	url          string
	checksum     string
	unicodeRange string
}

// fontSubsets lists JetBrains Mono's Unicode-range subsets, mirroring
// exactly what fonts.googleapis.com/css2?family=JetBrains+Mono serves by
// default (fetched once with a browser user agent to resolve the woff2
// URLs and ranges below — see the asset checksum comment on vendoredAssets
// for why these are pinned rather than re-resolved on every generate).
func fontSubsets() []fontSubset {
	return []fontSubset{
		{
			name: "cyrillic-ext",
			url: "https://fonts.gstatic.com/s/jetbrainsmono/v24/" +
				"tDbv2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwBNntkaToggR7BYRbKPx3cwhsk.woff2",
			checksum:     "62213be8a78b42f1e29d1452d91e2f8b3e745572a9dd98d3941e39fa00b37d76",
			unicodeRange: "U+0460-052F, U+1C80-1C8A, U+20B4, U+2DE0-2DFF, U+A640-A69F, U+FE2E-FE2F",
		},
		{
			name: "cyrillic",
			url: "https://fonts.gstatic.com/s/jetbrainsmono/v24/" +
				"tDbv2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwBNntkaToggR7BYRbKPxTcwhsk.woff2",
			checksum:     "e17cfd15fb96909d64095015f958207063a0c07191da3512df7d560a781aebdf",
			unicodeRange: "U+0301, U+0400-045F, U+0490-0491, U+04B0-04B1, U+2116",
		},
		{
			name: "greek",
			url: "https://fonts.gstatic.com/s/jetbrainsmono/v24/" +
				"tDbv2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwBNntkaToggR7BYRbKPxPcwhsk.woff2",
			checksum:     "0a557721b1f8b36d3f3f84442689a71ca4a744300abcb46a1953f51bfc663b66",
			unicodeRange: "U+0370-0377, U+037A-037F, U+0384-038A, U+038C, U+038E-03A1, U+03A3-03FF",
		},
		{
			name: "vietnamese",
			url: "https://fonts.gstatic.com/s/jetbrainsmono/v24/" +
				"tDbv2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwBNntkaToggR7BYRbKPx_cwhsk.woff2",
			checksum: "c89b9cc0bc6262bd4f8d8494b6961601f3aefa829d08c2e3635f4d501d3a47c2",
			unicodeRange: "U+0102-0103, U+0110-0111, U+0128-0129, U+0168-0169, U+01A0-01A1, U+01AF-01B0, " +
				"U+0300-0301, U+0303-0304, U+0308-0309, U+0323, U+0329, U+1EA0-1EF9, U+20AB",
		},
		{
			name: "latin-ext",
			url: "https://fonts.gstatic.com/s/jetbrainsmono/v24/" +
				"tDbv2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwBNntkaToggR7BYRbKPx7cwhsk.woff2",
			checksum: "db5ff4db83e580426280e9337a58dc57d3a83784a1b03ad80914651594441d52",
			unicodeRange: "U+0100-02BA, U+02BD-02C5, U+02C7-02CC, U+02CE-02D7, U+02DD-02FF, U+0304, U+0308, " +
				"U+0329, U+1D00-1DBF, U+1E00-1E9F, U+1EF2-1EFF, U+2020, U+20A0-20AB, U+20AD-20C0, U+2113, " +
				"U+2C60-2C7F, U+A720-A7FF",
		},
		{
			name: "latin",
			url: "https://fonts.gstatic.com/s/jetbrainsmono/v24/" +
				"tDbv2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwBNntkaToggR7BYRbKPxDcwg.woff2",
			checksum: "83c005d49d8a6a50474c73a5a36ac0468076e9c4a29da7bdb14995d80560a5be",
			unicodeRange: "U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, " +
				"U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD",
		},
	}
}

// fontFile returns the local filename fontCSS references for subset — kept
// in one place so the download destination and the generated @font-face
// src always agree.
func fontFile(subset fontSubset) string {
	return "jetbrains-mono-" + subset.name + ".woff2"
}

// fontFace is one @font-face block's template data — see fontCSS.
// Exported fields are required here, not stylistic: text/template reads
// struct fields by reflection, which cannot see unexported fields even from
// within the same package.
type fontFace struct {
	Subset       string
	Weight       int
	File         string
	UnicodeRange string
}

// fontCSSTemplate mirrors the @font-face blocks fonts.googleapis.com would
// otherwise serve dynamically, pointing at the locally vendored files
// instead of fonts.gstatic.com. Written by hand (rather than rewriting a
// fetched copy of Google's CSS) so generation has no runtime dependency on
// parsing that response — the structure (one block per weight × subset) is
// simple and stable enough to own directly.
const fontCSSTemplate = `{{range .}}/* {{.Subset}} */
@font-face {
  font-family: 'JetBrains Mono';
  font-style: normal;
  font-weight: {{.Weight}};
  font-display: swap;
  src: url({{.File}}) format('woff2');
  unicode-range: {{.UnicodeRange}};
}
{{end}}`

// fontCSS renders fontCSSTemplate over every (weight, subset) pair.
func fontCSS() (string, error) {
	faces := make([]fontFace, 0, len(fontWeights())*len(fontSubsets()))

	for _, weight := range fontWeights() {
		for _, subset := range fontSubsets() {
			faces = append(faces, fontFace{
				Subset:       subset.name,
				Weight:       weight,
				File:         fontFile(subset),
				UnicodeRange: subset.unicodeRange,
			})
		}
	}

	tmpl, err := template.New("font-css").Parse(fontCSSTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse font CSS template: %w", err)
	}

	var builder strings.Builder

	err = tmpl.Execute(&builder, faces)
	if err != nil {
		return "", fmt.Errorf("failed to render font CSS: %w", err)
	}

	return builder.String(), nil
}

const (
	httpTimeout = 30 * time.Second
	dirPerm     = 0o750
	filePerm    = 0o600
)

// errUnexpectedStatus and errChecksumMismatch are the static, wrapped bases
// for this tool's two failure modes — see fetch.
var (
	errUnexpectedStatus = errors.New("unexpected response status")
	errChecksumMismatch = errors.New("checksum mismatch")
)

func main() {
	// Relative to pkg/ui — go:generate runs commands with the working
	// directory set to the directory containing the directive (pkg/ui), and
	// this default lets the tool also be run standalone from repo root via
	// `go run ./cmd/vendor-web-assets -out pkg/ui/assets/vendor`.
	out := flag.String("out", "assets/vendor", "output directory for vendored assets")

	flag.Parse()

	err := run(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "vendor-web-assets:", err)
		os.Exit(1)
	}
}

func run(out string) error {
	err := os.MkdirAll(out, dirPerm)
	if err != nil {
		return fmt.Errorf("failed to create output directory %q: %w", out, err)
	}

	client := &http.Client{Timeout: httpTimeout}

	for _, entry := range vendoredAssets() {
		err := fetchAndLog(client, out, entry)
		if err != nil {
			return err
		}
	}

	for _, subset := range fontSubsets() {
		entry := asset{url: subset.url, dest: fontFile(subset), checksum: subset.checksum}

		err := fetchAndLog(client, out, entry)
		if err != nil {
			return err
		}
	}

	const fontCSSDest = "jetbrains-mono.css"

	css, err := fontCSS()
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(out, fontCSSDest), []byte(css), filePerm)
	if err != nil {
		return fmt.Errorf("failed to write %s: %w", fontCSSDest, err)
	}

	_, err = fmt.Fprintln(os.Stdout, "vendored", fontCSSDest)
	if err != nil {
		return fmt.Errorf("failed to write progress output: %w", err)
	}

	return nil
}

// fetchAndLog wraps fetch with the download's own error context and the
// tool's "vendored <file>" progress line, shared by both the CDN assets and
// the font subsets so run doesn't repeat it for each.
func fetchAndLog(client *http.Client, out string, entry asset) error {
	err := fetch(client, out, entry)
	if err != nil {
		return fmt.Errorf("%s: %w", entry.dest, err)
	}

	_, err = fmt.Fprintln(os.Stdout, "vendored", entry.dest)
	if err != nil {
		return fmt.Errorf("failed to write progress output: %w", err)
	}

	return nil
}

func fetch(client *http.Client, out string, entry asset) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, entry.url, nil)
	if err != nil {
		return fmt.Errorf("failed to build request for %s: %w", entry.url, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download %s: %w", entry.url, err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: got %s for %s", errUnexpectedStatus, resp.Status, entry.url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body from %s: %w", entry.url, err)
	}

	sum := sha256.Sum256(body)

	got := hex.EncodeToString(sum[:])
	if got != entry.checksum {
		return fmt.Errorf("%w for %s: got %s, want %s", errChecksumMismatch, entry.url, got, entry.checksum)
	}

	err = os.WriteFile(filepath.Join(out, entry.dest), body, filePerm)
	if err != nil {
		return fmt.Errorf("failed to write %s: %w", entry.dest, err)
	}

	return nil
}
