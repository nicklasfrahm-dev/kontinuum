package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

// vendorURLPrefix is where vendored third-party assets are served from —
// referenced directly by templates/layout.html and
// pkg/auth/templates/login.html.
const vendorURLPrefix = "/static/vendor/"

//go:generate go run ../../cmd/vendor-web-assets -out assets/vendor
//go:embed assets/vendor
var vendorFS embed.FS

// vendorHandler serves the vendored copies of Tailwind CSS, HTMX, PrismJS,
// and the JetBrains Mono webfont from vendorFS, so the running binary has no
// runtime dependency on any CDN — the container image that ships it is what
// needs to work air-gapped, not this source tree. assets/vendor is gitignored
// and populated by `go generate ./...` (wired into `make generate`, itself a
// dependency of build/test/vet/lint — see Makefile and Containerfile) rather
// than committed: see cmd/vendor-web-assets, which downloads and pins these
// files, for the versions in use.
func vendorHandler() http.Handler {
	sub, err := fs.Sub(vendorFS, "assets/vendor")
	if err != nil {
		// vendorFS is a compile-time embed of a directory literal above, so
		// a broken subtree here is a build-time programming error, not a
		// runtime condition to recover from.
		panic(err)
	}

	return http.StripPrefix(vendorURLPrefix, http.FileServerFS(sub))
}
