package auth

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates/login.html
var loginPageFS embed.FS

// parseLoginPage parses the /app landing page template. Called once by
// NewHandler and cached on Handler.loginPage.
func parseLoginPage() *template.Template {
	return template.Must(template.ParseFS(loginPageFS, "templates/login.html"))
}

// renderLoginPage writes the /app landing page: a "Login via SSO" button
// linking to /app/login. It never redirects on its own — the OIDC flow only
// starts once the user clicks through.
func (h *Handler) renderLoginPage(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Errors here mean the response is already partially written; there is
	// nothing more useful to do than let the client see a truncated body.
	_ = h.loginPage.Execute(writer, nil)
}
