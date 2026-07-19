package auth

import "context"

// tokenContextKey is the context key Protect uses to carry the signed-in
// user's raw ID token to downstream handlers.
type tokenContextKey struct{}

// WithToken returns a copy of ctx carrying rawIDToken. Handlers downstream
// of Protect use TokenFromContext to act as the signed-in user on further
// API calls — e.g. the UI forwards it so its own namespace listing runs as
// the browser's identity instead of a separate privileged client.
func WithToken(ctx context.Context, rawIDToken string) context.Context {
	return context.WithValue(ctx, tokenContextKey{}, rawIDToken)
}

// TokenFromContext returns the ID token stashed by WithToken, or "" if none
// is present.
func TokenFromContext(ctx context.Context) string {
	token, _ := ctx.Value(tokenContextKey{}).(string)

	return token
}
