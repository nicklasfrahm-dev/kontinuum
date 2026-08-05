# Authentication

Kontinuum refuses to start unless authentication is configured deliberately: set `KONTINUUM_OIDC_ISSUER_URL` to require OIDC, or explicitly set `KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true` to opt into **anonymous authentication and always-allow authorization** — there is no login and no access control in that mode, so put a TLS-terminating, authenticating proxy in front before exposing it outside a trusted network. The two are mutually exclusive; setting both, or neither, fails startup with a descriptive error. See [Reference](reference.md#authentication-oidc) for the full env var table.

| `KONTINUUM_INSECURE_ALLOW_ANONYMOUS` | Issuer URL set? | Startup behavior |
| --- | --- | --- |
| `false` | Yes | Starts normally, no special log output. |
| `false` | No | Fails to start with a descriptive error. |
| `true` | No | Starts with a warning that anonymous access is enabled. |
| `true` | Yes | Fails to start with a descriptive error (mutually exclusive). |

## Enabling OIDC

Setting `KONTINUUM_OIDC_ISSUER_URL` (e.g. to a [Dex](https://dexidp.io/) issuer) turns on three things at once:

- **API bearer-token validation** — requests to the Kubernetes-style API must carry a valid `Authorization: Bearer <id_token>` issued by the configured issuer for `KONTINUUM_OIDC_CLIENT_ID`.
- **Deny-by-default authorization** — only `system:masters`, authenticated service accounts, and the groups listed in `KONTINUUM_OIDC_ADMIN_GROUPS` get access; every other group gets nothing (`libkapi.WithAdminAuthorizer`). **`KONTINUUM_OIDC_ADMIN_GROUPS` is required once OIDC is enabled** — the server refuses to start with OIDC on and no admin groups configured, since that would lock everyone out.
- **The `/app` UI's PKCE login flow** — see below.

## The PKCE login flow (`pkg/auth`)

Since the `/app` UI's OAuth client is public (no client secret), the browser exchanges an authorization code for an ID token using [PKCE](https://www.rfc-editor.org/rfc/rfc7636) (RFC 7636), and the resulting ID token is stored in an HttpOnly session cookie:

1. `/app` never auto-redirects into the login flow — it shows a "Login via SSO" button (linking to `/app/login`) unless a valid session is already present, in which case it forwards straight to `/app/home`.
2. `/app/login` starts the PKCE exchange against the configured issuer and redirects to it.
3. On success, the issuer redirects back to `KONTINUUM_OIDC_REDIRECT_URL`, which must exactly match one of the redirect URIs registered with the issuer for the client.
4. The resulting ID token is verified and stored in an HttpOnly session cookie.

Everything under `/app/*` other than `/app`, `/app/login`, and `/app/logout` requires a valid session; `/app/logout` clears it.

## Admin group role bindings

The UI's **IAM** page visualizes which OIDC groups (from `KONTINUUM_OIDC_ADMIN_GROUPS`) are bound to the `system:masters`-equivalent role, so operators can see the effective admin set at a glance without cross-referencing environment variables.

See [Local setup](local-setup.md) for configuring these variables in the hot-reload dev environment.
