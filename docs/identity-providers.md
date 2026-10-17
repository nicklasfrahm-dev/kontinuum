# Identity providers

Besides the single issuer configured through `KONTINUUM_OIDC_ISSUER_URL` (see [Authentication](authentication.md)), kontinuum can trust additional OIDC issuers configured at runtime through two custom resources. Each one describes how a user is discovered from a JWT using [CEL](https://kubernetes.io/docs/reference/using-api/cel/), modelled on one `jwt[]` entry of Kubernetes' own [`AuthenticationConfiguration`](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#using-authentication-configuration):

| Kind | Scope | Identities it produces |
| --- | --- | --- |
| `ClusterIdentityProvider` | Cluster | Used exactly as mapped, so they can be granted cluster-wide access through `ClusterRoleBinding`s or the configured admin groups. Only cluster administrators should be allowed to create one. |
| `IdentityProvider` | Namespace (tenant) | Always prefixed with `tenant:<namespace>:` — both the username and every group. |

Both are only consulted when OIDC is enabled: with `KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true` every request is already allowed, so there's nothing for an identity to change. The CRDs and their controller still run in that mode, so providers can be prepared before OIDC is turned on.

## Example

```yaml
apiVersion: kontinuum.sh/v1alpha2
kind: ClusterIdentityProvider
metadata:
  name: corporate-sso
spec:
  issuer:
    url: https://login.example.com
    audiences:
      - kontinuum
  claimValidationRules:
    # Required by Kubernetes' validation whenever claims.email is mapped.
    - expression: claims.?email_verified.orValue(true) == true
      message: email must be verified
  claimMappings:
    username:
      expression: claims.email
    groups:
      expression: "has(claims.groups) ? claims.groups : []"
    uid:
      expression: claims.sub
  userValidationRules:
    - expression: "!user.username.startsWith('system:')"
      message: system identities cannot be mapped
```

An `IdentityProvider` has the same `spec` and lives in a tenant namespace. With the spec above in namespace `acme`, a token for `alice@example.com` in group `engineering` authenticates as user `tenant:acme:alice@example.com` in group `tenant:acme:engineering`, which a `RoleBinding` in `acme` can then grant access to:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: engineering-edit
  namespace: acme
subjects:
  - kind: Group
    name: tenant:acme:engineering
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: editor
```

The `/app` UI manages both kinds from the **Identity providers** tab of the **Tenant** and **Global** IAM pages, validating every CEL expression before creating the object.

## Spec

The spec mirrors `AuthenticationConfiguration`'s `jwt[]` entry and is validated by the exact same rules kube-apiserver applies, including CEL compilation:

| Field | Purpose |
| --- | --- |
| `issuer.url` | The `https` issuer URL; must equal the token's `iss` claim. |
| `issuer.discoveryUrl` | Optional override for where the discovery document is fetched from (Kubernetes spells this `discoveryURL`). The document's own `issuer` must still equal `issuer.url`. |
| `issuer.audiences` | Accepted `aud` values. More than one requires `audienceMatchPolicy: MatchAny`. |
| `issuer.certificateAuthority` | Optional PEM bundle for verifying the discovery and JWKS endpoints' TLS certificates. |
| `claimValidationRules[]` | `claim`/`requiredValue` pairs or CEL `expression`s over `claims` that every token must satisfy. |
| `claimMappings.username` | Required. `claim` + `prefix`, or a CEL `expression` returning a string. |
| `claimMappings.groups` | `claim` + `prefix`, or a CEL `expression` returning a string or list of strings. |
| `claimMappings.uid` | `claim` or a CEL `expression`. |
| `claimMappings.extra[]` | `key` plus a CEL `valueExpression`. |
| `userValidationRules[]` | CEL `expression`s over the mapped `user` that must all be true. |

Every identity also carries the extra attribute `kontinuum.sh/identity-provider`, naming the provider that authenticated it (`<name>` or `<namespace>/<name>`), so audit logs show where it came from.

## Status

| Field | Meaning |
| --- | --- |
| `conditions[Ready]` | `True` (reason `Active`) once the provider authenticates requests and its signing keys are cached. Otherwise `False` with reason `InvalidConfiguration`, `Conflict`, or `KeyFetchFailed` and a human-readable message. |
| `signingKeys` | Number of signing keys currently cached for the issuer. |
| `lastKeyRefreshTime` | When the keys were last fetched successfully. |
| `usernamePrefix`, `groupsPrefix` | The prefix kontinuum forces onto mapped identities — empty for a `ClusterIdentityProvider`. |

## Runtime behavior

**Hot-swappable.** Every kontinuum process runs the `identityprovider` controller and keeps its own in-memory authenticator, plugged into the API server's authentication chain right after the static OIDC issuer. Creating, changing, or deleting a provider recompiles only what changed and atomically swaps the active set, so it takes effect on the next request with no restart. Status is written by whichever process holds the global zone lease.

**Signing key cache.** Discovery documents and JWKS are cached per issuer with stale-while-revalidate semantics. Reconciling a new provider fetches its keys right away, so the first token doesn't pay for discovery. Cached keys are served without revalidating for five minutes, then for up to 24 hours more while a background refresh runs, so a short issuer outage doesn't lock anyone out. Concurrent fetches for the same issuer, whether from blocked requests or background refreshes, are collapsed into one through `golang.org/x/sync/singleflight`. A token signed by an unknown key ID triggers one refresh to pick up a key rotation, rate-limited to once every ten seconds per issuer.

**Conflicts.** Two authenticators accepting the same issuer URL and audience would make it ambiguous who a token belongs to. Kontinuum resolves overlaps deterministically and marks every loser `Ready=False` with reason `Conflict`:

1. The static `KONTINUUM_OIDC_ISSUER_URL`/`KONTINUUM_OIDC_CLIENT_ID` pair always wins.
2. A `ClusterIdentityProvider` beats any `IdentityProvider`.
3. Within a kind, the oldest object wins.

So a tenant can never shadow an existing provider — or another tenant's — by creating one with the same issuer and audience.
