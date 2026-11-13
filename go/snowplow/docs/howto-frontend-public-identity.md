# Frontend integration: pages served without a token (the public identity, #1334)

This guide is for the frontend session building a page that must load with no
sign-in, such as a public "request access" page. When the install opts in,
snowplow serves a request that carries no `Authorization` header as one
dedicated identity, the **public identity**. What that identity may read is
decided by ordinary Kubernetes RBAC. Snowplow has no allowlist of pages, paths
or widget kinds.

Everything below comes from the code: the middleware
`internal/handlers/middleware/public_identity.go` and `userconfig.go`, the
passthrough in `internal/handlers/call.go`, the route wiring in `main.go`, and
the chart value `publicIdentity.enabled` (`helm/snowplow`). The tests named in
each section are the proof.

## 1. What a token-less request gets

Send the request with **no `Authorization` header at all**.

| Install | Answer |
|---|---|
| `publicIdentity.enabled: false` (the default) | `401`, cause `TokenMissing`, exactly as before (`howto-frontend-credential-401-causes.md`) |
| `publicIdentity.enabled: true` | the request is served as the public identity: `200` for what RBAC grants it, `403` for anything else |
| enabled, but snowplow may not impersonate the identity (the grant is missing, for example mid-roll) | `503` |

A `503` on a token-less request means **the page is temporarily unavailable;
try again**. It is never a sign-in problem: don't send the visitor to log in.
Show a page-level "temporarily unavailable" state and retry. Snowplow refused
the request before dispatching anything, so a retry is safe.

The public identity requires the cache. The chart refuses
`publicIdentity.enabled: true` unless `env.CACHE_ENABLED` is `"true"`, and a
binary started with the cache off answers `503`.

Tests: `TestIssue1334_PublicIdentityOff_TokenlessIs401`,
`TestIssue1334_PublicIdentityUnavailableIs503NeverTheServiceAccount`.

## 2. Never send a stale or empty token to mean "public"

The public identity applies only when the header is **absent**. Any
`Authorization` header keeps its 401: an empty value, `Basic ...`, `Bearer `
with nothing after it, a malformed token, a token signed by another issuer, an
expired token, a token signed by a retired key, or a valid token whose session
was revoked. A broken sign-in never silently turns into the public view.

So on a public page, **drop the header**; don't send an empty or old one. On a
signed-in page, a 401 still means "log in again".

Test: `TestIssue1334_BadCredentialNeverFallsBackToPublic`.

## 3. Nothing on the request changes the identity

A token-less request is the public identity, whatever else it carries.
`Impersonate-*` headers, `X-Remote-User` and similar proxy headers, a
`krateo-session` cookie (even one holding a valid token), query parameters and
`extras` keys such as `username` aren't read. If the page needs the visitor's
own identity, the visitor must sign in.

Test: `TestIssue1334_TokenlessRequestCannotNameAnotherIdentity`.

## 4. Which routes accept it

Every route that authorizes its work by the caller's identity: `GET /call`,
`POST /call/read`, `GET`/`POST /export`, the `/call` write verbs and
`/call/dry-run`, `GET /list` and `POST /jq`. `GET /rbac` does not: its
enumeration runs under snowplow's own ServiceAccount, so it still needs a
token. `/refreshes` (live refresh) uses its own token-or-cookie check and is
not served to the public identity.

Writes through `/call` are sent to the apiserver as the public identity. They
succeed only where RBAC grants them, and each one is audited like any write.
The request-access form described in #1334 posts to identity-lifecycle, not to
snowplow.

Test: `TestIssue1334_PublicPassthrough_ImpersonatedAndDecidedByRBAC`.

## 5. Granting a page (for the page's owner, not the frontend)

The identity is user `krateo:public` with the single group
`system:unauthenticated`. Bind a Role to the **user** `krateo:public`, with
`get` and `resourceNames` on the page's widgets and RESTActions (and on what
those read), in the page's namespace (by convention `krateo-public`). Never bind
the group: `system:unauthenticated` is also what an anonymous caller of the
apiserver itself is.

The namespace convention is shared with the frontend. Its short URL
`/public/:slug` resolves the page in the namespace named by a frontend config
value whose chart default is `krateo-public`. Snowplow itself doesn't read that
value: whatever the namespace, RBAC decides.

Grants to `system:authenticated` don't reach the public identity.

## 6. Caching

The public identity is one cache identity: every visitor shares its cells,
which are keyed like any other identity's and never shared with a signed-in
user's.

Test: `TestIssue1334_CacheKey_OnePublicIdentityFoldingItsGroupSet`.
