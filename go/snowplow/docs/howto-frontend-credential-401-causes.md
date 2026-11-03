# Frontend integration: every 401 snowplow refuses before dispatch says why (#1029, #1076)

This guide is for a frontend session, and for any other client of snowplow
(oasgen's RDC, scripts). It answers two questions about a 401 from snowplow:

1. **Was anything dispatched?** Did snowplow refuse the request before sending
   anything upstream, or did the 401 come back from upstream, possibly after a
   write had already happened?
2. **What should the portal do?** Log in again, step up again, or neither.

The rule is short:

- **A 401 whose `details.causes[0].reason` is one of the five in section 2**
  is a snowplow refusal **before dispatch**. Nothing was sent upstream: no
  apiserver call, no RESTAction stage, no write.
- **Any other 401** (no `details`, or a reason outside the five) is one
  snowplow does **not** vouch for. It may have been relayed from upstream
  after the request was dispatched, and a write may already have happened. Do
  **not** drop the token and replay the request on it.

Everything below comes from the handler code:

- `internal/handlers/middleware/userconfig.go` (`UserConfig`: token check,
  Secret selection, the NotFound branch);
- `internal/handlers/middleware/refreshauth.go` (`RefreshAuth`: the
  header-or-cookie token check);
- `internal/handlers/middleware/credential_status.go` (the closed set and the
  Status it writes);
- `internal/credsecret/credsecret.go` (which Secret a token reads, #994);
- `internal/handlers/call.go` and `internal/handlers/dispatchers/restactions.go`
  (the two relays of an upstream 401).

The #1029 contract was agreed on that issue: authn emits its causes in
`details.causes[0].reason`, the RFC 9470 header carries no `acr_values`, the
body is the contract and the header is optional. #1076 extends the same shape
to every pre-dispatch 401.

The SPA references are to `frontend-draganddrop/frontend` main as of
`9441551`, which includes frontend #562: `ui/src/utils/authFailureCause.ts`
and `ui/src/hooks/useCatchError.tsx`.

## 1. Where the pre-dispatch 401s come from

Two middlewares run before any handler. Each one either refuses the request
with a cause, or lets it through. Nothing upstream is contacted before they
let it through.

**`middleware.UserConfig`** reads a bearer token from the `Authorization`
header, validates it, then reads the caller's own credential Secret from
AUTHN_NAMESPACE. It guards:

- `GET /call` and `POST /call/read`;
- the `/call` write routes (`POST`, `PUT`, `PATCH`, `DELETE /call`, and
  `/call/dry-run`);
- `GET /list`;
- `GET /export` and `POST /export`;
- `GET /rbac`;
- `POST /jq`.

**`middleware.RefreshAuth`** reads the token from the `Authorization: Bearer`
header or, if there is none, from the session cookie (`REFRESH_SESSION_COOKIE`,
default `krateo-session`). It validates the token and reads no Secret. It
guards:

- `GET` and `POST /refreshes` (SSE);
- `POST /telemetry/v1/traces` (browser RUM);
- the `/debug/*` surface.

Which credential Secret `UserConfig` reads depends only on the **validated
JWT's `groups`**:

| token | Secret read |
|---|---|
| `groups` contains `krateo:loa:2` (a step-up login, authn `POST /oidc/stepup`) | `<user>-clientconfig-stepup` |
| any other token | `<user>-clientconfig` |

If that Secret is not in snowplow's informer, snowplow re-reads it from the
apiserver. If the apiserver answers **NotFound**, the credential is missing.

- **Step-up case.** Two facts here come from authn, and snowplow cannot verify
  them:
  - Per authn, the step-up credential lives about 15 minutes, so the usual
    reason is that it expired and authn removed it.
  - Per authn, the step-up JWT expires no later than the credential (its `exp`
    is clamped to the certificate's NotAfter). So an expired step-up JWT is
    normally `TokenExpired`, not `StepUpRequired`.
- **Session case.** The session Secret was deleted, for example by an admin or
  by a logout on another device.

## 2. The closed set

Snowplow emits exactly these five reasons, and every one of them means
**nothing was dispatched**:

| reason | emitted when | middleware | `WWW-Authenticate` | portal outcome |
|---|---|---|---|---|
| `TokenMissing` | no bearer token: no `Authorization` header, one whose scheme is not `Bearer`, or `Bearer ` with an empty token (and, on `RefreshAuth` routes, no session cookie either) | both | `Bearer` (no error code) | **log in** |
| `TokenInvalid` | the token failed validation: malformed, bad signature, wrong algorithm | both | `Bearer error="invalid_token"` | **log in again** |
| `TokenExpired` | the token is correctly signed but expired | both | `Bearer error="invalid_token"` | **log in again** (the session-resume flow) |
| `SessionCredentialMissing` | a valid token without `krateo:loa:2`; its session credential Secret is missing | `UserConfig` | `Bearer error="invalid_token"` | **log in again** |
| `StepUpRequired` | a valid token with `krateo:loa:2`; its step-up credential Secret is missing | `UserConfig` | `Bearer error="insufficient_user_authentication"` | **prompt to step up** |

`TokenExpired` is only reported once the signature has been verified. An
expired token signed with the wrong key is `TokenInvalid`.

Each example below is byte-identical to the wire (a snowplow test checks
it). `alice` stands for the caller's username.

### 2a. TokenMissing

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"missing authorization header","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"TokenMissing","message":"no bearer token was presented; log in"}]}}
```

The challenge is a bare `Bearer`: RFC 6750 §3.1 says a request that carried
no authentication information gets no error code.

The `message` field differs by case, and is not a contract:

| case | `UserConfig` routes | `RefreshAuth` routes |
|---|---|---|
| no header | `missing authorization header` | `missing credentials: no bearer header or session cookie` |
| a non-Bearer scheme | `invalid authorization header format` | `missing credentials: no bearer header or session cookie` |
| `Bearer ` with an empty token | `token is invalid` (unchanged from before) | `missing credentials: no bearer header or session cookie` |

### 2b. TokenInvalid

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer error="invalid_token"

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"token is invalid","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"TokenInvalid","message":"the bearer token is not valid; log in again"}]}}
```

### 2c. TokenExpired

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer error="invalid_token"

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"token is expired","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"TokenExpired","message":"the bearer token has expired; log in again"}]}}
```

### 2d. SessionCredentialMissing

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer error="invalid_token"

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"secrets \"alice-clientconfig\" not found","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"SessionCredentialMissing","message":"the session credential is missing; log in again"}]}}
```

Before #1076 this case sent no `WWW-Authenticate` header. It now sends
`Bearer error="invalid_token"`. The body is unchanged.

### 2e. StepUpRequired

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer error="insufficient_user_authentication"

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"secrets \"alice-clientconfig-stepup\" not found","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"StepUpRequired","message":"the step-up credential is missing or expired; step up again"}]}}
```

### 2f. What is guaranteed

- **The body is the contract.** Read `details.causes[0].reason`.
- **The set is closed.** Snowplow writes no other reason on a 401. A future
  reason would be a contract change, announced like this one.
- **The cause is chosen by snowplow, never copied.** It comes from what the
  middleware itself established: whether a token was presented, the token
  validator's typed error, and the validated token's groups (the same input
  that chose the Secret). It is never taken from the request body or from an
  upstream answer. A token without `krateo:loa:2` can never receive
  `StepUpRequired`.
- **Existing readers are unaffected.** `kind`, `apiVersion`, `status`,
  `message`, `reason:"Unauthorized"` and `code:401` are byte-identical to the
  plumbing Status snowplow sent before, in the same order. `details` is
  appended after them. `error.data.message` in `useCatchError` reads exactly
  what it read before.
- **`causes[0].message` is for humans.** It is a fixed English sentence with no
  identity in it. Do not branch on it.
- **`message` is unchanged.** For the credential causes it still names the
  caller's own Secret (as before, and only ever sent to that caller). Do not
  parse it.
- **The header is optional.** A bare `Bearer` (no token presented) and
  `Bearer error="invalid_token"` (a token presented and refused) are the RFC
  6750 §3.1 challenges. `Bearer error="insufficient_user_authentication"` is the RFC 9470
  step-up challenge, with **no `acr_values`**: `krateo:loa:2` is authn's group,
  not an IdP ACR value. Portal behaviour must never depend on the header. Most
  401s reach `useCatchError`, which has no headers. Only `useWidgetQuery.ts`
  and `sseClient.ts` hold the `Response`.
- **The header is readable same-origin only.** `WWW-Authenticate` is
  deliberately not in snowplow's CORS `ExposedHeaders` (`snowplowCORSOptions`
  in `main.go`). The portal is served same-origin through the gateway, where
  the header is readable. A cross-origin caller gets `null` from
  `res.headers.get('WWW-Authenticate')`. That is fine, because nothing may
  depend on the header; the body carries the cause either way.

## 3. A 401 without a snowplow cause: snowplow does not vouch that nothing was dispatched

A 401 with no `details`, or with a reason outside the five, is one snowplow
does not vouch for. It may be an upstream 401 snowplow relayed, a route
mounted without its auth middleware (no route is, today), or a hop in front
of snowplow.

Snowplow relays an upstream 401 in four places. Each one writes plumbing's
Status, which has no `details` field. So a relayed 401 **never** carries a
cause or a `WWW-Authenticate` header, even when the upstream body or headers
carry one. Snowplow tests send a forged cause and challenge from upstream on
the first two paths and check that both are dropped. The relays are:

- **A raw `/call` passthrough** (`internal/handlers/call.go`). The apiserver
  answered 401 to the call snowplow sent with the caller's credential. On a
  write, the call was sent and audited before the 401 came back.
- **A RESTAction stage** (`internal/handlers/dispatchers/restactions.go`,
  #709). A stage without `continueOnError` got a 401 from upstream, and the
  filter needs that stage's value. The stage's 401 then becomes the `/call`'s
  own 401. Earlier stages **have already run**, including any write stage (a
  `POST` stage before it, for example).
- **A widget dependency**
  (`internal/handlers/dispatchers/widgets_resolve_error.go`;
  `docs/howto-frontend-widget-dependency-status.md`). It answers with the
  dependency's 401 and no cause.
- **An export** (`passThroughCallFailure` in `internal/handlers/export.go`).
  It passes through the inner `/call` lane's failure, and that lane runs
  without the auth middleware, so its 401 can only be one of the relays
  above.

What this means for a client:

- **Do not drop the token and replay** the request because of a 401 without a
  snowplow cause. On a write, a replay can send the write twice.
- The portal may keep its current handling of such a 401 (the session-resume
  flow). That flow re-authenticates; it must not re-send the failed write by
  itself.

## 4. Edge cases and fallback

- **A 503** means the JWT signing key could not be resolved: authn's JWKS is
  unreachable, or the token names a key id the key set does not have. It
  carries no cause and no `WWW-Authenticate` header, and it is retryable.
  Nothing was dispatched. (An unknown key id cannot yet be told apart from an
  unreachable JWKS; that needs a distinct error in
  krateo-platformops/plumbing first.)
- **A 500 with no `details`** from the credential re-read: the apiserver
  answered 403 or 5xx, the snowplow pod cannot build its in-cluster config, or
  the Secret is malformed. Snowplow does not know the credential is missing,
  so it invents no cause. Treat it as today's 500.
- **A credential deleted after the middleware read it.** The middleware reads
  the credential once, before the handler runs. If the Secret disappears while
  the request is already being resolved (for example, the step-up credential
  expires mid-request), a stage that dials the caller's own credential gets a
  401 from upstream. That 401 is **relayed**, so it carries no cause
  (section 3):
  - if the stage has `continueOnError`, or the filter does not need it, the
    failure shows inside the response: in the RESTAction's stage outcomes, or
    as a failed widget dependency;
  - otherwise it **becomes the `/call`'s own 401**, without a cause, and
    earlier stages have already run.

  The next request gets the pre-dispatch 401 with its cause.
- **An unknown `causes[0].reason`** (a future value, `""`, `null`, or a
  reason from something other than snowplow): the portal gives it the plain
  401 handling (sign in again). It is **never** a reason to replay a request
  automatically: snowplow does not vouch that nothing was dispatched (section
  3). Never prompt for step-up on an unknown reason.
- **A burst of concurrent 401s** from one expired credential: every in-flight
  request gets the same cause. Raise one prompt, the same way
  `raiseSessionExpired` already de-duplicates.

## 5. One parser for snowplow and authn

The authn reasons are authn's. They come from authn's own endpoints, never
from snowplow. They are listed here only so that one parser covers both
components. Keep them distinct; never collapse them into one.

| reason | emitted by | HTTP | portal outcome |
|---|---|---|---|
| `TokenMissing` | **snowplow** | 401 | log in |
| `TokenInvalid` | **snowplow** | 401 | log in again |
| `TokenExpired` | **snowplow** | 401 | log in again (session resume) |
| `SessionCredentialMissing` | **snowplow** | 401 | log in again (session resume) |
| `StepUpRequired` | **snowplow** | 401 | **prompt to step up** |
| `InsufficientAssurance` | **authn** (`POST /oidc/stepup`) | 403 | prompt to step up |
| `StaleAuthentication` | **authn** (`POST /oidc/stepup`) | 403 | **re-authenticate** |
| `StepUpNotEnabled` | **authn** | 403 | **configuration fault**: tell the admin; never prompt |
| `StepUpMisconfigured` | **authn** | 500 | configuration fault: tell the admin; never prompt |
| `IssuanceFailed` | **authn** (its login endpoints) | 500 | **server fault**: the caller authenticated, but authn could not issue the credential or sign the token; retrying may work |

## 6. Code example (TypeScript)

### 6a. The portal: `ui/src/utils/authFailureCause.ts`

Frontend main already classifies a 401 by `details.causes[0].reason`
(frontend #562):

- `CatchError.data.details.causes` carries the body's cause into
  `useCatchError`;
- `authFailureCauseOf(body, { origin, tokenKind })` maps the reason to an
  `AuthFailureAction` (`reauthenticate`, `step-up`, `configuration-fault`,
  `server-fault`) through `BY_REASON`;
- `useCatchError` calls `raiseSessionExpired(cause)`.

It already knows `SessionCredentialMissing`, `StepUpRequired` and authn's
reasons, including `IssuanceFailed`. An unknown or absent reason falls back
to `reauthenticate`.

The one change left for the portal is the three token reasons. Today they
fall to that default, which is the right action for a session token.

They need a rule of their own, not three more `BY_REASON` rows. A token
reason describes **the token the request carried**, and an expired
**step-up** JWT now answers `TokenExpired`: it is rejected at token
validation, before any Secret is chosen. The `authFailureCauseOf` comment
still expects that case to arrive with no reason at all. `authFailureCauseOf`
lets a recognised reason win over `tokenKind`. So a `BY_REASON` row
`TokenExpired → reauthenticate` would show a full sign-in for a session that
is still valid. Let `tokenKind` decide instead:

```ts
// ui/src/utils/authFailureCause.ts

export const AUTH_FAILURE_REASONS = {
  // ...the existing entries, unchanged...
  /** snowplow: no bearer token was presented (#1076). */
  tokenMissing: 'TokenMissing',
  /** snowplow: the bearer token failed validation (#1076). */
  tokenInvalid: 'TokenInvalid',
  /** snowplow: the bearer token is correctly signed but expired (#1076). */
  tokenExpired: 'TokenExpired',
} as const

/** Reasons about the token the request carried: the caller's tokenKind decides the action. */
const TOKEN_REASONS: ReadonlySet<string> = new Set([
  AUTH_FAILURE_REASONS.tokenMissing,
  AUTH_FAILURE_REASONS.tokenInvalid,
  AUTH_FAILURE_REASONS.tokenExpired,
])

// in authFailureCauseOf, right after `reason` is computed:
if (reason !== null && TOKEN_REASONS.has(reason)) {
  return { action: tokenKind === 'step-up' ? 'step-up' : 'reauthenticate', reason }
}
```

### 6b. Automatic replay: oasgen's RDC, or any client that retries

A client that re-sends a request by itself must gate the replay on
**membership** in the closed set, never on "a cause is present". Only the
four reasons a fresh login fixes may trigger "drop the token, log in, replay":

```ts
// The five snowplow pre-dispatch causes (#1029, #1076): nothing was dispatched.
export const SNOWPLOW_PRE_DISPATCH_CAUSES = [
  'TokenMissing',
  'TokenInvalid',
  'TokenExpired',
  'SessionCredentialMissing',
  'StepUpRequired',
] as const

// The four a fresh login fixes. StepUpRequired is NOT one of them: a login does
// not restore a step-up credential, so re-login-and-replay would loop, or would
// re-send the request under the session credential instead.
export const LOGIN_REPLAY_CAUSES = [
  'TokenMissing',
  'TokenInvalid',
  'TokenExpired',
  'SessionCredentialMissing',
] as const

const has = (set: readonly string[], c: string | null): c is string => c !== null && set.includes(c)

/** The cause as a non-empty string, or null (absent, "", null, not a string). */
export function causeOf(body: unknown): string | null {
  const causes = (body as { details?: { causes?: { reason?: unknown }[] } } | null | undefined)?.details?.causes
  const r = Array.isArray(causes) ? causes[0]?.reason : undefined
  return typeof r === 'string' && r.length > 0 ? r : null
}

/** True only for a 401 whose cause is one of snowplow's five: nothing was dispatched. */
export function refusedBeforeDispatch(status: number, body: unknown): boolean {
  return status === 401 && has(SNOWPLOW_PRE_DISPATCH_CAUSES, causeOf(body))
}

/** True only when a fresh login fixes it AND nothing was dispatched. */
export function replayAfterLogin(status: number, body: unknown): boolean {
  return status === 401 && has(LOGIN_REPLAY_CAUSES, causeOf(body))
}
```

```ts
// the client's 401 handling
if (replayAfterLogin(res.status, body)) {
  await dropTokenAndLogin()
  return retryOnce() // safe: snowplow sent nothing upstream; a second 401 is surfaced, not replayed
}
if (res.status === 401 && causeOf(body) === 'StepUpRequired') {
  return startStepUp() // the step-up flow owns what happens next; never re-login-and-replay
}
// Anything else (no cause, an unknown cause, "", null): surface it. Never replay:
// snowplow does not vouch that nothing was dispatched, and a write may have happened.
```

At the two sites that hold the `Response` (`useWidgetQuery.ts`,
`sseClient.ts`), you may additionally log
`res.headers.get('WWW-Authenticate')` for diagnostics (same-origin only; see
section 2f). Do not branch on it.

## 7. How to see it

Token causes, with no setup:

```sh
curl -si "$SNOWPLOW/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=ns&name=ra" \
  | sed -n '1p;/^WWW-Authenticate/p;$p'
```

The output is `HTTP/1.1 401`, a bare `WWW-Authenticate: Bearer`, and a body
whose `details.causes[0].reason` is `TokenMissing`. Send
`-H "Authorization: Bearer not.a.jwt"` for `TokenInvalid` (with
`WWW-Authenticate: Bearer error="invalid_token"`), or an expired JWT for
`TokenExpired`.

Credential causes: against a test user, with a valid step-up JWT, delete the
user's `<user>-clientconfig-stepup` Secret in AUTHN_NAMESPACE and call any
`UserConfig` route:

```sh
curl -si -H "Authorization: Bearer $STEPUP_JWT" "$SNOWPLOW/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=ns&name=ra" \
  | sed -n '1p;/^WWW-Authenticate/p;$p'
```

The reason is `StepUpRequired`, with the step-up challenge. Repeat with a
session JWT whose session Secret is deleted: the reason is
`SessionCredentialMissing`, with the `invalid_token` challenge.

The snowplow tests that pin all of the above:

- `internal/handlers/middleware/predispatch_401_1076_test.go` (token causes on
  both middlewares, the six Status fields against upstream, the 503);
- `internal/handlers/middleware/credential_status_1029_test.go` (credential
  causes, and every example in section 2 against the wire);
- `internal/handlers/middleware/credential_status_closedset_1076_internal_test.go`
  (the set is closed);
- `internal/handlers/dispatchers/predispatch_401_relay_1076_test.go` (a stage
  401 after a write carries no cause, even a forged one);
- `internal/handlers/call_401_relay_1076_test.go` (a passthrough 401 carries
  no cause, even a forged one).
