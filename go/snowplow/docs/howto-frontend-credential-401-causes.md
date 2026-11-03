# Frontend integration: every 401 snowplow refuses before dispatch says why (#1029, #1076)

This guide is for a frontend session, and for any other client of snowplow
(oasgen's RDC, scripts). It answers two questions about a 401 from snowplow:

1. **Was anything dispatched?** Did snowplow refuse the request before sending
   anything upstream, or did the 401 come back from upstream, possibly after a
   write had already happened?
2. **What should the portal do?** Log in again, step up again, or neither.

The rule is short:

- **A 401 with `details.causes[0].reason`** is a snowplow refusal **before
  dispatch**. Nothing was sent upstream: no apiserver call, no RESTAction
  stage, no write. The reason comes from a closed set of five (section 2).
- **A 401 without `details`** was **relayed from upstream**. The request may
  have been dispatched, and a write may already have happened. Do **not** drop
  the token and replay the request on this 401 alone.

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

The SPA references are to `frontend-draganddrop/frontend`,
`ui/src/hooks/useCatchError.tsx`, as it is on the main checkout.

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
| `TokenMissing` | no bearer token: no `Authorization` header, or one whose scheme is not `Bearer` (and, on `RefreshAuth` routes, no session cookie either) | both | `Bearer error="invalid_token"` | **log in** |
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
WWW-Authenticate: Bearer error="invalid_token"

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"missing authorization header","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"TokenMissing","message":"no bearer token was presented; log in"}]}}
```

A non-Bearer header on `UserConfig` has the message `invalid authorization
header format`. On `RefreshAuth` both cases have the message `missing
credentials: no bearer header or session cookie`.

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
- **The header is optional.** `Bearer error="invalid_token"` is the RFC 6750
  challenge. `Bearer error="insufficient_user_authentication"` is the RFC 9470
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

## 3. A 401 without a cause: relayed from upstream

Snowplow relays an upstream 401 in two places. Both write plumbing's Status,
which has no `details` field. So a relayed 401 **never** carries a cause or a
`WWW-Authenticate` header, even when the upstream body or headers carry one
(a snowplow test sends a forged cause and challenge from upstream and checks
that both are dropped):

- **A raw `/call` passthrough.** The apiserver answered 401 to the call
  snowplow sent with the caller's credential. On a write, the call was sent
  and audited before the 401 came back.
- **A RESTAction stage.** A stage without `continueOnError` got a 401 from
  upstream, and the filter needs that stage's value. The stage's 401 then
  becomes the `/call`'s own 401 (#709). Earlier stages **have already run**,
  including any write stage (a `POST` stage before it, for example).

A widget whose dependency answered 401 is answered the same way
(`docs/howto-frontend-widget-dependency-status.md`): the dependency's 401,
with no cause.

What this means for a client:

- **Do not drop the token and replay** the request because of a 401 without a
  cause. On a write, a replay can send the write twice.
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
- **An unknown `causes[0].reason`** (a future value): treat it as a
  pre-dispatch refusal with the plain 401 handling (log in again). Never
  prompt for step-up on an unknown reason.
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

## 6. Code example (TypeScript)

Extend the error shape that `useCatchError` already receives. The body is
already in `error.data`.

```ts
// ui/src/utils/credentialCause.ts

// The snowplow pre-dispatch causes (#1029, #1076): nothing was dispatched.
export const SNOWPLOW_PRE_DISPATCH_CAUSES = [
  'TokenMissing',
  'TokenInvalid',
  'TokenExpired',
  'SessionCredentialMissing',
  'StepUpRequired',
] as const

export interface StatusBody {
  kind?: string
  message?: string
  reason?: string
  code?: number
  details?: { causes?: Array<{ reason?: string; message?: string }> }
}

export type CredentialOutcome = 'step-up' | 'reauthenticate' | 'login' | 'config-fault' | 'none'

export function causeOf(body?: StatusBody): string | undefined {
  return body?.details?.causes?.[0]?.reason
}

// True only for a snowplow 401 that carries a cause: snowplow refused before
// dispatch, so the request can be retried after a new login. Any other 401
// (no cause) was relayed from upstream and may have been dispatched.
export function refusedBeforeDispatch(status: number, body?: StatusBody): boolean {
  return status === 401 && causeOf(body) !== undefined
}

export function credentialOutcome(body?: StatusBody): CredentialOutcome {
  switch (causeOf(body)) {
    case 'StepUpRequired':
    case 'InsufficientAssurance':
      return 'step-up'
    case 'StaleAuthentication':
      return 'reauthenticate'
    case 'StepUpNotEnabled':
    case 'StepUpMisconfigured':
      return 'config-fault'
    case 'TokenMissing':
    case 'TokenInvalid':
    case 'TokenExpired':
    case 'SessionCredentialMissing':
      return 'login'
    default:
      return 'none' // no cause, or one this build does not know: today's handling
  }
}
```

```tsx
// ui/src/hooks/useCatchError.tsx — inside the 401 branch
import { credentialOutcome, type StatusBody } from '../utils/credentialCause'

if (error?.status === 401 || error?.code === 401) {
  const outcome = credentialOutcome(error?.data as StatusBody | undefined)
  if (outcome === 'step-up') {
    void raiseStepUpRequired() // one prompt, de-duplicated like raiseSessionExpired
    if (type === 'notification') return
  } else {
    // 'login', 'none' (no cause): today's behaviour, unchanged
    description = error?.data?.message || 'Your session has expired.'
    void raiseSessionExpired()
    if (type === 'notification') return
  }
}
```

A client that retries automatically after re-authenticating (for example
oasgen's RDC) must gate the retry on `refusedBeforeDispatch`:

```ts
if (res.status === 401 && refusedBeforeDispatch(res.status, body)) {
  await dropTokenAndLogin()
  return retry() // safe: snowplow sent nothing upstream
}
// 401 without a cause: surface it; never replay a write on it
```

`CatchError.data` must be widened from `{ message?: string }` to `StatusBody`
for this to type-check. `data.message` keeps its meaning.

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

The output is `HTTP/1.1 401`, `WWW-Authenticate: Bearer
error="invalid_token"`, and a body whose `details.causes[0].reason` is
`TokenMissing`. Send `-H "Authorization: Bearer not.a.jwt"` for
`TokenInvalid`, or an expired JWT for `TokenExpired`.

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
