# Frontend integration: a 401 for a missing credential says why (#1029)

This guide is for a frontend session. Snowplow now answers a missing per-user
credential with a 401 whose body names the cause. The portal can then tell
"step up again" from "log in again" without parsing text.

Everything below comes from the handler code:

- `internal/handlers/middleware/userconfig.go` (`UserConfig`: token check,
  Secret selection, the NotFound branch);
- `internal/handlers/middleware/credential_status.go` (the Status it writes);
- `internal/credsecret/credsecret.go` (which Secret a token reads, #994).

The contract was agreed on #1029:

- authn's comment: it emits causes in `details.causes[0].reason`, and the RFC
  9470 header carries no `acr_values`;
- frontend's comment: the body is the contract, and the header is optional.

The SPA references are to `frontend-draganddrop/frontend`,
`ui/src/hooks/useCatchError.tsx`, as it is on the main checkout.

## 1. When this happens

Every route mounted behind `middleware.UserConfig` reads the caller's own
credential Secret from AUTHN_NAMESPACE before the handler runs:

- `GET /call` and `POST /call/read`;
- the `/call` write routes;
- `GET /list`;
- `GET /export` and `POST /export`;
- `GET /rbac`;
- `POST /jq`.

`/refreshes` (SSE) does not read a Secret and is unaffected.

Which Secret is read depends only on the **validated JWT's `groups`**:

| token | Secret read |
|---|---|
| `groups` contains `krateo:loa:2` (a step-up login, authn `POST /oidc/stepup`) | `<user>-clientconfig-stepup` |
| any other token | `<user>-clientconfig` |

If that Secret is not in snowplow's informer, snowplow re-reads it from the
apiserver. If the apiserver answers **NotFound**, the credential is missing and
the request answers **401** with a cause (section 2).

- **Step-up case.** The step-up credential lives about 15 minutes, so the
  usual reason is that it expired and authn removed it. The step-up JWT itself
  expires no later than the credential, so an expired step-up JWT is still a
  plain 401 from token validation (section 4).
- **Session case.** The session Secret was deleted, for example by an admin or
  by a logout on another device.

## 2. The response

### 2a. Step-up credential missing (the token carries `krateo:loa:2`)

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer error="insufficient_user_authentication"

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"secrets \"alice-clientconfig-stepup\" not found","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"StepUpRequired","message":"the step-up credential is missing or expired; step up again"}]}}
```

### 2b. Session credential missing (any other token)

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"secrets \"alice-clientconfig\" not found","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"SessionCredentialMissing","message":"the session credential is missing; log in again"}]}}
```

There is no `WWW-Authenticate` header in this case.

### 2c. What is guaranteed

- **The body is the contract.** Read `details.causes[0].reason`. Snowplow emits
  exactly two values here:

  | reason | emitted when |
  |---|---|
  | `StepUpRequired` | the token carries `krateo:loa:2` and its step-up credential Secret is missing |
  | `SessionCredentialMissing` | any other token, and its session credential Secret is missing |

  The cause is chosen from the same validated token groups that chose the
  Secret, never from anything in the request. A token without `krateo:loa:2`
  can never receive `StepUpRequired`.
- **Existing readers are unaffected.** `kind`, `apiVersion`, `status`,
  `message`, `reason:"Unauthorized"` and `code:401` are byte-identical to the
  plumbing Status snowplow sent before, in the same order. `details` is
  appended after them. `error.data.message` in `useCatchError` reads exactly
  what it read before.
- **`causes[0].message` is for humans.** It is a fixed English sentence with no
  identity in it. Do not branch on it.
- **`message` is unchanged.** It still names the caller's own Secret (as
  before #1029, and only ever sent to that caller). Do not parse it.
- **The header is optional.** `WWW-Authenticate: Bearer
  error="insufficient_user_authentication"` is the RFC 9470 step-up challenge,
  sent on the step-up case only. It carries **no `acr_values`**: `krateo:loa:2`
  is authn's group, not an IdP ACR value, and the IdP's values come from authn.
  Portal behaviour must never depend on the header. Most 401s reach
  `useCatchError`, which has no headers. Only `useWidgetQuery.ts` and
  `sseClient.ts` hold the `Response`.

## 3. The three portal outcomes

These reasons come from two components. Keep them distinct; never collapse
them into one.

| reason | emitted by | HTTP | portal outcome |
|---|---|---|---|
| `StepUpRequired` | **snowplow** (this guide) | 401 | **prompt to step up** |
| `InsufficientAssurance` | **authn** (`POST /oidc/stepup`) | 403 | prompt to step up |
| `StaleAuthentication` | **authn** (`POST /oidc/stepup`) | 403 | **re-authenticate** |
| `StepUpNotEnabled` | **authn** | 403 | **configuration fault**: tell the admin; never prompt |
| `StepUpMisconfigured` | **authn** | 500 | configuration fault: tell the admin; never prompt |
| `SessionCredentialMissing` | **snowplow** (this guide) | 401 | **log in again** (the existing session-resume flow) |

The authn reasons are authn's. They come from authn's own endpoints, never
from snowplow; they are listed only so that one parser covers both components.

## 4. Edge cases and fallback

- **A 401 with no `details`.** This is every other 401, from any route: a
  missing `Authorization` header, a malformed bearer, a bad signature or an
  expired JWT (including an expired step-up JWT). Treat it as today: the
  session-resume flow (`raiseSessionExpired`).
- **A 500 with no `details`.** The credential re-read failed for another
  reason: the apiserver answered 403 or 5xx, the snowplow pod cannot build its
  in-cluster config, or the Secret is malformed. Snowplow does not know the
  credential is missing, so it invents no cause. Treat it as today's 500.
- **A 503** means the JWT signing key could not be resolved (authn's JWKS
  unreachable). It is unchanged and retryable.
- **An unknown `causes[0].reason`** (a future value): fall back to the plain
  401 handling. Never prompt for step-up on an unknown reason.
- **A burst of concurrent 401s** from one expired step-up credential: every
  in-flight request gets the same `StepUpRequired`. Raise one prompt, the same
  way `raiseSessionExpired` already de-duplicates.

## 5. Code example (TypeScript)

Extend the error shape that `useCatchError` already receives. The body is
already in `error.data`.

```ts
// ui/src/utils/credentialCause.ts
export type CredentialCause =
  | 'StepUpRequired'           // snowplow, 401 — prompt to step up
  | 'SessionCredentialMissing' // snowplow, 401 — log in again
  | 'InsufficientAssurance'    // authn, 403 — prompt to step up
  | 'StaleAuthentication'      // authn, 403 — re-authenticate
  | 'StepUpNotEnabled'         // authn, 403 — configuration fault
  | 'StepUpMisconfigured'      // authn, 500 — configuration fault

export interface StatusBody {
  kind?: string
  message?: string
  reason?: string
  code?: number
  details?: { causes?: Array<{ reason?: string; message?: string }> }
}

export type CredentialOutcome = 'step-up' | 'reauthenticate' | 'login' | 'config-fault' | 'none'

export function credentialOutcome(body?: StatusBody): CredentialOutcome {
  switch (body?.details?.causes?.[0]?.reason) {
    case 'StepUpRequired':
    case 'InsufficientAssurance':
      return 'step-up'
    case 'StaleAuthentication':
      return 'reauthenticate'
    case 'StepUpNotEnabled':
    case 'StepUpMisconfigured':
      return 'config-fault'
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
    // 'login', 'none' (no details): today's behaviour, unchanged
    description = error?.data?.message || 'Your session has expired.'
    void raiseSessionExpired()
    if (type === 'notification') return
  }
}
```

`CatchError.data` must be widened from `{ message?: string }` to `StatusBody`
for this to type-check. `data.message` keeps its meaning.

At the two sites that hold the `Response` (`useWidgetQuery.ts`,
`sseClient.ts`), you may additionally log
`res.headers.get('WWW-Authenticate')` for diagnostics. Do not branch on it.

## 6. How to see it

Against a test user, with a valid step-up JWT, delete the user's
`<user>-clientconfig-stepup` Secret in AUTHN_NAMESPACE and call any
`UserConfig` route:

```sh
curl -si -H "Authorization: Bearer $STEPUP_JWT" "$SNOWPLOW/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=ns&name=ra" \
  | sed -n '1p;/^WWW-Authenticate/p;$p'
```

The output is `HTTP/1.1 401`, the challenge header, and a body whose
`details.causes[0].reason` is `StepUpRequired`. Repeat with a session JWT
whose session Secret is deleted: the reason is `SessionCredentialMissing`, and
there is no challenge header.

The snowplow tests that pin all of the above are in
`internal/handlers/middleware/credential_status_1029_test.go`.
