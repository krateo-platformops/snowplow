# Frontend integration: a /call whose extras name a reserved key answers 400 (#1116)

This guide is for a frontend session. From #1116, snowplow refuses a `/call`
whose `extras` contain a key that the RESTAction resolver writes itself. The
answer is a **400** whose body names every such key, machine-readably.

The rule is a security fix. Before it, a caller could put a fake stage output
into `extras` under the stage's name. A stage that was denied or never ran then
left the fake in place, and a later stage dialled a credential snowplow holds
(the ServiceAccount, a named endpoint) with the caller's value.

Everything below comes from the handler code:

- `internal/resolvers/restactions/api/reserved_extras.go`: the rule, which keys
  are reserved, and the refusal;
- `internal/resolvers/restactions/restactions.go` and `api/resolve.go`, where it
  is checked: first, before anything is read or dialled;
- `internal/handlers/dispatchers/reserved_extras_status.go`: the 400 it writes;
- `internal/handlers/dispatchers/restactions.go` and `widgets_resolve_error.go`:
  the two routes that render it.

The SPA references are to `frontend-draganddrop/frontend`, `ui/src`, as it is
on the main checkout:

- `hooks/useWidgetQuery.ts`, in particular `buildExtrasParam` and
  `shouldRetryWidgetFetch`;
- `hooks/useCatchError.tsx`.

## 1. Which keys are reserved

The reserved keys are per RESTAction: the RESTAction a `/call` resolves, or the
`spec.apiRef` RESTAction of a widget. They are exactly the keys the resolver
writes into its jq dict:

| key | why the resolver writes it |
|---|---|
| every `spec.api[].name` | the stage's output |
| every `spec.api[].errorKey` | the stage's errors |
| `error` | the errors of any stage that declares **no** `errorKey`; reserved only when such a stage exists |
| `slice` | the pagination block (`page`, `perPage`, `offset`, `cursor`) |

Any other key is accepted unchanged, as before. That includes `name`,
`namespace`, `q`, `projects`, `username` and even `extras`.

Pagination is sent as the `page` and `perPage` query parameters, never as
`extras.slice`.

## 2. When it happens

The rule is checked on every RESTAction resolve, before anything is read or
dialled:

- `GET /call` and `POST /call/read` of a **RESTAction**;
- `GET /call` and `POST /call/read` of a **widget**: its apiRef RESTAction
  receives the request extras;
- `GET /export` and `POST /export`, which relay the `/call` envelope unchanged;
- `GET /rbac?apiRefName=…&apiRefNamespace=…&extras=…`. Its read-set is
  enumerated from the same dict, so it answers the same 400 instead of a
  read-set computed from a forged stage output. It sends no
  `X-Snowplow-Stage-Outcomes`.
- a nested `resolve: true` stage, and the background refresher. These never
  reach the browser; a refused `/call` stores no cell for the refresher to
  re-resolve.

**Cache off and cache on answer the same.**

**A served widget cell is not a resolve.** A widget's cell key does not include
the extras keys the widget does not declare in `spec.keyExtras` (F6). So a
request whose extras add a reserved key the widget does not declare can be
served that widget's warm cell (200, the reserved key unused) and, on a cold
cell, get this 400. The key never reaches a resolve either way. Do not rely on
either answer: see section 5.

## 3. The response

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json
X-Snowplow-Stage-Outcomes: [{"name":"","ok":false,"reason":"Aborted"},{"name":"stage1","ok":false,"reason":"NotRun"},{"name":"stage2","ok":false,"reason":"NotRun"}]

{"kind":"Status","apiVersion":"v1","status":"Failure","message":"extras name keys the RESTAction resolver writes itself: \"error\" is the errorKey of stage \"stage2\"; \"stage1\" is the name of stage \"stage1\" (its output)","reason":"BadRequest","code":400,"details":{"causes":[{"reason":"ReservedExtrasKey","field":"extras.error","message":"\"error\" is the errorKey of stage \"stage2\""},{"reason":"ReservedExtrasKey","field":"extras.stage1","message":"\"stage1\" is the name of stage \"stage1\" (its output)"}]}}
```

This is the body for `?extras={"namespace":"team-b","stage1":{…},"error":"x"}`
against a RESTAction whose stages are `stage1` (errorKey `stage1Err`) and
`stage2` (no errorKey). It is copied from the dispatcher test
`TestReservedExtras1116_CallAnswers400`.

### What is guaranteed

- **The body is the contract.** `details.causes` has one entry per reserved key
  the request sent, sorted by key.
  - `reason` is always `ReservedExtrasKey`.
  - `field` is `extras.<key>`: the key to stop sending. Branch on these two
    only.
- **Existing readers are unaffected.** `kind`, `apiVersion`, `status`,
  `message`, `reason:"BadRequest"` and `code:400` are byte-identical to
  plumbing's 400 Status, in the same order, and `details` is appended after
  them. `useCatchError` shows `error.data.message` exactly as for any other 4xx.
- **`message` and `causes[].message` are for humans.** They are English text
  naming the key and the stage that owns it. Do not parse them.
- **The names are not secret from the caller.** The refusal is only ever
  reached after the RESTAction (or widget) was read under the caller's own
  authorisation. A caller who may not read it gets the 403 or 404 first, never
  this 400.
- **Nothing ran.** On a RESTAction `/call`, `X-Snowplow-Stage-Outcomes` carries
  the resolve-level `Aborted` row and every stage as `NotRun`. A widget `/call`
  does not send that header.

## 4. What the SPA sends today, and where it could collide

`buildExtrasParam` puts four inputs into every widget's extras, in this order:

1. every **URL query parameter** of the page;
2. the **route params**;
3. `displayName`/`username`, when identity injection is on;
4. the host's **callerExtras**.

I checked the portal RESTActions (portal main 10e359e, and 057's live set)
against every route param, widget `keyExtras`, `queryParam` and
`apiRef.extras` key. Only one stage, errorKey or `slice` matches, and it is a
latent collision:

- The observability-page composition on 057 (v1-8-85) has 20 `sp-*`
  RESTActions whose single stage is named **`q`**.
- The same page's `obs-by-kind-*` and `obs-by-namespace-*` widgets read `?q=`.
- So `/observability?q=…` sends `extras.q` to the `sp-*` charts too.
- Before #1116 that request already failed: the stage output was appended to the
  string, and the RESTAction filter errored (500).
- Now it is a 400 naming `extras.q`. The fix belongs to the portal: rename that
  stage. The SPA needs no special case.

## 5. Edge cases and fallback

- **The SPA does not retry a 400.** `shouldRetryWidgetFetch` never retries a 4xx
  other than 404, and that is right here: the same request is refused the same
  way every time. Render the error once.
- **Do not strip the key and resend automatically.** A reserved key in extras
  means the page put something into extras that a data source reads as its own
  output. The right fix is upstream: rename the URL parameter, the route param
  or the stage. A silent resend would hide the misconfiguration and change what
  the user asked for.
- **A widget can answer 200 or 400** for the same URL, depending on whether its
  cell is warm (section 2). Both are correct; a page that never sends a
  reserved key never sees either.
- **Pagination**: a 400 naming `extras.slice` means something put `slice` into
  extras. Use the `page`/`perPage` query parameters.
- **An unknown `causes[].reason`**: treat the response as an ordinary 400.

## 6. Code example (TypeScript)

```ts
// ui/src/utils/reservedExtras.ts
export interface StatusBody {
  kind?: string
  message?: string
  reason?: string
  code?: number
  details?: { causes?: Array<{ reason?: string; field?: string; message?: string }> }
}

/** The extras keys a 400 says the data source reserves (#1116), or [] for any other answer. */
export function reservedExtrasKeys(status: number, body?: StatusBody): string[] {
  if (status !== 400) return []
  return (body?.details?.causes ?? [])
    .filter((c) => c.reason === 'ReservedExtrasKey' && typeof c.field === 'string' && c.field.startsWith('extras.'))
    .map((c) => (c.field as string).slice('extras.'.length))
}

// In the widget error path: report it as a configuration fault of the page,
// naming the key, once; never retry, never resend without the key.
//   const keys = reservedExtrasKeys(res.status, body)
//   if (keys.length > 0) {
//     console.error(`snowplow #1116: this page sends extras ${keys.join(', ')}, which the data source reserves`)
//   }
```

`useCatchError` already renders `error.data.message` for a 4xx, so the user
sees the sentence naming the key with no change. The helper above is for
logging and telemetry only.
