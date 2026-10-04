# Frontend integration: dry-run writes, field validation, raw reads, inline resolve and capability discovery (#443)

This guide is for a frontend session integrating snowplow's #443 contract. The
first consumer is the builder gate (krateo-platformops/frontend#446,
`builder-gate/docs/snowplow-contract.md`). Everything below comes from the
handler code: `internal/handlers/call.go`, `internal/handlers/proxy.go`,
`internal/handlers/capabilities.go`, the route table in `main.go`
(`mountCallWriteRoutes`), and for the inline resolve
`internal/handlers/middleware/callread.go` and
`internal/handlers/dispatchers/restactions.go`.

Part 1 (dry-run, fieldValidation, discovery) and part 3 (raw reads) shipped
together. Part 2, the inline dry-run resolve of a RESTAction body (section 5),
is advertised as `call.read.inline`.

## 1. Gate on capabilities, never on a version

```
GET /capabilities            (no auth)
200 {"capabilities":["call.dryRun","call.fieldValidation","call.raw","call.read.inline"]}
```

- The list is static and sorted.
- Gate each feature on the PRESENCE of its token.
- A `404` (an older snowplow) means no tokens. Treat it exactly like an empty
  list.
- Don't parse a version number for this.

| Token | What it promises |
|---|---|
| `call.dryRun` | `POST\|PUT\|PATCH /call/dry-run` sends the apiserver `dryRun=All` and echoes `X-Snowplow-Dry-Run: All`. |
| `call.fieldValidation` | `fieldValidation=Ignore\|Strict` is forwarded on write verbs of `/call` and `/call/dry-run`, and echoed in `X-Snowplow-Field-Validation`. |
| `call.raw` | `raw=true` on `GET /call` and `POST /call/read` returns the stored object for restactions and the widgets group, read as the caller, and echoes `X-Snowplow-Raw: true`. |
| `call.read.inline` | `POST /call/read` on a RESTAction with body `{"extras":{…},"object":{<RESTAction>}}` resolves the body instead of the stored CR, as the caller, persisting nothing, and echoes `X-Snowplow-Dry-Run: All` and `X-Snowplow-Resolve-Source: request-body`. |

Not advertised yet: `call.warnings` (apiserver `Warning` passthrough and
`fieldValidation=Warn`, which needs a plumbing change) and
`call.read.inline.dependencies` (a draft that references another draft).

## 2. Dry-run writes: `POST|PUT|PATCH /call/dry-run`

The query parameters and body are the same as for the matching `/call` write.
The request runs with the caller's own credentials, exactly like `/call`.

```
POST /call/dry-run?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo-system&name=my-draft
Authorization: Bearer <jwt>
Content-Type: application/json

{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction","metadata":{...},"spec":{...}}
```

- The outbound apiserver request ALWAYS carries `dryRun=All`. The apiserver
  runs validation and admission, and persists nothing.
- The reply has the same shape as `/call`: `200` plus the would-be object, or
  the apiserver's `Status` with its code (`403`, `409`, `422`, …).
- `DELETE /call/dry-run` doesn't exist (`405`).
- You don't need to send `dryRun` at all. If you do, it must be exactly `All`.
  Anything else (`all`, `true`, empty, given twice) is a `400`.
- Never send `dryRun` to plain `/call`: it's a `400` with the message
  "dry-run writes use /call/dry-run". It is never forwarded and never dropped.

Why a separate route: an older snowplow ignores `dryRun` on `/call`, so
`POST /call?dryRun=All` would be a REAL create there. An older snowplow has no
`/call/dry-run` route, so it answers `404` and writes nothing. That holds even
in the middle of a rolling deploy, whichever pod you land on.

## 3. Field validation: `fieldValidation=Ignore|Strict`

- It's allowed on `POST`, `PUT` and `PATCH` of `/call` and `/call/dry-run`.
- Values are case-sensitive. `strict` or `Foo` is a `400`.
- `Warn` is a `400` for now, because apiserver `Warning` headers are not passed
  through yet.
- `fieldValidation` on `GET`, `DELETE` or `POST /call/read` is a `400`.
- Under `Strict`, an unknown field is the apiserver's rejection, and the
  message names the field (for example `unknown field "spec.bogusField"`). Known
  gap: snowplow currently returns at most 2 KiB of an apiserver error body and
  drops `details.causes`, so read the field from `message`.

## 4. Raw reads: `raw=true`

```
GET /call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=ns&name=x&raw=true
POST /call/read?…&raw=true          (body {"extras":{…}} is ignored for a raw read)
```

- For restactions and every resource in the widgets group, a plain `GET /call`
  RESOLVES the object. `raw=true` skips the resolver and returns the stored
  object as the apiserver has it, `.status` included.
- The read is made as the caller. The apiserver's `403` and `404` pass through
  unchanged, so use them to check that a reference exists and is readable.
- Other kinds already return the stored object on `GET /call`. `raw=true` is
  accepted for them too and still echoes, so one code path works for any
  reference.
- `raw` must be exactly `true`. `false`, `1`, `True` or an empty value is a
  `400`, and so is `raw` on any write verb.
- A raw read doesn't touch snowplow's cache or informers.

## 5. Inline dry-run resolve: `POST /call/read` with an `object`

```
POST /call/read?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=<ns>&name=<name>[&page=&perPage=]
Authorization: Bearer <jwt>
Content-Type: application/json

{"extras": {…}, "object": {"apiVersion":"templates.krateo.io/v1","kind":"RESTAction","metadata":{"name":"<name>","namespace":"<ns>"},"spec":{…}}}
```

- The reply has the same envelope as resolving the stored RESTAction (`GET
  /call` or `POST /call/read` without `object`), including per-stage errors in
  `.status`. For the same caller and the same spec it is byte-identical.
- Echoes on every reply from the resolver (`200`, a stage-error `200`, a filter
  `500`): `X-Snowplow-Dry-Run: All` and `X-Snowplow-Resolve-Source:
  request-body`. A missing echo means an older snowplow resolved the STORED
  RESTAction instead: treat it as a failure.
- **Nothing is persisted.** No cache entry is written or touched, no informer
  is registered, and nothing is announced on `/refreshes`. A draft with the
  same name as a stored RESTAction never reads the stored cell.
- **Write-verb stages are not executed.** A `POST`/`PUT`/`PATCH`/`DELETE`
  stage gets, under its `errorKey`, the error object
  `{"reason":"StageNotExecuted","message":"dry-run: stage \"<id>\" verb <V> is not executed"}`.
  Both are a **stable contract**: match on `reason == "StageNotExecuted"`
  (preferred), or on the message prefix `dry-run: stage ` and suffix
  ` is not executed`.
- **Per-stage outcomes, outside the body.** Every inline reply from the
  resolver carries `X-Snowplow-Stage-Outcomes`, compact JSON in topological
  stage order: `[{"name":"<stage>","ok":true},{"name":"<stage>","ok":false,"reason":"<code>"}]`.
  Use it when the draft's own `filter` drops the error keys from the body.
  - `reason` is a code from a closed set: `StageNotExecuted`, `Forbidden`,
    `NotFound`, `Unauthorized`, `NotRun` (the resolve stopped before this
    stage), `Error` (anything else). The header never carries an error
    message, a path or response data.
  - Bounded: above 4 KiB it is `{"truncated":true,"failed":<N>}`.
  - Never set on a stored resolve. The body is unaffected (still
    byte-identical to a stored resolve).
- **Everything runs as the caller.** A named `endpointRef` Secret is read with
  the caller's own credentials (a Secret the caller cannot read is a stage
  error), and a `-clientconfig` endpointRef is refused. A `userAccessFilter`
  stage reads with the caller's own RBAC, never through snowplow's
  ServiceAccount: rows the caller cannot read are not returned, and a path the
  caller cannot list that is not served from snowplow's informers is the
  apiserver's `403` stage error. The filter still narrows the rows. A STORED
  RESTAction that the draft nests keeps its stored behaviour.
- **Validation (each a `400`, nothing resolved):** the object must be
  `templates.krateo.io/v1` `RESTAction` and convert to one; its
  `metadata.name`/`namespace` must equal the query's; the query must address
  `restactions` in `templates.krateo.io` (an `object` on a widget or any other
  resource is refused, and so is `object` together with `raw=true`); the body
  must be at most 1 MiB.
- Scope cut: a draft that references ANOTHER draft is not supported; nested
  references resolve stored objects.
- On an older snowplow the `object` is ignored and the STORED RESTAction is
  resolved (a read, so harmless), without the echoes.

## 6. Echo headers: a missing echo means FAIL

| Header | Set when |
|---|---|
| `X-Snowplow-Dry-Run: All` | The apiserver request carried `dryRun=All`. |
| `X-Snowplow-Field-Validation: <Ignore\|Strict>` | The apiserver request carried that `fieldValidation`. |
| `X-Snowplow-Raw: true` | The stored object was read without resolving. |
| `X-Snowplow-Resolve-Source: request-body` | The RESTAction resolved was the request body (with `X-Snowplow-Dry-Run: All`). |
| `X-Snowplow-Stage-Outcomes: [...]` | Inline replies only: per-stage outcome codes (section 5). |

- The echoes are derived from the outbound request snowplow actually built,
  and set before the reply is written.
- They are present on a `2xx` and on an apiserver failure (`403`, `404`,
  `422`, …).
- They are ABSENT on snowplow's own validation `400`. Nothing was sent to the
  apiserver.
- All of them, plus `Warning` (and the part-2 `X-Snowplow-Resolve-Source`), are
  in CORS `Access-Control-Expose-Headers`, so a cross-origin `fetch` can read
  them.
- If the echo you depend on is missing, treat the reply as a failure. That
  includes an unexpected `200`, which could mean an old snowplow did something
  else.

## 7. Example (TypeScript)

```ts
type Caps = Set<string>;

async function capabilities(base: string): Promise<Caps> {
  const r = await fetch(`${base}/capabilities`);
  if (!r.ok) return new Set(); // 404 = an older snowplow: no capabilities
  const { capabilities = [] } = await r.json();
  return new Set(capabilities);
}

async function dryRunCreate(base: string, jwt: string, caps: Caps,
                            q: URLSearchParams, obj: unknown, strict = true) {
  if (!caps.has("call.dryRun")) throw new Error("snowplow cannot dry-run: refuse to send");
  if (strict && caps.has("call.fieldValidation")) q.set("fieldValidation", "Strict");
  const r = await fetch(`${base}/call/dry-run?${q}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${jwt}`, "Content-Type": "application/json" },
    body: JSON.stringify(obj),
  });
  if (r.headers.get("X-Snowplow-Dry-Run") !== "All") {
    // 400 = our request was malformed; anything else without the echo = not a dry run
    throw new Error(`dry-run not confirmed (HTTP ${r.status})`);
  }
  if (strict && q.has("fieldValidation") && r.headers.get("X-Snowplow-Field-Validation") !== "Strict") {
    throw new Error("strict validation not confirmed");
  }
  return { ok: r.ok, status: r.status, body: await r.json() }; // Status body on failure
}

async function rawGet(base: string, jwt: string, caps: Caps, q: URLSearchParams) {
  if (!caps.has("call.raw")) throw new Error("snowplow cannot raw-read");
  q.set("raw", "true");
  const r = await fetch(`${base}/call?${q}`, { headers: { Authorization: `Bearer ${jwt}` } });
  if (r.headers.get("X-Snowplow-Raw") !== "true") throw new Error(`raw read not confirmed (HTTP ${r.status})`);
  return { status: r.status, body: await r.json() }; // 200 stored object | 403 | 404
}
```

## 8. Edge cases

- **Spelling is exact:** any key that is a case, underscore or hyphen variant
  of `dryRun` or `fieldValidation` (`dryrun`, `DryRun`, `dry_run`,
  `FieldValidation`, `field_validation`, …) is a `400` naming the correct
  spelling. It applies on every verb and route, and nothing is sent to the
  apiserver. A misspelled key is never dropped.

- **Mixed-version rollout:** `GET /capabilities` and the write can land on
  different pods. The echo check closes that gap. An old pod answers
  `/call/dry-run` with `404` (no echo, nothing written).
- **`/call/read` and dry runs:** `POST /call/read` is read-only. `dryRun` or
  `fieldValidation` there is a `400`.
- **Errors:** the reply is `{"kind":"Status","code":…,"message":…}`, whether it
  comes from snowplow (`400`) or the apiserver. Tell them apart by the echo
  header, never by the code alone. A Strict field rejection can itself be a
  `400` from the apiserver.
- **RBAC:** everything runs as the caller. A dry-run create needs the same
  `create` permission as a real create, so a `403` on a dry run means the real
  write would be forbidden too.
