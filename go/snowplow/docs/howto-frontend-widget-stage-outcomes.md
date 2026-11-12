# Frontend integration: a widget's incomplete stages, `X-Snowplow-Stage-Outcomes` (#1319)

This guide is for a frontend session. It describes a new response header on a
widget `GET /call`. The portal uses it to show an exception-only notice: "some
of this widget's data could not be loaded". Everything below comes from the
snowplow code:

- `internal/handlers/dispatchers/widgets.go` (the cold resolve, the two L1
  hits, the Puts);
- `internal/handlers/dispatchers/stage_outcomes_header.go` (presence rules);
- `internal/resolvers/restactions/api/stage_outcomes.go` (the format and the
  reason codes);
- `internal/cache/content_digest.go` and `internal/cache/put_outcome.go` (the
  refresh digest and the frame rule).

It is additive. A portal that ignores the header behaves exactly as today.

## 1. What it says

A widget with an `apiRef` renders the output of a RESTAction. That RESTAction
runs stages (`spec.api[]`). A stage with `continueOnError: true` can fail and
the widget still answers `200`, with that stage's data missing. The widget's
own JQ (`widgetDataTemplate`) may hide the error keys, so the body alone cannot
tell a complete widget from a partial one. snowplow does not constrain the JQ.
The header says it outside the body.

## 2. The header

```
X-Snowplow-Stage-Outcomes: [{"name":"<stage>","ok":false,"reason":"<code>"}]
```

- **Same name and format as a RESTAction `/call`** (see
  [howto-frontend-call-dryrun-raw.md](howto-frontend-call-dryrun-raw.md)
  section 5). It describes the stages of the widget's `apiRef` RESTAction.
- **Compact JSON array** in topological stage order. It lists **only the stages
  that did not succeed**. Each row has:
  - `name`: the stage name from `spec.api[].name`;
  - `ok`: always `false`;
  - `reason`: a code from the closed set below;
  - `dependsOn`: only on a `Skipped` row. It names the stage that did not
    succeed.
- **A resolve-level row** has `name` `""` and reason `Aborted`. The resolve ran
  no stage (its stages cannot be ordered, or it had no identity or client
  config). Every declared stage is then listed as `NotRun`.
- **Bounded.** Above 4096 bytes the array is replaced by
  `{"truncated":true,"failed":N}`. `N` counts the rows it replaced. Treat it as
  incomplete.
- **No identity, no upstream text.** It never carries a message, a path, a
  user, a body or any upstream error text. Only stage names (from the CR) and
  codes.

## 3. Reason codes (closed set), in three groups

The portal shows its notice only when data was cut short or failed. It never
shows it for a per-user denial or for a stage snowplow skipped by design.
Every code snowplow can send is in exactly one of the three groups below. The
groups come from the code's own map (`api.StageReasonGroups` in
`internal/resolvers/restactions/api/stage_outcomes.go`):
- `TestIssue1319_G1` fails when a code is added without a group.
- `TestIssue1319_G2` fails when these tables differ from the map.

**Show the notice when at least one row's code is in "cut / failed".** If every
row is "denied" or "by design", show no notice. A truncated header
(`{"truncated":true,"failed":N}`) names no codes: treat it as "cut / failed".

**Treat an unknown code (from a future snowplow) as "cut / failed".**

### cut / failed

| code | meaning |
|---|---|
| `Timeout` | the page budget ended the exchange (504): the page's 10 s goal ran out (#1115). This is the budget code; there is no separate "Budget" code |
| `Error` | any other failure |
| `NotFound` | a plain 404: the upstream answered "not found" to a dial |
| `Unauthorized` | the credential was refused (401). Never a per-user denial (#681) |
| `NotRun` | the resolve stopped before this stage ran |
| `Skipped` | a write stage not sent because the stage in `dependsOn` failed |
| `Aborted` | the resolve-level row (`name` `""`): no stage ran |
| `InvalidPath` | snowplow refused the rendered apiserver path (a write) |
| `TemplateError` | the path, payload or a header did not render (a write) |
| `NotSent` | a write planned and not sent |
| `DestinationRefused` | the destination policy refused the stage's address (the same for every user) |
| `UserAccessFilterOnNonReadVerb` | the stage is misconfigured (`userAccessFilter` on a write verb). It fails for every user, so it is not a per-user denial |

### denied

| code | meaning |
|---|---|
| `Forbidden` | the requester may not read it (a 403) |
| `NotFoundForRequester` | a watched named object that does not exist for this requester: the definite per-user NotFound (#1080), stored like a denial (#681) |

### by design

| code | meaning |
|---|---|
| `StageNotExecuted` | a dry run did not send this write stage |
| `WriteRefused` | snowplow did not send this write because the request is not the user's own write (an export, background work) |

Only these codes are ever sent.

**One reason per stage: the group decides first (#1347).** A stage can have
several items (an `iterator`), and each item can fail with its own code. The
row still carries ONE `reason`. snowplow picks it by group first:

1. any "cut / failed" code, if one item has one;
2. else any "denied" code;
3. else a "by design" code.

Inside a group, snowplow picks the most specific code by a fixed order. So a
stage with one `403` item and one item cut by the page budget reads
`Timeout`, never `Forbidden`: data was cut, and the notice shows. A stage
whose only failures are denials reads a "denied" code. The order comes from
the group each code is in (`stageGroupPrecedence` in `stage_outcomes.go`), so
a new code takes its group's place. The RESTAction `/call` header uses the
same rule: both headers read the one reason the stage kept. The portal can
keep reading each row's group: one row never hides a cut behind a denial.

**Two NotFound codes.** A watched named object that does not exist for this
requester is the definite per-user answer (#1080): it reads
`NotFoundForRequester` ("denied", no notice). A plain 404 from a dial reads
`NotFound` ("cut / failed"). `NotFoundForRequester` is new in #1319 and is in
the closed set for RESTAction `/call` too; an older snowplow sends `NotFound`
for both.

## 4. Presence: absent means complete

- **A widget answer carries the header ONLY when at least one stage did not
  succeed.** No header means every stage of its RESTAction completed. Show the
  notice when the header is present, and clear it when a later answer has
  none.
- A widget **without an `apiRef`** has no RESTAction stages. It never carries
  the header.
- **Older snowplow** never sends the header on a widget. Every widget then
  reads complete, which is today's behaviour. No version check is needed.
- The header can also be on a widget's error answer (`424`, `5xx`), as on a
  RESTAction `/call`. It is set before the error branch.
- **RESTAction `/call` is different and unchanged:** it ALWAYS carries the
  header, and `[]` means every stage succeeded (#904). Do not reuse a
  RESTAction "is it `[]`?" check for widgets. For a widget, test whether the
  header is present.

## 5. Cold and hit

- **Cold resolve.** The header comes from the resolve that produced the body.
- **L1 hit** (per-identity cell, or the identity-free content shell). The
  header is the one the resolve that **stored** the cell produced. It is kept
  on the cell and replayed: a hit answers exactly what its cold resolve
  answered. This also holds when the RESTAction output came from a stored
  full list (a paginated widget) or from the seed memo.
- **What can be stored.** Storage did not change: the same Put gate decides it
  for every class. It refuses a result whose resolve recorded a stage error,
  unless every error was a per-identity denial (#681: `Forbidden`, or the
  definite `NotFoundForRequester` of #1080). Two kinds of incomplete answer pass it, so a
  hit's header lists one of them:
  - **per-identity denials** (`Forbidden`, `NotFoundForRequester` rows). The answer is
    correct and repeatable for everyone who shares the cell's RBAC class;
  - **a resolve that ran no stage** (`Aborted` and one `NotRun` row per
    stage). Its stages cannot be ordered (a `dependsOn` naming no stage, a
    cycle), or it had no identity or no client configuration. Nothing ran, so
    no stage error was recorded, and the cell is stored with these rows.
- **A budget cut is answered, not stored.** A stage cut by the page budget
  (`Timeout`), or any other failure, is answered cold with its header. The
  cell is not stored, so the next `/call` resolves again and can complete.
  The reasons:
  - The cut depends on this request's timing, not on the data. Storing it
    would serve the partial body to everyone in the cell's class.
  - Nothing would replace it. A stored cell is replaced only when its inputs
    change, so a cut cell would stay until its TTL.
  - The rule is "stale, then replace": a complete older cell stays served
    instead of being overwritten by a partial one.
  - It is the same rule for every class, RESTAction `/call` included. Nothing
    special-cases widgets or budget cuts.

## 6. Refresh behaviour

- **A `/refreshes` frame tells you the answer changed, and the stage outcomes
  are part of the answer.** A refresher re-resolve that keeps the same body
  but changes the outcomes (a denial cleared, an object now exists) publishes
  a frame (#1321's frame rule now compares body **and** stored outcomes).
- **The refetch after a frame answers the refreshed cell.** If it completed,
  the refetch has **no header**. Clear the notice.
- **`X-Snowplow-Refresh-Digest`** (#1324) names the whole answer: the body,
  plus the stored stage outcomes when a stage did not succeed. A complete
  answer's digest is unchanged from #1324. An incomplete one differs from it,
  so a tab holding the incomplete answer gets one catch-up frame when it
  re-arms after the cell completed. Store and echo the digest as today, as an
  opaque string.
- A cold answer that was not stored (a budget cut) arms nothing. The portal's
  next `/call` of that widget (a navigation, a retry) resolves again.

## 7. The portal's single origin

The header reaches the browser through the portal's single origin, like the
other `X-Snowplow-*` headers. I checked each edge on the path:

| edge | what it does to the header |
|---|---|
| snowplow's own CORS (`main.go`, `ExposedHeaders`) | lists `X-Snowplow-Stage-Outcomes`, so a cross-origin `fetch` can read it too |
| snowplow chart (`helm/snowplow`) | no header manipulation; its `Ingress` is off by default with no annotations |
| agentgateway route (`agentgateway-policies` chart, `httproute-platform.yaml`, gateway path mode) | `PathPrefix /content` → snowplow, `URLRewrite` only. No `ResponseHeaderModifier`, no response transformation. Not stripped. |
| agentgateway CORS policy (`policy-cors.yaml`, Gateway-level) | sets `allowOrigins/allowMethods/allowHeaders`, no `exposeHeaders`. It matters only cross-origin. In path mode the SPA (`/`) and snowplow (`/content`) share one origin, so the browser exposes every response header without CORS. |
| snowplow gzip middleware | compresses the body only |

No edge on the single-origin path strips it. The same path already carries
`X-Snowplow-Refresh-Key` and `X-Snowplow-Refresh-Digest`, which the portal
reads today.

One edge is **not verified**: a **cross-origin** deployment behind the
agentgateway CORS policy. I did not check whether agentgateway's CORS filter
keeps the backend's `Access-Control-Expose-Headers` on a non-preflight
response. That deployment would also affect `X-Snowplow-Refresh-Key`, so it is
not new to this header.

## 8. Example

A widget whose RESTAction reads `x/y` (the user may not) and `pub/p`. Both
stages have `continueOnError: true`. Pinned by
`TestIssue1319_W3_ADenialIsStoredWithItsOutcomeAndReplayedOnHit`:

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-Snowplow-Stage-Outcomes: [{"name":"sa","ok":false,"reason":"Forbidden"}]
```

The same widget cut by the page budget (`TestIssue1319_W2_…`):

```http
X-Snowplow-Stage-Outcomes: [{"name":"sa","ok":false,"reason":"Timeout"}]
```

The same widget when every stage completes (`TestIssue1319_W1_…`,
`TestIssue1319_W5_…`): no `X-Snowplow-Stage-Outcomes` header at all.

## 9. Observability (snowplow side)

The request's Server span carries the same attributes as a RESTAction `/call`
(#1264), from the same header, with no identity:

- `snowplow.stages.failed`: failures listed;
- `snowplow.stages.not_executed`: by-design non-executions listed;
- `snowplow.stages.failure_reasons`: the distinct failure codes, sorted.

A complete widget span has none of them, because its header is absent. A
RESTAction span reads `snowplow.stages.failed=0` when it is complete.
