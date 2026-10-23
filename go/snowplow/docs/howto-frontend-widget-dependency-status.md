# Frontend integration: a widget's failed dependency answers 424, not 404 (#853)

This guide is for a frontend session. It describes a change to the status
code of a widget `GET /call`, and what the SPA does with it. Everything below
comes from the handler code:

- `internal/handlers/dispatchers/widgets.go` (the CR read and the resolve
  call);
- `internal/handlers/dispatchers/widgets_resolve_error.go`
  (`encodeWidgetResolveError`).

The SPA behaviour was traced in `frontend-draganddrop/frontend` at `21f14bd`:

- `ui/src/hooks/useWidgetQuery.ts` (`shouldRetryWidgetFetch`, `fetchWidget`);
- `ui/src/components/WidgetRenderer/WidgetRenderer.tsx` (the error branch);
- `ui/src/components/WidgetStates/WidgetStates.tsx` (`isTimeoutError`).

**No SPA change is required.** The SPA at `21f14bd` already handles a 424
safely: it does not retry, it does not blank the page, and it shows the red
`WidgetError` with Retry (section 3).

The frontend's matching branch, `fix/widget-424-dependency` at `8f096d1`, is the
chosen rendering, confirmed by the owner. It shows a calm state for 424:

- `status=info`;
- titled "Data source unavailable";
- this response's `message` as its subtitle;
- a "Check again" button that refetches only that widget (section 3a).

Section 5 has a test you can add to pin the retry decision.

## 1. What changed

A widget `/call` makes two kinds of read:

1. **The widget CR itself**, for example the Panel `agent-create`.
2. **What the widget depends on**:
   - the `spec.apiRef` RESTAction and every stage of it;
   - nested resolves (`resourcesRefs` children);
   - the CRD used to validate the widget's status.

Before #853, a 404 from either kind reached the browser as 404. So "this
widget does not exist" and "this widget exists, but a RESTAction or CRD it
reads does not" were the same answer.

The SPA retries a 404, because right after a page load snowplow can answer 404
for a widget whose CR is not in its informer yet. The retry policy is 3
retries at 700, 1400 and 2800 ms, about 4.9 s in total. A widget with a
permanently missing dependency therefore cost about 5 s on every load before
it rendered "Not found". Bench run 12 measured `/agents/new` at about 6 s on
every load for exactly this reason: on an install without kagent, the
`agent-formdef` RESTAction's first stage GETs the `agents.kagent.dev` CRD,
which does not exist. Without that page, Chrome warm p95 was 1.9 s instead of
4.8 s.

From #853 on:

| What failed | Status | `reason` |
|---|---|---|
| The widget CR is not found (or not yet in the informer) | `404` | `NotFound` (unchanged) |
| The CR was read; a dependency is not found (`404`) or gone (`410`) | **`424`** | **`FailedDependency`** |
| The CR was read; a dependency answered `401` | `401` | unchanged |
| The CR was read; a dependency answered `403` | `403` | unchanged |
| The CR was read; a dependency answered `400`, `409` or `429` | same code | unchanged |
| The CR was read; a dependency answered `5xx` (`500`, `502`, `503`, `504`) | same code | unchanged |
| The CR was read; the resolve failed with no HTTP status (e.g. a jq error) | `500` | unchanged |

There is one rule for every widget kind and every dependency. It changes only
the status line and the body's `code`, `status` and `reason`. The message is
the dependency's own, as before. Nothing is cached on this path: neither the
failure nor a negative entry.

## 2. Wire shape

```
GET /call?apiVersion=widgets.templates.krateo.io/v1beta1&resource=panels&namespace=krateo-system&name=agent-create
Authorization: Bearer <jwt>

HTTP/1.1 424 Failed Dependency
Content-Type: application/json

{
  "kind": "Status",
  "apiVersion": "v1",
  "status": "Failure",
  "message": "restaction krateo-system/agent-formdef stage \"crd\" failed: customresourcedefinitions.apiextensions.k8s.io \"agents.kagent.dev\" not found",
  "reason": "FailedDependency",
  "code": 424
}
```

That `message` is the exact one the `/agents/new` shape produces; a test
records it (`TestIssue853_424Body_IsAnIdentityFreeStatus_EndToEnd`).

- **The status is exactly `424`**, and the body is always a k8s-style
  `Status`: `kind`, `apiVersion`, `status: "Failure"`,
  `reason: "FailedDependency"`, `code: 424` and a non-empty `message`.
- **`message` is product copy.** The frontend's matching branch
  (`fix/widget-424-dependency`) renders it verbatim, truncated to 300
  characters, under the title "Data source unavailable". So snowplow
  guarantees the following:
  - It names the failing stage and the missing object, and only the
    request's own coordinates. A stage failure's message is built from the
    request, never from the upstream (#709 A7). An apiserver NotFound names
    only the resource and the object.
  - It never names an identity. The message is replaced by the fixed text
    `The details of this data source can't be shown here.` when it is
    empty, or when any form of it names one. Under the title "Data source
    unavailable" (frontend#500), that subtitle adds what the title does not:
    the detail exists and was withheld, not that it is unknown. It names
    nothing.
    - The forms checked are the message as sent, plus its path-unescaped and
      query-unescaped forms (up to three rounds, for double escaping), all
      case-folded.
    - The caller's identity is checked in all of these: the username, its
      DNS-1123 form, its local part before `@`, and every group (`system:`
      groups included).
    - So are identity shapes, whoever they belong to: a requester
      (`User "…"`), a `-clientconfig` Secret (quoted, unquoted or in a
      path), `CN=`, `username:` / `user=`, `token=`, `Bearer …`, a JWT, and
      any `system:` name.
  - Why it matters: a stage's 404 message names the object in the stage's
    path, and a RESTAction path can be templated from the caller's identity.
    #288 refuses to dial a name that is not DNS-1123-valid, so names with `@`
    or `%40` never get that far. The DNS-valid forms do: a lower-cased group,
    the local part, the DNS form. On main before #856, those reached the 404
    body, which the SPA shows as the `WidgetNotFound` subtitle. #856 closes
    that for the 424.
  - Tests cover it:
    - every rule above, row by row, with one mutation per rule;
    - the reachable forms end to end, through a real identity-templated
      stage;
    - an upstream 404 stuffed with the caller's username, group, clientconfig
      Secret and certificate CN.
  - It carries no stack text. It is a single Status message.
- No refresh headers (`X-Snowplow-Refresh-Key` / `X-Snowplow-Refresh-Class`)
  are set on any error answer, before or after #853.
- The widget CR's own 404 is unchanged: `reason: "NotFound"`, `code: 404`.

## 3. What the SPA does with it today (traced, no change needed)

1. `fetchWidget` gets `res.ok === false`.
   - `res.status` is `424`, not `401`, so the session-resume flow is not
     raised.
   - `readErrorDetail` hoists `message` into `WidgetFetchError.detail`.
   - The thrown error has `status = 424` and the message
     `Widget fetch failed: 424 Failed Dependency`.
2. `shouldRetryWidgetFetch` returns `false`, because 424 is a 4xx other than
   404. **No retry, no backoff**: the error renders on the first answer instead
   of after about 4.9 s.
3. `isTimeoutError` returns `false`: the status is not 503/504, and the message
   has none of `deadline`, `timeout`, `timed out`, `canceled`, `cancelled`,
   `aborted`, `503` or `504`. So it is not the calm timeout state.
4. `WidgetRenderer` reaches its generic error branch, because 424 is neither
   403 nor 404. It renders `<WidgetError subtitle={detail} onRetry={refetch} />`
   in **this widget's slot only**:
   - the subtitle is snowplow's message;
   - the user has a manual Retry button.

   The rest of the page renders normally. The page is not blanked, even when the
   failing widget is a page-level widget: its slot shows the error card.
5. `components/Autopilot/useAutopilotContext.ts` (`fetchErrorOf`, around line
   519) also reads the failed fetch's `detail` and `status` into the page
   context it hands the model. It is unaffected: it gets status `424` and
   the same identity-free message.

## 3a. The chosen 424 state (frontend `fix/widget-424-dependency`, `8f096d1`)

The owner chose this rendering. It was traced in that branch's
`WidgetRenderer.tsx` and `WidgetStates.tsx`:

- `WidgetRenderer` handles `status === 424` before the generic error branch. It
  renders `WidgetDependencyMissing`:
  - an antd `Result` with `status='info'`, titled "Data source unavailable";
  - `subTitle` set to this response's `message`, verbatim, up to 300
    characters. The fallback, when there is no message, is "Something this
    needs is not installed on this cluster.";
  - a primary "Check again" button that calls `refetch()` for that widget's
    query only.
- It is not the red `WidgetError`, and not `WidgetNotFound`. Without the
  branch (the SPA at `21f14bd`), a 424 shows the red `WidgetError` with Retry
  (section 3, step 4). Either way it is not retried automatically.

**Recovery in an open tab.**

- An error response is never armed for `/refreshes`. The handler sets
  `X-Snowplow-Refresh-Key` / `X-Snowplow-Refresh-Class` only on a served body
  (an L1 hit or a resolved 200), never on an error answer. So no refresh frame
  will ever announce that the missing dependency appeared.
- A tab sitting on the 424 state recovers only when the user presses "Check
  again" or navigates (a new query).
- Automatic recovery, when the missing dependency appears, is a design question
  tracked in #858.

Snowplow's wire contract is the same whichever state renders it.

## 4. Edge cases

- **The CR is not in a cold informer yet.** This is still `404` from the CR
  read, so the SPA's 404 retry keeps doing its job for it. #853 does not touch
  that path.
- **A refetch triggered by a refresh frame (#256).**
  - If the widget CR was deleted, it is still `404`, and `eventTriggered` still
    fails fast.
  - A deleted widget never reaches the dependency path. The CR read fails
    first, so the resolve does not run, even when the widget's dependency is
    missing too.
  - A test pins this with a widget that is really deleted. It runs the real
    CR read: informer miss, then the apiserver fallthrough, which answers 404.
    It covers a healthy, cached widget and a widget whose dependency is
    missing; in the second case the widget answers 424 before the delete and
    404 after it.
  - Two logic-only mutations each turn that test red: answering the CR read's
    404 as 424, and routing it through the dependency encoder. Without that
    guard, a bulk delete would silently go back to 4 requests per widget.
  - The refetch a refresh frame triggers is a plain `GET /call`; the server
    answers it exactly as any other request, so the test drives that request.
  - If a dependency was deleted, the answer is now `424`, which also fails
    fast. No new handling is needed.
- **A 410 on the widget CR read itself.** This is not a dependency failure and
  never becomes 424.
  - On the real read path (informer miss, then the caller's apiserver),
    `objects.Get` maps only NotFound to 404 and Forbidden to 403. Every other
    apiserver error, a 410 included, becomes 500, unchanged by #853, and the
    resolve never runs. The SPA retries that 500 as transient.
  - A CR read that does produce a 410 Status is answered 410 as it is, which
    the SPA treats as a permanent 4xx.
  - Both are pinned by `TestIssue853_WidgetCRRead410_IsNot424`.
- **During a roll (mixed versions).** For about one roll's length, the same
  widget can get `404` from an old pod and `424` from a new one.
  - On a 404 the SPA retries for about 4.9 s and then shows `WidgetNotFound`
    ("Not found").
  - On a 424 it shows the 424 state at once: "Data source unavailable" with
    the frontend branch, otherwise the red `WidgetError`.
  - So a widget with a missing dependency can flicker between the two states
    from one load to the next, the 404 one arriving about 5 s later.
  - This is cosmetic and heals itself once every pod is new. No data is
    wrong, and nothing is cached on either answer.
- **A dependency answers 403.** This stays `403`, so the calm `WidgetForbidden`
  state is unchanged.
- **A dependency answers 5xx or 429.** This stays the same code and is still
  retried as transient; 503/504 still show `WidgetTimeout`.
- **An older snowplow.** It answers 404 for a missing dependency, and the SPA
  behaves exactly as today. No capability token is needed, because the SPA's
  existing branches already cover both answers.
- **Dashboards and alerts (ClickStack).** Anything that counts widget `/call`
  404s will see the missing-dependency share of them move to 424. That is any
  ClickStack panel or alert built on the response's HTTP status. The
  `dispatcher.call.complete` log line is not one of them: it carries no status
  code. A widget whose CR is missing still counts as 404. A panel that reads "404 = broken widget" should add 424 to see the
  same population, and can now tell the two apart.

## 5. Optional: pin the behaviour with a test

The SPA already behaves correctly. If you want a regression guard next to the
existing `shouldRetryWidgetFetch` cases:

```ts
import { describe, expect, it } from 'vitest'

import { isTimeoutError } from '../components/WidgetStates'

import { shouldRetryWidgetFetch, WidgetFetchError } from './useWidgetQuery'

describe('snowplow #853: 424 Failed Dependency', () => {
  const err = new WidgetFetchError('Widget fetch failed: 424 Failed Dependency', 424, 'resolve apiRef …: "agents.kagent.dev" not found')

  it('is not retried, on a first failure or after a refresh frame', () => {
    expect(shouldRetryWidgetFetch(0, err)).toBe(false)
    expect(shouldRetryWidgetFetch(0, err, true)).toBe(false)
  })

  it('is not classified as a timeout', () => {
    expect(isTimeoutError(err)).toBe(false)
  })

  it('a missing widget CR (404) keeps the cold-informer retry', () => {
    expect(shouldRetryWidgetFetch(0, new WidgetFetchError('Widget fetch failed: 404 Not Found', 404))).toBe(true)
  })
})
```
