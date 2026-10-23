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

**No SPA change is needed.** The SPA already handles 424 the way this change
intends. Section 5 has a test you can add to pin that.

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
  "message": "resolve apiRef krateo-system/agent-formdef: ... stage \"crd\": customresourcedefinitions.apiextensions.k8s.io \"agents.kagent.dev\" not found",
  "reason": "FailedDependency",
  "code": 424
}
```

- `message` names the failing stage and the missing object. As before #853,
  it carries only the request's own coordinates, never the caller's identity
  (#709 A7).
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

## 4. Edge cases

- **The CR is not in a cold informer yet.** This is still `404` from the CR
  read, so the SPA's 404 retry keeps doing its job for it. #853 does not touch
  that path.
- **A refetch triggered by a refresh frame (#256).**
  - If the widget CR was deleted, it is still `404`, and `eventTriggered` still
    fails fast.
  - If a dependency was deleted, it is now `424`, which also fails fast. No new
    handling is needed.
- **A dependency answers 403.** This stays `403`, so the calm `WidgetForbidden`
  state is unchanged.
- **A dependency answers 5xx or 429.** This stays the same code and is still
  retried as transient; 503/504 still show `WidgetTimeout`.
- **An older snowplow.** It answers 404 for a missing dependency, and the SPA
  behaves exactly as today. No capability token is needed, because the SPA's
  existing branches already cover both answers.
- **Dashboards and alerts.** Anything that counts widget `/call` 404s (the
  `dispatcher.call.complete` log line, ClickStack panels) will see these
  answers move to 424.

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
