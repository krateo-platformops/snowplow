---
type: Decision
title: "ADR 0006 — Snowplow owns the external api-step HTTP fetch (accept YAML as well as JSON)"
description: >-
  The external api-step branch bypasses plumbing's JSON-only request.Do with a
  snowplow-owned transcription that reuses plumbing's security helpers verbatim
  and accepts YAML or JSON. The external cache posture has since been made
  explicit: an external-touched resolve declines the L1 Put by default, with a
  per-widget bounded-TTL opt-in annotation.
resource: snowplow
tags:
  - adr
  - restaction
  - external-endpoints
  - plumbing
status: diverged
timestamp: 2026-08-06T00:00:00Z
---

# ADR 0006 — Snowplow owns the external api-step HTTP fetch (accept YAML as well as JSON)

- **Status:** Accepted (shipped in snowplow 1.2.0); the transport decision is implemented as
  recorded, the *cache posture* consequence has since been superseded by an explicit
  external-no-cache mechanism with a bounded-TTL opt-in (below).
- **Lead (verify against code):** `internal/resolvers/restactions/api/external_fetch.go`
  (`httpFetchAllowingNonJSON`), wired at the external branch of
  `internal/resolvers/restactions/api/resolve.go`.
- **Deep dive:** [`request-lifecycle.md`](../architecture/request-lifecycle.md) §2.9.

## Context

A `RESTAction` `spec.api[]` stage that targets an external `endpointRef` is dispatched
through `github.com/krateo-platformops/plumbing` `http/request.Do`. That function **rejects any
non-JSON `Content-Type` with HTTP 406 *before* the `ResponseHandler` is invoked**
(the gate in `http/request/request.go`, present through the currently-pinned `v1.13.0`), and
its handler type is `func(io.ReadCloser) error` — the `*http.Response`/headers are **not**
passed to snowplow. snowplow does not patch plumbing for this (kept a shared, unforked library).

The portal Marketplace blueprint-discovery RA must GET a Helm repo `index.yaml` — which repos
commonly serve as `text/plain` / `text/yaml` / `application/x-yaml`. Under the plumbing path that
body is 406'd and never reaches jq, so the RA cannot consume it. More generally, no RESTAction api
step could consume *any* YAML endpoint.

## Decision

**For the external api-step branch, snowplow owns the HTTP round-trip** rather than calling
plumbing's gating `Do`. `httpFetchAllowingNonJSON` (`external_fetch.go`) is a faithful
transcription of `plumbing` `request.Do` **minus only the 406 JSON gate**, and it **reuses every
security-critical path verbatim** via plumbing's *exported* helpers:

- `HTTPClientForEndpoint` — TLS, custom CA, client certs, proxy, timeouts, **and** the
  bearer / basic / AWS-SigV4 auth roundtrippers.
- `util.NewRetryClient` / `RetryClient.Do` — QPS limiter + 429/5xx retry.
- `ComputeAwsHeaders` — SigV4 header pre-compute.

Only ~40–50 LOC of pure *request assembly* + the non-2xx → `response.Status` error-envelope
shaping (transcribed byte-identical) is snowplow-local. With the response in hand, snowplow now
sees the `Content-Type` and **accepts JSON or YAML transparently** — a three-step sniff: explicit
YAML content-type → `sigs.k8s.io/yaml` `YAMLToJSON`; a JSON-parsing body → passthrough unchanged;
a body that fails `json.Unmarshal` → last-resort `YAMLToJSON` (covers YAML served as
`text/plain`). `YAMLToJSON` round-trips valid JSON losslessly, so the JSON fast-path is
byte-identical.

Since the original decision, the same external branch also gained **templated `endpointRef`
support** (#113, `evalEndpointRef` in `resolve.go`): the `endpointRef.name` may be a jq template,
with two hard guardrails — the *namespace* stays the author-literal (never templated), and a
templated name resolving to the reserved `-clientconfig` suffix is refused before any Secret
lookup fires.

## Consequences

- **A RESTAction api step can consume any YAML *or* JSON external endpoint** (Helm `index.yaml`,
  `Chart.yaml`, any `*.yaml`). Purely **additive**: non-JSON was a hard 406 before, so nothing that
  worked previously changes (the application/json control is byte-identical).
- **The security surface stays minimal.** TLS / CA / client-certs / bearer-basic-AWS creds / proxy
  / retry all remain delegated to plumbing's exported helpers; only stable request *assembly* is
  transcribed. A future plumbing change to assembly must be mirrored on a plumbing bump (the
  transcription was taken from plumbing v0.9.3 and re-verified unchanged at the pinned v1.13.0);
  the volatile parts evolve under us for free.
- **One new, bounded error edge.** A 2xx body that is neither JSON nor YAML now reaches the decode
  and errors there (instead of an upstream 406) — caught by the per-item error path
  (`recordItemError`), no panic, no credential leak (the request still carried creds via the reused
  roundtrippers).
- **Cache posture — now an explicit mechanism, no longer a side-effect.** External results have no
  informer/dep edge to invalidate them, so by default they are **not** L1-cached — but this is now
  *enforced*, not incidental: the resolve threads an external-touched sink
  (`cache.WithExternalTouchedSink`; bumped whenever a stage reaches `httpFetchAllowingNonJSON`),
  and the dispatcher Put-gates (`restactions.go` / `widgets.go` / the seed's external-bound skip)
  **decline the Put** for any external-touched result — the body is served, every `/call`
  re-fetches the external API live. One sanctioned exception exists: a widget CR may opt in via
  the `krateo.io/external-cache-ttl-seconds` annotation
  (`internal/handlers/dispatchers/external_ttl.go`), which converts the decline into a
  bounded-staleness Put under the unchanged per-binding `widgets` key (hard-capped at 120s,
  `ExternalTTL`-marked so the entry never arms a `/refreshes` subscription). Correctness basis for
  that path is time-bounded staleness, not dep-edge invalidation.
- **Toggle-able / removable** per ADR 0004: the conversion is on the live external path; under
  `CACHE_ENABLED=false` the same fetch runs (the cache toggle is orthogonal).

## Related

- ADR 0004 — caching is provisional and removable.
- The companion correctness fix shipped alongside (snowplow 1.2.0): a per-item **feed/decode error
  on any served dispatch branch records into `errorKey` and does *not* truncate the resolve** (the
  #313 Option C-A contract). Its cache-side halves are live in the dispatcher: a stage-error
  resolve serves 200 but declines the Put (the optional `PARTIAL_RESULT_TTL_SECONDS`
  bounded-stale backstop that once accompanied the decline was retired in 1.12.6 C9) — see
  [`request-lifecycle.md`](../architecture/request-lifecycle.md) §3.
