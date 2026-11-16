---
type: Architecture
title: Corpus audit — `/call`-loopback retirement blast radius (2026-06-22)
description: "Corpus audit gating the /call-loopback retirement: zero live api-step usages found; retirement is dead-code removal."
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [archive]
timestamp: 2026-06-22T00:00:00Z
---

# Corpus audit — `/call`-loopback retirement blast radius (2026-06-22)

**Purpose:** the durable, inspectable artifact behind the empirical claims that gate (C) retiring the `/call` loopback and (B) shipping `spec.api[].resolve` default-true. Prerequisite for the unified ship in `external-ra-no-l1-cache-proposal-2026-06-22.md`. Author: cache-architect (arch-corpus-audit). All findings TRACED.

**Verdict:** big-bang is **FEASIBLE as a snowplow-ONLY ship; portal-chart needs NO change.** Retiring the loopback is **dead-code removal** (it fires on zero paths today). The one trap is a shared parser — see §2/§5.

## Corpus scanned
- `~/krateo/krateo-portal-chart/chart/templates/*.yaml` — all 35 `path:` values across RESTAction + widget + resourcesRefs + button/nav templates.
- `~/krateo/snowplow-cache/snowplow/testdata/**` (RA fixtures + curl samples) and in-tree RA fixtures.
- snowplow internal resolve/walker/prewarm code (`internal/resolvers/restactions/api`, `internal/handlers/dispatchers`).

## §1 — `/call`-as-api-step-path usages (migration work-list): EMPTY
Across all 35 portal-chart `path:` values, **zero are `/call?…` api-step paths.** Every `/call` token in the chart is a **comment** (layout.app-shell.yaml:1, restaction.compositions-panels.yaml:15, restaction.compositions-list.yaml:12/18, restaction.global-search.yaml:4, list.search-results.yaml:3, form.blueprint-create.yaml:13, configmap.blueprint-drafts.yaml:4). No `${…}`-templated path builds a `/call` URL (all `path:` lines are `/apis/…`, `/api/…`, or SPA nav routes). **No author RA needs rewriting.**

## §2 — Internal/walker dependency on the loopback resolve branch: NONE (the make-or-break)
Two mechanisms were historically conflated under "the `/call` machinery"; they are independent:
- **(A) `/call` URL parse + emission — STAYS (SPA + walker contract).** `resourcesrefs/resolve.go:155-176` (`buildPath`) emits `Path: "/call?resource=…"` into every widget's `status.resourcesRefs[].path`. The SPA issues these as follow-up HTTP `/call`s; the F2 walker reads them purely to extract `(GVR,ns,name,page)` via `objects.ParseCallPathToObjectRef` (phase1_walk.go:1335) → `objects.Get` (:1383) + recurse (:1407) — an informer GET, **not** a loopback resolve. Other decode-only callers: phase1_roots.go:166, phase1_walk_pagination.go:535, phase1_walk_metrics.go:105, widget_content.go:537.
- **(B) the loopback *resolve* branch — being retired.** `api/resolve.go:625-700` → `ResolveNestedCall` (nested_call.go:60). Reached **only** when a RESTAction's own api-step `path` is a `/call?…` shape (gated by `ParseCallPathToObjectRef(call.Path)` at resolve.go:627). Per §1, **no RA has such a stage path** → branch B fires on **zero paths today.**

**Conclusion:** retiring branch B does NOT break the walker, PIP seed, or content-prewarm — they use mechanism A (parse), never branch B (resolve). The "loopback is the hard prerequisite for F2 SA-prewarm" note in `nested_call_seam.go:20-21` / `resolve.go:611` was true for the historical `exportJwt` `/call` stage (0.30.120) but is **incorrect for the current corpus** — no RA does that today.

## §3 — Raw RA/widget CR fetches (resolve default-true flip review): ZERO realized flips
The only RA/widget-touching fetches both **LIST** a widget GVR (no single-CR GET by name) and project to metadata only:
- restaction.compositions-panels.yaml:34 — `/apis/widgets.templates.krateo.io/v1beta1/panels` (LIST); filter → `{name,namespace,uid,creationTimestamp,labels,apiVersion,kind}`.
- restaction.global-search.yaml:42 — `/apis/widgets.templates.krateo.io/v1beta1/cards` (LIST); filter → `{name,ns,title}`.
A grep for any single-CR fetch of `…restactions|widgets|panels|cards…/<name>` across the chart returned **empty**. `resolve:true` resolves a *fetched RA/widget object*; these are LISTs whose filter discards all but metadata, so even item-wise resolution is invisible to the consumer. **Flipping `resolve` default-true changes nothing in the corpus** — default-true ships safe, no phased opt-in needed (release note is courtesy).

## §4 — Snowplow test fixtures with `/call`
The ~9 testdata `/call` occurrences are **external client→snowplow curl docs** (testdata/curl-samples.txt, testdata/pagination/curl.txt) — HTTP URLs to the public `/call` endpoint, NOT RA api-step paths. The `/call` HTTP endpoint is **not** retired → none need changing.
**Go tests exercising branch B (update/retire on C):** nested_call_falsifier_test.go, nested_call_testshim_test.go, resolve_extraction_parity_test.go, peer_dispatch_feed_error_test.go (loopback arm), refresher_stage_error_falsifier_test.go (loopback arm), external_fetch_falsifier_test.go (loopback bits).
**A-side tests that MUST stay green (the A⊥B retirement-safety proof — do NOT touch):** phase1_walk*_test.go, phase1_roots_test.go, phase1_walk_traversal_falsifier_test.go, widget_content_test.go, deps_extract_walk_test.go.

## §5 — Big-bang feasibility + the SHARED-PARSER trap
**Feasible, snowplow-only.** Work-list: (1) portal-chart RA rewrites = NONE; (2) snowplow: ship direct-path+`resolve` + external-no-cache, delete branch B (resolve.go:625-700) + seam + `RESOLVER_INPROCESS_NESTED_CALL` + the 6 branch-B Go tests; (3) update those 6 tests; (4) walker/prewarm change = NONE.

**THE TRAP (hard guardrail):** `objects.ParseCallPathToObjectRef` (`internal/objects/callpath.go`) is a **shared parser** used by branch B AND mechanism-A callers (resourcesrefs/resolve.go, resourcesrefstemplate/resolve.go, widgets/resolve.go). C must delete ONLY the dispatch branch at api/resolve.go:625-700 (+ seam/flag/tests). It MUST NOT delete `ParseCallPathToObjectRef`, `ParseCallPathPagination`, or `buildPath`'s `/call?…` emission, nor any non-resolve caller — doing so breaks the SPA navigation + F2 walker catastrophically.

> **Superseded 2026-10-29 for `ParseCallPathPagination` (#966 / #967):** the guardrail no longer holds for that one function. Since #966 the F2 walker reads a child's pagination through the request's own parser, `paginationFromQuery`, the one `paginationInfo` applies to the browser's `/call` (via `prewarmPaginationFor`). `util.ParseCallPathPagination` was left with no caller and was deleted in #967. `ParseCallPathToObjectRef` and `buildPath`'s `/call?…` emission are unaffected, so the rest of this guardrail stands.

**Retirement-safety proof:** after deleting branch B + its 6 tests, the A-side suite (§4) must stay green — that is the empirical proof mechanism A ⊥ branch B. A named CI gate.
