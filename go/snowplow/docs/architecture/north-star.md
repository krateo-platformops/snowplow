---
type: Architecture
title: snowplow — north-star performance contract
description: The performance contract the cache project exists to satisfy — what is aspirational vs what is implemented and enforced today by the Playwright bench harness, and exactly how it is measured.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [performance, benchmark, north-star, ledger, playwright]
timestamp: 2026-08-06T00:00:00Z
---

# North-star: the performance contract and how it is measured

This is the contract the snowplow cache project exists to satisfy, and — more
importantly — the *exact* harness that decides whether a build meets it. The
north-star is **aspirational by nature**; this document separates, explicitly,
what is **implemented and enforced today** from what remains **aspirational**.
Every number below is traced to the code that encodes or enforces it (the
harness has not changed since these anchors were verified; the `file:line`
references are current).

The harness is the Python package under `e2e/bench/bench/` (driven by
`python -m bench`); it drives a **real headless Chromium via Playwright**, not
`curl`.

## Implemented vs aspirational — the summary

**Implemented today (real, enforced):**

- The full measurement harness: real-Chromium page-load `waterfallMs`, the
  cold/warm/fresh methodology, sample-validity gating, the canonical ledger
  row + frozen schema, per-stage proofs, resumable runs (§2, §4).
- The mix-weighted scoring (0.95 narrow-RBAC + 0.05 admin) (§3).
- The enforced verdict tiers: **2200 ms cold / 1000 ms warm-p50 / 30000 ms
  convergence**, zero restarts, `FLOOR` detection for a broken cache toggle,
  the S7 delete-convergence band+slope gate (§1, §2).

**Aspirational (stated goal, NOT the enforced gate):**

- **1000 ms cold / 500 ms warm / 1000 ms fresh** at 50K compositions × 1000
  users. Retained as desiderata; the tiers were relaxed to the
  measured-achievable floor only after data proved the residual warm latency
  is the SPA fetch-waterfall fan-out (a frontend-scoped limit), with
  per-call server compute sub-millisecond.
- A canonical re-captured baseline: the in-tree `.baseline.json` is a
  semantic-mismatch placeholder (§4), so the ±15% regression gate is not yet
  binding.

---

## 1. The contract

| Metric | Aspirational target (north-star) | Enforced verdict tier (current harness) | Where the tier is encoded |
|---|---|---|---|
| **Cold** page-load (first nav, empty browser/cache) | 1000 ms | **2200 ms** | `e2e/bench/bench/ledger.py:385` `COLD_TIER_MS = 2200` |
| **Warm** page-load p50 (repeat nav, cache hot) | 500 ms | **1000 ms** | `e2e/bench/bench/ledger.py:384` `WARM_P50_TIER_MS = 1000` |
| **Fresh** / convergence p99 (serve-stale settle after a mutation) | 1000 ms | **30000 ms** (at 50K) | `e2e/bench/bench/ledger.py:383` `CONV_TIER_MS = 30000` |
| Pod restarts during the run | 0 | 0 (any restart ⇒ `FAIL`) | `e2e/bench/bench/ledger.py:481-482` |

Operating point the contract is asserted at:

- **Scale = 50 000 compositions.** The scale is a run parameter (`--scale` /
  `SCALE`, resolved in `e2e/bench/bench/cli.py`; default 5000). The 50K
  operating point is hard-wired into the EXPECTED_CALLS calibration comments
  and formula, e.g. `e2e/bench/bench/expected.py:49`
  (`admin S6 (N=50K): actual_calls=30 → 10 + 4×min(50000, 5)`).
- **1000 concurrent users.** The user-scaling ramp tops out at 1000 synthetic
  users: `USER_COUNTS = [10, 50, 100, 500, 1000]` at
  `e2e/bench/bench/lifecycle.py:1394`; the full cold-start warmup is run at
  `max(USER_COUNTS)` = 1000 (`e2e/bench/bench/storm.py`, "Full warmup with
  1000 users (cold start)"). User scaling is Phase 7 (`storm.py`); the scored
  latency tiers are measured in Phase 6 (`phases.py`).
- **Mix weighting = 0.95 narrow-RBAC + 0.05 admin.** See §3.

> **Verdict tiers ≠ north-star.** The aspirational 1000ms-cold / 500ms-warm /
> 1000ms-fresh targets are explicitly retained as desiderata and were *relaxed
> to the measured-achievable floor only after data proved a structural limit*
> outside the snowplow+chart control surface. The derivation is recorded inline
> in `e2e/bench/bench/ledger.py:329-385`: per-call server compute is
> sub-millisecond; the residual warm page latency (~908–914ms p50) is the SPA
> fetch-waterfall fan-out (browser↔ingress RTT × wave count + render), which is
> a frontend problem, not server overhead. The tiers are set to
> `~1.06–1.09× worst-observed clean run` (run-variance headroom), not to a
> magic number.

---

## 2. How it is measured — real Chrome page-load, not curl (implemented)

The single load-bearing measurement is **`waterfallMs`**: the wall-clock span of
the `/call` request waterfall observed *inside a real Chromium page*, from the
first `/call` resource-timing entry's start to the last entry's end.

It is computed in browser JS via the Resource Timing API, evaluated inside the
Playwright page:

- `e2e/bench/bench/browser.py:2039-2084` — `page.evaluate(...)` reads
  `performance.getEntriesByType('resource')`, filters to `/call`, and returns
  `waterfallMs = Math.round(last - first)` where `first` is the first call's
  `startTime` and `last` is `max(startTime + duration)` across all `/call`
  entries.
- The navigation itself is a real browser navigation:
  `page.goto(f"{FRONTEND}{page_path}", wait_until="domcontentloaded", …)`,
  followed by a `/call`-count stability poll so the waterfall is read only
  after the SPA's fan-out settles.

This is **deliberately a page-load, not a `curl p50`**: the harness does not
read the `/call` performance timeline via a synthetic client — the metric is
the browser's own resource-timing view, which is why warm latency is dominated
by the SPA's per-card / per-child fetch waves (the fan-out), exactly the
user-felt page-load latency the north-star is scoped to.

### Cold vs warm vs fresh

- **Cold** = the first navigation of a stage. **Warm** = every subsequent
  navigation (the cache is hot). The split is set per nav inside
  `browser_measure_stage`'s nav loop:
  `cold_warm = 'COLD' if nav_num == 1 else 'WARM'`. Default `num_navs=3`, so
  each (stage, user, page) cell yields 1 cold + 2 warm samples. The cold nav
  also sets `min_calls` so warm navs don't exit the stability poll early.
- **Fresh** (`convergence_ms`) is measured separately: after a cluster
  mutation, `browser_measure_stage` runs a VERIFY poll until the rendered UI
  count AND the API count both equal cluster truth, then records
  `convergence_ms = int((time.time() - verify_start) * 1000)` (`-1` sentinel
  when it never matched). This is the eventually-consistent serve-stale settle
  time, gated by `CONV_TIER_MS` against the S8 p99.

### Sample validity gate

A nav's `waterfallMs` is zeroed (marked `incomplete`) and excluded from
percentiles unless it reached a clean terminal state:

- widget-scoped `.ant-skeleton` count must be 0 (hard fail),
- the actual `/call` count must equal `expected_calls(user, page, n_visible=N)`
  within `EXPECTED_CALLS_TOLERANCE = 0` (`e2e/bench/bench/expected.py:112`),
- enforced in `_validate_widget_terminal_state` (`e2e/bench/bench/browser.py`)
  (`incomplete=True` ⇒ `waterfallMs=0`).

`waterfallMs <= 0` or `incomplete` samples are filtered before percentile
computation in the ledger; a cell with navs but zero valid samples reports
`terminal_fail_rate` and null latencies, which forces the run verdict to
`INVALID`.

### From samples to verdict

Per (user, cache_mode) cell, the ledger computes
`cold_ms = pct(cold_samples, 50)`, `warm_p50_ms = pct(warm_samples, 50)`,
`warm_p99_ms = pct(warm_samples, 99)`. The verdict is then computed from the
**mix-weighted** cell:

- `compute_verdict(...)` at `e2e/bench/bench/ledger.py:436-520`: a tier is
  "missed" only when strictly greater than its threshold; `restarts > 0`
  short-circuits to `FAIL`.
- **PASS** = 0 tiers missed; **WEAK_PASS** = exactly 1 missed; **FAIL** = ≥2
  missed, or a restart, or the S7 delete-convergence band+slope gate fires
  (`S7_CONV_BAND_MS`/`S7_SLOPE_FLOOR_PER_S`, `ledger.py:425-426`).
- `FLOOR` = the deployed chart has no working `CACHE_ENABLED` toggle (cache-ON
  cells are zero while cache-OFF cells have data).

---

## 3. The mix-weighting (0.95 narrow-RBAC + 0.05 admin) (implemented)

The scored latency is **not** admin's; it is weighted toward the narrow-RBAC end
user, because the north-star is the *frontend UX of a normal portal user*, not an
operator. Two real subjects are measured:

- `admin` — broad RBAC (sees all 50K compositions; 4 widgets/card),
- `cyberjoker` (`cj`) — narrow RBAC (sees only its namespace; 2 widgets/card
  because the 2 Button widgets come back `allowed=false` and are filtered by the
  SPA) — see `e2e/bench/bench/expected.py:67-70`.

The mix is applied per field in `build_canonical_ledger_row`:

```
mix_weighted[field] = round(0.95 * cyber + 0.05 * admin)
```

(`e2e/bench/bench/ledger.py` `_mw_pick`, declared in the module docstring:
*"Mix-weighted = 0.95 * cyberjoker + 0.05 * admin"*.) For each field the picker
prefers the cache-ON cell and falls back to cache-OFF; if only one subject has
data the other's weight collapses to it. When `cj` has no navs its cell is
mirrored from admin first.

The four cells (`admin_on`, `admin_off`, `cyber_on`, `cyber_off`) are built in
the ledger; the mix-weighted dict (`cold_ms`, `warm_p50_ms`, `warm_p99_ms`) is
what `compute_verdict` consumes.

---

## 4. The measurement ledger (implemented; baseline capture pending)

Each scored run emits a canonical ledger row (the durable record of whether the
contract was met):

- **Builder:** `build_canonical_ledger_row(...)` in `e2e/bench/bench/ledger.py`
  (the `cells` / `mix_weighted` / `verdict` assembly described in §2–§3).
- **Schema (frozen):** `e2e/bench/bench/ledger_row.schema.json` is the
  machine-readable surface for the row.
- **Run bundle:** `write_run_bundle(...)` serializes the full run tree
  (proofs, per-stage `pod_logs/S<N>.txt[.gz]`, screenshots, `--video` `.webm`/
  `.gif`); nothing is dropped — the 200MB cap was removed.
- **Per-stage proofs:** every stage persists a `StageProof` to
  `proofs/S<N>.json` with a required `what_breaks_if_skipped`
  (`e2e/bench/bench/phases.py`); `state.json` round-trips the run so
  `--from-stage` can resume.
- **Baseline / regression gate — PROVISIONAL:** the last-known-good is pinned
  in `e2e/bench/.baseline.json` (`baseline_warm_p50_ms = 2233`, captured
  2026-06-02 against the *portal-chart Chrome-MCP `lastCall_ms`* metric, whose
  own `_notes` field flags it as a *semantic-mismatch placeholder* for the
  field-literal `mix_weighted.warm_p50_ms`). `read_baseline` /
  `compute_baseline_delta` apply a ±15% gate — but treat it as provisional
  until a full Phase-6 lifecycle re-captures the canonical baseline. This is
  the one piece of the ledger machinery that is *not* yet grounded in a
  canonical measurement.
- **EXPECTED_CALLS overlay freshness:** a scored run refuses to start if the
  calibration overlay at `{runs root}/calibration/expected_calls.json`
  is older than 14 days (`overlay_freshness_or_die`,
  `e2e/bench/bench/expected.py`); freshness is the file's `st_mtime`, not a
  forgeable JSON field.

---

## 5. Invariants and how a regression shows up

**Invariants the harness defends:**

1. **The scored metric is browser page-load latency, not synthetic client
   latency.** `waterfallMs` is read from the Chromium Resource Timing API. A
   change that "improves" a `curl`/server p50 but adds a fetch wave to the SPA
   fan-out moves nothing the gate measures — the gate only sees the browser
   waterfall span.
2. **The end user, not the operator, sets the bar.** 95% of the weight is the
   narrow-RBAC `cj`. A regression that only hurts admin barely moves the
   verdict; one that hurts `cj` is amplified 19×.
3. **Cache must stay a transparent toggle.** If cache-ON shows no benefit while
   cache-OFF has data, the verdict is `FLOOR`, surfacing a broken/absent
   `CACHE_ENABLED` rather than silently passing.
4. **Tiers move only on proven structural limits.** The relaxations from
   1000→2200 (cold) and 500→1000 (warm) carry inline derivations citing the
   measured floor + headroom (`ledger.py:329-385`); the aspirational targets
   are retained, not deleted.
5. **No restarts, zero call-count drift.** `restarts > 0` ⇒ `FAIL`; `/call`
   count must hit `expected_calls(...)` exactly (`EXPECTED_CALLS_TOLERANCE =
   0`) or the sample is voided.

**How a regression surfaces:**

- **Added SPA fetch wave / lost cache hit** → warm `waterfallMs` p50 climbs;
  `mix_weighted.warm_p50_ms > 1000` ⇒ one miss (`WEAK_PASS`), two misses
  (e.g. cold too) ⇒ `FAIL`.
- **Cold-path regression** → `cold_ms > 2200`.
- **Serve-stale / refresher regression** → S8 convergence p99 `> 30000ms`
  counts as a miss; a *delete-path* refresher stall is caught specifically by
  the S7 band+slope gate (above the 270s band AND drain slope `< 3.0/s`) ⇒
  hard `FAIL`.
- **Cache toggle broke** → `FLOOR`.
- **Pod crash / OOM under 50K×1000** → restart count > 0 ⇒ `FAIL`; also
  surfaced as mid-stage re-roll annotation (`phases.py`).
- **Unusable samples** (skeletons never resolved, call-count drift, terminal
  fail) → cells null, `navs_terminal_fail > 0` ⇒ `INVALID`, i.e. the run is
  thrown out rather than scored — a regression can't hide behind a
  half-rendered page.

---

## Appendix — discrepancies between the dated bench notes and the live harness

The dated notes under `docs/` (`bench-path-b-*`, `path3-*`, `task-*`) are leads;
the harness is authoritative. Verified divergences:

- **Cold tier 1300 vs 2200.** Several notes/comment fragments reference a cold
  tier of 1300ms (the original Task #121 value). The live constant is
  **`COLD_TIER_MS = 2200`** (`ledger.py:385`), re-baselined 2026-06-11 for the
  lazy-context two-window cold methodology. The inline comment at
  `ledger.py:358-367` documents the 1300→2200 change; 1300 now applies only to
  pre-methodology rows.
- **Aspirational vs enforced tiers.** `ARCHITECTURE.md`'s north-star bullet and
  several notes state "1s cold / 500ms warm / 1s fresh". Those are the
  *aspirational* targets (correct as the contract's stated goal) but are
  **not** the values the gate enforces — the harness enforces 2200 / 1000 /
  30000 (`ledger.py:383-385`), explicitly relaxed-on-proof. This doc states
  both.
- **`.baseline.json` metric mismatch.** See §4 — the ±15% baseline gate becomes
  binding only once a full Phase-6 lifecycle re-captures it.
- **`docs/scaling-roadmap.md` is not in the current tree**, so it was not used
  as an anchor; the 50K / 1000-user operating point is grounded in
  `expected.py`, `lifecycle.py`, and `storm.py` instead.
