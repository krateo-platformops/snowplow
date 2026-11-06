"""Canonical ledger row builder + report emission (Block 4).

Mix-weighted = 0.95 * cyberjoker + 0.05 * admin (feedback_north_star_is_frontend_ux).
Schema is FROZEN — see source 7109-7133 docstring; the JSON-Schema artifact at
`bench/ledger_row.schema.json` is the machine-readable surface for acceptance (b).

Functions moved (per plan §A.7):
  411-415   record
  411       test_results global  -> _results + add_result/get_results
  7032-7064 _convergence_p99_for_stage  -> convergence_p99_for_stage
  7065-7108 _aggregate_validation       -> aggregate_validation
  7109-7338 _build_canonical_ledger_row -> build_canonical_ledger_row
  7340-7350 _load_per_mutation_metric   -> _load_per_mutation_metric (run_dir-relative now)
  7351-7416 _compute_verdict            -> compute_verdict
  7417-7426 get_runtime_metrics
  8164-8224 print_report
   939-942  pct(data, p)                -> re-exported from bench.cluster.pct

New (Block 4):
  write_run_bundle      — §F directory tree serializer (replaces source 8663-8682)
  compute_verdict_with_falsifier
  emit_ledger_row_schema — writes JSON-Schema 2020-12 covering §F.1 key list
  read_baseline          — read e2e/bench/.baseline.json (acceptance (c))
  compute_baseline_delta — ±15% gate helper (acceptance (c))

Bundle finalize per Task #121 (Diego, 2026-06-10): the 200 MB bundle cap and the
artifact-dropping truncation (#299) are REMOVED. NOTHING is ever dropped from the
run bundle — videos, screenshots, and pod_logs are all retained regardless of total
size — because the final clean re-run is `--video all` at 50K (~1GB+ of video) and
the old cap would trim exactly the videos Diego wants. The only retained behaviour
is the #299 pod_log gzip (S6.txt → S6.txt.gz, ~10-20× smaller): it drops nothing,
only compresses, saving disk while keeping traceability (zcat/zgrep work on the .gz).
`oversize_bundle.json` is now a zero-drop size + gzip-saving report
(`cap_removed: True`, `dropped_count: 0`); summary.json.bundle_truncated is always
False.

Per docs/bench-restructure-path-b-plan-2026-06-02.md §A.7 + §F.
"""

from __future__ import annotations

import datetime
import gzip
import json
import os
import shutil
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

# Deferred — cluster.pct is the same helper from worktree source 939-942.
from bench import evidence
from bench.cluster import kubectl, pct as _pct  # type: ignore

__all__ = [
    "record",
    "add_result",
    "get_results",
    "reset_results",
    "build_canonical_ledger_row",
    "compute_verdict",
    "compute_verdict_with_falsifier",
    "aggregate_validation",
    "convergence_p99_for_stage",
    "s7_min_slope_per_s",
    "pct",
    "get_runtime_metrics",
    "print_report",
    "write_run_bundle",
    "emit_ledger_row_schema",
    "read_baseline",
    "compute_baseline_delta",
    "BASELINE_PATH",
]


# ─── Module-level results buffer (per-process, rename of test_results) ──────


_results: list[dict] = []


def add_result(entry: dict) -> None:
    """Append a result row (stage-runner side); per-process buffer."""
    _results.append(entry)


def get_results() -> list[dict]:
    """Return the per-process results buffer (defensive copy)."""
    return list(_results)


def reset_results() -> None:
    """Clear the per-process results buffer (test fixture hook)."""
    _results.clear()


# ─── ANSI helpers (mirror cli.py constants so reports render uniformly) ─────


_GREEN = "\033[32m"
_RED = "\033[31m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


# ─── record (worktree source 411-415) ───────────────────────────────────────


def record(name: str, passed: bool, ms: int = 0, code: int = 0,
           note: str = "") -> None:
    """Append a result row; matches worktree source 411-415 behaviour."""
    _results.append({
        "name": name, "passed": passed, "ms": ms, "code": code, "note": note,
    })
    tag = f"{_GREEN}PASS{_RESET}" if passed else f"{_RED}FAIL{_RESET}"
    print(f"  [{tag}] {name:<65s} {ms:>5d}ms  HTTP {code:<4}  {note}")


# ─── pct helper (worktree source 939-942) ───────────────────────────────────


def pct(data, p):
    """Nearest-rank percentile. Returns 0 for empty input (legacy behavior).

    The cluster.pct helper returns None for empty; ledger callers historically
    guarded with `if cold_samples else 0`, so we keep the 0 sentinel here
    while delegating the non-empty path to cluster.pct for code re-use.
    """
    if not data:
        return 0
    return _pct(data, p)


# ─── runtime metrics (worktree source 7417-7426) ────────────────────────────


def get_runtime_metrics():
    """Fetch /metrics/runtime from snowplow. Returns dict or None."""
    snowplow = os.environ.get("SNOWPLOW_URL", "http://34.135.50.203:8081")
    try:
        req = urllib.request.Request(snowplow + "/metrics/runtime")
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception:
        return None


# ─── convergence_p99_for_stage (worktree source 7032-7064) ──────────────────


def convergence_p99_for_stage(all_results: Iterable[dict], stage_label: str) -> int:
    """p99 of convergence_ms across navigations for a given stage."""
    def _samples_for(mode):
        out = []
        for entry in all_results:
            if str(entry.get("stage")) != stage_label:
                continue
            if entry.get("cache") != mode:
                continue
            for _, pg in (entry.get("pages") or {}).items():
                for nav in (pg.get("navigations") or []):
                    cm = nav.get("convergence_ms")
                    if isinstance(cm, (int, float)) and cm >= 0:
                        out.append(int(cm))
        return out
    samples = _samples_for("ON") or _samples_for("OFF")
    if not samples:
        return -1
    return pct(samples, 99)


# ─── s7_min_slope_per_s (Task #334-B) ───────────────────────────────────────


def s7_min_slope_per_s(all_results: Iterable[dict]) -> float | None:
    """Min refresh_completed_slope_per_s across S7 Dashboard navs that carry a
    Task #334-A discrimination tuple.

    Returns None when NO nav carries a non-None slope (cache-OFF pods publish no
    expvars, or expvar unreachable / window<=0) -> the #334-B gate FAIL-OPENs on
    None (missing telemetry is not a code-regression). MIN (not mean) so a single
    stalled user trips the gate.

    Mirrors convergence_p99_for_stage's traversal (stage filter + ON-then-OFF
    cache preference) and reads the sibling key nav["conv_discrimination"]
    ["refresh_completed_slope_per_s"]. Purely additive: it does NOT read or feed
    nav["convergence_ms"] (the field convergence_p99_for_stage reads)."""
    def _slopes_for(mode):
        out: list[float] = []
        for entry in all_results:
            if str(entry.get("stage")) != "7":
                continue
            if entry.get("cache") != mode:
                continue
            for _, pg in (entry.get("pages") or {}).items():
                for nav in (pg.get("navigations") or []):
                    tup = nav.get("conv_discrimination")
                    if not isinstance(tup, dict):
                        continue
                    s = tup.get("refresh_completed_slope_per_s")
                    if isinstance(s, (int, float)):
                        out.append(float(s))
        return out
    # ON-then-OFF preference matches convergence_p99_for_stage: cache-OFF runs
    # publish all-None tuples, so _slopes_for("OFF") stays empty -> None ->
    # FAIL-OPEN (the cache-off signal is the None-ness itself).
    slopes = _slopes_for("ON") or _slopes_for("OFF")
    if not slopes:
        return None
    return min(slopes)


# ─── aggregate_validation (worktree source 7065-7108) ───────────────────────


def aggregate_validation(all_results: Iterable[dict]) -> dict:
    """Aggregate widget-terminal-state validation across every nav."""
    summary = {
        "navs_terminal_pass": 0,
        "navs_terminal_fail": 0,
        "skeleton_failures": [],
        "errored_widgets_total": 0,
        "call_count_mismatches": [],
    }
    for entry in all_results:
        for _, pg in (entry.get("pages") or {}).items():
            for nav in (pg.get("navigations") or []):
                v = nav.get("validation")
                if not isinstance(v, dict):
                    continue
                label = nav.get("label") or "<unlabeled>"
                if v.get("terminal_state") == "pass":
                    summary["navs_terminal_pass"] += 1
                else:
                    summary["navs_terminal_fail"] += 1
                # #289a: respect the efaf1a4 skeleton demotion. A skeleton
                # that was demoted to a benign WARN (terminal_state=='pass'
                # AND skeleton_materializing==True — the small-N
                # materialization race) is NOT a failure and must not be
                # reported as one. A skeleton that is still a HARD FAIL
                # (terminal_state=='fail', or any skeleton not carrying the
                # materializing demotion) IS still recorded. Telemetry-only:
                # this does not change the verdict (that keys off
                # navs_terminal_fail, which already reflects the demotion).
                demoted = (v.get("terminal_state") == "pass"
                           and v.get("skeleton_materializing") is True)
                if v.get("skeleton_count", 0) > 0 and not demoted:
                    summary["skeleton_failures"].append(label)
                summary["errored_widgets_total"] += int(
                    v.get("errored_count", 0) or 0)
                # #296: a nav whose call_count_mismatch was DEMOTED to a
                # recorded WARN (terminal_state=='pass' AND
                # s10_churn_demoted==True — S10 controller-churn ghost panels
                # 404'd OUTSIDE the bench-deleted ns) is NOT a mismatch and
                # must NOT be appended to call_count_mismatches. Mirrors the
                # #289a skeleton exclusion above. Without this the ledger is
                # self-contradictory (navs_terminal_fail==0 alongside
                # mismatch tuples). Telemetry-only: the verdict already keys
                # off navs_terminal_fail, which reflects the demotion.
                churn_demoted = (v.get("terminal_state") == "pass"
                                 and v.get("s10_churn_demoted") is True)
                exp = v.get("expected_calls")
                if (exp is not None
                        and not v.get("calls_within_tolerance", True)
                        and not churn_demoted):
                    summary["call_count_mismatches"].append(
                        (label, exp, v.get("actual_calls", 0)))
    return summary


# ─── _load_per_mutation_metric (relocated; worktree source 7340-7350) ───────


def legacy_per_mutation_path() -> Path:
    """Phase 8's results when it was given no run_dir, in the durable runs root (#1194)."""
    return evidence.runs_root() / "phase8" / "per_mutation_results.json"


def _load_per_mutation_metric(key: str, *, run_dir: Path | None = None):
    """Load a Phase-8 metric.

    Block 4 relocation: prefer `{run_dir}/phase8/per_mutation_results.json`
    when `run_dir` is provided; fall back to `legacy_per_mutation_path()`
    so behaviour is identical when no run_dir is supplied.
    """
    candidates: list[str] = []
    if run_dir is not None:
        candidates.append(str(Path(run_dir) / "phase8" / "per_mutation_results.json"))
    candidates.append(str(legacy_per_mutation_path()))

    for path in candidates:
        try:
            with open(path) as f:
                d = json.load(f)
            v = d.get(key)
            if v is not None:
                return v
        except Exception:
            continue
    return None


def _load_stage_proof_metric(stage_id: str, key: str, *,
                             run_dir: Path | None = None):
    """Read a single scalar by-key from a per-stage proof JSON on disk.

    Task #314: the S8b/S8c reconcile stages stamp `conv_widget_mod_ms` /
    `conv_ra_mod_ms` into their `proofs/{stage}.json` proof body. This
    mirrors `_load_per_mutation_metric`'s by-key disk read so the canonical
    ledger row can surface them REPORT-ONLY (no verdict coupling). Returns
    None when run_dir is absent, the proof file is missing/malformed, or the
    key is not present (forward-compatible with pre-#314 runs).
    """
    if run_dir is None:
        return None
    path = Path(run_dir) / "proofs" / f"{stage_id}.json"
    try:
        with open(path) as f:
            d = json.load(f)
        # _write_stage_proof nests the stage payload under "proof".
        body = d.get("proof") if isinstance(d, dict) else None
        if isinstance(body, dict):
            v = body.get(key)
            if v is not None:
                return v
    except Exception:
        return None
    return None


# ─── Latency tier thresholds (ms) ───────────────────────────────────────────
#
# warm_p50 (1000) and cold (2200) are the REVISED verdict tiers (Task #121,
# Diego-ratified 2026-06-10; cold re-baselined 2026-06-11 — see below). They are NOT north-star: 500ms warm / 1000ms cold
# remain the ASPIRATIONAL frontend-scope target (documented below, NOT deleted).
# These verdict tiers are set to the measured-achievable floor under the
# snowplow+chart control surface, because the warm/cold tiers are STRUCTURALLY
# UNREACHABLE within that scope — proved, not assumed (per
# feedback_north_star_is_desiderata: "relax ONLY when data proves a structural
# limit — the data now proves it"; derivation, not a magic number, per
# feedback_capacity_caps_empirical_per_entry_cost):
#
#   STRUCTURAL PROOF (warm/cold are frontend-gated, not server-gated):
#   - docs/task-288-lane1-measurement-2026-06-10.md: per-call snowplow server
#     compute is sub-millisecond (warm serve stack <1% CPU; the ~914ms warm
#     PAGE p50 is the SPA fetch waterfall = wave-count × per-wave browser↔ingress
#     RTT + render — a FAN-OUT problem on the frontend, NOT server overhead).
#   - docs/task-300-c1-fanout-collapse-design-2026-06-10.md: C1 (the only chart
#     lever, /compositions fan-out collapse) is INFEASIBLE without a frontend JS
#     change (W2+W3 per-card/child fetch is the content-fill mechanism the SPA
#     insists on issuing; verdict D: "warm floor stands at ~908ms"). The residual
#     is frontend/ingress, out of the snowplow+chart scope.
#
#   TIER DERIVATION (measured-achievable floor × run-variance headroom):
#   - warm_p50 measured across 3 clean 50K runs = 914 (0.30.251 run3) /
#     908 (0.30.253) / 890 (0.30.254). Tier 1000 = ~1.09× worst-observed (914),
#     i.e. run-variance headroom over the worst clean run.
#   - cold measured across the same 3 runs = 1196 / 1185 / 1175. Tier 1300 =
#     ~1.09× worst-observed (1196), same headroom basis.
#   - cold RE-BASELINED 1300 -> 2200 (Diego-ratified 2026-06-11): the bench
#     cold METHODOLOGY changed — per-stage lazy recording contexts + the
#     fresh-login two-window flow (commits e6feee1 / 8a69848) start the cold
#     navigation from a colder browser state than the pre-#304 flow did.
#     Mix-weighted cold across the clean 50K validations under the CURRENT
#     methodology = 2043 / 2052 / ~2070 (0.30.255-0.30.257), while warm_p50
#     stayed FLAT (~911 vs the 890-914 band) — proving the shift is the
#     measurement methodology, not a serve-path regression. Tier 2200 =
#     ~1.06× worst-observed (~2070), same headroom basis as #121. The old
#     1300 applies only to pre-methodology rows; 1000 stays aspirational.
#
# CONV_TIER_MS = 30000 is the 50K-scale REVISED convergence tier (Task #289,
# Diego-ratified 2026-06-10). The prior 1000ms tier is structurally
# unreachable at 50K — proved, not assumed (per feedback_north_star_is_desiderata:
# "relax only when data proves structural limit"):
#   - docs/task-282-serve-stale-depth-trace-2026-06-10.md §1: a single
#     compositions-panels LIST re-resolve costs 11.5s p50 / 52.8s p99 at 50K
#     (krateo-system/blueprints-panels: n=57, p50=11,535ms, p99=52,753ms), and
#   - the VERIFY poll cadence imposes a ~22-24s floor observed across every
#     run: convergence_mass_s8_p99 ≈ 23.8s measured (run-20260609-221611,
#     run-20260609-234201, and the prior 0.30.248 run-20260605-114100 all sit
#     in the 22-24s band).
# 30000ms covers the measured ~23.8s p99 + margin. This is the eventually-
# consistent serve-stale contract at scale; #286 (fast-lane) may allow
# tightening this tier later.
CONV_TIER_MS = 30000
WARM_P50_TIER_MS = 1000  # Task #121: 500 -> 1000 (~1.09× worst clean run 914; 500 stays aspirational)
COLD_TIER_MS = 2200      # #121 pattern, re-baselined 2026-06-11: 1300 -> 2200 (~1.06× worst clean run ~2070 under the lazy-context two-window methodology; 1000 stays aspirational)

# ─── Task #334-B: S7 delete-convergence BAND + SLOPE gate constants ──────────
#
# S7's convergence_mass_s7_p99 measures refresher-queue latency for ONE resident
# piechart cell on the delete path — NOT serve latency, NOT cluster delivery.
# #335 (docs/task-335-s7-delete-conv-trace-2026-06-12.md) traced a 31.2s -> 151.5s
# S7 swing across 0.30.263..0.30.264 to CLUSTER controller-churn backlog ahead of
# the piechart cell, NOT code (the 8-commit diff never touched the
# DELETE->evict->dirty-mark->refresher->served-flip path). The load-bearing
# discriminator: the refresher DRAIN SLOPE stayed HEALTHY (baseline ~9-10/s, new
# ~6-8/s) while the new run carried MORE ADD/UPDATE churn — a monotonic CODE
# slowdown DROPS the drain slope, whereas cluster-state churn leaves the slope
# high but pushes conv up via competing dirty work (docs/task-335…:56-73,181-184).
#
# So the ONLY configuration that makes a real code regression FAIL while a
# by-design cluster-state swing PASSes is: FAIL iff conv is OUTSIDE the documented
# serve-stale band AND the drain slope has DROPPED below a floor.
#
# BAND (empirical, not invented): the documented serve-stale convergence class is
# 30-270s (docs/task-280, docs/task-282-serve-stale-depth-trace §4); #335 placed
# BOTH today's S7 numbers (31.2s, 151.5s) inside it.
#
# FLOOR DERIVATION (worst-observed-healthy × safety, per
# feedback_capacity_caps_empirical_per_entry_cost — NOT a design-time estimate):
#   source (TRACED)         | S7-window refresh_completed slope | conv    | healthy?
#   baseline 0.30.263       | ~9-10 /s                          | 31.2 s  | YES
#   new 0.30.264            | ~6-8 /s                           | 151.5 s | YES (cluster-state)
#   #335 series, healthy range = 6-10/s. Worst-observed HEALTHY slope = 6/s.
#   Floor = worst_healthy(6) × safety(0.5) = 3.0 — a 2× headroom below the lowest
#   healthy slope (so the entire 6-10/s healthy envelope clears it), yet well
#   above a stalled refresher (~0-1/s). A regression that merely halves throughput
#   (~3-5/s) brushes the floor; only a clear stall toward 0-1/s trips it — the
#   conservatism is deliberate (#335's whole point is that false-FAIL is the
#   expensive error). This is the conv tier's structural sibling (like CONV_TIER_MS,
#   derived from the measured healthy envelope, not from north-star aspiration).
#
# VERDICT-SEMANTICS NOTE: this is the FIRST verdict input added since the Task #121
# tier re-baseline. Ledger rows written before this commit lack verdict_semantics_rev
# (None) => unambiguously "pre-#334-B semantics" (the gate did NOT gate S7).
S7_CONV_BAND_MS = (30_000, 270_000)   # documented serve-stale class; task-280 / task-282 §4 / #335
S7_SLOPE_FLOOR_PER_S = 3.0            # worst-observed healthy slope 6/s (#335) × 0.5 safety; a stalled refresher -> ~0-1/s

# Series-transition marker stamped into every row built by THIS verdict logic.
# Pre-#334-B rows lack the key (None) -> "pre-#334-B semantics".
VERDICT_SEMANTICS_REV = "2026-06-12/s7-band-slope"


# ─── compute_verdict (worktree source 7351-7416) ────────────────────────────


def compute_verdict(mix_weighted, restarts, conv_s8_p99, cells=None,
                    *, conv_s7_p99=None, s7_slope_per_s=None):
    """Verdict per the architect's gates.

    A tier is "missed" only when the value is strictly GREATER than its
    threshold (the boundary value exactly at the tier is NOT a miss).

    PASS:        warm_p50 <= WARM_P50_TIER_MS, cold <= COLD_TIER_MS,
                 conv <= CONV_TIER_MS, 0 restarts
    WEAK_PASS:   one tier missed
    FAIL:        2+ tiers missed OR restarts > 0 OR the S7 band+slope gate fires
    FLOOR:       measurements present, but the deployed chart has no cache
                 toggle (cache_supported=false). Surfaces as structural N/A.
    REJECT:      pod crashed, no usable measurements

    Tiers: warm_p50 = WARM_P50_TIER_MS (1000ms) revised Task #121; cold =
    COLD_TIER_MS (2200ms; #121 then re-baselined 2026-06-11 for the
    lazy-context cold methodology); conv = CONV_TIER_MS (30000ms at 50K)
    revised Task #289 — see each constant's comment block for the
    structural-limit derivation.

    Task #334-B — S7 delete-convergence BAND+SLOPE gate (kw-only inputs,
    default None so every existing positional caller is byte-compatible). The
    gate is a HARD-FAIL PEER of `restarts > 0` (short-circuits to FAIL; it is
    NOT a +1 to the miss counter — a dropped refresher drain slope on the delete
    path is a correctness/serve-stale regression, categorically worse than a
    latency-tier miss, per feedback_no_park_broken_behind_flag). The truth table
    (conv_s7_p99 = S7 convergence p99; s7_slope_per_s = MIN healthy-window drain
    slope across S7 tuples):
      - below band (conv <= 30000, incl. the -1 no-sample sentinel): PASS, any slope.
      - in band   (30000 < conv <= 270000):                          PASS, any slope.
      - above band (conv > 270000) AND slope >= floor (3.0):         PASS (cluster
        state, by design — annotated by the row builder as s7_above_band_cluster_state).
      - above band (conv > 270000) AND slope <  floor (3.0):         FAIL (the
        code-regression signature: above-band AND dropped drain slope — #335).
      - above band (conv > 270000) AND slope is None:                PASS (FAIL-OPEN;
        cannot discriminate — missing telemetry is not a regression; annotated
        s7_slope_unavailable). This is the ONLY FAIL combination.
    """
    if not mix_weighted:
        return "REJECT"
    wp50 = mix_weighted.get("warm_p50_ms")
    cold = mix_weighted.get("cold_ms")
    if wp50 is None or wp50 <= 0:
        return "REJECT"
    if restarts > 0:
        return "FAIL"

    # Task #334-B: S7 delete-convergence BAND+SLOPE gate. FAIL iff conv is
    # ABOVE the documented serve-stale band AND the refresher drain slope
    # DROPPED below floor (the code-regression signature, #335). Above-band +
    # healthy slope = by-design cluster-state -> PASS (annotated by the row
    # builder). None slope -> FAIL-OPEN (cannot discriminate; do NOT fail on
    # missing telemetry). The -1 no-sample sentinel from convergence_p99_for_stage
    # is < band upper edge -> never enters this branch. Hard-FAIL peer of
    # restarts>0 (NOT a miss-counter increment), so rows 1/2/3/5 keep the
    # warm/cold/conv_s8 tier arithmetic completely unchanged.
    if (conv_s7_p99 is not None and conv_s7_p99 > S7_CONV_BAND_MS[1]
            and s7_slope_per_s is not None
            and s7_slope_per_s < S7_SLOPE_FLOOR_PER_S):
        return "FAIL"

    def _wp50(c):
        v = (c or {}).get("warm_p50_ms")
        return v if v is not None else 0

    if cells:
        on_zero = (_wp50(cells.get("admin_on")) <= 0 and
                   _wp50(cells.get("cyber_on")) <= 0)
        off_nonzero = (_wp50(cells.get("admin_off")) > 0 or
                       _wp50(cells.get("cyber_off")) > 0)
        if on_zero and off_nonzero:
            return "FLOOR"
    misses = 0
    if wp50 > WARM_P50_TIER_MS:
        misses += 1
    if cold is None or cold > COLD_TIER_MS:
        misses += 1
    if conv_s8_p99 is not None and conv_s8_p99 > CONV_TIER_MS:
        misses += 1
    if misses == 0:
        return "PASS"
    if misses == 1:
        return "WEAK_PASS"
    return "FAIL"


def compute_verdict_with_falsifier(mix_weighted, restarts, conv_s8_p99,
                                   *, cells=None,
                                   per_stage_proofs: dict | None = None) -> dict:
    """Wraps compute_verdict and answers the falsifier acceptance (d):
    "would deleting THIS stage's proof change the verdict?"

    Returns a dict with:
      verdict:       same as compute_verdict()
      proof_impact:  {stage_id: bool}  — True if dropping the proof flips
                     the verdict (or makes it INVALID).
    """
    verdict = compute_verdict(mix_weighted, restarts, conv_s8_p99, cells=cells)
    impact: dict[str, bool] = {}
    if per_stage_proofs:
        for sid, proof in per_stage_proofs.items():
            # The proofs underwrite the cell counts. Dropping a proof in
            # this verdict surface does NOT mutate the cell percentiles
            # (those come from all_results). Falsifier tag is therefore
            # `False` by default — if a future schema change ties verdict
            # to a proof field (e.g. content_match in S6), set True here.
            impact[sid] = bool(proof.get("convergence_timeout"))
    return {"verdict": verdict, "proof_impact": impact}


# ─── Canonical ledger row builder (worktree source 7109-7338) ───────────────


def build_canonical_ledger_row(all_results: list[dict], *,
                               run_dir: Path | None = None,
                               tag: str | None = None,
                               scale: int | None = None) -> dict:
    """Assemble the canonical ledger row from Phase-6 measurements.

    Schema FROZEN — do NOT add/rename keys without updating the architect's
    plan AND the tester's reader. Refer to docs/bench-restructure-path-b-plan-2026-06-02.md
    §F.1 for the full key list and bench/ledger_row.schema.json for the
    JSON-Schema artifact.
    """
    tag = tag or os.environ.get("EXPECTED_IMAGE_TAG", "unknown")
    ship_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    if scale is None:
        scale = int(os.environ.get("SCALE", "5000"))

    # Per-cell waterfall stats. The validation framework marks
    # `waterfallMs == 0` as an "incomplete" sentinel; those MUST be
    # filtered before percentile computation. See worktree 7138-7152.
    def _cell_stats(user, cache_mode):
        cold_samples: list[int] = []
        warm_samples: list[int] = []
        valid_nav_count = 0
        invalid_nav_count = 0
        for entry in all_results:
            if entry.get("cache") != cache_mode:
                continue
            for _, pg in (entry.get("pages") or {}).items():
                for nav in (pg.get("navigations") or []):
                    if nav.get("user") and nav["user"] != user:
                        continue
                    wf = nav.get("waterfallMs", 0) or 0
                    if wf <= 0 or nav.get("incomplete"):
                        invalid_nav_count += 1
                        continue
                    valid_nav_count += 1
                    if nav.get("nav_num") == 1 or nav.get("cold_warm") == "COLD":
                        cold_samples.append(wf)
                    else:
                        warm_samples.append(wf)
        total_navs = valid_nav_count + invalid_nav_count
        terminal_fail_rate = (
            float(invalid_nav_count) / total_navs if total_navs else 0.0
        )
        if total_navs > 0 and valid_nav_count == 0:
            return {
                "cold_ms":              None,
                "warm_p50_ms":          None,
                "warm_p99_ms":          None,
                "valid_nav_count":      0,
                "invalid_nav_count":    invalid_nav_count,
                "terminal_fail_rate":   round(terminal_fail_rate, 4),
            }
        return {
            "cold_ms":            pct(cold_samples, 50) if cold_samples else 0,
            "warm_p50_ms":        pct(warm_samples, 50) if warm_samples else 0,
            "warm_p99_ms":        pct(warm_samples, 99) if warm_samples else 0,
            "valid_nav_count":    valid_nav_count,
            "invalid_nav_count":  invalid_nav_count,
            "terminal_fail_rate": round(terminal_fail_rate, 4),
        }

    cells = {
        "admin_on":   _cell_stats("admin",      "ON"),
        "admin_off":  _cell_stats("admin",      "OFF"),
        "cyber_on":   _cell_stats("cyberjoker", "ON"),
        "cyber_off":  _cell_stats("cyberjoker", "OFF"),
    }

    # Mirror admin -> cyber ONLY when no cyber samples exist (worktree 7210-7234).
    def _cell_has_navs(c):
        v = c.get("valid_nav_count") or 0
        iv = c.get("invalid_nav_count") or 0
        return (v + iv) > 0

    def _cell_has_data(c):
        cold = c.get("cold_ms")
        wp50 = c.get("warm_p50_ms")
        return (cold is not None and cold > 0) or (wp50 is not None and wp50 > 0)

    def _mirror(target, source):
        if _cell_has_navs(cells[target]):
            return
        if not _cell_has_data(cells[source]):
            return
        cells[target] = dict(cells[source])
        cells[target]["mirrored_from"] = source
    _mirror("cyber_on", "admin_on")
    _mirror("cyber_off", "admin_off")

    # Mix-weighted = 0.95 * cyber + 0.05 * admin (worktree 7236-7269).
    def _val(cell, field):
        v = cell.get(field)
        return v if (v is not None and v > 0) else None

    def _mw_pick(field):
        cyber = _val(cells["cyber_on"], field)
        if cyber is None:
            cyber = _val(cells["cyber_off"], field)
        admin = _val(cells["admin_on"], field)
        if admin is None:
            admin = _val(cells["admin_off"], field)
        if cyber is None and admin is None:
            return None
        if cyber is None:
            return int(round(admin))
        if admin is None:
            return int(round(cyber))
        return int(round(0.95 * cyber + 0.05 * admin))
    mix_weighted = {
        "cold_ms":     _mw_pick("cold_ms"),
        "warm_p50_ms": _mw_pick("warm_p50_ms"),
        "warm_p99_ms": _mw_pick("warm_p99_ms"),
    }

    # Pod restart count (worktree 7272-7281).
    restarts = 0
    try:
        rc, out, _ = kubectl(
            "get", "pods", "-n", "krateo-system",
            "-l", "app.kubernetes.io/name=snowplow",
            "-o",
            "jsonpath={.items[0].status.containerStatuses[0].restartCount}")
        if rc == 0 and out.strip():
            restarts = int(out.strip())
    except Exception:
        pass

    # Uptime at capture (worktree 7286-7293).
    uptime_s = 0
    try:
        rc, out, _ = kubectl(
            "get", "pods", "-n", "krateo-system",
            "-l", "app.kubernetes.io/name=snowplow",
            "-o", "jsonpath={.items[0].status.startTime}")
        if rc == 0 and out.strip():
            t0 = datetime.datetime.fromisoformat(
                out.strip().replace("Z", "+00:00"))
            uptime_s = int((datetime.datetime.now(datetime.timezone.utc) - t0)
                           .total_seconds())
    except Exception:
        pass

    validation = aggregate_validation(all_results)

    # Bind the convergence p99s + the S7 drain slope ONCE: the S7/S8 p99s feed
    # both the row fields below AND the verdict (Task #334-B gate), so compute
    # them here and reuse rather than recompute at each site.
    conv_s6_p99 = convergence_p99_for_stage(all_results, "6")
    conv_s7_p99 = convergence_p99_for_stage(all_results, "7")
    conv_s8_p99 = convergence_p99_for_stage(all_results, "8")
    s7_slope = s7_min_slope_per_s(all_results)

    mix_has_null = any(v is None for v in mix_weighted.values())
    base_verdict = compute_verdict(
        mix_weighted, restarts, conv_s8_p99,
        cells=cells,
        conv_s7_p99=conv_s7_p99,
        s7_slope_per_s=s7_slope)
    if validation["navs_terminal_fail"] > 0 or mix_has_null:
        verdict = "INVALID"
    else:
        verdict = base_verdict

    # Task #334-B series annotations: re-derive the S7 band/slope classification
    # so above-band excursions are LOUDLY recorded in the row (rows 3/5 of the
    # truth table — PASS-with-annotation, never a silent verdict change). The
    # thresholds mirror compute_verdict's gate exactly (same constants) so the
    # two cannot drift. Row 4 (above-band + dropped slope) is the FAIL case and
    # is surfaced via failed_gates in write_run_bundle, not here.
    series_annotations: list[str] = []
    if conv_s7_p99 is not None and conv_s7_p99 > S7_CONV_BAND_MS[1]:
        if s7_slope is None:
            series_annotations.append(
                f"s7_slope_unavailable conv={conv_s7_p99}ms "
                f"(telemetry missing — gate FAIL-OPEN; not scored)")
        elif s7_slope >= S7_SLOPE_FLOOR_PER_S:
            series_annotations.append(
                f"s7_above_band_cluster_state conv={conv_s7_p99}ms "
                f"slope={s7_slope}/s (healthy; cluster churn, not code — see #335)")

    return {
        "tag": tag,
        "ship_date": ship_date,
        "scale": [scale, 5000] if scale != 5000 else [5000],
        "uptime_at_capture_s": uptime_s,
        "cells": cells,
        "mix_weighted": mix_weighted,
        "convergence_mass_s6_p99": conv_s6_p99,
        "convergence_mass_s7_p99": conv_s7_p99,
        "convergence_mass_s8_p99": conv_s8_p99,
        "convergence_per_mutation_p99_mix": _load_per_mutation_metric(
            "p99_mix", run_dir=run_dir),
        "convergence_per_class_hot_p99":    _load_per_mutation_metric(
            "hot_p99", run_dir=run_dir),
        "convergence_per_class_warm_p99":   _load_per_mutation_metric(
            "warm_p99", run_dir=run_dir),
        "convergence_per_class_cold_p99":   _load_per_mutation_metric(
            "cold_p99", run_dir=run_dir),
        # Task #314 — CR-definition-modification reconcile latency, REPORT-
        # ONLY (the #121 pattern). Read by-key from the S8b/S8c proofs; -1
        # sentinel = the stage's poll timed out (FAIL). These do NOT feed
        # compute_verdict this ship (a CONV_MOD_TIER_MS gate is a follow-up
        # after the first 50K distribution is known). Null when the stage
        # did not run (pre-#314 run, or window excluded S8b/S8c).
        "conv_widget_mod_ms": _load_stage_proof_metric(
            "S8b", "conv_widget_mod_ms", run_dir=run_dir),
        "conv_ra_mod_ms": _load_stage_proof_metric(
            "S8c", "conv_ra_mod_ms", run_dir=run_dir),
        "tag_specific_verifications": {},
        "pod_restart_count": restarts,
        "validation": validation,
        # Task #334-B: every row self-declares which verdict logic produced it.
        # Rows written before this commit lack the key (None) -> unambiguously
        # "pre-#334-B semantics" (the gate did NOT gate S7). Durable
        # series-transition record (survives even if the prose ledger note is
        # lost). series_annotations carries the LOUD above-band notes (rows 3/5).
        "verdict_semantics_rev": VERDICT_SEMANTICS_REV,
        "series_annotations": series_annotations,
        "verdict": verdict,
    }


# ─── print_report (worktree source 8164-8174) ───────────────────────────────


def print_report() -> bool:
    """Print final report; True iff zero failures in `_results`."""
    print(f"\n  {_BOLD}=== FINAL REPORT ==={_RESET}", flush=True)
    passed = [r for r in _results if r["passed"]]
    failed = [r for r in _results if not r["passed"]]
    print(f"\n  Total: {len(_results)}   "
          f"{_GREEN}Passed: {len(passed)}{_RESET}   "
          f"{_RED}Failed: {len(failed)}{_RESET}\n")
    if failed:
        print(f"  {_RED}{_BOLD}FAILED TESTS:{_RESET}")
        for r in failed:
            print(f"    {_RED}x{_RESET} {r['name']:<65s}  "
                  f"HTTP {r['code']:<4}  {r['ms']}ms  {r['note']}")
        print()
    return len(failed) == 0


# ─── Run-bundle writer + truncate logic (per R3.1) ──────────────────────────


# Task #121 (Diego, 2026-06-10): the 200 MB RUN_BUNDLE_MAX_BYTES cap is
# REMOVED. Nothing is ever dropped from the run bundle now (videos +
# screenshots + pod_logs all retained), so there is no ceiling constant.
BASELINE_PATH = Path(__file__).resolve().parent.parent / ".baseline.json"


def _bundle_size_bytes(run_dir: Path) -> int:
    """Total bytes of videos + screenshots + pod_logs under run_dir.

    Reporting-only as of Task #121 (cap removed): used to populate the
    bundle-size figure in oversize_bundle.json / summary.json. Nothing is
    ever dropped, so this is no longer compared against a ceiling."""
    total = 0
    for sub in ("videos", "screenshots", "pod_logs"):
        d = run_dir / sub
        if not d.is_dir():
            continue
        for p in d.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except OSError:
                    pass
    return total


def _gzip_pod_logs(run_dir: Path) -> list[dict]:
    """Gzip every uncompressed pod_logs/*.txt in place: write `<name>.gz`,
    remove the original. Returns a list of dicts {name, raw_size,
    gz_size} for the files compressed (empty when none / already gzipped).

    Task #299 / #121: pod_logs are the irreplaceable trace inputs (≈196 MB
    this run) but compress ~10-20× (S6 98 MB → ~5-8 MB). Gzip is RETAINED
    after the Task #121 cap removal because it drops NOTHING — it only
    compresses, saving disk while keeping traceability (zcat/zgrep work on
    the .gz; trace primitives confirmed by the architect). The .gz keeps the
    original stem so the trace docs' `S6.txt` references survive as
    `S6.txt.gz` (discoverable).
    """
    pdir = run_dir / "pod_logs"
    if not pdir.is_dir():
        return []
    out: list[dict] = []
    for p in sorted(pdir.rglob("*.txt")):
        if not p.is_file():
            continue
        gz = p.with_suffix(p.suffix + ".gz")  # S6.txt -> S6.txt.gz
        if gz.exists():
            continue
        try:
            raw_size = p.stat().st_size
            with open(p, "rb") as fin, gzip.open(gz, "wb") as fout:
                shutil.copyfileobj(fin, fout)
            gz_size = gz.stat().st_size
            p.unlink()
        except OSError:
            # Best-effort: leave the original in place on any IO error.
            continue
        out.append({"name": gz.name, "raw_size": raw_size,
                    "gz_size": gz_size})
    return out


def _finalize_bundle(run_dir: Path) -> tuple[bool, list[dict]]:
    """Gzip pod_logs; NEVER drop any artifact (Task #121 — cap removed).

    Task #121 (Diego, 2026-06-10): the 200 MB bundle size cap and the
    artifact-DROPPING truncation it drove are REMOVED, unconditionally and in
    every video mode. Rationale: the final clean re-run is `--video all` at
    50K (~1GB+ of all-stage video) and the old cap would trim exactly the
    videos Diego wants. Nothing is ever removed from the bundle now — videos,
    screenshots, and pod_logs are all retained regardless of total size.

    The ONLY retained behaviour is the Task #299 pod_log gzip: it drops
    nothing, it only compresses (~10-20×), saving disk while keeping
    traceability (zcat/zgrep work on the .gz). The `<name>.txt` → `<name>.txt.gz`
    stem rename is preserved for discoverability.

    Returns (truncated, trimmed_list). `truncated` is now ALWAYS False (no cap,
    no drops). trimmed_list carries only the gzip summary (reason
    `bundle_pod_logs_gzipped`) — NO drop entries are ever produced.
    oversize_bundle.json is written as a zero-drop size + gzip report when any
    log was compressed (it never reports a dropped file).
    """
    trimmed: list[dict] = []

    # Gzip pod_logs (shrink, never drop). Records the before/after so the
    # saving stays auditable in oversize_bundle.json.
    gzipped = _gzip_pod_logs(run_dir)
    if gzipped:
        raw_total = sum(g["raw_size"] for g in gzipped)
        gz_total = sum(g["gz_size"] for g in gzipped)
        trimmed.append({
            "reason": "bundle_pod_logs_gzipped",
            "files": len(gzipped),
            "raw_bytes": raw_total,
            "gz_bytes": gz_total,
            "saved_bytes": raw_total - gz_total,
            "names": [g["name"] for g in gzipped],
        })
        _write_oversize_bundle(run_dir, trimmed)

    # No cap, no drops, ever → truncated is always False.
    return False, trimmed


# Back-compat alias: the writer + any external caller referenced
# `_truncate_bundle` before Task #121 removed the truncation. The function no
# longer truncates (it gzips + reports), but the name is retained so callers
# don't break.
_truncate_bundle = _finalize_bundle


def _write_oversize_bundle(run_dir: Path, trimmed: list[dict]) -> None:
    """Write oversize_bundle.json (zero-drop size + gzip report) + print the
    BUNDLE summary line.

    Task #121: with the cap removed nothing is ever dropped, so this is now a
    size/gzip-saving report — `dropped_count` is always 0 and there is no
    `max_bytes` ceiling. The filename is kept for back-compat with downstream
    readers; the schema gains `cap_removed: True` and `bundle_size_bytes` and
    drops the obsolete `max_bytes` field."""
    bundle_size = _bundle_size_bytes(run_dir)
    (run_dir / "oversize_bundle.json").write_text(
        json.dumps({
            "cap_removed": True,
            "bundle_size_bytes": bundle_size,
            "dropped_count": 0,
            "trimmed_count": len(trimmed),
            "trimmed": trimmed,
        }, indent=2))
    saved_mb = sum(t.get("saved_bytes", 0) for t in trimmed) / (1024 * 1024)
    print(f"  BUNDLE: gzip saved {saved_mb:.1f} MB; "
          f"{bundle_size / (1024 * 1024):.1f} MB retained "
          f"(cap removed — nothing dropped)", flush=True)


def write_run_bundle(run_dir: Path,
                     all_results: list[dict],
                     *,
                     per_stage_proofs: dict | None = None,
                     tag: str | None = None,
                     scale: int | None = None,
                     duration_s: float | None = None,
                     overlay_age_days: float | None = None) -> dict:
    """Serialize the §F directory tree.

    Replaces inline json.dump at worktree source 8663-8682. Side-effect:
    writes ledger_row.json, summary.json, per_stage.json, expected_calls.json
    snapshot, and pod_logs/full_run.txt under `run_dir`.

    Returns the canonical ledger row dict for caller convenience.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "pod_logs").mkdir(exist_ok=True)

    # 1) Build canonical row.
    row = build_canonical_ledger_row(
        all_results, run_dir=run_dir, tag=tag, scale=scale)
    (run_dir / "ledger_row.json").write_text(json.dumps(row, indent=2,
                                                       default=str))

    # 2) Aggregated per-stage proofs.
    if per_stage_proofs:
        (run_dir / "per_stage.json").write_text(
            json.dumps(per_stage_proofs, indent=2, default=str))

    # 3) Expected-calls snapshot — verbatim copy of the overlay at run start.
    try:
        from bench.expected import EXPECTED_CALLS_OVERLAY_PATH
        if os.path.exists(EXPECTED_CALLS_OVERLAY_PATH):
            with open(EXPECTED_CALLS_OVERLAY_PATH, "rb") as f:
                (run_dir / "expected_calls.json").write_bytes(f.read())
    except Exception:
        pass

    # 4) Full-run pod logs (single kubectl call).
    try:
        rc, out, _ = kubectl(
            "logs", "deployment/snowplow",
            "-n", "krateo-system", "-c", "snowplow",
            "--tail=1000000", timeout_secs=120,
        )
        if rc == 0 and out:
            (run_dir / "pod_logs" / "full_run.txt").write_text(out)
    except Exception:
        pass

    # 5) Bundle finalize: gzip pod_logs only; NEVER drop (Task #121 — cap
    # removed). `truncated` is always False now.
    truncated, trimmed = _finalize_bundle(run_dir)

    # 6) summary.json — see §F.2.
    baseline_tag, baseline_warm = read_baseline()
    actual_warm = row.get("mix_weighted", {}).get("warm_p50_ms")
    baseline_delta = compute_baseline_delta(actual_warm, baseline_warm)

    failed_gates: list[str] = []
    if row["verdict"] != "PASS":
        if row["validation"]["navs_terminal_fail"] > 0:
            failed_gates.append("terminal_state")
        if row["pod_restart_count"] > 0:
            failed_gates.append("pod_restarts")
        if any(v is None for v in row["mix_weighted"].values()):
            failed_gates.append("mix_weighted_null")
        # #289b: enumerate latency-tier misses so a non-PASS verdict never
        # reads as a contradiction (verdict=FAIL with failed_gates=[]). Each
        # entry names the tier and the offending value, e.g.
        # "tier:warm_p50 914>500". Thresholds mirror compute_verdict's tier
        # logic exactly (same constants) so the two cannot drift.
        mw = row["mix_weighted"]
        wp50 = mw.get("warm_p50_ms")
        cold = mw.get("cold_ms")
        conv = row.get("convergence_mass_s8_p99")
        if wp50 is not None and wp50 > WARM_P50_TIER_MS:
            failed_gates.append(f"tier:warm_p50 {wp50}>{WARM_P50_TIER_MS}")
        if cold is None or (cold is not None and cold > COLD_TIER_MS):
            failed_gates.append(
                f"tier:cold {cold}>{COLD_TIER_MS}" if cold is not None
                else f"tier:cold null>{COLD_TIER_MS}")
        if conv is not None and conv not in (-1,) and conv > CONV_TIER_MS:
            failed_gates.append(f"tier:conv_s8_p99 {conv}>{CONV_TIER_MS}")
        # Task #334-B row 4 (the S7 band+slope FAIL): conv above the documented
        # serve-stale band AND the refresher drain slope dropped below floor —
        # the code-regression signature (#335). Re-derives the same
        # classification compute_verdict gated on (same constants + the same
        # s7_min_slope_per_s helper from all_results) so the two cannot drift.
        # Rows 3/5 (above-band PASS-with-annotation) are surfaced via
        # series_annotations, not here.
        conv_s7 = row.get("convergence_mass_s7_p99")
        s7_slope = s7_min_slope_per_s(all_results)
        if (conv_s7 is not None and conv_s7 > S7_CONV_BAND_MS[1]
                and s7_slope is not None
                and s7_slope < S7_SLOPE_FLOOR_PER_S):
            failed_gates.append(
                f"s7:above_band_slope_dropped {conv_s7}>{S7_CONV_BAND_MS[1]} "
                f"slope {s7_slope}<{S7_SLOPE_FLOOR_PER_S}")

    summary = {
        "verdict": row["verdict"],
        "mix_weighted": row["mix_weighted"],
        "tag": row["tag"],
        "scale": scale or int(os.environ.get("SCALE", "5000")),
        "run_dir": str(run_dir.absolute()),
        "stages_completed": (
            list(per_stage_proofs.keys()) if per_stage_proofs else []),
        "duration_s": duration_s,
        "pod_restarts": row["pod_restart_count"],
        "convergence_p99_s": {
            "s6": row["convergence_mass_s6_p99"] / 1000.0
                  if row["convergence_mass_s6_p99"] not in (None, -1) else None,
            "s7": row["convergence_mass_s7_p99"] / 1000.0
                  if row["convergence_mass_s7_p99"] not in (None, -1) else None,
            "s8": row["convergence_mass_s8_p99"] / 1000.0
                  if row["convergence_mass_s8_p99"] not in (None, -1) else None,
        },
        "failed_gates": failed_gates,
        # Task #334-B: the LOUD above-band annotations (rows 3/5 —
        # PASS-with-annotation, FAIL-OPEN) + the verdict-semantics marker so a
        # reader diffing rows across this commit can tell a verdict letter
        # changed MEANING, not that the build regressed.
        "series_annotations": row.get("series_annotations", []),
        "verdict_semantics_rev": row.get("verdict_semantics_rev"),
        "convergence_timeout_stage": _find_convergence_timeout_stage(
            per_stage_proofs),
        "ledger_row_path": "ledger_row.json",
        "bundle_truncated": truncated,
        "baseline_tag": baseline_tag,
        "baseline_warm_p50_ms": baseline_warm,
        "baseline_delta_ratio": baseline_delta,
        "overlay_age_days_at_start": overlay_age_days,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2,
                                                    default=str))
    return row


def _find_convergence_timeout_stage(per_stage_proofs: dict | None) -> str | None:
    if not per_stage_proofs:
        return None
    for sid, proof in per_stage_proofs.items():
        if isinstance(proof, dict) and proof.get("convergence_timeout"):
            return sid
    return None


# ─── Baseline (acceptance (c)) ──────────────────────────────────────────────


def read_baseline() -> tuple[str | None, float | None]:
    """Read `e2e/bench/.baseline.json` if present.

    Returns (baseline_tag, baseline_warm_p50_ms). Both None when the
    file is missing or malformed — acceptance (c) treats absence as
    "no anchor; record but don't gate."
    """
    try:
        with open(BASELINE_PATH) as f:
            d = json.load(f)
        return d.get("baseline_tag"), d.get("baseline_warm_p50_ms")
    except Exception:
        return None, None


def compute_baseline_delta(actual_warm_p50_ms,
                           baseline_warm_p50_ms) -> float | None:
    """Signed relative delta: (actual - baseline) / baseline.

    Returns None when either input is missing/non-positive. Acceptance (c)
    gates |delta| <= 0.15 — but the gate ITSELF lives in Block 5's
    acceptance step; this helper is the math.
    """
    if not actual_warm_p50_ms or not baseline_warm_p50_ms:
        return None
    if baseline_warm_p50_ms <= 0:
        return None
    return (actual_warm_p50_ms - baseline_warm_p50_ms) / baseline_warm_p50_ms


# ─── JSON-Schema emitter (acceptance (b)) ───────────────────────────────────


def _ledger_row_schema_doc() -> dict:
    """Return the JSON-Schema 2020-12 covering §F.1 frozen key list."""
    cell_props = {
        "cold_ms":             {"type": ["integer", "null"]},
        "warm_p50_ms":         {"type": ["integer", "null"]},
        "warm_p99_ms":         {"type": ["integer", "null"]},
        "valid_nav_count":     {"type": "integer", "minimum": 0},
        "invalid_nav_count":   {"type": "integer", "minimum": 0},
        "terminal_fail_rate":  {"type": "number"},
        "mirrored_from":       {"type": "string"},
    }
    cell_schema = {
        "type": "object",
        "required": ["cold_ms", "warm_p50_ms", "warm_p99_ms",
                     "valid_nav_count", "invalid_nav_count",
                     "terminal_fail_rate"],
        "properties": cell_props,
        "additionalProperties": False,
    }
    mw_schema = {
        "type": "object",
        "required": ["cold_ms", "warm_p50_ms", "warm_p99_ms"],
        "properties": {
            "cold_ms":     {"type": ["integer", "null"]},
            "warm_p50_ms": {"type": ["integer", "null"]},
            "warm_p99_ms": {"type": ["integer", "null"]},
        },
        "additionalProperties": False,
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://krateo.io/snowplow/bench/ledger_row.schema.json",
        "title": "Snowplow bench canonical ledger row",
        "description": (
            "Frozen schema covering docs/bench-restructure-path-b-plan-"
            "2026-06-02.md §F.1 key list. Bumping requires a fresh PM gate."
        ),
        "type": "object",
        "required": [
            "tag", "ship_date", "scale", "uptime_at_capture_s",
            "cells", "mix_weighted",
            "convergence_mass_s6_p99", "convergence_mass_s7_p99",
            "convergence_mass_s8_p99",
            "convergence_per_mutation_p99_mix",
            "convergence_per_class_hot_p99",
            "convergence_per_class_warm_p99",
            "convergence_per_class_cold_p99",
            "tag_specific_verifications", "pod_restart_count",
            "validation", "verdict",
        ],
        "properties": {
            "tag":                  {"type": "string"},
            "ship_date":            {"type": "string"},
            "scale": {
                "type": "array",
                "items": {"type": "integer", "minimum": 1},
                "minItems": 1, "maxItems": 2,
            },
            "uptime_at_capture_s":  {"type": "integer", "minimum": 0},
            "cells": {
                "type": "object",
                "required": ["admin_on", "admin_off", "cyber_on", "cyber_off"],
                "properties": {
                    "admin_on":  cell_schema,
                    "admin_off": cell_schema,
                    "cyber_on":  cell_schema,
                    "cyber_off": cell_schema,
                },
                "additionalProperties": False,
            },
            "mix_weighted": mw_schema,
            "convergence_mass_s6_p99": {"type": "integer"},
            "convergence_mass_s7_p99": {"type": "integer"},
            "convergence_mass_s8_p99": {"type": "integer"},
            "convergence_per_mutation_p99_mix": {"type": ["integer", "null"]},
            "convergence_per_class_hot_p99":    {"type": ["integer", "null"]},
            "convergence_per_class_warm_p99":   {"type": ["integer", "null"]},
            "convergence_per_class_cold_p99":   {"type": ["integer", "null"]},
            # Task #314 — additive optional report-only reconcile metrics.
            "conv_widget_mod_ms": {"type": ["integer", "null"]},
            "conv_ra_mod_ms":     {"type": ["integer", "null"]},
            # Task #334-B — additive verdict-semantics marker + loud above-band
            # annotations (rows 3/5). Pre-#334-B rows lack verdict_semantics_rev
            # (null) -> "pre-#334-B semantics".
            "verdict_semantics_rev": {"type": ["string", "null"]},
            "series_annotations": {"type": "array",
                                   "items": {"type": "string"}},
            "tag_specific_verifications": {"type": "object"},
            "pod_restart_count": {"type": "integer", "minimum": 0},
            "validation": {
                "type": "object",
                "required": ["navs_terminal_pass", "navs_terminal_fail",
                             "skeleton_failures", "errored_widgets_total",
                             "call_count_mismatches"],
                "properties": {
                    "navs_terminal_pass": {"type": "integer", "minimum": 0},
                    "navs_terminal_fail": {"type": "integer", "minimum": 0},
                    "skeleton_failures":  {"type": "array",
                                           "items": {"type": "string"}},
                    "errored_widgets_total": {"type": "integer",
                                              "minimum": 0},
                    "call_count_mismatches": {"type": "array"},
                },
                "additionalProperties": True,
            },
            "verdict": {
                "type": "string",
                "enum": ["PASS", "WEAK_PASS", "FAIL", "FLOOR", "INVALID",
                         "REJECT"],
            },
        },
        "additionalProperties": True,
    }


def emit_ledger_row_schema(path: Path | None = None) -> Path:
    """Write the JSON-Schema artifact next to this module by default."""
    target = (Path(path) if path else
              Path(__file__).resolve().parent / "ledger_row.schema.json")
    target.write_text(json.dumps(_ledger_row_schema_doc(), indent=2))
    return target


# ─── Legacy aliases for back-compat (callers using the worktree names) ──────


_build_canonical_ledger_row = build_canonical_ledger_row
_compute_verdict = compute_verdict
_aggregate_validation = aggregate_validation
_convergence_p99_for_stage = convergence_p99_for_stage
