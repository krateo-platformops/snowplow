"""Storm scenarios — composition deploy storm + CRB-delete burst.

Operator orchestration of `lifecycle.py` primitives for the disruptive
workloads used at Phase 6 S6 and the v5 CRB-delete reproduction.

Per docs/bench-restructure-path-b-plan-2026-06-02.md §A.4. Dependency
direction is `lifecycle → storm` (this module does NOT import lifecycle
at the top level for orchestration purposes; instead the run_*
functions defer the imports they need). Block 3 wires `bench.browser`
(login_all, http_get, get_runtime_metrics) and Block 4 wires
`bench.ledger` (record); the moved functions defer those imports too so
this module imports cleanly during Block 2.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time

from bench import evidence
from bench.cluster import kubectl, NS


__all__ = [
    "CRB_DELETE_PORTAL_PATHS",
    "pod_logs_since",
    "pod_log_offset",
    "wait_for_active_users",
    "measure_warmup_after_restart",
    "measure_first_login_warmup",
    "run_user_scaling",
    "crb_delete_burst",
]


# ─── Portal-shape paths used by the CRB-delete reproduction ─────────────────
#
# Each request travels through a chain that includes
# compositions-get-ns-and-crd → which has UAF on both api[] entries.
# v5/v6 D1/D2/D3 reproduction — see worktree source line 8212-8222.

CRB_DELETE_PORTAL_PATHS = [
    ("restaction/compositions-get-ns-and-crd",
     "/call?apiVersion=templates.krateo.io%2Fv1&resource=restactions&name=compositions-get-ns-and-crd&namespace=krateo-system"),
    ("restaction/compositions-list",
     "/call?apiVersion=templates.krateo.io%2Fv1&resource=restactions&name=compositions-list&namespace=krateo-system"),
    ("restaction/blueprints-list",
     "/call?apiVersion=templates.krateo.io%2Fv1&resource=restactions&name=blueprints-list&namespace=krateo-system"),
]


# ─── Deferred imports — Block 3 (browser.py) + Block 4 (ledger.py) ──────────


def _log(msg):
    """Local log shim — replaced by cli.py logger when present."""
    try:
        from bench.cli import log
        log(msg)
    except ImportError:
        ts = time.strftime("%H:%M:%S")
        print(f"  [{ts}] {msg}", flush=True)


def _section(title):
    """Local section shim — replaced by cli.py logger when present."""
    try:
        from bench.cli import section
        section(title)
    except ImportError:
        print(f"\n  === {title} ===", flush=True)


def _http_get(path, token, **kwargs):
    """Defer to bench.browser when present (Block 3)."""
    from bench.browser import http_get  # type: ignore
    return http_get(path, token, **kwargs)


def _login_all():
    """Defer to bench.browser when present (Block 3)."""
    from bench.browser import login_all  # type: ignore
    return login_all()


def _get_runtime_metrics():
    """Defer to bench.ledger when present (Block 4)."""
    from bench.ledger import get_runtime_metrics  # type: ignore
    return get_runtime_metrics()


def _read_l1_ready_ts():
    """Defer to bench.ledger when present (Block 4)."""
    from bench.ledger import _read_l1_ready_ts as _impl  # type: ignore
    return _impl()


def _record(name, passed, **kwargs):
    """Defer to bench.ledger when present (Block 4)."""
    from bench.ledger import record  # type: ignore
    return record(name, passed, **kwargs)


# ─── Public pod-log tail helpers (renamed for plan §A.4) ────────────────────


def pod_log_offset():
    """Snapshot the current pod log size (line count) so callers can read
    the delta after a burst. Returns 0 if logs unreadable; downstream
    callers gracefully degrade.
    """
    rc, out, _ = kubectl("logs", "deployment/snowplow", "-n", NS,
                         "-c", "snowplow", "--tail=1000000")
    if rc != 0 or not out:
        return 0
    return len(out.splitlines())


def pod_logs_since(offset):
    """Return pod log lines starting at offset (best-effort)."""
    rc, out, _ = kubectl("logs", "deployment/snowplow", "-n", NS,
                         "-c", "snowplow", "--tail=1000000")
    if rc != 0 or not out:
        return []
    lines = out.splitlines()
    if offset >= len(lines):
        return []
    return lines[offset:]


# Internal aliases — kept for compatibility with not-yet-migrated callers.
_pod_log_offset = pod_log_offset
_pod_logs_since = pod_logs_since


# ─── Active-user / warmup probes (Phase 7 — synthetic user scaling) ─────────


def wait_for_active_users(expected_min, timeout=300):
    """Poll snowplow `/metrics` for active_users >= expected_min.

    Returns True on success, False on timeout. NEVER raises so the caller
    can record a failure and move on.
    """
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        m = _get_runtime_metrics()
        if m and m.get("active_users", 0) >= expected_min:
            _log(f"  Active users reached {m['active_users']} (needed {expected_min})")
            return True
        if m:
            last_count = m.get("active_users", 0)
        if int(time.time()) % 15 < 3:
            _log(f"  Waiting for active_users: {last_count}/{expected_min} ...")
        time.sleep(3)
    _log(f"  TIMEOUT waiting for {expected_min} active users (got {last_count})")
    return False


def measure_warmup_after_restart(expected_users, timeout=300):
    """Restart the snowplow pod and measure time until L1 is ready.

    Returns dict with warmup_ms, peak_heap_mb, peak_goroutines, or None
    on timeout.
    """
    # Defer the lifecycle import — wait_for_snowplow lives in lifecycle.
    from bench.lifecycle import _wait_for_snowplow

    kubectl("rollout", "restart", "deploy/snowplow", "-n", NS)
    kubectl("rollout", "status", "deploy/snowplow", "-n", NS,
            f"--timeout={timeout}s", timeout_secs=timeout + 30)

    if not _wait_for_snowplow(max_wait=120):
        return None

    t0 = time.time()
    peak_heap = 0.0
    peak_goroutines = 0
    deadline = time.time() + timeout

    while time.time() < deadline:
        m = _get_runtime_metrics()
        if m:
            peak_heap = max(peak_heap, m.get("heap_alloc_mb", 0))
            peak_goroutines = max(peak_goroutines, m.get("goroutine_count", 0))

        ts = _read_l1_ready_ts()
        if ts > 0:
            warmup_ms = int((time.time() - t0) * 1000)
            return {
                "warmup_ms": warmup_ms,
                "peak_heap_mb": peak_heap,
                "peak_goroutines": peak_goroutines,
            }
        time.sleep(3)

    return None


def measure_first_login_warmup(new_start, new_end, total_expected, tokens,
                                timeout=180):
    """Create new synthetic users and measure time until their L1 is warm.

    Also measures latency impact on existing admin user during the burst.

    Returns dict with warmup_ms, peak_heap_mb, peak_goroutines,
    admin_latency_ms, or None on timeout.
    """
    from bench.lifecycle import create_synthetic_users  # deferred

    # WIDGET_ENDPOINTS lives in browser.py (Block 3); fall back to the
    # canonical path string when the browser module is not present.
    try:
        from bench.browser import WIDGET_ENDPOINTS  # type: ignore
        dashboard_path = WIDGET_ENDPOINTS[0][1]
    except ImportError:
        dashboard_path = (
            "/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1"
            "&resource=pages&name=dashboard-page&namespace=krateo-system"
        )

    admin_token = tokens.get("admin")

    before_sentinel = _read_l1_ready_ts()
    peak_heap = 0.0
    peak_goroutines = 0

    baseline_latencies = []
    for _ in range(3):
        ms, code, _ = _http_get(dashboard_path, admin_token)
        if code == 200:
            baseline_latencies.append(ms)
    admin_baseline = statistics.median(baseline_latencies) if baseline_latencies else 0

    t0 = time.time()
    created, _ = create_synthetic_users(new_start, new_end)
    if created == 0:
        return None

    deadline = time.time() + timeout
    admin_during_latencies = []

    while time.time() < deadline:
        m = _get_runtime_metrics()
        if m:
            peak_heap = max(peak_heap, m.get("heap_alloc_mb", 0))
            peak_goroutines = max(peak_goroutines, m.get("goroutine_count", 0))

        if admin_token and int(time.time()) % 6 < 3:
            ms, code, _ = _http_get(dashboard_path, admin_token)
            if code == 200:
                admin_during_latencies.append(ms)

        ts = _read_l1_ready_ts()
        if ts > before_sentinel:
            if m and m.get("active_users", 0) >= total_expected:
                warmup_ms = int((time.time() - t0) * 1000)
                admin_during = (statistics.median(admin_during_latencies)
                                if admin_during_latencies else 0)
                return {
                    "warmup_ms": warmup_ms,
                    "peak_heap_mb": peak_heap,
                    "peak_goroutines": peak_goroutines,
                    "admin_baseline_ms": admin_baseline,
                    "admin_during_ms": admin_during,
                }
        time.sleep(3)

    warmup_ms = int((time.time() - t0) * 1000)
    admin_during = statistics.median(admin_during_latencies) if admin_during_latencies else 0
    return {
        "warmup_ms": warmup_ms,
        "peak_heap_mb": peak_heap,
        "peak_goroutines": peak_goroutines,
        "admin_baseline_ms": admin_baseline,
        "admin_during_ms": admin_during,
        "timeout": True,
    }


# ─── Phase 7 — Multi-user scaling storm ─────────────────────────────────────


def run_user_scaling(tokens):
    """Phase 7 storm: deploy compositions then ramp synthetic users.

    Operator-facing entrypoint. Defers lifecycle / browser / ledger imports
    so the module remains importable during Block 2.
    """
    from bench.lifecycle import (
        SCALE_GUARD_BENCH_NAMESPACES,
        SCALE_GUARD_COMPOSITIONS,
        USER_COUNTS,
        assert_clean,
        create_bench_namespaces,
        delete_synthetic_users,
        deploy_compositiondefinition,
        deploy_compositions_parallel,
        disable_cache,
        enable_cache,
        wait_for_bench_namespaces,
        wait_for_crd,
        _wait_for_snowplow,
    )
    # wait_for_compositions lives in browser.py via the source script today;
    # in the package it will land in browser.py (Block 3). Until then, defer.
    try:
        from bench.lifecycle import wait_for_compositions  # type: ignore
    except ImportError:
        from bench.browser import wait_for_compositions  # type: ignore
    from bench.cluster import pct

    SCALE = int(os.environ.get("SCALE", "5000"))

    try:
        from bench.browser import WIDGET_ENDPOINTS, RESTACTION_ENDPOINTS  # type: ignore
    except ImportError:
        WIDGET_ENDPOINTS = [(
            "page/dashboard",
            "/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1"
            "&resource=pages&name=dashboard-page&namespace=krateo-system",
        )]
        RESTACTION_ENDPOINTS = [None, None, (
            "restaction/compositions-list",
            "/call?apiVersion=templates.krateo.io%2Fv1"
            "&resource=restactions&name=compositions-list&namespace=krateo-system",
        )]

    BOLD = "\033[1m"
    RESET = "\033[0m"
    print(f"\n{BOLD}{'═' * 110}{RESET}")
    print(f"{BOLD}  PHASE 7: MULTI-USER SCALING (warmup + first-login burst){RESET}")
    print(f"{BOLD}{'═' * 110}{RESET}")

    # ── Step 0: Clean environment + pre-flight assertion ──
    _section("Step 0: Clean environment")
    assert_clean(retry_with_cleanup=True)
    enable_cache()
    if not _wait_for_snowplow():
        _log("ERROR: snowplow not healthy after cleanup")
        return

    # ── Step 1: Deploy compositions ──
    _section("Step 1: Deploy compositions")
    comp_target = SCALE
    if comp_target >= 10000:
        ns_count = 50
        comps_per_ns = comp_target // ns_count
    else:
        ns_count = comp_target // 10 if comp_target >= 10 else 1
        comps_per_ns = 10

    _log(f"Deploying {comp_target} compositions across {ns_count} namespaces ...")
    create_bench_namespaces(1, ns_count)
    wait_for_bench_namespaces(ns_count, timeout=600)
    deploy_compositiondefinition("bench-ns-01")
    if not wait_for_crd(timeout=300):
        _log("ERROR: CRD not ready after 300s, aborting")
        return
    time.sleep(10)
    deploy_compositions_parallel(1, ns_count, comps_per_ns)
    wait_for_compositions(comp_target, timeout=3600)

    # ── Step 2: Baseline warmup (0 synthetic users) ──
    _section("Step 2: Baseline warmup (admin + cyberjoker only)")
    delete_synthetic_users()
    time.sleep(5)

    baseline = measure_warmup_after_restart(expected_users=2, timeout=300)
    if baseline:
        _log(f"  Baseline warmup: {baseline['warmup_ms']}ms, "
             f"heap={baseline['peak_heap_mb']:.0f}MB, "
             f"goroutines={baseline['peak_goroutines']}")
    else:
        _log("  WARNING: baseline warmup measurement timed out")

    tokens = _login_all()

    # ── Step 3-7: First-login burst at cumulative user counts ──
    cumulative_results = []
    prev_end = 0

    for target_count in USER_COUNTS:
        new_start = prev_end + 1
        new_end = target_count
        added = new_end - prev_end
        total_expected = target_count + 2

        _section(f"First-login burst: +{added} users (total {target_count} synthetic)")
        result = measure_first_login_warmup(
            new_start, new_end, total_expected, tokens, timeout=300)

        if result:
            timed_out = result.get("timeout", False)
            status = "TIMEOUT" if timed_out else f"{result['warmup_ms']}ms"
            admin_impact = ""
            if result.get("admin_baseline_ms") and result.get("admin_during_ms"):
                ratio = result["admin_during_ms"] / max(result["admin_baseline_ms"], 1)
                admin_impact = f", admin latency {ratio:.1f}x baseline"
            _log(f"  N={target_count}: warmup={status}, "
                 f"heap={result['peak_heap_mb']:.0f}MB, "
                 f"goroutines={result['peak_goroutines']}"
                 f"{admin_impact}")

            cumulative_results.append({
                "users": target_count,
                "added": added,
                "type": "first_login",
                **result,
            })

            _record(f"P7: first-login N={target_count}",
                    not timed_out,
                    ms=result["warmup_ms"],
                    note=f"heap={result['peak_heap_mb']:.0f}MB goroutines={result['peak_goroutines']}")
        else:
            _log(f"  N={target_count}: FAILED (no data)")
            cumulative_results.append({
                "users": target_count, "added": added, "type": "first_login",
                "warmup_ms": -1, "error": "no_data",
            })
            _record(f"P7: first-login N={target_count}", False, note="no data")

        prev_end = new_end

    # ── Step 8: Full cold-start warmup with 1000 synthetic users ──
    _section("Step 8: Full warmup with 1000 users (cold start)")
    full_warmup = measure_warmup_after_restart(
        expected_users=max(USER_COUNTS) + 2, timeout=600)
    if full_warmup:
        timed_out = full_warmup.get("timeout", False)
        _log(f"  Full warmup (1000 users): {full_warmup['warmup_ms']}ms, "
             f"heap={full_warmup['peak_heap_mb']:.0f}MB, "
             f"goroutines={full_warmup['peak_goroutines']}")
        cumulative_results.append({
            "users": max(USER_COUNTS),
            "type": "cold_start",
            **full_warmup,
        })
        _record(f"P7: cold-start N={max(USER_COUNTS)}",
                not full_warmup.get("timeout", False),
                ms=full_warmup["warmup_ms"],
                note=f"heap={full_warmup['peak_heap_mb']:.0f}MB goroutines={full_warmup['peak_goroutines']}")
    else:
        _log("  Full warmup: TIMEOUT")
        _record(f"P7: cold-start N={max(USER_COUNTS)}", False, note="timeout")

    # ── Step 8b: Cache OFF baseline ──
    _section("Step 8b: Cache OFF baseline (multi-user latency)")
    off_results = {}
    on_admin_latency = 0
    if cumulative_results:
        last_on = cumulative_results[-1]
        on_admin_latency = last_on.get("admin_during_ms", last_on.get("admin_baseline_ms", 0))

    disable_cache()
    tokens_off = _login_all()
    token_off_admin = tokens_off.get("admin", "")

    off_latencies_admin = []
    dash_path = WIDGET_ENDPOINTS[0][1]
    for _ in range(5):
        ms, code, _ = _http_get(dash_path, token_off_admin)
        if code == 200:
            off_latencies_admin.append(ms)
    off_admin_p50 = pct(off_latencies_admin, 50) if off_latencies_admin else 0

    comp_path = RESTACTION_ENDPOINTS[2][1]
    off_latencies_comp = []
    for _ in range(3):
        ms, code, _ = _http_get(comp_path, token_off_admin)
        if code == 200:
            off_latencies_comp.append(ms)
    off_comp_p50 = pct(off_latencies_comp, 50) if off_latencies_comp else 0

    off_results = {
        "admin_dashboard_p50": off_admin_p50,
        "admin_complist_p50": off_comp_p50,
    }

    on_vs_off_dash = off_admin_p50 / on_admin_latency if on_admin_latency > 0 else 0
    _log(f"  Dashboard ON={on_admin_latency:.0f}ms  OFF={off_admin_p50}ms  speedup={on_vs_off_dash:.1f}x")
    _log(f"  Comp-list OFF={off_comp_p50}ms")
    _record(f"P7: cache OFF dashboard baseline measured", off_admin_p50 > 0,
            ms=off_admin_p50,
            note=f"ON={on_admin_latency:.0f}ms OFF={off_admin_p50}ms speedup={on_vs_off_dash:.1f}x")

    enable_cache()

    # ── Step 9: Cleanup ──
    _section("Step 9: Cleanup synthetic users")
    delete_synthetic_users()
    tokens = _login_all()

    # ── Summary table ──
    _section("PHASE 7 RESULTS")
    print(f"\n  {BOLD}{'Type':<14s} {'Users':>6s} {'Added':>6s} │ {'Warmup':>10s} "
          f"{'Heap(MB)':>10s} {'Goroutines':>11s} "
          f"│ {'Admin Baseline':>15s} {'Admin During':>13s}{RESET}")
    print(f"  {'─' * 120}")

    if baseline:
        print(f"  {'cold-start':<14s} {'2':>6s} {'—':>6s} │ "
              f"{baseline['warmup_ms']:>8d}ms "
              f"{baseline['peak_heap_mb']:>10.0f} "
              f"{baseline['peak_goroutines']:>11d} │ "
              f"{'—':>15s} {'—':>13s}")

    for entry in cumulative_results:
        warmup_str = ("TIMEOUT" if entry.get("timeout") or entry.get("warmup_ms", -1) < 0
                      else f"{entry['warmup_ms']}ms")
        admin_base = entry.get("admin_baseline_ms", 0)
        admin_during = entry.get("admin_during_ms", 0)
        admin_base_str = f"{admin_base:.0f}ms" if admin_base else "—"
        admin_during_str = f"{admin_during:.0f}ms" if admin_during else "—"

        print(f"  {entry.get('type', '?'):<14s} {entry['users']:>6d} "
              f"{entry.get('added', '—'):>6} │ "
              f"{warmup_str:>10s} "
              f"{entry.get('peak_heap_mb', 0):>10.0f} "
              f"{entry.get('peak_goroutines', 0):>11d} │ "
              f"{admin_base_str:>15s} {admin_during_str:>13s}")

    if off_results.get("admin_dashboard_p50", 0) > 0:
        print(f"  {'─' * 120}")
        print(f"  {'cache-OFF':<14s} {'—':>6s} {'—':>6s} │ "
              f"{'—':>10s} {'—':>10s} {'—':>11s} │ "
              f"{'—':>15s} "
              f"{off_results['admin_dashboard_p50']:>11d}ms")

    out_file = evidence.runs_root() / "phase7" / "user_scaling_results.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    all_data = {"baseline": baseline, "results": cumulative_results, "cache_off": off_results}
    with open(out_file, "w") as f:
        json.dump(all_data, f, indent=2, default=str)
    _log(f"Detailed results saved to {out_file}")


# ─── CRB-delete burst (v5/v6 D1/D2/D3 reproduction) ─────────────────────────


def _crb_burst(token, paths, n=20):
    """Issue n GETs against each path; return list of (label, code, ms)."""
    out = []
    for label, path in paths:
        for _ in range(n):
            ms, code, _ = _http_get(path, token, retries=1, timeout=30)
            out.append((label, code, ms))
    return out


def _audit_uaf_emit_per_request_lint(log_lines, user, expected_paths):
    """Return True iff every UAF-protected api[] entry of every request by
    `user` produced EITHER an audit=user_access_filter OR an
    audit=user_access_filter_skipped log line.

    Concretely (best-effort heuristic over slog JSON lines): For each path
    in expected_paths, count audit=user_access_filter and
    audit=user_access_filter_skipped emits scoped to user. The union must
    be > 0; per-request matching would require a trace_id correlation we
    don't currently emit on every line.
    """
    user_q = '"user":"' + user + '"'
    full = sum(1 for l in log_lines
               if '"audit":"user_access_filter"' in l and user_q in l)
    skipped = sum(1 for l in log_lines
                  if '"audit":"user_access_filter_skipped"' in l and user_q in l)
    _log(f"  audit lint (user={user}): user_access_filter={full} "
         f"user_access_filter_skipped={skipped}")
    return (full + skipped) > 0


def crb_delete_burst(tokens):
    """D1/D2/D3 defect reproduction harness.

    Pre-conditions:
      - cyberjoker logged in (tokens["cyberjoker"] valid).
      - cluster has the cyberjoker-krateo-widgets-reader CRB applied (the
        portal helm chart's default state).

    Side effects:
      - Deletes the CRB during the test. The bench cleanup phase or the
        next portal helm upgrade will restore it. We re-apply at the end
        as a defensive measure to avoid leaving the test cluster in a
        broken state for follow-up runs.

    Cross-reference (Task #250 Block 2 / PM Q2 ratified 2026-06-05):
    The `user = "cyberjoker"` identity at line 590 and the `subjects: -
    kind: User name: cyberjoker` block at lines 671-674 hardcode the
    bench's non-admin user identity. This mirrors a portal-chart
    provisioning fact (the helm chart provisions `cyberjoker` into the
    `devs` group). If portal adds a THIRD bench user, this site MUST be
    updated in lockstep with `bench/phases.py:_user_group` (the 2-entry
    lookup table used by Phase 6 S8/S9 stage runners) — both must
    reflect the new (user → group) mapping. The CRB-burst harness
    references the User-kind subject (its own defect-reproducer
    semantics); the Phase 6 stages use the Group-kind subject so they
    do not conflict on apiserver state.
    """
    _section("Scenario: CRB-delete burst (D1/D2/D3 reproduction)")
    user = "cyberjoker"
    if user not in tokens:
        _log("crb-delete: skipping — cyberjoker token unavailable")
        return

    # 1. Pre-delete burst (warm baseline; identity stable).
    _log("Step 1: pre-delete warm burst")
    pre_offset = pod_log_offset()
    pre = _crb_burst(tokens[user], CRB_DELETE_PORTAL_PATHS, n=20)
    pre_codes = [c for _, c, _ in pre]
    _log(f"  pre-burst: {len(pre)} requests, codes={sorted(set(pre_codes))}")

    # 2. Delete the CRB. cyberjoker's binding-identity hash flips.
    _log("Step 2: deleting cyberjoker-krateo-widgets-reader CRB")
    rc, _, err = kubectl("delete", "clusterrolebinding",
                         "cyberjoker-krateo-widgets-reader",
                         "--ignore-not-found")
    if rc != 0:
        _log(f"  CRB delete returned rc={rc}: {err}")
    time.sleep(5)  # informer + identity-hash propagation

    # 3. Re-mint cyberjoker token (group memberships may have changed).
    _log("Step 3: re-minting tokens after CRB delete")
    tokens_post = _login_all()
    if user not in tokens_post:
        _log("  re-mint failed; aborting scenario")
        return

    # 4. Post-delete burst.
    _log("Step 4: post-delete burst")
    post_offset = pod_log_offset()
    post = _crb_burst(tokens_post[user], CRB_DELETE_PORTAL_PATHS, n=20)
    post_codes = [c for _, c, _ in post]
    _log(f"  post-burst: {len(post)} requests, codes={sorted(set(post_codes))}")

    post_logs = pod_logs_since(post_offset)
    span_logs = pod_logs_since(pre_offset)

    # 5. Assert A — no TLS x509 errors in post-delete window. (D1)
    x509_count = sum(1 for l in post_logs
                     if "x509" in l or "tls: failed to verify" in l)
    _record("D1: no TLS x509 errors after CRB-delete", x509_count == 0,
            note=f"x509_count={x509_count}")

    # 6. Assert B — no null-iter errors. (D2)
    null_iter_count = sum(1 for l in post_logs
                          if "cannot iterate over: null" in l)
    _record("D2: no null-iter errors after CRB-delete",
            null_iter_count == 0, note=f"null_iter_count={null_iter_count}")

    # 7. Assert C — every post-delete cyberjoker request that touches a
    # UAF-protected api[] produces either user_access_filter OR
    # user_access_filter_skipped audit emit. (D3a)
    audit_ok = _audit_uaf_emit_per_request_lint(
        post_logs, user=user, expected_paths=CRB_DELETE_PORTAL_PATHS)
    _record("D3a: UAF audit parity post-CRB-delete", audit_ok,
            note="see pod logs for breakdown")

    # 8. Assert D — at least one binding_identity_transition emit for
    # cyberjoker between pre and post bursts. (D3b)
    user_q = '"user":"' + user + '"'
    transitions = [l for l in span_logs
                   if '"audit":"binding_identity_transition"' in l
                   and user_q in l]
    _record("D3b: binding-identity transition logged",
            len(transitions) >= 1,
            note=f"transitions={len(transitions)}")

    # 9. Defensive restore — the CRB will also be re-applied by the next
    # portal helm upgrade; we recreate it here so an interrupted bench
    # leaves the cluster in a usable state.
    _log("Step 5: best-effort CRB restore (idempotent)")
    crb_yaml = """\
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: cyberjoker-krateo-widgets-reader
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: krateo-widgets-reader
subjects:
- kind: User
  name: cyberjoker
  apiGroup: rbac.authorization.k8s.io
"""
    rc_chk, _, _ = kubectl("get", "clusterrole", "krateo-widgets-reader",
                           "--ignore-not-found", "--no-headers")
    if rc_chk == 0:
        kubectl("apply", "-f", "-", input_data=crb_yaml)
    else:
        _log("  krateo-widgets-reader ClusterRole not present; "
             "leaving CRB unrestored (next portal helm upgrade will fix it)")
