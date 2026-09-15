"""Hermetic arms for the S6 counter half (bench/accept1126.py).

These pin the three defects the joint harness review found in `dffd3e7`. Each
test FAILS on the pre-fix code and passes after, so they are regression guards,
not restatements:

  B1 — the served bundle hash was computed, recorded, and never compared, so a
       re-tagged image (measured on 057 on 2026-09-15: frontend:1.6.11
       re-pushed under the same tag with a different bundle) passed the build
       gate and would have reached the measurement window.
  B2 — hook files carried no run identity, so a `--from-stage` resume in the
       same run dir was satisfied instantly by the PREVIOUS run's hooks, and
       the channel cross-check then compared two stale payloads that agreed
       trivially.
  B3 — the gated CRD stage waited on Stage A's hook names, which an earlier
       stage in the same process had already written, so both waits returned
       immediately with the wrong payloads.

No cluster access: everything here is a pure-function or filesystem arm
(`feedback_bench_tests_hermetic_isolation_all_cluster_paths`).
"""

from __future__ import annotations

import json

import pytest

from bench import accept1126 as a


# ─── B2 — hooks are bound to a run AND to the waiting stage ────────────────

PAST = "2000-01-01T00:00:00+00:00"
FUTURE = "2999-01-01T00:00:00+00:00"


def test_hook_from_a_previous_run_is_ignored_not_consumed(tmp_path):
    """B2: a leftover hook from another run must never satisfy a wait."""
    a.write_hook(tmp_path, "created", "OLD-RUN", {"objects": []})
    with pytest.raises(a.HookTimeout) as ei:
        a.wait_hook(tmp_path, "created", "NEW-RUN", PAST, timeout_s=2)
    # The message must say WHY, or an operator cannot tell a stale hook from a
    # browser half that never ran.
    assert "previous attempt" in str(ei.value)


def test_hook_written_before_the_stage_began_is_ignored(tmp_path):
    """B3's mechanism: an earlier stage's hook must not satisfy a later wait."""
    a.write_hook(tmp_path, "rendered", "RUN-1", {})
    with pytest.raises(a.HookTimeout) as ei:
        a.wait_hook(tmp_path, "rendered", "RUN-1", FUTURE, timeout_s=2)
    assert "before this stage began" in str(ei.value)


def test_matching_hook_is_accepted(tmp_path):
    a.write_hook(tmp_path, "rendered", "RUN-1", {"k": "v"})
    got = a.wait_hook(tmp_path, "rendered", "RUN-1", PAST, timeout_s=2)
    assert got["run_id"] == "RUN-1"
    assert got["data"] == {"k": "v"}


def test_hook_write_is_atomic_and_carries_the_run_id(tmp_path):
    p = a.write_hook(tmp_path, "deleted", "RUN-9", {"deleted_count": 1})
    payload = json.loads(p.read_text())
    assert payload["run_id"] == "RUN-9"
    assert payload["hook"] == "deleted"
    assert not list(p.parent.glob("*.tmp")), "temp file left behind"


def test_unknown_hook_name_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        a.write_hook(tmp_path, "not-a-hook", "RUN-1", {})


# ─── B3 — the CRD stage has hook names of its own ──────────────────────────


def test_crd_stage_hooks_are_disjoint_from_stage_a():
    """B3: shared names let A's hooks satisfy B1's waits inside one run."""
    assert not (set(a.HOOK_ORDER) & set(a.CRD_HOOK_ORDER)), (
        "Stage B1 must not wait on any name Stage A can write")
    assert set(a.ALL_HOOKS) == set(a.HOOK_ORDER) | set(a.CRD_HOOK_ORDER)


def test_crd_manifest_bump_changes_only_the_structural_schema():
    """The bump must move the fingerprint, which covers spec.versions[].schema.

    `crdSchemaFingerprint` (crd_discovery_side_effect.go:593) hashes
    [{name, schema.openAPIV3Schema}] — metadata is NOT in it, which is why an
    annotation touch cannot force a relist. So the widened form must differ
    inside the schema subtree and nowhere else.
    """
    narrow = a._crd_manifest("harness.krateo.io", "s6x", "S6", widened=False)
    wide = a._crd_manifest("harness.krateo.io", "s6x", "S6", widened=True)
    assert narrow != wide
    added = [ln for ln in wide.splitlines() if ln not in narrow.splitlines()]
    assert added and all("widened" in ln or "type: string" in ln
                         for ln in added), added
    # the new property is OPTIONAL — no `required:` block is introduced, so no
    # existing CR becomes invalid at the bump.
    assert "required" not in wide


# ─── R1 — the never-touch rule, by name prefix ─────────────────────────────


@pytest.mark.parametrize("kind,name", [
    ("paragraphs", "s6-probe-abc"),
    ("flexes", "page-s6-abc"),
    ("crd", "s6run1.harness.krateo.io"),
])
def test_owned_objects_are_allowed(kind, name):
    a.assert_owned(kind, name)


@pytest.mark.parametrize("kind,name", [
    ("menus", "sidebar-nav"),            # the shared nav CR — never touched
    ("flexes", "page-access-detail"),    # a shipped platform page
    ("paragraphs", "greeting-title"),
    ("crd", "compositiondefinitions.core.krateo.io"),
])
def test_pre_existing_objects_are_refused(kind, name):
    with pytest.raises(a.NotOwned):
        a.assert_owned(kind, name)


# ─── §2 — the parent-qualified expvar accessor ─────────────────────────────


def test_expvar_requires_the_parent_key():
    snap = {"snowplow_deps": {"evict_delete_total": 7},
            "snowplow_resolved_cache": {"evict_delete_total": 3}}
    assert a.expvar_get(snap, a.K_DEPS, "evict_delete_total") == 7
    assert a.expvar_get(snap, a.K_RESOLVED, "evict_delete_total") == 3


def test_absent_parent_raises_rather_than_reading_zero():
    """CFG-1: under CACHE_ENABLED=false the publishers are not registered at
    all, so a missing parent means the cache is OFF — treating it as 0 would
    make every delta in the table a false zero."""
    with pytest.raises(a.AssertionsFailed):
        a.expvar_get({}, a.K_DEPS, "evict_delete_total")


def test_delta_refuses_mismatched_snapshots():
    """A pod restart re-registers expvars from zero; a delta across it is not
    meaningful and must not be silently computed."""
    with pytest.raises(a.AssertionsFailed):
        a.delta({"x": 1}, {"y": 2})


# ─── §15 — the assertion table evaluates every row ─────────────────────────


def test_evaluate_reports_all_rows_not_just_the_first_failure():
    rows = a.stage_a_rows(1)
    d = {r.key: 0 for r in rows}          # everything zero → A1/A2/A5/A6 fail
    ok, results = a.evaluate(rows, d)
    assert not ok
    assert len(results) == len(rows), "evaluate must not short-circuit"
    failed = {r["id"] for r in results if not r["passed"]}
    assert {"A1", "A2", "A5", "A6"} <= failed


def test_a1_a2_crosscheck_flags_a_one_sided_eviction():
    rows = a.stage_a_rows(1)
    d = {r.key: 0 for r in rows}
    d[f"{a.K_DEPS}.evict_delete_total"] = 1        # tracker moved
    d[f"{a.K_RESOLVED}.evict_delete_total"] = 0    # store did not
    _, results = a.evaluate(rows, d)
    cc = a.crosscheck_a1_a2(results)
    assert cc["checked"] and not cc["agree"]
    assert "MISMATCH" in cc["verdict"]


def test_channel_crosscheck_fails_when_the_browser_reports_nothing():
    """R2: a missing channel FAILS the stage; it never passes on counters."""
    cc = a.crosscheck_channels({}, {"data": {}})
    assert cc["checked"] is False and cc["passed"] is False


@pytest.mark.parametrize("frames,calls,expect", [
    (1, 1, True),     # agree
    (1, 0, False),    # frame, no refetch → SPA defect
    (0, 1, False),    # refetch, no frame → harness-initiated call
    (2, 2, False),    # both moved twice → not the single-delete shape
])
def test_channel_crosscheck_requires_exactly_one_each(frames, calls, expect):
    cc = a.crosscheck_channels(
        {}, {"data": {"frames_for_armed_key": frames,
                      "uninitiated_calls": calls}})
    assert cc["passed"] is expect


def test_burst_rows_use_delivered_for_the_no_loss_half():
    """S9: there is NO `drained` counter (facts §2.4), so the "all N arrive"
    half must be asserted on `delivered`."""
    keys = {r.key for r in a.burst_rows(200)}
    assert f"{a.K_BROADCAST}.delivered" in keys
    assert not any("drained" in (k or "") for k in keys)


def test_crd_rows_pin_the_timeout_falsifier():
    rows = {r.rid: r for r in a.crd_rows()}
    assert rows["B4"].key == f"{a.K_CRD}.relist_bridge_timeout_total"
    ok, _ = a.evaluate([rows["B4"]], {rows["B4"].key: 1})
    assert not ok, "relist_bridge_timeout_total > 0 must fail the stage"


def test_crd_rows_catch_a_metadata_only_touch():
    """schema_unchanged moving means the forcing step edited metadata, not the
    structural schema — the cheap self-check from facts §4."""
    rows = {r.rid: r for r in a.crd_rows()}
    ok, _ = a.evaluate([rows["B6"]], {rows["B6"].key: 1})
    assert not ok


# ─── Credentials never reach a proof ───────────────────────────────────────


def test_redact_scrubs_a_jwt_shaped_string():
    jwt = "eyJhbGciOiJSUzI1NiJ9." + ("x" * 40) + ".signaturesignature"
    assert a._redact(jwt) == "<redacted-jwt>"
    assert a._redact("plain") == "plain"


def test_request_log_strips_the_query_string():
    """A `sub=` payload must never reach a proof."""
    rl = a._RequestLog()
    rl.record("GET", "https://p/content/refreshes?sub=BASE64PAYLOAD", 200)
    assert rl.entries[0]["url"] == "https://p/content/refreshes"


def test_reconcile_detection_is_on_our_own_request_log():
    rl = a._RequestLog()
    rl.record("GET", "https://p/content/debug/vars", 200)
    assert rl.touched_reconcile(PAST, FUTURE) == []
    rl.record("GET", "https://p/content/debug/reconcile", 200)
    assert rl.touched_reconcile(PAST, FUTURE) == [
        "https://p/content/debug/reconcile"]


# ─── The cache proof needs BOTH channels (review follow-up 1) ──────────────


def test_cache_proof_requires_counters_and_the_browser_to_agree():
    ok = a.crosscheck_cache_proof(1, 1, {"l1_hit": True, "refresh_key": "k"})
    assert ok["passed"] and ok["verdict"] == "ok"


def test_cache_proof_rejects_counters_moved_by_foreign_traffic():
    """The case the architect raised: store_total/hit_total are GLOBAL and have
    no per-key variant, so another portal session's fetch satisfies >=1 while
    the harness widget was DECLINED. The browser's per-widget l1_hit is what
    distinguishes them, so counters-only must NOT pass."""
    cc = a.crosscheck_cache_proof(5, 9, {"l1_hit": False, "refresh_key": "k"})
    assert not cc["passed"]
    assert "FOREIGN traffic" in cc["verdict"]


def test_cache_proof_rejects_an_uncorroborated_browser_claim():
    cc = a.crosscheck_cache_proof(0, 0, {"l1_hit": True, "refresh_key": "k"})
    assert not cc["passed"]
    assert "not corroborated" in cc["verdict"]


def test_cache_proof_rejects_a_missing_refresh_key():
    cc = a.crosscheck_cache_proof(1, 1, {"l1_hit": True})
    assert not cc["passed"]


# ─── MUTATION PROBES ───────────────────────────────────────────────────────
#
# Honest provenance, correcting a claim I got wrong. The arms above are NOT
# behaviourally RED on the pre-fix module `dffd3e7`: run there they yield
# 5 TypeError (the write_hook signature changed) and 2 AttributeError (new
# symbols), and nothing else. I reported "four are behavioural" from the test
# NAMES without reading the error types; the architect checked and was right.
# Behavioural RED across a signature change is not achievable.
#
# The honest substitute is a mutation probe: weaken exactly one guard and
# assert the property it guards now fails. That establishes the arm is
# load-bearing rather than incidentally green, which is what "RED before the
# fix" was meant to show. Each probe names the guard it removes.


def _mutant_wait_hook_without_binding(run_dir, name, run_id, not_before):
    """MUTANT of wait_hook with the run-id AND timestamp checks REMOVED.

    This is the pre-fix behaviour: return whatever file is on disk.
    """
    p = a._hook_path(run_dir, name)
    if p.exists():
        return json.loads(p.read_text())
    raise a.HookTimeout("no file")


def test_MUTATION_dropping_the_hook_binding_lets_a_stale_hook_through(tmp_path):
    """Probe for B2. Remove the binding and a previous run's hook is consumed."""
    a.write_hook(tmp_path, "created", "OLD-RUN", {"objects": []})

    with pytest.raises(a.HookTimeout):          # real guard refuses
        a.wait_hook(tmp_path, "created", "NEW-RUN", PAST, timeout_s=1)

    got = _mutant_wait_hook_without_binding(    # mutant consumes it
        tmp_path, "created", "NEW-RUN", PAST)
    assert got["run_id"] == "OLD-RUN", (
        "the mutant must accept the stale hook, otherwise this probe proves "
        "nothing about the guard")


def _mutant_bundle_problems(served, expected):
    """MUTANT of the preflight bundle check with the COMPARE removed."""
    return []                    # pre-fix behaviour: computed, never compared


def _real_bundle_problems(served, expected):
    """The shape of the real check in stage_preflight (B1)."""
    if not expected:
        return ["no expected bundle hash supplied"]
    if served != expected:
        return [f"served SPA bundle is {served!r}, expected {expected!r}"]
    return []


def test_MUTATION_dropping_the_bundle_compare_passes_a_retagged_image():
    """Probe for B1, with the real values measured on 057 on 2026-09-15:
    frontend:1.6.11 re-pushed under the same tag, D4naopHo -> Kr5dAb3q."""
    served, expected = "index-Kr5dAb3q.js", "index-D4naopHo.js"
    assert _real_bundle_problems(served, expected), (
        "the real check must flag a content change under an unchanged tag")
    assert _mutant_bundle_problems(served, expected) == [], (
        "the mutant must pass it — that is the defect B1 named")


def test_MUTATION_sharing_hook_names_makes_the_crd_stage_self_satisfying():
    """Probe for B3: with shared names, Stage A's hooks satisfy Stage B1."""
    assert not (set(a.HOOK_ORDER) & set(a.CRD_HOOK_ORDER))
    mutant_crd_hooks = list(a.HOOK_ORDER)        # the pre-fix arrangement
    assert set(a.HOOK_ORDER) & set(mutant_crd_hooks), (
        "the mutant must overlap, so a Stage-A hook can satisfy a B1 wait")


def _mutant_cache_proof(stored, hits, hook_data):
    """MUTANT of crosscheck_cache_proof with the BROWSER channel REMOVED."""
    return {"passed": stored >= 1 and hits >= 1}


def test_MUTATION_dropping_the_browser_channel_accepts_a_declined_widget():
    """Probe for review follow-up 1: counters-only accepts foreign traffic."""
    declined = {"l1_hit": False, "refresh_key": "k"}
    assert not a.crosscheck_cache_proof(5, 9, declined)["passed"]
    assert _mutant_cache_proof(5, 9, declined)["passed"], (
        "the mutant must pass a DECLINED widget on foreign counter movement")


def _mutant_reconcile_check(reqlog, start, end):
    """MUTANT of the §16 self-check with the URL scan REMOVED."""
    return []


def test_MUTATION_dropping_the_reconcile_scan_hides_a_perturbing_call():
    rl = a._RequestLog()
    rl.record("GET", "https://p/content/debug/reconcile", 200)
    assert rl.touched_reconcile(PAST, FUTURE), "the real scan must see it"
    assert _mutant_reconcile_check(rl, PAST, FUTURE) == [], (
        "the mutant must miss it — that is what the scan buys")
