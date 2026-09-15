"""Hermetic arms for the S6 counter half (bench/accept1126.py).

These pin the three defects the joint harness review found in `dffd3e7`. Each
test FAILS on the pre-fix code and passes after, so they are regression guards,
not restatements:

  B1 — the served bundle hash was computed, recorded, and never compared, so a
       tag-only gate could pass while the browser was served different content.
       (An earlier version of this note attributed that to a same-tag re-push
       of frontend:1.6.11. That was RETRACTED — it was the 1.6.10 -> 1.6.11
       roll, and the mistake was comparing a hash captured on 09-14 against a
       tag read on 09-15. The assertion stands on the narrower principle: the
       published tag is not the deployed build, and only a hash captured in the
       same preflight describes the same moment.)
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

import base64
import json
import os
import subprocess

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


def test_a6_tolerates_a_second_delivery_from_the_ordinary_publish_path():
    """`delivered` is fed by PublishRefresh AND PublishEviction; evict_published counts only
    the second. The probe page arms the whole shell (~10 keys), so an ordinary refresh of any
    other armed key delivers a second frame inside the window — run 9 read delivered 2 against
    evict_published 1 for exactly that reason. A6 is bounded so ordinary refresh traffic cannot
    fail a run; exactness lives in A5 and the per-key channels, not in a global counter.
    """
    a6 = next(r for r in a.stage_a_rows(1) if r.rid == "A6")
    ok, actual = a6.check({f"{a.K_BROADCAST}.delivered": 2})
    assert ok and actual == 2
    # Bounded is not unbounded: zero deliveries still fails.
    assert a6.check({f"{a.K_BROADCAST}.delivered": 0})[0] is False


@pytest.mark.parametrize("delivered,published,frames_total,flagged", [
    (1, 1, 1, False),  # clean: the only delivery in the window is the eviction frame
    (2, 3, 2, False),  # the extra delivery reached us and the browser recorded it
    (2, 3, 1, False),  # ...or landed in the settle gap, and published Δ covers it
    (2, 0, 1, True),   # no publish accounts for it — contradicts subscribers == 1
    (1, 1, 4, True),   # more frames than deliveries — a channel is miscounting
])
def test_delivery_attribution_flags_only_unaccountable_deliveries(
        delivered, published, frames_total, flagged):
    r = a.delivery_attribution(
        {f"{a.K_BROADCAST}.delivered": delivered,
         f"{a.K_BROADCAST}.published": published,
         f"{a.K_BROADCAST}.evict_published": 1},
        {"data": {"frames_total": frames_total, "frames_for_armed_key": 1}})
    assert r["checked"] is True
    assert r["flagged"] is flagged


def test_delivery_attribution_is_inert_without_frames_total():
    """A browser half that predates frames_total must leave the diagnostic unevaluated rather
    than flag — attribution never gates the stage."""
    r = a.delivery_attribution(
        {f"{a.K_BROADCAST}.delivered": 2, f"{a.K_BROADCAST}.published": 3},
        {"data": {"frames_for_armed_key": 1}})
    assert r["checked"] is False and r["flagged"] is False


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


def _insp(count, sha="abc123", age=3, cls="widgets"):
    """One /debug/apistage?key_hash= reading. count=0 models a DECLINED widget: the key was
    stamped, no entry was ever stored, so nothing can be evicted for it later."""
    meta = {"cacheEntryClass": cls, "bodySha256": sha, "ageSeconds": age} if count else None
    return {"count": count, "meta": meta}


def test_cache_proof_passes_when_the_entry_is_resident_and_served_from_the_store():
    ok = a.crosscheck_cache_proof(1, 1, _insp(1, age=3), _insp(1, age=6))
    assert ok["passed"] and ok["verdict"] == "ok"


def test_cache_proof_rejects_a_declined_widget_riding_foreign_counter_traffic():
    """THE case the whole per-key channel exists for. store_total/hit_total are GLOBAL, so
    another session's fetch satisfies >=1 while our widget was DECLINED and never stored.
    The inspector says our key is not resident, so counters-only must NOT pass."""
    cc = a.crosscheck_cache_proof(5, 9, _insp(0), _insp(0))
    assert not cc["passed"]
    assert "NOT resident" in cc["verdict"]


def test_cache_proof_rejects_an_entry_that_was_replaced_rather_than_served():
    cc = a.crosscheck_cache_proof(1, 1, _insp(1, sha="aaa", age=3),
                                  _insp(1, sha="zzz", age=6))
    assert not cc["passed"]
    assert "sha256 CHANGED" in cc["verdict"]


def test_cache_proof_rejects_a_re_resolve_that_reset_the_age():
    """A re-resolve REPLACES the entry and restarts its age, so an age that went backwards
    means the resolver served that call, not the store — even when the bytes are identical,
    which is entirely likely for a static Paragraph."""
    cc = a.crosscheck_cache_proof(1, 1, _insp(1, age=9), _insp(1, age=1))
    assert not cc["passed"]
    assert "age RESET" in cc["verdict"]


def test_cache_proof_REPORTS_a_MISSING_body_hash_as_missing_not_as_changed():
    """Run 7. I read the Go FIELD name (BodySHA256) and assumed the wire name; the tag is
    `bodySha256`, so the lookup returned None on every read, `same_body` was permanently False,
    and the stage failed with a verdict blaming a change that never happened. An absent hash is
    a harness or endpoint problem; a changed hash is a cache finding. They must not read alike."""
    cc = a.crosscheck_cache_proof(1, 1, _insp(1, sha=None), _insp(1, sha=None))
    assert not cc["passed"]
    assert "no body hash" in cc["verdict"] and "not evidence" in cc["verdict"]


def test_cache_proof_REFUSES_a_zero_age_rather_than_passing_vacuously():
    """RB2. At age 0 the reset comparison degrades to `>= 0`, which nothing can fail — the
    same defect as the header-based proof this replaced. A check that cannot fail must not
    silently pass, so age 0 reaching the cross-check FAILS."""
    cc = a.crosscheck_cache_proof(1, 1, _insp(1, age=0), _insp(1, age=0))
    assert not cc["passed"]
    assert "cannot discriminate" in cc["verdict"]


def test_cache_proof_rejects_the_wrong_cache_entry_class():
    """Run 6 measured the child's key as class `widgets` and the facts doc's `widgetContent`
    prediction was wrong: widgetContent is the identity-FREE shell layer the walker populates
    under the SA, while a widget the BROWSER fetches through /call is keyed per-identity under
    `widgets`. So `widgets` is correct here and widgetContent is the anomaly."""
    cc = a.crosscheck_cache_proof(1, 1, _insp(1, cls="widgetContent"),
                                  _insp(1, cls="widgetContent"))
    assert not cc["passed"]
    assert "widgets" in cc["verdict"]


def test_cache_proof_rejects_an_inspector_hit_the_counters_do_not_corroborate():
    cc = a.crosscheck_cache_proof(0, 0, _insp(1, age=3), _insp(1, age=6))
    assert not cc["passed"]
    assert "neither global counter moved" in cc["verdict"]


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
    """Probe for B1, with two real bundle hashes observed on 057 (the 1.6.10
    and 1.6.11 bundles). The point is only that the compare distinguishes two
    genuinely different served bundles — NOT that either came from a re-push.
    """
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


def _mutant_cache_proof(stored, hits, after_render, after_hit):
    """MUTANT of crosscheck_cache_proof with the PER-KEY channel REMOVED."""
    return {"passed": stored >= 1 and hits >= 1}


def test_MUTATION_dropping_the_per_key_channel_accepts_a_declined_widget():
    """Probe for review follow-up 1, restated for the inspector channel: counters-only
    accepts foreign traffic. The declined widget has NO entry, so the real check sees
    count=0 and fails; the mutant sees only the global counters and passes."""
    assert not a.crosscheck_cache_proof(5, 9, _insp(0), _insp(0))["passed"]
    assert _mutant_cache_proof(5, 9, _insp(0), _insp(0))["passed"], (
        "the mutant must pass a DECLINED widget on foreign counter movement")


def _mutant_age_guard(age0, age1):
    """MUTANT of the age check as it was committed at 375d301: the comparison alone, with no
    measurability precondition."""
    return (age1 or 0) >= (age0 or 0)


def test_MUTATION_the_unguarded_age_comparison_cannot_fail_on_a_fresh_entry():
    """Probe for RB2. AgeSeconds is an integer and the first lookup used to happen immediately
    after the cold fill, so age0 was 0 and `age1 >= age0` reduced to `age1 >= 0` — true for
    EVERY reading, including a re-resolve that reset the age to 0. The mutant accepts exactly
    that; the real check refuses it, because a predicate that cannot fail must not pass."""
    assert _mutant_age_guard(0, 0), "the mutant must accept a reset at age 0 — the defect"
    assert not a.crosscheck_cache_proof(1, 1, _insp(1, age=0), _insp(1, age=0))["passed"], (
        "the real check must refuse an unmeasurable age rather than pass vacuously")
    # And with a measurable age it still discriminates in the direction that matters.
    assert a.crosscheck_cache_proof(1, 1, _insp(1, age=3), _insp(1, age=6))["passed"]
    assert not a.crosscheck_cache_proof(1, 1, _insp(1, age=9), _insp(1, age=1))["passed"]


def _mutant_reconcile_check(reqlog, start, end):
    """MUTANT of the §16 self-check with the URL scan REMOVED."""
    return []


def test_MUTATION_dropping_the_reconcile_scan_hides_a_perturbing_call():
    rl = a._RequestLog()
    rl.record("GET", "https://p/content/debug/reconcile", 200)
    assert rl.touched_reconcile(PAST, FUTURE), "the real scan must see it"
    assert _mutant_reconcile_check(rl, PAST, FUTURE) == [], (
        "the mutant must miss it — that is what the scan buys")


# ─── Credential routes (harness-user-spec.md §6/§7) ────────────────────────


def test_creds_prefers_the_environment_password(monkeypatch):
    monkeypatch.setenv("HARNESS_USER", "s6-harness")
    monkeypatch.setenv("HARNESS_PASSWORD", "pw")
    monkeypatch.delenv("S6_PASSWORD_FROM_SECRET", raising=False)
    assert a._creds() == ("s6-harness", "pw")


def test_creds_raises_rather_than_skipping_when_absent(monkeypatch):
    monkeypatch.delenv("HARNESS_USER", raising=False)
    monkeypatch.delenv("HARNESS_PASSWORD", raising=False)
    monkeypatch.delenv("S6_PASSWORD_FROM_SECRET", raising=False)
    with pytest.raises(a.PreflightFailed):
        a._creds()


def test_creds_reads_the_scoped_secret_when_asked(monkeypatch):
    """Option (b): the harness user's OWN Secret, not a platform credential."""
    monkeypatch.setenv("HARNESS_USER", "s6-harness")
    monkeypatch.delenv("HARNESS_PASSWORD", raising=False)
    monkeypatch.setenv("S6_PASSWORD_FROM_SECRET", "s6-harness-password")
    seen = {}

    def _fake_kubectl(*args, **kw):
        seen["args"] = args
        # kubectl -o jsonpath returns Secret data STILL base64-encoded.
        return 0, base64.b64encode(b"sekrit").decode(), ""

    monkeypatch.setattr(a.cluster, "kubectl", _fake_kubectl)
    assert a._creds() == ("s6-harness", "sekrit")
    assert "s6-harness-password" in seen["args"]


def test_secret_read_decodes_base64_rather_than_passing_the_blob(monkeypatch):
    """kubectl jsonpath does NOT decode Secret data; forgetting that sends a
    base64 blob as the password and yields a confusing 401."""
    def _fake_kubectl(*args, **kw):
        return 0, base64.b64encode(b"hunter2").decode(), ""
    monkeypatch.setattr(a.cluster, "kubectl", _fake_kubectl)
    assert a._password_from_secret("s6-harness-password") == "hunter2"


def test_secret_read_failure_is_loud(monkeypatch):
    def _fake_kubectl(*args, **kw):
        return 1, "", "Error from server (Forbidden)"
    monkeypatch.setattr(a.cluster, "kubectl", _fake_kubectl)
    with pytest.raises(a.PreflightFailed):
        a._password_from_secret("s6-harness-password")


# ─── S6 browser half: the manifest pair ────────────────────────────────────
#
# Run 1 died at `kubectl apply` on a required field the facts-doc sketch omitted
# (Flex.spec.widgetData.allowedResources). The offline checks that preceded it asserted the
# shape the DOCUMENT pins — child free of apiRef/resourcesRefs/keyExtras, root ref GET at the
# child, ownership prefix, root name — every one of which passed on an invalid CR, because
# none of them asked the schema. The structural arms stay; the dry-run below is what actually
# catches this class, and it is the arm that would have saved the run.

from bench import s6browser as sb  # noqa: E402


def _pair(run_id="deadbeefcafe"):
    return json.loads(sb._manifests(run_id))["items"]


def test_s6_manifest_child_avoids_every_decline_branch():
    child = next(i for i in _pair() if i["kind"] == "Paragraph")
    for forbidden in ("apiRef", "resourcesRefs", "keyExtras"):
        assert forbidden not in child["spec"], (
            f"{forbidden} on the child would make it decline-eligible (facts §10.2); a declined "
            f"body arms a key with no entry behind it and nothing can ever be evicted for it")


def test_s6_manifest_root_is_named_for_the_shipped_nav_entry():
    root = next(i for i in _pair() if i["kind"] == "Flex")
    assert root["metadata"]["name"] == "page-s6-probe", (
        "the /s6-probe route resolves flexes/page-<slug> by convention, so a per-run root name "
        "resolves to nothing and the widget never renders or arms")


def test_s6_manifest_root_declares_allowedResources_for_the_child():
    """The field run 1 died on. It is REQUIRED by the Flex CRD and names the child's resource
    plural; the §10.5 sketch had only `items`."""
    root = next(i for i in _pair() if i["kind"] == "Flex")
    wd = root["spec"]["widgetData"]
    assert wd.get("allowedResources") == ["paragraphs"]
    assert wd.get("items"), "the CRD requires items alongside allowedResources"


#: Deliberately NOT gated on BENCH_GKE_CONTEXT: conftest.py:30 pops that variable so the suite
#: is hermetic against the operator's environment, so a gate on it could never fire and this
#: arm would skip forever — a guard that cannot run is not a guard. Its own opt-in instead, and
#: it carries the context explicitly rather than inheriting one.
@pytest.mark.skipif(os.environ.get("S6_LIVE_SCHEMA_CHECK") != "1",
                    reason="live schema check: set S6_LIVE_SCHEMA_CHECK=1 (+ S6_SCHEMA_CONTEXT)")
def test_s6_manifest_IS_ACCEPTED_BY_THE_LIVE_CRD():
    """The arm that would have saved run 1.

    Every other manifest arm asserts the shape the facts doc describes, and all of them passed
    on a CR the API server rejected — a document is not a schema. This one asks the schema,
    server-side, creating nothing: `kubectl apply --dry-run=server` validates exactly as a real
    apply would and writes nothing.
    """
    ctx = os.environ.get("S6_SCHEMA_CONTEXT") or sb.accept1126.EXPECTED_CONTEXT
    proc = subprocess.run(
        ["kubectl", "--context", ctx, "apply", "--dry-run=server", "-f", "-"],
        input=sb._manifests("dryrunprobe0"), capture_output=True, text=True)
    assert proc.returncode == 0, (
        f"the live CRDs REJECT the manifest pair:\n{proc.stderr.strip()[:500]}")
