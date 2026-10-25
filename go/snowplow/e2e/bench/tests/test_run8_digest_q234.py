"""Run 8 rulings (coordinator, 2026-10-19):

1. Image-digest VOID gate: after the reboot and before any load, every live
   snowplow pod's status.containerStatuses[].imageID must carry
   EXPECTED_IMAGE_DIGEST, else the run aborts VOID (no load, torn down). The
   read digest is recorded ($OUT/image-digest.jsonl) and fed to the verdict.
2. Q2–Q4 in `evaluate --run8`, from keys checked against the /debug/vars the
   1.12.50 tree serves (tests/fixtures/debug_vars_keys_1.12.50.json). Gates:
   stale_to_push p95 <= 3 s / max <= 10 s, push_unarmed == 0, overdue == 0
   (parked debt is NOT overdue), no restart. Bars are constants in load606,
   never read from the instrument under test.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from bench import load606
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import (  # noqa: F401
    BENCH_DIR, FAKE_DIGEST, OTHER_DIGEST, TEARDOWN_TAIL, _calls, harness)
from tests.test_run8 import DELIV, PODS, H, _mem, _navs, q234

K = load606.RUN8_KEYS
FIXTURE = Path(__file__).parent / "fixtures" / "debug_vars_keys_1.12.50.json"
D = "sha256:" + "0123456789abcdef" * 4


def _pod(name, image_id, terminating=False, statuses=True):
    meta = {"name": name}
    if terminating:
        meta["deletionTimestamp"] = "2026-10-19T08:00:00Z"
    st = {"containerStatuses": [{"name": "snowplow", "image": "ghcr.io/x/snowplow:1.12.50",
                                 "imageID": image_id}]} if statuses else {}
    return {"metadata": meta, "status": st}


# ─── 1. the digest gate ─────────────────────────────────────────────────────


@pytest.mark.parametrize("pods,result,why", [
    ([_pod("a", f"ghcr.io/x/snowplow@{D}")], "MATCH", None),
    ([_pod("a", f"docker-pullable://ghcr.io/x/snowplow@{D}")], "MATCH", None),
    ([_pod("a", f"ghcr.io/x/snowplow@{OTHER_DIGEST}")], "VOID", "digest mismatch"),
    ([_pod("a", f"ghcr.io/x/snowplow@{D}"), _pod("b", f"ghcr.io/x/snowplow@{OTHER_DIGEST}")],
     "VOID", "b/snowplow"),
    ([_pod("a", "")], "VOID", "no digest"),                      # not pulled yet
    ([_pod("a", None, statuses=False)], "VOID", "no container status"),
    ([], "VOID", "no live snowplow pod"),
    ([_pod("old", f"ghcr.io/x/snowplow@{OTHER_DIGEST}", terminating=True)],
     "VOID", "no live snowplow pod"),
    # a terminating rollout leftover is recorded, not judged
    ([_pod("old", f"ghcr.io/x/snowplow@{OTHER_DIGEST}", terminating=True),
      _pod("new", f"ghcr.io/x/snowplow@{D}")], "MATCH", None),
])
def test_digest_gate(pods, result, why):
    rec = load606.image_digest_gate({"items": pods}, D)
    assert rec["result"] == result, rec
    if why:
        assert why in rec["why"]
    assert rec["kind"] == "image_digest" and rec["expected"] == D
    assert len(rec["pods"]) == sum(len((p.get("status") or {}).get("containerStatuses") or [])
                                   for p in pods)


@pytest.mark.parametrize("bad", ["", "sha256:abc", D.upper(), D + "0", "latest"])
def test_digest_gate_refuses_a_malformed_expectation(bad):
    rec = load606.image_digest_gate({"items": [_pod("a", f"x@{D}")]}, bad)
    assert rec["result"] == "VOID" and "not sha256" in rec["why"]


def test_digest_gate_cli_exit_codes():
    def run(digest):
        body = json.dumps({"items": [_pod("a", f"ghcr.io/x/snowplow@{digest}")]})
        return subprocess.run(["python3", "-m", "bench.load606", "digest-gate", "--expected", D],
                              input=body, cwd=BENCH_DIR, capture_output=True, text=True)
    ok, bad = run(D), run(OTHER_DIGEST)
    assert ok.returncode == 0 and json.loads(ok.stdout)["result"] == "MATCH"
    assert bad.returncode == 3 and json.loads(bad.stdout)["pods"][0]["digest"] == OTHER_DIGEST


def _gate(result="MATCH", expected=D):
    return {"kind": "image_digest", "expected": expected, "result": result,
            "why": None if result == "MATCH" else "digest mismatch: a/snowplow: x",
            "pods": [{"digest": D if result == "MATCH" else OTHER_DIGEST}]}


def _pods(after=D):
    return [dict(PODS[0], digest=D), dict(PODS[1], digest=after)]


@pytest.mark.parametrize("gates,pods,ok,why", [
    ([_gate()], _pods(), True, None),
    ([], _pods(), False, "gate did not run"),
    ([_gate("VOID")], _pods(), False, "digest mismatch"),
    ([_gate(expected=OTHER_DIGEST)], _pods(), False, "VOID"),
    ([_gate()], _pods(after=OTHER_DIGEST), False, "(after) runs"),   # replaced mid-run
])
def test_verdict_is_invalid_unless_the_digest_held(gates, pods, ok, why):
    r = load606.evaluate([], [], [], snowplow_pods=pods, expected_digest=D, image_digests=gates)
    c = next(v for v in r["validity"] if v["check"] == "snowplow.image_digest")
    assert (c["result"] == "PASS") is ok, c
    if not ok:
        assert r["verdict"] == "INVALID" and why in c["why"]
    assert r["report"]["image_digest"]["expected"] == D


def test_run606_mismatched_digest_aborts_void_before_any_load(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1",
               FAKE_IMAGE_DIGEST=OTHER_DIGEST)                 # pod runs another image
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode != 0
    assert "=== step digest ===" in p.stderr
    assert f"VOID: a snowplow pod does not run {FAKE_DIGEST}" in p.stderr
    assert "=== step measure ===" not in p.stderr
    calls = _calls(state)
    applied = [c["argv"] for c in calls if c["tool"] == "kubectl" and c["argv"][:1] == ["apply"]]
    assert not any("loadgen-job.yaml" in " ".join(a) for a in applied)   # no load Job
    assert not (tmp / "out" / "verdict.json").exists()
    rec = json.loads((tmp / "out" / "image-digest.jsonl").read_text())  # the read is kept
    assert rec["result"] == "VOID" and rec["expected"] == FAKE_DIGEST
    assert [r["digest"] for r in rec["pods"]] == [OTHER_DIGEST]
    gcl = [c["argv"][:3] for c in calls if c["tool"] == "gcloud"]
    assert gcl[-3:] == TEARDOWN_TAIL                                  # torn down


def test_run606_matching_digest_is_recorded_in_the_verdict(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    assert f"image digest MATCH: every live snowplow pod runs {FAKE_DIGEST}" in p.stderr
    assert p.stderr.index("=== step digest ===") < p.stderr.index("=== step measure ===")
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    c = next(x for x in v["validity"] if x["check"] == "snowplow.image_digest")
    assert c["result"] == "PASS" and c["value"] == FAKE_DIGEST
    assert v["report"]["image_digest"]["gate"][0]["pods"][0]["digest"] == FAKE_DIGEST
    assert all(r["digest"] == FAKE_DIGEST for r in v["report"]["snowplow_pod"])


@pytest.mark.parametrize("value", [None, "sha256:short", "1.12.50"])
def test_run606_refuses_a_missing_or_malformed_digest_before_any_cloud_call(harness, value):
    env, state, ambient, tmp = harness
    if value is None:
        env.pop("EXPECTED_IMAGE_DIGEST")
    else:
        env["EXPECTED_IMAGE_DIGEST"] = value
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode != 0 and "EXPECTED_IMAGE_DIGEST" in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]


def test_run606_refuses_prefixes_that_miss_a_run8_key(harness):
    env, state, ambient, tmp = harness
    env["MEM_VARS_PREFIXES"] = "snowplow_apistage_content_fill snowplow_apistage_stale_"
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 2
    assert "snowplow_stale_retry_viewers" in p.stderr and "refusing" in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]


# ─── 2. the keys exist on 1.12.50, and the capture takes them ───────────────


def test_every_run8_key_is_served_by_1_12_50_with_the_shape_read():
    inv = json.loads(FIXTURE.read_text())
    keys = inv["keys"]
    missing = sorted(set(K.values()) - set(keys))
    assert not missing, missing
    assert set(load606.RUN8_RESOLVED_CACHE_STATS) <= set(inv["resolved_cache_members"])
    assert any(m.startswith("known_stale_served_") and m != "known_stale_served_total"
               for m in inv["resolved_cache_members"])
    # the shapes the report reads
    assert set(keys[K["admission_queued"]]) == {"background", "customer", "debt"}
    assert set(keys[K["admission_timeout"]]) == {"background", "customer", "debt"}
    assert set(keys[K["admission_wait"]]) == {"background", "customer", "debt"}
    assert "le_3000" in keys[K["stale_to_push"]]["histogram"]          # p95 decidable
    assert "le_10" in keys[K["admission_wait"]]["background"]["histogram"]
    assert set(keys[K["stale_serve"]]) == {"known_stale"}
    assert set(keys[K["stale_withdrawn"]]) == {"unarmable"}
    assert set(keys[K["retry_total"]]) == {"transient", "genuine"}
    assert set(keys[K["retry_total"]]["genuine"]) == {"background", "demand", "debt"}
    assert set(keys[K["retry_abandoned"]]) == {"no_viewer", "evicted"}
    for k in ("stale_push_unarmed", "stale_overdue", "debt_parked", "retry_viewers",
              "customer_inflight"):
        assert keys[K[k]] == "number", k


def test_default_capture_prefixes_cover_every_run8_key():
    assert load606.prefixes_missing(load606.RUN8_VARS_PREFIXES) == []
    assert "snowplow_stale_retry_viewers" in load606.prefixes_missing(
        [p for p in load606.RUN8_VARS_PREFIXES if p != "snowplow_stale_retry_"])
    assert load606.prefixes_missing([]) == sorted(set(K.values()) | set(load606.RUN9_KEYS.values()))


# ─── 3. Q2–Q4 gates: the bars are the rulings, and both sides of each bar ──


def test_bars_are_the_rulings_not_instrument_readings():
    assert (load606.STALE_TO_PUSH_P95_BAR_MS, load606.STALE_TO_PUSH_MAX_BAR_MS) == (3000, 10000)
    assert load606.STALE_PUSH_UNARMED_BAR == 0 and load606.STALE_OVERDUE_BAR == 0
    assert load606.BYPASS_QUEUED_BAR == 0 and load606.BYPASS_TIMEOUT_BAR == 0


def _checks(**over):
    rep, checks = load606.run8_report(_navs(), [_mem(150, **q234(**over))], [], PODS, **DELIV)
    return rep, {c["check"]: c for c in checks}


def H2b(le3000: int, total: int, max_ms: int):
    """1.12.50-shaped stale_to_push histogram (bounds include 3000)."""
    b = {"le_100": 0, "le_250": 0, "le_500": 0, "le_1000": 0, "le_2000": 0,
         "le_3000": le3000, "le_5000": total, "le_10000": total, "le_30000": total,
         "le_inf": total}
    return {"buckets": b, "count": total, "sum_ms": 0, "max_ms": max_ms}


@pytest.mark.parametrize("hist,p95,mx", [
    (H2b(95, 100, 10000), "PASS", "PASS"),          # exactly 95% <= 3 s, max exactly 10 s
    (H2b(9499, 10000, 4000), "FAIL", "PASS"),       # 94.99%
    (H2b(100, 100, 10001), "PASS", "FAIL"),         # 1 ms over the max bar
])
def test_stale_to_push_both_sides_of_the_bars(hist, p95, mx):
    _, c = _checks(**{K["stale_to_push"]: hist})
    assert c["run8.stale_to_push.p95"]["result"] == p95
    assert c["run8.stale_to_push.max"]["result"] == mx


@pytest.mark.parametrize("n,result", [(0, "PASS"), (1, "FAIL")])
def test_push_unarmed_gated_at_zero(n, result):
    _, c = _checks(**{K["stale_push_unarmed"]: n})
    assert c["run8.stale_push_unarmed_total"]["result"] == result


@pytest.mark.parametrize("overdue,parked,result", [
    (0, 0, "PASS"), (0, 500, "PASS"),               # parked debt is NOT overdue
    (1, 0, "FAIL"), (1, 500, "FAIL")])
def test_overdue_gated_at_zero_and_parked_is_not_overdue(overdue, parked, result):
    rep, c = _checks(**{K["stale_overdue"]: overdue, K["debt_parked"]: parked})
    assert c["run8.stale_overdue_total"]["result"] == result
    pv = c["run8.740.parked_vs_overdue"]
    assert pv["result"] == "PASS" and pv["value"] == {"parked_max": parked,
                                                      "overdue_total": overdue}


@pytest.mark.parametrize("key,cls,check", [
    ("admission_queued", "customer", "run8.users_never_queue.queued"),
    ("admission_queued", "debt", "run8.users_never_queue.queued"),
    ("admission_timeout", "customer", "run8.users_never_queue.timeout"),
    ("admission_timeout", "debt", "run8.users_never_queue.timeout"),
])
def test_a_queued_or_timed_out_user_fails(key, cls, check):
    m = {"background": 9, "customer": 0, "debt": 0}
    m[cls] = 1
    _, c = _checks(**{K[key]: m})
    assert c[check]["result"] == "FAIL"
    _, c = _checks(**{K[key]: {"background": 9, "customer": 0, "debt": 0}})
    assert c[check]["result"] == "PASS" and "structural" in c[check]["why"]


Q234_KEYS = sorted(set(q234()) - {K["customer_inflight"]})


@pytest.mark.parametrize("key", Q234_KEYS)
def test_every_q234_key_missing_is_not_exposed_and_fails(key):
    full = q234()
    del full[key]
    rep, checks = load606.run8_report(_navs(), [_mem(150, **full)], [], PODS, **DELIV)
    bad = [c for c in checks if c["result"] == "FAIL" and c.get("key") == key]
    assert bad and all("not exposed" in c["why"] for c in bad), (key, checks)


def test_customer_inflight_falls_back_to_the_top_level_reading():
    full = q234()
    del full[K["customer_inflight"]]
    m = _mem(150, **full)
    rep, checks = load606.run8_report(_navs(), [m], [], PODS, **DELIV)
    assert any(c["check"] == "run8.customer_inflight" and c["result"] == "FAIL" for c in checks)
    m["customer_resolve_inflight"] = 12
    rep, checks = load606.run8_report(_navs(), [m], [], PODS, **DELIV)
    assert rep["q3_users_never_queue"]["customer_inflight_max"] == 12
    assert not any(c["check"] == "run8.customer_inflight" for c in checks)


# ─── Q2–Q4 reported numbers ─────────────────────────────────────────────────


def test_q2_stale_served_share_and_age():
    rc0 = {"hit_total": 10_000, "stale_served_total": 40, "stale_served_age_ms_max": 900,
           "known_stale_served_total": 3, "known_stale_served_declined_total": 3}
    rc1 = {"hit_total": 20_000, "stale_served_total": 540, "stale_served_age_ms_max": 300,
           "known_stale_served_total": 10, "known_stale_served_declined_total": 7}
    mem = [_mem(100, **q234(**{K["resolved_cache"]: rc0, K["stale_serve"]: {"known_stale": 1}})),
           _mem(200, **q234(**{K["resolved_cache"]: rc1, K["stale_serve"]: {"known_stale": 31},
                              K["stale_withdrawn"]: {"unarmable": 9}}))]
    rep, checks = load606.run8_report(_navs(), mem, [], PODS, **DELIV)
    q2 = rep["q2_stale"]
    assert q2["resolved_cache"]["stale_served_share_of_hits"] == 0.05      # 500 / 10000
    assert q2["resolved_cache"]["stale_served_age_ms_max"] == 900           # max over samples
    assert q2["resolved_cache"]["known_stale_served_by_reason_delta"] == {
        "known_stale_served_declined_total": 4}
    assert q2["apistage_stale_serve_total"]["delta"] == 30
    assert q2["apistage_stale_withdrawn_total"]["delta"] == 7
    assert q2["stale_serve_age_ms"]["max_ms"] == 2100


def test_q3_bypass_vs_throttled():
    wait = {"background": H({10: 6, 1000: 3, 5000: 1}, 4000),
            "customer": H({10: 50}, 0), "debt": H({10: 7}, 0)}
    rep, c = _checks(**{K["admission_wait"]: wait})
    b = rep["q3_users_never_queue"]["bypass_vs_throttled"]
    assert (b["customer_bypass_admissions"], b["debt_bypass_admissions"]) == (50, 7)
    assert b["background_admissions"] == 10 and b["background_waited_over_10ms"] == 4
    assert b["background_timeouts"] == 1
    assert b["per_class_wait"]["background"]["max_ms"] == 4000


def test_q4_retry_deltas_and_the_viewer_gate_lead():
    def rt(g):
        return {"transient": {"background": 0, "demand": 0, "debt": 0},
                "genuine": {"background": 0, "demand": g, "debt": 0}}
    mem = [_mem(100, **q234(**{K["retry_total"]: rt(1), K["retry_viewers"]: 1})),
           _mem(105, **q234(**{K["retry_total"]: rt(2), K["retry_viewers"]: 1})),   # viewed
           _mem(110, **q234(**{K["retry_total"]: rt(2), K["retry_viewers"]: 0})),
           _mem(115, **q234(**{K["retry_total"]: rt(4), K["retry_viewers"]: 0,      # nobody
                              K["retry_abandoned"]: {"no_viewer": 5, "evicted": 1}}))]
    rep, checks = load606.run8_report(_navs(), mem, [], PODS, **DELIV)
    q4 = rep["q4_retry_740"]
    assert q4["retry_total_delta"]["genuine"]["demand"] == 3
    assert q4["retry_abandoned_delta"] == {"no_viewer": 3, "evicted": 1}
    vg = q4["viewer_gate"]
    # stale_retry_total counts SCHEDULED retries (stale_retry.go:227-231); a
    # wake with no viewer is abandoned. Reported as scheduled vs abandoned,
    # never as "retries run without a viewer" (run 8's misreading).
    assert vg["genuine_retries_scheduled"] == 3 and vg["abandoned_no_viewer"] == 3
    assert vg["abandoned_evicted"] == 1 and vg["viewers_max"] == 1
    assert "SCHEDULED" in vg["note"] and "suspect" not in json.dumps(vg)
    assert not [c for c in checks if c["check"].startswith("run8.740") and c["result"] == "FAIL"]


def test_a_restart_turns_the_new_q234_checks_into_restarted_fails():
    pods = [dict(PODS[0]), dict(PODS[1], restarts=1,
                                last_state={"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                                           "finishedAt": "2026-10-19T08:00:00Z"}})]
    rep, checks = load606.run8_report(_navs(), [_mem(150, **q234())], [], pods, **DELIV)
    for name in ("run8.stale_push_unarmed_total", "run8.users_never_queue.queued",
                 "run8.740.parked_vs_overdue"):
        c = next(x for x in checks if x["check"] == name)
        assert c["result"] == "FAIL" and c["why"].startswith("RESTARTED")


# ─── reviewer-small on 2cd1827f: Terminating leftovers, missing max_ms ──────


def _old_and_new():
    old = _pod("snowplow-old", f"ghcr.io/x/snowplow@{OTHER_DIGEST}", terminating=True)
    new = _pod("snowplow-new", f"ghcr.io/x/snowplow@{D}")
    for p in (old, new):
        p["status"]["containerStatuses"][0]["restartCount"] = 0
    return old, new


def test_pod_record_skips_a_terminating_old_pod_that_sorts_first():
    """items[0] is the old ReplicaSet's Terminating pod (old digest): the
    before-record must be the live pod, so the run is NOT INVALID."""
    old, new = _old_and_new()
    gate = load606.image_digest_gate({"items": [old, new]}, D)
    before = load606.pod_record({"items": [old, new]}, "before")
    after = load606.pod_record({"items": [new]}, "after")
    assert before["pod"] == "snowplow-new" and before["digest"] == D
    r = load606.evaluate([], [], [], snowplow_pods=[before, after],
                         expected_digest=D, image_digests=[gate])
    v = {c["check"]: c for c in r["validity"]}
    assert v["snowplow.image_digest"]["result"] == "PASS"
    assert v["snowplow.restarts_during_measure"]["result"] == "PASS"


def test_pod_record_with_only_terminating_pods_is_unknown_not_a_pass():
    old, new = _old_and_new()
    rec = load606.pod_record({"items": [old]}, "before")
    assert rec["pod"] is None and rec["terminating_only"]
    after = load606.pod_record({"items": [new]}, "after")
    assert load606.restart_check([rec, after])["result"] == "FAIL"


def test_gate_with_require_settled_is_unsettled_while_a_leftover_remains():
    old, new = _old_and_new()
    rec = load606.image_digest_gate({"items": [old, new]}, D, require_settled=True)
    assert rec["result"] == "UNSETTLED" and rec["terminating"] == ["snowplow-old"]
    assert load606.image_digest_gate({"items": [new]}, D, require_settled=True)["result"] == "MATCH"


def test_run606_waits_for_the_terminating_leftover_then_is_not_invalid(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1",
               FAKE_TERMINATING_READS="2", DIGEST_POLL_S="0.1", DIGEST_SETTLE_S="30")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    assert "waiting for Terminating snowplow pod(s) to go (snowplow-old)" in p.stderr
    assert f"image digest MATCH: every live snowplow pod runs {FAKE_DIGEST}" in p.stderr
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    val = {c["check"]: c for c in v["validity"]}
    assert val["snowplow.image_digest"]["result"] == "PASS"
    assert val["snowplow.restarts_during_measure"]["result"] == "PASS"
    assert v["report"]["image_digest"]["gate"][0]["terminating"] == []


def test_run606_leftover_that_never_clears_fails_clearly_before_load(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1",
               FAKE_TERMINATING_READS="-1", DIGEST_POLL_S="0.1", DIGEST_SETTLE_S="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode != 0
    assert "a snowplow pod is still Terminating after 1s" in p.stderr
    assert "FAILED: rollout leftovers did not clear; no load is applied" in p.stderr
    assert "=== step measure ===" not in p.stderr
    rec = json.loads((tmp / "out" / "image-digest.jsonl").read_text())
    assert rec["result"] == "UNSETTLED" and rec["terminating"] == ["snowplow-old"]
    gcl = [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]
    assert gcl[-3:] == TEARDOWN_TAIL


def test_wrong_digest_with_a_terminating_leftover_is_void_not_unsettled():
    """reviewer-small F5: the mismatch is decided before the settle wait."""
    old, _ = _old_and_new()
    wrong = _pod("snowplow-new", f"ghcr.io/x/snowplow@{OTHER_DIGEST}")
    rec = load606.image_digest_gate({"items": [old, wrong]}, D, require_settled=True)
    assert rec["result"] == "VOID" and "digest mismatch" in rec["why"]
    body = json.dumps({"items": [old, wrong]})
    p = subprocess.run(["python3", "-m", "bench.load606", "digest-gate", "--expected", D,
                        "--require-settled"], input=body, cwd=BENCH_DIR,
                       capture_output=True, text=True)
    assert p.returncode == 3                       # VOID, not 4 (UNSETTLED)


@pytest.mark.parametrize("before_live,after_live", [(False, False), (False, True), (True, False)])
def test_restart_check_fails_when_any_record_has_no_live_pod(before_live, after_live):
    """#758: neither record live used to PASS with value 0."""
    old, new = _old_and_new()
    b = load606.pod_record({"items": [old, new] if before_live else [old]}, "before")
    a = load606.pod_record({"items": [new] if after_live else [old]}, "after")
    c = load606.restart_check([b, a])
    assert c["result"] == "FAIL" and "no live snowplow pod" in c["why"]


@pytest.mark.parametrize("max_ms", ["absent", None])
def test_stale_to_push_with_pushes_but_no_max_fails_the_max_bar(max_ms):
    h = H2b(100, 100, 0)
    if max_ms == "absent":
        del h["max_ms"]
    else:
        h["max_ms"] = max_ms
    _, c = _checks(**{K["stale_to_push"]: h})
    assert c["run8.stale_to_push.max"]["result"] == "FAIL"
