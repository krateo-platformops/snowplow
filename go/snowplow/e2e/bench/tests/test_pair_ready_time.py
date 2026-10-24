"""The paired customer-latency verdict when snowplow restarts (team lead
2026-10-24, after run 15 came back INVALID: both arms OOMKilled twice).

  1. a restart (or OOM) of B, the candidate, FAILs the pair outright;
  2. a restart of A, the baseline, is recorded and reported, never voids;
  3. every cell is computed over the time BOTH arms' snowplow was Ready —
     each run's not-Ready stretches (mem606's readyz samples, the
     prewarm-complete recovery tail, counter resets, the pod records) are
     removed from both runs, mapped by phase-relative time;
  4. a cell the shortened overlap cannot decide is INCONCLUSIVE;
  5. every cell says how many seconds each run lost and why;
  6. every other reason a run is INVALID still voids the pair; k6's error
     rate counts only the jointly-Ready time and only B's voids.

Real data: tests/fixtures/customer_latency_run15_pair_every100.json.gz is
RECORDED from run 15's collected artifacts (bench 983b17d8; A 1.12.54, B
1.12.55): every 100th k6 http_req_duration and http_req_failed point, the
stream's frames, the k6 config, mem606's readiness and anchor fields for
EVERY sample, fresh606's churn records, kubectl top and the snowplow pod
records (pod names replaced), and each run's own FAILed validity entries.
The replay writes both runs out in run606's $OUT layout, runs `load606
evaluate` and `custlat pair` as CLIs (as run606.sh / run606_pair.sh do):
the evaluate path is where churn phases were lost once.

Synthetic arms: run 14's recorded paired-schedule run (test_run15's P14,
every sample Ready) with not-Ready stretches written into its mem606
samples and pod records.
"""

from __future__ import annotations

import copy
import gzip
import inspect
import io
import json
import math
import random
import shutil
import subprocess
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from statistics import NormalDist

import pytest

from bench import custlat, load606
from tests.test_run15_customer_latency import (BENCH, FIX, NA, NB, P14, REPLICA, _iso, _launch,  # noqa: F401
                                               _write_run, pairbed)

R15 = json.load(gzip.open(FIX / "customer_latency_run15_pair_every100.json.gz", "rt"))
DIG_A, DIG_B = "sha256:" + "a" * 64, "sha256:" + "b" * 64


# ─── run 15, replayed through both CLIs ───────────────────────────────────

def _write15(d: Path, arm: dict) -> list[str]:
    """One recorded run 15 arm in run606's $OUT layout; s_evaluate's inputs."""
    (d / "k6").mkdir(parents=True)
    (d / "browser").mkdir()
    t0 = arm["t0"]
    with gzip.open(d / "k6" / "k6.json.gz", "wt") as fh:
        def frame(name, t):
            fh.write(json.dumps({"metric": "bench_frame", "type": "Point",
                                 "data": {"time": _iso(t), "value": 1, "tags": {"frame": name}}}) + "\n")
        if arm["frames"].get("start"):
            frame("start", t0)
        for off, v in arm["calls"]:
            fh.write(json.dumps({"metric": "http_req_duration", "type": "Point",
                                 "data": {"time": _iso(t0 + off), "value": v, "tags": {"scenario": "navigate"}}})
                     + "\n")
        for off, v in arm["failed"]:
            fh.write(json.dumps({"metric": "http_req_failed", "type": "Point",
                                 "data": {"time": _iso(t0 + off), "value": v, "tags": {"scenario": "navigate"}}})
                     + "\n")
        if arm["frames"].get("end"):
            frame("end", t0 + max(off for off, _ in arm["calls"]))
    (d / "k6" / "k6-summary.json").write_text(json.dumps({"config": arm["config"]}))
    (d / "browser" / "browser-summary.json").write_text(json.dumps({"sessions": arm["browser_sessions"]}))
    files = {"fresh-x.jsonl": arm["churns"], "mem-x.jsonl": arm["mem"], "top.jsonl": arm["tops"],
             "pod-before.jsonl": [p for p in arm["pods"] if p["phase"] == "before"],
             "pod-after.jsonl": [p for p in arm["pods"] if p["phase"] == "after"]}
    for name, rows in files.items():
        (d / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
    return [str(d / n) for n in ("fresh-x.jsonl", "pod-after.jsonl", "pod-before.jsonl", "mem-x.jsonl",
                                 "top.jsonl")]


def _evaluate15(d: Path, arm: dict) -> dict:
    """`load606 evaluate` (a subprocess, as run606.sh runs it) on one arm. The
    run's OWN validity is the real run's, recorded: the fixture holds a sample
    of k6's stream and none of the browser / image-digest / job records."""
    files = _write15(d, arm)
    p = subprocess.run(["python3", "-m", "bench.load606", "evaluate", *files, "--expect-digest", arm["digest"],
                        "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
                        "--browser-sessions", "5", "--no-viewers"],
                       cwd=BENCH, capture_output=True, text=True, check=False)
    v = json.loads(p.stdout)
    v["verdict"] = arm["verdict"]
    v["validity"] = arm["validity_failed"]
    v["invalid_reasons"] = [f"{c['check']}: {c.get('why') or c.get('value')}" for c in arm["validity_failed"]]
    (d / "verdict.json").write_text(json.dumps(v))
    return v


def _pair_cli(da: Path, db: Path, dig=(None, None)) -> tuple[int, dict]:
    """`custlat pair` as run606_pair.sh runs it (a subprocess)."""
    p = subprocess.run(["python3", "-m", "bench.custlat", "pair", "--a", str(da / "verdict.json"),
                        "--b", str(db / "verdict.json"), "--a-dir", str(da), "--b-dir", str(db),
                        "--replica", str(BENCH / "customer_latency_replica_runs12_13.json"),
                        "--expect-a", dig[0] or R15["a"]["digest"], "--expect-b", dig[1] or R15["b"]["digest"],
                        "--completed-a", "1", "--completed-b", "1"],
                       cwd=BENCH, capture_output=True, text=True, check=False)
    return p.returncode, json.loads(p.stdout)


def _no_restarts(arm: dict) -> dict:
    """The arm as if snowplow had never restarted: every sample Ready, its
    counters running on, its pod records without restarts, its own restart
    check gone, and no k6 call or request inside its outages (they had
    none)."""
    a = copy.deepcopy(arm)
    down = [(e["t0"], e["t1"]) for e in custlat.readiness(a["mem"], a["pods"], None)["excluded"]]
    assert len(down) == 2

    def kept(off):
        return not any(x < a["t0"] + off < y for x, y in down)
    a["calls"] = [c for c in a["calls"] if kept(c[0])]
    a["failed"] = [f for f in a["failed"] if kept(f[0])]
    off, last = 0, None
    for r in (r for r in a["mem"] if r["kind"] == "mem"):
        r["readyz"], r["vars_status"] = 200, 200
        if not (r.get("prewarm_complete") or {}).get("done"):
            r["prewarm_complete"] = {"done": 1, "elapsed_ms": 380000}
        g = r.get("num_gc")
        if g is None:
            r["num_gc"] = last
            continue
        g += off
        if last is not None and g < last:
            off += last - g
            g = last
        r["num_gc"] = last = g
    for p in a["pods"]:
        p["restarts"], p["last_state"] = 0, {}
    a["validity_failed"] = [c for c in a["validity_failed"] if c["check"] != custlat.RESTART_CHECK]
    a["verdict"] = "INVALID" if a["validity_failed"] else "FAIL"
    return a


@pytest.fixture(scope="module")
def run15(tmp_path_factory):
    d = tmp_path_factory.mktemp("r15")
    va = _evaluate15(d / "a", R15["a"])
    vb = _evaluate15(d / "b", R15["b"])
    rc, p = _pair_cli(d / "a", d / "b")
    return d, va, vb, rc, p


def _k6t(v):
    """k6's first call in the REAL run (the fixture keeps every 100th)."""
    return R15["a" if v["report"]["image_digest"]["expected"] == R15["a"]["digest"] else "b"]["t0"]


def test_run15_replayed_is_fail_because_b_restarted(run15):
    _, va, vb, rc, p = run15
    assert rc == 1 and p["verdict"] == "FAIL"
    c = p["candidate_restarts"]
    assert c["result"] == "FAIL" and c["value"] == 2
    assert "OOMKilled (exit 137) at 2026-10-24T11:23:38Z" in c["why"]
    assert c in p["checks"] and p["fail_reasons"][0].startswith("customer_latency.pair.candidate_restarts")
    # never hidden, never INVALID: A's own broken k6 / viewer streams are still reported
    assert {r.split(":")[0] for r in p["invalid_reasons"]} == {"customer_latency.pair.runs_valid"}
    rv = next(x for x in p["validity"] if x["check"] == "customer_latency.pair.runs_valid")["value"]
    assert [r.split(":")[0] for r in rv["a"]] == ["k6.stream_complete", "viewer.stream_complete"]
    assert rv["b"] == []                       # B: its restart and error rate are the pair's to judge
    # A's restarts: recorded, placed, reported
    ra = p["restarts"]["a"]
    assert ra["count"] == 2 and ra["in_samples"] == 2 and ra["pod"] == {"record": "same pod", "restarts": 2}
    assert ra["last_termination"]["located"] and not ra["unlocated"]
    assert any(n.startswith("A (the baseline) restarted 2x") for n in p["notes"])
    # B's not-Ready time is all restart recovery: no separate not-Ready FAIL
    assert p["candidate_not_ready"]["result"] == "PASS"


def test_run15_a_healthy_sensitivity_and_the_note(run15):
    """A restarted inside churn:list_dep@10 and churn:list_dep@50: churn's
    A-healthy line drops both steps whole from both runs; B passes churn
    req_s there, so a NOTE says A restarted inside that cell."""
    _, va, vb, _, p = run15
    by = {(c["phase"], c["metric"]): c for c in p["cells"]}
    h = by[("churn", "req_s")]["a_healthy"]
    assert h["phases_dropped"] == ["churn:list_dep@10", "churn:list_dep@50"] and not h["same_as_cell"]
    ph = va["report"]["customer_latency"]["phases"]
    full = va["report"]["customer_latency"]["gate_phases"]["churn"]["seconds"]
    # the dropped steps whole, plus B's restart #2 (in a kept step), 675.2 s
    want = full - ph["churn:list_dep@10"]["seconds"] - ph["churn:list_dep@50"]["seconds"] - 675.2
    assert abs(h["a_kept_s"] - want) < 1.0, (h["a_kept_s"], want)
    assert by[("churn", "req_s")]["result"] == "PASS"
    assert any(n.startswith("B passes churn req_s") and "list_dep@10, churn:list_dep@50" in n for n in p["notes"])
    # failed cells get no such note; cells A never restarted in say so
    assert not any(n.startswith("B passes churn p95_ms") for n in p["notes"])
    assert by[("storm", "p95_ms")]["a_healthy"]["same_as_cell"]
    assert by[("churn:list_dep@10", "p95_ms")]["a_healthy"]["ratio"] is None      # nothing left of that step
    assert len(p["a_healthy_lines"]) == len(p["cells"])


def test_run15_not_ready_stretches_from_the_runs_own_signals(run15):
    """Each run's two restarts: from the last Ready sample before to the first
    Ready sample with its cold start complete (prewarm's own elapsed time),
    the real samples' times."""
    _, va, vb, _, p = run15
    for v, want in ((va, [(2738.4, 3413.6, 654.2), (5720.2, 6470.3, 724.3)]),
                    (vb, [(3165.9, 3841.1, 660.5), (8374.8, 9050.1, 662.2)])):
        rd = v["report"]["customer_latency"]["readiness"]
        ex = [e for e in rd["excluded"] if e["kind"] == "restart"]
        got = [(round(e["t0"] - _k6t(v), 1), round(e["t1"] - _k6t(v), 1), e["cold_start_s"]) for e in ex]
        assert got == want
        assert [e["kind"] for e in rd["excluded"]] == ["restart", "restart"]     # nothing else excluded
        mem = [r for r in R15["a" if v is va else "b"]["mem"] if r["kind"] == "mem"]
        for e in ex:
            # the stretch's end is a Ready sample with prewarm complete, its start the last Ready one
            end = next(r for r in mem if r["t"] == e["t1"])
            assert end["readyz"] == 200 and end["prewarm_complete"]["done"] == 1
            assert next(r for r in mem if r["t"] == e["t0"])["readyz"] == 200
            assert all(r["readyz"] != 200 for r in mem if e["t0"] < r["t"] < e["t1"])


def _phase_offset(v, t):
    """(phase, offset from that phase's start) of epoch t in run v."""
    wins = custlat._wins(v["report"]["customer_latency"])
    p = custlat._phase(t, wins)
    return p, round(t - next(w0 for n, w0, _ in wins if n == p), 1)


def test_run15_both_runs_lose_the_same_windows_by_phase_relative_time(run15):
    _, va, vb, _, p = run15
    items = {it["what"]: it for it in p["exclusion"]["items"]}
    assert sorted(items) == ["A restart #1", "A restart #2", "B restart #1", "B restart #2"]
    for it in items.values():
        (a0, a1), = it["a"]
        (b0, b1), = it["b"]
        assert round(a1 - a0, 1) == round(b1 - b0, 1)               # the same length in both
        assert _phase_offset(va, a0) == _phase_offset(vb, b0)       # at the same place in its phase
    # the arms' churn steps last differently: B's restart #2 sits 332.6 s into
    # list_dep_status@10 in both runs, though 1,260 s apart from k6's start
    (a0, _), = items["B restart #2"]["a"]
    (b0, _), = items["B restart #2"]["b"]
    ph, off = _phase_offset(va, a0)
    assert ph == "churn:list_dep_status@10" and abs(off - 332.6) < 0.1
    assert abs((a0 - _k6t(va)) - (b0 - _k6t(vb)) - 1260.6) < 0.1


def test_run15_every_cell_says_what_it_lost_and_why(run15):
    _, va, vb, _, p = run15
    assert p["cells"] and all("excluded" in c and " | excluded" in c["line"] for c in p["cells"])
    churn = {c["metric"]: c for c in p["cells"] if c["phase"] == "churn"}
    e = churn["p95_ms"]["excluded"]
    assert e["a_s"] == e["b_s"] == 2775.7
    assert {w["what"]: (w["a_s"], w["b_s"]) for w in e["why"]} == {
        "A restart #1": (675.2, 675.2), "A restart #2": (750.1, 750.1),
        "B restart #1": (675.2, 675.2), "B restart #2": (675.2, 675.2)}
    assert all("restarted" in w["why"] for w in e["why"])
    # the cell is computed on what is left: A's churn seconds less the excluded
    full = va["report"]["customer_latency"]["gate_phases"]["churn"]["seconds"]
    assert churn["p95_ms"]["full_s"] == full and abs(churn["p95_ms"]["kept_s"] - (full - 2775.7)) < 0.5
    for p_ in ("storm", "steady", "burst"):
        x = next(c for c in p["cells"] if c["phase"] == p_)
        assert x["excluded"] == {"a_s": 0.0, "b_s": 0.0, "why": []} and "excluded: none" in x["line"]
    steps = {c["phase"]: c["excluded"] for c in p["cells"] if c["phase"].startswith("churn:")}
    assert steps["churn:list_dep@1"]["a_s"] == 0.0
    assert steps["churn:list_dep@10"]["a_s"] == 1350.3 and steps["churn:list_dep_status@10"]["b_s"] == 675.2


def test_run15_error_rate_is_counted_in_the_jointly_ready_time(run15):
    """Run 15's ~1.05 % whole-run error rates came from the outages: inside
    the jointly-Ready time B's is far under 1 %. A's whole-run rate is
    reported beside it."""
    _, va, vb, _, p = run15
    er = p["k6_error_rate"]
    assert er["a"]["whole_run_summary"] == 0.01060688424440024           # A's k6 summary, reported
    assert er["b"]["whole_run_stream"]["rate"] > 0.01 and er["a"]["whole_run_stream"]["rate"] > 0.01
    assert er["b"]["jointly_ready"]["rate"] < 0.01 and er["a"]["jointly_ready"]["rate"] < 0.01
    assert er["b"]["jointly_ready"]["requests"] < er["b"]["whole_run_stream"]["requests"]
    chk = next(x for x in p["validity"] if x["check"] == "customer_latency.pair.k6_error_rate[b]")
    assert chk["result"] == "PASS"
    assert any(n.startswith("A's k6 error rate: whole run 0.0106") for n in p["notes"])


def test_run15_without_b_restarts_computes_over_the_jointly_ready_windows(tmp_path):
    """B as if it had never restarted: no FAIL; the cells lose A's two
    stretches in both runs; A's own broken k6 / viewer streams still void."""
    va = _evaluate15(tmp_path / "a", R15["a"])
    vb = _evaluate15(tmp_path / "b", _no_restarts(R15["b"]))
    rc, p = _pair_cli(tmp_path / "a", tmp_path / "b")
    assert p["candidate_restarts"]["result"] == "PASS"
    assert rc == 3 and p["verdict"] == "INVALID"
    assert [r.split(":")[0] for r in p["invalid_reasons"]] == ["customer_latency.pair.runs_valid"]
    assert sorted(it["what"] for it in p["exclusion"]["items"]) == ["A restart #1", "A restart #2"]
    churn = next(c for c in p["cells"] if c["phase"] == "churn" and c["metric"] == "p95_ms")
    assert churn["excluded"]["a_s"] == churn["excluded"]["b_s"] == 1425.3
    # and once A's streams are whole too, the cells decide the pair
    a = copy.deepcopy(R15["a"])
    a["validity_failed"] = [c for c in a["validity_failed"] if c["check"] in (custlat.RESTART_CHECK,
                                                                            custlat.ERROR_RATE_CHECK)]
    va = _evaluate15(tmp_path / "a2", a)
    rc, p = _pair_cli(tmp_path / "a2", tmp_path / "b")
    assert p["verdict"] in ("PASS", "FAIL", "INCONCLUSIVE") and not p["invalid_reasons"], p["invalid_reasons"]
    assert rc == {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 4}[p["verdict"]]
    assert len(p["checks"]) == 9 and all(c["excluded"]["a_s"] == c["excluded"]["b_s"] for c in p["checks"])


# ─── synthetic arms: run 14's recorded run with not-Ready stretches ───────

K0 = P14["t0"] + P14["calls"][0][0]           # k6's first call


def _down(doc, t0, t1, *, restart=True, prewarm_tail=0, code=0):
    """doc with snowplow down over [K0 + t0, K0 + t1): readyz `code` inside
    (0 = no reply); after a restart its counters start again, and
    `prewarm_tail` more samples answer readyz 200 with the cold start not
    complete. Pod records count the restarts."""
    d = copy.deepcopy(doc)
    rows = [r for r in d["mem"] if r["kind"] == "mem"]
    a, b = K0 + t0, K0 + t1
    for r in rows:
        if a < r["t"] < b:
            r.update(readyz=code, vars_status=0, prewarm_complete=None)
            r.pop("num_gc", None)
    after = [r for r in rows if r["t"] >= b]
    if restart:
        base = after[0]["num_gc"] - 1
        for r in after:
            r["num_gc"] -= base
        for r in after[:prewarm_tail]:
            r["prewarm_complete"] = {"done": 0, "elapsed_ms": 0}
        after[prewarm_tail]["prewarm_complete"] = {"done": 1, "elapsed_ms": (after[prewarm_tail]["t"] - a) * 1000}
        n = (d.get("_restarts") or 0) + 1
        d["_restarts"] = n
        d["pods"] = [{"kind": "snowplow_pod", "phase": "before", "pod": "snowplow-x-0", "restarts": 0,
                      "last_state": {}},
                     {"kind": "snowplow_pod", "phase": "after", "pod": "snowplow-x-0", "restarts": n,
                      "last_state": {"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                                    "finishedAt": _rfc(a + 5)}}}]
    return d


def _rfc(t):
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _shift(doc, dt):
    """The same run started dt seconds later."""
    d = copy.deepcopy(doc)
    d["t0"] += dt
    for r in d["mem"] + d["tops"]:
        r["t"] += dt
    for c in d["churns"]:
        c["t0"] += dt
        c["t1"] += dt
    return d


def _verdict(d: Path, doc, digest, *, scale=lambda t, v: v, restarts_check=True):
    """evaluate (the CLI's main) on doc written out; the run's own validity is
    what doc says: its restart check from the pod records, nothing else FAILs."""
    files = _write_run(d, doc, scale=scale)
    buf = io.StringIO()
    with redirect_stdout(buf):
        load606.main(["evaluate", *files, "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
                      "--browser-sessions", "5", "--no-viewers", "--expect-digest", digest])
    v = json.loads(buf.getvalue())
    n = doc.get("_restarts") or 0
    v["validity"] = ([{"check": custlat.RESTART_CHECK, "result": "FAIL", "value": n,
                       "why": f"snowplow restarted {n}x during measure"}] if n and restarts_check else [])
    v["verdict"] = "INVALID" if v["validity"] else "FAIL"
    (d / "verdict.json").write_text(json.dumps(v))
    return v


def _pair(da: Path, db: Path, *extra) -> dict:
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = custlat.main(["pair", "--a", str(da / "verdict.json"), "--b", str(db / "verdict.json"),
                           "--replica", str(BENCH / "customer_latency_replica_runs12_13.json"), *extra])
    p = json.loads(buf.getvalue())
    assert rc == {"PASS": 0, "FAIL": 1, "INVALID": 3, "INCONCLUSIVE": 4}[p["verdict"]]
    return p


def _arms(tmp, a_doc, b_doc, **kw):
    _verdict(tmp / "a", a_doc, DIG_A)
    _verdict(tmp / "b", b_doc, DIG_B, **kw)
    return _pair(tmp / "a", tmp / "b")


SHIFT = 300.0                                    # B starts 5 minutes after A


def test_synthetic_a_only_restart_is_recorded_and_excluded_from_both(tmp_path):
    p = _arms(tmp_path, _down(P14, 900, 1100), _shift(P14, SHIFT))
    assert p["verdict"] == "PASS", (p["invalid_reasons"], p["fail_reasons"])
    assert p["candidate_restarts"]["result"] == "PASS"
    assert p["restarts"]["a"]["count"] == 1 and any(n.startswith("A (the baseline) restarted 1x") for n in p["notes"])
    (it,) = p["exclusion"]["items"]
    assert it["what"] == "A restart #1"
    (a0, a1), = it["a"]
    (b0, b1), = it["b"]
    assert round(b0 - a0, 6) == SHIFT and round(b1 - a1, 6) == SHIFT     # the same windows, B's time
    churn = [c for c in p["checks"] if c["check"].startswith("customer_latency.pair[churn]")]
    assert churn and all(c["excluded"]["a_s"] == c["excluded"]["b_s"] > 0 for c in churn)


def _failing(doc, lo, hi):
    """every k6 request from lo to hi s after the run's first call failed"""
    d = copy.deepcopy(doc)
    d["failed"] = [[o, 1 if lo <= o - d["calls"][0][0] < hi else v] for o, v in d["failed"]]
    return d


def test_synthetic_b_not_ready_without_restarting_fails(tmp_path):
    """B's snowplow itself answers readyz 503 for ~200 s without restarting
    (no counter reset, no pod restart): its users saw it, so the pair FAILs,
    with the stretch, its seconds and its signal; A's identical stretch would
    only be excluded."""
    p = _arms(tmp_path / "b", P14, _shift(_down(P14, 900, 1100, restart=False, code=503), SHIFT))
    assert p["verdict"] == "FAIL" and p["candidate_restarts"]["result"] == "PASS"
    c = p["candidate_not_ready"]
    assert c["result"] == "FAIL" and c in p["checks"]
    (st,) = c["value"]["stretches"]
    assert c["value"]["seconds"] == st["seconds"] >= 200 and "readyz 503" in st["why"]
    assert "not Ready without restarting" in c["why"] and "readyz 503" in c["why"]
    assert {x["result"] for x in p["checks"] if ".pair[" in x["check"]} == {"PASS"}
    p = _arms(tmp_path / "a", _down(P14, 900, 1100, restart=False, code=503), _shift(P14, SHIFT))
    assert p["verdict"] == "PASS" and p["candidate_not_ready"]["result"] == "PASS"
    assert [it["what"] for it in p["exclusion"]["items"]] == ["A not_ready #1"]


def test_synthetic_b_readyz_no_reply_k6_is_the_witness(tmp_path):
    """B's sampler gets no reply to readyz (readyz 0) for ~200 s, no restart
    signal. k6 decides: errors over its 1 % bar inside the stretch -> B was
    not serving: FAIL; k6 served -> the stretch is jointly-Ready time (not
    cut, its calls judged), with a NOTE. A's stretch stays excluded."""
    down = _shift(_down(P14, 900, 1100, restart=False), SHIFT)
    p = _arms(tmp_path / "fail", P14, _failing(down, 950, 1050))
    c = p["candidate_not_ready"]
    assert p["verdict"] == "FAIL" and c["result"] == "FAIL"
    (st,) = c["value"]["stretches"]
    assert st["k6"]["rate"] > 0.01 and "k6 saw" in st["why"] and "over the 0.01 bar" in st["why"]
    p = _arms(tmp_path / "ok", P14, down)
    assert p["verdict"] == "PASS" and p["candidate_not_ready"]["result"] == "PASS"
    assert p["exclusion"]["items"] == []                     # not cut from either run
    (w,) = p["b_witnessed_ready"]
    assert w["k6"]["requests"] > 0 and w["k6"]["rate"] <= 0.01
    assert any(n.startswith("B's readiness sampler:") and "kept as jointly-Ready time" in n for n in p["notes"])
    p = _arms(tmp_path / "a", _down(P14, 900, 1100, restart=False), _shift(P14, SHIFT))
    assert p["verdict"] == "PASS" and [it["what"] for it in p["exclusion"]["items"]] == ["A not_ready #1"]


def test_synthetic_a_healthy_drops_the_phases_a_restarted_in(tmp_path):
    p = _arms(tmp_path, _down(P14, 900, 1100), _shift(P14, SHIFT))
    v = json.loads((tmp_path / "a" / "verdict.json").read_text())["report"]["customer_latency"]
    by = {(c["phase"], c["metric"]): c for c in p["cells"]}
    h = by[("churn", "p95_ms")]["a_healthy"]
    dropped = h["phases_dropped"]
    assert dropped and all(n.startswith("churn:") for n in dropped)
    want = v["gate_phases"]["churn"]["seconds"] - sum(v["phases"][n]["seconds"] for n in dropped)
    assert abs(h["a_kept_s"] - want) < 1.0 and abs(h["b_kept_s"] - want) < 1.0
    assert abs(h["ratio"] - 1.0) < 0.02                         # B is A, shifted
    assert by[("burst", "p95_ms")]["a_healthy"]["phases_dropped"] == ["burst"]
    assert by[("storm", "p95_ms")]["a_healthy"]["same_as_cell"]
    for m in custlat.METRICS:
        assert by[("churn", m)]["result"] == "PASS"
        assert any(n.startswith(f"B passes churn {m}") for n in p["notes"]), p["notes"]
    assert not any(n.startswith("B passes storm") for n in p["notes"])


def test_synthetic_b_only_restart_fails_whatever_the_cells_say(tmp_path):
    p = _arms(tmp_path, P14, _shift(_down(P14, 900, 1100), SHIFT))
    assert p["verdict"] == "FAIL" and p["candidate_restarts"]["result"] == "FAIL"
    assert {c["result"] for c in p["checks"] if c["check"] != "customer_latency.pair.candidate_restarts"} == {"PASS"}
    assert [it["what"] for it in p["exclusion"]["items"]] == ["B restart #1"]


def test_synthetic_b_restart_fails_even_when_the_pair_is_otherwise_invalid(tmp_path):
    _verdict(tmp_path / "a", P14, DIG_A)
    v = _verdict(tmp_path / "b", _shift(_down(P14, 900, 1100), SHIFT), DIG_B)
    v["validity"].append({"check": "k6.stream_complete", "result": "FAIL", "why": "no end frame"})
    (tmp_path / "b" / "verdict.json").write_text(json.dumps(v))
    p = _pair(tmp_path / "a", tmp_path / "b", "--expect-b", "sha256:" + "c" * 64)
    assert p["verdict"] == "FAIL" and p["candidate_restarts"]["result"] == "FAIL"
    assert {r.split(":")[0] for r in p["invalid_reasons"]} == {"customer_latency.pair.runs_valid",
                                                               "customer_latency.pair.images"}


def test_synthetic_b_restart_seen_only_by_its_samples_still_fails(tmp_path):
    """No pod record and no restart check in B's verdict: the counter reset
    in its readiness samples is enough."""
    b = _shift(_down(P14, 900, 1100), SHIFT)
    b.pop("pods")
    p = _arms(tmp_path, P14, b, restarts_check=False)
    assert p["verdict"] == "FAIL" and p["candidate_restarts"]["value"] == 1


def test_synthetic_both_restart(tmp_path):
    p = _arms(tmp_path, _down(P14, 700, 900), _shift(_down(P14, 1200, 1350), SHIFT))
    assert p["verdict"] == "FAIL" and p["candidate_restarts"]["result"] == "FAIL"
    assert sorted(it["what"] for it in p["exclusion"]["items"]) == ["A restart #1", "B restart #1"]
    for it in p["exclusion"]["items"]:
        (a0, _), = it["a"]
        (b0, _), = it["b"]
        assert round(b0 - a0, 6) == SHIFT


def test_synthetic_recovery_tail_runs_until_prewarm_completes(tmp_path):
    """readyz answers 200 three samples before the cold start completes (an
    image whose readiness does not wait for prewarm): those samples are
    still the restart's recovery."""
    a = _down(P14, 900, 1100, prewarm_tail=3)
    v = _verdict(tmp_path / "a", a, DIG_A)
    (e,) = v["report"]["customer_latency"]["readiness"]["excluded"]
    mem = [r for r in a["mem"] if r["kind"] == "mem"]
    first_200 = next(r for r in mem if r["t"] >= K0 + 1100)
    done = next(r for r in mem if (r.get("prewarm_complete") or {}).get("done") == 1 and r["t"] >= K0 + 1100)
    assert first_200["readyz"] == 200 and done["t"] > first_200["t"]
    assert e["kind"] == "restart" and e["t1"] == done["t"]


def test_synthetic_restart_between_two_ready_samples_is_still_a_restart(tmp_path):
    """A restart no sample saw down (its counters reset between two Ready
    samples): the stretch between them is excluded."""
    a = copy.deepcopy(P14)
    rows = [r for r in a["mem"] if r["kind"] == "mem"]
    i = next(i for i, r in enumerate(rows) if r["t"] > K0 + 1000)
    for r in rows[i:]:
        r["num_gc"] -= rows[i]["num_gc"] - 1
    rd = custlat.readiness(a["mem"], None, [K0, K0 + 1800])
    (e,) = rd["excluded"]
    assert (e["t0"], e["t1"], e["kind"]) == (rows[i - 1]["t"], rows[i]["t"], "restart")


def test_synthetic_a_restart_the_samples_cannot_place_voids(tmp_path):
    a = _down(P14, 900, 1100)
    a["pods"][1]["restarts"] = 2                     # the pod restarted twice; the samples saw once
    p = _arms(tmp_path, a, _shift(P14, SHIFT))
    assert p["verdict"] == "INVALID"
    assert [r.split(":")[0] for r in p["invalid_reasons"]] == ["customer_latency.pair.restarts_placed[a]"]


def test_synthetic_overlap_too_small_is_inconclusive(tmp_path):
    """A down for most of the storm: the storm cells keep ~1/5 of A's storm.
    B slower there by a factor between the bar a full-length storm would
    hold and the kept sample's own bar: the shortened overlap is what
    decides it, so INCONCLUSIVE, never PASS. Past the kept sample's bar:
    FAIL."""
    a = _down(P14, 20, 400)
    base = _arms(tmp_path / "base", a, _shift(P14, SHIFT))
    c = next(x for x in base["cells"] if x["phase"] == "storm" and x["metric"] == "p95_ms")
    bar, fail_bar = float(c["bar"].split()[1]), float(c["fail_bar"].split()[1])
    assert c["kept_s"] < c["full_s"] / 3 and bar < fail_bar
    # B = A on a fifth of the storm: not a FAIL; a PASS only if the full-length bar allows it
    assert c["ratio"] == 1.0 and c["result"] == ("PASS" if bar >= 1.0 else "INCONCLUSIVE")
    wins = custlat._wins(json.loads((tmp_path / "base" / "b" / "verdict.json").read_text())
                         ["report"]["customer_latency"])          # B's own time: scale() gets B's epochs

    def run(k, name):
        return _arms(tmp_path / name, a, _shift(P14, SHIFT),
                     scale=lambda t, v: v * k if custlat._phase(t, wins) == "storm" else v)
    p = run(bar * 0.97, "under")                          # inside the PASS bar: still a PASS
    assert next(x for x in p["cells"] if x["phase"] == "storm" and x["metric"] == "p95_ms")["result"] == "PASS"
    p = run((bar + fail_bar) / 2, "mid")
    st = {x["metric"]: x for x in p["cells"] if x["phase"] == "storm"}
    assert st["p95_ms"]["result"] == "INCONCLUSIVE", st["p95_ms"]["line"]
    assert p["verdict"] == "INCONCLUSIVE"
    assert "the shortened jointly-Ready time decides it" in st["p95_ms"]["why"]
    p = run(fail_bar * 1.05, "past")
    assert next(x for x in p["cells"] if x["phase"] == "storm" and x["metric"] == "p95_ms")["result"] == "FAIL"
    assert p["verdict"] == "FAIL"


def test_synthetic_no_overlap_left_is_inconclusive(tmp_path):
    """A down through the whole storm: no jointly-Ready storm call at all."""
    p = _arms(tmp_path, _down(P14, 0, 600), _shift(P14, SHIFT))
    st = [x for x in p["checks"] if x["check"].startswith("customer_latency.pair[storm]")]
    assert len(st) == 3 and {x["result"] for x in st} == {"INCONCLUSIVE"}
    assert p["verdict"] == "INCONCLUSIVE"


def test_synthetic_nothing_excluded_is_the_runs_own_stats(tmp_path):
    """With every sample Ready the pair judges on each run's stored phase
    stats, and they are what recomputing over no exclusion gives."""
    p = _arms(tmp_path, P14, _shift(P14, SHIFT))
    assert p["verdict"] == "PASS" and p["exclusion"]["items"] == []
    v = json.loads((tmp_path / "a" / "verdict.json").read_text())
    r = v["report"]["customer_latency"]
    raw = custlat.raw_from_dir(str(tmp_path / "a"))
    wins = custlat._wins(r)
    assert custlat.phase_stats(raw["calls"], custlat.pooled(wins), []) == r["gate_phases"]
    assert custlat.phase_stats(raw["calls"], wins, []) == r["phases"]


def test_synthetic_b_errors_inside_a_not_ready_window_do_not_void(tmp_path):
    """B's failed requests while A was down (mapped) are outside the jointly-
    Ready time; the same failures elsewhere void the pair. A's own whole-run
    rate over 1 % never voids: it is reported."""
    def failing(doc, lo, hi):
        """every request from lo to hi s after the run's first call failed"""
        d = copy.deepcopy(doc)
        d["failed"] = [[o, 1 if lo <= o - d["calls"][0][0] < hi else v] for o, v in d["failed"]]
        return d
    a = failing(_down(P14, 900, 1100), 900, 1100)         # A: 1/9 of its run failed, all while down
    p = _arms(tmp_path / "x", a, failing(_shift(P14, SHIFT), 950, 1050))
    er = p["k6_error_rate"]
    assert er["b"]["whole_run_stream"]["rate"] > 0.01 and er["b"]["jointly_ready"]["rate"] < 0.01
    assert er["a"]["whole_run_stream"]["rate"] > 0.01 and p["verdict"] == "PASS"
    p = _arms(tmp_path / "y", a, failing(_shift(P14, SHIFT), 1300, 1400))
    assert p["verdict"] == "INVALID"
    assert [r.split(":")[0] for r in p["invalid_reasons"]] == ["customer_latency.pair.k6_error_rate[b]"]


@pytest.mark.parametrize("arm, check", [("a", "k6.stream_complete"), ("b", "k6.stream_complete"),
                                        ("a", "snowplow.image_digest"), ("b", "browser.sessions")])
def test_other_invalid_reasons_still_void(tmp_path, arm, check):
    p0 = _arms(tmp_path, _down(P14, 900, 1100), _shift(P14, SHIFT))
    assert p0["verdict"] == "PASS"
    v = json.loads((tmp_path / arm / "verdict.json").read_text())
    v["validity"].append({"check": check, "result": "FAIL", "why": "x"})
    v["verdict"] = "INVALID"
    (tmp_path / arm / "verdict.json").write_text(json.dumps(v))
    p = _pair(tmp_path / "a", tmp_path / "b")
    assert p["verdict"] == "INVALID"
    rv = next(x for x in p["validity"] if x["check"] == "customer_latency.pair.runs_valid")
    assert rv["result"] == "FAIL" and rv["value"][arm] == [f"{check}: x"]


def test_a_verdict_without_a_readiness_record_is_invalid(tmp_path):
    _arms(tmp_path, P14, _shift(P14, SHIFT))
    v = json.loads((tmp_path / "b" / "verdict.json").read_text())
    v["report"]["customer_latency"].pop("readiness")
    (tmp_path / "b" / "verdict.json").write_text(json.dumps(v))
    p = _pair(tmp_path / "a", tmp_path / "b")
    assert p["verdict"] == "INVALID" and "customer_latency.pair.readiness[b]" in {
        r.split(":")[0] for r in p["invalid_reasons"]}


def test_the_k6_stream_must_be_the_one_the_verdict_measured(tmp_path):
    _arms(tmp_path, _down(P14, 900, 1100), _shift(P14, SHIFT))
    (tmp_path / "b" / "k6" / "k6.json.gz").unlink()
    p = _pair(tmp_path / "a", tmp_path / "b")
    assert p["verdict"] == "INVALID" and "customer_latency.pair.raw_calls[b]" in {
        r.split(":")[0] for r in p["invalid_reasons"]}


def test_map_interval_is_phase_relative():
    wa = [("storm", float("-inf"), 100.0), ("churn:x@1", 50.0, 400.0), ("churn:x@10", 400.0, 600.0)]
    wb = [("storm", float("-inf"), 1100.0), ("churn:x@1", 1050.0, 1300.0), ("churn:x@10", 1300.0, 1700.0)]
    # 100 s into churn:x@1 in A is 100 s into it in B (the step starts at 50 s
    # in A, under the storm, and at 1050 s in B)
    assert custlat.map_interval(150, 250, wa, wb, 0, 1000) == [(1150.0, 1250.0)]
    # B's step is 250 s, A's 350 s: A's 250..350 s into it have no counterpart
    # in B; the next step maps from its own start
    assert custlat.map_interval(300, 450, wa, wb, 0, 1000) == [(1300.0, 1350.0)]
    assert custlat.map_interval(250, 450, wa, wb, 0, 1000) == [(1250.0, 1300.0), (1300.0, 1350.0)]
    # the storm (open start) maps from its end; steady after the last step from that step's end
    assert custlat.map_interval(80, 90, wa, wb, 0, 1000) == [(1080.0, 1090.0)]
    assert custlat.map_interval(610, 620, wa, wb, 0, 1000) == [(1710.0, 1720.0)]


# ─── review of 805ad3bb: the bars, missing signals, the crash path, the log ─

Z9 = NormalDist().inv_cdf(1 - custlat.FAMILY_ALPHA / 9)


def _blocks_calls(n_blocks, gen, t0=1000.0, per=20):
    return [(t0 + k * 10 + j * (10 / per), gen(k, j)) for k in range(n_blocks) for j in range(per)]


def _probe_cell(p, m, a_cs, b_cs, wins, excl, replica=None):
    """_cell as pair() calls it: A's and B's stats over the kept time, A's
    whole-phase seconds and sd (the sd only where _cell takes it: a test run
    against an older custlat measures that one's bars)."""
    full = custlat.phase_stats(a_cs, wins, [])
    ka = custlat.phase_stats(a_cs, wins, excl)
    kb = custlat.phase_stats(b_cs, wins, excl)
    kw = {"full_s": full[p]["seconds"]}
    if "full_sd" in inspect.signature(custlat._cell).parameters:
        kw["full_sd"] = full[p]["half_sd"][m]
    return custlat._cell(p, m, ka.get(p), kb.get(p), Z9, replica, judged=True,
                         excluded={"a_s": 0.0, "b_s": 0.0, "why": []}, **kw)


def _num(bar):
    return float(bar.split()[1])


def test_review_probe1_a_shorter_sample_never_makes_pass_easier():
    """The reviewer's probe 1: B = k x A at every call; the calm 80 % of A is
    cut, the volatile 20 % kept. On the full phase k FAILs; after the cut the
    PASS bar must not have moved past the full one, so k must not PASS
    (805ad3bb: p99 full bar <= 1.0004, cut PASS bar <= 1.9267: 1.46 PASSed)."""
    rnd = random.Random(1)
    calm = {(k, j): 100 + rnd.random() * 2 for k in range(40) for j in range(20)}
    vol = {(k, j): (100 + rnd.random() * 2) * (1 + (rnd.random() * 3 if j == 0 and k % 2 else 0))
           for k in range(40) for j in range(20)}
    a_cs = _blocks_calls(40, lambda k, j: vol[(k, j)] if k >= 32 else calm[(k, j)])
    excl = [(1000.0, 1320.0)]
    for m in ("p95_ms", "p99_ms"):
        full_c = _probe_cell("steady", m, a_cs, a_cs, [], [])
        cut_c = _probe_cell("steady", m, a_cs, a_cs, [], excl)
        assert _num(cut_c["bar"]) <= _num(full_c["bar"]), (m, cut_c["bar"], full_c["bar"])
        assert _num(cut_c["bar"]) < _num(cut_c["fail_bar"])
        k = 1.46
        b_cs = [(t, v * k) for t, v in a_cs]
        assert _probe_cell("steady", m, a_cs, b_cs, [], []) ["result"] == "FAIL"
        cut = _probe_cell("steady", m, a_cs, b_cs, [], excl)["result"]
        # p99 (the probe's flip): undecidable on the volatile fifth; p95: past even its FAIL bar
        assert cut == {"p95_ms": "FAIL", "p99_ms": "INCONCLUSIVE"}[m], (m, cut)


def test_review_the_cap_keeps_a_shorter_sample_from_loosening_pass():
    """Calm blocks 0-29, volatile 30-39 (a 3x spike in 1 of 40 calls); the
    first 12 calm blocks cut. Kept p99 is volatile, the whole phase's calm:
    without the cap at A's whole-phase sd the scaled bar (1.43) is looser than
    the full phase's (1.03) and B = 1.2 x A everywhere would PASS here while it
    FAILs on the full phase. With the cap it does not PASS."""
    rnd = random.Random(1)
    calm = {(k, j): 100 + rnd.random() * 2 for k in range(40) for j in range(20)}
    vol = {(k, j): (100 + rnd.random() * 2) * (1 + (rnd.random() * 3 if j == 0 and k % 2 else 0))
           for k in range(40) for j in range(20)}
    a_cs = _blocks_calls(40, lambda k, j: vol[(k, j)] if k >= 30 else calm[(k, j)])
    excl = [(1000.0, 1120.0)]
    b_cs = [(t, v * 1.2) for t, v in a_cs]
    full = _probe_cell("steady", "p99_ms", a_cs, b_cs, [], [])
    cut = _probe_cell("steady", "p99_ms", a_cs, b_cs, [], excl)
    assert full["result"] == "FAIL" and _num(full["bar"]) < 1.2
    assert _num(cut["bar"]) <= _num(full["bar"]) and cut["pass_bar_from"] == "A's whole-phase sd"
    assert cut["result"] != "PASS", cut["line"]


def test_review_probe2_the_replica_floor_keeps_the_band():
    """The reviewer's probe 2: 39.5 of a 1999.5 s storm jointly Ready, the
    replica above A's own spread. The kept sample's extra spread is charged
    against the replica too: the PASS bar sits below the FAIL bar and below
    the full storm's bar (805ad3bb: both bars = the replica, 1.10x PASSed,
    no INCONCLUSIVE at all), and the line names the replica. The kept bar
    is sized with a t quantile on its block pairs - 1 (4 blocks: 1 df), so
    1.10x on 2 % of a calm storm does not PASS (23de078c: z, and it did)."""
    rnd = random.Random(2)
    a_cs = _blocks_calls(200, lambda k, j: 100 + rnd.random())
    wins = [("storm", float("-inf"), 3001.0)]
    excl = [(1000.0, 2960.0)]
    rep = {"phases": {"storm": {"p95_ms": 1.1149, "p99_ms": 1.0651, "req_s": 1.0036}}}
    full_c = _probe_cell("storm", "p95_ms", a_cs, a_cs, wins, [], replica=rep)
    c = _probe_cell("storm", "p95_ms", a_cs, a_cs, wins, excl, replica=rep)
    assert c["kept_s"] == 39.5 and c["full_s"] == 1999.5
    assert c.get("kept_quantile", {}).get("df") == 1
    bar, fail_bar = _num(c["bar"]), _num(c["fail_bar"])
    assert fail_bar >= 1.1149 and bar < fail_bar and bar < _num(full_c["bar"]) == 1.1149
    assert "PASS <= %s = replica bar 1.1149" % bar in c["line"] and "on 1 df" in c["line"]
    assert "(full-length sd)" not in c["line"]

    def at(k):
        return _probe_cell("storm", "p95_ms", a_cs, [(t, v * k) for t, v in a_cs], wins, excl, replica=rep)
    assert at(1.10)["result"] != "PASS"
    assert at((bar + fail_bar) / 2)["result"] == "INCONCLUSIVE"
    assert at(fail_bar * 1.05)["result"] == "FAIL"


def test_review_nothing_excluded_is_the_bar_of_before():
    rnd = random.Random(3)
    a_cs = _blocks_calls(40, lambda k, j: rnd.lognormvariate(4, 0.3))
    for rp in (None, {"phases": {"steady": {"p95_ms": 1.3, "p99_ms": 1.01, "req_s": 1.2}}}):
        for m in custlat.METRICS:
            c = _probe_cell("steady", m, a_cs, a_cs, [], [], replica=rp)
            sd = c["a_half_sd"]
            tol = max(math.exp(Z9 * math.sqrt(2) * sd), (custlat._replica(rp, "steady", m) or 1.0))
            want = round(1 / tol, 4) if m == "req_s" else round(tol, 4)
            assert _num(c["bar"]) == _num(c["fail_bar"]) == want and c["extra_spread"] == 1.0


def test_review_property_a_pass_after_exclusion_is_a_pass_on_full_data():
    """Random volatility splits, random cuts, random replicas, B = A with one
    true effect everywhere (x k latency; req/s thinned to a share q in every
    block): whatever is cut, a PASS on the kept time is a PASS on the full
    phase. Both PASS and INCONCLUSIVE occur (the property is not vacuous)."""
    rnd = random.Random(606)
    seen = {"PASS": 0, "INCONCLUSIVE": 0, "FAIL": 0}
    for trial in range(36):
        m = custlat.METRICS[trial % 3]
        nb = rnd.randint(24, 48)
        vol = [rnd.choice((0.02, 0.05, 0.3, 1.0)) for _ in range(nb)]
        val = {(k, j): rnd.lognormvariate(4.5, vol[k]) for k in range(nb) for j in range(20)}
        a_cs = _blocks_calls(nb, lambda k, j: val[(k, j)])
        lo = rnd.randint(0, nb - 6)
        hi = rnd.randint(lo + 2, min(nb, lo + int(nb * 0.9)))
        excl = [(1000.0 + lo * 10, 1000.0 + hi * 10)]
        rp = None if rnd.random() < 0.5 else {"phases": {"steady": {m: round(1 + rnd.random() * 0.3, 4)}}}
        if m == "req_s":
            q = rnd.choice((0.8, 0.85, 0.9, 0.95, 1.0))
            keep = round(20 * q)
            b_cs = [c for i, c in enumerate(a_cs) if i % 20 < keep]
        else:
            k = rnd.choice((0.95, 1.0, 1.02, 1.05, 1.1, 1.2, 1.4))
            b_cs = [(t, v * k) for t, v in a_cs]
        cut = _probe_cell("steady", m, a_cs, b_cs, [], excl, replica=rp)
        full = _probe_cell("steady", m, a_cs, b_cs, [], [], replica=rp)
        if cut is None or full is None:
            continue
        seen[cut["result"]] += 1
        lower = m == "req_s"
        # one true effect: the ratios differ only by the stats' 2-decimal rounding
        assert abs(cut["ratio"] - full["ratio"]) <= 0.01 * full["ratio"], (trial, cut["ratio"], full["ratio"])
        if cut["result"] == "PASS":
            # the same effect passes the full phase's bar
            r = cut["ratio"]
            assert (r >= _num(full["bar"])) if lower else (r <= _num(full["bar"])), (trial, m, cut["line"],
                                                                                    full["line"])
        assert (_num(cut["bar"]) >= _num(full["bar"])) if lower else (_num(cut["bar"]) <= _num(full["bar"]))
    assert seen["PASS"] and seen["INCONCLUSIVE"], seen


def test_review_b_restart_seen_only_by_its_own_check_fails(tmp_path):
    """B's report has no readiness record (an older verdict), but its own
    restart check counted 1: the pair FAILs, never reads INVALID."""
    _verdict(tmp_path / "a", P14, DIG_A)
    v = _verdict(tmp_path / "b", _shift(_down(P14, 900, 1100), SHIFT), DIG_B)
    v["report"]["customer_latency"].pop("readiness")
    (tmp_path / "b" / "verdict.json").write_text(json.dumps(v))
    p = _pair(tmp_path / "a", tmp_path / "b")
    assert p["verdict"] == "FAIL" and p["candidate_restarts"]["value"] == 1
    assert "seen by its own restart check" in p["candidate_restarts"]["why"]
    # ... and a replaced pod counts as a restart too
    v["validity"] = [{"check": custlat.RESTART_CHECK, "result": "FAIL", "value": None,
                      "why": "snowplow pod REPLACED during measure: x -> y"}]
    (tmp_path / "b" / "verdict.json").write_text(json.dumps(v))
    assert _pair(tmp_path / "a", tmp_path / "b")["verdict"] == "FAIL"


def _crash_pair(tmp, b_doc, mangle=None, **kw):
    """The pair CLI when pair() raises (B's phases malformed)."""
    _verdict(tmp / "a", P14, DIG_A)
    v = _verdict(tmp / "b", b_doc, DIG_B, **kw)
    v["report"]["customer_latency"]["phases"] = 7
    if mangle:
        mangle(v)
    (tmp / "b" / "verdict.json").write_text(json.dumps(v))
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = custlat.main(["pair", "--a", str(tmp / "a" / "verdict.json"), "--b", str(tmp / "b" / "verdict.json")])
    assert buf.getvalue().strip(), "the paired verdict is never empty"
    return rc, json.loads(buf.getvalue())


def test_review_crash_path_keeps_b_restart_and_not_ready_a_fail(tmp_path):
    rc, p = _crash_pair(tmp_path / "r", _shift(_down(P14, 900, 1100), SHIFT))
    assert rc == 1 and p["verdict"] == "FAIL" and "candidate_restarts" in p["fail_reasons"][0]
    assert p["invalid_reasons"][0].startswith("the paired verdict could not be computed")
    rc, p = _crash_pair(tmp_path / "n", _shift(_down(P14, 900, 1100, restart=False, code=503), SHIFT))
    assert rc == 1 and p["verdict"] == "FAIL" and "candidate_not_ready" in p["fail_reasons"][0]
    rc, p = _crash_pair(tmp_path / "h", _shift(P14, SHIFT))
    assert rc == 3 and p["verdict"] == "INVALID"
    # no reply to readyz needs k6 as its witness, which the crash path has not read: INVALID
    rc, p = _crash_pair(tmp_path / "z", _shift(_down(P14, 900, 1100, restart=False), SHIFT))
    assert rc == 3 and p["verdict"] == "INVALID"


def test_review_crash_path_reads_a_malformed_record_without_raising(tmp_path):
    def mangle(v):
        ex = v["report"]["customer_latency"]["readiness"]["excluded"]
        ex[0].pop("seconds")                                   # a stretch without its seconds
        ex.append("not a stretch")
    rc, p = _crash_pair(tmp_path, _shift(_down(P14, 900, 1100, restart=False, code=503), SHIFT), mangle=mangle)
    assert rc == 1 and p["verdict"] == "FAIL" and "candidate_not_ready" in p["fail_reasons"][0]


def _sampler_stops(doc, at_s, k0):
    d = copy.deepcopy(doc)
    d["mem"] = [r for r in d["mem"] if r["kind"] != "mem" or r["t"] < k0 + at_s]
    return d


def test_review_b_sampler_stops_then_b_degrades_is_invalid(tmp_path):
    """The reviewer's probe: B's mem606 stops at 900 s, then B runs 3x
    slower with failing requests, no restart. B's readiness is unknown there:
    the pair is INVALID — never a PASS, never an accidental INCONCLUSIVE."""
    k0b = K0 + SHIFT
    b = _sampler_stops(_shift(P14, SHIFT), 900, k0b)
    first = b["calls"][0][0]
    b["failed"] = [[o, 1 if (o - first) >= 1000 and i % 10 == 0 else v] for i, (o, v) in enumerate(b["failed"])]
    p = _arms(tmp_path, P14, b, scale=lambda t, v: v * 3 if t >= k0b + 1000 else v)
    assert p["verdict"] == "INVALID"
    assert "customer_latency.pair.readiness_known[b]" in {r.split(":")[0] for r in p["invalid_reasons"]}
    rk = next(c for c in p["validity"] if c["check"] == "customer_latency.pair.readiness_known[b]")
    assert rk["value"]["seconds"] > 800 and "after the last readiness sample" in rk["why"]
    # A's sampler stopping the same way is excluded from both runs, never voids
    p = _arms(tmp_path / "a", _sampler_stops(P14, 900, K0), _shift(P14, SHIFT))
    assert "customer_latency.pair.readiness_known[b]" not in {r.split(":")[0] for r in p["invalid_reasons"]}
    assert [it["what"] for it in p["exclusion"]["items"]] == ["A unknown #1"]


def _gap(doc, t0_s, t1_s, k0):
    """doc's mem606 samples missing from k0 + t0_s to k0 + t1_s (Ready on both sides)."""
    d = copy.deepcopy(doc)
    d["mem"] = [r for r in d["mem"] if r["kind"] != "mem" or not (k0 + t0_s < r["t"] < k0 + t1_s)]
    return d


def _no_calls(doc, lo, hi):
    """doc without any k6 call or request from lo to hi s after its first call"""
    d = copy.deepcopy(doc)
    first = d["calls"][0][0]
    d["calls"] = [c for c in d["calls"] if not lo <= c[0] - first < hi]
    d["failed"] = [f for f in d["failed"] if not lo <= f[0] - first < hi]
    return d


def test_review_a_gap_over_twice_the_interval(tmp_path):
    """A gap between two Ready samples longer than twice the sampler's own
    interval_s (15 s), no restart signal, is a "gap" stretch. A's: excluded
    from both runs (as before). B's: k6 is the witness — served within its 1 %
    bar -> jointly-Ready time, judged in the cells (a slow /debug/vars under
    OOM pressure must not void a paid run); errors over the bar -> B was not
    serving: FAIL; not one k6 request in it -> no witness: void. A gap under
    twice the interval is plain Ready time."""
    rd = custlat.readiness(_gap(P14, 1000, 1040, K0)["mem"], None, [K0, K0 + 1800])
    (e,) = rd["excluded"]
    assert e["kind"] == "gap" and e["seconds"] > 30 and "over twice the sampler's interval, 15.0 s" in e["why"]
    late = copy.deepcopy(P14)                                  # one sample 12 s late: a ~27 s gap, under 30 s
    rows = [r for r in late["mem"] if r["kind"] == "mem"]
    i = next(i for i, r in enumerate(rows) if r["t"] > K0 + 1000)
    rows[i]["t"] += 12
    assert 2 * 15 > rows[i]["t"] - rows[i - 1]["t"] > 15
    assert custlat.readiness(late["mem"], None, [K0, K0 + 1800])["excluded"] == []
    p = _arms(tmp_path / "a", _gap(P14, 1000, 1100, K0), _shift(P14, SHIFT))
    assert p["verdict"] == "PASS" and [it["what"] for it in p["exclusion"]["items"]] == ["A gap #1"]
    gapb = _shift(_gap(P14, 1000, 1100, K0), SHIFT)
    # served: jointly-Ready time, nothing cut, B's calls there judged
    p = _arms(tmp_path / "b", P14, gapb)
    assert p["verdict"] == "PASS" and p["exclusion"]["items"] == [] and not p["invalid_reasons"]
    (w,) = p["b_witnessed_ready"]
    assert w["kind"] == "gap" and w["k6"]["requests"] > 0
    # 3x slower inside the gap: the cells see it (judged, not hidden)
    k0b = K0 + SHIFT
    slow = _arms(tmp_path / "slow", P14, gapb, scale=lambda t, v: v * 3 if k0b + 990 <= t < k0b + 1110 else v)
    base = {c["check"]: c["value"]["ratio"] for c in p["checks"]}
    assert any(c["value"]["ratio"] > base[c["check"]] for c in slow["checks"] if ".pair[" in c["check"])
    # errors over the bar inside it: B was not serving -> FAIL
    p = _arms(tmp_path / "err", P14, _failing(gapb, 1010, 1090))
    assert p["verdict"] == "FAIL" and p["candidate_not_ready"]["result"] == "FAIL"
    assert p["candidate_not_ready"]["value"]["stretches"][0]["k6"]["rate"] > 0.01
    # not one k6 request in it: no witness -> void
    p = _arms(tmp_path / "none", P14, _no_calls(gapb, 950, 1150))
    assert p["verdict"] == "INVALID"
    assert [r.split(":")[0] for r in p["invalid_reasons"]] == ["customer_latency.pair.readiness_known[b]"]
    assert "no witness" in p["invalid_reasons"][0]


def test_review_b_inside_a_outage_is_shown(tmp_path):
    """B's customers inside the window cut for A's restart: 4x slower there
    and failing; the cells never see it, the shown-only line does."""
    k0b = K0 + SHIFT
    b = _shift(P14, SHIFT)
    first = b["calls"][0][0]
    b["failed"] = [[o, 1 if 900 <= o - first < 1100 else v] for o, v in b["failed"]]
    p = _arms(tmp_path, _down(P14, 900, 1100), b,
              scale=lambda t, v: v * 4 if k0b + 900 <= t < k0b + 1100 else v)
    assert p["verdict"] == "PASS"                          # the cells cut that window from both
    (row,) = p["b_inside_a_outage"]
    assert row["what"] == "A restart #1" and row["calls"] > 0 and row["error_rate"] > 0.5
    base = _arms(tmp_path / "base", _down(P14, 900, 1100), _shift(P14, SHIFT))["b_inside_a_outage"][0]
    assert row["p95_ms"] > 3 * base["p95_ms"] and row["calls"] == base["calls"]
    assert row["line"].startswith("INFO B inside A's outage: A restart #1") and "shown, not judged" in row["line"]


def test_review_run15_b_inside_a_outage(run15):
    _, _, _, _, p = run15
    rows = {r["what"]: r for r in p["b_inside_a_outage"]}
    assert sorted(rows) == ["A restart #1", "A restart #2"]
    assert rows["A restart #1"]["b_s"] == 675.2 and rows["A restart #2"]["b_s"] == 750.1
    assert all(r["calls"] > 0 and r["error_rate"] is not None for r in rows.values())


def _stage(tmp, fake, a_doc, b_doc):
    """Each run's verdict for the stand-in run606 to place in its OUT, and its
    REAL k6 stream already in that OUT (the pair reads it through --a-dir/--b-dir)."""
    for name, out, doc, dig in ((NA, "A", a_doc, DIG_A), (NB, "B", b_doc, DIG_B)):
        d = tmp / f"src-{out}"
        _verdict(d, doc, dig)
        shutil.copytree(d / "k6", tmp / out / "k6")
        shutil.copy(d / "verdict.json", fake / f"{name}.verdict.json")


@pytest.mark.parametrize("case", ["a_restart_only", "b_restart"])
def test_run606_pair_logs_fail_note_excluded_lines(pairbed, case):  # noqa: F811
    """run606_pair.sh end to end (stand-in run606, real k6 streams in each
    OUT): the pair CLI gets --a-dir/--b-dir, the raw calls match, and the log
    carries the FAIL / NOTE / EXCLUDED / A-healthy / B-inside-A's-outage lines."""
    repo, fake, env, tmp = pairbed
    a = _down(P14, 900, 1100)
    b = _shift(P14, SHIFT) if case == "a_restart_only" else _shift(_down(P14, 1200, 1350), SHIFT)
    _stage(tmp, fake, a, b)
    p = _launch(repo, env)
    log = (tmp / "pair" / "pair.log").read_text()
    v = json.loads((tmp / "pair" / "paired-verdict.json").read_text())
    assert "--a-dir" in (repo / "run606_pair.sh").read_text()
    assert "EXCLUDED from both runs: A restart #1" in log
    assert not any(c["check"].startswith("customer_latency.pair.raw_calls") and c["result"] == "FAIL"
                   for c in v["validity"])
    assert "INFO B inside A's outage: A restart #1" in log
    if case == "a_restart_only":
        assert p.returncode == 0 and v["verdict"] == "PASS"
        assert "NOTE: A (the baseline) restarted 1x" in log and "NOTE: B passes churn" in log
        assert "INFO A-healthy churn p95_ms" in log
    else:
        assert p.returncode == 1 and v["verdict"] == "FAIL"
        assert "FAIL: customer_latency.pair.candidate_restarts" in log
        assert "EXCLUDED from both runs: B restart #1" in log
        assert "paired verdict exit=1" in log
