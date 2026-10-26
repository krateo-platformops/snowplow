"""Run 15 (#865, Diego 2026-10-23): PAIRED runs — 1.12.54 (A) and 1.12.55 (B)
in parallel, k6's customers on through every churn step (K6_SPAN=churn),
and the paired customer-latency verdict (bench/custlat.py pair, launched by
run606_pair.sh).

tests/fixtures/customer_latency_run14_gate_phases.json is run 14's REAL
gate-phase spread (custlat over its whole collected artifacts): option A's
bars on it, and whether x0.87 req/s / x1.17 p95 would be caught.

tests/fixtures/customer_latency_run14_every50.json.gz is RECORDED from run
14's artifacts (1.12.54, bench adc2e702): every 50th k6 http_req_duration
point in stream order, the k6 config and browser sessions, mem606's phase
anchors and refresher counter, fresh606's churn records and kubectl top
(pod names replaced); since the paired gate's Ready-time rule (2026-10-24),
from the same artifacts, each mem606 sample's readyz and prewarm_complete
(all Ready) and every 50th http_req_failed point. Run 14's k6 stopped inside the first churn step; the
paired-schedule runs below keep its calls and lay its six churn steps over
k6's span instead (_paired), the shape K6_SPAN=churn produces. The tests
write a run out in run606's $OUT layout and run `load606 evaluate` on it, as
run606.sh does.
"""

from __future__ import annotations

import copy
import gzip
import json
import math
import os
import random
import shutil
import statistics
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from statistics import NormalDist

import pytest

from bench import custlat, load606
from tests.test_run14_list_narrow import FRESH, LN_KEY, _mem

FIX = Path(__file__).resolve().parent / "fixtures"
BENCH = Path(__file__).resolve().parents[1]
with gzip.open(FIX / "customer_latency_run14_every50.json.gz", "rt") as fh:
    R14 = json.load(fh)
REPLICA = custlat.load_replica(str(BENCH / "customer_latency_replica_runs12_13.json"))
DIG_A, DIG_B = "sha256:" + "a" * 64, "sha256:" + "b" * 64


def _paired(doc=R14):
    """Run 14 on the paired schedule: its six churn steps, in order, laid
    evenly over k6's span, the last ending 200 s before k6's last call (a
    steady tail with customers, as K6_SPAN=churn leaves when fresh606 ends
    before its bound)."""
    d = copy.deepcopy(doc)
    t0 = d["t0"] + d["calls"][0][0]
    t1 = d["t0"] + max(off for off, _ in d["calls"]) - 200
    step = (t1 - t0) / len(d["churns"])
    for i, c in enumerate(d["churns"]):
        c["t0"], c["t1"] = t0 + i * step, t0 + (i + 1) * step
    return d


P14 = _paired()


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "123Z"


def _write_run(d: Path, doc=P14, scale=lambda t, v: v, drop=lambda t: False, config=None) -> list[str]:
    """`doc` in run606's $OUT layout; `scale` rewrites a call's ms, `drop`
    removes calls (by epoch). Returns evaluate's input files."""
    (d / "k6").mkdir(parents=True)
    (d / "browser").mkdir()
    with gzip.open(d / "k6" / "k6.json.gz", "wt") as fh:
        for off, v in doc["calls"]:
            t = doc["t0"] + off
            if drop(t):
                continue
            fh.write(json.dumps({"metric": "http_req_duration", "type": "Point",
                                 "data": {"time": _iso(t), "value": scale(t, v),
                                          "tags": {"scenario": "navigate", "status": "200"}}}) + "\n")
        for off, v in doc.get("failed") or []:
            t = doc["t0"] + off
            if drop(t):
                continue
            fh.write(json.dumps({"metric": "http_req_failed", "type": "Point",
                                 "data": {"time": _iso(t), "value": v, "tags": {"scenario": "navigate"}}}) + "\n")
    (d / "k6" / "k6-summary.json").write_text(json.dumps({"config": config or doc["config"]}))
    (d / "browser" / "browser-summary.json").write_text(json.dumps({"sessions": doc["browser_sessions"]}))
    files = {"mem-x.jsonl": doc["mem"], "fresh-x.jsonl": doc["churns"], "top.jsonl": doc["tops"],
             **({"pod-x.jsonl": doc["pods"]} if doc.get("pods") else {}),
             **({"api-outages.jsonl": doc["api_outages"]} if doc.get("api_outages") else {})}
    for name, rows in files.items():
        (d / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
    return [str(d / n) for n in files]


def _evaluate(d: Path, files, capsys, digest=DIG_A) -> dict:
    capsys.readouterr()
    load606.main(["evaluate", *files, "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
                  "--browser-sessions", "5", "--no-viewers", "--expect-digest", digest])
    v = json.loads(capsys.readouterr().out)
    # the fixture has no snowplow pods / image-digest gate records: the
    # run's own validity is not what these tests judge
    v["verdict"] = "FAIL"
    return v


def _run(tmp, name, capsys, digest=DIG_B, **kw) -> dict:
    d = tmp / name
    return _evaluate(d, _write_run(d, **kw), capsys, digest)


@pytest.fixture(scope="module")
def va(tmp_path_factory):
    d = tmp_path_factory.mktemp("a")
    files = _write_run(d)
    args = ["evaluate", *files, "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
            "--browser-sessions", "5", "--no-viewers", "--expect-digest", DIG_A]
    p = subprocess.run(["python3", "-m", "bench.load606", *args], cwd=BENCH, capture_output=True,
                       text=True, check=False)
    v = json.loads(p.stdout)
    v["verdict"] = "FAIL"
    return v


def _pair(va, vb, **kw):
    return custlat.pair(va, vb, replica=kw.pop("replica", REPLICA), **kw)


def _cells(p):
    return {c["check"]: c for c in p["checks"]}


def _gp(v):
    return v["report"]["customer_latency"]["gate_phases"]


def _bar(p, phase, m):
    return float(_cells(p)[f"customer_latency.pair[{phase}].{m}"]["bar"].split()[2])


def test_the_paired_schedule_puts_customers_in_every_churn_step(va):
    r = va["report"]["customer_latency"]
    ph = r["phases"]
    assert {"storm", "burst"} <= set(ph)
    churn = [p for p in ph if p.startswith("churn:") and ph[p]["n"]]
    assert len(churn) >= 4, list(ph)             # storm / burst take the rest
    for s in ph.values():
        assert s["p99_ms"] > s["p95_ms"]
        assert s["req_s"] == round(s["n"] / s["seconds"], 2)
    assert sum(s["n"] for s in ph.values()) == len(P14["calls"]) == r["calls"]
    # the gate's phases: every churn step pooled, the same calls
    g = r["gate_phases"]
    assert set(g) == {"storm", "burst", "churn", "steady"}
    assert g["churn"]["n"] == sum(ph[p]["n"] for p in churn)
    assert g["storm"] == ph["storm"] and g["burst"] == ph["burst"]
    # refresher completions / CPU per churn step, while customers were on
    rc = r["refresher_completions"]["by_phase"]
    assert all(rc[p]["with_customers"]["seconds"] > 0 for p in churn)
    cpu = r["snowplow_cpu"]["by_phase"]
    assert all(cpu[p]["with_customers"]["samples"] > 0 for p in churn)
    assert r["schedule"]["churn"][0] == ["list_dep@1", 1.0] and len(r["schedule"]["churn"]) == 6


def test_a_against_itself_passes_on_the_nine_judged_cells(va):
    a, b = _ab(va)
    p = _pair(a, b, expect_a=DIG_A, expect_b=DIG_B)
    assert p["verdict"] == "PASS", p["invalid_reasons"]
    assert p["method"]["judged"] == ["storm", "churn", "steady"] and p["method"]["cells"] == 9
    assert sorted(c["check"] for c in p["checks"]) == sorted(
        f"customer_latency.pair[{ph}].{m}" for ph in ("storm", "churn", "steady") for m in custlat.METRICS)
    assert {c["value"]["ratio"] for c in p["checks"]} == {1.0}
    assert p["method"]["z"] == round(NormalDist().inv_cdf(1 - 0.05 / 9), 4)


def test_burst_and_each_churn_step_are_shown_never_judged(va):
    a, b = _ab(va)
    gb, steps = _gp(b), [s for s in b["report"]["customer_latency"]["phases"] if s.startswith("churn:")]
    gb["burst"]["p95_ms"] *= 50                                  # far past any bar
    for s in steps:
        b["report"]["customer_latency"]["phases"][s]["p99_ms"] *= 50
    p = _pair(a, b)
    assert p["verdict"] == "PASS" and not [c for c in p["checks"] if "burst" in c["check"]]
    info = [c for c in p["cells"] if not c["judged"]]
    assert {c["phase"] for c in info} == {"burst", *[s for s in steps if _gp(a)["churn"]["n"]
                                                     and a["report"]["customer_latency"]["phases"][s]["n"]]}
    assert all(c["result"] == "INFO" and "not judged" in c["line"] for c in info)
    bp = next(c for c in info if c["phase"] == "burst" and c["metric"] == "p95_ms")
    assert bp["ratio"] == 50.0 and bp["ratio"] > float(bp["bar"].split()[1])


def test_b_slower_in_pooled_churn_fails_just_past_its_spread(va, tmp_path, capsys):
    wins = custlat.pooled([tuple(w) for w in va["report"]["customer_latency"]["windows"]])
    a, _ = _ab(va)
    base = _pair(a, a)
    tol = [_bar(base, "churn", m) for m in ("p95_ms", "p99_ms")]
    for k, want in ((max(tol) * 1.02, "FAIL"), (min(tol) * 0.98, "PASS")):
        vb = _run(tmp_path, f"slow{k}", capsys,
                  scale=lambda t, ms, k=k: ms * k if custlat._phase(t, wins) == "churn" else ms)
        p = _pair(a, vb, expect_a=DIG_A, expect_b=DIG_B)
        res = {c: x["result"] for c, x in _cells(p).items()}
        assert res.pop("customer_latency.pair[churn].p95_ms") == want, k
        assert res.pop("customer_latency.pair[churn].p99_ms") == want, k
        assert set(res.values()) == {"PASS"}
        assert p["verdict"] == want, p["invalid_reasons"]


def test_b_with_fewer_calls_fails_req_s_only_below_the_floor(va, tmp_path, capsys):
    wins = custlat.pooled([tuple(w) for w in va["report"]["customer_latency"]["windows"]])
    floor = _bar(_pair(va, va), "churn", "req_s")
    ts = [P14["t0"] + off for off, _ in P14["calls"] if custlat._phase(P14["t0"] + off, wins) == "churn"]
    for share, want in ((floor - 0.05, "FAIL"), (min(1.0, floor + 0.05), "PASS")):
        gone = {t for i, t in enumerate(ts) if math.floor((i + 1) * share) == math.floor(i * share)}
        vb = _run(tmp_path, f"drop{share}", capsys, drop=lambda t, gone=gone: t in gone)
        c = _cells(_pair(va, vb))["customer_latency.pair[churn].req_s"]
        assert c["result"] == want, (share, c)


# ─── option A on run 14's REAL spread ─────────────────────────────────────
GP14 = json.loads((FIX / "customer_latency_run14_gate_phases.json").read_text())["gate_phases"]
# a hand-built run's readiness and k6 errors: Ready all along, no failed request
READY_NO_ERRORS = {"readiness": {"excluded": [], "restarts": {"restarted": False, "count": 0, "in_samples": 0,
                                                              "unlocated": []}},
                   "k6_errors": {"requests": 1000, "failed": 0, "rate": 0.0}}


def _run14_pair(scale: dict, phases=("storm", "churn"), extra=None):
    """A = run 14's real gate phases (fixture); B = A with metrics scaled."""
    gpa = {p: copy.deepcopy(GP14[p]) for p in (*phases, "burst")}
    gpa.update(extra or {})
    gpb = copy.deepcopy(gpa)
    for (p, m), k in scale.items():
        gpb[p][m] = round(gpb[p][m] * k, 4)

    def v(gp, dig):
        return {"verdict": "FAIL", "report": {"image_digest": {"expected": dig}, "customer_latency": {
            "gate_phases": gp, "phases": {}, "k6_span": [0, 100], "schedule": {}, **READY_NO_ERRORS,
            "windows": [["storm", float("-inf"), 10], ["churn:list_dep@1", 10, 90]]}}}
    return custlat.pair(v(gpa, DIG_A), v(gpb, DIG_B), replica=REPLICA)


def test_run14_spread_x087_req_s_and_x117_p95():
    """Option A's bars on run 14's REAL spread (what Diego asked to see).

    Run 14 had customers only in its first churn step and no steady, so it
    judges 6 cells (z 2.394): there x0.87 req/s in pooled churn FAILs (floor
    0.8719) and x1.17 storm p95 FAILs (bar 1.1571), x1.17 churn p95 passes
    (bar 1.4992: run 14's churn p95 spread is wide)."""
    p = _run14_pair({("churn", "req_s"): 0.87})
    assert p["method"]["cells"] == 6 and p["method"]["z"] == 2.394
    assert _bar(p, "churn", "req_s") == 0.8719 and p["verdict"] == "FAIL"
    assert _cells(p)["customer_latency.pair[churn].req_s"]["result"] == "FAIL"
    p = _run14_pair({("storm", "p95_ms"): 1.17})
    assert _bar(p, "storm", "p95_ms") == 1.1571 and p["verdict"] == "FAIL"
    p = _run14_pair({("churn", "p95_ms"): 1.17})
    assert _bar(p, "churn", "p95_ms") == 1.4992 and p["verdict"] == "PASS"     # missed


def test_run14_spread_with_nine_cells_misses_x087_by_a_hair():
    """With a steady phase too (the paired schedule's 9 cells, z 2.539), the
    same run-14 spread gives a churn floor of 0.8646: x0.87 PASSes, missed by
    0.006. It is caught once pooled churn's req/s sd < 0.0388 (run 14's,
    over 991 s of churn with customers: 0.0405; a paired run pools ~11x the
    seconds — run 14's own sd falls with length, 0.0636 at 240 s, 0.0477 at
    481 s — but that is a projection, not a measurement)."""
    steady = {"steady": copy.deepcopy(GP14["churn"])}          # a stand-in third judged phase
    p = _run14_pair({("churn", "req_s"): 0.87}, extra=steady)
    assert p["method"]["cells"] == 9 and p["method"]["z"] == 2.5392
    assert _bar(p, "churn", "req_s") == 0.8646 and p["verdict"] == "PASS"
    sd_needed = math.log(1 / 0.87) / (p["method"]["z"] * math.sqrt(2))
    assert round(sd_needed, 4) == 0.0388
    tighter = copy.deepcopy(steady)
    gp = copy.deepcopy(GP14["churn"])
    gp["half_sd"]["req_s"] = 0.0387                             # just under the sd it needs
    p = _run14_pair({("churn", "req_s"): 0.87}, extra=dict(tighter, churn=gp))
    assert p["verdict"] == "FAIL"
    # x1.17 storm p95 is still caught at 9 cells (bar 1.1674)
    p = _run14_pair({("storm", "p95_ms"): 1.17}, extra=steady)
    assert _bar(p, "storm", "p95_ms") == 1.1674 and p["verdict"] == "FAIL"


def test_b_own_spread_never_sets_the_bar(va):
    a, b = _ab(va)
    for s in _gp(b).values():
        s["half_sd"] = {m: 5.0 for m in s["half_sd"]}
    assert [c["bar"] for c in _pair(a, b)["checks"]] == [c["bar"] for c in _pair(a, a)["checks"]]
    wide = copy.deepcopy(a)
    for s in _gp(wide).values():
        s["half_sd"] = {m: 2 * x for m, x in s["half_sd"].items()}
    assert [c["bar"] for c in _pair(wide, b)["checks"]] != [c["bar"] for c in _pair(a, b)["checks"]]


def test_the_tolerance_is_the_method():
    def rep(sd, p95=100.0):
        ph = {"n": 10, "p95_ms": p95, "p99_ms": 200.0, "req_s": 50.0,
              "half_sd": {"p95_ms": sd, "p99_ms": sd, "req_s": sd}}
        return {"verdict": "FAIL", "report": {"image_digest": {"expected": DIG_A}, "customer_latency": {
            "gate_phases": {"storm": dict(ph), "churn": dict(ph)}, "phases": {}, "k6_span": [0, 100],
            "windows": [["storm", float("-inf"), 10], ["churn:list_dep@10", 10, 90]], "schedule": {},
            **READY_NO_ERRORS}}}
    a = rep(0.05)
    b = copy.deepcopy(a)
    b["report"]["image_digest"]["expected"] = DIG_B
    p = custlat.pair(a, b, replica={"phases": {"storm": {"p95_ms": 1.9}, "churn": {"p95_ms": 1.01}}})
    z = NormalDist().inv_cdf(1 - 0.05 / 6)
    w = math.exp(z * math.sqrt(2) * 0.05)
    within = round(w, 4)
    bars = {c["check"]: c["bar"].split()[2] for c in p["checks"]}
    assert bars["customer_latency.pair[storm].p95_ms"] == "1.9"                 # replica above within
    assert bars["customer_latency.pair[storm].p99_ms"] == str(within)          # no replica for it
    assert bars["customer_latency.pair[churn].p95_ms"] == str(within)          # replica 1.01 below it
    assert bars["customer_latency.pair[storm].req_s"] == str(round(1 / w, 4))
    assert p["verdict"] == "PASS" and p["method"]["cells"] == 6


def _gpu(v, phase, **kw):
    _gp(v)[phase].update(**kw)


@pytest.mark.parametrize("breaks, check", [
    (lambda a, b: b["report"]["customer_latency"]["schedule"]["k6"].update(thinkS=30), "schedules_match"),
    (lambda a, b: b["report"]["customer_latency"]["schedule"]["churn"].pop(), "schedules_match"),
    (lambda a, b: b["report"]["image_digest"].update(expected=DIG_A), "images"),
    (lambda a, b: b.update(verdict="INVALID"), "runs_valid"),
    (lambda a, b: a.update(verdict="INVALID"), "runs_valid"),
    (lambda a, b: b["report"].pop("customer_latency"), "k6_reports"),
    (lambda a, b: b["report"]["customer_latency"].update(k6_span=[0.0, 1.0]), "customers_through_churn[b]"),
    (lambda a, b: _gp(b).pop("steady"), "measured[steady]"),
    (lambda a, b: _gp(a)["churn"]["half_sd"].update(p95_ms=None), "spread[churn].p95_ms"),
    # k6 stopped inside the 3rd of the 6 churn steps: the LAST step is what counts
    (lambda a, b: b["report"]["customer_latency"]["k6_span"].__setitem__(1, _churn_mid(b, 2)),
     "customers_through_churn[b]"),
    (lambda a, b: b.pop("verdict"), "runs_valid"),
    (lambda a, b: a.update(verdict="WAT"), "runs_valid"),
    (lambda a, b: a["report"]["image_digest"].update(expected=None), "images"),
    # not measurements: a zero / negative / NaN value, an infinite or negative spread
    (lambda a, b: _gpu(a, "storm", p95_ms=0), "spread[storm].p95_ms"),
    (lambda a, b: _gpu(b, "storm", p95_ms=-5.0), "spread[storm].p95_ms"),
    (lambda a, b: _gpu(b, "storm", req_s=float("nan")), "spread[storm].req_s"),
    (lambda a, b: _gp(a)["storm"]["half_sd"].update(p95_ms=float("inf")), "spread[storm].p95_ms"),
    (lambda a, b: _gp(a)["storm"]["half_sd"].update(p99_ms=-0.1), "spread[storm].p99_ms"),
    (lambda a, b: _gpu(a, "storm", p99_ms=True), "spread[storm].p99_ms"),
    # option A: storm and churn must be there to judge
    (lambda a, b: _gpu(a, "churn", n=0), "judged[churn]"),
    (lambda a, b: _gp(a).pop("storm"), "judged[storm]"),
])
def test_a_pair_that_cannot_be_compared_is_invalid(va, breaks, check):
    a, b = _ab(va)
    breaks(a, b)
    p = _pair(a, b)
    assert p["verdict"] == "INVALID"
    assert [c["check"] for c in p["validity"] if c["result"] == "FAIL"] == [f"customer_latency.pair.{check}"]


def test_an_a_with_no_judged_calls_is_invalid(va):
    a, b = _ab(va)
    for s in _gp(a).values():
        s["n"] = 0
    p = _pair(a, b)
    assert p["verdict"] == "INVALID" and not p["checks"]
    assert _failed(p) == {"customer_latency.pair.judged[storm]", "customer_latency.pair.judged[churn]"}


def test_steady_is_judged_only_when_a_has_it(va):
    a, b = _ab(va)
    _gp(a).pop("steady")
    p = _pair(a, b)
    assert p["verdict"] == "PASS" and p["method"]["judged"] == ["storm", "churn"] and p["method"]["cells"] == 6


def _churn_mid(v, i):
    w = [w for w in v["report"]["customer_latency"]["windows"] if w[0].startswith("churn:")][i]
    return (w[1] + w[2]) / 2


def _ab(va):
    b = copy.deepcopy(va)
    b["report"]["image_digest"] = {"expected": DIG_B}
    return copy.deepcopy(va), b


def _failed(p):
    return {c["check"] for c in p["validity"] if c["result"] == "FAIL"}


def test_the_launch_digests_are_the_runs(va):
    a, b = _ab(va)
    assert _pair(a, b, expect_a=DIG_A, expect_b=DIG_B)["verdict"] == "PASS"
    assert _pair(a, b, expect_a=DIG_B, expect_b=DIG_A)["verdict"] == "INVALID"
    # each side on its own: A launched as another image, B right — and the reverse
    assert _failed(_pair(a, b, expect_a="sha256:" + "c" * 64, expect_b=DIG_B)) == {"customer_latency.pair.images"}
    assert _failed(_pair(a, b, expect_a=DIG_A, expect_b="sha256:" + "c" * 64)) == {"customer_latency.pair.images"}


def test_a_run_that_failed_as_tooling_makes_the_pair_invalid(va):
    a, b = _ab(va)
    assert _pair(a, b, completed=(True, True))["verdict"] == "PASS"
    assert _failed(_pair(a, b, completed=(True, False))) == {"customer_latency.pair.runs_completed"}
    assert _failed(_pair(a, b, completed=(False, True))) == {"customer_latency.pair.runs_completed"}


def test_invalid_wins_over_fail(va):
    a, b = _ab(va)
    _gp(b)["storm"]["p95_ms"] *= 100
    assert _pair(a, b)["verdict"] == "FAIL"
    b["verdict"] = "INVALID"
    p = _pair(a, b)
    assert p["verdict"] == "INVALID" and any(c["result"] == "FAIL" for c in p["checks"])


def test_a_bad_replica_is_invalid(va):
    a, b = _ab(va)
    for bad in (0.5, float("inf"), -1.0):
        p = _pair(a, b, replica={"phases": {"storm": {"p95_ms": bad}}})
        assert _failed(p) == {"customer_latency.pair.spread[storm].p95_ms"}, bad


def test_every_cell_is_printed_with_its_ratio_and_bar(va):
    a, b = _ab(va)
    p = _pair(a, b)
    judged = [c for c in p["cells"] if c["judged"]]
    assert len(judged) == len(p["checks"]) == 9 and len(p["cells"]) > 9
    for c in p["cells"]:
        assert {"phase", "metric", "a", "b", "ratio", "bar", "within", "replica", "result", "line"} <= set(c)
        assert f"B/A = {c['ratio']}" in c["line"] and c["bar"] in c["line"]


@pytest.mark.parametrize("change, key", [
    (lambda d: d["mem"][0]["cpu_plan"].update(burst_post_s=180.0), "cpu_plan"),
    (lambda d: d.update(browser_sessions=d["browser_sessions"][:4]), "browser_sessions"),
    (lambda d: d["config"].update(users=999), "k6"),
])
def test_a_schedule_key_that_differs_is_invalid(va, tmp_path, capsys, change, key):
    """Through the per-run report (schedule() builds what pair compares):
    the CPU-profile plan, the browser sessions, k6's users."""
    d = copy.deepcopy(P14)
    assert d["mem"][0]["kind"] == "mem_start"
    change(d)
    vb = _run(tmp_path, "sched", capsys, doc=d)
    p = _pair(va, vb)
    assert p["verdict"] == "INVALID" and "customer_latency.pair.schedules_match" in _failed(p)
    sm = next(c for c in p["validity"] if c["check"] == "customer_latency.pair.schedules_match")
    assert list(sm["value"]) == [key]


def test_pair_cli_never_reads_a_crash_as_fail(va, tmp_path):
    """A 0 in A (INVALID, not ZeroDivisionError) and a malformed verdict (an
    exception inside pair): both exit 3 = INVALID, never 1 = FAIL."""
    a, b = _ab(va)
    _gp(a)["storm"]["p95_ms"] = 0
    (tmp_path / "a.json").write_text(json.dumps(a))
    (tmp_path / "b.json").write_text(json.dumps(b))
    (tmp_path / "bad.json").write_text(json.dumps(dict(b, report=dict(b["report"], customer_latency={"phases": 7}))))
    for bfile in ("b.json", "bad.json"):
        p = subprocess.run(["python3", "-m", "bench.custlat", "pair", "--a", str(tmp_path / "a.json"),
                            "--b", str(tmp_path / bfile)], cwd=BENCH, capture_output=True, text=True,
                           check=False)
        assert p.returncode == 3, (bfile, p.stderr[-500:])
        assert json.loads(p.stdout)["verdict"] == "INVALID"


def test_run14_itself_is_not_a_paired_run(tmp_path, capsys):
    """Run 14's k6 stopped inside the first churn step: as either side of a
    pair it is INVALID — its later steps had no customers."""
    v = _run(tmp_path, "r14", capsys, digest=DIG_A, doc=R14)
    b = copy.deepcopy(v)
    b["report"]["image_digest"] = {"expected": DIG_B}
    p = _pair(v, b)
    assert p["verdict"] == "INVALID"
    assert {c["check"] for c in p["validity"] if c["result"] == "FAIL"} == {
        "customer_latency.pair.customers_through_churn[a]", "customer_latency.pair.customers_through_churn[b]"}


def test_pair_cli_missing_verdict_is_invalid(va, tmp_path):
    (tmp_path / "a.json").write_text(json.dumps(va))
    p = subprocess.run(["python3", "-m", "bench.custlat", "pair", "--a", str(tmp_path / "a.json"),
                        "--b", str(tmp_path / "nope.json")], cwd=BENCH, capture_output=True, text=True,
                       check=False)
    assert p.returncode == 3 and json.loads(p.stdout)["verdict"] == "INVALID"


def test_the_committed_replica_is_runs_12_and_13():
    """Over the gate's phases: runs 12 / 13 had customers only in their first
    churn step, so their pooled churn IS that step."""
    assert set(REPLICA["phases"]) == {"storm", "burst", "churn"}
    assert [x["dir"] for x in REPLICA["provenance"]] == ["snowplow-bench-run12-662515cb",
                                                         "snowplow-bench-run13-662515cb"]
    assert all(r >= 1.0 for ph in REPLICA["phases"].values() for r in ph.values())
    a = {"gate_phases": {"storm": {"n": 1, "p95_ms": 110.0, "p99_ms": 100.0, "req_s": 0}}}
    b = {"gate_phases": {"storm": {"n": 1, "p95_ms": 100.0, "p99_ms": 125.0, "req_s": 0}}}
    assert custlat.replica_ratios(a, b, [])["phases"] == {"storm": {"p95_ms": 1.1, "p99_ms": 1.25}}
    # from the gate's (pooled) phases, never the per-step ones
    step = {"n": 1, "p95_ms": 300.0, "p99_ms": 300.0, "req_s": 1.0}
    a["phases"], b["phases"] = {"churn:list_dep@1": step}, {"churn:list_dep@1": step}
    assert set(custlat.replica_ratios(a, b, [])["phases"]) == {"storm"}


def test_half_sd_keeps_the_time_profile():
    # a ramp, the same in every pair of blocks: no sampling spread at all
    ramp = [([float(10 * (k // 2) + 1)] * 50, 10.0) for k in range(40)]
    assert custlat.half_sd(ramp, "p95_ms") == 0.0
    assert custlat.half_sd(ramp, "req_s") == 0.0

    # iid calls: the estimate from ONE run is the spread of many runs' p95
    def run(seed):
        rnd = random.Random(seed)
        return [([rnd.lognormvariate(0, 1) for _ in range(50)], 10.0) for _ in range(40)]
    true = statistics.pstdev(math.log(custlat._stat(run(s), "p95_ms")) for s in range(300))
    for seed in (1000, 1001, 1002):
        est = custlat.half_sd(run(seed), "p95_ms")
        assert 0.6 * true < est < 1.5 * true, (seed, est, true)
    assert custlat.half_sd(ramp[:3], "p95_ms") is None             # one pair: no estimate


def test_seconds_split_by_precedence():
    wins = [("storm", float("-inf"), 100.0), ("burst", 90.0, 150.0)]
    assert custlat._seconds(80, 120, wins) == {"storm": 20.0, "burst": 20.0, "steady": 0.0}
    assert custlat._seconds(140, 160, wins) == {"storm": 0.0, "burst": 10.0, "steady": 10.0}
    assert custlat._phase(95, wins) == "storm" and custlat._phase(150, wins) == "steady"


def test_refresher_restart_and_cpu_pods_sum():
    mem = [{"kind": "mem", "t": 0, "num_gc": 5, "extra": {custlat.REFRESHER_KEY: 10}},
           {"kind": "mem", "t": 10, "num_gc": 6, "extra": {custlat.REFRESHER_KEY: 30}},
           {"kind": "mem", "t": 20, "num_gc": 1, "extra": {custlat.REFRESHER_KEY: 4}},
           {"kind": "mem", "t": 40, "num_gc": 2, "extra": {custlat.REFRESHER_KEY: 10}}]   # after k6
    r = custlat.refresher_per_phase(mem, [], [0, 20])
    assert r["restarts"] == 1
    assert r["by_phase"]["steady"] == {"with_customers": {"completed": 24, "seconds": 20.0, "per_s": 1.2},
                                       "whole_run": {"completed": 30, "seconds": 40.0, "per_s": 0.75}}
    assert custlat.refresher_per_phase([{"kind": "mem", "t": 0}], [], [0, 1])["status"] == \
        load606.REPORT_ABSENT
    tops = [{"kind": "top", "t": 5, "raw": "snowplow-a 300m 1Gi\nsnowplow-b 200m 900Mi"},
            {"kind": "top", "t": 50, "raw": "snowplow-a 999m 1Gi"}]
    c = custlat.cpu_per_phase(tops, [], [0, 20])["by_phase"]["steady"]
    assert c["with_customers"] == {"samples": 1, "median_m": 500, "p90_m": 500, "max_m": 500}
    assert c["whole_run"] == {"samples": 2, "median_m": 749.5, "p90_m": 949, "max_m": 999}


def test_clicks_vs_open_schedule():
    o = {"summary": {"config": {"thinkS": 10, "durationS": 35}},
         "times": {"arrivals": [0.0, 5.0], "clicks": [11.0, 21.0, 31.0, 16.0]}}
    c = custlat.clicks_vs_schedule(o, [("storm", float("-inf"), 12.0)])
    # open schedule: user 0 at 10, 20, 30; user 1 at 15, 25 (35 is the end)
    assert c["storm"] == {"clicks": 1, "open_schedule": 1, "achieved_share": 1.0}
    assert c["steady"] == {"clicks": 3, "open_schedule": 4, "achieved_share": 0.75}


def test_cli_evaluate_feeds_churn_records_to_the_phase_reports(tmp_path, capsys):
    """The per-phase reports (#855, #877) read churn windows from fresh606's
    churn records; evaluate's CLI used to hand them only the probe samples, so
    every churn second came out as "steady" (run 14's verdict.json)."""
    (tmp_path / "mem.jsonl").write_text("".join(json.dumps(r) + "\n" for r in _mem()))
    (tmp_path / "fresh.jsonl").write_text("".join(json.dumps(r) + "\n" for r in FRESH))
    capsys.readouterr()
    load606.main(["evaluate", str(tmp_path / "mem.jsonl"), str(tmp_path / "fresh.jsonl")])
    v = json.loads(capsys.readouterr().out)
    r = v["report"]["list_narrow_shadow_report_only"]
    assert r["status"] == "present" and "churn:list_dep@10" in r["by_phase"], list(r["by_phase"])
    assert not [w for w in v["report"]["report_warnings"] if w.startswith(LN_KEY + " ")]
    # the freshness gate still reads the probe samples only
    assert [c["check"] for c in v["checks"] if c["check"].startswith("fresh_p95_ms")] == \
        ["fresh_p95_ms[list_dep@10]"]


# ─── run606.sh K6_SPAN ─────────────────────────────────────────────────────

def _run606(env_extra):
    env = dict(os.environ, PYTHONPATH=str(BENCH), RUN_NAME="snowplow-bench-r15lat", INSTALLER_VERSION="0.3.422",
               SNOWPLOW_TAG="1.12.46", BENCH_606_DRY_RUN="1",
               EXPECTED_IMAGE_DIGEST="sha256:" + "0" * 64, **env_extra)
    for k in [k for k, v in env.items() if v is None]:
        env.pop(k)
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=env, capture_output=True,
                          text=True, timeout=120, check=False)


def test_k6_span_churn_keeps_customers_on_through_every_step(tmp_path):
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o"), "K6_SPAN": "churn"})
    assert p.returncode == 0, p.stderr[-2000:]
    log = (tmp_path / "o" / "run.log").read_text()
    # THINK_S 60 + 2 modes x 3 rates x (60 x (30 + 5) x 1.25 + 60)
    assert "driver 17070" in log                                  # DRIVE_S + the login budget
    assert "K6_SPAN=churn, DRIVE_S=16170 s, composite burst at 900 s" in log   # the burst stays
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o2")})
    log = (tmp_path / "o2" / "run.log").read_text()
    assert p.returncode == 0 and "K6_SPAN=drive, DRIVE_S=1800 s, composite burst at 900 s" in log


@pytest.mark.parametrize("env, why", [
    ({"K6_SPAN": "churn", "DRIVE_S": "3600"}, "K6_SPAN=churn derives DRIVE_S"),
    ({"K6_SPAN": "churn", "LOAD_DRIVER": "load606"}, "K6_SPAN=churn keeps k6's customers on"),
    ({"K6_SPAN": "always"}, "K6_SPAN must be drive or churn"),
])
def test_k6_span_refusals(tmp_path, env, why):
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o"), **env})
    assert p.returncode == 2 and why in p.stderr, p.stderr[-1000:]


# ─── run606_pair.sh ────────────────────────────────────────────────────────

# A run606.sh stand-in with run606's trap shape: `trap teardown EXIT`, and
# INT/TERM/HUP -> exit 130 (-> teardown). It records its calls, its signal and
# its teardown in $FAKE/calls; FAKE/<RUN_NAME>.{dryrc,sleep,verdict.json,rc,
# incomplete} steer it. Without .incomplete it logs "run complete" (run606's
# last line after its evaluate step), then exits .rc.
FAKE_RUN606 = r"""#!/usr/bin/env bash
set -Eeuo pipefail
echo "$RUN_NAME dry=${BENCH_606_DRY_RUN:-0} span=$K6_SPAN driver=$LOAD_DRIVER go=${BENCH_606_GO:-} tag=$SNOWPLOW_TAG digest=$EXPECTED_IMAGE_DIGEST pid=$$" >> "$FAKE/calls"
SLEEP_PID=""
teardown() { local rc=$?; trap - EXIT; [[ -n "$SLEEP_PID" ]] && kill "$SLEEP_PID" 2>/dev/null; echo "$RUN_NAME teardown rc=$rc" >> "$FAKE/calls"; exit "$rc"; }
trap teardown EXIT
on_signal() { echo "$RUN_NAME signal" >> "$FAKE/calls"; exit 130; }
trap on_signal INT TERM HUP
mkdir -p "$OUT"
if [[ "${BENCH_606_DRY_RUN:-0}" == "1" ]]; then exit "$(cat "$FAKE/$RUN_NAME.dryrc" 2>/dev/null || echo 0)"; fi
echo "=== step measure ===" >> "$OUT/run.log"
if [[ -f "$FAKE/$RUN_NAME.sleep" ]]; then sleep "$(cat "$FAKE/$RUN_NAME.sleep")" & SLEEP_PID=$!; wait "$SLEEP_PID"; SLEEP_PID=""; fi
[[ -f "$FAKE/$RUN_NAME.verdict.json" ]] && cp "$FAKE/$RUN_NAME.verdict.json" "$OUT/verdict.json"
[[ -f "$FAKE/$RUN_NAME.incomplete" ]] || echo "run complete: $OUT" >> "$OUT/run.log"
exit "$(cat "$FAKE/$RUN_NAME.rc" 2>/dev/null || echo 0)"
"""
NA, NB = "snowplow-bench-r15a", "snowplow-bench-r15b"
# the region's quota read (#895), as gcloud returns it; FAKE_SSD_USAGE steers it
FAKE_QUOTA_GCLOUD = (
    "#!/usr/bin/env python3\nimport json, os, sys\n"
    "if sys.argv[1:4] == ['compute', 'regions', 'describe']:\n"
    "    print(json.dumps({'name': sys.argv[4], 'quotas': [{'metric': 'SSD_TOTAL_GB', 'limit': 500.0,\n"
    "                      'usage': float(os.environ.get('FAKE_SSD_USAGE', '333'))}]}))\n"
    "    sys.exit(0)\n"
    "sys.exit(9)\n")
# the bash the script claims to support (macOS /bin/bash 3.2) and a current one
BASHES = [b for b in dict.fromkeys(["/bin/bash", shutil.which("bash")]) if b and os.path.exists(b)]


def _bash_major(b):
    out = subprocess.run([b, "-c", "echo ${BASH_VERSINFO[0]}"], capture_output=True, text=True, check=True)
    return int(out.stdout.strip())


def _git_repo(d: Path):
    for c in (["init", "-q"], ["add", "-A"],
              ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x"]):
        subprocess.run(["git", *c], cwd=d, check=True, capture_output=True)


@pytest.fixture
def pairbed(tmp_path):
    """A git checkout holding the launcher, the replica file and a stand-in
    run606.sh (the launcher runs its own directory's run606.sh)."""
    repo = tmp_path / "bench"
    repo.mkdir()
    shutil.copy(BENCH / "run606_pair.sh", repo)
    shutil.copy(BENCH / "customer_latency_replica_runs12_13.json", repo)
    (repo / "run606.sh").write_text(FAKE_RUN606)
    fake = tmp_path / "fake"
    fake.mkdir()
    _git_repo(repo)
    # #895: the launcher reads the region's SSD quota; never the real gcloud
    fbin = tmp_path / "fakebin"
    fbin.mkdir()
    (fbin / "gcloud").write_text(FAKE_QUOTA_GCLOUD)
    (fbin / "gcloud").chmod(0o755)
    today = datetime.now(UTC).strftime("%Y%m%d")
    env = {k: v for k, v in os.environ.items()
           if k not in ("RUN_NAME", "SNOWPLOW_TAG", "EXPECTED_IMAGE_DIGEST", "BENCH_606_GO", "OUT",
                        "DRIVE_S", "K6_SPAN", "LOAD_DRIVER", "BENCH_606_DRY_RUN")}
    env.update(PATH=f"{fbin}{os.pathsep}{env['PATH']}", FAKE_SSD_USAGE="333",
               PYTHONPATH=str(BENCH), FAKE=str(fake), PAIR_OUT=str(tmp_path / "pair"),
               OUT_A=str(tmp_path / "A"), OUT_B=str(tmp_path / "B"), PAIR_POLL_S="0.2",
               RUN_NAME_A=NA, RUN_NAME_B=NB, SNOWPLOW_TAG_A="1.12.54", SNOWPLOW_TAG_B="1.12.55",
               EXPECTED_IMAGE_DIGEST_A=DIG_A, EXPECTED_IMAGE_DIGEST_B=DIG_B,
               BENCH_606_GO_A=f"{today}-{NA}", BENCH_606_GO_B=f"{today}-{NB}")
    return repo, fake, env, tmp_path


def _launch(repo, env, bash="bash", **over):
    e = dict(env, **over)
    for k in [k for k, v in e.items() if v is None]:
        e.pop(k)
    return subprocess.run([bash, str(repo / "run606_pair.sh")], env=e, capture_output=True,
                          text=True, timeout=120, check=False)


def _calls(fake):
    p = fake / "calls"
    return p.read_text().splitlines() if p.exists() else []


def _verdicts(fake, va, **b_over):
    a, b = _ab(va)
    b.update(b_over)
    (fake / f"{NA}.verdict.json").write_text(json.dumps(a))
    (fake / f"{NB}.verdict.json").write_text(json.dumps(b))
    return a, b


def test_pair_runs_both_after_both_dry_passes_and_writes_the_paired_verdict(pairbed, va):
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    p = _launch(repo, env)
    assert p.returncode == 0, p.stderr[-3000:]
    calls = [c for c in _calls(fake) if " dry=" in c]
    assert [c.split()[1] for c in calls] == ["dry=1", "dry=1", "dry=0", "dry=0"]
    assert sorted(c.split()[0] for c in calls[2:]) == [NA, NB]
    assert all("span=churn driver=k6" in c for c in calls)
    real = calls[2:]
    assert any(f"go={env['BENCH_606_GO_A']} tag=1.12.54 digest={DIG_A}" in c for c in real)
    assert any(f"go={env['BENCH_606_GO_B']} tag=1.12.55 digest={DIG_B}" in c for c in real)
    v = json.loads((tmp / "pair" / "paired-verdict.json").read_text())
    assert v["verdict"] == "PASS" and v["method"]["replica"] == REPLICA["provenance"]
    assert next(c for c in v["validity"] if c["check"].endswith("runs_completed"))["value"] == {"a": True, "b": True}
    pj = json.loads((tmp / "pair" / "pair.json").read_text())
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True,
                         check=True).stdout.strip()
    assert pj["bench_sha"] == sha and pj["a"]["digest"] == DIG_A and pj["b"]["tag"] == "1.12.55"
    log = (tmp / "pair" / "pair.log").read_text()
    assert "paired verdict exit=0" in log
    # every cell, with its ratio and its bar
    assert len([ln for ln in log.splitlines() if " B/A = " in ln]) == len(v["cells"]) > 0
    assert "PASS storm p95_ms: B/A = 1.0 (<= " in log


def test_pair_slower_b_exits_fail(pairbed, va):
    repo, fake, env, tmp = pairbed
    _, b = _verdicts(fake, va)
    b["report"]["customer_latency"]["gate_phases"]["storm"]["p95_ms"] *= 3
    (fake / f"{NB}.verdict.json").write_text(json.dumps(b))
    p = _launch(repo, env)
    assert p.returncode == 1, p.stderr[-3000:]
    assert "FAIL storm p95_ms: B/A = 3.0" in (tmp / "pair" / "pair.log").read_text()


def test_pair_a_run_on_another_image_is_invalid(pairbed, va):
    repo, fake, env, tmp = pairbed
    _, b = _verdicts(fake, va)
    b["report"]["image_digest"] = {"expected": "sha256:" + "c" * 64}   # not the launched B
    (fake / f"{NB}.verdict.json").write_text(json.dumps(b))
    p = _launch(repo, env)
    assert p.returncode == 3, p.stderr[-3000:]
    assert "INVALID: customer_latency.pair.images" in (tmp / "pair" / "pair.log").read_text()


@pytest.mark.parametrize("bash", BASHES)
def test_a_run_that_completed_never_stops_its_partner(pairbed, va, bash):
    """A completed (its evaluate step ran) and then exited 1 — e.g. its
    gke-down failed: a measurement, not a tooling failure. B runs to its end."""
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    (fake / f"{NA}.rc").write_text("1")
    (fake / f"{NB}.sleep").write_text("2")
    p = _launch(repo, env, bash=bash)
    assert p.returncode == 0, p.stderr[-3000:]
    calls = _calls(fake)
    assert f"{NB} signal" not in calls and f"{NB} teardown rc=0" in calls
    assert "failed as tooling" not in (tmp / "pair" / "pair.log").read_text()


@pytest.mark.parametrize("bash", BASHES)
def test_a_run_that_failed_as_tooling_stops_its_partner_and_its_trap_runs(pairbed, va, bash):
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    (fake / f"{NA}.rc").write_text("2")
    (fake / f"{NA}.incomplete").write_text("")
    (fake / f"{NB}.sleep").write_text("60")
    t0 = datetime.now(UTC)
    p = _launch(repo, env, bash=bash)
    assert (datetime.now(UTC) - t0).total_seconds() < 30, "B ran on: the TERM never reached it"
    assert p.returncode == 3, p.stderr[-3000:]
    calls = _calls(fake)
    # the TERM reached run606's own shell: its trap ran, then its teardown
    assert f"{NB} signal" in calls and f"{NB} teardown rc=130" in calls
    pid_b = next(c for c in calls if c.startswith(f"{NB} dry=0")).rsplit("pid=", 1)[1]
    log = (tmp / "pair" / "pair.log").read_text()
    assert f"run B pid {pid_b}" in log                       # $! is run606's shell, not a subshell
    assert "run A failed as tooling: stopping run B" in log
    assert "INVALID: customer_latency.pair.runs_completed" in log


@pytest.mark.parametrize("bash", BASHES)
@pytest.mark.parametrize("how", ["ctrl-c to the process group", "TERM to the launcher"])
def test_a_ctrl_c_tears_both_runs_down(pairbed, va, bash, how):
    import signal
    import time
    repo, fake, env, _ = pairbed
    _verdicts(fake, va)
    for n in (NA, NB):
        (fake / f"{n}.sleep").write_text("60")
    proc = subprocess.Popen([bash, str(repo / "run606_pair.sh")], env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, start_new_session=True)
    deadline = time.time() + 30
    while time.time() < deadline and len([c for c in _calls(fake) if " dry=0 " in c]) < 2:
        time.sleep(0.1)
    time.sleep(0.5)                                        # both inside their measure
    if how.startswith("ctrl-c"):
        os.killpg(proc.pid, signal.SIGINT)                 # what a terminal Ctrl-C sends
    else:
        proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=30)
    assert proc.returncode == 130, err[-2000:]
    calls = _calls(fake)
    for n in (NA, NB):
        assert f"{n} teardown rc=130" in calls, (n, calls)


def test_a_refused_dry_pass_starts_nothing(pairbed):
    repo, fake, env, _ = pairbed
    (fake / f"{NB}.dryrc").write_text("2")
    p = _launch(repo, env)
    assert p.returncode == 2 and "run B's dry pass was refused" in p.stderr
    assert not [c for c in _calls(fake) if "dry=0" in c]


@pytest.mark.parametrize("over, why", [
    ({"RUN_NAME_B": None}, "RUN_NAME_B is required"),
    ({"RUN_NAME_B": NA}, "RUN_NAME_A and RUN_NAME_B are the same"),
    ({"EXPECTED_IMAGE_DIGEST_B": DIG_A}, "the two digests are the same image"),
    ({"RUN_NAME": NA}, "RUN_NAME is per run"),
    ({"DRIVE_S": "3600"}, "DRIVE_S is derived by K6_SPAN=churn"),
    ({"K6_SPAN": "drive"}, "a pair runs K6_SPAN=churn"),
    ({"LOAD_DRIVER": "load606"}, "a pair compares k6's customers"),
    ({"BENCH_606_GO_B": "20200101-x"}, "BENCH_606_GO_B must be"),
    ({"OUT_B": "SAME"}, "OUT_A and OUT_B are the same directory"),
])
def test_pair_refusals_before_anything_runs(pairbed, over, why):
    repo, fake, env, _ = pairbed
    if over.get("OUT_B") == "SAME":
        over = {"OUT_B": env["OUT_A"]}
    p = _launch(repo, env, **over)
    assert p.returncode == 2 and why in p.stderr, p.stderr[-1000:]
    assert _calls(fake) == []


@pytest.mark.parametrize("left", ["verdict.json", "run.log"])
def test_pair_refuses_an_out_that_holds_an_earlier_run(pairbed, left):
    repo, fake, env, _ = pairbed
    Path(env["OUT_B"]).mkdir(parents=True)
    (Path(env["OUT_B"]) / left).write_text("{}")
    p = _launch(repo, env)
    assert p.returncode == 2 and f"OUT_B ({env['OUT_B']}) already holds a run ({left})" in p.stderr
    assert _calls(fake) == []


def test_pair_refuses_an_uncommitted_bench(pairbed):
    repo, _, env, _ = pairbed
    (repo / "run606.sh").write_text(FAKE_RUN606 + "\n# edited\n")
    p = _launch(repo, env)
    assert p.returncode == 2 and "uncommitted changes under the bench" in p.stderr
    p = _launch(repo, env, BENCH_606_DRY_RUN="1")                 # a dry pass may run on a working tree
    assert p.returncode == 0 and "DRY: both runs would start now" in p.stderr


def test_bash_versions_under_test():
    """The stop / Ctrl-C arms run under macOS's /bin/bash 3.2 and a bash >= 4
    whenever both exist (they do on the macOS this bench is driven from)."""
    majors = {_bash_major(b) for b in BASHES}
    if os.path.exists("/bin/bash") and _bash_major("/bin/bash") == 3:
        assert 3 in majors
    assert majors


def test_pair_dry_run_with_the_real_run606(tmp_path):
    """Both real run606.sh dry passes (every refusal of each run), from a git
    checkout of this bench (copied: the gate's archive is not a checkout);
    nothing starts."""
    repo = tmp_path / "bench"
    shutil.copytree(BENCH, repo, ignore=shutil.ignore_patterns("__pycache__", ".ruff_cache", ".pytest_cache"))
    _git_repo(repo)
    env = {k: v for k, v in os.environ.items()
           if k not in ("RUN_NAME", "SNOWPLOW_TAG", "EXPECTED_IMAGE_DIGEST", "BENCH_606_GO", "OUT",
                        "DRIVE_S", "K6_SPAN", "LOAD_DRIVER")}
    env.update(PYTHONPATH=str(repo), HOME=str(tmp_path), PAIR_OUT=str(tmp_path / "pair"),
               OUT_A=str(tmp_path / "A"), OUT_B=str(tmp_path / "B"), BENCH_606_DRY_RUN="1",
               INSTALLER_VERSION="0.3.422", RUN_NAME_A=NA, RUN_NAME_B=NB,
               SNOWPLOW_TAG_A="1.12.54", SNOWPLOW_TAG_B="1.12.55",
               EXPECTED_IMAGE_DIGEST_A=DIG_A, EXPECTED_IMAGE_DIGEST_B=DIG_B)
    p = subprocess.run(["bash", str(repo / "run606_pair.sh")], env=env, capture_output=True, text=True,
                       timeout=300, check=False)
    assert p.returncode == 0, p.stderr[-3000:]
    for x in ("A", "B"):
        log = (tmp_path / "pair" / f"dry-{x}" / "run.log").read_text()
        assert "K6_SPAN=churn, DRIVE_S=16170 s" in log
    assert "DRY: both runs would start now" in p.stderr


# ─── run606.sh's evaluate step: a verdict is a result ─────────────────────

def _s_evaluate_body():
    src = (BENCH / "run606.sh").read_text()
    i = src.index("s_evaluate() {")
    j = src.index("\n}\n", i) + 3
    return src[i:j]


@pytest.mark.parametrize("verdict, warnings, invalid", [
    ("FAIL", [], []), ("INVALID", [], ["x: y"]), ("PASS", ["w"], []), ("PASS", [], [])])
def test_run606_evaluate_step_succeeds_whatever_the_verdict(tmp_path, verdict, warnings, invalid):
    """run606.sh's own s_evaluate, under set -e, against a stand-in evaluate
    that exits with the verdict's code: the step returns 0 (runs 12 and 14
    exited 1 here when no warning was logged)."""
    out = tmp_path / "out"
    out.mkdir()
    (out / "mem-x.jsonl").write_text("{}\n")
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    doc = json.dumps({"verdict": verdict, "invalid_reasons": invalid, "report": {"report_warnings": warnings}})
    rc = {"PASS": 0, "FAIL": 1, "INVALID": 3}[verdict]
    real = shutil.which("python3")
    (bin_ / "python3").write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1 $2 $3" == "-m bench.load606 evaluate" ]]; then echo {json.dumps(doc)}; exit {rc}; fi\n'
        f'exec {real} "$@"\n')
    (bin_ / "python3").chmod(0o755)
    script = ("set -Eeuo pipefail\nlog() { echo \"$*\" >> \"$OUT/run.log\"; }\n"
              "DRY=0 RUN8_REPORT=1 LOAD_DRIVER=k6 VIEWERS=1 BROWSER_SESSIONS=5 EXPECTED_IMAGE_DIGEST=x\n"
              + _s_evaluate_body() + "\ns_evaluate\necho step-ok\n")
    p = subprocess.run(["bash", "-c", script], env=dict(os.environ, OUT=str(out),
                       PATH=f"{bin_}{os.pathsep}{os.environ['PATH']}"),
                       capture_output=True, text=True, timeout=60, check=False)
    assert p.returncode == 0 and "step-ok" in p.stdout, p.stderr[-2000:]
    assert f"verdict exit={rc}" in (out / "run.log").read_text()
