"""Run 15 (#865): the customer-latency gate — k6's /call p95/p99 and req/s per
phase against run 14, a VERDICT check (bench/custlat.py).

tests/fixtures/customer_latency_run14_every50.json.gz is RECORDED from run
14's artifacts (1.12.54, bench adc2e702): every 50th k6 http_req_duration
point in stream order, the k6 config and browser sessions, mem606's phase
anchors and refresher counter, fresh606's churn records and kubectl top
(pod names replaced). The tests write it back out in run606's $OUT layout
and run `load606 evaluate` on it, as run606.sh does.
"""

from __future__ import annotations

import copy
import gzip
import json
import math
import os
import random
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
COMMITTED = BENCH / "customer_latency_baseline_run14.json"


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "123Z"


def _write_run(d: Path, doc=R14, scale=lambda t, v: v, drop=lambda t: False, config=None) -> list[str]:
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
    (d / "k6" / "k6-summary.json").write_text(json.dumps({"config": config or doc["config"]}))
    (d / "browser" / "browser-summary.json").write_text(json.dumps({"sessions": doc["browser_sessions"]}))
    files = {"mem-x.jsonl": doc["mem"], "fresh-x.jsonl": doc["churns"], "top.jsonl": doc["tops"]}
    for name, rows in files.items():
        (d / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
    return [str(d / n) for n in files]


def _evaluate(d: Path, files, baseline: Path | None, capsys) -> dict:
    args = ["evaluate", *files, "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
            "--browser-sessions", "5", "--no-viewers"]
    if baseline is not None:
        args += ["--latency-baseline", str(baseline)]
    capsys.readouterr()
    load606.main(args)
    return json.loads(capsys.readouterr().out)


def _baseline_of(d: Path, out: Path, name="run14") -> Path:
    rep = custlat.report_from_dir(str(d))
    out.write_text(json.dumps(custlat.derive_baseline(name, rep, {name: rep}, (rep, rep), {})))
    return out


@pytest.fixture(scope="module")
def r14(tmp_path_factory):
    d = tmp_path_factory.mktemp("run14")
    files = _write_run(d)
    return d, files, _baseline_of(d, d / "baseline.json")


def _lat(v):
    return [c for c in v["checks"] + v["validity"] if c["check"].startswith("customer_latency.")]


def test_run14_against_itself_passes_and_keeps_its_verdict(r14, capsys):
    d, files, base = r14
    plain = _evaluate(d, files, None, capsys)
    gated = _evaluate(d, files, base, capsys)
    cells = [c for c in gated["checks"] if c["check"].startswith("customer_latency.vs_run14[")]
    assert len(cells) == 9 and {c["result"] for c in cells} == {"PASS"}
    assert {c["value"]["ratio"] for c in cells} == {1.0}
    assert gated["verdict"] == plain["verdict"]
    assert gated["invalid_reasons"] == plain["invalid_reasons"]
    # apart from the new rows, the verdict document is unchanged
    strip = copy.deepcopy(gated)
    strip["checks"] = [c for c in gated["checks"] if c not in _lat(gated)]
    strip["validity"] = [c for c in gated["validity"] if c not in _lat(gated)]
    assert strip == plain
    assert _lat(plain) == []                    # no baseline: no gate, report only
    assert [(c["check"], c["result"]) for c in gated["validity"] if c in _lat(gated)] == \
        [("customer_latency.schedule_matches_run14", "PASS")]


def test_report_phases_refresher_and_cpu(r14, capsys):
    d, files, _ = r14
    rep = _evaluate(d, files, None, capsys)["report"]["customer_latency"]
    st = rep["stats"]["phases"]
    assert set(st) == {"storm", "burst", "rest"}
    assert sum(p["n"] for p in st.values()) == len(R14["calls"]) == rep["stats"]["calls"]
    # run 14's windows: the CPU plan (storm 600 s, burst 900 - 60 .. 900 + 240 s from the anchor)
    assert st["burst"]["seconds"] == 300.0
    assert st["storm"]["p95_ms"] > 10 * st["rest"]["p95_ms"]           # the login storm
    for p in st.values():
        assert p["p99_ms"] > p["p95_ms"]
        assert p["req_s"] == round(p["n"] / p["seconds"], 2)
    assert rep["refresher_completions"]["status"] == "present"
    assert rep["refresher_completions"]["by_phase"]["rest"]["per_s"] > 0
    # run 14's churn steps after k6 stopped carry no calls but do have refresher work
    assert set(rep["stats"]["by_run_phase"]) == {"storm", "burst", "churn:list_dep@1"}
    assert "churn:list_dep_status@50" in rep["refresher_completions"]["by_run_phase"]
    assert rep["snowplow_cpu"]["by_phase"]["storm"]["samples"] > 0
    assert rep["schedule"]["browser_sessions"] == 5
    assert rep["schedule"]["churn"][0] == ["list_dep@1", 1.0]


def test_a_slower_run_fails_just_past_the_spread(r14, tmp_path, capsys):
    """Every call of the storm k times slower: FAIL just past the storm's
    tolerance, PASS just inside it; the other phases untouched."""
    _, _, base = r14
    b = json.loads(base.read_text())["phases"]["storm"]
    wins = custlat.windows(R14["mem"], R14["churns"])
    tol = (b["p95_ms"]["tolerance"], b["p99_ms"]["tolerance"])
    for k, want in ((max(tol) * 1.02, "FAIL"), (min(tol) * 0.98, "PASS")):
        d = tmp_path / f"slow{k}"
        v = _evaluate(d, _write_run(d, scale=lambda t, ms, k=k: ms * k if custlat._phase(t, wins) == "storm"
                                    else ms), base, capsys)
        res = {c["check"]: c["result"] for c in v["checks"] if c["check"].startswith("customer_latency.")}
        assert res.pop("customer_latency.vs_run14[storm].p95_ms") == want, k
        assert res.pop("customer_latency.vs_run14[storm].p99_ms") == want, k
        assert set(res.values()) == {"PASS"}             # req/s: same calls, same seconds
        if want == "FAIL":
            assert v["verdict"] in ("FAIL", "INVALID")


def test_fewer_calls_fail_req_s_only_below_the_floor(r14, tmp_path, capsys):
    _, _, base = r14
    b = json.loads(base.read_text())
    floor = 1 / b["phases"]["rest"]["req_s"]["tolerance"]
    wins = custlat.windows(R14["mem"], R14["churns"])
    rest = [(R14["t0"] + off) for off, _ in R14["calls"]
            if custlat._phase(R14["t0"] + off, wins) == "rest"]
    for share, want in ((floor - 0.05, "FAIL"), (min(1.0, floor + 0.05), "PASS")):
        # keep `share` of the rest phase's calls, evenly over its time
        gone = {t for i, t in enumerate(rest) if math.floor((i + 1) * share) == math.floor(i * share)}
        d = tmp_path / f"drop{share}"
        v = _evaluate(d, _write_run(d, drop=lambda t, gone=gone: t in gone), base, capsys)
        c = next(c for c in v["checks"] if c["check"] == "customer_latency.vs_run14[rest].req_s")
        assert c["result"] == want, (share, c)


def test_a_different_schedule_is_invalid(r14, tmp_path, capsys):
    _, _, base = r14
    d = tmp_path / "sched"
    v = _evaluate(d, _write_run(d, config=dict(R14["config"], thinkS=30)), base, capsys)
    c = next(c for c in v["validity"] if c["check"] == "customer_latency.schedule_matches_run14")
    assert c["result"] == "FAIL" and "k6" in c["value"]
    assert v["verdict"] == "INVALID" and any("schedule_matches_run14" in r for r in v["invalid_reasons"])


def test_a_phase_without_calls_is_invalid(r14, tmp_path, capsys):
    _, _, base = r14
    wins = custlat.windows(R14["mem"], R14["churns"])
    d = tmp_path / "noburst"
    v = _evaluate(d, _write_run(d, drop=lambda t: custlat._phase(t, wins) == "burst"), base, capsys)
    c = next(c for c in v["validity"] if c["check"] == "customer_latency.vs_run14[burst].measured")
    assert c["result"] == "FAIL" and v["verdict"] == "INVALID"


def test_a_baseline_without_k6_is_invalid():
    b = json.loads(COMMITTED.read_text())
    v = load606.evaluate([], [], [], latency_baseline=b)
    assert v["verdict"] == "INVALID"
    assert any(r.startswith("customer_latency.k6:") for r in v["invalid_reasons"])


def test_the_committed_baseline_follows_its_method():
    """customer_latency_baseline_run14.json (custlat derive on runs 12/13/14):
    every tolerance is the method's, from the numbers the file records."""
    b = custlat.load_baseline(str(COMMITTED))
    m = b["method"]
    assert b["baseline_run"] == "run14" and m["cells"] == 9
    assert m["z"] == round(NormalDist().inv_cdf(1 - m["family_alpha"] / m["cells"]), 4)
    assert b["provenance"]["run14"]["role"] == ["baseline", "spread"]
    assert b["provenance"]["run12"]["role"] == ["spread", "replica"]
    assert b["provenance"]["run13"]["role"] == ["replica"]
    for p, cells in b["phases"].items():
        for name, c in cells.items():
            within = math.exp(m["z"] * math.sqrt(2) * max(c["within_sd"].values()))
            assert abs(c["within_bound"] - within) < 2e-3 * within, (p, name)
            assert c["tolerance"] == max(c["within_bound"], c["replica_ratio"], 1.0), (p, name)
            assert c["baseline"] == b["baseline_stats"][p][name]
    assert b["schedule"]["k6"] == {k: R14["config"][k] for k in custlat.SCHEDULE_KEYS}


def test_derive_takes_the_larger_spread():
    def rep(p95, sd):
        ph = {"n": 1, "p95_ms": p95, "p99_ms": None, "req_s": None,
              "half_sd": {"p95_ms": sd, "p99_ms": None, "req_s": None}}
        return {"stats": {"phases": {p: dict(ph) for p in custlat.PHASES}}, "schedule": {}}
    z = NormalDist().inv_cdf(1 - 0.05 / 3)
    b = custlat.derive_baseline("x", rep(100, 0.1), {"a": rep(100, 0.1), "b": rep(100, 0.05)},
                                (rep(100, 0), rep(150, 0)), {})
    c = b["phases"]["storm"]["p95_ms"]
    assert b["method"]["cells"] == 3                     # p99/req_s None: not gated
    assert c["within_bound"] == round(math.exp(z * math.sqrt(2) * 0.1), 4)
    assert c["replica_ratio"] == 1.5 and c["tolerance"] == 1.5 and c["set_by"] == "replica"
    b = custlat.derive_baseline("x", rep(100, 0.1), {"a": rep(100, 0.2)}, (rep(110, 0), rep(100, 0)), {})
    c = b["phases"]["storm"]["p95_ms"]
    assert c["tolerance"] == round(math.exp(z * math.sqrt(2) * 0.2), 4) and c["set_by"] == "within"
    assert c["replica_ratio"] == 1.1                     # the ratio's size, either direction


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
    assert custlat._seconds(80, 120, wins) == {"storm": 20.0, "burst": 20.0, "rest": 0.0}
    assert custlat._seconds(140, 160, wins) == {"storm": 0.0, "burst": 10.0, "rest": 10.0}
    assert custlat._phase(95, wins) == "storm" and custlat._phase(150, wins) == "rest"


def test_refresher_restart_and_cpu_pods_sum():
    mem = [{"kind": "mem", "t": 0, "num_gc": 5, "extra": {custlat.REFRESHER_KEY: 10}},
           {"kind": "mem", "t": 10, "num_gc": 6, "extra": {custlat.REFRESHER_KEY: 30}},
           {"kind": "mem", "t": 20, "num_gc": 1, "extra": {custlat.REFRESHER_KEY: 4}},
           {"kind": "mem", "t": 40, "num_gc": 2, "extra": {custlat.REFRESHER_KEY: 10}}]   # after k6
    r = custlat.refresher_per_phase(mem, [], [0, 20])
    assert r["restarts"] == 1 and r["by_phase"]["rest"] == {"completed": 24, "seconds": 20.0, "per_s": 1.2}
    assert r["by_run_phase"]["steady"] == {"completed": 30, "seconds": 40.0, "per_s": 0.75}
    assert custlat.refresher_per_phase([{"kind": "mem", "t": 0}], [], [0, 1])["status"] == \
        load606.REPORT_ABSENT
    tops = [{"kind": "top", "t": 5, "raw": "snowplow-a 300m 1Gi\nsnowplow-b 200m 900Mi"},
            {"kind": "top", "t": 50, "raw": "snowplow-a 999m 1Gi"}]
    c = custlat.cpu_per_phase(tops, [], [0, 20])
    assert c["by_phase"]["rest"] == {"samples": 1, "median_m": 500, "p90_m": 500, "max_m": 500}


def test_clicks_vs_open_schedule():
    o = {"summary": {"config": {"thinkS": 10, "durationS": 35}},
         "times": {"arrivals": [0.0, 5.0], "clicks": [11.0, 21.0, 31.0, 16.0]}}
    c = custlat.clicks_vs_schedule(o, [("storm", float("-inf"), 12.0)])
    # open schedule: user 0 at 10, 20, 30; user 1 at 15, 25 (35 is the end)
    assert c["storm"] == {"clicks": 1, "open_schedule": 1, "achieved_share": 1.0}
    assert c["rest"] == {"clicks": 3, "open_schedule": 4, "achieved_share": 0.75}


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


def _run606(env_extra):
    env = dict(os.environ, PYTHONPATH=str(BENCH), RUN_NAME="snowplow-bench-r15lat", INSTALLER_VERSION="0.3.422",
               SNOWPLOW_TAG="1.12.46", BENCH_606_DRY_RUN="1",
               EXPECTED_IMAGE_DIGEST="sha256:" + "0" * 64, **env_extra)
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=env, capture_output=True,
                          text=True, timeout=120, check=False)


def test_run606_refuses_a_latency_baseline_it_cannot_apply(tmp_path):
    p = _run606({"LOAD_DRIVER": "load606", "LATENCY_BASELINE": str(COMMITTED), "HOME": str(tmp_path),
                 "OUT": str(tmp_path / "o1")})
    assert p.returncode == 2 and "LATENCY_BASELINE gates k6's calls" in p.stderr
    bad = tmp_path / "bad.json"
    bad.write_text("{}")
    p = _run606({"LATENCY_BASELINE": str(bad), "HOME": str(tmp_path), "OUT": str(tmp_path / "o2")})
    assert p.returncode == 2 and "is not a customer_latency_baseline/1 file" in p.stderr


def test_run606_passes_the_baseline_to_evaluate(tmp_path):
    """Off by default (small and kind runs keep their own schedules); run 15
    sets LATENCY_BASELINE, and run.log says which way it went."""
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o"),
                 "LATENCY_BASELINE": "customer_latency_baseline_run14.json"})
    assert p.returncode == 0, p.stderr[-2000:]
    log = (tmp_path / "o" / "run.log").read_text()
    ev = [ln for ln in log.splitlines() if "DRY: python3 -m bench.load606 evaluate" in ln]
    assert len(ev) == 1 and "--latency-baseline customer_latency_baseline_run14.json" in ev[0], ev
    assert "customer-latency gate: customer_latency_baseline_run14.json" in log
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o2")})
    assert p.returncode == 0, p.stderr[-2000:]
    log = (tmp_path / "o2" / "run.log").read_text()
    assert "--latency-baseline" not in log and "customer-latency gate: OFF" in log


def test_run606_refuses_a_schedule_that_is_not_the_baselines(tmp_path):
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o"), "THINK_S": "30",
                 "CHURN_RATES": "1 10", "LATENCY_BASELINE": str(COMMITTED)})
    assert p.returncode == 2, p.stderr[-2000:]
    assert "THINK_S=30 but run14 ran 60" in p.stderr
    assert "FRESH_MODES x CHURN_RATES=" in p.stderr and "is not the latency baseline's" in p.stderr
    assert not [ln for ln in p.stderr.splitlines() if ln.startswith(("USERS=", "DRIVE_S="))]
    p = _run606({"HOME": str(tmp_path), "OUT": str(tmp_path / "o2"), "THINK_S": "30"})
    assert "is not the latency baseline's" not in p.stderr
