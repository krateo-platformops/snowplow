"""#1297: the orchestrator's own host load, an instrument (bench/hostload.py).

Run 24's data step failed on every arm because the laptop ran at a load of ~90 on 8 CPUs, and the
evidence held no host load at all. Now run606.sh (and run606_pair.sh, once for the launch) refuses to
start, before any cloud call, when the 1-minute load exceeds the CPU count, saying why; a quiet run
records the load every 15 s into host-load.jsonl, and evaluate says it with the verdict (INFO, never
judged), with the host's load during every API outage run606's own polls recorded.

The host's reading is stood in for by BENCH_606_HOST_LOAD (the suite's quiet default: conftest).
"""

from __future__ import annotations

import io
import json
import os
import subprocess
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from bench import hostload, load606
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import pairbed  # noqa: F401  (fixture)
from tests.test_run24_aa_replica_nodes_goenv import ROOM_FOR_THREE, SPEC, _launch, _launch_env

LOADED = "90.15 87.55 69.31 8"     # run 24's laptop, as uptime read it


@pytest.mark.parametrize("load1, cpus, ok", [(8.0, 8, True), (8.01, 8, False), (0.1, 0, False)])
def test_the_bar_is_the_cpu_count(load1, cpus, ok):
    assert hostload.check({"load1": load1, "load5": 1, "load15": 1, "cpus": cpus})[0] is ok


def test_a_loaded_orchestrator_is_refused_before_any_cloud_call(harness):  # noqa: F811
    env, state, _ambient, tmp = harness
    env["BENCH_606_HOST_LOAD"] = LOADED
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    assert p.returncode == 2, p.stderr[-3000:]
    assert ("refusing: the orchestrator's 1-minute load 90.15 exceeds its 8 CPUs (5-min 87.55, 15-min 69.31): "
            "the run would measure this machine, not snowplow") in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]
    # nothing is written into OUT that would refuse the same RUN_NAME once the host is quiet
    out = tmp / "out"
    assert not (out / "run.log").exists() and not (out / "host-load.jsonl").exists()


def _dry_env(tmp_path: Path) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("RUN_NAME", "OUT")}
    env.update(BENCH_606_DRY_RUN="1", RUN_NAME="snowplow-bench-1297-dry", INSTALLER_VERSION="0.3.422",
               SNOWPLOW_TAG="1.12.67", EXPECTED_IMAGE_DIGEST="sha256:" + "ab" * 32, OUT=str(tmp_path / "out"),
               PYTHONPATH=str(BENCH_DIR), HOME=str(tmp_path / "home"),
               # installer 0.3.422 pins frontend 1.8.4, older than the login form contract (run 26d):
               # the run names one that carries it, as every k6 run must
               FRONTEND_TAG="1.8.40", EXPECTED_FRONTEND_DIGEST="sha256:" + "1a" * 32)
    (tmp_path / "home").mkdir()
    return env


def test_a_dry_run_says_it_would_refuse_and_goes_on(tmp_path):
    env = {**_dry_env(tmp_path), "BENCH_606_HOST_LOAD": LOADED}
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    assert p.returncode == 0, p.stderr[-3000:]
    assert "host load: DRY: would refuse: the orchestrator's 1-minute load 90.15 exceeds its 8 CPUs" in p.stderr
    assert "DRY: python3 -m bench.hostload record --every-s 15" in p.stderr
    pre = [json.loads(ln) for ln in (tmp_path / "out" / "host-load.jsonl").read_text().splitlines()]
    assert [(r["kind"], r["ok"], r["dry"]) for r in pre] == [("host_load_preflight", False, True)]


def test_the_launch_is_refused_once_for_every_run(pairbed):  # noqa: F811
    repo, _fake, env, tmp = pairbed
    e = _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE)
    e["BENCH_606_HOST_LOAD"] = LOADED
    p = _launch(repo, e)
    assert p.returncode == 2, p.stderr[-2000:]
    assert "refusing: the orchestrator's 1-minute load 90.15 exceeds its 8 CPUs" in p.stderr
    assert p.stderr.rstrip().endswith(": nothing was created")
    assert not (tmp / "pair" / "pair.json").exists()


def test_the_recorder_samples_until_stopped_and_never_bursts(tmp_path):
    t = [1000.0]
    reads = iter([{"t": 1000.0 + 15 * i, "load1": float(i), "load5": 1.0, "load15": 1.0, "cpus": 8}
                  for i in range(3)])

    slept = []

    def sleep(s):
        slept.append(s)
        t[0] += s + (40 if t[0] == 1000.0 else 0)       # the first sleep overran by 40 s (a stalled host)

    out = tmp_path / "host-load.jsonl"
    hostload.record_loop(str(out), 15, samples=3, clock=lambda: t[0], sleep=sleep, read=lambda: next(reads))
    rows = [json.loads(ln) for ln in out.read_text().splitlines()]
    assert [r["kind"] for r in rows] == ["host_load"] * 3 and [r["load1"] for r in rows] == [0, 1, 2]
    assert slept == [15, 0]                              # late: the next one at once, no burst to catch up


def test_evaluate_says_the_host_load_and_its_load_in_each_api_outage(tmp_path):
    """Through the CLI: the preflight reading, the samples, and the outage run606's polls recorded."""
    hl = tmp_path / "host-load.jsonl"
    hl.write_text("\n".join(json.dumps(r) for r in [
        {"kind": "host_load_preflight", "t": 100, "load1": 2.5, "load5": 2, "load15": 2, "cpus": 8, "ok": True,
         "dry": False, "why": hostload.check({"load1": 2.5, "load5": 2, "load15": 2, "cpus": 8})[1]},
        {"kind": "host_load", "t": 100, "load1": 2.5, "load5": 2, "load15": 2, "cpus": 8},
        {"kind": "host_load", "t": 115, "load1": 11.0, "load5": 3, "load15": 2, "cpus": 8},
        {"kind": "host_load", "t": 400, "load1": 3.0, "load5": 3, "load15": 2, "cpus": 8}]) + "\n")
    outages = tmp_path / "api-outages.jsonl"
    outages.write_text(json.dumps({"kind": "api_unreachable", "t0": 110, "t1": 120, "seconds": 10}) + "\n")
    buf = io.StringIO()
    with redirect_stdout(buf):
        load606.main(["evaluate", str(hl), str(outages)])
    rep = json.loads(buf.getvalue())["report"]["host_load_report_only"]
    assert rep["result"] == "INFO" and rep["samples"] == 3 and rep["samples_over_cpus"] == 1
    assert rep["max_load1"] == 11.0 and rep["longest_gap_s"] == 285.0
    assert rep["lines"] == [
        "INFO orchestrator host load at preflight: the orchestrator's 1-minute load 2.5 is within its 8 CPUs "
        "(5-min 2, 15-min 2)",
        "INFO orchestrator host load: 3 samples, highest 1-min load 11 on 8 CPUs, 1 sample(s) over the CPU "
        "count, longest gap between samples 285 s",
        "INFO host load during the API outage run606's polls saw from 110 to 120: 1-min load up to 11 on 8 CPUs"]


def test_a_run_without_a_host_load_record_reads_unknown(tmp_path):
    f = tmp_path / "x.jsonl"
    f.write_text(json.dumps({"kind": "api_unreachable", "t0": 1, "t1": 2}) + "\n")
    buf = io.StringIO()
    with redirect_stdout(buf):
        load606.main(["evaluate", str(f)])
    rep = json.loads(buf.getvalue())["report"]["host_load_report_only"]
    assert rep["state"] == "UNKNOWN" and "a run before #1297" in rep["lines"][0]


def test_run606_evaluates_and_logs_the_host_load():
    src = (BENCH_DIR / "run606.sh").read_text()
    assert '"$OUT"/host-load.jsonl' in src
    assert '"host_load_report_only",' in src
    # checked and started before the first step; stopped in every teardown, after gke-down
    assert src.index("\nhost_load_preflight\nstart_host_load\n") < src.index("\nstep source ")
    td = src[src.index("\nteardown() {"):src.index("\ntrap teardown EXIT")]
    assert td.index("stop_host_load") > td.index("gke-down")
    assert td.rstrip().endswith('stop_host_load\n  exit "$rc"\n}')
