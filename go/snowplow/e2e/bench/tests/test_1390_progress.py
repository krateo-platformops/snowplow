"""#1390 item 5: run 26e's replica ran for hours with nothing in the pair log but its last line before it, so
nobody could tell slow from stuck. Each statistics step now writes a start and an end line to the pair log,
the step's own progress lines between them (custlat.progress on stderr), and records its duration and input
in $PAIR_OUT/statistics.jsonl (bench/statstime.py), the measurement a later launch's TTL budget comes from."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

from bench import statstime
from tests.test_run24_aa_replica_nodes_goenv import (DIG_N1, DIG_N2, NAMES, ROOM_FOR_THREE, SPEC, _copies_out,
                                                    _evaluate_dir, _launch, _launch_env, _steady_x, pairbed,  # noqa: F401
                                                    steady_of)  # noqa: F401

BENCH = Path(__file__).resolve().parents[1]


def test_each_statistics_step_logs_its_start_progress_and_end_and_is_recorded(pairbed, steady_of):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _copies_out(repo)
    _evaluate_dir(fake / f"{NAMES['N1']}.out", DIG_N1)
    _evaluate_dir(fake / f"{NAMES['N2']}.out", DIG_N2, scale=_steady_x(steady_of, 2.5))
    _evaluate_dir(fake / f"{NAMES['N2B']}.out", DIG_N2, scale=_steady_x(steady_of, 0.9))
    p = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE))
    assert p.returncode == 0, p.stderr[-3000:]
    log = (tmp / "pair" / "pair.log").read_text()
    for step in ("replica N2:N2B", "pair N1:N2"):
        start = re.search(rf"statistics: {step} started \((\d+) bytes of k6 and browser streams\)", log)
        end = re.search(rf"statistics: {step} ended exit=(\d+) after (\d+) s", log)
        assert start and end and start.start() < end.start(), (step, log[-3000:])
        assert int(start.group(1)) > 0
        between = log[start.end():end.start()]
        assert f"statistics: {step}: custlat " in between, (step, between[-2000:])
    # the replica's own sub-steps: each arm's report and its phase statistics
    assert re.search(r"statistics: replica N2:N2B: custlat \S+Z report of \S+: done \(\d+\.\d s\)", log)
    assert "statistics: replica N2:N2B: custlat" in log and "phase statistics done:" in log
    assert log.index("statistics: replica N2:N2B ended") < log.index("statistics: pair N1:N2 started")
    recs = [json.loads(x) for x in (tmp / "pair" / "statistics.jsonl").read_text().splitlines()]
    assert [(r["step"], r["command"], r["exit"]) for r in recs] == [("replica N2:N2B", "replica", 0),
                                                                     ("pair N1:N2", "pair", 0)]
    for r in recs:
        assert r["kind"] == "statistics_step" and r["seconds"] >= 0 and r["ended"] >= r["started"]
        assert r["input_bytes"] > 0 and isinstance(r["k6_seconds"], float) and r["k6_seconds"] > 0
        assert r["host"]["cpus"] >= 1
    # the JSON each step writes is still the step's stdout only
    json.loads((tmp / "pair" / "replica-N2-N2B.json").read_text())
    json.loads((tmp / "pair" / "paired-verdict-N1-N2.json").read_text())


def test_custlat_writes_its_progress_on_stderr_and_only_json_on_stdout(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _evaluate_dir(a, DIG_N1)
    _evaluate_dir(b, DIG_N1)
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "replica", str(a), str(b)], cwd=BENCH,
                       capture_output=True, text=True, check=True)
    json.loads(p.stdout)
    lines = p.stderr.splitlines()
    assert lines and all(ln.startswith("custlat ") for ln in lines), lines
    assert sum(f"report of {d}: done" in p.stderr for d in (a, b)) == 2


def test_the_input_is_what_grows_with_the_run(tmp_path):
    d = tmp_path / "arm"
    (d / "k6").mkdir(parents=True)
    (d / "browser").mkdir()
    (d / "k6" / "k6.json.gz").write_bytes(b"x" * 1000)
    (d / "browser" / "browser.json.gz").write_bytes(b"y" * 300)
    (d / "run.log").write_bytes(b"z" * 5000)                    # not a statistics input
    assert statstime.input_bytes([d]) == 1300
    assert statstime.k6_seconds([d]) is None                     # no verdict: unknown, never 0
    (d / "verdict.json").write_text(json.dumps({"report": {"customer_latency": {"k6_span": [100.0, 400.5]}}}))
    assert statstime.k6_seconds([d, d]) == 601.0
