"""A paired verdict says "measured in this launch" of its in-run replica only when it was.

Run 24's replica (replica-N1-N1B.json) also judges run 23's N1:N2 (Diego 2026-11-09): a verdict that
re-judges run 23 against it must not say the replica was measured in run 23's launch. The replica
records the launch that measured it (run606_pair.sh's PAIR_RUN_ID, `custlat replica --launch`); each
pair gets its own launch id (`custlat pair --launch`); _replica_source says "measured in this
launch" only on a match, and otherwise names the replica's launch and its own run dirs: another
launch's replica, a replica that recorded no launch id (#1282's files), a pair judged with no
--launch.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench import custlat
from tests.test_run24_aa_replica_nodes_goenv import (DIG_N1, DIG_N2, NAMES, ROOM_FOR_THREE, SPEC, _copies_out,
                                                    _evaluate_dir, _launch, _launch_env, _steady_x, pairbed,  # noqa: F401
                                                    steady_of)  # noqa: F401

BENCH = Path(__file__).resolve().parents[1]
THIS = "pair-snowplow-bench-r24n1-snowplow-bench-r24n1b-snowplow-bench-r24n2-20261109T133700Z-4242"
OTHER = "pair-snowplow-bench-run23r-c7c87a3d-snowplow-bench-run23n1-c7c87a3d-20261108T101500Z-777"
PHASES = {p: {"p95_ms": 1.05, "p99_ms": 1.05, "req_s": 1.01} for p in ("storm", "burst", "churn", "steady")}


def _replica(launch, usable=True):
    return {"schema": "customer_latency_replica/1", "phases": PHASES if usable else {},
            "provenance": [{"dir": "snowplow-bench-r24n1"}, {"dir": "snowplow-bench-r24n1b"}],
            "in_run": {"pair": "N1:N1B", "image_digest": DIG_N1, "usable": usable,
                       "why": [] if usable else ["2nd run (snowplow-bench-r24n1b): restarted 1x"],
                       **({} if launch is ... else {"launch": launch})}}


@pytest.mark.parametrize("recorded, pair_launch, same", [
    (THIS, THIS, True),            # the launcher: the replica's launch is the pair's
    (THIS, OTHER, False),          # another launch's replica
    (THIS, None, False),           # a pair judged with no --launch (a manual re-judge)
    (..., THIS, False),            # a replica that recorded no launch id (#1282's files)
    (None, None, False),           # neither knows its launch: never "this launch"
])
def test_this_launch_only_on_a_match(recorded, pair_launch, same):
    for usable in (True, False):
        s = custlat._replica_source(_replica(recorded, usable), pair_launch)
        assert s["kind"] == "in_run" and s["same_launch"] is same, (recorded, pair_launch, usable)
        if same:
            assert f"measured in this launch ({THIS})" in s["line"]
        else:
            assert "this launch" not in s["line"]
            want = recorded if isinstance(recorded, str) else "its launch not recorded"
            assert f"from another launch ({want}), runs snowplow-bench-r24n1, snowplow-bench-r24n1b" in s["line"]
        assert ("NOT measured" in s["line"]) is (not usable)
    # the committed file and no replica are unchanged
    committed = custlat.load_replica(str(BENCH / "customer_latency_replica_runs12_13.json"))
    assert custlat._replica_source(committed, THIS)["kind"] == "file"
    assert custlat._replica_source(None, THIS)["kind"] == "none"


def test_the_cli_records_the_launch_and_a_re_judge_names_the_replicas_own_runs(tmp_path, steady_of):  # noqa: F811
    """Through the CLIs the launcher runs: `custlat replica --in-run --launch` writes the id;
    `custlat pair --launch` with that id says this launch; the same verdict re-judged by hand (no
    --launch, as run 23's N1:N2 against run 24's replica) or from another launch names the
    replica's launch and its two run dirs."""
    n1, n1b, n2 = tmp_path / "snowplow-bench-r24n1", tmp_path / "snowplow-bench-r24n1b", tmp_path / "n2"
    _evaluate_dir(n1, DIG_N1)
    _evaluate_dir(n1b, DIG_N1, scale=_steady_x(steady_of, 1.1))
    _evaluate_dir(n2, DIG_N2)
    rep = subprocess.run([sys.executable, "-m", "bench.custlat", "replica", str(n1), str(n1b), "--in-run", "N1:N1B",
                          "--expect-digest", DIG_N1, "--completed", "1", "1", "--launch", THIS],
                         cwd=BENCH, capture_output=True, text=True, check=True).stdout
    rf = tmp_path / "replica-N1-N1B.json"
    rf.write_text(rep)
    r = json.loads(rep)
    assert r["in_run"]["usable"] is True and r["in_run"]["launch"] == THIS
    assert f"measured in this launch ({THIS})" in r["in_run"]["line"]

    def judge(*launch):
        p = subprocess.run([sys.executable, "-m", "bench.custlat", "pair", "--a", str(n1 / "verdict.json"),
                            "--b", str(n2 / "verdict.json"), "--replica", str(rf), "--expect-a", DIG_N1,
                            "--expect-b", DIG_N2, "--a-label", "N1", "--b-label", "N2", *launch],
                           cwd=BENCH, capture_output=True, text=True)
        return json.loads(p.stdout)
    v = judge("--launch", THIS)
    assert v["replica_source"]["same_launch"] is True and "measured in this launch" in v["replica_source"]["line"]
    for launch in ((), ("--launch", OTHER)):
        w = judge(*launch)
        src = w["replica_source"]
        assert src["same_launch"] is False and "this launch" not in src["line"], launch
        assert f"from another launch ({THIS}), runs snowplow-bench-r24n1, snowplow-bench-r24n1b" in src["line"]
        assert src["dirs"] == ["snowplow-bench-r24n1", "snowplow-bench-r24n1b"]
        # the wording is all that differs: the same cells, the same verdict
        assert [c["result"] for c in w["cells"]] == [c["result"] for c in v["cells"]] and w["verdict"] == v["verdict"]
    # a #1282 replica file (no launch id) is never "this launch", whatever the pair's launch
    old = copy.deepcopy(r)
    del old["in_run"]["launch"]
    rf.write_text(json.dumps(old))
    src = judge("--launch", THIS)["replica_source"]
    assert src["same_launch"] is False and "from another launch (its launch not recorded)" in src["line"]


def test_the_launcher_passes_its_own_launch_id_to_the_replica_and_every_pair(pairbed, steady_of):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _copies_out(repo)
    _evaluate_dir(fake / f"{NAMES['N1']}.out", DIG_N1)
    _evaluate_dir(fake / f"{NAMES['N2']}.out", DIG_N2, scale=_steady_x(steady_of, 2.5))
    _evaluate_dir(fake / f"{NAMES['N2B']}.out", DIG_N2, scale=_steady_x(steady_of, 0.9))
    p = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE))
    assert p.returncode == 0, p.stderr[-3000:]
    rep = json.loads((tmp / "pair" / "replica-N2-N2B.json").read_text())
    launch = rep["in_run"]["launch"]
    assert launch.startswith("pair-" + "-".join(NAMES[x] for x in ("N1", "N2", "N2B")) + "-")
    v = json.loads((tmp / "pair" / "paired-verdict-N1-N2.json").read_text())
    assert v["replica_source"]["launch"] == launch and v["replica_source"]["same_launch"] is True
    log = (tmp / "pair" / "pair.log").read_text()
    assert f"N1 vs N2: INFO replica: in-run A/A pair N2:N2B ({DIG_N2}), measured in this launch ({launch})" in log


def test_the_dry_plan_shows_the_launch_id_on_the_replica_and_the_pair(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    dry = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", BENCH_606_DRY_RUN="1",
                                    **ROOM_FOR_THREE))
    assert dry.returncode == 0, dry.stderr[-2000:]
    plan = [ln for ln in dry.stderr.splitlines() if "DRY:   " in ln]
    assert len(plan) == 2 and all(" --launch pair-" in ln for ln in plan), plan
    ids = {ln.split(" --launch ", 1)[1].split()[0] for ln in plan}
    assert len(ids) == 1                                   # one launch id, the same on both
