"""Run 24 (Diego 2026-11-09): three arms, N1 = 1.12.67, N2 = 1.12.68, N2B = 1.12.68 again on its own
cluster. N1 vs N2 is judged against the N2:N2B spread measured in the SAME launch.

    RUNS="N1 N2 N2B" PAIRS="N1:N2 N2:N2B" REPLICA_PAIR="N2:N2B"

1. The in-run A/A replica (run606_pair.sh REPLICA_PAIR, `custlat replica --in-run`): the same
   replica_ratios over the same report_from_dir as customer_latency_replica_runs12_13.json, so
   steady (which the committed file has no ratio for, #1053) gets its between-run term; the launch
   is refused when the declared pair is not one image or not one of PAIRS; a replica whose arms are
   not proven one image, or whose own measurement is void, holds no phase (the no-replica rule).
2. Every node's CPU platform (bench/nodes.py), per reading, INFO in evaluate and in the pair log.
3. snowplow's GO* env (GOGC, GOMEMLIMIT, GOMAXPROCS, ...: bench/goruntime.py, in the tracing
   reading), Secret-sourced or unknowable = UNKNOWN; and #1276: an optional envFrom ConfigMap
   proven absent before the pod was created contributes nothing.
4. (coordinator 2026-11-09) Every k6 request point carries its user bucket (`ub`) beside its
   scenario; the per-bucket split is INFO, the judged cells pool every tag and are unchanged.
"""

from __future__ import annotations

import copy
import io
import json
import os
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from bench import custlat, goruntime, load606, nodes
from bench import tracing as tr
from tests.test_k6_830 import INIT, NODE, NS, needs_node
from tests.test_run15_customer_latency import DIG_A, DIG_B, P14, _write_run, pairbed  # noqa: F401
from tests.test_run22_three_runs import ROOM_FOR_THREE, _launch_env
from tests.test_run23_tracing_egress_detach import CHART_DATA, STARTED, _agent, _cms, _node, _pod, _pods

BENCH = Path(__file__).resolve().parents[1]
DIG_N1, DIG_N2 = DIG_A, DIG_B
SECRET_NAME = "tenant-x-clientconfig"


# ─── run dirs as run606 leaves them, evaluated by the real `load606 evaluate` ──

def _evaluate_dir(d: Path, digest: str, *, scale=lambda t, v: v, digest_gate="PASS", tags=None) -> dict:
    """A run dir in run606's $OUT layout (run 14's real calls on the paired schedule), its
    verdict.json from `load606 evaluate`. The fixture has no image-digest gate record: the gate
    run606 runs is stood in for by `digest_gate` (what its check would say)."""
    files = _write_run(d, scale=scale)
    if tags is not None:
        _retag(d, tags)
    buf = io.StringIO()
    with redirect_stdout(buf):
        load606.main(["evaluate", *files, "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
                      "--browser-sessions", "5", "--no-viewers", "--expect-digest", digest])
    v = json.loads(buf.getvalue())
    v["verdict"] = "FAIL"                          # the fixture's own stream checks are not judged here
    for c in v["validity"]:
        if c["check"] == custlat.IMAGE_DIGEST_CHECK:
            c["result"] = digest_gate
    (d / "verdict.json").write_text(json.dumps(v))
    return v


def _retag(d: Path, tags):
    import gzip
    p = d / "k6" / "k6.json.gz"
    with gzip.open(p, "rt") as fh:
        rows = [json.loads(ln) for ln in fh]
    for i, r in enumerate(rows):
        r["data"]["tags"].update(tags(i))
    with gzip.open(p, "wt") as fh:
        fh.write("".join(json.dumps(r) + "\n" for r in rows))


@pytest.fixture(scope="module")
def steady_of(tmp_path_factory):
    """Is epoch t in P14's steady phase (the windows its evaluate measured)?"""
    d = tmp_path_factory.mktemp("w")
    v = _evaluate_dir(d / "x", DIG_N1)
    wins = custlat.pooled([tuple(w) for w in v["report"]["customer_latency"]["windows"]])
    return lambda t: custlat._phase(t, wins) == custlat.STEADY


def _steady_x(steady_of, k):
    return lambda t, v: v * k if steady_of(t) else v


def _cell(p, phase, m):
    return next(c for c in p["cells"] if c["phase"] == phase and c["metric"] == m and c["judged"])


# ─── 1. the in-run replica: exactly `custlat replica`, checked first ─────────

def test_the_in_run_replica_is_custlat_replicas_own_ratios_with_steady(tmp_path, steady_of):
    """Two runs of one image, one 1.30x slower in steady only: the replica's steady ratio is that
    spread, and every number is the committed file's method (replica_ratios over report_from_dir),
    reproduced by calling it directly."""
    a, b = tmp_path / "n2", tmp_path / "n2b"
    _evaluate_dir(a, DIG_N2)
    _evaluate_dir(b, DIG_N2, scale=_steady_x(steady_of, 1.30))
    r = custlat.replica_in_run((str(a), str(b)), "N2:N2B", DIG_N2, (True, True))
    assert r["in_run"]["usable"] is True and r["in_run"]["why"] == []
    assert r["schema"] == "customer_latency_replica/1"
    assert set(r["phases"]) == {"storm", "burst", "churn", "steady"}       # steady is there
    direct = custlat.replica_ratios(custlat.report_from_dir(str(a)), custlat.report_from_dir(str(b)),
                                    [custlat._provenance(str(a)), custlat._provenance(str(b))])
    assert r["phases"] == direct["phases"] and r["provenance"] == direct["provenance"]
    assert r["phases"]["steady"]["p95_ms"] == pytest.approx(1.30, abs=0.002)
    assert r["phases"]["storm"] == {"p95_ms": 1.0, "p99_ms": 1.0, "req_s": 1.0}
    # the CLI is the same code, and a file it writes loads as a replica
    out = subprocess.run([sys.executable, "-m", "bench.custlat", "replica", str(a), str(b), "--in-run", "N2:N2B",
                          "--expect-digest", DIG_N2, "--completed", "1", "1"], cwd=BENCH, capture_output=True,
                         text=True, check=True).stdout
    (tmp_path / "rep.json").write_text(out)
    assert custlat.load_replica(str(tmp_path / "rep.json"))["phases"] == r["phases"]
    # without --in-run it is the committed file's command, unchanged
    plain = json.loads(subprocess.run([sys.executable, "-m", "bench.custlat", "replica", str(a), str(b)],
                                      cwd=BENCH, capture_output=True, text=True, check=True).stdout)
    assert plain["phases"] == r["phases"] and "in_run" not in plain


@pytest.mark.parametrize("case", ["other_digest", "gate_not_pass", "not_completed", "invalid_arm", "restart"])
def test_a_replica_not_proven_one_image_or_void_holds_no_phase(tmp_path, case):
    a, b = tmp_path / "n2", tmp_path / "n2b"
    _evaluate_dir(a, DIG_N2)
    vb = _evaluate_dir(b, DIG_N1 if case == "other_digest" else DIG_N2,
                       digest_gate="FAIL" if case == "gate_not_pass" else "PASS")
    if case == "invalid_arm":
        vb["verdict"] = "INVALID"
    if case == "restart":
        vb["report"]["customer_latency"]["readiness"]["restarts"].update(restarted=True, count=1)
    (b / "verdict.json").write_text(json.dumps(vb))
    done = (True, False) if case == "not_completed" else (True, True)
    r = custlat.replica_in_run((str(a), str(b)), "N2:N2B", DIG_N2, done)
    assert r["in_run"]["usable"] is False and r["phases"] == {}, r["in_run"]
    why = " ".join(r["in_run"]["why"])
    assert {"other_digest": "not an A/A pair", "gate_not_pass": "snowplow.image_digest check is FAIL",
            "not_completed": "evaluate step did not run", "invalid_arm": "its own verdict is 'INVALID'",
            "restart": "restarted 1x"}[case] in why, why
    assert "NOT measured" in r["in_run"]["line"]


@pytest.mark.parametrize("kind, answered, usable", [("gap", None, True), ("not_ready", False, False),
                                                     ("not_ready", True, False)])
def test_a_replica_arm_not_ready_is_void_and_a_sampler_gap_is_only_noted(tmp_path, kind, answered, usable):
    a, b = tmp_path / "n2", tmp_path / "n2b"
    _evaluate_dir(a, DIG_N2)
    vb = _evaluate_dir(b, DIG_N2)
    cl = vb["report"]["customer_latency"]
    t0 = cl["k6_span"][0]
    e = {"t0": t0 + 100, "t1": t0 + 190, "seconds": 90.0, "kind": kind, "why": "planted"}
    if answered is not None:
        e["answered"] = answered
    cl["readiness"]["excluded"].append(e)
    (b / "verdict.json").write_text(json.dumps(vb))
    r = custlat.replica_in_run((str(a), str(b)), "N2:N2B", DIG_N2, (True, True))
    assert r["in_run"]["usable"] is usable, r["in_run"]
    if usable:
        assert r["phases"] and "saw nothing for 90.0 s" in r["in_run"]["notes"][0]
    else:
        assert "was not Ready (or did not answer readyz) for 90.0 s" in " ".join(r["in_run"]["why"])


def test_a_pair_says_its_replica_source_and_an_unmeasured_one_leaves_steady_unjudged(tmp_path, steady_of):
    """pair() on N1 vs N2 (2.5x slower in steady, past A's within-run bar of ~2.11): with an in-run
    replica of 1.0 in steady it FAILs there (steady can now FAIL); with the replica unmeasured it is
    INCONCLUSIVE with a NOTE; the committed file says it is the committed file."""
    va = _evaluate_dir(tmp_path / "n1", DIG_N1)
    vb = _evaluate_dir(tmp_path / "n2", DIG_N2, scale=_steady_x(steady_of, 2.5))
    good = {"schema": "customer_latency_replica/1", "provenance": [],
            "phases": {p: {"p95_ms": 1.0, "p99_ms": 1.0, "req_s": 1.0} for p in ("storm", "burst", "churn", "steady")},
            "in_run": {"pair": "N2:N2B", "image_digest": DIG_N2, "usable": True, "why": []}}
    p = custlat.pair(copy.deepcopy(va), copy.deepcopy(vb), replica=good)
    assert p["replica_source"]["kind"] == "in_run" and p["replica_source"]["usable"] is True
    assert "in-run A/A pair N2:N2B" in p["replica_source"]["line"] and "steady" in p["replica_source"]["line"]
    assert _cell(p, "steady", "p95_ms")["result"] == "FAIL" and p["verdict"] == "FAIL"
    assert _cell(p, "storm", "p95_ms")["result"] == "PASS"
    bad = dict(good, phases={}, in_run=dict(good["in_run"], usable=False, why=["2nd run (x): restarted 1x"]))
    p = custlat.pair(copy.deepcopy(va), copy.deepcopy(vb), replica=bad)
    assert _cell(p, "steady", "p95_ms")["result"] == custlat.INCONCLUSIVE and p["verdict"] == custlat.INCONCLUSIVE
    assert any("in-run replica N2:N2B could not be measured" in n for n in p["notes"])
    committed = custlat.load_replica(str(BENCH / "customer_latency_replica_runs12_13.json"))
    p = custlat.pair(copy.deepcopy(va), copy.deepcopy(vb), replica=committed)
    assert p["replica_source"]["kind"] == "file" and "snowplow-bench-run12" in p["replica_source"]["line"]
    assert _cell(p, "steady", "p95_ms")["result"] == custlat.INCONCLUSIVE        # #1053, unchanged


# ─── 1b. the launcher: RUNS="N1 N2 N2B" PAIRS="N1:N2 N2:N2B" REPLICA_PAIR="N2:N2B" ──

NAMES = {"N1": "snowplow-bench-r24n1", "N2": "snowplow-bench-r24n2", "N2B": "snowplow-bench-r24n2b"}
SPEC = {"N1": (NAMES["N1"], DIG_N1), "N2": (NAMES["N2"], DIG_N2), "N2B": (NAMES["N2B"], DIG_N2)}
_COPY_OUT = '[[ -d "$FAKE/$RUN_NAME.out" ]] && cp -R "$FAKE/$RUN_NAME.out/." "$OUT/"\n'


def _copies_out(repo: Path):
    """The stand-in run606 leaves a whole run dir ($FAKE/<name>.out: k6 stream, mem, fresh, verdict),
    as a real run does: the in-run replica reads each run's collected files."""
    s = (repo / "run606.sh").read_text()
    a = 'echo "=== step measure ===" >> "$OUT/run.log"\n'
    (repo / "run606.sh").write_text(s.replace(a, a + _COPY_OUT))
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qam", "out"], cwd=repo,
                   check=True, capture_output=True)


def _launch(repo, env):
    return subprocess.run(["bash", str(repo / "run606_pair.sh")], env=env, capture_output=True, text=True,
                          timeout=300, check=False)


@pytest.mark.parametrize("n2b_steady, want", [(0.9, "PASS"), (2.5, "FAIL")])
def test_steady_is_judged_against_the_in_run_replica(pairbed, steady_of, n2b_steady, want):  # noqa: F811
    """N2 is 2.5x N1 in steady. N2B (the same image as N2) at 0.9x N1 there: the A/A spread is
    2.5 / 0.9 = 2.78, so the 2.5x is inside it: PASS, the bar from the replica. N2B at 2.5x (no
    spread): the 2.5x is past A's within-run bar and the replica's 1.0: FAIL. The committed file
    (no steady ratio) could only say INCONCLUSIVE (the control, below)."""
    repo, fake, env, tmp = pairbed
    _copies_out(repo)
    _evaluate_dir(fake / f"{NAMES['N1']}.out", DIG_N1)
    _evaluate_dir(fake / f"{NAMES['N2']}.out", DIG_N2, scale=_steady_x(steady_of, 2.5))
    _evaluate_dir(fake / f"{NAMES['N2B']}.out", DIG_N2, scale=_steady_x(steady_of, n2b_steady))
    p = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE))
    assert p.returncode == {"PASS": 0, "FAIL": 1}[want], p.stderr[-3000:]
    po = tmp / "pair"
    assert not (po / "paired-verdict-N2-N2B.json").exists()            # the replica gets no verdict of its own
    rep = json.loads((po / "replica-N2-N2B.json").read_text())
    assert rep["in_run"]["usable"] is True
    assert rep["phases"]["steady"]["p95_ms"] == pytest.approx(2.5 / n2b_steady, abs=0.003)
    v = json.loads((po / "paired-verdict-N1-N2.json").read_text())
    assert v["verdict"] == want and v["replica_source"]["kind"] == "in_run"
    c = _cell(v, "steady", "p95_ms")
    assert c["result"] == want and c["replica"] == rep["phases"]["steady"]["p95_ms"]
    if want == "PASS":
        assert c["pass_bar_from"] == "replica"
    log = (po / "pair.log").read_text()
    assert "replica N2:N2B: INFO replica: in-run A/A pair N2:N2B" in log
    assert "N1 vs N2: INFO replica: in-run A/A pair N2:N2B" in log
    assert "replica N2:N2B: N2B (replica arm): INFO node CPU platforms: UNKNOWN" in log   # per arm, said
    assert "N1 vs N2: N1 (baseline): INFO Go runtime env: UNKNOWN" in log
    pj = json.loads((po / "pair.json").read_text())
    assert pj["replica_pair"] == {"first": "N2", "second": "N2B"}


def test_the_control_without_a_replica_pair_leaves_steady_inconclusive(pairbed, steady_of):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _copies_out(repo)
    _evaluate_dir(fake / f"{NAMES['N1']}.out", DIG_N1)
    _evaluate_dir(fake / f"{NAMES['N2']}.out", DIG_N2, scale=_steady_x(steady_of, 2.5))
    spec = {k: SPEC[k] for k in ("N1", "N2")}
    p = _launch(repo, _launch_env(env, tmp, spec, "N1:N2"))
    assert p.returncode == 4, p.stderr[-3000:]
    v = json.loads((tmp / "pair" / "paired-verdict-N1-N2.json").read_text())
    assert v["replica_source"]["kind"] == "file" and _cell(v, "steady", "p95_ms")["result"] == custlat.INCONCLUSIVE


@pytest.mark.parametrize("over, refusal", [
    ({"EXPECTED_IMAGE_DIGEST_N2B": DIG_N1},
     "REPLICA_PAIR N2:N2B: an A/A replica runs ONE image on both arms"),
    ({"REPLICA_PAIR": "N2B:N2"}, "REPLICA_PAIR N2B:N2 is not in PAIRS (N1:N2 N2:N2B)"),
    ({"REPLICA_PAIR": "N1:N2B"}, "REPLICA_PAIR N1:N2B is not in PAIRS"),
    ({"PAIRS": "N2:N2B", "RUNS": "N2 N2B"}, "REPLICA_PAIR N2:N2B is the only pair"),
    ({"REPLICA_PAIR": None}, "pair N2:N2B: the two digests are the same image"),
])
def test_the_launch_is_refused_before_anything_starts(pairbed, over, refusal):  # noqa: F811
    repo, fake, env, tmp = pairbed
    e = _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE)
    e.update(over)
    e = {k: v for k, v in e.items() if v is not None}
    p = _launch(repo, e)
    assert p.returncode == 2 and f"refusing: {refusal}" in p.stderr, p.stderr[-2000:]
    calls = (fake / "calls").read_text().splitlines() if (fake / "calls").exists() else []
    assert not [c for c in calls if " dry=" in c]                      # not even a dry pass


def test_the_dry_launch_shows_the_replica_first_and_no_verdict_for_it(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", BENCH_606_DRY_RUN="1",
                                  **ROOM_FOR_THREE))
    assert p.returncode == 0, p.stderr[-2000:]
    dry = [ln.split("DRY:", 1)[1].strip() for ln in p.stderr.splitlines() if "DRY:   " in ln]
    assert dry[0].startswith("python3 -m bench.custlat replica ") and "--in-run N2:N2B" in dry[0]
    assert f"--expect-digest {DIG_N2}" in dry[0] and "replica-N2-N2B.json" in dry[0]
    assert len(dry) == 2 and "custlat pair" in dry[1] and "--replica " + str(tmp / "pair" / "replica-N2-N2B.json") in dry[1]
    assert "paired-verdict-N1-N2.json" in dry[1]


@pytest.mark.parametrize("failing, stopped", [("N2B", ()), ("N1", ("N2", "N2B"))])
def test_a_failed_replica_arm_does_not_stop_the_pair_it_calibrates_and_a_lost_pair_stops_the_replica(
        pairbed, failing, stopped):  # noqa: F811
    """N2B fails as tooling: N1:N2 is intact, so N1 and N2 run on (and N1:N2 is judged with the
    replica unmeasured). N1 fails: the only pair the replica calibrates is lost, so N2 and N2B are
    stopped (their clusters feed no comparison)."""
    repo, fake, env, tmp = pairbed
    for nm in NAMES.values():
        (fake / f"{nm}.sleep").write_text("20" if stopped else "2")
    (fake / f"{NAMES[failing]}.sleep").write_text("0")
    (fake / f"{NAMES[failing]}.rc").write_text("1")
    (fake / f"{NAMES[failing]}.incomplete").write_text("")
    p = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE))
    assert (fake / "calls").exists(), p.stderr[-2000:]
    calls = (fake / "calls").read_text().splitlines()
    assert sorted(c.split()[0] for c in calls if c.endswith("pair-stop")) == sorted(NAMES[x] for x in stopped), calls
    if not stopped:
        rep = json.loads((tmp / "pair" / "replica-N2-N2B.json").read_text())
        assert rep["in_run"]["usable"] is False and rep["phases"] == {}
        assert "2nd run (N2B): its evaluate step did not run" in rep["in_run"]["why"]
        log = (tmp / "pair" / "pair.log").read_text()
        assert "replica N2:N2B: NOT MEASURED: 2nd run (N2B): its evaluate step did not run" in log


# ─── 2. node CPU platforms ──────────────────────────────────────────────────

def _k8s_node(name, pool, zone="europe-west4-a", mtype="n2-standard-8", bench_pool=None):
    lb = {nodes.POOL_LABEL: pool, nodes.ZONE_LABEL: zone, nodes.TYPE_LABEL: mtype}
    if bench_pool:
        lb[nodes.BENCH_POOL_LABEL] = bench_pool
    return {"metadata": {"name": name, "labels": lb}, "status": {"addresses": [{"address": "10.0.0.1"}]}}


NODES = {"items": [_k8s_node("gke-c-default-pool-1", "default-pool"),
                   _k8s_node("gke-c-snowplow-1", "snowplow", bench_pool="snowplow"),
                   _k8s_node("gke-c-loadgen-1", "loadgen", mtype="n2-standard-16", bench_pool="loadgen")]}


def _desc(plat):
    return {"name": "x", "cpuPlatform": plat, "minCpuPlatform": "Automatic",
            "machineType": "https://www.googleapis.com/compute/v1/projects/p/zones/z/machineTypes/n2-standard-8"}


def test_node_platforms_present_absent_and_unknown():
    d = {"gke-c-default-pool-1": (_desc("Intel Cascade Lake"), None),
         "gke-c-snowplow-1": (_desc("Intel Ice Lake"), None),
         "gke-c-loadgen-1": (None, "gcloud compute instances describe exit 1: ERROR: (gcloud) NOT_FOUND")}
    r = nodes.reading("up", NODES, d, t=1.0)
    by = {n["node"]: n for n in r["nodes"]}
    assert r["state"] == "read" and by["gke-c-snowplow-1"]["cpu_platform"] == "Intel Ice Lake"
    assert by["gke-c-snowplow-1"]["pool"] == "snowplow" and by["gke-c-snowplow-1"]["instance_machine_type"] == "n2-standard-8"
    assert by["gke-c-loadgen-1"]["cpu_platform"] == nodes.UNKNOWN and "exit 1" in by["gke-c-loadgen-1"]["why"]
    line = nodes.reading_line(r)
    assert "default-pool: gke-c-default-pool-1 Intel Cascade Lake (n2-standard-8, europe-west4-a)" in line
    assert "loadgen: gke-c-loadgen-1 UNKNOWN" in line
    # nothing read, and nothing listed, are UNKNOWN with why, never a platform
    assert nodes.reading("up", None, {}, nodes_why="kubectl get nodes did not answer within 5 s")["state"] == nodes.UNKNOWN
    assert nodes.reading("up", {"items": []}, {})["why"] == "kubectl listed no node"
    # the report: both readings, and a node whose platform changed between them
    later = copy.deepcopy(r)
    later.update(phase="collect", t=2.0)
    later["nodes"][1]["cpu_platform"] = "AMD Milan"
    rep = nodes.report([r, later, {"kind": "cluster_shape"}])
    assert rep["result"] == "INFO" and len(rep["readings"]) == 2
    assert rep["lines"][0].startswith("INFO node CPU platforms (up): ")
    assert rep["changed"] == [later["nodes"][1]["node"]] and "changed between the readings" in rep["lines"][-1]
    assert nodes.report([])["state"] == nodes.UNKNOWN


_FAKE_GCLOUD = r"""#!/usr/bin/env python3
import json, os, sys
a = sys.argv[1:]
open(os.environ["CALLS"], "a").write(json.dumps(["gcloud"] + a) + "\n")
if a[:3] == ["compute", "instances", "describe"]:
    name = a[3]
    if "loadgen" in name:
        sys.stderr.write("ERROR: (gcloud.compute.instances.describe) authenticated as op@example.invalid: NOT_FOUND\n")
        sys.exit(1)
    print(json.dumps({"name": name, "cpuPlatform": "Intel Ice Lake" if "snowplow" in name else "Intel Cascade Lake",
                      "minCpuPlatform": "Automatic", "machineType": "zones/z/machineTypes/n2-standard-8"}))
    sys.exit(0)
sys.exit(9)
"""
_FAKE_KUBECTL_NODES = r"""
import json, os, sys
a = sys.argv[1:]
open(os.environ["CALLS"], "a").write(json.dumps(["kubectl"] + a) + "\n")
if "nodes" in a:
    print(os.environ["NODES"]); sys.exit(0)
sys.exit(3)
"""


def test_the_node_cli_reads_kubectl_then_one_projected_describe_per_node(tmp_path):
    fb = tmp_path / "bin"
    fb.mkdir()
    (fb / "gcloud").write_text(_FAKE_GCLOUD)
    (fb / "gcloud").chmod(0o755)
    (tmp_path / "kubectl.py").write_text(_FAKE_KUBECTL_NODES)
    env = dict(os.environ, PATH=f"{fb}{os.pathsep}{os.environ['PATH']}", CALLS=str(tmp_path / "calls"),
               NODES=json.dumps(NODES), PYTHONPATH=str(BENCH))
    out = tmp_path / "cluster-shape-nodes.jsonl"
    for ph in ("up", "collect"):
        p = subprocess.run([sys.executable, "-m", "bench.nodes", "read", "--phase", ph, "--project", "proj",
                            "--out", str(out), "--timeout-s", "60", "--", sys.executable, str(tmp_path / "kubectl.py"),
                            "--context=bench"], cwd=BENCH, env=env, capture_output=True, text=True, timeout=120)
        assert p.returncode == 0, p.stderr
        assert p.stdout.startswith(f"node CPU platforms ({ph}): ") and "snowplow: gke-c-snowplow-1 Intel Ice Lake" in p.stdout
    recs = [json.loads(ln) for ln in out.read_text().splitlines()]
    assert [r["phase"] for r in recs] == ["up", "collect"] and all(r["kind"] == "node_platforms" for r in recs)
    calls = [json.loads(ln) for ln in (tmp_path / "calls").read_text().splitlines()]
    desc = [c for c in calls if c[:4] == ["gcloud", "compute", "instances", "describe"]]
    assert len(desc) == 6 and all(c[-2:] == ["--format", nodes.DESCRIBE_FIELDS] for c in desc)
    assert all(["--project", "proj"] == c[5:7] and c[7:9] == ["--zone", "europe-west4-a"] for c in desc)
    text = out.read_text()
    assert "op@example.invalid" not in text and "authenticated" not in text      # the code only, never the account
    # evaluate reads the file with the cluster's shape (run606's cluster-shape*.jsonl glob)
    v = _evaluate_with(tmp_path, [str(out)])
    rep = v["report"]["nodes_report_only"]
    assert rep["state"] == "read" and rep["lines"][0].startswith("INFO node CPU platforms (up): ")
    assert rep["by_pool"]["snowplow"] == ["Intel Ice Lake"]


def test_run606_reads_the_node_platforms_after_gke_up_and_at_collect():
    src = (BENCH / "run606.sh").read_text()
    assert '  log "$shape_msg"\n  node_platforms up\n}' in src
    assert "  tracing_reading after\n  node_platforms collect\n" in src
    assert '--out "$OUT/cluster-shape-nodes.jsonl"' in src
    assert '"$OUT"/cluster-shape*.jsonl' in src                         # evaluate's existing glob reads it


def _evaluate_with(tmp_path, extra, tracing=None):
    d = tmp_path / "run"
    files = _write_run(d)
    buf = io.StringIO()
    with redirect_stdout(buf):
        load606.main(["evaluate", *files, *extra, "--k6-out", str(d / "k6"), "--browser-out", str(d / "browser"),
                      "--browser-sessions", "5", "--no-viewers", "--expect-digest", DIG_A]
                     + (["--tracing", tracing] if tracing else []))
    return json.loads(buf.getvalue())


# ─── 3. GO* env (and #1276) ─────────────────────────────────────────────────

def _go_pod(env=None, env_from=None, *, created="2026-11-08T09:59:00Z"):
    p = _pod(env_from=env_from, env=env)
    p["metadata"]["creationTimestamp"] = created
    return p


GO_ENV = [{"name": "GOMEMLIMIT", "value": "20GiB"}, {"name": "LOG_LEVEL", "value": "warn"},
          {"name": "GOOGLE_API_KEY", "value": "not-a-go-key-and-never-kept"},
          {"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://$(HOST_IP):4318"}]


def test_go_env_present_keeps_go_keys_only_and_says_the_defaults():
    data = dict(CHART_DATA, GOGC="200", GODEBUG="madvdontneed=1")
    r = tr.reading("before", _pods(_go_pod(GO_ENV)), _cms(data), _node(_agent()))
    g = r["go_env"]
    assert g["state"] == "read"
    assert g["effective"]["GOGC"] == {"state": "set", "value": "200", "from": "envFrom configMapRef snowplow"}
    assert g["effective"]["GOMEMLIMIT"] == {"state": "set", "value": "20GiB", "from": "env"}
    assert g["effective"]["GOMAXPROCS"] == {"state": "unset"}
    assert g["effective"]["GODEBUG"]["value"] == "madvdontneed=1"
    text = json.dumps(g)
    for k in ("GOOGLE_API_KEY", "not-a-go-key", "LOG_LEVEL", "URL_AUTHN", "OTEL_"):
        assert k not in text, k
    line = goruntime.reading_text(g)
    assert line.startswith("GOGC=200 (envFrom configMapRef snowplow), GOMEMLIMIT=20GiB (env), GOMAXPROCS unset (")
    assert "GODEBUG=madvdontneed=1" in line
    assert goruntime.is_go_key("GOTRACEBACK") and not goruntime.is_go_key("GOOGLE_APPLICATION_CREDENTIALS")
    assert not goruntime.is_go_key("GO_SOMETHING") and not goruntime.is_go_key("GOOGLE")


def test_go_env_absent_or_unreadable_configmap_reads_unknown():
    r = tr.reading("before", _pods(_go_pod([])), {"items": []}, _node())
    assert r["go_env"]["effective"]["GOGC"]["state"] == tr.UNKNOWN
    assert "not there now" in r["go_env"]["effective"]["GOGC"]["why"]
    r = tr.reading("before", _pods(_go_pod([])), None, _node(), configmaps_why="kubectl exit 1: Forbidden")
    assert r["go_env"]["effective"]["GOMEMLIMIT"]["state"] == tr.UNKNOWN
    r = tr.reading("before", None, None, None, pods_why="no answer")
    assert r["go_env"]["state"] == tr.UNKNOWN and goruntime.reading_text(r["go_env"]) == "UNKNOWN (no answer)"


def test_go_env_secret_sourced_is_unknown_and_the_secret_is_never_named():
    env = [{"name": "GOGC", "valueFrom": {"secretKeyRef": {"name": SECRET_NAME, "key": "gogc"}}}]
    ef = [{"configMapRef": {"name": "snowplow"}}, {"secretRef": {"name": SECRET_NAME}}]
    r = tr.reading("before", _pods(_go_pod(env, ef)), _cms(), _node())
    g = r["go_env"]
    assert g["effective"]["GOGC"]["state"] == tr.UNKNOWN and "Secret" in g["effective"]["GOGC"]["why"]
    # a later envFrom Secret could set any key the env does not: UNKNOWN, and said
    assert g["effective"]["GOMEMLIMIT"]["state"] == tr.UNKNOWN
    assert "secret_source" in g and "UNKNOWN" in goruntime.reading_text(g)
    assert SECRET_NAME not in json.dumps(r)


def test_the_go_runtime_report_has_both_readings_and_the_runtimes_own_view(tmp_path):
    f = tmp_path / "tracing.json"
    tr.record(str(f), tr.reading("before", _pods(_go_pod(GO_ENV)), _cms(), _node(_agent())))
    later = copy.deepcopy(GO_ENV)
    later[0]["value"] = "16GiB"
    tr.record(str(f), tr.reading("after", _pods(_go_pod(later)), _cms(), _node(_agent())))
    mem = [{"kind": "mem", "t": 1, "next_gc_mb": 900.0, "gc_cpu_fraction": 0.01, "num_gc": 10},
           {"kind": "mem", "t": 2, "next_gc_mb": 1200.0, "gc_cpu_fraction": 0.02, "num_gc": 30}]
    rep = goruntime.report(str(f), mem)
    assert rep["result"] == "INFO" and rep["state"] == "read" and rep["changed"] == ["GOMEMLIMIT"]
    assert rep["lines"][0].startswith("INFO Go runtime env before the measure: GOGC unset (the runtime's default, 100)")
    assert "GOMEMLIMIT=16GiB" in rep["lines"][1]
    assert rep["lines"][2] == "INFO Go runtime env changed during the measure: GOMEMLIMIT"
    assert "NextGC max 1200.0 MB, GCCPUFraction 0.02 (last), NumGC 30" in rep["lines"][3]
    assert "not exposed" in rep["lines"][3]
    assert goruntime.report(None, [])["state"] == goruntime.UNKNOWN
    # through evaluate, from run606's tracing.json
    v = _evaluate_with(tmp_path, [], tracing=str(f))
    assert v["report"]["go_runtime_report_only"]["readings"]["before"]["effective"]["GOMEMLIMIT"]["value"] == "20GiB"


OVERRIDE = [{"configMapRef": {"name": "snowplow"}}, {"configMapRef": {"name": "snowplow-api-override", "optional": True}}]
POD_CREATED = "2026-11-08T09:59:00Z"
BEFORE_POD = 1_794_131_000.0          # 2026-11-08T09:43:20Z: before the pod was created (09:59:00Z)
AFTER_POD = 1_794_132_000.0           # 2026-11-08T10:00:00Z: after


def _pre(names=("kube-root-ca.crt",), t=BEFORE_POD, pods=()):
    return tr.preinstall_reading({"items": [{"metadata": {"name": p}} for p in pods]},
                                 {"items": [{"metadata": {"name": n}, "data": {"SECRETISH": "x"}} for n in names]}, t=t)


def test_1276_an_optional_configmap_absent_before_the_install_and_now_contributes_nothing():
    pod = _go_pod(env_from=OVERRIDE, created=POD_CREATED)
    # run 23's reading: UNKNOWN on every key the override could set
    r = tr.reading("before", _pods(pod), _cms(), _node(_agent()))
    assert r["tracing"]["state"] == tr.UNKNOWN and "snowplow-api-override" in r["tracing"]["why"]
    # proven absent before the pod existed: the chart's ConfigMap decides, the defaults apply
    r = tr.reading("before", _pods(pod), _cms(), _node(_agent()), preinstall=_pre())
    assert r["tracing"]["state"] == tr.ON and r["sampler"]["value"] == "parentbased_always_on"
    assert r["go_env"]["effective"]["GOGC"] == {"state": "unset"}
    src = next(s for s in r["env_sources"] if s.get("configmap") == "snowplow-api-override")
    assert src["state"] == "absent_since_before_install" and src["optional"] is True
    assert _pre()["configmaps"] == ["kube-root-ca.crt"] and "SECRETISH" not in json.dumps(_pre())   # names only


@pytest.mark.parametrize("case", ["created_after_start", "deleted_after_start", "pre_after_pod", "pre_saw_pod",
                                  "pre_unread", "not_optional"])
def test_1276_what_the_readings_cannot_prove_stays_unknown(case):
    ef = copy.deepcopy(OVERRIDE)
    if case == "not_optional":
        ef[1]["configMapRef"].pop("optional")
    pod = _go_pod(env_from=ef, created=POD_CREATED)
    cms = _cms()
    pre = _pre()
    if case == "created_after_start":       # there now, created after the container started
        cms["items"].append({"metadata": {"name": "snowplow-api-override", "creationTimestamp": "2026-11-08T11:00:00Z",
                                          "managedFields": [{"time": "2026-11-08T11:00:00Z"}]},
                             "data": {"OTEL_ENABLED": "false"}})
    elif case == "deleted_after_start":     # there before the install, gone now
        pre = _pre(names=("kube-root-ca.crt", "snowplow-api-override"))
    elif case == "pre_after_pod":
        pre = _pre(t=AFTER_POD)
    elif case == "pre_saw_pod":
        pre = _pre(pods=("snowplow-old",))
    elif case == "pre_unread":
        pre = tr.preinstall_reading(None, None, pods_why="kubectl exit 1: Forbidden")
    r = tr.reading("before", _pods(pod), cms, _node(_agent()), preinstall=pre)
    assert r["tracing"]["state"] == tr.UNKNOWN, (case, r["tracing"])
    assert r["go_env"]["effective"]["GOGC"]["state"] == tr.UNKNOWN, case
    assert STARTED                                          # the pod's container started at 10:00Z


_FAKE_KUBECTL_PRE = r"""
import json, os, sys
a = sys.argv[1:]
open(os.environ["CALLS"], "a").write(json.dumps(a) + "\n")
phase = open(os.environ["PHASE"]).read()
if "pods" in a and "-l" in a:
    print(json.dumps({"items": []}) if phase == "pre" else os.environ["PODS"]); sys.exit(0)
if "configmaps" in a:
    print(json.dumps({"items": [{"metadata": {"name": "kube-root-ca.crt"}, "data": {"DATA_MARKER": "x"}}]})
          if phase == "pre" else os.environ["CMS"]); sys.exit(0)
if "pods" in a and "-A" in a:
    print(os.environ["NODE"]); sys.exit(0)
sys.exit(3)
"""


def test_1276_the_cli_takes_the_preinstall_reading_and_the_later_reading_uses_it(tmp_path):
    (tmp_path / "kubectl.py").write_text(_FAKE_KUBECTL_PRE)
    ph = tmp_path / "phase"
    pod = _go_pod(env_from=OVERRIDE, created="2099-01-01T00:00:00Z")     # created after the pre-install read
    env = dict(os.environ, CALLS=str(tmp_path / "calls"), PHASE=str(ph), PODS=json.dumps(_pods(pod)),
               CMS=json.dumps(_cms()), NODE=json.dumps(_node(_agent())), PYTHONPATH=str(BENCH))
    out = tmp_path / "tracing.json"

    def read(phase):
        return subprocess.run([sys.executable, "-m", "bench.tracing", "read", "--phase", phase, "--out", str(out),
                               "--timeout-s", "60", "--", sys.executable, str(tmp_path / "kubectl.py")],
                              cwd=BENCH, env=env, capture_output=True, text=True, timeout=120)
    ph.write_text("pre")
    p = read("preinstall")
    assert p.returncode == 0 and p.stdout.startswith("pre-install: 1 ConfigMap(s) in the namespace, 0 snowplow pod(s)")
    ph.write_text("live")
    p = read("before")
    assert p.returncode == 0 and p.stdout.startswith("tracing ON"), p.stdout
    doc = json.loads(out.read_text())
    assert doc["preinstall"]["configmaps"] == ["kube-root-ca.crt"] and "DATA_MARKER" not in json.dumps(doc["preinstall"])
    assert "INFO tracing pre-install: 1 ConfigMap(s)" in tr.report(str(out))["lines"][-1]
    calls = [json.loads(ln) for ln in (tmp_path / "calls").read_text().splitlines()]
    assert not any("secret" in x.lower() for c in calls for x in c)


def test_run606_takes_the_preinstall_reading_before_anything_of_the_install_exists():
    src = (BENCH / "run606.sh").read_text()
    body = src[src.index("s_install() {"):]
    assert body.index("tracing_preinstall") < body.index("create namespace krateo-system")
    assert body.index("tracing_preinstall") < body.index('install installer "$INSTALLER_CHART"')


# ─── 4. k6's user bucket tag ────────────────────────────────────────────────

_HTTP_REC = ("export default { batch: (reqs) => { for (const r of reqs) globalThis.__REQ.push("
             "{ t: globalThis.__clock, url: r.url, tags: r.params.tags }); "
             "return reqs.map(() => ({ status: 200, body: '', headers: {} })) } }\n")
_SPA = """
export function firstLoad (portal, fetch, deepLink) {
  fetch(['/call?shell'], 0); globalThis.__clock += 3000
  return { shell: { calls: 1, errors: 0, armed: [] }, content: null,
           target: { path: deepLink || '/a' }, routes: globalThis.__routes, ok: true }
}
export function firstLoadFailed (fl) { return false }
export function sameMenu (routes, table) { return true }
export function userRoutes (fl, failed, table) { return fl.routes }
export function pickRoute (routes, user, visit) { return routes[visit % routes.length] }
export function walkTree (endpoints, fetch) {
  fetch(endpoints, 0); fetch(endpoints.map((e) => e + '&child'), 1); globalThis.__clock += 200
  return { calls: 2, errors: 0, armed: [] }
}
"""


def _k6_session(tmp_path, src: str, fn: str, i: int):
    from bench import k6_job
    from tests.test_k6_session_lag import STUBS, T0
    stubs = dict(STUBS, **{"http.js": _HTTP_REC})
    pkg = tmp_path / "node_modules" / "k6"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "package.json").write_text(json.dumps({
        "name": "k6", "type": "module",
        "exports": {("." if n == "index.js" else f"./{n[:-3]}"): f"./{n}" for n in stubs}}))
    for n, s in stubs.items():
        (pkg / n).write_text(s)
    (tmp_path / "navigate.mjs").write_text(src.replace("'./spa.js'", "'./spa.mjs'"))
    (tmp_path / "spa.mjs").write_text(_SPA)
    routes = [{"path": "/a", "endpoint": "/route?p=/a"}, {"path": "/b", "endpoint": "/route?p=/b"}]
    cfg = k6_job.k6_config(snowplow_url="http://s", init=INIT, frontend_ns=NS, builders_ns=NS, routes=routes,
                           menu_classes={"devs": routes}, class_of=lambda i: "devs", users=1000, burst_users=200,
                           think_s=2, duration_s=20, burst_at_s=8, composite_routes=["/b"], viewers=False)
    (tmp_path / "config.json").write_text(json.dumps(cfg))
    (tmp_path / "tokens.json").write_text(json.dumps([f"t{k}" for k in range(1000)]))
    name = "composite_burst" if fn == "compositeBurst" else "navigate"
    (tmp_path / "globals.mjs").write_text(
        "import { readFileSync } from 'node:fs'\n"
        "globalThis.__ENV = { K6_CONFIG: 'config.json', TOKENS_FILE: 'tokens.json' }\n"
        "globalThis.open = (p) => readFileSync(p, 'utf8')\n"
        f"globalThis.__clock = {T0}\nDate.now = () => globalThis.__clock\n"
        "globalThis.__oversleep = []\nglobalThis.__firstMs = 0\n"
        f"globalThis.__routes = {json.dumps(routes)}\nglobalThis.__M = {{}}\nglobalThis.__REQ = []\n"
        f"globalThis.__exec = {{ scenario: {{ name: '{name}', iterationInTest: {i}, startTime: {T0} }} }}\n")
    (tmp_path / "run.mjs").write_text(
        f"const m = await import('./navigate.mjs')\nm.{fn}({{ t0: {T0} }})\n"
        "process.stdout.write(JSON.stringify(globalThis.__REQ))\n")
    p = subprocess.run([NODE, "--import", "./globals.mjs", "run.mjs"], capture_output=True, text=True,
                       timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout), cfg


@needs_node
@pytest.mark.parametrize("fn, i", [("navigate", 0), ("navigate", 450), ("compositeBurst", 7)])
def test_every_request_carries_its_users_bucket_and_the_schedule_is_unchanged(tmp_path, fn, i):
    src = (BENCH / "k6" / "navigate.js").read_text()
    reqs, cfg = _k6_session(tmp_path / "tag", src, fn, i)
    user = (cfg["navUserIds"] if fn == "navigate" else cfg["burstUserIds"])[i]
    lo = (user - 1) // 100 * 100 + 1
    assert reqs and all(r["tags"]["ub"] == f"{lo}-{lo + 99}" for r in reqs), (user, reqs[:2])
    assert all(set(r["tags"]) == {"level", "ub", "kind"} for r in reqs)
    # the control: the same session from navigate.js without the tag sends the same requests at the
    # same times (a tag is the point's metadata, not a request)
    plain = src.replace(", ub: bucketOf(user)", "")
    assert plain != src
    ctl, _ = _k6_session(tmp_path / "plain", plain, fn, i)
    assert [(r["t"], r["url"]) for r in ctl] == [(r["t"], r["url"]) for r in reqs]
    assert [{k: v for k, v in r["tags"].items() if k != "ub"} for r in reqs] == [r["tags"] for r in ctl]


def test_the_judged_cells_are_unchanged_by_the_tag_and_the_split_is_info(tmp_path):
    """The same calls with and without the bucket tag: the customer-latency report (every judged
    input) is identical; the per-bucket split appears only in its own INFO section."""
    plain = _evaluate_dir(tmp_path / "plain", DIG_N1)
    tagged = _evaluate_dir(tmp_path / "tag", DIG_N1,
                           tags=lambda i: {"ub": ["1-100", "101-200", "801-900"][i % 3],
                                           "scenario": "composite_burst" if i % 3 == 2 else "navigate"})
    assert plain["report"]["customer_latency"] == tagged["report"]["customer_latency"]
    ub = tagged["report"]["user_buckets_report_only"]
    assert ub["result"] == "INFO" and ub["untagged"] == 0
    steady = {(r["scenario"], r["users"]): r for r in ub["phases"]["steady"]}
    assert set(steady) == {("navigate", "1-100"), ("navigate", "101-200"), ("composite_burst", "801-900")}
    gp = tagged["report"]["customer_latency"]["gate_phases"]["steady"]
    assert sum(r["n"] for r in steady.values()) == gp["n"]
    assert any(ln.startswith("INFO k6 calls by user bucket, steady: ") and "navigate users 1-100 n " in ln for ln in ub["lines"])
    old = plain["report"]["user_buckets_report_only"]
    assert old["untagged"] == plain["report"]["customer_latency"]["calls"] and "untagged" in old["lines"][-1]
    # the paired verdict shows it per arm, never as a check
    p = custlat.info_lines_pair(plain, tagged, "user_buckets_report_only", "ABSENT", ("N1", "N2"))
    assert any(ln.startswith("N2 (candidate): INFO k6 calls by user bucket, steady: ") for ln in p["lines"])
    assert custlat.info_lines_pair({}, {}, "user_buckets_report_only", "ABSENT")["lines"][0].startswith(
        "A (baseline): INFO user_buckets_report_only: ABSENT")
