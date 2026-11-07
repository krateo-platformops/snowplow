"""Run 22 (Diego 2026-11-07): three runs in ONE window, two paired verdicts, C shared.

    R = 1.12.65, N = the candidate without #1085, C = the candidate with #1085
    RUNS="R N C" PAIRS="R:C N:C"  ->  paired-verdict-R-C.json (the release), -N-C.json (#1085)

run606_pair.sh takes N runs and any pairs among them; two runs and PAIRS="A:B" is the launch it
always was (the other suites' pair arms run that unchanged). These arms hold what N adds:
  - every run's dry pass, then ONE quota read for every cluster, before anything starts: three
    clusters that europe-west4 cannot hold are refused even though any two fit (run 21 attempt 2
    died on IN_USE_ADDRESSES; europe-west4's limit of 16 refuses three of 18 live, 2026-11-07);
  - the stop rule: a run that failed as tooling stops only the runs it left in no comparable pair;
  - the launch's exit ranks its pairs' verdicts: a FAIL is that comparison's result, whatever another
    pair says; then INVALID, INCONCLUSIVE, PASS;
  - every run's evidence verified, every pair's verdict published;
  - #1085's refresher tiers, report only: per-phase deltas of the per-tier completions and the
    viewed reserve's parks, per-phase max / median of the four dirty-age gauges, UNMEASURED with why
    for what a build does not publish (N, a #1244 build, has all but the parks). Through the
    `load606 evaluate` and `custlat pair` CLIs, as run606.sh and the launcher run them.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from bench import load606
from tests.test_run15_customer_latency import (DIG_A, DIG_B, _ab, _gp, _launch, pairbed, va)  # noqa: F401
from tests.test_1194_evidence import _PUBLISH, _owes

BENCH = Path(__file__).resolve().parents[1]
DIG_N = "sha256:" + "e" * 64
NAMES = {"R": "snowplow-bench-r22r", "N": "snowplow-bench-r22n", "C": "snowplow-bench-r22c"}
DIGS = {"R": DIG_A, "N": DIG_N, "C": DIG_B}
# europe-west4 has room for two clusters (12 of its 16 in-use addresses): an arm that starts three
# raises the fake region's limits and frees what the 2026-10-25 listing had in use
ROOM_FOR_THREE = {"FAKE_LIMIT_IN_USE_ADDRESSES": "24", "FAKE_USAGE_INSTANCES": "0", "FAKE_USAGE_N2_CPUS": "0",
                  "FAKE_SSD_USAGE": "0"}


def _three(env, tmp, **over):
    """The pair bed's environment as run 22's three runs."""
    e = {k: v for k, v in env.items() if not k.endswith(("_A", "_B"))}
    today = datetime.now(UTC).strftime("%Y%m%d")
    e.update(RUNS="R N C", PAIRS="R:C N:C")
    for x, name in NAMES.items():
        e.update({f"RUN_NAME_{x}": name, f"SNOWPLOW_TAG_{x}": {"R": "1.12.65", "N": "cand-no-1085", "C": "cand"}[x],
                  f"EXPECTED_IMAGE_DIGEST_{x}": DIGS[x], f"BENCH_606_GO_{x}": f"{today}-{name}",
                  f"OUT_{x}": str(tmp / x)})
    e.update(over)
    return e


def _verdicts3(fake, va, **mod):  # noqa: F811
    """Each run's verdict as the stand-in run606 copies it: R and N the baseline's, C the candidate's;
    `mod[x]` changes run x's."""
    a, b = _ab(va)
    n = copy.deepcopy(a)
    n["report"]["image_digest"] = {"expected": DIG_N}
    vs = {"R": a, "N": n, "C": b}
    for x, f in mod.items():
        f(vs[x])
    for x, v in vs.items():
        (fake / f"{NAMES[x]}.verdict.json").write_text(json.dumps(v))
    return vs


def _calls(fake):
    p = fake / "calls"
    return p.read_text().splitlines() if p.exists() else []


def _started(fake):
    return sorted(c.split()[0] for c in _calls(fake) if " dry=0 " in c)


# ─── the launcher ──────────────────────────────────────────────────────────

def test_three_runs_two_pairs_one_window_the_candidate_shared(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts3(fake, va)
    _owes(repo, _PUBLISH)                                # every run reaches its cluster and publishes
    p = _launch(repo, _three(env, tmp, **ROOM_FOR_THREE))
    assert p.returncode == 0, p.stderr[-3000:]
    calls = [c for c in _calls(fake) if " dry=" in c]
    assert [c.split()[1] for c in calls] == ["dry=1"] * 3 + ["dry=0"] * 3     # every dry pass first
    assert _started(fake) == sorted(NAMES.values())
    for pair, (a, b) in {"R-C": ("R", "C"), "N-C": ("N", "C")}.items():
        v = json.loads((tmp / "pair" / f"paired-verdict-{pair}.json").read_text())
        assert v["verdict"] == "PASS", (pair, v.get("invalid_reasons"))
        images = next(c for c in v["validity"] if c["check"].endswith(".images"))
        assert images["result"] == "PASS" and DIGS[a] in json.dumps(images) and DIGS[b] in json.dumps(images)
    pj = json.loads((tmp / "pair" / "pair.json").read_text())
    assert set(pj["runs"]) == {"R", "N", "C"} and pj["runs"]["N"]["digest"] == DIG_N
    assert pj["pairs"] == [{"baseline": "R", "candidate": "C"}, {"baseline": "N", "candidate": "C"}]
    log = (tmp / "pair" / "pair.log").read_text()
    assert "paired verdict R vs C exit=0" in log and "paired verdict N vs C exit=0" in log
    for x in ("R", "N", "C"):                           # every run's evidence verified, not only two
        assert f"run {x}: evidence final: verified" in log, log[-2000:]
    dest = run_trees[1] / "606" / f"pair-{NAMES['R']}-{NAMES['N']}-{NAMES['C']}"
    from bench import evidence
    assert evidence.verify(dest, "final") == []
    assert {"paired-verdict-R-C.json", "paired-verdict-N-C.json"} <= {q.name for q in dest.iterdir()}


def test_one_quota_read_for_every_cluster_refuses_three_that_two_would_fit(pairbed, va):  # noqa: F811
    """europe-west4 as listed: 16 in-use addresses, each cluster 6. Any two fit (12), three (18) do
    not, so the third cluster is what refuses the launch: before any run starts."""
    repo, fake, env, tmp = pairbed
    _verdicts3(fake, va)
    room = {k: v for k, v in ROOM_FOR_THREE.items() if k != "FAKE_LIMIT_IN_USE_ADDRESSES"}
    p = _launch(repo, _three(env, tmp, **room))
    assert p.returncode == 2, p.stderr[-2000:]
    assert "refusing: IN_USE_ADDRESSES in europe-west4: 0 of 16 used" in p.stderr
    assert "the run needs 18: 15 node external IP(s) + 3 LoadBalancer IP(s)" in p.stderr
    assert [c.split()[1] for c in _calls(fake) if " dry=" in c] == ["dry=1"] * 3 and not _started(fake)
    # the same region holds two of them
    p = _launch(repo, _three(env, tmp, RUNS="R C", PAIRS="R:C", BENCH_606_DRY_RUN="1", **room))
    assert p.returncode == 0, p.stderr[-2000:]
    assert "the run needs 12" in p.stderr and "DRY: all 2 runs would start now" in p.stderr


def test_a_run_that_fails_as_tooling_stops_only_the_runs_left_in_no_pair(pairbed, va):  # noqa: F811
    """N fails: N:C cannot be compared, R:C still can, so R and C run on and R:C is judged. C fails:
    both pairs lost a run, so R and N are stopped."""
    repo, fake, env, tmp = pairbed
    _verdicts3(fake, va)
    for name in NAMES.values():
        (fake / f"{name}.sleep").write_text("3")
    (fake / f"{NAMES['N']}.sleep").write_text("0")
    (fake / f"{NAMES['N']}.rc").write_text("1")
    (fake / f"{NAMES['N']}.incomplete").write_text("")
    p = _launch(repo, _three(env, tmp, **ROOM_FOR_THREE))
    calls = _calls(fake)
    assert not [c for c in calls if c.endswith("pair-stop")], calls
    assert f"{NAMES['R']} teardown rc=0" in calls and f"{NAMES['C']} teardown rc=0" in calls
    assert json.loads((tmp / "pair" / "paired-verdict-R-C.json").read_text())["verdict"] == "PASS"
    nc = json.loads((tmp / "pair" / "paired-verdict-N-C.json").read_text())
    assert nc["verdict"] == "INVALID"
    assert p.returncode == 3                             # PASS and INVALID: INVALID
    log = (tmp / "pair" / "pair.log").read_text()
    assert "run N exited 1 (it did not complete)" in log and "stopping run" not in log


def test_a_failed_shared_candidate_stops_every_other_run(pairbed, va, tmp_path):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts3(fake, va)
    for name in NAMES.values():
        (fake / f"{name}.sleep").write_text("30")
    (fake / f"{NAMES['C']}.sleep").write_text("0")
    (fake / f"{NAMES['C']}.rc").write_text("1")
    (fake / f"{NAMES['C']}.incomplete").write_text("")
    _launch(repo, _three(env, tmp, **ROOM_FOR_THREE))
    calls = _calls(fake)
    assert f"{NAMES['R']} pair-stop" in calls and f"{NAMES['N']} pair-stop" in calls, calls
    log = (tmp / "pair" / "pair.log").read_text()
    assert "run C failed as tooling: stopping run R" in log and "run C failed as tooling: stopping run N" in log


def test_the_launch_exit_ranks_a_pairs_fail_above_another_pairs_invalid(pairbed, va):  # noqa: F811
    """C regresses against R (FAIL, a result); N's own measurement is INVALID, so N:C is. The launch
    says FAIL: R:C's regression is real whatever N:C could not say."""
    repo, fake, env, tmp = pairbed

    def regress(v):
        _gp(v)["storm"]["p95_ms"] *= 100
    _verdicts3(fake, va, C=regress, N=lambda v: v.update(verdict="INVALID"))
    p = _launch(repo, _three(env, tmp, **ROOM_FOR_THREE))
    assert json.loads((tmp / "pair" / "paired-verdict-R-C.json").read_text())["verdict"] == "FAIL"
    assert json.loads((tmp / "pair" / "paired-verdict-N-C.json").read_text())["verdict"] == "INVALID"
    assert p.returncode == 1, p.stderr[-2000:]


@pytest.mark.parametrize("over, words", [
    ({"PAIRS": "R:C"}, "run N is in no pair"),
    ({"PAIRS": "R:C N:X"}, "PAIRS: N:X names a run that is not in RUNS"),
    ({"PAIRS": "R:C N:C R:C"}, "PAIRS names R:C twice"),
    ({"PAIRS": "R:C N:N"}, "PAIRS: N:N compares a run with itself"),
    ({"RUNS": "R N C N"}, "RUNS names N twice"),
    ({"RUNS": "R n C", "PAIRS": "R:C"}, "RUNS: 'n' is not a label"),
    ({"RUNS": "R"}, "a comparison needs two or more"),
    ({"EXPECTED_IMAGE_DIGEST_N": DIG_B}, "pair N:C: the two digests are the same image"),
    ({"RUN_NAME_N": NAMES["C"]}, "RUN_NAME_C and RUN_NAME_N are the same"),
    ({"BENCH_1004_GO_R": "x", "BENCH_1004_GO_C": "y"}, "BENCH_1004_GO_<run>: set it on every run of R N C"),
    ({"RUN_NAME_N": None}, "RUN_NAME_N is required"),
])
def test_a_launch_the_runs_and_pairs_cannot_make_is_refused_before_anything(pairbed, va, over, words):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, _three(env, tmp, **ROOM_FOR_THREE, **over))
    assert p.returncode == 2 and words in p.stderr, p.stderr[-1500:]
    assert not _calls(fake)                              # not even a dry pass


def test_the_pair_log_carries_each_runs_refresher_tiers(pairbed, va):  # noqa: F811
    """The lines `custlat pair` writes, in the launcher's log under their pair: C's from its own
    verdict's section, R's (an evaluate before the section) as UNMEASURED."""
    repo, fake, env, tmp = pairbed

    def tiers(v):
        v["report"]["refresher_tiers_report_only"] = {"lines": ["viewed_reserve_parks: all 7"]}

    def before_1085(v):             # the fixture is evaluated by today's code: take the section out
        v["report"].pop("refresher_tiers_report_only", None)
    _verdicts3(fake, va, C=tiers, R=before_1085)
    p = _launch(repo, _three(env, tmp, **ROOM_FOR_THREE))
    assert p.returncode == 0, p.stderr[-2000:]
    log = (tmp / "pair" / "pair.log").read_text()
    assert "R vs C: refresher tiers, B (candidate): viewed_reserve_parks: all 7" in log
    assert "N vs C: refresher tiers, B (candidate): viewed_reserve_parks: all 7" in log
    assert "R vs C: refresher tiers, A (baseline): UNMEASURED (the run's verdict.json has no " in log


# ─── #1085's refresher tiers, through the evaluate CLI ─────────────────────

K = {n: load606.TIER_KEYS[n] for n in load606.REFRESHER_TIER_COUNTERS + load606.REFRESHER_TIER_GAUGES}
# the schedule: two churn phases, steady around them (fresh606's churn records, t0 / t1 as it writes them)
CHURNS = [{"kind": "churn", "label": "list_dep@1", "t0": 100, "t1": 200, "target_rate": 1},
          {"kind": "churn", "label": "list_dep@10", "t0": 300, "t1": 400, "target_rate": 10}]


def _phase(t):
    return "churn:list_dep@1" if 100 <= t < 200 else "churn:list_dep@10" if 300 <= t < 400 else "steady"


def _sample(t, keys):
    """One mem606 sample at t with the given counters / gauges (a sample every 10 s)."""
    ph = _phase(t)
    vals = {"normal_completed": t, "viewed_completed": 2 * t, "viewed_reserve_parks": t // 50,
            "viewed_dirty_age_p95": {"steady": 10, "churn:list_dep@1": 40, "churn:list_dep@10": 70}[ph],
            # one spike per phase: the max is it, the median is not
            "viewed_dirty_age_max": 500 if t in (150, 350, 450) else {"steady": 20, "churn:list_dep@1": 50,
                                                                     "churn:list_dep@10": 80}[ph],
            "unviewed_dirty_age_p95": 1000 + t, "unviewed_dirty_age_max": 3000}
    return {"kind": "mem", "t": t, "vars_status": 200, "extra": {K[n]: vals[n] for n in keys}}


def _evaluate(tmp: Path, samples: list[dict]) -> dict:
    """`load606 evaluate` as run606.sh runs it, on a fresh606 stream and a mem606 stream."""
    (tmp / "fresh-x.jsonl").write_text("".join(json.dumps(c) + "\n" for c in CHURNS))
    (tmp / "mem-x.jsonl").write_text("".join(json.dumps(m) + "\n" for m in samples))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(tmp / "fresh-x.jsonl"),
                        str(tmp / "mem-x.jsonl")], cwd=BENCH, capture_output=True, text=True, check=False)
    v = json.loads(p.stdout)
    (tmp / "verdict.json").write_text(p.stdout)
    return v["report"]["refresher_tiers_report_only"]


ALL = list(K)


def test_the_counters_are_per_phase_deltas_and_the_gauges_per_phase_max_and_median(tmp_path):
    r = _evaluate(tmp_path, [_sample(t, ALL) for t in range(0, 501, 10)])
    # 10 s pairs, each counted in the phase its later sample is in: 10 pairs in each churn
    # phase, 30 steady (9 before the first, 10 between, 11 after)
    assert r["normal_completed"]["by_phase"] == {"churn:list_dep@1": 100, "churn:list_dep@10": 100,
                                                 "steady": 300, "churn": 200, "all": 500}
    assert r["viewed_completed"]["by_phase"]["all"] == 1000
    assert r["viewed_completed"]["by_phase"]["churn:list_dep@10"] == 200
    # t // 50 steps at 50, 100, ..., 500: two in each churn phase, six steady
    assert r["viewed_reserve_parks"]["by_phase"] == {"churn:list_dep@1": 2, "churn:list_dep@10": 2,
                                                     "steady": 6, "churn": 4, "all": 10}
    p95 = r["viewed_dirty_age_p95"]["by_phase"]
    assert (p95["steady"]["max"], p95["churn:list_dep@1"]["max"], p95["churn:list_dep@10"]["max"]) == (10, 40, 70)
    assert p95["churn"]["max"] == 70 and p95["all"]["max"] == 70 and p95["churn:list_dep@1"]["samples"] == 10
    mx = r["viewed_dirty_age_max"]["by_phase"]
    assert (mx["churn:list_dep@1"]["max"], mx["churn:list_dep@1"]["median"]) == (500, 50)
    assert (mx["steady"]["max"], mx["steady"]["median"]) == (500, 20)
    assert r["unviewed_dirty_age_max"]["by_phase"]["all"] == {"max": 3000, "median": 3000, "samples": 51}
    assert "normal_completed: churn:list_dep@1 100, churn:list_dep@10 100, steady 300, churn 200, all 500" in r["lines"]


def test_a_1244_build_has_everything_but_the_reserve_parks(tmp_path):
    """N: the completions and the ages without #1085's reserve. Its parks are UNMEASURED with why,
    never zero; everything else is measured."""
    r = _evaluate(tmp_path, [_sample(t, [k for k in ALL if k != "viewed_reserve_parks"])
                             for t in range(0, 501, 10)])
    assert r["viewed_reserve_parks"]["status"] == "UNMEASURED"
    assert K["viewed_reserve_parks"] in r["viewed_reserve_parks"]["why"] and "51 mem606 samples" in r["viewed_reserve_parks"]["why"]
    assert r["normal_completed"]["by_phase"]["all"] == 500
    assert any(ln.startswith("viewed_reserve_parks: UNMEASURED (") for ln in r["lines"])


def test_an_older_build_reads_unmeasured_for_every_value_never_zero(tmp_path):
    r = _evaluate(tmp_path, [_sample(t, []) for t in range(0, 501, 10)])
    for n in ALL:
        assert r[n]["status"] == "UNMEASURED", n
    assert len([ln for ln in r["lines"] if "UNMEASURED" in ln]) == 7


def test_a_restart_counts_what_the_new_process_did(tmp_path):
    """The counter resets at the restart (110 s): the pair across it counts the new process's value
    (0 here), each later pair its step. 100 before, 0 across, 9 x 10 after."""
    samples = [_sample(t, ["normal_completed"]) for t in range(0, 101, 10)]
    for t in range(110, 201, 10):
        s = _sample(t, ["normal_completed"])
        s["extra"][K["normal_completed"]] = t - 110
        samples.append(s)
    r = _evaluate(tmp_path, samples)
    assert r["normal_completed"]["by_phase"]["all"] == 190 and r["normal_completed"]["restarts"] == 1
    assert any(ln.startswith("normal_completed:") and "1 restart(s) spanned" in ln for ln in r["lines"])


def test_custlat_pair_writes_both_runs_tiers_by_role(tmp_path):
    """The paired verdict, through its CLI: B's section from its evaluate, A's verdict with none."""
    db = tmp_path / "b"
    db.mkdir()
    _evaluate(db, [_sample(t, ALL) for t in range(0, 501, 10)])
    da = tmp_path / "a"
    da.mkdir()
    (da / "verdict.json").write_text(json.dumps({"verdict": "PASS", "report": {}}))
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "pair", "--a", str(da / "verdict.json"),
                        "--b", str(db / "verdict.json")], cwd=BENCH, capture_output=True, text=True, check=False)
    lines = json.loads(p.stdout)["refresher_tiers_report_only"]["lines"]
    assert "refresher tiers, B (candidate): normal_completed: churn:list_dep@1 100, churn:list_dep@10 100, " \
           "steady 300, churn 200, all 500" in lines
    assert any(ln.startswith("refresher tiers, A (baseline): UNMEASURED (the run's verdict.json has no ")
               for ln in lines)


def test_the_tier_keys_are_1085s_names_and_not_1_12_50s():
    """As #1085 publishes them (its expvar.Publish calls at 8412e8cc). 1.12.50 serves none of them, so
    they sit apart from RUN8_KEYS, which its /debug/vars inventory pins."""
    assert sorted(load606.TIER_KEYS.values()) == sorted([
        "snowplow_refresher_normal_completed_total", "snowplow_refresher_viewed_completed_total",
        "snowplow_refresher_viewed_reserve_parks_total", "snowplow_refresher_viewed_dirty_age_ms_p95",
        "snowplow_refresher_viewed_dirty_age_ms_max", "snowplow_refresher_unviewed_dirty_age_ms_p95",
        "snowplow_refresher_unviewed_dirty_age_ms_max"])
    assert not set(load606.TIER_KEYS.values()) & set(load606.RUN8_KEYS.values())


def test_the_capture_check_names_every_tier_value_a_prefix_list_would_miss():
    """run606.sh's MEM_VARS_PREFIXES default captures them; a list without snowplow_refresher_ is
    refused, naming each of the seven, so a run never reads them as UNMEASURED by a capture slip."""
    full = load606.RUN8_VARS_PREFIXES
    assert subprocess.run([sys.executable, "-m", "bench.load606", "check-prefixes", *full], cwd=BENCH,
                          capture_output=True, text=True).returncode == 0
    narrowed = [x for x in full if x != "snowplow_refresher_"] + [
        "snowplow_refresher_dirty_", "snowplow_refresher_completed", "snowplow_refresher_enqueue",
        "snowplow_refresher_floored", "snowplow_refresher_queue"]
    p = subprocess.run([sys.executable, "-m", "bench.load606", "check-prefixes", *narrowed], cwd=BENCH,
                       capture_output=True, text=True)
    assert p.returncode == 2
    for key in K.values():
        assert key in p.stderr + p.stdout, key
