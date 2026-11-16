"""The load starts right after the pod's Ready in every arm (the owner's ruling, 2026-11-16).

Start-up is over when the readiness probe makes the pod Ready. So in every arm the load Jobs are applied
right after the reboot, the digest gate and the pod record, and nothing of variable time stands between
them. The Go runtime read (the GC settings the process runs with) runs ALONGSIDE the load, its deadline the
new pod's Ready + GO_RUNTIME_S (the same anchor in every arm, from pod-before.jsonl's ready_at). The run
counts only if the read answered by then and as declared (evaluate: INVALID, named). The arms' readings are
compared AFTER the measure, by the pair (custlat pair and replica): a key no arm declared that differs makes
the pair INVALID, named. Per arm the record and the report say when the pod was Ready, when the load started
(its offset from Ready) and when the read answered; the pair states both offsets and their difference (the
bench holds no tolerance for it: reported, not judged).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

from bench import custlat, snowplow_env
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)


def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def _job_args(state):
    job = json.load(open(state / "job-runtime606"))
    return " ".join(job["spec"]["template"]["spec"]["containers"][0]["args"])


def test_the_load_starts_right_after_ready_and_the_read_runs_alongside_it(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    # the order of the measure: the pod record, the load, then the read (never the read before the load)
    m = log[log.index("=== step measure ==="):]
    i = [m.index(x) for x in ("snowplow pod (before):", "loadgen-job.yaml", "load start: the drivers applied at Ready + ",
                              "go-runtime-job.yaml", "snowplow env verified from the running process")]
    assert i == sorted(i), i
    # the anchor is the pod's Ready, not when the read (or anything else) returned
    ready = json.loads((out / "pod-before.jsonl").read_text().splitlines()[0])["ready_at"]
    ls = json.loads((out / "load-start.json").read_text())
    assert ls["ready_at"] == ready and ls["offset_s"] == round(ls["applied_at"] - ready, 1)
    assert f"--deadline {int(ready) + 300}" in _job_args(state)          # Ready + GO_RUNTIME_S (300 by default)
    # the record, the run report and the validity check
    rec = json.loads((out / "snowplow-env.json").read_text())
    assert rec["load_start"]["offset_s"] == ls["offset_s"] and rec["load_start"]["read_answered_from_ready_s"] is not None
    v = json.loads((out / "verdict.json").read_text())
    chk = next(c for c in v["validity"] if c["check"] == "snowplow_env.verified")
    assert chk["result"] == "PASS"
    said = "\n".join(v["report"]["snowplow_env"]["lines"])
    assert f"INFO load start: load starts at Ready + {ls['offset_s']} s in this arm, and in every arm right after " \
           f"the reboot, the digest gate and the pod record" in said
    assert re.search(r"the drivers applied at \d\d:\d\d:\d\dZ = Ready \+ [0-9.]+ s; the Go runtime read answered at "
                     r"Ready \+ -?[0-9.]+ s", said), said


def test_a_stalled_read_still_counts_when_it_answers_by_its_deadline(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", FAKE_GO_RUNTIME_FAIL="stall")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    v = json.loads((out / "verdict.json").read_text())
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    assert next(c for c in v["validity"] if c["check"] == "snowplow_env.verified")["result"] == "PASS"
    assert not [r for r in v["invalid_reasons"] if "snowplow_env" in r]
    assert f"{snowplow_env.READ_FINDING}: STALLED: 1 attempt(s) with no answer" in log
    rec = json.loads((out / "snowplow-env.json").read_text())
    # the read answered after the load had started: its answer is ~35 s after the read's first attempt
    assert rec["load_start"]["read_answered_from_ready_s"] > rec["load_start"]["offset_s"]


def test_a_measuring_run_without_its_pods_ready_time_applies_no_load(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", FAKE_NO_READY_TIME="1")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "the new snowplow pod's Ready time could not be read" in log, log[-3000:]
    assert "load start: the drivers applied" not in log


# ─── the pair: both offsets, and the arms' env compared after the measure ───

def _rec(arm, gomemlimit, offset, *, verified=True, gogc="50"):
    return {"kind": "snowplow_env", "arm": arm, "verified": verified, "declared": {"GOGC": gogc},
            "effective": {"gogc": int(gogc), "memory_limit_bytes": 7516192768, "memory_limit_source": "cgroup_v2",
                          "cgroup_memory_limit_bytes": 8589934592},
            "undeclared": {"GOMEMLIMIT": {"state": "set", "value": gomemlimit}},
            "load_start": {"kind": "load_start", "ready_at": 1000.0, "applied_at": 1000.0 + offset, "offset_s": offset}}


def _verdict(rec):
    return {"verdict": "PASS", "report": {"snowplow_env": dict(rec, state="VERIFIED" if rec["verified"] else
                                                               "NOT_VERIFIED"),
                                          "image_digest": {"expected": "sha256:" + "ab" * 32}}}


def test_the_pair_states_both_load_start_offsets_and_their_difference_without_judging_it():
    out = snowplow_env.pair(_verdict(_rec("A", "7GiB", 4.2)), _verdict(_rec("B", "7GiB", 6.0, gogc="100")),
                            ("A (baseline)", "B (candidate)"))
    assert out["load_start_offsets_s"] == {"A (baseline)": 4.2, "B (candidate)": 6.0} and out["disagree"] == []
    assert ("INFO load start: A (baseline) at Ready + 4.2 s, B (candidate) at Ready + 6.0 s: a difference of 1.8 s "
            "(the bench holds no tolerance for it: reported, not judged); in every arm the load starts right after "
            "the reboot, the digest gate and the pod record") in out["lines"]


def test_arms_whose_undeclared_env_differs_make_the_pair_invalid_after_the_measure(tmp_path, capsys):
    fa, fb = tmp_path / "a.json", tmp_path / "b.json"
    fa.write_text(json.dumps(_verdict(_rec("A", "7GiB", 4.2))))
    fb.write_text(json.dumps(_verdict(_rec("B", "6GiB", 4.0, gogc="100"))))
    rc = custlat.main(["pair", "--a", str(fa), "--b", str(fb), "--a-label", "N1", "--b-label", "N2"])
    out = json.loads(capsys.readouterr().out)
    assert rc == 3 and out["verdict"] == "INVALID"
    assert ("customer_latency.pair.snowplow_env_agreement: GOMEMLIMIT is declared on no run and differs between "
            "them: N1 (baseline): GOMEMLIMIT=7GiB, N2 (candidate): GOMEMLIMIT=6GiB") in out["invalid_reasons"]
    # the same arms, agreeing: no such reason (whatever else the minimal verdicts make of the pair)
    fb.write_text(json.dumps(_verdict(_rec("B", "7GiB", 4.0, gogc="100"))))
    custlat.main(["pair", "--a", str(fa), "--b", str(fb), "--a-label", "N1", "--b-label", "N2"])
    out = json.loads(capsys.readouterr().out)
    assert not [r for r in out.get("invalid_reasons") or [] if "snowplow_env_agreement" in r]


def test_an_a_a_replica_whose_arms_disagree_cannot_calibrate(tmp_path):
    dirs = []
    for arm, lim in (("N1", "7GiB"), ("N1B", "6GiB")):
        d = tmp_path / arm
        d.mkdir()
        (d / "verdict.json").write_text(json.dumps(_verdict(_rec(arm, lim, 4.0))))
        dirs.append(str(d))
    out = custlat.replica_in_run((dirs[0], dirs[1]), "N1:N1B", "sha256:" + "ab" * 32, (True, True))
    assert out["in_run"]["usable"] is False
    assert ("the two runs' snowplow env, compared after the measure: GOMEMLIMIT is declared on no run and differs "
            "between them: N1: GOMEMLIMIT=7GiB, N1B: GOMEMLIMIT=6GiB") in out["in_run"]["why"]


def test_evaluate_makes_a_run_whose_read_is_not_verified_invalid_named():
    ok = snowplow_env.validity({"state": "VERIFIED"})
    bad = snowplow_env.validity({"state": "NOT_VERIFIED", "why": "the settings the process runs with could not be read"})
    none = snowplow_env.validity({"state": "UNKNOWN", "why": "the run has no snowplow-env record"})
    assert ok["result"] == "PASS" and bad["result"] == "FAIL" and none["result"] == "FAIL"
    assert bad["why"].startswith("snowplow env NOT as declared (the Go runtime read's deadline is the new pod's Ready")
    assert "could not be read" in bad["why"]


def test_the_dry_pass_says_the_load_starts_before_the_read(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", SNOWPLOW_ENV="GOGC=50")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    m = log[log.index("=== step measure ==="):]
    i = [m.index(x) for x in ("DRY: snowplow pod record (before)", "loadgen-job.yaml", "DRY: load start: the drivers applied",
                              "--go-runtime-deadline <Ready + 300s>", "DRY: python3 -m bench.snowplow_env check")]
    assert i == sorted(i), i
    # data606's pod is checked gone before the restart (s_reboot), so nothing of variable time stands
    # between the new pod's Ready and the load
    src = (BENCH_DIR / "run606.sh").read_text()
    reboot = src[src.index("s_reboot() {"):src.index("s_digest() {")]
    assert reboot.index("data606_gone || return 1") < reboot.index("rollout restart deploy/snowplow")
