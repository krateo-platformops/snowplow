"""#825: the load606 driver as N pods (Diego's ruling after run 10: 4 x 250).

Run 10 was INVALID only on driver schedule lag (p95 14.2 s): one GIL-bound
process and 128 navigation workers for 1000 users, swamped by the first-load
arrival. These arms pin the split: the Job, the shard map, the burst split,
the node budget, collect of every pod's stream, and the per-pod + overall
checks the evaluator now makes.
"""

from __future__ import annotations

import json
import subprocess

import pytest
import yaml

from bench import load606, load606_job
from tests.test_navmodel import INIT
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

KW = dict(users=1000, shards=4, think_s=60, duration_s=1800, snowplow_url="http://s",
          authn_url="http://a", password="pw", init=INIT, src="#",
          composite_routes=["/dashboard"], burst_users=200, burst_at_s=900)


def _job(**over):
    docs = list(yaml.safe_load_all(load606_job.render(**dict(KW, **over))))
    return next(d for d in docs if d and d.get("kind") == "Job" and d["metadata"]["name"] == "load606")


def _shard_env(script: str, index: int) -> dict:
    """Run the pod script's own shard selection for JOB_COMPLETION_INDEX."""
    head = script.split("rc=0\n", 1)[0]
    out = subprocess.run(["sh", "-c", head + 'echo "$S $E $BURST $NAVW"'],
                         env={"JOB_COMPLETION_INDEX": str(index), "PATH": "/usr/bin:/bin"},
                         capture_output=True, text=True, check=True).stdout.split()
    return dict(zip(("S", "E", "BURST", "NAVW"), map(int, out)))


# ─── the Job ────────────────────────────────────────────────────────────────


def test_an_indexed_job_of_four_pods_with_disjoint_ranges_and_one_split_burst():
    job = _job()
    assert job["spec"]["completionMode"] == "Indexed"
    assert job["spec"]["completions"] == job["spec"]["parallelism"] == 4
    script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    shards = [_shard_env(script, i) for i in range(4)]
    assert [(s["S"], s["E"]) for s in shards] == [(1, 250), (251, 500), (501, 750), (751, 1000)]
    assert [s["BURST"] for s in shards] == [50, 50, 50, 50]        # 200 once, not 4 x 200
    assert all(s["NAVW"] == 250 for s in shards)                    # one worker per user
    assert "--login-max-concurrency 16" in script                   # 4 x 16 = run 10's 64
    assert "--burst-users \"$BURST\"" in script and "--burst-at-s 900" in script


def test_driver_requests_fit_the_loadgen_node():
    c = _job()["spec"]["template"]["spec"]["containers"][0]
    assert c["resources"]["requests"] == {"cpu": "1", "memory": "1Gi"}
    fit = load606_job.loadgen_fit(4, "1", "1Gi")
    assert fit["cpu_m"] == 4 * 1000 + 1000 + 250 <= fit["budget_cpu_m"]
    with pytest.raises(ValueError, match="over budget"):
        load606_job.loadgen_fit(8, "1", "1Gi")                       # 9.25 CPU
    assert load606_job.loadgen_fit(8, "600m", "1Gi")["cpu_m"] == 6050


def test_the_burst_split_never_exceeds_a_shard_and_sums_to_the_cohort():
    ranges = load606_job.shard_ranges(1001, 4)
    split = load606_job.split_burst(203, ranges)
    assert sum(split) == 203 and max(split) - min(split) <= 1
    with pytest.raises(ValueError):
        load606_job.split_burst(5, load606_job.shard_ranges(4, 4))   # 2 > 1 user per shard


def test_mem606_now_requests_resources():
    m = load606_job.render_mem(snowplow_url="http://s", authn_url="http://a", user="u",
                               password="pw", duration_s=60)
    c = next(d for d in yaml.safe_load_all(m) if d and d.get("kind") == "Job")
    assert c["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"] == {
        "cpu": "250m", "memory": "512Mi"}


# ─── the evaluator: per pod and overall ────────────────────────────────────


def _stream(i, k=250, lag=1.0, navs=4):
    recs = [{"kind": "drive_start", "t": 0, "user_start": i * k + 1, "user_end": i * k + k}]
    recs += [{"kind": "nav", "nav": "route", "visit": 2, "wall_ms": 50, "calls": 3, "errors": 0,
              "sched_lag_ms": lag, "start": 10.0 + j, "user": f"scaleuser-{i * k + 1:04d}"}
             for j in range(navs)]
    recs += [{"kind": "drive_end", "ok": True, "rc": 0, "navs": navs},
             {"kind": "drive_exit", "rc": 0}]
    return recs


def _evaluate(tmp_path, capsys, streams: dict, *extra):
    paths = []
    for name, recs in streams.items():
        p = tmp_path / name
        p.write_text("".join(json.dumps(r) + "\n" for r in recs))
        paths.append(str(p))
    rc = load606.main(["evaluate", *paths, *extra])
    return rc, json.loads(capsys.readouterr().out)


def _checks(v, prefix):
    return {c["check"]: c for c in v["validity"] if c["check"].startswith(prefix)}


def test_four_whole_streams_merge_and_pass_every_shard_check(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i) for i in range(4)}
    _, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4", "--expect-users", "1000")
    assert v["report"]["navs"] == 16
    c = _checks(v, "driver.")
    assert c["driver.streams"]["result"] == "PASS"
    assert c["driver.user_ranges"]["result"] == "PASS"
    assert [k for k in c if k.startswith("driver.sched_lag_p95_ms[")] == [
        f"driver.sched_lag_p95_ms[nav-load606-{i}-x.jsonl]" for i in range(4)]


def test_one_lagging_pod_is_invalid_even_when_the_overall_p95_passes(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i, navs=30) for i in range(3)}
    s["nav-load606-3-x.jsonl"] = _stream(3, lag=5000.0, navs=3)     # 3 of 93 navs: < 5 %
    rc, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4")
    c = _checks(v, "driver.")
    assert c["driver.sched_lag_p95_ms"]["result"] == "PASS"          # overall hides it
    assert c["driver.sched_lag_p95_ms[nav-load606-3-x.jsonl]"]["result"] == "FAIL"
    assert rc == 3


def test_a_missing_pod_stream_is_invalid_and_named(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i) for i in (0, 1, 3)}
    pods = [{"kind": "job_pod", "job": "load606", "pod": f"load606-{i}-x", "phase": "Running",
             "containers": []} for i in range(4)]
    s["job-pods.jsonl"] = pods
    rc, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4", "--expect-users", "1000")
    assert rc == 3
    reasons = "\n".join(v["invalid_reasons"])
    assert "driver.streams: collected 3 of 4 driver streams; no stream from pod(s) load606-2-x" \
        in reasons
    assert "users 501-750 belong to no stream" in reasons


@pytest.mark.parametrize("ranges,why", [
    ([(1, 300), (251, 500)], "overlap"),
    ([(1, 250), (252, 500)], "users 251-251 belong to no stream"),
    ([(1, 250), (251, 400)], "the streams end at user 400, not 500"),
])
def test_user_ranges_must_be_disjoint_and_cover_every_user(tmp_path, capsys, ranges, why):
    s = {}
    for i, (a, b) in enumerate(ranges):
        st = _stream(0)
        st[0] = dict(st[0], user_start=a, user_end=b)
        s[f"nav-load606-{i}-x.jsonl"] = st
    _, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "2", "--expect-users", "500")
    c = _checks(v, "driver.user_ranges")["driver.user_ranges"]
    assert c["result"] == "FAIL" and why in c["why"]


def test_login_report_is_correct_across_concurrent_pods():
    curve = lambda c, wall: [{"phase": "round1", "concurrency": c, "median_ms": 3000,   # noqa: E731
                              "batch_wall_ms": wall}]
    rep = load606.login_report([{"users": 250, "failed": 0, "curve": curve(16, 20000)},
                                {"users": 250, "failed": 0, "curve": curve(16, 30000)}])
    assert rep["users"] == 500 and rep["pods"] == 2
    assert rep["max_concurrency"] == 16 and rep["max_concurrency_all_pods"] == 32
    assert rep["total_batch_wall_s"] == 30.0                         # concurrent: the max


def test_dead_tabs_and_the_composite_burst_are_reported_across_pods(tmp_path, capsys):
    s = {}
    for i in range(2):
        st = _stream(i)
        st.insert(1, {"kind": "viewer_dead", "t": 5, "tabs_dead": i + 1, "tabs_armed": 250,
                      "dead_keys_seen": 3})
        st.insert(2, {"kind": "nav", "nav": "composite_first_load", "visit": 1, "wall_ms": 900,
                      "calls": 52, "errors": 0, "sched_lag_ms": 1, "start": 900.0 + 2 * i,
                      "user": "u"})
        st[-2] = dict(st[-2], navs=5)
        s[f"nav-load606-{i}-x.jsonl"] = st
    _, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "2")
    vd = v["report"]["viewer_dead_report_only"]
    assert vd["tabs_dead_last"] == 3 and vd["pods"] == 2
    assert v["report"]["composite_burst"] == {"users": 2, "failed": 0, "arrival_spread_s": 2.0,
                                              "wall_p50_ms": 900, "wall_p95_ms": 900}


# ─── run606 end to end (fake tools): four pods ─────────────────────────────


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def _four(env, **extra):
    env.update(DRIVER_SHARDS="4", USERS="8", FAKE_DRIVER_PODS="4", FAKE_USERS_PER_POD="2",
               COMPOSITE_BURST_USERS="4", **extra)
    return env


def test_run606_collects_and_checks_every_driver_pod(harness):
    env, state, ambient, tmp = harness
    p = _run(_four(env))
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    out = tmp / "out"
    assert sorted(f.name for f in out.glob("nav-*.jsonl")) == [
        f"nav-load606-{i}.jsonl" for i in range(4)]
    job = (out / "loadgen-job.yaml").read_text()
    assert "completions: 4" in job and "parallelism: 4" in job
    v = json.loads((out / "verdict.json").read_text())
    c = _checks(v, "driver.")
    assert c["driver.streams"]["result"] == "PASS" and c["driver.user_ranges"]["result"] == "PASS"
    assert sum(k.startswith("driver.sched_lag_p95_ms[") for k in c) == 4
    assert "driver done: 4/4 load606 pod(s) Ready" in p.stderr


def test_run606_names_the_pod_whose_stream_is_missing(harness):
    env, state, ambient, tmp = harness
    _run(_four(env, FAKE_BAD_POD="2", FAKE_DRIVE_STREAM="nofile"))
    log = (tmp / "out" / "run.log").read_text()
    assert "could not copy the driver's results (/out/nav.jsonl) from load606-2" in log
    assert ("INVALID: driver.streams: collected 3 of 4 driver streams; no stream from pod(s) "
            "load606-2") in log


def test_run606_names_the_pod_whose_stream_is_cut(harness):
    env, state, ambient, tmp = harness
    _run(_four(env, FAKE_BAD_POD="1", FAKE_DRIVE_STREAM="headcut"))
    log = (tmp / "out" / "run.log").read_text()
    assert "INVALID: driver.stream_complete[nav-load606-1.jsonl]: the stream starts mid-run" in log


def test_run606_a_lagging_pod_is_invalid(harness):
    env, state, ambient, tmp = harness
    _run(_four(env, FAKE_LAGGING_POD="3", FAKE_POD_LAG_MS="5000"))
    log = (tmp / "out" / "run.log").read_text()
    assert "INVALID: driver.sched_lag_p95_ms[nav-load606-3.jsonl]" in log


def test_run606_eight_shards_fit_at_a_smaller_cpu_request(harness):
    env, state, ambient, tmp = harness
    env.update(DRIVER_SHARDS="8", USERS="16", DRIVER_CPU="600m", FAKE_DRIVER_PODS="8",
               FAKE_USERS_PER_POD="2", COMPOSITE_BURST_USERS="8")
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert 'cpu: "600m"' in job and "completions: 8" in job


def test_run606_refuses_a_plan_the_loadgen_node_cannot_hold(harness):
    env, state, ambient, tmp = harness
    env.update(DRIVER_SHARDS="8", USERS="16")                          # 8 x 1 CPU + 1.25
    p = _run(env)
    assert p.returncode != 0
    assert "refusing the load plan: loadgen node over budget" in p.stderr
