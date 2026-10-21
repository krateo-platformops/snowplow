"""#827 gate (review827.md at 1e075fae): one arm per finding.

1. The call pool can hide driver queueing inside page-load time: the pool is
   now sized for every in-flight page's calls, and each call's wait for a
   worker is MEASURED and checked (driver.call_wait_p95_ms), overall and per
   pod. Reproduced against a real HTTP server.
2. Each driver pod samples its own CPU into its stream; the verdict reports
   per-pod peak / p95 and names saturated pods.
3. Error rate and warm navs are checked per pod as well as overall.
4. Test gaps: the CPU headroom margin; the stream count without job_pod records.
5. Both 4 and 8 pods render and evaluate end to end.
"""

from __future__ import annotations

import concurrent.futures
import http.server
import json
import subprocess
import threading
import time

import pytest

from bench import load606, load606_job
from tests.test_driver_shards_825 import _checks, _evaluate, _job, _run, _stream
from tests.test_nav_stream_817 import portal  # noqa: F401  (fixture)
from tests.test_navmodel import INIT
from tests.test_run606_real_sequence import harness  # noqa: F401  (fixture)

# ─── 1. call-pool queueing ──────────────────────────────────────────────────


class _Slow(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(0.1)                      # every call takes 100 ms server-side
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


@pytest.fixture(scope="module")
def slow_server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Slow)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _pages(url, pages, pool_size):
    """`pages` concurrent page walks of 6 calls each on ONE shared call pool."""
    client = load606.HttpClient(url)
    pool = concurrent.futures.ThreadPoolExecutor(pool_size)
    roots = [f"/call?resource=x&name=w{i}" for i in range(6)]
    with concurrent.futures.ThreadPoolExecutor(pages) as nav:
        walks = list(nav.map(lambda _: load606.walk(client, "t", roots, pool=pool),
                             range(pages)))
    pool.shutdown()
    return walks


def _navs(walks):
    return [{"kind": "nav", "nav": "route", "visit": 2, "wall_ms": w.wall_ms, "calls": w.calls,
             "errors": w.errors, "sched_lag_ms": 1, "call_wait_max_ms": w.wait_max_ms}
            for w in walks]


def test_a_starved_call_pool_inflates_page_load_and_fails_the_wait_check(slow_server):
    walks = _pages(slow_server, pages=20, pool_size=12)          # 120 calls, 12 workers
    wall = sorted(w.wall_ms for w in walks)
    slowest = max(w.slowest_ms for w in walks)
    assert slowest < 300                                          # snowplow: ~100 ms a call
    assert wall[len(wall) // 2] > 2 * slowest                     # the page: queued in the driver
    v = load606.evaluate(_navs(walks), [], [])
    c = {x["check"]: x for x in v["validity"]}["driver.call_wait_p95_ms"]
    assert c["result"] == "FAIL" and c["value"] > 50


def test_a_pool_sized_for_every_page_adds_no_wait(slow_server):
    walks = _pages(slow_server, pages=20, pool_size=20 * 6)
    v = load606.evaluate(_navs(walks), [], [])
    c = {x["check"]: x for x in v["validity"]}["driver.call_wait_p95_ms"]
    assert c["result"] == "PASS", c


@pytest.mark.parametrize("shards,cpu,navw,callw", [(4, "1", 250, 250 * 6 + 250),
                                                 (8, "600m", 128, 128 * 6 + 125)])
def test_the_job_sizes_each_pods_call_pool_for_its_pages(shards, cpu, navw, callw):
    script = _job(shards=shards, burst_users=8 * shards, driver_cpu=cpu)["spec"]["template"]["spec"][
        "containers"][0]["args"][0]
    assert '--call-workers "$CALLW"' in script
    head = script.split("rc=0\n", 1)[0]
    for i in range(shards):
        out = subprocess.run(["sh", "-c", head + 'echo "$NAVW $CALLW"'],
                             env={"JOB_COMPLETION_INDEX": str(i), "PATH": "/usr/bin:/bin"},
                             capture_output=True, text=True, check=True).stdout.split()
        assert list(map(int, out)) == [navw, callw]


def test_call_wait_is_checked_per_pod(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i, navs=60) for i in range(4)}
    for r in s["nav-load606-2-x.jsonl"]:
        if r["kind"] == "nav":
            r["call_wait_max_ms"] = 400.0                         # pod 2's pool starved
    for i in (0, 1, 3):
        for r in s[f"nav-load606-{i}-x.jsonl"]:
            if r["kind"] == "nav":
                r["call_wait_max_ms"] = 0.2
    rc, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4")
    c = _checks(v, "driver.call_wait")
    assert c["driver.call_wait_p95_ms[nav-load606-2-x.jsonl]"]["result"] == "FAIL"
    assert c["driver.call_wait_p95_ms[nav-load606-0-x.jsonl]"]["result"] == "PASS"
    assert rc == 3


# ─── 2. per-pod CPU ─────────────────────────────────────────────────────────


def test_the_cpu_sampler_reports_cores_used_between_samples():
    reads = iter([(10.0, "cgroup"), (10.5, "cgroup"), (11.4, "cgroup")])
    clock = iter([100.0, 101.0, 102.0])
    recs, n = [], [0]

    class Stop:                                  # two samples, then stop
        def wait(self, s):
            n[0] += 1
            return n[0] > 2
    load606.cpu_sampler(recs.append, Stop(), 1.0, read=lambda: next(reads),
                        clock=lambda: next(clock))
    assert [(r["kind"], r["cores"], r["source"]) for r in recs] == [
        ("driver_cpu", 0.5, "cgroup"), ("driver_cpu", 0.9, "cgroup")]



def test_the_verdict_reports_each_pods_cpu_and_names_the_saturated(tmp_path, capsys):
    s = {}
    for i, cores in enumerate(([0.2, 0.3, 0.25], [0.9, 0.95, 0.92])):
        st = _stream(i)
        for c in cores:
            st.insert(1, {"kind": "driver_cpu", "t": 1, "cores": c, "source": "cgroup"})
        s[f"nav-load606-{i}-x.jsonl"] = st
    _, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "2")
    rep = v["report"]["driver_cpu_report_only"]
    assert rep["by_pod"]["nav-load606-0-x.jsonl"]["peak_cores"] == 0.3
    assert rep["by_pod"]["nav-load606-1-x.jsonl"]["p95_cores"] >= 0.9
    assert rep["saturated_pods"] == ["nav-load606-1-x.jsonl"]


def test_drive_samples_its_own_cpu_into_its_stream(portal, tmp_path):
    """The real `drive` against an HTTP portal: driver_cpu records land
    inside the framed stream (before drive_end), and the evaluator turns
    them into the pod's CPU report."""
    out = tmp_path / "nav-load606-0-x.jsonl"
    rc = load606.main(["drive", "--snowplow-url", portal, "--authn-url", portal,
                       "--password", "pw", "--user-start", "1", "--user-end", "3",
                       "--init", INIT, "--think-s", "0.3", "--duration-s", "1.5",
                       "--vars-interval", "0", "--cpu-interval", "0.2",
                       "--nav-workers", "8", "--call-workers", "32", "--out", str(out)])
    recs = [json.loads(x) for x in out.read_text().splitlines()]
    kinds = [r["kind"] for r in recs]
    assert rc == 0 and kinds.count("driver_cpu") >= 3
    assert kinds.index("driver_cpu") < kinds.index("drive_end") and kinds[-1] == "drive_end"
    assert all(r["cores"] >= 0 for r in recs if r["kind"] == "driver_cpu")
    s = load606.framed_stream(load606.DRIVER_FRAMING, str(out), recs)
    assert s is not None


# ─── 3. error rate and warm navs per pod ───────────────────────────────────


def test_one_pods_errors_fail_its_own_check_while_overall_passes(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i, navs=60) for i in range(4)}
    for r in s["nav-load606-1-x.jsonl"][1:4]:
        r["errors"] = 1                                            # 3 of 180 calls in pod 1
    rc, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4")
    c = _checks(v, "driver.error_rate")
    assert c["driver.error_rate"]["result"] == "PASS"             # 3 / 720 overall
    assert c["driver.error_rate[nav-load606-1-x.jsonl]"]["result"] == "FAIL"
    assert rc == 3


def test_an_idle_pod_fails_its_share_of_the_warm_nav_floor(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i, navs=80) for i in range(3)}
    s["nav-load606-3-x.jsonl"] = _stream(3, navs=10)               # 10 < ceil(200 / 4)
    rc, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4")
    c = _checks(v, "driver.warm_navs")
    assert c["driver.warm_navs"]["result"] == "PASS"               # 250 overall
    assert c["driver.warm_navs[nav-load606-3-x.jsonl]"]["result"] == "FAIL"
    assert c["driver.warm_navs[nav-load606-3-x.jsonl]"]["bar"] == ">= 50"


# ─── 4. the two surviving mutations ─────────────────────────────────────────


def test_six_one_cpu_drivers_are_refused_only_because_of_the_headroom():
    """6 x 1 CPU + fresh + mem = 7250m: under allocatable (7910m), over the
    budget (allocatable minus 1000m headroom)."""
    assert 6 * 1000 + 1250 < load606_job.LOADGEN_ALLOCATABLE_MCPU
    with pytest.raises(ValueError, match="over budget"):
        load606_job.loadgen_fit(6, "1", "1Gi")


def test_a_missing_stream_fails_the_count_even_without_job_pod_records(tmp_path, capsys):
    s = {f"nav-load606-{i}-x.jsonl": _stream(i) for i in range(3)}
    rc, v = _evaluate(tmp_path, capsys, s, "--expect-driver-streams", "4")
    c = _checks(v, "driver.streams")["driver.streams"]
    assert c["result"] == "FAIL" and c["why"] == "collected 3 of 4 driver streams"
    assert rc == 3


# ─── 5. four and eight pods, end to end ────────────────────────────────────


@pytest.mark.parametrize("shards,cpu", [("4", "1"), ("8", "600m")])
def test_four_and_eight_pods_render_and_evaluate(harness, shards, cpu):
    env, state, ambient, tmp = harness
    n = int(shards)
    env.update(DRIVER_SHARDS=shards, DRIVER_CPU=cpu, USERS=str(2 * n), FAKE_DRIVER_PODS=shards,
               FAKE_USERS_PER_POD="2", COMPOSITE_BURST_USERS=shards)
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    c = _checks(v, "driver.")
    assert c["driver.streams"]["result"] == "PASS" and c["driver.streams"]["value"] == n
    assert c["driver.user_ranges"]["result"] == "PASS"
    for name in ("sched_lag_p95_ms", "error_rate", "warm_navs"):
        assert sum(k.startswith(f"driver.{name}[") for k in c) == n, name
