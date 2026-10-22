"""Run 12: mem606 takes CPU profiles (back to back through the login storm
and around the composite burst, periodic otherwise) on its own thread, and
samples GC pauses and goroutines; the verdict reports them, marks the
profiled windows, compares latency inside / outside them, and is INVALID only
when the storm window's profiles are missing."""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time

import pytest

from bench import k6eval, load606, load606_job
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

TOKEN = "proof-bearer-value"       # what the fakes accept; must never reach /out


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


class ProfSnow:
    """/debug/pprof/profile?seconds=N takes N seconds on the fake clock."""

    def __init__(self, clock, fail_first=0):
        self.clock, self.fail = clock, fail_first
        self.calls = 0

    def get(self, path, token):
        assert token == TOKEN
        m = re.match(r"/debug/pprof/profile\?seconds=(\d+)$", path)
        assert m, path
        self.calls += 1
        if self.fail > 0:
            self.fail -= 1
            self.clock.sleep(1)
            return load606.Response(500, b"", 1, "http 500")
        self.clock.sleep(int(m.group(1)) + 0.2)
        return load606.Response(200, b"\x1f\x8bcpu", 1)


PLAN = load606.CpuPlan(profile_s=30, storm_s=600, every_s=300, burst_at_s=1350)


def _profile(tmp_path, plan=PLAN, until=2700.0, anchor=0.0, fail_first=0):
    clk = FakeClock()
    snow = ProfSnow(clk, fail_first)
    recs = []
    n = load606.cpu_profiler(snow, plan, token_fn=lambda: TOKEN, anchor_fn=lambda: anchor,
                             stopped=lambda: clk.t >= until, out_dir=str(tmp_path),
                             sink=recs.append, clock=clk.now, sleep=clk.sleep)
    return n, recs


def test_the_plan_names_each_window():
    assert PLAN.window(0, 0) == "storm" and PLAN.window(599, 0) == "storm"
    assert PLAN.window(600, 0) is None and PLAN.window(1289, 0) is None
    assert PLAN.window(1290, 0) == "burst" and PLAN.window(1589, 0) == "burst"
    assert PLAN.window(1590, 0) is None
    # the storm runs from the sampler's start until storm_s after the load is seen
    assert PLAN.window(700, 200) == "storm" and PLAN.window(1490, 200) == "burst"
    assert PLAN.next_window_start(700, 0) == 1290 and PLAN.next_window_start(1300, 0) is None
    assert load606.CpuPlan(burst_at_s=None).window(1350, 0) is None


def test_profiles_back_to_back_in_storm_and_burst_periodic_otherwise(tmp_path):
    n, recs = _profile(tmp_path)
    ok = [r for r in recs if r["profile"]]
    assert n == len(ok) == len(recs)
    storm = [r for r in ok if r["window"] == "storm"]
    burst = [r for r in ok if r["window"] == "burst"]
    periodic = [r for r in ok if r["window"] == "periodic"]
    # back to back: each starts where the previous ended
    for w in (storm, burst):
        assert all(abs(b["t_start"] - a["t_end"]) < 1e-9 for a, b in zip(w, w[1:]))
    assert storm[0]["t_start"] == 0 and storm[-1]["t_start"] < 600 <= storm[-1]["t_end"]
    assert len(storm) == 20
    assert 1290 <= burst[0]["t_start"] < 1290 + 5 and burst[-1]["t_end"] >= 1590
    assert len(burst) == 10
    # one every 300 s outside them, never inside
    starts = [r["t_start"] for r in periodic]
    assert starts and all(b - a >= 300 for a, b in zip(starts, starts[1:]))
    assert all(PLAN.window(t, 0) is None for t in starts)
    assert all(not (600 <= r["t_start"] < 1290) or r["window"] == "periodic" for r in ok)
    # files named cpu-NN-HHMMSS.pb.gz, written next to each other
    assert all(re.fullmatch(r"cpu-\d{2,}-\d{6}\.pb\.gz", r["profile"]) for r in ok)
    assert sorted(os.listdir(tmp_path)) == sorted(r["profile"] for r in ok)


def test_a_failed_profile_is_recorded_and_retried(tmp_path):
    n, recs = _profile(tmp_path, until=100, fail_first=2)
    bad = [r for r in recs if not r["profile"]]
    assert len(bad) == 2 and bad[0]["status"] == 500 and bad[0]["error"] == "http 500"
    assert n == len(recs) - 2 and recs[2]["profile"]


def test_no_token_no_request(tmp_path):
    clk = FakeClock()
    recs = []
    load606.cpu_profiler(ProfSnow(clk), PLAN, token_fn=lambda: None, anchor_fn=lambda: 0,
                         stopped=lambda: clk.t >= 60, out_dir=str(tmp_path),
                         sink=recs.append, clock=clk.now, sleep=clk.sleep)
    assert recs == [] and os.listdir(tmp_path) == []


# ─── GC pauses, goroutines, scheduler ──────────────────────────────────────


def _ring(pauses_by_gc: dict[int, int]) -> list[int]:
    ring = [0] * 256
    for k, ns in pauses_by_gc.items():
        ring[(k + 255) % 256] = ns
    return ring


def test_gc_pauses_are_the_ones_since_the_previous_sample():
    ms = {"NumGC": 12, "PauseNs": _ring({10: 1_000_000, 11: 2_500_000, 12: 40_000})}
    assert load606.gc_pauses(ms, None) == ([], 0)         # first sample: no baseline
    assert load606.gc_pauses(ms, 10) == ([2.5, 0.04], 0)
    assert load606.gc_pauses(ms, 12) == ([], 0)
    big = {"NumGC": 400, "PauseNs": _ring({k: k * 1000 for k in range(145, 401)})}
    p, lost = load606.gc_pauses(big, 100)
    assert len(p) == 256 and lost == 44 and p[-1] == 0.4 and p[0] == 0.145


class SampleSnow:
    def __init__(self, extra=None):
        self.extra = extra or {}

    def get(self, path, token):
        if path == "/readyz":
            return load606.Response(200, b"", 1)
        if path == "/debug/vars":
            v = {"memstats": {"HeapInuse": 1048576, "NumGC": 3, "PauseTotalNs": 7_500_000,
                              "GCCPUFraction": 0.02,
                              "PauseNs": _ring({2: 3_000_000, 3: 4_500_000})}, **self.extra}
            return load606.Response(200, json.dumps(v).encode(), 1)
        if path == "/debug/pprof/goroutine?debug=1":
            return load606.Response(200, b"goroutine profile: total 4321\n1 @ 0x1\n", 1)
        return load606.Response(404, b"", 1)


def test_a_sample_carries_gc_pauses_goroutines_and_scheduler_latency_if_exposed():
    r = load606.mem_sample(SampleSnow(), TOKEN, (), prev_num_gc=1)
    assert r["gc_pauses_ms"] == [3.0, 4.5] and r["gc_pauses_lost"] == 0
    assert r["gc_pause_total_ms"] == 7.5 and r["gc_cpu_fraction"] == 0.02
    assert r["goroutines"] == 4321 and r["sched_latencies"] is None
    h = {"counts": [1, 2], "buckets": [0, 1e-6, 1e-3]}
    r = load606.mem_sample(SampleSnow({load606.SCHED_LATENCY_KEY: h}), TOKEN, ())
    assert r["sched_latencies"] == h


# ─── memwatch: the profiler on its own thread ──────────────────────────────


class ThreadSnow(SampleSnow):
    """Real time: a 1 s profile; the memstats requests stay instant."""

    def __init__(self):
        super().__init__()
        self.lock = threading.Lock()
        self.in_profile = 0
        self.samples_during_profile = 0

    def get(self, path, token):
        if path.startswith("/debug/pprof/profile"):
            assert token == TOKEN
            with self.lock:
                self.in_profile += 1
            time.sleep(1.0)
            with self.lock:
                self.in_profile -= 1
            return load606.Response(200, b"\x1f\x8bcpu", 1)
        if path == "/debug/vars":
            with self.lock:
                self.samples_during_profile += self.in_profile > 0
        return super().get(path, token)


def test_memwatch_profiles_on_its_own_thread_and_keeps_its_cadence(tmp_path):
    snow, recs = ThreadSnow(), []
    plan = load606.CpuPlan(profile_s=1, storm_s=100, every_s=300)
    out = tmp_path / "out"
    load606.memwatch(snow, lambda: TOKEN, duration_s=3.2, interval_s=0.2, burst_interval_s=0.2,
                     out_dir=str(out), sink=recs.append, cpu=plan)
    kinds = [r["kind"] for r in recs]
    assert kinds[0] == "mem_start" and kinds[-1] == "mem_end"
    assert recs[0]["cpu_plan"] == {"profile_s": 1, "storm_s": 100, "every_s": 300,
                                   "burst_at_s": None, "burst_pre_s": 60, "burst_post_s": 240}
    cpus = [r for r in recs if r["kind"] == "cpu_profile"]
    assert len(cpus) >= 2 and all(r["profile"] and r["window"] == "storm" for r in cpus)
    assert recs[-1]["cpu_profiles"] == len(cpus)
    # memstats kept sampling while a profile was in flight
    assert snow.samples_during_profile >= 5
    s = load606.framed_stream(load606.MEM_FRAMING, "mem-x.jsonl",
                              recs + [{"kind": "mem_exit", "rc": 0}])
    assert [c["result"] for c in
            load606.framed_stream_checks(load606.MEM_FRAMING, [s], required=True)] == ["PASS"]
    # the bearer never reaches /out or the stream
    for f in out.iterdir():
        assert TOKEN.encode() not in f.read_bytes(), f
    assert TOKEN not in json.dumps(recs)


class LoadSnow(ThreadSnow):
    """The load arrives at the 10th memstats sample (L1 lookups jump)."""

    def __init__(self):
        super().__init__()
        self.n = 0

    def get(self, path, token):
        if path == "/debug/vars":
            self.n += 1
            lookups = 0 if self.n < 10 else 500
            v = {"memstats": {"HeapInuse": 1048576},
                 "snowplow_dispatch_l1_lookups": {"w|x": {"hit_total": lookups, "miss_total": 0}}}
            return load606.Response(200, json.dumps(v).encode(), 1)
        return super().get(path, token)


def test_the_storm_runs_until_storm_s_after_the_load_is_seen(tmp_path):
    """The storm window moves with the load: without the load it would end
    1.5 s after the start; the load is seen at ~2 s, so profiles keep going
    back to back until ~3.5 s."""
    snow, recs = LoadSnow(), []
    plan = load606.CpuPlan(profile_s=1, storm_s=1.5, every_s=100)
    load606.memwatch(snow, lambda: TOKEN, duration_s=4.6, interval_s=0.2, burst_interval_s=0.2,
                     out_dir=str(tmp_path / "out"), sink=recs.append, cpu=plan)
    t0 = recs[0]["t"]
    seen = next(r["t"] for r in recs if r.get("load_detected"))
    assert 1.5 < seen - t0 < 3.0
    storm = [r for r in recs if r["kind"] == "cpu_profile" and r["window"] == "storm"]
    assert any(r["t_start"] > t0 + 1.5 for r in storm), [r["t_start"] - t0 for r in storm]
    assert len(storm) >= 3


def test_memwatch_cli_renders_the_plan_into_mem606():
    m = load606_job.render_mem(snowplow_url="http://s", authn_url="http://a", user="u",
                               password="pw", duration_s=60, burst_at_s=1350)
    assert "--cpu-profile-s 30" in m and "--cpu-storm-s 600" in m and "--cpu-every-s 300" in m
    assert "--burst-at-s 1350" in m
    assert "--burst-at-s" not in load606_job.render_mem(
        snowplow_url="http://s", authn_url="http://a", user="u", password="pw", duration_s=60)


def test_run606_mem606_profiles_the_composite_burst(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    mem = job[job.index("name: mem606"):]
    mem = mem[:mem.index("\n---")] if "\n---" in mem else mem
    burst = re.search(r"--burst-at-s (\d+)", mem)
    drive = re.search(r"--burst-at-s (\d+)", job[:job.index("name: mem606")]) or \
        re.search(r'"burstAtS": (\d+)', job)
    assert "--cpu-profile-s 30" in mem and burst and drive
    assert burst.group(1) == drive.group(1)          # the driver's own burst time


# ─── the verdict ────────────────────────────────────────────────────────────


def _stream(profiles: list[tuple[float, float, str]], *, anchor=100.0, end=3000.0,
            failed=0, burst_at=1350.0):
    plan = dict(profile_s=30, storm_s=600, every_s=300, burst_at_s=burst_at,
                burst_pre_s=60, burst_post_s=240)
    mem = [{"kind": "mem_start", "t": 0.0, "cpu_plan": plan},
           {"kind": "mem", "t": 15.0, "gc_pauses_ms": [], "goroutines": 50},
           {"kind": "mem", "t": anchor, "load_detected": True, "gc_pauses_ms": [1.0, 2.0],
            "gc_pauses_lost": 0, "goroutines": 900, "gc_cpu_fraction": 0.1},
           {"kind": "mem", "t": 2000.0, "gc_pauses_ms": [0.5], "goroutines": 300,
            "gc_cpu_fraction": 0.05}]
    mem += [{"kind": "cpu_profile", "t": b, "t_start": a, "t_end": b, "window": w,
             "profile": f"cpu-{i:02d}-000000.pb.gz", "seconds": 30}
            for i, (a, b, w) in enumerate(profiles, 1)]
    mem += [{"kind": "cpu_profile", "t": 50.0, "t_start": 50.0, "t_end": 51.0, "window": "storm",
             "profile": None, "status": 500, "error": "http 500"}] * failed
    mem.append({"kind": "mem_end", "t": end, "samples": 3})
    return mem


def _b2b(t0, t1, w):
    out, t = [], t0
    while t < t1:
        out.append((t, t + 30, w))
        t += 30
    return out


FULL = _b2b(0, 700, "storm") + _b2b(1390, 1690, "burst") + [(2000, 2030, "periodic")]


def test_the_storm_window_covered_passes():
    win = load606.cpu_profile_windows(_stream(FULL))
    assert win["anchor_t"] == 100.0 and win["storm"]["t0"] == 100 and win["storm"]["t1"] == 700
    assert win["storm"]["coverage"] == 1.0 and win["burst"]["coverage"] == 1.0
    assert load606.cpu_storm_check(win)["result"] == "PASS"


def test_missing_storm_profiles_make_the_run_invalid():
    lost = [p for p in FULL if not (300 <= p[0] < 420)]          # 4 of 20 gone
    mem = _stream(lost, failed=4)
    c = load606.cpu_storm_check(load606.cpu_profile_windows(mem))
    assert c["result"] == "FAIL" and c["value"] == 0.8
    assert "480.0 s of 600.0 s covered, 4 profile request(s) failed" in c["why"]
    v = load606.evaluate([], [], [], mem=mem)
    assert v["verdict"] == "INVALID"
    assert any(r.startswith("mem606.cpu_profiles_storm") for r in v["invalid_reasons"])
    # profiles elsewhere are report-only: a lost burst profile is not INVALID
    v = load606.evaluate([], [], [], mem=_stream([p for p in FULL if p[2] != "burst"]))
    assert not any(r.startswith("mem606.cpu_profiles") for r in v["invalid_reasons"])


def test_one_lost_storm_profile_is_tolerated():
    one = [p for p in FULL if p[0] != 300]
    assert load606.cpu_storm_check(load606.cpu_profile_windows(_stream(one)))["result"] == "PASS"


def test_no_plan_no_check():
    mem = [{"kind": "mem_start", "t": 0.0}, {"kind": "mem", "t": 1.0}]
    assert load606.cpu_profile_windows(mem) is None
    assert load606.cpu_storm_check(None) is None


def test_the_report_marks_profiled_windows_and_compares_latency():
    mem = _stream(FULL)
    navs = ([{"start": 2010.0, "wall_ms": 900.0}, {"start": 2100.0, "wall_ms": 300.0},
             {"start": 2200.0, "wall_ms": 310.0}, {"start": 150.0, "wall_ms": 5000.0},
             {"start": 1500.0, "wall_ms": 4000.0}])          # in the burst
    k6 = [(2015.0, 1000.0, {}), (2500.0, 400.0, {})]
    rep = load606.profiling_report(mem, navs, k6)
    cpu = rep["cpu"]
    assert cpu["written_by_window"] == {"storm": 24, "burst": 10, "periodic": 1}
    assert cpu["profiled_windows"] == [[0, 720], [1390, 1690], [2000, 2030]]
    lat = rep["latency"]["page_loads"]
    assert lat["all"]["inside_profile"]["n"] == 3 and lat["all"]["outside_profile"]["n"] == 2
    # steady = outside the storm and burst windows: only the periodic profile
    assert lat["steady"]["inside_profile"] == {"n": 1, "p50_ms": 900.0, "p95_ms": 900.0,
                                               "max_ms": 900.0}
    assert lat["steady"]["outside_profile"]["n"] == 2
    assert rep["latency"]["k6_page_loads"]["steady"]["inside_profile"]["n"] == 1
    gc = rep["gc_sched"]
    assert gc["gc_pauses"]["all"]["n"] == 3 and gc["gc_pauses"]["storm"]["max_ms"] == 2.0
    assert gc["gc_pauses"]["steady"]["n"] == 1
    assert gc["goroutines"]["storm"]["max"] == 900 and gc["goroutines"]["steady"]["max"] == 300
    assert gc["sched_latencies"].startswith("not exposed")
    assert len(rep["overhead_measured_on_kind"]["cpu_overhead_pct_mean_by_arm"]) == 2
    v = load606.evaluate(navs and [dict(n, visit=2, kind="nav") for n in navs], [], [], mem=mem)
    assert v["report"]["profiling"]["cpu"]["storm"]["coverage"] == 1.0


# ─── k6 point times ─────────────────────────────────────────────────────────


def test_k6_point_times_become_page_load_starts(tmp_path):
    import gzip
    lines = [
        {"metric": "browser_page_load_ms", "type": "Point",
         "data": {"time": "2026-10-22T09:45:39.422766631Z", "value": 2000.0,
                  "tags": {"session": "s1", "visit": "first", "route": "/d"}}},
        {"metric": "page_load_ms", "type": "Point",
         "data": {"time": "2026-10-22T11:45:41+02:00", "value": 500.0,
                  "tags": {"kind": "nav", "visit": "warm"}}}]
    with gzip.open(tmp_path / "browser.json.gz", "wt") as fh:
        fh.write("\n".join(json.dumps(x) for x in lines) + "\n")
    o = k6eval.read_k6_out(str(tmp_path), "browser")
    (nav,) = k6eval.browser_navs(o)
    assert nav["start"] == pytest.approx(1792662339.422766631 - 2.0, abs=1e-3)
    ((t, ms, tags),) = k6eval.k6_page_loads(o)
    assert t == pytest.approx(1792662341.0 - 0.5, abs=1e-3) and ms == 500.0
    assert k6eval.epoch("not a time") is None


def test_gc_over_time_reports_pause_p95_max_and_gc_cpu_fraction_per_minute():
    rows = [{"kind": "mem", "t": 0.0, "gc_pauses_ms": [1.0, 3.0], "gc_cpu_fraction": 0.01,
             "heap_alloc_mb": 100.0, "next_gc_mb": 200.0},
            {"kind": "mem", "t": 30.0, "gc_pauses_ms": [float(x) for x in range(2, 21)],
             "gc_cpu_fraction": 0.02,
             "heap_alloc_mb": 150.0, "next_gc_mb": 210.0},
            {"kind": "mem", "t": 75.0, "gc_pauses_ms": [], "gc_cpu_fraction": 0.03,
             "heap_alloc_mb": 90.0, "next_gc_mb": 180.0}]
    rep = load606.gc_sched_report(rows, None)
    assert rep["over_time"] == [
        {"t": 0.0, "gcs": 21, "pause_p95_ms": 19.0, "pause_max_ms": 20.0, "gc_cpu_fraction": 0.02,
         "heap_alloc_mb_max": 150.0, "next_gc_mb_max": 210.0},
        {"t": 60.0, "gcs": 0, "pause_p95_ms": None, "pause_max_ms": None,
         "gc_cpu_fraction": 0.03, "heap_alloc_mb_max": 90.0, "next_gc_mb_max": 180.0}]


def test_a_sample_records_the_memstats_the_gc_report_reads():
    """NumGC, NextGC, HeapAlloc, PauseTotalNs, GCCPUFraction and the PauseNs ring."""
    class S(SampleSnow):
        def get(self, path, token):
            r = super().get(path, token)
            if path == "/debug/vars":
                v = json.loads(r.body)
                v["memstats"].update(HeapAlloc=3 * 1048576, NextGC=8 * 1048576)
                return load606.Response(200, json.dumps(v).encode(), 1)
            return r
    r = load606.mem_sample(S(), TOKEN, (), prev_num_gc=2)
    assert (r["num_gc"], r["heap_alloc_mb"], r["next_gc_mb"]) == (3, 3.0, 8.0)
    assert (r["gc_pause_total_ms"], r["gc_cpu_fraction"], r["gc_pauses_ms"]) == (7.5, 0.02, [4.5])


def test_the_default_plan_profiles_t0_30_60_120_and_the_burst_at_900(tmp_path):
    """The schedule the lead asked for: the storm back to back from t=0 (0,
    30, 60, ..., 120, ...), and the composite burst at t=900 covered from 840
    to 1140."""
    plan = load606.CpuPlan(burst_at_s=900)                 # every other field: the default
    n, recs = _profile(tmp_path, plan=plan, until=1800)
    starts = [r["t_start"] for r in recs if r["profile"]]
    for t in (0, 30, 60, 120):
        assert any(abs(s - t) < 2 for s in starts), (t, starts[:6])
    burst = [r for r in recs if r["window"] == "burst"]
    assert burst[0]["t_start"] <= 840 + 30 and burst[-1]["t_end"] >= 1140
    assert any(r["t_start"] <= 900 < r["t_end"] for r in recs)


# ─── #839 gate: storm coverage from what each profile recorded itself ──────


def _varint(v):
    out = b""
    while True:
        b, v = v & 0x7F, v >> 7
        out += bytes([b | (0x80 if v else 0)])
        if not v:
            return out


def _pprof(start_s: float, duration_s: float) -> bytes:
    """A gzipped profile.proto with a string table entry, time_nanos (9)
    and duration_nanos (10), as Go writes them."""
    import gzip as gz
    s = b"cpu"
    msg = (_varint(6 << 3 | 2) + _varint(len(s)) + s
           + _varint(9 << 3) + _varint(int(start_s * 1e9))
           + _varint(10 << 3) + _varint(int(duration_s * 1e9)))
    return gz.compress(msg)


def test_pprof_window_reads_the_profiles_own_start_and_duration():
    w = load606.pprof_window(_pprof(1792672820.0105, 30.138))
    assert abs(w[0] - 1792672820.0105) < 1e-6 and abs(w[1] - 30.138) < 1e-6
    assert load606.pprof_window(b"\x1f\x8bnot gzip") is None
    assert load606.pprof_window(_pprof(1, 2)[:-4]) is None          # a cut file: None, no raise


def test_a_profile_queued_behind_the_storm_covers_only_what_it_recorded():
    """Requests back to back over the whole storm, but each profile only ran
    the last 20 s of its 30 s request (queued 10 s): coverage is 2/3."""
    profs = []
    for a, b, w in _b2b(0, 700, "storm"):
        profs.append((a, b, w))
    mem = _stream(profs + FULL[-11:])
    for r in mem:
        if r.get("kind") == "cpu_profile" and r.get("window") == "storm":
            r.update(prof_start=r["t_start"] + 10, prof_duration_s=20.0)
    win = load606.cpu_profile_windows(mem)
    assert 0.6 < win["storm"]["coverage"] < 0.7
    assert load606.cpu_storm_check(win)["result"] == "FAIL"


class PprofSnow(ProfSnow):
    """The profile's body is a real profile.proto; one request is slow."""

    def get(self, path, token):
        self.calls += 1
        t0 = self.clock.t
        self.clock.sleep(30 + (12 if self.calls == 2 else 0.2))
        return load606.Response(200, _pprof(t0 + (12 if self.calls == 2 else 0), 30.0), 1)


def test_the_profiler_records_the_profiles_window_and_flags_a_slow_request(tmp_path):
    clk = FakeClock()
    recs = []
    load606.cpu_profiler(PprofSnow(clk), PLAN, token_fn=lambda: TOKEN, anchor_fn=lambda: 0,
                         stopped=lambda: clk.t >= 100, out_dir=str(tmp_path),
                         sink=recs.append, clock=clk.now, sleep=clk.sleep)
    assert recs[0]["prof_start"] == recs[0]["t_start"] and recs[0]["prof_duration_s"] == 30.0
    assert "slow_request" not in recs[0]
    assert recs[1]["slow_request"] is True and recs[1]["prof_start"] == recs[1]["t_start"] + 12
    win = load606.cpu_profile_windows(
        [{"kind": "mem_start", "t": 0.0, "cpu_plan": dict(profile_s=30, storm_s=600, every_s=300,
                                                           burst_at_s=None, burst_pre_s=60,
                                                           burst_post_s=240)}]
        + recs + [{"kind": "mem_end", "t": 200.0}])
    assert win["slow_requests"] == 1
    assert win["profiles"][1]["t_start"] == recs[1]["prof_start"]
