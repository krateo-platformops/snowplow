"""#710 capture resolution: the run-7 heap went 0.6 -> 6.35 GB in 16 s, so
15 s samples see at most one point inside the burst. memwatch samples every
5 s until the load is detected and for 60 s after, then every 15 s; and the
run records the PR-0 fill counters by default."""

from __future__ import annotations

import json
import subprocess

from bench import load606
from tests.test_measure_validity import Clock
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401


def test_memwatch_samples_fast_through_the_burst_then_slows(tmp_path):
    clk = Clock()
    lookups = iter([0, 10, 20, 150] + [400] * 200)

    class S:
        def get(self, path, token):
            if path == "/readyz":
                return load606.Response(200, b"", 1)
            if path == "/debug/vars":
                v = {"memstats": {"HeapInuse": 1048576},
                     "snowplow_dispatch_l1_lookups": {"w|x": {"hit_total": next(lookups),
                                                             "miss_total": 0}}}
                return load606.Response(200, json.dumps(v).encode(), 1)
            return load606.Response(200, b"\x1f\x8b", 1)

    recs = []
    load606.memwatch(S(), lambda: "tok", duration_s=200, interval_s=15,
                     profile_every_s=1000, out_dir=str(tmp_path), sink=recs.append,
                     clock=clk.now, sleep=clk.sleep)
    mems = [r for r in recs if r["kind"] == "mem"]
    detected = next(i for i, r in enumerate(mems) if r.get("load_detected"))
    assert detected == 3                                          # +150 lookups ≥ 100
    assert all(r["interval_s"] == 5.0 for r in mems[:detected + 1])
    t_detect = mems[detected]["t"]
    inside = [r for r in mems if t_detect <= r["t"] < t_detect + 60]
    after = [r for r in mems if r["t"] >= t_detect + 60]
    assert len(inside) >= 12 and all(r["interval_s"] == 5.0 for r in inside)
    assert after and all(r["interval_s"] == 15.0 for r in after)


def test_run606_records_the_710_counters_by_default():
    src = (BENCH_DIR / "run606.sh").read_text()
    assert ('MEM_VARS_PREFIXES="${MEM_VARS_PREFIXES-snowplow_apistage_content_fill '
            'snowplow_resolved_cache snowplow_informer_dispatch_shape}"') in src


def test_mem_job_carries_the_default_prefixes(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    for prefix in ("snowplow_apistage_content_fill", "snowplow_resolved_cache",
                   "snowplow_informer_dispatch_shape"):
        assert f'--vars-prefix "{prefix}"' in job
