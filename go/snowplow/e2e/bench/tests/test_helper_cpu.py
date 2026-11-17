"""The renderers sidecar's own cost, measured directly (Diego 2026-11-17: the rehearsal checks #1517's helper by
measuring the helper, no with/without stage): the sidecar records its own cgroup with the browser container's
sampler (k6_job._cpu_sampler / _cpu_end, one implementation) into /out/renderers-cpu.jsonl; evaluate reads it,
reports its cores (mean, p95, throttled, PSI, and the resolution of the mean from the arm's own 60 s spread) next
to the browser container's, and loadgen.renderers_cpu FAILS when a pod that had the helper left no record."""

from __future__ import annotations

import json
import os
import subprocess
import time

import pytest
import yaml

from bench import k6_job, k6eval
from tests.test_k6_830 import _browser_points, _browser_summary, _cpu_file, _write_out
from tests.test_renderer_stall import _case

#: the fixture's sidecar: 0.04 cores for a minute, 0.06 for a minute, ..., 5 minutes (tests/test_renderer_stall.py)
SIDECAR = ([0.04] * 12 + [0.06] * 12) * 2 + [0.04] * 12


def _sidecar(cfg_sessions=3):
    cfg = k6_job.browser_config(sessions=[{"name": f"s{i}", "user": f"u{i}", "offset": i, "cold": True}
                                          for i in range(1, cfg_sessions + 1)],
                                routes=["/a"], password="x", think_s=1, duration_s=10, frontend_url="http://f",
                                snowplow_url="http://s", authn_url="http://a", debug_user="d")
    job = next(d for d in yaml.safe_load_all(k6_job.render_browser(config=cfg, linger_s=0))
               if d and d["kind"] == "Job")
    return next(c for c in job["spec"]["template"]["spec"]["containers"] if c["name"] == "renderers")


def test_the_rendered_sidecar_samples_its_own_cgroup_with_the_browsers_sampler():
    """Its script carries the ONE sampler (the browser container's), framed for "renderers" at its pod's 250m
    request and limit, started before the watcher and ended before renderers-done (mutation: the sampler not
    started turns this RED)."""
    side = _sidecar()
    script = side["args"][0]
    sampler = k6_job._cpu_sampler("renderers", k6_job.RENDERERS_CPU, k6_job.RENDERERS_CPU)
    assert sampler in script and k6_job._cpu_end("renderers") in script
    assert '"container": "renderers", "request_m": 250, "limit_m": 250' in script
    assert script.index("while :; do cpu_sample; sleep") < script.index("rsampler_loop &")
    assert script.index('"kind": "cpu_end"') < script.index("echo 0 > /out/renderers-done")
    assert side["resources"]["requests"]["cpu"] == side["resources"]["limits"]["cpu"] == "250m"


def test_the_browser_container_and_the_sidecar_share_one_sampler():
    """No second sampler implementation: the wrapper ends its record with the same _cpu_end."""
    w = k6_job._wrapper("true", "browser", 0, cpu=("3", "3"))
    assert k6_job._cpu_end("browser") in w and k6_job._cpu_sampler("browser", "3", "3") in w


@pytest.mark.skipif(not os.path.exists("/sys/fs/cgroup/cpu.stat") or not os.path.isdir("/proc/self/task"),
                    reason="needs Linux cgroup v2 (/sys/fs/cgroup/cpu.stat) and /proc")
def test_the_sidecars_script_records_its_cpu_from_its_start_to_the_browsers_end(tmp_path):
    """The sidecar's own script, run (its /out a directory here): its record opens with cpu_start, samples while
    the browser runs, and closes with cpu_end once /out/done appears, before renderers-done."""
    script = _sidecar(1)["args"][0].replace("/out/", f"{tmp_path}/")
    p = subprocess.Popen(["sh", "-c", script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)
        (tmp_path / "done").write_text("0\n")
        p.wait(timeout=60)
    finally:
        if p.poll() is None:
            p.kill()
    assert (tmp_path / "renderers-done").exists()
    rows = [json.loads(x) for x in (tmp_path / "renderers-cpu.jsonl").read_text().splitlines() if x.strip()]
    kinds = [r["kind"] for r in rows]
    assert kinds[0] == "cpu_start" and kinds[-1] == "cpu_end" and kinds.count("cpu") >= 2, kinds
    assert rows[0]["container"] == "renderers" and rows[0]["limit_m"] == 250


def test_the_sidecars_record_is_read_with_the_browsers(tmp_path):
    """read_k6_out("browser") holds both containers' records (mutation: the sidecar's not read turns this RED)."""
    _write_out(tmp_path, "browser", _browser_points(), _browser_summary(63), 0)
    _cpu_file(tmp_path, "renderers", 250, [0.05] * 4, limit_m=250)
    o = k6eval.read_k6_out(str(tmp_path), "browser")
    assert o["cpu"]["browser"] and o["cpu"]["renderers"]
    assert o["cpu"]["renderers"][0]["container"] == "renderers"


def test_the_report_states_the_mean_and_its_resolution_from_the_arms_own_spread(tmp_path):
    _cpu_file(tmp_path, "renderers", 250, SIDECAR, limit_m=250)
    r = k6eval.container_cpu_report(k6eval.read_cpu(str(tmp_path), "renderers"))
    assert r["cores_mean"] == pytest.approx(0.048) and r["cores_p95"] == pytest.approx(0.06)
    assert r["throttled_s_per_s"] == 0 and r["psi_some_s_per_s"] == 0 and r["limit_m"] == 250
    sp = r["spread"]                                          # five 60 s windows: .04 .06 .04 .06 .04
    assert sp["windows"] == 5 and sp["sd_cores"] == pytest.approx(0.011, abs=1e-3)
    assert sp["resolution_cores"] == pytest.approx(2 * sp["sd_cores"] / 5 ** 0.5, abs=1e-4)
    short = tmp_path / "s"
    _cpu_file(short, "renderers", 250, [0.05] * 12, limit_m=250)     # one window: no spread, said
    assert k6eval.container_cpu_report(k6eval.read_cpu(str(short), "renderers"))["spread"]["resolution_cores"] is None


def test_evaluate_reports_the_sidecars_cores_next_to_the_browsers_and_passes_its_check(tmp_path):
    _, v = _case(tmp_path)
    pod = v["report"]["browser_pod_cpu"]
    assert pod["browser"]["cores_mean"] == pytest.approx(1.0)
    assert pod["renderers"]["cores_mean"] == pytest.approx(0.048) and pod["renderers"]["spread"]["windows"] == 5
    chk = next(c for c in v["validity"] if c["check"] == "loadgen.renderers_cpu")
    assert chk["result"] == "PASS"


@pytest.mark.parametrize("record, why", [
    (None, "the renderers sidecar left no CPU record (renderers-cpu.jsonl): its cost is unknown, never zero"),
    ("start-only", "the renderers sidecar's CPU record holds fewer than two samples"),
])
def test_a_helper_whose_cost_was_not_measured_fails_its_check_never_reads_as_zero(tmp_path, record, why):
    """A pod that had the sidecar and no record of its cost: loadgen.renderers_cpu FAILS, the arm is INVALID
    (mutation: a missing record read as zero cores turns this RED)."""
    if record is None:
        _, v = _case(tmp_path, helper_cpu=False)
    else:
        _case(tmp_path)
        f = tmp_path / "browser" / "renderers-cpu.jsonl"
        f.write_text(f.read_text().splitlines()[0] + "\n")
        from tests.test_k6_830 import _evaluate_cli
        _, v = _evaluate_cli(tmp_path, tmp_path / "k6", tmp_path / "browser")
    chk = next(c for c in v["validity"] if c["check"] == "loadgen.renderers_cpu")
    assert chk["result"] == "FAIL" and why in chk["why"] and chk["value"] is None, chk
    assert v["verdict"] == "INVALID" and any(r.startswith("loadgen.renderers_cpu: ") for r in v["invalid_reasons"])


def test_a_run_without_the_sidecar_has_no_helper_to_measure(tmp_path):
    """Old evidence (no per-renderer records: no sidecar): no check, nothing failed for it."""
    _, v = _case(tmp_path, renderer=False)
    assert not any(c["check"] == "loadgen.renderers_cpu" for c in v["validity"])
