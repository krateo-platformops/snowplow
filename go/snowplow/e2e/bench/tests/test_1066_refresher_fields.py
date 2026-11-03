"""#1066: three report fields read null in both run 20 verdicts although their
raw samples exist.

Causes (read from run 20b's own files, before any change):
  Q/D  report.refresher_queue_depth_{max,last} and
       report.server_dirty_to_fresh_p95_ms_{max,median}_window read only the
       load606 driver's `vars` records (its /debug/vars sampler). The k6 driver
       has no such sampler: 0 `vars` records, null. The same gauges are in
       mem606's samples (1065 in run 20b: queue depth top-level, the windowed
       d2f p95 under extra[snowplow_refresher_dirty_to_fresh_ms_p95]). Wrong place.
  R    report.push_refetch_report_only read the load606 driver's push_refetch
       records; the k6 driver's are the viewer sidecar's k6/viewer.jsonl
       (268,542 in run 20b), never read for it. Wrong place.
A field with no source reads UNMEASURED with its reason, never null.
Every arm runs the evaluate CLI (`python -m bench.load606 evaluate`), on run
20b's real mem606 and viewer files where they are on this machine, or the
function the CLI calls.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from bench import load606
from tests.test_k6_830 import (T_POINTS, _browser_points, _browser_summary, _k6_summary, _point,
                               _write_out, _write_viewer)

BENCH_DIR = Path(__file__).resolve().parents[1]
RUN20B = Path("/tmp/snowplow-runs/606/snowplow-bench-run20b-871a4e09")
RUN20B_MEM = RUN20B / "mem-mem606-qxxcj.jsonl"
RUN20B_VIEWER = RUN20B / "k6" / "viewer.jsonl"
D2F = "snowplow_refresher_dirty_to_fresh_ms_p95"
QD = "snowplow_refresher_queue_depth"

need_run20b = pytest.mark.skipif(not (RUN20B_MEM.is_file() and RUN20B_VIEWER.is_file()),
                                 reason="run 20b's files are not on this machine")


def _k6_points():
    pts = []
    for i, kind in enumerate(["first_load"] * 4 + ["route"] * 8):
        tags = {"kind": kind, "route": f"/r{i % 3}", "visit": "first" if kind != "route" else "warm"}
        pts += [_point("page_load_ms", 150.0 + i, **tags), _point("page_loads", 1, **tags),
                _point("http_req_duration", 10 + i, level=str(i % 2), status="200")]
    return pts


def _mem_sample(i, qd=None, d2f=None, *, t=None, status=200):
    rec = {"kind": "mem", "t": T_POINTS - 20 + 10 * i if t is None else t, "readyz": 200,
           "vars_status": status, "heap_inuse_mb": 100.0, "extra": {}}
    if qd is not None:
        rec["refresher_queue_depth"] = qd
        rec["extra"][QD] = qd
    if d2f is not None:
        rec["extra"][D2F] = d2f
    return rec


def _write_mem(path: Path, samples):
    recs = [{"kind": "mem_start", "t": T_POINTS - 30}, *samples, {"kind": "mem_end", "t": T_POINTS + 10}]
    path.write_text("".join(json.dumps(r) + "\n" for r in recs))
    return path


def _cli(tmp_path, *, mems=(), viewer="synthetic", viewer_kw=None, extra_args=()):
    """The evaluate CLI on k6-driver outputs. `mems`: mem606 files; `viewer`:
    "synthetic" (_write_viewer), None (no sidecar output), or a file to link
    in as k6/viewer.jsonl (run 20b's, read in place)."""
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(), _k6_summary(12, burst=0, burst_loads=0), 0)
    if viewer == "synthetic":
        _write_viewer(k6dir, **(viewer_kw or {}))
    elif viewer is not None:
        os.symlink(viewer, k6dir / "viewer.jsonl")
        (k6dir / "viewer-exit.json").write_text(json.dumps({"kind": "viewer_exit", "rc": 0}))
    _write_out(bdir, "browser", _browser_points(), _browser_summary(63), 0)
    fresh = tmp_path / "fresh.jsonl"
    fresh.write_text(json.dumps({"kind": "fresh", "label": "list_dep@1", "converged": True, "latency_ms": 100,
                                 "polls": 1, "poll_ms": 100}) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(fresh), *map(str, mems), "--run8",
                        "--k6-out", str(k6dir), "--browser-out", str(bdir), "--browser-sessions", "3",
                        *extra_args],
                       capture_output=True, text=True, cwd=BENCH_DIR, timeout=600)
    assert p.stdout, p.stderr[-3000:]
    return json.loads(p.stdout)["report"]


def _nearest_rank_median(xs):
    xs = sorted(xs)
    return xs[(len(xs) + 1) // 2 - 1]


def _raw_run20b():
    """The raw expvar samples, read here without the bench's readers: each
    mem606 line's /debug/vars copy of the two gauges, and the sidecar's
    push_refetch lines."""
    qd, d2f = [], []
    with open(RUN20B_MEM) as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            ex = r.get("extra") or {}
            if r.get("kind") == "mem" and QD in ex:
                qd.append((r["t"], ex[QD]))
            if r.get("kind") == "mem" and D2F in ex:
                d2f.append(ex[D2F])
    refetch = []
    with open(RUN20B_VIEWER) as fh:
        for line in fh:
            if '"push_refetch"' in line:
                r = json.loads(line)
                if r.get("kind") == "push_refetch":
                    refetch.append(r)
    return [v for _, v in sorted(qd)], d2f, refetch


# ─── run 20b's own files ─────────────────────────────────────────────────────

@need_run20b
def test_run20b_files_give_the_three_fields_matching_the_raw_samples(tmp_path):
    qd, d2f, refetch = _raw_run20b()
    assert qd and d2f and refetch                       # the samples the investigator read
    r = _cli(tmp_path, mems=[RUN20B_MEM], viewer=RUN20B_VIEWER)
    assert r["refresher_queue_depth_max"] == max(qd)
    assert r["refresher_queue_depth_last"] == qd[-1]
    assert r["server_dirty_to_fresh_p95_ms_max_window"] == max(d2f)
    assert r["server_dirty_to_fresh_p95_ms_median_window"] == _nearest_rank_median(d2f)
    assert r["refresher_gauges_source"] == f"mem606's /debug/vars samples ({len(qd)})"
    # run 8's own whole-run max of the same gauge agrees
    assert r["run8"]["refresher_dirty_to_fresh"]["dirty_to_fresh_p95_max_over_samples"] == max(d2f)
    pr = r["push_refetch_report_only"]
    ok = [x["wall_ms"] for x in refetch if x.get("status") == 200]
    assert pr["status"] == "present" and pr["refetches"] == len(refetch)
    assert pr["non_200"] == len(refetch) - len(ok)
    assert pr["users"] == len({x.get("user") for x in refetch})
    assert pr["wall_p95_ms"] == load606.pct(ok, 95)
    assert pr["source"].startswith("k6/viewer.jsonl (")
    # the same records as the viewer report and run 8's delivery
    assert {k: pr[k] for k in ("refetches", "non_200", "users", "wall_p95_ms", "start_lag_p95_ms")} \
        == r["viewer_report_only"]["push_refetch"]
    assert r["run8"]["exercised_710_740"]["push_refetches"] == len(refetch)


@need_run20b
def test_run20b_values_are_the_ones_the_investigator_read():
    """Pinned from the raw samples (2026-11-01): a reader that drifts to
    another key or another sample reads RED here."""
    mem = load606._read_jsonl(str(RUN20B_MEM))
    g = load606.refresher_gauges_report([], mem, True)
    assert (g["refresher_queue_depth_max"], g["refresher_queue_depth_last"]) == (4307, 72)
    assert g["server_dirty_to_fresh_p95_ms_max_window"] == 2411693
    rf = [r for r in load606._read_jsonl(str(RUN20B_VIEWER)) if r.get("kind") == "push_refetch"]
    pr = load606.push_refetch_field(rf, "k6/viewer.jsonl")
    assert (pr["refetches"], pr["non_200"], pr["users"], pr["wall_p95_ms"]) == (268542, 0, 995, 112.1)


# ─── synthetic k6-driver outputs ─────────────────────────────────────────────

def test_k6_driver_reads_mem606s_samples_in_time_order(tmp_path):
    """Two mem606 files handed in later-first: `last` is the latest sample."""
    late = _write_mem(tmp_path / "mem-b.jsonl", [_mem_sample(3, 40, 900), _mem_sample(4, 7, 300)])
    early = _write_mem(tmp_path / "mem-a.jsonl", [_mem_sample(0, 10, 100), _mem_sample(1, 55, 200),
                                                  _mem_sample(2, None, None, status=503)])
    r = _cli(tmp_path, mems=[late, early])
    assert r["refresher_queue_depth_max"] == 55 and r["refresher_queue_depth_last"] == 7
    assert r["server_dirty_to_fresh_p95_ms_max_window"] == 900
    assert r["server_dirty_to_fresh_p95_ms_median_window"] == 200
    assert r["refresher_gauges_source"] == "mem606's /debug/vars samples (4)"   # the 503 one holds no vars
    pr = r["push_refetch_report_only"]
    assert pr["status"] == "present" and pr["refetches"] == 1 and pr["users"] == 1


def test_gauges_in_no_sample_are_unmeasured_with_why(tmp_path):
    mem = _write_mem(tmp_path / "mem-x.jsonl", [_mem_sample(0), _mem_sample(1)])
    r = _cli(tmp_path, mems=[mem])
    for f in ("refresher_queue_depth_max", "refresher_queue_depth_last"):
        assert r[f] == {"status": "UNMEASURED",
                        "why": f"{QD} is in none of the 2 mem606's /debug/vars samples"}, r[f]
    for f in ("server_dirty_to_fresh_p95_ms_max_window", "server_dirty_to_fresh_p95_ms_median_window"):
        assert r[f] == {"status": "UNMEASURED",
                        "why": f"{D2F} is in none of the 2 mem606's /debug/vars samples"}, r[f]


def test_no_mem606_sample_is_unmeasured(tmp_path):
    r = _cli(tmp_path)
    assert r["refresher_queue_depth_max"] == {"status": "UNMEASURED",
                                              "why": "mem606's /debug/vars samples: none recorded"}
    assert r["server_dirty_to_fresh_p95_ms_median_window"]["status"] == "UNMEASURED"


def test_one_gauge_present_the_other_unmeasured(tmp_path):
    """Each field stands on its own source: a build or prefix set without the
    d2f gauge leaves queue depth measured."""
    mem = _write_mem(tmp_path / "mem-x.jsonl", [_mem_sample(0, 3), _mem_sample(1, 9)])
    r = _cli(tmp_path, mems=[mem])
    assert (r["refresher_queue_depth_max"], r["refresher_queue_depth_last"]) == (9, 9)
    assert r["server_dirty_to_fresh_p95_ms_max_window"]["status"] == "UNMEASURED"


def test_no_viewer_output_is_unmeasured(tmp_path):
    r = _cli(tmp_path, viewer=None)
    assert r["push_refetch_report_only"] == {
        "status": "UNMEASURED", "why": "no push_refetch record in k6/viewer.jsonl: not collected"}


def test_a_run_without_viewers_says_so(tmp_path):
    r = _cli(tmp_path, viewer=None, extra_args=("--no-viewers",))
    assert r["push_refetch_report_only"] == {
        "status": "UNMEASURED", "why": "no push_refetch record in k6/viewer.jsonl: the run drove no viewers"}


def test_viewer_output_without_a_refetch_is_unmeasured_not_zero(tmp_path):
    k6dir = tmp_path / "k6"
    _write_viewer(k6dir)
    recs = [json.loads(x) for x in (k6dir / "viewer.jsonl").read_text().splitlines()]
    keep = [r for r in recs if r["kind"] != "push_refetch"]
    vfile = tmp_path / "viewer-norefetch.jsonl"
    vfile.write_text("".join(json.dumps(r) + "\n" for r in keep))
    for f in k6dir.iterdir():
        f.unlink()
    k6dir.rmdir()
    r = _cli(tmp_path, viewer=vfile)
    assert r["push_refetch_report_only"] == {
        "status": "UNMEASURED", "why": f"no push_refetch record in k6/viewer.jsonl ({len(keep)} records)"}


# ─── the load606 driver: its own sampler, as before ──────────────────────────

def _vars(i, qd, d2f):
    return {"kind": "vars", "t": 100 + i, "refresher_queue_depth": qd, "dirty_to_fresh_p95_ms": d2f}


def test_load606_driver_reads_its_own_vars_samples():
    mem = [_mem_sample(0, 999, 999)]          # mem606's: not this driver's source
    rep = load606.evaluate([], [], [_vars(0, 4, 50), _vars(1, 2, 70), _vars(2, 3, 60)], mem=mem,
                           push_refetches=[{"kind": "push_refetch", "user": "u", "status": 200,
                                            "wall_ms": 5.0, "start_lag_ms": 1.0}])["report"]
    assert (rep["refresher_queue_depth_max"], rep["refresher_queue_depth_last"]) == (4, 3)
    assert (rep["server_dirty_to_fresh_p95_ms_max_window"],
            rep["server_dirty_to_fresh_p95_ms_median_window"]) == (70, 60)
    assert rep["refresher_gauges_source"] == "the load606 driver's /debug/vars samples (3)"
    assert rep["push_refetch_report_only"]["refetches"] == 1


def test_load606_driver_without_samples_is_unmeasured():
    rep = load606.evaluate([], [], [])["report"]
    assert rep["refresher_queue_depth_last"] == {
        "status": "UNMEASURED", "why": "the load606 driver's /debug/vars samples: none recorded"}
    assert rep["push_refetch_report_only"] == {
        "status": "UNMEASURED", "why": "no push_refetch record in the load606 driver's viewer records"}
