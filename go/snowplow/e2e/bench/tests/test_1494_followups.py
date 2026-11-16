"""#1494, the follow-ups of #1491's review.

(1) The pair's env agreement fails CLOSED: a comparison that raised makes the pair INVALID, named (and an A/A
    replica unusable), never "they agree".
(2) PRECEDENCE: a disagreement turns a FAIL (a candidate restart) into INVALID; the FAIL's own reasons are kept and
    the rule is said in the pair report.
(3) The goroutine dump's exact-time line is in the boot-window block.
(4) The measured and gate phases a boot window's probes or its goroutine dump overlapped are marked, per arm and in
    the pair report (and on the pair's cells over those phases); where a stall is reported in a measuring run, the
    report says runtime606 shares the loadgen node with the drivers.
(5) The drivers' first request (their own records) is in the load start: Ready -> first request, per arm and pair.
(6) The observer's top switch and the tracing read, as offsets from the load start.
And: a fetched run directory can be evaluated again with this bench (`load606 reevaluate`), its evidence as it is.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess

import pytest

from bench import bootwindow, custlat, load606, snowplow_env
from tests.test_load_start import _rec, _verdict
from tests.test_pair_ready_time import _pair_cli as _pair15, run15  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)


def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def _pair_cli(tmp_path, va, vb, capsys):
    fa, fb = tmp_path / "a.json", tmp_path / "b.json"
    fa.write_text(json.dumps(va))
    fb.write_text(json.dumps(vb))
    rc = custlat.main(["pair", "--a", str(fa), "--b", str(fb), "--a-label", "N1", "--b-label", "N2"])
    return rc, json.loads(capsys.readouterr().out)


# ─── (1) fails closed ───────────────────────────────────────────────────────

def test_a_comparison_that_raises_makes_the_pair_invalid_named(tmp_path, capsys, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(snowplow_env, "pair", boom)
    rc, out = _pair_cli(tmp_path, _verdict(_rec("A", "7GiB", 4.0)), _verdict(_rec("B", "7GiB", 4.0, gogc="100")), capsys)
    assert rc == 3 and out["verdict"] == "INVALID"
    assert ("customer_latency.pair.snowplow_env_agreement: the arms' snowplow env could not be compared "
            "(RuntimeError: boom): whether a key no arm declared differs is UNKNOWN") in out["invalid_reasons"]


def test_a_replica_whose_comparison_raises_cannot_calibrate(tmp_path, monkeypatch):
    dirs = []
    for arm in ("N1", "N1B"):
        d = tmp_path / arm
        d.mkdir()
        (d / "verdict.json").write_text(json.dumps(_verdict(_rec(arm, "7GiB", 4.0))))
        dirs.append(str(d))

    def boom(*a, **k):
        raise ValueError("bad record")
    monkeypatch.setattr(snowplow_env, "agreement", boom)
    out = custlat.replica_in_run((dirs[0], dirs[1]), "N1:N1B", "sha256:" + "ab" * 32, (True, True))
    assert out["in_run"]["usable"] is False
    assert ("the two runs' snowplow env, compared after the measure: they could not be compared (ValueError: bad "
            "record): whether a key no run declared differs is UNKNOWN") in out["in_run"]["why"]


# ─── (2) precedence ─────────────────────────────────────────────────────────

def _restarted(v):
    return dict(v, validity=[{"check": custlat.RESTART_CHECK, "result": "FAIL", "value": 1, "why": "restarted 1x"}])


def test_a_disagreement_turns_a_candidate_restart_fail_into_invalid_and_keeps_its_reasons(tmp_path, capsys):
    va = _verdict(_rec("A", "7GiB", 4.0))
    # the candidate restarted (alone: a FAIL) ...
    rc, out = _pair_cli(tmp_path, va, _restarted(_verdict(_rec("B", "7GiB", 4.0, gogc="100"))), capsys)
    assert rc == 1 and out["verdict"] == "FAIL" and "PRECEDENCE" not in " ".join(out.get("notes") or [])
    # ... and its arms disagree: INVALID, the restart's FAIL reasons kept, the rule said
    rc, out = _pair_cli(tmp_path, va, _restarted(_verdict(_rec("B", "6GiB", 4.0, gogc="100"))), capsys)
    assert rc == 3 and out["verdict"] == "INVALID"
    assert any("candidate_restarts" in r for r in out["fail_reasons"])
    assert any(r.startswith("customer_latency.pair.snowplow_env_agreement: GOMEMLIMIT") for r in out["invalid_reasons"])
    assert ("PRECEDENCE: INVALID (the arms' snowplow env, compared after the measure) over FAIL: a pair whose arms did "
            "not run the same undeclared environment compares more than the two images, so neither its FAIL nor its "
            "PASS stands; the FAIL's own reasons are kept (fail_reasons)") in out["notes"]


def test_the_pair_tags_its_cells_over_a_gate_phase_a_boot_window_overlapped(run15, tmp_path):  # noqa: F811
    """custlat main's wiring: the pair (as run606_pair.sh runs it) tags exactly the cells of the gate phase either
    arm's boot window overlapped, in their record and their line."""
    d, va, vb, _, p0 = run15
    assert p0["cells"] and not any("boot_window" in c for c in p0["cells"])      # no section: no tag
    phases = {c["phase"] for c in p0["cells"]}
    for arm, v, inside in (("a", va, True), ("b", vb, False)):
        gate = custlat.pooled([(n, float(x), float(y)) for n, x, y in v["report"]["customer_latency"]["windows"]])
        # the last churn step: past storm (open from -inf) and burst, so churn alone holds it
        name, g0, _ = [w for w in gate if w[0] == "churn"][-1]
        assert name in phases and all(y < g0 for n, _, y in gate if n != "churn")
        end = max(y for _, _, y in gate)
        bw = _bw(g0 + 5.0, g0 + 30.0) if inside else _bw(end + 1000.0, end + 1001.0, stalled=False)
        vv = dict(v, report=dict(v["report"], boot_window_overlap_report_only=bootwindow.overlap_report(bw, [], gate)))
        shutil.copytree(d / arm, tmp_path / arm)
        (tmp_path / arm / "verdict.json").write_text(json.dumps(vv))
    _, p = _pair15(tmp_path / "a", tmp_path / "b")
    mark = p["boot_window_overlap_report_only"]["gate_marked"]["churn"]
    assert mark.endswith(": OVERLAPPED by the probes; B (candidate): not overlapped"), mark
    churn = [c for c in p["cells"] if c["phase"] == "churn"]
    assert churn and all(c["boot_window"] == mark and c["line"].endswith(
        f" [boot window overlapped this phase: {mark}]") for c in churn)
    other = [(c["phase"], c.get("boot_window"), c["line"][-160:]) for c in p["cells"] if c["phase"] != "churn"
             and ("boot_window" in c or "boot window overlapped" in c["line"])]
    assert not other, (other, p["boot_window_overlap_report_only"]["gate_marked"])
    # the cells are the same cells, only tagged
    assert [(c["phase"], c["metric"]) for c in p["cells"]] == [(c["phase"], c["metric"]) for c in p0["cells"]]


# ─── (3) the dump's exact time ──────────────────────────────────────────────

def test_the_boot_window_says_the_dumps_exact_time_and_duration(tmp_path):
    ready = 1794812705.0
    rows = [{"kind": "boot_window_start", "t": ready + 6.0, "every_s": 5.0, "stall_s": 5.0, "timeout_s": 30.0},
            {"kind": "go_runtime_attempt", "n": 1, "t": ready + 6.0, "t_end": ready + 36.0, "status": 0,
             "elapsed_ms": 30000.0, "first_byte_ms": None, "error_class": "TimeoutError", "stalled_at": "first_byte"},
            {"kind": "boot_dump", "t": ready + 11.25, "trigger": {"route": "vars", "waited_s": 5.3}, "status": 200,
             "elapsed_ms": 412.0, "first_byte_ms": 300.0, "bytes": 9, "chunks": 1},
            {"kind": "boot_dump_chunk", "i": 0, "n": 1,
             "data": __import__("base64").b64encode(gzip.compress(b"goroutine")).decode()},
            {"kind": "boot_window_end", "t": ready + 46.0, "rounds": 4, "dump": True, "skipped": None}]
    (tmp_path / "go-runtime.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    (tmp_path / "pod-before.jsonl").write_text(json.dumps({"kind": "snowplow_pod", "ready_at": ready}) + "\n")
    (tmp_path / "load-start.json").write_text(json.dumps({"kind": "load_start", "ready_at": ready,
                                                          "applied_at": ready + 1.5, "offset_s": 1.5}))
    rec = bootwindow.summarize(str(tmp_path / "go-runtime.jsonl"), str(tmp_path / "pod-before.jsonl"),
                               str(tmp_path / "d.gz"), str(tmp_path / "load-start.json"))
    assert ("INFO boot window: the goroutine dump (debug=2: it stops the world while it walks every goroutine) ran at "
            "2026-11-16T07:05:16.250Z (Ready +11.2 s, load start +9.8 s) for 412.0 ms") in rec["lines"]
    # where the stall is reported in a measuring run: runtime606 shares the loadgen node with the drivers
    assert f"INFO boot window: {snowplow_env.STALL_SHARED_NODE}" in rec["lines"]


# ─── (4) the phases the boot window overlapped ──────────────────────────────

def _bw(start, end, dump_t=None, dump_ms=500.0, stalled=True):
    return {"kind": "boot_window", "window": {"start": start, "end": end},
            "dump": {"t": dump_t, "elapsed_ms": dump_ms} if dump_t is not None else None,
            "routes": {"vars": {"not_served": 1 if stalled else 0}}}


def test_the_overlap_marks_each_measured_and_gate_phase_the_probes_or_the_dump_ran_in():
    phases = [("boot", 0.0, 100.0), ("storm", 110.0, 200.0), ("burst", 900.0, 960.0), ("churn:list_dep@10", 1000.0, None)]
    gate = [("storm", 110.0, 200.0), ("steady", 200.0, 900.0), ("burst", 900.0, 960.0)]
    ov = bootwindow.overlap_report(_bw(105.0, 250.0, dump_t=150.0), phases, gate)
    rows = {p["phase"]: (p["probes"], p["dump"]) for p in ov["phases"]}
    assert rows == {"boot": (False, False), "storm": (True, True), "burst": (False, False),
                    "churn:list_dep@10": (None, None)}
    assert ov["gate"] == {"storm": {"probes": True, "dump": True}, "steady": {"probes": True, "dump": False},
                          "burst": {"probes": False, "dump": False}}
    assert "INFO boot window overlap, phase storm [" in "\n".join(ov["lines"])
    assert any(ln.endswith("OVERLAPPED by the probes and the goroutine dump") for ln in ov["lines"])
    assert f"INFO boot window overlap: a route was not served; {snowplow_env.STALL_SHARED_NODE}" in ov["lines"]
    # nothing overlapped, and no record: said so, never "not overlapped" for the unknown
    assert bootwindow.overlap_report(_bw(950.0, 955.0, stalled=False), [("storm", 0.0, 900.0)], [])["phases"][0][
        "probes"] is False
    assert bootwindow.overlap_report(None, phases, gate)["state"] == "UNKNOWN"


def test_the_pair_report_and_its_cells_mark_the_phases_either_arm_overlapped():
    def v(ov):
        return {"report": {"boot_window_overlap_report_only": ov}}
    gate = [("storm", 110.0, 200.0), ("steady", 200.0, 900.0)]
    oa = bootwindow.overlap_report(_bw(105.0, 150.0, dump_t=120.0), [("storm", 110.0, 200.0)], gate)
    ob = bootwindow.overlap_report(_bw(950.0, 960.0, stalled=False), [("storm", 110.0, 200.0)], gate)
    pr = bootwindow.pair_overlap(v(oa), v(ob), ("N1 (baseline)", "N2 (candidate)"))
    assert pr["gate_marked"] == {"storm": "N1 (baseline): OVERLAPPED by the probes and the dump; N2 (candidate): "
                                          "not overlapped"}
    assert "INFO boot window overlap, phase storm: N1 (baseline): OVERLAPPED by the probes and the dump; " \
           "N2 (candidate): not overlapped" in pr["lines"]
    cell = bootwindow.cell_mark({"phase": "storm", "line": "PASS storm p95: B/A = 1.01"}, pr["gate_marked"])
    assert cell["line"].endswith("[boot window overlapped this phase: N1 (baseline): OVERLAPPED by the probes and the "
                                 "dump; N2 (candidate): not overlapped]")
    assert bootwindow.cell_mark({"phase": "steady", "line": "x"}, pr["gate_marked"])["line"] == "x"
    # an arm evaluated before #1494: UNKNOWN, said
    assert "UNKNOWN" in " ".join(bootwindow.pair_overlap(v(oa), {"report": {}}, ("A", "B"))["lines"])


# ─── (5) first request, (6) top / tracing; and the stall note, from a real run ───

def test_the_first_request_comes_from_the_drivers_own_records():
    # k6 stamps each http_req_duration point at the request's END: a call started at time - duration. The call
    # that ended first (1000.0, 50 ms) is not the one that started first (1000.3, 1 s: started at 999.3)
    o = {"points": {"http_req_duration": [(50.0, {}), (1000.0, {}), (5.0, {})]},
         "times": {"http_req_duration": [1000.0, 1000.3, 1001.0]}}
    cl = custlat.report(o, None, [], [], [], [])
    assert cl["k6_span"] == [1000.0, 1001.0] and cl["k6_first_start"] == 999.3
    assert load606.first_request({"customer_latency": cl}, [{"kind": "nav", "start": 5.0}]) == (
        999.3, "k6's first customer call, its start: the point's time (the request's end) minus its duration")
    assert custlat.first_start([(1000.0, 250.0)]) == 999.75 and custlat.first_start([]) is None
    navs = [{"kind": "nav", "start": 1010.0}, {"kind": "nav", "start": 1003.0}, {"kind": "mem", "start": 1.0}]
    assert load606.first_request({}, navs) == (1003.0, "the load606 driver's first navigation, its start")
    assert load606.first_request({}, [])[0] is None


def test_a_run_reports_ready_to_first_request_the_top_switch_the_tracing_read_and_the_stalls_node(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", FAKE_GO_RUNTIME_FAIL="stall", FAKE_READY_AGO_S="3")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    ls = json.loads((out / "load-start.json").read_text())
    assert ls["applied_at"] <= ls["top_switch_at"] <= ls["tracing_read_at"]
    v = json.loads((out / "verdict.json").read_text())
    env_ = v["report"]["snowplow_env"]
    first = min(json.loads(x)["start"] for f in out.glob("nav-*.jsonl") for x in f.read_text().splitlines()
                if json.loads(x).get("kind") == "nav")
    assert env_["load_start"]["first_request_at"] == first
    assert env_["load_start"]["first_request_from_ready_s"] == round(first - ls["ready_at"], 1)
    said = "\n".join(env_["lines"])
    assert (f"INFO load start: the drivers' first request at " in said
            and f"= Ready + {env_['load_start']['first_request_from_ready_s']} s" in said
            and "(the load606 driver's first navigation, its start)" in said)
    assert (f"INFO load start: after the apply, the observer's top switch at load start + "
            f"{round(ls['top_switch_at'] - ls['applied_at'], 1)} s, the tracing read at load start + "
            f"{round(ls['tracing_read_at'] - ls['applied_at'], 1)} s") in said
    # where the stall is reported, the read's own line says the read shares the loadgen node with the drivers
    [read_line] = [ln for ln in env_["lines"] if ln.startswith(f"INFO {snowplow_env.READ_FINDING}: STALLED")]
    assert read_line.endswith(f"; {snowplow_env.STALL_SHARED_NODE}")
    assert f"INFO boot window: {snowplow_env.STALL_SHARED_NODE}" in said
    assert any(w.endswith(snowplow_env.STALL_SHARED_NODE) and w.startswith(snowplow_env.READ_FINDING)
               for w in v["report"]["report_warnings"])
    assert v["report"]["boot_window_overlap_report_only"]["state"] == "READ"
    assert "INFO boot window overlap: the probes " in log


# ─── a fetched run, evaluated again with this bench (bench/reevaluate.py) ───

def _judged(v: dict, d) -> dict:
    """What the verdict judged, with the run dir as {OUT}: the verdict, every invalid and fail reason, and every
    validity check's result."""
    s = json.dumps({"verdict": v.get("verdict"), "invalid": sorted(v.get("invalid_reasons") or []),
                    "fail": sorted(v.get("fail_reasons") or []),
                    "validity": sorted((c.get("check"), c.get("result")) for c in v.get("validity") or [])})
    return json.loads(s.replace(str(d), "{OUT}"))


def _reevaluate(copy, out, *args):
    return subprocess.run(["python3", "-m", "bench.reevaluate", str(copy), "--out", str(out), *args], cwd=BENCH_DIR,
                          capture_output=True, text=True, env=dict(os.environ, PYTHONPATH=str(BENCH_DIR)))


def _as_before_1494(copy):
    """The run dir as a run from before #1494 left it: no evaluate-argv.json, a load start without the top / tracing
    times."""
    (copy / "evaluate-argv.json").unlink()
    ls = json.loads((copy / "load-start.json").read_text())
    (copy / "load-start.json").write_text(json.dumps({k: ls[k] for k in ("kind", "ready_at", "applied_at", "offset_s")}))


def _check_reevaluation(out, tmp_path, *, expect: dict):
    first = _judged(json.loads((out / "verdict.json").read_text()), out)
    recorded = json.loads((out / "evaluate-argv.json").read_text())
    assert recorded[0].startswith("{OUT}/")
    copy = tmp_path / "fetched"
    shutil.copytree(out, copy)
    # with its own evaluate-argv.json
    r = _reevaluate(copy, tmp_path / "v1.json")
    assert r.returncode in (0, 1, 3, 4), r.stderr[-2000:]
    assert _judged(json.loads((tmp_path / "v1.json").read_text()), copy) == first, r.stderr[-2000:]
    # as a run from before #1494: every setting derived from the evidence, none from a default
    _as_before_1494(copy)
    r = _reevaluate(copy, tmp_path / "v2.json")
    assert (tmp_path / "v2.json").exists(), r.stderr[-3000:]
    v2 = json.loads((tmp_path / "v2.json").read_text())
    assert _judged(v2, copy) == first, r.stderr[-2000:]
    got = {k: s["value"] for k, s in v2["reevaluated"]["settings"].items()}
    assert got == expect, got
    # the rebuilt argv is the one run606 ran, to the flag
    assert v2["reevaluated"]["argv"] == ["evaluate", *(x.replace("{OUT}", str(copy)) for x in recorded)]
    return copy


def test_a_run_directory_is_evaluated_again_its_evidence_as_it_is(harness, tmp_path):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100")
    out = tmp / "out"
    assert p.returncode == 0, (out / "run.log").read_text()[-3000:]
    from tests.test_run606_real_sequence import FAKE_DIGEST
    copy = _check_reevaluation(out, tmp_path, expect={"driver": "load606", "expect_digest": FAKE_DIGEST,
                                                      "frontend": "none", "run8": True})
    # never over the run's own verdict
    assert _reevaluate(copy, copy / "verdict.json").returncode == 2
    # what the evidence cannot decide is refused, named, unless stated; a statement it contradicts too
    (copy / "image-digest.jsonl").unlink()
    r = _reevaluate(copy, tmp_path / "v3.json")
    assert r.returncode == 2 and "REFUSED: expect_digest: not decided by the run's evidence" in r.stderr
    assert _reevaluate(copy, tmp_path / "v3.json", "--expect-digest", FAKE_DIGEST).returncode != 2
    (copy / "verdict.json").unlink()
    r = _reevaluate(copy, tmp_path / "v4.json", "--expect-digest", FAKE_DIGEST)
    assert r.returncode == 2 and "run8: not decided by the run's evidence (no readable verdict.json of the run)" in r.stderr
    r = _reevaluate(copy, tmp_path / "v4.json", "--expect-digest", FAKE_DIGEST, "--run8", "on", "--driver", "k6")
    assert r.returncode == 2 and "driver: stated 'k6', the run's evidence says 'load606'" in r.stderr


def test_a_k6_run_with_a_frontend_override_three_sessions_no_viewers_no_run8_is_evaluated_again_as_it_ran(harness, tmp_path):  # noqa: F811
    from tests.test_run26_frontend_override import FD, FTAG, _fenv
    env, state, _, tmp = harness
    e = dict(_fenv(env), FAKE_FRONTEND_DIGEST=FD, LOAD_DRIVER="k6", USERS="20", THINK_S="1", COMPOSITE_BURST_USERS="5",
             BROWSER_SESSIONS="3", VIEWERS="0", RUN8_REPORT="0", PREFLIGHT_TIMEOUT_S="10", PREFLIGHT_POLL_S="0.5", SNOWPLOW_ENV="GOGC=100")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=e, capture_output=True, text=True, timeout=600)
    out = tmp / "out"
    assert p.returncode == 0, (out / "run.log").read_text()[-3000:]
    from tests.test_run606_real_sequence import FAKE_DIGEST
    v = json.loads((out / "verdict.json").read_text())
    assert any(c["check"] == "frontend.image_digest" for c in v["validity"])          # the override was judged
    assert "run8" not in v["report"]
    copy = _check_reevaluation(out, tmp_path, expect={
        "driver": "k6", "expect_digest": FAKE_DIGEST, "frontend": [FTAG, FD], "run8": False,
        "browser_sessions": 3, "viewers": False})
    # a statement the evidence contradicts is refused, naming the setting
    r = _reevaluate(copy, tmp_path / "v5.json", "--browser-sessions", "5", "--viewers", "on", "--no-frontend",
                    "--run8", "on")
    assert r.returncode == 2
    for k in ("browser_sessions: stated 5", "viewers: stated True", "frontend: stated 'none'", "run8: stated True"):
        assert k in r.stderr, r.stderr
    # viewer output beside a manifest with no viewer sidecar: the evidence contradicts itself, refused
    (copy / "k6" / "viewer-exit.json").write_text('{"kind": "viewer_exit", "rc": 0}')
    r = _reevaluate(copy, tmp_path / "v6.json")
    assert r.returncode == 2 and "viewers: not decided by the run's evidence (the viewer left output" in r.stderr


def _k6_run_dir(d, *, browser_config=True, frontend_digest=False, frontend_tag=None):
    """A minimal k6 run dir as run606 leaves it: the manifest, the digest gate's record, the verdict, the
    installer values; optionally the frontend's digest record and tag."""
    import yaml
    docs = [{"kind": "ConfigMap", "metadata": {"name": "k6"}, "data": {"config.json": json.dumps({"viewers": True})}},
            {"kind": "Job", "metadata": {"name": "k6load"},
             "spec": {"template": {"spec": {"containers": [{"name": "k6"}, {"name": "viewer"}]}}}},
            {"kind": "Job", "metadata": {"name": "k6browser"}, "spec": {"template": {"spec": {"containers": []}}}}]
    if browser_config:
        docs.insert(0, {"kind": "ConfigMap", "metadata": {"name": "k6browser"},
                        "data": {"browser-config.json": json.dumps({"sessions": [{"name": f"s{i}"} for i in range(4)]})}})
    (d / "loadgen-job.yaml").write_text(yaml.safe_dump_all(docs))
    (d / "image-digest.jsonl").write_text(json.dumps({"kind": "image_digest", "expected": "sha256:" + "ab" * 32}) + "\n")
    (d / "verdict.json").write_text(json.dumps({"verdict": "PASS", "report": {"run8": {}}}))
    vals = {"componentValues": {"snowplow": {"image": {"tag": "1.12.70"}}}}
    if frontend_tag:
        vals["componentValues"]["frontend"] = {"image": {"tag": frontend_tag}}
    (d / "installer-values.yaml").write_text(yaml.safe_dump(vals))
    if frontend_digest:
        (d / "frontend-digest.jsonl").write_text(json.dumps({"kind": "frontend_image_digest",
                                                             "expected": "sha256:" + "1a" * 32}) + "\n")
    return d


def test_reevaluate_derives_a_k6_run_dirs_settings(tmp_path):
    from bench import reevaluate
    s = reevaluate.settings(str(_k6_run_dir(tmp_path)), {})
    assert {k: v for k, (v, _) in s.items()} == {"driver": "k6", "expect_digest": "sha256:" + "ab" * 32,
                                                 "frontend": "none", "run8": True, "browser_sessions": 4,
                                                 "viewers": True}
    (tmp_path / "f").mkdir()
    s = reevaluate.settings(str(_k6_run_dir(tmp_path / "f", frontend_digest=True, frontend_tag="1.8.44")), {})
    assert s["frontend"][0] == ("1.8.44", "sha256:" + "1a" * 32)


def test_reevaluate_refuses_a_frontend_whose_evidence_is_present_but_undecided(tmp_path):
    """V02: frontend-digest.jsonl says an override ran, installer-values.yaml holds no frontend tag: refused,
    named, never read as "none"; stated, it is taken."""
    from bench import reevaluate
    d = str(_k6_run_dir(tmp_path, frontend_digest=True))
    with pytest.raises(reevaluate.Refused, match=r"^frontend: not decided by the run's evidence \(frontend-digest.jsonl "
                                                 r"present with its expected, installer-values.yaml tag None"):
        reevaluate.settings(d, {})
    s = reevaluate.settings(d, {"frontend": ("1.8.44", "sha256:" + "1a" * 32)})
    assert s["frontend"][0] == ("1.8.44", "sha256:" + "1a" * 32) and s["frontend"][1].startswith("stated")
    # the tag without the digest record: undecided too
    with pytest.raises(reevaluate.Refused, match=r"^frontend: not decided"):
        (tmp_path / "t").mkdir()
        reevaluate.settings(str(_k6_run_dir(tmp_path / "t", frontend_tag="1.8.44")), {})


def test_reevaluate_refuses_a_k6_run_with_no_browser_config(tmp_path):
    """V03: no browser-config.json in the manifest: the session count is refused, named, never 5 by default."""
    from bench import reevaluate
    d = str(_k6_run_dir(tmp_path, browser_config=False))
    with pytest.raises(reevaluate.Refused, match=r"^browser_sessions: not decided by the run's evidence "
                                                 r"\(loadgen-job.yaml holds no browser-config.json with its sessions\)"):
        reevaluate.settings(d, {})
    assert reevaluate.settings(d, {"browser_sessions": 5})["browser_sessions"][0] == 5
    # the browser's own summary disagreeing with the manifest: undecided as well
    (tmp_path / "x").mkdir()
    d2 = _k6_run_dir(tmp_path / "x")
    (d2 / "browser").mkdir()
    (d2 / "browser" / "browser-summary.json").write_text(json.dumps({"kind": "browser_summary", "sessions": ["a", "b"]}))
    with pytest.raises(reevaluate.Refused, match=r"^browser_sessions: not decided .* 4 sessions, the browser's summary 2"):
        reevaluate.settings(str(d2), {})
