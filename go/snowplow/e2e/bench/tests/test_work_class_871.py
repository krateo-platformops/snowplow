"""#871 (owner ruling 2026-10-25, "measure first"), report only: storm CPU and
unit counts per snowplow work class, read from snowplow's pprof label
snowplow_work and its expvar snowplow_work_class.

The inputs are RECORDED from snowplow's own code, not invented:
  * tests/fixtures/work_class_cpu_871.pb.gz — a real Go CPU profile taken
    around snowplow's internal/workclass (a byte-identical copy of the file at
    the #871 PR head, git blob 7990c5e3): a customer unit, a refresh unit, a
    prewarm unit with a nested seed unit — each spinning on its own goroutine,
    on a goroutine it starts, and inside a pprof.Do with the content fill's
    label — then an unlabelled spin. `go tool pprof -tags` on the same file
    (the independent instrument) reads snowplow_work: customer 600 ms, prewarm
    440 ms, seed 330 ms, refresh 290 ms, of 1.85 s.
  * tests/fixtures/work_class_expvar_871.json — the snowplow_work_class expvar
    document before / after those units.
They go through mem606's real sampler (load606.mem_sample, default prefixes)
and the `load606 evaluate` CLI, as run606.sh runs it.
"""

from __future__ import annotations

import copy
import gzip
import json
import shutil
from pathlib import Path

from bench import load606, workclass
from tests.test_run14_list_narrow import F13, _sample

FIX = Path(__file__).resolve().parent / "fixtures"
PROFILE = (FIX / "work_class_cpu_871.pb.gz").read_bytes()
EV = json.loads((FIX / "work_class_expvar_871.json").read_text())
KEY = "snowplow_work_class"


# ─── the decoder, against the independent instrument ────────────────────────

def test_attribution_matches_go_tool_pprof_on_the_recorded_profile():
    a = workclass.attribute([PROFILE])
    assert a["label_seen"] and a["profiles"] == 1 and a["cpu_s"] == 1.85
    got = {c: v["cpu_s"] for c, v in a["by_class"].items()}
    # go tool pprof -tags: customer 600ms, prewarm 440ms, seed 330ms, refresh 290ms
    assert got == {"customer": 0.6, "prewarm": 0.44, "seed": 0.33, "refresh": 0.29,
                   "unlabelled:other": 0.19}
    # background = refresh + prewarm + seed (+ the classes with no samples)
    assert a["background_share"] == round((0.29 + 0.44 + 0.33) / 1.85, 4)
    assert a["customer_share"] == round(0.6 / 1.85, 4)
    assert a["unlabelled_share"] == round(0.19 / 1.85, 4)


def test_a_sample_inside_a_fill_pprof_do_keeps_its_work_class():
    """The content fill runs under its own pprof.Do: its samples carry both
    labels, and the work class wins over nothing."""
    both = [(ns, lab) for ns, lab, _ in workclass.decode(PROFILE) if "apistage_fill_reason" in lab]
    assert both and all(lab.get(workclass.KEY) in workclass.CLASSES for _, lab in both)


def test_unlabelled_samples_split_by_root_frame():
    assert workclass.classify({}, ["runtime.scanobject", "runtime.gcBgMarkWorker"]) == "unlabelled:gc"
    assert workclass.classify({}, ["x", "runtime.bgsweep"]) == "unlabelled:gc"
    assert workclass.classify({}, ["auth.Validate", "net/http.(*conn).serve"]) == "unlabelled:http"
    assert workclass.classify({}, ["x", "runtime.main"]) == "unlabelled:other"
    assert workclass.classify({}, []) == "unlabelled:other"
    assert workclass.classify({"snowplow_work": "drain"}, ["x", "net/http.(*conn).serve"]) == "drain"


# ─── synthetic profiles (label absent, GC) via a minimal encoder ────────────

def _v(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _f(field: int, payload) -> bytes:
    if isinstance(payload, int):
        return _v(field << 3) + _v(payload)
    return _v(field << 3 | 2) + _v(len(payload)) + payload


def _profile(samples: list[tuple[int, dict, list[str]]]) -> bytes:
    """A gzipped CPU profile.proto: (ns, labels, frames leaf first) per sample."""
    strs = [""]

    def s(x):
        if x not in strs:
            strs.append(x)
        return strs.index(x)
    body = b""
    body += _f(1, _f(1, s("samples")) + _f(2, s("count")))
    body += _f(1, _f(1, s("cpu")) + _f(2, s("nanoseconds")))
    fns, locs = {}, {}
    for ns, labels, frames in samples:
        ids = []
        for fr in frames:
            if fr not in fns:
                fns[fr] = len(fns) + 1
                locs[fr] = len(locs) + 1
            ids.append(locs[fr])
        packed = b"".join(_v(i) for i in ids)
        lab = b"".join(_f(3, _f(1, s(k)) + _f(2, s(v))) for k, v in labels.items())
        body += _f(2, _f(1, packed) + _f(2, _v(1) + _v(ns)) + lab)
    for fr, fid in fns.items():
        body += _f(4, _f(1, locs[fr]) + _f(4, _f(1, fid)))
        body += _f(5, _f(1, fid) + _f(2, s(fr)))
    for x in strs:
        body += _f(6, x.encode())
    return gzip.compress(body)


def test_the_encoder_round_trips_through_the_decoder():
    p = _profile([(10_000_000, {"snowplow_work": "refresh"}, ["a", "b"]),
                  (20_000_000, {}, ["runtime.gcBgMarkWorker"])])
    got = workclass.decode(p)
    assert got == [(10_000_000, {"snowplow_work": "refresh"}, ["a", "b"]),
                   (20_000_000, {}, ["runtime.gcBgMarkWorker"])]


def test_a_profile_set_with_no_label_is_missing_not_zero():
    p = _profile([(10_000_000, {}, ["x", "runtime.main"]), (10_000_000, {}, ["runtime.gcBgMarkWorker"])])
    a = workclass.attribute([p])
    assert not a["label_seen"] and a["background_share"] is None and a["customer_share"] is None


# ─── mem606 and evaluate ────────────────────────────────────────────────────

def test_mem606_captures_the_expvar_with_the_default_prefixes():
    assert KEY in load606.RUN8_VARS_PREFIXES
    rec = _sample(1, EV["after"])
    assert rec["extra"][KEY]["prewarm"] == {"started": 1, "completed": 1}
    assert set(rec["extra"][KEY]) == set(workclass.CLASSES)


def _counts(**started) -> dict:
    d = copy.deepcopy(EV["after"])
    for c, n in started.items():
        d[KEY][c] = {"started": n, "completed": n}
    return d


def _run(tmp: Path, profiles: dict[str, bytes], with_expvar=True, copy_files=True) -> list[str]:
    """A run in run606's $OUT layout: mem-x.jsonl (run 13's real phase
    timeline) and heap/ with the CPU profiles mem606 wrote."""
    s0 = F13["mem_start"]["t"]
    t_load = F13["load_detected_t"]
    heap = tmp / "heap"
    heap.mkdir(parents=True)
    mem = [dict(F13["mem_start"]), {"kind": "mem", "t": t_load, "load_detected": True}]
    # storm profiles back to back from the load, one periodic after the burst
    for i, (name, body) in enumerate(profiles.items()):
        t0 = t_load + i * 30 if not name.endswith("-periodic.pb.gz") else t_load + 2000
        mem.append({"kind": "cpu_profile", "t": t0 + 30, "t_start": t0, "t_end": t0 + 30, "seconds": 30,
                    "window": "periodic" if name.endswith("-periodic.pb.gz") else "storm",
                    "status": 200, "profile": name, "prof_start": t0, "prof_duration_s": 30.0})
        if copy_files:
            (heap / name).write_bytes(body)
    if with_expvar:
        for dt, doc in ((100, EV["before"]), (500, EV["after"]),
                        (1000, _counts(refresh=40, drain=3)),       # burst
                        (2000, _counts(refresh=90, drain=3, fill_bg=7))):  # churn
            mem.append(_sample(s0 + dt, doc))
    mem.append({"kind": "mem_end", "t": s0 + 3000})
    f = tmp / "mem-x.jsonl"
    f.write_text("".join(json.dumps(r) + "\n" for r in mem))
    fr = tmp / "fresh-x.jsonl"
    fr.write_text("".join(json.dumps(r) + "\n" for r in F13["fresh"]))
    return [str(f), str(fr)]


def _evaluate(tmp: Path, files: list[str], capsys, profile_dir=True) -> dict:
    capsys.readouterr()
    load606.main(["evaluate", *files, *(["--profile-dir", str(tmp / "heap")] if profile_dir else [])])
    return json.loads(capsys.readouterr().out)["report"]


def test_evaluate_reports_cpu_per_work_class_in_the_storm(tmp_path, capsys):
    files = _run(tmp_path, {"cpu-01-120000.pb.gz": PROFILE, "cpu-02-120030.pb.gz": PROFILE})
    r = _evaluate(tmp_path, files, capsys)
    wc = r["work_class_report_only"]
    cpu = wc["cpu"]
    assert cpu["status"] == "present" and cpu["missing_profiles"] == [] and cpu["unreadable_profiles"] == []
    st = cpu["by_window"]["storm"]
    assert st["profiles"] == 2 and st["cpu_s"] == 3.7 and st["profiled_s"] == 60.0
    assert st["cores"] == round(3.7 / 60, 2)
    assert {c: v["cpu_s"] for c, v in st["by_class"].items()} == {
        "customer": 1.2, "prewarm": 0.88, "seed": 0.66, "refresh": 0.58, "unlabelled:other": 0.38}
    assert st["background_share"] == round((0.29 + 0.44 + 0.33) / 1.85, 4)
    assert not [w for w in r["report_warnings"] if "871" in w]


def test_evaluate_reports_units_per_phase(tmp_path, capsys):
    files = _run(tmp_path, {"cpu-01-120000.pb.gz": PROFILE})
    u = _evaluate(tmp_path, files, capsys)["work_class_report_only"]["units"]
    assert u["status"] == "present" and u["restarts"] == 0
    p = u["by_phase"]
    assert p["storm"]["started"] == {"customer": 1, "refresh": 1, "prewarm": 1, "seed": 1, "comparator": 0,
                                     "drain": 0, "fill_bg": 0, "refresh_viewed": 0}
    assert p["burst"]["started"]["refresh"] == 39 and p["burst"]["started"]["drain"] == 3
    assert p["churn:list_dep@1"]["started"]["refresh"] == 50
    assert p["churn:list_dep@1"]["started"]["fill_bg"] == 7
    assert p["all"]["completed"]["refresh"] == 90


def test_evaluate_says_missing_when_the_image_has_no_label(tmp_path, capsys):
    bare = _profile([(10_000_000, {}, ["x", "runtime.main"])])
    files = _run(tmp_path, {"cpu-01-120000.pb.gz": bare}, with_expvar=False)
    r = _evaluate(tmp_path, files, capsys)
    wc = r["work_class_report_only"]
    assert wc["cpu"]["status"] == "label_absent" and wc["units"]["status"] == load606.REPORT_ABSENT
    warns = [w for w in r["report_warnings"] if "871" in w or KEY in w]
    assert len(warns) == 2 and any("MISSING, not zero" in w for w in warns)


def test_evaluate_without_profile_dir_or_files(tmp_path, capsys):
    files = _run(tmp_path, {"cpu-01-120000.pb.gz": PROFILE}, copy_files=False)
    cpu = _evaluate(tmp_path, files, capsys)["work_class_report_only"]["cpu"]
    assert cpu["status"] == "no_profiles" and cpu["missing_profiles"] == ["cpu-01-120000.pb.gz"]
    assert "1 missing" in cpu["note"]
    cpu = _evaluate(tmp_path, files, capsys, profile_dir=False)["work_class_report_only"]["cpu"]
    assert cpu["status"] == "no_profiles"


def test_work_class_cli_attributes_profiles(tmp_path, capsys):
    p = tmp_path / "cpu.pb.gz"
    shutil.copyfile(FIX / "work_class_cpu_871.pb.gz", p)
    capsys.readouterr()
    assert load606.main(["work-class", str(p)]) == 0
    a = json.loads(capsys.readouterr().out)
    assert a["by_class"]["customer"]["cpu_s"] == 0.6 and a["label_seen"]


def test_run606_hands_the_heap_dir_to_evaluate():
    src = (Path(load606.__file__).resolve().parent.parent / "run606.sh").read_text()
    assert '--profile-dir "$OUT/heap"' in src
