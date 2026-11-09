"""The refresher's resolve memo in the bench (snowplow #1284): mem606 captures
snowplow_resolve_memo, check-prefixes enforces it, and evaluate reports, INFO
only and never judged, its hits, misses, stale misses and would-joins per phase
with the hit share and the would-join share (the share of produces an
in-flight join could remove). An image without the counters reads ABSENT,
never 0. The report arms run through the evaluate CLI.
"""

from __future__ import annotations

import subprocess
import sys

from bench import load606
from tests.test_1066_refresher_fields import _cli, _mem_sample, _write_mem

KEY = load606.RESOLVE_MEMO_KEY


def _memo(hits, misses, stale, joins, **gauges):
    return {"hits_total": hits, "misses_total": misses, "stale_misses_total": stale,
            "would_join_total": joins, "entries": 3, "body_bytes": 900, **gauges}


def _mem(i, memo=None):
    s = _mem_sample(i)
    if memo is not None:
        s["extra"][KEY] = memo
    return s


def test_mem606_captures_it_and_check_prefixes_enforces_it():
    assert KEY in load606.RUN8_VARS_PREFIXES
    assert load606.prefixes_missing(load606.RUN8_VARS_PREFIXES) == []
    cut = [p for p in load606.RUN8_VARS_PREFIXES if p != KEY]
    assert load606.prefixes_missing(cut) == [KEY]
    from tests.test_run606_real_sequence import BENCH_DIR
    p = subprocess.run([sys.executable, "-m", "bench.load606", "check-prefixes", *cut], cwd=BENCH_DIR,
                       capture_output=True, text=True)
    assert p.returncode == 2 and KEY in p.stderr


def test_evaluate_reports_the_lookups_and_the_would_join_share(tmp_path):
    mem = _write_mem(tmp_path / "mem.jsonl", [_mem(0, _memo(100, 40, 4, 2)), _mem(1, _memo(400, 140, 10, 27)),
                                              _mem(2, _memo(700, 240, 20, 52))])
    rep = _cli(tmp_path, mems=[mem])["resolve_memo_report_only"]
    assert rep["result"] == "INFO"
    w = rep["memo"]["by_phase"]["all"]
    assert (w["hits"], w["misses"], w["stale_misses"], w["would_join"]) == (600, 200, 16, 50)
    assert w["hit_share"] == 0.75 and w["would_join_share"] == 0.25
    assert rep["lines"][0].startswith("INFO resolve memo (whole measure, 20 s): hits 600, misses 200 (stale 16), "
                                      "hit share 75.00%")
    assert rep["lines"][1] == ("INFO resolve memo would-join (whole measure): 50 of 200 misses (25.00%) started "
                               "while a produce of the same key was in flight")


def test_a_restart_counts_the_pair_after_it_from_zero(tmp_path):
    s0, s1 = _mem(0, _memo(500, 100, 0, 30)), _mem(1, _memo(20, 10, 0, 1))
    s0["num_gc"], s1["num_gc"] = 40, 2                       # the process began again
    r = load606.resolve_memo_report([s0, s1], [])
    w = r["memo"]["by_phase"]["all"]
    assert (w["hits"], w["misses"], w["would_join"]) == (20, 10, 1)
    assert r["memo"]["restarts"] == 1 and any("1 restart(s)" in ln for ln in r["lines"])


def test_no_miss_reads_no_share_never_zero():
    r = load606.resolve_memo_report([_mem(0, _memo(5, 2, 0, 0)), _mem(1, _memo(9, 2, 0, 0))], [])
    w = r["memo"]["by_phase"]["all"]
    assert w["misses"] == 0 and w["would_join_share"] is None
    assert "0 of 0 misses (n/a)" in r["lines"][1]


def test_an_image_without_the_counters_is_absent_never_zero(tmp_path):
    # 1.12.x before the counters: the map carries the #883 gauges only
    old = {"entries": 3, "body_bytes": 900, "dropped_stale_total": 1}
    mem = _write_mem(tmp_path / "mem.jsonl", [_mem(0, old), _mem(1, old)])
    rep = _cli(tmp_path, mems=[mem])["resolve_memo_report_only"]
    assert rep["memo"]["status"] == "ABSENT" and "absent, not 0" in rep["memo"]["why"]
    assert rep["lines"] == [f"INFO resolve memo: ABSENT ({rep['memo']['why']})"]
    assert not any("share" in ln or "hits" in ln for ln in rep["lines"])
    r = load606.resolve_memo_report([_mem(0), _mem(1)], [])           # no map at all
    assert r["memo"]["status"] == "ABSENT"
