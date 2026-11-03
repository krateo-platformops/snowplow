"""Run 21's two new collections (report only), through the evaluate CLI:
  #1099 refresh_cycle: the refresh-cycle wait and duration per tier, Put ->
        frame per tier, identical vs changed Puts and the changed share per class;
  #1082 stale_push_by_layer: stale serve -> the replacing frame written per
        layer, serves, unreplaced (past 10 s), open, closes; not comparable
        with run 20 (a sample exists only once a frame was written).
An image without the keys is UNMEASURED, never 0; a malformed value is named.
"""

from __future__ import annotations

import subprocess
import sys

from bench import cycle_stale as cs, load606
from tests.test_1054_slow_calls_and_blind_spots import BENCH_DIR, _evaluate, _mem


def _h(count, le, sum_ms, max_ms, bounds=(100, 1000, 3000, 10000)):
    """A cumulative histogram: `le` maps a bound to its cumulative count."""
    b = {f"le_{x}": le.get(x, count) for x in bounds}
    b["le_inf"] = count
    return {"buckets": b, "count": count, "sum_ms": sum_ms, "max_ms": max_ms}


def _cycle_vars():
    first = {cs.PUTS: {"widgets|viewed|identical": 10, "widgets|viewed|changed": 2, "restactions|normal|identical": 5,
                       "restactions|normal|changed": 5},
             cs.WAIT: {"viewed": _h(10, {100: 10}, 300, 90), "normal": _h(0, {}, 0, 0)},
             cs.DURATION: {"viewed": _h(10, {100: 8, 1000: 10}, 900, 400)},
             cs.PUT_TO_FRAME: {"viewed": _h(4, {100: 4}, 40, 20)}}
    last = {cs.PUTS: {"widgets|viewed|identical": 40, "widgets|viewed|changed": 12, "restactions|normal|identical": 5,
                      "restactions|normal|changed": 15},
            cs.WAIT: {"viewed": _h(30, {100: 20, 1000: 28, 3000: 29}, 9000, 5200), "normal": _h(0, {}, 0, 0)},
            cs.DURATION: {"viewed": _h(30, {100: 18, 1000: 30}, 2900, 800)},
            cs.PUT_TO_FRAME: {"viewed": _h(14, {100: 14}, 140, 30)}}
    return [first, last]


def _stale_vars(unreplaced_l1=2):
    first = {cs.STALE_SERVES: {"apistage": 0, "l1": 10}, cs.STALE_UNREPLACED: {"apistage": 0, "l1": 1},
             cs.STALE_OPEN: {"apistage": 0, "l1": 1}, cs.STALE_CLOSES: {"apistage": {"pushed": 0, "dropped": 0},
                                                                        "l1": {"pushed": 8, "dropped": 0}},
             cs.STALE_TO_PUSH: {"apistage": _h(0, {}, 0, 0), "l1": _h(8, {100: 2, 1000: 8}, 3000, 900)}}
    last = {cs.STALE_SERVES: {"apistage": 3, "l1": 110}, cs.STALE_UNREPLACED: {"apistage": 0, "l1": 1 + unreplaced_l1},
            cs.STALE_OPEN: {"apistage": 0, "l1": 4}, cs.STALE_CLOSES: {"apistage": {"pushed": 3, "dropped": 0},
                                                                       "l1": {"pushed": 100, "dropped": 2}},
            cs.STALE_TO_PUSH: {"apistage": _h(3, {100: 1, 1000: 3}, 600, 400),
                               "l1": _h(100, {100: 30, 1000: 92, 3000: 98, 10000: 98}, 90000, 31000)}}
    return [first, last]


def test_the_two_prefixes_are_named_collection_entries():
    assert load606.REFRESH_CYCLE_PREFIX in load606.RUN8_VARS_PREFIXES and load606.STALE_PREFIX in load606.RUN8_VARS_PREFIXES
    p = subprocess.run([sys.executable, "-m", "bench.load606", "prefixes"], capture_output=True, text=True,
                       cwd=BENCH_DIR, check=True)
    got = p.stdout.split()
    assert load606.REFRESH_CYCLE_PREFIX in got and load606.STALE_PREFIX in got
    for k in (cs.PUTS, cs.WAIT, cs.DURATION):
        assert k.startswith(load606.REFRESH_CYCLE_PREFIX)
    assert any(cs.PUT_TO_FRAME.startswith(x) for x in got)                  # the broadcaster's prefix
    for k in (cs.STALE_TO_PUSH, cs.STALE_SERVES, cs.STALE_UNREPLACED, cs.STALE_OPEN, cs.STALE_CLOSES):
        assert k.startswith(load606.STALE_PREFIX)


def test_the_refresh_cycle_reports_wait_duration_and_the_changed_share(tmp_path):
    r = _evaluate(tmp_path, extras=_cycle_vars())["report"]["refresh_cycle"]
    assert r["status"] == "present" and r["malformed"] == []
    assert r["puts_by_class"] == {"widgets": {"identical": 30, "changed": 10, "changed_share": 0.25},
                                  "restactions": {"identical": 0, "changed": 10, "changed_share": 1.0}}
    w = r["wait_by_tier"]["viewed"]                          # 20 waits during the run (30 - 10)
    assert w["count"] == 20 and w["mean_ms"] == 435.0 and w["max_ms_lifetime"] == 5200
    assert w["share_le_1000"] == 0.9 and w["share_le_3000"] == 0.95 and w["p95_bucket"] == "<= 3000"
    assert r["wait_by_tier"]["normal"] == {"count": 0, "max_ms_lifetime": 0}
    assert r["duration_by_tier"]["viewed"]["count"] == 20 and r["put_to_frame_by_tier"]["viewed"]["count"] == 10


def test_the_stale_push_reports_each_layer_and_the_unreplaced(tmp_path):
    r = _evaluate(tmp_path, extras=_stale_vars())["report"]["stale_push_by_layer"]
    assert r["status"] == "present" and r["malformed"] == [] and "not comparable with run 20" in r["note"]
    l1 = r["layers"]["l1"]
    assert (l1["serves"], l1["unreplaced"], l1["open_last"]) == (100, 2, 4)
    assert l1["closes"] == {"pushed": 92, "dropped": 2}
    assert l1["to_push"]["count"] == 92 and l1["to_push"]["share_le_1000"] == round(84 / 92, 4)
    assert l1["to_push"]["p95_bucket"] == "<= 3000"
    assert r["layers"]["apistage"]["to_push"]["count"] == 3 and r["unreplaced_total"] == 2


def test_an_image_without_the_keys_is_unmeasured_never_zero(tmp_path):
    rep = _evaluate(tmp_path, extras=[{"snowplow_resolved_cache": {}}] * 2)["report"]
    for name, pr in (("refresh_cycle", "#1099"), ("stale_push_by_layer", "#1082")):
        assert rep[name]["status"] == "UNMEASURED" and pr in rep[name]["why"], rep[name]
    assert cs.refresh_cycle([])["status"] == "UNMEASURED" and cs.stale_push_by_layer([])["status"] == "UNMEASURED"


def test_a_malformed_value_is_named_and_the_rest_reported():
    v = _cycle_vars()
    v[1][cs.PUTS]["widgets|viewed"] = 3
    v[1][cs.WAIT]["viewed"] = 42
    r = cs.refresh_cycle(_mem(v))
    assert r["status"] == "present" and r["puts_by_class"]["widgets"]["changed"] == 10
    assert r["wait_by_tier"]["viewed"] is None and r["duration_by_tier"]["viewed"]["count"] == 20
    assert r["malformed"] == [f"{cs.PUTS}[widgets|viewed] is not a class|tier|content count",
                              f"{cs.WAIT}[viewed] is not a histogram"]
    s = _stale_vars()
    s[1][cs.STALE_UNREPLACED] = ["l1"]
    r = cs.stale_push_by_layer(_mem(s))
    assert r["unreplaced_total"] is None and f"{cs.STALE_UNREPLACED} is not a {{layer: count}} map" in r["malformed"]
    assert r["layers"]["l1"]["serves"] == 100
