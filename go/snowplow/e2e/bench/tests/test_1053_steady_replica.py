"""#1053: run 20's paired verdict (A = 1.12.56, B = 1.12.60, bench 871a4e09)
FAILed on steady p99 alone: B / A 1.0262 against a bar of 1.024. That bar
had no between-run term: the replica (runs 12/13) holds storm, burst and
churn, never steady (those runs had no steady phase), so the cell's
tolerance was A's within-run sampling spread alone.

No data gives steady a between-run term today: the three pairs with a
steady phase (17a5/17b5, 19a3/19b3, 20a/20b) all compare two different
images, and the only same-image runs (1.12.56 in 17a5 and 20a, 1.12.57 in
17b5 and 19a3) cross the workload change of bench d249d7d2 (#999; steady
p99 ~40 ms against ~250 ms). A's within-run spread cannot stand in at any
block length: a cluster's constant offset adds nothing to it (custlat's
module doc; the measured swings are consistent with that, they do not
show it). So a cell with no replica ratio PASSes
inside the within-run bar (the between-run term could only widen it) and
has no FAIL bar: past it, INCONCLUSIVE with the reason.

tests/fixtures/customer_latency_run20_pair.json.gz is run 20's two REAL
verdicts trimmed to the fields custlat.pair() reads (the source files'
sha256 inside), with the judged cells of the paired verdict run606_pair.sh
wrote.
"""

from __future__ import annotations

import copy
import gzip
import io
import json
import math
from contextlib import redirect_stdout
from pathlib import Path
from statistics import NormalDist

import pytest

from bench import custlat
from tests import fake_region

FIX = Path(__file__).resolve().parent / "fixtures"
BENCH = Path(__file__).resolve().parents[1]
REPLICA_FILE = BENCH / "customer_latency_replica_runs12_13.json"
REPLICA = custlat.load_replica(str(REPLICA_FILE))
with gzip.open(FIX / "customer_latency_run20_pair.json.gz", "rt") as fh:
    R20 = json.load(fh)
# #1383 gap 7: each arm's cluster shape with its GKE versions (run 20 predates the record)
for _x in ("a", "b"):
    R20[_x]["report"]["cluster_shape"] = fake_region.cluster_shape()
P99 = "customer_latency.pair[steady].p99_ms"


def _gp(v, phase="steady"):
    return v["report"]["customer_latency"]["gate_phases"][phase]


def _pair(a=None, b=None, replica=REPLICA):
    return custlat.pair(copy.deepcopy(a or R20["a"]), copy.deepcopy(b or R20["b"]), replica=replica)


def _checks(p):
    return {c["check"]: c for c in p["checks"]}


def _planted(k, phase="steady", metric="p99_ms"):
    """B = run 20's B with one cell set to k x A's value: a real k-fold
    change of the candidate, everything else as measured."""
    b = copy.deepcopy(R20["b"])
    _gp(b, phase)[metric] = round(_gp(R20["a"], phase)[metric] * k, 4)
    return b


def _with_steady(terms: dict) -> dict:
    """The committed replica plus a steady ratio per metric (what an A/A
    pair's `custlat replica` would add)."""
    rep = copy.deepcopy(REPLICA)
    rep["phases"]["steady"] = dict(terms)
    return rep


# ── run 20's own data reproduces the issue ──

def test_run20_steady_bar_had_no_between_run_term():
    """The recorded FAIL is steady p99 1.0262 past 1.024, and that bar is A's
    within-run spread alone: exp(z sqrt2 0.0066) at the 9-cell z, the
    replica None (the committed file has no steady)."""
    rec = {(c["phase"], c["metric"]): c for c in R20["recorded"]["cells"]}
    c = rec[("steady", "p99_ms")]
    assert R20["recorded"]["verdict"] == "FAIL"
    assert R20["recorded"]["fail_reasons"] == [
        f"{P99}: steady p99_ms: B is 1.0262x A, beyond the spread (<= 1.024)"]
    assert (c["ratio"], c["bar"], c["replica"], c["a_half_sd"]) == (1.0262, "<= 1.024", None, 0.0066)
    z = NormalDist().inv_cdf(1 - 0.05 / 9)
    assert round(math.exp(z * math.sqrt(2) * c["a_half_sd"]), 4) == 1.024 == c["within"]
    assert "steady" not in REPLICA["phases"] and set(REPLICA["phases"]) == {"storm", "burst", "churn"}
    # the other eight judged cells PASSed: steady p99 alone decided the pair
    assert [k for k, x in rec.items() if x["result"] != "PASS"] == [("steady", "p99_ms")]


def test_the_trimmed_fixture_recomputes_the_recorded_cells():
    """pair() on the trimmed verdicts gives the recorded ratio, bar, within
    and replica of every judged cell: the fixture is run 20's pair."""
    p = _pair()
    got = {(c["phase"], c["metric"]): c for c in p["cells"] if c["judged"]}
    for r in R20["recorded"]["cells"]:
        g = got[(r["phase"], r["metric"])]
        assert (g["a"], g["b"], g["ratio"], g["bar"], g["within"], g["a_half_sd"], g["replica"]) == (
            r["a"], r["b"], r["ratio"], r["bar"], r["within"], r["a_half_sd"], r["replica"]), r


# ── the new bar's verdict on run 20 ──

def test_run20_steady_p99_is_inconclusive_with_its_reason():
    p = _pair()
    c = _checks(p)[P99]
    assert p["verdict"] == "INCONCLUSIVE" and p["fail_reasons"] == []
    assert c["result"] == "INCONCLUSIVE"
    assert "past the within-run bar <= 1.024" in c["why"]
    assert "no between-run replica for steady p99_ms" in c["why"]
    assert "A/A calibration" in c["why"]
    cell = next(x for x in p["cells"] if x["phase"] == "steady" and x["metric"] == "p99_ms")
    assert cell["fail_bar"] == "n/a (no between-run replica)"
    assert cell["fail_bar_from"] == "no between-run replica: no FAIL bar"
    assert p["method"]["no_replica"] == ["steady.p95_ms", "steady.p99_ms", "steady.req_s"]
    # everything else is the recorded result: only the cell without a
    # between-run term moved, and it moved to INCONCLUSIVE, not to PASS
    assert {k: x["result"] for k, x in _checks(p).items() if k != P99} == {
        f"customer_latency.pair[{c_['phase']}].{c_['metric']}": c_["result"]
        for c_ in R20["recorded"]["cells"] if (c_["phase"], c_["metric"]) != ("steady", "p99_ms")}


def test_run20_through_the_cli_exits_inconclusive(tmp_path):
    """The path run606_pair.sh runs: `custlat pair` with the committed
    replica file, exit 4 = INCONCLUSIVE."""
    for x in ("a", "b"):
        (tmp_path / x).mkdir()
        (tmp_path / x / "verdict.json").write_text(json.dumps(R20[x]))
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = custlat.main(["pair", "--a", str(tmp_path / "a" / "verdict.json"),
                           "--b", str(tmp_path / "b" / "verdict.json"), "--replica", str(REPLICA_FILE)])
    out = json.loads(buf.getvalue())
    assert (rc, out["verdict"]) == (4, "INCONCLUSIVE")
    assert _checks(out)[P99]["result"] == "INCONCLUSIVE"


def test_inside_the_within_bar_passes_without_any_replica():
    """PASS needs no between-run term: run 20's steady p95 (0.9807) and
    req/s (0.9996) PASS with no replica at all, and a steady p99 just
    inside the within-run bar PASSes too."""
    p = _pair(replica=None)
    ch = _checks(p)
    assert ch["customer_latency.pair[steady].p95_ms"]["result"] == "PASS"
    assert ch["customer_latency.pair[steady].req_s"]["result"] == "PASS"
    assert _checks(_pair(b=_planted(1.023)))[P99]["result"] == "PASS"


# ── a planted 1.5x regression ──

@pytest.mark.parametrize("rp", [1.0, 1.024, 1.031, 1.039, 1.0603, 1.2, 1.4999])
def test_planted_x15_steady_p99_fails_once_steady_has_a_replica(rp):
    """With a measured steady term (an A/A pair's), a 1.5x steady p99
    FAILs for every between-run ratio below 1.5: the tolerance is
    max(within, replica), the larger of two ratios, never a sum or a
    product of slacks (runs 19/20's segment bars 1.031-1.039 and run 19's
    steady p95 B/A, 1.0603, all included)."""
    p = _pair(b=_planted(1.5), replica=_with_steady({"p95_ms": rp, "p99_ms": rp, "req_s": rp}))
    c = _checks(p)[P99]
    assert c["result"] == "FAIL" and p["verdict"] == "FAIL", c
    assert c["why"] == f"steady p99_ms: B is 1.5x A, beyond the spread (<= {round(max(1.024, rp), 4)})"


def test_a_steady_replica_built_by_custlat_replica_sets_the_steady_bar():
    """The A/A path end to end: `custlat replica`'s ratios of two reports
    with a steady phase carry a steady term, and pair() bars steady with it
    (run 20's 1.0262 PASSes inside a 1.04 between-run ratio, a 1.5x FAILs)."""
    r1 = copy.deepcopy(R20["a"]["report"]["customer_latency"])
    r2 = copy.deepcopy(r1)
    for m, k in (("p95_ms", 1.04), ("p99_ms", 1.04), ("req_s", 1 / 1.01)):
        r2["gate_phases"]["steady"][m] = round(r2["gate_phases"]["steady"][m] * k, 4)
    aa = custlat.replica_ratios(r1, r2, [{"dir": "aa-1"}, {"dir": "aa-2"}])
    assert aa["phases"]["steady"]["p99_ms"] == 1.04
    rep = _with_steady(aa["phases"]["steady"])
    p = _pair(replica=rep)
    assert _checks(p)[P99]["result"] == "PASS" and p["verdict"] == "PASS" and p["method"]["no_replica"] == []
    p = _pair(b=_planted(1.5), replica=rep)
    assert _checks(p)[P99]["result"] == "FAIL" and p["verdict"] == "FAIL"


@pytest.mark.parametrize("k", [1.03, 1.5, 3.0, 10.0])
def test_planted_steady_p99_without_a_replica_is_never_pass(k):
    """Without a steady term no size of regression is a measured FAIL —
    and none is a PASS: the pair is INCONCLUSIVE (exit 4), with the reason."""
    p = _pair(b=_planted(k))
    c = _checks(p)[P99]
    assert c["result"] == "INCONCLUSIVE" and p["verdict"] == "INCONCLUSIVE", (k, c)
    assert "no between-run replica" in c["why"]


def test_req_s_is_mirrored_without_a_replica():
    """req/s, lower is worse: x0.67 steady req/s with no steady term is
    INCONCLUSIVE (never PASS), and FAILs once a steady term is measured."""
    b = _planted(0.67, metric="req_s")
    c = _checks(_pair(b=b))["customer_latency.pair[steady].req_s"]
    assert c["result"] == "INCONCLUSIVE" and "no between-run replica for steady req_s" in c["why"]
    c = _checks(_pair(b=b, replica=_with_steady({"req_s": 1.03})))["customer_latency.pair[steady].req_s"]
    assert c["result"] == "FAIL"


def test_cells_with_a_replica_still_fail_a_planted_x15():
    """Only cells without a between-run term changed: a 1.5x storm p99 (its
    replica 1.0651) FAILs as before."""
    p = _pair(b=_planted(1.5, phase="storm"))
    assert _checks(p)["customer_latency.pair[storm].p99_ms"]["result"] == "FAIL" and p["verdict"] == "FAIL"


# ── a missing replica source gives INCONCLUSIVE with its reason ──

def test_no_replica_file_at_all_never_fails_a_latency_cell():
    """No replica file: no cell has a between-run term, so a 1.5x anywhere
    is INCONCLUSIVE with the reason, never FAIL and never PASS."""
    for phase in ("storm", "churn", "steady"):
        p = _pair(b=_planted(1.5, phase=phase), replica=None)
        c = _checks(p)[f"customer_latency.pair[{phase}].p99_ms"]
        assert c["result"] == "INCONCLUSIVE" and p["verdict"] == "INCONCLUSIVE", phase
        assert f"no between-run replica for {phase} p99_ms" in c["why"]
    assert len(_pair(replica=None)["method"]["no_replica"]) == 9


def test_shortened_ready_time_without_a_replica_is_inconclusive_for_the_replica_reason():
    """The jointly-Ready path: a kept sample past its PASS bar, no replica
    for the cell -> INCONCLUSIVE naming the missing between-run term, the
    FAIL bar n/a."""
    z = NormalDist().inv_cdf(1 - 0.05 / 9)
    sa = {"n": 1000, "seconds": 5000.0, "blocks": 500, "p99_ms": 250.0, "half_sd": {"p99_ms": 0.01}}
    sb = dict(sa, p99_ms=375.0)
    e = {"a_s": 9000.0, "b_s": 9000.0, "why": [{"what": "A restart", "a_s": 9000.0, "b_s": 9000.0}]}
    c = custlat._cell("steady", "p99_ms", sa, sb, z, REPLICA, judged=True, full_s=14000.0,
                      full_sd=0.0066, excluded=e)
    assert c["result"] == "INCONCLUSIVE" and "no between-run replica for steady p99_ms" in c["why"]
    assert c["fail_bar"] == "n/a (no between-run replica)"
    c = custlat._cell("steady", "p99_ms", sa, sb, z, _with_steady({"p99_ms": 1.04}), judged=True,
                      full_s=14000.0, full_sd=0.0066, excluded=e)
    assert c["result"] == "FAIL"
