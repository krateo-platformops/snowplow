"""#1422: GKE resized the control plane of run 26i's clusters by itself, at different times per arm, and
the bench neither recorded it nor knew whether it overlapped an arm's measurement.

  1. record: collect writes each arm's cluster operations (bench/clusterops.py record), a failed listing
     as "could not be read", never as none;
  2. mark: evaluate says, per measured phase, "no operation overlapped", "overlapped by: ..." or
     "operations could not be read: ..."; the pair names the phases only one arm measured under one;
  3. prevent: gke-up adds a maintenance exclusion over the cluster's TTL window, reads it back, and
     refuses a cluster that does not hold it.

Every report is read through the CLI the run uses (`python -m bench.clusterops record`, `python -m
bench.load606 evaluate`, `python -m bench.custlat pair | replica`, run606.sh), against a fake gcloud that
answers in gcloud's own shapes (tests/fake_gke_ops.py says where each comes from).
"""

from __future__ import annotations

import inspect
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

from bench import clusterops, gke
from tests import fake_gke_ops
from tests.fake_gke_ops import operation
from tests.test_gke_pvc_disks import BENCH, NAME, _calls, fakes  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _write_run
from tests.test_run606_real_sequence import BENCH_DIR, RUN_NAME, harness  # noqa: F401  (fixture)

T = 1794650000.0                 # 2026-11-14T09:53:20Z
P1 = ("churn:list_dep@1", T, T + 600)
P2 = ("churn:list_dep@10", T + 600, T + 1200)
DIGEST = "sha256:" + "ab" * 32
# the cluster under test: GKE says it was created an hour before the first phase (Cluster.createTime, as
# describe prints it), and its CREATE_CLUSTER started 4 s later, as on run 26i's clusters
CREATED = "2026-11-14T08:53:16+00:00"            # T - 3604
CREATE = operation("CREATE_CLUSTER", NAME, start=T - 3600, end=T - 3200)


def _phases_file(d):
    """A run's input that gives evaluate two measured phases with a recorded start and end."""
    f = d / "fresh-fresh606-0.jsonl"
    # the measured reboot at the first phase's start: the measure (#1422, 2026-11-16) is its first phase
    f.write_text(json.dumps({"kind": "boot", "t_restart": P1[1]}) + "\n"
                 + "".join(json.dumps({"kind": "churn", "label": n.split(":", 1)[1], "t0": a, "t1": b,
                                       "target_rate": 1}) + "\n" for n, a, b in (P1, P2)))
    return f


def _record(d, ops, name=NAME, control=CREATE, created=CREATED, **env):
    """The record run606's collect takes: the CLI, against the fake gcloud on PATH. `control`: the
    cluster's own CREATE_CLUSTER, which the region lists with `ops` (None: it does not); `created`: the
    creation time gke-up read back from the cluster, in its maintenance-exclusion.json (None: no file)."""
    out, excl = d / "cluster-operations.json", d / "maintenance-exclusion.json"
    if created is not None:
        excl.write_text(json.dumps({"kind": "maintenance_exclusion", "cluster": name, "cluster_created": created}))
    listed = ([control] if control is not None else []) + list(ops)
    p = subprocess.run([sys.executable, "-m", "bench.clusterops", "record", "--name", name, "--created-from",
                        str(excl), "--out", str(out), "--yes"],
                       cwd=BENCH, capture_output=True, text=True, timeout=120,
                       env=dict(os.environ, FAKE_OPERATIONS_EXTRA=json.dumps(listed), **env))
    assert p.returncode == 0, p.stderr           # a record is written whatever the read answered
    return out, json.loads(out.read_text()), p.stdout


def _evaluate(d, rec_path):
    """The run's verdict.json, as s_evaluate makes it: the evaluate CLI, its stdout."""
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(_phases_file(d)),
                        "--cluster-operations", str(rec_path), "--expect-digest", DIGEST],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    v = json.loads(p.stdout)
    (d / "verdict.json").write_text(p.stdout)
    return v, v["report"]["cluster_operations_report_only"], p.stderr


def _arm(tmp, label, ops, **env):
    d = tmp / label
    d.mkdir()
    path, _, _ = _record(d, ops, **env)
    return d, _evaluate(d, path)


def _by_phase(sec):
    return {p["phase"]: p for p in sec["phases"]}


# ─── 1. record ──────────────────────────────────────────────────────────────

def test_the_record_holds_this_clusters_operations_only_by_exact_target(fakes):
    ops = [operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200),
           operation("UPGRADE_NODES", NAME, start=T + 300, end=T + 400, pool="snowplow"),
           operation("RESIZE_CLUSTER", NAME + "b", start=T + 100, end=T + 200),       # a longer name
           operation("RESIZE_CLUSTER", NAME[:-1], start=T + 100, end=T + 200),        # a shorter one
           operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200, region="europe-west1")]
    # the region's listing: gcloud is asked for the cluster's region and answers only that region's
    path, rec, said = _record(fakes, ops)
    assert rec["readable"] is True and rec["unreadable"] is None
    assert [(o["type"], o["target"], o["status"]) for o in rec["operations"]] == [
        ("CREATE_CLUSTER", "cluster", "DONE"), ("RESIZE_CLUSTER", "cluster", "DONE"),
        ("UPGRADE_NODES", "nodePool/snowplow", "DONE")]
    # the proof that this is the cluster's own listing: its CREATE_CLUSTER, named
    assert rec["control"]["operation"] == CREATE["name"] and rec["control"]["cluster_created"] == "2026-11-14T08:53:16Z"
    first = rec["operations"][1]
    assert set(first) == {"id", "type", "status", "target", "start", "end", "start_t", "end_t"}
    assert first["id"] == ops[0]["name"] and first["start"] == ops[0]["startTime"] and first["end"] == ops[0]["endTime"]
    assert (first["start_t"], first["end_t"]) == (T + 100, T + 200)
    # nothing of the operation but that: no link, so no project number the evidence did not hold
    assert fake_gke_ops.PROJECT_NUMBER not in path.read_text() and "googleapis" not in path.read_text()
    assert f"cluster operations of {NAME} (its CREATE_CLUSTER {CREATE['name']} is in the listing): 3 recorded" in said
    listed = [c for c in _calls(fakes) if c[:4] == ["gcloud", "container", "operations", "list"]]
    assert listed == [["gcloud", "container", "operations", "list", "--project", gke.DEFAULT_PROJECT,
                       "--region", "europe-west4", "--format", "json"]]          # no client-side --filter


def test_a_running_operation_is_recorded_with_no_end(fakes):
    _, rec, said = _record(fakes, [operation("RESIZE_CLUSTER", NAME, start=T + 650)])
    op = rec["operations"][-1]
    assert (op["status"], op["end"], op["end_t"]) == ("RUNNING", None, None) and len(rec["operations"]) == 2
    assert "not ended at the read" in said


def test_times_are_read_with_nanoseconds_and_with_an_offset():
    # the listing's own forms (2026-11-14): 7 and 9 fractional digits; describe's createTime carries an offset
    assert clusterops.epoch("2026-11-14T08:54:14.0659289Z") == pytest.approx(1794646454.0659289)
    assert clusterops.epoch("2026-11-14T09:32:18.428608411Z") == pytest.approx(1794648738.428608411)
    assert clusterops.epoch("2026-11-14T08:54:10+00:00") == 1794646450
    assert clusterops.epoch("2026-11-14T09:54:10+01:00") == 1794646450
    assert clusterops.epoch(None) is None and clusterops.epoch("soon") is None


FAILED_LISTINGS = {
    "non-zero exit": {"FAKE_OPERATIONS_RC": "1"},
    "non-zero exit, a list printed, nothing on stderr": {"FAKE_OPERATIONS_KILLED_RC": "143"},
    "exit 0, a warning on stderr, a partial list": {
        "FAKE_OPERATIONS_STDERR": "WARNING: The following zones did not respond: europe-west4-b. List results may "
                                  "be incomplete."},
    "exit 0, an error on stderr, an empty list": {
        "FAKE_OPERATIONS_OUT": "[]\n",
        "FAKE_OPERATIONS_STDERR": "ERROR: (gcloud.container.operations.list) Some requests did not succeed."},
    "exit 0, nothing printed": {"FAKE_OPERATIONS_OUT": ""},
    "exit 0, not a list": {"FAKE_OPERATIONS_OUT": "{}"},
}


@pytest.mark.parametrize("case", list(FAILED_LISTINGS))
def test_a_failed_listing_is_recorded_and_reported_as_unreadable_never_as_none(fakes, case):
    path, rec, said = _record(fakes, [operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200)],
                              **FAILED_LISTINGS[case])
    assert rec["readable"] is False and rec["operations"] is None, rec
    assert rec["unreadable"].startswith("could not be read: ")
    # the reason is the listing's own failure, not what a failed listing read as a list would be missing
    assert "gcloud container operations list " in rec["unreadable"] and "the control is missing" not in rec["unreadable"]
    assert "PLACEHOLDER-OPERATOR" not in path.read_text()          # gcloud's error text names the account
    assert rec["unreadable"] in said
    v, sec, err = _evaluate(fakes, path)
    assert sec["state"] == "UNREADABLE" and sec["operations"] is None
    assert {p["overlap"] for p in sec["phases"]} == {clusterops.UNKNOWN} and len(sec["phases"]) == 3
    text = "\n".join(sec["lines"])
    assert text.count("operations could not be read: ") == 4 and "no operation overlapped" not in text
    assert "overlapped by" not in text
    assert any("cluster operations: operations could not be read" in w for w in v["report"]["report_warnings"])
    assert "INFO cluster operations: operations could not be read" in err


# The positive control: a listing that answers cleanly and does not hold this cluster's own CREATE_CLUSTER,
# started when GKE says the cluster was created, proves nothing about the cluster.
RESIZE = operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200)
UNPROVEN = {
    "an empty list": dict(ops=[], control=None),
    "another region's list, this cluster's name and times in it": dict(ops=[], control=None, FAKE_OPERATIONS_OUT=json.dumps(
        [operation("CREATE_CLUSTER", NAME, start=T - 3600, end=T - 3200, region="europe-west1"),
         operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200, region="europe-west1")])),
    "only another cluster's operations": dict(control=None, ops=[
        operation("CREATE_CLUSTER", NAME + "b", start=T - 3600, end=T - 3200),
        operation("RESIZE_CLUSTER", NAME + "b", start=T + 100, end=T + 200)]),
    "this cluster's later operations, not its CREATE_CLUSTER": dict(control=None, ops=[
        RESIZE, operation("CREATE_NODE_POOL", NAME, start=T - 3100, end=T - 3000, pool="snowplow")]),
    "a re-used name: the earlier cluster's CREATE_CLUSTER, not this one's": dict(control=None, ops=[
        operation("CREATE_CLUSTER", NAME, start=T - 90000, end=T - 89600),
        operation("DELETE_CLUSTER", NAME, start=T - 80000, end=T - 79700), RESIZE]),
    "a CREATE_CLUSTER of the name that started 3 minutes off the creation time": dict(control=None, ops=[
        operation("CREATE_CLUSTER", NAME, start=T - 3604 + 180, end=T - 3200), RESIZE]),
    "a node pool's operation at the creation time, no CREATE_CLUSTER": dict(control=None, ops=[
        operation("CREATE_NODE_POOL", NAME, start=T - 3600, end=T - 3200, pool="snowplow"), RESIZE]),
    "no creation time recorded for the cluster": dict(ops=[RESIZE], created=None),
    "a creation time that is not a time": dict(ops=[RESIZE], created="soon"),
}


@pytest.mark.parametrize("case", list(UNPROVEN))
def test_a_listing_not_proven_to_be_this_clusters_is_unreadable_never_none(fakes, case):
    kw = dict(UNPROVEN[case])
    path, rec, said = _record(fakes, kw.pop("ops"), **kw)
    assert rec["readable"] is False and rec["operations"] is None and "control" not in rec, rec
    assert rec["unreadable"].startswith("could not be read: ") and rec["unreadable"] in said
    if "creation time" in case and "CREATE_CLUSTER" not in case:
        assert "the cluster's creation time is not recorded" in rec["unreadable"]
    elif "another region" in case:
        assert f"cannot be told to be {NAME}'s or not" in rec["unreadable"]
        assert f"its target is {NAME} in europe-west1, and the listing asked for is europe-west4's" in rec["unreadable"]
    else:
        assert f"the control is missing: no CREATE_CLUSTER of {NAME} in europe-west4 that started within 120 s of " \
               f"the cluster's creation (2026-11-14T08:53:16Z) is in the listing" in rec["unreadable"]
    if "re-used" in case:
        assert "its CREATE_CLUSTER operation(s) started 2026-11-13T08:53:20Z: another cluster of the same name" in \
            rec["unreadable"]
    _, sec, _ = _evaluate(fakes, path)
    assert sec["state"] == "UNREADABLE" and {p["overlap"] for p in sec["phases"]} == {clusterops.UNKNOWN}
    text = "\n".join(sec["lines"])
    assert "operations could not be read: " in text and "no operation overlapped" not in text
    assert "overlapped by" not in text


def _retarget(op, link):
    op = dict(op)
    if link is None:
        del op["targetLink"]
    else:
        op["targetLink"] = link(op["targetLink"])
    return op


UNPLACED = {
    "no target link": (None, "it has no target link"),
    "the zonal form of the link": (lambda u: u.replace("/locations/europe-west4/", "/zones/europe-west4-a/"),
                                   "in a form that is not .../locations/europe-west4/clusters/"),
    "a trailing slash": (lambda u: u + "/", "in a form that is not .../locations/europe-west4/clusters/"),
    "an empty link": (lambda u: "", "it has no target link"),
}


@pytest.mark.parametrize("case", list(UNPLACED))
def test_an_operation_that_cannot_be_placed_makes_the_record_unreadable_not_shorter(fakes, case):
    """The cluster's CREATE_CLUSTER is there (the listing is this cluster's), and one operation on the
    cluster does not carry the exact target form: dropped, the phase it overlapped would read "none"."""
    link, why = UNPLACED[case]
    odd = _retarget(RESIZE, link)
    path, rec, _ = _record(fakes, [odd])
    assert rec["readable"] is False and rec["operations"] is None
    assert f"1 operation(s) of the listing cannot be told to be {NAME}'s or not: {RESIZE['name']} (RESIZE_CLUSTER): " \
        in rec["unreadable"] and why in rec["unreadable"]
    _, sec, _ = _evaluate(fakes, path)
    assert {p["overlap"] for p in sec["phases"]} == {clusterops.UNKNOWN}
    assert "no operation overlapped" not in "\n".join(sec["lines"])


def test_another_clusters_oddly_formed_target_is_not_this_clusters_problem(fakes):
    other = _retarget(operation("RESIZE_CLUSTER", NAME + "b", start=T + 100, end=T + 200), lambda u: u + "/")
    _, rec, _ = _record(fakes, [other])
    assert rec["readable"] is True and [o["type"] for o in rec["operations"]] == ["CREATE_CLUSTER"]


def test_a_reused_name_keeps_this_clusters_operations_and_drops_the_earlier_clusters(fakes):
    """Both clusters' CREATE_CLUSTER are listed: the creation time picks this one's, and nothing the earlier
    cluster did is recorded as this run's."""
    old = [operation("CREATE_CLUSTER", NAME, start=T - 90000, end=T - 89600),
           operation("RESIZE_CLUSTER", NAME, start=T - 85000, end=T - 84000),
           operation("DELETE_CLUSTER", NAME, start=T - 80000, end=T - 79700)]
    path, rec, _ = _record(fakes, old + [RESIZE])
    assert rec["readable"] is True and rec["control"]["operation"] == CREATE["name"]
    assert [o["id"] for o in rec["operations"]] == [CREATE["name"], RESIZE["name"]]
    # read against the earlier cluster's creation time, the same listing is the EARLIER cluster's
    _, rec, _ = _record(fakes, old + [RESIZE], created="2026-11-13T08:53:18+00:00")
    assert rec["control"]["operation"] == old[0]["name"]


def test_a_listing_that_does_not_answer_in_time_is_unreadable():
    def hung(argv, **kw):
        raise subprocess.TimeoutExpired(argv, kw["timeout"])
    rec = clusterops.read(gke.default_spec(NAME), created=T - 3604, run=hung, timeout=7)
    assert rec["readable"] is False and rec["operations"] is None
    assert rec["unreadable"] == "could not be read: gcloud container operations list did not answer within 7 s"


def test_a_missing_record_reads_unreadable_through_evaluate(fakes):
    v, sec, _ = _evaluate(fakes, fakes / "never-written.json")
    assert sec["state"] == "UNREADABLE" and "the run has no cluster-operations record" in sec["why"]
    assert all(p["overlap"] == clusterops.UNKNOWN for p in sec["phases"])
    assert "no operation overlapped" not in "\n".join(sec["lines"])


# ─── 2. mark ────────────────────────────────────────────────────────────────

N, O = clusterops.NONE, clusterops.OVERLAPPED
OVERLAPS = {
    # name: (operation's start, end relative to T; None = still running), then P1's and P2's outcome
    "inside the first phase": ((100, 200), O, N),
    "ending exactly at the phase's start": ((-900, 0), N, N),
    "starting exactly at the last phase's end": ((1200, 1300), N, N),
    "started before the first phase, ended inside the second": ((-100, 700), O, O),
    "around a whole phase": ((-100, 650), O, O),
    "still running at collect, started inside the second": ((650, None), N, O),
    "still running at collect, started before both": ((-50, None), O, O),
    "ended before any phase": ((-900, -100), N, N),
}


@pytest.mark.parametrize("case", list(OVERLAPS))
def test_each_measured_phase_says_which_operation_overlapped_it(fakes, case):
    (a, b), want1, want2 = OVERLAPS[case]
    op = operation("RESIZE_CLUSTER", NAME, start=T + a, end=None if b is None else T + b)
    path, _, _ = _record(fakes, [op])
    v, sec, err = _evaluate(fakes, path)
    rows = _by_phase(sec)
    assert sec["state"] == "READ"
    assert (rows[P1[0]]["overlap"], rows[P2[0]]["overlap"]) == (want1, want2)
    assert (rows[P1[0]]["t0"], rows[P1[0]]["t1"]) == (P1[1], P1[2])            # the phase's own window
    for name, want in ((P1[0], want1), (P2[0], want2)):
        (line,) = [ln for ln in sec["lines"] if f"phase {name} [" in ln]
        assert line in err                                                     # s_evaluate logs evaluate's stderr lines
        if want == O:
            assert [o["id"] for o in rows[name]["by"]] == [op["name"]]
            assert "overlapped by: RESIZE_CLUSTER 2026-11-14T" in line and op["name"] in line
            assert ("not ended at the read" in line) == (b is None)
            assert any(w.startswith(f"cluster operations: phase {name} overlapped by: RESIZE_CLUSTER")
                       for w in v["report"]["report_warnings"])
        else:
            assert rows[name]["by"] == [] and line.endswith("no operation overlapped")


def test_a_node_pool_operation_of_this_cluster_counts_and_another_clusters_does_not(fakes):
    mine = operation("UPGRADE_NODES", NAME, start=T + 100, end=T + 200, pool="loadgen")
    path, _, _ = _record(fakes, [mine, operation("RESIZE_CLUSTER", NAME + "-b", start=T + 100, end=T + 900)])
    _, sec, _ = _evaluate(fakes, path)
    rows = _by_phase(sec)
    assert [o["target"] for o in rows[P1[0]]["by"]] == ["nodePool/loadgen"] and rows[P2[0]]["overlap"] == N
    assert "overlapped by: UPGRADE_NODES on nodePool/loadgen" in "\n".join(sec["lines"])


def test_the_three_outcomes_never_share_a_wording(fakes):
    """none / overlapped / unreadable, each through the record and evaluate CLIs."""
    said = {}
    for label, ops, env in (("none", [], {}), ("overlapped", [operation("RESIZE_CLUSTER", NAME, start=T, end=T + 1)], {}),
                            ("unreadable", [], {"FAKE_OPERATIONS_RC": "1"})):
        d = fakes / label
        d.mkdir()
        path, _, _ = _record(d, ops, **env)
        (said[label],) = [ln for ln in _evaluate(d, path)[1]["lines"] if f"phase {P1[0]} [" in ln]
    words = ("no operation overlapped", "overlapped by: ", "operations could not be read: ")
    for label, own in zip(("none", "overlapped", "unreadable"), words, strict=True):
        assert [w in said[label] for w in words] == [w == own for w in words], said[label]


def test_an_operation_with_no_start_time_leaves_every_phase_unknown(fakes):
    op = operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200)
    del op["startTime"]
    path, rec, _ = _record(fakes, [op])
    assert rec["operations"][-1]["start_t"] is None
    _, sec, _ = _evaluate(fakes, path)
    assert sec["state"] == "UNREADABLE" and "no start time for operation(s)" in sec["why"]
    assert all(p["overlap"] == clusterops.UNKNOWN for p in sec["phases"])


# ─── the latency blocks: the customers' hours are marked 10 s at a time ──────

@pytest.fixture(scope="module")
def k6run(tmp_path_factory):
    """A run directory in run606's layout with run 14's real customer calls (tests/test_run15's fixture)
    and its span, as evaluate reads it."""
    d = tmp_path_factory.mktemp("k6run")
    files = _write_run(d / "src")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", *files, "--k6-out", str(d / "src" / "k6"),
                        "--browser-out", str(d / "src" / "browser"), "--browser-sessions", "5", "--no-viewers",
                        "--expect-digest", DIGEST], cwd=BENCH, capture_output=True, text=True, timeout=600)
    cl = json.loads(p.stdout)["report"]["customer_latency"]
    return d / "src", cl["k6_span"]


def _blocks_arm(tmp, k6run, label, ops, **env):
    """One arm: run 14's calls, the cluster's operations `ops` (times relative to the first call)."""
    src, (s0, _s1) = k6run
    d = tmp / label
    shutil.copytree(src, d)
    made = [operation(k, NAME, start=s0 + a, end=None if b is None else s0 + b) for k, a, b in ops]
    control = operation("CREATE_CLUSTER", NAME, start=s0 - 3600, end=s0 - 3200)
    created = gke.rfc3339(s0 - 3604)
    path, _, _ = _record(d, made, control=control, created=created, **env)
    files = sorted(str(f) for f in d.glob("*.jsonl"))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", *files, "--k6-out", str(d / "k6"),
                        "--browser-out", str(d / "browser"), "--browser-sessions", "5", "--no-viewers",
                        "--expect-digest", DIGEST, "--cluster-operations", str(path)],
                       cwd=BENCH, capture_output=True, text=True, timeout=600)
    (d / "verdict.json").write_text(p.stdout)
    v = json.loads(p.stdout)
    return d, v, v["report"]["cluster_operations_report_only"], made


def test_each_latency_block_is_marked_and_the_long_span_is_not_one_phase(fakes, k6run):
    (s0, s1) = k6run[1]
    # one operation over the blocks that start at 90, 100, 110 and 120 s; one that ENDS exactly where the
    # block at 300 s starts (over 280 and 290 only); one still running, from 15 s before the last call
    d, v, sec, made = _blocks_arm(fakes, k6run, "a", [("RESIZE_CLUSTER", 95, 125), ("UPGRADE_MASTER", 285, 300),
                                                     ("REPAIR_CLUSTER", s1 - s0 - 15, None)])
    bl = sec["blocks"]
    nb = int((s1 - s0) // 10) + 1
    assert bl["state"] == "READ" and bl["block_s"] == 10 and bl["blocks"] == nb
    last = [st for st in bl["stretches"] if st["by"] == [made[2]["name"]]]
    assert [(round(st["t0"] - s0), round(st["t1"] - s0), st["blocks"], st["by"]) for st in bl["stretches"][:2]] == [
        (90, 130, 4, [made[0]["name"]]), (280, 300, 2, [made[1]["name"]])]
    assert len(last) == 1 and last[0]["blocks"] in (2, 3) and round(last[0]["t1"]) == round(s1)
    assert bl["overlapped"] == 6 + last[0]["blocks"]
    # a block counts in each gate phase it holds time of, as the pair statistics count it
    assert set(bl["by_phase"]) <= {"storm", "burst", "churn", "steady"}
    assert sum(r["overlapped"] for r in bl["by_phase"].values()) >= bl["overlapped"]
    assert all(0 <= r["overlapped"] <= r["blocks"] for r in bl["by_phase"].values())
    text = "\n".join(sec["lines"])
    assert f"INFO cluster operations, latency blocks: {bl['overlapped']} of {nb} blocks of 10 s overlapped by an " \
           f"operation (" in text
    assert "4 block(s): overlapped by: RESIZE_CLUSTER" in text and "2 block(s): overlapped by: UPGRADE_MASTER" in text
    assert "customer calls, first to last" not in json.dumps(sec)              # hours long: never one phase
    assert f"cluster operations: {bl['overlapped']} of {nb} latency blocks overlapped by an operation" in \
        v["report"]["report_warnings"]


def test_blocks_say_none_and_unreadable_in_their_own_words_and_the_pair_counts_them_per_arm(fakes, k6run):
    da, _, sa, _ = _blocks_arm(fakes, k6run, "a", [("RESIZE_CLUSTER", 95, 125)])
    db, _, sb, _ = _blocks_arm(fakes, k6run, "b", [])
    dc, _, sc, _ = _blocks_arm(fakes, k6run, "c", [], FAKE_OPERATIONS_RC="1")
    nb = sb["blocks"]["blocks"]
    assert sb["blocks"]["overlapped"] == 0 and sb["blocks"]["stretches"] == []
    assert f"INFO cluster operations, latency blocks: no operation overlapped any of the {nb} blocks of 10 s" in \
        "\n".join(sb["lines"])
    assert sc["blocks"]["state"] == clusterops.UNKNOWN and sc["blocks"]["overlapped"] is None
    (line,) = [ln for ln in sc["lines"] if "latency blocks" in ln]
    assert "operations could not be read: " in line and "no operation overlapped" not in line
    # the pair: how many of each arm's blocks, per gate phase
    pair = _pair_cli(da, db)
    hit = next(p for p, r in sa["blocks"]["by_phase"].items() if r["overlapped"])
    ra, rb = sa["blocks"]["by_phase"][hit], sb["blocks"]["by_phase"][hit]
    assert (f"INFO cluster operations, latency blocks, phase {hit}: N1 (baseline): {ra['overlapped']} of "
            f"{ra['blocks']} blocks overlapped by an operation; N2 (candidate): 0 of {rb['blocks']} blocks "
            f"overlapped by an operation") in pair["lines"]
    assert ra["overlapped"] == 4 and pair["blocks"]["a"]["overlapped"] == 4 and pair["blocks"]["b"]["overlapped"] == 0
    unk = _pair_cli(da, dc)
    assert any("latency blocks" in ln and "N2 (candidate): operations could not be read: " in ln for ln in unk["lines"])
    assert not any("latency blocks" in ln and "N2 (candidate): 0 of" in ln for ln in unk["lines"])


def _pair_cli(da, db, a="N1", b="N2"):
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "pair", "--a", str(da / "verdict.json"),
                        "--b", str(db / "verdict.json"), "--a-label", a, "--b-label", b],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    return json.loads(p.stdout)["cluster_operations_report_only"]


def test_the_pair_names_the_phase_only_one_arm_measured_under_an_operation(fakes):
    resize = operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200)
    da, _ = _arm(fakes, "a", [resize])
    db, _ = _arm(fakes, "b", [])
    sec = _pair_cli(da, db)
    assert sec["asymmetric"] == ["measure", P1[0]] and sec["unknown"] == []
    assert [(p["phase"], p["a"], p["b"], p["asymmetric"]) for p in sec["phases"]] == [
        ("measure", O, N, True), (P1[0], O, N, True), (P2[0], N, N, False)]
    _, one, two, blocks = sec["lines"]
    assert blocks == ("INFO cluster operations, latency blocks: N1 (baseline): no latency block; "
                      "N2 (candidate): no latency block")          # these arms have no customer calls
    assert one.startswith(f"INFO cluster operations, phase {P1[0]}: ASYMMETRIC, one arm only measured under a "
                          f"cluster operation: N1 (baseline): overlapped by: RESIZE_CLUSTER 2026-11-14T09:55:00Z.."
                          f"2026-11-14T09:56:40Z (DONE, {resize['name']})")
    assert one.endswith("N2 (candidate): no operation overlapped")
    assert two == f"INFO cluster operations, phase {P2[0]}: no operation overlapped on either arm"
    # the other way round is the same finding, on the other arm
    assert _pair_cli(db, da)["asymmetric"] == ["measure", P1[0]]


def test_the_pair_is_not_asymmetric_when_both_or_neither_overlapped_and_unknown_when_one_is_unreadable(fakes):
    resize = [operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200)]
    da, _ = _arm(fakes, "a", resize)
    db, _ = _arm(fakes, "b", resize)
    dc, _ = _arm(fakes, "c", [], FAKE_OPERATIONS_RC="1")
    both = _pair_cli(da, db)
    assert both["asymmetric"] == [] and both["unknown"] == [] and "both arms overlapped" in both["lines"][0]
    unk = _pair_cli(da, dc)
    assert unk["asymmetric"] == [] and unk["unknown"] == ["measure", P1[0], P2[0]]
    assert "whether the arms differ is UNKNOWN" in unk["lines"][0]
    assert "N2 (candidate): operations could not be read: " in unk["lines"][0]
    assert "ASYMMETRIC" not in "\n".join(unk["lines"]) and "on either arm" not in "\n".join(unk["lines"])
    # a verdict from before #1422 has no section: unknown, never symmetric
    (dc / "verdict.json").write_text(json.dumps({"verdict": "PASS", "report": {}}))
    old = _pair_cli(da, dc)
    assert old["unknown"] == ["measure", P1[0], P2[0]] and "has no cluster_operations_report_only" in old["lines"][0]


def test_the_in_run_replica_says_it_too(fakes):
    """Run 26i's own case: N1 resized mid-run, its replica N1B not. `custlat replica --in-run` is the
    command run606_stats.sh runs for the A/A pair."""
    da, _ = _arm(fakes, "n1", [operation("RESIZE_CLUSTER", NAME, start=T + 650)])
    db, _ = _arm(fakes, "n1b", [])
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "replica", str(da), str(db), "--in-run", "N1:N1B",
                        "--expect-digest", DIGEST, "--completed", "1", "1"],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, p.stderr
    sec = json.loads(p.stdout)["cluster_operations_report_only"]
    assert sec["asymmetric"] == ["measure", P2[0]]
    assert ("ASYMMETRIC, one arm only measured under a cluster operation: N1 (replica arm): overlapped by: "
            "RESIZE_CLUSTER 2026-11-14T10:04:10Z..not ended at the read (RUNNING,") in sec["lines"][2]
    assert sec["lines"][2].endswith("N1B (replica arm): no operation overlapped")


def test_the_statistics_stage_logs_the_asymmetric_phase_for_the_replica_and_for_the_pair(fakes):
    """run606_stats.sh, run as `bench.durable rejudge` and the launcher run it: the pair log is where
    the launch's reader sees it."""
    dirs = {"N1": _arm(fakes, "n1", [operation("RESIZE_CLUSTER", NAME, start=T + 650)])[0],
            "N1B": _arm(fakes, "n1b", [])[0], "N2": _arm(fakes, "n2", [])[0]}
    env = dict(os.environ, PAIR_OUT=str(fakes / "pair"), PAIR_RUN_ID="run26x", STATS_LABELS="N1 N1B N2",
               PAIRS="N1:N1B N1:N2", REPLICA_PAIR="N1:N1B", PYTHONPATH=str(BENCH))
    for label, d in dirs.items():
        env.update({f"SRC_{label}": str(d), f"EXPECTED_IMAGE_DIGEST_{label}": DIGEST, f"DONE_{label}": "1"})
    subprocess.run(["bash", str(BENCH / "run606_stats.sh")], env=env, cwd=BENCH, capture_output=True, text=True,
                   timeout=300)
    log = (fakes / "pair" / "pair.log").read_text()
    said = (f"INFO cluster operations, phase {P2[0]}: ASYMMETRIC, one arm only measured under a cluster "
            f"operation: ")
    assert f"replica N1:N1B: {said}N1 (replica arm): overlapped by: RESIZE_CLUSTER" in log, log
    assert f"N1 vs N2: {said}N1 (baseline): overlapped by: RESIZE_CLUSTER" in log, log
    assert f"N1 vs N2: INFO cluster operations, phase {P1[0]}: no operation overlapped on either arm" in log


# ─── 3. the maintenance exclusion ───────────────────────────────────────────

def _up(tmp):
    out = tmp / "maintenance-exclusion.json"
    rc = gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--ttl-hours", "13",
                   "--maintenance-out", str(out), "--yes"])
    return rc, json.loads(out.read_text()) if out.exists() else None


def _flag(argv, name):
    return argv[argv.index(name) + 1]


def test_the_exclusions_window_is_the_clusters_ttl_window():
    spec = gke.default_spec(NAME, now=T, ttl_hours=13)
    argv = gke.exclusion_command(spec)
    assert argv[:5] == ["gcloud", "container", "clusters", "update", NAME]
    assert (_flag(argv, "--project"), _flag(argv, "--region")) == (spec.project, "europe-west4")
    assert _flag(argv, "--add-maintenance-exclusion-name") == gke.EXCLUSION_NAME
    # no_upgrades: the one scope that holds both the control plane's and the nodes' minor and patch upgrades
    assert _flag(argv, "--add-maintenance-exclusion-scope") == "no_upgrades"
    start, end = (_flag(argv, f"--add-maintenance-exclusion-{x}") for x in ("start", "end"))
    assert (start, end) == ("2026-11-14T09:53:20Z", "2026-11-14T22:53:20Z")
    assert gke.rfc3339_epoch(start) == spec.created_at == T
    assert gke.rfc3339_epoch(end) == spec.ttl_expires == T + 13 * 3600
    # ... which is the epoch the cluster's own ttl-expires label carries, in the create gke-up runs
    create = gke.create_commands(spec)[0]
    assert f"ttl-expires={int(gke.rfc3339_epoch(end))}" in _flag(create, "--labels").split(",")
    # `clusters create` takes no exclusion flag (SDK 561): the exclusion is its own update
    assert not any("maintenance-exclusion" in t for c in gke.create_commands(spec) for t in c)


def test_gke_up_adds_the_exclusion_reads_it_back_and_records_it(fakes, capsys):
    rc, rec = _up(fakes)
    out = capsys.readouterr()
    assert rc == 0, out.err
    calls = _calls(fakes)
    i_up = next(i for i, c in enumerate(calls) if c[:4] == ["gcloud", "container", "clusters", "update"])
    i_create = next(i for i, c in enumerate(calls) if c[:4] == ["gcloud", "container", "clusters", "create"])
    i_proof = max(i for i, c in enumerate(calls) if c[0] == "kubectl")       # the TTL backstop stands first
    assert i_create < i_proof < i_up
    assert calls[i_up + 1][:5] == ["gcloud", "container", "clusters", "describe", NAME]      # the read-back
    create, update = calls[i_create], calls[i_up]
    ttl = int(dict(kv.split("=") for kv in _flag(create, "--labels").split(","))["ttl-expires"])
    assert gke.rfc3339_epoch(_flag(update, "--add-maintenance-exclusion-end")) == ttl
    assert ttl - gke.rfc3339_epoch(_flag(update, "--add-maintenance-exclusion-start")) == 13 * 3600
    assert rec["verified"] is True and rec["cluster"] == NAME and rec["name"] == gke.EXCLUSION_NAME
    assert rec["requested"]["end_t"] == rec["requested"]["ttl_expires"] == ttl
    assert rec["read_back"] == {"startTime": rec["requested"]["start"], "endTime": rec["requested"]["end"],
                                "maintenanceExclusionOptions": {"scope": "NO_UPGRADES"}}
    assert "control-plane resizes and repairs" in rec["does_not_cover"]
    # when GKE says the cluster was created, from the same read-back: what the operations record is proven against
    assert gke.rfc3339_epoch(rec["cluster_created"]) is not None
    assert "maintenance exclusion in place: bench-run-ttl (no_upgrades, " in out.out
    assert rec["seconds"] > 0 and f"(the step took {rec['seconds']:g} s)" in out.out and "refused_once" not in rec
    assert "it does not cover control-plane resizes and repairs" in out.out


REFUSED = {
    "the update exits 0 and the cluster does not hold it": (
        {"FAKE_EXCLUSION_DROPPED": "1"}, "the cluster holds no maintenance exclusion named bench-run-ttl"),
    "the cluster holds it with another scope": (
        {"FAKE_EXCLUSION_SCOPE": "NO_MINOR_UPGRADES"}, "its scope is 'NO_MINOR_UPGRADES', not NO_UPGRADES"),
    "the cluster's exclusion ends before the TTL": ({"FAKE_EXCLUSION_END_SHIFT_S": "-3600"}, "its end is '"),
    "the cluster's exclusion ends one second early": ({"FAKE_EXCLUSION_END_SHIFT_S": "-1"}, "its end is '"),
    "the update fails": ({"FAKE_EXCLUSION_UPDATE_RC": "1"}, "the update that adds it failed (rc=1)"),
}


@pytest.mark.parametrize("case", list(REFUSED))
def test_gke_up_refuses_a_cluster_that_does_not_hold_the_exclusion_as_asked(fakes, monkeypatch, capsys, case):
    env, why = REFUSED[case]
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    rc, rec = _up(fakes)
    err = capsys.readouterr().err
    assert rc == gke.EXIT_EXCLUSION_MISSING == 7
    assert rec["verified"] is False and why in rec["why"]
    assert f"maintenance exclusion NOT in place: {rec['why']}" in err and "the run does not go on" in err
    assert f"gke-down --name {NAME} --yes" in err


def test_a_read_back_that_does_not_answer_refuses(fakes):
    spec = gke.default_spec(NAME, now=T)

    def run(argv, **kw):
        if argv[3] == "describe":
            kw["stderr_out"].append("ERROR: (gcloud.container.clusters.describe) ResponseError: code=503, "
                                    "message=unavailable. This command is authenticated as x@example.invalid\n")
            return 1, ""
        return gke.execute(argv, **kw)
    rec = gke.apply_exclusion(spec, run=run, log=lambda _m: None)
    assert rec["verified"] is False and rec["read_back"] is None
    assert rec["why"] == ("the cluster could not be read back (describe failed, rc=1, ResponseError: code=503): "
                          "whether it holds the exclusion is unknown")


def test_an_exclusion_with_no_scope_printed_is_the_apis_default_no_upgrades(fakes, monkeypatch):
    monkeypatch.setenv("FAKE_EXCLUSION_NO_SCOPE", "1")
    rc, rec = _up(fakes)
    assert rc == 0 and rec["verified"] is True and "maintenanceExclusionOptions" not in rec["read_back"]


def test_the_exclusion_waits_once_for_an_operation_in_flight(fakes, monkeypatch):
    monkeypatch.setenv("FAKE_EXCLUSION_BUSY", "operation-1794648225127-825c8ada-5282-4330-a1a3-8e4c717c325f")
    rc, rec = _up(fakes)
    assert rc == 0 and rec["verified"] is True
    # GKE's own words for the refusal, and how long the wait took: what a rehearsal needs to show
    assert rec["refused_once"]["operation"] == "operation-1794648225127-825c8ada-5282-4330-a1a3-8e4c717c325f"
    assert "Cluster is running incompatible operation operation-1794648225127" in rec["refused_once"]["said"]
    assert rec["refused_once"]["waited_s"] >= 0 and rec["seconds"] >= rec["refused_once"]["waited_s"]
    seq = [c[1:4] for c in _calls(fakes) if c[:3] in (["gcloud", "container", "clusters"],
                                                       ["gcloud", "container", "operations"]) and c[3] in (
        "update", "wait")]
    assert seq == [["container", "clusters", "update"], ["container", "operations", "wait"],
                   ["container", "clusters", "update"]]


def test_the_check_reads_the_clusters_answer_not_the_request():
    spec = gke.default_spec(NAME, now=T, ttl_hours=13)
    good = {"startTime": "2026-11-14T09:53:20Z", "endTime": "2026-11-14T22:53:20Z",
            "maintenanceExclusionOptions": {"scope": "NO_UPGRADES"}}

    def describe(excl):
        return {"name": NAME, "maintenancePolicy": {"resourceVersion": "1d2b3f4a", "window": {
            "maintenanceExclusions": excl}}}
    assert gke.exclusion_check(describe({gke.EXCLUSION_NAME: good}), spec)[0] is True
    # the same instants written with an offset are the same window
    off = dict(good, startTime="2026-11-14T10:53:20+01:00", endTime="2026-11-14T22:53:20+00:00")
    assert gke.exclusion_check(describe({gke.EXCLUSION_NAME: off}), spec)[0] is True
    for bad, why in ((describe({}), "holds no maintenance exclusion named"),
                     ({"name": NAME, "maintenancePolicy": {"resourceVersion": "e3b0c442"}}, "its exclusions: none"),
                     (describe({"someone-elses": good}), "its exclusions: someone-elses"),
                     (describe({gke.EXCLUSION_NAME: dict(good, startTime="2026-11-14T09:53:21Z")}), "its start is"),
                     (describe({gke.EXCLUSION_NAME: dict(good, endTime=None)}), "its end is None"),
                     (describe({gke.EXCLUSION_NAME: dict(good, maintenanceExclusionOptions={
                         "scope": "NO_MINOR_OR_NODE_UPGRADES"})}), "its scope is"),
                     (describe({gke.EXCLUSION_NAME: dict(good, maintenanceExclusionOptions={
                         "scope": "NO_UPGRADES", "endTimeBehavior": "UNTIL_END_OF_SUPPORT"})}), "not the fixed time"),
                     (None, "could not be read back")):
        ok, said, _ = gke.exclusion_check(bad, spec)
        assert ok is False and why in said, said


def test_the_dry_run_prints_the_update_and_runs_nothing(fakes, capsys):
    assert gke.main(["gke-up", "--name", NAME, "--maintenance-out", str(fakes / "m.json")]) == 0
    out = capsys.readouterr().out
    assert f"DRY-RUN: gcloud container clusters update {NAME} " in out
    assert "--add-maintenance-exclusion-scope no_upgrades" in out
    assert "it does not cover control-plane resizes and repairs" in out
    assert _calls(fakes) == [] and not (fakes / "m.json").exists()


# ─── gke-up's bound is the sum of its parts ──────────────────────────────────

def test_gke_ups_outer_bound_is_the_sum_of_the_budgets_its_parts_enforce():
    parts = {name: float(getattr(gke, name)) for _, name in gke.UP_BUDGET_PARTS}
    assert parts == {"UP_CREATE_BUDGET_S": 1200.0, "TTL_PROOF_BUDGET_S": 600.0, "_POLL_S": 15.0,
                     "EXCLUSION_BUDGET_S": 900.0}
    assert gke.up_budget_s() == sum(parts.values()) == 2715
    # each constant is the one its step runs under
    assert inspect.signature(gke.prove_ttl_delete).parameters["budget_s"].default == gke.TTL_PROOF_BUDGET_S
    assert inspect.signature(gke.apply_exclusion).parameters["budget_s"].default == gke.EXCLUSION_BUDGET_S
    # and run606 bounds gke-up by exactly that sum: its plan says so, and so its TTL counts it
    src = (BENCH / "run606.sh").read_text()
    assert "UP_BUDGET_S=\"${UP_BUDGET_S:-$(python3 -c 'from bench import gke; print(gke.up_budget_s())')}\"" in src
    assert 'run bounded "$UP_BUDGET_S" python3 -m bench.gke gke-up' in src


def test_run606s_plan_counts_gke_ups_whole_budget(harness):  # noqa: F811
    env, _, _, tmp = harness
    p = _run606(dict(env, BENCH_606_DRY_RUN="1"))
    m = re.search(r"run budget: up (\d+) \+ .* = (\d+) s; fits TTL_HOURS=(\d+) \((\d+) s", p.stderr)
    assert m and int(m.group(1)) == gke.up_budget_s(), p.stderr[-1500:]
    assert int(m.group(2)) + 1800 <= int(m.group(4))                           # the plan, with it, fits the TTL


def test_the_exclusion_step_never_outlasts_its_budget():
    """Every call gets what is left of the step's budget, and none is made once it is spent."""
    spec = gke.default_spec(NAME, now=T)
    now, limits = [0.0], []

    def run(argv, **kw):
        limits.append((argv[3], kw["timeout"]))
        now[0] += 400                                    # each call takes 400 s
        if argv[3] == "update" and len(limits) == 1:
            kw["stderr_out"].append("ERROR: ResponseError: code=400, message=Cluster is running incompatible "
                                    "operation operation-1-abc.\n")
            return 1, ""
        return 0, ""
    rec = gke.apply_exclusion(spec, run=run, log=lambda _m: None, clock=lambda: now[0])
    # update (900 left), wait (500 left), update (100 left); the read-back is never called: 0 left
    assert limits == [("update", 900.0), ("wait", 500.0), ("update", 100.0)]
    assert rec["verified"] is False and "the step's 900 s were spent" in rec["why"] and rec["seconds"] == 1200.0


def test_a_create_that_outlasts_its_budget_is_a_failed_create(fakes, monkeypatch, capsys):
    monkeypatch.setattr(gke, "UP_CREATE_BUDGET_S", 0.0)
    rc, rec = _up(fakes)
    err = capsys.readouterr().err
    assert rc == gke.GCLOUD_TIMED_OUT and rec is None
    assert "the create's 0 s (UP_CREATE_BUDGET_S, bench/gke.py) ran out at gcloud container clusters create" in err
    assert not any(c[:4] == ["gcloud", "container", "clusters", "create"] for c in _calls(fakes))


# ─── run606's own shell lines, run as run606 runs them ───────────────────────

def _fn(name):
    src = (BENCH / "run606.sh").read_text()
    i = src.index(f"{name}() {{")
    return src[i:src.index("\n}\n", i) + 3]


def _stand_in_python(tmp, module, body):
    """A python3 first on PATH that answers `-m <module>` with `body` (bash) and is python3 otherwise."""
    b = tmp / "standin"
    b.mkdir()
    (b / "python3").write_text(f'#!/usr/bin/env bash\nif [[ "$1 $2" == "-m {module}" ]]; then\n{body}\nfi\n'
                               f'exec {shutil.which("python3")} "$@"\n')
    (b / "python3").chmod(0o755)
    return f"{b}{os.pathsep}{os.environ['PATH']}"


def test_a_record_step_that_fails_leaves_no_half_written_record(tmp_path):
    """run606's cluster_operations: the record command dies after it began to write (killed at its bound,
    a full disk). What it left is removed, so evaluate reads "no record", never half of one."""
    out = tmp_path / "out"
    out.mkdir()
    half = 'for ((i = 1; i <= $#; i++)); do [[ "${!i}" == "--out" ]] && { j=$((i + 1)); ' \
           'printf \'{"kind": "cluster_operations", "readable": true, "operations": [\' > "${!j}"; }; done\n' \
           'echo "Killed" >&2; exit 137'
    script = ("set -Eeuo pipefail\nlog() { echo \"$*\" >> \"$OUT/run.log\"; }\n"
              "bounded() { shift; \"$@\"; }\nkerr_line() { cat \"$KERR\"; }\n"
              "DRY=0 RUN_NAME=snowplow-bench-x PROJECT=p ZONE=europe-west4-a COLLECT_TIMEOUT_S=5 KERR=\"$OUT/.kerr\"\n"
              + _fn("cluster_operations") + "\ncluster_operations\necho step-ok\n")
    p = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60,
                       env=dict(os.environ, OUT=str(out), PATH=_stand_in_python(tmp_path, "bench.clusterops", half)))
    assert p.returncode == 0 and "step-ok" in p.stdout, p.stderr
    assert not (out / "cluster-operations.json").exists()
    log = (out / "run.log").read_text()
    assert "could not record the cluster's operations: Killed; the verdict will say they could not be read" in log
    # and that is what evaluate then says
    v = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(_phases_file(tmp_path)),
                        "--cluster-operations", str(out / "cluster-operations.json")],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    sec = json.loads(v.stdout)["report"]["cluster_operations_report_only"]
    assert sec["state"] == "UNREADABLE" and "the run has no cluster-operations record" in sec["why"]


# ─── an input that was never written; an evaluate that wrote no verdict ──────

def test_evaluate_says_a_missing_input_and_still_gives_its_verdict(tmp_path):
    """run606 names top-boot.jsonl whether or not the boot sampler wrote it: evaluate died on it."""
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(_phases_file(tmp_path)),
                        str(tmp_path / "top-boot.jsonl")], cwd=BENCH, capture_output=True, text=True, timeout=300)
    v = json.loads(p.stdout)
    assert v["verdict"] in ("PASS", "FAIL", "INVALID", "INCONCLUSIVE") and p.returncode in (0, 1, 3, 4)
    assert v["report"]["missing_inputs"] == ["top-boot.jsonl"]
    assert "input top-boot.jsonl does not exist: its records are missing from this verdict, not zero" in \
        v["report"]["report_warnings"]
    assert "Traceback" not in p.stderr


@pytest.mark.parametrize("said, rc", [("", 1), ("Traceback (most recent call last):", 1), ('{"report": {}}', 0)])
def test_run606_fails_the_evaluate_step_when_there_is_no_verdict(tmp_path, said, rc):
    """A crashed evaluate exits 1, FAIL's own code, with an empty verdict.json: the step logged
    "verdict exit=1" and the run "run complete"."""
    out = tmp_path / "out"
    out.mkdir()
    (out / "mem-x.jsonl").write_text("{}\n")
    script = ("set -Eeuo pipefail\nlog() { echo \"$*\" >> \"$OUT/run.log\"; }\n"
              "DRY=0 RUN8_REPORT=1 LOAD_DRIVER=k6 VIEWERS=1 BROWSER_SESSIONS=5 EXPECTED_IMAGE_DIGEST=x FRONTEND_TAG=\n"
              "VERDICT_MARK=\"$OUT/.verdict-final\"\n" + _fn("s_evaluate")
              + "\nif s_evaluate; then echo step-ok; else echo step-failed; fi\n")
    body = f'if [[ "$3" == "evaluate" ]]; then echo {json.dumps(said)}; exit {rc}; fi'
    p = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60,
                       env=dict(os.environ, OUT=str(out), PATH=_stand_in_python(tmp_path, "bench.load606", body)))
    assert "step-failed" in p.stdout and "step-ok" not in p.stdout, p.stderr
    assert not (out / ".verdict-final").exists()
    log = (out / "run.log").read_text()
    assert f"evaluate FAILED (exit {rc}): {out}/verdict.json holds no verdict; this run has none" in log
    assert "verdict exit=" not in log


def test_the_orchestrators_roles_cover_the_two_new_calls():
    from bench import vm606
    scan = vm606.scan(BENCH)
    keys = {k for k, _ in scan["calls"]}
    assert {("container", "clusters", "update"), ("container", "operations", "list")} <= keys
    assert scan["uncovered"] == []


# ─── the real run: run606.sh against the fakes ──────────────────────────────

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra),
                          capture_output=True, text=True, timeout=600)


def _gcloud(state):
    f = state / "calls.jsonl"
    return [json.loads(x)["argv"] for x in (f.read_text().splitlines() if f.exists() else [])
            if json.loads(x)["tool"] == "gcloud"]


def test_run606_records_the_operations_marks_the_phases_and_holds_the_exclusion(harness):  # noqa: F811
    """One whole run: GKE starts resizing the control plane as the measured reboot begins (run 27c N2), after the
    settle step saw it settled, and has not finished at collect: every measured phase is marked, and the arm is
    INVALID by environment (the owner's ruling, 2026-11-16)."""
    env, state, _, tmp = harness
    p = _run606(env, FAKE_GKE_RESIZE_AT_REBOOT="1")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    # the exclusion: asked for the TTL window of THIS cluster, read back, in the evidence
    gcl = _gcloud(state)
    create = next(a for a in gcl if a[:3] == ["container", "clusters", "create"])
    update = next(a for a in gcl if a[:3] == ["container", "clusters", "update"])
    ttl = int(dict(kv.split("=") for kv in _flag(create, "--labels").split(","))["ttl-expires"])
    assert gke.rfc3339_epoch(_flag(update, "--add-maintenance-exclusion-end")) == ttl
    excl = json.loads((out / "maintenance-exclusion.json").read_text())
    assert excl["verified"] is True and excl["requested"]["ttl_expires"] == ttl
    # the record: taken at collect, before the cluster is deleted
    i_list = next(i for i, a in enumerate(gcl) if a[:3] == ["container", "operations", "list"])
    assert i_list < next(i for i, a in enumerate(gcl) if a[:3] == ["container", "clusters", "delete"])
    rec = json.loads((out / "cluster-operations.json").read_text())
    assert rec["readable"] is True and rec["cluster"] == RUN_NAME
    # proven to be this cluster's listing: its own CREATE_CLUSTER, started when GKE said the cluster was created
    made = next(o for o in rec["operations"] if o["type"] == "CREATE_CLUSTER")
    assert rec["control"]["operation"] == made["id"]
    assert 0 <= made["start_t"] - gke.rfc3339_epoch(excl["cluster_created"]) <= 5
    kinds = [o["type"] for o in rec["operations"]]
    assert kinds.count("CREATE_CLUSTER") == 1 and kinds.count("CREATE_NODE_POOL") == 2
    # the exclusion's own update is there; the resize GKE started after the create, not ended at collect
    assert "UPDATE_CLUSTER" in kinds and kinds[0] == "CREATE_CLUSTER" and kinds.count("RESIZE_CLUSTER") == 1
    assert next(o for o in rec["operations"] if o["type"] == "RESIZE_CLUSTER")["end"] is None
    # the mark: every measured phase started after the resize, which had not ended at collect
    v = json.loads((out / "verdict.json").read_text())
    sec = v["report"]["cluster_operations_report_only"]
    assert sec["state"] == "READ" and sec["phases"], sec
    assert sec["phases"][0]["phase"] == "measure"
    for row in sec["phases"]:
        if row["phase"] == "boot":           # the restart came a moment before GKE listed the resize
            continue
        assert row["overlap"] == clusterops.OVERLAPPED and [o["type"] for o in row["by"]] == ["RESIZE_CLUSTER"], row
    assert "overlapped by: RESIZE_CLUSTER" in log and "not ended at the read" in log
    assert f"WARNING: cluster operations: phase {sec['phases'][0]['phase']} overlapped by: RESIZE_CLUSTER" in log
    # the settle saw the control plane settled; the operation that came after it still voids the arm
    assert json.loads((out / "settle.json").read_text())["state"] == clusterops.SETTLED
    assert v["verdict"] == "INVALID" and any(r.startswith("environment.cluster_operations: INVALID by environment: "
                                                          "GKE's RESIZE_CLUSTER") for r in v["invalid_reasons"])


def test_run606_says_unreadable_when_the_listing_fails(harness):  # noqa: F811
    """A listing that fails is never 'settled' (#1422, 2026-11-16): the arm is INVALID by environment and not
    measured; never 'no operation overlapped'."""
    env, state, _, tmp = harness
    p = _run606(env, FAKE_OPERATIONS_RC="1")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode != 0 and "=== step reboot ===" not in log, log[-3000:]
    rec = json.loads((out / "settle.json").read_text())
    assert rec["state"] == clusterops.UNSETTLED and rec["operations"] is None
    assert all(x["state"] == clusterops.NOT_READ for x in rec["polls"]) and "could not be read" in rec["why"]
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID" and v["invalid_reasons"][0].startswith("environment.control_plane_settled: ")
    assert "no operation overlapped" not in log


def test_run606_tears_down_a_cluster_that_does_not_hold_the_exclusion(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_EXCLUSION_DROPPED="1")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and f"up FAILED: gke-up exited {gke.EXIT_EXCLUSION_MISSING}" in log, log[-3000:]
    assert "maintenance exclusion NOT in place" in p.stderr
    gcl = _gcloud(state)
    assert any(a[:4] == ["container", "clusters", "delete", RUN_NAME] for a in gcl)
    assert "=== step install ===" not in log                                  # nothing ran on that cluster
    excl = json.loads((tmp / "out" / "maintenance-exclusion.json").read_text())
    assert excl["verified"] is False


def test_the_dry_run_of_run606_shows_the_record_and_evaluates_with_it(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(dict(env, BENCH_606_DRY_RUN="1"))
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert "DRY: python3 -m bench.clusterops record --name " + RUN_NAME in log
    assert f"--cluster-operations {tmp / 'out' / 'cluster-operations.json'}" in log
    assert "--maintenance-out " in log
    assert not any(a[:2] == ["container", "operations"] for a in _gcloud(state))
