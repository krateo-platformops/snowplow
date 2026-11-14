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

import json
import os
import subprocess
import sys

import pytest

from bench import clusterops, gke
from tests import fake_gke_ops
from tests.fake_gke_ops import operation
from tests.test_gke_pvc_disks import BENCH, NAME, _calls, fakes  # noqa: F401  (fixture)
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
    f.write_text("".join(json.dumps({"kind": "churn", "label": n.split(":", 1)[1], "t0": a, "t1": b,
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
    assert {p["overlap"] for p in sec["phases"]} == {clusterops.UNKNOWN} and len(sec["phases"]) == 2
    text = "\n".join(sec["lines"])
    assert text.count("operations could not be read: ") == 3 and "no operation overlapped" not in text
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
    assert sec["asymmetric"] == [P1[0]] and sec["unknown"] == []
    assert [(p["phase"], p["a"], p["b"], p["asymmetric"]) for p in sec["phases"]] == [
        (P1[0], O, N, True), (P2[0], N, N, False)]
    one, two = sec["lines"]
    assert one.startswith(f"INFO cluster operations, phase {P1[0]}: ASYMMETRIC, one arm only measured under a "
                          f"cluster operation: N1 (baseline): overlapped by: RESIZE_CLUSTER 2026-11-14T09:55:00Z.."
                          f"2026-11-14T09:56:40Z (DONE, {resize['name']})")
    assert one.endswith("N2 (candidate): no operation overlapped")
    assert two == f"INFO cluster operations, phase {P2[0]}: no operation overlapped on either arm"
    # the other way round is the same finding, on the other arm
    assert _pair_cli(db, da)["asymmetric"] == [P1[0]]


def test_the_pair_is_not_asymmetric_when_both_or_neither_overlapped_and_unknown_when_one_is_unreadable(fakes):
    resize = [operation("RESIZE_CLUSTER", NAME, start=T + 100, end=T + 200)]
    da, _ = _arm(fakes, "a", resize)
    db, _ = _arm(fakes, "b", resize)
    dc, _ = _arm(fakes, "c", [], FAKE_OPERATIONS_RC="1")
    both = _pair_cli(da, db)
    assert both["asymmetric"] == [] and both["unknown"] == [] and "both arms overlapped" in both["lines"][0]
    unk = _pair_cli(da, dc)
    assert unk["asymmetric"] == [] and unk["unknown"] == [P1[0], P2[0]]
    assert "whether the arms differ is UNKNOWN" in unk["lines"][0]
    assert "N2 (candidate): operations could not be read: " in unk["lines"][0]
    assert "ASYMMETRIC" not in "\n".join(unk["lines"]) and "on either arm" not in "\n".join(unk["lines"])
    # a verdict from before #1422 has no section: unknown, never symmetric
    (dc / "verdict.json").write_text(json.dumps({"verdict": "PASS", "report": {}}))
    old = _pair_cli(da, dc)
    assert old["unknown"] == [P1[0], P2[0]] and "has no cluster_operations_report_only" in old["lines"][0]


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
    assert sec["asymmetric"] == [P2[0]]
    assert ("ASYMMETRIC, one arm only measured under a cluster operation: N1 (replica arm): overlapped by: "
            "RESIZE_CLUSTER 2026-11-14T10:04:10Z..not ended at the read (RUNNING,") in sec["lines"][1]
    assert sec["lines"][1].endswith("N1B (replica arm): no operation overlapped")


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
    """One whole run: GKE resizes the control plane before the measure and has not finished at collect."""
    env, state, _, tmp = harness
    p = _run606(env, FAKE_GKE_RESIZES="1")
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
    sec = json.loads((out / "verdict.json").read_text())["report"]["cluster_operations_report_only"]
    assert sec["state"] == "READ" and sec["phases"], sec
    for row in sec["phases"]:
        assert row["overlap"] == clusterops.OVERLAPPED and [o["type"] for o in row["by"]] == ["RESIZE_CLUSTER"], row
    assert "overlapped by: RESIZE_CLUSTER" in log and "not ended at the read" in log
    assert f"WARNING: cluster operations: phase {sec['phases'][0]['phase']} overlapped by: RESIZE_CLUSTER" in log


def test_run606_says_unreadable_when_the_listing_fails(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_OPERATIONS_RC="1")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]          # evidence, not a reason to end the run
    rec = json.loads((out / "cluster-operations.json").read_text())
    assert rec["readable"] is False and rec["operations"] is None
    sec = json.loads((out / "verdict.json").read_text())["report"]["cluster_operations_report_only"]
    assert sec["state"] == "UNREADABLE" and sec["phases"]
    assert all(r["overlap"] == clusterops.UNKNOWN for r in sec["phases"])
    assert "WARNING: cluster operations: operations could not be read: " in log
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
