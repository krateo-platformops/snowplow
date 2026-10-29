"""#899 review follow-ups (regional control plane).

F1  The run's cluster-shape record is READ BACK from GKE after create
    (`gcloud container clusters describe`: location, locations,
    currentNodeCount, node pools), not copied from the spec. A describe that
    does not answer records the shape as UNKNOWN, never the spec in its place;
    a dry run records the spec and says so.
F2  ZONE must be a zone. A region passed as ZONE (europe-west4) is refused at
    once by run606.sh — before any cloud call — and by bench.gke.
F3  `gke-down --legacy-zonal` (a manual cleanup; run606 never passes it)
    finds a zonal bench cluster made before run 17: the region's describe
    answers NotFound, so it looks in the node zone and deletes there. Only
    NotFound sends it to the zone; any other describe failure raises. Without
    the flag the zone is never looked at.
B1  (#900 review) gcloud's error text names the authenticated account: only
    its code (`ResponseError: code=<n>`) reaches a record or a log.

A stand-in gcloud on PATH plays GKE. No cluster is created or deleted.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

from bench import gke
from tests.test_run606_real_sequence import BENCH_DIR, RUN_NAME, _calls, harness  # noqa: F401

NAME = "snowplow-bench-r16z"
SPEC = gke.default_spec(NAME)
REGION, ZONE = "europe-west4", "europe-west4-a"
OP1 = "operation-1792823484322-b2555fe0"
# gcloud's real error texts (gcloud 561, captured by the #900 reviewer), the
# account replaced by a placeholder
REAL_404 = ("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, message=Not found: "
            "projects/operations-dev-krateo-io/locations/europe-west4/clusters/snowplow-bench-r16z. This command "
            "is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active account specified by "
            "the [core/account] property.\nNo cluster named 'snowplow-bench-r16z' in operations-dev-krateo-io.\n")
REAL_503 = ("ERROR: (gcloud.container.clusters.describe) ResponseError: code=503, message=The service is "
            "currently unavailable. This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid "
            "which is the active account specified by the [core/account] property.\n")

FAKE_GCLOUD = textwrap.dedent("""\
    #!/usr/bin/env python3
    import json, os, sys, time
    a = sys.argv[1:]
    st = os.environ["FAKE_STATE"]
    def opt(n):
        return a[a.index(n) + 1] if n in a else None
    loc = opt("--region") or opt("--zone")
    flag = "--region" if "--region" in a else "--zone"
    def log(x):
        open(os.path.join(st, "calls"), "a").write(x + "\\n")
    BENCH = {"purpose": "snowplow-bench", "issue": "606", "created-by": "bench-gke", "owner": "t",
             "ttl-expires": str(int(time.time()) + 3600)}
    if a[:3] == ["container", "clusters", "describe"]:
        log(f"describe {flag} {loc}")
        mode = os.environ.get("FAKE_REGION_DESCRIBE" if flag == "--region" else "FAKE_ZONE_DESCRIBE", "notfound")
        if os.path.exists(os.path.join(st, f"deleted_{loc}")):
            mode = "notfound"                                   # deleted there: gone
        # gcloud's REAL answers (gcloud 561, captured by the #900 reviewer), the
        # account replaced by a placeholder: both name the authenticated account
        kind = "locations" if flag == "--region" else "zones"
        if mode == "notfound":
            sys.stderr.write("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, message=Not "
                             f"found: projects/operations-dev-krateo-io/{kind}/{loc}/clusters/{a[3]}. This command "
                             "is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active account "
                             "specified by the [core/account] property.\\n"
                             f"No cluster named '{a[3]}' in operations-dev-krateo-io.\\n")
            sys.exit(1)
        if mode == "error":
            sys.stderr.write("ERROR: (gcloud.container.clusters.describe) ResponseError: code=503, message=The "
                             "service is currently unavailable. This command is authenticated as "
                             "PLACEHOLDER-OPERATOR@example.invalid which is the active account specified by the "
                             "[core/account] property.\\n")
            sys.exit(1)
        if mode == "nojson":
            print("not json"); sys.exit(0)
        labels = dict(BENCH) if mode != "foreign" else {"purpose": "someone-else"}
        name = a[3] if mode != "othername" else "snowplow-bench-other"
        print(json.dumps({"name": name, "id": "d8cac43bc0ac45d28fe8bdd1ccbe101a68fc42887613486d883c5066a9fc7796", "location": loc, "locations": [os.environ.get("FAKE_NODE_ZONE", "europe-west4-a")],
                          "currentNodeCount": int(os.environ.get("FAKE_NODES", "5")),
                          "nodePools": [{"name": "default-pool", "config": {"machineType": "n2-standard-8"},
                                         "initialNodeCount": 3, "locations": ["europe-west4-a"]}],
                          "resourceLabels": labels}))
        sys.exit(0)
    if a[:3] == ["container", "clusters", "delete"]:
        ops = os.environ.get("FAKE_BLOCKING_OPS", "").split()
        n = sum(1 for l in open(os.path.join(st, "calls")).read().splitlines() if l.startswith("delete"))
        log(f"delete {flag} {loc}")
        if n < len(ops):
            sys.stderr.write(f" - ResponseError: code=400, message=Cluster is running incompatible operation {ops[n]}.\\n")
            sys.exit(1)
        open(os.path.join(st, f"deleted_{loc}"), "w").write("1")
        sys.exit(0)
    if a[:3] == ["compute", "disks", "list"]:
        log("disks list")
        print("[]"); sys.exit(0)
    if a[:3] == ["container", "operations", "wait"]:
        log(f"wait {a[3]} {flag} {loc}")
        sys.exit(0)
    log("other " + " ".join(a))
    sys.exit(2)
    """)


@pytest.fixture
def fakegke(tmp_path, monkeypatch):
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "gcloud").write_text(FAKE_GCLOUD)
    (bin_ / "gcloud").chmod(0o755)
    (bin_ / "kubectl").write_text("#!/bin/sh\necho 'Unable to connect to the server' >&2\nexit 1\n")
    (bin_ / "kubectl").chmod(0o755)
    st = tmp_path / "state"
    st.mkdir()
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_STATE", str(st))
    return st


def calls(st: Path) -> list[str]:
    p = st / "calls"
    return p.read_text().splitlines() if p.exists() else []


def _flag(argv, f):
    return argv[argv.index(f) + 1] if f in argv else None


# ─── F1: the shape is read back from GKE ────────────────────────────────────

def _shape(capsys, *args):
    assert gke.main(["gke-shape", "--name", NAME, *args]) == 0
    out = capsys.readouterr()
    return json.loads(out.out), out.err


def test_the_record_is_what_gke_reports_not_the_spec(fakegke, monkeypatch, capsys):
    """GKE reports a ZONAL control plane and 4 nodes in another zone: the
    record says so, although the spec asked for regional, 5 nodes, zone -a.
    A record copied from the spec would say regional."""
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "found")
    monkeypatch.setenv("FAKE_NODE_ZONE", "europe-west4-b")
    monkeypatch.setenv("FAKE_NODES", "4")
    rec, err = _shape(capsys, "--yes")
    assert rec["source"] == "gcloud container clusters describe, after create"
    assert (rec["control_plane"], rec["location"], rec["region"]) == ("regional", REGION, REGION)
    assert rec["node_locations"] == ["europe-west4-b"] and rec["current_node_count"] == 4
    assert rec["node_pools"] == [{"name": "default-pool", "machine_type": "n2-standard-8",
                                  "locations": [ZONE], "nodes_per_location": 3}]
    assert calls(fakegke) == [f"describe --region {REGION}"]          # one read-only call
    assert "control plane regional (europe-west4); nodes in europe-west4-b; 4 nodes" in err


def test_a_zonal_location_reads_as_a_zonal_control_plane():
    rec = gke.observed_shape({"name": NAME, "location": ZONE, "locations": [ZONE], "currentNodeCount": 5})
    assert (rec["control_plane"], rec["location"], rec["region"]) == ("zonal", ZONE, REGION)
    rec = gke.observed_shape({"name": NAME, "location": REGION, "locations": [ZONE]})
    assert rec["control_plane"] == "regional" and rec["current_node_count"] is None
    assert gke.observed_shape({"name": NAME})["control_plane"] is None      # no location: unknown


@pytest.mark.parametrize("mode", ["error", "notfound", "nojson"])
def test_an_unanswered_describe_records_unknown_never_the_spec(fakegke, monkeypatch, capsys, mode):
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", mode)
    rec, err = _shape(capsys, "--yes")
    assert rec["control_plane"] is None and rec["source"].startswith("unobserved: ")
    assert "node_locations" not in rec and "current_node_count" not in rec
    assert rec["requested"]["source"] == "spec: what gke-up asked for"
    assert rec["requested"]["control_plane"] == "regional"
    assert "cluster shape UNKNOWN" in err
    if mode in ("error", "notfound"):
        assert ("code=503" if mode == "error" else "code=404") in rec["source"]
    # B1 (#900 review): gcloud's real text names the account; none of it is kept
    for text in (json.dumps(rec), err):
        assert "authenticated as" not in text and "@" not in text and "PLACEHOLDER" not in text, text


@pytest.mark.parametrize("stderr, want", [
    (REAL_404, "ResponseError: code=404"),
    (REAL_503, "ResponseError: code=503"),
    ('Error from server (Forbidden): pods is forbidden: User "someone@example.invalid" cannot list', "Error from server (Forbidden)"),
    ("ERROR: (gcloud.auth) You do not currently have an active account selected. someone@example.invalid", None),
    ("", "no error text"),
])
def test_error_summary_keeps_the_code_never_the_text(stderr, want):
    got = gke.error_summary(stderr)
    assert "@" not in got and "authenticated as" not in got and "PLACEHOLDER" not in got
    assert got == want if want else got.startswith("no error code")


@pytest.mark.parametrize("region_mode, legacy", [("notfound", True), ("notfound", False), ("error", True)])
def test_no_log_line_carries_the_account(fakegke, monkeypatch, region_mode, legacy):
    """execute()'s failure line, F3's two describes: the code, never gcloud's text"""
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", region_mode)
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", "notfound")
    logs: list[str] = []
    with pytest.raises(RuntimeError) as e:
        gke.teardown(SPEC, dry_run=False, log=logs.append, budget_s=60, legacy_zonal=legacy)
    for line in logs + [str(e.value)]:
        assert "authenticated as" not in line and "@" not in line and "PLACEHOLDER" not in line, line
    assert any("rc=1 (ResponseError: code=" in m for m in logs)


def test_a_dry_run_records_the_spec_and_says_so(fakegke, capsys):
    rec, _ = _shape(capsys)
    assert rec["source"] == gke.DRY_SHAPE_SOURCE and "dry run" in rec["source"]
    assert rec["control_plane"] == "regional" and rec["node_locations"] == [ZONE]
    assert calls(fakegke) == []                                     # no cloud call


def test_run606_records_the_shape_gke_reports_after_create(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=600)
    assert p.returncode == 0, p.stderr[-2000:]
    gcl = [c["argv"] for c in _calls(state) if c["tool"] == "gcloud"]
    i_create = next(i for i, a in enumerate(gcl) if a[:3] == ["container", "clusters", "create"])
    i_shape = next(i for i, a in enumerate(gcl) if a[:3] == ["container", "clusters", "describe"])
    assert i_shape > i_create                                       # read back AFTER create
    rec = json.loads((tmp / "out" / "cluster-shape.jsonl").read_text())
    assert rec["source"] == "gcloud container clusters describe, after create"
    assert rec["current_node_count"] == 5 and rec["node_locations"] == [ZONE]
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    assert v["report"]["cluster_shape"]["current_node_count"] == 5
    log = (tmp / "out" / "run.log").read_text()
    assert ("cluster shape (gcloud container clusters describe, after create): control plane regional "
            "(europe-west4); nodes in europe-west4-a; 5 nodes") in log


# ─── F2: ZONE must be a zone ────────────────────────────────────────────────

def test_run606_refuses_a_region_as_zone_before_any_call(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, ZONE=REGION),
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 2
    assert "ZONE must be a zone (e.g. europe-west4-a), got 'europe-west4'" in p.stderr
    assert _calls(state) == []                                      # nothing ran, nothing created


@pytest.mark.parametrize("bad", ["europe-west4", "europe", "europe-west4-", "europe-west4-ab", "EUROPE-WEST4-A", ""])
def test_bench_gke_refuses_a_non_zone(bad, capsys):
    with pytest.raises(ValueError, match="is not a zone"):
        gke.default_spec(NAME, zone=bad)
    with pytest.raises(SystemExit) as e:
        gke.main(["gke-up", "--name", NAME, "--zone", bad])
    assert e.value.code == 2 and "is not a zone" in capsys.readouterr().err


@pytest.mark.parametrize("z", ["europe-west4-a", "us-central1-f", "asia-northeast1-b", "europe-west4", "europe",
                               "europe-west4-ab", "us-central-a", "europe-west4-a "])
def test_run606_and_bench_gke_agree_on_what_a_zone_is(z):
    src = (BENCH_DIR / "run606.sh").read_text()
    m = re.search(r'\[\[ "\$ZONE" =~ (\S+) \]\]', src)
    assert m, "run606.sh no longer checks ZONE"
    sh = subprocess.run(["bash", "-c", f'[[ "$1" =~ {m.group(1)} ]]', "_", z]).returncode == 0
    assert sh == gke._is_zone(z), z


# ─── F3: gke-down finds a zonal cluster made before run 17 ──────────────────

def test_without_legacy_zonal_the_zone_is_never_looked_at(fakegke, monkeypatch):
    """The default (run606's teardown): a region NotFound is the end — a
    regional create that failed before the cluster existed must never lead
    to a same-named zonal bench cluster (#900 review)."""
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "notfound")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", "found")
    with pytest.raises(RuntimeError, match="only by hand: gke-down --legacy-zonal"):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60)
    assert calls(fakegke) == [f"describe --region {REGION}"]
    # #985: gke-down says "nothing to delete" (its own exit); run606 tells a create that
    # never made the cluster (nothing to delete) from one gke-up made (TEARDOWN FAILED)
    assert gke.main(["gke-down", "--name", NAME, "--zone", ZONE, "--budget-s", "60", "--yes"]) == \
        gke.EXIT_NOTHING_TO_DELETE
    assert not any("--zone" in c or c.startswith("delete") for c in calls(fakegke))


def test_run606_teardown_never_passes_legacy_zonal():
    src = (BENCH_DIR / "run606.sh").read_text()
    lines = [ln for ln in src.splitlines() if "bench.gke gke-down" in ln]
    assert lines and not any("legacy" in ln for ln in lines)
    assert "legacy" not in (BENCH_DIR / "run606_pair.sh").read_text()


def test_run606_teardown_never_uses_the_zone(harness):  # noqa: F811
    """run606 against a region that answers NotFound (gcloud's real text) and
    a same-named zonal bench cluster: the teardown fails loudly, nothing in
    the zone is described or deleted, and the account is in no log."""
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_REGIONAL_NOTFOUND="1"),
                       capture_output=True, text=True, timeout=600)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "TEARDOWN FAILED" in log
    gcl = [c["argv"] for c in _calls(state) if c["tool"] == "gcloud"]
    assert not any("--zone" in a for a in gcl if a[:2] == ["container", "clusters"])
    assert not any(a[:3] == ["container", "clusters", "delete"] for a in gcl)
    for text in (log, p.stderr, p.stdout):
        assert "authenticated as" not in text and "PLACEHOLDER-OPERATOR" not in text


def test_a_zonal_cluster_is_found_in_its_zone_and_deleted_there(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "notfound")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", "found")
    monkeypatch.setenv("FAKE_BLOCKING_OPS", OP1)
    logs: list[str] = []
    gke.teardown(SPEC, dry_run=False, log=logs.append, budget_s=60, legacy_zonal=True)
    # deleted, waited on AND polled until gone in its zone (#896 reviewer: a
    # regional poll 404s at once — a false "no disk left" while nodes still
    # hold the PVC disks); then the disk backstop
    assert calls(fakegke) == [f"describe --region {REGION}", f"describe --zone {ZONE}",
                              f"delete --zone {ZONE}", f"wait {OP1} --zone {ZONE}", f"delete --zone {ZONE}",
                              f"describe --zone {ZONE}", "disks list"]
    assert any("not found in region europe-west4; looking in zone europe-west4-a" in m for m in logs)


@pytest.mark.parametrize("legacy", [False, True])
def test_a_regional_cluster_never_looks_in_the_zone(fakegke, monkeypatch, legacy):
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "found")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", "found")
    gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60, legacy_zonal=legacy)
    assert calls(fakegke) == [f"describe --region {REGION}", f"delete --region {REGION}",
                              f"describe --region {REGION}", "disks list"]


def test_only_notfound_sends_it_to_the_zone(fakegke, monkeypatch):
    """A region describe that FAILS (503, a blip) is not evidence the cluster
    is zonal: raise, no zone describe, nothing deleted."""
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "error")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", "found")
    with pytest.raises(RuntimeError, match=f"describe {NAME} in {REGION} failed"):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60, legacy_zonal=True)
    assert calls(fakegke) == [f"describe --region {REGION}"]


@pytest.mark.parametrize("zone_mode", ["notfound", "error"])
def test_not_in_the_region_nor_answered_in_the_zone_deletes_nothing(fakegke, monkeypatch, zone_mode):
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "notfound")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", zone_mode)
    with pytest.raises(RuntimeError, match=("nor in zone europe-west4-a" if zone_mode == "notfound"
                                            else f"describe {NAME} in {ZONE} failed")):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60, legacy_zonal=True)
    assert not any(c.startswith("delete") for c in calls(fakegke))


@pytest.mark.parametrize("zone_mode", ["foreign", "othername"])
def test_the_zone_fallback_keeps_every_safety_check(fakegke, monkeypatch, zone_mode):
    """What the zone answers must still be THIS bench cluster: its labels and
    its name."""
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "notfound")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", zone_mode)
    with pytest.raises(gke.NotDeletable):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60, legacy_zonal=True)
    assert not any(c.startswith("delete") for c in calls(fakegke))


def test_gke_down_cli_deletes_a_zonal_cluster(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_REGION_DESCRIBE", "notfound")
    monkeypatch.setenv("FAKE_ZONE_DESCRIBE", "found")
    assert gke.main(["gke-down", "--name", NAME, "--zone", ZONE, "--budget-s", "60", "--legacy-zonal",
                     "--yes"]) == 0
    assert f"delete --zone {ZONE}" in calls(fakegke) and not any(c.startswith("delete --region") for c in calls(fakegke))


def test_gke_down_dry_run_makes_no_call(fakegke, capsys):
    assert gke.main(["gke-down", "--name", NAME]) == 0
    assert calls(fakegke) == []
    out = capsys.readouterr().out
    assert f"DRY-RUN: gcloud container clusters describe {NAME} --project operations-dev-krateo-io --region {REGION}" in out
    assert f"DRY-RUN: gcloud container clusters delete {NAME}" in out
