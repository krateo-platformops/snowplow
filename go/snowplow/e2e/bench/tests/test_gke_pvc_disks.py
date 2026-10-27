"""#895: gke-down leaked the CSI PVC disks; europe-west4's SSD_TOTAL_GB (500)
filled up and run 16 attempt 1 died at the platform step (ClickStack's MongoDB
PVC: QUOTA_EXCEEDED).

  1. teardown deletes the PVCs and waits for the CSI driver to delete their
     disks BEFORE it deletes the cluster — only through the bench kubeconfig
     whose server is this cluster's endpoint, and only ever reclaiming by
     Delete a disk the PD CSI driver provisioned here;
  2. after the cluster is gone, an exact-name backstop deletes what is left of
     THIS cluster only (its own PVs' disks; disks GKE labelled with this
     cluster's name AND id) — never a disk of another cluster; a backstop that
     cannot list, cannot delete, or never ran (the budget ended first) is
     loud: DISK BACKSTOP FAILED, gke-down exit 4, run606 says so;
  3. gke-up (and run606_pair.sh for two clusters) refuses a run whose PVC
     disks do not fit the region's SSD_TOTAL_GB;
  4. the disks are attributable through GKE itself: it copies the cluster's
     resource labels (created-by=bench-gke...) and its name and id onto every
     PD CSI disk, so the cluster create carrying the bench labels is enough
     (the StorageClass re-create of d39129ee is gone, review of #896).

No cloud call: a fake gcloud / kubectl on PATH plays GKE. The disk label
shapes are the project's real ones (read 2026-10-25): krateo-057's PVC disks
carry cluster-id-gke-io AND goog-gke-cluster-id-base32, the release
cluster's goog-gke-cluster-id-base32 only, both base32 of the cluster id;
a leaked bench disk carries the bench cluster's own labels.
tests/fixtures/pvcs_run14.json is run 14's diagnostics/pvcs.json;
tests/fixtures/region_europe_west4_quotas_20261025.json the region's quotas.
tests/test_gke_kubeconfig_guard.py runs the release against REAL kubectl.
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

FIX = Path(__file__).resolve().parent / "fixtures"
BENCH = Path(__file__).resolve().parents[1]
QUOTAS = json.loads((FIX / "region_europe_west4_quotas_20261025.json").read_text())
PVCS14 = json.loads((FIX / "pvcs_run14.json").read_text())

# real cluster ids (gcloud container clusters list --format=value(name,id))
# and the disk label values GKE derived from them
ID_RELEASE = "7c8fc7a854f04d21ad7e1e7b3967ba3340feb65f4aca4c29a999c95ef007f84c"
B32_RELEASE = "psh4pkcu6bgsdll6dz5tsz52gnap5ns7jlfeyknjthev54ah7bga"
ID_057 = "e59515e1af274dc082d2e2b3bf3bf64ee86eb6b43865406f8e060902d288b642"
B32_057 = "4wkrlynpe5g4baws4kz36o7wj3ug5nvuhbsua34oayeqfuuiwzba"
NAME = "snowplow-bench-r16x"
ID_OURS = "d8cac43bc0ac45d28fe8bdd1ccbe101a68fc42887613486d883c5066a9fc7796"   # a 64-hex id shape
ID_PREVIOUS = "1a37d77f990840b4b94ce7bd6c3ab9faae8ec5029701420687056e120bff8fed"  # same name, earlier cluster
ZONE = "europe-west4-a"
PROJECT = gke.DEFAULT_PROJECT
ENDPOINT = "127.0.0.1"         # the bench kubeconfig's server host (test_gke_kubeconfig_guard's too)


def _gke_labels(name, b32, location, *, both_ids=False, publish=None):
    """GKE's own PVC disk labels, in the shapes the project's disks carry."""
    lab = {"goog-gke-cluster-id-base32": b32, "goog-gke-cost-management": "",
           "goog-gke-node-pool-provisioning-model": "on-demand", "goog-gke-volume": "",
           "goog-k8s-cluster-location": location, "goog-k8s-cluster-name": name,
           "goog-k8s-node-pool-name": "default-pool"}
    if both_ids:
        lab.update({"cluster-id-gke-io": b32, "reclaim-policy-gke-io": "delete"})
    if publish:
        lab["volume-publish-status-gke-io"] = publish
    return lab


def _disk(name, labels, *, zone=ZONE, users=None):
    return {"name": name, "zone": f"https://www.googleapis.com/compute/v1/projects/p/zones/{zone}",
            "sizeGb": "10", "type": "pd-balanced", "labels": labels, **({"users": users} if users else {})}


OURS_B32 = gke.cluster_id_b32({"id": ID_OURS})
PREV_B32 = gke.cluster_id_b32({"id": ID_PREVIOUS})
DISKS = [
    # GKE copies the cluster's resource labels (create_commands) onto the disk
    _disk("pvc-ours-leftover", {**_gke_labels(NAME, OURS_B32, ZONE), **gke.BENCH_LABELS}),
    _disk("pvc-ours-attached", _gke_labels(NAME, OURS_B32, ZONE), users=["projects/p/zones/z/instances/n1"]),
    _disk("pvc-ours-recorded-unlabelled", None),              # older shape: no labels at all
    # same cluster NAME, an earlier cluster's id: not this cluster's
    _disk("pvc-same-name-other-cluster", _gke_labels(NAME, PREV_B32, ZONE)),
    # our id under another cluster's name: both must agree, neither alone decides
    _disk("pvc-our-id-other-name", _gke_labels("snowplow-bench-other", OURS_B32, ZONE)),
    # the live clusters' real shapes (unattached here, to prove the filter, not the users check)
    _disk("pvc-release-kagent", _gke_labels("krateo-installer-release", B32_RELEASE, "europe-west4",
                                            publish="attaching"), zone="europe-west4-c"),
    _disk("pvc-release-in-our-zone", _gke_labels("krateo-installer-release", B32_RELEASE, "europe-west4"),
          zone=ZONE),
    _disk("pvc-057-mongodb", _gke_labels("krateo-057", B32_057, "europe-west3-a", both_ids=True),
          zone="europe-west3-a"),
    _disk("pvc-057-mongodb-in-our-zone", _gke_labels("krateo-057", B32_057, "europe-west3-a", both_ids=True)),
    # our labels, another zone: not ours to delete from here
    _disk("pvc-ours-other-zone", _gke_labels(NAME, OURS_B32, ZONE), zone="europe-west4-b"),
]


# ─── labels, footprint, quota (pure) ───────────────────────────────────────

def test_the_live_clusters_disk_ids_are_base32_of_their_cluster_ids():
    assert gke.cluster_id_b32({"id": ID_RELEASE}) == B32_RELEASE
    assert gke.cluster_id_b32({"id": ID_057}) == B32_057
    assert gke.cluster_id_b32({}) is None and gke.cluster_id_b32({"id": "zz"}) is None


def test_the_cluster_create_carries_the_labels_gke_copies_onto_its_disks():
    """4: no StorageClass is touched; the bench labels ride on the cluster."""
    create = gke.create_commands(gke.default_spec(NAME))[0]
    labels = dict(kv.split("=", 1) for kv in create[create.index("--labels") + 1].split(","))
    assert {k: labels[k] for k in gke.BENCH_LABELS} == gke.BENCH_LABELS
    assert not hasattr(gke, "label_storageclasses") and not hasattr(gke, "labelled_storageclasses")


def test_the_footprint_is_run14s_recorded_pvcs():
    """BENCH_PVC_FOOTPRINT_GB is what run 14's PVCs provision (15Gi + 5Gi +
    10G + 2G, each rounded up to the whole GB a GCE disk has)."""
    assert [p["request"] for p in PVCS14] == ["15Gi", "5Gi", "10G", "2G"]
    assert gke.pvc_footprint_gb(PVCS14) == gke.BENCH_PVC_FOOTPRINT_GB == 32
    assert [gke.disk_gb(q) for q in ("15Gi", "10G", "2G", "1Mi", "1.5Gi", "100Gi")] == [15, 10, 2, 1, 2, 100]
    with pytest.raises(ValueError):
        gke.disk_gb("lots")


def test_quota_fit_on_the_regions_real_quotas():
    """2026-10-25: 397 of 500 GB used (the release cluster's 333 + run 16's two
    clusters): two more bench clusters fit, four do not."""
    ok, msg = gke.ssd_quota_fit(QUOTAS, 2 * gke.BENCH_PVC_FOOTPRINT_GB)
    assert ok and "397 of 500 GB used, 103 free" in msg
    ok, msg = gke.ssd_quota_fit(QUOTAS, 4 * gke.BENCH_PVC_FOOTPRINT_GB)
    assert not ok and "needs 128 GB" in msg
    ok, msg = gke.ssd_quota_fit({"name": "x", "quotas": []}, 1)
    assert not ok and "unknown" in msg


def test_the_default_teardown_budget_is_run606s():
    """3: one bound for every teardown wait, run606's DOWN_BUDGET_S default."""
    src = (BENCH / "run606.sh").read_text()
    assert float(re.search(r'DOWN_BUDGET_S="\$\{DOWN_BUDGET_S:-(\d+)\}"', src).group(1)) == gke.DEFAULT_DOWN_BUDGET_S
    import argparse
    p = argparse.ArgumentParser()
    gke.add_parsers(p.add_subparsers(dest="cmd"))
    assert p.parse_args(["gke-down", "--name", NAME]).budget_s == gke.DEFAULT_DOWN_BUDGET_S


# ─── 2. the backstop's exact filter ────────────────────────────────────────

def test_the_backstop_takes_this_clusters_leftovers_only():
    got = gke.backstop_disks(DISKS, name=NAME, cluster_id=OURS_B32, zone=ZONE,
                             recorded=["pvc-ours-recorded-unlabelled"])
    assert got == ["pvc-ours-leftover", "pvc-ours-recorded-unlabelled"]


@pytest.mark.parametrize("other", ["pvc-release-kagent", "pvc-release-in-our-zone", "pvc-057-mongodb",
                                   "pvc-057-mongodb-in-our-zone", "pvc-same-name-other-cluster",
                                   "pvc-our-id-other-name"])
def test_the_backstop_never_takes_another_clusters_disk(other):
    """Not even when that disk is in our zone, unattached, and named among
    this cluster's recorded PVs (a corrupted record)."""
    got = gke.backstop_disks(DISKS, name=NAME, cluster_id=OURS_B32, zone=ZONE, recorded=[other])
    assert other not in got
    # and as THEIR cluster's backstop it would be refused outright
    for live in gke.FORBIDDEN_CLUSTER_NAMES:
        with pytest.raises(gke.NotDeletable):
            gke.backstop_disks(DISKS, name=live, cluster_id=B32_057, zone=ZONE)


def test_the_backstop_without_this_clusters_id_takes_only_recorded_unlabelled_disks():
    got = gke.backstop_disks(DISKS, name=NAME, cluster_id=None, zone=ZONE,
                             recorded=["pvc-ours-recorded-unlabelled", "pvc-ours-leftover"])
    assert got == ["pvc-ours-recorded-unlabelled"]


# ─── 1+2. teardown, end to end against fakes ───────────────────────────────

FAKE = textwrap.dedent("""\
    #!/usr/bin/env python3
    import json, os, sys
    st = os.environ["FAKE_STATE"]
    tool = os.path.basename(sys.argv[0])
    a = [x for x in sys.argv[1:] if not x.startswith(("--kubeconfig=", "--context=", "--request-timeout="))]
    with open(os.path.join(st, "calls.jsonl"), "a") as fh:
        fh.write(json.dumps([tool] + a) + "\\n")
    def has(f):
        return os.path.exists(os.path.join(st, f))
    if tool == "kubectl":
        if a[:2] == ["config", "view"]:
            print("https://" + os.environ.get("FAKE_SERVER", "127.0.0.1") + ":443", end=""); sys.exit(0)
        if a[:2] == ["get", "pv"]:
            pvs = [] if has("pvcs_deleted") and not os.environ.get("FAKE_PV_STUCK") else json.load(
                open(os.environ["FAKE_PVS"]))
            print(json.dumps({"items": pvs})); sys.exit(0)
        if a[:2] == ["patch", "pv"] and os.environ.get("FAKE_PATCH_FAIL") == a[2]:
            sys.stderr.write('Error from server (Forbidden): persistentvolumes "x" is forbidden: User '
                             '"someone@example.invalid" cannot patch resource\\n'); sys.exit(1)
        if a[:2] == ["delete", "pvc"]:
            open(os.path.join(st, "pvcs_deleted"), "w").write("1"); sys.exit(0)
        if a[:2] == ["get", "pods"]:
            print(json.dumps({"items": [
                {"metadata": {"name": "mongodb-0", "namespace": "krateo-system"},
                 "spec": {"volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "data"}}]}},
                {"metadata": {"name": "web-0", "namespace": "krateo-system"},
                 "spec": {"volumes": [{"name": "c", "configMap": {"name": "x"}}]}}]})); sys.exit(0)
        sys.exit(0)
    if a[:3] == ["container", "clusters", "describe"]:
        if has("deleted") and not os.environ.get("FAKE_NEVER_GONE"):
            sys.stderr.write("ERROR: ResponseError: code=404, message=Not found: clusters/x.\\n"); sys.exit(1)
        print(json.dumps({"name": a[3], "id": os.environ["FAKE_ID"],
                          "endpoint": os.environ.get("FAKE_ENDPOINT", "127.0.0.1"), "resourceLabels": {
            "purpose": "snowplow-bench", "issue": "606", "created-by": "bench-gke", "owner": "t",
            "ttl-expires": "9999999999"}})); sys.exit(0)
    if a[:3] == ["container", "clusters", "delete"]:
        open(os.path.join(st, "deleted"), "w").write("1"); sys.exit(0)
    if a[:3] == ["compute", "disks", "list"]:
        if os.environ.get("FAKE_DISK_LIST_RC"):
            sys.exit(int(os.environ["FAKE_DISK_LIST_RC"]))
        print(open(os.environ["FAKE_DISKS"]).read()); sys.exit(0)   # unfiltered: the Python filter decides
    if a[:3] == ["compute", "disks", "describe"]:
        for d in json.load(open(os.environ["FAKE_DISKS"])):
            if d["name"] == a[3]:
                print(json.dumps(d)); sys.exit(0)
        sys.exit(1)
    if a[:3] == ["compute", "disks", "delete"]:
        sys.exit(int(os.environ.get("FAKE_DISK_DELETE_RC", "0")))
    if a[:3] == ["compute", "regions", "describe"]:
        q = json.load(open(os.environ["FAKE_QUOTAS"])); print(json.dumps(q)); sys.exit(0)
    sys.exit(0)
    """)


def _pv(name, handle, reclaim, *, dynamic=True, size="10Gi"):
    return {"metadata": {"name": name, **({"annotations": {gke.PROVISIONED_BY: gke.PD_CSI}} if dynamic else {})},
            "spec": {"csi": {"driver": gke.PD_CSI, "volumeHandle": handle},
                     "persistentVolumeReclaimPolicy": reclaim, "capacity": {"storage": size}}}


PVS = [
    _pv("pvc-ours-leftover", f"projects/{PROJECT}/zones/{ZONE}/disks/pvc-ours-leftover", "Delete"),
    _pv("pvc-ours-recorded-unlabelled", f"projects/{PROJECT}/zones/{ZONE}/disks/pvc-ours-recorded-unlabelled",
        "Retain", size="2G"),
    # bound statically to a disk this cluster did not provision: never Delete
    _pv("static-foreign", f"projects/{PROJECT}/zones/{ZONE}/disks/pvc-057-mongodb-in-our-zone", "Delete",
        dynamic=False),
    # provisioned by the driver, but in another project: not ours either
    _pv("other-project", f"projects/someone-else/zones/{ZONE}/disks/theirs", "Retain"),
    {"metadata": {"name": "nfs"}, "spec": {"nfs": {"server": "x"}}},
]


@pytest.fixture
def fakes(tmp_path, monkeypatch):
    b = tmp_path / "bin"
    b.mkdir()
    for t in ("gcloud", "kubectl"):
        (b / t).write_text(FAKE)
        (b / t).chmod(0o755)
    st = tmp_path / "state"
    st.mkdir()
    (tmp_path / "pvs.json").write_text(json.dumps(PVS))
    (tmp_path / "disks.json").write_text(json.dumps(DISKS))
    (tmp_path / "quotas.json").write_text(json.dumps(QUOTAS))
    monkeypatch.setenv("PATH", f"{b}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    for k, v in (("FAKE_STATE", st), ("FAKE_PVS", tmp_path / "pvs.json"), ("FAKE_DISKS", tmp_path / "disks.json"),
                 ("FAKE_QUOTAS", tmp_path / "quotas.json"), ("FAKE_ID", ID_OURS)):
        monkeypatch.setenv(k, str(v))
    return tmp_path


def _calls(tmp):
    p = tmp / "state" / "calls.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


def _first(calls, prefix):
    return next(i for i, c in enumerate(calls) if c[:len(prefix)] == prefix)


def _mutating(calls):
    return [c for c in calls if c[0] == "kubectl" and c[1] in ("delete", "patch", "create", "apply")]


def _ticks(step=10):
    it = iter(range(0, 10_000_000, step))
    return lambda: next(it)


def test_teardown_releases_the_pvcs_before_the_cluster_then_backstops_exactly(fakes):
    log = []
    assert gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60,
                        sleep=lambda s: None) == []
    c = _calls(fakes)
    i_view = _first(c, ["kubectl", "config", "view"])
    i_patch = _first(c, ["kubectl", "patch", "pv", "pvc-ours-recorded-unlabelled"])   # Retain -> Delete
    i_pvc = _first(c, ["kubectl", "delete", "pvc", "--all", "--all-namespaces"])
    i_pod = _first(c, ["kubectl", "delete", "pod", "-n", "krateo-system", "mongodb-0"])
    i_gone_pv = max(i for i, x in enumerate(c) if x[:3] == ["kubectl", "get", "pv"])
    i_del = _first(c, ["gcloud", "container", "clusters", "delete", NAME])
    i_list = _first(c, ["gcloud", "compute", "disks", "list"])
    i_disk = _first(c, ["gcloud", "compute", "disks", "delete"])
    assert i_view < i_patch < i_pvc < i_pod < i_gone_pv < i_del < i_list < i_disk
    assert not any(x[:4] == ["kubectl", "delete", "pod", "-n"] and "web-0" in x for x in c)   # no PVC: left alone
    deleted = c[i_disk][4:c[i_disk].index("--zone")]
    assert deleted == ["pvc-ours-leftover", "pvc-ours-recorded-unlabelled"]
    assert c[i_disk][c[i_disk].index("--zone") + 1] == ZONE
    assert any("every PV is gone" in m for m in log)
    assert any(m.startswith("disk backstop: deleted 2 leftover disk(s)") for m in log)


def test_only_a_disk_provisioned_here_is_reclaimed_by_delete(fakes):
    """5: the Retain->Delete patch only for PVs the PD CSI driver provisioned
    in this project and zone; a static PV on a foreign disk reclaimed by
    Delete is set to Retain BEFORE any PVC goes; another project's: untouched."""
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60, sleep=lambda s: None)
    c = _calls(fakes)
    patches = {x[3]: json.loads(x[-1])["spec"]["persistentVolumeReclaimPolicy"]
               for x in c if x[:3] == ["kubectl", "patch", "pv"]}
    assert patches == {"pvc-ours-recorded-unlabelled": "Delete", "static-foreign": "Retain"}
    assert _first(c, ["kubectl", "patch", "pv", "static-foreign"]) < _first(c, ["kubectl", "delete", "pvc"])


@pytest.mark.parametrize("pv, refused", [("static-foreign", True), ("pvc-ours-recorded-unlabelled", False)])
def test_a_failed_retain_patch_deletes_no_pvc(fakes, monkeypatch, capsys, pv, refused):
    """#896 reviewer: the exit code of the Retain patch decides. A foreign
    Delete-reclaimed PV that could not be set to Retain: no `delete pvc --all`
    (it could take that disk), said loudly; the cluster delete and the disk
    backstop still run. A failed Delete patch on OUR PV only leaves the disk to
    the backstop: the PVCs still go."""
    monkeypatch.setenv("FAKE_PATCH_FAIL", pv)
    logs: list[str] = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=logs.append, budget_s=60, sleep=lambda s: None)
    c = _calls(fakes)
    deleted_pvcs = any(x[:3] == ["kubectl", "delete", "pvc"] for x in c)
    assert deleted_pvcs is not refused
    err = capsys.readouterr().err
    assert ("PVC release REFUSED: static-foreign" in err) is refused
    assert any(x[:4] == ["gcloud", "container", "clusters", "delete"] for x in c)      # the cluster still goes
    assert any(x[:4] == ["gcloud", "compute", "disks", "list"] for x in c)            # and the backstop runs
    assert not any("@" in m for m in logs + [err])                                    # B1: no user in a log


def test_a_kubeconfig_that_is_not_this_cluster_changes_nothing(fakes, monkeypatch):
    """5: the bench kubeconfig's server must be the cluster's endpoint
    (describe.endpoint) before any mutating kubectl call."""
    monkeypatch.setenv("FAKE_SERVER", "34.90.12.7")         # some other cluster
    log = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60, sleep=lambda s: None)
    c = _calls(fakes)
    assert _mutating(c) == []
    assert any("refusing — the bench kubeconfig's server (34.90.12.7) is not" in m for m in log)
    i_disk = _first(c, ["gcloud", "compute", "disks", "delete"])
    assert c[i_disk][4:c[i_disk].index("--zone")] == ["pvc-ours-leftover"]   # the backstop, by GKE's labels


def test_a_stuck_pv_is_bounded_and_the_backstop_still_runs(fakes, monkeypatch):
    monkeypatch.setenv("FAKE_PV_STUCK", "1")
    log = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60,
                 clock=_ticks(), sleep=lambda s: None)
    c = _calls(fakes)
    assert any("still there at the budget's end" in m for m in log)
    assert _first(c, ["gcloud", "container", "clusters", "delete"]) < _first(c, ["gcloud", "compute", "disks",
                                                                                 "delete"])


def test_without_a_budget_every_wait_is_bounded_by_the_default(fakes, monkeypatch):
    """3: no --budget-s (gke-up's own teardown after a failed create): a PV
    that never goes and a cluster that never reads NotFound still end, at
    DEFAULT_DOWN_BUDGET_S — loudly, since the backstop could not run."""
    monkeypatch.setenv("FAKE_PV_STUCK", "1")
    monkeypatch.setenv("FAKE_NEVER_GONE", "1")
    t = [0.0]

    def clock():
        t[0] += 5
        return t[0]
    problems = gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, clock=clock,
                            sleep=lambda s: None)
    assert problems and "budget ran out" in problems[0]
    assert gke.DEFAULT_DOWN_BUDGET_S <= t[0] <= gke.DEFAULT_DOWN_BUDGET_S + 20


def test_an_unreachable_cluster_skips_the_release_but_not_the_backstop(fakes, monkeypatch):
    (fakes / "bin" / "kubectl").write_text("#!/bin/sh\nexit 1\n")
    log = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60, sleep=lambda s: None)
    c = _calls(fakes)
    assert any("could not list" in m for m in log)
    i_disk = _first(c, ["gcloud", "compute", "disks", "delete"])
    assert c[i_disk][4:c[i_disk].index("--zone")] == ["pvc-ours-leftover"]   # by GKE's labels alone


@pytest.mark.parametrize("how, env", [
    ("the disks cannot be deleted", {"FAKE_DISK_DELETE_RC": "1"}),
    ("the disks cannot be listed", {"FAKE_DISK_LIST_RC": "1"}),       # 1: never "no disk left"
    ("the cluster never reads gone", {"FAKE_NEVER_GONE": "1"}),       # 2: the skip is loud
])
def test_a_backstop_that_could_not_do_its_job_is_loud(fakes, monkeypatch, capsys, how, env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    log = []
    problems = gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60,
                            clock=_ticks(), sleep=lambda s: None)
    err = capsys.readouterr().err
    assert problems and f"{gke.BACKSTOP_FAILED} for {NAME}" in err, how
    assert not any(m.startswith("disk backstop: no disk") for m in log)
    # gke-down: exit 4 (the cluster is deleted; its disks may not be)
    (fakes / "state" / "deleted").unlink()
    monkeypatch.setattr(gke, "_POLL_S", 0.01)                # gke-down's real clock and sleep
    assert gke.main(["gke-down", "--name", NAME, "--budget-s", "1", "--yes"]) == gke.EXIT_BACKSTOP_FAILED


def test_the_dry_run_teardown_names_every_step_and_runs_nothing(monkeypatch, capsys):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("ran")))
    gke.teardown(gke.default_spec(NAME), dry_run=True)
    out = capsys.readouterr().out
    i_pvc, i_del, i_list = (out.index("DRY-RUN: kubectl"), out.index("DRY-RUN: gcloud container clusters delete"),
                            out.index("DRY-RUN: gcloud compute disks list"))
    assert i_pvc < i_del < i_list and "delete pvc --all" in out


# ─── 3. the quota refusal, through gke-up ──────────────────────────────────

def test_gke_up_refuses_a_run_that_does_not_fit_and_creates_nothing(fakes, capsys):
    q = json.loads((fakes / "quotas.json").read_text())
    next(x for x in q["quotas"] if x["metric"] == "SSD_TOTAL_GB")["usage"] = 480.0
    (fakes / "quotas.json").write_text(json.dumps(q))
    assert gke.main(["gke-up", "--name", NAME, "--yes"]) == 2
    err = capsys.readouterr().err
    assert "refusing: SSD_TOTAL_GB in europe-west4: 480 of 500 GB used, 20 free; the run needs 32 GB" in err
    assert not any(x[:3] == ["gcloud", "container", "clusters"] for x in _calls(fakes))


def test_gke_up_that_fits_creates_the_labelled_cluster_and_touches_no_storageclass(fakes, capsys):
    assert gke.main(["gke-up", "--name", NAME, "--yes"]) == 0
    c = _calls(fakes)
    assert _first(c, ["gcloud", "compute", "regions", "describe", "europe-west4"]) < _first(
        c, ["gcloud", "container", "clusters", "create"])
    assert not any("storageclass" in x for x in c)
    assert [x[:2] for x in _mutating(c)] == [["kubectl", "apply"]]               # the self-destruct only
    assert "SSD_TOTAL_GB in europe-west4: 397 of 500 GB used, 103 free" in capsys.readouterr().out


def test_gke_ssd_quota_for_a_pair(fakes, capsys):
    assert gke.main(["gke-ssd-quota", "--runs", "2"]) == 0
    assert gke.main(["gke-ssd-quota", "--runs", "4"]) == 2
    err = capsys.readouterr().err
    assert "SSD_TOTAL_GB" in err and "refusing" not in err          # the caller says "refusing" once


def test_run606_pair_refuses_when_both_clusters_do_not_fit(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, FAKE_SSD_USAGE="450")
    assert p.returncode == 2 and "refusing: SSD_TOTAL_GB" in p.stderr, p.stderr[-800:]
    assert "refusing: refusing:" not in p.stderr
    assert "needs 64 GB" in p.stderr and "2 bench cluster(s)" in p.stderr
    assert not [c for c in _pair_calls(fake) if " dry=0 " in c]          # neither run started


def test_run606_reports_a_failed_disk_backstop(harness):  # noqa: F811
    """2: gke-down exit 4 (cluster deleted, its disks unchecked): run606 never
    claims "gke-down OK", says DISK BACKSTOP FAILED and how to list them."""
    env, state, _, tmp = harness
    env = dict(env, FAKE_DISKS_FAIL="1")
    p = subprocess.run(["bash", str(BENCH / "run606.sh")], env=env, capture_output=True, text=True, timeout=600)
    assert "deleted (gke-down OK)" not in p.stderr
    assert "deleted, but DISK BACKSTOP FAILED" in p.stderr and "labels.goog-k8s-cluster-name=" in p.stderr
    assert p.returncode != 0


from tests.test_run15_customer_latency import _launch, pairbed  # noqa: E402,F401
from tests.test_run606_real_sequence import harness  # noqa: E402,F401


def _pair_calls(fake):
    p = fake / "calls"
    return p.read_text().splitlines() if p.exists() else []


@pytest.mark.parametrize("stderr, gone", [
    ("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, message=Not found: "
     "projects/p/locations/europe-west4/clusters/c.", True),
    ("ERROR: ResponseError: code=503, message=backend not found, try again", False),
    ("ERROR: (gcloud.container.clusters.describe) the server was not found (dial tcp: lookup)", False),
    ("ERROR: (gcloud.container.clusters.describe) ResponseError: code=500, message=error 404 upstream", False),
])
def test_the_gone_poll_counts_only_gclouds_notfound(monkeypatch, stderr, gone):
    """#907 N1: the same NotFound test as the cluster lookup — code=404 /
    NOT_FOUND — never "not found" or "404" anywhere in a failure, which could
    run the disk backstop while the cluster's nodes still hold its disks."""
    def fake(argv, *, stderr_out=None, **kw):
        stderr_out.append(stderr)
        return 1, ""
    monkeypatch.setattr(gke, "execute", fake)
    t = iter([0.0, 1e9])
    assert gke.wait_cluster_gone(gke.default_spec(NAME), log=lambda m: None, deadline=1.0,
                                 clock=lambda: next(t), sleep=lambda s: None) is gone
