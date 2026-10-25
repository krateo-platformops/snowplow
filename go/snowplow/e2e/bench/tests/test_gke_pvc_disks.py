"""#895: gke-down leaked the CSI PVC disks; europe-west4's SSD_TOTAL_GB (500)
filled up and run 16 attempt 1 died at the platform step (ClickStack's MongoDB
PVC: QUOTA_EXCEEDED).

  1. teardown deletes the PVCs and waits for the CSI driver to delete their
     disks BEFORE it deletes the cluster;
  2. after the cluster is gone, an exact-name backstop deletes what is left of
     THIS cluster only (its own PVs' disks; disks GKE labelled with this
     cluster's name AND id) — never a disk of another cluster;
  3. gke-up (and run606_pair.sh for two clusters) refuses a run whose PVC
     disks do not fit the region's SSD_TOTAL_GB;
  4. every PD CSI StorageClass is re-created with the bench disk labels
     (created-by=bench-gke, bench-cluster=<name>), so a leak is attributable.

No cloud call: a fake gcloud / kubectl on PATH plays GKE. The disk label
shapes are the project's real ones (read 2026-10-25): krateo-057's PVC disks
carry cluster-id-gke-io AND goog-gke-cluster-id-base32, the release
cluster's goog-gke-cluster-id-base32 only, both base32 of the cluster id.
tests/fixtures/pvcs_run14.json is run 14's diagnostics/pvcs.json;
tests/fixtures/region_europe_west4_quotas_20261025.json the region's quotas.
"""

from __future__ import annotations

import json
import os
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
    _disk("pvc-ours-leftover", {**_gke_labels(NAME, OURS_B32, ZONE), **gke.BENCH_LABELS, "bench-cluster": NAME}),
    _disk("pvc-ours-attached", _gke_labels(NAME, OURS_B32, ZONE), users=["projects/p/zones/z/instances/n1"]),
    _disk("pvc-ours-recorded-unlabelled", None),              # older shape: no labels at all
    # same cluster NAME, an earlier cluster's id: not this cluster's
    _disk("pvc-same-name-other-cluster", _gke_labels(NAME, PREV_B32, ZONE)),
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


# ─── 4. labels, footprint, quota (pure) ────────────────────────────────────

def test_the_live_clusters_disk_ids_are_base32_of_their_cluster_ids():
    assert gke.cluster_id_b32({"id": ID_RELEASE}) == B32_RELEASE
    assert gke.cluster_id_b32({"id": ID_057}) == B32_057
    assert gke.cluster_id_b32({}) is None and gke.cluster_id_b32({"id": "zz"}) is None


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


def test_storageclasses_are_recreated_with_the_bench_disk_labels():
    spec = gke.default_spec(NAME)
    scs = {"items": [
        {"metadata": {"name": "standard-rwo", "uid": "u", "resourceVersion": "9",
                      "annotations": {"storageclass.kubernetes.io/is-default-class": "true",
                                      "kubectl.kubernetes.io/last-applied-configuration": "{}"},
                      "labels": {"addonmanager.kubernetes.io/mode": "EnsureExists"}},
         "provisioner": gke.PD_CSI, "parameters": {"type": "pd-balanced"}, "reclaimPolicy": "Delete",
         "volumeBindingMode": "WaitForFirstConsumer", "allowVolumeExpansion": True},
        {"metadata": {"name": "premium-rwo"}, "provisioner": gke.PD_CSI,
         "parameters": {"type": "pd-ssd", "labels": "team=x"}},
        {"metadata": {"name": "standard"}, "provisioner": "kubernetes.io/gce-pd",
         "parameters": {"type": "pd-standard"}},
    ]}
    new = gke.labelled_storageclasses(scs, gke.bench_disk_labels(spec))
    assert [s["metadata"]["name"] for s in new] == ["standard-rwo", "premium-rwo"]   # PD CSI only
    std = new[0]
    labels = dict(kv.split("=") for kv in std["parameters"]["labels"].split(","))
    assert labels == {"created-by": "bench-gke", "purpose": "snowplow-bench", "issue": "606", "bench-cluster": NAME}
    assert std["parameters"]["type"] == "pd-balanced" and std["volumeBindingMode"] == "WaitForFirstConsumer"
    assert std["metadata"]["annotations"] == {"storageclass.kubernetes.io/is-default-class": "true"}
    assert "uid" not in std["metadata"] and "resourceVersion" not in std["metadata"]
    assert "team=x" in new[1]["parameters"]["labels"] and "created-by=bench-gke" in new[1]["parameters"]["labels"]


# ─── 2. the backstop's exact filter ────────────────────────────────────────

def test_the_backstop_takes_this_clusters_leftovers_only():
    got = gke.backstop_disks(DISKS, name=NAME, cluster_id=OURS_B32, zone=ZONE,
                             recorded=["pvc-ours-recorded-unlabelled"])
    assert got == ["pvc-ours-leftover", "pvc-ours-recorded-unlabelled"]


@pytest.mark.parametrize("other", ["pvc-release-kagent", "pvc-release-in-our-zone", "pvc-057-mongodb",
                                   "pvc-057-mongodb-in-our-zone", "pvc-same-name-other-cluster"])
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


# ─── 1+2. teardown order, end to end against fakes ─────────────────────────

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
        if a[:2] == ["get", "pv"]:
            pvs = [] if has("pvcs_deleted") and not os.environ.get("FAKE_PV_STUCK") else json.load(
                open(os.environ["FAKE_PVS"]))
            print(json.dumps({"items": pvs})); sys.exit(0)
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
        if has("deleted"):
            sys.stderr.write("ERROR: ResponseError: code=404, message=Not found: clusters/x.\\n"); sys.exit(1)
        print(json.dumps({"name": a[3], "id": os.environ["FAKE_ID"], "resourceLabels": {
            "purpose": "snowplow-bench", "issue": "606", "created-by": "bench-gke", "owner": "t",
            "ttl-expires": "9999999999"}})); sys.exit(0)
    if a[:3] == ["container", "clusters", "delete"]:
        open(os.path.join(st, "deleted"), "w").write("1"); sys.exit(0)
    if a[:3] == ["compute", "disks", "list"]:
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

PVS = [
    {"metadata": {"name": "pvc-ours-leftover"},
     "spec": {"csi": {"driver": gke.PD_CSI, "volumeHandle": f"projects/p/zones/{ZONE}/disks/pvc-ours-leftover"},
              "persistentVolumeReclaimPolicy": "Delete", "capacity": {"storage": "10Gi"}}},
    {"metadata": {"name": "pvc-ours-recorded-unlabelled"},
     "spec": {"csi": {"driver": gke.PD_CSI,
                      "volumeHandle": f"projects/p/zones/{ZONE}/disks/pvc-ours-recorded-unlabelled"},
              "persistentVolumeReclaimPolicy": "Retain", "capacity": {"storage": "2G"}}},
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
    q = json.loads(json.dumps(QUOTAS))
    (tmp_path / "quotas.json").write_text(json.dumps(q))
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


def test_teardown_releases_the_pvcs_before_the_cluster_then_backstops_exactly(fakes):
    log = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60, sleep=lambda s: None)
    c = _calls(fakes)
    i_patch = _first(c, ["kubectl", "patch", "pv", "pvc-ours-recorded-unlabelled"])   # Retain -> Delete
    i_pvc = _first(c, ["kubectl", "delete", "pvc", "--all", "--all-namespaces"])
    i_pod = _first(c, ["kubectl", "delete", "pod", "-n", "krateo-system", "mongodb-0"])
    i_gone_pv = max(i for i, x in enumerate(c) if x[:3] == ["kubectl", "get", "pv"])
    i_del = _first(c, ["gcloud", "container", "clusters", "delete", NAME])
    i_list = _first(c, ["gcloud", "compute", "disks", "list"])
    i_disk = _first(c, ["gcloud", "compute", "disks", "delete"])
    assert i_patch < i_pvc < i_pod < i_gone_pv < i_del < i_list < i_disk     # PVCs first, cluster, backstop
    assert not any(x[:4] == ["kubectl", "delete", "pod", "-n"] and "web-0" in x for x in c)   # no PVC: left alone
    assert not any(x[:3] == ["kubectl", "patch", "pv"] and "pvc-ours-leftover" in x for x in c)
    deleted = c[i_disk][4:c[i_disk].index("--zone")]
    assert deleted == ["pvc-ours-leftover", "pvc-ours-recorded-unlabelled"]
    assert c[i_disk][c[i_disk].index("--zone") + 1] == ZONE
    assert any("every PV is gone" in m for m in log)
    assert any(m.startswith("disk backstop: deleted 2 leftover disk(s)") for m in log)


def test_a_stuck_pv_is_bounded_and_the_backstop_still_runs(fakes, monkeypatch):
    monkeypatch.setenv("FAKE_PV_STUCK", "1")
    ticks = iter(range(0, 100_000, 10))                       # 10 s per clock read
    log = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60,
                 clock=lambda: next(ticks), sleep=lambda s: None)
    c = _calls(fakes)
    assert any("still there at the budget's end" in m for m in log)
    assert _first(c, ["gcloud", "container", "clusters", "delete"]) < _first(c, ["gcloud", "compute", "disks",
                                                                                 "delete"])


def test_an_unreachable_cluster_skips_the_release_but_not_the_backstop(fakes, monkeypatch):
    (fakes / "bin" / "kubectl").write_text("#!/bin/sh\nexit 1\n")
    log = []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60, sleep=lambda s: None)
    c = _calls(fakes)
    assert any("could not list" in m for m in log)
    i_disk = _first(c, ["gcloud", "compute", "disks", "delete"])
    assert c[i_disk][4:c[i_disk].index("--zone")] == ["pvc-ours-leftover"]   # by GKE's labels alone


def test_a_failed_disk_delete_is_loud_but_the_cluster_delete_stands(fakes, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_DISK_DELETE_RC", "1")
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60, sleep=lambda s: None)
    assert "DISK BACKSTOP FAILED" in capsys.readouterr().err


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


def test_gke_up_that_fits_labels_the_storageclasses_before_the_self_destruct(fakes, capsys):
    # the fake's `get storageclass` prints nothing: give it GKE's default class
    (fakes / "bin" / "kubectl").write_text(FAKE.replace(
        'if a[:2] == ["get", "pv"]:',
        'if a[:2] == ["get", "storageclass"]:\n'
        '        print(json.dumps({"items": [{"metadata": {"name": "standard-rwo"}, "provisioner": '
        f'"{gke.PD_CSI}", "parameters": {{"type": "pd-balanced"}}}}]}})); sys.exit(0)\n'
        '    if a[:2] == ["create", "-f"]:\n'
        '        open(os.path.join(st, "created.json"), "w").write(sys.stdin.read()); sys.exit(0)\n'
        '    if a[:2] == ["get", "pv"]:'))
    assert gke.main(["gke-up", "--name", NAME, "--yes"]) == 0
    c = _calls(fakes)
    i_cred = _first(c, ["gcloud", "container", "clusters", "get-credentials"])
    i_del = _first(c, ["kubectl", "delete", "storageclass", "standard-rwo"])
    i_new = _first(c, ["kubectl", "create", "-f", "-"])
    i_ttl = _first(c, ["kubectl", "apply", "-f", "-"])
    assert _first(c, ["gcloud", "compute", "regions", "describe", "europe-west4"]) < _first(
        c, ["gcloud", "container", "clusters", "create"])
    assert i_cred < i_del < i_new < i_ttl
    sc = json.loads((fakes / "state" / "created.json").read_text())
    assert f"bench-cluster={NAME}" in sc["parameters"]["labels"]
    assert "created-by=bench-gke" in sc["parameters"]["labels"]
    assert "SSD_TOTAL_GB in europe-west4: 397 of 500 GB used, 103 free" in capsys.readouterr().out


def test_gke_ssd_quota_for_a_pair(fakes, capsys):
    assert gke.main(["gke-ssd-quota", "--runs", "2"]) == 0
    assert gke.main(["gke-ssd-quota", "--runs", "4"]) == 2
    assert "refusing: SSD_TOTAL_GB" in capsys.readouterr().err


def test_run606_pair_refuses_when_both_clusters_do_not_fit(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, FAKE_SSD_USAGE="450")
    assert p.returncode == 2 and "refusing: refusing: SSD_TOTAL_GB" in p.stderr, p.stderr[-800:]
    assert "needs 64 GB" in p.stderr and "2 bench cluster(s)" in p.stderr
    assert not [c for c in _pair_calls(fake) if " dry=0 " in c]          # neither run started


def test_run606_labels_the_pvc_disks_on_the_real_sequence(harness):  # noqa: F811
    """run606.sh against the fakes: gke-up re-creates standard-rwo with the
    bench disk labels before the platform is installed."""
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH / "run606.sh")], env=env, capture_output=True, text=True, timeout=600)
    assert p.returncode == 0, p.stderr[-2000:]
    created = (state / "created.jsonl").read_text()
    assert "created-by=bench-gke" in created and f"bench-cluster={env['RUN_NAME']}" in created
    calls = [json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines()]
    i_sc = next(i for i, c in enumerate(calls) if c["tool"] == "kubectl" and "create" in c["argv"])
    i_helm = next(i for i, c in enumerate(calls) if c["tool"] == "helm" and "install" in c["argv"])
    assert i_sc < i_helm


from tests.test_run15_customer_latency import _launch, pairbed  # noqa: E402,F401
from tests.test_run606_real_sequence import harness  # noqa: E402,F401


def _pair_calls(fake):
    p = fake / "calls"
    return p.read_text().splitlines() if p.exists() else []
