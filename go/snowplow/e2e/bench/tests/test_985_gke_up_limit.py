"""#985: run 18 lost both arms ~19 s into gke-up to GKE's per-location
cluster limit ("Your project already has the maximum number of clusters (3)
in location europe-west4"), and the bench recorded no reason:
command-failures.jsonl empty, run.log only the RUN line, then "TEARDOWN
FAILED — delete by hand" for a cluster that never existed.

1. gke-up's stderr goes through record_failure (the log, command-failures.jsonl,
   the verdict), redacted;
2. the limit (Cloud Quotas `ClustersPerRegion`, READ) is checked before any
   cluster is created: by gke-up for its own cluster, and by the pair for both,
   in its dry pass too; a limit that cannot be read refuses the run;
3. a teardown after a create that never made a cluster (exact-name describe:
   NotFound) says "nothing to delete".
"""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import gke
from tests import fake_cloudquotas
from tests.test_gke_pvc_disks import NAME, _calls, fakes  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

R = "europe-west4"
# gcloud's text for run 18's refusal (the account a placeholder, as gcloud appends it)
LIMIT_ERR = ("ERROR: (gcloud.container.clusters.create) ResponseError: code=403, message=Insufficient quota: "
             "Your project already has the maximum number of clusters (3) in location europe-west4. "
             "This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active "
             "account specified by the [core/account] property.")


def _clusters(*names_locations):
    return json.dumps([{"name": n, "location": loc, "status": st} for n, loc, st in names_locations])


THREE = _clusters(("krateo-installer-release", R, "RUNNING"), ("snowplow-bench-run17a5-x", R, "RECONCILING"),
                  ("snowplow-bench-run17b5-x", R, "RUNNING"))


# ─── 2. the limit, read ─────────────────────────────────────────────────────

def test_the_limit_is_the_regions_own_entry_then_the_applicable_one():
    info = fake_cloudquotas.quota_info(3, region_override={R: 5})
    assert gke.cluster_limit(info, R) == 5                     # the region's own override
    assert gke.cluster_limit(info, "europe-west3") == 3        # the default that applies to it
    assert gke.cluster_limit(info, "mars-central1") is None    # none applies: unknown
    assert gke.cluster_limit(None, R) is None


def test_every_cluster_in_the_region_counts_whatever_its_owner_or_status():
    clusters = json.loads(THREE) + [{"name": "krateo-057", "location": "europe-west3-a", "status": "RUNNING"}]
    ok, msg = gke.cluster_count_fit(3, clusters, R, 1)
    assert not ok and "3 of 3" in msg and "krateo-installer-release (RUNNING)" in msg and "0 free" in msg
    ok, msg = gke.cluster_count_fit(5, clusters, R, 2)
    assert ok and "2 free, the run needs 2" in msg
    assert gke.cluster_count_fit(-1, clusters, R, 9)[0]                       # GKE's unlimited
    ok, msg = gke.cluster_count_fit(None, clusters, R, 1)
    assert not ok and "unknown" in msg                                        # an unread limit never fits
    ok, msg = gke.cluster_count_fit(3, None, R, 1)
    assert not ok and "unknown" in msg


def test_gke_cluster_quota_reads_the_api_and_refuses_when_full(fakes, monkeypatch, capsys):  # noqa: F811
    monkeypatch.setenv("FAKE_CLUSTERS", _clusters(("krateo-installer-release", R, "RUNNING")))
    assert gke.main(["gke-cluster-quota", "--runs", "2"]) == 0
    assert "1 of 3 (ClustersPerRegion)" in capsys.readouterr().out
    assert gke.main(["gke-cluster-quota", "--runs", "3"]) == 2
    (fakes / "state" / "cluster_limit").write_text("4")                        # the limit is READ
    assert gke.main(["gke-cluster-quota", "--runs", "3"]) == 0
    (fakes / "state" / "cluster_limit").write_text("missing")                  # an API that does not answer
    assert gke.main(["gke-cluster-quota", "--runs", "1"]) == 2
    assert "could not be read" in capsys.readouterr().err


def test_gke_up_refuses_when_the_region_is_full_and_creates_nothing(fakes, monkeypatch, capsys):  # noqa: F811
    monkeypatch.setenv("FAKE_CLUSTERS", THREE)
    assert gke.main(["gke-up", "--name", NAME, "--yes"]) == 2
    err = capsys.readouterr().err
    assert "refusing: GKE clusters in europe-west4: 3 of 3 (ClustersPerRegion)" in err and "nothing was created" in err
    assert not any(x[:3] == ["gcloud", "container", "clusters"] and x[3:4] == ["create"] for x in _calls(fakes))


def test_the_pair_s_dry_pass_refuses_a_pair_the_region_cannot_hold(pairbed):  # noqa: F811
    """The dry pass catches the cluster-count case: two runs beside run 18's
    three clusters, before anything is created."""
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_CLUSTERS=THREE)
    assert p.returncode == 2, p.stderr[-1500:]
    assert "refusing: GKE clusters in europe-west4: 3 of 3 (ClustersPerRegion)" in p.stderr
    assert "the run needs 2" in p.stderr and "DRY: both runs would start now" not in p.stderr
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_CLUSTERS=_clusters(("krateo-installer-release", R, "RUNNING")))
    assert p.returncode == 0, p.stderr[-1500:]
    assert "GKE clusters in europe-west4: 1 of 3" in p.stderr


# ─── 1 + 3. run606: the reason recorded; no false teardown alarm ───────────

def test_a_refused_create_is_recorded_and_its_teardown_says_nothing_to_delete(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_CREATE_ERROR=LIMIT_ERR),
                       capture_output=True, text=True, timeout=600)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0
    line = next(ln for ln in log.splitlines() if "up FAILED: gke-up exited" in ln)
    assert "maximum number of clusters (3) in location europe-west4" in line
    recs = [json.loads(x) for x in (tmp / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert [r["step"] for r in recs] == ["up"]
    assert any("maximum number of clusters (3)" in s for s in recs[0]["stderr"])
    for text in (log, p.stderr, p.stdout, json.dumps(recs)):                  # redacted
        assert "PLACEHOLDER-OPERATOR" not in text
    assert "nothing to delete" in log and "TEARDOWN FAILED" not in log
    gcl = [c["argv"] for c in json.loads("[" + ",".join((state / "calls.jsonl").read_text().splitlines()) + "]")
           if c["tool"] == "gcloud"]
    assert not any(a[:3] == ["container", "clusters", "delete"] for a in gcl)  # nothing deleted
    assert any(a[:3] == ["container", "clusters", "describe"] and a[3] == env["RUN_NAME"] for a in gcl)


def test_a_cluster_gke_up_made_that_vanished_is_still_a_failed_teardown(harness):  # noqa: F811
    """Only a create that did not complete reads NotFound as nothing to delete."""
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_REGIONAL_NOTFOUND="1"),
                       capture_output=True, text=True, timeout=600)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "TEARDOWN FAILED" in log and "nothing to delete" not in log


def test_gke_down_says_nothing_to_delete_for_a_cluster_that_does_not_exist(fakes, monkeypatch, capsys):  # noqa: F811
    (fakes / "state" / "deleted").write_text("1")                              # the describe answers NotFound
    assert gke.main(["gke-down", "--name", NAME, "--yes"]) == gke.EXIT_NOTHING_TO_DELETE
    assert "nothing to delete" in capsys.readouterr().err
    assert not any(x[:3] == ["gcloud", "container", "clusters"] and x[3:4] == ["delete"] for x in _calls(fakes))


@pytest.mark.parametrize("stderr,want", [
    (LIMIT_ERR, "maximum number of clusters (3)"),
    ("WARNING: something\nERROR: (gcloud.container.clusters.create) code=400 bad", "code=400 bad"),
    ("no error line here", "the error text is withheld"),
])
def test_a_failed_create_says_why_redacted(stderr, want):
    out = gke.gcloud_error_lines(stderr)
    assert want in out and "PLACEHOLDER-OPERATOR@example.invalid" not in out
