"""#1383 gap 3: the in-cluster TTL CronJob, the one teardown layer that survives the operator's machine, runs as
the GKE node SA, and nothing checked that identity may delete the cluster ("UNVERIFIED"). gke-up now proves it on
every run, right after applying the CronJob, and fails the run when it is not proven, so run606's EXIT trap tears
the cluster down.

The proof Job runs only inside a real bench run (it asks IAM, from the cluster, as the node SA). Here: its pod
spec IS the CronJob's (the identity it proves is the CronJob's own), and IAM's answers, played by a fake kubectl,
decide gke-up's exit.
"""

from __future__ import annotations

import copy
import json
import subprocess

import pytest
import yaml

from bench import gke
from tests.test_gke_pvc_disks import BENCH, NAME, _calls, fakes  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import harness  # noqa: F401  (fixture)


def _pod(doc: dict) -> dict:
    spec = doc["spec"]
    return (spec["jobTemplate"]["spec"] if doc["kind"] == "CronJob" else spec)["template"]["spec"]


# ─── gap 3: the proof runs as the CronJob's identity ────────────────────────

def test_the_proof_jobs_pod_spec_is_the_cronjobs():
    """Same namespace, image, tolerations, restart policy and (default) service account: only the script
    differs. The identity IAM answers for is therefore the one the CronJob deletes the cluster as."""
    spec = gke.default_spec(NAME)
    cron = next(d for d in yaml.safe_load_all(gke.self_destruct_manifest(spec)) if d["kind"] == "CronJob")
    job = yaml.safe_load(gke.ttl_proof_manifest(spec))
    assert job["kind"] == "Job" and job["metadata"]["namespace"] == cron["metadata"]["namespace"]
    a, b = copy.deepcopy(_pod(cron)), copy.deepcopy(_pod(job))
    script_cron, script_job = a["containers"][0].pop("args"), b["containers"][0].pop("args")
    assert a == b
    assert "serviceAccountName" not in a and "serviceAccount" not in a        # the namespace's default KSA
    # #1387 review (blocker): BOTH pods on the one pool whose nodes carry the cloud-platform scope, and neither
    # tolerates a taint (that pool has none; the dedicated pools, which lack the scope, are tainted)
    for pod in (_pod(cron), _pod(job)):
        assert pod["nodeSelector"] == {"bench.krateo.io/pool": "platform"}
        assert "tolerations" not in pod
    assert "testIamPermissions" in script_job[0] and gke.TTL_PERMISSION in script_job[0]
    assert "clusters delete" in script_cron[0] and "delete" not in script_job[0].replace(gke.TTL_PERMISSION, "")
    assert job["spec"]["backoffLimit"] == 0 and job["spec"]["activeDeadlineSeconds"] == gke.TTL_PROOF_BUDGET_S
    assert f"projects/{spec.project}:testIamPermissions" in script_job[0]


@pytest.mark.parametrize("logs, proven, says", [
    ('TTL_PROOF {"permissions": ["container.clusters.delete"]}', True, "holds container.clusters.delete"),
    ("TTL_PROOF {}", False, "does NOT hold container.clusters.delete"),
    ('TTL_PROOF {"permissions": ["container.clusters.get"]}', False, "does NOT hold"),
    ('TTL_PROOF {"error": {"code": 403, "status": "PERMISSION_DENIED", "message": "for x@example.invalid"}}',
     False, "HTTP 403, PERMISSION_DENIED"),
    ("gcloud: no credentials\n", False, "printed no answer"),
    ("TTL_PROOF <html>", False, "not JSON"),
])
def test_only_iams_own_yes_proves_it(logs, proven, says):
    ok, why = gke.ttl_proof_answer(logs)
    assert ok is proven and says in why
    assert "@" not in why                                       # an error's message (it names the account) never


def test_gke_up_proves_the_backstop_and_records_it(fakes, capsys):
    out = fakes / "ttl-proof.json"
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--ttl-proof-out", str(out), "--yes"]) == 0
    rec = json.loads(out.read_text())
    assert rec["proven"] is True and rec["permission"] == "container.clusters.delete" and rec["cluster"] == NAME
    assert (rec["node"], rec["pool"], rec["pool_required"]) == ("gke-node-proof", "platform",
                                                                "bench.krateo.io/pool=platform")
    assert "TTL backstop proven" in capsys.readouterr().out
    c = _calls(fakes)
    order = [i for i, x in enumerate(c) if x[:2] == ["kubectl", "apply"] or x[1:3] in (["get", "job"], ["logs", "job/ttl-delete-proof"])
             or x[1:3] == ["delete", "job"]]
    kinds = [c[i][1:3] for i in order]
    assert kinds == [["apply", "-f"], ["apply", "-f"], ["get", "job"], ["logs", "job/ttl-delete-proof"],
                     ["delete", "job"]], kinds
    applied = (fakes / "state" / "applied.txt").read_text()
    assert applied.index("kind: CronJob") < applied.index("name: ttl-delete-proof")


@pytest.mark.parametrize("proof, says", [
    ("denied", "does NOT hold container.clusters.delete"),
    ("error", "IAM refused the permission test (HTTP 403, PERMISSION_DENIED)"),
    ("silent", "printed no answer"),
    ("failed", "the proof Job failed"),
])
def test_a_backstop_not_proven_fails_gke_up(fakes, monkeypatch, capsys, proof, says):
    monkeypatch.setenv("FAKE_TTL_PROOF", proof)
    out = fakes / "ttl-proof.json"
    rc = gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--ttl-proof-out", str(out), "--yes"])
    err = capsys.readouterr().err
    assert rc == gke.EXIT_TTL_BACKSTOP_MISSING != 0
    assert "TTL backstop NOT proven" in err and says in err, err
    assert "@" not in err
    assert json.loads(out.read_text())["proven"] is False


def test_a_proof_job_that_never_ends_is_not_proven(fakes, monkeypatch):
    monkeypatch.setenv("FAKE_TTL_PROOF", "never")
    t = iter(range(0, 10**6, 100))
    ok, why, _placement = gke.prove_ttl_delete(gke.default_spec(NAME), clock=lambda: next(t), sleep=lambda s: None)
    assert not ok and "did not end within" in why
    assert any(x[1:3] == ["delete", "job"] for x in _calls(fakes))            # cleaned up all the same


def test_run606_stops_and_tears_down_when_the_backstop_is_not_proven(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, FAKE_TTL_PROOF="denied"),
                       capture_output=True, text=True, timeout=600)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and f"up FAILED: gke-up exited {gke.EXIT_TTL_BACKSTOP_MISSING}" in log, log[-3000:]
    assert "TTL backstop NOT proven" in log or "TTL backstop NOT proven" in p.stderr
    gcl = [json.loads(x)["argv"] for x in (state / "calls.jsonl").read_text().splitlines()
           if json.loads(x)["tool"] == "gcloud"]
    assert any(a[:4] == ["container", "clusters", "delete", env["RUN_NAME"]] for a in gcl)   # the trap's teardown
    assert json.loads((tmp / "out" / "ttl-proof.json").read_text())["proven"] is False


def test_plan_iam_reads_the_proof_as_the_node_sas_call_and_nothing_else(tmp_path):
    """The orchestrator SA gets no resourcemanager role for it (vm606.NODE_SA_REST: that one constant, in
    bench/gke.py); the same host anywhere else is an orchestrator call plan-iam does not cover."""
    import shutil

    from bench import vm606
    assert vm606.scan(BENCH)["uncovered"] == []
    d = tmp_path / "copy"
    shutil.copytree(BENCH / "bench", d / "bench", ignore=shutil.ignore_patterns("__pycache__"))
    with open(d / "bench" / "nodes.py", "a") as fh:
        fh.write('\nOTHER = "https://cloudresourcemanager.googleapis.com/v1"\n')
    assert [u for u in vm606.scan(d)["uncovered"] if "cloudresourcemanager" in u]


# ─── #1387 review (blocker): the CronJob and its proof run on the pool that can delete the cluster ──

def test_only_the_platform_pool_carries_the_cloud_platform_scope():
    """The selector's pool is the cluster create's own (--scopes cloud-platform, its --node-labels); the node-pools
    create for snowplow and loadgen sets no scope (gcloud's gke-default: no GKE API) and taints them."""
    spec = gke.default_spec(NAME)
    create, *pools, _creds = gke.create_commands(spec)
    label, value = gke.ttl_pool(spec)
    assert create[create.index("--scopes") + 1] == "cloud-platform"
    assert f"{label}={value}" in create[create.index("--node-labels") + 1].split(",")
    assert spec.platform_pool.taint is None
    for p in pools:
        assert "--scopes" not in p and f"{label}={value}" not in " ".join(p) and "--node-taints" in p


@pytest.mark.parametrize("pool, says", [
    ("snowplow", "the proof pod ran on node gke-node-proof of pool snowplow, not the bench.krateo.io/pool=platform pool"),
    ("none", "the proof pod ran on node None of pool None"),
])
def test_a_proof_from_another_pool_proves_nothing(fakes, monkeypatch, capsys, pool, says):  # noqa: F811
    monkeypatch.setenv("FAKE_TTL_PROOF_POOL", pool)
    out = fakes / "ttl-proof.json"
    rc = gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--ttl-proof-out", str(out), "--yes"])
    err = capsys.readouterr().err
    assert rc == gke.EXIT_TTL_BACKSTOP_MISSING and says in err, err
    rec = json.loads(out.read_text())
    assert rec["proven"] is False and rec["pool"] == (None if pool == "none" else pool)
