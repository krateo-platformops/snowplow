"""Run 26h: every arm died in gke-up with "TTL backstop NOT proven: IAM answers that the CronJob's identity (the GKE
node SA) does NOT hold container.clusters.delete". The bench clusters ran as the project's default Compute Engine SA,
which may not delete clusters. Diego's decision (2026-11-13): ONE dedicated node SA for bench clusters, allowed to
delete ONLY clusters named snowplow-bench-*, so it can never delete krateo-057.

  1. the SA and its exact grants are declared once (bench/gke.py) and every node pool is created with it;
  2. its reaper grant is name-limited by an IAM condition: tested here against a model of IAM's documented
     condition semantics (tests/fake_gke_api.py), itself checked against IAM's own documented examples;
  3. the TTL proof is an EFFECTIVE test, as the SA, from the cluster: GET / DELETE of a non-existent
     snowplow-bench- name must answer 404 NOT_FOUND and of a non-existent name outside it 403 PERMISSION_DENIED;
  4. vm606 plan-iam prints it all; vm606 node-sa makes the SA, the role and the bindings, idempotent and verified.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

from bench import gke, vm606
from tests import fake_gke_api, fake_vm606
from tests.test_gke_pvc_disks import NAME, fakes  # noqa: F401  (fixture)
from tests.test_vm606 import _changing, _run, _up, _verbs, fake  # noqa: F401  (fixture)

PROJECT = "operations-dev-krateo-io"
NODE_SA = f"snowplow-bench-node@{PROJECT}.iam.gserviceaccount.com"
REAPER = f"projects/{PROJECT}/roles/snowplowBenchClusterReaper"
PROBE = "abcdef0123"


# ─── 1. every cluster and node pool runs as the bench's own node SA ─────────

@pytest.mark.parametrize("project", [PROJECT, "another-project-1"])
def test_every_cluster_and_node_pool_is_created_with_the_bench_node_sa(project):
    spec = gke.default_spec(NAME, project=project)
    cmds = gke.create_commands(spec)
    creates = [c for c in cmds if c[:3] in (["gcloud", "container", "clusters"], ["gcloud", "container", "node-pools"])
               and c[3] == "create"]
    assert len(creates) == 1 + len(spec.extra_pools) == 3
    for c in creates:
        assert c[c.index("--service-account") + 1] == f"snowplow-bench-node@{project}.iam.gserviceaccount.com", c
        assert c.count("--service-account") == 1
    assert not [c for c in cmds if any("-compute@developer.gserviceaccount.com" in t for t in c)]
    assert "--service-account" not in cmds[-1]                     # get-credentials takes none


def test_the_node_sa_holds_exactly_the_declared_bindings():
    assert gke.node_sa_email(PROJECT) == NODE_SA
    assert gke.node_sa_bindings(PROJECT) == [("roles/container.defaultNodeServiceAccount", None),
                                             (REAPER, gke.reaper_condition())]
    # the reaper role carries the CronJob's two calls and nothing else, and the proof tests exactly those
    assert sorted(gke.REAPER_PERMISSIONS) == ["container.clusters.delete", "container.clusters.get"]
    assert set(gke.TTL_PROBE_METHODS) == set(gke.REAPER_PERMISSIONS)
    cron = gke.self_destruct_manifest(gke.default_spec(NAME))
    assert "gcloud container clusters describe" in cron and "gcloud container clusters delete" in cron


# ─── 2. the condition: bench clusters only, in any location ─────────────────

# IAM's own extract() examples (conditions-attribute-reference, "Extract"), on its documented object name
_DOC_NAME = "projects/_/buckets/acme-orders-aaa/objects/data_lake/orders/order_date=2019-11-03/aef87g87ae0876"


@pytest.mark.parametrize("template, want", [
    ("/order_date={date}/", "2019-11-03"),
    ("buckets/{name}/", "acme-orders-aaa"),
    ("/orders/{empty}order_date", ""),
    ("{start}/objects/data_lake", "projects/_/buckets/acme-orders-aaa"),
    ("orders/{end}", "order_date=2019-11-03/aef87g87ae0876"),
    ("{all}", _DOC_NAME),
    ("/orders/{none}/order_date=", ""),
    ("/orders/order_date=2019-11-03/{id}/data_lake", ""),
])
def test_the_condition_model_answers_iams_documented_extract_examples(template, want):
    assert fake_gke_api.extract(_DOC_NAME, template) == want


def _admits(name: str, rtype: str = gke.CLUSTER_RESOURCE_TYPE) -> bool:
    return fake_gke_api.evaluate(gke.reaper_condition()["expression"], rtype=rtype, name=name)


@pytest.mark.parametrize("name", [
    f"projects/{PROJECT}/locations/europe-west4/clusters/snowplow-bench-run26h1-1a2b3c4d",
    f"projects/{PROJECT}/locations/europe-west1/clusters/snowplow-bench-1113-1200",
    f"projects/{PROJECT}/zones/europe-west4-a/clusters/snowplow-bench-1013-0900",       # a zonal bench cluster
    f"projects/{PROJECT}/locations/europe-west4/clusters/snowplow-bench-ttlprobe-{PROBE}",
    "projects/357378285144/locations/europe-west4/clusters/snowplow-bench-x",            # the project by number
])
def test_the_reaper_condition_admits_bench_clusters_in_any_location(name):
    assert _admits(name)


@pytest.mark.parametrize("name", [
    *(f"projects/{PROJECT}/locations/europe-west4/clusters/{c}" for c in gke.FORBIDDEN_CLUSTER_NAMES),
    *(f"projects/{PROJECT}/zones/europe-west4-a/clusters/{c}" for c in gke.FORBIDDEN_CLUSTER_NAMES),
    f"projects/{PROJECT}/locations/europe-west4/clusters/ttlprobe-{PROBE}",
    f"projects/{PROJECT}/locations/europe-west4/clusters/xsnowplow-bench-1",
    f"projects/{PROJECT}/locations/europe-west4/clusters/snowplow-benchx-1",
    f"projects/{PROJECT}/locations/europe-west4/clusters/snowplow-bench",
    f"projects/{PROJECT}/locations/snowplow-bench-x/clusters/krateo-057",
    f"projects/{PROJECT}/locations/europe-west4/clusters/krateo-057/nodePools/snowplow-bench-x",
])
def test_the_reaper_condition_refuses_every_other_cluster(name):
    assert not _admits(name)


def test_the_reaper_condition_is_about_clusters_only():
    name = f"projects/{PROJECT}/locations/europe-west4/clusters/snowplow-bench-x"
    assert _admits(name) and not _admits(name, rtype="compute.googleapis.com/Instance")


def test_the_reaper_condition_is_written_once_and_reads_as_declared():
    cond = gke.reaper_condition()
    assert cond["title"] == "snowplow-bench-clusters-only"
    assert cond["expression"] == (
        'resource.type == "container.googleapis.com/Cluster" && '
        '(resource.name.extract("/locations/{location}/clusters/snowplow-bench-") != "" || '
        'resource.name.extract("/zones/{zone}/clusters/snowplow-bench-") != "")')
    assert "@" not in json.dumps(cond)


# ─── 3. the effective-permission proof: 404 inside the prefix, 403 outside ───

INSIDE, CONTROL = gke.ttl_probe_names(PROBE)


def _err(code: int, status: str, reason: str | None = None, service: str | None = None, message: str = "") -> str:
    e = {"code": code, "status": status, "message": message or f"for {fake_gke_api.CALLER}"}
    if reason:
        e["details"] = [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": reason,
                         **({"metadata": {"service": service}} if service else {})}]
    return json.dumps({"error": e})


NF = ("404", _err(404, "NOT_FOUND"))
PD = ("403", _err(403, "PERMISSION_DENIED", "IAM_PERMISSION_DENIED"))


def _logs(get=(NF, PD), delete=(NF, PD), drop=()) -> str:
    out = []
    for perm, (inside, control) in (("container.clusters.get", get), ("container.clusters.delete", delete)):
        for name, (code, body) in ((INSIDE, inside), (CONTROL, control)):
            if (perm, name) not in drop:
                out.append(f"TTL_PROOF {perm} {name} {code} {body}")
    return "\n".join(out) + "\n"


def test_404_inside_and_403_outside_for_both_calls_is_proven():
    ok, why = gke.ttl_proof_answer(_logs())
    assert ok, why
    assert "get and delete snowplow-bench-* clusters, and no other" in why
    assert f"{INSIDE} answered HTTP 404 (404 NOT_FOUND)" in why
    assert f"{CONTROL} answered HTTP 403 (403 PERMISSION_DENIED; reason: IAM_PERMISSION_DENIED)" in why
    assert "@" not in why


@pytest.mark.parametrize("logs, says", [
    (_logs(delete=(PD, PD)), "container.clusters.delete is not granted to the node SA, not even on snowplow-bench-*"),
    (_logs(get=(PD, PD), delete=(PD, PD)), "container.clusters.get is not granted"),
    (_logs(delete=(NF, NF)), "container.clusters.delete: the name limit does not hold: ttlprobe-abcdef0123, which "
                             "does not start with snowplow-bench-, answered HTTP 404 (404 NOT_FOUND) too, so the node "
                             "SA may delete ANY cluster of the project, krateo-057 included"),
    (_logs(get=(NF, NF)), "container.clusters.get: the name limit does not hold"),
    (_logs(delete=(("200", json.dumps({"name": "operation-1", "operationType": "DELETE_CLUSTER"})), PD)),
     "answered HTTP 200, a success (a cluster of that name exists)"),
    (_logs(delete=(("404", "<html>Not Found</html>"), PD)), "HTTP 404, an answer that is not a JSON object"),
    (_logs(delete=(NF, ("404", "<html>Not Found</html>"))), "not 404 NOT_FOUND and 403 PERMISSION_DENIED"),
    (_logs(delete=(("000", ""), PD)), "no HTTP answer (the request failed)"),
    (_logs(delete=(("403", _err(403, "PERMISSION_DENIED", "SERVICE_DISABLED", "container.googleapis.com")),) * 2),
     "reason: SERVICE_DISABLED (service container.googleapis.com)"),
    (_logs(delete=(("403", _err(403, "PERMISSION_DENIED", "ACCESS_TOKEN_SCOPE_INSUFFICIENT")), PD)),
     "ACCESS_TOKEN_SCOPE_INSUFFICIENT"),
    (_logs(delete=(NF, ("401", _err(401, "UNAUTHENTICATED")))), "HTTP 401 (401 UNAUTHENTICATED)"),
    # a 404 is GKE's lookup only when its error body says so too: a body whose status or code disagrees refuses
    (_logs(get=(("404", _err(404, "PERMISSION_DENIED", "IAM_PERMISSION_DENIED")), PD)),
     "container.clusters.get: snowplow-bench-ttlprobe-abcdef0123 answered HTTP 404 (404 PERMISSION_DENIED; reason: "
     "IAM_PERMISSION_DENIED) and ttlprobe-abcdef0123 answered HTTP 403"),
    (_logs(delete=(("404", _err(403, "NOT_FOUND")), PD)), "answered HTTP 404 (403 NOT_FOUND) and"),
    (_logs(delete=(NF, ("403", _err(403, "NOT_FOUND")))), "answered HTTP 403 (403 NOT_FOUND), not 404"),
    (_logs(delete=(NF, ("403", _err(404, "PERMISSION_DENIED")))), "answered HTTP 403 (404 PERMISSION_DENIED), not 404"),
    (_logs(drop=(("container.clusters.delete", CONTROL),)), "container.clusters.delete: the proof pod printed no "
                                                            "answer for its control name"),
    (_logs(drop=(("container.clusters.get", INSIDE),)), "printed no answer for its snowplow-bench- name"),
    (_logs() + f"TTL_PROOF container.clusters.delete {CONTROL} 404 {NF[1]}\n", "answered twice"),
    (_logs() + "TTL_PROOF container.clusters.update snowplow-bench-ttlprobe-x 404 {}\n", "not one of its probes"),
    ("gcloud: no credentials\n", "printed no answer"),
    ("", "printed no answer"),
])
def test_anything_but_404_inside_and_403_outside_refuses(logs, says):
    ok, why = gke.ttl_proof_answer(logs)
    assert not ok and says in why, why
    assert "@" not in why and fake_gke_api.MESSAGE_SENTINEL not in why    # an error's message never


def test_a_status_with_free_text_is_not_printed():
    body = json.dumps({"error": {"code": 403, "status": "denied for someone@example.invalid", "details": [
        {"reason": "x@example.invalid"}, {"reason": "IAM_PERMISSION_DENIED", "metadata": {"service": "a@b.c"}}]}})
    ok, why = gke.ttl_proof_answer(_logs(delete=(("403", body), PD)))
    assert not ok and "(403 None; reason: IAM_PERMISSION_DENIED)" in why and "@" not in why


def _script(token: str = PROBE, spec=None) -> str:
    job = yaml.safe_load(gke.ttl_proof_manifest(spec or gke.default_spec(NAME), token=token))
    return job["spec"]["template"]["spec"]["containers"][0]["args"][0]


FAKE_CURL = r'''#!/usr/bin/env python3
import json, os, sys
sys.path.insert(0, os.environ["FAKE_TESTS_PARENT"])
from tests import fake_gke_api
a = sys.argv[1:]
out, fmt, method, url = None, None, "GET", None
i = 0
while i < len(a):
    if a[i] in ("-o", "-w", "-X", "-H"):
        if a[i] == "-o": out = a[i + 1]
        if a[i] == "-w": fmt = a[i + 1]
        if a[i] == "-X": method = a[i + 1]
        if a[i] == "-H" and a[i + 1].startswith("Authorization: Bearer "):
            assert a[i + 1] == "Authorization: Bearer fake-node-sa-token", a[i + 1]
        i += 2
        continue
    if not a[i].startswith("-"): url = a[i]
    i += 1
with open(os.path.join(os.environ["FAKE_STATE"], "curl.jsonl"), "a") as fh:
    fh.write(json.dumps({"method": method, "url": url}) + "\n")
base, cluster = url.rsplit("/", 1)
perm = {"GET": "container.clusters.get", "DELETE": "container.clusters.delete"}[method]
code, body = fake_gke_api.answer(os.environ.get("FAKE_TTL_PROOF", "granted"), perm, base, cluster)
open(out, "w").write(json.dumps(body, indent=2))
assert fmt == "%{http_code}"
sys.stdout.write(str(code))
'''


@pytest.fixture
def pod(tmp_path, monkeypatch):
    """The proof pod's tools: gcloud (its metadata token) and curl (GKE, tests/fake_gke_api.py), on PATH."""
    b = tmp_path / "bin"
    b.mkdir()
    (b / "gcloud").write_text("#!/bin/sh\n[ \"$1 $2\" = 'auth print-access-token' ] && echo fake-node-sa-token\n")
    (b / "curl").write_text(FAKE_CURL)
    for f in b.iterdir():
        f.chmod(0o755)
    env = dict(os.environ, PATH=f"{b}{os.pathsep}{os.environ['PATH']}", FAKE_STATE=str(tmp_path),
               FAKE_TESTS_PARENT=str(Path(__file__).resolve().parent.parent), TMPDIR=str(tmp_path))
    return env, tmp_path


@pytest.mark.parametrize("mode, proven, says", [
    ("granted", True, "get and delete snowplow-bench-* clusters, and no other"),
    ("denied", False, "container.clusters.get is not granted to the node SA"),
    ("unlimited", False, "the name limit does not hold"),
    ("disabled", False, "SERVICE_DISABLED (service container.googleapis.com)"),
])
def test_the_proof_jobs_own_script_under_sh_against_gke(pod, mode, proven, says):
    """The script the Job runs, run by /bin/sh as the pod's would be, its curl answered as GKE answers the node SA
    (with node-sa's grant, none, an unconditional one, or the API off): what it prints decides."""
    env, st = pod
    p = subprocess.run(["/bin/sh", "-c", _script()], env=dict(env, FAKE_TTL_PROOF=mode), capture_output=True,
                       text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    ok, why = gke.ttl_proof_answer(p.stdout)
    assert ok is proven and says in why, why
    assert "fake-node-sa-token" not in p.stdout + p.stderr                   # the token never leaves the pod
    calls = [json.loads(x) for x in (st / "curl.jsonl").read_text().splitlines()]
    base = f"https://container.googleapis.com/v1/projects/{PROJECT}/locations/europe-west4/clusters"
    assert calls == [{"method": m, "url": f"{base}/{n}"} for m in ("GET", "DELETE") for n in (INSIDE, CONTROL)]
    assert NAME not in json.dumps(calls)                                     # never the run's own cluster


def test_the_probe_names_are_valid_gke_names_that_no_bench_run_uses():
    inside, control = gke.ttl_probe_names(gke.new_probe_token())
    assert inside.startswith(gke.BENCH_NAME_PREFIX) and not control.startswith(gke.BENCH_NAME_PREFIX)
    for n in (inside, control):
        assert re.fullmatch(r"[a-z][a-z0-9-]{0,38}[a-z0-9]", n), n
    assert gke.new_probe_token() != gke.new_probe_token()
    gke._check_name(inside)                                  # a bench name by the module's own rule


def test_the_probes_go_to_the_clusters_own_region(pod):
    env, st = pod
    spec = gke.default_spec(NAME, zone="europe-west1-b")
    subprocess.run(["/bin/sh", "-c", _script(spec=spec)], env=env, capture_output=True, text=True, timeout=60)
    urls = {json.loads(x)["url"].rsplit("/", 1)[0] for x in (st / "curl.jsonl").read_text().splitlines()}
    assert urls == {f"https://container.googleapis.com/v1/projects/{PROJECT}/locations/europe-west1/clusters"}


@pytest.mark.parametrize("mode, rc, says", [
    ("granted", 0, "TTL backstop proven: GKE lets the CronJob's identity (the node SA) get and delete"),
    ("denied", gke.EXIT_TTL_BACKSTOP_MISSING, "is not granted to the node SA"),
    ("unlimited", gke.EXIT_TTL_BACKSTOP_MISSING, "the name limit does not hold"),
    ("disabled", gke.EXIT_TTL_BACKSTOP_MISSING, "reason: SERVICE_DISABLED (service container.googleapis.com)"),
])
def test_gke_up_proves_the_name_limited_backstop_and_records_it(fakes, monkeypatch, capsys, mode, rc, says):  # noqa: F811
    monkeypatch.setenv("FAKE_TTL_PROOF", mode)
    out = fakes / "ttl-proof.json"
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--ttl-proof-out", str(out), "--yes"]) == rc
    o, e = capsys.readouterr()
    assert says in (o if rc == 0 else e), o + e
    # GKE's error text (it names the caller) never: only codes and reasons. The node SA is in the create argv
    # gke-up prints (stdout), never in a refusal (stderr)
    assert fake_gke_api.MESSAGE_SENTINEL not in o + e and "@" not in e
    rec = json.loads(out.read_text())
    assert rec["proven"] is (rc == 0) and rec["node_sa"] == NODE_SA
    assert rec["permissions"] == ["container.clusters.get", "container.clusters.delete"]
    inside, control = rec["probe_names"]
    assert inside.startswith("snowplow-bench-ttlprobe-") and control.startswith("ttlprobe-") and inside.endswith(
        control[len("ttlprobe-"):])
    applied = (fakes / "state" / "applied.txt").read_text()
    assert inside in applied and control in applied                          # the names the Job probed


def test_gke_up_dry_run_says_what_the_proof_calls(fakes, capsys):  # noqa: F811
    assert gke.main(["gke-up", "--name", NAME]) == 0
    out = capsys.readouterr().out
    assert ("container.clusters.get and container.clusters.delete on snowplow-bench-ttlprobe-<random> (must answer "
            "404 NOT_FOUND) and on ttlprobe-<random> (must answer 403 PERMISSION_DENIED)") in out
    assert f"--service-account {NODE_SA}" in out


# ─── 4. plan-iam prints the node SA; node-sa makes it, verified ─────────────

def test_plan_iam_prints_the_node_sa_and_its_exact_grants(fake, capsys):  # noqa: F811
    rc, out, err = _run(capsys, "plan-iam")
    assert rc == 0, err
    cmds = [ln for ln in out.splitlines() if ln.startswith("gcloud ")]
    assert shlex.join(["gcloud", *vm606.node_sa_create_argv(PROJECT)]) in cmds
    assert (f"gcloud iam roles create snowplowBenchClusterReaper --project {PROJECT} --title 'snowplow bench cluster "
            f"reaper'") in out
    assert "--permissions container.clusters.delete,container.clusters.get --stage GA" in out
    assert (f"gcloud projects add-iam-policy-binding {PROJECT} --member serviceAccount:{NODE_SA} --role "
            f"roles/container.defaultNodeServiceAccount --condition=None") in cmds
    assert (f"gcloud projects add-iam-policy-binding {PROJECT} --member serviceAccount:{NODE_SA} --role {REAPER} "
            f"--condition-from-file reaper-condition.json") in cmds
    assert json.dumps(gke.reaper_condition(), sort_keys=True) in out
    # the orchestrator acts as the node SA (and no longer as the Compute Engine default SA), under the run's condition
    assert (f"gcloud iam service-accounts add-iam-policy-binding {NODE_SA} --project {PROJECT} --member "
            f"serviceAccount:snowplow-bench-orchestrator@{PROJECT}.iam.gserviceaccount.com --role "
            f"roles/iam.serviceAccountUser --condition-from-file iam-condition.json") in cmds
    assert "-compute@developer.gserviceaccount.com" not in out
    assert _verbs(fake) == []                                              # prints only: no GCP call at all


def test_node_sa_dry_run_reads_and_prints_what_it_would_make(fake, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    rc, out, err = _run(capsys, "node-sa")
    assert rc == 0, err
    for argv in (vm606.node_sa_create_argv(PROJECT), vm606.reaper_role_create_argv(PROJECT),
                 vm606.node_binding_argv(PROJECT, "roles/container.defaultNodeServiceAccount", None)):
        assert "DRY-RUN: " + shlex.join(["gcloud", *argv]) in out
    assert (f"DRY-RUN: gcloud projects add-iam-policy-binding {PROJECT} --member serviceAccount:{NODE_SA} --role "
            f"{REAPER} --condition-from-file ") in out
    assert "DRY: nothing was changed" in out
    assert _changing(fake) == []
    assert _verbs(fake) == [f"projects describe {PROJECT}", f"iam service-accounts describe",
                            "iam roles describe", f"projects get-iam-policy {PROJECT}"]


def _node_bindings(fake) -> list[dict]:  # noqa: F811
    return [b for b in fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
            if f"serviceAccount:{NODE_SA}" in b["members"]]


def test_node_sa_makes_the_sa_the_role_and_the_bindings_then_reads_them_back(fake, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    rc, out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 0, err
    assert f"read back: {NODE_SA} enabled, {REAPER} with exactly container.clusters.delete, container.clusters.get, " \
           f"and exactly 2 project bindings" in out
    role = json.loads((fake / "roles" / "snowplowBenchClusterReaper.json").read_text())
    assert sorted(role["includedPermissions"]) == ["container.clusters.delete", "container.clusters.get"]
    cond = gke.reaper_condition()
    assert sorted(_node_bindings(fake), key=lambda b: b["role"]) == [
        {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"], "condition": cond},
        {"role": "roles/container.defaultNodeServiceAccount", "members": [f"serviceAccount:{NODE_SA}"]}]
    # again: everything is there, so nothing is made
    n = len(_changing(fake))
    rc, out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 0, err
    assert len(_changing(fake)) == n and "read back:" in out


def test_up_refuses_before_any_grant_while_the_node_sa_does_not_exist(fake, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "the bench clusters' node SA is not as node-sa makes it: " in err, err
    assert f"the node SA {NODE_SA} answers NOT_FOUND" in err and f"the role {REAPER} answers not found" in err
    assert "python3 -m bench.vm606 node-sa --yes" in err
    assert _changing(fake) == []


def _granted(fake, *extra):  # noqa: F811
    fake_vm606.put_role(fake, "snowplowBenchClusterReaper", sorted(gke.REAPER_PERMISSIONS))
    fake_vm606.put_policy(fake, f"project-{PROJECT}", [
        {"role": "roles/container.defaultNodeServiceAccount", "members": [f"serviceAccount:{NODE_SA}"]},
        {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"], "condition": gke.reaper_condition()}, *extra])


@pytest.mark.parametrize("setup, says", [
    # the reaper role bound with no condition: the SA could delete krateo-057
    (lambda f: _granted(f, {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"]}),
     f"the node SA holds {REAPER} unconditionally, which node-sa does not grant"),
    (lambda f: _granted(f, {"role": "roles/editor", "members": [f"serviceAccount:{NODE_SA}"]}),
     "the node SA holds roles/editor unconditionally, which node-sa does not grant"),
    (lambda f: _granted(f, {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"],
                            "condition": {"title": "snowplow-bench-clusters-only", "expression": "true"}}),
     f"the node SA holds {REAPER} under snowplow-bench-clusters-only: true, which node-sa does not grant"),
    (lambda f: fake_vm606.put_role(f, "snowplowBenchClusterReaper",
                                   [*gke.REAPER_PERMISSIONS, "container.clusters.update"]),
     "holds ['container.clusters.delete', 'container.clusters.get', 'container.clusters.update'], not exactly"),
    (lambda f: fake_vm606.put_role(f, "snowplowBenchClusterReaper", list(gke.REAPER_PERMISSIONS), deleted=True),
     "is deleted"),
    (lambda f: fake_vm606.put_sa(f, NODE_SA, disabled=True), "exists and is disabled"),
])
def test_node_sa_refuses_what_it_did_not_make_and_changes_nothing(fake, capsys, setup, says):  # noqa: F811
    setup(fake)
    before = fake_vm606.policy(fake, f"project-{PROJECT}")
    rc, _out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 2 and says in err and "nothing was changed" in err, err
    assert _changing(fake) == [] and fake_vm606.policy(fake, f"project-{PROJECT}") == before


def test_node_sa_refuses_a_role_iam_made_otherwise(fake, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    monkeypatch.setenv("FAKE_ROLE_EXTRA_PERMISSIONS", '["container.clusters.update"]')
    rc, _out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 2 and "after node-sa," in err and "container.clusters.update" in err


def test_node_sa_refuses_a_lost_binding_on_its_read_back(fake, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    monkeypatch.setenv("FAKE_IAM_LOST_WRITE", REAPER)
    rc, _out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 2 and f"after node-sa, the node SA does not hold {REAPER} under snowplow-bench-clusters-only" in err


@pytest.mark.parametrize("verb", ["iam service-accounts describe", "iam roles describe", "projects get-iam-policy "
                                  + PROJECT])
def test_a_read_that_answers_rc_0_and_nothing_is_never_an_answer(fake, capsys, monkeypatch, verb):  # noqa: F811
    """gcloud's reads can exit 0 with "[]" when their request failed (the shape its lists fail open with): node-sa
    reads that as no answer, never as "absent" (which would create) nor as "as declared"."""
    _granted(fake)
    monkeypatch.setenv("FAKE_RC0_EMPTY", verb)
    rc, _out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 2 and ("answered no description of itself" in err or "could not be read" in err), err
    assert _changing(fake) == []


def test_node_sa_when_all_is_there_changes_nothing_and_says_so(fake, capsys):  # noqa: F811
    _granted(fake)
    rc, out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 0, err
    assert f"the node SA {NODE_SA} exists" in out and "read back:" in out
    assert _changing(fake) == []


@pytest.mark.parametrize("verb, says", [
    ("iam service-accounts describe", f"the node SA {NODE_SA} could not be read (rc=1; "),
    ("iam roles describe", f"the role {REAPER} could not be read (rc=1; "),
])
def test_a_denied_read_is_never_read_as_absent(fake, capsys, monkeypatch, verb, says):  # noqa: F811
    """Only IAM's own NotFound for the SA or the role is "absent" (which creates it); a PERMISSION_DENIED refuses,
    named, with the operator's account removed, and nothing is made."""
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    monkeypatch.setenv("FAKE_READ_DENIED", verb)
    rc, _out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 2 and says in err and "PERMISSION_DENIED" in err, err
    assert "PLACEHOLDER-OPERATOR" not in err
    assert _changing(fake) == []


# ─── 5. up pays for nothing unless the node SA's grants are exactly node-sa's ─

_DEFAULT_NODE = {"role": "roles/container.defaultNodeServiceAccount", "members": [f"serviceAccount:{NODE_SA}"]}
_REAPER_BOUND = {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"], "condition": gke.reaper_condition()}
# the reaper binding under a condition that reads the same but for its description: another binding in IAM
_REAPER_OTHER_DESC = {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"],
                      "condition": {**gke.reaper_condition(), "description": "made by hand"}}


def _no_role(f):
    """The SA and both bindings there, the role not (FAKE_NO_NODE_SA=1 below: the fake serves no role by itself)."""
    fake_vm606.put_sa(f, NODE_SA)
    fake_vm606.put_policy(f, f"project-{PROJECT}", [_DEFAULT_NODE, _REAPER_BOUND])


@pytest.mark.parametrize("setup, says", [
    (lambda f: fake_vm606.put_sa(f, NODE_SA, disabled=True), f"the node SA {NODE_SA} is disabled"),
    (_no_role, f"the role {REAPER} answers not found"),
    (lambda f: fake_vm606.put_role(f, "snowplowBenchClusterReaper", list(gke.REAPER_PERMISSIONS), deleted=True),
     f"the role {REAPER} is deleted"),
    (lambda f: fake_vm606.put_role(f, "snowplowBenchClusterReaper",
                                   [*gke.REAPER_PERMISSIONS, "container.clusters.update"]),
     "'container.clusters.update'], not exactly"),
    (lambda f: fake_vm606.put_policy(f, f"project-{PROJECT}", [_DEFAULT_NODE]),
     f"the node SA does not hold {REAPER} under snowplow-bench-clusters-only"),
    (lambda f: fake_vm606.put_policy(f, f"project-{PROJECT}", [_REAPER_BOUND]),
     "the node SA does not hold roles/container.defaultNodeServiceAccount unconditionally"),
    (lambda f: _granted(f, {"role": "roles/editor", "members": [f"serviceAccount:{NODE_SA}"]}),
     "the node SA holds roles/editor unconditionally, which node-sa does not grant"),
    (lambda f: _granted(f, {"role": REAPER, "members": [f"serviceAccount:{NODE_SA}"]}),
     f"the node SA holds {REAPER} unconditionally, which node-sa does not grant"),
    (lambda f: fake_vm606.put_policy(f, f"project-{PROJECT}", [
        _DEFAULT_NODE, {**_REAPER_BOUND, "condition": {"title": "snowplow-bench-clusters-only", "expression": "true"}}]),
     f"the node SA holds {REAPER} under snowplow-bench-clusters-only: true, which node-sa does not grant"),
    (lambda f: _granted(f, _REAPER_OTHER_DESC), "(description: made by hand), which node-sa does not grant"),
], ids=["sa-disabled", "role-missing", "role-deleted", "role-more-permissions", "reaper-binding-missing",
        "node-binding-missing", "extra-binding", "reaper-unconditional", "wrong-condition", "same-condition-other-desc"])
def test_up_refuses_before_anything_unless_the_node_sa_is_exactly_as_node_sa_makes_it(fake, capsys, monkeypatch,  # noqa: F811
                                                                                      setup, says):
    """#1406 review: the SA present but its grants wrong would pass up, pay for a VM and clusters, and fail at gke-up's
    TTL proof (or worse, a wider grant would pass it): up reads node_sa_drift and refuses on any difference."""
    if setup is _no_role:
        monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    setup(fake)
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "the bench clusters' node SA is not as node-sa makes it: " in err and says in err, err
    assert "Nothing was created" in err
    assert _changing(fake) == []
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][:3] == ["compute", "instances", "create"]]


def test_up_passes_the_node_sa_check_when_all_is_exactly_as_node_sa_makes_it(fake, capsys, monkeypatch):  # noqa: F811
    _granted(fake)
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 0, err


def test_a_binding_held_twice_is_counted_twice():
    """node_sa_held is a count, never a set: whatever a policy answer holds twice is one more than node-sa makes."""
    held = vm606.node_sa_held({"bindings": [_DEFAULT_NODE, _REAPER_BOUND, dict(_DEFAULT_NODE)]}, PROJECT)
    assert held.total() == 3
    assert list((held - vm606.node_sa_want(PROJECT)).elements()) == [
        ("roles/container.defaultNodeServiceAccount", None, None, None)]


@pytest.mark.parametrize("extra, says", [
    (_REAPER_OTHER_DESC,f"after node-sa, the node SA holds {REAPER} under snowplow-bench-clusters-only: "),
    ({"role": "roles/editor", "members": [f"serviceAccount:{NODE_SA}"]},
     "after node-sa, the node SA holds roles/editor unconditionally, which node-sa does not grant"),
])
def test_node_sa_refuses_an_extra_binding_on_its_read_back(fake, capsys, monkeypatch, extra, says):  # noqa: F811
    """#1406 review: a binding another writer adds while node-sa runs (here: with node-sa's own first grant) is read
    back and refused, never counted away: "exactly 2 project bindings" is a count of what IAM holds."""
    monkeypatch.setenv("FAKE_NO_NODE_SA", "1")
    monkeypatch.setenv("FAKE_IAM_EXTRA_WRITE", json.dumps(extra))
    rc, out, err = _run(capsys, "node-sa", "--yes")
    assert rc == 2 and says in err, out + err
    assert "read back:" not in out and "exactly 2 project bindings" not in out
    assert len(_node_bindings(fake)) == 3                          # what IAM holds: node-sa's two and the extra
