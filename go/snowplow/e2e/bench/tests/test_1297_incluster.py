"""#1297: data606 and every timed sampler run IN-CLUSTER on the loadgen node (bench/incluster.py).

The laptop orchestrates only. What these arms hold:
  - each Job's ServiceAccount has exactly the RBAC it needs: every object data606 applies is
    covered, the reader rules its Roles grant are held (no escalate / bind), no wildcard verb;
  - the bench code the pods run is the import closure of their entry points, stdlib only (the
    python image has no PyYAML / kubernetes), and its ConfigMap fits the API;
  - the pods' kubectl is the pinned release binary (sha256), their kubeconfig the pod's own token;
  - data606 in the pod is bounded as run606 bounded it;
  - through run606 (the real sequence, the pods stood in for by the fake kubectl running the
    containers' own commands): a pod off the loadgen pool fails its step, a data606 pod that failed
    fails the data step and is still collected and deleted, and a data606 pod still listed when the
    drivers come fails the measure (the two never share the node).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

from bench import data_manifests, incluster, rbac_model
from bench.load606 import list_dep_fixture_yaml, list_dep_status_fixture_yaml
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

PLURAL = {"Namespace": ("", "namespaces"), "Secret": ("", "secrets"),
          "Role": ("rbac.authorization.k8s.io", "roles"), "RoleBinding": ("rbac.authorization.k8s.io", "rolebindings"),
          "ClusterRole": ("rbac.authorization.k8s.io", "clusterroles"),
          "ClusterRoleBinding": ("rbac.authorization.k8s.io", "clusterrolebindings"),
          "CompositionDefinition": ("core.krateo.io", "compositiondefinitions"),
          "GithubScaffoldingWithCompositionPage": (data_manifests.COMP_GVR, data_manifests.COMP_RES),
          "User": ("basic.authn.krateo.io", "users"), "RESTAction": ("templates.krateo.io", "restactions")}


def _data606_documents(namespaces=3, users=4):
    """Every object data606 applies, from the renderers it applies them from."""
    docs = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": f"bench-ns-{i:02d}"}}
            for i in range(1, namespaces + 1)]
    docs += [d for _, d in data_manifests.documents(namespaces=namespaces, users=users,
                                                    fresh_modes=("annotation", "status"))]
    for text in rbac_model.rbac_yaml(namespaces):
        docs += [d for d in yaml.safe_load_all(text) if d]
    for f in (list_dep_fixture_yaml, list_dep_status_fixture_yaml):
        docs += [d for d in yaml.safe_load_all(f("bench-ns-01", data_manifests.COMP_GVR, data_manifests.COMP_VERSION,
                                                 data_manifests.COMP_RES)) if d]
    return docs


def _grants(rules, group, resource, verb, ns_scoped=False):
    return any((group in r.get("apiGroups", []) or "*" in r.get("apiGroups", []))
               and (resource in r.get("resources", []) or "*" in r.get("resources", []))
               and verb in r.get("verbs", []) for r in rules)


def test_every_object_data606_applies_is_covered_by_its_serviceaccount():
    cluster, authn = incluster.data606_rbac()
    kinds = set()
    for d in _data606_documents():
        kinds.add(d["kind"])
        assert d["kind"] in PLURAL, f"a kind data606 applies that the RBAC test does not know: {d['kind']}"
        group, res = PLURAL[d["kind"]]
        ns = (d.get("metadata") or {}).get("namespace")
        rules = cluster + (authn if ns == data_manifests.AUTHN_NS else [])
        for verb in ("create", "patch"):          # a server-side apply creates or patches
            assert _grants(rules, group, res, verb), (d["kind"], ns, verb)
    assert {"User", "Secret", "RESTAction", "CompositionDefinition", "Role", "ClusterRole"} <= kinds


def test_the_rules_its_roles_grant_are_held_so_no_escalate_or_bind_is_needed():
    cluster, authn = incluster.data606_rbac()
    for d in _data606_documents():
        if d["kind"] in ("Role", "ClusterRole"):
            for r in d["rules"]:
                for g in r["apiGroups"]:
                    for res in r["resources"]:
                        for v in r["verbs"]:
                            assert _grants(cluster, g, res, v), (d["metadata"]["name"], g, res, v)
    for rules in (*incluster.data606_rbac(), *incluster.observe_rbac()):
        for r in rules:
            assert not {"escalate", "bind", "impersonate", "*", "delete", "deletecollection"} & set(r["verbs"]), r


def test_the_observer_reads_only():
    cluster, ns = incluster.observe_rbac()
    assert cluster == [{"nonResourceURLs": ["/metrics"], "verbs": ["get"]}]
    assert all(set(r["verbs"]) <= {"get", "list"} for r in ns)


def test_each_job_is_pinned_to_the_loadgen_pool_with_its_own_serviceaccount():
    d = incluster.render_data606(context="gke_p_r_snowplow-bench-x", timeout_s=2400, linger_s=1800,
                                 machine="n2-standard-16", data_args=["--scale", "50000"])
    o = incluster.render_observe(context="gke_p_r_snowplow-bench-x", every_s=15, retry_s=5, selector="a=b",
                                 user_prefix="scaleuser-", password="pw", linger_s=9000)
    for objs, job, sa in ((d, "data606", "bench-data606"), (o, "observe606", "bench-observe606")):
        j = next(x for x in objs if x["kind"] == "Job")
        spec = j["spec"]["template"]["spec"]
        assert j["metadata"]["name"] == job and spec["serviceAccountName"] == sa
        assert spec["nodeSelector"] == {"bench.krateo.io/pool": "loadgen"}
        assert spec["tolerations"] == [{"key": "dedicated", "operator": "Equal", "value": "loadgen",
                                        "effect": "NoSchedule"}]
        env = {e["name"]: e.get("value") for e in spec["containers"][0]["env"]}
        assert env["KUBECONFIG"] == "/tools/kubeconfig" and env["BENCH_GKE_CONTEXT"] == "gke_p_r_snowplow-bench-x"
        # nothing of the laptop's kubeconfig, no token: only what the kubelet projects
        text = json.dumps(objs)
        assert "token:" not in text and "client-certificate" not in text and spec["automountServiceAccountToken"]
    c = next(x for x in d if x["kind"] == "Job")["spec"]["template"]["spec"]["containers"][0]
    assert c["resources"] == {"requests": {"cpu": "14390m", "memory": "8Gi"}}   # the node's budget but the observer's
    assert c["command"][-2:] == ["--scale", "50000"]
    oc = next(x for x in o if x["kind"] == "Job")["spec"]["template"]["spec"]["containers"][0]
    assert oc["resources"]["limits"]["cpu"] == "1"                              # never the browser's CPU
    sec = next(x for x in o if x["kind"] == "Secret")
    assert sec["stringData"] == {"deny": "pw"}                                  # kapply redacts it on disk


def test_data606_takes_what_the_loadgen_node_leaves_and_the_k6_plan_still_fits_beside_the_observer():
    from bench import k6_job
    assert incluster.data606_requests("n2-standard-8") == ("6410m", "8Gi")
    k6_job.loadgen_fit("n2-standard-16", k6_job.k6_plan_requests())
    with pytest.raises(ValueError, match="unknown loadgen machine type"):
        incluster.data606_requests("e2-small")


def test_the_shipped_code_is_the_closure_and_runs_on_the_stdlib_alone(tmp_path):
    files = incluster.source_closure()
    for need in ("__main__.py", "cli.py", "data606.py", "lifecycle.py", "cluster.py", "sampler.py", "logscrub.py",
                 "identity.py", "incluster.py", "log_stream.sh"):
        assert need in files
    tarfile.open(fileobj=io.BytesIO(incluster.source_archive())).extractall(tmp_path, filter="data")
    assert sorted(p.name for p in (tmp_path / "bench").iterdir()) == sorted(files)
    # -S: no site-packages, as the python image has none of the bench's (PyYAML, kubernetes)
    for mod in (["bench", "data606", "--help"], ["bench.sampler", "--help"], ["bench.logscrub", "--help"],
                ["bench.incluster", "closure"]):
        p = subprocess.run([sys.executable, "-S", "-m", *mod], cwd=tmp_path, capture_output=True, text=True,
                           timeout=120, env={"PATH": os.environ["PATH"], "BENCH_ALLOW_NON_GKE": "1"})
        assert p.returncode == 0, (mod, p.stderr[-2000:])
    # reproducible: the same bytes for the same code
    assert incluster.source_archive() == incluster.source_archive()


def test_the_source_configmap_fits_the_api():
    cm = incluster.source_configmap()
    size = sum(len(v) for v in cm["binaryData"].values())
    assert size < 0.8 * 1024 * 1024, f"bench-src binaryData {size} B: near the API's 1 MiB ConfigMap limit"


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_tools_installs_the_pinned_kubectl_and_a_kubeconfig_of_the_pods_own_token(tmp_path, monkeypatch):
    body = b"fake kubectl binary"
    monkeypatch.setitem(incluster.KUBECTL_SHA256, "arm64", hashlib.sha256(body).hexdigest())
    urls = []
    incluster.tools(str(tmp_path), "gke_p_r_c", fetch=lambda u, timeout: urls.append(u) or _Resp(body),
                    machine=lambda: "aarch64", env={"KUBERNETES_SERVICE_HOST": "10.0.0.1",
                                                    "KUBERNETES_SERVICE_PORT": "443"})
    assert urls == ["https://dl.k8s.io/release/v1.36.2/bin/linux/arm64/kubectl"]
    assert (tmp_path / "kubectl").read_bytes() == body and os.access(tmp_path / "kubectl", os.X_OK)
    cfg = json.loads((tmp_path / "kubeconfig").read_text())
    assert cfg["current-context"] == "gke_p_r_c"
    assert cfg["users"] == [{"name": "serviceaccount",
                             "user": {"tokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token"}}]
    assert cfg["clusters"][0]["cluster"]["server"] == "https://10.0.0.1:443"


def test_tools_refuses_a_kubectl_whose_digest_is_not_the_pinned_one(tmp_path, monkeypatch):
    monkeypatch.setattr(incluster.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit, match="no verified kubectl"):
        incluster.tools(str(tmp_path), "c", fetch=lambda u, timeout: _Resp(b"tampered"), machine=lambda: "x86_64",
                        env={"KUBERNETES_SERVICE_HOST": "h"})
    assert not (tmp_path / "kubectl").exists()


def test_data606_in_the_pod_is_bounded_and_says_its_exit(tmp_path):
    stand_in = tmp_path / "py"
    stand_in.write_text("#!/bin/sh\necho started\nsleep 30\n")
    stand_in.chmod(0o755)
    out = tmp_path / "out"
    rc = incluster.run_data606(str(out), 1, 0, ["--scale", "4"], python=str(stand_in), linger=lambda o, s: None)
    assert rc == 0
    assert (out / "done").read_text().strip() == "124"
    run = json.loads((out / "data606-run.json").read_text())
    assert run["timed_out"] is True and run["args"] == ["--scale", "4"]
    assert "started" in (out / "data606.log").read_text()


def _run(harness, **env):  # noqa: F811
    e, state, _, tmp = harness
    e.update(env)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=e, capture_output=True, text=True, timeout=600)
    return p, p.stdout + p.stderr, state, tmp / "out"


def test_a_pod_off_the_loadgen_pool_fails_its_step_before_the_data_load(harness):  # noqa: F811
    p, out, state, o = _run(harness, FAKE_INC_POOL="default")
    assert p.returncode != 0
    assert "observe606 FAILED: its pod is not where it must run: node loadgen-0 is in pool 'default', not 'loadgen'" in out
    assert "=== step data ===" not in out
    rec = json.loads((o / "incluster-placement.jsonl").read_text().splitlines()[0])
    assert rec["ok"] is False and rec["node_pool"] == "default"
    # the teardown deleted the observer's Job, ServiceAccount and RBAC before the cluster
    deleted = [json.loads(ln) for ln in (state / "deleted.jsonl").read_text().splitlines()]
    assert any("job/observe606" in d["names"] for d in deleted)


def test_a_failed_data606_pod_fails_the_data_step_and_is_still_deleted(harness):  # noqa: F811
    p, out, state, o = _run(harness, FAKE_INC_FAILED="data606")
    assert p.returncode != 0
    assert "data606 FAILED: its pod failed before its program ran" in out
    assert "=== step reboot ===" not in out
    deleted = [json.loads(ln) for ln in (state / "deleted.jsonl").read_text().splitlines()]
    assert any("job/data606" in d["names"] for d in deleted)
    assert any(d["names"] == ["clusterrolebinding/bench-data606", "clusterrole/bench-data606"] for d in deleted)
    # the observer's streams still came into OUT (the teardown collected them)
    assert (o / "apf.jsonl.gz").exists() and (o / "snowplow-stream.log").exists()
    fails = [json.loads(ln) for ln in (o / "command-failures.jsonl").read_text().splitlines()]
    assert any(f["step"] == "data" for f in fails)


def test_a_data606_pod_still_on_the_node_when_the_drivers_come_fails_the_measure(harness):  # noqa: F811
    p, out, state, o = _run(harness, FAKE_INC_LEFT="1")
    assert p.returncode != 0
    assert "measure FAILED: data606's pod is still on the loadgen node (pod/data606-0): the data load would overlap the drivers" in out
    calls = [json.loads(ln)["argv"] for ln in (state / "calls.jsonl").read_text().splitlines()]
    assert not [c for c in calls if any(x.endswith("loadgen-job.yaml") for x in c)]


def test_the_dry_pass_says_where_each_job_runs_and_the_budget_counts_it(tmp_path):
    from tests.test_1297_host_load import _dry_env
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=_dry_env(tmp_path), capture_output=True,
                       text=True, timeout=300)
    assert p.returncode == 0, p.stderr[-3000:]
    assert "DRY: python3 -m bench.incluster render-data606 --context " in p.stderr
    assert "DRY: python3 -m bench.incluster render-observe --context " in p.stderr
    line = next(ln for ln in p.stderr.splitlines() if "run budget:" in ln)
    assert ("+ in-cluster 4215 (observer start 300 + data606 start 300 + last poll 15 + finish 6x300 "
            "+ teardown 1800)") in line
    src = (BENCH_DIR / "run606.sh").read_text()
    # nothing timed or per-request runs here any more
    for gone in ("bench.sampler --kind", "logs -f", "python3 -m bench data606 --"):
        assert gone not in src.replace("bench/log_stream.sh", ""), gone
