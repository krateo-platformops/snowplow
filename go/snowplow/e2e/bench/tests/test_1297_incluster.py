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

from bench import data_manifests, incluster, load606, rbac_model
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
                                 room=_room(), data_args=["--scale", "50000"])
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
    assert c["resources"] == {"requests": {"cpu": "2667m", "memory": "8192Mi"}}     # its need, not the room; no limit
    assert c["command"][-2:] == ["--scale", "50000"]
    oc = next(x for x in o if x["kind"] == "Job")["spec"]["template"]["spec"]["containers"][0]
    assert oc["resources"]["limits"]["cpu"] == "1"                              # never the browser's CPU
    sec = next(x for x in o if x["kind"] == "Secret")
    assert sec["stringData"] == {"deny": "pw"}                                  # kapply redacts it on disk


MIB = 1 << 20


def _node(cpu="15890m", mem="59825Mi"):
    """The loadgen node as `kubectl get nodes -l bench.krateo.io/pool=loadgen -o json` lists it
    (default: an n2-standard-16's allocatable, k6_job.LOADGEN_ALLOCATABLE)."""
    return {"items": [{"metadata": {"name": "gke-loadgen-1", "labels": {"bench.krateo.io/pool": "loadgen"}},
                       "status": {"allocatable": {"cpu": cpu, "memory": mem}}}]}


def _pod(name, cpu, mem, *, ns="kube-system", phase="Running", init=None, labels=None):
    spec = {"containers": [{"name": "c", "resources": {"requests": {"cpu": cpu, "memory": mem}}}]}
    if init:
        spec["initContainers"] = [{"name": "i", "resources": {"requests": {"cpu": init[0], "memory": init[1]}}}]
    return {"metadata": {"namespace": ns, "name": name, "labels": labels or {}}, "spec": spec, "status": {"phase": phase}}


def _room(ds=(("ds", "1000m", "4096Mi"),), **kw):
    return incluster.loadgen_room(_node(**kw), {"items": [_pod(n, c, m) for n, c, m in ds]})


def test_run_23s_shape_is_unchanged():
    """An n2-standard-16 whose DaemonSets request what run 23's plan reserved for them (1000m / 4096Mi):
    its room is what it was (14390m / 54705Mi); data606 asks for its need, the rest is slack."""
    r = _room()
    assert r["ok"] and (r["cpu_m"], r["memory_mi"]) == (14390, 54705) and r["observer_counted"] == "to come"
    assert incluster.data606_requests(r) == ("2667m", "8192Mi")
    assert (r["slack_cpu_m"], r["slack_memory_mi"]) == (14390 - 2667, 54705 - 8192)
    from bench import k6_job
    k6_job.loadgen_fit("n2-standard-16", k6_job.k6_plan_requests())          # the drivers' plan still fits after it


def test_heavy_daemonsets_give_a_smaller_room_and_the_same_request():
    """Seven DaemonSets (run 24's loadgen node had 7, unrecorded) requesting 3.2 CPUs: the room is
    smaller, data606 still asks for its need; the observer, once there, is counted as scheduled, not
    twice."""
    ds = [(f"ds-{i}", "457m", "700Mi") for i in range(7)]
    r = _room(ds)
    assert r["ok"] and r["cpu_m"] == 15890 - 7 * 457 - 500 and r["memory_mi"] == 59825 - 7 * 700 - 1024
    sched = incluster.loadgen_room(_node(), {"items": [_pod(n, c, m) for n, c, m in ds] + [
        _pod("observe606-x", "500m", "1Gi", ns="bench-loadgen", init=("100m", "128Mi"), labels={"job-name": "observe606"})]})
    assert sched["observer_counted"] == "scheduled" and sched["cpu_m"] == r["cpu_m"]
    # what the scheduler counts: finished pods do not hold requests; an init container larger than its
    # containers counts instead of them
    done = incluster.loadgen_room(_node(), {"items": [_pod("job-done", "8", "8Gi", phase="Succeeded")]})
    assert done["cpu_m"] == 15890 - 500
    assert incluster.pod_requests(_pod("p", "100m", "64Mi", init=("2", "1Gi"))) == (2000, 1024 * MIB)
    assert incluster.render_data606(context="c", timeout_s=1, linger_s=1, room=r, data_args=[])[-1][
        "spec"]["template"]["spec"]["containers"][0]["resources"] == {"requests": {"cpu": "2667m", "memory": "8192Mi"}}


def test_too_little_room_is_refused_with_the_numbers():
    # memory short only: 59825 - 51200 - 1024 = 7601Mi < 8192Mi; the CPU (3390m) is above its floor
    r = _room((("ds", "12", "50Gi"),))
    assert not r["ok"] and r["why"].startswith(
        "the loadgen node gke-loadgen-1 leaves data606 too little: memory 7601Mi < its need 8192Mi (allocatable 15890m")
    # CPU short only: 15890 - 13500 - 500 = 1890m
    tight = _room((("ds", "13500m", "4Gi"),))
    assert not tight["ok"] and "cpu 1890m < its need 2667m" in tight["why"] and "memory" not in tight["why"].split("(")[0]
    with pytest.raises(ValueError, match="leaves data606 too little"):
        incluster.data606_requests(tight)
    two = incluster.loadgen_room({"items": [_node()["items"][0], _node()["items"][0]]}, {"items": []})
    assert not two["ok"] and two["why"] == "2 node(s) in the loadgen pool, want 1"


def test_the_floor_is_run_23s_share_from_its_own_records():
    """Run 23's three 50K phases overlapped three deep (its consoles' times): each arm had at most a
    third of the laptop's 8 CPUs / 24 GiB."""
    assert incluster.max_overlap(incluster.RUN23_DATA606["phases_50k"]) == 3
    assert incluster.data606_floor() == (2667, 8192)
    assert incluster.max_overlap({"a": ("00:00:00", "00:00:10"), "b": ("00:00:10", "00:00:20")}) == 1


def test_the_pods_are_hardened_as_netns606_is():
    """Review L4: non-root, no privilege escalation, every capability dropped, the default seccomp
    profile, a read-only root filesystem (all writes go to emptyDirs, TMPDIR = /scratch included)."""
    objs = (incluster.render_data606(context="c", timeout_s=1, linger_s=1, room=_room(), data_args=[])
            + incluster.render_observe(context="c", every_s=15, retry_s=5, selector="a=b", user_prefix="u",
                                       password="pw", linger_s=1))
    for j in (o for o in objs if o["kind"] == "Job"):
        spec = j["spec"]["template"]["spec"]
        assert spec["securityContext"]["runAsNonRoot"] is True and spec["securityContext"]["runAsUser"] != 0
        for c in spec["containers"] + spec["initContainers"]:
            sc = c["securityContext"]
            assert sc["allowPrivilegeEscalation"] is False and sc["readOnlyRootFilesystem"] is True
            assert sc["capabilities"] == {"drop": ["ALL"]} and sc["seccompProfile"] == {"type": "RuntimeDefault"}
            mounts = {m["mountPath"] for m in c["volumeMounts"]}
            assert "/scratch" in mounts and {"/opt/bench", "/tools"} <= mounts
        assert {"name": "TMPDIR", "value": "/scratch"} in spec["containers"][0]["env"]
        vols = {v["name"]: v for v in spec["volumes"]}
        assert all("emptyDir" in vols[m["name"]] for m in spec["containers"][0]["volumeMounts"])


def test_an_observer_that_died_is_named_in_the_verdict(harness):  # noqa: F811
    """Review M2: the observer OOM-killed before collect: its pod's outcome is recorded (reason, exit
    code) and said as INFO, not left to read as streams that are merely missing."""
    p, out, state, o = _run(harness, FAKE_INC_OOM="observe606")
    assert p.returncode == 0, out[-3000:]
    rec = [json.loads(ln) for ln in (o / "observer-pod.jsonl").read_text().splitlines()]
    assert rec[0]["kind"] == "observer_pod" and rec[0]["phase"] == "Failed"
    assert rec[0]["containers"][0]["terminated"] == {"reason": "OOMKilled", "exit_code": 137, "finished_at": None}
    v = json.loads((o / "verdict.json").read_text())
    line = v["report"]["incluster_report_only"]["lines"][-1]
    assert line == ("INFO observer pod: observe606-0 Failed: DIED before collect, container observe terminated "
                    "(OOMKilled, exit 137): the APF, top and log streams end where it died")
    assert line in out                                          # in the run log too
    assert v["verdict"] != "INVALID" or "observe606" not in json.dumps(v.get("invalid_reasons"))   # INFO, never judged


def test_too_little_room_fails_the_env_step_before_anything_runs_there(harness):  # noqa: F811
    p, out, state, o = _run(harness, FAKE_INC_DS="15000m 4Gi")
    assert p.returncode != 0
    assert "loadgen room FAILED (env): the loadgen node loadgen-0 leaves data606 too little: cpu 390m < its need 2667m" in out
    assert "=== step data ===" not in out
    calls = [json.loads(ln)["argv"] for ln in (state / "calls.jsonl").read_text().splitlines()]
    assert not [c for c in calls if any(x.endswith(("observe-job.yaml", "data606-job.yaml")) for x in c)]


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
    recs = [json.loads(ln) for ln in (o / "incluster-placement.jsonl").read_text().splitlines()]
    assert [r["kind"] for r in recs] == ["loadgen_room", "incluster_placement"]     # the room, then the pod
    rec = recs[1]
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
    assert ("+ in-cluster 4803 (observer start 300 + data606 start 300 + its last poll 3 + finish 8x300 "
            "+ cleanup 1800)") in line
    src = (BENCH_DIR / "run606.sh").read_text()
    # nothing timed or per-request runs here any more
    for gone in ("bench.sampler --kind", "logs -f", "python3 -m bench data606 --"):
        assert gone not in src.replace("bench/log_stream.sh", ""), gone


# ─── run 24b: data606 Pending on "Insufficient memory" on every arm ─────────────────────────────
#
# Its loadgen-room.json (snowplow-bench-run24n1-f8eaceb9): allocatable 15890m / 60040096Ki, eight pods
# (the observer among them), data606 told to request 15089m / 56940Mi, the scheduler: "0/5 nodes are
# available: 1 Insufficient memory, ...". The gmp-system collector's requests are decimal (GMP's
# operator manifest: prometheus 4m / 32M, config-reloader 1m / 4M, init config-init 1m / 4M); the
# room counted 32M + 4M as 30 + 3 Mi. The other pods' requests as that record counted them.
RUN24B_ALLOC = ("15890m", "60040096Ki")


def _c(name, cpu, mem, **extra):
    return {"name": name, "resources": {"requests": {k: v for k, v in (("cpu", cpu), ("memory", mem)) if v}}, **extra}


def _pod_of(ns, name, containers, init=(), labels=None, **meta):
    return {"metadata": {"namespace": ns, "name": name, "labels": labels or {}, **meta},
            "spec": {"containers": list(containers), "initContainers": list(init)}, "status": {"phase": "Running"}}


RUN24B_PODS = [
    _pod_of("bench-loadgen", "observe606-smwh6", [_c("observe", "500m", "1Gi")], [_c("tools", "100m", "128Mi")],
            labels={"job-name": "observe606"}),
    _pod_of("gmp-system", "collector-wsjmm", [_c("config-reloader", "1m", "4M"), _c("prometheus", "4m", "32M")],
            [_c("config-init", "1m", "4M")]),
    _pod_of("kube-system", "filestore-node-rrwk9", [_c("c", "25m", "160Mi")]),
    _pod_of("kube-system", "fluentbit-gke-7qgrb", [_c("c", "105m", "230Mi")]),
    _pod_of("kube-system", "gke-metrics-agent-td7rl", [_c("c", "21m", "145Mi")]),
    _pod_of("kube-system", "kube-proxy-gke-x", [_c("c", "100m", None)]),
    _pod_of("kube-system", "node-local-dns-46kt9", [_c("c", "30m", "50Mi")]),
    _pod_of("kube-system", "pdcsi-node-7mzv4", [_c("c", "15m", "50Mi")]),
]
# the scheduler's arithmetic, by hand (not incluster's): allocatable and every request in bytes
RUN24B_ALLOC_BYTES = 60040096 * 1024
RUN24B_USED_BYTES = (1024 + 160 + 230 + 145 + 50 + 50) * MIB + 36_000_000


def _fits(request_mem: str, used_bytes: int) -> bool:
    """The scheduler's NodeResourcesFit on memory, by hand: used + the request within allocatable."""
    n, unit = int(request_mem[:-2]), request_mem[-2:]
    assert unit == "Mi"
    return used_bytes + n * MIB <= RUN24B_ALLOC_BYTES


def _cpu_m_of(q: str) -> int:
    assert q.endswith("m")
    return int(q[:-1])


def test_run_24bs_node_holds_the_data606_it_renders_and_a_pod_that_lands_after_the_read():
    """Run 24b's own node: the room is the scheduler's exact free memory (not 0.43 MiB more), and
    data606's request fits it, before and after a pod lands on the node after the read (the
    bench-self-destruct CronJob's, every 10 min there: 100Mi here). The old request (the room
    itself, 56940Mi) fit neither."""
    r = incluster.loadgen_room(_node(*RUN24B_ALLOC), {"items": RUN24B_PODS})
    assert r["ok"] and r["observer_counted"] == "scheduled"
    assert r["memory_bytes"] == RUN24B_ALLOC_BYTES - RUN24B_USED_BYTES        # exact, decimal units included
    used_cpu = 500 + 5 + 25 + 105 + 21 + 100 + 30 + 15
    assert r["cpu_m"] == 15890 - used_cpu
    collector = next(s for s in r["scheduled"] if s["pod"] == "collector-wsjmm")
    assert collector == {"namespace": "gmp-system", "pod": "collector-wsjmm", "cpu_m": 5, "memory_bytes": 36_000_000}
    assert not _fits("56940Mi", RUN24B_USED_BYTES)                            # what run 24b asked: refused
    cpu, mem = incluster.data606_requests(r)
    assert _fits(mem, RUN24B_USED_BYTES) and _fits(mem, RUN24B_USED_BYTES + 100 * MIB)
    assert _cpu_m_of(cpu) + used_cpu + 100 <= 15890
    # the record says the slack beside the request: the room a late pod has
    assert r["slack_memory_mi"] == (RUN24B_ALLOC_BYTES - RUN24B_USED_BYTES - 8192 * MIB) // MIB
    assert "of slack beside it" in r["why"] and f"requesting {used_cpu}m / 1693.33Mi" in r["why"], r["why"]


def test_the_request_is_the_need_whatever_the_room():
    """The request does not depend on the read: two rooms, one request (data606_floor: run 23's
    share). A room below the need is refused, never rendered smaller."""
    big = _room((("ds", "100m", "100Mi"),))
    small = _room((("ds", "10", "40Gi"),))
    assert big["ok"] and small["ok"] and big["cpu_m"] != small["cpu_m"]
    assert incluster.data606_requests(big) == incluster.data606_requests(small) == (
        f"{incluster.data606_floor()[0]}m", f"{incluster.data606_floor()[1]}Mi")
    short = _room((("ds", "10", "50Gi"),))
    assert not short["ok"]
    with pytest.raises(ValueError):
        incluster.data606_requests(short)


def test_a_terminating_pod_still_holds_its_requests():
    """The scheduler frees a pod's requests once it is deleted, not when its deletion starts: a pod
    with a deletionTimestamp counts (and is said so); a finished one does not."""
    term = _pod("old-job-x", "2", "8Gi")
    term["metadata"]["deletionTimestamp"] = "2026-11-11T13:17:15Z"
    r = incluster.loadgen_room(_node(), {"items": [term]})
    assert r["cpu_m"] == 15890 - 2000 - 500 and r["memory_mi"] == 59825 - 8192 - 1024
    assert r["scheduled"][0].get("terminating") is True and "(1 of them terminating)" in r["why"]
    done = incluster.loadgen_room(_node(), {"items": [_pod("j", "2", "8Gi", phase="Succeeded")]})
    assert done["cpu_m"] == 15890 - 500


def test_pod_requests_follow_the_schedulers_formula():
    """k8s.io/component-helpers resource.PodRequests: native sidecars (restartPolicy Always init
    containers) run beside the containers and add to them; a plain init container counts with the
    sidecars started before it; pod-level requests replace the containers'; overhead adds."""
    sidecar = _pod_of("ns", "p", [_c("main", "1", "200Mi")],
                      [_c("side", "100m", "100Mi", restartPolicy="Always"), _c("init", "200m", "500Mi")])
    assert incluster.pod_requests(sidecar) == (1100, 600 * MIB)       # memory: the init (500) + the sidecar (100)
    late_sidecar = _pod_of("ns", "p", [_c("main", "1", "200Mi")],
                           [_c("init", "200m", "500Mi"), _c("side", "100m", "100Mi", restartPolicy="Always")])
    assert incluster.pod_requests(late_sidecar) == (1100, 500 * MIB)
    over = _pod_of("ns", "p", [_c("main", "1", "1Gi")])
    over["spec"]["overhead"] = {"cpu": "250m", "memory": "120Mi"}
    assert incluster.pod_requests(over) == (1250, 1144 * MIB)
    pod_level = _pod_of("ns", "p", [_c("a", "1", "1Gi"), _c("b", "1", "1Gi")])
    pod_level["spec"]["resources"] = {"requests": {"cpu": "1500m", "memory": "1536Mi"}}
    assert incluster.pod_requests(pod_level) == (1500, 1536 * MIB)


@pytest.mark.parametrize("q,cpu,mem", [
    ("32M", None, 32_000_000), ("4M", None, 4_000_000), ("60040096Ki", None, 60040096 * 1024),
    ("1.5Gi", None, 3 * 2 ** 29), ("129e6", None, 129_000_000), ("1e3", None, 1000), ("0.1", 100, 1),
    ("100m", 100, 1), ("1", 1000, 1), ("100u", 1, 1), ("2.0005", 2001, 3), ("", 0, 0),
])
def test_quantities_are_exact_and_rounded_up_as_the_scheduler_rounds(q, cpu, mem):
    if cpu is not None:
        assert incluster.cpu_m(q) == cpu
    assert incluster.mem_bytes(q) == mem
    with pytest.raises(ValueError):
        incluster.mem_bytes("12 Mi")


def test_a_pod_that_never_schedules_fails_its_step_with_the_schedulers_own_reason(harness):  # noqa: F811
    """Run 24b: `data606 FAILED: its pod did not start within 300s (phase Pending): ` said nothing of
    why. Now the line carries the scheduler's own FailedScheduling message (the pod's PodScheduled
    condition)."""
    p, out, state, o = _run(harness, FAKE_INC_PENDING="data606", PREFLIGHT_TIMEOUT_S="3")
    assert p.returncode != 0
    want = ("data606 FAILED: its pod did not start within 3s: pod data606-0 Pending, not scheduled: Unschedulable: "
            "0/5 nodes are available: 1 Insufficient memory, 1 node(s) had untolerated taint(s), 3 node(s) didn't "
            "match Pod's node affinity/selector.")
    assert want in out, out[-3000:]
    fails = [json.loads(ln) for ln in (o / "command-failures.jsonl").read_text().splitlines()]
    assert any(f["step"] == "data" and "Insufficient memory" in f["what"] for f in fails)


@pytest.mark.parametrize("pods,want", [
    (None, "the pods of job data606 could not be read (no pod listing): where its pod stands is unknown"),
    ({"items": []}, "job data606 has no pod (the Job controller created none, or it was deleted)"),
    ({"items": [{"metadata": {"name": "data606-x"}, "status": {"phase": "Pending"}}]},
     "pod data606-x Pending, the scheduler has not said why yet"),
    ({"items": [{"metadata": {"name": "data606-x"}, "spec": {"nodeName": "n"}, "status": {
        "phase": "Pending", "initContainerStatuses": [{"name": "tools", "state": {"waiting": {
            "reason": "ImagePullBackOff", "message": "secret program output"}}}]}}]},
     "pod data606-x Pending, waiting: tools ImagePullBackOff"),
])
def test_where_a_pod_stands_is_never_empty_and_never_a_containers_message(pods, want):
    why = load606.pod_why(pods, "data606")
    assert why == want and "secret program output" not in why


def test_a_preflight_job_that_never_schedules_says_the_schedulers_reason(harness):  # noqa: F811
    """run_preflight_job's timeout line carries where its pod stands (review #1335 M1)."""
    p, out, state, o = _run(harness, FAKE_PREFLIGHT="pending", PREFLIGHT_TIMEOUT_S="3")
    assert p.returncode != 0
    assert ("PREFLIGHT did not finish within 3s (the run): pod preflight606-0 Pending, not scheduled: Unschedulable: "
            "0/5 nodes are available: 1 Insufficient cpu.") in out, out[-3000:]


def test_a_quantity_the_room_cannot_read_is_named_and_writes_no_blank_record(tmp_path):
    """Review #1335 L1: `room` on a pod whose request is not a Kubernetes quantity answers one
    refused record naming it (exit 1), never an empty line."""
    nodes, pods = tmp_path / "nodes.json", tmp_path / "pods.json"
    nodes.write_text(json.dumps(_node()))
    pods.write_text(json.dumps({"items": [_pod("odd-ds-x", "100m", "12 Mi")]}))
    p = subprocess.run([sys.executable, "-m", "bench.incluster", "room", "--nodes", str(nodes), "--pods", str(pods),
                        "--phase", "data"], cwd=BENCH_DIR, capture_output=True, text=True)
    assert p.returncode == 1 and p.stdout.count("\n") == 1, (p.stdout, p.stderr)
    rec = json.loads(p.stdout)
    assert rec["kind"] == "loadgen_room" and rec["ok"] is False and rec["phase"] == "data"
    assert rec["why"] == ("the loadgen room could not be read: pod kube-system/odd-ds-x: its memory request "
                          "'12 Mi' is not a Kubernetes quantity")
    bad_node = tmp_path / "bad-node.json"
    bad_node.write_text(json.dumps(_node(mem="lots")))
    p = subprocess.run([sys.executable, "-m", "bench.incluster", "room", "--nodes", str(bad_node), "--pods",
                        str(tmp_path / "pods.json"), "--phase", "env"], cwd=BENCH_DIR, capture_output=True, text=True)
    assert p.returncode == 1 and "node gke-loadgen-1: its allocatable memory 'lots' is not a Kubernetes quantity" in \
        json.loads(p.stdout)["why"]
