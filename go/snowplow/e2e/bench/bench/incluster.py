"""#1297: the operator's laptop orchestrates only; the run's own work runs IN-CLUSTER, on the loadgen node.

Paid run 24 (2026-11-09) failed its data step on all three arms: data606 ran on the laptop (96
concurrent `kubectl apply` across the arms) while other agents' test suites held it at a load of ~90 on
8 CPUs, and the laptop's APF sampler lost 70-75 % of its samples. The clusters were idle. So the
bench no longer measures through the laptop: everything per-request or timed runs as a Job on the
run's loadgen node (bench.krateo.io/pool=loadgen, its `dedicated=loadgen` taint tolerated, the node the
k6 drivers use), and the laptop applies, waits and collects.

  data606     the 50K data load, `python3 -m bench data606 ...` exactly as the laptop ran it (the same
              code, shipped; the same arguments), in a Job of its own (render_data606). Its proof
              (/out/data-proof.json) and its log are copied into the run dir as before. It requests
              the loadgen node's whole budget but the observer's, and it runs before the measure: the
              Job and its pod are gone before any k6 Job is applied, so the two never share the node.
  observe606  every timed sampler the laptop ran (render_observe; one pod, one supervisor): the API
              server's APF scrape (apf), `kubectl top` of snowplow through the boot (top_boot) and the
              measure (top), and snowplow's scrubbed log stream (bench/log_stream.sh). Each is the same
              program as before (bench/sampler.py, bench/logscrub.py); the files it writes keep their
              names, so evaluate reads them unchanged. Started at the env step, collected at collect
              (or by the teardown, whatever stopped the run).

Credentials: each Job runs as a per-run ServiceAccount of its own, with exactly the RBAC it needs
(data606_rbac, observe_rbac), created at its start and deleted when it is done and at teardown. The
pod's kubectl uses that ServiceAccount's projected token through a kubeconfig written IN the pod
(`tools`): its context is named like the laptop's ($BENCH_GKE_CONTEXT), so bench/cluster.py's context
pin and guard run unchanged, and the only cluster the token can reach is the one it runs in. Nothing
of the laptop's kubeconfig, and no token, is ever copied into the cluster.

kubectl in the pod: the official release binary (KUBECTL_VERSION, the laptop's), fetched from
dl.k8s.io by the init container and verified against its pinned sha256 (KUBECTL_SHA256): the python
image has no kubectl, and the upstream kubectl image is distroless (nothing to copy it out with).

The bench code: the import closure of what the pods run (source_closure, read from the source's own
imports, lazy ones included), as one tar.xz in a ConfigMap (bench-src), extracted by the init container.
"""

from __future__ import annotations

import argparse
import ast
import base64
import hashlib
import io
import json
import os
import platform
import signal
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
from pathlib import Path

__all__ = ["KUBECTL_VERSION", "KUBECTL_SHA256", "source_closure", "source_configmap", "data606_rbac",
           "observe_rbac", "cleanup_names", "render_data606", "render_observe", "placement", "main"]

BENCH_PKG = Path(__file__).resolve().parent
NAMESPACE = "bench-loadgen"
IMAGE = "python:3.12-slim"                # the image every in-cluster bench Job runs (load606_job.IMAGE)
POOL_LABEL, POOL = "bench.krateo.io/pool", "loadgen"
TAINT = {"key": "dedicated", "operator": "Equal", "value": "loadgen", "effect": "NoSchedule"}

# the laptop's kubectl (`kubectl version --client`, run 23 and 24), from its release, by digest
KUBECTL_VERSION = "v1.36.2"
KUBECTL_SHA256 = {"amd64": "1e9045ec32bea85da43de85f0065358529ea7c7a152eca78154fba5b58c27d82",
                  "arm64": "c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e"}
KUBECTL_URL = "https://dl.k8s.io/release/{version}/bin/linux/{arch}/kubectl"
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

SRC_CONFIGMAP = "bench-src"
SRC_ARCHIVE = "bench.tar.xz"
# what the pods run: `python3 -m bench data606` (bench/__main__.py), the samplers and the log stream
ENTRY_MODULES = ("__main__", "incluster", "sampler", "logscrub", "identity")
LOG_STREAM_SCRIPT = "log_stream.sh"

DATA_JOB, OBSERVE_JOB = "data606", "observe606"
DATA_SA, OBSERVE_SA = "bench-data606", "bench-observe606"
AUTHN_NS = "krateo-system"
# the observer: small, and LIMITED on CPU, so it never takes the CPU the browser measures with
OBSERVE_CPU, OBSERVE_MEMORY, OBSERVE_CPU_LIMIT = "500m", "1Gi", "1"
DATA_MEMORY = "8Gi"

OUT = "/out"
CTL = "ctl"                     # /out/ctl/<segment>: a sampler segment starts (run606 observer_signal)
STOP, EXITED, COLLECTED, DONE = "stop", "exited", "collected", "done"


# ─── the bench code the pods run ─────────────────────────────────────────────────────────────────

def _imports(path: Path) -> set[str]:
    """Every bench module the file imports, at module level or inside a function."""
    out: set[str] = set()
    for n in ast.walk(ast.parse(path.read_text(), str(path))):
        if isinstance(n, ast.ImportFrom) and n.module:
            if n.module == "bench":
                out |= {a.name for a in n.names}
            elif n.module.startswith("bench."):
                out.add(n.module.split(".")[1])
        elif isinstance(n, ast.Import):
            out |= {a.name.split(".")[1] for a in n.names if a.name.startswith("bench.")}
    return {m for m in out if (path.parent / f"{m}.py").is_file()}


def source_closure(pkg: Path = BENCH_PKG, entries=ENTRY_MODULES) -> list[str]:
    """The files the pods need: the entry modules and everything they import, transitively."""
    seen: set[str] = set()
    todo = list(entries)
    while todo:
        m = todo.pop()
        if m not in seen:
            seen.add(m)
            todo.extend(_imports(pkg / f"{m}.py"))
    return sorted(f"{m}.py" for m in seen | {"__init__"}) + [LOG_STREAM_SCRIPT]


def source_archive(pkg: Path = BENCH_PKG) -> bytes:
    """The closure as one reproducible tar.xz (bench/<file>, fixed mtime and owner)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:xz") as tf:
        for name in source_closure(pkg):
            data = (pkg / name).read_bytes()
            info = tarfile.TarInfo(f"bench/{name}")
            info.size, info.mtime, info.mode = len(data), 0, 0o755 if name.endswith(".sh") else 0o644
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def source_configmap(pkg: Path = BENCH_PKG) -> dict:
    archive = source_archive(pkg)
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": SRC_CONFIGMAP, "namespace": NAMESPACE,
                         "annotations": {"bench.krateo.io/sha256": hashlib.sha256(archive).hexdigest()}},
            "binaryData": {SRC_ARCHIVE: base64.b64encode(archive).decode()}}


# ─── RBAC: exactly what each Job does ────────────────────────────────────────────────────────────

def _rule(groups, resources, verbs, names=None) -> dict:
    r = {"apiGroups": list(groups), "resources": list(resources), "verbs": list(verbs)}
    if names:
        r["resourceNames"] = list(names)
    return r


def data606_rbac() -> tuple[list[dict], list[dict]]:
    """(cluster rules, krateo-system rules) of the data606 ServiceAccount, one per call data606 makes
    (bench/data606.py generate and what it calls). The reader rules the bench's Roles grant
    (rbac_model.READER_RULES) are HELD, so the ServiceAccount may create those Roles and bind them
    without `escalate` or `bind`."""
    from bench import data_manifests as dm
    from bench import rbac_model
    held = [_rule(g, r, v) for g, r, v in rbac_model.READER_RULES]
    cluster = held + [
        # create_bench_namespaces (server-side apply), wait_for_bench_namespaces (count_bench_ns)
        _rule([""], ["namespaces"], ["get", "list", "create", "patch"]),
        # assert_no_external_children: is each child kind installed (cluster._crd_exists), then list it
        _rule(["apiextensions.k8s.io"], ["customresourcedefinitions"], ["get"],
              ["repoes.git.krateo.io", "applications.argoproj.io"]),
        _rule(["git.krateo.io"], ["repoes"], ["list"]),
        _rule(["argoproj.io"], ["applications"], ["list"]),
        # deploy_compositiondefinition (apply, then its Ready condition), hold_composition_controller
        # (annotate paused; scale its controller to 0)
        _rule(["core.krateo.io"], ["compositiondefinitions"], ["get", "create", "patch"]),
        _rule(["apps"], ["deployments"], ["get"]),
        _rule(["apps"], ["deployments/scale"], ["get", "patch", "update"]),
        # deploy_compositions_parallel (apply); count_compositions lists (held: READER_RULES)
        _rule([dm.COMP_GVR], [dm.COMP_RES], ["create", "patch"]),
        # the teams' Roles + RoleBindings, the admins' ClusterRole + binding (rbac_model.rbac_yaml)
        _rule(["rbac.authorization.k8s.io"], ["roles", "rolebindings", "clusterroles", "clusterrolebindings"],
              ["create", "patch"]),
        # the freshness fixtures (load606.list_dep_fixture_yaml / list_dep_status_fixture_yaml)
        _rule(["templates.krateo.io"], ["restactions"], ["create", "patch"]),
    ]
    from bench import authn_user
    users_group = authn_user.API_VERSION.split("/")[0]
    authn = [_rule([""], ["secrets"], ["create", "patch"]),         # each user's password Secret
             _rule([users_group], ["users"], ["create", "patch"])]  # each authn User
    return cluster, authn


def observe_rbac() -> tuple[list[dict], list[dict]]:
    """(cluster rules, krateo-system rules) of the observer: the API server's /metrics (apf); snowplow's
    pods, their logs and its Deployment (the log stream); their metrics (top)."""
    cluster = [{"nonResourceURLs": ["/metrics"], "verbs": ["get"]}]
    ns = [_rule([""], ["pods"], ["get", "list"]),
          _rule([""], ["pods/log"], ["get"]),
          _rule(["apps"], ["deployments"], ["get"]),
          _rule(["metrics.k8s.io"], ["pods"], ["list"])]
    return cluster, ns


def _rbac_objects(sa: str, cluster_rules: list[dict], ns_rules: list[dict]) -> list[dict]:
    subj = [{"kind": "ServiceAccount", "name": sa, "namespace": NAMESPACE}]
    meta = {"name": sa, "labels": {"app": "scaletest", "bench.krateo.io/run-sa": sa}}
    return [
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {**meta, "namespace": NAMESPACE}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": meta,
         "rules": cluster_rules},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": meta,
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": sa},
         "subjects": subj},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {**meta, "namespace": AUTHN_NS},
         "rules": ns_rules},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
         "metadata": {**meta, "namespace": AUTHN_NS},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": sa}, "subjects": subj},
    ]


CLEANUP = {DATA_JOB: DATA_SA, OBSERVE_JOB: OBSERVE_SA}


def cleanup_names(job: str) -> list[tuple[str, list[str]]]:
    """Everything run606 created for one Job, as (namespace or "-" for cluster-scoped, [kind/name]):
    the Job, its ServiceAccount and RBAC (and the observer's Secret). Deleted when the Job is done
    and again at teardown."""
    sa = CLEANUP[job]
    loadgen = [f"job/{job}", f"serviceaccount/{sa}"] + ([f"secret/{OBSERVE_JOB}"] if job == OBSERVE_JOB else [])
    return [(NAMESPACE, loadgen), (AUTHN_NS, [f"rolebinding/{sa}", f"role/{sa}"]),
            ("-", [f"clusterrolebinding/{sa}", f"clusterrole/{sa}"])]


# ─── the Jobs ────────────────────────────────────────────────────────────────────────────────────

def _bootstrap(context: str) -> str:
    """The init container: the bench code out of its ConfigMap, then kubectl and the in-pod
    kubeconfig (`tools`)."""
    return ("set -eu\n"
            "python3 -c 'import sys, tarfile; tarfile.open(sys.argv[1]).extractall(sys.argv[2], filter=\"data\")' "
            f"/src/{SRC_ARCHIVE} /opt/bench\n"
            f"PYTHONPATH=/opt/bench exec python3 -m bench.incluster tools --dest /tools --context {json.dumps(context)}\n")


def _pod_spec(*, sa: str, context: str, container: dict) -> dict:
    env = [{"name": "PATH", "value": "/tools:/usr/local/bin:/usr/bin:/bin"},
           {"name": "KUBECONFIG", "value": "/tools/kubeconfig"},
           {"name": "BENCH_GKE_CONTEXT", "value": context},
           {"name": "PYTHONPATH", "value": "/opt/bench"},
           {"name": "PYTHONUNBUFFERED", "value": "1"},
           {"name": "HOME", "value": "/work"}]
    mounts = [{"name": "bench", "mountPath": "/opt/bench", "readOnly": True},
              {"name": "tools", "mountPath": "/tools", "readOnly": True},
              {"name": "out", "mountPath": OUT}, {"name": "work", "mountPath": "/work"}]
    container = {**container, "image": IMAGE, "env": env + container.get("env", []),
                 "volumeMounts": mounts, "workingDir": "/work"}
    return {
        "serviceAccountName": sa, "automountServiceAccountToken": True, "restartPolicy": "Never",
        "nodeSelector": {POOL_LABEL: POOL}, "tolerations": [TAINT],
        "initContainers": [{
            "name": "tools", "image": IMAGE, "command": ["sh", "-c", _bootstrap(context)],
            "env": [{"name": "HOME", "value": "/work"}],
            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
            "volumeMounts": [{"name": "src", "mountPath": "/src", "readOnly": True},
                             {"name": "bench", "mountPath": "/opt/bench"},
                             {"name": "tools", "mountPath": "/tools"}, {"name": "work", "mountPath": "/work"}]}],
        "containers": [container],
        "volumes": [{"name": "src", "configMap": {"name": SRC_CONFIGMAP}},
                    {"name": "bench", "emptyDir": {}}, {"name": "tools", "emptyDir": {}},
                    {"name": "out", "emptyDir": {}}, {"name": "work", "emptyDir": {}}],
    }


def _job(name: str, sa: str, spec: dict) -> dict:
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"app": name}},
            "spec": {"backoffLimit": 0, "template": {"metadata": {"labels": {"app": name}}, "spec": spec}}}


def _namespace() -> dict:
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE, "labels": {"app": "scaletest"}}}


def data606_requests(machine: str) -> tuple[str, str]:
    """data606's (cpu, memory) requests: the loadgen node's whole budget (allocatable minus the
    headroom kept for its daemonsets, k6_job.loadgen_fit) but the observer's, which shares the node
    with it; nothing else runs there during the data step (the drivers come after it)."""
    from bench import k6_job
    if machine not in k6_job.LOADGEN_ALLOCATABLE:
        raise ValueError(f"unknown loadgen machine type {machine!r}: known {sorted(k6_job.LOADGEN_ALLOCATABLE)}")
    budget = k6_job.LOADGEN_ALLOCATABLE[machine]["cpu_m"] - k6_job.LOADGEN_HEADROOM["cpu_m"]
    cpu = f"{budget - k6_job._mcpu(OBSERVE_CPU)}m"
    k6_job.loadgen_fit(machine, [(cpu, DATA_MEMORY), (OBSERVE_CPU, OBSERVE_MEMORY)])
    return cpu, DATA_MEMORY


def render_data606(*, context: str, timeout_s: int, linger_s: int, machine: str,
                   data_args: list[str]) -> list[dict]:
    """The data606 Job: `python3 -m bench data606 <data_args>` bounded by timeout_s, under its own
    ServiceAccount, on the loadgen node. Its pod turns Ready once /out/done holds data606's exit
    code, and stays up (at most linger_s) for run606 to copy /out."""
    cpu, mem = data606_requests(machine)
    cluster, authn = data606_rbac()
    container = {
        "name": "data606",
        "command": ["python3", "-m", "bench.incluster", "data606", "--out", OUT, "--timeout-s", str(timeout_s),
                    "--linger-s", str(linger_s), "--", *data_args],
        "resources": {"requests": {"cpu": cpu, "memory": mem}},
        "readinessProbe": {"exec": {"command": ["test", "-f", f"{OUT}/{DONE}"]}, "periodSeconds": 5},
    }
    return ([_namespace(), source_configmap()] + _rbac_objects(DATA_SA, cluster, authn)
            + [_job(DATA_JOB, DATA_SA, _pod_spec(sa=DATA_SA, context=context, container=container))])


def render_observe(*, context: str, every_s: float, retry_s: float, selector: str, user_prefix: str,
                   password: str, linger_s: int) -> list[dict]:
    """The observer Job (see the module doc). The password the scrubber denies comes from a Secret
    (stringData: run606's kapply redacts the manifest on disk, as every bench manifest)."""
    cluster, ns = observe_rbac()
    secret = {"apiVersion": "v1", "kind": "Secret",
              "metadata": {"name": OBSERVE_JOB, "namespace": NAMESPACE}, "stringData": {"deny": password}}
    container = {
        "name": "observe",
        "command": ["python3", "-m", "bench.incluster", "observe", "--out", OUT, "--every-s", f"{every_s:g}",
                    "--selector", selector, "--linger-s", str(linger_s)],
        "env": [{"name": "PW", "valueFrom": {"secretKeyRef": {"name": OBSERVE_JOB, "key": "deny"}}},
                {"name": "BENCH_USER_PREFIX", "value": user_prefix},
                {"name": "SNOWPLOW_POD_SELECTOR", "value": selector},
                {"name": "LOG_STREAM_RETRY_S", "value": f"{retry_s:g}"}],
        "resources": {"requests": {"cpu": OBSERVE_CPU, "memory": OBSERVE_MEMORY},
                      "limits": {"cpu": OBSERVE_CPU_LIMIT, "memory": OBSERVE_MEMORY}},
    }
    return ([_namespace(), source_configmap()] + _rbac_objects(OBSERVE_SA, cluster, ns)
            + [secret, _job(OBSERVE_JOB, OBSERVE_SA, _pod_spec(sa=OBSERVE_SA, context=context, container=container))])


def manifest(objs: list[dict]) -> str:
    """kubectl's input: one JSON document per object (JSON is YAML)."""
    return "".join("---\n" + json.dumps(o, indent=1) + "\n" for o in objs)


# ─── what ran where (run606 records it, and fails the step when it is not the loadgen node) ─────

def placement(job: str, pods: dict, nodes: dict, want: dict | None = None) -> dict:
    """The Job's pod: its node, that node's pool, and the requests it runs with against `want`
    (the rendered Job's container requests). ok only on the loadgen pool with exactly those."""
    items = [p for p in pods.get("items") or [] if not (p.get("metadata") or {}).get("deletionTimestamp")]
    rec = {"kind": "incluster_placement", "job": job, "t": round(time.time(), 3)}
    if len(items) != 1:
        return {**rec, "ok": False, "why": f"{len(items)} live pod(s) of job {job}, want 1"}
    pod = items[0]
    node = (pod.get("spec") or {}).get("nodeName")
    labels = next(((n.get("metadata") or {}).get("labels") or {} for n in nodes.get("items") or []
                   if (n.get("metadata") or {}).get("name") == node), None)
    reqs = {c["name"]: ((c.get("resources") or {}).get("requests") or {}) for c in pod["spec"]["containers"]}
    rec.update({"pod": pod["metadata"]["name"], "node": node,
                "node_pool": None if labels is None else labels.get(POOL_LABEL),
                "machine_type": None if labels is None else labels.get("node.kubernetes.io/instance-type"),
                "requests": reqs})
    if not node:
        return {**rec, "ok": False, "why": "the pod is not scheduled"}
    if labels is None:
        return {**rec, "ok": False, "why": f"node {node} is not in the node list"}
    if labels.get(POOL_LABEL) != POOL:
        return {**rec, "ok": False, "why": f"node {node} is in pool {labels.get(POOL_LABEL)!r}, not {POOL!r}"}
    if want is not None and reqs != want:
        return {**rec, "ok": False, "why": f"requests {reqs} != rendered {want}"}
    return {**rec, "ok": True, "why": f"on node {node} (pool {POOL}), requests {reqs}"}


def rendered_requests(objs: list[dict], job: str) -> dict:
    j = next(o for o in objs if o["kind"] == "Job" and o["metadata"]["name"] == job)
    return {c["name"]: c["resources"]["requests"] for c in j["spec"]["template"]["spec"]["containers"]}


# ─── in the pod ──────────────────────────────────────────────────────────────────────────────────

def tools(dest: str, context: str, *, fetch=urllib.request.urlopen, machine=platform.machine,
          env=os.environ) -> None:
    """The init container: kubectl (KUBECTL_VERSION, verified against KUBECTL_SHA256) and a kubeconfig
    naming the context `context` that authenticates with this pod's own ServiceAccount token
    (tokenFile: the kubelet keeps it current) against the in-cluster API server."""
    arch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(machine())
    if arch is None:
        raise SystemExit(f"tools: no pinned kubectl for this node's architecture {machine()!r}")
    want = KUBECTL_SHA256[arch]
    url = KUBECTL_URL.format(version=KUBECTL_VERSION, arch=arch)
    os.makedirs(dest, exist_ok=True)
    part, last = os.path.join(dest, "kubectl.part"), None
    for attempt in range(1, 4):
        try:
            h = hashlib.sha256()
            with fetch(url, timeout=120) as r, open(part, "wb") as fh:
                while chunk := r.read(1 << 20):
                    h.update(chunk)
                    fh.write(chunk)
            if h.hexdigest() != want:
                raise ValueError(f"sha256 {h.hexdigest()} != pinned {want}")
            break
        except Exception as e:  # noqa: BLE001 — retried, then said
            last = f"{type(e).__name__}: {e}"
            print(f"tools: kubectl {KUBECTL_VERSION} {arch} attempt {attempt}/3 failed: {last}", flush=True)
            time.sleep(5 * attempt)
    else:
        raise SystemExit(f"tools: no verified kubectl {KUBECTL_VERSION} ({last})")
    os.chmod(part, 0o755)
    os.replace(part, os.path.join(dest, "kubectl"))
    server = f"https://{env['KUBERNETES_SERVICE_HOST']}:{env.get('KUBERNETES_SERVICE_PORT', '443')}"
    cfg = {"apiVersion": "v1", "kind": "Config", "current-context": context,
           "clusters": [{"name": context, "cluster": {"server": server,
                                                      "certificate-authority": f"{SA_DIR}/ca.crt"}}],
           "users": [{"name": "serviceaccount", "user": {"tokenFile": f"{SA_DIR}/token"}}],
           "contexts": [{"name": context, "context": {"cluster": context, "user": "serviceaccount",
                                                      "namespace": NAMESPACE}}]}
    with open(os.path.join(dest, "kubeconfig"), "w") as fh:
        json.dump(cfg, fh)
    print(f"tools: kubectl {KUBECTL_VERSION} {arch} verified (sha256 {want}); kubeconfig context {context} "
          f"-> {server} as this pod's ServiceAccount", flush=True)


def _linger(out: Path, linger_s: float, sleep=time.sleep, clock=time.monotonic) -> None:
    """Stay up for run606's copy of /out: until it says /out/collected, at most linger_s."""
    end = clock() + linger_s
    while clock() < end and not (out / COLLECTED).exists():
        sleep(1)


def _kill_group(p: subprocess.Popen, grace_s: float = 10.0) -> None:
    for sig, wait in ((signal.SIGTERM, grace_s), (signal.SIGKILL, 5.0)):
        try:
            os.killpg(p.pid, sig)
        except (ProcessLookupError, PermissionError):
            return
        try:
            p.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def run_data606(out: str, timeout_s: float, linger_s: float, data_args: list[str],
                python: str = sys.executable, linger=_linger) -> int:
    """data606 as the laptop ran it, bounded by timeout_s (its whole process group stopped then:
    exit 124, as run606's `bounded` stopped it at 143). Its output goes to the container log and to
    /out/data606.log; its exit code to /out/done; then the pod lingers for the copy."""
    o = Path(out)
    o.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    p = subprocess.Popen([python, "-m", "bench", "data606", *data_args, "--out", str(o / "data-proof.json")],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         start_new_session=True)

    def tee():
        with open(o / "data606.log", "ab") as fh:
            for line in iter(p.stdout.readline, b""):
                fh.write(line)
                fh.flush()
                sys.stdout.buffer.write(line)
                sys.stdout.flush()

    t = threading.Thread(target=tee, daemon=True)
    t.start()
    timed_out = False
    try:
        rc = p.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        _kill_group(p)
        rc = 124
    t.join(timeout=30)
    run = {"kind": "data606_run", "t_start": round(t0, 3), "t_end": round(time.time(), 3), "rc": rc,
           "timed_out": timed_out, "timeout_s": timeout_s, "args": data_args}
    (o / "data606-run.json").write_text(json.dumps(run) + "\n")
    (o / DONE).write_text(f"{rc}\n")
    print(f"data606 exited rc={rc}{' (its bound reached)' if timed_out else ''}; holding for the copy", flush=True)
    linger(o, linger_s)
    return 0


class Segment:
    """One stream of the observer: its program runs from its start file (None: at once) until its
    stop file or the observer's stop, in a process group of its own."""

    def __init__(self, name: str, argv: list[str], *, start: str | None, stop: str | None,
                 stdout: str | None, stderr: str, env: dict | None = None, on_stop: str | None = None):
        self.name, self.argv, self.start, self.stop = name, argv, start, stop
        self.stdout, self.stderr, self.env, self.on_stop = stdout, stderr, env, on_stop
        self.proc: subprocess.Popen | None = None
        self.record: dict = {"segment": name, "started": None, "stopped": None, "rc": None}

    def launch(self, out: Path) -> None:
        so = open(out / self.stdout, "ab") if self.stdout else subprocess.DEVNULL
        se = open(out / self.stderr, "ab")
        self.proc = subprocess.Popen(self.argv, stdout=so, stderr=se, stdin=subprocess.DEVNULL,
                                     start_new_session=True, env={**os.environ, **(self.env or {})})
        self.record["started"] = round(time.time(), 3)

    def halt(self, out: Path) -> None:
        if self.proc is None or self.record["stopped"] is not None:
            return
        if self.on_stop:
            (out / self.on_stop).touch()         # the log stream's own stop file, before its group's TERM
        _kill_group(self.proc)
        self.record.update(stopped=round(time.time(), 3), rc=self.proc.poll())


def observe_segments(out: str, every_s: float, selector: str) -> list[Segment]:
    """apf, the log stream (both at once), top_boot (from ctl/top_boot to ctl/top) and top (from
    ctl/top): each run606's own sampler program, each file as run606 named it."""
    k = ["kubectl", f"--request-timeout={every_s:g}s"]
    sampler = [sys.executable, "-m", "bench.sampler", "--every-s", f"{every_s:g}"]
    top = ["-n", "krateo-system", "top", "pod", "-l", selector, "--no-headers"]
    return [
        Segment("apf", sampler + ["--kind", "apf", "--out", f"{out}/apf.jsonl.gz", "--", *k, "get", "--raw=/metrics"],
                start=None, stop=None, stdout=None, stderr="sampler-apf.err"),
        Segment("stream", ["bash", str(BENCH_PKG / LOG_STREAM_SCRIPT)], start=None, stop=None, stdout=None,
                stderr="snowplow-stream.err", env={"OUT": out}, on_stop=".log-stream-stop"),
        Segment("top_boot", sampler + ["--kind", "top_boot", "--out", f"{out}/top-boot.jsonl", "--", *k, *top],
                start=f"{CTL}/top_boot", stop=f"{CTL}/top", stdout=None, stderr="sampler-top_boot.err"),
        Segment("top", sampler + ["--kind", "top", "--out", f"{out}/top.jsonl", "--", *k, *top],
                start=f"{CTL}/top", stop=None, stdout=None, stderr="sampler-top.err"),
    ]


def supervise(out: str, segments: list[Segment], linger_s: float, poll_s: float = 0.5,
              linger=_linger) -> int:
    """Run each segment between its start and stop files; on /out/stop, stop them all, write
    observe606.json (each segment's start, stop and exit) and /out/exited, then linger."""
    o = Path(out)
    (o / CTL).mkdir(parents=True, exist_ok=True)
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    while not stopping.is_set():
        halt_all = (o / STOP).exists()
        for s in segments:
            if halt_all or (s.stop and (o / s.stop).exists()):
                s.halt(o)
            elif s.proc is None and (s.start is None or (o / s.start).exists()):
                s.launch(o)
        if halt_all:
            break
        time.sleep(poll_s)
    for s in segments:
        s.halt(o)
    (o / "observe606.json").write_text(json.dumps({"kind": "observe606", "t": round(time.time(), 3),
                                                   "segments": [s.record for s in segments]}) + "\n")
    (o / EXITED).touch()
    if not stopping.is_set():            # a TERM (the Job deleted): nothing will copy /out any more
        linger(o, linger_s)
    return 0


# ─── the CLI ─────────────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    rest: list[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, rest = argv[:i], argv[i + 1:]
    p = argparse.ArgumentParser(prog="bench.incluster")
    sub = p.add_subparsers(dest="cmd", required=True)
    rd = sub.add_parser("render-data606", help="the data606 Job and its ServiceAccount (stdout); data606's "
                                               "arguments after --")
    rd.add_argument("--context", required=True)
    rd.add_argument("--timeout-s", type=int, required=True)
    rd.add_argument("--linger-s", type=int, required=True)
    rd.add_argument("--machine", required=True, help="the loadgen node's machine type")
    ro = sub.add_parser("render-observe", help="the observer Job and its ServiceAccount (stdout)")
    ro.add_argument("--context", required=True)
    ro.add_argument("--every-s", type=float, required=True)
    ro.add_argument("--retry-s", type=float, required=True)
    ro.add_argument("--selector", required=True)
    ro.add_argument("--user-prefix", required=True)
    ro.add_argument("--password", required=True)
    ro.add_argument("--linger-s", type=int, required=True)
    rq = sub.add_parser("requests", help="a rendered Job's container requests, as JSON")
    rq.add_argument("--manifest", required=True)
    rq.add_argument("--job", required=True)
    pl = sub.add_parser("placement", help="one incluster_placement record; exit 1 unless on the loadgen pool "
                                          "with the rendered requests")
    pl.add_argument("--job", required=True)
    pl.add_argument("--pods", required=True)
    pl.add_argument("--nodes", required=True)
    pl.add_argument("--manifest", required=True, help="the rendered manifest: the requests it must run with")
    rb = sub.add_parser("cleanup-names", help="what run606 deletes for a Job: one line per namespace "
                                              "('-': cluster-scoped), then kind/name ...")
    rb.add_argument("--job", required=True, choices=sorted(CLEANUP))
    sub.add_parser("closure", help="the files shipped to the pods")
    t = sub.add_parser("tools", help="in the pod: kubectl and the pod's kubeconfig")
    t.add_argument("--dest", required=True)
    t.add_argument("--context", required=True)
    d = sub.add_parser("data606", help="in the pod: data606, bounded, then hold for the copy")
    d.add_argument("--out", required=True)
    d.add_argument("--timeout-s", type=float, required=True)
    d.add_argument("--linger-s", type=float, required=True)
    ob = sub.add_parser("observe", help="in the pod: the samplers and the log stream, until /out/stop")
    ob.add_argument("--out", required=True)
    ob.add_argument("--every-s", type=float, required=True)
    ob.add_argument("--selector", required=True)
    ob.add_argument("--linger-s", type=float, required=True)
    a = p.parse_args(argv)
    if a.cmd == "render-data606":
        sys.stdout.write(manifest(render_data606(context=a.context, timeout_s=a.timeout_s, linger_s=a.linger_s,
                                                 machine=a.machine, data_args=rest)))
    elif a.cmd == "render-observe":
        sys.stdout.write(manifest(render_observe(context=a.context, every_s=a.every_s, retry_s=a.retry_s,
                                                 selector=a.selector, user_prefix=a.user_prefix,
                                                 password=a.password, linger_s=a.linger_s)))
    elif a.cmd == "requests":
        print(json.dumps(rendered_requests(_load_manifest(a.manifest), a.job), sort_keys=True))
    elif a.cmd == "placement":
        rec = placement(a.job, json.load(open(a.pods)), json.load(open(a.nodes)),
                        rendered_requests(_load_manifest(a.manifest), a.job))
        print(json.dumps(rec))
        return 0 if rec["ok"] else 1
    elif a.cmd == "cleanup-names":
        for ns, names in cleanup_names(a.job):
            print(ns, *names)
    elif a.cmd == "closure":
        print("\n".join(source_closure()))
    elif a.cmd == "tools":
        tools(a.dest, a.context)
    elif a.cmd == "data606":
        return run_data606(a.out, a.timeout_s, a.linger_s, rest)
    elif a.cmd == "observe":
        return supervise(a.out, observe_segments(a.out, a.every_s, a.selector), a.linger_s)
    return 0


def _load_manifest(path: str) -> list[dict]:
    """A manifest render_* wrote (JSON documents; kapply may have redacted a value in place)."""
    text = open(path).read()
    return [json.loads(doc) for doc in text.split("---\n") if doc.strip()]


if __name__ == "__main__":
    sys.exit(main())
