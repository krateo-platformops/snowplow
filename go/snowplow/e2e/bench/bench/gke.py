"""Temporary dedicated GKE cluster lifecycle for the #606 production-scale bench.

Owner ruling (Diego, 2026-10-13): the 50K bench runs on a TEMPORARY dedicated
GKE cluster created per run and torn down afterwards. NEVER on krateo-057.

This module owns that lifecycle and nothing else:

  - `default_spec()` / `create_commands()` — the exact gcloud argv for the
    cluster and its node pools, labelled with an owner, the issue and a TTL.
    From run 17 the control plane is REGIONAL (3 replicas; run 16 attempt 2
    lost B's zonal one for 5+ minutes) while every node stays in the spec's
    zone (--node-locations); every cluster call names the region, and the
    kube context is gke_<project>_<region>_<name>;
  - `delete_command()` + `assert_deletable()` — teardown, which REFUSES any
    cluster that does not carry the bench labels and the bench name prefix,
    so a mistyped name can never delete 057 or the release cluster;
  - `expired_clusters()` — the reaper's selection: bench-labelled clusters
    whose `ttl-expires` epoch is in the past;
  - `run_with_teardown()` — try/finally + SIGTERM/SIGHUP/SIGINT handling, so
    the body's exit path ALWAYS reaches teardown;
  - `self_destruct_manifest()` — an in-cluster CronJob that deletes the
    cluster once its TTL passes, the only layer that survives the operator's
    laptop dying mid-run;
  - `resolve_endpoints()` — SNOWPLOW_URL / AUTHN_URL / FRONTEND_URL read from
    the new cluster's Services (LB IPs are new every run, and the harness's
    baked defaults point at a cluster deleted 2026-06-14);
  - PVC disks (#895): gke-up refuses a run whose PVC disks do not fit the
    region's SSD_TOTAL_GB; teardown releases every PVC (the CSI driver
    deletes its disk) before the cluster, then deletes what is left of THIS
    cluster by exact name (`backstop_disks`). GKE itself labels each PVC disk
    with the cluster's resource labels, name and id.

Every command is built as an argv list and only EXECUTED by `execute()`,
which honours dry-run. The unit tests drive the builders and never shell out.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import signal
import subprocess
import sys
import time
from typing import Callable, Iterable

__all__ = [
    "BENCH_NAME_PREFIX",
    "BENCH_LABELS",
    "FORBIDDEN_CLUSTER_NAMES",
    "NodePool",
    "ClusterSpec",
    "default_spec",
    "kube_context",
    "create_commands",
    "delete_command",
    "assert_deletable",
    "expired_clusters",
    "reap_commands",
    "resolve_endpoints",
    "self_destruct_manifest",
    "run_with_teardown",
    "execute",
    "sanitize_label_value",
    "NotDeletable",
]

# ─── Identity of a bench cluster ────────────────────────────────────────────

BENCH_NAME_PREFIX = "snowplow-bench-"

# Labels every bench cluster carries. `assert_deletable` requires ALL of them,
# so teardown and the reaper can only ever select clusters this module made.
BENCH_LABELS = {
    "purpose": "snowplow-bench",
    "issue": "606",
    "created-by": "bench-gke",
}
TTL_LABEL = "ttl-expires"     # unix epoch seconds, as a decimal string
OWNER_LABEL = "owner"

# Clusters the bench must never create, touch or delete, whatever the labels
# say. krateo-057 is demo-reserved (owner ruling 2026-10-13); the installer
# clusters are the live deploy target (west4) and frozen evidence (west3).
FORBIDDEN_CLUSTER_NAMES = (
    "krateo-057",
    "krateo-installer-release",
    "krateo-installer-test",
)

DEFAULT_PROJECT = "operations-dev-krateo-io"
DEFAULT_ZONE = "europe-west4-a"
# The task pins Kubernetes 1.36 to match 057. gcloud resolves a minor-only
# version to the newest patch the zone offers; `create_commands` does not pin a
# patch because GKE retires patches on its own schedule.
DEFAULT_CLUSTER_VERSION = "1.36"
DEFAULT_TTL_HOURS = 8


class NotDeletable(RuntimeError):
    """Raised when teardown is asked to delete a cluster it does not own."""


_LABEL_VALUE_RE = re.compile(r"[^a-z0-9_-]+")


def sanitize_label_value(value: str) -> str:
    """GCP label values: lowercase letters, digits, `_` and `-`, <= 63 chars."""
    v = _LABEL_VALUE_RE.sub("-", (value or "").strip().lower()).strip("-")
    return (v or "unknown")[:63]


@dataclasses.dataclass(frozen=True)
class NodePool:
    name: str
    machine_type: str
    count: int
    # (key, value, effect) — a dedicated pool keeps neighbours off the node
    # whose CPU we measure, and keeps the load generator off snowplow's node.
    taint: tuple[str, str, str] | None = None
    labels: tuple[tuple[str, str], ...] = ()
    disk_gb: int = 100


@dataclasses.dataclass(frozen=True)
class ClusterSpec:
    name: str
    project: str = DEFAULT_PROJECT
    zone: str = DEFAULT_ZONE
    cluster_version: str = DEFAULT_CLUSTER_VERSION
    owner: str = "unknown"
    created_at: int = 0          # epoch seconds
    ttl_hours: int = DEFAULT_TTL_HOURS
    # The default pool runs the Krateo platform (portal, authn, frontend,
    # core-provider, composition controllers, observability).
    platform_pool: NodePool = NodePool(
        "default-pool", "n2-standard-8", 3,
        labels=(("bench.krateo.io/pool", "platform"),))
    extra_pools: tuple[NodePool, ...] = ()
    kubeconfig: str = ""

    @property
    def ttl_expires(self) -> int:
        return int(self.created_at) + int(self.ttl_hours) * 3600

    @property
    def region(self) -> str:
        """Where the control plane runs: a REGIONAL cluster (3 control-plane
        replicas; run 16 attempt 2 lost B's zonal one for 5+ minutes). Every
        node stays in `zone` (--node-locations)."""
        return region_of(self.zone)

    def labels(self) -> dict[str, str]:
        out = dict(BENCH_LABELS)
        out[OWNER_LABEL] = sanitize_label_value(self.owner)
        out[TTL_LABEL] = str(self.ttl_expires)
        return out


def region_of(zone: str) -> str:
    """europe-west4-a -> europe-west4 (a region is returned as is)."""
    return zone.rsplit("-", 1)[0] if _is_zone(zone) else zone


ZONE_PATTERN = r"[a-z]+-[a-z]+\d+-[a-z]"      # run606.sh checks ZONE against the same


def _is_zone(location: str) -> bool:
    return bool(re.fullmatch(ZONE_PATTERN, location or ""))


def check_zone(zone: str) -> str:
    """The node zone, e.g. europe-west4-a. A REGION here (europe-west4) would
    put every node in --node-locations europe-west4 and make the region
    europe-west4 itself: refused at once instead (#899 F2)."""
    if not _is_zone(zone):
        raise ValueError(f"zone {zone!r} is not a zone (e.g. europe-west4-a): every node goes in "
                         f"this zone and the control plane in its region")
    return zone


def _location_flag(location: str) -> list[str]:
    """`--zone` for a zonal cluster's location, `--region` for a regional
    one's (gke-reap still meets zonal bench clusters made before run 17)."""
    return ["--zone" if _is_zone(location) else "--region", location]


def _check_name(name: str) -> None:
    if name in FORBIDDEN_CLUSTER_NAMES:
        raise NotDeletable(f"cluster {name!r} is forbidden for the bench")
    if not name.startswith(BENCH_NAME_PREFIX):
        raise NotDeletable(
            f"cluster {name!r} does not start with {BENCH_NAME_PREFIX!r}; "
            f"the bench only creates/deletes its own clusters")
    # GKE: lowercase, digits, hyphens; starts with a letter; <= 40 chars.
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,38}[a-z0-9]", name):
        raise NotDeletable(f"cluster name {name!r} is not a valid GKE name")


# The load generator's node. n2-standard-8 for the Python driver of runs
# 1-10; run 11's k6 + real-Chrome plan needs ~8.35 CPU of requests, which only
# an n2-standard-16 holds with headroom (#830; bench/k6_job.loadgen_fit).
DEFAULT_LOADGEN_MACHINE = "n2-standard-8"


def default_spec(name: str | None = None, *, now: float | None = None,
                 owner: str | None = None, ttl_hours: int = DEFAULT_TTL_HOURS,
                 project: str = DEFAULT_PROJECT, zone: str = DEFAULT_ZONE,
                 cluster_version: str = DEFAULT_CLUSTER_VERSION,
                 kubeconfig_dir: str | None = None,
                 loadgen_machine_type: str = DEFAULT_LOADGEN_MACHINE) -> ClusterSpec:
    """The #606 cluster shape. See the run plan for the sizing rationale.

    - platform pool: 3 x n2-standard-8 (the reference release cluster runs the
      full platform on 3);
    - snowplow pool: 1 x n2-standard-8, tainted, so snowplow's CPU is measured
      without neighbours (057 steady state is 3.2-3.9 cores of a 4-core limit);
    - loadgen pool: 1 x `loadgen_machine_type` (n2-standard-8; n2-standard-16
      for run 11's k6 + Chrome, #830), tainted, for the in-cluster load
      driver, so 1000 virtual users never share a node with what they measure.

    Quota-shaped (read 2026-10-14, operations-dev-krateo-io / europe-west4):
    E2_CPUS limit is 24, so every pool is N2 (N2_CPUS 200); SSD_TOTAL_GB has
    ~133 GB free, so node disks are pd-standard (DISKS_TOTAL_GB 4096);
    IN_USE_ADDRESSES is 16 with 3 used, so 5 nodes + ~5 LoadBalancers fit.
    With an n2-standard-16 loadgen the cluster is 48 N2 vCPUs (40 before).
    """
    now = time.time() if now is None else now
    if name is None:
        name = BENCH_NAME_PREFIX + time.strftime("%m%d-%H%M", time.gmtime(now))
    _check_name(name)
    check_zone(zone)
    owner = owner or os.environ.get("USER", "unknown")
    kdir = kubeconfig_dir or os.path.expanduser("~/.kube/snowplow-bench")
    pools = (
        NodePool("snowplow", "n2-standard-8", 1,
                 taint=("dedicated", "snowplow", "NoSchedule"),
                 labels=(("bench.krateo.io/pool", "snowplow"),)),
        NodePool("loadgen", loadgen_machine_type, 1,
                 taint=("dedicated", "loadgen", "NoSchedule"),
                 labels=(("bench.krateo.io/pool", "loadgen"),)),
    )
    return ClusterSpec(
        name=name, project=project, zone=zone,
        cluster_version=cluster_version, owner=owner,
        created_at=int(now), ttl_hours=ttl_hours,
        extra_pools=pools,
        kubeconfig=os.path.join(kdir, f"{name}.kubeconfig"),
    )


def kube_context(spec: ClusterSpec) -> str:
    """The context name gcloud writes for the (regional) cluster."""
    return f"gke_{spec.project}_{spec.region}_{spec.name}"


DRY_SHAPE_SOURCE = "spec (dry run: nothing created, nothing observed)"


def cluster_shape(spec: ClusterSpec, source: str = DRY_SHAPE_SOURCE) -> dict:
    """The shape gke-up ASKS for (runs 14-16: zonal control plane; 17+:
    regional). What the run records is what GKE reports (`observed_shape`);
    this is only the dry run's record, and says so."""
    return {"kind": "cluster_shape", "source": source,
            "control_plane": "regional", "region": spec.region,
            "node_locations": [spec.zone], "cluster": spec.name,
            "node_pools": [{"name": p.name, "machine_type": p.machine_type, "nodes": p.count}
                           for p in (spec.platform_pool, *spec.extra_pools)]}


def observed_shape(describe: dict) -> dict:
    """The run's record of the cluster's shape, from `gcloud container
    clusters describe` AFTER create (#899 F1): where GKE put the control plane
    (`location`: a region → regional, a zone → zonal), where it put the nodes
    (`locations`) and how many there are (`currentNodeCount`) — not what the
    spec asked for."""
    loc = describe.get("location") or describe.get("zone") or ""
    return {"kind": "cluster_shape", "source": "gcloud container clusters describe, after create",
            "control_plane": "zonal" if _is_zone(loc) else ("regional" if loc else None),
            "location": loc or None, "region": region_of(loc) if loc else None,
            "node_locations": list(describe.get("locations") or []),
            "current_node_count": describe.get("currentNodeCount"),
            "cluster": describe.get("name"),
            "node_pools": [{"name": p.get("name"),
                            "machine_type": (p.get("config") or {}).get("machineType"),
                            "locations": list(p.get("locations") or []),
                            "nodes_per_location": p.get("initialNodeCount")}
                           for p in describe.get("nodePools") or []]}


def unobserved_shape(spec: ClusterSpec, why: str) -> dict:
    """The record when the describe after create did not answer: the shape is
    UNKNOWN (control_plane null), with what was asked for beside it."""
    return {"kind": "cluster_shape", "source": f"unobserved: {why}", "control_plane": None,
            "cluster": spec.name, "requested": cluster_shape(spec, "spec: what gke-up asked for")}


def _labels_arg(labels: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(labels.items()))


def _pool_flags(pool: NodePool) -> list[str]:
    flags = [
        "--machine-type", pool.machine_type,
        "--num-nodes", str(pool.count),
        "--disk-type", "pd-standard",
        "--disk-size", str(pool.disk_gb),
    ]
    if pool.taint:
        k, v, eff = pool.taint
        flags += ["--node-taints", f"{k}={v}:{eff}"]
    if pool.labels:
        flags += ["--node-labels", ",".join(f"{k}={v}" for k, v in pool.labels)]
    return flags


def create_commands(spec: ClusterSpec) -> list[list[str]]:
    """gcloud argv lists, in order: cluster, extra node pools, credentials.

    Credentials go to a DEDICATED kubeconfig (never merged into ~/.kube/config),
    so the run cannot flip anyone's current-context and no ambient context can
    leak into the run.
    """
    _check_name(spec.name)
    # REGIONAL control plane, every node in ONE zone: --num-nodes is per node
    # location, so with the one zone each pool keeps its count (no extra nodes)
    loc = ["--project", spec.project, "--region", spec.region]
    nodes = ["--node-locations", spec.zone]
    cmds = [[
        "gcloud", "container", "clusters", "create", spec.name, *loc, *nodes,
        "--cluster-version", spec.cluster_version,
        # REGULAR carries 1.36.4-gke.1391002, the exact version 057 runs
        # (rapid only offers newer 1.36 patches).
        "--release-channel", "regular",
        "--labels", _labels_arg(spec.labels()),
        "--scopes", "cloud-platform",
        "--enable-ip-alias",
        "--logging", "SYSTEM",
        "--monitoring", "SYSTEM",
        "--enable-autorepair",
        *_pool_flags(spec.platform_pool),
        "--quiet",
    ]]
    for pool in spec.extra_pools:
        cmds.append([
            "gcloud", "container", "node-pools", "create", pool.name,
            "--cluster", spec.name, *loc, *nodes, *_pool_flags(pool), "--quiet",
        ])
    cmds.append([
        "gcloud", "container", "clusters", "get-credentials", spec.name, *loc,
    ])
    return cmds


def delete_command(spec_or_name, *, project: str = DEFAULT_PROJECT,
                   location: str | None = None) -> list[str]:
    """`location`: the cluster's (a region, or a zone for a zonal cluster made
    before run 17); a spec's is its region."""
    if isinstance(spec_or_name, ClusterSpec):
        name, project, location = (spec_or_name.name, spec_or_name.project,
                                   spec_or_name.region)
    else:
        name = spec_or_name
    _check_name(name)
    return ["gcloud", "container", "clusters", "delete", name,
            "--project", project, *_location_flag(location or region_of(DEFAULT_ZONE)),
            "--quiet", "--async"]


def assert_deletable(describe: dict) -> None:
    """Refuse to delete unless `describe` (gcloud ... describe --format=json)
    is a cluster this module created: bench name prefix, not forbidden, and
    EVERY bench label present with its exact value.
    """
    name = describe.get("name", "")
    _check_name(name)
    labels = describe.get("resourceLabels") or {}
    missing = {k: v for k, v in BENCH_LABELS.items() if labels.get(k) != v}
    if missing:
        raise NotDeletable(
            f"cluster {name!r} lacks bench labels {missing}; refusing to delete")
    if TTL_LABEL not in labels:
        raise NotDeletable(f"cluster {name!r} lacks {TTL_LABEL}; refusing")


def expired_clusters(clusters: Iterable[dict], *, now: float) -> list[dict]:
    """Bench clusters (per `assert_deletable`) whose TTL epoch is < now."""
    out = []
    for c in clusters:
        try:
            assert_deletable(c)
        except NotDeletable:
            continue
        try:
            exp = int((c.get("resourceLabels") or {}).get(TTL_LABEL, ""))
        except ValueError:
            continue
        if exp < now:
            out.append(c)
    return out


def reap_commands(clusters: Iterable[dict], *, now: float,
                  project: str = DEFAULT_PROJECT) -> list[list[str]]:
    cmds = []
    for c in expired_clusters(clusters, now=now):
        # `location` is the cluster's own (a region now; a zone for an older one)
        loc = c.get("location") or c.get("zone") or region_of(DEFAULT_ZONE)
        cmds.append(delete_command(c["name"], project=project, location=loc))
    return cmds


def list_command(project: str = DEFAULT_PROJECT) -> list[str]:
    return ["gcloud", "container", "clusters", "list", "--project", project,
            "--filter", "resourceLabels.purpose=snowplow-bench",
            "--format", "json"]


def describe_command(spec: ClusterSpec, location: str | None = None) -> list[str]:
    """`location`: the region (default), or the node zone for a zonal cluster
    made before run 17."""
    return ["gcloud", "container", "clusters", "describe", spec.name,
            "--project", spec.project, *_location_flag(location or spec.region),
            "--format", "json"]


# ─── Endpoint resolution ────────────────────────────────────────────────────

# (env var, Service-name predicate). The installer exposes these as
# LoadBalancer Services in krateo-system; LB IPs are new on every cluster.
_ENDPOINT_SERVICES = (
    ("SNOWPLOW_URL", lambda n: n == "snowplow"),
    ("AUTHN_URL", lambda n: n == "authn"),
    ("FRONTEND_URL", lambda n: n.startswith("frontend")),
)


def resolve_endpoints(svc_list: dict) -> dict[str, str]:
    """Map `kubectl get svc -n krateo-system -o json` to the harness env.

    Raises RuntimeError naming every endpoint that has no LoadBalancer IP yet,
    so a run never silently falls back to the harness's dead baked defaults.
    """
    found: dict[str, str] = {}
    for item in svc_list.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        ingress = (item.get("status", {}).get("loadBalancer", {})
                   .get("ingress") or [])
        ports = item.get("spec", {}).get("ports") or []
        if not ingress or not ports:
            continue
        host = ingress[0].get("ip") or ingress[0].get("hostname")
        if not host:
            continue
        for env, pred in _ENDPOINT_SERVICES:
            if env not in found and pred(name):
                found[env] = f"http://{host}:{ports[0].get('port')}"
    missing = [env for env, _ in _ENDPOINT_SERVICES
               if env not in found and env in REQUIRED_ENDPOINTS]
    if missing:
        raise RuntimeError(
            f"no LoadBalancer endpoint yet for {missing}; wait for the "
            f"Services to get an external IP, then retry")
    return found


# Only these are needed by the bench (the HTTP driver and the freshness probe
# talk to authn + snowplow; FRONTEND_URL is informational).
REQUIRED_ENDPOINTS = ("SNOWPLOW_URL", "AUTHN_URL")


def _svc_for(svc_list: dict, env: str) -> dict | None:
    pred = dict(_ENDPOINT_SERVICES)[env]
    for item in svc_list.get("items", []):
        if pred(item.get("metadata", {}).get("name", "")):
            return item
    return None


def resolve_incluster(svc_list: dict, namespace: str = "krateo-system") -> dict[str, str]:
    """In-cluster Service DNS for every endpoint, ports from the Service objects.

    For anything that runs IN the cluster (the load driver and the freshness
    probe Jobs): needs no external IP at all, so LoadBalancer provisioning
    (run 5: GCE refused forwarding rules, IN_USE_ADDRESSES quota 14 global)
    cannot block the bench.
    """
    out, missing = {}, []
    for env, _ in _ENDPOINT_SERVICES:
        svc = _svc_for(svc_list, env)
        ports = ((svc or {}).get("spec") or {}).get("ports") or []
        if not svc or not ports:
            if env in REQUIRED_ENDPOINTS:
                missing.append(env)
            continue
        name = svc["metadata"]["name"]
        ns = svc["metadata"].get("namespace") or namespace
        out[env] = f"http://{name}.{ns}.svc:{ports[0].get('port')}"
    if missing:
        raise RuntimeError(f"no Service found for {missing} in {namespace}")
    return out


def lb_progress(svc_list: dict, events: list[dict] | None = None) -> tuple[dict, list[str], list[str]]:
    """External-mode readiness: (found endpoints, pending lines, fatal lines).

    fatal: a required Service that is NOT type LoadBalancer — it will never
    get an external IP, so waiting is pointless. pending: LoadBalancer with no
    ingress yet, annotated with the latest SyncLoadBalancerFailed event.
    """
    found, pending, fatal = {}, [], []
    last_err = {}
    for e in events or []:
        io = e.get("involvedObject") or {}
        if io.get("kind") == "Service" and e.get("reason") == "SyncLoadBalancerFailed":
            last_err[io.get("name")] = (e.get("message") or "")[-200:]
    for env, _ in _ENDPOINT_SERVICES:
        svc = _svc_for(svc_list, env)
        req = env in REQUIRED_ENDPOINTS
        if svc is None:
            (fatal if req else pending).append(f"{env}: no Service")
            continue
        name = svc["metadata"]["name"]
        typ = (svc.get("spec") or {}).get("type", "ClusterIP")
        ingress = ((svc.get("status") or {}).get("loadBalancer") or {}).get("ingress") or []
        port = ((svc.get("spec") or {}).get("ports") or [{}])[0].get("port")
        if ingress and (ingress[0].get("ip") or ingress[0].get("hostname")):
            found[env] = f"http://{ingress[0].get('ip') or ingress[0].get('hostname')}:{port}"
        elif typ != "LoadBalancer":
            if req:
                fatal.append(f"{env}: Service {name} is type {typ}; it will never get "
                             f"an external IP (use ENDPOINT_MODE=incluster)")
        else:
            msg = f"{env}: Service {name} LoadBalancer pending"
            if name in last_err:
                msg += f" (last error: {last_err[name]})"
            if req:
                pending.append(msg)
    return found, pending, fatal


# ─── In-cluster dead-man switch ─────────────────────────────────────────────


def self_destruct_manifest(spec: ClusterSpec,
                           image: str = "gcr.io/google.com/cloudsdktool/"
                                        "google-cloud-cli:slim") -> str:
    """A CronJob that deletes THIS cluster once its TTL has passed.

    The only teardown layer that survives the operator's machine dying. It
    runs with the node service account (the cluster is created with the
    cloud-platform scope), so it works only if that account may call
    container.clusters.delete — UNVERIFIED for operations-dev-krateo-io; the
    run plan lists it as a check before the first run. It re-checks the
    labels itself, so a copy applied to the wrong cluster deletes nothing.
    """
    _check_name(spec.name)
    exp = spec.ttl_expires
    want = " ".join(f"{k}={v}" for k, v in sorted(BENCH_LABELS.items()))
    script = (
        "set -eu\n"
        f"now=$(date +%s); [ \"$now\" -ge {exp} ] || exit 0\n"
        f"labels=$(gcloud container clusters describe {spec.name} "
        f"--project {spec.project} --region {spec.region} "
        "--format='value(resourceLabels)')\n"
        f"for kv in {want}; do k=${{kv%%=*}}; v=${{kv#*=}}; "
        "echo \"$labels\" | tr ';' '\\n' | grep -qx \"$k=$v\" "
        "|| { echo \"label $kv missing; refusing\"; exit 1; }; done\n"
        f"gcloud container clusters delete {spec.name} --project {spec.project} "
        f"--region {spec.region} --quiet --async\n"
    )
    indented = "\n".join("              " + ln for ln in script.splitlines())
    return f"""\
apiVersion: v1
kind: Namespace
metadata:
  name: bench-self-destruct
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ttl-self-destruct
  namespace: bench-self-destruct
  labels:
    purpose: snowplow-bench
spec:
  schedule: "*/10 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          tolerations:
          - operator: Exists
          containers:
          - name: reaper
            image: {image}
            command: ["/bin/sh", "-c"]
            args:
            - |
{indented}
"""


# ─── Execution ──────────────────────────────────────────────────────────────


# gcloud's error text names the operator's account ("This command is
# authenticated as <account> …", in its 404 and 503 answers), and kubectl's
# can name a user ("User \"…\" cannot …"). None of it may reach a log, a
# record or verdict.json (#900 review): only the code — bench.identity, the
# bench's one identity redaction (run606.sh's kerr_line uses it too).
from bench.identity import error_summary  # noqa: E402,F401  (imports nothing from bench)


def execute(argv: list[str], *, dry_run: bool, env: dict | None = None,
            input_data: str | None = None, timeout: float = 3600,
            log: Callable[[str], None] = print,
            stderr_out: list[str] | None = None) -> tuple[int, str]:
    """Run one argv (or print it under dry-run). Returns (rc, stdout); the
    stderr is appended to `stderr_out` when given."""
    shown = " ".join(argv)
    if dry_run:
        log(f"DRY-RUN: {shown}")
        return 0, ""
    log(f"RUN: {shown}")
    proc = subprocess.run(argv, capture_output=True, text=True,
                          env=env, input=input_data, timeout=timeout)
    if proc.returncode != 0:
        # never the free stderr: gcloud's names the operator's account (error_summary)
        log(f"  rc={proc.returncode} ({error_summary(proc.stderr)})")
    if stderr_out is not None:
        stderr_out.append(proc.stderr or "")
    return proc.returncode, proc.stdout


def run_with_teardown(spec: ClusterSpec, body: Callable[[], int], *,
                      teardown: Callable[[ClusterSpec], None],
                      install_signals: bool = True) -> int:
    """Run `body`, then ALWAYS call `teardown(spec)` — on return, on an
    exception, and on SIGINT/SIGTERM/SIGHUP (converted to an exception so the
    finally runs). A teardown failure is reported loudly and turns the exit
    code non-zero; it never masks the body's own exception.
    """
    previous = {}

    def _raise(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    if install_signals:
        for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            previous[s] = signal.signal(s, _raise)
    rc = 1
    teardown_failed = False
    try:
        rc = int(body())
    finally:
        try:
            teardown(spec)
        except Exception as e:  # noqa: BLE001 — must report, never mask
            teardown_failed = True
            sys.stderr.write(
                f"TEARDOWN FAILED for {spec.name}: {type(e).__name__}: {e}\n"
                f"Delete it by hand: {' '.join(delete_command(spec))}\n")
        finally:
            for s, h in previous.items():
                signal.signal(s, h)
    # Reached only when body() returned (an exception propagates above).
    return rc or (1 if teardown_failed else 0)


# GKE refuses a delete while another operation runs on the cluster (run 15
# B: a node-pool / upgrade operation in flight at teardown): "Cluster is
# running incompatible operation operation-<id>".
_INCOMPATIBLE_OP = re.compile(r"running incompatible operation (operation-[A-Za-z0-9-]+)")


def operation_wait_command(op: str, spec: ClusterSpec, location: str | None = None) -> list[str]:
    return ["gcloud", "container", "operations", "wait", op,
            "--project", spec.project, *_location_flag(location or spec.region)]


# gcloud's answer for a cluster that is not at the location asked:
# "ResponseError: code=404, message=Not found: projects/…/locations/…/clusters/…"
_NOT_FOUND = re.compile(r"code=404|NOT_FOUND|Not found:")


def find_cluster(spec: ClusterSpec, *, log: Callable[[str], None] = print,
                 legacy_zonal: bool = False) -> tuple[str, dict]:
    """(location, describe) of the bench cluster `spec` names: in its region
    (runs 17+). Only with `legacy_zonal` (gke-down --legacy-zonal, a manual
    cleanup; run606 never passes it) and only when the region answers
    NotFound, in its node zone (a zonal cluster made before run 17, #899 F3):
    in run606's teardown a regional create that failed before the cluster
    existed must never lead to a same-named zonal cluster. Any other describe
    failure raises: an unanswered describe is not evidence the cluster is
    elsewhere."""
    for location in (spec.region, spec.zone) if legacy_zonal else (spec.region,):
        err: list[str] = []
        rc, out = execute(describe_command(spec, location), dry_run=False, log=log, stderr_out=err)
        if rc == 0:
            d = json.loads(out)
            if d.get("name") != spec.name:
                raise NotDeletable(f"describe of {spec.name} in {location} returned {d.get('name')!r}")
            return location, d
        not_found = bool(_NOT_FOUND.search(err[-1] if err else ""))
        if not not_found:
            raise RuntimeError(f"describe {spec.name} in {location} failed (rc={rc})")
        if not legacy_zonal:
            raise RuntimeError(f"describe {spec.name} failed (rc={rc}): not found in region {spec.region}; "
                               f"it may already be gone (a zonal cluster made before run 17 is deleted "
                               f"only by hand: gke-down --legacy-zonal)")
        if location == spec.zone:
            raise RuntimeError(f"describe {spec.name} failed (rc={rc}): not found in region "
                               f"{spec.region} nor in zone {spec.zone}; it may already be gone")
        log(f"{spec.name} not found in region {spec.region}; looking in zone {spec.zone} "
            f"(a zonal cluster made before run 17)")
    raise AssertionError("unreachable")


# gke-down's budget when none is given (run606.sh passes DOWN_BUDGET_S, whose
# default is this same 900 s: a test pins the two). Bounds every wait of a
# teardown — the PV release, an in-flight operation, the NotFound poll —
# including gke-up's own teardown after a failed create.
DEFAULT_DOWN_BUDGET_S = 900.0
BACKSTOP_FAILED = "DISK BACKSTOP FAILED"


def teardown(spec: ClusterSpec, *, dry_run: bool,
             log: Callable[[str], None] = print,
             budget_s: float | None = None,
             clock: Callable[[], float] = time.monotonic,
             sleep: Callable[[float], None] = time.sleep,
             legacy_zonal: bool = False) -> list[str]:
    """Describe → assert_deletable → release the PVC disks → delete → wait
    for the cluster to be gone → delete this cluster's leftover disks by exact
    name. Raises when not deletable or the delete call fails, so
    run_with_teardown reports it. Returns the disk backstop's problems (each
    also written to stderr as DISK BACKSTOP FAILED): the cluster is deleted,
    but disks of it may be left.

    #895: PVC disks survive `clusters delete`. First every PVC goes and the CSI
    driver deletes its disk (release_pvc_disks, within half of the budget);
    then, the cluster gone, disk_backstop deletes whatever of THIS cluster is
    left (its own PVs' disks, and disks GKE labelled with this cluster's name
    and id) — never another cluster's. `budget_s` None = DEFAULT_DOWN_BUDGET_S.

    A delete refused because another operation is in flight on the cluster
    waits for THAT operation (`gcloud container operations wait`, bounded by
    what is left of the budget), then retries the delete — as often as the
    cluster names a new in-flight operation, until the budget is spent.
    Any other delete failure raises at once.

    The cluster is looked up in its region. With `legacy_zonal` only (a
    manual cleanup: gke-down --legacy-zonal; run606 never passes it), on
    NotFound there it is looked up in its node zone (a zonal cluster made
    before run 17) and deleted where it was found (#899 F3)."""
    if dry_run:
        execute(describe_command(spec), dry_run=True, log=log)
        execute(_kubectl(spec) + ["delete", "pvc", "--all", "--all-namespaces", "--wait=false"],
                dry_run=True, log=log)
        execute(delete_command(spec), dry_run=True, log=log)
        execute(disks_list_command(spec), dry_run=True, log=log)
        return []
    location, describe = find_cluster(spec, log=log, legacy_zonal=legacy_zonal)
    assert_deletable(describe)
    delete_argv = delete_command(spec.name, project=spec.project, location=location)
    budget = DEFAULT_DOWN_BUDGET_S if budget_s is None else budget_s
    start = clock()
    deadline = start + budget
    # half the budget releases the PVs; the rest deletes the cluster and runs the backstop
    recorded = release_pvc_disks(spec, describe, log=log, clock=clock, sleep=sleep,
                                 deadline=start + budget / 2)
    while True:
        err: list[str] = []
        rc, _ = execute(delete_argv, dry_run=False, log=log, stderr_out=err)
        if rc == 0:
            problems = []
            try:
                if wait_cluster_gone(spec, log=log, deadline=deadline, clock=clock, sleep=sleep,
                                     location=location):
                    disk_backstop(spec, cluster_id=cluster_id_b32(describe), recorded=recorded, log=log)
                else:
                    raise RuntimeError(
                        f"skipped: {spec.name} still being deleted when the {budget:g} s budget ran out, so "
                        f"its disks were never checked; once it is gone list them with "
                        f"{' '.join(disks_list_command(spec))}")
            except RuntimeError as e:
                # the cluster IS deleted; a disk possibly left over is said loudly, not as a failed delete
                problems.append(str(e))
                log(f"{BACKSTOP_FAILED}: {e}")
                sys.stderr.write(f"{BACKSTOP_FAILED} for {spec.name}: {e}\n")
            return problems
        m = _INCOMPATIBLE_OP.search(err[-1] if err else "")
        left = deadline - clock()
        if m is None or left <= 0:
            raise RuntimeError(f"delete {spec.name} failed (rc={rc})"
                               + (f"; still blocked by {m.group(1)} when the budget ran out" if m else ""))
        log(f"delete {spec.name} blocked by in-flight {m.group(1)}: waiting for it, then retrying")
        try:
            wrc, _ = execute(operation_wait_command(m.group(1), spec, location), dry_run=False, log=log,
                             timeout=left)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"delete {spec.name}: {m.group(1)} still running when the "
                               f"budget ran out") from None
        if wrc != 0:
            log(f"  waiting for {m.group(1)} failed (rc={wrc}); retrying the delete anyway")


# ─── PVC disks: quota, release at teardown, exact backstop (#895) ───────────
#
# Run 16 attempt 1 never measured: the CSI-provisioned PVC disks (pd-balanced,
# counted against the region's SSD_TOTAL_GB) survive `clusters delete`; ~32 GB
# per bench cluster piled up until ClickStack's MongoDB PVC could not be
# provisioned (QUOTA_EXCEEDED, limit 500 in europe-west4).
#
# The disks are attributable without anything of ours: GKE puts the cluster's
# own resource labels (created-by=bench-gke, purpose, issue, owner — see
# create_commands) and its name and id on every PD CSI disk it provisions.

PD_CSI = "pd.csi.storage.gke.io"
SSD_QUOTA_METRIC = "SSD_TOTAL_GB"
# The disk labels GKE's PD CSI driver puts on every PVC disk (read 2026-10-25
# from the project's disks: krateo-057's carry both id keys, the release
# cluster's the base32 one): the value is base32 of the cluster's `id`.
GKE_DISK_CLUSTER_NAME = "goog-k8s-cluster-name"
GKE_DISK_CLUSTER_ID_KEYS = ("goog-gke-cluster-id-base32", "cluster-id-gke-io")
PROVISIONED_BY = "pv.kubernetes.io/provisioned-by"
# One bench cluster's PVCs: run 14's diagnostics/pvcs.json — ClickStack
# ClickHouse 15Gi + Keeper 5Gi + MongoDB data 10G + logs 2G, each rounded up to
# whole GB as GCE provisions a disk (pvc_footprint_gb; a test pins the two).
BENCH_PVC_FOOTPRINT_GB = 32
_POLL_S = 15.0

_QTY = re.compile(r"^([0-9.]+)\s*([KMGTPE]i?|[kmgtpe])?$")
_QTY_MULT = {None: 1, "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18,
             "Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30, "Ti": 2 ** 40, "Pi": 2 ** 50, "Ei": 2 ** 60}


def disk_gb(quantity: str) -> int:
    """A Kubernetes storage quantity as the whole-GB disk GCE provisions for it
    (GCE "GB" are GiB; a request is rounded up)."""
    m = _QTY.match(str(quantity).strip())
    if not m:
        raise ValueError(f"not a storage quantity: {quantity!r}")
    return max(1, -(-int(float(m.group(1)) * _QTY_MULT[m.group(2)]) // 2 ** 30))


def pvc_footprint_gb(pvcs: Iterable[dict]) -> int:
    """Sum of the disks a set of PVC records (diag606 pvcs.json rows, or
    `kubectl get pvc -o json` items) provisions."""
    tot = 0
    for p in pvcs:
        req = p.get("request") or ((((p.get("spec") or {}).get("resources") or {}).get("requests") or {})
                                   .get("storage"))
        if req:
            tot += disk_gb(req)
    return tot




def ssd_quota_command(project: str, zone: str) -> list[str]:
    return ["gcloud", "compute", "regions", "describe", region_of(zone), "--project", project,
            "--format", "json"]


def ssd_quota_fit(region: dict, need_gb: float) -> tuple[bool, str]:
    """Does `need_gb` of PVC disk fit the region's SSD_TOTAL_GB (limit - usage)?"""
    q = next((x for x in region.get("quotas") or [] if x.get("metric") == SSD_QUOTA_METRIC), None)
    if q is None or q.get("limit") is None or q.get("usage") is None:
        return False, f"{SSD_QUOTA_METRIC} not in the region's quotas: whether the run fits is unknown"
    free = float(q["limit"]) - float(q["usage"])
    msg = (f"{SSD_QUOTA_METRIC} in {region.get('name')}: {q['usage']:g} of {q['limit']:g} GB used, "
           f"{free:g} free; the run needs {need_gb:g} GB of PVC disk")
    return free >= need_gb, msg


def _kubectl(spec: ClusterSpec) -> list[str]:
    return ["kubectl", f"--kubeconfig={spec.kubeconfig}", f"--context={kube_context(spec)}",
            "--request-timeout=30s"]


_HANDLE = re.compile(r"^projects/([^/]+)/(zones|regions)/([^/]+)/disks/([^/]+)$")


def pv_disks(pv_list: dict) -> list[dict]:
    """The GCE disks behind the cluster's PD CSI PersistentVolumes, with
    whether the PD CSI driver provisioned each (its provisioned-by
    annotation) — a statically bound PV may point at any disk."""
    out = []
    for pv in pv_list.get("items") or []:
        csi = ((pv.get("spec") or {}).get("csi") or {})
        m = _HANDLE.match(csi.get("volumeHandle") or "")
        if csi.get("driver") != PD_CSI or not m:
            continue
        md = pv.get("metadata") or {}
        out.append({"pv": md.get("name"), "project": m.group(1), "location": m.group(3), "disk": m.group(4),
                    "dynamic": (md.get("annotations") or {}).get(PROVISIONED_BY) == PD_CSI,
                    "reclaim": (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy"),
                    "gb": disk_gb(((pv.get("spec") or {}).get("capacity") or {}).get("storage") or "1Gi")})
    return out


def _ours(d: dict, spec: ClusterSpec) -> bool:
    """A PV's disk this cluster provisioned: by the PD CSI driver, in this
    project and this zone."""
    return d["dynamic"] and d["project"] == spec.project and d["location"] == spec.zone


def cluster_id_b32(describe: dict) -> str | None:
    """The value GKE's PD CSI driver writes as goog-gke-cluster-id-base32 (and
    cluster-id-gke-io): base32 of the cluster's `id`, lowercase, no padding."""
    import base64
    cid = describe.get("id")
    try:
        return base64.b32encode(bytes.fromhex(cid)).decode().lower().rstrip("=") if cid else None
    except ValueError:
        return None


def backstop_disks(disks: Iterable[dict], *, name: str, cluster_id: str | None, zone: str,
                   recorded: Iterable[str] = ()) -> list[str]:
    """The exact names of THIS cluster's leftover disks, safe to delete: in the
    cluster's zone, attached to nothing, and either carrying this cluster's
    name AND this cluster's id in GKE's own disk labels, or recorded from this
    cluster's own PersistentVolumes before it went (and labelled for no other
    cluster). Never a pattern: another cluster's disk differs in name or id."""
    _check_name(name)
    rec = set(recorded)
    out = []
    for d in disks:
        lab = d.get("labels") or {}
        dz = str(d.get("zone") or "").rsplit("/", 1)[-1]
        if dz != zone or d.get("users"):
            continue
        by_label = (lab.get(GKE_DISK_CLUSTER_NAME) == name and cluster_id is not None
                    and any(lab.get(k) == cluster_id for k in GKE_DISK_CLUSTER_ID_KEYS))
        by_record = (d.get("name") in rec and lab.get(GKE_DISK_CLUSTER_NAME) in (None, name)
                     and all(lab.get(k) in (None, cluster_id) for k in GKE_DISK_CLUSTER_ID_KEYS))
        if by_label or by_record:
            out.append(d["name"])
    return sorted(set(out))


def _json(out: str):
    try:
        return json.loads(out or "null")
    except ValueError:
        return None


def _host(url: str | None) -> str | None:
    m = re.match(r"^(?:https?://)?\[?([^\]/:]+)\]?(?::\d+)?(?:/.*)?$", (url or "").strip())
    return m.group(1) if m else None


def release_pvc_disks(spec: ClusterSpec, describe: dict, *, log: Callable[[str], None],
                      deadline: float, clock: Callable[[], float],
                      sleep: Callable[[float], None]) -> list[dict]:
    """Before the cluster goes: every PVC deleted and the pods holding them
    deleted (their controllers' new pods cannot be scheduled on a terminating
    PVC, so the PVC is released), then wait — bounded by `deadline` — until
    the CSI driver deleted the PVs' disks. Best effort: an unreachable
    cluster or a slow driver leaves the rest to the backstop.

    Nothing is changed unless the bench kubeconfig's server IS this cluster's
    endpoint (describe.endpoint). Only a PV the PD CSI driver provisioned in
    this project and zone is set to reclaim by Delete; any other PD CSI PV
    reclaimed by Delete is set to Retain first, so no PVC deletion here can
    delete a disk this cluster did not create. Returns this cluster's own
    PV disks (for the backstop)."""
    k = _kubectl(spec)
    rc, out = execute(k + ["get", "pv", "-o", "json"], dry_run=False, log=log)
    pvs = _json(out) if rc == 0 else None
    if not isinstance(pvs, dict):
        log(f"PVC release: could not list {spec.name}'s PVs (rc={rc}); the disk backstop runs after the delete")
        return []
    rc, out = execute(k + ["config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"],
                      dry_run=False, log=log)
    server, endpoint = _host(out) if rc == 0 else None, describe.get("endpoint")
    if not server or not endpoint or server != endpoint:
        log(f"PVC release: refusing — the bench kubeconfig's server ({server}) is not {spec.name}'s endpoint "
            f"({endpoint}); nothing changed, the disk backstop runs after the delete")
        return []
    disks = pv_disks(pvs)
    mine = [d for d in disks if _ours(d, spec)]
    log(f"PVC release: {len(disks)} PD CSI volume(s), {len(mine)} provisioned here "
        f"({sum(d['gb'] for d in mine)} GB): " + ", ".join(f"{d['pv']}={d['disk']}" for d in mine))
    if not disks:
        return []
    for d in disks:
        want = "Delete" if _ours(d, spec) else "Retain"
        if d["reclaim"] != want and (want == "Delete" or d["reclaim"] == "Delete"):
            rc, _ = execute(k + ["patch", "pv", d["pv"], "--type=merge", "-p",
                                 json.dumps({"spec": {"persistentVolumeReclaimPolicy": want}})],
                            dry_run=False, log=log)
            if rc != 0 and want == "Retain":
                # a disk this cluster did not provision is still reclaimed by
                # Delete: deleting the PVCs now could delete it (#896 reviewer)
                msg = (f"PVC release REFUSED: {d['pv']} (a disk this cluster did not provision, reclaimed by "
                       f"Delete) could not be set to Retain (rc={rc}); no PVC deleted, so it cannot take that "
                       f"disk; the disk backstop runs after the delete")
                log(msg)
                sys.stderr.write(msg + "\n")
                return mine
    execute(k + ["delete", "pvc", "--all", "--all-namespaces", "--wait=false"], dry_run=False, log=log)
    rc, out = execute(k + ["get", "pods", "--all-namespaces", "-o", "json"], dry_run=False, log=log)
    for p in ((_json(out) or {}).get("items") or []) if rc == 0 else []:
        if any((v or {}).get("persistentVolumeClaim") for v in (p.get("spec") or {}).get("volumes") or []):
            md = p.get("metadata") or {}
            execute(k + ["delete", "pod", "-n", md.get("namespace", "default"), md.get("name", ""),
                         "--wait=false"], dry_run=False, log=log)
    want = {d["pv"] for d in mine}
    while want:
        rc, out = execute(k + ["get", "pv", "-o", "json"], dry_run=False, log=lambda _: None)
        left = {p.get("metadata", {}).get("name") for p in ((_json(out) or {}).get("items") or [])} & want \
            if rc == 0 else want
        if not left:
            log("PVC release: every PV is gone (the CSI driver deleted their disks)")
            break
        if clock() >= deadline:
            log(f"PVC release: {len(left)} PV(s) still there at the budget's end ({', '.join(sorted(left))}); "
                f"the disk backstop runs after the delete")
            break
        sleep(_POLL_S)
    return mine


def disks_list_command(spec: ClusterSpec) -> list[str]:
    return ["gcloud", "compute", "disks", "list", "--project", spec.project,
            "--filter", f"zone:({spec.zone}) AND labels.{GKE_DISK_CLUSTER_NAME}={spec.name}",
            "--format", "json"]


def disk_backstop(spec: ClusterSpec, *, cluster_id: str | None, recorded: list[dict],
                  log: Callable[[str], None]) -> list[str]:
    """After the cluster is gone: delete THIS cluster's leftover disks by exact
    name (backstop_disks). Returns what it deleted; raises when the disks
    cannot be listed or deleted (a leak must never read as "none left")."""
    rc, out = execute(disks_list_command(spec), dry_run=False, log=log)
    found = _json(out) if rc == 0 else None
    if not isinstance(found, list):
        raise RuntimeError(f"listing {spec.name}'s disks failed (rc={rc}): whether any is left is unknown; "
                           f"run {' '.join(disks_list_command(spec))}")
    disks = list(found)
    have = {d.get("name") for d in disks}
    for d in recorded:
        if d["disk"] not in have and d["location"] == spec.zone and d["project"] == spec.project:
            rc, out = execute(["gcloud", "compute", "disks", "describe", d["disk"], "--zone", spec.zone,
                               "--project", spec.project, "--format", "json"], dry_run=False, log=log)
            one = _json(out) if rc == 0 else None
            if isinstance(one, dict):
                disks.append(one)
    names = backstop_disks(disks, name=spec.name, cluster_id=cluster_id, zone=spec.zone,
                           recorded=[d["disk"] for d in recorded])
    if not names:
        log(f"disk backstop: no disk of {spec.name} left")
        return []
    rc, _ = execute(["gcloud", "compute", "disks", "delete", *names, "--zone", spec.zone,
                     "--project", spec.project, "--quiet"], dry_run=False, log=log)
    if rc != 0:
        raise RuntimeError(f"deleting {', '.join(names)} failed (rc={rc}); "
                           f"delete them by hand (gcloud compute disks delete ... --zone {spec.zone})")
    log(f"disk backstop: deleted {len(names)} leftover disk(s) of {spec.name}: {', '.join(names)}")
    return names


def wait_cluster_gone(spec: ClusterSpec, *, log: Callable[[str], None], deadline: float,
                      clock: Callable[[], float], sleep: Callable[[float], None],
                      location: str | None = None) -> bool:
    """Poll describe until GKE reports the cluster NotFound (an --async delete
    returns at once; its nodes, and the disks attached to them, go when the
    operation ends). False when the budget ends first. `location`: where the
    cluster was found and deleted (its region; its zone for a zonal cluster
    made before run 17) — polled THERE, or the region's describe 404s at once
    and the backstop runs while the nodes still hold the PVC disks (#896
    reviewer: a false "no disk left")."""
    while True:
        err: list[str] = []
        rc, _ = execute(describe_command(spec, location), dry_run=False, log=lambda _: None, stderr_out=err)
        # the same NotFound test as the lookup (find_cluster): code=404 / NOT_FOUND only — a
        # looser match ("not found" anywhere, any 404) could run the disk backstop early (#907 N1)
        if rc != 0 and _NOT_FOUND.search(err[-1] if err else ""):
            return True
        if clock() >= deadline:
            return False
        sleep(_POLL_S)


# ─── CLI (registered from cli._build_parser) ────────────────────────────────


def _spec_from_args(a) -> ClusterSpec:
    return default_spec(a.name, owner=getattr(a, "owner", None),
                        ttl_hours=getattr(a, "ttl_hours", DEFAULT_TTL_HOURS),
                        project=a.project, zone=a.zone,
                        cluster_version=getattr(a, "cluster_version",
                                                DEFAULT_CLUSTER_VERSION),
                        loadgen_machine_type=getattr(a, "loadgen_machine_type",
                                                     DEFAULT_LOADGEN_MACHINE))


def cmd_gke_up(a) -> int:
    """Create the cluster + pools, write the dedicated kubeconfig, apply the
    self-destruct CronJob. Without --yes it is a dry run (prints argv only)."""
    spec = _spec_from_args(a)
    dry = not a.yes
    env = dict(os.environ, KUBECONFIG=spec.kubeconfig)
    print(f"control plane: regional ({spec.region}); every node in {spec.zone}")
    print(f"cluster={spec.name} context={kube_context(spec)} "
          f"kubeconfig={spec.kubeconfig} ttl-expires={spec.ttl_expires} "
          f"({time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(spec.ttl_expires))})")
    # #895: refuse a run whose PVC disks cannot fit the region's SSD quota,
    # before anything is created (run 16 found out 45 minutes in)
    need = getattr(a, "ssd_need_gb", BENCH_PVC_FOOTPRINT_GB)
    rc, out = execute(ssd_quota_command(spec.project, spec.zone), dry_run=dry)
    if not dry:
        region = _json(out) if rc == 0 else None
        ok, msg = ssd_quota_fit(region if isinstance(region, dict) else {}, need)
        if rc != 0:
            ok, msg = False, f"could not read the region's quotas (rc={rc}): whether the run fits is unknown"
        if not ok:
            sys.stderr.write(f"refusing: {msg}. Free SSD_TOTAL_GB first (leaked PVC disks: "
                             f"gcloud compute disks list --filter='labels.created-by=bench-gke'), nothing "
                             f"was created\n")
            return 2
        print(msg)
    if not dry:
        os.makedirs(os.path.dirname(spec.kubeconfig), exist_ok=True)
    for argv in create_commands(spec):
        rc, _ = execute(argv, dry_run=dry, env=env)
        if rc != 0:
            sys.stderr.write(
                f"create step failed (rc={rc}); tearing down what exists\n")
            if not dry:
                try:
                    teardown(spec, dry_run=False)
                except Exception as e:  # noqa: BLE001
                    sys.stderr.write(f"teardown after failed create: {e}\n")
            return rc
    rc, _ = execute(["kubectl", f"--kubeconfig={spec.kubeconfig}",
                     f"--context={kube_context(spec)}", "apply", "-f", "-"],
                    dry_run=dry, input_data=self_destruct_manifest(spec))
    if rc != 0:
        sys.stderr.write("self-destruct CronJob NOT applied; the trap and "
                         "`gke-reap` are the remaining teardown layers\n")
    return 0


def cmd_gke_shape(a) -> int:
    """The cluster-shape record run606 keeps with the run, read back from GKE
    after create (a read-only describe). A dry run (no --yes) records the
    spec, marked as such. A describe that fails records the shape as
    UNKNOWN — never the spec in its place — and exits 0: the record is
    evidence for the verdict, not a reason to end the run."""
    spec = _spec_from_args(a)
    say = lambda m: sys.stderr.write(m + "\n")  # noqa: E731
    if not a.yes:
        rec = cluster_shape(spec)
    else:
        err: list[str] = []
        rc, out = execute(describe_command(spec), dry_run=False, log=lambda _m: None, stderr_out=err)
        try:
            rec = observed_shape(json.loads(out)) if rc == 0 else None
            # the code only: gcloud's text names the operator's account (#900 review)
            why = f"describe failed (rc={rc}, {error_summary(err[-1] if err else '')})"
        except ValueError as e:
            rec, why = None, f"describe answered with no JSON ({e})"
        if rec is None:
            rec = unobserved_shape(spec, why)
    if rec["control_plane"] is None:
        say(f"cluster shape UNKNOWN: {rec['source']}")
    else:
        say(f"cluster shape ({rec['source']}): control plane {rec['control_plane']} "
            f"({rec.get('location') or rec.get('region')}); nodes in {','.join(rec['node_locations']) or '?'}"
            + (f"; {rec['current_node_count']} nodes" if rec.get("current_node_count") is not None else ""))
    print(json.dumps(rec))
    return 0


# gke-down's exit when the cluster IS deleted but the disk backstop failed or
# was skipped (run606 reports it: disks of the run may be left)
EXIT_BACKSTOP_FAILED = 4


def cmd_gke_down(a) -> int:
    spec = default_spec(a.name, project=a.project, zone=a.zone)
    try:
        problems = teardown(spec, dry_run=not a.yes, budget_s=a.budget_s, legacy_zonal=a.legacy_zonal)
    except Exception as e:  # noqa: BLE001
        sys.stderr.write(f"gke-down: {type(e).__name__}: {e}\n")
        return 1
    return EXIT_BACKSTOP_FAILED if problems else 0


def cmd_gke_ssd_quota(a) -> int:
    """#895: run606_pair.sh, before two clusters start at once (each gke-up
    alone would see room for itself)."""
    need = a.runs * BENCH_PVC_FOOTPRINT_GB
    rc, out = execute(ssd_quota_command(a.project, a.zone), dry_run=False, log=lambda _: None)
    region = _json(out) if rc == 0 else None
    ok, msg = ssd_quota_fit(region if isinstance(region, dict) else {}, need)
    if rc != 0:
        ok, msg = False, f"could not read the region's quotas (rc={rc}): whether the runs fit is unknown"
    # no "refusing:" prefix: the caller (run606_pair.sh) says it once
    print(msg + f" ({a.runs} bench cluster(s) x {BENCH_PVC_FOOTPRINT_GB} GB)",
          file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 2


def cmd_gke_reap(a) -> int:
    rc, out = execute(list_command(a.project), dry_run=False)
    if rc != 0:
        return rc
    cmds = reap_commands(json.loads(out or "[]"), now=time.time(),
                         project=a.project)
    if not cmds:
        print("no expired bench clusters")
    for argv in cmds:
        execute(argv, dry_run=not a.yes)
    return 0


def cmd_gke_env(a) -> int:
    """Print the endpoint exports. Mode (stated on stderr AND exported):

    incluster (default)  Service DNS (<svc>.<ns>.svc:<port>) for the in-cluster
                         Jobs (load driver + freshness probe). No external IP
                         needed; immediate.
    external             LoadBalancer IPs, for anything run on the operator's
                         machine. Bounded wait (--wait-s), progress every
                         --interval-s with the latest SyncLoadBalancerFailed
                         reason; a required Service that is not type
                         LoadBalancer fails at once with a clear message.
    """
    spec = default_spec(a.name, project=a.project, zone=a.zone)
    err = lambda m: sys.stderr.write(m + "\n")  # noqa: E731
    base = ["kubectl", f"--kubeconfig={spec.kubeconfig}",
            f"--context={kube_context(spec)}"]

    def get(*args):
        rc, out = execute(base + list(args), dry_run=False, log=lambda m: None)
        if rc != 0:
            raise RuntimeError(f"kubectl {' '.join(args)} failed (rc={rc})")
        return json.loads(out or "{}")

    mode = a.mode
    err(f"[env] endpoint mode: {mode}")
    try:
        if mode == "incluster":
            eps = resolve_incluster(get("get", "svc", "-n", "krateo-system", "-o", "json"))
        else:
            deadline = time.monotonic() + a.wait_s
            while True:
                svcs = get("get", "svc", "-n", "krateo-system", "-o", "json")
                try:
                    events = get("get", "events", "-n", "krateo-system",
                                 "-o", "json").get("items", [])
                except RuntimeError:
                    events = []
                eps, pending, fatal = lb_progress(svcs, events)
                if fatal:
                    raise RuntimeError("; ".join(fatal))
                if not pending:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"no LoadBalancer IP after {a.wait_s:.0f}s: " + "; ".join(pending))
                err("[env] waiting for LoadBalancer IPs: " + "; ".join(pending))
                time.sleep(a.interval_s)
    except RuntimeError as e:
        err(f"[env] FAILED ({mode}): {e}")
        return 3
    for k, v in eps.items():
        err(f"[env] {k}={v}")
        print(f"export {k}={v}")
    print(f"export BENCH_ENDPOINT_MODE={mode}")
    print(f"export KUBECONFIG={spec.kubeconfig}")
    print(f"export BENCH_GKE_CONTEXT={kube_context(spec)}")
    return 0


def _zone_arg(v: str) -> str:
    import argparse
    try:
        return check_zone(v)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from None


def add_parsers(sub) -> None:
    def common(p, *, name_required=True):
        p.add_argument("--name", required=name_required, default=None,
                       help=f"cluster name; must start with {BENCH_NAME_PREFIX}")
        p.add_argument("--project", default=DEFAULT_PROJECT)
        p.add_argument("--zone", default=DEFAULT_ZONE, type=_zone_arg,
                       help="the zone every node is in; the control plane is regional (its region)")
        p.add_argument("--yes", action="store_true",
                       help="actually run gcloud (default: dry run)")

    up = sub.add_parser("gke-up", help="#606: create a TEMPORARY bench cluster "
                                       "(dry run unless --yes)")
    common(up, name_required=False)
    up.add_argument("--owner", default=None)
    up.add_argument("--ttl-hours", type=int, default=DEFAULT_TTL_HOURS)
    up.add_argument("--cluster-version", default=DEFAULT_CLUSTER_VERSION)
    up.add_argument("--loadgen-machine-type", default=DEFAULT_LOADGEN_MACHINE,
                    help="#830: n2-standard-16 for run 11's k6 + Chrome load")
    up.add_argument("--ssd-need-gb", type=float, default=BENCH_PVC_FOOTPRINT_GB,
                    help=f"#895: PVC disk the run needs free in SSD_TOTAL_GB (one bench cluster: "
                         f"{BENCH_PVC_FOOTPRINT_GB})")
    up.set_defaults(func=cmd_gke_up)

    shape = sub.add_parser("gke-shape", help="the cluster-shape record of a run: read back from GKE "
                                             "with --yes (read-only describe); the spec without")
    common(shape)
    shape.add_argument("--loadgen-machine-type", default=DEFAULT_LOADGEN_MACHINE)
    shape.set_defaults(func=cmd_gke_shape)

    sq = sub.add_parser("gke-ssd-quota", help="#895: exit 2 unless RUNS bench clusters' PVC disks fit "
                                               "the region's SSD_TOTAL_GB (a read; nothing is created)")
    sq.add_argument("--project", default=DEFAULT_PROJECT)
    sq.add_argument("--zone", default=DEFAULT_ZONE)
    sq.add_argument("--runs", type=int, default=1)
    sq.set_defaults(func=cmd_gke_ssd_quota)

    down = sub.add_parser("gke-down", help="#606: delete a bench cluster "
                                           "(label-checked; dry run unless --yes)")
    common(down)
    down.add_argument("--budget-s", type=float, default=DEFAULT_DOWN_BUDGET_S,
                      help="bound of every teardown wait: the PV release (half of it), an in-flight "
                           "cluster operation, the gone-poll before the disk backstop (run606: "
                           f"DOWN_BUDGET_S; default {DEFAULT_DOWN_BUDGET_S:g} s)")
    down.add_argument("--legacy-zonal", action="store_true",
                      help="manual cleanup only (run606 never passes it): when the region answers NotFound, "
                           "look for a zonal bench cluster made before run 17 in --zone and delete it there")
    down.set_defaults(func=cmd_gke_down)

    reap = sub.add_parser("gke-reap", help="#606: delete bench clusters past "
                                           "their TTL (dry run unless --yes)")
    reap.add_argument("--project", default=DEFAULT_PROJECT)
    reap.add_argument("--yes", action="store_true")
    reap.set_defaults(func=cmd_gke_reap)

    env = sub.add_parser("gke-env", help="#606: print SNOWPLOW_URL/AUTHN_URL/"
                                         "FRONTEND_URL exports for a bench cluster")
    common(env)
    env.add_argument("--mode", choices=["incluster", "external"], default="incluster")
    env.add_argument("--wait-s", type=float, default=600)
    env.add_argument("--interval-s", type=float, default=15)
    env.set_defaults(func=cmd_gke_env)


def main(argv: list[str] | None = None) -> int:
    """`python -m bench.gke gke-up|gke-down|gke-reap|gke-env ...`

    Deliberately NOT a `python -m bench` subcommand: that entrypoint imports
    bench.cluster, whose import-time kube-context guard compares the AMBIENT
    current-context to a pinned context. These commands act on GKE by
    project + cluster name (never through a kube context) and carry their own
    guards (bench name prefix, forbidden names, label check), so the ambient
    context must neither block them nor be consulted by them. The first
    shakedown attempt (2026-10-14) died at gke-reap on exactly that guard.
    This module imports nothing from bench but bench.identity, which imports
    nothing at all (and bench/__init__ imports nothing).
    """
    import argparse
    p = argparse.ArgumentParser(prog="python -m bench.gke")
    sub = p.add_subparsers(dest="cmd", required=True)
    add_parsers(sub)
    a = p.parse_args(argv)
    return int(a.func(a))


if __name__ == "__main__":
    sys.exit(main())
