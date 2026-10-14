"""Temporary dedicated GKE cluster lifecycle for the #606 production-scale bench.

Owner ruling (Diego, 2026-10-13): the 50K bench runs on a TEMPORARY dedicated
GKE cluster created per run and torn down afterwards. NEVER on krateo-057.

This module owns that lifecycle and nothing else:

  - `default_spec()` / `create_commands()` — the exact gcloud argv for the
    cluster and its node pools, labelled with an owner, the issue and a TTL;
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
    baked defaults point at a cluster deleted 2026-06-14).

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

    def labels(self) -> dict[str, str]:
        out = dict(BENCH_LABELS)
        out[OWNER_LABEL] = sanitize_label_value(self.owner)
        out[TTL_LABEL] = str(self.ttl_expires)
        return out


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


def default_spec(name: str | None = None, *, now: float | None = None,
                 owner: str | None = None, ttl_hours: int = DEFAULT_TTL_HOURS,
                 project: str = DEFAULT_PROJECT, zone: str = DEFAULT_ZONE,
                 cluster_version: str = DEFAULT_CLUSTER_VERSION,
                 kubeconfig_dir: str | None = None) -> ClusterSpec:
    """The #606 cluster shape. See the run plan for the sizing rationale.

    - platform pool: 3 x n2-standard-8 (the reference release cluster runs the
      full platform on 3);
    - snowplow pool: 1 x n2-standard-8, tainted, so snowplow's CPU is measured
      without neighbours (057 steady state is 3.2-3.9 cores of a 4-core limit);
    - loadgen pool: 1 x n2-standard-8, tainted, for the in-cluster load driver,
      so 1000 virtual users never share a node with what they measure.

    Quota-shaped (read 2026-10-14, operations-dev-krateo-io / europe-west4):
    E2_CPUS limit is 24, so every pool is N2 (N2_CPUS 200); SSD_TOTAL_GB has
    ~133 GB free, so node disks are pd-standard (DISKS_TOTAL_GB 4096);
    IN_USE_ADDRESSES is 16 with 3 used, so 5 nodes + ~5 LoadBalancers fit.
    """
    now = time.time() if now is None else now
    if name is None:
        name = BENCH_NAME_PREFIX + time.strftime("%m%d-%H%M", time.gmtime(now))
    _check_name(name)
    owner = owner or os.environ.get("USER", "unknown")
    kdir = kubeconfig_dir or os.path.expanduser("~/.kube/snowplow-bench")
    pools = (
        NodePool("snowplow", "n2-standard-8", 1,
                 taint=("dedicated", "snowplow", "NoSchedule"),
                 labels=(("bench.krateo.io/pool", "snowplow"),)),
        NodePool("loadgen", "n2-standard-8", 1,
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
    """The context name gcloud writes for a zonal cluster."""
    return f"gke_{spec.project}_{spec.zone}_{spec.name}"


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
    loc = ["--project", spec.project, "--zone", spec.zone]
    cmds = [[
        "gcloud", "container", "clusters", "create", spec.name, *loc,
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
            "--cluster", spec.name, *loc, *_pool_flags(pool), "--quiet",
        ])
    cmds.append([
        "gcloud", "container", "clusters", "get-credentials", spec.name, *loc,
    ])
    return cmds


def delete_command(spec_or_name, *, project: str = DEFAULT_PROJECT,
                   zone: str = DEFAULT_ZONE) -> list[str]:
    if isinstance(spec_or_name, ClusterSpec):
        name, project, zone = (spec_or_name.name, spec_or_name.project,
                               spec_or_name.zone)
    else:
        name = spec_or_name
    _check_name(name)
    return ["gcloud", "container", "clusters", "delete", name,
            "--project", project, "--zone", zone, "--quiet", "--async"]


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
        zone = c.get("zone") or c.get("location") or DEFAULT_ZONE
        cmds.append(delete_command(c["name"], project=project, zone=zone))
    return cmds


def list_command(project: str = DEFAULT_PROJECT) -> list[str]:
    return ["gcloud", "container", "clusters", "list", "--project", project,
            "--filter", "resourceLabels.purpose=snowplow-bench",
            "--format", "json"]


def describe_command(spec: ClusterSpec) -> list[str]:
    return ["gcloud", "container", "clusters", "describe", spec.name,
            "--project", spec.project, "--zone", spec.zone, "--format", "json"]


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
    missing = [env for env, _ in _ENDPOINT_SERVICES if env not in found]
    if missing:
        raise RuntimeError(
            f"no LoadBalancer endpoint yet for {missing}; wait for the "
            f"Services to get an external IP, then retry")
    return found


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
        f"--project {spec.project} --zone {spec.zone} "
        "--format='value(resourceLabels)')\n"
        f"for kv in {want}; do k=${{kv%%=*}}; v=${{kv#*=}}; "
        "echo \"$labels\" | tr ';' '\\n' | grep -qx \"$k=$v\" "
        "|| { echo \"label $kv missing; refusing\"; exit 1; }; done\n"
        f"gcloud container clusters delete {spec.name} --project {spec.project} "
        f"--zone {spec.zone} --quiet --async\n"
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


def execute(argv: list[str], *, dry_run: bool, env: dict | None = None,
            input_data: str | None = None, timeout: int = 3600,
            log: Callable[[str], None] = print) -> tuple[int, str]:
    """Run one argv (or print it under dry-run). Returns (rc, stdout)."""
    shown = " ".join(argv)
    if dry_run:
        log(f"DRY-RUN: {shown}")
        return 0, ""
    log(f"RUN: {shown}")
    proc = subprocess.run(argv, capture_output=True, text=True,
                          env=env, input=input_data, timeout=timeout)
    if proc.returncode != 0:
        log(f"  rc={proc.returncode} stderr={proc.stderr.strip()[:500]}")
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


def teardown(spec: ClusterSpec, *, dry_run: bool,
             log: Callable[[str], None] = print) -> None:
    """Describe → assert_deletable → delete. Raises when not deletable or the
    delete call fails, so run_with_teardown reports it."""
    rc, out = execute(describe_command(spec), dry_run=dry_run, log=log)
    if dry_run:
        execute(delete_command(spec), dry_run=True, log=log)
        return
    if rc != 0:
        raise RuntimeError(f"describe {spec.name} failed (rc={rc}); "
                           f"cluster may already be gone")
    assert_deletable(json.loads(out))
    rc, _ = execute(delete_command(spec), dry_run=False, log=log)
    if rc != 0:
        raise RuntimeError(f"delete {spec.name} failed (rc={rc})")


# ─── CLI (registered from cli._build_parser) ────────────────────────────────


def _spec_from_args(a) -> ClusterSpec:
    return default_spec(a.name, owner=getattr(a, "owner", None),
                        ttl_hours=getattr(a, "ttl_hours", DEFAULT_TTL_HOURS),
                        project=a.project, zone=a.zone,
                        cluster_version=getattr(a, "cluster_version",
                                                DEFAULT_CLUSTER_VERSION))


def cmd_gke_up(a) -> int:
    """Create the cluster + pools, write the dedicated kubeconfig, apply the
    self-destruct CronJob. Without --yes it is a dry run (prints argv only)."""
    spec = _spec_from_args(a)
    dry = not a.yes
    env = dict(os.environ, KUBECONFIG=spec.kubeconfig)
    print(f"cluster={spec.name} context={kube_context(spec)} "
          f"kubeconfig={spec.kubeconfig} ttl-expires={spec.ttl_expires} "
          f"({time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(spec.ttl_expires))})")
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


def cmd_gke_down(a) -> int:
    spec = default_spec(a.name, project=a.project, zone=a.zone)
    try:
        teardown(spec, dry_run=not a.yes)
    except Exception as e:  # noqa: BLE001
        sys.stderr.write(f"gke-down: {type(e).__name__}: {e}\n")
        return 1
    return 0


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
    spec = default_spec(a.name, project=a.project, zone=a.zone)
    rc, out = execute(["kubectl", f"--kubeconfig={spec.kubeconfig}",
                       f"--context={kube_context(spec)}", "get", "svc",
                       "-n", "krateo-system", "-o", "json"], dry_run=False,
                      log=lambda m: sys.stderr.write(m + "\n"))
    if rc != 0:
        return rc
    for k, v in resolve_endpoints(json.loads(out)).items():
        print(f"export {k}={v}")
    print(f"export KUBECONFIG={spec.kubeconfig}")
    print(f"export BENCH_GKE_CONTEXT={kube_context(spec)}")
    return 0


def add_parsers(sub) -> None:
    def common(p, *, name_required=True):
        p.add_argument("--name", required=name_required, default=None,
                       help=f"cluster name; must start with {BENCH_NAME_PREFIX}")
        p.add_argument("--project", default=DEFAULT_PROJECT)
        p.add_argument("--zone", default=DEFAULT_ZONE)
        p.add_argument("--yes", action="store_true",
                       help="actually run gcloud (default: dry run)")

    up = sub.add_parser("gke-up", help="#606: create a TEMPORARY bench cluster "
                                       "(dry run unless --yes)")
    common(up, name_required=False)
    up.add_argument("--owner", default=None)
    up.add_argument("--ttl-hours", type=int, default=DEFAULT_TTL_HOURS)
    up.add_argument("--cluster-version", default=DEFAULT_CLUSTER_VERSION)
    up.set_defaults(func=cmd_gke_up)

    down = sub.add_parser("gke-down", help="#606: delete a bench cluster "
                                           "(label-checked; dry run unless --yes)")
    common(down)
    down.set_defaults(func=cmd_gke_down)

    reap = sub.add_parser("gke-reap", help="#606: delete bench clusters past "
                                           "their TTL (dry run unless --yes)")
    reap.add_argument("--project", default=DEFAULT_PROJECT)
    reap.add_argument("--yes", action="store_true")
    reap.set_defaults(func=cmd_gke_reap)

    env = sub.add_parser("gke-env", help="#606: print SNOWPLOW_URL/AUTHN_URL/"
                                         "FRONTEND_URL exports for a bench cluster")
    common(env)
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
    This module imports nothing from bench.
    """
    import argparse
    p = argparse.ArgumentParser(prog="python -m bench.gke")
    sub = p.add_subparsers(dest="cmd", required=True)
    add_parsers(sub)
    a = p.parse_args(argv)
    return int(a.func(a))


if __name__ == "__main__":
    sys.exit(main())
