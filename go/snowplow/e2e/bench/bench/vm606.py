"""Run the paired #606 launch from a DISPOSABLE GCE VM (Diego 2026-11-10: "Execute a run fully relying
on GCP. If you need a workload outside the cluster, spawn a disposable VM.").

Since #1297 the data load and every timed sampler run in-cluster; what is left on the operator's
machine is the orchestration (gcloud / kubectl / helm calls, the wait polls, evaluate, the evidence),
and the laptop it ran on is shared with other sessions that keep it loaded (run 24: load ~90 on 8
CPUs). This module moves that orchestration, unchanged, to a VM of its own that runs as a dedicated
service account and deletes itself at its TTL. The VM runs the SAME launcher (run606_pair.sh, started
DETACHED through bench.detach) at one bench commit; the host-load gate (bench/hostload.py) then
judges the VM.

    python3 -m bench.vm606 plan-iam                         # the roles, derived from the code; prints only
    python3 -m bench.vm606 up --run R --ttl-hours H [--yes]  # create the VM, wait for its bootstrap
    python3 -m bench.vm606 push-secret --run R [--yes]       # the ghcr pull-token file, as an opaque file
    python3 -m bench.vm606 launch --run R --bench-sha SHA --env-file F \\
        --pin-manifest M --pin-sha256 HEX [--yes]            # clone at SHA, dry pass, start detached
    python3 -m bench.vm606 status --run R [--lines N]        # the launcher's log, its liveness, the VM's time left
    python3 -m bench.vm606 fetch --run R                     # run dirs + evidence back, hashes verified
    python3 -m bench.vm606 down --run R [--yes]              # delete the VM and its disk, verify both gone

Every subcommand that changes something in GCP prints its gcloud argv and runs none of them unless
--yes (as bench.gke). plan-iam never changes IAM: it prints the commands for a human to run. status
and fetch create nothing in GCP (an SSH and a copy).

IAM (plan-iam). The orchestrator's GCP calls are every `["gcloud", ...]` argv in bench/*.py plus the
Cloud Quotas REST read and the kubectl / helm calls against the bench cluster. plan-iam SCANS the
code for them each time it runs, maps each call to the predefined role that holds its permission, and
refuses (exit 1, naming the call) when the code makes a call the plan does not cover, so a new call
cannot ship with the plan silently short. This module's own calls are not in the scan: they run on
the laptop as the operator (Owner covers them: compute.instanceAdmin.v1, iam.serviceAccountUser on
the orchestrator SA, iap.tunnelResourceAccessor, compute.osAdminLogin). Nor is the in-cluster
ttl-self-destruct CronJob's gcloud (bench.gke.self_destruct_manifest): it runs as the GKE node SA.

THE VM (up).
  - e2-standard-8 (8 vCPU, 32 GB). What runs on it: run606_pair.sh and one run606.sh per run (three
    in run 24), their kubectl polls and log streams, #1004's full-widen applies
    (data_manifests.COMPOSITION_APPLY_WORKERS = 32 concurrent kubectl per run), and evaluate over each
    run's OUT (run 23: ~950 MB per run, 475 MB of it k6 streams), the three at once at the end. The
    host-load gate refuses at a 1-minute load above the CPU count, and the laptop's 8 CPUs are the
    only size this orchestration has run at. E2 has its own regional quota (E2_CPUS, 0 of 24 used
    on 2026-11-10), so the VM takes nothing from the N2_CPUS the bench nodes use.
  - Boot disk pd-balanced 50 GB: a three-run launch writes ~3 GB of OUT and the same again as its
    published evidence copy (bench/evidence.py), plus the checkout, the venv and the OS; a balanced
    disk so the evidence publish before each gke-down does not hold a cluster up on a slow disk. It
    counts against SSD_TOTAL_GB (33 of 500 used on 2026-11-10; a run needs 96), which the bench's own
    quota pre-check reads live, so the VM's share is counted, never assumed.
  - Debian 13 (python 3.13; the bench's CI runs 3.12, the laptop 3.14), bootstrapped by a startup
    script to the laptop's versions: kubectl v1.36.2 and helm v3.19.0 (each download checked against
    the sha256 pinned below), google-cloud-cli and gke-gcloud-auth-plugin 561.0.0 from Google's apt
    repository, git, python3-venv. The bench's Python deps go into a venv per bench commit at launch,
    from that commit's requirements.txt (as CI installs them).
  - Labels owner=snowplow-bench, run=<R>, ttl-hours=<H>. Auto-delete: GCE's own `--termination-time`
    (now + H hours) with `--instance-termination-action=DELETE` and the boot disk auto-deleted: the
    VM and its disk go at the TTL whatever happens to the session that made it, and the service
    account needs no permission for it. `launch` refuses unless the VM outlives the run's own TTL
    (the `--ttl-hours` its dry pass hands gke-up) plus LAUNCH_MARGIN_S; give `up` the run's TTL plus
    the hours fetch may wait (13 h at 8a395c45 + 2 = 15).
  - AN EPHEMERAL EXTERNAL IP, not Cloud NAT: the project's network has no Cloud Router / NAT
    (`gcloud compute routers list`: [] on 2026-11-10) and the VM must reach github.com, ghcr.io, the
    apt / k8s / helm downloads and the bench's LoadBalancer IPs; a NAT would be permanent shared
    infrastructure for a disposable VM. The IP counts in IN_USE_ADDRESSES, which the bench's quota
    pre-check counts from the instances list (a run needs 18 of 24).
  - SSH: OS Login (enable-oslogin=TRUE, project keys blocked) through IAP (`--tunnel-through-iap`;
    default-allow-ssh admits 35.235.240.0/20). `up` refuses while iap.googleapis.com is not enabled.

THE PULL TOKEN (push-secret). ~/.config/snowplow-bench/ghcr-pull.env goes to the same path on the
VM, mode 0600, by `gcloud compute scp`. This module never opens it: it stats it (presence, regular
file, mode 0600) and hands its path to scp; on the VM it checks presence and mode the same way. It
lives on the boot disk only, so it goes with the VM (down verifies the disk is gone).

LAUNCH. On the VM: clone krateo-platformops/snowplow at exactly --bench-sha (verified after the
fetch), the venv, the pin manifest (copied by scp, its sha256 checked on both sides), then the
launcher's own dry pass (BENCH_606_DRY_RUN=1, its own PAIR_OUT) with the operator's env, then the
paid launch: `python3 -m bench.detach --log ~/vm606/<R>/launch.log --pidfile ~/vm606/<R>/launch.pid
-- bash run606_pair.sh`, with the manifest copied into the pair's run directory (where the laptop
launch kept it, so it is published with the pair's evidence). The env file holds the laptop
launcher's variables, one KEY=VALUE per line (RUNS, PAIRS, REPLICA_PAIR, RUN_NAME_<L>,
SNOWPLOW_TAG_<L>, EXPECTED_IMAGE_DIGEST_<L>, BENCH_606_GO_<L>, BENCH_1004_GO_<L>, INSTALLER_VERSION,
...): it is passed as is, run606_pair.sh refuses what it refuses. Refused here, before any SSH: a
variable naming a place on the laptop (OUT_<L>, PAIR_OUT, the data roots, HOME, PATH, KUBECONFIG,
GHCR_PULL_ENV_FILE, any value that is an absolute or ~ path), the test hooks (BENCH_606_DRY_RUN,
BENCH_606_FAIL_AT, BENCH_606_HOST_LOAD) and gcloud overrides (CLOUDSDK_*).

FETCH. Once the launcher has exited (refused while it runs): one tar on the VM of every run's
directory and evidence directory and the pair's, its sha256 checked after the copy, unpacked beside
the laptop's trees, every evidence directory verified against its own MANIFEST.json
(bench.evidence.verify, stage final), and only then moved into ~/.local/share/snowplow-bench/
{runs,evidence}/606/; a directory the laptop already holds is never overwritten. No bucket: fetch
needs no role beyond SSH.

DOWN. Refuses an instance not labelled owner=snowplow-bench and run=<R>, or whose name is not
vm_name(R), a launcher still running, and a launch not fetched (--discard-unfetched says the evidence
may go). Deletes the instance with --delete-disks=all, then reads it back: the instance must answer
NotFound and its disks must list as gone; the pull-token file lived on that disk only.

THE OPERATOR'S SEQUENCE (the team lead, from the laptop; every step but status/fetch is a dry run
without --yes). The env file is the laptop launcher's variables, e.g. for run 24's shape:

    RUNS="N1 N2 N1B"
    PAIRS="N1:N2 N1:N1B"
    REPLICA_PAIR=N1:N1B
    INSTALLER_VERSION=0.3.545
    RUN_NAME_N1=snowplow-bench-run25n1-<sha8>     # and _N2, _N1B
    SNOWPLOW_TAG_N1=1.12.67                       # and _N2, _N1B
    EXPECTED_IMAGE_DIGEST_N1=sha256:<64 hex>      # and _N2, _N1B, read from the registry
    BENCH_606_GO_N1=<YYYYMMDD>-snowplow-bench-run25n1-<sha8>   # Diego's go per run (UTC date)

    python3 -m bench.vm606 up --run run25 --ttl-hours 15 --yes
    python3 -m bench.vm606 push-secret --run run25 --yes
    python3 -m bench.vm606 launch --run run25 --bench-sha <40-hex> --env-file run25.env \
        --pin-manifest .../installer-0.3.545-pin-manifest.json \
        --pin-sha256 1e79f6a7e611d80be2c878694b1adc84038b4f7f181cf162bc39c6af5587c72b --yes
    python3 -m bench.vm606 status --run run25          # as often as wanted; the run survives any disconnect
    python3 -m bench.vm606 fetch --run run25           # once status says the launcher exited
    python3 -m bench.vm606 down --run run25 --yes

Stopping a paid run on the VM: `gcloud compute ssh <vm> --tunnel-through-iap -- 'kill -TERM "$(cat
~/vm606/<R>/launch.pid)"'` (run606_pair.sh forwards it to every run; each tears its cluster down).
A detached launcher survives the SSH session ending (bench.detach: its own session, SIGHUP ignored;
Debian's logind leaves user processes alone, KillUserProcesses=no).

gcloud's error text names the operator's account: only its code reaches the output
(bench.identity.error_summary); the VM's own output is passed through redact_identity.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import time
from pathlib import Path

from bench import evidence
from bench.gke import DEFAULT_PROJECT, DEFAULT_ZONE, ZONE_PATTERN
from bench.identity import error_summary, redact_identity

BENCH_DIR = Path(__file__).resolve().parent.parent

# ─── what the VM is ─────────────────────────────────────────────────────────

SA_NAME = "snowplow-bench-orchestrator"
VM_PREFIX = "snowplow-bench-orch-"
OWNER = "snowplow-bench"
MACHINE_TYPE = "e2-standard-8"
BOOT_DISK_GB = 50
BOOT_DISK_TYPE = "pd-balanced"
IMAGE_FAMILY, IMAGE_PROJECT = "debian-13", "debian-cloud"
REPO_URL = "https://github.com/krateo-platformops/snowplow.git"
MAX_TTL_HOURS = 72
# the VM must outlive the run's own TTL by this much at launch: the launch-to-gke-up time, the pair's
# verdicts after the last gke-down, and the fetch
LAUNCH_MARGIN_S = 3600

# the laptop's tool versions (kubectl version --client, helm version, gcloud version: 2026-11-10)
KUBECTL_VERSION = "v1.36.2"
KUBECTL_SHA256 = "1e9045ec32bea85da43de85f0065358529ea7c7a152eca78154fba5b58c27d82"   # dl.k8s.io .sha256
HELM_VERSION = "v3.19.0"
HELM_SHA256 = "a7f81ce08007091b86d8bd696eb4d86b8d0f2e1b9f6c714be62f82f96a594496"      # get.helm.sh .sha256sum
GCLOUD_APT_VERSION = "561.0.0-0"     # google-cloud-cli and its gke-gcloud-auth-plugin (apt, cloud-sdk main)

GHCR_FILE_REL = ".config/snowplow-bench/ghcr-pull.env"    # bench.ghcr_pull.DEFAULT_ENV_FILE, from ~
BOOTSTRAP_MARKER = "/var/lib/vm606/bootstrap.json"
# a test hook (as BENCH_606_FAIL_AT): the fake VM's root; the remote scripts prefix the marker with it
TEST_ROOT_ENV = "VM606_TEST_ROOT"

RUN_PATTERN = re.compile(r"[a-z][a-z0-9-]{0,30}[a-z0-9]")
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")

# gcloud's `compute ... list` exits 0 with "[]" when its requests failed unless this is false
# (bench.gke.PRECHECK_LIST_ENV, #1172 B3)
LIST_ENV = {"CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR": "false"}


class Refused(Exception):
    """A refusal; the message says why. Nothing after it ran."""


def vm_name(run: str) -> str:
    return VM_PREFIX + run


def sa_email(project: str) -> str:
    return f"{SA_NAME}@{project}.iam.gserviceaccount.com"


def check_run(run: str) -> str:
    if not RUN_PATTERN.fullmatch(run or ""):
        raise Refused(f"--run {run!r}: lowercase letters, digits and '-', starting with a letter, at most 32 "
                      f"(it names the VM, {VM_PREFIX}<run>, and its run label)")
    return run


def state_dir(run: str) -> Path:
    """The laptop's record of one VM launch (beside the runs and evidence trees)."""
    return evidence.data_root() / "vm606" / run


def _state(run: str) -> dict:
    try:
        return json.loads((state_dir(run) / "state.json").read_text())
    except FileNotFoundError:
        return {}


def _save_state(run: str, **kv) -> dict:
    st = {**_state(run), **kv}
    d = state_dir(run)
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / "state.json.tmp"
    tmp.write_text(json.dumps(st, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, d / "state.json")
    return st


# ─── running gcloud ─────────────────────────────────────────────────────────

def _say(msg: str) -> None:
    print(msg, flush=True)


def gcloud(args: list[str], *, dry: bool = False, env: dict | None = None, input_data: str | None = None,
           timeout: float = 600, quiet: bool = False) -> tuple[int, str, str]:
    """One gcloud call: (rc, stdout, stderr). Dry: printed, not run. Its stderr is the caller's to
    summarize (it names the account): it is never printed here."""
    argv = ["gcloud", *args]
    if dry:
        _say("DRY-RUN: " + shlex.join(argv))
        return 0, "", ""
    if not quiet:
        _say("RUN: " + shlex.join(argv))
    try:
        p = subprocess.run(argv, capture_output=True, text=True, input=input_data, timeout=timeout,
                           env=dict(os.environ, **(env or {})))
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout:.0f} s"
    return p.returncode, p.stdout, p.stderr


def _where(project: str, zone: str) -> list[str]:
    return ["--project", project, "--zone", zone]


def ssh(name: str, project: str, zone: str, script: str, *, timeout: float = 1800) -> tuple[int, str, str]:
    """Run `script` with bash on the VM (stdin; the script holds every value it needs), OS Login
    through IAP."""
    return gcloud(["compute", "ssh", name, *_where(project, zone), "--tunnel-through-iap", "--quiet",
                   "--ssh-flag=-T", "--command", "bash -s"], input_data=script, timeout=timeout, quiet=True)


def scp(src: str, dst: str, project: str, zone: str, *, dry: bool = False,
        timeout: float = 3600) -> tuple[int, str, str]:
    return gcloud(["compute", "scp", *_where(project, zone), "--tunnel-through-iap", "--quiet", src, dst],
                  dry=dry, timeout=timeout)


def _remote_lines(out: str) -> list[str]:
    return [redact_identity(ln) for ln in (out or "").splitlines()]


def _marker(out: str, key: str) -> str | None:
    for ln in (out or "").splitlines():
        if ln.startswith(key + "="):
            return ln[len(key) + 1:]
    return None


def _failed(what: str, rc: int, out: str, err: str, tail: int = 20) -> Refused:
    lines = [ln for ln in _remote_lines(out) if ln.startswith("vm606:")] or _remote_lines(out)[-tail:]
    body = ("\n  " + "\n  ".join(lines[-tail:])) if lines else ""
    return Refused(f"{what} failed (rc={rc}; gcloud: {error_summary(err)}){body}")


def _describe(name: str, project: str, zone: str) -> tuple[dict | None, str]:
    """(the instance, "") or (None, "notfound" | why it was not read)."""
    rc, out, err = gcloud(["compute", "instances", "describe", name, *_where(project, zone), "--format", "json"],
                          quiet=True)
    if rc != 0:
        return None, ("notfound" if re.search(r"\bcode=404\b|was not found|notFound", err or "")
                      else f"describe failed (rc={rc}; {error_summary(err)})")
    try:
        got = json.loads(out)
    except ValueError:
        return None, "describe answered no JSON"
    return (got, "") if isinstance(got, dict) else (None, "describe answered no JSON object")


def _owned(inst: dict, run: str) -> None:
    """Refuse an instance this module did not make for `run`."""
    labels = inst.get("labels") or {}
    if inst.get("name") != vm_name(run):
        raise Refused(f"instance {inst.get('name')!r} is not {vm_name(run)!r}: refusing")
    if labels.get("owner") != OWNER or labels.get("run") != run:
        raise Refused(f"instance {inst.get('name')!r} is not labelled owner={OWNER}, run={run} "
                      f"(labels: {labels}): refusing")


def _time_left_s(inst: dict, now: float) -> float | None:
    t = ((inst.get("scheduling") or {}).get("terminationTime")) or ""
    try:
        end = _dt.datetime.fromisoformat(t.replace("Z", "+00:00"))
    except ValueError:
        return None
    return end.timestamp() - now


# ─── plan-iam: the roles, derived from the code ─────────────────────────────

# (group, resource, verb) of a gcloud argv -> (role, why); "*" = a variable in the argv
NODE_SA_ROLE = "roles/iam.serviceAccountUser"
CALL_ROLES: dict[tuple[str, ...], tuple[str, ...]] = {
    ("container", "clusters", "create"): ("roles/container.admin", NODE_SA_ROLE),
    ("container", "node-pools", "create"): ("roles/container.admin", NODE_SA_ROLE),
    ("container", "clusters", "get-credentials"): ("roles/container.admin",),
    ("container", "clusters", "delete"): ("roles/container.admin",),
    ("container", "clusters", "describe"): ("roles/container.admin",),
    ("container", "clusters", "list"): ("roles/container.admin",),
    ("container", "operations", "wait"): ("roles/container.admin",),
    ("compute", "regions", "describe"): ("roles/compute.viewer",),
    ("compute", "project-info", "describe"): ("roles/compute.viewer",),
    ("compute", "instances", "describe"): ("roles/compute.viewer",),
    ("compute", "instances", "list"): ("roles/compute.viewer",),
    ("compute", "disks", "list"): ("roles/compute.viewer",),
    # forwarding-rules / addresses / instances lists (region_list_command, _gce_list_command): compute.*.list
    ("compute", "*", "list"): ("roles/compute.viewer",),
    ("compute", "disks", "delete"): ("roles/compute.storageAdmin",),
    ("compute", "addresses", "delete"): ("roles/compute.publicIpAdmin",),
    ("auth", "print-access-token"): (),          # gcloud's own credential: the VM's metadata token
}
# a Google API the code calls over REST, by host, and a request header that needs a role of its own
REST_ROLES = {"cloudquotas": ("roles/cloudquotas.viewer",)}
HEADER_ROLES = {"x-goog-user-project": ("roles/serviceusage.serviceUsageConsumer",)}
KUBE_ROLE = "roles/container.admin"

WHY = {
    "roles/container.admin": (
        "GKE clusters create / node-pools create / get-credentials / describe / list / delete, operations "
        "wait, AND every kubectl / helm call against the bench cluster: the installer's helm install writes "
        "CRDs, ClusterRoles and ClusterRoleBindings; the run uses Jobs, pods/exec and logs. "
        "container.clusterAdmin has no Kubernetes-API access; container.developer cannot create clusters or "
        "write RBAC: container.admin is the one predefined role that holds both."),
    "roles/compute.viewer": (
        "read-only: the run's quota pre-check (regions / project-info describe, instances / forwarding-rules "
        "/ addresses lists), the disk backstop's disks list, the node shape's instances describe."),
    "roles/compute.storageAdmin": (
        "compute.disks.delete for the disk backstop (the cluster's leaked PVC disks): the narrowest "
        "predefined role holding it (instanceAdmin.v1 adds control of every instance)."),
    "roles/compute.publicIpAdmin": (
        "compute.addresses.delete for the #1026 address backstop: the narrowest predefined role holding it "
        "(networkAdmin is far broader)."),
    "roles/cloudquotas.viewer": (
        "cloudquotas.quotas.get: the #985 read of ClustersPerRegion over REST."),
    "roles/serviceusage.serviceUsageConsumer": (
        "serviceusage.services.use: that request bills the project (x-goog-user-project)."),
    NODE_SA_ROLE: (
        "granted ON the GKE node SA only: clusters / node-pools create pass no --service-account, so the "
        "nodes run as the Compute Engine default SA, and GKE requires actAs on it."),
}
NOT_NEEDED = (
    "firewall roles: no gcloud firewall call in the code; GKE's own service agent writes the k8s-fw-* rules",
    "storage roles: fetch copies over IAP SSH, no bucket",
    "compute.instances.* writes for the VM itself: its TTL delete is GCE's (--termination-time, "
    "--instance-termination-action=DELETE), no permission of the SA",
    "logging / monitoring roles: the bench calls neither",
)

_PY_ARGV = re.compile(r'\[\s*"gcloud"\s*((?:,\s*(?:"[^"]*"|[A-Za-z_][A-Za-z_0-9]*)\s*){1,3})')
_PY_KUBE = re.compile(r'\[\s*"(?:kubectl|helm)"')
_REST_HOST = re.compile(r"https?://([a-z0-9-]+)\.googleapis\.com")
_SH_QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'')
_SH_COMMENT = re.compile(r"(?:^|\s)#.*$")
_SH_TOOL = re.compile(r"(?:^|[\s;&|(`])(gcloud|kubectl|helm)\b")
SELF = Path(__file__).name     # this module's calls are the operator's, run on the laptop


def scan(bench_dir: Path = BENCH_DIR) -> dict:
    """Every GCP call the orchestrator makes, read from the code: {"calls": [(key, where)],
    "rest": [(host, where)], "headers": [(header, where)], "kube": [where], "uncovered": [text]}."""
    calls, rest, headers, kube, uncovered = [], [], [], [], []
    py = sorted(p for p in (bench_dir / "bench").glob("*.py") if p.name != SELF)
    for f in py:
        src = f.read_text()
        rel = f.relative_to(bench_dir)
        for m in _PY_ARGV.finditer(src):
            toks = [t.strip() for t in m.group(1).split(",") if t.strip()]
            key = tuple(t[1:-1] if t.startswith('"') else "*" for t in toks)
            key = key[:2] if key[:1] == ("auth",) else key[:3]
            where = f"{rel}:{src.count(chr(10), 0, m.start()) + 1}"
            (calls if key in CALL_ROLES else uncovered).append(
                (key, where) if key in CALL_ROLES else f"gcloud {' '.join(key)} ({where})")
        for m in _REST_HOST.finditer(src):
            where = f"{rel}:{src.count(chr(10), 0, m.start()) + 1}"
            host = m.group(1)
            if host in REST_ROLES:
                rest.append((host, where))
            else:
                uncovered.append(f"REST {host}.googleapis.com ({where})")
        for m in _PY_KUBE.finditer(src):
            kube.append(f"{rel}:{src.count(chr(10), 0, m.start()) + 1}")
        for h in HEADER_ROLES:
            for m in re.finditer(re.escape(h), src):
                headers.append((h, f"{rel}:{src.count(chr(10), 0, m.start()) + 1}"))
    shells = sorted({*bench_dir.glob("run606*.sh"), *(bench_dir / "bench").glob("*.sh")})
    for f in shells:
        rel = f.relative_to(bench_dir)
        for n, line in enumerate(f.read_text().splitlines(), 1):
            code = _SH_COMMENT.sub("", _SH_QUOTED.sub('""', line))
            for m in _SH_TOOL.finditer(code):
                if m.group(1) == "gcloud":
                    uncovered.append(f"gcloud in shell ({rel}:{n}): {line.strip()[:120]}")
                else:
                    kube.append(f"{rel}:{n}")
    return {"calls": calls, "rest": rest, "headers": headers, "kube": kube, "uncovered": uncovered}


def plan(found: dict) -> dict[str, list[str]]:
    """role -> the calls that need it (file:line), in a stable order."""
    need: dict[str, list[str]] = {}
    for key, where in found["calls"]:
        for role in CALL_ROLES[key]:
            need.setdefault(role, []).append(f"gcloud {' '.join(key)}  {where}")
    for host, where in found["rest"]:
        for role in REST_ROLES[host]:
            need.setdefault(role, []).append(f"REST {host}.googleapis.com  {where}")
    for h, where in found["headers"]:
        for role in HEADER_ROLES[h]:
            need.setdefault(role, []).append(f"header {h}  {where}")
    if found["kube"]:
        files = sorted({w.split(":")[0] for w in found["kube"]})
        need.setdefault(KUBE_ROLE, []).append(
            f"kubectl / helm against the bench cluster: {len(found['kube'])} call sites in {', '.join(files)}")
    order = list(WHY)
    return {r: need[r] for r in sorted(need, key=lambda r: order.index(r) if r in order else len(order))}


def iam_commands(project: str, roles: list[str], node_sa: str) -> list[str]:
    member = f"serviceAccount:{sa_email(project)}"
    cmds = [shlex.join(["gcloud", "iam", "service-accounts", "create", SA_NAME, "--project", project,
                        "--display-name", "snowplow bench orchestrator (vm606)",
                        "--description", "Runs run606_pair.sh from a disposable GCE VM (bench/vm606.py)"])]
    for r in roles:
        if r == NODE_SA_ROLE:
            continue
        cmds.append(shlex.join(["gcloud", "projects", "add-iam-policy-binding", project, "--member", member,
                                "--role", r, "--condition", "None"]))
    if NODE_SA_ROLE in roles:
        cmds.append(shlex.join(["gcloud", "iam", "service-accounts", "add-iam-policy-binding", node_sa,
                                "--project", project, "--member", member, "--role", NODE_SA_ROLE]))
    return cmds


def cmd_plan_iam(a) -> int:
    found = scan(Path(a.bench_dir))
    if found["uncovered"]:
        print("refusing: the code makes GCP calls this plan does not cover (add each to CALL_ROLES / "
              "REST_ROLES with the role that holds its permission):\n  " + "\n  ".join(found["uncovered"]),
              file=sys.stderr)
        return 1
    number = a.project_number
    if not number:
        rc, out, err = gcloud(["projects", "describe", a.project, "--format", "value(projectNumber)"], quiet=True)
        number = out.strip()
        if rc != 0 or not number.isdigit():
            print(f"refusing: the project number of {a.project} could not be read (rc={rc}; {error_summary(err)}): "
                  f"the node SA is <number>-compute@developer.gserviceaccount.com; pass --project-number",
                  file=sys.stderr)
            return 1
    node_sa = f"{number}-compute@developer.gserviceaccount.com"
    need = plan(found)
    _say(f"# vm606 plan-iam: {sa_email(a.project)}")
    _say(f"# derived from the code under {Path(a.bench_dir).resolve()} (every [\"gcloud\", ...] argv in bench/*.py "
         f"but {SELF}, REST hosts, kubectl/helm in run606*.sh and bench/*.sh); nothing here is run")
    _say("")
    for i, (role, uses) in enumerate(need.items(), 1):
        on = f"  (ON {node_sa} only)" if role == NODE_SA_ROLE else "  (project)"
        _say(f"{i}. {role}{on}")
        _say(f"   why: {WHY.get(role, '')}")
        for u in uses:
            _say(f"   - {u}")
    _say("")
    _say("not needed:")
    for n in NOT_NEEDED:
        _say(f"   - {n}")
    _say("")
    _say("# commands (for a human to run; this tool never changes IAM):")
    for c in iam_commands(a.project, list(need), node_sa):
        _say(c)
    _say("")
    _say("# the operator's own account (up / ssh / scp / down from the laptop) needs, Owner covers all: "
         "compute.instanceAdmin.v1, iam.serviceAccountUser on the orchestrator SA, iap.tunnelResourceAccessor, "
         "compute.osAdminLogin")
    return 0


# ─── up ─────────────────────────────────────────────────────────────────────

def startup_script() -> str:
    """The VM's bootstrap, as root at every boot; a no-op once the marker is written."""
    return f"""#!/bin/bash
# bench/vm606.py: the orchestrator VM's tools at the laptop's versions
set -euo pipefail
M=/var/lib/vm606
mkdir -p "$M"
[ -s "$M/bootstrap.json" ] && exit 0
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q --no-install-recommends git python3 python3-venv python3-pip ca-certificates curl gnupg
if ! apt-cache madison google-cloud-cli | grep -q " {GCLOUD_APT_VERSION} "; then
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/vm606-cloud.google.gpg
  echo "deb [signed-by=/usr/share/keyrings/vm606-cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \\
    > /etc/apt/sources.list.d/vm606-google-cloud-sdk.list
  apt-get update -q
fi
apt-get install -y -q --allow-downgrades google-cloud-cli={GCLOUD_APT_VERSION} google-cloud-cli-gke-gcloud-auth-plugin={GCLOUD_APT_VERSION}
T="$(mktemp -d)"
cd "$T"
curl -fsSLo kubectl https://dl.k8s.io/release/{KUBECTL_VERSION}/bin/linux/amd64/kubectl
echo "{KUBECTL_SHA256}  kubectl" | sha256sum -c -
install -m 0755 kubectl /usr/local/bin/kubectl
curl -fsSLo helm.tgz https://get.helm.sh/helm-{HELM_VERSION}-linux-amd64.tar.gz
echo "{HELM_SHA256}  helm.tgz" | sha256sum -c -
tar -xzf helm.tgz linux-amd64/helm
install -m 0755 linux-amd64/helm /usr/local/bin/helm
cd /
rm -rf "$T"
python3 - > "$M/bootstrap.json.tmp" <<'PY'
import json, subprocess
def out(*a):
    return subprocess.run(a, capture_output=True, text=True, check=True).stdout.strip()
print(json.dumps({{
    "kubectl": json.loads(out("kubectl", "version", "--client", "-o", "json"))["clientVersion"]["gitVersion"],
    "helm": out("helm", "version", "--template", "{{{{.Version}}}}"),
    "gcloud": json.loads(out("gcloud", "version", "--format", "json"))["Google Cloud SDK"],
    "gke-gcloud-auth-plugin": out("gke-gcloud-auth-plugin", "--version").splitlines()[0],
    "python3": out("python3", "--version"),
    "bash": out("bash", "--version").splitlines()[0],
    "git": out("git", "--version"),
}}))
PY
mv "$M/bootstrap.json.tmp" "$M/bootstrap.json"
"""


def bootstrap_mismatch(got: dict) -> list[str]:
    want = {"kubectl": KUBECTL_VERSION, "helm": HELM_VERSION, "gcloud": GCLOUD_APT_VERSION.split("-")[0]}
    bad = [f"{k} {got.get(k)!r}, want {v!r}" for k, v in want.items() if got.get(k) != v]
    if not got.get("gke-gcloud-auth-plugin"):
        bad.append("gke-gcloud-auth-plugin missing")
    return bad


def termination_time(now: float, ttl_hours: int) -> str:
    return _dt.datetime.fromtimestamp(now + ttl_hours * 3600, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_command(run: str, project: str, zone: str, ttl_hours: int, startup_file: str, now: float) -> list[str]:
    labels = f"owner={OWNER},run={run},ttl-hours={ttl_hours}"
    return ["compute", "instances", "create", vm_name(run), *_where(project, zone),
            "--machine-type", MACHINE_TYPE,
            "--image-family", IMAGE_FAMILY, "--image-project", IMAGE_PROJECT,
            "--boot-disk-size", f"{BOOT_DISK_GB}GB", "--boot-disk-type", BOOT_DISK_TYPE, "--boot-disk-auto-delete",
            "--service-account", sa_email(project), "--scopes", "cloud-platform",
            "--labels", labels,
            "--metadata", "enable-oslogin=TRUE,block-project-ssh-keys=TRUE",
            "--metadata-from-file", f"startup-script={startup_file}",
            "--termination-time", termination_time(now, ttl_hours),
            "--instance-termination-action", "DELETE",
            "--shielded-secure-boot", "--shielded-vtpm", "--shielded-integrity-monitoring"]


def bootstrap_check_script() -> str:
    return f'cat "${{{TEST_ROOT_ENV}:-}}{BOOTSTRAP_MARKER}" 2>/dev/null || true\n'


def cmd_up(a) -> int:
    run = check_run(a.run)
    if not 1 <= a.ttl_hours <= MAX_TTL_HOURS:
        raise Refused(f"--ttl-hours {a.ttl_hours}: 1 to {MAX_TTL_HOURS} (the run's own TTL + the hours fetch may wait)")
    name = vm_name(run)
    rc, out, err = gcloud(["services", "list", "--enabled", "--project", a.project,
                           "--filter", "config.name=iap.googleapis.com", "--format", "value(config.name)"], quiet=True)
    if rc != 0:
        raise Refused(f"could not read whether IAP is enabled (rc={rc}; {error_summary(err)})")
    if "iap.googleapis.com" not in out.split():
        raise Refused(f"iap.googleapis.com is not enabled in {a.project}: the VM is reached through IAP only "
                      f"(gcloud services enable iap.googleapis.com --project {a.project})")
    rc, out, err = gcloud(["iam", "service-accounts", "describe", sa_email(a.project), "--project", a.project,
                           "--format", "value(email)"], quiet=True)
    if rc != 0 or out.strip() != sa_email(a.project):
        raise Refused(f"the orchestrator SA {sa_email(a.project)} could not be read (rc={rc}; {error_summary(err)}): "
                      f"create it from `python3 -m bench.vm606 plan-iam` first")
    d = state_dir(run)
    d.mkdir(parents=True, exist_ok=True)
    sf = d / "startup.sh"
    sf.write_text(startup_script())
    now = time.time()
    rc, out, err = gcloud(create_command(run, a.project, a.zone, a.ttl_hours, str(sf), now), dry=not a.yes)
    if not a.yes:
        _say(f"DRY: would wait for {BOOTSTRAP_MARKER} on {name} (kubectl {KUBECTL_VERSION}, helm {HELM_VERSION}, "
             f"gcloud {GCLOUD_APT_VERSION.split('-')[0]}); nothing was created")
        return 0
    if rc != 0:
        raise Refused(f"instances create failed (rc={rc}; {error_summary(err)})")
    _save_state(run, vm=name, project=a.project, zone=a.zone, ttl_hours=a.ttl_hours,
                terminates_at=termination_time(now, a.ttl_hours), created_at=now)
    _say(f"{name} created; it deletes itself (and its disk) at {termination_time(now, a.ttl_hours)}")
    deadline = time.monotonic() + a.bootstrap_timeout_s
    while True:
        rc, out, _err = ssh(name, a.project, a.zone, bootstrap_check_script(), timeout=120)
        if rc == 0 and out.strip():
            try:
                got = json.loads(out)
            except ValueError:
                got = None
            if isinstance(got, dict):
                bad = bootstrap_mismatch(got)
                if bad:
                    raise Refused(f"{name}'s tools are not the laptop's: {'; '.join(bad)}")
                _save_state(run, bootstrap=got)
                _say(f"{name} bootstrapped: " + ", ".join(f"{k} {v}" for k, v in sorted(got.items())))
                return 0
        if time.monotonic() >= deadline:
            raise Refused(f"{name}'s bootstrap did not finish in {a.bootstrap_timeout_s} s ({BOOTSTRAP_MARKER} "
                          f"absent; the startup script's log: sudo journalctl -u google-startup-scripts on the VM)")
        time.sleep(a.poll_s)


# ─── push-secret ────────────────────────────────────────────────────────────

def _local_secret(path: str) -> None:
    """Presence, a regular file, mode 0600: from its inode only, never its bytes."""
    try:
        st = os.stat(path)
    except FileNotFoundError:
        raise Refused(f"{path} does not exist") from None
    if not stat.S_ISREG(st.st_mode):
        raise Refused(f"{path} is not a regular file")
    if stat.S_IMODE(st.st_mode) != 0o600:
        raise Refused(f"{path} has mode {stat.S_IMODE(st.st_mode):04o}, not 0600")


_MODE_PY = ("python3 -c 'import os,stat,sys; p=os.path.expanduser(sys.argv[1]); "
            "print(oct(stat.S_IMODE(os.stat(p).st_mode))[2:] if os.path.isfile(p) else \"absent\")'")


def cmd_push_secret(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    local = os.path.expanduser("~/" + GHCR_FILE_REL)
    _local_secret(local)
    _say(f"local {local}: present, mode 0600")
    prep = (f"set -euo pipefail\numask 077\nmkdir -p \"$HOME/{os.path.dirname(GHCR_FILE_REL)}\"\n"
            f"chmod 700 \"$HOME/{os.path.dirname(GHCR_FILE_REL)}\"\nrm -f \"$HOME/{GHCR_FILE_REL}\"\n")
    verify = (f"set -euo pipefail\nchmod 600 \"$HOME/{GHCR_FILE_REL}\"\n"
              f"echo \"VM606_MODE=$({_MODE_PY} \"~/{GHCR_FILE_REL}\")\"\n")
    if not a.yes:
        _say(f"DRY: on {name}: mkdir -m 700 ~/{os.path.dirname(GHCR_FILE_REL)}")
        scp(local, f"{name}:{GHCR_FILE_REL}", a.project, a.zone, dry=True)
        _say(f"DRY: on {name}: chmod 600 ~/{GHCR_FILE_REL}; check presence and mode; nothing was copied")
        return 0
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    rc, out, err = ssh(name, a.project, a.zone, prep, timeout=300)
    if rc != 0:
        raise _failed("preparing the token's directory on the VM", rc, out, err)
    rc, out, err = scp(local, f"{name}:{GHCR_FILE_REL}", a.project, a.zone)
    if rc != 0:
        raise Refused(f"scp of the token file failed (rc={rc}; {error_summary(err)})")
    rc, out, err = ssh(name, a.project, a.zone, verify, timeout=300)
    mode = _marker(out, "VM606_MODE")
    if rc != 0 or mode != "600":
        raise Refused(f"the token file on {name} is {mode or 'unreadable'} (want present, mode 600; rc={rc})")
    _local_secret(local)
    _save_state(run, secret_pushed_at=time.time())
    _say(f"{name}:~/{GHCR_FILE_REL}: present, mode 0600")
    return 0


# ─── launch ─────────────────────────────────────────────────────────────────

LAPTOP_ONLY = re.compile(r"^(OUT_[A-Z][A-Z0-9]*|PAIR_OUT|BENCH_RUNS_DIR|BENCH_EVIDENCE_DIR|XDG_DATA_HOME|HOME|PATH|"
                         r"KUBECONFIG|GHCR_PULL_ENV_FILE|PYTHONPATH|BENCH_GKE_CONTEXT)$")
TEST_HOOKS = re.compile(r"^(BENCH_606_DRY_RUN|BENCH_606_FAIL_AT|BENCH_606_HOST_LOAD|BENCH_ALLOW_[A-Z_]+|"
                        r"CLOUDSDK_[A-Z_]+|" + TEST_ROOT_ENV + r")$")
_ENV_LINE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def read_env_file(path: str) -> dict[str, str]:
    """The laptop launcher's variables, KEY=VALUE per line (shell quoting allowed, no expansion)."""
    env: dict[str, str] = {}
    for n, raw in enumerate(Path(path).read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE.match(line)
        if not m:
            raise Refused(f"{path}:{n}: not KEY=VALUE")
        k, v = m.group(1), m.group(2)
        try:
            toks = shlex.split(v, posix=True) if v else []
        except ValueError as e:
            raise Refused(f"{path}:{n}: {k}: {e}") from None
        if len(toks) > 1 or "$" in v or "`" in v:
            raise Refused(f"{path}:{n}: {k}: one literal value per variable (no expansion, no spaces unquoted)")
        if k in env:
            raise Refused(f"{path}:{n}: {k} is set twice")
        env[k] = toks[0] if toks else ""
    return env


def check_env(env: dict[str, str]) -> list[str]:
    """The labels of the runs (RUNS); raises Refused on what cannot travel to the VM."""
    bad = []
    for k, v in env.items():
        if LAPTOP_ONLY.match(k):
            bad.append(f"{k} names a place on the laptop: unset it (the VM's launcher uses its own defaults)")
        elif TEST_HOOKS.match(k):
            bad.append(f"{k} is a test hook or a gcloud override: a paid run never carries it")
        elif v.startswith("/") or v.startswith("~"):
            bad.append(f"{k}={v} is a path on the laptop, which the VM does not have")
    if bad:
        raise Refused("the env file cannot be launched on the VM:\n  " + "\n  ".join(bad))
    labels = (env.get("RUNS") or "").split()
    if len(labels) < 2:
        raise Refused("the env file's RUNS names fewer than two runs (run606_pair.sh's own default is not taken: "
                      "the VM launches what the laptop launched, stated)")
    for x in labels:
        for v in ("RUN_NAME", "SNOWPLOW_TAG", "EXPECTED_IMAGE_DIGEST", "BENCH_606_GO"):
            if not env.get(f"{v}_{x}"):
                raise Refused(f"the env file lacks {v}_{x} (one per run of RUNS)")
    if not env.get("INSTALLER_VERSION"):
        raise Refused("the env file lacks INSTALLER_VERSION")
    if not env.get("PAIRS"):
        raise Refused("the env file lacks PAIRS")
    return labels


def pair_name(env: dict[str, str]) -> str:
    """run606_pair.sh's PAIR_NAME: "pair" then "-<RUN_NAME_L>" per label of RUNS, in order (its
    `PAIR_NAME="pair"` / `PAIR_NAME="${PAIR_NAME}-${!nm}"` lines; tests/test_vm606.py holds them to it)."""
    return "pair" + "".join(f"-{env['RUN_NAME_' + x]}" for x in env["RUNS"].split())


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _exports(env: dict[str, str]) -> str:
    return "".join(f"export {k}={shlex.quote(v)}\n" for k, v in sorted(env.items()))


_HASH_PY = ("python3 -c 'import hashlib,sys; h=hashlib.sha256(); f=open(sys.argv[1],\"rb\"); "
            "[h.update(b) for b in iter(lambda: f.read(1<<20), b\"\")]; print(h.hexdigest())'")
_TTL_RX = r"bench\.gke gke-up .*--ttl-hours ([0-9]+)"


def _head(run: str, sha: str) -> str:
    q = shlex.quote
    return (f"set -euo pipefail\nexec 2>&1\n"
            f"refuse() {{ echo \"vm606: refusing: $*\"; exit 3; }}\n"
            f"RUN={q(run)}; SHA={q(sha)}\n"
            f"W=\"$HOME/vm606/$RUN\"; B=\"$W/snowplow/go/snowplow/e2e/bench\"; V=\"$HOME/vm606/venv-$SHA\"\n")


def prepare_script(run: str, sha: str, repo: str, env: dict[str, str], pin_name: str, pin_sha: str) -> str:
    q = shlex.quote
    return _head(run, sha) + f"""\
test -s "${{{TEST_ROOT_ENV}:-}}{BOOTSTRAP_MARKER}" || refuse "the VM's bootstrap has not finished ({BOOTSTRAP_MARKER})"
mode="$({_MODE_PY} "~/{GHCR_FILE_REL}")"
[ "$mode" = 600 ] || refuse "~/{GHCR_FILE_REL} is $mode on the VM, not present with mode 600: push-secret first"
[ ! -e "$W/snowplow" ] || refuse "$W/snowplow exists: one launch per --run"
got="$({_HASH_PY} "$W/pin/"{q(pin_name)})"
[ "$got" = {q(pin_sha)} ] || refuse "the pin manifest on the VM has sha256 $got, not {pin_sha}"
git init -q "$W/snowplow"
git -C "$W/snowplow" remote add origin {q(repo)}
git -C "$W/snowplow" fetch -q --depth 1 origin "$SHA"
git -C "$W/snowplow" checkout -q --detach FETCH_HEAD
head="$(git -C "$W/snowplow" rev-parse HEAD)"
[ "$head" = "$SHA" ] || refuse "the checkout is at $head, not $SHA"
echo "vm606: bench checkout at $head"
[ -x "$V/bin/python3" ] || python3 -m venv "$V"
"$V/bin/pip" install -q -r "$B/requirements.txt" pyyaml jsonschema
export PATH="$V/bin:$PATH" USE_GKE_GCLOUD_AUTH_PLUGIN=True
{_exports(env)}cd "$B"
rm -rf "$W/dry"
if ! BENCH_606_DRY_RUN=1 PAIR_OUT="$W/dry" bash run606_pair.sh > "$W/dry.log" 2>&1; then
  tail -20 "$W/dry.log"
  refuse "the launcher's dry pass was refused ($W/dry.log)"
fi
echo "vm606: dry pass OK ($W/dry.log)"
python3 - "$W/dry" <<'PY'
import glob, re, sys
hours = []
for f in sorted(glob.glob(sys.argv[1] + "/dry-*/run.log")):
    hours += [int(m.group(1)) for m in re.finditer({_TTL_RX!r}, open(f).read())]
print("VM606_RUN_TTL_HOURS=" + (str(max(hours)) if hours else ""))
PY
"""


def launch_script(run: str, sha: str, env: dict[str, str], pin_name: str, pin_sha: str, pair: str) -> str:
    q = shlex.quote
    return _head(run, sha) + f"""\
[ -d "$B" ] || refuse "no checkout at $B: the prepare step did not run"
export PATH="$V/bin:$PATH" USE_GKE_GCLOUD_AUTH_PLUGIN=True
cd "$B"
PO="$(python3 -m bench.evidence run-dir {q(pair)})"
mkdir -p "$PO"
cp "$W/pin/"{q(pin_name)} "$PO/"{q(pin_name)}
got="$({_HASH_PY} "$PO/"{q(pin_name)})"
[ "$got" = {q(pin_sha)} ] || refuse "the pin manifest in $PO has sha256 $got, not {pin_sha}"
(umask 077; cat > "$W/launch-env.sh" <<'ENV'
{_exports(env)}ENV
)
{_exports(env)}python3 -m bench.detach --log "$W/launch.log" --pidfile "$W/launch.pid" -- bash run606_pair.sh
echo "VM606_PAIR_OUT=$PO"
"""


def cmd_launch(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    if not SHA_PATTERN.fullmatch(a.bench_sha or ""):
        raise Refused(f"--bench-sha {a.bench_sha!r}: the full 40-hex commit (copy it, never type it)")
    if not HEX64.fullmatch(a.pin_sha256 or ""):
        raise Refused("--pin-sha256: 64 lowercase hex")
    env = read_env_file(a.env_file)
    labels = check_env(env)
    pin = Path(a.pin_manifest)
    if not pin.is_file():
        raise Refused(f"{pin} does not exist")
    got = _sha256(str(pin))
    if got != a.pin_sha256:
        raise Refused(f"{pin} has sha256 {got}, not {a.pin_sha256}")
    _say(f"pin manifest {pin.name}: sha256 {got} (verified on the laptop)")
    pair = pair_name(env)
    names = [env[f"RUN_NAME_{x}"] for x in labels]
    prep = prepare_script(run, a.bench_sha, a.repo_url, env, pin.name, got)
    go = launch_script(run, a.bench_sha, env, pin.name, got, pair)
    if not a.yes:
        _say(f"DRY: on {name}: mkdir ~/vm606/{run}/pin")
        scp(str(pin), f"{name}:vm606/{run}/pin/{pin.name}", a.project, a.zone, dry=True)
        _say(f"DRY: on {name}: clone {a.repo_url} at {a.bench_sha}, venv, the launcher's dry pass; then, if the VM "
             f"outlives the run's TTL + {LAUNCH_MARGIN_S} s: the pin manifest into the pair's run directory and "
             f"python3 -m bench.detach --log ~/vm606/{run}/launch.log --pidfile ~/vm606/{run}/launch.pid -- bash "
             f"run606_pair.sh, with {len(env)} variables: {', '.join(sorted(env))}")
        _say(f"DRY: runs {', '.join(names)}; pair {pair}; nothing was started")
        return 0
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    rc, out, err = ssh(name, a.project, a.zone, f"set -euo pipefail\nmkdir -p \"$HOME/vm606/{run}/pin\"\n", timeout=300)
    if rc != 0:
        raise _failed("mkdir on the VM", rc, out, err)
    rc, out, err = scp(str(pin), f"{name}:vm606/{run}/pin/{pin.name}", a.project, a.zone)
    if rc != 0:
        raise Refused(f"scp of the pin manifest failed (rc={rc}; {error_summary(err)})")
    rc, out, err = ssh(name, a.project, a.zone, prep, timeout=a.prepare_timeout_s)
    for ln in _remote_lines(out):
        if ln.startswith("vm606:"):
            _say(ln)
    if rc != 0:
        raise _failed("the VM's prepare step", rc, out, err)
    ttl = _marker(out, "VM606_RUN_TTL_HOURS")
    if not (ttl or "").isdigit():
        raise Refused("the dry pass handed gke-up no --ttl-hours (no `bench.gke gke-up ... --ttl-hours N` in any "
                      f"dry-*/run.log under ~/vm606/{run}/dry): how long the run lasts is unknown")
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    left = _time_left_s(inst, time.time())
    need = int(ttl) * 3600 + LAUNCH_MARGIN_S
    if left is None or left < need:
        raise Refused(f"{name} is deleted in {'an unknown time' if left is None else f'{left / 3600:.1f} h'}; the run's "
                      f"TTL is {ttl} h and the launch needs {need / 3600:.1f} h with the margin: `down` it and `up` "
                      f"with --ttl-hours >= {-(-need // 3600)}")
    _say(f"the run's TTL {ttl} h + margin fits the VM's {left / 3600:.1f} h left")
    rc, out, err = ssh(name, a.project, a.zone, go, timeout=600)
    for ln in _remote_lines(out):
        if ln.startswith("vm606:") or ln.startswith("detached:"):
            _say(ln)
    if rc != 0:
        raise _failed("the detached launch", rc, out, err)
    _save_state(run, launched_at=time.time(), bench_sha=a.bench_sha, runs=names, pair=pair,
                pair_out=_marker(out, "VM606_PAIR_OUT"), pin_manifest=pin.name, pin_sha256=got, run_ttl_hours=int(ttl),
                fetched=False)
    _say(f"launched on {name}: `python3 -m bench.vm606 status --run {run}`; the log is ~/vm606/{run}/launch.log there")
    return 0


# ─── status ─────────────────────────────────────────────────────────────────

def _alive_script(run: str) -> str:
    """Defines launcher_state (running <pid> | exited | never) and prints it as VM606_LAUNCHER."""
    return (f"W=\"$HOME/vm606/{run}\"\n"
            "launcher_state() {\n"
            "  if [ -s \"$W/launch.pid\" ] && kill -0 \"$(cat \"$W/launch.pid\")\" 2>/dev/null; then\n"
            "    echo \"running $(cat \"$W/launch.pid\")\"\n"
            "  elif [ -s \"$W/launch.pid\" ]; then echo exited\n"
            "  else echo never\n  fi\n}\n"
            "echo \"VM606_LAUNCHER=$(launcher_state)\"\n")


def cmd_status(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    left = _time_left_s(inst, time.time())
    script = _alive_script(run) + f"echo ---\ntail -n {int(a.lines)} \"$W/launch.log\" 2>/dev/null || true\n"
    rc, out, err = ssh(name, a.project, a.zone, script, timeout=120)
    if rc != 0:
        raise _failed("reading the launcher's state", rc, out, err)
    _say(f"{name}: {inst.get('status')}, deleted in "
         f"{'an unknown time' if left is None else f'{left / 3600:.1f} h'}; launcher "
         f"{_marker(out, 'VM606_LAUNCHER')}")
    body = out.split("---\n", 1)[1] if "---\n" in out else ""
    for ln in _remote_lines(body):
        _say(ln)
    return 0


# ─── fetch ──────────────────────────────────────────────────────────────────

def fetch_script(run: str, sha: str, names: list[str]) -> str:
    q = shlex.quote
    return _head(run, sha) + _alive_script(run) + f"""\
case "$(launcher_state)" in running*) refuse "the launcher is still running: fetch once it has exited";; esac
D="$("$V/bin/python3" -c 'import sys; sys.path.insert(0, sys.argv[1]); from bench import evidence; print(evidence.data_root())' "$B")"
members=""
for n in {" ".join(q(n) for n in names)}; do
  for t in runs evidence; do
    if [ -d "$D/$t/{evidence.CAMPAIGN}/$n" ]; then members="$members $t/{evidence.CAMPAIGN}/$n"; fi
  done
done
[ -n "$members" ] || refuse "no run or evidence directory of {", ".join(names)} under $D"
rm -rf "$W/fetch"; mkdir -p "$W/fetch/vm606/{run}"
for f in launch.log dry.log launch-env.sh; do
  if [ -f "$W/$f" ]; then cp "$W/$f" "$W/fetch/vm606/{run}/$f"; fi
done
# the names are the bench's own (snowplow-bench-*, pair-...): no blank in any, word splitting is safe.
# COPYFILE_DISABLE: a bsdtar (macOS) would add AppleDouble ._ members; GNU tar ignores it
COPYFILE_DISABLE=1 tar -cf "$W/fetch.tar" -C "$D" $members -C "$W/fetch" "vm606/{run}"
echo "VM606_FETCH_SHA256=$({_HASH_PY} "$W/fetch.tar")"
echo "VM606_FETCH_MEMBERS=$members"
"""


def _dest_of(rel: str, run: str) -> Path | None:
    """Where a member of the fetch tar lands on the laptop."""
    parts = Path(rel).parts
    if len(parts) == 3 and parts[0] == "runs" and parts[1] == evidence.CAMPAIGN:
        return evidence.run_dir(parts[2])
    if len(parts) == 3 and parts[0] == "evidence" and parts[1] == evidence.CAMPAIGN:
        return evidence.evidence_dir(parts[2])
    if parts[:2] == ("vm606", run) and len(parts) == 2:
        return state_dir(run) / "vm"
    return None


def cmd_fetch(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    st = _state(run)
    names = [*st.get("runs", []), st["pair"]] if st.get("pair") else []
    if not names:
        raise Refused(f"no launch of {run} is recorded on this laptop ({state_dir(run) / 'state.json'})")
    if st.get("fetched"):
        raise Refused(f"{run} was fetched already ({st.get('fetch_sha256')}); its directories are in place")
    for n in names:
        for dest in (evidence.run_dir(n), evidence.evidence_dir(n)):
            if dest.exists():
                raise Refused(f"{dest} exists on the laptop: a fetch never overwrites a run")
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    rc, out, err = ssh(name, a.project, a.zone, fetch_script(run, st["bench_sha"], names), timeout=3600)
    if rc != 0:
        raise _failed("packing the run on the VM", rc, out, err)
    want = _marker(out, "VM606_FETCH_SHA256") or ""
    members = (_marker(out, "VM606_FETCH_MEMBERS") or "").split()
    if not HEX64.fullmatch(want):
        raise Refused("the VM reported no sha256 for the fetch archive")
    d = state_dir(run)
    staging = d / "staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    tar = d / "fetch.tar"
    rc, _o, err = scp(f"{name}:vm606/{run}/fetch.tar", str(tar), a.project, a.zone)
    if rc != 0:
        raise Refused(f"scp of the fetch archive failed (rc={rc}; {error_summary(err)})")
    got = _sha256(str(tar))
    if got != want:
        raise Refused(f"the fetch archive arrived with sha256 {got}, the VM's is {want}: nothing was moved")
    _say(f"fetch archive: sha256 {got} (the VM's), {tar.stat().st_size} bytes")
    allowed = {f"{t}/{evidence.CAMPAIGN}/{n}" for t in ("runs", "evidence") for n in names} | {f"vm606/{run}"}
    with tarfile.open(tar) as tf:
        for m in tf.getmembers():
            parts = Path(m.name).parts
            if not any("/".join(parts[:k]) in allowed for k in (2, 3)):
                raise Refused(f"the fetch archive holds {m.name!r}, outside the run's directories: nothing was moved")
        tf.extractall(staging, filter="data")
    problems = []
    for n in names:
        ev = staging / "evidence" / evidence.CAMPAIGN / n
        if ev.is_dir():
            bad = evidence.verify(ev, "final")
            problems += bad
            _say(f"evidence {n}: {'NOT verified' if bad else 'verified against its MANIFEST.json (stage final)'}")
        elif (staging / "runs" / evidence.CAMPAIGN / n).is_dir():
            _say(f"evidence {n}: none on the VM (a run that never reached a cluster publishes none)")
        else:
            _say(f"{n}: nothing on the VM")
    if problems:
        raise Refused("the evidence did not verify; nothing was moved (staging: "
                      f"{staging}):\n  " + "\n  ".join(problems[:evidence.MAX_NAMED]))
    moved = []
    for rel in members + [f"vm606/{run}"]:
        src, dest = staging / rel, _dest_of(rel, run)
        if not src.exists() or dest is None:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
        moved.append(str(dest))
    shutil.rmtree(staging)
    tar.unlink()
    _save_state(run, fetched=True, fetch_sha256=got, fetched_at=time.time(), fetched_to=moved)
    for m in moved:
        _say(f"in place: {m}")
    return 0


# ─── down ───────────────────────────────────────────────────────────────────

def _disk_names(inst: dict) -> list[str]:
    return sorted(str(d.get("source", "")).rsplit("/", 1)[-1] for d in inst.get("disks") or [] if d.get("source"))


def cmd_down(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}" + (" (nothing to delete)" if why == "notfound" else ""))
    _owned(inst, run)
    rc, out, err = gcloud(["compute", "instances", "list", "--project", a.project,
                           "--filter", f"labels.owner={OWNER} AND labels.run={run}", "--format", "json"],
                          env=LIST_ENV, quiet=True)
    try:
        listed = json.loads(out) if rc == 0 else None
    except ValueError:
        listed = None
    if not isinstance(listed, list):
        raise Refused(f"the instances labelled owner={OWNER}, run={run} could not be listed (rc={rc}; "
                      f"{error_summary(err)}): refusing")
    if sorted(i.get("name") for i in listed) != [name]:
        raise Refused(f"the label run={run} selects {sorted(i.get('name') for i in listed)}, not [{name}]: refusing")
    st = _state(run)
    if st.get("launched_at") and not st.get("fetched") and not a.discard_unfetched:
        raise Refused(f"{run} was launched and not fetched: `fetch` first (--discard-unfetched lets its evidence go "
                      f"with the VM)")
    if not a.skip_launcher_check:
        rc, out, err = ssh(name, a.project, a.zone, _alive_script(run), timeout=120)
        state = _marker(out, "VM606_LAUNCHER")
        if rc != 0 or not state:
            raise Refused(f"whether the launcher still runs on {name} could not be read (rc={rc}; {error_summary(err)}); "
                          f"--skip-launcher-check deletes it anyway")
        if state.startswith("running"):
            raise Refused(f"the launcher is still running on {name} ({state}): deleting the VM would kill every run's "
                          f"teardown; stop it (kill -TERM <pid> on the VM) and wait for it to exit")
    disks = _disk_names(inst)
    rc, out, err = gcloud(["compute", "instances", "delete", name, *_where(a.project, a.zone),
                           "--delete-disks", "all", "--quiet"], dry=not a.yes)
    if not a.yes:
        _say(f"DRY: would verify {name} answers NotFound and its disks {disks} are gone; nothing was deleted")
        return 0
    if rc != 0:
        raise Refused(f"instances delete failed (rc={rc}; {error_summary(err)})")
    gone, why = _describe(name, a.project, a.zone)
    if gone is not None or why != "notfound":
        raise Refused(f"{name} is not verified gone after the delete ({why or 'it still answers'})")
    flt = " OR ".join(f"name={d}" for d in disks) or f"name={name}"
    rc, out, err = gcloud(["compute", "disks", "list", "--project", a.project, "--filter", flt, "--format", "json"],
                          env=LIST_ENV, quiet=True)
    try:
        left = json.loads(out) if rc == 0 else None
    except ValueError:
        left = None
    if left is None:
        raise Refused(f"{name} is gone but its disks {disks} could not be listed (rc={rc}; {error_summary(err)}): "
                      f"whether the pull-token file went with them is unknown")
    if left:
        raise Refused(f"{name} is gone but disk(s) {[d.get('name') for d in left]} remain: delete them by hand "
                      f"(gcloud compute disks delete ... --zone {a.zone}); the pull-token file is on one of them")
    _save_state(run, deleted_at=time.time())
    _say(f"{name} deleted and verified gone; its disk(s) {disks} too, and with them ~/{GHCR_FILE_REL}")
    return 0


# ─── CLI ────────────────────────────────────────────────────────────────────

def _zone(v: str) -> str:
    if not re.fullmatch(ZONE_PATTERN, v):
        raise argparse.ArgumentTypeError(f"{v!r} is not a zone (e.g. {DEFAULT_ZONE})")
    return v


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.vm606", description="the paired #606 launch from a disposable GCE VM "
                                                                 "(see the module doc)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, *, yes=True):
        p.add_argument("--run", required=True, help=f"the launch's name; the VM is {VM_PREFIX}<run>")
        p.add_argument("--project", default=DEFAULT_PROJECT)
        p.add_argument("--zone", default=DEFAULT_ZONE, type=_zone)
        if yes:
            p.add_argument("--yes", action="store_true", help="actually do it (default: dry run)")

    p = sub.add_parser("plan-iam", help="the orchestrator SA's roles, derived from the code (prints only)")
    p.add_argument("--project", default=DEFAULT_PROJECT)
    p.add_argument("--project-number", default=None, help="default: read with gcloud projects describe")
    p.add_argument("--bench-dir", default=str(BENCH_DIR), help=argparse.SUPPRESS)
    p.set_defaults(func=cmd_plan_iam)

    p = sub.add_parser("up", help="create the VM and wait for its bootstrap")
    common(p)
    p.add_argument("--ttl-hours", type=int, required=True,
                   help="the VM deletes itself after this: the run's TTL + the hours fetch may wait")
    p.add_argument("--bootstrap-timeout-s", type=int, default=1200)
    p.add_argument("--poll-s", type=float, default=20, help=argparse.SUPPRESS)
    p.set_defaults(func=cmd_up)

    p = sub.add_parser("push-secret", help="copy the ghcr pull-token file to the VM (never read)")
    common(p)
    p.set_defaults(func=cmd_push_secret)

    p = sub.add_parser("launch", help="clone at the bench SHA and start the paired launch, detached")
    common(p)
    p.add_argument("--bench-sha", required=True)
    p.add_argument("--env-file", required=True, help="the laptop launcher's variables, KEY=VALUE per line")
    p.add_argument("--pin-manifest", required=True)
    p.add_argument("--pin-sha256", required=True)
    p.add_argument("--repo-url", default=REPO_URL, help=argparse.SUPPRESS)
    p.add_argument("--prepare-timeout-s", type=int, default=3600)
    p.set_defaults(func=cmd_launch)

    p = sub.add_parser("status", help="the launcher's log tail, its liveness, the VM's time left")
    common(p, yes=False)
    p.add_argument("--lines", type=int, default=40)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("fetch", help="copy the run and evidence directories back, verified")
    common(p, yes=False)
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("down", help="delete the VM and its disk, verified")
    common(p)
    p.add_argument("--discard-unfetched", action="store_true")
    p.add_argument("--skip-launcher-check", action="store_true")
    p.set_defaults(func=cmd_down)

    a = ap.parse_args(argv)
    try:
        return a.func(a)
    except Refused as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
