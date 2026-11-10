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
    python3 -m bench.vm606 up --run R --ttl-hours H [--yes]  # grants, firewall, the VM; waits for its bootstrap
    python3 -m bench.vm606 push-secret --run R [--yes]       # the ghcr pull-token file, as an opaque file
    python3 -m bench.vm606 launch --run R --bench-sha SHA --env-file F \\
        --pin-manifest M --pin-sha256 HEX [--yes]            # clone at SHA, dry pass, start detached
    python3 -m bench.vm606 status --run R [--lines N]        # the launcher's log, its liveness, the VM's time left
    python3 -m bench.vm606 fetch --run R                     # run dirs + evidence back, hashes verified
    python3 -m bench.vm606 down --run R [--yes]              # VM, disk, firewall rules, grants: deleted, verified

Every subcommand that changes something in GCP prints its gcloud argv and runs none of them unless
--yes (as bench.gke). status and fetch create nothing in GCP (an SSH and a copy).

IAM: ACCESS EXPIRES WITH EACH RUN (Diego's ruling, 2026-11-10). Between runs the orchestrator SA holds
nothing. `up` grants it the roles plan-iam derives (the project roles, and iam.serviceAccountUser ON
the GKE node SA), each binding with the IAM condition title "snowplow-bench-<R>", expression
`request.time < timestamp("<the VM's --termination-time>")`, BEFORE the VM exists, and reads both
policies back to verify each binding with that exact condition. `down` removes exactly those bindings
(by that title and that expression form; never an unconditional binding, never another run's) and
verifies they are gone. If the VM's TTL fires first, the grants have already expired with it.
plan-iam SCANS the code for the orchestrator's GCP calls (every `["gcloud", ...]` argv in bench/*.py,
the Cloud Quotas REST read, kubectl / helm against the bench cluster), maps each to the predefined
role that holds its permission, and refuses (exit 1, naming the call) when the code makes a call it
does not cover; `up` grants from the same derivation, of THIS checkout: run vm606 from a checkout at
(or after) the bench commit the VM runs. This module's own calls are not in the scan: they run on the
laptop as the operator (roles/owner covers them: plan-iam lists them). Nor is the in-cluster
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
    repository, git, python3-venv. The bench's Python deps go into a venv per run at launch, from
    the bench commit's requirements.txt (as CI installs them).
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
  - SSH THROUGH IAP ONLY. The project's default-allow-ssh admits tcp:22 from 0.0.0.0/0 to every
    instance (no target tags), so the VM carries the network tag vm_name(R) and `up` makes two rules
    for that tag before the VM exists: <vm>-deny-ssh (DENY tcp:22 from 0.0.0.0/0, priority 1000, which
    beats default-allow-ssh's 65534) and <vm>-iap-ssh (ALLOW tcp:22 from IAP's 35.235.240.0/20,
    priority 900). Firewall rules carry no labels: their description says owner and run, and `down`
    deletes only rules with exactly the expected name, target tag and description. OS Login
    (enable-oslogin=TRUE, project keys blocked). `up` refuses while iap.googleapis.com is not enabled.
  - A failed `up` says so and names `down`, which removes whatever up made (grants, rules, VM): the
    SSH wait reads every failure (IAP / firewall / OS Login / ssh) and stops at once on one that
    waiting cannot fix, never as a silent timeout.

ON THE VM, NOTHING NAMES THE OPERATOR. An OS Login home is /home/<the operator's account, as a POSIX
name>, so no remote path this module prints or stores may sit under it. Every remote step runs in
/srv/snowplow-bench/<R> (VM_ROOT, made by the startup script, sticky like /tmp; the run's own
directory mode 0700) with HOME=/srv/snowplow-bench/<R>/home: the launcher's data root, its run and
evidence directories, the pull-token file and every path in its logs live there. USER and LOGNAME are
"snowplow-bench" there too: bench.gke labels each cluster owner=$USER. Each remote script
reaches bash on stdin wrapped in one function called with stdin /dev/null: bash has read all of it
before any of it runs, and no child can read the rest of the script.

THE PULL TOKEN (push-secret). ~/.config/snowplow-bench/ghcr-pull.env goes to the same path under the
launcher's HOME on the VM (/srv/snowplow-bench/<R>/home/.config/snowplow-bench/ghcr-pull.env), mode
0600 in a 0700 directory, by `gcloud compute scp`. This module never opens it: it stats it (presence,
regular file, mode 0600) and hands its path to scp; on the VM it checks presence and mode the same
way. It lives on the boot disk only, so it goes with the VM (down verifies the disk is gone).

LAUNCH. On the VM: clone krateo-platformops/snowplow at exactly --bench-sha (HEAD verified after the
checkout: an annotated tag's object id fetches but checks out its commit), the venv, the pin manifest
(copied by scp, its sha256 checked on the laptop, on the VM, and again in the pair's run directory,
where the laptop launch kept it, so it is published with the pair's evidence, right before the
launch), then the launcher's own dry pass (BENCH_606_DRY_RUN=1, its own PAIR_OUT), then the paid
launch: `python3 -m bench.detach --log <R>/launch.log --pidfile <R>/launch.pid -- bash
run606_pair.sh`. "Launched" is said only once every run has passed its paid-only checks, read from
the launcher's own files: each run's run.log holds its "run budget:" line (run606.sh logs it after the
BENCH_606_GO date, the OUT and evidence preflights and the host-load gate), and a refusal ("refusing:"
in a run's console, or the launcher exiting: a dirty tree, a PAIR_OUT that holds a pair) is reported
as one. The env file holds the laptop launcher's variables, one KEY=VALUE per line (RUNS, PAIRS,
REPLICA_PAIR, RUN_NAME_<L>, SNOWPLOW_TAG_<L>, EXPECTED_IMAGE_DIGEST_<L>, BENCH_606_GO_<L>,
BENCH_1004_GO_<L>, INSTALLER_VERSION, ...): passed as is, run606_pair.sh refuses what it refuses.
Refused here, before any SSH: a variable naming a place on the laptop (OUT_<L>, PAIR_OUT, the data
roots, HOME, PATH, KUBECONFIG, GHCR_PULL_ENV_FILE, any value that is an absolute or ~ path), the pull
token's own keys (GHCR_PULL_*: it travels only as the file), the test hooks (BENCH_606_DRY_RUN,
BENCH_606_FAIL_AT, BENCH_606_HOST_LOAD) and gcloud overrides (CLOUDSDK_*).

FETCH. Once the launcher has exited (refused while it runs): one tar on the VM of every run's
directory and evidence directory and the pair's, its sha256 checked after the copy, unpacked beside
the laptop's trees, every evidence directory verified against its own MANIFEST.json
(bench.evidence.verify, stage final), and only then moved into ~/.local/share/snowplow-bench/
{runs,evidence}/606/; a directory the laptop already holds is never overwritten. No bucket: fetch
needs no role beyond SSH.

SELECTION. vm606 passes no `--filter` to gcloud. gcloud 561 parses `name ~ ^(a|b)$` into `name eq
".*(^).*"` and answers an empty list with rc 0 (#1307 re-review N1, --log-http), and `labels.x=y` is
applied client-side by matching rules gcloud says are changing: a filter can fail open. Each resource
is read by its exact name (`describe`: NotFound is "absent", any other failure refuses), or selected by
exact equality from one unfiltered list (the project's instances, every zone, partial-error property
false; the enabled services).

A RUN NAME IS USED ONCE. `up` refuses, before granting anything, a --run of which anything exists: its
laptop record, an instance named or labelled for it in any zone, its firewall rules, bindings under
its condition title; and it refuses while the orchestrator SA holds any unconditional binding (on the
project or the node SA): between runs it holds nothing.

KUBERNETES API. Whether GKE's Kubernetes-API authorizer honours an IAM condition on request.time is not
in GKE's documentation (IAM's attribute reference: request.time "is supported for all Google Cloud
services and resource types"; GKE's IAM page does not mention conditions). run606.sh asks right after
the first cluster exists (kubectl auth can-i create clusterrolebindings, a SelfSubjectAccessReview the
IAM authorizer answers) and stops by name, tearing down, when the answer is no.

DOWN. Checks everything before deleting anything: the instance (if GCE has not deleted it at its
TTL) labelled owner=snowplow-bench, run=<R> and named vm_name(R), the only one the label selects; no
launcher running; the launch fetched (--discard-unfetched says the evidence may go); the firewall
rules and the grants its own. Then deletes the instance with --delete-disks=all, the two rules and
the run's conditional grants, and reads each back: the instance answers NotFound, its disks list as
gone (the pull-token file lived on that disk only), no rule and no binding of the run is left.

PARALLEL RUNS, ANY REGION. A launch is one --run: its VM, labels, firewall rules, IAM condition
title, VM directory and laptop record are all named after it, and every subcommand selects by exact
name (never by prefix), so two launches can go at once (run 24 in europe-west4, run 25 in
europe-west1: a region holds one paired launch, ClustersPerRegion = 3) and `down` / `fetch` of one
never touch the other. The VM's zone (--zone, here and on every subcommand) and the bench clusters'
zone (launch --bench-zone, handed to run606_pair.sh as ZONE) are separate. `up` reads the VM's own
footprint against the quotas of the region it sits in (INSTANCES 1, E2_CPUS 8, SSD_TOTAL_GB 50,
IN_USE_ADDRESSES 1, counted from the region's instances / forwarding rules / addresses as the bench's
pre-check counts them); once the VM exists, the bench's own pre-check counts it among the instances
and external IPs of its region when the clusters share it.

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
    python3 -m bench.vm606 launch --run run25 --bench-sha <40-hex> --env-file run25.env \\
        --bench-zone europe-west1-b --pin-manifest .../installer-0.3.545-pin-manifest.json \\
        --pin-sha256 1e79f6a7e611d80be2c878694b1adc84038b4f7f181cf162bc39c6af5587c72b --yes
    python3 -m bench.vm606 status --run run25          # as often as wanted; the run survives any disconnect
    python3 -m bench.vm606 fetch --run run25           # once status says the launcher exited
    python3 -m bench.vm606 down --run run25 --yes

Stopping a paid run on the VM: `gcloud compute ssh <vm> --tunnel-through-iap -- 'kill -TERM "$(cat
/srv/snowplow-bench/<R>/launch.pid)"'` (run606_pair.sh forwards it to every run; each tears its
cluster down). A detached launcher survives the SSH session ending (bench.detach: its own session,
SIGHUP ignored; tests/test_vm606.py hangs up the session's process group and checks).

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

from bench import evidence, gke
from bench.gke import DEFAULT_PROJECT, DEFAULT_ZONE, ZONE_PATTERN
from bench.identity import error_summary, redact_identity

BENCH_DIR = Path(__file__).resolve().parent.parent

# ─── what the VM is ─────────────────────────────────────────────────────────

SA_NAME = "snowplow-bench-orchestrator"
VM_PREFIX = "snowplow-bench-orch-"
OWNER = "snowplow-bench"
MACHINE_TYPE = "e2-standard-8"
VM_VCPUS = 8                           # e2-standard-8: in E2_CPUS, the family's own regional quota
BOOT_DISK_GB = 50
BOOT_DISK_TYPE = "pd-balanced"
IMAGE_FAMILY, IMAGE_PROJECT = "debian-13", "debian-cloud"
NETWORK = "default"
IAP_RANGE = "35.235.240.0/20"          # IAP's TCP-forwarding source range
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

# on the VM: identity-free paths only (the module doc)
VM_ROOT = "/srv/snowplow-bench"
BOOTSTRAP_MARKER = f"{VM_ROOT}/.bootstrap/bootstrap.json"
GHCR_FILE_REL = ".config/snowplow-bench/ghcr-pull.env"    # bench.ghcr_pull.DEFAULT_ENV_FILE, from ~

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


def vm_dir(run: str) -> str:
    """The run's directory on the VM; HOME of every remote step is its home/."""
    return f"{VM_ROOT}/{run}"


def sa_email(project: str) -> str:
    return f"{SA_NAME}@{project}.iam.gserviceaccount.com"


def check_run(run: str) -> str:
    if not RUN_PATTERN.fullmatch(run or ""):
        raise Refused(f"--run {run!r}: lowercase letters, digits and '-', starting with a letter, at most 32 "
                      f"(it names the VM, {VM_PREFIX}<run>, and its run label)")
    return run


def down_hint(run: str) -> str:
    return f"`python3 -m bench.vm606 down --run {run} --yes` removes what up made (grants, firewall rules, VM)"


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
    """One gcloud call: (rc, stdout, stderr). Dry: printed, not run. Its stdout and stderr are the
    caller's (stderr names the account; an IAM call's stdout is the whole policy): never printed here."""
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


def remote(run: str, body: str, *, in_dir: bool = True) -> str:
    """A remote script. Wrapped in main(), called with stdin /dev/null: bash reads the whole function
    before running any of it, and no child can read the rest of the script. HOME is the run's
    directory under VM_ROOT, so no path names the login user's home; USER / LOGNAME are the bench's,
    not the login user's (bench.gke labels each cluster owner=$USER)."""
    head = ("set -euo pipefail\nexec 2>&1\n"
            "refuse() { echo \"vm606: refusing: $*\"; exit 3; }\n"
            f"R={shlex.quote(vm_dir(run))}\nexport HOME=\"$R/home\" USER={OWNER} LOGNAME={OWNER}\n")
    head += ('[ -d "$R" ] || refuse "no $R on the VM: push-secret makes it"\ncd "$R"\n' if in_dir else "cd /\n")
    return "main() {\n" + head + body + "}\nmain </dev/null\n"


def ssh(name: str, project: str, zone: str, script: str, *, timeout: float = 1800) -> tuple[int, str, str]:
    """Run `script` (remote()) with bash on the VM, OS Login through IAP."""
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


# gcloud compute ssh --tunnel-through-iap: which stage refused, from its stderr (never printed: an OS
# Login refusal names the POSIX user). (pattern, stage, waiting can fix it)
SSH_STAGES = (
    (re.compile(r"\b4033\b|not authorized"), "IAP: the tunnel was refused (iap.tunnelInstances.accessViaIAP)", False),
    (re.compile(r"\b4047\b|\b4049\b|iap\.googleapis\.com"), "IAP: the API refused the tunnel", False),
    (re.compile(r"\b4003\b|failed to connect to backend"),
     "firewall / sshd: IAP reached the network, port 22 did not answer (the VM may still be booting)", True),
    (re.compile(r"Permission denied \(publickey"), "OS Login: the key was refused (compute.osAdminLogin)", False),
    (re.compile(r"Could not fetch resource|was not found|code=404"), "the instance: GCE does not know it", False),
    (re.compile(r"Connection (refused|reset|closed|timed out)|timed out after|kex_exchange"),
     "ssh: the connection failed (the VM may still be booting)", True),
)


def ssh_stage(rc: int, err: str) -> tuple[str, bool]:
    for rx, stage, transient in SSH_STAGES:
        if rx.search(err or ""):
            return stage, transient
    return f"ssh: rc={rc}, {error_summary(err)}", True


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


def _project_number(project: str) -> str:
    rc, out, err = gcloud(["projects", "describe", project, "--format", "value(projectNumber)"], quiet=True)
    number = out.strip()
    if rc != 0 or not number.isdigit():
        raise Refused(f"the project number of {project} could not be read (rc={rc}; {error_summary(err)}): the node "
                      f"SA is <number>-compute@developer.gserviceaccount.com")
    return number


def node_sa(number: str) -> str:
    return f"{number}-compute@developer.gserviceaccount.com"


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
        "write RBAC: container.admin is the one predefined role that holds both. IT IS PROJECT-WIDE: it "
        "covers every GKE cluster in the project, krateo-057 included (GKE grants no per-cluster IAM role); "
        "bounded in time only: up grants it under the run's condition, expiring with the VM."),
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
    "firewall roles for the SA: no gcloud firewall call in the bench code (GKE's own service agent writes the "
    "k8s-fw-* rules); the VM's two SSH rules are the operator's (up / down)",
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


# ─── the run's grants: conditional, expiring with the VM ────────────────────

OPERATOR_NEEDS = (
    "resourcemanager.projects.setIamPolicy and iam.serviceAccounts.setIamPolicy (on the node SA): up grants the "
    "run's conditional bindings, down removes them",
    "compute.securityAdmin: up makes and down deletes the run's two SSH firewall rules (IAP only)",
    "compute.instanceAdmin.v1: the VM itself",
    "iam.serviceAccountUser on the orchestrator SA: the VM runs as it",
    "iap.tunnelResourceAccessor and compute.osAdminLogin: SSH and scp to the VM through IAP",
)


def iam_condition(run: str, terminates_at: str) -> dict:
    """The condition every grant of `run` carries: it ends no later than the VM's own auto-delete."""
    return {"title": f"snowplow-bench-{run}",
            "expression": f'request.time < timestamp("{terminates_at}")',
            "description": f"vm606: expires with {vm_name(run)} (owner={OWNER}, run={run})"}


_EXPIRY_FORM = re.compile(r'request\.time < timestamp\("\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ"\)')


def grant_targets(project: str, number: str, roles: list[str]) -> list[tuple[str, str, str]]:
    """(kind, resource, role): each project role on the project, iam.serviceAccountUser on the node SA."""
    return ([("project", project, r) for r in roles if r != NODE_SA_ROLE]
            + ([("sa", node_sa(number), NODE_SA_ROLE)] if NODE_SA_ROLE in roles else []))


def _binding_argv(verb: str, kind: str, resource: str, project: str, role: str, cond_file: str) -> list[str]:
    member = f"serviceAccount:{sa_email(project)}"
    head = (["projects", f"{verb}-iam-policy-binding", resource] if kind == "project"
            else ["iam", "service-accounts", f"{verb}-iam-policy-binding", resource, "--project", project])
    return [*head, "--member", member, "--role", role, "--condition-from-file", cond_file]


def _policy(kind: str, resource: str, project: str) -> dict:
    argv = (["projects", "get-iam-policy", resource, "--format", "json"] if kind == "project"
            else ["iam", "service-accounts", "get-iam-policy", resource, "--project", project, "--format", "json"])
    rc, out, err = gcloud(argv, quiet=True)
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    if not isinstance(got, dict):
        raise Refused(f"the IAM policy of {resource} could not be read (rc={rc}; {error_summary(err)})")
    return got


def run_bindings(policy: dict, project: str, run: str) -> list[tuple[str, dict]]:
    """(role, condition) of every binding of the orchestrator SA under `run`'s condition title."""
    member = f"serviceAccount:{sa_email(project)}"
    title = f"snowplow-bench-{run}"
    return [(b.get("role"), b.get("condition") or {}) for b in policy.get("bindings") or []
            if member in (b.get("members") or []) and (b.get("condition") or {}).get("title") == title]


def _cond_file(run: str, cond: dict, tag: str) -> str:
    d = state_dir(run)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"iam-condition-{tag}.json"
    p.write_text(json.dumps(cond, indent=1, sort_keys=True) + "\n")
    return str(p)


def grant(run: str, project: str, number: str, roles: list[str], terminates_at: str, *, dry: bool) -> None:
    cond = iam_condition(run, terminates_at)
    f = _cond_file(run, cond, "grant")
    targets = grant_targets(project, number, roles)
    for kind, res, role in targets:
        rc, _out, err = gcloud(_binding_argv("add", kind, res, project, role, f), dry=dry)
        if rc != 0:
            raise Refused(f"granting {role} on {res} failed (rc={rc}; {error_summary(err)}); {down_hint(run)}")
    if dry:
        _say(f"DRY: would read both policies back and verify {len(targets)} bindings under the condition {cond}")
        return
    policies = {(k, r): _policy(k, r, project) for k, r in {(k, r) for k, r, _ in targets}}
    for kind, res, role in targets:
        held = [c for r, c in run_bindings(policies[(kind, res)], project, run) if r == role]
        if not any(c.get("expression") == cond["expression"] and c.get("title") == cond["title"] for c in held):
            raise Refused(f"{role} on {res} is not held under the condition {cond['expression']!r} after the grant; "
                          f"{down_hint(run)}")
    _say(f"granted and read back: {len(targets)} bindings of {sa_email(project)}, each under condition "
         f"{cond['title']}: {cond['expression']}")


def revoke(run: str, project: str, number: str, *, dry: bool) -> int:
    """Remove the run's conditional bindings (exactly; never another's, never an unconditional one), then
    verify none is left. Returns how many were removed."""
    found = []
    for kind, res in (("project", project), ("sa", node_sa(number))):
        for role, cond in run_bindings(_policy(kind, res, project), project, run):
            if not _EXPIRY_FORM.fullmatch(cond.get("expression") or ""):
                raise Refused(f"a binding of {role} on {res} carries the title {cond.get('title')!r} but not the "
                              f"expiry form vm606 writes ({cond.get('expression')!r}): refusing to remove it")
            found.append((kind, res, role, cond))
    for i, (kind, res, role, cond) in enumerate(found):
        f = _cond_file(run, {k: cond[k] for k in ("title", "expression", "description") if k in cond}, f"revoke-{i}")
        rc, _out, err = gcloud(_binding_argv("remove", kind, res, project, role, f), dry=dry)
        if rc != 0:
            raise Refused(f"removing {role} on {res} (condition {cond.get('title')}) failed (rc={rc}; "
                          f"{error_summary(err)})")
    if not dry:
        left = [(r, res) for kind, res in (("project", project), ("sa", node_sa(number)))
                for r, _c in run_bindings(_policy(kind, res, project), project, run)]
        if left:
            raise Refused(f"bindings of run {run} are still held after their removal: {left}")
    return len(found)


def iam_commands(project: str, roles: list[str], number: str) -> list[str]:
    cmds = [shlex.join(["gcloud", "iam", "service-accounts", "create", SA_NAME, "--project", project,
                        "--display-name", "snowplow bench orchestrator (vm606)",
                        "--description", "Runs run606_pair.sh from a disposable GCE VM (bench/vm606.py)"])]
    for kind, res, role in grant_targets(project, number, roles):
        cmds.append(shlex.join(["gcloud", *_binding_argv("add", kind, res, project, role, "iam-condition.json")]))
    return cmds


def cmd_plan_iam(a) -> int:
    found = scan(Path(a.bench_dir))
    if found["uncovered"]:
        print("refusing: the code makes GCP calls this plan does not cover (add each to CALL_ROLES / "
              "REST_ROLES with the role that holds its permission):\n  " + "\n  ".join(found["uncovered"]),
              file=sys.stderr)
        return 1
    try:
        number = a.project_number or _project_number(a.project)
    except Refused as e:
        print(f"refusing: {e}; pass --project-number", file=sys.stderr)
        return 1
    need = plan(found)
    _say(f"# vm606 plan-iam: {sa_email(a.project)}")
    _say(f"# derived from the code under {Path(a.bench_dir).resolve()} (every [\"gcloud\", ...] argv in bench/*.py "
         f"but {SELF}, REST hosts, kubectl/helm in run606*.sh and bench/*.sh); plan-iam runs nothing")
    _say("")
    for i, (role, uses) in enumerate(need.items(), 1):
        on = f"  (ON {node_sa(number)} only)" if role == NODE_SA_ROLE else "  (project)"
        _say(f"{i}. {role}{on}")
        _say(f"   why: {WHY.get(role, '')}")
        for u in uses:
            _say(f"   - {u}")
    _say("")
    _say("not needed:")
    for n in NOT_NEEDED:
        _say(f"   - {n}")
    _say("")
    _say("# access expires with each run: between runs the SA holds nothing. Once, the SA itself (it exists):")
    cmds = iam_commands(a.project, list(need), number)
    _say(cmds[0])
    _say("# per run, `up --yes` runs these BEFORE the VM exists (and reads the policies back), `down --yes` removes "
         "them by exact condition; iam-condition.json holds:")
    _say("#   " + json.dumps(iam_condition("<run>", "<the VM's --termination-time, RFC 3339 UTC>"), sort_keys=True))
    for c in cmds[1:]:
        _say(c)
    _say("")
    _say("# the operator's own account (up / ssh / scp / down from the laptop) needs, roles/owner covers all:")
    for n in OPERATOR_NEEDS:
        _say(f"#   - {n}")
    return 0


# ─── up ─────────────────────────────────────────────────────────────────────

def startup_script() -> str:
    """The VM's bootstrap, as root at every boot; a no-op once the marker is written."""
    return f"""#!/bin/bash
# bench/vm606.py: the orchestrator VM's tools at the laptop's versions
set -euo pipefail
M={os.path.dirname(BOOTSTRAP_MARKER)}
mkdir -p {VM_ROOT}
chmod 1777 {VM_ROOT}
install -d -m 0755 "$M"
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


def firewall_rules(run: str) -> dict[str, dict]:
    """The run's two SSH rules, by name: what up makes and the only rules down deletes."""
    tag, desc = vm_name(run), f"vm606 owner={OWNER} run={run}"
    return {f"{tag}-deny-ssh": {"action": "DENY", "sourceRanges": ["0.0.0.0/0"], "priority": 1000,
                                "targetTags": [tag], "description": desc},
            f"{tag}-iap-ssh": {"action": "ALLOW", "sourceRanges": [IAP_RANGE], "priority": 900,
                               "targetTags": [tag], "description": desc}}


def firewall_create_argv(name: str, rule: dict, project: str) -> list[str]:
    return ["compute", "firewall-rules", "create", name, "--project", project, "--network", NETWORK,
            "--direction", "INGRESS", "--action", rule["action"], "--rules", "tcp:22",
            "--source-ranges", ",".join(rule["sourceRanges"]), "--target-tags", ",".join(rule["targetTags"]),
            "--priority", str(rule["priority"]), "--description", rule["description"]]


def create_command(run: str, project: str, zone: str, ttl_hours: int, startup_file: str,
                   terminates_at: str) -> list[str]:
    labels = f"owner={OWNER},run={run},ttl-hours={ttl_hours}"
    return ["compute", "instances", "create", vm_name(run), *_where(project, zone),
            "--machine-type", MACHINE_TYPE,
            "--image-family", IMAGE_FAMILY, "--image-project", IMAGE_PROJECT,
            "--boot-disk-size", f"{BOOT_DISK_GB}GB", "--boot-disk-type", BOOT_DISK_TYPE, "--boot-disk-auto-delete",
            "--service-account", sa_email(project), "--scopes", "cloud-platform",
            "--network", NETWORK, "--tags", vm_name(run),
            "--labels", labels,
            "--metadata", "enable-oslogin=TRUE,block-project-ssh-keys=TRUE",
            "--metadata-from-file", f"startup-script={startup_file}",
            "--termination-time", terminates_at,
            "--instance-termination-action", "DELETE",
            "--shielded-secure-boot", "--shielded-vtpm", "--shielded-integrity-monitoring"]


VM_FOOTPRINT = (("INSTANCES", 1, "the VM", ""),
                ("E2_CPUS", VM_VCPUS, f"its {MACHINE_TYPE}", ""),
                ("SSD_TOTAL_GB", BOOT_DISK_GB, f"its {BOOT_DISK_TYPE} boot disk", " GB"),
                ("IN_USE_ADDRESSES", 1, "its ephemeral external IP", ""))


def vm_quota_check(project: str, zone: str) -> tuple[list[str], list[str]]:
    """The VM's own footprint against the quotas of the region it sits in, reads only, each failing
    closed (bench.gke's primitives: the region's quotas, its instances / forwarding rules / addresses
    listed with the partial-error property false, in-use addresses counted from their holders)."""
    region = gke.region_of(zone)
    rc, out, err = gcloud(gke.region_quotas_command(project, zone)[1:], quiet=True)
    quotas = gke._json(out) if rc == 0 else None
    if not isinstance(quotas, dict):
        raise Refused(f"the quotas of {region} could not be read (rc={rc}; {error_summary(err)}): whether the VM fits "
                      f"is unknown")
    listed: dict[str, list[dict]] = {}
    for kind in ("instances", "forwarding-rules", "addresses"):
        rc, out, err = gcloud(gke.region_list_command(kind, project, region)[1:], env=LIST_ENV, quiet=True)
        got = gke._json(out) if rc == 0 else None
        if not isinstance(got, list):
            raise Refused(f"the {kind} of {region} could not be listed (rc={rc}; {error_summary(err)}): whether the "
                          f"VM fits is unknown")
        listed[kind] = got
    fits, refusals = [], []
    for metric, need, what, unit in VM_FOOTPRINT:
        used = gke.in_use_addresses(listed, region) if metric == "IN_USE_ADDRESSES" else None
        ok, msg = gke.quota_fit(quotas, metric, need, f"the VM needs {need}{unit}: {what}", unit, used)
        (fits if ok else refusals).append(msg)
    return fits, refusals


def cmd_up(a) -> int:
    run = check_run(a.run)
    if not 1 <= a.ttl_hours <= MAX_TTL_HOURS:
        raise Refused(f"--ttl-hours {a.ttl_hours}: 1 to {MAX_TTL_HOURS} (the run's own TTL + the hours fetch may wait)")
    name = vm_name(run)
    found = scan(BENCH_DIR)
    if found["uncovered"]:
        raise Refused("the code makes GCP calls plan-iam does not cover: " + "; ".join(found["uncovered"]))
    roles = list(plan(found))
    rc, out, err = gcloud(["services", "list", "--enabled", "--project", a.project, "--format", "json"], quiet=True)
    enabled = gke._json(out) if rc == 0 else None
    if not isinstance(enabled, list):
        raise Refused(f"could not read whether IAP is enabled (rc={rc}; {error_summary(err)})")
    if "iap.googleapis.com" not in {(x.get("config") or {}).get("name") for x in enabled}:
        raise Refused(f"iap.googleapis.com is not enabled in {a.project}: the VM is reached through IAP only "
                      f"(gcloud services enable iap.googleapis.com --project {a.project})")
    rc, out, err = gcloud(["iam", "service-accounts", "describe", sa_email(a.project), "--project", a.project,
                           "--format", "value(email)"], quiet=True)
    if rc != 0 or out.strip() != sa_email(a.project):
        raise Refused(f"the orchestrator SA {sa_email(a.project)} could not be read (rc={rc}; {error_summary(err)}): "
                      f"create it (plan-iam prints the command)")
    number = _project_number(a.project)
    used = run_in_use(run, a.project, number)
    if used:
        raise Refused(f"run {run} is in use: {'; '.join(used)}. `down --run {run}` removes what vm606 made; a new "
                      f"launch takes a new --run")
    held = unconditional_bindings(a.project, number)
    if held:
        raise Refused(f"{sa_email(a.project)} holds unconditional binding(s): {'; '.join(held)}. Between runs it holds "
                      f"nothing (access expires with each run): remove them first")
    fits, refusals = vm_quota_check(a.project, a.zone)
    for msg in fits:
        _say(msg)
    if refusals:
        raise Refused(f"the VM does not fit {gke.region_of(a.zone)}'s quotas: " + "; ".join(refusals))
    d = state_dir(run)
    d.mkdir(parents=True, exist_ok=True)
    sf = d / "startup.sh"
    sf.write_text(startup_script())
    terminates_at = termination_time(time.time(), a.ttl_hours)
    if a.yes:
        _save_state(run, vm=name, project=a.project, zone=a.zone, ttl_hours=a.ttl_hours, terminates_at=terminates_at,
                    node_sa=node_sa(number), project_number=number, created_at=time.time())
    # 1. the grants, expiring with the VM, before the VM exists
    grant(run, a.project, number, roles, terminates_at, dry=not a.yes)
    # 2. SSH through IAP only, before the VM exists
    for fw, rule in firewall_rules(run).items():
        rc, _out, err = gcloud(firewall_create_argv(fw, rule, a.project), dry=not a.yes)
        if rc != 0:
            raise Refused(f"firewall rule {fw} could not be created (rc={rc}; {error_summary(err)}); {down_hint(run)}")
    # 3. the VM
    rc, _out, err = gcloud(create_command(run, a.project, a.zone, a.ttl_hours, str(sf), terminates_at), dry=not a.yes)
    if not a.yes:
        _say(f"DRY: would wait for {BOOTSTRAP_MARKER} on {name} (kubectl {KUBECTL_VERSION}, helm {HELM_VERSION}, "
             f"gcloud {GCLOUD_APT_VERSION.split('-')[0]}); nothing was created")
        return 0
    if rc != 0:
        raise Refused(f"instances create failed (rc={rc}; {error_summary(err)}); {down_hint(run)}")
    _say(f"{name} created; it deletes itself (and its disk) at {terminates_at}, when its grants expire")
    wait_for_bootstrap(run, a)
    return 0


def wait_for_bootstrap(run: str, a) -> None:
    """SSH until the marker answers. Each failure is read: one waiting cannot fix stops at once, naming its
    stage; a booting VM's (port 22 not yet answering) is waited out until --ssh-ready-timeout-s."""
    name = vm_name(run)
    t0 = time.monotonic()
    reached = False
    script = remote(run, f'cat {shlex.quote(BOOTSTRAP_MARKER)} 2>/dev/null || true\n', in_dir=False)
    while True:
        rc, out, err = ssh(name, a.project, a.zone, script, timeout=120)
        now = time.monotonic() - t0
        if rc != 0:
            stage, transient = ssh_stage(rc, err)
            if not transient:
                raise Refused(f"SSH to {name} failed at {stage}; the VM is up and billing until its TTL: "
                              f"{down_hint(run)}")
            if not reached and now >= a.ssh_ready_timeout_s:
                raise Refused(f"SSH to {name} never answered in {a.ssh_ready_timeout_s} s, last at {stage}; the VM "
                              f"is up and billing until its TTL: {down_hint(run)}")
        else:
            reached = True
            try:
                got = json.loads(out) if out.strip() else None
            except ValueError:
                got = None
            if isinstance(got, dict):
                bad = bootstrap_mismatch(got)
                if bad:
                    raise Refused(f"{name}'s tools are not the laptop's: {'; '.join(bad)}; {down_hint(run)}")
                _save_state(run, bootstrap=got)
                _say(f"{name} bootstrapped: " + ", ".join(f"{k} {v}" for k, v in sorted(got.items())))
                return
        if now >= a.bootstrap_timeout_s:
            raise Refused(f"{name}'s bootstrap did not finish in {a.bootstrap_timeout_s} s ({BOOTSTRAP_MARKER} "
                          f"absent; SSH {'answers' if reached else 'never answered'}; the startup script's log: "
                          f"sudo journalctl -u google-startup-scripts on the VM); {down_hint(run)}")
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


_MODE_PY = ("python3 -c 'import os,stat,sys; p=sys.argv[1]; "
            "print(oct(stat.S_IMODE(os.stat(p).st_mode))[2:] if os.path.isfile(p) else \"absent\")'")


def remote_ghcr(run: str) -> str:
    return f"{vm_dir(run)}/home/{GHCR_FILE_REL}"


def cmd_push_secret(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    local = os.path.expanduser("~/" + GHCR_FILE_REL)
    _local_secret(local)
    _say(f"local {local}: present, mode 0600")
    gdir = os.path.dirname(GHCR_FILE_REL)
    prep = remote(run, f"""\
test -s {shlex.quote(BOOTSTRAP_MARKER)} || refuse "the VM's bootstrap has not finished"
umask 077
mkdir -p "$R/home/{gdir}"
chmod 700 "$R" "$R/home" "$R/home/.config" "$R/home/{gdir}"
rm -f "$R/home/{GHCR_FILE_REL}"
""", in_dir=False)
    verify = remote(run, f"""\
chmod 600 "$R/home/{GHCR_FILE_REL}"
echo "VM606_MODE=$({_MODE_PY} "$R/home/{GHCR_FILE_REL}")"
echo "VM606_DIR_MODE=$(python3 -c 'import os,stat,sys; print(oct(stat.S_IMODE(os.stat(sys.argv[1]).st_mode))[2:])' "$R/home/{gdir}")"
""")
    if not a.yes:
        _say(f"DRY: on {name}: mkdir -m 700 {vm_dir(run)}/home/{gdir}")
        scp(local, f"{name}:{remote_ghcr(run)}", a.project, a.zone, dry=True)
        _say(f"DRY: on {name}: chmod 600 {remote_ghcr(run)}; check presence and mode; nothing was copied")
        return 0
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    rc, out, err = ssh(name, a.project, a.zone, prep, timeout=300)
    if rc != 0:
        raise _failed("preparing the token's directory on the VM", rc, out, err)
    rc, out, err = scp(local, f"{name}:{remote_ghcr(run)}", a.project, a.zone)
    if rc != 0:
        raise Refused(f"scp of the token file failed (rc={rc}; {error_summary(err)})")
    rc, out, err = ssh(name, a.project, a.zone, verify, timeout=300)
    mode, dmode = _marker(out, "VM606_MODE"), _marker(out, "VM606_DIR_MODE")
    if rc != 0 or mode != "600" or dmode != "700":
        raise Refused(f"the token file on {name} is {mode or 'unreadable'} in a directory of mode {dmode or '?'} "
                      f"(want present, 600, in 700; rc={rc})")
    _local_secret(local)
    _save_state(run, secret_pushed_at=time.time())
    _say(f"{name}:{remote_ghcr(run)}: present, mode 0600 (directory 0700)")
    return 0


# ─── launch ─────────────────────────────────────────────────────────────────

LAPTOP_ONLY = re.compile(r"^(OUT_[A-Z][A-Z0-9]*|PAIR_OUT|BENCH_RUNS_DIR|BENCH_EVIDENCE_DIR|XDG_DATA_HOME|HOME|PATH|"
                         r"KUBECONFIG|GHCR_PULL_ENV_FILE|PYTHONPATH|BENCH_GKE_CONTEXT)$")
SECRET_KEYS = re.compile(r"^GHCR_PULL_[A-Z0-9_]*$")
TEST_HOOKS = re.compile(r"^(BENCH_606_DRY_RUN|BENCH_606_FAIL_AT|BENCH_606_HOST_LOAD|BENCH_ALLOW_[A-Z_]+|"
                        r"CLOUDSDK_[A-Z_]+)$")
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
        if SECRET_KEYS.match(k):
            bad.append(f"{k} is the pull token's: it travels only as the file push-secret copies")
        elif LAPTOP_ONLY.match(k):
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
        if not re.fullmatch(r"[A-Z][A-Z0-9]*", x):
            raise Refused(f"RUNS: {x!r} is not a label")
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


def _bench_vars(sha: str) -> str:
    return (f"SHA={shlex.quote(sha)}\nB=\"$R/snowplow/go/snowplow/e2e/bench\"\nV=\"$R/venv\"\n")


def prepare_script(run: str, sha: str, repo: str, env: dict[str, str], pin_name: str, pin_sha: str,
                   pair: str) -> str:
    q = shlex.quote
    return remote(run, _bench_vars(sha) + f"""\
test -s {q(BOOTSTRAP_MARKER)} || refuse "the VM's bootstrap has not finished"
mode="$({_MODE_PY} "$R/home/{GHCR_FILE_REL}")"
[ "$mode" = 600 ] || refuse "the pull-token file is $mode on the VM, not present with mode 600: push-secret first"
[ ! -e "$R/snowplow" ] || refuse "$R/snowplow exists: one launch per --run"
got="$({_HASH_PY} "$R/pin/"{q(pin_name)})"
[ "$got" = {q(pin_sha)} ] || refuse "the pin manifest on the VM has sha256 $got, not {pin_sha}"
git init -q "$R/snowplow"
git -C "$R/snowplow" remote add origin {q(repo)}
git -C "$R/snowplow" fetch -q --depth 1 origin "$SHA"
git -C "$R/snowplow" checkout -q --detach FETCH_HEAD
head="$(git -C "$R/snowplow" rev-parse HEAD)"
[ "$head" = "$SHA" ] || refuse "the checkout is at $head, not $SHA"
echo "vm606: bench checkout at $head"
[ -x "$V/bin/python3" ] || python3 -m venv "$V"
"$V/bin/pip" install -q -r "$B/requirements.txt" pyyaml jsonschema
export PATH="$V/bin:$PATH" USE_GKE_GCLOUD_AUTH_PLUGIN=True
{_exports(env)}cd "$B"
PO="$(python3 -m bench.evidence run-dir {q(pair)})"
mkdir -p "$PO"
cp "$R/pin/"{q(pin_name)} "$PO/"{q(pin_name)}
rm -rf "$R/dry"
if ! BENCH_606_DRY_RUN=1 PAIR_OUT="$R/dry" bash run606_pair.sh > "$R/dry.log" 2>&1; then
  tail -20 "$R/dry.log"
  refuse "the launcher's dry pass was refused ($R/dry.log)"
fi
echo "vm606: dry pass OK ($R/dry.log)"
python3 - "$R/dry" <<'PY'
import glob, re, sys
hours = []
for f in sorted(glob.glob(sys.argv[1] + "/dry-*/run.log")):
    hours += [int(m.group(1)) for m in re.finditer({_TTL_RX!r}, open(f).read())]
print("VM606_RUN_TTL_HOURS=" + (str(max(hours)) if hours else ""))
PY
echo "VM606_PAIR_OUT=$PO"
""")


def launch_script(run: str, sha: str, env: dict[str, str], labels: list[str], pin_name: str, pin_sha: str,
                  pair: str, started_timeout_s: int) -> str:
    q = shlex.quote
    outs = "".join(f'OUT_{x}="$(python3 -m bench.evidence run-dir {q(env["RUN_NAME_" + x])})"\n' for x in labels)
    return remote(run, _bench_vars(sha) + f"""\
[ -d "$B" ] || refuse "no checkout at $B: the prepare step did not run"
export PATH="$V/bin:$PATH" USE_GKE_GCLOUD_AUTH_PLUGIN=True
cd "$B"
PO="$(python3 -m bench.evidence run-dir {q(pair)})"
got="$({_HASH_PY} "$PO/"{q(pin_name)})"
[ "$got" = {q(pin_sha)} ] || refuse "the pin manifest in $PO has sha256 $got, not {pin_sha}"
(umask 077; cat > "$R/launch-env.sh" <<'ENV'
{_exports(env)}ENV
)
{_exports(env)}{outs}python3 -m bench.detach --log "$R/launch.log" --pidfile "$R/launch.pid" -- bash run606_pair.sh
pid="$(cat "$R/launch.pid")"
deadline=$(( $(date +%s) + {int(started_timeout_s)} ))
while :; do
  ok=0; refused=""
  for x in {" ".join(labels)}; do
    o="OUT_$x"
    if grep -q "run budget: " "${{!o}}/run.log" 2>/dev/null; then ok=$((ok + 1)); fi
    r="$(grep -h -m1 '^refusing: ' "$PO/$x.console" 2>/dev/null || true)"
    if [ -n "$r" ]; then refused="$refused run $x: $r;"; fi
  done
  if [ "$ok" -eq {len(labels)} ]; then echo "VM606_STARTED=ok"; break; fi
  if [ -n "$refused" ]; then echo "VM606_STARTED=refused"; echo "vm606: $refused"; break; fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "VM606_STARTED=exited"; tail -n 15 "$R/launch.log" | sed 's/^/vm606: launcher: /'; break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then echo "VM606_STARTED=timeout"; break; fi
  sleep 1
done
echo "VM606_PID=$pid"
""")


def cmd_launch(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    if not SHA_PATTERN.fullmatch(a.bench_sha or ""):
        raise Refused(f"--bench-sha {a.bench_sha!r}: the full 40-hex commit (copy it, never type it)")
    if not HEX64.fullmatch(a.pin_sha256 or ""):
        raise Refused("--pin-sha256: 64 lowercase hex")
    env = read_env_file(a.env_file)
    labels = check_env(env)
    if env.get("ZONE", a.bench_zone) != a.bench_zone:
        raise Refused(f"the env file's ZONE={env['ZONE']} is not --bench-zone {a.bench_zone}: one zone for the clusters")
    env["ZONE"] = a.bench_zone
    pin = Path(a.pin_manifest)
    if not pin.is_file():
        raise Refused(f"{pin} does not exist")
    got = _sha256(str(pin))
    if got != a.pin_sha256:
        raise Refused(f"{pin} has sha256 {got}, not {a.pin_sha256}")
    _say(f"pin manifest {pin.name}: sha256 {got} (verified on the laptop)")
    pair = pair_name(env)
    names = [env[f"RUN_NAME_{x}"] for x in labels]
    remote_pin = f"{vm_dir(run)}/pin/{pin.name}"
    prep = prepare_script(run, a.bench_sha, a.repo_url, env, pin.name, got, pair)
    go = launch_script(run, a.bench_sha, env, labels, pin.name, got, pair, a.started_timeout_s)
    if not a.yes:
        _say(f"DRY: on {name}: mkdir {vm_dir(run)}/pin")
        scp(str(pin), f"{name}:{remote_pin}", a.project, a.zone, dry=True)
        _say(f"DRY: on {name}: clone {a.repo_url} at {a.bench_sha}, venv, the launcher's dry pass; then, if the VM "
             f"outlives the run's TTL + {LAUNCH_MARGIN_S} s: python3 -m bench.detach --log {vm_dir(run)}/launch.log "
             f"--pidfile {vm_dir(run)}/launch.pid -- bash run606_pair.sh, with {len(env)} variables: "
             f"{', '.join(sorted(env))}; launched once every run logs its run budget")
        _say(f"DRY: runs {', '.join(names)}; pair {pair}; clusters in ZONE={a.bench_zone}; nothing was started")
        return 0
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    rc, out, err = ssh(name, a.project, a.zone, remote(run, 'umask 077\nmkdir -p "$R/pin"\n'), timeout=300)
    if rc != 0:
        raise _failed("mkdir on the VM", rc, out, err)
    rc, out, err = scp(str(pin), f"{name}:{remote_pin}", a.project, a.zone)
    if rc != 0:
        raise Refused(f"scp of the pin manifest failed (rc={rc}; {error_summary(err)})")
    rc, out, err = ssh(name, a.project, a.zone, prep, timeout=a.prepare_timeout_s)
    for ln in _remote_lines(out):
        if ln.startswith("vm606:"):
            _say(ln)
    if rc != 0:
        raise _failed("the VM's prepare step", rc, out, err)
    ttl = _marker(out, "VM606_RUN_TTL_HOURS")
    pair_out = _marker(out, "VM606_PAIR_OUT")
    if not (ttl or "").isdigit():
        raise Refused("the dry pass handed gke-up no --ttl-hours (no `bench.gke gke-up ... --ttl-hours N` in any "
                      f"dry-*/run.log under {vm_dir(run)}/dry): how long the run lasts is unknown")
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
    rc, out, err = ssh(name, a.project, a.zone, go, timeout=a.started_timeout_s + 300)
    for ln in _remote_lines(out):
        if ln.startswith("vm606:") or ln.startswith("detached:"):
            _say(ln)
    if rc != 0:
        raise _failed("the detached launch", rc, out, err)
    started = _marker(out, "VM606_STARTED") or "unknown"
    _save_state(run, launched_at=time.time(), bench_sha=a.bench_sha, bench_zone=a.bench_zone, runs=names, pair=pair,
                pair_out=pair_out,
                pin_manifest=pin.name, pin_sha256=got, run_ttl_hours=int(ttl), started=started, fetched=False)
    if started != "ok":
        raise Refused(f"the launcher on {name} did not get every run past its paid-only checks ({started}; the lines "
                      f"above say why). Whatever it wrote is on the VM: `fetch` it, or `down --discard-unfetched`")
    _say(f"launched on {name}: every run passed its paid-only checks (its run budget is logged); "
         f"`python3 -m bench.vm606 status --run {run}`")
    return 0


# ─── status ─────────────────────────────────────────────────────────────────

LAUNCHER_STATE = ("launcher_state() {\n"
                  "  if [ -s \"$R/launch.pid\" ] && kill -0 \"$(cat \"$R/launch.pid\")\" 2>/dev/null; then\n"
                  "    echo \"running $(cat \"$R/launch.pid\")\"\n"
                  "  elif [ -s \"$R/launch.pid\" ]; then echo exited\n"
                  "  else echo never\n  fi\n}\n")


def alive_script(run: str) -> str:
    return remote(run, LAUNCHER_STATE + 'echo "VM606_LAUNCHER=$(launcher_state)"\n', in_dir=False)


def cmd_status(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    inst, why = _describe(name, a.project, a.zone)
    if inst is None:
        raise Refused(f"{name}: {why}")
    _owned(inst, run)
    left = _time_left_s(inst, time.time())
    script = remote(run, LAUNCHER_STATE + 'echo "VM606_LAUNCHER=$(launcher_state)"\necho ---\n'
                    f'tail -n {int(a.lines)} "$R/launch.log" 2>/dev/null || true\n', in_dir=False)
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
    return remote(run, _bench_vars(sha) + LAUNCHER_STATE + f"""\
case "$(launcher_state)" in running*) refuse "the launcher is still running: fetch once it has exited";; esac
D="$("$V/bin/python3" -c 'import sys; sys.path.insert(0, sys.argv[1]); from bench import evidence; print(evidence.data_root())' "$B")"
members=""
for n in {" ".join(q(n) for n in names)}; do
  for t in runs evidence; do
    if [ -d "$D/$t/{evidence.CAMPAIGN}/$n" ]; then members="$members $t/{evidence.CAMPAIGN}/$n"; fi
  done
done
[ -n "$members" ] || refuse "no run or evidence directory of {", ".join(names)} under $D"
rm -rf "$R/fetch"; mkdir -p "$R/fetch/vm606/{run}"
for f in launch.log dry.log launch-env.sh; do
  if [ -f "$R/$f" ]; then cp "$R/$f" "$R/fetch/vm606/{run}/$f"; fi
done
# the names are the bench's own (snowplow-bench-*, pair-...): no blank in any, word splitting is safe.
# COPYFILE_DISABLE: a bsdtar (macOS) would add AppleDouble ._ members; GNU tar ignores it
COPYFILE_DISABLE=1 tar -cf "$R/fetch.tar" -C "$D" $members -C "$R/fetch" "vm606/{run}"
echo "VM606_FETCH_SHA256=$({_HASH_PY} "$R/fetch.tar")"
echo "VM606_FETCH_MEMBERS=$members"
""")


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
    rc, _o, err = scp(f"{name}:{vm_dir(run)}/fetch.tar", str(tar), a.project, a.zone)
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


def _get(args: list[str], what: str) -> dict | None:
    """One describe: the resource, or None when GCE answers NotFound; any other failure refuses (a
    describe cannot fail open the way a list can)."""
    rc, out, err = gcloud([*args, "--format", "json"], quiet=True)
    if rc != 0:
        if re.search(r"\bcode=404\b|HTTPError 404|was not found|notFound", err or ""):
            return None
        raise Refused(f"{what} could not be read (rc={rc}; {error_summary(err)}): refusing")
    try:
        got = json.loads(out)
    except ValueError:
        got = None
    if not isinstance(got, dict):
        raise Refused(f"{what} answered no JSON object: refusing")
    return got


def _list(args: list[str], what: str) -> list[dict]:
    rc, out, err = gcloud(args, env=LIST_ENV, quiet=True)
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    if not isinstance(got, list):
        raise Refused(f"{what} could not be listed (rc={rc}; {error_summary(err)}): refusing")
    return got


def _run_rules(run: str, project: str) -> list[dict]:
    """The run's firewall rules that exist, each read by its exact name (`describe`: NotFound is
    "absent", any other failure refuses; never a list filter, which gcloud can mis-parse into an
    empty answer with rc 0, #1307 re-review N1); refuses one that is not exactly what up makes."""
    rows = []
    for fw, w in firewall_rules(run).items():
        r = _get(["compute", "firewall-rules", "describe", fw, "--project", project], f"firewall rule {fw}")
        if r is None:
            continue
        if r.get("targetTags") != w["targetTags"] or r.get("description") != w["description"]:
            raise Refused(f"firewall rule {fw!r} is not one vm606 made for run {run} (target tags "
                          f"{r.get('targetTags')}, description {r.get('description')!r}): refusing")
        rows.append(r)
    return rows


def _instances_of(run: str, project: str) -> list[dict]:
    """Every instance of the project (all zones, one unfiltered list with the partial-error property
    false) that is the run's by name or by its run label: selected here, by exact equality."""
    rows = _list(["compute", "instances", "list", "--project", project, "--format", "json"],
                 f"the instances of {project}")
    return [i for i in rows if i.get("name") == vm_name(run) or (i.get("labels") or {}).get("run") == run]


def run_in_use(run: str, project: str, number: str) -> list[str]:
    """What of `run` exists anywhere: its laptop record, an instance (any zone) named or labelled for it,
    its firewall rules, bindings under its condition title. Empty: the name is free."""
    found = []
    if (state_dir(run) / "state.json").exists():
        found.append(f"its record {state_dir(run) / 'state.json'}")
    for i in _instances_of(run, project):
        found.append(f"instance {i.get('name')} ({str(i.get('zone', '')).rsplit('/', 1)[-1]})")
    found += [f"firewall rule {r['name']}" for r in _run_rules(run, project)]
    for kind, res in (("project", project), ("sa", node_sa(number))):
        found += [f"binding {role} on {res} under {c.get('title')}"
                  for role, c in run_bindings(_policy(kind, res, project), project, run)]
    return found


def unconditional_bindings(project: str, number: str) -> list[str]:
    """Every binding the orchestrator SA holds without a condition: between runs it holds nothing."""
    member = f"serviceAccount:{sa_email(project)}"
    held = []
    for kind, res in (("project", project), ("sa", node_sa(number))):
        held += [f"{b.get('role')} on {res}" for b in _policy(kind, res, project).get("bindings") or []
                 if member in (b.get("members") or []) and not b.get("condition")]
    return held


def cmd_down(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    st = _state(run)
    # ── every check first ──
    inst, why = _describe(name, a.project, a.zone)
    if inst is None and why != "notfound":
        raise Refused(f"{name}: {why}")
    disks = [name]
    if inst is not None:
        _owned(inst, run)
        listed = [i for i in _instances_of(run, a.project) if (i.get("labels") or {}).get("run") == run]
        if sorted(i.get("name") for i in listed) != [name]:
            raise Refused(f"the label run={run} selects {sorted(i.get('name') for i in listed)}, not [{name}]: refusing")
        if st.get("launched_at") and not st.get("fetched") and not a.discard_unfetched:
            raise Refused(f"{run} was launched and not fetched: `fetch` first (--discard-unfetched lets its evidence "
                          f"go with the VM)")
        if not a.skip_launcher_check:
            rc, out, err = ssh(name, a.project, a.zone, alive_script(run), timeout=120)
            state = _marker(out, "VM606_LAUNCHER")
            if rc != 0 or not state:
                raise Refused(f"whether the launcher still runs on {name} could not be read (rc={rc}; "
                              f"{ssh_stage(rc, err)[0]}); --skip-launcher-check deletes it anyway")
            if state.startswith("running"):
                raise Refused(f"the launcher is still running on {name} ({state}): deleting the VM would kill every "
                              f"run's teardown; stop it (kill -TERM <pid> on the VM) and wait for it to exit")
        disks = _disk_names(inst)
    rules = _run_rules(run, a.project)
    number = st.get("project_number") or _project_number(a.project)
    # ── then delete, each read back ──
    if inst is not None:
        rc, _o, err = gcloud(["compute", "instances", "delete", name, *_where(a.project, a.zone),
                              "--delete-disks", "all", "--quiet"], dry=not a.yes)
        if rc != 0:
            raise Refused(f"instances delete failed (rc={rc}; {error_summary(err)})")
        if a.yes:
            gone, why = _describe(name, a.project, a.zone)
            if gone is not None or why != "notfound":
                raise Refused(f"{name} is not verified gone after the delete ({why or 'it still answers'})")
    else:
        _say(f"{name}: already gone (its TTL, or an earlier down)")
    for r in rules:
        rc, _o, err = gcloud(["compute", "firewall-rules", "delete", r["name"], "--project", a.project, "--quiet"],
                             dry=not a.yes)
        if rc != 0:
            raise Refused(f"firewall rule {r['name']} could not be deleted (rc={rc}; {error_summary(err)})")
    removed = revoke(run, a.project, number, dry=not a.yes)
    if not a.yes:
        _say(f"DRY: would verify {name} answers NotFound, its disks {disks}, its {len(rules)} firewall rule(s) and "
             f"its {removed} grant(s) gone; nothing was deleted")
        return 0
    left = [d for d in disks if _get(["compute", "disks", "describe", d, *_where(a.project, a.zone)],
                                      f"disk {d}") is not None]
    if left:
        raise Refused(f"disk(s) {left} remain: delete them by hand (gcloud compute disks delete ... --zone "
                      f"{a.zone}); the pull-token file is on one of them")
    still = [r["name"] for r in _run_rules(run, a.project)]
    if still:
        raise Refused(f"firewall rule(s) {still} of {name} remain after their delete")
    _save_state(run, deleted_at=time.time())
    _say(f"{name} gone and verified: disk(s) {disks} (and with them the pull-token file), {len(rules)} firewall "
         f"rule(s), {removed} conditional grant(s) of run {run}")
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

    p = sub.add_parser("up", help="the run's grants and SSH rules, the VM; wait for its bootstrap")
    common(p)
    p.add_argument("--ttl-hours", type=int, required=True,
                   help="the VM deletes itself, and its grants expire, after this: the run's TTL + the hours fetch "
                        "may wait")
    p.add_argument("--ssh-ready-timeout-s", type=int, default=300,
                   help="how long a booting VM's SSH may stay unanswered")
    p.add_argument("--bootstrap-timeout-s", type=int, default=1200)
    p.add_argument("--poll-s", type=float, default=15, help=argparse.SUPPRESS)
    p.set_defaults(func=cmd_up)

    p = sub.add_parser("push-secret", help="copy the ghcr pull-token file to the VM (never read)")
    common(p)
    p.set_defaults(func=cmd_push_secret)

    p = sub.add_parser("launch", help="clone at the bench SHA and start the paired launch, detached")
    common(p)
    p.add_argument("--bench-sha", required=True)
    p.add_argument("--bench-zone", required=True, type=_zone,
                   help="the bench clusters' zone, handed to run606_pair.sh as ZONE (the VM's is --zone)")
    p.add_argument("--env-file", required=True, help="the laptop launcher's variables, KEY=VALUE per line")
    p.add_argument("--pin-manifest", required=True)
    p.add_argument("--pin-sha256", required=True)
    p.add_argument("--repo-url", default=REPO_URL, help=argparse.SUPPRESS)
    p.add_argument("--prepare-timeout-s", type=int, default=3600)
    p.add_argument("--started-timeout-s", type=int, default=900,
                   help="how long every run may take to log its run budget (its paid-only checks passed)")
    p.set_defaults(func=cmd_launch)

    p = sub.add_parser("status", help="the launcher's log tail, its liveness, the VM's time left")
    common(p, yes=False)
    p.add_argument("--lines", type=int, default=40)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("fetch", help="copy the run and evidence directories back, verified")
    common(p, yes=False)
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("down", help="delete the VM, its disk, its firewall rules and its grants, verified")
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
