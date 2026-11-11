"""A DISPOSABLE GCE VM for any team's heavy test suite, instead of the shared laptop (#1338; Diego
2026-11-11: "if peers need to execute heavy tests on the host, provision a disposable google compute
instance"; "only CI should provision kind clusters. if local tests are done, provision a disposable gke
cluster or gce instance").

    python3 -m bench.testvm up --run R --repo OWNER/NAME --clone PATH --sha SHA \\
        --parallelism N --session-hours S --hours H [--workflow W] [--project P] [--zone Z] [--yes]
    python3 -m bench.testvm push --run R --repo OWNER/NAME --clone PATH --sha SHA [--workflow W]
    python3 -m bench.testvm run --run R --budget-minutes M [--cluster NAME --cluster-location L]
        [--cluster-probe] -- COMMAND [ARG...]
    python3 -m bench.testvm extend --run R --hours H [--yes]
    python3 -m bench.testvm fetch --run R
    python3 -m bench.testvm down --run R [--discard-unfetched] [--yes]

ONE VM PER WORK SESSION. `up` once, then any number of `run`s against the same VM and checkout, `push`
of a later SHA to the same VM (fix and retest without reprovisioning), `fetch` whenever, one `down`.
up / extend / down change GCP and print their gcloud argv, running none unless --yes; push, run and
fetch create nothing in GCP (SSH only).

NO CREDENTIALS ON THE VM. It is created with --no-service-account --no-scopes: no metadata token, no
IAM grant of any kind (so no grant to manage or expire), no GitHub or registry token. The code arrives
as a `git bundle` of the exact 40-hex SHA, made on the laptop from the caller's local clone (a bare
scratch repository borrowing the clone's objects: the clone itself is never written), streamed over
IAP, fetched into the VM's checkout and checked out there: HEAD must be the SHA and HEAD^{tree} the
SHA's tree as the laptop read it, with a clean status. A later push bundles only what the VM lacks
(every SHA pushed before is a prerequisite). The one exception is opt-in and scoped: `run --cluster`
(below) hands the suite a Kubernetes token for ONE disposable GKE cluster.

PROVENANCE. testvm never types a remote: --clone is a local path. --repo OWNER/NAME (or HOST/OWNER/NAME;
the host defaults to github.com) is the caller's declaration of the repository the clone is of, and the
bundle is refused unless the clone's `git remote get-url origin` resolves to exactly it (https, ssh and
scp-like forms normalised; owner and name compared as GitHub does, case-insensitively). The declared repo,
the origin URL (any userinfo removed) and the resolved SHA and tree go into the run's record and into
every result's header.json (a no-hyphen org still serves stale twins of krateo repos: a result is always
traced to the repo it tested).

DISPOSABLE BY CONSTRUCTION. The VM deletes itself, and its boot disk, at GCE's own --termination-time
with --instance-termination-action=DELETE, whatever happens to the session that made it. up's
termination time is now + the setup window (BOOTSTRAP_TIMEOUT_S + PREPARE_TIMEOUT_S) + the declared
--hours + FETCH_WINDOW_S. The session's CEILING, recorded at up, is the same sum with --session-hours:
`extend --hours H` moves the termination time to now + H + FETCH_WINDOW_S, never past the ceiling, and
records the new deadline before it changes anything. GCE changes a termination time only on a STOPPED
VM (set-scheduling's own help), so extend refuses while a command runs, then stops the VM, sets the new
time, starts it, reads the time back, and waits for its SSH again; a stopped VM holds no termination
time (GCE clears it at stop), so a failure between stop and start says so and names `down`. `run`
refuses a budget the VM does not outlive by FETCH_WINDOW_S (and names `extend`). Labels:
owner=snowplow-testvm, testvm=true, run=<R>. `down` deletes the WHOLE VM, never only its processes: a
suite whose TestMain made a cluster and was killed leaks nothing past the VM.

CLUSTERS. testvm never installs or runs kind. A suite that needs a cluster runs against a disposable GKE
cluster the caller made (bench.gke gke-up): `run --cluster NAME --cluster-location L` reads the cluster
(it must pass bench.gke.assert_deletable: a bench-made cluster with its TTL, never a forbidden one such as
krateo-057), and, the first time, makes ServiceAccount testvm-<R> in namespace testvm with a
ClusterRoleBinding to cluster-admin, through a scratch kubeconfig (never ~/.kube/config), and a token
for it from the TokenRequest API that expires at the session's ceiling. The token goes to the VM in a
kubeconfig (JSON, mode 0600, <VM_ROOT>/<R>/kube/<NAME>.json, kept for the session's later runs of that
cluster), exported as KUBECONFIG only for a run that names the cluster (any other run has KUBECONFIG
unset); the laptop keeps no copy, and `down` deletes the binding and the ServiceAccount (revoking the
token) once the VM is gone. This is the one credential that ever reaches the VM: opt-in, one disposable
cluster, never a GCP credential. A suite whose TestMain creates kind must find the existing cluster through KUBECONFIG
instead: that is the owning repo's harness change, not testvm's. --cluster-probe (opt-in, with --cluster)
records a cheap liveness probe of the cluster's controllers at the run's start and end (ironic's, from a
day of contended measurements): create a 1-replica Deployment, wait for its ReplicaSet, delete the
Deployment, and time how long the ReplicaSet survives (~1 s when healthy). Recorded only, no verdict.

THE TOOLCHAIN COMES FROM THE REPO, at each pushed SHA, read on the laptop from the SHA's tree: Go is the
highest of every go.mod's `toolchain` line (else its `go` line), installed from go.dev's own download
index with its sha256 checked, under GOTOOLCHAIN=local; Python is the CI workflow's actions/setup-python
`python-version` (every workflow must agree, or --workflow names the one the suite mirrors), installed
by uv, and the workflow's own `pip install` steps run in a fresh venv in their working-directory. No
version is pinned here. A workflow's other setup-* actions are named as not provided.

MACHINE SIZE from the suite's parallelism: the smallest e2-standard type of the zone (read from GCE, by
exact equality over the zone's machine types) with at least --parallelism vCPU. E2 has its own regional
quota (E2_CPUS), so the VM takes nothing from the N2_CPUS the bench's clusters use; the footprint
(INSTANCES 1, E2_CPUS, SSD_TOTAL_GB of its boot disk, IN_USE_ADDRESSES 1 for its ephemeral external IP,
which reaches go.dev, PyPI and the module proxies: ingress is SSH from IAP only) is read against the
region's quotas before anything is made (bench.gcevm.quota_check, vm606's).

RUN. `run -- COMMAND...` runs the argv, as given, in the checkout, detached on the VM (it survives the SSH
session and the laptop), under --budget-minutes: past it, its process group gets SIGTERM and, KILL_GRACE_S
later, SIGKILL; its group is killed when it exits too, so nothing it started outlives it. One run at a
time. Each run's results directory (<VM_ROOT>/<R>/runs/<n>) holds header.json (the run's provenance and
argv), stdout.log, stderr.log, done.json (exit code, how it ended, elapsed), host.json (the VM's
1/5/15-min load, CPU count, MemAvailable and /proc/stat's cpu jiffies, steal among them, at start and at
end, and the steal share between: a "ran clean" on a starved e2 host is visible; recorded, no
threshold), the cluster probe's when asked, and junit/: every JUnit XML the command wrote under the
checkout (root <testsuite> or <testsuites>, modified since the start) and anything it wrote into
$TESTVM_RESULTS. `run` prints a RUN / NOT RUN line per phase and exits 0 only when the command did.

TRANSFERS ARE BOUNDED (#1336): the bundle up and the results down go as ssh byte streams (bench.gcevm:
never `compute scp`, which hung for 55 min twice on 2026-11-11), each bounded by its byte count and a
rate measured just before it, a stall refused by name; the bytes' sha256 is checked on the far side.
`fetch` brings back every finished, unfetched run, into <data root>/testvm/<R>/results/<n>.

THE SHARED RULES (bench.gcevm, factored out of vm606): IAP-only SSH through two per-run rules (deny
0.0.0.0/0:22 at 1000, allow 35.235.240.0/20:22 at 900), the IAP close-code classification, partial-error
off on every list, selection by exact name (no --filter), parents confirmed before any absent verdict,
RECORD BEFORE CREATE (up writes what it is about to make, field for field, to the run's record first),
every later read-back compared against that record (never this checkout's constants), and `down`
deleting what it owns even when a field drifted, each drift reported and kept in the record. A RUN NAME
IS USED ONCE: up refuses a --run of which anything exists (its record, an instance named or labelled for
it in any zone, its firewall rules). The project and zone of an existing run come from its record only.

ON THE VM, NOTHING NAMES THE OPERATOR: every step runs under <VM_ROOT>/<R> with HOME there and USER
testvm (an OS Login home is the operator's account as a POSIX name); gcloud's error text reaches output
as its code only (bench.identity.error_summary), the VM's output through redact_identity.
"""

from __future__ import annotations

import argparse
import base64
import functools
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

from bench import evidence, gcevm, gke
from bench.gcevm import Refused, gcloud, ssh
from bench.gke import DEFAULT_PROJECT, DEFAULT_ZONE, ZONE_PATTERN
from bench.identity import error_summary, redact_identity

# ─── what the VM is ─────────────────────────────────────────────────────────

VM_PREFIX = "testvm-"
OWNER = "snowplow-testvm"
LABEL_KEYS = ("owner", "testvm", "run")
MACHINE_FAMILY = "e2-standard"         # E2_CPUS: the family's own regional quota, not the bench's N2_CPUS
CPU_QUOTA = "E2_CPUS"
BOOT_DISK_GB = 50                      # the checkout, its toolchains and module / build caches, the results
BOOT_DISK_TYPE = "pd-balanced"
IMAGE_FAMILY, IMAGE_PROJECT = "debian-13", "debian-cloud"
VM_ROOT = "/srv/testvm"
BOOTSTRAP_MARKER = f"{VM_ROOT}/.bootstrap/bootstrap.json"

# the setup window up's termination time includes: the bootstrap wait and the first push's prepare step
SSH_READY_TIMEOUT_S = 600
BOOTSTRAP_TIMEOUT_S = 1200
PREPARE_TIMEOUT_S = 1800               # a Go download, uv's Python, the workflow's pip installs
FETCH_WINDOW_S = 1800                  # what every deadline leaves after the last run: its fetch, then down
MAX_SESSION_HOURS = 72
KILL_GRACE_S = 30                      # SIGTERM to SIGKILL, past the budget
CLUSTER_PROBE_BOUND_S = 120            # each wait of the cluster probe; past it, the probe says so
POLL_S = 15

RUN_PATTERN = re.compile(r"[a-z][a-z0-9-]{0,30}[a-z0-9]")
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")
CLUSTER_NS = "testvm"

_failed = functools.partial(gcevm.failed, prefix="testvm:")


def vm_name(run: str) -> str:
    return VM_PREFIX + run


def vm_dir(run: str) -> str:
    return f"{VM_ROOT}/{run}"


def check_run(run: str) -> str:
    if not RUN_PATTERN.fullmatch(run or ""):
        raise Refused(f"--run {run!r}: lowercase letters, digits and '-', starting with a letter, at most 32 "
                      f"(it names the VM, {VM_PREFIX}<run>, and its run label)")
    return run


def check_sha(sha: str) -> str:
    if not SHA_PATTERN.fullmatch(sha or ""):
        raise Refused(f"--sha {sha!r}: the full 40-hex commit (copy it, never type it)")
    return sha


def down_hint(run: str) -> str:
    return f"`python3 -m bench.testvm down --run {run} --yes` deletes the VM and its firewall rules"


def state_dir(run: str) -> Path:
    return evidence.data_root() / "testvm" / run


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


def _recorded(run: str) -> tuple[dict, str, str]:
    """(record, project, zone) of a run `up --yes` made: its project and zone are read only from here."""
    st = _state(run)
    project, zone = st.get("project"), st.get("zone")
    if st.get("vm") != vm_name(run) or not project or not re.fullmatch(ZONE_PATTERN, zone or ""):
        raise Refused(f"no record of `up --yes` for run {run} on this laptop ({state_dir(run) / 'state.json'}): the "
                      f"run's project and zone are read only from it")
    return st, project, zone


def remote(run: str, body: str, *, in_dir: bool = True) -> str:
    """A remote script (vm606's form): wrapped in main(), called with stdin /dev/null; HOME is the run's
    directory under VM_ROOT, so no path names the login user's home."""
    head = ("set -euo pipefail\nexec 2>&1\n"
            "refuse() { echo \"testvm: refusing: $*\"; exit 3; }\n"
            f"R={shlex.quote(vm_dir(run))}\nexport HOME=\"$R/home\" USER=testvm LOGNAME=testvm\n")
    head += ('[ -d "$R" ] || refuse "no $R on the VM"\ncd "$R"\n' if in_dir else "cd /\n")
    return "main() {\n" + head + body + "}\nmain </dev/null\n"


# ─── the repository: provenance, bundle, toolchain (read on the laptop) ─────

def _git(clone: str, *args: str, check: bool = True) -> str:
    p = subprocess.run(["git", "-C", clone, *args], capture_output=True, text=True)
    if check and p.returncode != 0:
        raise Refused(f"git {' '.join(args[:2])} in {clone} failed (rc={p.returncode}): "
                      f"{redact_identity((p.stderr or '').strip().splitlines()[-1] if p.stderr.strip() else '')}")
    return p.stdout


_SCP_LIKE = re.compile(r"^(?:[^@/:]+@)?([^/:]+):(?!//)(.+)$")
_URL = re.compile(r"^(?:https?|ssh|git)://(?:[^@/]+@)?([^/:]+)(?::\d+)?/(.+)$")


def parse_remote(url: str) -> tuple[str, str] | None:
    """(host, owner/name) of a git remote URL, in its https, ssh:// or scp-like form (userinfo and a trailing .git
    dropped), or None for anything else (a local path, a bundle)."""
    u = (url or "").strip()
    m = _URL.match(u) or (None if "://" in u else _SCP_LIKE.match(u))
    if not m:
        return None
    path = m.group(2).rstrip("/").removesuffix(".git")
    parts = path.split("/")
    if len(parts) != 2 or not all(parts):
        return None
    return m.group(1).lower(), path


def declared_repo(repo: str) -> tuple[str, str]:
    parts = (repo or "").strip("/").split("/")
    if len(parts) == 2 and all(parts):
        return "github.com", "/".join(parts)
    if len(parts) == 3 and all(parts):
        return parts[0].lower(), "/".join(parts[1:])
    raise Refused(f"--repo {repo!r}: OWNER/NAME (or HOST/OWNER/NAME), the repository the clone is of")


def safe_url(url: str) -> str:
    """The origin URL as recorded: any userinfo (a token in an https URL) removed."""
    return re.sub(r"^((?:https?|ssh|git)://)[^@/]+@", r"\1", (url or "").strip())


def source(clone: str, repo: str, sha: str) -> dict:
    """The clone, checked against the caller's declaration, and the SHA in it: {repo, origin, sha, tree}."""
    if not Path(clone).is_dir():
        raise Refused(f"--clone {clone}: not a directory")
    host, path = declared_repo(repo)
    origin = _git(clone, "remote", "get-url", "origin").strip()
    got = parse_remote(origin)
    if got is None or got[0] != host or got[1].lower() != path.lower():
        raise Refused(f"the clone's origin is {safe_url(origin)!r}, not {host}/{path} (--repo): refusing to bundle it")
    commit = _git(clone, "rev-parse", "--verify", "--quiet", f"{sha}^{{commit}}", check=False).strip()
    if commit != sha:
        raise Refused(f"{sha} is not a commit in {clone} (fetch it first)")
    tree = _git(clone, "rev-parse", f"{sha}^{{tree}}").strip()
    return {"repo": f"{host}/{path}", "origin": safe_url(origin), "sha": sha, "tree": tree}


def make_bundle(clone: str, sha: str, have: list[str], out: Path) -> dict:
    """A bundle of `sha` (as refs/heads/testvm) with every SHA of `have` as a prerequisite, made in a bare scratch
    repository that borrows the clone's objects (the clone is never written). {bytes, sha256}."""
    objects = Path(_git(clone, "rev-parse", "--path-format=absolute", "--git-common-dir").strip()) / "objects"
    with tempfile.TemporaryDirectory() as d:
        subprocess.run(["git", "init", "-q", "--bare", d], check=True, capture_output=True)
        (Path(d) / "objects" / "info" / "alternates").write_text(f"{objects}\n")
        _git(d, "update-ref", "refs/heads/testvm", sha)
        out.parent.mkdir(parents=True, exist_ok=True)
        _git(d, "bundle", "create", "-q", str(out), "refs/heads/testvm", *[f"^{h}" for h in have if h != sha])
    return {"bytes": out.stat().st_size, "sha256": gcevm.sha256_file(str(out))}


_GOMOD_TOOLCHAIN = re.compile(r"^toolchain\s+go(\S+)\s*$", re.M)
_GOMOD_GO = re.compile(r"^go\s+(\S+)\s*$", re.M)
_PIP = re.compile(r"^\s*(?:python3?\s+-m\s+)?pip3?\s+install\b")


def go_release(v: str) -> str:
    """A go.mod version as go.dev names its release: from 1.21 on, `go 1.N` means 1.N.0."""
    m = re.fullmatch(r"1\.(\d+)", v)
    return f"{v}.0" if m and int(m.group(1)) >= 21 else v


def _go_key(v: str) -> tuple:
    m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?(.*)$", v)
    if not m:
        raise Refused(f"go version {v!r} in a go.mod is not one go.dev names")
    return int(m[1]), int(m[2]), int(m[3] or 0), m[4] == "", m[4]


def toolchain(clone: str, sha: str, workflow: str | None = None) -> dict:
    """What the suite's toolchain is, read from the SHA's tree: {"go", "go_from", "python", "python_from", "pip":
    [{"workflow", "dir", "run"}], "not_provided": [...]}."""
    files = _git(clone, "ls-tree", "-r", "--name-only", sha).splitlines()
    gos = {}
    for f in files:
        if f == "go.mod" or f.endswith("/go.mod"):
            text = _git(clone, "show", f"{sha}:{f}")
            m = _GOMOD_TOOLCHAIN.search(text) or _GOMOD_GO.search(text)
            if m:
                gos[f] = go_release(m.group(1))
    go = max(gos.values(), key=_go_key) if gos else None
    flows = sorted(f for f in files if f.startswith(".github/workflows/") and f.endswith((".yml", ".yaml")))
    if workflow is not None:
        if workflow not in flows:
            raise Refused(f"--workflow {workflow}: not a workflow in {sha} ({', '.join(flows) or 'none'})")
        flows = [workflow]
    import yaml
    pythons: dict[str, set] = {}
    pip: dict[str, list] = {}
    other = set()
    for f in flows:
        try:
            doc = yaml.safe_load(_git(clone, "show", f"{sha}:{f}")) or {}
        except yaml.YAMLError as e:
            raise Refused(f"{f} at {sha} is not YAML: {e}") from None
        for job in (doc.get("jobs") or {}).values():
            base = ((job.get("defaults") or {}).get("run") or {}).get("working-directory") or "."
            for step in job.get("steps") or []:
                uses = str(step.get("uses") or "")
                if uses.startswith("actions/setup-python"):
                    v = (step.get("with") or {}).get("python-version")
                    if v is not None:
                        pythons.setdefault(str(v), set()).add(f)
                elif re.match(r"actions/setup-(?!python)", uses):
                    other.add(f"{uses.split('@')[0]} ({f})")
                run = step.get("run")
                lines = [ln for ln in str(run or "").splitlines() if ln.strip() and not ln.strip().startswith("#")]
                if lines and all(_PIP.match(ln) for ln in lines):
                    pip.setdefault(f, []).append({"workflow": f, "dir": step.get("working-directory") or base,
                                                  "run": "\n".join(lines)})
    if len(pythons) > 1:
        raise Refused("the workflows set different Pythons (" + "; ".join(
            f"{v}: {', '.join(sorted(fs))}" for v, fs in sorted(pythons.items())) + "): --workflow names the one the "
            "suite mirrors")
    if len(pip) > 1:
        raise Refused(f"{len(pip)} workflows install Python packages ({', '.join(sorted(pip))}): --workflow names the "
                      f"one the suite mirrors")
    python = next(iter(pythons), None)
    return {"go": go, "go_from": sorted(f for f, v in gos.items() if v == go), "python": python,
            "python_from": sorted(pythons.get(python, ())), "pip": next(iter(pip.values()), []),
            "not_provided": sorted(other)}


def _describe_toolchain(tc: dict) -> str:
    return (f"go {tc['go'] or '(no go.mod)'}" + (f" ({', '.join(tc['go_from'])})" if tc["go_from"] else "")
            + f", python {tc['python'] or '(no setup-python)'}"
            + (f" ({', '.join(tc['python_from'])})" if tc["python_from"] else "")
            + f", {len(tc['pip'])} pip step(s)")


# ─── the VM, as made ────────────────────────────────────────────────────────

def startup_script() -> str:
    """The VM's bootstrap, as root at every boot (a no-op once its marker is written): the distribution's base
    tools only. Every versioned toolchain comes from the repo, per push (push_script)."""
    return f"""#!/bin/bash
# bench/testvm.py: a disposable VM for a test suite (no service account, no credentials)
set -euo pipefail
M={os.path.dirname(BOOTSTRAP_MARKER)}
mkdir -p {VM_ROOT}
chmod 1777 {VM_ROOT}
install -d -m 0755 "$M"
[ -s "$M/bootstrap.json" ] && exit 0
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q --no-install-recommends git ca-certificates curl python3 python3-venv gcc libc6-dev make
python3 - > "$M/bootstrap.json.tmp" <<'PY'
import json, subprocess
def out(*a):
    return subprocess.run(a, capture_output=True, text=True, check=True).stdout.strip().splitlines()[0]
print(json.dumps({{"git": out("git", "--version"), "python3": out("python3", "--version"),
                  "gcc": out("gcc", "--version"), "bash": out("bash", "--version")}}))
PY
mv "$M/bootstrap.json.tmp" "$M/bootstrap.json"
"""


def firewall_rules(run: str) -> dict[str, dict]:
    return gcevm.ssh_rules(vm_name(run), f"testvm owner={OWNER} run={run}")


def labels(run: str) -> dict[str, str]:
    return {"owner": OWNER, "testvm": "true", "run": run}


def create_command(run: str, project: str, zone: str, machine: str, startup_file: str, terminates_at: str) -> list[str]:
    return ["compute", "instances", "create", vm_name(run), *gcevm.where(project, zone),
            "--machine-type", machine,
            "--image-family", IMAGE_FAMILY, "--image-project", IMAGE_PROJECT,
            "--boot-disk-size", f"{BOOT_DISK_GB}GB", "--boot-disk-type", BOOT_DISK_TYPE, "--boot-disk-auto-delete",
            "--no-service-account", "--no-scopes",
            "--network", gcevm.NETWORK, "--tags", vm_name(run),
            "--labels", ",".join(f"{k}={v}" for k, v in labels(run).items()),
            "--metadata", "enable-oslogin=TRUE,block-project-ssh-keys=TRUE",
            "--metadata-from-file", f"startup-script={startup_file}",
            "--termination-time", terminates_at,
            "--instance-termination-action", "DELETE",
            "--shielded-secure-boot", "--shielded-vtpm", "--shielded-integrity-monitoring"]


# each describe field and the create flags that set it (tests check the flags of create_command are exactly these)
INSTANCE_FIELDS = {
    "name": ("<name>",), "zone": ("--project", "--zone"), "machineType": ("--machine-type",),
    "boot image": ("--image-family", "--image-project"),
    "boot disk": ("--boot-disk-size", "--boot-disk-type", "--boot-disk-auto-delete"),
    "serviceAccounts": ("--no-service-account", "--no-scopes"), "network": ("--network",), "tags": ("--tags",),
    "labels": ("--labels",), "metadata": ("--metadata", "--metadata-from-file"),
    "terminationTime": ("--termination-time",), "instanceTerminationAction": ("--instance-termination-action",),
    "shieldedInstanceConfig": ("--shielded-secure-boot", "--shielded-vtpm", "--shielded-integrity-monitoring"),
}


def instance_want(run: str, project: str, zone: str, machine: str, terminates_at: str) -> dict:
    name = vm_name(run)
    zurl = f"{gcevm.COMPUTE_API}/projects/{project}/zones/{zone}"
    return {"name": name, "zone": zurl, "machineType": f"{zurl}/machineTypes/{machine}",
            "boot image": {"project": IMAGE_PROJECT, "family": IMAGE_FAMILY},
            "boot disk": {"disks": 1, "boot": True, "autoDelete": True, "source": f"{zurl}/disks/{name}",
                          "sizeGb": str(BOOT_DISK_GB), "type": f"{zurl}/diskTypes/{BOOT_DISK_TYPE}"},
            "serviceAccounts": [],
            "network": [{"network": gcevm.network_url(project), "accessConfigs": ["ONE_TO_ONE_NAT"]}],
            "tags": [name], "labels": labels(run),
            "metadata": {"enable-oslogin": "TRUE", "block-project-ssh-keys": "TRUE",
                         "startup-script": gcevm.sha256_text(startup_script())},
            "terminationTime": gcevm.utc(terminates_at), "instanceTerminationAction": "DELETE",
            "shieldedInstanceConfig": {"enableSecureBoot": True, "enableVtpm": True, "enableIntegrityMonitoring": True}}


def as_made(run: str, project: str, zone: str, machine: str, terminates_at: str) -> dict:
    """What up creates, field for field: written to the record before anything is created, and every later
    read-back compares against it."""
    return {"instance": instance_want(run, project, zone, machine, terminates_at),
            "rules": {fw: gcevm.firewall_want(fw, r, project) for fw, r in firewall_rules(run).items()}}


def _made(st: dict, run: str) -> dict:
    made = st.get("made")
    if not isinstance(made, dict) or not isinstance(made.get("instance"), dict) \
            or set(made.get("rules") or {}) != set(firewall_rules(run)):
        raise Refused(f"run {run}'s record holds no as-made of what up created: its resources cannot be read back")
    return made


def instance_as_made(run: str, st: dict, inst: dict) -> list[str]:
    disk, image = gcevm.boot_disk_and_image(inst, st["project"], st["zone"])
    view = gcevm.instance_view(inst, disk, image, label_keys=LABEL_KEYS, account=lambda _e: "a service account")
    return gcevm.diff(f"instance {vm_name(run)}", view, _made(st, run)["instance"])


def _owned(inst: dict, run: str) -> None:
    got = inst.get("labels") or {}
    if inst.get("name") != vm_name(run):
        raise Refused(f"instance {inst.get('name')!r} is not {vm_name(run)!r}: refusing")
    if any(got.get(k) != v for k, v in labels(run).items()):
        raise Refused(f"instance {inst.get('name')!r} is not labelled {labels(run)} (labels: {got}): refusing")


def _trusted(run: str, st: dict) -> dict:
    """The run's instance, there, testvm's, and every field as up recorded making it: anything else refuses."""
    name = vm_name(run)
    inst = gcevm.instance(name, st["project"], st["zone"])
    if inst is None:
        raise Refused(f"{name}: GCE answers NotFound in {st['zone']}, the run's recorded zone (its termination time, "
                      f"or a down)")
    _owned(inst, run)
    off = instance_as_made(run, st, inst)
    if off:
        raise Refused(f"{name} is not as up made it: {'; '.join(off)}: refusing; nothing was changed")
    return inst


def _rule_as_made(run: str, st: dict, r: dict) -> list[str]:
    return gcevm.diff(f"firewall rule {r.get('name')}", gcevm.firewall_view(r), _made(st, run)["rules"][r.get("name")])


def machine_for(project: str, zone: str, parallelism: int) -> dict:
    """The smallest MACHINE_FAMILY type of `zone` with at least `parallelism` vCPU, read from GCE (one list of the
    zone's types, partial errors off, selected by exact equality): {"name", "guestCpus", "memoryMb"}."""
    rows = gcevm.list_json(["compute", "machine-types", "list", "--zones", zone, "--project", project,
                            "--format", "json"], f"the machine types of {zone}")
    fam = re.compile(rf"{re.escape(MACHINE_FAMILY)}-\d+")
    fit = [r for r in rows if gcevm.zone_of(r) == zone and fam.fullmatch(str(r.get("name")))
           and isinstance(r.get("guestCpus"), int) and r["guestCpus"] >= parallelism]
    if not fit:
        raise Refused(f"{zone} lists no {MACHINE_FAMILY} type with {parallelism} vCPU or more")
    best = min(fit, key=lambda r: r["guestCpus"])
    return {k: best.get(k) for k in ("name", "guestCpus", "memoryMb")}


def footprint(machine: dict) -> tuple:
    return (("INSTANCES", 1, "the VM", ""),
            (CPU_QUOTA, machine["guestCpus"], f"its {machine['name']}", ""),
            ("SSD_TOTAL_GB", BOOT_DISK_GB, f"its {BOOT_DISK_TYPE} boot disk", " GB"),
            ("IN_USE_ADDRESSES", 1, "its ephemeral external IP", ""))


def run_in_use(run: str, project: str) -> list[str]:
    found = []
    if (state_dir(run) / "state.json").exists():
        found.append(f"its record {state_dir(run) / 'state.json'}")
    for i in gcevm.instances_claiming(project, vm_name(run), run):
        found.append(f"instance {i.get('name')} ({gcevm.zone_of(i)})")
    found += [f"firewall rule {r['name']}" for r in gcevm.rule_docs(firewall_rules(run), project)]
    return found


def deadlines(now: float, hours: float, session_hours: float) -> tuple[str, str]:
    """(up's termination time, the session's ceiling): the setup window + the declared hours + the fetch window."""
    setup = BOOTSTRAP_TIMEOUT_S + PREPARE_TIMEOUT_S
    return (gcevm.rfc3339(now + setup + hours * 3600 + FETCH_WINDOW_S),
            gcevm.rfc3339(now + setup + session_hours * 3600 + FETCH_WINDOW_S))


# ─── up ─────────────────────────────────────────────────────────────────────

def cmd_up(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    check_sha(a.sha)
    if a.parallelism < 1:
        raise Refused("--parallelism: at least 1 (the suite's -p / -n: the vCPU it wants)")
    if not 0 < a.hours <= a.session_hours <= MAX_SESSION_HOURS:
        raise Refused(f"--hours {a.hours} and --session-hours {a.session_hours}: 0 < hours <= session-hours <= "
                      f"{MAX_SESSION_HOURS}")
    src = source(a.clone, a.repo, a.sha)
    tc = toolchain(a.clone, a.sha, a.workflow)
    gcevm.say(f"{src['repo']} at {a.sha} (tree {src['tree']}; origin {src['origin']}): {_describe_toolchain(tc)}")
    for x in tc["not_provided"]:
        gcevm.say(f"NOT PROVIDED: {x}: testvm installs Go and Python only")
    project, zone = a.project or DEFAULT_PROJECT, a.zone or DEFAULT_ZONE
    number = gcevm.parents(project, zone)
    gcevm.require_iap(project)
    used = run_in_use(run, project)
    if used:
        raise Refused(f"run {run} is in use: {'; '.join(used)}. A run name is used once: a new session takes a new "
                      f"--run")
    machine = machine_for(project, zone, a.parallelism)
    fits, refusals = gcevm.quota_check(project, zone, footprint(machine))
    for msg in fits:
        gcevm.say(msg)
    if refusals:
        raise Refused(f"the VM does not fit {gke.region_of(zone)}'s quotas: " + "; ".join(refusals))
    terminates_at, ceiling = deadlines(time.time(), a.hours, a.session_hours)
    d = state_dir(run)
    sf = d / "startup.sh"
    if a.yes:
        d.mkdir(parents=True, exist_ok=True)
        sf.write_text(startup_script())
        # record before create: what up is about to make, field for field, and the session it serves
        _save_state(run, made=as_made(run, project, zone, machine["name"], terminates_at))
        _save_state(run, vm=name, project=project, zone=zone, project_number=number, machine=machine,
                    parallelism=a.parallelism, hours=a.hours, session_hours=a.session_hours,
                    terminates_at=terminates_at, ceiling=ceiling, created_at=time.time(), source=src, runs=[],
                    pushes=[])
    for fw, rule in firewall_rules(run).items():
        rc, _o, err = gcloud(gcevm.firewall_create_argv(fw, rule, project), dry=not a.yes)
        if rc != 0:
            raise Refused(f"firewall rule {fw} could not be created (rc={rc}; {error_summary(err)}); {down_hint(run)}")
    if a.yes:
        st = _state(run)
        rules = {r["name"]: r for r in gcevm.rule_docs(firewall_rules(run), project)}
        off = [f"firewall rule {fw} answers NotFound" for fw in firewall_rules(run) if fw not in rules]
        off += [x for r in rules.values() for x in _rule_as_made(run, st, r)]
        if off:
            raise Refused(f"after their create, {'; '.join(off)}; {down_hint(run)}")
    rc, _o, err = gcloud(create_command(run, project, zone, machine["name"], str(sf), terminates_at), dry=not a.yes)
    if not a.yes:
        gcevm.say(f"DRY: {machine['name']} ({machine['guestCpus']} vCPU) for --parallelism {a.parallelism}; deletes "
                  f"itself at {terminates_at}, extendable to {ceiling}; would bundle {a.sha} and push it; nothing "
                  f"was created")
        return 0
    if rc != 0:
        raise Refused(f"instances create failed (rc={rc}; {error_summary(err)}); {down_hint(run)}")
    _save_state(run, instance_created_at=time.time())
    try:
        _trusted(run, _state(run))
    except Refused as e:
        raise Refused(f"{e}; {down_hint(run)}") from None
    gcevm.say(f"{name} created ({machine['name']}, no service account); it deletes itself and its disk at "
              f"{terminates_at}")
    got = gcevm.wait_for_marker(name, project, zone, remote(run, f"cat {shlex.quote(BOOTSTRAP_MARKER)} 2>/dev/null "
                                                                 f"|| true\n", in_dir=False),
                                ready_timeout_s=SSH_READY_TIMEOUT_S, timeout_s=BOOTSTRAP_TIMEOUT_S, poll_s=a.poll_s,
                                hint=down_hint(run), marker_path=BOOTSTRAP_MARKER)
    missing = [k for k in ("git", "python3") if not got.get(k)]
    if missing:
        raise Refused(f"{name}'s bootstrap lacks {missing}; {down_hint(run)}")
    _save_state(run, bootstrap=got)
    gcevm.say(f"{name} bootstrapped: " + ", ".join(f"{k} {redact_identity(str(v))}" for k, v in sorted(got.items())))
    rc, out, err = ssh(name, project, zone, remote(run, 'umask 077\nmkdir -p "$R/home" "$R/incoming" "$R/runs"\n'
                                                   'chmod 700 "$R"\n', in_dir=False), timeout=120)
    if rc != 0:
        raise _failed("making the run's directory on the VM", rc, out, err)
    push(run, src, tc, a.clone)
    return 0


# ─── push: a SHA's bundle, its checkout and its toolchain ───────────────────

_HASH_PY = ("python3 -c 'import hashlib,sys; h=hashlib.sha256(); f=open(sys.argv[1],\"rb\"); "
            "[h.update(b) for b in iter(lambda: f.read(1<<20), b\"\")]; print(h.hexdigest())'")
_RUNNING = ('running_run() {\n  for p in "$R"/runs/*/pid; do\n    [ -s "$p" ] || continue\n'
            '    if [ ! -e "$(dirname "$p")/done.json" ] && kill -0 "$(cat "$p")" 2>/dev/null; then\n'
            '      basename "$(dirname "$p")"; return\n    fi\n  done\n}\n')


def push_script(run: str, sha: str, tree: str, tc: dict) -> str:
    q = shlex.quote
    go = tc["go"]
    py = tc["python"]
    pip = "".join(f'echo "testvm: pip step in {s["dir"]} ({s["workflow"]})"\n'
                  f'( cd "$S"/{q(s["dir"])} && PATH="$R/venv/bin:$PATH" bash -euo pipefail -c {q(s["run"])} ) '
                  f'>> "$R/prepare.log" 2>&1 || {{ tail -n 20 "$R/prepare.log"; refuse "the pip step in {s["dir"]} '
                  f'failed ($R/prepare.log)"; }}\n'
                  for s in tc["pip"])
    go_block = "" if not go else f"""\
G="$T/go{go}"
if [ ! -x "$G/bin/go" ]; then
  case "$(uname -m)" in x86_64) arch=amd64;; aarch64|arm64) arch=arm64;; *) refuse "no Go release for $(uname -m)";; esac
  f="go{go}.linux-$arch.tar.gz"
  curl -fsSL 'https://go.dev/dl/?mode=json&include=all' -o "$T/go-dl.json"
  want="$(python3 -c 'import json,sys; print(next((x["sha256"] for r in json.load(open(sys.argv[1])) for x in r["files"] if x["filename"] == sys.argv[2]), ""))' "$T/go-dl.json" "$f")"
  [ -n "$want" ] || refuse "go.dev's download index lists no $f"
  curl -fsSLo "$T/$f" "https://go.dev/dl/$f"
  got="$({_HASH_PY} "$T/$f")"
  [ "$got" = "$want" ] || refuse "$f arrived with sha256 $got, go.dev lists $want"
  rm -rf "$G.tmp"; mkdir -p "$G.tmp"; tar -C "$G.tmp" -xzf "$T/$f"; mv "$G.tmp/go" "$G"; rm -rf "$G.tmp" "$T/$f"
fi
echo "TESTVM_GO=$("$G/bin/go" version)"
"""
    if py:
        venv = f"""\
U="$T/uv"
if [ ! -x "$U/bin/uv" ]; then python3 -m venv "$U"; "$U/bin/pip" install -q uv; fi
UV_PYTHON_INSTALL_DIR="$T/python" "$U/bin/uv" venv -q --seed --python {q(py)} "$R/venv"
"""
    elif tc["pip"]:
        venv = 'python3 -m venv "$R/venv"\n'
    else:
        venv = ""
    return remote(run, _RUNNING + f"""\
r="$(running_run)"; [ -z "$r" ] || refuse "run $r is still running: push once it has ended"
S="$R/src"; B="$R/incoming/{sha}.bundle"; T="$R/toolchains"
mkdir -p "$T"
[ -d "$S/.git" ] || git init -q "$S"
git -C "$S" bundle verify -q "$B" >/dev/null 2>&1 || refuse "the bundle does not verify against the checkout"
heads="$(git -C "$S" bundle list-heads "$B")"
[ "$heads" = "{sha} refs/heads/testvm" ] || refuse "the bundle's heads are not exactly {sha}"
git -C "$S" fetch -q "$B" "refs/heads/testvm:refs/testvm/{sha}"
git -C "$S" checkout -q -f --detach {sha}
git -C "$S" clean -q -ffdx
head="$(git -C "$S" rev-parse HEAD)"; [ "$head" = {sha} ] || refuse "the checkout is at $head, not {sha}"
t="$(git -C "$S" rev-parse 'HEAD^{{tree}}')"; [ "$t" = {tree} ] || refuse "the checkout's tree is $t, not {tree}"
[ -z "$(git -C "$S" status --porcelain)" ] || refuse "the checkout is not clean"
rm -f "$B"
echo "testvm: checkout at {sha} (tree {tree})"
echo "TESTVM_HEAD=$head"
echo "TESTVM_TREE=$t"
: > "$R/prepare.log"
{go_block}rm -rf "$R/venv"
{venv}{pip}if [ -x "$R/venv/bin/python" ]; then echo "TESTVM_PYTHON=$("$R/venv/bin/python" --version 2>&1)"; fi
""")


def _toolchain_mismatch(tc: dict, out: str) -> list[str]:
    bad = []
    got_go = gcevm.marker(out, "TESTVM_GO") or ""
    if tc["go"] and not got_go.startswith(f"go version go{tc['go']} "):
        bad.append(f"go: {got_go or 'none'}, the repo wants go{tc['go']}")
    got_py = gcevm.marker(out, "TESTVM_PYTHON") or ""
    if tc["python"] and not (got_py == f"Python {tc['python']}" or got_py.startswith(f"Python {tc['python']}.")):
        bad.append(f"python: {got_py or 'none'}, the repo wants {tc['python']}")
    return bad


def push(run: str, src: dict, tc: dict, clone: str) -> None:
    """Bundle the SHA (what the VM lacks only), stream it over, check it out, and install its toolchain."""
    st, project, zone = _recorded(run)
    name = vm_name(run)
    sha = src["sha"]
    have = [p["sha"] for p in st.get("pushes") or []]
    bundle = state_dir(run) / f"{sha}.bundle"
    b = make_bundle(clone, sha, have, bundle)
    gcevm.say(f"bundle of {sha}: {b['bytes']} bytes, sha256 {b['sha256']}"
              + (f" ({len(have)} earlier SHA(s) as prerequisites)" if have else ""))
    try:
        gcevm.upload(name, project, zone, str(bundle), f"{vm_dir(run)}/incoming/{sha}.bundle",
                     what="the bundle transfer", scratch=f"{vm_dir(run)}/incoming")
    finally:
        bundle.unlink(missing_ok=True)
    rc, out, err = ssh(name, project, zone, push_script(run, sha, src["tree"], tc), timeout=PREPARE_TIMEOUT_S)
    for ln in gcevm.remote_lines(out):
        if ln.startswith("testvm:"):
            gcevm.say(ln)
    if rc != 0:
        raise _failed("the VM's checkout and toolchain", rc, out, err)
    if gcevm.marker(out, "TESTVM_HEAD") != sha or gcevm.marker(out, "TESTVM_TREE") != src["tree"]:
        raise Refused(f"the VM reports its checkout at {gcevm.marker(out, 'TESTVM_HEAD')} (tree "
                      f"{gcevm.marker(out, 'TESTVM_TREE')}), not {sha} (tree {src['tree']})")
    bad = _toolchain_mismatch(tc, out)
    if bad:
        raise Refused(f"the VM's toolchain is not the repo's: {'; '.join(bad)}")
    entry = {**src, "bundle_sha256": b["sha256"], "bundle_bytes": b["bytes"], "toolchain": tc,
             "go_version": gcevm.marker(out, "TESTVM_GO"), "python_version": gcevm.marker(out, "TESTVM_PYTHON"),
             "pushed_at": time.time()}
    _save_state(run, pushes=[*(st.get("pushes") or []), entry], checkout=entry, source=src)
    tools = ", ".join(x for x in (entry["go_version"], entry["python_version"]) if x) or "no toolchain declared"
    gcevm.say(f"RUN: push of {sha}: checkout verified (HEAD and tree); {tools}")


def cmd_push(a) -> int:
    run = check_run(a.run)
    check_sha(a.sha)
    st, project, zone = _recorded(run)
    if st.get("deleted_at"):
        raise Refused(f"run {run} was taken down")
    src = source(a.clone, a.repo, a.sha)
    if st.get("source") and src["repo"] != st["source"]["repo"]:
        raise Refused(f"run {run} tests {st['source']['repo']}, not {src['repo']}: a session tests one repository")
    tc = toolchain(a.clone, a.sha, a.workflow)
    gcevm.say(f"{src['repo']} at {a.sha} (tree {src['tree']}; origin {src['origin']}): {_describe_toolchain(tc)}")
    for x in tc["not_provided"]:
        gcevm.say(f"NOT PROVIDED: {x}: testvm installs Go and Python only")
    gcevm.parents(project, zone, st.get("project_number"))
    _trusted(run, st)
    push(run, src, tc, a.clone)
    return 0


# ─── run ────────────────────────────────────────────────────────────────────

HOST_PY = r'''
import json, os, sys, time
def host():
    h = {"at": time.time(), "cpus": os.cpu_count()}
    try:
        h["load"] = list(os.getloadavg())
    except OSError:
        h["load"] = None
    try:
        mem = dict(ln.split(":", 1) for ln in open("/proc/meminfo"))
        h["mem_available_kb"] = int(mem["MemAvailable"].split()[0])
    except (OSError, KeyError, ValueError):
        h["mem_available_kb"] = None
    try:
        cpu = open("/proc/stat").readline().split()
        keys = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
        h["cpu_jiffies"] = dict(zip(keys, map(int, cpu[1:9])))
    except (OSError, ValueError, IndexError):
        h["cpu_jiffies"] = None
    return h
'''

RUNNER_PY = HOST_PY + r'''
import signal, subprocess
res, budget, grace = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
argv = sys.argv[5:]
start = host()
t0 = time.monotonic()
with open(os.path.join(res, "stdout.log"), "wb") as out, open(os.path.join(res, "stderr.log"), "wb") as err:
    p = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
    open(os.path.join(res, "pid.cmd"), "w").write(str(p.pid))
    ended = "exited"
    try:
        rc = p.wait(timeout=budget)
    except subprocess.TimeoutExpired:
        ended = "budget"
        os.killpg(p.pid, signal.SIGTERM)
        try:
            rc = p.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            ended = "budget, killed"
            os.killpg(p.pid, signal.SIGKILL)
            rc = p.wait()
    try:
        os.killpg(p.pid, signal.SIGKILL)       # nothing the command started outlives it
    except (ProcessLookupError, PermissionError):
        pass
elapsed = time.monotonic() - t0
end = host()
share = None
a, b = start.get("cpu_jiffies"), end.get("cpu_jiffies")
if a and b and sum(b.values()) > sum(a.values()):
    share = (b["steal"] - a["steal"]) / (sum(b.values()) - sum(a.values()))
json.dump({"start": start, "end": end, "steal_share": share}, open(os.path.join(res, "host.json"), "w"), indent=1)
json.dump({"rc": rc, "ended": ended, "elapsed_s": round(elapsed, 3), "budget_s": budget},
          open(os.path.join(res, "command.json"), "w"), indent=1)
'''

JUNIT_PY = r'''
import os, shutil, sys, xml.etree.ElementTree as ET
src, res, t0 = sys.argv[1], sys.argv[2], float(sys.argv[3])
dst = os.path.join(res, "junit")
found = 0
for root, dirs, files in os.walk(src):
    dirs[:] = [d for d in dirs if d != ".git"]
    for f in files:
        p = os.path.join(root, f)
        if not f.endswith(".xml") or os.path.getmtime(p) < t0:
            continue
        try:
            tag = next(ET.iterparse(p, events=("start",)))[1].tag
        except (ET.ParseError, StopIteration, OSError):
            continue
        if tag in ("testsuite", "testsuites"):
            rel = os.path.relpath(p, src)
            os.makedirs(os.path.dirname(os.path.join(dst, rel)), exist_ok=True)
            shutil.copy2(p, os.path.join(dst, rel))
            found += 1
for root, _dirs, files in os.walk(res):
    if root.startswith(dst):
        continue
    for f in files:
        if f.endswith(".xml"):
            found += 1
print(found)
'''

# ironic's cheap liveness probe of a cluster's controllers: a 1-replica Deployment, its ReplicaSet awaited, the
# Deployment deleted, and how long the ReplicaSet survives it timed (~1 s when healthy). Python's stdlib only (the VM
# has no kubectl); the kubeconfig is the JSON one testvm wrote. Recorded, never judged.
PROBE_PY = r'''
import base64, json, os, ssl, sys, time, urllib.request
kc, name, bound = sys.argv[1], sys.argv[2], float(sys.argv[3])
cfg = json.load(open(kc))
server = cfg["clusters"][0]["cluster"]["server"]
ca = cfg["clusters"][0]["cluster"].get("certificate-authority-data")
token = cfg["users"][0]["user"]["token"]
ctx = ssl.create_default_context(cadata=base64.b64decode(ca).decode()) if ca else None
def call(method, path, body=None):
    req = urllib.request.Request(server + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=bound) as r:
        return json.loads(r.read() or b"{}")
ns = "testvm"
sel = "/apis/apps/v1/namespaces/%s/replicasets?labelSelector=testvm-probe%%3D%s" % (ns, name)
out = {"deployment": name}
try:
    t0 = time.monotonic()
    call("POST", "/apis/apps/v1/namespaces/%s/deployments" % ns, {
        "apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"testvm-probe": name}},
                 "template": {"metadata": {"labels": {"testvm-probe": name}},
                              "spec": {"containers": [{"name": "probe", "image": "registry.k8s.io/pause"}]}}}})
    while not call("GET", sel).get("items"):
        if time.monotonic() - t0 > bound:
            raise TimeoutError("no ReplicaSet within %.0f s of the Deployment's create" % bound)
        time.sleep(0.05)
    out["replicaset_appeared_s"] = round(time.monotonic() - t0, 3)
    t1 = time.monotonic()
    call("DELETE", "/apis/apps/v1/namespaces/%s/deployments/%s" % (ns, name), {"propagationPolicy": "Background"})
    while call("GET", sel).get("items"):
        if time.monotonic() - t1 > bound:
            raise TimeoutError("the ReplicaSet outlived its Deployment by more than %.0f s" % bound)
        time.sleep(0.05)
    out["replicaset_survived_s"] = round(time.monotonic() - t1, 3)
except Exception as e:
    out["error"] = "%s: %s" % (type(e).__name__, str(e)[:200])
print(json.dumps(out))
'''


def run_env(st: dict, kubeconfig: str | None) -> str:
    """The command's environment on the VM: the repo's toolchain first on PATH, Go's own caches under the run's
    HOME, GOTOOLCHAIN=local (the installed Go is the repo's: nothing downloads another silently)."""
    tc = (st.get("checkout") or {}).get("toolchain") or {}
    lines = ['export PATH="$R/venv/bin:$PATH"\n'] if (tc.get("python") or tc.get("pip")) else []
    if tc.get("go"):
        lines.append(f'export PATH="$R/toolchains/go{tc["go"]}/bin:$HOME/go/bin:$PATH" GOTOOLCHAIN=local '
                     'GOPATH="$HOME/go" GOCACHE="$HOME/.cache/go-build"\n')
    if kubeconfig:
        lines.append(f"export KUBECONFIG={shlex.quote(kubeconfig)}\n")
    else:
        lines.append("unset KUBECONFIG\n")
    return "".join(lines)


def start_script(run: str, n: int, header: dict, argv: list[str], budget_s: int, env: str, kubeconfig: str | None,
                 probe: bool, wait_s: float) -> str:
    q = shlex.quote
    probe_at = (lambda at: f'python3 "$RES/probe.py" {q(kubeconfig)} testvm-probe-{n}-{at} {CLUSTER_PROBE_BOUND_S} '
                f'> "$RES/cluster-probe-{at}.json" 2>&1 || true\n') if probe else (lambda at: "")
    wrapper = (f"#!/bin/bash\nset -uo pipefail\nR={q(vm_dir(run))}\nRES=\"$R/runs/{n}\"\n"
               f"export HOME=\"$R/home\" USER=testvm LOGNAME=testvm TESTVM_RESULTS=\"$RES\"\n{env}"
               f"cd \"$R/src\"\n{probe_at('start')}"
               f"t0=\"$(python3 -c 'import time; print(time.time())')\"\n"
               f"python3 \"$RES/runner.py\" \"$RES\" {budget_s} {KILL_GRACE_S} -- {shlex.join(argv)}\n"
               f"{probe_at('end')}"
               f"python3 \"$RES/junit.py\" \"$R/src\" \"$RES\" \"$t0\" > \"$RES/junit.count\" 2>/dev/null || true\n"
               "python3 - \"$RES\" <<'PY'\nimport json, os, sys\nres = sys.argv[1]\n"
               "def load(f):\n    try:\n        return json.load(open(os.path.join(res, f)))\n"
               "    except (OSError, ValueError) as e:\n        return {\"error\": type(e).__name__}\n"
               "d = {\"command\": load(\"command.json\"), \"host\": load(\"host.json\")}\n"
               "for at in (\"start\", \"end\"):\n    if os.path.exists(os.path.join(res, \"cluster-probe-%s.json\" % at)):\n"
               "        d[\"cluster_probe_\" + at] = load(\"cluster-probe-%s.json\" % at)\n"
               "try:\n    d[\"junit\"] = int(open(os.path.join(res, \"junit.count\")).read().strip() or 0)\n"
               "except (OSError, ValueError):\n    d[\"junit\"] = None\n"
               "tmp = os.path.join(res, \"done.json.tmp\")\njson.dump(d, open(tmp, \"w\"), indent=1)\n"
               "os.replace(tmp, os.path.join(res, \"done.json\"))\nPY\n")
    files = {"header.json": json.dumps(header, indent=1, sort_keys=True) + "\n", "runner.py": RUNNER_PY,
             "junit.py": JUNIT_PY, "wrapper.sh": wrapper, **({"probe.py": PROBE_PY} if probe else {})}
    writes = "".join(f"cat > \"$RES/{f}\" <<'TESTVM_{i}'\n{body}TESTVM_{i}\n" for i, (f, body) in
                     enumerate(files.items()))
    return remote(run, _RUNNING + f"""\
r="$(running_run)"; [ -z "$r" ] || refuse "run $r is still running: one run at a time"
[ -d "$R/src/.git" ] || refuse "no checkout on the VM: push first"
RES="$R/runs/{n}"
[ ! -e "$RES" ] || refuse "$RES exists"
umask 077
mkdir -p "$RES"
{writes}chmod 700 "$RES/wrapper.sh"
nohup python3 -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' bash "$RES/wrapper.sh" \\
  > "$RES/wrapper.log" 2>&1 < /dev/null &
echo $! > "$RES/pid"
echo "TESTVM_STARTED=$(cat "$RES/pid")"
""" + wait_body(n, wait_s))


def wait_body(n: int, wait_s: float) -> str:
    return f"""\
RES="$R/runs/{n}"
deadline=$(( $(date +%s) + {int(wait_s)} ))
while [ ! -s "$RES/done.json" ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then echo "TESTVM_WAIT=timeout"; exit 0; fi
  if ! kill -0 "$(cat "$RES/pid")" 2>/dev/null && [ ! -s "$RES/done.json" ]; then
    sleep 1; [ -s "$RES/done.json" ] || {{ echo "TESTVM_WAIT=vanished"; tail -n 5 "$RES/wrapper.log" | sed 's/^/testvm: wrapper: /'; exit 0; }}
  fi
  sleep 1
done
echo "TESTVM_DONE=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))))' "$RES/done.json")"
"""


def _host_line(h: dict | None) -> str:
    if not isinstance(h, dict):
        return "absent"
    load = h.get("load")
    j = h.get("cpu_jiffies") or {}
    return (f"load {'/'.join(f'{x:.2f}' for x in load) if load else '?'} on {h.get('cpus')} CPU, MemAvailable "
            f"{h.get('mem_available_kb')} kB, steal {j.get('steal', '?')} of {sum(j.values()) if j else '?'} jiffies")


def _probe_line(p: dict | None) -> tuple[bool, str]:
    if not isinstance(p, dict):
        return False, "absent"
    if p.get("error"):
        return False, redact_identity(str(p["error"]))
    return True, (f"ReplicaSet appeared in {p.get('replicaset_appeared_s')} s, survived its Deployment by "
                  f"{p.get('replicaset_survived_s')} s")


def report(n: int, done: dict, probe: bool) -> int:
    """A RUN / NOT RUN line per phase of run n; the command's exit code (1 for any failure, 0 when it passed)."""
    host = done.get("host") or {}
    cmd = done.get("command") or {}
    gcevm.say(f"RUN: host health at start: {_host_line(host.get('start'))}")
    if probe:
        ok, line = _probe_line(done.get("cluster_probe_start"))
        gcevm.say(f"{'RUN' if ok else 'NOT RUN'}: cluster probe at start: {line}")
    rc = cmd.get("rc")
    if cmd.get("error") or rc is None:
        gcevm.say("NOT RUN: command: no record of its end")
    elif cmd.get("ended") == "exited":
        gcevm.say(f"RUN: command: exit {rc} in {cmd.get('elapsed_s')} s (budget {cmd.get('budget_s'):.0f} s)")
    else:
        gcevm.say(f"RUN: command: stopped at its budget of {cmd.get('budget_s'):.0f} s ({cmd.get('ended')}; exit {rc})")
    j = done.get("junit")
    gcevm.say(f"RUN: junit: {j} file(s)" if j else "NOT RUN: junit: the command wrote no JUnit XML")
    gcevm.say(f"RUN: host health at end: {_host_line(host.get('end'))}; steal share over the run "
              f"{host.get('steal_share') if host.get('steal_share') is not None else 'unknown'}")
    if probe:
        ok, line = _probe_line(done.get("cluster_probe_end"))
        gcevm.say(f"{'RUN' if ok else 'NOT RUN'}: cluster probe at end: {line}")
    return 0 if (rc == 0 and cmd.get("ended") == "exited") else 1


def cmd_run(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    argv = list(a.command or [])
    if argv[:1] == ["--"]:
        argv = argv[1:]
    if not argv:
        raise Refused("run: the command goes after `--`")
    if a.budget_minutes <= 0:
        raise Refused("--budget-minutes: the command's declared budget, more than 0")
    if a.cluster_probe and not a.cluster:
        raise Refused("--cluster-probe probes the run's cluster: it needs --cluster")
    st, project, zone = _recorded(run)
    if st.get("deleted_at"):
        raise Refused(f"run {run} was taken down")
    co = st.get("checkout")
    if not co:
        raise Refused(f"run {run} has no verified checkout: push first")
    gcevm.parents(project, zone, st.get("project_number"))
    inst = _trusted(run, st)
    budget_s = int(a.budget_minutes * 60)
    left = gcevm.time_left_s(inst, time.time())
    if left is None or left < budget_s + FETCH_WINDOW_S:
        raise Refused(f"{name} is deleted in {'an unknown time' if left is None else f'{left / 60:.0f} min'}: a "
                      f"{a.budget_minutes} min budget and the {FETCH_WINDOW_S // 60} min fetch window do not fit "
                      f"(`extend --run {run} --hours H --yes`, up to the session's ceiling {st.get('ceiling')})")
    kubeconfig = attach_cluster(run, st, a.cluster, a.cluster_location) if a.cluster else None
    st = _state(run)
    n = len(st.get("runs") or []) + 1
    header = {"run": run, "n": n, "vm": name, "machine": st.get("machine"), "repo": co["repo"],
              "origin": co["origin"], "sha": co["sha"], "tree": co["tree"], "argv": argv, "budget_s": budget_s,
              "cluster": a.cluster, "cluster_probe": bool(a.cluster_probe), "go": co.get("go_version"),
              "python": co.get("python_version"), "started_at": gcevm.rfc3339(time.time())}
    entry = {"n": n, "sha": co["sha"], "argv": argv, "budget_s": budget_s, "cluster": a.cluster,
             "started_at": time.time(), "fetched": False}
    _save_state(run, runs=[*(st.get("runs") or []), entry])            # recorded before it starts
    wait_s = budget_s + KILL_GRACE_S + (2 * 2 * CLUSTER_PROBE_BOUND_S if a.cluster_probe else 0) + 120
    script = start_script(run, n, header, argv, budget_s, run_env(st, kubeconfig), kubeconfig, a.cluster_probe, wait_s)
    t_end = time.monotonic() + wait_s + 300
    rc, out, err = ssh(name, project, zone, script, timeout=wait_s + 300)
    started = gcevm.marker(out, "TESTVM_STARTED")
    if rc != 0 and not started:
        raise _failed(f"starting run {n}", rc, out, err)
    gcevm.say(f"RUN: start: run {n} of {co['sha'][:12]} on {name}, detached (pid {started}), budget {budget_s} s"
              if started else f"NOT RUN: start: run {n}")
    while gcevm.marker(out, "TESTVM_DONE") is None:
        why = gcevm.marker(out, "TESTVM_WAIT")
        if why in ("timeout", "vanished") or time.monotonic() > t_end:
            for ln in gcevm.remote_lines(out):
                if ln.startswith("testvm:"):
                    gcevm.say(ln)
            raise Refused(f"run {n} on {name} reported no end ({why or 'the wait timed out'}); its files are on the "
                          f"VM: `fetch` brings back what it wrote")
        stage, transient = gcevm.ssh_stage(rc, err) if rc != 0 else ("", True)
        if not transient:
            raise Refused(f"waiting for run {n}: SSH failed at {stage}; the run goes on, detached")
        rc, out, err = ssh(name, project, zone, remote(run, wait_body(n, max(t_end - time.monotonic() - 60, 1))),
                           timeout=max(t_end - time.monotonic(), 1))
    done = json.loads(gcevm.marker(out, "TESTVM_DONE"))
    code = report(n, done, a.cluster_probe)
    runs = _state(run).get("runs") or []
    for r in runs:
        if r.get("n") == n:
            r.update(ended_at=time.time(), rc=(done.get("command") or {}).get("rc"),
                     ended=(done.get("command") or {}).get("ended"))
    _save_state(run, runs=runs)
    gcevm.say(f"run {n}: results in {vm_dir(run)}/runs/{n} on the VM; `python3 -m bench.testvm fetch --run {run}`")
    return code


# ─── clusters: a disposable GKE cluster's token, for one session ────────────

def _kubectl(kubeconfig: str, *args: str, input_data: str | None = None) -> tuple[int, str, str]:
    p = subprocess.run(["kubectl", "--kubeconfig", kubeconfig, *args], capture_output=True, text=True,
                       input=input_data, timeout=300)
    return p.returncode, p.stdout, p.stderr


def _cluster_rbac(run: str) -> str:
    sa = f"testvm-{run}"
    return json.dumps({"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": CLUSTER_NS}},
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": sa, "namespace": CLUSTER_NS,
                                                                     "labels": labels(run)}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
         "metadata": {"name": sa, "labels": labels(run)},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
         "subjects": [{"kind": "ServiceAccount", "name": sa, "namespace": CLUSTER_NS}]}]})


def _cluster(name: str, location: str, project: str) -> dict | None:
    """The GKE cluster by its exact name, or None on GKE's NotFound (bench.gke's rule: code=404 or NOT_FOUND,
    never a message text); any other failure refuses."""
    rc, out, err = gcloud(["container", "clusters", "describe", name, "--location", location, "--project", project,
                           "--format", "json"], quiet=True)
    if rc != 0:
        if gke._NOT_FOUND.search(err or ""):
            return None
        raise Refused(f"GKE cluster {name} could not be read (rc={rc}; {error_summary(err)}): refusing")
    got = gke._json(out)
    if not isinstance(got, dict) or got.get("name") != name:
        raise Refused(f"GKE cluster {name} answered no description of itself: refusing")
    return got


def _scratch_kubeconfig(run: str, name: str, location: str, project: str, d: str) -> str:
    kc = os.path.join(d, "kubeconfig")
    rc, _o, err = gcloud(["container", "clusters", "get-credentials", name, "--location", location,
                          "--project", project], env={"KUBECONFIG": kc})
    if rc != 0:
        raise Refused(f"credentials for GKE cluster {name} could not be read (rc={rc}; {error_summary(err)})")
    return kc


def _jwt_exp(token: str) -> float | None:
    try:
        payload = token.split(".")[1]
        return float(json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["exp"])
    except (IndexError, ValueError, KeyError, TypeError):
        return None


def attach_cluster(run: str, st: dict, name: str, location: str | None) -> str:
    """The VM path of the kubeconfig for cluster `name`, made the first time a run of the session targets it (see
    the module doc): a disposable bench cluster only; a token of ServiceAccount testvm-<run>, expiring at the
    session's ceiling. The laptop keeps no copy of the token."""
    if not location:
        raise Refused("--cluster: --cluster-location names its zone or region")
    project = st["project"]
    remote_kc = f"{vm_dir(run)}/kube/{name}.json"
    att = (st.get("clusters") or {}).get(name)
    ceiling = gcevm.epoch(st.get("ceiling")) or 0
    if att and att.get("location") == location and (att.get("expires") or 0) >= time.time() + 60:
        return remote_kc
    doc = _cluster(name, location, project)
    if doc is None:
        raise Refused(f"GKE cluster {name} ({location}) answers NotFound")
    try:
        gke.assert_deletable(doc)
    except gke.NotDeletable as e:
        raise Refused(f"--cluster {name}: not a disposable bench cluster ({e}): testvm hands no token for it") from None
    endpoint = doc.get("endpoint")
    ca = (doc.get("masterAuth") or {}).get("clusterCaCertificate")
    if not endpoint or not ca:
        raise Refused(f"GKE cluster {name} reports no endpoint or CA")
    # recorded before it is made: down revokes it even if this attach stops half way
    _save_state(run, clusters={**(st.get("clusters") or {}), name: {"location": location, "attached_at": time.time()}})
    with tempfile.TemporaryDirectory() as d:
        kc = _scratch_kubeconfig(run, name, location, project, d)
        rc, _o, err = _kubectl(kc, "apply", "-f", "-", input_data=_cluster_rbac(run))
        if rc != 0:
            raise Refused(f"the ServiceAccount testvm-{run} in {name} could not be made (rc={rc}; "
                          f"{error_summary(err)})")
        duration = max(int(ceiling - time.time()), 600)
        rc, token, err = _kubectl(kc, "-n", CLUSTER_NS, "create", "token", f"testvm-{run}",
                                  f"--duration={duration}s")
        token = token.strip()
        if rc != 0 or not token:
            raise Refused(f"no token for testvm-{run} in {name} (rc={rc}; {error_summary(err)})")
        exp = _jwt_exp(token)
        cfg = {"apiVersion": "v1", "kind": "Config", "current-context": name,
               "clusters": [{"name": name, "cluster": {"server": f"https://{endpoint}",
                                                       "certificate-authority-data": ca}}],
               "users": [{"name": f"testvm-{run}", "user": {"token": token}}],
               "contexts": [{"name": name, "context": {"cluster": name, "user": f"testvm-{run}"}}]}
        local = os.path.join(d, "vm-kubeconfig.json")
        fd = os.open(local, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(cfg, fh)
        del token, cfg
        gcevm.upload(vm_name(run), project, st["zone"], local, remote_kc, what=f"the kubeconfig of {name}",
                     scratch=f"{vm_dir(run)}/incoming")
    st = _state(run)
    _save_state(run, clusters={**(st.get("clusters") or {}), name: {"location": location, "attached_at": time.time(),
                                                                    "expires": exp}})
    gcevm.say(f"RUN: cluster {name}: ServiceAccount {CLUSTER_NS}/testvm-{run} (cluster-admin), its token on the VM "
              f"only, expiring at {gcevm.rfc3339(exp) if exp else 'an unknown time'}"
              + ("" if exp is None or exp >= ceiling - 60 else " (the API server's cap: before the session's ceiling)"))
    return remote_kc


def detach_clusters(run: str, st: dict) -> list[str]:
    """After the VM is gone: each attached cluster's binding and ServiceAccount deleted (the token revoked). What
    could not be revoked is reported, never a reason to stop: the token expires at the session's ceiling anyway."""
    notes = []
    for name, att in (st.get("clusters") or {}).items():
        doc = _cluster(name, att["location"], st["project"])
        if doc is None:
            gcevm.say(f"cluster {name}: gone (its ServiceAccount with it)")
            continue
        with tempfile.TemporaryDirectory() as d:
            try:
                kc = _scratch_kubeconfig(run, name, att["location"], st["project"], d)
            except Refused as e:
                notes.append(f"cluster {name}: {e}")
                continue
            for args in (["delete", "clusterrolebinding", f"testvm-{run}", "--ignore-not-found"],
                         ["-n", CLUSTER_NS, "delete", "serviceaccount", f"testvm-{run}", "--ignore-not-found"]):
                rc, _o, err = _kubectl(kc, *args)
                if rc != 0:
                    notes.append(f"cluster {name}: kubectl {' '.join(args[:3])} failed (rc={rc}; {error_summary(err)})")
        if not any(x.startswith(f"cluster {name}:") for x in notes):
            gcevm.say(f"cluster {name}: ServiceAccount {CLUSTER_NS}/testvm-{run} and its binding deleted (token revoked)")
    return notes


# ─── extend ─────────────────────────────────────────────────────────────────

def cmd_extend(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    st, project, zone = _recorded(run)
    if a.hours <= 0:
        raise Refused("--hours: more than 0")
    new = gcevm.rfc3339(time.time() + a.hours * 3600 + FETCH_WINDOW_S)
    if gcevm.epoch(new) > gcevm.epoch(st["ceiling"]):
        raise Refused(f"{new} is past the session's ceiling {st['ceiling']} (up's --session-hours): a longer session "
                      f"takes a new up")
    gcevm.parents(project, zone, st.get("project_number"))
    _trusted(run, st)
    rc, out, err = ssh(name, project, zone, remote(run, _RUNNING + 'echo "TESTVM_RUNNING=$(running_run)"\n'),
                       timeout=120)
    if rc != 0 or gcevm.marker(out, "TESTVM_RUNNING") is None:
        raise _failed("reading whether a run is running", rc, out, err)
    if gcevm.marker(out, "TESTVM_RUNNING"):
        raise Refused(f"run {gcevm.marker(out, 'TESTVM_RUNNING')} is running: GCE changes a termination time only on a "
                      f"stopped VM, which would kill it; extend once it has ended")
    stop = ["compute", "instances", "stop", name, *gcevm.where(project, zone), "--quiet"]
    sched = ["compute", "instances", "set-scheduling", name, *gcevm.where(project, zone), "--termination-time", new,
             "--instance-termination-action", "DELETE"]
    start = ["compute", "instances", "start", name, *gcevm.where(project, zone), "--quiet"]
    if not a.yes:
        for c in (stop, sched, start):
            gcloud(c, dry=True)
        gcevm.say(f"DRY: {name} would be deleted at {new} instead of {st['terminates_at']} (ceiling {st['ceiling']}); "
                  f"nothing was changed")
        return 0
    _save_state(run, extend_pending={"to": new, "from": st["terminates_at"], "at": time.time()})
    stopped = "the VM is STOPPED and holds no termination time (GCE clears it at stop): " + down_hint(run)
    rc, _o, err = gcloud(stop)
    if rc != 0:
        raise Refused(f"instances stop failed (rc={rc}; {error_summary(err)}); the VM still holds {st['terminates_at']}")
    rc, _o, err = gcloud(sched)
    if rc != 0:
        raise Refused(f"set-scheduling failed (rc={rc}; {error_summary(err)}); {stopped}")
    rc, _o, err = gcloud(start)
    if rc != 0:
        raise Refused(f"instances start failed (rc={rc}; {error_summary(err)}); the VM is stopped, deleted at {new}; "
                      f"{down_hint(run)}")
    made = _made(st, run)
    made["instance"]["terminationTime"] = gcevm.utc(new)
    st = _save_state(run, made=made, terminates_at=new, extend_pending=None,
                     extends=[*(st.get("extends") or []), {"to": new, "at": time.time()}])
    _trusted(run, st)
    gcevm.wait_for_marker(name, project, zone, remote(run, f"cat {shlex.quote(BOOTSTRAP_MARKER)} 2>/dev/null || true\n",
                                                      in_dir=False),
                          ready_timeout_s=SSH_READY_TIMEOUT_S, timeout_s=BOOTSTRAP_TIMEOUT_S, poll_s=a.poll_s,
                          hint=down_hint(run), marker_path=BOOTSTRAP_MARKER)
    gcevm.say(f"{name} now deletes itself at {new} (read back; ceiling {st['ceiling']}); SSH answers again")
    return 0


# ─── fetch ──────────────────────────────────────────────────────────────────

def fetch_script(run: str, ns: list[int]) -> str:
    return remote(run, f"""\
cd "$R"
have=""
for n in {' '.join(str(n) for n in ns)}; do
  if [ -s "runs/$n/done.json" ]; then have="$have runs/$n"; fi
done
[ -n "$have" ] || {{ echo "TESTVM_FETCH_RUNS="; exit 0; }}
rm -f "$R/fetch.tgz"
COPYFILE_DISABLE=1 tar -czf "$R/fetch.tgz" $have
echo "TESTVM_FETCH_RUNS=$have"
echo "TESTVM_FETCH_BYTES=$(python3 -c 'import os,sys; print(os.path.getsize(sys.argv[1]))' "$R/fetch.tgz")"
echo "TESTVM_FETCH_SHA256=$({_HASH_PY} "$R/fetch.tgz")"
""")


def cmd_fetch(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    st, project, zone = _recorded(run)
    todo = [r["n"] for r in st.get("runs") or [] if not r.get("fetched")]
    if not todo:
        gcevm.say(f"NOT RUN: fetch: every run of {run} is fetched already")
        return 0
    gcevm.parents(project, zone, st.get("project_number"))
    _trusted(run, st)
    rc, out, err = ssh(name, project, zone, fetch_script(run, todo), timeout=1800)
    if rc != 0:
        raise _failed("packing the results on the VM", rc, out, err)
    got = [int(x.split("/")[1]) for x in (gcevm.marker(out, "TESTVM_FETCH_RUNS") or "").split()]
    if not got:
        gcevm.say(f"NOT RUN: fetch: runs {todo} have not ended")
        return 0
    nbytes, sha = gcevm.marker(out, "TESTVM_FETCH_BYTES"), gcevm.marker(out, "TESTVM_FETCH_SHA256") or ""
    if not (nbytes or "").isdigit() or not HEX64.fullmatch(sha):
        raise Refused("the VM reported no size or sha256 for the results archive")
    d = state_dir(run)
    tgz = d / "fetch.tgz"
    gcevm.download(name, project, zone, f"{vm_dir(run)}/fetch.tgz", str(tgz), nbytes=int(nbytes), sha256=sha,
                   what="the results transfer")
    staging = d / "staging"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    allowed = {f"runs/{n}" for n in got}
    with tarfile.open(tgz) as tf:
        for m in tf.getmembers():
            if "/".join(Path(m.name).parts[:2]) not in allowed:
                raise Refused(f"the results archive holds {m.name!r}, outside runs {got}: nothing was kept")
        tf.extractall(staging, filter="data")
    results = d / "results"
    results.mkdir(exist_ok=True)
    for n in got:
        dest = results / str(n)
        if dest.exists():
            raise Refused(f"{dest} exists: a fetch never overwrites a result")
        shutil.move(str(staging / "runs" / str(n)), str(dest))
        gcevm.say(f"RUN: fetch: run {n} in {dest} (sha256 {sha} verified)")
    shutil.rmtree(staging)
    tgz.unlink()
    runs = _state(run).get("runs") or []
    for r in runs:
        if r["n"] in got:
            r.update(fetched=True, fetched_at=time.time(), fetch_sha256=sha)
    _save_state(run, runs=runs)
    for n in todo:
        if n not in got:
            gcevm.say(f"NOT RUN: fetch: run {n} has not ended")
    return 0


# ─── down ───────────────────────────────────────────────────────────────────

def cmd_down(a) -> int:
    run = check_run(a.run)
    name = vm_name(run)
    st, project, zone = _recorded(run)
    gcevm.parents(project, zone, st.get("project_number"))
    inst = gcevm.instance(name, project, zone)
    claims = gcevm.instances_claiming(project, name, run)
    drift: list[str] = []
    disks = [name]
    if inst is None:
        if claims:
            raise Refused(f"{name} answers NotFound in {zone} (the run's recorded zone), but {gcevm.named(claims)} "
                          f"claim(s) run {run}: refusing; nothing was changed")
    else:
        _owned(inst, run)
        drift += instance_as_made(run, st, inst)
        others = [i for i in claims if not (i.get("name") == name and gcevm.zone_of(i) == zone)]
        if others:
            raise Refused(f"besides {name} ({zone}), {gcevm.named(others)} claim(s) run {run}: refusing; nothing was "
                          f"changed")
        unfetched = [r["n"] for r in st.get("runs") or [] if not r.get("fetched")]
        if unfetched and not a.discard_unfetched:
            raise Refused(f"run(s) {unfetched} of {run} are not fetched: `fetch` first (--discard-unfetched lets them go "
                          f"with the VM)")
        disks = gcevm.disk_names(inst)
    rules = gcevm.rule_docs(firewall_rules(run), project)
    for r in rules:
        gcevm.owned_rule(r, _made(st, run)["rules"][r["name"]], "testvm", run)
        drift += _rule_as_made(run, st, r)
    for x in drift:
        gcevm.say(f"WARNING: drifted since up, deleted all the same: {x}")
    if a.yes:
        _save_state(run, down_drift=drift)
    if inst is not None:
        # the whole VM, never only its processes: whatever a killed suite left running dies with it
        rc, _o, err = gcloud(["compute", "instances", "delete", name, *gcevm.where(project, zone), "--delete-disks",
                              "all", "--quiet"], dry=not a.yes)
        if rc != 0:
            raise Refused(f"instances delete failed (rc={rc}; {error_summary(err)})")
        if a.yes and gcevm.instance(name, project, zone) is not None:
            raise Refused(f"{name} still answers in {zone} after its delete: its rules are left in place")
    else:
        gcevm.say(f"{name}: already gone (its termination time, or an earlier down)")
    for r in rules:
        rc, _o, err = gcloud(["compute", "firewall-rules", "delete", r["name"], "--project", project, "--quiet"],
                             dry=not a.yes)
        if rc != 0:
            raise Refused(f"firewall rule {r['name']} could not be deleted (rc={rc}; {error_summary(err)})")
    if not a.yes:
        gcevm.say(f"DRY: would verify {name} answers NotFound, its disks {disks} and its {len(rules)} firewall rule(s) "
                  f"gone, and revoke {len(st.get('clusters') or {})} cluster token(s); nothing was deleted")
        return 0
    left = [x for x in disks if gcevm.disk(x, project, zone) is not None]
    if left:
        raise Refused(f"disk(s) {left} remain: delete them by hand (gcloud compute disks delete ... --zone {zone})")
    still = [r["name"] for r in gcevm.rule_docs(firewall_rules(run), project)]
    if still:
        raise Refused(f"firewall rule(s) {still} of {name} remain after their delete")
    notes = detach_clusters(run, st)
    for x in notes:
        gcevm.say(f"WARNING: not revoked (the token expires at {st.get('ceiling')}): {x}")
    _save_state(run, deleted_at=time.time(), detach_notes=notes)
    gcevm.say(f"{name} gone and verified: disk(s) {disks}, {len(rules)} firewall rule(s)"
              + (f"; {len(drift)} field(s) had drifted since up (above, and in the run's record)" if drift else ""))
    return 0


# ─── CLI ────────────────────────────────────────────────────────────────────

def _zone(v: str) -> str:
    if not re.fullmatch(ZONE_PATTERN, v):
        raise argparse.ArgumentTypeError(f"{v!r} is not a zone (e.g. {DEFAULT_ZONE})")
    return v


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.testvm", description="a disposable GCE VM for a heavy test suite "
                                                                  "(see the module doc)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def source_args(p):
        p.add_argument("--repo", required=True, help="OWNER/NAME (or HOST/OWNER/NAME): the repository the clone is of; "
                                                     "its origin must resolve to it")
        p.add_argument("--clone", required=True, help="the local clone the bundle is made from")
        p.add_argument("--sha", required=True, help="the full 40-hex commit")
        p.add_argument("--workflow", default=None, help="the CI workflow the suite mirrors (when several disagree)")

    p = sub.add_parser("up", help="the VM, its SSH rules, its bootstrap, the first push")
    p.add_argument("--run", required=True)
    source_args(p)
    p.add_argument("--parallelism", type=int, required=True, help="the suite's parallelism: the vCPU it wants")
    p.add_argument("--hours", type=float, required=True, help="the hours of runs before the VM deletes itself")
    p.add_argument("--session-hours", type=float, required=True, help="the most `extend` may stretch the session to")
    p.add_argument("--project", default=None, help=f"default {DEFAULT_PROJECT}")
    p.add_argument("--zone", default=None, type=_zone, help=f"default {DEFAULT_ZONE}")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--poll-s", type=float, default=POLL_S, help=argparse.SUPPRESS)
    p.set_defaults(func=cmd_up)

    p = sub.add_parser("push", help="a later SHA to the same VM: bundle, checkout, toolchain")
    p.add_argument("--run", required=True)
    source_args(p)
    p.set_defaults(func=cmd_push)

    p = sub.add_parser("run", help="run a command in the checkout, under its budget")
    p.add_argument("--run", required=True)
    p.add_argument("--budget-minutes", type=float, required=True)
    p.add_argument("--cluster", default=None, help="a disposable GKE cluster (bench.gke) the suite targets")
    p.add_argument("--cluster-location", default=None)
    p.add_argument("--cluster-probe", action="store_true", help="record the cluster's controller liveness at start "
                                                                "and end")
    p.add_argument("command", nargs=argparse.REMAINDER)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("extend", help="move the VM's termination time, up to the session's ceiling")
    p.add_argument("--run", required=True)
    p.add_argument("--hours", type=float, required=True)
    p.add_argument("--yes", action="store_true")
    p.add_argument("--poll-s", type=float, default=POLL_S, help=argparse.SUPPRESS)
    p.set_defaults(func=cmd_extend)

    p = sub.add_parser("fetch", help="copy every ended run's results back, sha256 verified")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("down", help="delete the whole VM, its disk and its firewall rules, verified")
    p.add_argument("--run", required=True)
    p.add_argument("--discard-unfetched", action="store_true")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_down)

    a = ap.parse_args(argv)
    try:
        return a.func(a)
    except Refused as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
