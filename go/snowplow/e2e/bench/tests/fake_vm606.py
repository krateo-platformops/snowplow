"""A fake gcloud for bench/vm606.py: GCE instances, disks and firewall rules, IAM policies, IAP SSH and
scp to a VM that is a directory (tests/test_vm606.py). Modelled on gcloud 561's answers, not on an
outcome:

  - the VM: <state>/vm/NAME. Its login user's home is identity-shaped, as OS Login makes it
    (home/IDENTITY, the operator's account as a POSIX name); VM_ROOT (/srv/snowplow-bench) is
    <state>/vm/NAME/srv: the fake rewrites that prefix in what it runs and copies, so the module's
    remote scripts carry no test hook;
  - `compute instances create` records the argv and makes the instance (labels, tags, scheduling
    terminationTime, a boot disk named after it) and its disk; FAKE_BOOTSTRAP (JSON) is written as
    the VM's bootstrap marker at create, as the startup script would;
  - `compute instances describe` / `firewall-rules describe`: the resource, or gcloud's 404 (which
    names the account);
  - `compute instances | disks | firewall-rules list` (--filter on labels / names): a list whose
    requests fail (FAKE_LIST_FAILS=1) exits 0 with "[]" and a WARNING unless the call's environment
    sets CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR=false, then rc 1 (#1172 B3, tests/fake_region.py);
  - `compute instances delete --delete-disks all`: the instance and its disk go (FAKE_KEEP_DISKS=1
    keeps the disk); `firewall-rules create / delete`;
  - `projects | iam service-accounts get-iam-policy / add-iam-policy-binding /
    remove-iam-policy-binding` with --condition-from-file: a v3 policy per resource; a remove matches
    the condition exactly, as gcloud does. FAKE_IAM_LOST_WRITE=<role>: the add of that role answers
    rc 0 and is lost (another writer's set-policy replaced it);
  - `compute ssh NAME ... --command CMD`: CMD runs under bash, the script on its stdin, with the
    VM's environment only (HOME = the identity-shaped home, the test's PATH), in a session of its
    own that is hung up (SIGHUP, then SIGTERM to its process group) when the command ends, as sshd
    does. FAKE_SSH_FAIL=4033|4003|publickey answers as gcloud does when IAP / the firewall / OS Login
    refuse, FAKE_SSH_FAIL_TIMES times (default: always). FAKE_TAMPER_AFTER_SSH="<n>:<path under srv>"
    appends a byte to that file when the n-th ssh call ends;
  - `compute scp`: the side written NAME:<path> is on the VM; the bytes are copied (this process is
    scp: copying is its job) and the source's mode kept, as scp does for a new file;
    FAKE_SCP_CORRUPT=<basename> flips the last byte of a file of that name in transit; FAKE_SCP_MODE
    (octal) is the mode the copy gets instead of the source's;
  - `projects describe`, `services list --enabled`, `iam service-accounts describe`: the project
    number, IAP enabled unless FAKE_IAP_DISABLED=1, the SA unless FAKE_NO_SA=1;
  - `compute regions describe REGION`: tests/fake_region.py's quotas (FAKE_LIMIT_<METRIC> steers a
    limit); `compute forwarding-rules | addresses list`: none. An instance carries its zone and one
    external IP (natIP), as GCE lists an instance with an ephemeral external IP.

Every call is one JSON line in <FAKE_STATE>/calls.jsonl: its argv and the partial-error property.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests import fake_region

IDENTITY = "jane_doe_example_com"       # an OS Login POSIX name: the operator's account
VM_ROOT = "/srv/snowplow-bench"

ACCOUNT_404 = ("ERROR: (gcloud.compute.{kind}.describe) Could not fetch resource:\n - The resource "
               "'projects/operations-dev-krateo-io/{where}/{name}' was not found. This command "
               "is authenticated as PLACEHOLDER-OPERATOR@example.invalid\n")
SSH_FAILURES = {
    "4033": (1, "ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [4033: 'not authorized'].\n"),
    "4003": (1, "ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [4003: 'failed to connect to "
                "backend'].\n"),
    "publickey": (255, f"{IDENTITY}@compute.123: Permission denied (publickey).\n"),
}

SOURCE = r'''#!/usr/bin/env python3
import json, os, re, shutil, signal, subprocess, sys
from pathlib import Path
a = sys.argv[1:]
st = Path(os.environ["FAKE_STATE"])
for d in ("instances", "disks", "firewalls", "policies"):
    (st / d).mkdir(parents=True, exist_ok=True)
with open(st / "calls.jsonl", "a") as fh:
    fh.write(json.dumps({"argv": a, "partial_error": os.environ.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR")}) + "\n")
def opt(n):
    return a[a.index(n) + 1] if n in a else None
def load(kind, name):
    p = st / kind / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None
def vm(name):
    return st / "vm" / name
def onvm(name, path):
    return Path(path.replace(VM_ROOT, str(vm(name) / "srv"), 1)) if path.startswith(VM_ROOT) else vm(name) / "home" / IDENTITY / path
def failed_list(kind):
    if os.environ.get("FAKE_LIST_FAILS") != "1":
        return
    why = f" - Required 'compute.{kind}.list' permission for 'projects/operations-dev-krateo-io'\n"
    if (os.environ.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR") or "").lower() == "false":
        print("[]"); sys.stderr.write(f"ERROR: (gcloud.compute.{kind}.list) Some requests did not succeed:\n{why}"); sys.exit(1)
    print("[]"); sys.stderr.write(f"WARNING: Some requests did not succeed.\n{why}"); sys.exit(0)
def notfound(kind, where, name):
    sys.stderr.write(ACCOUNT_404.format(kind=kind, where=where, name=name)); sys.exit(1)
API = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io/zones/europe-west4-a"
APIP = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io"
# ── project, services, SA, region ──
if a[:3] == ["compute", "regions", "describe"]:
    print(json.dumps(region_quotas(a[3]))); sys.exit(0)
if a[:2] == ["compute", "forwarding-rules"] or a[:2] == ["compute", "addresses"]:
    failed_list(a[1]); print("[]"); sys.exit(0)
if a[:2] == ["projects", "describe"]:
    print(os.environ.get("FAKE_PROJECT_NUMBER", "357378285144")); sys.exit(0)
if a[:3] == ["services", "list", "--enabled"]:
    if os.environ.get("FAKE_IAP_DISABLED") != "1":
        print("iap.googleapis.com")
    sys.exit(0)
if a[:3] == ["iam", "service-accounts", "describe"]:
    if os.environ.get("FAKE_NO_SA") == "1":
        sys.stderr.write("ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account\n"); sys.exit(1)
    print(a[3]); sys.exit(0)
# ── IAM policies ──
iam = None
if a[:1] == ["projects"] and "iam-policy" in a[1]:
    iam = (a[1], "project-" + a[2])
if a[:2] == ["iam", "service-accounts"] and "iam-policy" in a[2]:
    iam = (a[2], "sa-" + a[3])
if iam:
    verb, key = iam
    p = st / "policies" / f"{key}.json"
    pol = json.loads(p.read_text()) if p.exists() else {"version": 1, "etag": "BwX=", "bindings": []}
    if verb == "get-iam-policy":
        print(json.dumps(pol)); sys.exit(0)
    member, role = opt("--member"), opt("--role")
    cond = json.loads(Path(opt("--condition-from-file")).read_text()) if opt("--condition-from-file") else None
    if cond is not None:
        cond = {k: cond[k] for k in ("title", "expression", "description") if k in cond}
    if verb == "add-iam-policy-binding" and os.environ.get("FAKE_IAM_LOST_WRITE") == role:
        print(json.dumps(pol)); sys.exit(0)          # answered, then lost to a concurrent policy write
    if verb == "add-iam-policy-binding":
        for b in pol["bindings"]:
            if b["role"] == role and b.get("condition") == cond:
                if member not in b["members"]:
                    b["members"].append(member)
                break
        else:
            pol["bindings"].append({"role": role, "members": [member], **({"condition": cond} if cond else {})})
        pol["version"] = 3 if any("condition" in b for b in pol["bindings"]) else 1
    else:
        hit = [b for b in pol["bindings"] if b["role"] == role and b.get("condition") == cond and member in b["members"]]
        if not hit:
            sys.stderr.write(f"ERROR: (gcloud.{key.split('-')[0]}.remove-iam-policy-binding) Policy binding with the "
                             "specified principal, role, and condition not found!\n"); sys.exit(1)
        hit[0]["members"].remove(member)
        pol["bindings"] = [b for b in pol["bindings"] if b["members"]]
    p.write_text(json.dumps(pol))
    print(json.dumps(pol)); sys.exit(0)
# ── firewall rules ──
if a[:3] == ["compute", "firewall-rules", "create"]:
    name = a[3]
    if load("firewalls", name):
        sys.stderr.write(f"ERROR: (gcloud.compute.firewall-rules.create) The resource '{name}' already exists\n"); sys.exit(1)
    (st / "firewalls" / f"{name}.json").write_text(json.dumps({
        "name": name, "direction": opt("--direction"), "priority": int(opt("--priority")),
        "sourceRanges": opt("--source-ranges").split(","), "targetTags": opt("--target-tags").split(","),
        "description": opt("--description"), "network": opt("--network"),
        ("denied" if opt("--action") == "DENY" else "allowed"): [{"IPProtocol": "tcp", "ports": ["22"]}]}))
    sys.exit(0)
if a[:3] == ["compute", "firewall-rules", "list"]:
    failed_list("firewalls")
    m = re.search(r"name\s*~\s*(\S+)", opt("--filter") or "")
    rows = [json.loads(p.read_text()) for p in sorted((st / "firewalls").glob("*.json"))]
    print(json.dumps([r for r in rows if not m or re.search(m.group(1), r["name"])])); sys.exit(0)
if a[:3] == ["compute", "firewall-rules", "delete"]:
    if not load("firewalls", a[3]):
        notfound("firewall-rules", "global/firewalls", a[3])
    (st / "firewalls" / f"{a[3]}.json").unlink(); sys.exit(0)
# ── instances, disks ──
if a[:3] == ["compute", "instances", "create"]:
    name = a[3]
    labels = dict(kv.split("=", 1) for kv in opt("--labels").split(","))
    doc = {"name": name, "status": "RUNNING", "labels": labels, "tags": {"items": opt("--tags").split(",")},
           "zone": f"{APIP}/zones/{opt('--zone')}",
           "networkInterfaces": [{"networkIP": "10.164.0.9", "accessConfigs": [{"type": "ONE_TO_ONE_NAT",
                                                                               "natIP": "34.90.0.9"}]}],
           "scheduling": {"terminationTime": opt("--termination-time"),
                          "instanceTerminationAction": opt("--instance-termination-action")},
           "disks": [{"boot": True, "autoDelete": True, "source": f"{API}/disks/{name}"}]}
    (st / "instances" / f"{name}.json").write_text(json.dumps(doc))
    (st / "disks" / f"{name}.json").write_text("{}")
    (vm(name) / "home" / IDENTITY).mkdir(parents=True, exist_ok=True)
    (vm(name) / "srv").mkdir(parents=True, exist_ok=True)
    if os.environ.get("FAKE_BOOTSTRAP"):
        m = vm(name) / "srv" / ".bootstrap" / "bootstrap.json"
        m.parent.mkdir(parents=True, exist_ok=True)
        m.write_text(os.environ["FAKE_BOOTSTRAP"])
    sys.exit(0)
if a[:3] == ["compute", "instances", "describe"]:
    d = load("instances", a[3])
    if d is None:
        notfound("instances", "zones/europe-west4-a/instances", a[3])
    print(json.dumps(d)); sys.exit(0)
if a[:3] == ["compute", "instances", "list"]:
    failed_list("instances")
    want = dict(re.findall(r"labels\.([a-z-]+)=([A-Za-z0-9_-]+)", opt("--filter") or ""))
    rows = [json.loads(p.read_text()) for p in sorted((st / "instances").glob("*.json"))]
    print(json.dumps([r for r in rows if all(r.get("labels", {}).get(k) == v for k, v in want.items())])); sys.exit(0)
if a[:3] == ["compute", "disks", "list"]:
    failed_list("disks")
    names = re.findall(r"name=([a-z0-9-]+)", opt("--filter") or "")
    print(json.dumps([{"name": p.stem} for p in sorted((st / "disks").glob("*.json")) if p.stem in names])); sys.exit(0)
if a[:3] == ["compute", "instances", "delete"]:
    name = a[3]
    if load("instances", name) is None:
        notfound("instances", "zones/europe-west4-a/instances", name)
    (st / "instances" / f"{name}.json").unlink()
    if opt("--delete-disks") == "all" and os.environ.get("FAKE_KEEP_DISKS") != "1":
        (st / "disks" / f"{name}.json").unlink()
        shutil.rmtree(vm(name), ignore_errors=True)
    sys.exit(0)
# ── ssh, scp ──
if a[:2] == ["compute", "ssh"]:
    name = a[2]
    if load("instances", name) is None:
        sys.stderr.write("ERROR: (gcloud.compute.ssh) Could not fetch resource\n"); sys.exit(1)
    n = len([1 for ln in (st / "calls.jsonl").read_text().splitlines() if json.loads(ln)["argv"][:2] == ["compute", "ssh"]])
    fail = os.environ.get("FAKE_SSH_FAIL")
    if fail and n <= int(os.environ.get("FAKE_SSH_FAIL_TIMES", "1000000")):
        rc, text = SSH_FAILURES[fail]
        sys.stderr.write(text); sys.exit(rc)
    home = vm(name) / "home" / IDENTITY
    home.mkdir(parents=True, exist_ok=True)
    (vm(name) / "srv").mkdir(parents=True, exist_ok=True)
    script = sys.stdin.read().replace(VM_ROOT, str(vm(name) / "srv"))
    env = {"HOME": str(home), "PATH": os.environ["PATH"], "LANG": "C.UTF-8", "USER": IDENTITY}
    p = subprocess.Popen(["bash", "-c", opt("--command")], cwd=home, env=env, stdin=subprocess.PIPE,
                         start_new_session=True, text=True)
    p.communicate(script)
    for sig in (signal.SIGHUP, signal.SIGTERM):          # sshd hangs the session up
        try:
            os.killpg(p.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
    tamper = os.environ.get("FAKE_TAMPER_AFTER_SSH", "")
    if tamper and int(tamper.split(":", 1)[0]) == n:
        with open(vm(name) / "srv" / tamper.split(":", 1)[1], "ab") as fh:
            fh.write(b"\n")
    sys.exit(p.returncode)
if a[:2] == ["compute", "scp"]:
    src, dst = a[-2], a[-1]
    def where(x):
        if ":" in x and not x.startswith("/"):
            name, rel = x.split(":", 1)
            if load("instances", name) is None:
                sys.stderr.write("ERROR: (gcloud.compute.scp) Could not fetch resource\n"); sys.exit(1)
            return onvm(name, rel)
        return Path(x)
    s, d = where(src), where(dst)
    if not s.is_file() or not d.parent.is_dir():
        sys.stderr.write("scp: No such file or directory\n"); sys.exit(1)
    data = s.read_bytes()
    if os.environ.get("FAKE_SCP_CORRUPT") == s.name and data:
        data = data[:-1] + bytes([data[-1] ^ 1])
    d.write_bytes(data)
    os.chmod(d, int(os.environ["FAKE_SCP_MODE"], 8) if os.environ.get("FAKE_SCP_MODE") else s.stat().st_mode & 0o777)
    sys.exit(0)
sys.stderr.write("fake gcloud: unknown call " + " ".join(a) + "\n")
sys.exit(2)
'''


def install(bin_dir: Path, state: Path) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    g = bin_dir / "gcloud"
    consts = (f"ACCOUNT_404 = {ACCOUNT_404!r}\nSSH_FAILURES = {SSH_FAILURES!r}\nIDENTITY = {IDENTITY!r}\n"
              f"VM_ROOT = {VM_ROOT!r}\n")
    g.write_text(SOURCE.replace("API = ", consts + fake_region.LOADER + "API = ", 1))
    g.chmod(0o755)


def calls(state: Path) -> list[dict]:
    p = state / "calls.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


def put_instance(state: Path, name: str, labels: dict, termination_time: str, zone: str = "europe-west4-a") -> Path:
    """An instance as `create` makes it, for a test that starts after `up`; returns the VM's VM_ROOT."""
    for d in ("instances", "disks"):
        (state / d).mkdir(parents=True, exist_ok=True)
    api = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io/zones/europe-west4-a"
    (state / "instances" / f"{name}.json").write_text(json.dumps({
        "name": name, "status": "RUNNING", "labels": labels, "tags": {"items": [name]},
        "zone": f"https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io/zones/{zone}",
        "networkInterfaces": [{"networkIP": "10.164.0.9", "accessConfigs": [{"type": "ONE_TO_ONE_NAT",
                                                                            "natIP": "34.90.0.9"}]}],
        "scheduling": {"terminationTime": termination_time, "instanceTerminationAction": "DELETE"},
        "disks": [{"boot": True, "autoDelete": True, "source": f"{api}/disks/{name}"}]}))
    (state / "disks" / f"{name}.json").write_text("{}")
    (state / "vm" / name / "home" / IDENTITY).mkdir(parents=True, exist_ok=True)
    srv = state / "vm" / name / "srv"
    srv.mkdir(parents=True, exist_ok=True)
    return srv


def bootstrap_done(state: Path, name: str, doc: dict) -> None:
    m = state / "vm" / name / "srv" / ".bootstrap" / "bootstrap.json"
    m.parent.mkdir(parents=True, exist_ok=True)
    m.write_text(json.dumps(doc))


def policy(state: Path, key: str) -> dict:
    """key: project-<id> or sa-<email>."""
    p = state / "policies" / f"{key}.json"
    return json.loads(p.read_text()) if p.exists() else {"bindings": []}


def put_policy(state: Path, key: str, bindings: list) -> None:
    (state / "policies").mkdir(parents=True, exist_ok=True)
    (state / "policies" / f"{key}.json").write_text(json.dumps({"version": 3, "etag": "BwX=", "bindings": bindings}))
