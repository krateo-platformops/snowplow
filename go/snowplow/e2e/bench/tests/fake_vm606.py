"""A fake gcloud for bench/vm606.py: GCE instances and disks, IAP SSH and scp to a VM that is a
directory (tests/test_vm606.py). Modelled on gcloud 561's answers, not on an outcome:

  - `compute instances create` records the argv and makes the instance (labels, scheduling
    terminationTime, a boot disk named after it) and its disk; FAKE_BOOTSTRAP (JSON) is written as
    the VM's bootstrap marker at create, as the startup script would;
  - `compute instances describe`: the instance, or gcloud's 404 (which names the account);
  - `compute instances list` / `compute disks list` (--filter on labels / names): a list whose
    requests fail (FAKE_LIST_FAILS=1) exits 0 with "[]" and a WARNING unless the call's environment
    sets CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR=false, then rc 1 (#1172 B3, tests/fake_region.py);
  - `compute instances delete --delete-disks all`: the instance and its disk go (FAKE_KEEP_DISKS=1
    keeps the disk);
  - `compute ssh NAME ... --command CMD`: CMD runs under bash with the VM's environment only: HOME
    = <state>/vm/NAME/home, VM606_TEST_ROOT = <state>/vm/NAME/root, the test's PATH; stdin passed;
  - `compute scp`: the side written NAME:<path> is under the VM's home; the bytes are copied (this
    process is scp: copying is its job) and the source's mode kept, as scp does for a new file;
  - `projects describe`, `services list --enabled`, `iam service-accounts describe`: the project
    number, IAP enabled unless FAKE_IAP_DISABLED=1, the SA unless FAKE_NO_SA=1.

Every call is one JSON line in <FAKE_STATE>/calls.jsonl: its argv and the partial-error property.
"""

from __future__ import annotations

import json
from pathlib import Path

ACCOUNT_404 = ("ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n - The resource "
               "'projects/operations-dev-krateo-io/zones/europe-west4-a/instances/{name}' was not found. This command "
               "is authenticated as PLACEHOLDER-OPERATOR@example.invalid\n")

SOURCE = r'''#!/usr/bin/env python3
import json, os, re, shutil, subprocess, sys
from pathlib import Path
a = sys.argv[1:]
st = Path(os.environ["FAKE_STATE"])
(st / "instances").mkdir(parents=True, exist_ok=True)
(st / "disks").mkdir(parents=True, exist_ok=True)
with open(st / "calls.jsonl", "a") as fh:
    fh.write(json.dumps({"argv": a, "partial_error": os.environ.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR")}) + "\n")
def opt(n):
    return a[a.index(n) + 1] if n in a else None
def inst(name):
    p = st / "instances" / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None
def vm(name):
    return st / "vm" / name
def failed_list(kind):
    if os.environ.get("FAKE_LIST_FAILS") != "1":
        return
    why = f" - Required 'compute.{kind}.list' permission for 'projects/operations-dev-krateo-io'\n"
    if (os.environ.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR") or "").lower() == "false":
        print("[]"); sys.stderr.write(f"ERROR: (gcloud.compute.{kind}.list) Some requests did not succeed:\n{why}"); sys.exit(1)
    print("[]"); sys.stderr.write(f"WARNING: Some requests did not succeed.\n{why}"); sys.exit(0)
API = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io/zones/europe-west4-a"
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
if a[:3] == ["compute", "instances", "create"]:
    name = a[3]
    labels = dict(kv.split("=", 1) for kv in opt("--labels").split(","))
    doc = {"name": name, "status": "RUNNING", "labels": labels,
           "scheduling": {"terminationTime": opt("--termination-time"),
                          "instanceTerminationAction": opt("--instance-termination-action")},
           "disks": [{"boot": True, "autoDelete": True, "source": f"{API}/disks/{name}"}]}
    (st / "instances" / f"{name}.json").write_text(json.dumps(doc))
    (st / "disks" / name).write_text("{}")
    (vm(name) / "home").mkdir(parents=True, exist_ok=True)
    if os.environ.get("FAKE_BOOTSTRAP"):
        m = vm(name) / "root" / "var/lib/vm606/bootstrap.json"
        m.parent.mkdir(parents=True, exist_ok=True)
        m.write_text(os.environ["FAKE_BOOTSTRAP"])
    sys.exit(0)
if a[:3] == ["compute", "instances", "describe"]:
    d = inst(a[3])
    if d is None:
        sys.stderr.write(ACCOUNT_404.format(name=a[3])); sys.exit(1)
    print(json.dumps(d)); sys.exit(0)
if a[:3] == ["compute", "instances", "list"]:
    failed_list("instances")
    want = dict(re.findall(r"labels\.([a-z-]+)=([A-Za-z0-9_-]+)", opt("--filter") or ""))
    rows = [json.loads(p.read_text()) for p in sorted((st / "instances").glob("*.json"))]
    print(json.dumps([r for r in rows if all(r.get("labels", {}).get(k) == v for k, v in want.items())])); sys.exit(0)
if a[:3] == ["compute", "disks", "list"]:
    failed_list("disks")
    names = re.findall(r"name=([a-z0-9-]+)", opt("--filter") or "")
    print(json.dumps([{"name": p.name} for p in sorted((st / "disks").iterdir()) if p.name in names])); sys.exit(0)
if a[:3] == ["compute", "instances", "delete"]:
    name = a[3]
    if inst(name) is None:
        sys.stderr.write(ACCOUNT_404.format(name=name)); sys.exit(1)
    (st / "instances" / f"{name}.json").unlink()
    if opt("--delete-disks") == "all" and os.environ.get("FAKE_KEEP_DISKS") != "1":
        (st / "disks" / name).unlink()
        shutil.rmtree(vm(name), ignore_errors=True)
    sys.exit(0)
if a[:2] == ["compute", "ssh"]:
    name = a[2]
    if inst(name) is None:
        sys.stderr.write("ERROR: (gcloud.compute.ssh) Could not fetch resource\n"); sys.exit(1)
    home = vm(name) / "home"
    home.mkdir(parents=True, exist_ok=True)
    env = {"HOME": str(home), "PATH": os.environ["PATH"], "LANG": "C.UTF-8",
           "VM606_TEST_ROOT": str(vm(name) / "root")}
    p = subprocess.run(["bash", "-c", opt("--command")], cwd=home, env=env, stdin=sys.stdin)
    sys.exit(p.returncode)
if a[:2] == ["compute", "scp"]:
    src, dst = a[-2], a[-1]
    def where(x):
        if ":" in x and not x.startswith("/"):
            name, rel = x.split(":", 1)
            if inst(name) is None:
                sys.stderr.write("ERROR: (gcloud.compute.scp) Could not fetch resource\n"); sys.exit(1)
            return vm(name) / "home" / rel
        return Path(x)
    s, d = where(src), where(dst)
    if not s.is_file() or not d.parent.is_dir():
        sys.stderr.write("scp: No such file or directory\n"); sys.exit(1)
    shutil.copyfile(s, d)
    os.chmod(d, s.stat().st_mode & 0o777)
    sys.exit(0)
sys.stderr.write("fake gcloud: unknown call " + " ".join(a) + "\n")
sys.exit(2)
'''


def install(bin_dir: Path, state: Path) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    g = bin_dir / "gcloud"
    g.write_text(SOURCE.replace("API = ", f"ACCOUNT_404 = {ACCOUNT_404!r}\nAPI = ", 1))
    g.chmod(0o755)


def calls(state: Path) -> list[dict]:
    p = state / "calls.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


def put_instance(state: Path, name: str, labels: dict, termination_time: str) -> Path:
    """An instance as `create` makes it, for a test that starts after `up`; returns the VM's home."""
    (state / "instances").mkdir(parents=True, exist_ok=True)
    (state / "disks").mkdir(parents=True, exist_ok=True)
    api = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io/zones/europe-west4-a"
    (state / "instances" / f"{name}.json").write_text(json.dumps({
        "name": name, "status": "RUNNING", "labels": labels,
        "scheduling": {"terminationTime": termination_time, "instanceTerminationAction": "DELETE"},
        "disks": [{"boot": True, "autoDelete": True, "source": f"{api}/disks/{name}"}]}))
    (state / "disks" / name).write_text("{}")
    home = state / "vm" / name / "home"
    home.mkdir(parents=True, exist_ok=True)
    return home


def bootstrap_done(state: Path, name: str, doc: dict) -> None:
    m = state / "vm" / name / "root" / "var/lib/vm606/bootstrap.json"
    m.parent.mkdir(parents=True, exist_ok=True)
    m.write_text(json.dumps(doc))

