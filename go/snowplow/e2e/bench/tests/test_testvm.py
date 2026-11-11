"""bench/testvm.py (#1338): a disposable GCE VM for a heavy test suite, through its CLI (main(argv)), against
vm606's fake gcloud (tests/fake_vm606.py): the VM is a directory, `compute ssh` runs the module's remote
scripts and byte streams there, `compute instances create` records what was asked. Nothing is created in GCP.

Every test checks at its end that the VM login user's identity-shaped home (fake_vm606.IDENTITY) appears in no
output and no laptop record, and that no gcloud call used --filter or `compute scp`.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import io
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path

import pytest

from bench import gcevm, testvm
from tests import fake_vm606

RUN = "oasgen1"
VM = testvm.vm_name(RUN)
PROJECT = "operations-dev-krateo-io"
ZONE = "europe-west3-a"
REPO = "krateo-platformops/oasgen-provider"
BOOT = {"git": "git version 2.47.3", "python3": "Python 3.13.5", "gcc": "gcc (Debian 14.2.0-19) 14.2.0",
        "bash": "GNU bash, version 5.2.37(1)-release"}
SENTINEL = "SENTINEL-CREDENTIAL-never-on-the-vm"
GO = "1.25.11"

_OUTPUT: list[str] = []

FAKE_CURL = r'''#!/usr/bin/env python3
# go.dev for the fake VM: its download index (JSON) and one archive, as curl -fsSL[o] fetches them. The fake VM
# passes its commands none of the test's environment: the archive, the log and the version are written in here
import hashlib, json, sys
TGZ, STATE, VERSION = "@TGZ@", "@STATE@", "@VERSION@"
a = sys.argv[1:]
url = next(x for x in a if x.startswith("https://"))
out = a[a.index("-o") + 1] if "-o" in a else a[a.index("-fsSLo") + 1]
tgz = open(TGZ, "rb").read()
with open(STATE + "/curl.log", "a") as fh:
    fh.write(url + "\n")
if "mode=json" in url:
    files = [{"filename": f"go{VERSION}.linux-{arch}.tar.gz", "kind": "archive",
              "sha256": hashlib.sha256(tgz).hexdigest()} for arch in ("amd64", "arm64")]
    open(out, "w").write(json.dumps([{"version": "go" + VERSION, "files": files}]))
else:
    open(out, "wb").write(tgz)
'''

FAKE_KUBECTL = r'''#!/usr/bin/env python3
# kubectl for the laptop side of --cluster: apply / create token / delete, each logged; the token is JWT-shaped
import base64, json, os, sys, time
a = sys.argv[1:]
with open(os.environ["FAKE_STATE"] + "/kubectl.log", "a") as fh:
    fh.write(json.dumps({"argv": a, "stdin": sys.stdin.read() if "apply" in a else None}) + "\n")
if "token" in a:
    exp = int(time.time()) + int(next(x for x in a if x.startswith("--duration=")).split("=")[1][:-1])
    body = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    print("eyJhbGciOiJSUzI1NiJ9." + body + "." + os.environ["FAKE_TOKEN_SIG"])
'''


def _go_tgz(path: Path) -> None:
    """A Go release archive's shape: go/bin/go, which says its version."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        body = f'#!/bin/sh\necho "go version go{GO} linux/amd64"\n'.encode()
        ti = tarfile.TarInfo("go/bin/go")
        ti.size, ti.mode = len(body), 0o755
        tf.addfile(ti, io.BytesIO(body))
    path.write_bytes(buf.getvalue())


@pytest.fixture
def fake(tmp_path, monkeypatch, run_trees):
    st = tmp_path / "gce"
    fake_vm606.install(tmp_path / "bin", st)
    curl = FAKE_CURL.replace("@TGZ@", str(tmp_path / "go.tgz")).replace("@STATE@", str(st)).replace("@VERSION@", GO)
    for name, body in (("curl", curl), ("kubectl", FAKE_KUBECTL)):
        (tmp_path / "bin" / name).write_text(body)
        (tmp_path / "bin" / name).chmod(0o755)
    _go_tgz(tmp_path / "go.tgz")
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_STATE", str(st))
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    monkeypatch.setenv("FAKE_TOKEN_SIG", SENTINEL)
    home = tmp_path / "laptop"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "laptop-data"))
    for k in ("FAKE_LIST_FAILS", "FAKE_KEEP_DISKS", "FAKE_IAP_DISABLED", "FAKE_SSH_FAIL", "FAKE_SSH_FAIL_TIMES",
              "FAKE_STREAM_STALL", "FAKE_LIMIT_E2_CPUS", "FAKE_FW_DELETE_LOST", "FAKE_INSTANCE_DELETE_LOST",
              "FAKE_INSTANCE_ON_CREATE", "FAKE_SET_SCHEDULING_FAIL", "FAKE_CREATE_FAIL"):
        monkeypatch.delenv(k, raising=False)
    _OUTPUT.clear()
    yield st
    records = [p.read_text(errors="replace") for p in (tmp_path / "laptop-data").rglob("*")
               if p.is_file() and p.suffix in (".json", ".log", ".sh")]
    for text in [*_OUTPUT, *records]:
        assert fake_vm606.IDENTITY not in text, text[-2000:]
        assert SENTINEL not in text, text[-2000:]              # the cluster token never reaches the laptop's record
    calls = fake_vm606.calls(st)
    assert not [c for c in calls if "--filter" in c["argv"]]
    assert not [c for c in calls if c["argv"][:2] == ["compute", "scp"]]     # every transfer is an ssh stream


def _run(capsys, *argv) -> tuple[int, str, str]:
    rc = testvm.main(list(argv))
    out, err = capsys.readouterr()
    _OUTPUT.extend([out, err])
    return rc, out, err


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@example.invalid", "-c", "user.name=t", *args],
                          check=True, capture_output=True, text=True).stdout.strip()


def _repo(tmp_path, origin: str = f"https://github.com/{REPO}.git", files: dict | None = None) -> tuple[Path, str]:
    """A clone of REPO's stand-in: a go.mod (go line and toolchain line), a package; returns (clone, its commit)."""
    r = tmp_path / "clone"
    r.mkdir()
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    for rel, body in {"go.mod": f"module example.com/x\n\ngo 1.25.6\n\ntoolchain go{GO}\n",
                      "tools/go.mod": "module example.com/x/tools\n\ngo 1.22\n",
                      "pkg/a.go": "package pkg\n", **(files or {})}.items():
        (r / rel).parent.mkdir(parents=True, exist_ok=True)
        if isinstance(body, bytes):
            (r / rel).write_bytes(body)
        else:
            (r / rel).write_text(body)
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "c1")
    _git(r, "remote", "add", "origin", origin)
    return r, _git(r, "rev-parse", "HEAD")


def _commit(r: Path, files: dict) -> str:
    for rel, body in files.items():
        (r / rel).parent.mkdir(parents=True, exist_ok=True)
        (r / rel).write_text(body)
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "next")
    return _git(r, "rev-parse", "HEAD")


def _up_args(clone, sha, *extra, hours="2", session="6", parallelism="4"):
    return ["up", "--run", RUN, "--repo", REPO, "--clone", str(clone), "--sha", sha, "--parallelism", parallelism,
            "--hours", hours, "--session-hours", session, "--zone", ZONE, "--poll-s", "0", *extra]


def _srv(fake) -> Path:
    return fake / "vm" / VM / "srv"


def _rundir(fake) -> Path:
    return _srv(fake) / RUN


def _state() -> dict:
    return json.loads((testvm.state_dir(RUN) / "state.json").read_text())


def _verbs(st) -> list[str]:
    return [" ".join(c["argv"][:3]) for c in fake_vm606.calls(st)]


def _up(fake, capsys, tmp_path, **kw):
    clone, sha = _repo(tmp_path, **kw)
    rc, out, err = _run(capsys, *_up_args(clone, sha, "--yes"))
    assert rc == 0, err + out
    return clone, sha, out


# ─── the bundle: exactly the SHA's tree ─────────────────────────────────────

def test_the_bundle_holds_exactly_the_shas_tree_and_nothing_of_the_working_tree(tmp_path):
    clone, sha1 = _repo(tmp_path, files={".gitignore": "*.secret\n"})
    sha2 = _commit(clone, {"pkg/b.go": "package pkg\n// two\n"})
    # the working tree holds what no commit does: an edit, an untracked and an ignored file
    (clone / "pkg" / "a.go").write_text("package pkg\n// uncommitted\n")
    (clone / ".env").write_text(SENTINEL)
    (clone / "x.secret").write_text(SENTINEL)
    out = tmp_path / "b1.bundle"
    b = testvm.make_bundle(str(clone), sha1, [], out)
    assert b["bytes"] == out.stat().st_size and b["sha256"] == hashlib.sha256(out.read_bytes()).hexdigest()
    assert SENTINEL.encode() not in out.read_bytes()
    # its one head is the SHA asked (not the clone's HEAD, sha2, of which sha1 is an ancestor)
    assert _git(clone, "bundle", "list-heads", str(out)) == f"{sha1} refs/heads/testvm"
    vm = tmp_path / "vm-checkout"
    subprocess.run(["git", "init", "-q", str(vm)], check=True)
    _git(vm, "fetch", "-q", str(out), f"refs/heads/testvm:refs/testvm/{sha1}")
    _git(vm, "checkout", "-q", "--detach", sha1)
    assert _git(vm, "rev-parse", "HEAD") == sha1                       # the SHA asked, not the clone's HEAD (sha2)
    assert _git(vm, "rev-parse", "HEAD^{tree}") == _git(clone, "rev-parse", f"{sha1}^{{tree}}")
    assert _git(vm, "ls-tree", "-r", "HEAD") == _git(clone, "ls-tree", "-r", sha1)
    assert not (vm / ".env").exists() and (vm / "pkg" / "a.go").read_text() == "package pkg\n"
    # a later push carries only what the VM lacks, and needs what it has
    out2 = tmp_path / "b2.bundle"
    testvm.make_bundle(str(clone), sha2, [sha1], out2)
    empty = tmp_path / "empty"
    subprocess.run(["git", "init", "-q", str(empty)], check=True)
    assert subprocess.run(["git", "-C", str(empty), "bundle", "verify", str(out2)], capture_output=True).returncode != 0
    _git(vm, "bundle", "verify", str(out2))                            # its prerequisite is sha1, which the VM has
    _git(vm, "fetch", "-q", str(out2), f"refs/heads/testvm:refs/testvm/{sha2}")
    assert _git(vm, "rev-parse", f"{sha2}^{{tree}}") == _git(clone, "rev-parse", f"{sha2}^{{tree}}")
    assert out2.stat().st_size < out.stat().st_size + 400
    assert "testvm" not in _git(clone, "for-each-ref")                  # the clone was never written


# ─── provenance: the clone must be of the repo the caller declares ──────────

@pytest.mark.parametrize("origin", [
    f"https://github.com/{REPO}.git", f"git@github.com:{REPO}.git", f"ssh://git@github.com/{REPO}",
    "https://x-access-token:" + SENTINEL + "@github.com/Krateo-Platformops/oasgen-provider.git"])
def test_an_origin_in_any_form_that_resolves_to_the_declared_repo_is_accepted_and_recorded_without_userinfo(
        tmp_path, origin):
    clone, sha = _repo(tmp_path, origin=origin)
    src = testvm.source(str(clone), REPO, sha)
    assert src["repo"] == f"github.com/{REPO}" and src["sha"] == sha
    assert src["tree"] == _git(clone, "rev-parse", f"{sha}^{{tree}}")
    assert SENTINEL not in src["origin"] and "github.com" in src["origin"]


@pytest.mark.parametrize("origin", [
    "https://github.com/krateoplatformops/oasgen-provider.git",      # the no-hyphen org's stale twin
    "git@github.com:krateo-platformops/oasgen-providers.git", "https://gitlab.com/krateo-platformops/oasgen-provider",
    "/srv/mirrors/oasgen-provider.git"])
def test_a_clone_whose_origin_is_not_the_declared_repo_is_refused_at_bundle_time(fake, capsys, tmp_path, origin):
    clone, sha = _repo(tmp_path, origin=origin)
    rc, out, err = _run(capsys, *_up_args(clone, sha, "--yes"))
    assert rc == 2 and "refusing to bundle it" in err and f"not github.com/{REPO} (--repo)" in err
    assert fake_vm606.calls(fake) == []                                   # before anything in GCP
    assert not (testvm.state_dir(RUN) / "state.json").exists()


# ─── the toolchain: derived from the repo ───────────────────────────────────

WORKFLOW = """
name: ci
on: [push]
jobs:
  t:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: tools
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v6
        with: {go-version-file: go.mod}
      - uses: actions/setup-node@v4
        with: {node-version: "22"}
      - uses: actions/setup-python@v5
        with: {python-version: "3.12"}
      - name: deps
        run: python -m pip install --help
      - run: go test ./...
"""


def test_the_toolchain_is_read_from_the_shas_tree(tmp_path):
    clone, sha = _repo(tmp_path, files={".github/workflows/ci.yaml": WORKFLOW, "tools/x.txt": ""})
    tc = testvm.toolchain(str(clone), sha)
    assert tc["go"] == GO and tc["go_from"] == ["go.mod"]                 # the highest: go.mod's toolchain line
    assert tc["python"] == "3.12" and tc["python_from"] == [".github/workflows/ci.yaml"]
    assert tc["pip"] == [{"workflow": ".github/workflows/ci.yaml", "dir": "tools", "run": "python -m pip install --help"}]
    assert tc["not_provided"] == ["actions/setup-go (.github/workflows/ci.yaml)",
                                  "actions/setup-node (.github/workflows/ci.yaml)"]
    assert testvm.go_release("1.22") == "1.22.0" and testvm.go_release("1.20") == "1.20"
    # a later SHA that moves the toolchain line moves the derivation: nothing is pinned here
    sha2 = _commit(clone, {"go.mod": "module example.com/x\n\ngo 1.26.1\n"})
    assert testvm.toolchain(str(clone), sha2)["go"] == "1.26.1"


def test_workflows_that_disagree_on_python_refuse_until_one_is_named(tmp_path):
    other = WORKFLOW.replace('"3.12"', '"3.11"').replace("python -m pip install --help", "echo no pip")
    clone, sha = _repo(tmp_path, files={".github/workflows/ci.yaml": WORKFLOW, ".github/workflows/old.yml": other})
    with pytest.raises(testvm.Refused, match=r"different Pythons \(3.11: .github/workflows/old.yml; 3.12: "):
        testvm.toolchain(str(clone), sha)
    assert testvm.toolchain(str(clone), sha, ".github/workflows/old.yml")["python"] == "3.11"
    with pytest.raises(testvm.Refused, match="not a workflow"):
        testvm.toolchain(str(clone), sha, ".github/workflows/nope.yaml")


# ─── up: no credentials, disposable, sized, quota first ─────────────────────

def test_create_flags_are_exactly_the_as_made_fields_and_carry_no_service_account():
    argv = testvm.create_command(RUN, PROJECT, ZONE, "e2-standard-4", "/s.sh", "2026-11-11T20:00:00Z")
    flags = {f for f in argv if f.startswith("--")}
    assert flags == {f for fl in testvm.INSTANCE_FIELDS.values() for f in fl if f.startswith("--")}
    assert set(testvm.instance_want(RUN, PROJECT, ZONE, "e2-standard-4", "x")) == set(testvm.INSTANCE_FIELDS)
    assert "--no-service-account" in argv and "--no-scopes" in argv
    assert "--service-account" not in argv and "--scopes" not in argv
    assert argv[argv.index("--labels") + 1] == f"owner=snowplow-testvm,testvm=true,run={RUN}"
    assert argv[argv.index("--instance-termination-action") + 1] == "DELETE"


def test_the_termination_time_and_the_ceiling_are_derived_from_the_declared_hours():
    now = 1_800_000_000.0
    t, ceiling = testvm.deadlines(now, 2, 6)
    setup = testvm.BOOTSTRAP_TIMEOUT_S + testvm.PREPARE_TIMEOUT_S
    assert gcevm.epoch(t) == now + setup + 2 * 3600 + testvm.FETCH_WINDOW_S
    assert gcevm.epoch(ceiling) == now + setup + 6 * 3600 + testvm.FETCH_WINDOW_S


def test_up_dry_prints_the_vm_and_creates_nothing(fake, capsys, tmp_path):
    clone, sha = _repo(tmp_path)
    rc, out, err = _run(capsys, *_up_args(clone, sha, parallelism="5"))
    assert rc == 0, err
    create = next(ln for ln in out.splitlines() if ln.startswith("DRY-RUN: gcloud compute instances create"))
    assert "--machine-type e2-standard-8" in create                  # the smallest e2-standard with >= 5 vCPU
    assert "--no-service-account --no-scopes" in create and "--termination-time 20" in create
    assert "E2_CPUS" in out                                           # the quota, read first
    assert not [v for v in _verbs(fake) if re.search(r"\b(create|delete)\b", v)]
    assert not (testvm.state_dir(RUN) / "state.json").exists()


def test_up_refuses_parallelism_no_e2_type_has_and_a_vm_the_quota_cannot_hold(fake, capsys, tmp_path, monkeypatch):
    clone, sha = _repo(tmp_path)
    rc, _o, err = _run(capsys, *_up_args(clone, sha, parallelism="33"))
    assert rc == 2 and "lists no e2-standard type with 33 vCPU or more" in err
    monkeypatch.setenv("FAKE_LIMIT_E2_CPUS", "4")
    rc, _o, err = _run(capsys, *_up_args(clone, sha, "--yes", parallelism="8"))
    assert rc == 2 and "does not fit" in err and "E2_CPUS" in err
    assert not [v for v in _verbs(fake) if re.search(r"\b(create|delete)\b", v)]


def test_up_makes_a_vm_with_no_credentials_and_pushes_the_exact_sha(fake, capsys, tmp_path):
    t0 = time.time()
    clone, sha, out = _up(fake, capsys, tmp_path)
    st = _state()
    inst = json.loads((fake / "instances" / f"{VM}.json").read_text())
    assert inst["serviceAccounts"] == []                              # no metadata token on the VM
    assert inst["labels"] == {"owner": "snowplow-testvm", "testvm": "true", "run": RUN}
    assert inst["scheduling"]["instanceTerminationAction"] == "DELETE"
    setup = testvm.BOOTSTRAP_TIMEOUT_S + testvm.PREPARE_TIMEOUT_S
    got = gcevm.epoch(inst["scheduling"]["terminationTime"])
    assert t0 + setup + 2 * 3600 + testvm.FETCH_WINDOW_S - 1 <= got <= time.time() + setup + 2 * 3600 + testvm.FETCH_WINDOW_S
    assert st["machine"]["name"] == "e2-standard-4" and st["ceiling"] > st["terminates_at"]
    # record before create: the as-made was on the laptop before the first create call
    assert st["made"]["instance"]["serviceAccounts"] == []
    src = _rundir(fake) / "src"
    assert _git(src, "rev-parse", "HEAD") == sha and _git(src, "status", "--porcelain") == ""
    assert _git(src, "rev-parse", "HEAD^{tree}") == st["checkout"]["tree"] == _git(clone, "rev-parse", f"{sha}^{{tree}}")
    assert st["checkout"]["repo"] == f"github.com/{REPO}" and st["checkout"]["origin"] == f"https://github.com/{REPO}.git"
    assert st["checkout"]["go_version"] == f"go version go{GO} linux/amd64"     # from go.dev's index, sha256 checked
    assert f"RUN: push of {sha}: checkout verified" in out
    assert re.search(r"bundle transfer: \d+ bytes, bound \d+ s, stall \d+ s", out)
    assert not (testvm.state_dir(RUN) / f"{sha}.bundle").exists()     # the laptop keeps no copy


def test_a_run_name_is_used_once(fake, capsys, tmp_path):
    clone, sha, _ = _up(fake, capsys, tmp_path)
    rc, _o, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 0, err
    n = len([v for v in _verbs(fake) if "instances create" in v])
    rc, _o, err = _run(capsys, *_up_args(clone, sha, "--yes"))
    assert rc == 2 and f"run {RUN} is in use: its record" in err and "used once" in err
    assert len([v for v in _verbs(fake) if "instances create" in v]) == n


def test_a_run_name_another_instance_claims_by_label_is_in_use(fake, capsys, tmp_path):
    clone, sha = _repo(tmp_path)
    fake_vm606.put_instance(fake, "someone-else", {"run": RUN}, "2030-01-01T00:00:00Z", zone="europe-west1-b")
    rc, _o, err = _run(capsys, *_up_args(clone, sha, "--yes"))
    assert rc == 2 and "instance someone-else (europe-west1-b)" in err
    assert not [v for v in _verbs(fake) if re.search(r"\b(create|delete)\b", v)]


# ─── no credential file is ever copied ──────────────────────────────────────

def test_no_credential_file_ever_reaches_the_vm(fake, capsys, tmp_path):
    home = Path(os.environ["HOME"])
    for rel in (".config/gcloud/application_default_credentials.json", ".config/gcloud/credentials.db",
                ".config/snowplow-bench/ghcr-pull.env", ".docker/config.json", ".kube/config", ".netrc",
                ".git-credentials", ".ssh/id_ed25519"):
        (home / rel).parent.mkdir(parents=True, exist_ok=True)
        (home / rel).write_text(SENTINEL)
        (home / rel).chmod(0o600)
    clone, sha = _repo(tmp_path, files={".gitignore": "*.key\n"})
    (clone / "deploy.key").write_text(SENTINEL)                       # ignored, never committed
    (clone / ".env").write_text(SENTINEL)                             # untracked
    rc, out, err = _run(capsys, *_up_args(clone, sha, "--yes"))
    assert rc == 0, err + out
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "bash", "-c", "env; ls -la ~")
    assert rc == 0, err + out
    rc, out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 0, err + out
    on_vm = [p for p in (fake / "vm" / VM).rglob("*") if p.is_file()]
    assert on_vm and not [p for p in on_vm if SENTINEL.encode() in p.read_bytes()]
    streams = [c["argv"] for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]]
    commands = [a[a.index("--command") + 1] for a in streams]
    # the laptop's paths never appear in what reaches the VM; only the probe and the bundle travel up
    assert not [c for c in commands if str(home) + os.sep in c or str(clone) + os.sep in c]
    uploads = [c for c in commands if "head -c" in c and ".part" in c]
    assert len(uploads) == 2 and "incoming/probe" in uploads[0] and f"incoming/{sha}.bundle" in uploads[1]
    create = next(c["argv"] for c in fake_vm606.calls(fake) if c["argv"][:3] == ["compute", "instances", "create"])
    assert "--no-service-account" in create and not [x for x in create if str(home) + os.sep in x]
    inst = json.loads((fake / "instances" / f"{VM}.json").read_text())
    assert {i["key"]: i["value"] for i in inst["metadata"]["items"]} == {
        "enable-oslogin": "TRUE", "block-project-ssh-keys": "TRUE", "startup-script": testvm.startup_script()}
    stdout = (testvm.state_dir(RUN) / "results" / "1" / "stdout.log").read_text()
    assert SENTINEL not in stdout and "TESTVM_RESULTS=" in stdout


# ─── run: many runs on one VM, results, budget, host health ─────────────────

JUNIT = '<?xml version="1.0"?><testsuites><testsuite name="x" tests="1"><testcase name="a"/></testsuite></testsuites>'


def test_many_runs_on_one_vm_each_with_its_results_and_the_session_survives(fake, capsys, tmp_path):
    clone, sha, _ = _up(fake, capsys, tmp_path)
    junit_cmd = (f"echo out-1; echo err-1 >&2; mkdir -p pkg/report; printf '%s' {shlex.quote(JUNIT)} > "
                 f"pkg/report/junit.xml; go version")
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "bash", "-c", junit_cmd)
    assert rc == 0, err + out
    assert re.search(r"^RUN: start: run 1 of [0-9a-f]{12} on testvm-oasgen1, detached", out, re.M)
    assert re.search(r"^RUN: command: exit 0 in [0-9.]+ s \(budget 300 s\)$", out, re.M)
    assert "RUN: junit: 1 file(s)" in out
    assert re.search(r"^RUN: host health at start: load [0-9./]+ on \d+ CPU", out, re.M)
    assert re.search(r"^RUN: host health at end: load", out, re.M)
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "bash", "-c", "echo two; exit 3")
    assert rc == 1 and "RUN: command: exit 3" in out and "NOT RUN: junit: the command wrote no JUnit XML" in out
    rc, out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 0, err + out
    r1 = testvm.state_dir(RUN) / "results" / "1"
    assert (r1 / "stdout.log").read_text().startswith("out-1\n") and "err-1" in (r1 / "stderr.log").read_text()
    assert f"go version go{GO}" in (r1 / "stdout.log").read_text()        # the repo's Go, first on PATH
    assert (r1 / "junit" / "pkg" / "report" / "junit.xml").read_text() == JUNIT
    header = json.loads((r1 / "header.json").read_text())
    assert header["repo"] == f"github.com/{REPO}" and header["origin"] == f"https://github.com/{REPO}.git"
    assert header["sha"] == sha and header["tree"] == _git(clone, "rev-parse", f"{sha}^{{tree}}")
    host = json.loads((r1 / "host.json").read_text())
    for at in ("start", "end"):
        assert set(host[at]) >= {"load", "cpus", "mem_available_kb", "cpu_jiffies"}
    assert json.loads((testvm.state_dir(RUN) / "results" / "2" / "done.json").read_text())["command"]["rc"] == 3
    st = _state()
    assert [r["fetched"] for r in st["runs"]] == [True, True]
    assert len([v for v in _verbs(fake) if "instances create" in v]) == 1     # one VM for the whole session


@pytest.mark.serial  # #1166/#1174: asserts the run ends `< 60` s of wall clock
def test_a_command_past_its_budget_is_stopped_with_its_whole_process_group(fake, capsys, tmp_path, monkeypatch):
    _up(fake, capsys, tmp_path)
    monkeypatch.setattr(testvm, "KILL_GRACE_S", 1)
    t0 = time.monotonic()
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "0.05", "--", "bash", "-c",
                        "trap '' TERM; sleep 60 & sleep 60; wait")
    assert rc == 1, err + out
    assert "RUN: command: stopped at its budget of 3 s (budget, killed" in out
    assert time.monotonic() - t0 < 60
    pid = int((_rundir(fake) / "runs" / "1" / "pid.cmd").read_text())
    with pytest.raises(ProcessLookupError):
        os.killpg(pid, 0)                                             # nothing it started outlives it


def test_run_refuses_a_budget_the_vm_does_not_outlive(fake, capsys, tmp_path):
    _up(fake, capsys, tmp_path)
    rc, _o, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "600", "--", "true")
    assert rc == 2 and "do not fit" in err and f"extend --run {RUN}" in err
    assert _state()["runs"] == []


def test_push_moves_the_same_vm_to_a_later_sha_with_its_own_toolchain(fake, capsys, tmp_path):
    clone, sha1, _ = _up(fake, capsys, tmp_path)
    sha2 = _commit(clone, {"pkg/a.go": "package pkg\n// fixed\n", ".github/workflows/ci.yaml": WORKFLOW,
                           "tools/x.txt": ""})
    # uv, as the VM's earlier push would have installed it: a stub whose venv reports the Python asked
    uv = _rundir(fake) / "toolchains" / "uv" / "bin" / "uv"
    uv.parent.mkdir(parents=True)
    uv.write_text("#!/bin/sh\nfor a; do v=\"$a\"; done\nmkdir -p \"$v/bin\"\nwhile [ $# -gt 0 ]; do [ \"$1\" = --python ] "
                  "&& p=\"$2\"; shift; done\n"
                  f"printf '#!/bin/sh\\n[ \"$1\" = --version ] && {{ echo \"Python %s.7\"; exit 0; }}\\nexec {sys.executable} "
                  f"\"$@\"\\n' \"$p\" > \"$v/bin/python\"\nchmod 755 \"$v/bin/python\"\n")
    uv.chmod(0o755)
    (_rundir(fake) / "src" / "build-output.bin").write_text("left by run 1")        # an untracked build output
    rc, out, err = _run(capsys, "push", "--run", RUN, "--repo", REPO, "--clone", str(clone), "--sha", sha2)
    assert rc == 0, err + out
    assert "(1 earlier SHA(s) as prerequisites)" in out
    assert "NOT PROVIDED: actions/setup-node (.github/workflows/ci.yaml)" in out
    src = _rundir(fake) / "src"
    assert _git(src, "rev-parse", "HEAD") == sha2 and not (src / "build-output.bin").exists()
    st = _state()
    assert st["checkout"]["sha"] == sha2 and st["checkout"]["python_version"] == "Python 3.12.7"
    assert "Usage:" in (_rundir(fake) / "prepare.log").read_text()       # the workflow's pip step, in tools/
    assert [p["sha"] for p in st["pushes"]] == [sha1, sha2]
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "cat", "pkg/a.go")
    assert rc == 0 and "exit 0" in out


def test_push_refuses_while_a_run_is_running(fake, capsys, tmp_path):
    clone, _sha, _ = _up(fake, capsys, tmp_path)
    res = _rundir(fake) / "runs" / "1"
    res.mkdir(parents=True)
    p = subprocess.Popen(["sleep", "30"])
    try:
        (res / "pid").write_text(str(p.pid))
        sha2 = _commit(clone, {"pkg/c.go": "package pkg\n"})
        rc, _o, err = _run(capsys, "push", "--run", RUN, "--repo", REPO, "--clone", str(clone), "--sha", sha2)
        assert rc == 2 and "run 1 is still running: push once it has ended" in err
    finally:
        p.kill()
        p.wait()


# ─── transfers: bounded, a stall refused by name ────────────────────────────

def _ssh_calls(fake) -> int:
    return len([c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]])


@pytest.mark.serial  # #1166/#1174: asserts the refusal inside its bound and stall window, on the wall clock
def test_a_stalled_bundle_upload_is_refused_by_name_within_its_bound(fake, capsys, tmp_path, monkeypatch):
    clone, sha = _repo(tmp_path, files={"blob.bin": os.urandom(3 << 20)})       # beyond any pipe's buffer
    # up's ssh calls: the bootstrap marker (1), the run's directory (2), the upload probe (3), the bundle (4)
    monkeypatch.setenv("FAKE_STREAM_STALL", "4:131072")
    seen = {}
    real = gcevm.stream

    def spy(argv, **kw):
        if kw["what"] == "the bundle transfer":
            seen.update(kw, t0=time.monotonic())
        return real(argv, **kw)
    monkeypatch.setattr(gcevm, "stream", spy)
    rc, out, err = _run(capsys, *_up_args(clone, sha, "--yes"))
    took = time.monotonic() - seen["t0"]
    assert rc == 2, out
    assert re.search(r"the bundle transfer stalled at \d+ of \d+ bytes \(no byte moved for \d+ s, 8x the probe\)", err)
    assert took < seen["bound_s"] and took < seen["stall_s"] + 5
    assert "checkout" not in _state()


def test_a_stalled_results_download_is_refused_and_nothing_is_kept(fake, capsys, tmp_path, monkeypatch):
    _up(fake, capsys, tmp_path)
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "true")
    assert rc == 0, err
    n = _ssh_calls(fake)
    # fetch's ssh calls: the pack (n+1), the download probe (n+2), the archive (n+3)
    monkeypatch.setenv("FAKE_STREAM_STALL", f"{n + 3}:0")
    rc, out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 2 and re.search(r"the results transfer stalled at 0 of \d+ bytes", err)
    assert not (testvm.state_dir(RUN) / "results").exists() and _state()["runs"][0]["fetched"] is False
    monkeypatch.delenv("FAKE_STREAM_STALL")
    rc, out, err = _run(capsys, "fetch", "--run", RUN)                  # a retry starts clean, and completes
    assert rc == 0 and "verified" in out


@pytest.mark.serial  # #1166/#1174: asserts the refusal `< 10` s of wall clock
def test_a_transfer_that_completes_passes_and_one_slower_than_its_bound_is_refused(tmp_path, monkeypatch):
    src = tmp_path / "src"
    src.write_bytes(os.urandom(1 << 20))
    dst = tmp_path / "dst"
    p = tmp_path / "g"
    p.mkdir()
    (p / "gcloud").write_text("#!/bin/sh\nexec cat \"$1\"\n")
    (p / "gcloud").chmod(0o755)
    monkeypatch.setenv("PATH", f"{p}{os.pathsep}{os.environ['PATH']}")
    rc, _o, _e = gcevm.stream([str(src)], what="the copy", nbytes=1 << 20, bound_s=60, stall_s=60, dst=str(dst))
    assert rc == 0 and dst.read_bytes() == src.read_bytes()
    # a trickle: bytes keep moving (no stall), slower than the bound allows
    (p / "gcloud").write_text("#!/bin/sh\nwhile :; do head -c 1024 \"$1\"; sleep 0.2; done\n")
    t0 = time.monotonic()
    with pytest.raises(gcevm.Refused, match=r"the copy ran past its bound of 2 s at \d+ of 1048576 bytes"):
        gcevm.stream([str(src)], what="the copy", nbytes=1 << 20, bound_s=2, stall_s=60, dst=str(dst))
    assert time.monotonic() - t0 < 10


def test_the_bound_and_the_stall_window_scale_with_the_measured_rate():
    fast, slow = gcevm.Rate(4 << 20, 1.0), gcevm.Rate(4 << 20, 10.0)
    assert slow.bound_s(100 << 20) == 10 * fast.bound_s(100 << 20)
    assert fast.bound_s(100 << 20) == gcevm.SLACK * 1.0 * (1 + 25)
    assert slow.stall_s() == 10 * fast.stall_s() == gcevm.SLACK * 10.0


# ─── extend: an explicit, recorded deadline under the ceiling ───────────────

def test_extend_moves_the_termination_time_within_the_ceiling_and_records_it(fake, capsys, tmp_path):
    _up(fake, capsys, tmp_path)
    rc, out, err = _run(capsys, "extend", "--run", RUN, "--hours", "5", "--yes", "--poll-s", "0")
    assert rc == 0, err + out
    st = _state()
    inst = json.loads((fake / "instances" / f"{VM}.json").read_text())
    assert gcevm.utc(inst["scheduling"]["terminationTime"]) == st["terminates_at"] == st["extends"][-1]["to"]
    assert inst["status"] == "RUNNING" and st["extend_pending"] is None
    verbs = _verbs(fake)
    i = verbs.index("compute instances stop")
    assert verbs[i + 1] == "compute instances set-scheduling" and verbs[i + 2] == "compute instances start"
    rc, _o, err = _run(capsys, "extend", "--run", RUN, "--hours", "7", "--yes")
    assert rc == 2 and "past the session's ceiling" in err
    assert len([v for v in _verbs(fake) if v == "compute instances stop"]) == 1


def test_extend_refuses_while_a_run_is_running_and_names_a_stopped_vm_with_no_time(fake, capsys, tmp_path, monkeypatch):
    _up(fake, capsys, tmp_path)
    res = _rundir(fake) / "runs" / "1"
    res.mkdir(parents=True)
    p = subprocess.Popen(["sleep", "30"])
    try:
        (res / "pid").write_text(str(p.pid))
        rc, _o, err = _run(capsys, "extend", "--run", RUN, "--hours", "3", "--yes")
        assert rc == 2 and "run 1 is running" in err and "stopped VM" in err
    finally:
        p.kill()
        p.wait()
    assert "compute instances stop" not in _verbs(fake)
    (res / "done.json").write_text("{}")
    monkeypatch.setenv("FAKE_SET_SCHEDULING_FAIL", "1")
    rc, _o, err = _run(capsys, "extend", "--run", RUN, "--hours", "3", "--yes")
    assert rc == 2 and "STOPPED and holds no termination time" in err and "down --run" in err
    assert _state()["extend_pending"]["to"]                               # recorded before the change


# ─── down: the whole VM, verified ───────────────────────────────────────────

def test_down_deletes_the_whole_vm_its_disk_and_rules_and_verifies(fake, capsys, tmp_path):
    _up(fake, capsys, tmp_path)
    rc, _o, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "true")
    rc, _o, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "not fetched" in err                                # results are not lost silently
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 0, err + out
    assert not (fake / "instances" / f"{VM}.json").exists() and not (fake / "disks" / f"{VM}.json").exists()
    assert not list((fake / "firewalls").glob("*.json")) and not (fake / "vm" / VM).exists()
    assert "gone and verified" in out and _state()["deleted_at"]
    delete = next(c["argv"] for c in fake_vm606.calls(fake) if c["argv"][:3] == ["compute", "instances", "delete"])
    assert delete[delete.index("--delete-disks") + 1] == "all"


def test_down_deletes_what_drifted_and_reports_it(fake, capsys, tmp_path):
    _up(fake, capsys, tmp_path)
    p = fake / "instances" / f"{VM}.json"
    inst = json.loads(p.read_text())
    inst["metadata"]["items"].append({"key": "ssh-keys", "value": "someone:ssh-ed25519 AAAA"})
    p.write_text(json.dumps(inst))
    rc, _o, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--", "true")
    assert rc == 2 and "not as up made it" in err and "ssh-keys" in err      # trusted nowhere else
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err + out
    assert "WARNING: drifted since up, deleted all the same: instance testvm-oasgen1 metadata: ssh-keys" in out
    assert _state()["down_drift"] and not p.exists()


@pytest.mark.parametrize("lost, why", [("FAKE_INSTANCE_DELETE_LOST", "still answers"),
                                       ("FAKE_FW_DELETE_LOST", "remain after their delete")])
def test_down_verifies_each_delete_and_refuses_one_that_did_not_happen(fake, capsys, tmp_path, monkeypatch, lost, why):
    _up(fake, capsys, tmp_path)
    monkeypatch.setenv(lost, "1")
    rc, _o, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and why in err
    assert "deleted_at" not in _state()


# ─── a disposable GKE cluster: a scoped token, revoked at down ──────────────

def _gke(fake, name="snowplow-bench-oasgen", **labels):
    (fake / "gke").mkdir(exist_ok=True)
    rl = {**{"purpose": "snowplow-bench", "issue": "606", "created-by": "bench-gke", "ttl-expires": "2000000000"},
          **labels}
    (fake / "gke" / f"{name}.json").write_text(json.dumps({
        "name": name, "location": "europe-west3", "endpoint": "34.1.2.3", "resourceLabels": rl,
        "masterAuth": {"clusterCaCertificate": base64.b64encode(b"CA").decode()}}))


def test_run_cluster_hands_the_suite_a_scoped_kubeconfig_and_down_revokes_it(fake, capsys, tmp_path):
    _up(fake, capsys, tmp_path)
    _gke(fake)
    rc, out, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--cluster", "snowplow-bench-oasgen",
                        "--cluster-location", "europe-west3", "--", "bash", "-c", 'echo "KC=$KUBECONFIG"')
    assert rc == 0, err + out
    kc = _rundir(fake) / "kube" / "snowplow-bench-oasgen.json"
    assert stat.S_IMODE(kc.stat().st_mode) == 0o600
    cfg = json.loads(kc.read_text())
    assert cfg["clusters"][0]["cluster"]["server"] == "https://34.1.2.3" and SENTINEL in cfg["users"][0]["user"]["token"]
    assert f"KC={testvm.VM_ROOT}/{RUN}/kube/snowplow-bench-oasgen.json" in (_rundir(fake) / "runs" / "1" /
                                                                            "stdout.log").read_text().replace(
        str(_srv(fake)), testvm.VM_ROOT)
    log = [json.loads(x) for x in (fake / "kubectl.log").read_text().splitlines()]
    applied = json.loads(log[0]["stdin"])["items"]
    assert [i["kind"] for i in applied] == ["Namespace", "ServiceAccount", "ClusterRoleBinding"]
    assert log[1]["argv"][-3:-1] == ["token", f"testvm-{RUN}"]
    want = gcevm.epoch(_state()["ceiling"]) - time.time()
    assert abs(int(log[1]["argv"][-1].split("=")[1][:-1]) - want) < 30            # expires at the ceiling
    # the laptop's own kubeconfig is never written; the token lives on the VM only
    assert not (Path(os.environ["HOME"]) / ".kube" / "config").exists()
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 0, err + out
    log = [json.loads(x)["argv"] for x in (fake / "kubectl.log").read_text().splitlines()]
    assert ["delete", "clusterrolebinding", f"testvm-{RUN}"] == log[-2][2:5]
    assert ["delete", "serviceaccount", f"testvm-{RUN}"] == log[-1][4:7]
    assert "token revoked" in out


def test_run_cluster_refuses_a_cluster_that_is_not_a_disposable_bench_cluster(fake, capsys, tmp_path):
    _up(fake, capsys, tmp_path)
    _gke(fake, name="snowplow-bench-shared", **{"created-by": "someone"})
    rc, _o, err = _run(capsys, "run", "--run", RUN, "--budget-minutes", "5", "--cluster", "snowplow-bench-shared",
                       "--cluster-location", "europe-west3", "--", "true")
    assert rc == 2 and "not a disposable bench cluster" in err
    assert not (fake / "kubectl.log").exists() and _state()["runs"] == []


class _FakeAPI(http.server.BaseHTTPRequestHandler):
    """The Deployment / ReplicaSet endpoints the probe calls: the ReplicaSet appears at the create and goes
    `gc_s` after the Deployment's delete, as the garbage collector would."""
    rs: dict = {}
    gc_s = 0.3

    def _send(self, body):
        b = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        doc = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        _FakeAPI.rs[doc["metadata"]["name"]] = None
        self._send(doc)

    def do_GET(self):
        name = self.path.split("testvm-probe%3D", 1)[1]
        gone = name in _FakeAPI.rs and _FakeAPI.rs[name] is not None and time.monotonic() >= _FakeAPI.rs[name]
        self._send({"items": [] if gone or name not in _FakeAPI.rs else [{"metadata": {"name": name + "-abc"}}]})

    def do_DELETE(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        _FakeAPI.rs[self.path.rsplit("/", 1)[1]] = time.monotonic() + _FakeAPI.gc_s
        self._send({})

    def log_message(self, *a):
        pass


def test_the_cluster_probe_times_how_long_a_replicaset_outlives_its_deployment(tmp_path):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FakeAPI)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        kc = tmp_path / "kc.json"
        kc.write_text(json.dumps({"clusters": [{"cluster": {"server": f"http://127.0.0.1:{srv.server_address[1]}"}}],
                                  "users": [{"user": {"token": "t"}}]}))
        (tmp_path / "probe.py").write_text(testvm.PROBE_PY)
        p = subprocess.run([sys.executable, str(tmp_path / "probe.py"), str(kc), "testvm-probe-1-start", "10"],
                           capture_output=True, text=True, timeout=60)
        got = json.loads(p.stdout)
        assert "error" not in got, got
        assert 0.3 <= got["replicaset_survived_s"] < 5 and got["replicaset_appeared_s"] < 5
        ok, line = testvm._probe_line(got)
        assert ok and "survived its Deployment by" in line
        _FakeAPI.gc_s = 60
        p = subprocess.run([sys.executable, str(tmp_path / "probe.py"), str(kc), "testvm-probe-1-end", "1"],
                           capture_output=True, text=True, timeout=60)
        assert "outlived its Deployment by more than 1 s" in json.loads(p.stdout)["error"]
    finally:
        srv.shutdown()


# ─── the shared code is vm606's ─────────────────────────────────────────────

def test_testvm_and_vm606_share_one_implementation_of_the_gce_rules():
    from bench import vm606
    for shared in ("gcloud", "ssh_stage", "iap_code", "firewall_create_argv", "firewall_view", "Refused"):
        assert getattr(vm606, shared) is getattr(gcevm, shared)
    assert vm606.IAP_CODES is gcevm.IAP_CODES and vm606._get is gcevm.get and vm606._diff is gcevm.diff
    assert testvm.firewall_rules(RUN)[f"{VM}-deny-ssh"]["priority"] == 1000
    assert testvm.firewall_rules(RUN)[f"{VM}-iap-ssh"]["sourceRanges"] == [gcevm.IAP_RANGE]
    assert {f for f in gcevm.firewall_create_argv("x", testvm.firewall_rules(RUN)[f"{VM}-iap-ssh"], PROJECT)
            if f.startswith("--")} == {f for fl in gcevm.FIREWALL_FIELDS.values() for f in fl if f.startswith("--")}
    # plan-iam does not scan the operator-side modules, and refuses an orchestrator module that imports one
    assert not vm606.scan()["uncovered"]
