"""bench/vm606.py: the paired launch from a disposable GCE VM, through its CLI (main(argv)), against a
fake gcloud (tests/fake_vm606.py) whose VM is a directory: `compute ssh` runs the module's remote
scripts there with bash, under the VM's environment only. Nothing is created in GCP.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from bench import evidence, vm606
from tests import fake_vm606

BENCH_DIR = Path(__file__).resolve().parent.parent
RUN = "run25"
VM = vm606.vm_name(RUN)
LABELS = {"owner": "snowplow-bench", "run": RUN, "ttl-hours": "15"}
BOOT = {"kubectl": "v1.36.2", "helm": "v3.19.0", "gcloud": "561.0.0", "gke-gcloud-auth-plugin": "Kubernetes v1.34.2",
        "python3": "Python 3.13.5", "bash": "GNU bash, version 5.2.37", "git": "git version 2.47.3"}
SENTINEL = "GHCR_PULL_USERNAME=sentinel-user\nGHCR_PULL_TOKEN=opaque-SENTINEL-never-printed\n"


def _in(hours: float) -> str:
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def fake(tmp_path, monkeypatch, run_trees):
    """The fake gcloud on PATH, a laptop HOME and data root of the test's own."""
    st = tmp_path / "gce"
    fake_vm606.install(tmp_path / "bin", st)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_STATE", str(st))
    home = tmp_path / "laptop"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "laptop-data"))
    for k in ("FAKE_BOOTSTRAP", "FAKE_LIST_FAILS", "FAKE_KEEP_DISKS", "FAKE_IAP_DISABLED", "FAKE_NO_SA"):
        monkeypatch.delenv(k, raising=False)
    return st


def _run(capsys, *argv) -> tuple[int, str, str]:
    rc = vm606.main(list(argv))
    out, err = capsys.readouterr()
    return rc, out, err


def _verbs(st) -> list[str]:
    return [" ".join(c["argv"][:3]) for c in fake_vm606.calls(st)]


# ─── plan-iam ───────────────────────────────────────────────────────────────

ROLES = ["roles/container.admin", "roles/compute.viewer", "roles/compute.storageAdmin", "roles/compute.publicIpAdmin",
         "roles/cloudquotas.viewer", "roles/serviceusage.serviceUsageConsumer", "roles/iam.serviceAccountUser"]


def test_plan_iam_derives_the_roles_from_the_code_and_changes_nothing(fake, capsys):
    rc, out, err = _run(capsys, "plan-iam")
    assert rc == 0, err
    listed = [m.group(1) for m in re.finditer(r"^\d+\. (roles/\S+)", out, re.M)]
    assert listed == ROLES
    sa = "serviceAccount:snowplow-bench-orchestrator@operations-dev-krateo-io.iam.gserviceaccount.com"
    cmds = [ln for ln in out.splitlines() if ln.startswith("gcloud ")]
    assert cmds[0].startswith("gcloud iam service-accounts create snowplow-bench-orchestrator --project "
                              "operations-dev-krateo-io")
    for r in ROLES[:-1]:
        assert (f"gcloud projects add-iam-policy-binding operations-dev-krateo-io --member {sa} --role {r} "
                f"--condition None") in cmds
    assert cmds[-1] == ("gcloud iam service-accounts add-iam-policy-binding "
                        "357378285144-compute@developer.gserviceaccount.com --project operations-dev-krateo-io "
                        f"--member {sa} --role roles/iam.serviceAccountUser")
    # each role names the call that needs it, from the code
    assert "gcloud compute disks delete  bench/gke.py:" in out
    assert "gcloud compute addresses delete  bench/gke.py:" in out
    assert "REST cloudquotas.googleapis.com  bench/gke.py:" in out
    assert "gcloud compute instances describe  bench/nodes.py:" in out
    # the only call it makes is a read of the project number; nothing IAM is run
    assert _verbs(fake) == ["projects describe operations-dev-krateo-io"]


def _bench_copy(tmp_path) -> Path:
    d = tmp_path / "benchcopy"
    shutil.copytree(BENCH_DIR / "bench", d / "bench", ignore=shutil.ignore_patterns("__pycache__"))
    for f in BENCH_DIR.glob("run606*.sh"):
        shutil.copy(f, d / f.name)
    return d


def test_plan_iam_refuses_a_call_it_does_not_cover(fake, capsys, tmp_path):
    d = _bench_copy(tmp_path)
    with open(d / "bench" / "gke.py", "a") as fh:
        fh.write('\n\ndef _planted():\n    return ["gcloud", "compute", "firewall-rules", "create", "x"]\n')
    rc, out, err = _run(capsys, "plan-iam", "--project-number", "1", "--bench-dir", str(d))
    assert rc == 1 and "gcloud compute firewall-rules create (bench/gke.py:" in err
    assert "gcloud projects add-iam-policy-binding" not in out


def test_plan_iam_reads_gcloud_in_shell_as_a_call_but_not_in_quotes_or_comments(fake, capsys, tmp_path):
    d = _bench_copy(tmp_path)
    # the real run606.sh names gcloud only in log strings and comments: it passes (first test)
    with open(d / "run606.sh", "a") as fh:
        fh.write('\n# gcloud compute instances delete in a comment\nlog "gcloud in a string"\n'
                 'gcloud compute firewall-rules delete x --quiet\n')
    rc, _out, err = _run(capsys, "plan-iam", "--project-number", "1", "--bench-dir", str(d))
    assert rc == 1
    uncovered = [ln for ln in err.splitlines() if "gcloud in shell" in ln]
    assert len(uncovered) == 1 and "firewall-rules delete" in uncovered[0]


def test_plan_iam_refuses_a_rest_api_it_does_not_cover(fake, capsys, tmp_path):
    d = _bench_copy(tmp_path)
    with open(d / "bench" / "nodes.py", "a") as fh:
        fh.write('\nURL = "https://storage.googleapis.com/b/o"\n')
    rc, _out, err = _run(capsys, "plan-iam", "--project-number", "1", "--bench-dir", str(d))
    assert rc == 1 and "REST storage.googleapis.com (bench/nodes.py:" in err


# ─── up ─────────────────────────────────────────────────────────────────────

def test_up_dry_prints_the_vm_and_creates_nothing(fake, capsys):
    rc, out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15")
    assert rc == 0, err
    create = next(ln for ln in out.splitlines() if ln.startswith("DRY-RUN: gcloud compute instances create"))
    for part in (f"create {VM} --project operations-dev-krateo-io --zone europe-west4-a",
                 "--machine-type e2-standard-8", "--image-family debian-13 --image-project debian-cloud",
                 "--boot-disk-size 50GB --boot-disk-type pd-balanced --boot-disk-auto-delete",
                 "--service-account snowplow-bench-orchestrator@operations-dev-krateo-io.iam.gserviceaccount.com "
                 "--scopes cloud-platform",
                 f"--labels owner=snowplow-bench,run={RUN},ttl-hours=15",
                 "--metadata enable-oslogin=TRUE,block-project-ssh-keys=TRUE",
                 "--instance-termination-action DELETE", "--termination-time 20"):
        assert part in create, part
    assert "--no-address" not in create            # an ephemeral external IP: no Cloud NAT in the project
    assert "nothing was created" in out
    assert _verbs(fake) == ["services list --enabled", "iam service-accounts describe"]    # reads only
    script = (vm606.state_dir(RUN) / "startup.sh").read_text()
    assert "kubectl" in script and vm606.KUBECTL_SHA256 in script and vm606.HELM_SHA256 in script
    assert "google-cloud-cli=561.0.0-0 google-cloud-cli-gke-gcloud-auth-plugin=561.0.0-0" in script


def test_up_creates_the_vm_and_waits_for_the_laptops_tools(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    t0 = time.time()
    rc, out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes", "--poll-s", "0")
    assert rc == 0, err
    inst = json.loads((fake / "instances" / f"{VM}.json").read_text())
    assert inst["labels"] == LABELS
    assert inst["scheduling"]["instanceTerminationAction"] == "DELETE"
    end = dt.datetime.fromisoformat(inst["scheduling"]["terminationTime"].replace("Z", "+00:00")).timestamp()
    assert t0 + 15 * 3600 - 5 <= end <= time.time() + 15 * 3600 + 5
    assert "bootstrapped: bash GNU bash" in out and "kubectl v1.36.2" in out
    assert json.loads((vm606.state_dir(RUN) / "state.json").read_text())["bootstrap"] == BOOT


def test_up_refuses_a_vm_whose_tools_are_not_the_laptops(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps({**BOOT, "kubectl": "v1.35.0"}))
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes", "--poll-s", "0")
    assert rc == 2 and "kubectl 'v1.35.0', want 'v1.36.2'" in err


@pytest.mark.parametrize("env, why", [("FAKE_IAP_DISABLED", "iap.googleapis.com is not enabled"),
                                      ("FAKE_NO_SA", "the orchestrator SA")])
def test_up_refuses_before_creating_without_iap_or_the_sa(fake, capsys, monkeypatch, env, why):
    monkeypatch.setenv(env, "1")
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes")
    assert rc == 2 and why in err
    assert "compute instances create" not in _verbs(fake)


@pytest.mark.parametrize("args", [["--run", "Run_25", "--ttl-hours", "15"], ["--run", RUN, "--ttl-hours", "0"],
                                  ["--run", RUN, "--ttl-hours", "73"]])
def test_up_refuses_bad_names_and_ttls(fake, capsys, args):
    rc, _out, _err = _run(capsys, "up", *args, "--yes")
    assert rc == 2 and fake_vm606.calls(fake) == []


# ─── push-secret ────────────────────────────────────────────────────────────

def _token(mode=0o600) -> Path:
    p = Path(os.environ["HOME"]) / ".config" / "snowplow-bench" / "ghcr-pull.env"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(SENTINEL)
    p.chmod(mode)
    return p


_OPENED: list[str] = []
_WATCH: list[str] = []


def _audit(event, args):
    if event == "open" and _WATCH and args and str(args[0]) in _WATCH:
        _OPENED.append(str(args[0]))


sys.addaudithook(_audit)


def test_push_secret_copies_the_file_without_reading_printing_or_hashing_it(fake, capsys):
    home = fake_vm606.put_instance(fake, VM, LABELS, _in(15))
    tok = _token()
    _WATCH[:] = [str(tok), os.path.realpath(tok)]
    _OPENED.clear()
    try:
        rc, out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    finally:
        _WATCH.clear()
    assert rc == 0, err
    assert _OPENED == []                                            # this process never opened it
    digest = hashlib.sha256(SENTINEL.encode()).hexdigest()
    for text in (out, err):
        assert "SENTINEL" not in text and "sentinel-user" not in text and digest not in text
    remote = home / ".config" / "snowplow-bench" / "ghcr-pull.env"
    assert stat.S_IMODE(remote.stat().st_mode) == 0o600 and stat.S_IMODE(remote.parent.stat().st_mode) == 0o700
    assert remote.read_text() == SENTINEL                           # the test reads it; the tool never did
    assert f"{VM}:~/.config/snowplow-bench/ghcr-pull.env: present, mode 0600" in out


def test_push_secret_refuses_a_token_file_not_0600(fake, capsys):
    fake_vm606.put_instance(fake, VM, LABELS, _in(15))
    _token(0o644)
    rc, _out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    assert rc == 2 and "mode 0644, not 0600" in err
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "scp"]]


def test_push_secret_refuses_a_vm_it_did_not_make(fake, capsys):
    fake_vm606.put_instance(fake, VM, {"owner": "someone-else", "run": RUN}, _in(15))
    _token()
    rc, _out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    assert rc == 2 and "not labelled owner=snowplow-bench" in err
    assert _verbs(fake) == ["compute instances describe"]


# ─── launch ─────────────────────────────────────────────────────────────────

STUB_PAIR = """#!/usr/bin/env bash
set -eu
if [ "${BENCH_606_DRY_RUN:-0}" = 1 ]; then
  mkdir -p "$PAIR_OUT/dry-N1" "$PAIR_OUT/dry-N2"
  echo "[06:52:59] DRY: bounded 1800 python3 -m bench.gke gke-up --name $RUN_NAME_N1 --ttl-hours ${STUB_TTL:-13} --yes" > "$PAIR_OUT/dry-N1/run.log"
  echo "[06:52:59] DRY: bounded 1800 python3 -m bench.gke gke-up --name $RUN_NAME_N2 --ttl-hours 12 --yes" > "$PAIR_OUT/dry-N2/run.log"
  echo "DRY: all 2 runs would start now"
  exit 0
fi
echo "paid launch: RUNS=$RUNS PAIRS=$PAIRS INSTALLER_VERSION=$INSTALLER_VERSION go=$BENCH_606_GO_N2 $(git rev-parse HEAD)"
"""


def _repo(tmp_path) -> tuple[str, str]:
    """A snowplow stand-in: the bench's own detach and evidence, a stub launcher."""
    r = tmp_path / "upstream"
    b = r / "go" / "snowplow" / "e2e" / "bench"
    (b / "bench").mkdir(parents=True)
    for f in ("__init__.py", "detach.py", "evidence.py"):
        shutil.copy(BENCH_DIR / "bench" / f, b / "bench" / f)
    (b / "requirements.txt").write_text("")
    (b / "run606_pair.sh").write_text(STUB_PAIR)
    git = ["git", "-C", str(r), "-c", "user.email=t@example.invalid", "-c", "user.name=t"]
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "stub"], check=True)
    subprocess.run([*git, "config", "uploadpack.allowReachableSHA1InWant", "true"], check=True)
    sha = subprocess.run([*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    return str(r), sha


def _venv(home: Path, sha: str) -> Path:
    """The VM's venv for `sha`, pre-made: its python3 is this interpreter, its pip a stub (no network)."""
    v = home / "vm606" / f"venv-{sha}" / "bin"
    v.mkdir(parents=True)
    (v / "python3").symlink_to(sys.executable)
    (v / "pip").write_text(f"#!/bin/sh\necho \"$@\" >> {home / 'pip.log'}\n")
    (v / "pip").chmod(0o755)
    return v


ENV = {"RUNS": "N1 N2", "PAIRS": "N1:N2", "INSTALLER_VERSION": "0.3.545",
       "RUN_NAME_N1": "snowplow-bench-run25n1-8a395c45", "RUN_NAME_N2": "snowplow-bench-run25n2-8a395c45",
       "SNOWPLOW_TAG_N1": "1.12.67", "SNOWPLOW_TAG_N2": "1.12.68",
       "EXPECTED_IMAGE_DIGEST_N1": "sha256:" + "a" * 64, "EXPECTED_IMAGE_DIGEST_N2": "sha256:" + "b" * 64,
       "BENCH_606_GO_N1": "20261110-snowplow-bench-run25n1-8a395c45",
       "BENCH_606_GO_N2": "20261110-snowplow-bench-run25n2-8a395c45"}


def _env_file(tmp_path, extra: str = "") -> Path:
    p = tmp_path / "launch.env"
    p.write_text("# the laptop launcher's variables\n" + "".join(f"{k}={shlex.quote(v)}\n" for k, v in ENV.items())
                 + extra)
    return p


def _pin(tmp_path) -> tuple[Path, str]:
    p = tmp_path / "installer-0.3.545-pin-manifest.json"
    p.write_text(json.dumps({"installer": "0.3.545", "components": []}))
    return p, hashlib.sha256(p.read_bytes()).hexdigest()


def _launch_args(tmp_path, repo, sha, pin, pin_sha, env_file=None):
    return ["launch", "--run", RUN, "--bench-sha", sha, "--env-file", str(env_file or _env_file(tmp_path)),
            "--pin-manifest", str(pin), "--pin-sha256", pin_sha, "--repo-url", repo, "--yes"]


def _vm_ready(fake, tmp_path, sha, hours=15.0) -> Path:
    home = fake_vm606.put_instance(fake, VM, LABELS, _in(hours))
    fake_vm606.bootstrap_done(fake, VM, BOOT)
    tok = home / ".config" / "snowplow-bench" / "ghcr-pull.env"
    tok.parent.mkdir(parents=True)
    tok.write_text("opaque")
    tok.chmod(0o600)
    _venv(home, sha)
    return home


def _wait_for(path: Path, text: str, s: float = 20) -> str:
    end = time.monotonic() + s
    while time.monotonic() < end:
        if path.exists() and text in path.read_text():
            return path.read_text()
        time.sleep(0.05)
    raise AssertionError(f"{path} never held {text!r}: {path.read_text() if path.exists() else 'absent'}")


def test_launch_clones_at_the_sha_dry_passes_and_starts_detached(fake, capsys, tmp_path):
    repo, sha = _repo(tmp_path)
    home = _vm_ready(fake, tmp_path, sha)
    pin, pin_sha = _pin(tmp_path)
    rc, out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 0, err + out
    w = home / "vm606" / RUN
    assert f"vm606: bench checkout at {sha}" in out and "vm606: dry pass OK" in out
    assert "the run's TTL 13 h + margin fits" in out                      # the larger of the runs' TTLs
    log = _wait_for(w / "launch.log", "paid launch:")
    assert f"paid launch: RUNS=N1 N2 PAIRS=N1:N2 INSTALLER_VERSION=0.3.545 go={ENV['BENCH_606_GO_N2']} {sha}" in log
    assert (w / "launch.pid").read_text().strip().isdigit()
    pair = "pair-snowplow-bench-run25n1-8a395c45-snowplow-bench-run25n2-8a395c45"
    placed = home / ".local" / "share" / "snowplow-bench" / "runs" / "606" / pair / pin.name
    assert placed.read_bytes() == pin.read_bytes()                        # the manifest beside the pair's files
    assert stat.S_IMODE((w / "launch-env.sh").stat().st_mode) == 0o600
    assert "-r" in (home / "pip.log").read_text() and "jsonschema" in (home / "pip.log").read_text()
    st = json.loads((vm606.state_dir(RUN) / "state.json").read_text())
    assert st["bench_sha"] == sha and st["pair"] == pair and st["runs"] == [ENV["RUN_NAME_N1"], ENV["RUN_NAME_N2"]]
    assert st["run_ttl_hours"] == 13 and st["fetched"] is False


def test_launch_refuses_a_vm_that_dies_before_the_run_ends(fake, capsys, tmp_path):
    repo, sha = _repo(tmp_path)
    home = _vm_ready(fake, tmp_path, sha, hours=13.5)                    # 13 h run + 1 h margin does not fit
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "the run's TTL is 13 h" in err and "--ttl-hours >= 14" in err
    assert not (home / "vm606" / RUN / "launch.pid").exists()


def test_launch_refuses_a_sha_the_repo_does_not_have(fake, capsys, tmp_path):
    repo, sha = _repo(tmp_path)
    other = "0" * 40
    home = _vm_ready(fake, tmp_path, other)
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, other, pin, pin_sha))
    assert rc == 2 and "the VM's prepare step failed" in err
    assert not (home / "vm606" / RUN / "launch.pid").exists()


def test_launch_refuses_without_the_token_on_the_vm(fake, capsys, tmp_path):
    repo, sha = _repo(tmp_path)
    home = _vm_ready(fake, tmp_path, sha)
    (home / ".config" / "snowplow-bench" / "ghcr-pull.env").unlink()
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "push-secret first" in err


def test_launch_refuses_a_pin_manifest_whose_sha256_differs(fake, capsys, tmp_path):
    repo, sha = _repo(tmp_path)
    _vm_ready(fake, tmp_path, sha)
    pin, _ = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, "f" * 64))
    assert rc == 2 and f"has sha256 {hashlib.sha256(pin.read_bytes()).hexdigest()}, not {'f' * 64}" in err
    assert fake_vm606.calls(fake) == []


@pytest.mark.parametrize("extra, why", [
    ("OUT_N1=/Users/someone/.local/share/snowplow-bench/runs/606/x\n", "OUT_N1 names a place on the laptop"),
    ("BENCH_606_HOST_LOAD=0.5 0.5 0.5 8\n", "one literal value per variable"),
    ("BENCH_606_HOST_LOAD='0.5 0.5 0.5 8'\n", "BENCH_606_HOST_LOAD is a test hook"),
    ("BENCH_606_DRY_RUN=1\n", "BENCH_606_DRY_RUN is a test hook"),
    ("CLOUDSDK_CORE_ACCOUNT=x\n", "CLOUDSDK_CORE_ACCOUNT is a test hook or a gcloud override"),
    ("INSTALLER_CHART=/private/tmp/chart.tgz\n", "is a path on the laptop"),
    ("RUNS=N1 N2\n", "one literal value per variable"),
    ("SNOWPLOW_TAG_N1=$TAG\n", "no expansion"),
])
def test_launch_refuses_an_env_that_cannot_travel(fake, capsys, tmp_path, extra, why):
    pin, pin_sha = _pin(tmp_path)
    env = tmp_path / "e.env"
    env.write_text("".join(f"{k}={v!r}\n" if " " in v else f"{k}={v}\n" for k, v in ENV.items()
                           if not (extra.startswith("RUNS=") and k == "RUNS")) + extra)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha, env))
    assert rc == 2 and why in err, err
    assert fake_vm606.calls(fake) == []


def test_launch_dry_starts_nothing(fake, capsys, tmp_path):
    pin, pin_sha = _pin(tmp_path)
    args = _launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha)[:-1]
    rc, out, err = _run(capsys, *args)
    assert rc == 0, err
    assert f"DRY-RUN: gcloud compute scp --project operations-dev-krateo-io --zone europe-west4-a --tunnel-through-iap " \
           f"--quiet {pin} {VM}:vm606/{RUN}/pin/{pin.name}" in out
    assert "nothing was started" in out and fake_vm606.calls(fake) == []


def test_pair_name_is_run606_pairs():
    src = (BENCH_DIR / "run606_pair.sh").read_text()
    assert 'PAIR_NAME="pair"\n' in src
    assert 'for x in "${LABELS[@]}"; do nm="RUN_NAME_$x"; PAIR_NAME="${PAIR_NAME}-${!nm}"; done' in src
    assert vm606.pair_name(ENV) == f"pair-{ENV['RUN_NAME_N1']}-{ENV['RUN_NAME_N2']}"


# ─── status ─────────────────────────────────────────────────────────────────

def test_status_tails_the_log_redacted_and_says_the_vms_time_left(fake, capsys):
    home = fake_vm606.put_instance(fake, VM, LABELS, _in(10))
    w = home / "vm606" / RUN
    w.mkdir(parents=True)
    (w / "launch.log").write_text("".join(f"[06:5{i}:00] line {i}\n" for i in range(5))
                                  + "authenticated as someone@example.com\n")
    (w / "launch.pid").write_text(f"{os.getpid()}\n")
    rc, out, err = _run(capsys, "status", "--run", RUN, "--lines", "3")
    assert rc == 0, err
    assert f"{VM}: RUNNING, deleted in 10.0 h; launcher running {os.getpid()}" in out
    assert "line 2" not in out and "line 3" in out and "line 4" in out
    assert "someone@example.com" not in out and "authenticated as <redacted>" in out


# ─── fetch ──────────────────────────────────────────────────────────────────

NAMES = [ENV["RUN_NAME_N1"], ENV["RUN_NAME_N2"]]
PAIR = vm606.pair_name(ENV)


def _vm_after_run(fake, monkeypatch, *, alive=False) -> Path:
    """The VM after a launch: each run's directory and its evidence (published by bench.evidence)."""
    home = fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    sha = "c" * 40
    b = home / "vm606" / RUN / "snowplow" / "go" / "snowplow" / "e2e" / "bench" / "bench"
    b.mkdir(parents=True)
    for f in ("__init__.py", "evidence.py"):
        shutil.copy(BENCH_DIR / "bench" / f, b / f)
    _venv(home, sha)
    data = home / ".local" / "share" / "snowplow-bench"
    monkeypatch.setenv("BENCH_ALLOW_VOLATILE_PATHS", "1")
    for n in [*NAMES, PAIR]:
        src = data / "runs" / "606" / n
        (src / "k6").mkdir(parents=True)
        (src / "run.log").write_text(f"{n}: run complete\n")
        (src / "k6" / "stream.jsonl").write_text('{"t": 1}\n' * 100)
        evidence.publish(src, data / "evidence" / "606" / n, n, "final")
    w = home / "vm606" / RUN
    (w / "launch.log").write_text("launcher log\n")
    (w / "launch.pid").write_text(f"{os.getpid() if alive else 2 ** 22 + 7}\n")
    vm606._save_state(RUN, bench_sha=sha, runs=NAMES, pair=PAIR, launched_at=time.time(), fetched=False)
    return data


def test_fetch_brings_every_directory_back_verified(fake, capsys, monkeypatch):
    data = _vm_after_run(fake, monkeypatch)
    rc, out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 0, err + out
    for n in [*NAMES, PAIR]:
        assert (evidence.run_dir(n) / "k6" / "stream.jsonl").read_bytes() == \
            (data / "runs" / "606" / n / "k6" / "stream.jsonl").read_bytes()
        assert evidence.verify(evidence.evidence_dir(n), "final") == []
        assert f"evidence {n}: verified against its MANIFEST.json (stage final)" in out
    assert (vm606.state_dir(RUN) / "vm" / "launch.log").read_text() == "launcher log\n"
    st = json.loads((vm606.state_dir(RUN) / "state.json").read_text())
    assert st["fetched"] is True and len(st["fetch_sha256"]) == 64
    assert not (vm606.state_dir(RUN) / "staging").exists() and not (vm606.state_dir(RUN) / "fetch.tar").exists()


def test_fetch_refuses_evidence_that_does_not_verify_and_moves_nothing(fake, capsys, monkeypatch):
    data = _vm_after_run(fake, monkeypatch)
    (data / "evidence" / "606" / NAMES[1] / "run.log").write_text("changed after the publish\n")
    rc, _out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 2 and "the evidence did not verify; nothing was moved" in err
    assert "run.log: its bytes in" in err
    for n in [*NAMES, PAIR]:
        assert not evidence.run_dir(n).exists() and not evidence.evidence_dir(n).exists()


def test_fetch_refuses_while_the_launcher_runs(fake, capsys, monkeypatch):
    _vm_after_run(fake, monkeypatch, alive=True)
    rc, _out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 2 and "the launcher is still running" in err


def test_fetch_never_overwrites_a_run_on_the_laptop(fake, capsys, monkeypatch):
    _vm_after_run(fake, monkeypatch)
    evidence.run_dir(NAMES[0]).mkdir(parents=True)
    rc, _out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 2 and "exists on the laptop: a fetch never overwrites a run" in err
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]]


# ─── down ───────────────────────────────────────────────────────────────────

def test_down_deletes_the_vm_and_its_disk_and_verifies_both_gone(fake, capsys):
    fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err
    assert "DRY" not in out
    assert not (fake / "instances" / f"{VM}.json").exists() and not (fake / "disks" / VM).exists()
    assert f"{VM} deleted and verified gone; its disk(s) ['{VM}'] too" in out
    deletes = [c["argv"] for c in fake_vm606.calls(fake) if c["argv"][:3] == ["compute", "instances", "delete"]]
    assert deletes == [["compute", "instances", "delete", VM, "--project", "operations-dev-krateo-io", "--zone",
                        "europe-west4-a", "--delete-disks", "all", "--quiet"]]
    lists = [c for c in fake_vm606.calls(fake) if c["argv"][2:3] == ["list"]]
    assert lists and all(c["partial_error"] == "false" for c in lists)


def test_down_dry_deletes_nothing(fake, capsys):
    fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    rc, out, _err = _run(capsys, "down", "--run", RUN)
    assert rc == 0 and "DRY-RUN: gcloud compute instances delete" in out and "nothing was deleted" in out
    assert (fake / "instances" / f"{VM}.json").exists()


@pytest.mark.parametrize("labels", [{"owner": "someone-else", "run": RUN}, {"owner": "snowplow-bench", "run": "run9"},
                                    {"run": RUN}])
def test_down_refuses_an_instance_not_labelled_for_the_run(fake, capsys, labels):
    fake_vm606.put_instance(fake, VM, labels, _in(3))
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "is not labelled owner=snowplow-bench, run=run25" in err
    assert "compute instances delete" not in _verbs(fake)


def test_down_refuses_when_the_label_selects_another_vm(fake, capsys):
    fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    fake_vm606.put_instance(fake, "snowplow-bench-orch-other", LABELS, _in(3))
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "selects ['snowplow-bench-orch-other', 'snowplow-bench-orch-run25']" in err
    assert "compute instances delete" not in _verbs(fake)


def test_down_fails_closed_when_a_list_fails_open(fake, capsys, monkeypatch):
    """gcloud's list exits 0 with [] when its requests fail; vm606 sets the property that makes it exit 1."""
    fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    monkeypatch.setenv("FAKE_LIST_FAILS", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "could not be listed (rc=1" in err
    assert "compute instances delete" not in _verbs(fake)


def test_down_refuses_a_disk_left_behind(fake, capsys, monkeypatch):
    fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    monkeypatch.setenv("FAKE_KEEP_DISKS", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"disk(s) ['{VM}'] remain" in err and "the pull-token file is on one of them" in err


def test_down_refuses_an_unfetched_launch_and_a_running_launcher(fake, capsys, monkeypatch):
    home = fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    vm606._save_state(RUN, launched_at=time.time(), fetched=False)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "was launched and not fetched" in err
    w = home / "vm606" / RUN
    w.mkdir(parents=True)
    (w / "launch.pid").write_text(f"{os.getpid()}\n")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 2 and "the launcher is still running" in err
    assert "compute instances delete" not in _verbs(fake)
    (w / "launch.pid").write_text(f"{2 ** 22 + 7}\n")
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 0, err


def test_down_says_notfound_and_deletes_nothing(fake, capsys):
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "notfound (nothing to delete)" in err
    assert "PLACEHOLDER-OPERATOR" not in err
