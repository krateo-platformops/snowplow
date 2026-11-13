"""bench/vm606.py: the paired launch from a disposable GCE VM, through its CLI (main(argv)), against a
fake gcloud (tests/fake_vm606.py) whose VM is a directory: `compute ssh` runs the module's remote
scripts there with bash, under the VM's environment only, and hangs the session up afterwards as sshd
does. Nothing is created in GCP.

Every test also checks, at its end, that the VM login user's identity-shaped home
(fake_vm606.IDENTITY) appears in no output and no laptop record (#1307 review B1).
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
from tests.test_run606_real_sequence import _calls, harness  # noqa: F401  (fixture)

BENCH_DIR = Path(__file__).resolve().parent.parent
RUN = "run25"
VM = vm606.vm_name(RUN)
PROJECT = "operations-dev-krateo-io"
SA = f"serviceAccount:snowplow-bench-orchestrator@{PROJECT}.iam.gserviceaccount.com"
NODE_SA = "357378285144-compute@developer.gserviceaccount.com"
LABELS = {"owner": "snowplow-bench", "run": RUN, "ttl-hours": "15"}
BOOT = {"kubectl": "v1.36.2", "helm": "v3.19.0", "gcloud": "561.0.0", "gke-gcloud-auth-plugin": "Kubernetes v1.34.2",
        "python3": "Python 3.13.5", "bash": "GNU bash, version 5.2.37", "git": "git version 2.47.3"}
SENTINEL = "GHCR_PULL_USERNAME=sentinel-user\nGHCR_PULL_TOKEN=opaque-SENTINEL-never-printed\n"
ROLES = ["roles/container.admin", "roles/compute.viewer", "roles/compute.storageAdmin", "roles/compute.publicIpAdmin",
         "roles/cloudquotas.viewer", "roles/serviceusage.serviceUsageConsumer", "roles/iam.serviceAccountUser"]

_OUTPUT: list[str] = []


def _in(hours: float) -> str:
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def fake(tmp_path, monkeypatch, run_trees):
    """The fake gcloud on PATH, a laptop HOME and data root of the test's own; at the end, the VM login
    user's identity is in no output and no laptop record."""
    st = tmp_path / "gce"
    fake_vm606.install(tmp_path / "bin", st)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_STATE", str(st))
    home = tmp_path / "laptop"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "laptop-data"))
    for k in ("FAKE_BOOTSTRAP", "FAKE_LIST_FAILS", "FAKE_KEEP_DISKS", "FAKE_IAP_DISABLED", "FAKE_NO_SA",
              "FAKE_SSH_FAIL", "FAKE_SSH_FAIL_TIMES", "FAKE_SCP_CORRUPT", "FAKE_SCP_MODE", "FAKE_TAMPER_AFTER_SSH",
              "FAKE_IAM_LOST_WRITE", "FAKE_LIMIT_E2_CPUS", "FAKE_LIMIT_IN_USE_ADDRESSES",
              "FAKE_FW_DELETE_LOST", "FAKE_FW_DESCRIBE_503", "FAKE_FW_DESCRIBE_NOT_OBJECT", "FAKE_INSTANCE_DESCRIBE_FAIL", "FAKE_INSTANCE_DELETE_LOST",
              "FAKE_PROJECT_NUMBER", "FAKE_PROJECT_STATE", "FAKE_CREATE_FAIL", "FAKE_FW_CREATE_FAIL",
              "FAKE_PROJECT_USAGE_FIREWALLS", "FAKE_PROJECT_INFO_FAIL", "FAKE_NOT_READY", "FAKE_OP_DESCRIBE_FAIL",
              "FAKE_FW_LIST_FAILS", "FAKE_FW_DELETE_FAIL"):
        monkeypatch.delenv(k, raising=False)
    _OUTPUT.clear()
    yield st
    records = [p.read_text() for p in (tmp_path / "laptop-data").rglob("*") if p.is_file() and p.suffix in
               (".json", ".log", ".sh")]
    for text in [*_OUTPUT, *records]:
        assert fake_vm606.IDENTITY not in text, text[-2000:]
    # no selection through a gcloud --filter (#1307 re-review N1); the fake refuses one, and says so
    assert not [c for c in fake_vm606.calls(Path(os.environ["FAKE_STATE"])) if "--filter" in c["argv"]]


def _run(capsys, *argv) -> tuple[int, str, str]:
    rc = vm606.main(list(argv))
    out, err = capsys.readouterr()
    _OUTPUT.extend([out, err])
    return rc, out, err


def _verbs(st) -> list[str]:
    return [" ".join(c["argv"][:3]) for c in fake_vm606.calls(st)]


def _state() -> dict:
    return json.loads((vm606.state_dir(RUN) / "state.json").read_text())


# the run's VM zone in these tests: not vm606's default (europe-west4-a) nor the other run's (europe-west1-b),
# so a step that took a default or a flag instead of up's record misses the VM (#1307 round-3 R1)
ZONE = "europe-west3-a"
NUMBER = "357378285144"


def _record(run: str = RUN, zone: str = ZONE, **kv) -> None:
    """The laptop record `up --yes` writes (the run's VM, project, zone, TTL): a test that starts after up."""
    kv.setdefault("ttl_hours", 15)
    if "terminates_at" in kv or "made" not in vm606._state(run):      # what up records creating, as up does
        kv.setdefault("made", vm606.as_made(run, PROJECT, zone, kv["ttl_hours"], kv.get("terminates_at")))
    vm606._save_state(run, vm=vm606.vm_name(run), project=PROJECT, zone=zone, project_number=NUMBER, **kv)


def _vm(fake, labels=None, hours: float = 15.0, zone: str = ZONE) -> Path:
    """The run's VM as up makes it, and the record up writes for it (its zone, TTL, termination time)."""
    tt = _in(hours)
    srv = fake_vm606.put_instance(fake, VM, LABELS if labels is None else labels, tt, zone=zone)
    _record(zone=zone, terminates_at=tt)
    return srv


def _changing(st) -> list[str]:
    """Every call that changes GCP."""
    return [v for v in _verbs(st) if re.search(r"\b(create|delete|add-iam-policy-binding|remove-iam-policy-binding)\b", v)]


def test_the_fake_vm_home_is_identity_shaped(fake):
    srv = fake_vm606.put_instance(fake, VM, LABELS, _in(3))
    assert (srv.parent / "home" / fake_vm606.IDENTITY).is_dir()


# ─── plan-iam ───────────────────────────────────────────────────────────────

def test_plan_iam_derives_the_roles_from_the_code_and_changes_nothing(fake, capsys):
    rc, out, err = _run(capsys, "plan-iam")
    assert rc == 0, err
    assert [m.group(1) for m in re.finditer(r"^\d+\. (roles/\S+)", out, re.M)] == ROLES
    cmds = [ln for ln in out.splitlines() if ln.startswith("gcloud ")]
    assert cmds[0].startswith(f"gcloud iam service-accounts create snowplow-bench-orchestrator --project {PROJECT}")
    for r in ROLES[:-1]:
        assert (f"gcloud projects add-iam-policy-binding {PROJECT} --member {SA} --role {r} "
                f"--condition-from-file iam-condition.json") in cmds
    assert cmds[-1] == (f"gcloud iam service-accounts add-iam-policy-binding {NODE_SA} --project {PROJECT} "
                        f"--member {SA} --role roles/iam.serviceAccountUser --condition-from-file iam-condition.json")
    assert '"expression": "request.time < timestamp(' in out and '"title": "snowplow-bench-<run>"' in out
    assert "krateo-057 included" in out                                   # container.admin's reach, stated
    assert "resourcemanager.projects.setIamPolicy" in out and "compute.securityAdmin" in out
    assert "gcloud compute disks delete  bench/gke.py:" in out
    assert "REST cloudquotas.googleapis.com  bench/gke.py:" in out
    assert _verbs(fake) == [f"projects describe {PROJECT}"]               # a read; nothing IAM is run


def _bench_copy(tmp_path) -> Path:
    d = tmp_path / "benchcopy"
    shutil.copytree(BENCH_DIR / "bench", d / "bench", ignore=shutil.ignore_patterns("__pycache__"))
    for f in BENCH_DIR.glob("run606*.sh"):
        shutil.copy(f, d / f.name)
    return d


def test_plan_iam_refuses_an_orchestrator_module_that_imports_an_operator_side_one(fake, capsys, tmp_path):
    """bench.gcevm and bench.testvm are the operator's (#1338): plan-iam does not scan them, so a module the
    orchestrator runs may not import them (their calls would run as the SA, unscanned)."""
    d = _bench_copy(tmp_path)
    with open(d / "bench" / "nodes.py", "a") as fh:
        fh.write("\nfrom bench import gcevm  # noqa: E402,F401\n")
    rc, _out, err = _run(capsys, "plan-iam", "--project-number", "1", "--bench-dir", str(d))
    assert rc == 1 and "an operator-side module (gcevm) imported, its calls unscanned (bench/nodes.py:" in err


def test_plan_iam_refuses_a_call_it_does_not_cover(fake, capsys, tmp_path):
    d = _bench_copy(tmp_path)
    with open(d / "bench" / "gke.py", "a") as fh:
        fh.write('\n\ndef _planted():\n    return ["gcloud", "compute", "firewall-rules", "create", "x"]\n')
    rc, out, err = _run(capsys, "plan-iam", "--project-number", "1", "--bench-dir", str(d))
    assert rc == 1 and "gcloud compute firewall-rules create (bench/gke.py:" in err
    assert "add-iam-policy-binding" not in out


def test_plan_iam_reads_gcloud_in_shell_as_a_call_but_not_in_quotes_or_comments(fake, capsys, tmp_path):
    d = _bench_copy(tmp_path)
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

def _flag(argv, f):
    return argv[argv.index(f) + 1]


def test_up_dry_prints_grants_rules_and_the_vm_and_creates_nothing(fake, capsys):
    rc, out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15")
    assert rc == 0, err
    create = next(ln for ln in out.splitlines() if ln.startswith("DRY-RUN: gcloud compute instances create"))
    for part in (f"create {VM} --project {PROJECT} --zone europe-west4-a", "--machine-type e2-standard-8",
                 "--image-family debian-13 --image-project debian-cloud",
                 "--boot-disk-size 50GB --boot-disk-type pd-balanced --boot-disk-auto-delete",
                 f"--service-account snowplow-bench-orchestrator@{PROJECT}.iam.gserviceaccount.com --scopes "
                 "cloud-platform", f"--network default --tags {VM}",
                 f"--labels owner=snowplow-bench,run={RUN},ttl-hours=15",
                 "--metadata enable-oslogin=TRUE,block-project-ssh-keys=TRUE",
                 "--instance-termination-action DELETE", "--termination-time 20"):
        assert part in create, part
    assert "--no-address" not in create            # an ephemeral external IP: no Cloud NAT in the project
    assert out.count("DRY-RUN: gcloud projects add-iam-policy-binding") == 6
    assert out.count("DRY-RUN: gcloud iam service-accounts add-iam-policy-binding") == 1
    assert out.count("DRY-RUN: gcloud compute firewall-rules create") == 2
    assert "nothing was created" in out
    assert not [v for v in _verbs(fake)
                if re.search(r"\b(create|delete|add-iam-policy-binding|remove-iam-policy-binding)\b", v)]   # reads only
    script = (vm606.state_dir(RUN) / "startup.sh").read_text()
    assert vm606.KUBECTL_SHA256 in script and vm606.HELM_SHA256 in script and "chmod 1777 /srv/snowplow-bench" in script
    assert "google-cloud-cli=561.0.0-0 google-cloud-cli-gke-gcloud-auth-plugin=561.0.0-0" in script


def _up(capsys, monkeypatch, *extra):
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    return _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes", "--poll-s", "0", "--zone", ZONE, *extra)


def test_up_grants_expiring_with_the_vm_before_the_vm_then_iap_only_ssh(fake, capsys, monkeypatch):
    t0 = time.time()
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 0, err
    calls = [c["argv"] for c in fake_vm606.calls(fake)]
    create = next(c for c in calls if c[:3] == ["compute", "instances", "create"])
    tt = _flag(create, "--termination-time")
    end = dt.datetime.fromisoformat(tt.replace("Z", "+00:00")).timestamp()
    assert t0 + 15 * 3600 - 5 <= end <= time.time() + 15 * 3600 + 5
    want = {"title": f"snowplow-bench-{RUN}", "expression": f'request.time < timestamp("{tt}")',
            "description": f"vm606: expires with {VM} (owner=snowplow-bench, run={RUN})"}
    proj = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
    assert sorted(b["role"] for b in proj) == sorted(ROLES[:-1])
    assert all(b["members"] == [SA] and b["condition"] == want for b in proj)
    node = fake_vm606.policy(fake, f"sa-{NODE_SA}")["bindings"]
    assert node == [{"role": "roles/iam.serviceAccountUser", "members": [SA], "condition": want}]
    # order: every grant and both rules before the VM; the policies read back before the rules
    idx = {i: c for i, c in enumerate(calls)}
    first_create = min(i for i, c in idx.items() if c[:3] == ["compute", "instances", "create"])
    grants = [i for i, c in idx.items() if "add-iam-policy-binding" in c]
    reads = [i for i, c in idx.items() if "get-iam-policy" in c]
    rules = [i for i, c in idx.items() if c[:3] == ["compute", "firewall-rules", "create"]]
    back = [i for i in reads if i > max(grants)]                  # the read-back, after the last grant
    assert len(grants) == 7 and back and max(back) < min(rules) and max(rules) < first_create
    assert min(reads) < min(grants)                               # the run's name and the SA checked first
    fw = {p.stem: json.loads(p.read_text()) for p in (fake / "firewalls").glob("*.json")}
    assert fw[f"{VM}-deny-ssh"]["sourceRanges"] == ["0.0.0.0/0"] and fw[f"{VM}-deny-ssh"]["priority"] == 1000
    assert "denied" in fw[f"{VM}-deny-ssh"]
    assert fw[f"{VM}-iap-ssh"]["sourceRanges"] == ["35.235.240.0/20"] and fw[f"{VM}-iap-ssh"]["priority"] == 900
    assert all(r["targetTags"] == [VM] for r in fw.values())
    assert _flag(create, "--tags") == VM
    assert "bootstrapped: bash GNU bash" in out and _state()["bootstrap"] == BOOT
    assert _state()["terminates_at"] == tt


def test_up_reads_the_grants_back_and_creates_nothing_when_one_is_not_held(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_IAM_LOST_WRITE", "roles/compute.storageAdmin")
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "roles/compute.storageAdmin on operations-dev-krateo-io is not held under the condition" in err
    assert f"down --run {RUN} --yes" in err
    assert not [v for v in _verbs(fake) if v.endswith(" create")]


def test_up_refuses_a_vm_whose_tools_are_not_the_laptops(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps({**BOOT, "kubectl": "v1.35.0"}))
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes", "--poll-s", "0")
    assert rc == 2 and "kubectl 'v1.35.0', want 'v1.36.2'" in err and f"down --run {RUN} --yes" in err


@pytest.mark.parametrize("fail, stage", [("4033", "IAP 4033: not authorized"), ("4010", "IAP 4010: the VM closed"),
                                         ("publickey", "OS Login: the key was refused")])
def test_up_stops_at_once_on_an_ssh_failure_waiting_cannot_fix(fake, capsys, monkeypatch, fail, stage):
    monkeypatch.setenv("FAKE_SSH_FAIL", fail)
    rc, _out, err = _up(capsys, monkeypatch, "--ssh-ready-timeout-s", "5", "--bootstrap-timeout-s", "5")
    assert rc == 2 and f"failed at {stage}" in err and "billing until its TTL" in err
    assert f"`python3 -m bench.vm606 down --run {RUN} --yes`" in err and f"up --run {RUN} --yes` resumes" in err
    assert len([c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]]) == 1


def test_up_waits_out_a_booting_vms_ssh_and_names_the_firewall_when_it_never_answers(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_SSH_FAIL", "4003")
    monkeypatch.setenv("FAKE_SSH_FAIL_TIMES", "2")
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 0 and "bootstrapped" in out, err
    assert len([c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]]) == 3
    monkeypatch.setenv("FAKE_SSH_FAIL_TIMES", "1000000")
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    rc, _out, err = _run(capsys, "up", "--run", "run26", "--ttl-hours", "15", "--yes", "--poll-s", "0",
                         "--ssh-ready-timeout-s", "0")
    assert rc == 2 and "never answered in 0 s, last at IAP 4003: IAP reached the VM's network" in err
    assert "down --run run26" in err


@pytest.mark.parametrize("env, why", [("FAKE_IAP_DISABLED", "iap.googleapis.com is not enabled"),
                                      ("FAKE_NO_SA", "the orchestrator SA")])
def test_up_refuses_before_granting_without_iap_or_the_sa(fake, capsys, monkeypatch, env, why):
    monkeypatch.setenv(env, "1")
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes")
    assert rc == 2 and why in err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or "create" in v]


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


def _ready(fake) -> Path:
    srv = _vm(fake)
    fake_vm606.bootstrap_done(fake, VM, BOOT)
    _record(bootstrap=BOOT)
    return srv


def test_push_secret_copies_the_file_without_reading_printing_or_hashing_it(fake, capsys):
    srv = _ready(fake)
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
    remote = srv / RUN / "home" / ".config" / "snowplow-bench" / "ghcr-pull.env"
    assert stat.S_IMODE(remote.stat().st_mode) == 0o600 and stat.S_IMODE(remote.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE((srv / RUN).stat().st_mode) == 0o700
    assert remote.read_text() == SENTINEL                           # the test reads it; the tool never did
    assert f"{VM}:/srv/snowplow-bench/{RUN}/home/.config/snowplow-bench/ghcr-pull.env: present, mode 0600" in out


def test_push_secret_makes_the_modes_whatever_the_vm_held_or_the_copy_kept(fake, capsys, monkeypatch):
    """A token directory left looser on the VM, and a copy that kept another mode (an scp whose remote
    umask or protocol does not carry the source's 0600): push-secret's chmods make both, and it checks."""
    srv = _ready(fake)
    gdir = srv / RUN / "home" / ".config" / "snowplow-bench"
    gdir.mkdir(parents=True)
    gdir.chmod(0o755)
    _token()
    monkeypatch.setenv("FAKE_SCP_MODE", "644")
    rc, out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    assert rc == 0, err
    assert stat.S_IMODE((gdir / "ghcr-pull.env").stat().st_mode) == 0o600 and stat.S_IMODE(gdir.stat().st_mode) == 0o700


def test_push_secret_refuses_a_token_file_not_0600(fake, capsys):
    _ready(fake)
    _token(0o644)
    rc, _out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    assert rc == 2 and "mode 0644, not 0600" in err
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "scp"]]


def test_push_secret_refuses_a_vm_it_did_not_make(fake, capsys):
    _vm(fake, {"owner": "someone-else", "run": RUN})
    _record(bootstrap=BOOT)
    _token()
    rc, _out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    assert rc == 2 and "not labelled owner=snowplow-bench" in err
    assert _verbs(fake) == [f"projects describe {PROJECT}", "compute zones describe", "compute instances describe"]


# ─── launch ─────────────────────────────────────────────────────────────────

# a run606_pair.sh stand-in. Its dry pass READS STDIN (a child that would eat a script fed on bash's
# stdin), then logs each run's gke-up argv; its paid pass (STUB_MODE) logs each run's run budget
# into the run's OUT and waits for a release file, refuses as run606 does, or exits as the pair does
STUB_PAIR = r"""#!/usr/bin/env bash
set -eu
PO="$(python3 -m bench.evidence run-dir "pair-$RUN_NAME_N1-$RUN_NAME_N2")"
if [ "${BENCH_606_DRY_RUN:-0}" = 1 ]; then
  cat > /dev/null
  mkdir -p "$PAIR_OUT/dry-N1" "$PAIR_OUT/dry-N2"
  echo "[06:52:59] DRY: bounded 1800 python3 -m bench.gke gke-up --name $RUN_NAME_N1 --ttl-hours 13 --yes" > "$PAIR_OUT/dry-N1/run.log"
  echo "[06:52:59] DRY: bounded 1800 python3 -m bench.gke gke-up --name $RUN_NAME_N2 --ttl-hours 12 --yes" > "$PAIR_OUT/dry-N2/run.log"
  echo "DRY: all 2 runs would start now"
  exit 0
fi
echo "[07:00:00] runs: N1 N2; PAIR_OUT $PO; bench $(git rev-parse HEAD); owner $USER; zone $ZONE"
case "${STUB_MODE:-ok}" in
  dirty) echo "refusing: uncommitted changes under the bench: every run must be one commit" >&2; exit 2 ;;
  refuse) echo "refusing: BENCH_606_GO must be '20261111-$RUN_NAME_N1' (Diego's explicit go for THIS run)" > "$PO/N1.console"
          sleep 30; exit 1 ;;
esac
for n in "$RUN_NAME_N1" "$RUN_NAME_N2"; do
  o="$(python3 -m bench.evidence run-dir "$n")"; mkdir -p "$o"
  echo "[07:00:01] run budget: up 1800 + ...; fits TTL_HOURS=13" >> "$o/run.log"
done
echo "paid launch: RUNS=$RUNS PAIRS=$PAIRS INSTALLER_VERSION=$INSTALLER_VERSION go=$BENCH_606_GO_N2"
i=0; while [ ! -e "$HOME/release" ] && [ $i -lt 200 ]; do sleep 0.1; i=$((i + 1)); done
echo "finished"
"""


def _repo(tmp_path) -> tuple[str, str, str]:
    """A snowplow stand-in: the bench's own detach and evidence, a stub launcher. (path, commit, the
    object id of an annotated tag on it)."""
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
    subprocess.run([*git, "tag", "-a", "-m", "a tag", "v0"], check=True)
    subprocess.run([*git, "config", "uploadpack.allowAnySHA1InWant", "true"], check=True)

    def rev(x):
        return subprocess.run([*git, "rev-parse", x], check=True, capture_output=True, text=True).stdout.strip()
    return str(r), rev("HEAD"), rev("v0")


def _venv(run_dir: Path) -> None:
    """The run's venv on the VM, pre-made: its python3 is this interpreter, its pip a stub (no network)."""
    v = run_dir / "venv" / "bin"
    v.mkdir(parents=True)
    (v / "python3").symlink_to(sys.executable)
    (v / "pip").write_text(f"#!/bin/sh\necho \"$@\" >> {run_dir / 'pip.log'}\n")
    (v / "pip").chmod(0o755)


ENV = {"RUNS": "N1 N2", "PAIRS": "N1:N2", "INSTALLER_VERSION": "0.3.545",
       "RUN_NAME_N1": "snowplow-bench-run25n1-8a395c45", "RUN_NAME_N2": "snowplow-bench-run25n2-8a395c45",
       "SNOWPLOW_TAG_N1": "1.12.67", "SNOWPLOW_TAG_N2": "1.12.68",
       "EXPECTED_IMAGE_DIGEST_N1": "sha256:" + "a" * 64, "EXPECTED_IMAGE_DIGEST_N2": "sha256:" + "b" * 64,
       "BENCH_606_GO_N1": "20261110-snowplow-bench-run25n1-8a395c45",
       "BENCH_606_GO_N2": "20261110-snowplow-bench-run25n2-8a395c45"}
PAIR = vm606.pair_name(ENV)


def _env_file(tmp_path, extra: str = "", **more) -> Path:
    p = tmp_path / "launch.env"
    p.write_text("# the laptop launcher's variables\n"
                 + "".join(f"{k}={shlex.quote(v)}\n" for k, v in {**ENV, **more}.items()) + extra)
    return p


def _pin(tmp_path) -> tuple[Path, str]:
    p = tmp_path / "installer-0.3.545-pin-manifest.json"
    p.write_text(json.dumps({"installer": "0.3.545", "components": []}))
    return p, hashlib.sha256(p.read_bytes()).hexdigest()


def _launch_args(tmp_path, repo, sha, pin, pin_sha, env_file=None, bench_zone="europe-west1-b"):
    return ["launch", "--run", RUN, "--bench-sha", sha, "--env-file", str(env_file or _env_file(tmp_path)),
            "--pin-manifest", str(pin), "--pin-sha256", pin_sha, "--repo-url", repo, "--yes",
            "--started-timeout-s", "60", "--bench-zone", bench_zone]


def _vm_ready(fake, hours=15.0) -> Path:
    """After up and push-secret: the run's directory with the token in it and its venv. Returns it."""
    srv = _vm(fake, hours=hours)
    fake_vm606.bootstrap_done(fake, VM, BOOT)
    _record(bootstrap=BOOT)
    r = srv / RUN
    tok = r / "home" / ".config" / "snowplow-bench" / "ghcr-pull.env"
    tok.parent.mkdir(parents=True)
    tok.write_text("opaque")
    tok.chmod(0o600)
    r.chmod(0o700)
    _venv(r)
    return r


def _wait_for(path: Path, text: str, s: float = 30) -> str:
    end = time.monotonic() + s
    while time.monotonic() < end:
        if path.exists() and text in path.read_text():
            return path.read_text()
        time.sleep(0.05)
    raise AssertionError(f"{path} never held {text!r}: {path.read_text() if path.exists() else 'absent'}")


def test_launch_starts_the_launcher_detached_and_it_survives_the_session(fake, capsys, tmp_path):
    repo, sha, _tag = _repo(tmp_path)
    r = _vm_ready(fake)
    pin, pin_sha = _pin(tmp_path)
    rc, out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 0, err + out
    assert f"vm606: bench checkout at {sha}" in out and "vm606: dry pass OK" in out   # the dry pass read stdin
    assert "the run's TTL 13 h + margin fits" in out                       # the larger of the runs' TTLs
    assert "launched on" in out and "every run passed its paid-only checks" in out
    pid = int((r / "launch.pid").read_text())
    # launch returned while the launcher still waits for its release: it did not wait for it. The session
    # was hung up (the fake SIGHUPs and TERMs its process group); the launcher lives on, in a session of
    # its own, and finishes when released
    os.kill(pid, 0)
    assert os.getsid(pid) == pid
    assert "finished" not in (r / "launch.log").read_text()
    (r / "home" / "release").write_text("")
    log = _wait_for(r / "launch.log", "finished")
    assert f"paid launch: RUNS=N1 N2 PAIRS=N1:N2 INSTALLER_VERSION=0.3.545 go={ENV['BENCH_606_GO_N2']}" in log
    assert "; owner snowplow-bench" in log and fake_vm606.IDENTITY not in log      # gke-up's owner label: $USER
    assert "; zone europe-west1-b" in log                    # the clusters' zone, not the VM's (ZONE)
    placed = r / "home" / ".local" / "share" / "snowplow-bench" / "runs" / "606" / PAIR / pin.name
    assert placed.read_bytes() == pin.read_bytes()                        # the manifest beside the pair's files
    assert stat.S_IMODE((r / "launch-env.sh").stat().st_mode) == 0o600
    assert "jsonschema" in (r / "pip.log").read_text()
    st = _state()
    assert st["bench_sha"] == sha and st["pair"] == PAIR and st["runs"] == [ENV["RUN_NAME_N1"], ENV["RUN_NAME_N2"]]
    assert st["run_ttl_hours"] == 13 and st["fetched"] is False and st["started"] == "ok"
    assert st["bench_zone"] == "europe-west1-b"
    assert st["pair_out"].endswith(f"/{RUN}/home/.local/share/snowplow-bench/runs/606/{PAIR}")


@pytest.mark.parametrize("mode, why", [
    ("refuse", "run N1: refusing: BENCH_606_GO must be '20261111-snowplow-bench-run25n1-8a395c45'"),
    ("dirty", "vm606: launcher: refusing: uncommitted changes under the bench"),
])
def test_launch_says_launched_only_once_every_run_passed_its_paid_only_checks(fake, capsys, tmp_path, mode, why):
    repo, sha, _tag = _repo(tmp_path)
    _vm_ready(fake)
    pin, pin_sha = _pin(tmp_path)
    rc, out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha, _env_file(tmp_path, STUB_MODE=mode)))
    assert rc == 2 and why in out and "did not get every run past its paid-only checks" in err, out + err
    assert "launched on" not in out
    assert _state()["started"] == ("refused" if mode == "refuse" else "exited")


def test_launch_refuses_a_checkout_that_is_not_the_sha_asked(fake, capsys, tmp_path):
    """An annotated tag's object id fetches, and checks out its commit: HEAD is not the id asked."""
    repo, sha, tag = _repo(tmp_path)
    r = _vm_ready(fake)
    pin, pin_sha = _pin(tmp_path)
    rc, out, err = _run(capsys, *_launch_args(tmp_path, repo, tag, pin, pin_sha))
    assert rc == 2 and f"the checkout is at {sha}, not {tag}" in err, err + out
    assert not (r / "launch.pid").exists()


def test_launch_refuses_a_sha_the_repo_does_not_have(fake, capsys, tmp_path):
    repo, _sha, _tag = _repo(tmp_path)
    r = _vm_ready(fake)
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, "0" * 40, pin, pin_sha))
    assert rc == 2 and "the VM's prepare step failed" in err
    assert not (r / "launch.pid").exists()


def test_launch_refuses_a_pin_manifest_changed_on_its_way_to_the_vm(fake, capsys, tmp_path, monkeypatch):
    repo, sha, _tag = _repo(tmp_path)
    r = _vm_ready(fake)
    pin, pin_sha = _pin(tmp_path)
    monkeypatch.setenv("FAKE_SCP_CORRUPT", pin.name)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "the pin manifest on the VM has sha256" in err and f"not {pin_sha}" in err
    assert not (r / "snowplow").exists()


def test_launch_refuses_a_pin_manifest_changed_in_the_pairs_directory(fake, capsys, tmp_path, monkeypatch):
    repo, sha, _tag = _repo(tmp_path)
    r = _vm_ready(fake)
    pin, pin_sha = _pin(tmp_path)
    # after the second ssh (prepare), the copy in the pair's run directory changes
    monkeypatch.setenv("FAKE_TAMPER_AFTER_SSH",
                       f"2:{RUN}/home/.local/share/snowplow-bench/runs/606/{PAIR}/{pin.name}")
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "the pin manifest in" in err and f"/runs/606/{PAIR} has sha256" in err
    assert not (r / "launch.pid").exists()


def test_launch_refuses_a_vm_that_dies_before_the_run_ends(fake, capsys, tmp_path):
    repo, sha, _tag = _repo(tmp_path)
    r = _vm_ready(fake, hours=13.5)                         # 13 h run + 1 h margin does not fit
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "the run's TTL is 13 h" in err and "--ttl-hours >= 14" in err
    assert not (r / "launch.pid").exists()


def test_launch_refuses_without_the_token_on_the_vm(fake, capsys, tmp_path):
    repo, sha, _tag = _repo(tmp_path)
    r = _vm_ready(fake)
    (r / "home" / ".config" / "snowplow-bench" / "ghcr-pull.env").unlink()
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "push-secret first" in err


def test_launch_refuses_a_pin_manifest_whose_sha256_differs(fake, capsys, tmp_path):
    repo, sha, _tag = _repo(tmp_path)
    _vm_ready(fake)
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
    ("GHCR_PULL_TOKEN=opaque\n", "GHCR_PULL_TOKEN is the pull token's"),
    ("GHCR_PULL_USERNAME=someone\n", "GHCR_PULL_USERNAME is the pull token's"),
    ("RUNS=N1 N2\n", "one literal value per variable"),
    ("SNOWPLOW_TAG_N1=$TAG\n", "no expansion"),
])
def test_launch_refuses_an_env_that_cannot_travel(fake, capsys, tmp_path, extra, why):
    pin, pin_sha = _pin(tmp_path)
    env = tmp_path / "e.env"
    env.write_text("".join(f"{k}={shlex.quote(v)}\n" for k, v in ENV.items()
                           if not (extra.startswith("RUNS=") and k == "RUNS")) + extra)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha, env))
    assert rc == 2 and why in err, err
    assert fake_vm606.calls(fake) == []


def test_launch_refuses_an_env_zone_that_is_not_the_bench_zone(fake, capsys, tmp_path):
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha,
                                               _env_file(tmp_path, ZONE="europe-west4-a")))
    assert rc == 2 and "the env file's ZONE=europe-west4-a is not --bench-zone europe-west1-b" in err
    assert fake_vm606.calls(fake) == []


def test_launch_dry_starts_nothing(fake, capsys, tmp_path):
    _record(bootstrap=BOOT)
    pin, pin_sha = _pin(tmp_path)
    args = [a for a in _launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha) if a != "--yes"]
    rc, out, err = _run(capsys, *args)
    assert rc == 0, err
    assert (f"DRY-RUN: gcloud compute scp --project {PROJECT} --zone {ZONE} --tunnel-through-iap --quiet "
            f"{pin} {VM}:/srv/snowplow-bench/{RUN}/pin/{pin.name}") in out
    assert "clusters in ZONE=europe-west1-b; nothing was started" in out and fake_vm606.calls(fake) == []


def test_pair_name_is_run606_pairs():
    src = (BENCH_DIR / "run606_pair.sh").read_text()
    assert 'PAIR_NAME="pair"\n' in src
    assert 'for x in "${LABELS[@]}"; do nm="RUN_NAME_$x"; PAIR_NAME="${PAIR_NAME}-${!nm}"; done' in src
    assert PAIR == f"pair-{ENV['RUN_NAME_N1']}-{ENV['RUN_NAME_N2']}"


def test_a_remote_script_survives_a_child_that_reads_stdin(tmp_path):
    """vm606 feeds its scripts on bash's stdin: a child reading stdin must not eat the rest."""
    body = 'cat > /dev/null\necho "after the child"\n'
    p = subprocess.run(["bash", "-s"], input=vm606.remote(RUN, body, in_dir=False), capture_output=True, text=True,
                       timeout=30)
    assert p.returncode == 0 and "after the child" in p.stdout
    # the hazard it guards against, shown: a flat script loses its tail to the child, and exits 0
    flat = subprocess.run(["bash", "-s"], input=body, capture_output=True, text=True, timeout=30)
    assert flat.returncode == 0 and "after the child" not in flat.stdout


# ─── status ─────────────────────────────────────────────────────────────────

def test_status_tails_the_log_redacted_and_says_the_vms_time_left(fake, capsys):
    srv = _vm(fake, hours=10)
    r = srv / RUN
    r.mkdir()
    (r / "launch.log").write_text("".join(f"[06:5{i}:00] line {i}\n" for i in range(5))
                                  + "authenticated as someone@example.com\n")
    (r / "launch.pid").write_text(f"{os.getpid()}\n")
    rc, out, err = _run(capsys, "status", "--run", RUN, "--lines", "3")
    assert rc == 0, err
    assert f"{VM}: RUNNING, deleted in 10.0 h; launcher running {os.getpid()}" in out
    assert "line 2" not in out and "line 3" in out and "line 4" in out
    assert "someone@example.com" not in out and "authenticated as <redacted>" in out


# ─── fetch ──────────────────────────────────────────────────────────────────

NAMES = [ENV["RUN_NAME_N1"], ENV["RUN_NAME_N2"]]


def _vm_after_run(fake, monkeypatch, *, alive=False) -> Path:
    """The VM after a launch: each run's directory and its evidence (published by bench.evidence)."""
    srv = _vm(fake, hours=3)
    r = srv / RUN
    sha = "c" * 40
    b = r / "snowplow" / "go" / "snowplow" / "e2e" / "bench" / "bench"
    b.mkdir(parents=True)
    for f in ("__init__.py", "evidence.py"):
        shutil.copy(BENCH_DIR / "bench" / f, b / f)
    _venv(r)
    data = r / "home" / ".local" / "share" / "snowplow-bench"
    monkeypatch.setenv("BENCH_ALLOW_VOLATILE_PATHS", "1")
    for n in [*NAMES, PAIR]:
        src = data / "runs" / "606" / n
        (src / "k6").mkdir(parents=True)
        (src / "run.log").write_text(f"{n}: run complete\n")
        (src / "k6" / "stream.jsonl").write_text('{"t": 1}\n' * 100)
        evidence.publish(src, data / "evidence" / "606" / n, n, "final")
    (r / "launch.log").write_text("launcher log\n")
    (r / "launch.pid").write_text(f"{os.getpid() if alive else 2 ** 22 + 7}\n")
    _record(bench_sha=sha, runs=NAMES, pair=PAIR, launched_at=time.time(), fetched=False)
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
    st = _state()
    assert st["fetched"] is True and len(st["fetch_sha256"]) == 64
    assert not (vm606.state_dir(RUN) / "staging").exists() and not (vm606.state_dir(RUN) / "fetch.tar").exists()


def test_fetch_refuses_an_archive_changed_in_transit(fake, capsys, monkeypatch):
    _vm_after_run(fake, monkeypatch)
    monkeypatch.setenv("FAKE_SCP_CORRUPT", "fetch.tar")
    rc, _out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 2 and "the fetch archive arrived with sha256" in err and "nothing was moved" in err
    for n in [*NAMES, PAIR]:
        assert not evidence.run_dir(n).exists() and not evidence.evidence_dir(n).exists()


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

OTHER = {"role": "roles/container.admin", "members": [SA],
         "condition": {"title": "snowplow-bench-run9", "expression": 'request.time < timestamp("2026-11-12T00:00:00Z")',
                       "description": "vm606: another run's"}}
UNCONDITIONAL = {"role": "roles/compute.viewer", "members": [SA]}


def _upped(fake, capsys, monkeypatch):
    """After a real (fake) up: grants, rules, VM; then another run's binding and an unconditional one
    appear (granted by someone else after up, which refuses to start beside an unconditional one)."""
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 0, err
    pol = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
    fake_vm606.put_policy(fake, f"project-{PROJECT}", [OTHER, UNCONDITIONAL, *pol])


def test_down_deletes_the_vm_disk_rules_and_only_its_own_grants_and_verifies(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err
    assert not (fake / "instances" / f"{VM}.json").exists() and not (fake / "disks" / f"{VM}.json").exists()
    assert list((fake / "firewalls").glob("*.json")) == []
    assert fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"] == [OTHER, UNCONDITIONAL]
    assert fake_vm606.policy(fake, f"sa-{NODE_SA}")["bindings"] == []
    assert f"{VM} gone and verified: disk(s) ['{VM}']" in out and "2 firewall rule(s), 7 conditional grant(s)" in out
    lists = [c for c in fake_vm606.calls(fake) if c["argv"][2:3] == ["list"]]
    assert lists and all(c["partial_error"] == "false" for c in lists)


def test_down_reads_the_grants_back_after_removing_them(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    monkeypatch.setenv("FAKE_IAM_LOST_WRITE", "roles/cloudquotas.viewer")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "bindings of run run25 are still held after their removal" in err
    assert "roles/cloudquotas.viewer" in err


def test_down_after_the_vms_ttl_still_removes_its_rules_and_grants(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    (fake / "instances" / f"{VM}.json").unlink()            # GCE deleted it at --termination-time
    (fake / "disks" / f"{VM}.json").unlink()
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err
    assert "already gone" in out and list((fake / "firewalls").glob("*.json")) == []
    assert fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"] == [OTHER, UNCONDITIONAL]


def test_down_dry_deletes_nothing(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    rc, out, _err = _run(capsys, "down", "--run", RUN)
    assert rc == 0 and "DRY-RUN: gcloud compute instances delete" in out and "nothing was deleted" in out
    assert out.count("DRY-RUN: gcloud projects remove-iam-policy-binding") == 6
    assert (fake / "instances" / f"{VM}.json").exists() and len(list((fake / "firewalls").glob("*.json"))) == 2


@pytest.mark.parametrize("labels", [{"owner": "someone-else", "run": RUN}, {"owner": "snowplow-bench", "run": "run9"},
                                    {"run": RUN}])
def test_down_refuses_an_instance_not_labelled_for_the_run(fake, capsys, labels):
    _vm(fake, labels, 3)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "is not labelled owner=snowplow-bench, run=run25" in err
    assert "compute instances delete" not in _verbs(fake)


def test_down_refuses_a_firewall_rule_it_did_not_make_and_deletes_nothing(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    p = fake / "firewalls" / f"{VM}-iap-ssh.json"
    p.write_text(json.dumps({**json.loads(p.read_text()), "description": "someone else's"}))
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"firewall rule '{VM}-iap-ssh' is not one vm606 made for run {RUN}" in err
    assert "someone else" in err and "nothing was changed" in err
    assert not [v for v in _verbs(fake) if v.endswith(" delete") or "remove-iam" in v]


def test_down_removes_a_binding_with_its_title_but_not_its_form_and_reports_it(fake, capsys, monkeypatch):
    """The run's exact title and the orchestrator SA: the run's binding, whatever its expression became. Removing
    access is the safe direction: it goes, and the drift is said (ruling on #1331, 2026-11-11)."""
    _upped(fake, capsys, monkeypatch)
    pol = fake_vm606.policy(fake, f"project-{PROJECT}")
    pol["bindings"].append({"role": "roles/owner", "members": [SA],
                            "condition": {"title": f"snowplow-bench-{RUN}", "expression": "true"}})
    fake_vm606.put_policy(fake, f"project-{PROJECT}", pol["bindings"])
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err
    assert ("WARNING: drifted since up, removed all the same: binding roles/owner on operations-dev-krateo-io under "
            f"snowplow-bench-{RUN}: expression \"true\", not the expiry form vm606 writes") in out
    assert "8 conditional grant(s)" in out and "1 field(s) had drifted since up" in out
    assert not [b for b in fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
                if (b.get("condition") or {}).get("title") == f"snowplow-bench-{RUN}"]
    assert any("roles/owner" in d for d in _state()["down_drift"])


def test_down_refuses_when_the_label_selects_another_vm(fake, capsys):
    _vm(fake, hours=3)
    fake_vm606.put_instance(fake, "snowplow-bench-orch-other", LABELS, _in(3), zone=ZONE)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"besides {VM} ({ZONE}), snowplow-bench-orch-other ({ZONE}) claim(s) run {RUN}" in err
    assert _changing(fake) == []


def test_down_fails_closed_when_a_list_fails_open(fake, capsys, monkeypatch):
    """gcloud's list exits 0 with [] when its requests fail; vm606 sets the property that makes it exit 1."""
    _vm(fake, hours=3)
    monkeypatch.setenv("FAKE_LIST_FAILS", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "could not be listed (rc=1" in err
    assert "compute instances delete" not in _verbs(fake)


def test_down_refuses_a_disk_left_behind(fake, capsys, monkeypatch):
    _vm(fake, hours=3)
    monkeypatch.setenv("FAKE_KEEP_DISKS", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"disk(s) ['{VM}'] remain" in err and "the pull-token file is on one of them" in err


def test_down_refuses_an_unfetched_launch_and_a_running_launcher(fake, capsys, monkeypatch):
    srv = _vm(fake, hours=3)
    _record(launched_at=time.time(), fetched=False)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "was launched and not fetched" in err
    r = srv / RUN
    r.mkdir()
    (r / "launch.pid").write_text(f"{os.getpid()}\n")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 2 and "the launcher is still running" in err
    assert "compute instances delete" not in _verbs(fake)
    (r / "launch.pid").write_text(f"{2 ** 22 + 7}\n")
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 0, err


def test_down_with_nothing_left_says_so(fake, capsys):
    _record()                                       # an up that failed before its first grant
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err
    assert "already gone" in out and "0 firewall rule(s), 0 conditional grant(s)" in out
    assert "PLACEHOLDER-OPERATOR" not in out + err


# ─── the VM's own region; two launches at once ──────────────────────────────

def test_up_reads_the_vms_footprint_against_the_region_it_sits_in(fake, capsys, monkeypatch):
    rc, out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15")
    assert rc == 0, err
    for metric in ("INSTANCES", "E2_CPUS", "SSD_TOTAL_GB", "IN_USE_ADDRESSES"):
        assert re.search(rf"^{metric} in europe-west4: .*the VM needs", out, re.M), (metric, out)
    # #1361: and the project's FIREWALLS, one per SSH rule up makes
    assert re.search(rf"^FIREWALLS in operations-dev-krateo-io: 68 of 100 used, 32 free; the VM needs 2: its 2 "
                     rf"IAP-only SSH rules \({VM}-deny-ssh, {VM}-iap-ssh\)$", out, re.M), out
    lists = [c for c in fake_vm606.calls(fake) if c["argv"][2:3] == ["list"]]
    # the quota pre-check's lists, and #1383 gap 6's reap of the project's expired SSH rules before them
    assert {c["argv"][1] for c in lists} == {"firewall-rules", "instances", "forwarding-rules", "addresses"}
    assert all(c["partial_error"] == "false" for c in lists)
    monkeypatch.setenv("FAKE_LIMIT_E2_CPUS", "4")
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--yes", "--poll-s", "0",
                         "--ssh-ready-timeout-s", "1", "--bootstrap-timeout-s", "1")
    assert rc == 2 and "the VM does not fit europe-west4's or operations-dev-krateo-io's quotas: E2_CPUS" in err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or v.endswith(" create")]


def test_up_counts_a_vm_already_in_its_region_and_none_in_another(fake, capsys, monkeypatch):
    """Run 24's VM (external IP) in europe-west4: with room for one more in-use address there, run 25's
    VM does not fit in europe-west4 and fits in europe-west1."""
    fake_vm606.put_instance(fake, vm606.vm_name("run24"), {**LABELS, "run": "run24"}, _in(15), zone="europe-west4-a")
    monkeypatch.setenv("FAKE_LIMIT_IN_USE_ADDRESSES", "1")
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15")
    assert rc == 2 and "IN_USE_ADDRESSES in europe-west4: 1 of 1 used (listed: 1 VM external IP(s)" in err
    rc, out, err = _run(capsys, "up", "--run", RUN, "--ttl-hours", "15", "--zone", "europe-west1-b")
    assert rc == 0, err
    assert "IN_USE_ADDRESSES in europe-west1: 0 of 1 used (listed: 0 VM external IP(s)" in out


def test_the_benchs_pre_check_counts_the_vm_among_its_regions_external_ips(fake, capsys, monkeypatch):
    """The VM as GCE lists it, counted by bench.gke's in-use counter (the run's pre-check) in its region only."""
    _up(capsys, monkeypatch)
    listed = {"instances": [json.loads((fake / "instances" / f"{VM}.json").read_text())],
              "forwarding-rules": [], "addresses": []}
    assert vm606.gke.in_use_addresses(listed, "europe-west3")[0] == 1
    assert vm606.gke.in_use_addresses(listed, "europe-west4")[0] == 0


def _two_runs(fake, capsys, monkeypatch):
    """run24's VM in europe-west4 and run25's in europe-west1, each up as up makes it."""
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    for run, zone in (("run24", "europe-west4-a"), (RUN, "europe-west1-b")):
        rc, _out, err = _run(capsys, "up", "--run", run, "--ttl-hours", "15", "--yes", "--poll-s", "0", "--zone", zone)
        assert rc == 0, err


def test_two_launches_at_once_down_of_one_never_touches_the_other(fake, capsys, monkeypatch):
    _two_runs(fake, capsys, monkeypatch)
    vm24 = vm606.vm_name("run24")
    before = {"inst": (fake / "instances" / f"{vm24}.json").read_text(),
              "rules": sorted(p.name for p in (fake / "firewalls").glob(f"{vm24}-*.json")),
              "proj": [b for b in fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
                       if b["condition"]["title"] == "snowplow-bench-run24"],
              "sa": [b for b in fake_vm606.policy(fake, f"sa-{NODE_SA}")["bindings"]
                     if b["condition"]["title"] == "snowplow-bench-run24"]}
    assert len(before["rules"]) == 2 and len(before["proj"]) == 6 and len(before["sa"]) == 1
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")          # its zone: up's record, europe-west1-b
    assert rc == 0, err
    assert not (fake / "instances" / f"{VM}.json").exists()
    assert (fake / "instances" / f"{vm24}.json").read_text() == before["inst"]
    assert sorted(p.name for p in (fake / "firewalls").glob("*.json")) == before["rules"]
    assert fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"] == before["proj"]
    assert fake_vm606.policy(fake, f"sa-{NODE_SA}")["bindings"] == before["sa"]
    rc, _out, err = _run(capsys, "down", "--run", "run24", "--yes")
    assert rc == 0, err
    assert fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"] == []


def test_two_launches_at_once_fetch_of_one_never_touches_the_other(fake, capsys, monkeypatch):
    data = _vm_after_run(fake, monkeypatch)                     # run25, fetchable
    vm24 = vm606.vm_name("run24")
    srv24 = fake_vm606.put_instance(fake, vm24, {**LABELS, "run": "run24"}, _in(3))
    names24 = ["snowplow-bench-run24n1-x", "snowplow-bench-run24n2-x"]
    d24 = srv24 / "run24" / "home" / ".local" / "share" / "snowplow-bench" / "runs" / "606"
    for n in names24:
        (d24 / n).mkdir(parents=True)
        (d24 / n / "run.log").write_text("run 24 going\n")
    before = {p: p.read_bytes() for p in srv24.rglob("*") if p.is_file()}
    rc, _out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 0, err
    assert {p: p.read_bytes() for p in srv24.rglob("*") if p.is_file()} == before
    for n in names24:
        assert not evidence.run_dir(n).exists()
    assert all(evidence.run_dir(n).exists() for n in NAMES) and data.exists()


# ─── #1307 re-review: selection by exact name, a run name in use, the SA between runs ──

def test_down_reads_each_rule_back_and_refuses_a_delete_that_did_not_take(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    monkeypatch.setenv("FAKE_FW_DELETE_LOST", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"firewall rule(s) ['{VM}-deny-ssh', '{VM}-iap-ssh'] of {VM} remain after their delete" in err
    describes = [c["argv"][3] for c in fake_vm606.calls(fake) if c["argv"][:3] == ["compute", "firewall-rules", "describe"]]
    assert {f"{VM}-deny-ssh", f"{VM}-iap-ssh"} <= set(describes)


def _in_use(fake, how):
    if how == "record":
        vm606._save_state(RUN, vm=VM, created_at=time.time())
    elif how == "instance-elsewhere":
        fake_vm606.put_instance(fake, VM, LABELS, _in(3), zone="europe-west1-b")
    elif how == "instance-name-only":
        fake_vm606.put_instance(fake, VM, {}, _in(3), zone="europe-west1-b")
    elif how == "instance-label-only":
        fake_vm606.put_instance(fake, "snowplow-bench-orch-x", LABELS, _in(3), zone="europe-west1-b")
    elif how == "rule":
        fake_vm606.put_policy(fake, f"project-{PROJECT}", [])
        fake_vm606.put_rule(fake, RUN, f"{VM}-iap-ssh")
    elif how == "binding":
        fake_vm606.put_policy(fake, f"sa-{NODE_SA}", [{"role": "roles/iam.serviceAccountUser", "members": [SA],
                                                     "condition": vm606.iam_condition(RUN, "2026-11-11T00:00:00Z")}])


@pytest.mark.parametrize("how, named", [
    ("record", "its record"), ("instance-elsewhere", f"instance {VM} (europe-west1-b)"),
    ("instance-name-only", f"instance {VM} (europe-west1-b)"),
    ("instance-label-only", "instance snowplow-bench-orch-x (europe-west1-b)"),
    ("rule", f"firewall rule {VM}-iap-ssh"), ("binding", "binding roles/iam.serviceAccountUser on " + NODE_SA),
])
def test_up_refuses_a_run_name_in_use_before_granting_anything(fake, capsys, monkeypatch, how, named):
    _in_use(fake, how)
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f"run {RUN} is in use: " in err and named in err and f"`down --run {RUN}`" in err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or v.endswith(" create")]


@pytest.mark.parametrize("key", [f"{k}-{r}" for k, r in vm606.grant_policies(PROJECT, NUMBER)])
def test_up_refuses_while_the_sa_holds_an_unconditional_binding(fake, capsys, monkeypatch, key):
    fake_vm606.put_policy(fake, key, [{"role": "roles/compute.viewer", "members": [SA]}])
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "holds unconditional binding(s): roles/compute.viewer on" in err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or v.endswith(" create")]


def test_prefix_colliding_runs_down_of_run2_never_touches_run25(fake, capsys, monkeypatch):
    """run2 is a prefix of run25: titles, members and rule names are matched exactly, never by prefix."""
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    for run in ("run25", "run2"):
        rc, _out, err = _run(capsys, "up", "--run", run, "--ttl-hours", "15", "--yes", "--poll-s", "0")
        assert rc == 0, err
    other = {"role": "roles/compute.viewer", "members": ["serviceAccount:someone-else@example.invalid"],
             "condition": vm606.iam_condition("run2", "2026-11-11T00:00:00Z")}
    pol = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"] + [other]
    fake_vm606.put_policy(fake, f"project-{PROJECT}", pol)

    def run25_view():
        return ((fake / "instances" / f"{VM}.json").read_text(),
                sorted(p.name for p in (fake / "firewalls").glob(f"{VM}-*.json")),
                [b for k in (f"project-{PROJECT}", f"sa-{NODE_SA}") for b in fake_vm606.policy(fake, k)["bindings"]
                 if b.get("condition", {}).get("title") == "snowplow-bench-run25"])
    before = run25_view()
    assert len(before[1]) == 2 and len(before[2]) == 7
    rc, out, err = _run(capsys, "down", "--run", "run2", "--yes")
    assert rc == 0, err
    assert "2 firewall rule(s), 7 conditional grant(s) of run run2" in out
    assert run25_view() == before
    assert other in fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]          # another member's: kept
    assert not (fake / "instances" / "snowplow-bench-orch-run2.json").exists()


def test_run606_stops_named_when_the_kubernetes_api_refuses_the_orchestrator(harness):  # noqa: F811
    """#1307 L7: right after the first cluster exists, one SelfSubjectAccessReview; "no" stops the run with
    the cause named, before anything is installed, and the cluster is torn down."""
    env, state, _ambient, _tmp = harness
    env["FAKE_K8S_AUTHZ"] = "no"
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=600)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "the orchestrator SA's conditional container.admin is not honoured by the Kubernetes API" in out
    assert "=== step psi ===" not in out and "=== step install ===" not in out
    calls = _calls(state)
    assert any(c["tool"] == "kubectl" and "can-i" in c["argv"] for c in calls)
    assert not any(c["tool"] == "helm" and "install" in c["argv"] for c in calls)
    assert any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "delete"] for c in calls)


def test_a_describe_that_fails_otherwise_than_notfound_refuses_never_reads_as_absent(fake, capsys, monkeypatch):
    """A 503 is not "the rule is gone": down refuses before deleting anything, and up before granting."""
    _upped(fake, capsys, monkeypatch)
    monkeypatch.setenv("FAKE_FW_DESCRIBE_503", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    # #1361: gcloud's cause, not its code alone
    assert rc == 2 and (f"firewall rule {VM}-deny-ssh could not be read (rc=1; ERROR: (gcloud.compute.firewall-rules."
                        f"describe) Could not fetch resource: | - The service is currently unavailable. code=503)") in err
    assert not [v for v in _verbs(fake) if v.endswith(" delete") or "remove-iam" in v]


# ─── #1307 round 3: the run's own project and zone, every zone read, parents confirmed ──

def _snapshot(fake) -> dict:
    """What exists in the fake GCP: instances, disks, firewall rules, every policy."""
    return {d: {p.name: p.read_text() for p in sorted((fake / d).glob("*.json"))}
            for d in ("instances", "disks", "firewalls", "policies")}


@pytest.mark.parametrize("cmd", ["push-secret", "launch", "status", "fetch", "down"])
@pytest.mark.parametrize("flag", [["--zone", "europe-west4-a"], ["--project", PROJECT]])
def test_a_subcommand_on_an_existing_run_takes_no_zone_or_project(fake, capsys, tmp_path, cmd, flag):
    """Only up takes --zone / --project; every later step reads them from up's record (R1)."""
    _record()
    pin, pin_sha = _pin(tmp_path)
    argv = _launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha) if cmd == "launch" else [cmd, "--run", RUN]
    with pytest.raises(SystemExit) as e:
        vm606.main([*argv, *flag])
    assert e.value.code == 2 and f"unrecognized arguments: {' '.join(flag)}" in capsys.readouterr().err
    assert fake_vm606.calls(fake) == []


@pytest.mark.parametrize("argv", [["push-secret", "--yes"], ["status"], ["fetch"], ["down", "--yes"], ["down"]])
def test_a_subcommand_on_an_existing_run_refuses_without_ups_record(fake, capsys, argv):
    fake_vm606.put_instance(fake, VM, LABELS, _in(3), zone=ZONE)
    _token()
    rc, _out, err = _run(capsys, argv[0], "--run", RUN, *argv[1:])
    assert rc == 2 and f"no record of `up --yes` for run {RUN}" in err
    assert fake_vm606.calls(fake) == []


def test_launch_refuses_without_ups_record(fake, capsys, tmp_path):
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha))
    assert rc == 2 and f"no record of `up --yes` for run {RUN}" in err
    assert fake_vm606.calls(fake) == []


def test_r3_down_takes_the_zone_from_the_record_reviewer_two_runs_repro(fake, capsys, monkeypatch):
    """The reviewer's round-3 repro: run25's VM in europe-west1-b, launched and not fetched, `down` with no
    zone. At 27e65d5f the default zone read the live VM as gone and removed its rules and grants."""
    _two_runs(fake, capsys, monkeypatch)
    vm606._save_state(RUN, launched_at=1.0)
    before = _snapshot(fake)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"{RUN} was launched and not fetched" in err
    assert (fake / "instances" / f"{VM}.json").exists()
    assert _snapshot(fake) == before                  # the VM, its disk, its 2 rules, its 7 grants: all there
    assert "deleted_at" not in _state()


@pytest.mark.parametrize("zone, why", [
    ("europe-west4-a", f"{VM} answers NotFound in europe-west4-a (the run's recorded zone), but {VM} (europe-west1-b) "
                       f"claim(s) run {RUN}"),
    ("europe-west9-z", "zone europe-west9-z of project operations-dev-krateo-io could not be read"),
])
def test_down_with_a_wrong_zone_refuses_and_changes_nothing(fake, capsys, monkeypatch, zone, why):
    """A record whose zone is not the VM's (a zone that exists, and one GCE does not know): down refuses
    before any change, naming where the run's VM is."""
    _two_runs(fake, capsys, monkeypatch)
    _record(zone=zone)
    before = _snapshot(fake)
    n = len(fake_vm606.calls(fake))
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and why in err, err
    assert "already gone" not in out
    assert _snapshot(fake) == before
    assert not [c for c in fake_vm606.calls(fake)[n:]
                if re.search(r"\b(delete|remove-iam-policy-binding)\b", " ".join(c["argv"][:3]))]


@pytest.mark.parametrize("labels, name", [({}, VM), (LABELS, "snowplow-bench-orch-x")])
def test_down_refuses_while_an_instance_of_the_run_exists_in_another_zone(fake, capsys, monkeypatch, labels, name):
    """The VM is gone from the recorded zone, but an instance claiming the run (by its exact name alone, or by
    its run label alone) is in another: never "already gone"."""
    _upped(fake, capsys, monkeypatch)
    (fake / "instances" / f"{VM}.json").unlink()
    (fake / "disks" / f"{VM}.json").unlink()
    fake_vm606.put_instance(fake, name, labels, _in(3), zone="europe-west1-b")
    before = _snapshot(fake)
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"answers NotFound in {ZONE}" in err and f"{name} (europe-west1-b) claim(s) run {RUN}" in err
    assert "already gone" not in out and _snapshot(fake) == before
    assert not [v for v in _verbs(fake) if v.endswith(" delete") or "remove-iam" in v]


def test_down_refuses_while_the_vm_has_a_namesake_in_another_zone(fake, capsys, monkeypatch):
    """GCE names an instance per zone: a second snowplow-bench-orch-run25, in europe-west1-b, claims the run
    too. down deletes only the one instance that claims it, so it refuses."""
    _upped(fake, capsys, monkeypatch)
    fake_vm606.put_instance(fake, VM, LABELS, _in(3), zone="europe-west1-b", namesake=True)
    before = _snapshot(fake)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"besides {VM} ({ZONE}), {VM} (europe-west1-b) claim(s) run {RUN}" in err
    assert _snapshot(fake) == before
    assert not [v for v in _verbs(fake) if v.endswith(" delete") or "remove-iam" in v]


@pytest.mark.parametrize("code", ["403", "503"])
def test_down_refuses_an_instance_describe_that_fails_otherwise_than_notfound(fake, capsys, monkeypatch, code):
    """S4: a 403 or a 503 on the instance is not "gone": down refuses before any change."""
    _upped(fake, capsys, monkeypatch)
    before = _snapshot(fake)
    monkeypatch.setenv("FAKE_INSTANCE_DESCRIBE_FAIL", code)
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"instance {VM} ({ZONE}) could not be read (rc=1;" in err
    assert "already gone" not in out and _snapshot(fake) == before
    assert not [v for v in _verbs(fake) if v.endswith(" delete") or "remove-iam" in v]


def test_down_reads_the_instance_back_after_its_delete(fake, capsys, monkeypatch):
    """L9: an instance delete that answers rc 0 and does not take stops down before its rules and grants."""
    _upped(fake, capsys, monkeypatch)
    monkeypatch.setenv("FAKE_INSTANCE_DELETE_LOST", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"{VM} still answers in {ZONE} after its delete" in err
    assert (fake / "instances" / f"{VM}.json").exists() and len(list((fake / "firewalls").glob(f"{VM}-*.json"))) == 2
    assert len([b for k in (f"project-{PROJECT}", f"sa-{NODE_SA}") for b in fake_vm606.policy(fake, k)["bindings"]
                if (b.get("condition") or {}).get("title") == f"snowplow-bench-{RUN}"]) == 7
    assert not [v for v in _verbs(fake) if "firewall-rules delete" in v or "remove-iam" in v]


def test_up_refuses_an_unknown_project_or_zone_up_front(fake, capsys, monkeypatch):
    """S5: before anything else: the project, then the zone."""
    rc, _out, err = _up(capsys, monkeypatch, "--project", "no-such-project-zz123")
    assert rc == 2 and "project no-such-project-zz123 could not be read" in err
    assert "PLACEHOLDER-OPERATOR" not in err
    assert _verbs(fake) == ["projects describe no-such-project-zz123"]
    rc, _out, err = _up(capsys, monkeypatch, "--zone", "europe-west9-z")
    assert rc == 2 and f"zone europe-west9-z of project {PROJECT} could not be read" in err
    assert [" ".join(c["argv"][:4]) for c in fake_vm606.calls(fake)][1:] == [
        f"projects describe {PROJECT} --format", "compute zones describe europe-west9-z"]


def test_down_refuses_a_record_whose_project_is_unknown(fake, capsys):
    """S5: a missing project answers every describe in it with "The resource 'projects/<p>' was not found":
    down refuses at the parent, before any describe could read as "absent"."""
    vm606._save_state(RUN, vm=VM, project="no-such-project-zz123", zone=ZONE)
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "project no-such-project-zz123 could not be read" in err and "already gone" not in out
    assert _verbs(fake) == ["projects describe no-such-project-zz123"]


def test_down_refuses_a_project_that_is_not_active(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    before = _snapshot(fake)
    monkeypatch.setenv("FAKE_PROJECT_STATE", "DELETE_REQUESTED")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"project {PROJECT} is DELETE_REQUESTED, not ACTIVE" in err
    assert _snapshot(fake) == before


def test_down_refuses_a_record_whose_project_number_is_not_the_projects(fake, capsys, monkeypatch):
    _upped(fake, capsys, monkeypatch)
    before = _snapshot(fake)
    monkeypatch.setenv("FAKE_PROJECT_NUMBER", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"names project number {NUMBER}, {PROJECT} is 1" in err
    assert _snapshot(fake) == before


# gcloud 561's own answers, read live on 2026-11-11 (#1307 round 4; the PR body has the calls)
REAL_ABSENT = ("ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n - The resource "
               "'projects/operations-dev-krateo-io/zones/europe-west4-a/instances/snowplow-bench-orch-zz-absent' was "
               "not found\n")
REAL_PARENT = ("ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n - The resource "
               "'projects/no-such-project-zz123' was not found\n")
REAL_ZONE = ("ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n - Invalid value for field "
             "'zone': 'europe-west9-z'. Unknown zone.\n")
REAL_DISK = ("ERROR: (gcloud.compute.disks.describe) HTTPError 404: The resource "
             "'projects/operations-dev-krateo-io/zones/europe-west4-a/disks/snowplow-bench-orch-zz-absent' was not "
             "found. This command is authenticated as <redacted> which is the active account specified by the "
             "[core/account] property\n")
REAL_DISK_PARENT = ("ERROR: (gcloud.compute.disks.describe) NOT_FOUND: The resource 'projects/no-such-project-zz123' "
                    "was not found. This command is authenticated as <redacted> which is the active account specified "
                    "by the [core/account] property\n")
REAL_FW = ("ERROR: (gcloud.compute.firewall-rules.describe) Could not fetch resource:\n - The resource "
           "'projects/operations-dev-krateo-io/global/firewalls/snowplow-bench-orch-zz-absent-deny-ssh' was not "
           "found\n")


def test_a_notfound_is_about_the_resource_only_when_it_names_its_own_path():
    """S5: gcloud's words for a missing parent are a missing resource's; only the full path tells them apart."""
    inst = "projects/operations-dev-krateo-io/zones/europe-west4-a/instances/snowplow-bench-orch-zz-absent"
    disk = "projects/operations-dev-krateo-io/zones/europe-west4-a/disks/snowplow-bench-orch-zz-absent"
    fw = "projects/operations-dev-krateo-io/global/firewalls/snowplow-bench-orch-zz-absent-deny-ssh"
    assert vm606._not_found(REAL_ABSENT, inst) and vm606._not_found(REAL_DISK, disk) and vm606._not_found(REAL_FW, fw)
    for err in (REAL_PARENT, REAL_ZONE, REAL_DISK_PARENT):
        assert not vm606._not_found(err, inst) and not vm606._not_found(err, disk)
    assert not vm606._not_found(REAL_PARENT.replace("instances.describe", "firewall-rules.describe"), fw)
    # another resource's NotFound (a prefix of the name) is not this one's
    assert not vm606._not_found(REAL_ABSENT, inst[:-1])
    # the same name under another zone or project is another resource: its NotFound is not this one's
    assert not vm606._not_found(REAL_ABSENT, inst.replace("/zones/europe-west4-a/", "/zones/europe-west1-b/"))
    assert not vm606._not_found(REAL_ABSENT, inst.replace("operations-dev-krateo-io", "other-project"))


def test_grant_policies_are_every_policy_up_writes(fake, capsys, monkeypatch):
    """L11: the run-in-use, unconditional-binding and revoke reads go over grant_policies: exactly the
    policies up's grants are written to."""
    assert vm606.grant_policies(PROJECT, NUMBER) == [("project", PROJECT), ("sa", NODE_SA)]
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 0, err
    written = {("project" if c["argv"][0] == "projects" else "sa", c["argv"][2 if c["argv"][0] == "projects" else 3])
               for c in fake_vm606.calls(fake) if "add-iam-policy-binding" in " ".join(c["argv"][:3])}
    assert written == set(vm606.grant_policies(PROJECT, NUMBER))


def test_a_describe_that_answers_no_object_refuses_never_reads_as_absent(fake, capsys, monkeypatch):
    """rc 0 with something other than one resource (here `[]`) is not "the rule is gone": down refuses
    before any change (round-3 M12: an empty answer read as absent)."""
    _upped(fake, capsys, monkeypatch)
    before = _snapshot(fake)
    monkeypatch.setenv("FAKE_FW_DESCRIBE_NOT_OBJECT", "1")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and f"firewall rule {VM}-deny-ssh answered no JSON object: refusing" in err
    assert _snapshot(fake) == before


# ─── run 24's first real up: IAP 4047 on a fresh VM; the bootstrap gate; resuming up ──

def _ssh_calls(fake) -> int:
    return len([c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]])


@pytest.mark.parametrize("code", ["4047", "4003", "4049"])
def test_up_waits_out_an_iap_code_a_fresh_vm_answers_and_succeeds(fake, capsys, monkeypatch, code):
    """Run 24, 2026-11-11: a VM just created answered the tunnel with a code waiting fixes (4047: IAP's "make
    sure your VM ... has fully completed its startup sequence"); minutes later SSH answered. A code IAP's table
    does not list (4049) is waited out too."""
    monkeypatch.setenv("FAKE_SSH_FAIL", code)
    monkeypatch.setenv("FAKE_SSH_FAIL_TIMES", "3")
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 0 and f"{VM} bootstrapped" in out, err
    assert _ssh_calls(fake) == 4 and _state()["bootstrap"] == BOOT


@pytest.mark.parametrize("code", ["4047", "4049"])
def test_up_names_the_iap_code_when_the_vm_never_answers(fake, capsys, monkeypatch, code):
    monkeypatch.setenv("FAKE_SSH_FAIL", code)
    rc, out, err = _up(capsys, monkeypatch, "--ssh-ready-timeout-s", "0")
    assert rc == 2 and f"never answered in 0 s, last at IAP {code}:" in err
    assert "Failed to lookup" not in err and "PLACEHOLDER-OPERATOR" not in err      # the number, never gcloud's text


@pytest.mark.parametrize("code", ["4033", "4010"])
def test_up_stops_at_once_on_an_iap_code_waiting_cannot_fix(fake, capsys, monkeypatch, code):
    monkeypatch.setenv("FAKE_SSH_FAIL", code)
    rc, _out, err = _up(capsys, monkeypatch, "--ssh-ready-timeout-s", "600", "--bootstrap-timeout-s", "600")
    assert rc == 2 and f"failed at IAP {code}:" in err and _ssh_calls(fake) == 1


def test_an_ssh_failure_after_up_names_its_iap_code(fake, capsys, monkeypatch):
    _ready(fake)
    _token()
    monkeypatch.setenv("FAKE_SSH_FAIL", "4047")
    rc, _out, err = _run(capsys, "push-secret", "--run", RUN, "--yes")
    assert rc == 2 and "IAP tunnel code 4047" in err and "Failed to lookup" not in err


def test_the_iap_code_decides_before_ssh_s_own_closed_connection():
    """gcloud 561's text: the tunnel's code, then ssh's "Connection closed" (exit 255)."""
    err = ("ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [4033: 'not authorized'].\n"
           "kex_exchange_identification: Connection closed by remote host\n")
    assert vm606.ssh_stage(255, err) == (vm606.IAP_CODES["4033"][0], False)
    assert vm606.iap_code(err) == "4033" and vm606.iap_code("Connection closed by remote host") is None
    assert vm606.ssh_stage(255, err.replace("4033", "4047"))[1] is True
    assert vm606.ssh_stage(255, "kex_exchange_identification: Connection closed by remote host\n")[1] is True


@pytest.mark.parametrize("cmd", ["push-secret", "launch"])
@pytest.mark.parametrize("boot", [None, {**BOOT, "kubectl": "v1.35.0"}])
def test_push_secret_and_launch_refuse_a_run_whose_bootstrap_was_not_verified(fake, capsys, tmp_path, cmd, boot):
    """A run whose up stopped early (its record holds no verified bootstrap) is never pushed to or launched on."""
    _vm(fake)
    fake_vm606.bootstrap_done(fake, VM, BOOT)                  # the VM's marker alone is not the record's
    _record(**({"bootstrap": boot} if boot else {}))
    _token()
    pin, pin_sha = _pin(tmp_path)
    argv = (_launch_args(tmp_path, "unused", "a" * 40, pin, pin_sha) if cmd == "launch"
            else ["push-secret", "--run", RUN, "--yes"])
    rc, _out, err = _run(capsys, *argv)
    assert rc == 2 and f"run {RUN}'s record holds no verified bootstrap of {VM}" in err
    assert f"up --run {RUN} --yes` resumes its bootstrap wait" in err
    assert fake_vm606.calls(fake) == []


def _stopped_in_bootstrap(fake, capsys, monkeypatch) -> dict:
    """An up that made everything and stopped in its bootstrap wait (an IAP code waiting cannot fix)."""
    monkeypatch.setenv("FAKE_SSH_FAIL", "4033")
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "failed at IAP 4033" in err
    monkeypatch.delenv("FAKE_SSH_FAIL")
    st = _state()
    assert st.get("instance_created_at") and "bootstrap" not in st
    return st


def _no_writes_since(fake, n) -> list[str]:
    return [" ".join(c["argv"][:3]) for c in fake_vm606.calls(fake)[n:]
            if re.search(r"\b(create|delete|add-iam-policy-binding|remove-iam-policy-binding)\b",
                         " ".join(c["argv"][:3]))]


@pytest.mark.parametrize("extra", [[], ["--zone", ZONE, "--ttl-hours", "15", "--project", PROJECT]])
def test_up_resumes_a_run_that_stopped_in_its_bootstrap_wait(fake, capsys, monkeypatch, extra):
    _stopped_in_bootstrap(fake, capsys, monkeypatch)
    n = len(fake_vm606.calls(fake))
    before = _snapshot(fake)
    rc, out, err = _run(capsys, "up", "--run", RUN, "--yes", "--poll-s", "0", *extra)
    assert rc == 0, err
    assert "its 2 SSH rules and its 7 conditional bindings read back as up made them; resuming" in out
    assert f"{VM} bootstrapped" in out and _state()["bootstrap"] == BOOT
    assert _no_writes_since(fake, n) == [] and _snapshot(fake) == before        # nothing made again
    zones = {c["argv"][c["argv"].index("--zone") + 1] for c in fake_vm606.calls(fake)[n:] if "--zone" in c["argv"]}
    assert zones == {ZONE}                                                   # the record's zone, not the default


def test_up_resume_dry_reads_back_and_changes_nothing(fake, capsys, monkeypatch):
    _stopped_in_bootstrap(fake, capsys, monkeypatch)
    n = len(fake_vm606.calls(fake))
    rc, out, err = _run(capsys, "up", "--run", RUN)
    assert rc == 0 and "DRY: would wait for" in out, err
    assert _no_writes_since(fake, n) == [] and not [c for c in fake_vm606.calls(fake)[n:] if c["argv"][1] == "ssh"]
    assert "bootstrap" not in _state()


@pytest.mark.parametrize("flag, value", [("--zone", "europe-west4-a"), ("--project", "other-project"),
                                         ("--ttl-hours", "16")])
def test_up_resume_refuses_a_flag_that_is_not_the_records(fake, capsys, monkeypatch, flag, value):
    _stopped_in_bootstrap(fake, capsys, monkeypatch)
    n = len(fake_vm606.calls(fake))
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--yes", flag, value)
    assert rc == 2 and f"{flag} {value} is not run {RUN}'s recorded" in err
    assert fake_vm606.calls(fake)[n:] == []


def _patch(path: Path, fn) -> None:
    d = json.loads(path.read_text())
    fn(d)
    path.write_text(json.dumps(d))


def _set_item(key, value):
    def fn(d):
        items = [i for i in d["metadata"]["items"] if i["key"] != key]
        d["metadata"]["items"] = items + ([{"key": key, "value": value}] if value is not None else [])
    return fn


OTHER_NET = f"https://www.googleapis.com/compute/v1/projects/{PROJECT}/global/networks/other"
# #1331 review: each field carries an owner ruling (IAP-only SSH, expiring access, OS Login) or is one up sets
TAMPER = [
    ("deny-disabled", f"firewall rule {VM}-deny-ssh disabled: true, up made false",
     ("rule", f"{VM}-deny-ssh", lambda d: d.update(disabled=True))),
    ("iap-disabled", f"firewall rule {VM}-iap-ssh disabled: true, up made false",
     ("rule", f"{VM}-iap-ssh", lambda d: d.update(disabled=True))),
    ("rule-network", f'firewall rule {VM}-deny-ssh network: "{OTHER_NET}"',
     ("rule", f"{VM}-deny-ssh", lambda d: d.update(network=OTHER_NET))),
    ("rule-target-sa", f'firewall rule {VM}-iap-ssh targetServiceAccounts: ["<redacted>"], up made []',
     ("rule", f"{VM}-iap-ssh", lambda d: d.update(targetServiceAccounts=[NODE_SA]))),
    ("rule-source-tags", f'firewall rule {VM}-iap-ssh sourceTags: ["anything"], up made []',
     ("rule", f"{VM}-iap-ssh", lambda d: d.update(sourceTags=["anything"]))),
    ("rule-egress", f'firewall rule {VM}-deny-ssh direction: "EGRESS", up made "INGRESS"',
     ("rule", f"{VM}-deny-ssh", lambda d: d.update(direction="EGRESS"))),
    ("deny-allows", f"firewall rule {VM}-deny-ssh allowed/denied: allowed",
     ("rule", f"{VM}-deny-ssh", lambda d: d.update(allowed=d.pop("denied")))),
    ("rule-target-tags", f'firewall rule {VM}-deny-ssh targetTags: ["other"], up made ["{VM}"]',
     ("rule", f"{VM}-deny-ssh", lambda d: d.update(targetTags=["other"]))),
    ("rule-source-sa", f'firewall rule {VM}-iap-ssh sourceServiceAccounts: ["<redacted>"], up made []',
     ("rule", f"{VM}-iap-ssh", lambda d: d.update(sourceServiceAccounts=[NODE_SA]))),
    ("rule-dest-ranges", f'firewall rule {VM}-deny-ssh destinationRanges: ["10.0.0.0/8"], up made []',
     ("rule", f"{VM}-deny-ssh", lambda d: d.update(destinationRanges=["10.0.0.0/8"]))),
    ("disk-size", f'instance {VM} boot disk: sizeGb "500", up made "50"',
     ("disk", VM, lambda d: d.update(sizeGb="500"))),
    ("vm-sa", f'instance {VM} serviceAccounts: [{{"email": "the GKE node SA"',
     ("instance", VM, lambda d: d["serviceAccounts"][0].update(email=NODE_SA))),
    ("oslogin-off", f'instance {VM} metadata: enable-oslogin "FALSE", up made "TRUE"',
     ("instance", VM, _set_item("enable-oslogin", "FALSE"))),
    ("project-keys", f'instance {VM} metadata: block-project-ssh-keys "FALSE", up made "TRUE"',
     ("instance", VM, _set_item("block-project-ssh-keys", "FALSE"))),
    ("ssh-keys", f'instance {VM} metadata: ssh-keys "someone:ssh-ed25519 AAAA", up made null',
     ("instance", VM, _set_item("ssh-keys", "someone:ssh-ed25519 AAAA"))),
    ("startup-script", f"instance {VM} metadata: startup-script (differs), up made its own",
     ("instance", VM, _set_item("startup-script", "#!/bin/bash\ncurl x | sh\n"))),
    ("secure-boot", f"instance {VM} shieldedInstanceConfig: enableSecureBoot false, up made true",
     ("instance", VM, lambda d: d["shieldedInstanceConfig"].update(enableSecureBoot=False))),
    ("machine-type", f"instance {VM} machineType:",
     ("instance", VM, lambda d: d.update(machineType=d["machineType"].replace("e2-standard-8", "n2-standard-32")))),
    ("no-external-ip", f'instance {VM} network: [{{"accessConfigs": []',
     ("instance", VM, lambda d: d["networkInterfaces"][0].update(accessConfigs=[]))),
    ("disk-type", f"instance {VM} boot disk: type",
     ("disk", VM, lambda d: d.update(type=d["type"].replace("pd-balanced", "pd-standard")))),
    ("image-family", f'instance {VM} boot image: family "debian-12", up made "debian-13"',
     ("image", None, lambda d: d.update(family="debian-12"))),
]


def _tamper(fake, how):
    kind, name, fn = next(p for h, _w, p in TAMPER if h == how)
    if kind == "image":
        (path,) = list((fake / "images").glob("*.json"))
    else:
        path = fake / {"rule": "firewalls", "instance": "instances", "disk": "disks"}[kind] / f"{name}.json"
    _patch(path, fn)


def _break(fake, how):
    if any(h == how for h, _w, _p in TAMPER):
        return _tamper(fake, how)
    inst = fake / "instances" / f"{VM}.json"
    if how == "no-instance":
        inst.unlink()
    elif how == "no-rule":
        (fake / "firewalls" / f"{VM}-deny-ssh.json").unlink()
    elif how == "rule-priority":
        p = fake / "firewalls" / f"{VM}-deny-ssh.json"
        p.write_text(json.dumps({**json.loads(p.read_text()), "priority": 65535}))
    elif how == "rule-source":
        p = fake / "firewalls" / f"{VM}-iap-ssh.json"
        p.write_text(json.dumps({**json.loads(p.read_text()), "sourceRanges": ["0.0.0.0/0"]}))
    elif how == "binding-missing":
        pol = fake_vm606.policy(fake, f"sa-{NODE_SA}")["bindings"]
        fake_vm606.put_policy(fake, f"sa-{NODE_SA}", [])
        assert pol
    elif how == "binding-extra":
        pol = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
        title = f"snowplow-bench-{RUN}"
        cond = next(b["condition"] for b in pol if b["condition"]["title"] == title)
        fake_vm606.put_policy(fake, f"project-{PROJECT}", pol + [{"role": "roles/owner", "members": [SA],
                                                                  "condition": cond}])
    elif how == "binding-other-expiry":
        pol = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
        b = pol[0]
        b["condition"] = {**b["condition"], "expression": 'request.time < timestamp("2026-12-31T00:00:00Z")'}
        fake_vm606.put_policy(fake, f"project-{PROJECT}", pol)
    elif how == "unconditional":
        pol = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
        fake_vm606.put_policy(fake, f"project-{PROJECT}", pol + [{"role": "roles/compute.viewer", "members": [SA]}])
    elif how == "termination":
        d = json.loads(inst.read_text())
        d["scheduling"]["terminationTime"] = "2027-01-01T00:00:00Z"
        inst.write_text(json.dumps(d))
    elif how == "tags":
        d = json.loads(inst.read_text())
        d["tags"] = {"items": []}
        inst.write_text(json.dumps(d))
    elif how == "label":
        d = json.loads(inst.read_text())
        d["labels"] = {**d["labels"], "owner": "someone-else"}
        inst.write_text(json.dumps(d))
    elif how == "namesake":
        fake_vm606.put_instance(fake, VM, LABELS, _in(3), zone="europe-west1-b", namesake=True)
    elif how == "ttl-label":
        d = json.loads(inst.read_text())
        d["labels"] = {**d["labels"], "ttl-hours": "72"}
        inst.write_text(json.dumps(d))
    elif how == "termination-action":
        d = json.loads(inst.read_text())
        d["scheduling"]["instanceTerminationAction"] = "STOP"
        inst.write_text(json.dumps(d))


@pytest.mark.parametrize("how, why", [
    ("no-instance", f"instance {VM} answers NotFound in {ZONE}"),
    ("no-rule", f"firewall rule {VM}-deny-ssh answers NotFound"),
    ("rule-priority", f"firewall rule {VM}-deny-ssh priority: 65535, up made 1000"),
    ("rule-source", f'firewall rule {VM}-iap-ssh sourceRanges: ["0.0.0.0/0"], up made ["35.235.240.0/20"]'),
    ("binding-missing", f"roles/iam.serviceAccountUser on {NODE_SA} is not held under the condition"),
    ("binding-extra", "roles/owner on operations-dev-krateo-io is held under snowplow-bench-run25"),
    ("binding-other-expiry", "which up does not grant"),
    ("unconditional", "holds roles/compute.viewer on operations-dev-krateo-io unconditionally"),
    ("termination", f'instance {VM} terminationTime: "2027-01-01T00:00:00Z", up made'),
    ("tags", f'instance {VM} tags: [], up made ["{VM}"]'),
    ("label", "is not labelled owner=snowplow-bench, run=run25"),
    ("namesake", f"{VM} (europe-west1-b) claim(s) run {RUN} too"),
    ("ttl-label", f'instance {VM} labels: ttl-hours "72", up made "15"'),
    ("termination-action", f'instance {VM} instanceTerminationAction: "STOP", up made "DELETE"'),
    *[(how, why) for how, why, _p in TAMPER],
])
def test_up_resume_refuses_anything_partial_or_not_as_made_and_changes_nothing(fake, capsys, monkeypatch, how, why):
    _stopped_in_bootstrap(fake, capsys, monkeypatch)
    _break(fake, how)
    before = _snapshot(fake)
    n = len(fake_vm606.calls(fake))
    rc, out, err = _run(capsys, "up", "--run", RUN, "--yes", "--poll-s", "0")
    assert rc == 2 and why in err, err
    assert "resuming" not in out and _snapshot(fake) == before and _no_writes_since(fake, n) == []
    assert not [c for c in fake_vm606.calls(fake)[n:] if c["argv"][1] == "ssh"]


@pytest.mark.parametrize("state", [
    {"created_at": 1.0},                                         # stopped before its VM existed
    {"instance_created_at": 1.0, "bootstrap": BOOT},              # bootstrapped
    {"instance_created_at": 1.0, "secret_pushed_at": 1.0},
    {"instance_created_at": 1.0, "launched_at": 1.0},
    {"instance_created_at": 1.0, "deleted_at": 1.0},
])
def test_up_still_refuses_every_other_run_in_use(fake, capsys, monkeypatch, state):
    """S3: only an up that stopped in its bootstrap wait resumes; any other record is a run in use."""
    _record(**state)
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f"run {RUN} is in use: its record" in err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or v.endswith(" create")]


def test_an_up_whose_create_failed_is_not_resumed(fake, capsys, monkeypatch):
    """The record says the VM exists only once its create answered: a failed create is a run in use (down)."""
    monkeypatch.setenv("FAKE_CREATE_FAIL", "1")
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "instances create failed" in err and "instance_created_at" not in _state()
    monkeypatch.delenv("FAKE_CREATE_FAIL")
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f"run {RUN} is in use: its record" in err


def test_up_resume_confirms_the_parents_first(fake, capsys, monkeypatch):
    _stopped_in_bootstrap(fake, capsys, monkeypatch)
    monkeypatch.setenv("FAKE_PROJECT_STATE", "DELETE_REQUESTED")
    n = len(fake_vm606.calls(fake))
    rc, _out, err = _run(capsys, "up", "--run", RUN, "--yes", "--poll-s", "0")
    assert rc == 2 and f"project {PROJECT} is DELETE_REQUESTED, not ACTIVE" in err
    assert [" ".join(c["argv"][:2]) for c in fake_vm606.calls(fake)[n:]] == ["projects describe"]



# down's ownership is the exact name, owner/run labels, record's zone (a rule: its exact name, target tag and
# description); a field that drifted since up is deleted all the same, and said (ruling on #1331, 2026-11-11)
OWNERSHIP = {"rule-target-tags"}


@pytest.mark.parametrize("how, why", [(h, w) for h, w, _p in TAMPER if h not in OWNERSHIP])
def test_down_deletes_a_drifted_resource_and_reports_the_drift(fake, capsys, monkeypatch, how, why):
    """A VM moved to the node SA, OS Login off, a disabled deny rule...: exactly what is to go. down deletes it,
    prints each drifted field (named, no identity) and keeps them in the run's record."""
    _upped(fake, capsys, monkeypatch)
    _tamper(fake, how)
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 0, err
    assert f"WARNING: drifted since up, deleted all the same: {why}" in out
    assert "field(s) had drifted since up" in out
    assert not (fake / "instances" / f"{VM}.json").exists() and not (fake / "disks" / f"{VM}.json").exists()
    assert list((fake / "firewalls").glob("*.json")) == []
    assert not [b for k in (f"project-{PROJECT}", f"sa-{NODE_SA}") for b in fake_vm606.policy(fake, k)["bindings"]
                if (b.get("condition") or {}).get("title") == f"snowplow-bench-{RUN}"]
    st = _state()
    assert st["deleted_at"] and any(why in d for d in st["down_drift"])
    warnings = [ln for ln in out.splitlines() if ln.startswith("WARNING: drifted since up")]
    assert warnings and not [w for w in warnings + st["down_drift"] if "@" in w]      # roles, never addresses


@pytest.mark.parametrize("how", sorted(OWNERSHIP))
def test_down_refuses_a_rule_whose_ownership_fields_are_not_ups(fake, capsys, monkeypatch, how):
    _upped(fake, capsys, monkeypatch)
    _tamper(fake, how)
    before = _snapshot(fake)
    n = len(fake_vm606.calls(fake))
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "is not one vm606 made for run run25" in err and "nothing was changed" in err
    assert _snapshot(fake) == before and _no_writes_since(fake, n) == []


@pytest.mark.parametrize("how", ["vm-sa", "oslogin-off", "ssh-keys"])
@pytest.mark.parametrize("argv", [["status"], ["fetch"], ["push-secret", "--yes"]])
def test_every_other_step_still_refuses_any_drift(fake, capsys, monkeypatch, how, argv):
    """Only down acts on a drifted VM (to delete it): status, fetch and push-secret refuse, before any ssh / scp
    (resume refuses too: test_up_resume_refuses_anything_partial_or_not_as_made_and_changes_nothing)."""
    _ready(fake)
    _record(runs=NAMES, pair=PAIR, bench_sha="c" * 40, launched_at=1.0, fetched=False)
    _tamper(fake, how)
    _token()
    rc, _out, err = _run(capsys, argv[0], "--run", RUN, *argv[1:])
    assert rc == 2 and f"{VM} is not as up made it" in err
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][1] in ("ssh", "scp")]


@pytest.mark.parametrize("cmd", ["push-secret", "status"])
@pytest.mark.parametrize("how", ["vm-sa", "oslogin-off"])
def test_a_step_on_the_vm_refuses_a_vm_not_as_up_made_it(fake, capsys, cmd, how):
    _ready(fake)
    _tamper(fake, how)
    _token()
    rc, _out, err = _run(capsys, cmd, "--run", RUN, *(["--yes"] if cmd == "push-secret" else []))
    assert rc == 2 and f"{VM} is not as up made it" in err
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][1] in ("ssh", "scp")]


def test_up_reads_each_rule_back_right_after_creating_it(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_FW_ON_CREATE", json.dumps({"disabled": True}))
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f"firewall rule {VM}-deny-ssh disabled: true, up made false" in err
    assert "compute instances create" not in _verbs(fake)                  # before the VM exists


def test_up_reads_the_vm_back_right_after_creating_it(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_INSTANCE_ON_CREATE", json.dumps({"serviceAccounts": [{
        "email": NODE_SA, "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}]}))
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f'instance {VM} serviceAccounts: [{{"email": "the GKE node SA"' in err
    assert f"down --run {RUN} --yes" in err
    assert not [c for c in fake_vm606.calls(fake) if c["argv"][:2] == ["compute", "ssh"]]   # never waited on


def _flags(argv: list[str]) -> set[str]:
    return {x for x in argv if x.startswith("--")}


def test_the_read_back_covers_every_flag_of_the_create_commands():
    """Each flag up's create commands pass is a field the read-back compares (FIREWALL_FIELDS /
    INSTANCE_FIELDS), and each field compared has a value up made."""
    fw, rule = next(iter(vm606.firewall_rules(RUN, _in(15)).items()))
    assert _flags(vm606.firewall_create_argv(fw, rule, PROJECT)) == \
        {f for fl in vm606.FIREWALL_FIELDS.values() for f in fl if f.startswith("--")}
    assert _flags(vm606.create_command(RUN, PROJECT, ZONE, 15, "startup.sh", _in(15))) == \
        {f for fl in vm606.INSTANCE_FIELDS.values() for f in fl if f.startswith("--")}
    assert set(vm606.firewall_want(fw, rule, PROJECT)) == set(vm606.FIREWALL_FIELDS)
    st = {"project": PROJECT, "zone": ZONE, "ttl_hours": 15, "terminates_at": _in(15)}
    assert set(vm606.instance_want(RUN, st)) == set(vm606.INSTANCE_FIELDS)


# the keys gcloud 561's describe answered, read live and read-only on 2026-11-11 (the PR body has the calls):
# default-allow-ssh, an instance of the project, and its boot disk
REAL_FIREWALL_KEYS = {"allowed", "creationTimestamp", "description", "direction", "disabled", "id", "kind", "logConfig",
                      "name", "network", "priority", "selfLink", "sourceRanges"}
REAL_INSTANCE_KEYS = {"cpuPlatform", "creationTimestamp", "deletionProtection", "disks", "fingerprint", "id", "kind",
                      "labelFingerprint", "labels", "lastStartTimestamp", "machineType", "metadata", "name",
                      "networkInterfaces", "networkPerformanceConfig", "resourceStatus", "satisfiesPzi", "scheduling",
                      "selfLink", "serviceAccounts", "shieldedInstanceConfig", "shieldedInstanceIntegrityPolicy",
                      "startRestricted", "status", "tags", "zone"}
REAL_NIC_KEYS = {"accessConfigs", "aliasIpRanges", "fingerprint", "kind", "name", "network", "networkIP", "stackType",
                 "subnetwork"}
REAL_DISK_KEYS = {"architecture", "creationTimestamp", "enableConfidentialCompute", "guestOsFeatures", "id", "kind",
                  "labelFingerprint", "labels", "lastAttachTimestamp", "licenseCodes", "licenses", "name",
                  "physicalBlockSizeBytes", "satisfiesPzi", "selfLink", "sizeGb", "sourceImage", "sourceImageId",
                  "status", "type", "users", "zone"}
# what the read-back reads of each; the fake answers these in GCE's shapes
READ_FIREWALL = {"name", "network", "direction", "priority", "sourceRanges", "allowed", "description", "disabled"}
READ_INSTANCE = {"name", "zone", "machineType", "disks", "serviceAccounts", "networkInterfaces", "tags", "labels",
                 "metadata", "scheduling", "shieldedInstanceConfig"}


def test_the_fakes_describe_answers_in_gces_shapes(fake, capsys, monkeypatch):
    _up(capsys, monkeypatch)
    fw = json.loads((fake / "firewalls" / f"{VM}-iap-ssh.json").read_text())
    inst = json.loads((fake / "instances" / f"{VM}.json").read_text())
    disk = json.loads((fake / "disks" / f"{VM}.json").read_text())
    assert READ_FIREWALL <= REAL_FIREWALL_KEYS and READ_FIREWALL <= set(fw)
    assert READ_INSTANCE <= REAL_INSTANCE_KEYS and READ_INSTANCE <= set(inst)
    assert {"sizeGb", "type", "sourceImage"} <= REAL_DISK_KEYS and {"sizeGb", "type", "sourceImage"} <= set(disk)
    api = f"https://www.googleapis.com/compute/v1/projects/{PROJECT}"
    assert fw["network"] == f"{api}/global/networks/default" and fw["disabled"] is False
    assert fw["allowed"] == [{"IPProtocol": "tcp", "ports": ["22"]}]
    assert inst["zone"] == f"{api}/zones/{ZONE}" and inst["machineType"] == f"{api}/zones/{ZONE}/machineTypes/e2-standard-8"
    assert set(inst["networkInterfaces"][0]) <= REAL_NIC_KEYS
    assert inst["networkInterfaces"][0]["network"] == f"{api}/global/networks/default"
    assert inst["networkInterfaces"][0]["accessConfigs"][0]["type"] == "ONE_TO_ONE_NAT"
    assert inst["serviceAccounts"] == [{"email": f"snowplow-bench-orchestrator@{PROJECT}.iam.gserviceaccount.com",
                                        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}]
    assert {tuple(sorted(i)) for i in inst["metadata"]["items"]} == {("key", "value")}
    assert set(inst["metadata"]) == {"fingerprint", "items", "kind"} and set(inst["tags"]) == {"fingerprint", "items"}
    assert set(inst["shieldedInstanceConfig"]) == {"enableIntegrityMonitoring", "enableSecureBoot", "enableVtpm"}
    assert disk["sizeGb"] == "50" and disk["type"] == f"{api}/zones/{ZONE}/diskTypes/pd-balanced"
    assert disk["sourceImage"].startswith("https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/")



def test_the_read_back_is_against_what_up_recorded_not_this_checkout(fake, capsys, monkeypatch):
    """A vm606 checkout changed between up and down (a git pull mid-run: another machine type, disk size, startup
    script) does not make the run's own VM read as drifted: up records what it created, and every later
    read-back compares against the record. Real drift still refuses."""
    _vm_after_run(fake, monkeypatch)
    _record(bootstrap=BOOT)
    monkeypatch.setattr(vm606, "MACHINE_TYPE", "e2-standard-16")
    monkeypatch.setattr(vm606, "BOOT_DISK_GB", 100)
    monkeypatch.setattr(vm606, "startup_script", lambda: "#!/bin/bash\necho a later checkout\n")
    rc, out, err = _run(capsys, "status", "--run", RUN)
    assert rc == 0 and "is not as up made it" not in err, err
    rc, out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 0, err
    fake_vm606.put_rule(fake, RUN, f"{VM}-deny-ssh")
    fake_vm606.put_rule(fake, RUN, f"{VM}-iap-ssh")
    rc, out, err = _run(capsys, "down", "--run", RUN, "--yes", "--skip-launcher-check")
    assert rc == 0 and "drifted" not in out, err + out


def test_a_resume_after_a_checkout_change_still_refuses_real_drift(fake, capsys, monkeypatch):
    _stopped_in_bootstrap(fake, capsys, monkeypatch)
    monkeypatch.setattr(vm606, "MACHINE_TYPE", "e2-standard-16")
    monkeypatch.setattr(vm606, "startup_script", lambda: "#!/bin/bash\necho a later checkout\n")
    n = len(fake_vm606.calls(fake))
    rc, out, err = _run(capsys, "up", "--run", RUN, "--poll-s", "0")              # dry: the read-back only
    assert rc == 0 and "read back as up made them" in out, err                   # the record, not the checkout
    _tamper(fake, "oslogin-off")
    before = _snapshot(fake)
    rc, out, err = _run(capsys, "up", "--run", RUN, "--yes", "--poll-s", "0")
    assert rc == 2 and f'instance {VM} metadata: enable-oslogin "FALSE", up made "TRUE"' in err
    assert _snapshot(fake) == before and _no_writes_since(fake, n) == []


def test_a_record_without_what_up_made_reads_nothing_as_made(fake, capsys):
    _ready(fake)
    st = vm606._state(RUN)
    st.pop("made")
    (vm606.state_dir(RUN) / "state.json").write_text(json.dumps(st))
    rc, _out, err = _run(capsys, "status", "--run", RUN)
    assert rc == 2 and f"run {RUN}'s record holds no as-made of what up created" in err


# ─── #1361: the project's FIREWALLS before any grant; a refused create's cause; a resource not ready ─

def test_up_refuses_before_any_grant_when_the_projects_firewalls_do_not_fit(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_PROJECT_USAGE_FIREWALLS", "99")
    rc, _out, err = _up(capsys, monkeypatch)
    assert rc == 2 and ("the VM does not fit europe-west3's or operations-dev-krateo-io's quotas: FIREWALLS in "
                        "operations-dev-krateo-io: 99 of 100 used, 1 free; the VM needs 2: its 2 IAP-only SSH rules "
                        f"({VM}-deny-ssh, {VM}-iap-ssh)") in err, err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or v.endswith(" create")]


@pytest.mark.parametrize("which, what", [("FAKE_FW_CREATE_FAIL", f"firewall rule {VM}-deny-ssh could not be created"),
                                         ("FAKE_CREATE_FAIL", "instances create failed")])
@pytest.mark.parametrize("cause, words", [("QUOTA_EXCEEDED", ("Quota 'FIREWALLS' exceeded", "QUOTA_EXCEEDED")),
                                          ("ZONE_RESOURCE_POOL_EXHAUSTED", ("ZONE_RESOURCE_POOL_EXHAUSTED",)),
                                          ("the account", ("Required 'compute.instances.create' permission",)),
                                          ("a workforce principal quoted",
                                           ("Required 'compute.firewalls.create' permission",)),
                                          ("a wrapped authenticated-as sentence", ("HTTPError 404",))])
def test_a_refused_create_names_its_cause_and_never_the_account(fake, capsys, monkeypatch, which, what, cause, words):
    monkeypatch.setenv(which, cause)
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f"refusing: {what} (rc=1; ERROR: (gcloud.compute." in err, err
    assert all(w in err for w in words), err
    for leak in ("PLACEHOLDER", "authenticated as", "principal://"):
        assert leak not in out + err, (leak, err)


def test_a_subnetwork_not_ready_is_waited_for_by_its_operation_then_created(fake, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_NOT_READY", "2")
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 0, err
    assert f"instance {VM}: GCE answers projects/{PROJECT}/regions/europe-west3/subnetworks/default is not ready" in out
    assert _verbs(fake).count("compute instances create") == 2
