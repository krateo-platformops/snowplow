"""bench/vm606.py, the single-arm shape (Diego 2026-11-17, "Add a single-arm VM mode first"): ONE full measuring
arm launched on the VM, so the one-cluster rehearsal runs off the laptop. The shape is decided once from the
env file (launch_shape): RUNS naming exactly one arm (or --single naming it), no PAIRS. Then the launcher is
run606_single.sh: its dry pass hands the TTL from the arm's run.log, the TTL has no statistics term, the
started-check reads the one run.log, fetch brings the arm and the launch's directory back, down is unchanged.
Against the same fake gcloud and VM as tests/test_vm606.py."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess

import pytest

from bench import evidence, vm606
from tests import fake_vm606
from tests.test_vm606 import (BENCH_DIR, BOOT, CHART_CHECKS, ENV, RUN, ZONE, _pin, _record, _run, _state, _venv,
                              _vm, _vm_ready, _wait_for, fake, harness)  # noqa: F401  (fixtures)

ONE = {"RUNS": "N1", "INSTALLER_VERSION": "0.3.545", "RUN_NAME_N1": ENV["RUN_NAME_N1"],
       "SNOWPLOW_TAG_N1": ENV["SNOWPLOW_TAG_N1"], "EXPECTED_IMAGE_DIGEST_N1": ENV["EXPECTED_IMAGE_DIGEST_N1"],
       "BENCH_606_GO_N1": ENV["BENCH_606_GO_N1"]}
SINGLE = vm606.launch_name(ONE, vm606.SINGLE)

# a run606_single.sh stand-in, as tests/test_vm606.py's STUB_PAIR stands in for the pair's: its dry pass READS
# STDIN, then leaves the arm's run.log in $SINGLE_OUT/dry-N1 (the pair's layout) with its gke-up argv and its
# planned k6 seconds; its paid pass (STUB_MODE) logs the run budget into the arm's OUT and waits for a release
STUB_SINGLE = r"""#!/usr/bin/env bash
set -eu
if [ "${BENCH_606_DRY_RUN:-0}" = 1 ]; then
  cat > /dev/null
  mkdir -p "$SINGLE_OUT/dry-N1"
  echo "[06:52:59] DRY: bounded 1800 python3 -m bench.gke gke-up --name $RUN_NAME_N1 --ttl-hours ${STUB_TTL:-9} --yes" > "$SINGLE_OUT/dry-N1/run.log"
  echo "[06:52:59] k6 customers: K6_SPAN=churn, DRIVE_S=16170 s, composite burst at 900 s" >> "$SINGLE_OUT/dry-N1/run.log"
  echo "DRY: run N1 would start now"
  exit 0
fi
PO="$(python3 -m bench.evidence run-dir "single-$RUN_NAME_N1")"
case "${STUB_MODE:-ok}" in
  refuse) echo "refusing: BENCH_606_GO_N1 must be '20261111-$RUN_NAME_N1' (Diego's go for run N1)" > "$PO/N1.console"
          sleep 30; exit 1 ;;
  exit) echo "refusing: uncommitted changes under the bench: the run must be one commit" >&2; exit 2 ;;
  launcher-refuse) echo "refusing: OUT_N1 already holds a run (run.log): use a new RUN_NAME_N1 or OUT_N1" >&2
                   sleep 30; exit 2 ;;
  quiet) sleep 30; exit 0 ;;
esac
o="$(python3 -m bench.evidence run-dir "$RUN_NAME_N1")"; mkdir -p "$o"
echo "[07:00:01] run budget: up 1800 + ...; fits TTL_HOURS=9" >> "$o/run.log"
echo "single launch: RUNS=$RUNS PAIRS=${PAIRS:-unset} go=$BENCH_606_GO_N1"
i=0; while [ ! -e "$HOME/release" ] && [ $i -lt 200 ]; do sleep 0.1; i=$((i + 1)); done
echo "finished"
"""


def _repo(tmp_path) -> tuple[str, str]:
    """A snowplow stand-in with the single-arm launcher stub (and no pair launcher: a pair would fail)."""
    r = tmp_path / "upstream"
    b = r / "go" / "snowplow" / "e2e" / "bench"
    (b / "bench").mkdir(parents=True)
    for f in ("__init__.py", "detach.py", "evidence.py"):
        shutil.copy(BENCH_DIR / "bench" / f, b / "bench" / f)
    (b / "requirements.txt").write_text("")
    (b / "run606_single.sh").write_text(STUB_SINGLE)
    git = ["git", "-C", str(r), "-c", "user.email=t@example.invalid", "-c", "user.name=t"]
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "stub"], check=True)
    subprocess.run([*git, "config", "uploadpack.allowAnySHA1InWant", "true"], check=True)
    sha = subprocess.run([*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    return str(r), sha


def _env_file(tmp_path, **more) -> str:
    p = tmp_path / "single.env"
    p.write_text("".join(f"{k}={shlex.quote(v)}\n" for k, v in {**ONE, **more}.items() if v is not None))
    return str(p)


def _args(tmp_path, repo, sha, pin, pin_sha, *extra, env_file=None):
    return ["launch", "--run", RUN, "--bench-sha", sha, "--env-file", env_file or _env_file(tmp_path),
            "--pin-manifest", str(pin), "--pin-sha256", pin_sha, "--repo-url", repo, "--yes",
            "--started-timeout-s", "60", "--bench-zone", "europe-west1-b", "--single", "N1", *extra]


# ─── the shape, decided once ────────────────────────────────────────────────

def test_one_arm_and_no_pairs_is_the_single_shape_and_a_pair_stays_a_pair():
    assert vm606.launch_shape(dict(ONE), "N1") == (vm606.SINGLE, ["N1"])
    assert vm606.launch_shape(dict(ENV)) == (vm606.PAIR, ["N1", "N2"])
    assert SINGLE == f"single-{ENV['RUN_NAME_N1']}"


@pytest.mark.parametrize("env, single, why", [
    (dict(ONE), None, "the env file names one run (N1): a single-arm launch is stated on the command line too: pass "
                      "--single N1"),
    ({**ONE, "PAIRS": "N1:N2"}, "N1", "the env file names one run (N1) and PAIRS=N1:N2: a single arm compares nothing"),
    ({**ONE, "REPLICA_PAIR": "N1:N2"}, "N1", "the env file names one run (N1) and REPLICA_PAIR=N1:N2"),
    (dict(ENV), "N1", "--single N1: the env file's RUNS is 'N1 N2', not that one arm"),
    ({**{k: v for k, v in ENV.items() if k != "PAIRS"}, "RUNS": "N2"}, "N1",
     "--single N1: the env file's RUNS is 'N2', not that one arm"),
    ({k: v for k, v in ENV.items() if k != "PAIRS"}, None, "the env file lacks PAIRS"),
    ({**ONE, "RUNS": ""}, None, "the env file's RUNS names no run"),
    ({**ONE, "RUNS": "N1 N1"}, None, "RUNS names a run twice"),
    ({**ONE, "BENCH_606_REHEARSAL": "1"}, None, "BENCH_606_REHEARSAL is a test hook or a gcloud override"),
])
def test_a_shape_that_is_not_stated_is_refused_never_guessed(env, single, why):
    """Each refusal alone (mutation: dropping one turns its case RED); the pair's own refusals keep holding."""
    with pytest.raises(vm606.Refused) as e:
        vm606.launch_shape(env, single)
    assert why in str(e.value), str(e.value)


def test_launch_single_name_is_run606_singles():
    src = (BENCH_DIR / "run606_single.sh").read_text()
    assert 'SINGLE_NAME="single-${!nm}"\n' in src and 'nm="RUN_NAME_$L"' in src


def test_one_arm_has_no_statistics_term_and_a_pair_keeps_its_own():
    assert vm606.statistics_budget(None, "", vm606.SINGLE) == (0.0, "no statistics stage (one arm, no pair)")
    with pytest.raises(vm606.Refused):                       # a pair's unknown statistics still refuse
        vm606.statistics_budget(None, "N1:N2", vm606.PAIR)


# ─── launch, status, fetch, down: one arm ───────────────────────────────────

def test_launch_one_arm_dry_passes_its_launcher_reads_its_ttl_and_starts_it(fake, capsys, tmp_path):  # noqa: F811
    """The single launcher's dry pass, the TTL from the arm's own run.log with no statistics term, the started-check
    on the one run.log, the record's shape; the launcher runs detached and finishes when released."""
    repo, sha = _repo(tmp_path)
    r = _vm_ready(fake, hours=10.5)      # 9 h + 3600 s fits; with a statistics term (any) it would not need less
    pin, pin_sha = _pin(tmp_path)
    rc, out, err = _run(capsys, *_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 0, err + out
    assert "vm606: dry pass OK" in out
    assert "the run's TTL 9 h + its no statistics stage (one arm, no pair) + the 3600 s margin fit" in out
    assert "launched on" in out
    st = _state()
    assert st["shape"] == vm606.SINGLE and st["launch_dir"] == SINGLE and "pair" not in st
    assert st["runs"] == [ONE["RUN_NAME_N1"]] and st["run_ttl_hours"] == 9 and st["statistics_budget_s"] == 0
    assert st["started"] == "ok" and st["launch_out"].endswith(f"/runs/606/{SINGLE}")
    placed = r / "home" / ".local" / "share" / "snowplow-bench" / "runs" / "606" / SINGLE / pin.name
    assert placed.read_bytes() == pin.read_bytes()                       # the manifest beside the launch's files
    (r / "home" / "release").write_text("")
    log = _wait_for(r / "launch.log", "finished")
    assert f"single launch: RUNS=N1 PAIRS=unset go={ONE['BENCH_606_GO_N1']}" in log


def test_launch_one_arm_refuses_a_vm_that_dies_before_the_arm_ends(fake, capsys, tmp_path):  # noqa: F811
    repo, sha = _repo(tmp_path)
    _vm_ready(fake, hours=9.5)
    pin, pin_sha = _pin(tmp_path)
    rc, _out, err = _run(capsys, *_args(tmp_path, repo, sha, pin, pin_sha))
    assert rc == 2 and "the run's TTL is 9 h, its no statistics stage (one arm, no pair)" in err, err


@pytest.mark.parametrize("mode, why, started", [
    ("refuse", "run N1: refusing: BENCH_606_GO_N1 must be", "refused"),
    ("exit", "vm606: launcher: refusing: uncommitted changes under the bench", "refused"),
    ("launcher-refuse", "vm606: launcher: refusing: OUT_N1 already holds a run", "refused"),
])
def test_launch_one_arm_says_launched_only_once_its_run_passed_its_paid_only_checks(fake, capsys, tmp_path,  # noqa: F811
                                                                                    mode, why, started):
    repo, sha = _repo(tmp_path)
    _vm_ready(fake, hours=12)
    pin, pin_sha = _pin(tmp_path)
    rc, out, err = _run(capsys, *_args(tmp_path, repo, sha, pin, pin_sha, env_file=_env_file(tmp_path, STUB_MODE=mode)))
    assert rc == 2 and why in out and "did not get every run past its paid-only checks" in err, out + err
    assert _state()["started"] == started


def test_launch_one_arm_dry_says_the_single_launcher_and_starts_nothing(fake, capsys, tmp_path):  # noqa: F811
    _record(bootstrap=BOOT)
    pin, pin_sha = _pin(tmp_path)
    args = [a for a in _args(tmp_path, "unused", "a" * 40, pin, pin_sha) if a != "--yes"]
    rc, out, err = _run(capsys, *args)
    assert rc == 0, err
    assert "-- bash run606_single.sh, with " in out and f"one arm, no pair {ONE['RUN_NAME_N1']}; launch {SINGLE}" in out
    assert fake_vm606.calls(fake) == []


def _vm_after_one_arm(fake, monkeypatch):  # noqa: F811
    srv = _vm(fake, hours=3)
    r = srv / RUN
    b = r / "snowplow" / "go" / "snowplow" / "e2e" / "bench" / "bench"
    b.mkdir(parents=True)
    for f in ("__init__.py", "evidence.py"):
        shutil.copy(BENCH_DIR / "bench" / f, b / f)
    _venv(r)
    data = r / "home" / ".local" / "share" / "snowplow-bench"
    monkeypatch.setenv("BENCH_ALLOW_VOLATILE_PATHS", "1")
    for n in (ONE["RUN_NAME_N1"], SINGLE):
        src = data / "runs" / "606" / n
        src.mkdir(parents=True)
        (src / "run.log").write_text(f"{n}: run complete\n")
        evidence.publish(src, data / "evidence" / "606" / n, n, "final")
    (r / "launch.log").write_text("launcher log\n")
    (r / "launch.pid").write_text(f"{2 ** 22 + 7}\n")
    _record(bench_sha="c" * 40, runs=[ONE["RUN_NAME_N1"]], shape=vm606.SINGLE, launch_dir=SINGLE,
            launched_at=0, fetched=False)
    return data


def test_fetch_one_arm_brings_its_run_and_the_launchs_directory_back_verified(fake, capsys, monkeypatch):  # noqa: F811
    """fetch reads the record's runs and its launch directory, never a pair (mutation: names from st["pair"] only:
    nothing to fetch, RED)."""
    data = _vm_after_one_arm(fake, monkeypatch)
    rc, out, err = _run(capsys, "fetch", "--run", RUN)
    assert rc == 0, err + out
    for n in (ONE["RUN_NAME_N1"], SINGLE):
        assert (evidence.run_dir(n) / "run.log").read_bytes() == (data / "runs" / "606" / n / "run.log").read_bytes()
        assert f"evidence {n}: verified against its MANIFEST.json (stage final)" in out
    assert _state()["fetched"] is True


def test_down_of_a_single_arm_is_the_same_down(fake, capsys):  # noqa: F811
    """down reads only `fetched` and the launcher of the launch's record: a single arm's is downed as a pair's is."""
    srv = _vm(fake, hours=3)
    _record(runs=[ONE["RUN_NAME_N1"]], shape=vm606.SINGLE, launch_dir=SINGLE, launched_at=1.0, fetched=False)
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes")
    assert rc == 2 and "was launched and not fetched" in err
    (srv / RUN).mkdir()
    (srv / RUN / "launch.pid").write_text(f"{2 ** 22 + 7}\n")
    rc, _out, err = _run(capsys, "down", "--run", RUN, "--yes", "--discard-unfetched")
    assert rc == 0, err


def _up_single(capsys, monkeypatch, env_file, run=RUN):
    monkeypatch.setenv("FAKE_BOOTSTRAP", json.dumps(BOOT))
    return _run(capsys, "up", "--env-file", env_file, "--run", run, "--ttl-hours", "15", "--yes", "--poll-s", "0",
                "--zone", ZONE, "--single", "N1")


def test_up_takes_a_single_arm_env_file_and_refuses_one_that_also_names_pairs(fake, capsys, tmp_path,  # noqa: F811
                                                                             monkeypatch):
    rc, _out, err = _up_single(capsys, monkeypatch, _env_file(tmp_path))
    assert rc == 0, err
    assert CHART_CHECKS[-1] == ("0.3.545", ["N1"])
    rc, _out, err = _up_single(capsys, monkeypatch, _env_file(tmp_path, PAIRS="N1:N2"), run="run26")
    assert rc == 2 and "names one run (N1) and PAIRS=N1:N2" in err


def test_status_of_a_single_arm_is_the_same_status(fake, capsys):  # noqa: F811
    srv = _vm(fake, hours=4)
    (srv / RUN).mkdir()
    (srv / RUN / "launch.log").write_text("[07:00:00] run: N1=x; one arm, no pair\n")
    (srv / RUN / "launch.pid").write_text(f"{os.getpid()}\n")
    rc, out, err = _run(capsys, "status", "--run", RUN)
    assert rc == 0 and "one arm, no pair" in out, err


# ─── the started-check reads only this launch's own lines (#1522 review), both shapes ──

def _stale_run_log(r, name):
    """An earlier launch's run.log of the same RUN_NAME, its run budget logged, on the VM."""
    o = r / "home" / ".local" / "share" / "snowplow-bench" / "runs" / "606" / name
    o.mkdir(parents=True, exist_ok=True)
    (o / "run.log").write_text("[05:00:00] run budget: up 1800 + ...; fits TTL_HOURS=9\n")


def test_an_old_run_log_of_the_same_run_name_is_never_this_launchs_start(fake, capsys, tmp_path):  # noqa: F811
    """One arm: its OUT already holds an earlier launch's run budget, and this launch's arm never logs one: not
    started (timeout), never "ok" from the old line (mutation: reading the whole run.log turns this RED)."""
    repo, sha = _repo(tmp_path)
    r = _vm_ready(fake, hours=12)
    _stale_run_log(r, ONE["RUN_NAME_N1"])
    pin, pin_sha = _pin(tmp_path)
    args = _args(tmp_path, repo, sha, pin, pin_sha, env_file=_env_file(tmp_path, STUB_MODE="quiet"))
    args[args.index("--started-timeout-s") + 1] = "4"
    rc, _out, err = _run(capsys, *args)
    assert rc == 2 and "(timeout;" in err, err
    assert _state()["started"] == "timeout"


def test_a_launcher_refusing_on_its_own_log_is_said_at_once_not_waited_out(fake, capsys, tmp_path):  # noqa: F811
    """The launcher refuses into launch.log and has not exited yet: refused, named, before the timeout (mutation:
    not reading launch.log leaves it to the timeout, RED)."""
    repo, sha = _repo(tmp_path)
    r = _vm_ready(fake, hours=12)
    (r / "launch.log").write_text("refusing: an EARLIER launch's refusal: never this one's\n")
    pin, pin_sha = _pin(tmp_path)
    args = _args(tmp_path, repo, sha, pin, pin_sha, env_file=_env_file(tmp_path, STUB_MODE="launcher-refuse"))
    args[args.index("--started-timeout-s") + 1] = "20"
    rc, out, err = _run(capsys, *args)
    assert rc == 2 and _state()["started"] == "refused", out + err
    assert "vm606: launcher: refusing: OUT_N1 already holds a run" in out and "EARLIER" not in out


@pytest.mark.parametrize("mode, started", [("half", "timeout"), ("quiet", "timeout")])
def test_a_pair_is_started_only_once_every_arm_logged_its_budget_in_this_launch(fake, capsys, tmp_path, mode,  # noqa: F811
                                                                                started):
    """X4: one arm of the pair logs its run budget, the other never does (half), or each holds only an earlier
    launch's (quiet, stale run.logs): never "ok" (mutations: ok on one arm; reading the whole run.log)."""
    from tests.test_vm606 import _launch_args, _repo as _pair_repo
    from tests.test_vm606 import _env_file as _pair_env
    repo, sha, _tag = _pair_repo(tmp_path)
    r = _vm_ready(fake, hours=15)
    if mode == "quiet":
        for n in (ENV["RUN_NAME_N1"], ENV["RUN_NAME_N2"]):
            _stale_run_log(r, n)
    pin, pin_sha = _pin(tmp_path)
    args = _launch_args(tmp_path, repo, sha, pin, pin_sha, _pair_env(tmp_path, STUB_MODE=mode))
    args[args.index("--started-timeout-s") + 1] = "4"
    rc, _out, err = _run(capsys, *args)
    assert rc == 2 and _state()["started"] == started, err
