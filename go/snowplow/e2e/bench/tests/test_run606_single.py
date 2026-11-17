"""run606_single.sh: ONE measuring arm (Diego 2026-11-17, "Add a single-arm VM mode first"), with the steps
run606_pair.sh takes for each of its arms (run606_arm.sh, shared) and none it takes for a comparison: the dry
pass and the quota read before anything is created, the launch's record copied off the machine before the run
starts, the run's directory copied to the evidence bucket, verified and read back once it exits (#1390), the
run's verdict as the exit, 5 when the evidence is not safe. Against the pair's own test bed (a stand-in
run606.sh, the fake region and Cloud Storage), so the arm runs as a pair's arm does."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import time

import pytest

from bench import durable
from tests.test_run15_customer_latency import BENCH, DIG_A, NA, _calls, pairbed  # noqa: F401  (fixture)

BUCKET = durable.bucket_name("operations-dev-krateo-io")


@pytest.fixture
def singlebed(pairbed):  # noqa: F811
    """The pair's bed with the single-arm launcher committed beside it, and the env of ONE arm, A."""
    repo, fake, env, tmp = pairbed
    shutil.copy(BENCH / "run606_single.sh", repo)
    for c in (["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "single"]):
        subprocess.run(["git", *c], cwd=repo, check=True, capture_output=True)
    e = {k: v for k, v in env.items() if not k.endswith("_B") and k != "PAIR_OUT"}
    e.update(RUNS="A", SINGLE_OUT=str(tmp / "single"), SINGLE_POLL_S="0.2")
    return repo, fake, e, tmp


def _single(repo, env, **over):
    e = dict(env, **over)
    for k in [k for k, v in e.items() if v is None]:
        e.pop(k)
    return subprocess.run(["bash", str(repo / "run606_single.sh")], env=e, capture_output=True, text=True,
                          timeout=120, check=False)


def _verdict(fake, v):
    (fake / f"{NA}.verdict.json").write_text(json.dumps({"verdict": v, "invalid_reasons": []}))


def _launch_id(tmp):
    return next(ln.split("/runs/", 1)[1].split("/", 1)[0] for ln in (tmp / "single" / "single.log").read_text()
                .splitlines() if "durable copy verified: gs://" in ln)


@pytest.mark.parametrize("over, why", [
    ({"RUNS": "A B"}, "RUNS names 2 run(s): a single-arm launch runs exactly one"),
    ({"RUNS": None}, "RUNS names 0 run(s)"),
    ({"PAIRS": "A:B"}, "PAIRS is set (A:B): a single arm compares nothing"),
    ({"REPLICA_PAIR": "A:B"}, "REPLICA_PAIR is set (A:B): a single arm has no pair"),
    ({"BENCH_606_REHEARSAL": "1"}, "BENCH_606_REHEARSAL is set (1): a launch never carries it"),
    ({"RUN_NAME": "x"}, "RUN_NAME is per run (set RUN_NAME_A): unset it"),
    ({"RUN_NAME_A": None}, "RUN_NAME_A is required (the run's)"),
    ({"FRONTEND_TAG_A": "1.8.93"}, "FRONTEND_TAG_A and EXPECTED_FRONTEND_DIGEST_A are set together or not at all"),
    ({"K6_SPAN": "steady"}, "the arm runs a pair arm's schedule: K6_SPAN=churn"),
    ({"BENCH_606_GO_A": "20200101-x"}, "BENCH_606_GO_A must be '"),
])
def test_what_one_arm_cannot_be_is_refused_before_anything_runs(singlebed, over, why):
    """Each refusal alone, exit 2, before any dry pass (mutation: dropping any one turns its case RED)."""
    repo, fake, env, tmp = singlebed
    p = _single(repo, env, **over)
    assert p.returncode == 2 and f"refusing: {why}" in p.stderr, p.stderr[-2000:]
    assert _calls(fake) == []


def test_the_dry_pass_runs_the_arms_dry_pass_and_reads_its_footprint_and_creates_nothing(singlebed):
    """The arm's own dry pass, in $SINGLE_OUT/dry-A (a pair's layout: vm606 reads its TTL there), its footprint
    against the region's quotas; nothing runs for real."""
    repo, fake, env, tmp = singlebed
    p = _single(repo, env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr[-3000:]
    assert [c.split()[:2] for c in _calls(fake) if " dry=" in c] == [[NA, "dry=1"]]
    assert all("span=churn driver=k6" in c for c in _calls(fake) if " dry=" in c)
    assert (tmp / "single" / "dry-A" / "footprint.json").is_file()
    log = (tmp / "single" / "single.log").read_text()
    assert "run A: dry pass OK" in log and "DRY: run A would start now" in log and "PAIR" not in log
    assert not (tmp / "single" / "single.json").exists() and not (fake / "gcs").exists()


def test_a_dry_pass_that_did_not_derive_the_footprint_is_refused(singlebed):
    repo, fake, env, tmp = singlebed
    (fake / f"{NA}.lbneed").write_text("none")
    p = _single(repo, env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 2 and "did not derive its LoadBalancer Services and cluster footprint" in p.stderr


def test_the_arm_runs_and_its_evidence_leaves_the_machine_verified_and_read_back(singlebed):
    """The launch's record in the bucket before the run starts; the run's directory once it exits, every file
    listed back against its manifest, then downloaded fresh and verified (#1390, as each arm of a pair)."""
    repo, fake, env, tmp = singlebed
    _verdict(fake, "PASS")
    p = _single(repo, env)
    assert p.returncode == 0, p.stderr[-3000:]
    assert [c.split()[1] for c in _calls(fake) if " dry=" in c] == ["dry=1", "dry=0"]
    rec = json.loads((tmp / "single" / "single.json").read_text())
    assert rec["kind"] == "single" and rec["run"]["run_name"] == NA and rec["run"]["digest"] == DIG_A
    launch = _launch_id(tmp)
    root = fake / "gcs" / BUCKET / "runs" / launch
    assert (root / "plan" / "single.json").is_file() and (root / "plan.durable-manifest.json").is_file()
    m = json.loads((root / "A.durable-manifest.json").read_text())
    arm = tmp / "A"
    assert sorted(m["files"]) == sorted(q.relative_to(arm).as_posix() for q in arm.rglob("*") if q.is_file())
    log = (tmp / "single" / "single.log").read_text()
    copied = f"run A: durable copy verified: gs://{BUCKET}/runs/{launch}/A/"
    read_back = f"run A: durable copy downloaded and verified: {tmp / 'single.durable' / 'A'}"
    assert copied in log and read_back in log, log[-3000:]
    assert log.index("durable copy verified: gs://") < log.index(" pid ") < log.index(copied) < log.index(read_back)
    assert "run A verdict exit=0" in log
    assert (tmp / "single.durable" / "A" / "verdict.json").read_bytes() == (arm / "verdict.json").read_bytes()


@pytest.mark.parametrize("verdict, rc", [("FAIL", 1), ("INVALID", 3), ("INCONCLUSIVE", 4), (None, 1)])
def test_the_exit_is_the_arms_verdict(singlebed, verdict, rc):
    """The pair launcher's codes for one verdict; a run that exited 0 with no verdict decided nothing (1)."""
    repo, fake, env, tmp = singlebed
    if verdict:
        _verdict(fake, verdict)
    p = _single(repo, env)
    assert p.returncode == rc, p.stderr[-3000:]


@pytest.mark.parametrize("fault, why", [
    ({"FAKE_GCS_CORRUPT": "A/verdict.json"}, "EVIDENCE NOT DURABLE (run A): its durable copy could not be downloaded "
                                             "and verified"),
    ({"FAKE_GCS_LIST_DROP": "A/verdict.json"}, "EVIDENCE NOT DURABLE (run A): "),
])
def test_evidence_that_is_not_safely_off_the_machine_is_exit_5_whatever_the_verdict(singlebed, fault, why):
    """A copy that does not list back, or does not read back, verified: the launch is NOT finished (mutation:
    skipping the read-back, or the not-safe exit, turns its case RED)."""
    repo, fake, env, tmp = singlebed
    _verdict(fake, "PASS")
    p = _single(repo, env, **fault)
    log = (tmp / "single" / "single.log").read_text()
    assert p.returncode == 5 and why in log, log[-3000:]
    assert "the launch is NOT finished: its evidence is not safe (exit 5)" in log


def test_a_launch_directory_that_cannot_be_published_at_its_end_is_exit_5(singlebed):
    """X6: the run's copy is safe in the bucket, but the launch's own directory cannot be published at its final
    stage (its evidence destination went read-only during the run): exit 5, whatever the verdict (mutation:
    the final publish's failure not counted turns this RED)."""
    repo, fake, env, tmp = singlebed
    _verdict(fake, "PASS")
    (fake / f"{NA}.sleep").write_text("3")
    dest = subprocess.run(["python3", "-m", "bench.evidence", "dest", f"single-{NA}"], env=env, cwd=repo,
                          capture_output=True, text=True, check=True).stdout.strip()
    proc = subprocess.Popen(["bash", str(repo / "run606_single.sh")], env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    log = tmp / "single" / "single.log"
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and " pid " not in (log.read_text() if log.exists() else ""):
            time.sleep(0.05)
        os.chmod(dest, 0o500)                              # the disk under the launch's evidence went bad
        rc = proc.wait(timeout=90)
    finally:
        os.chmod(dest, 0o700)
    text = log.read_text()
    assert rc == 5 and "EVIDENCE NOT SAFE (launch): " in text, text[-3000:]
    assert "run A: durable copy downloaded and verified" in text      # the run's own copy was fine
    assert "the launch is NOT finished: its evidence is not safe (exit 5)" in text


@pytest.mark.parametrize("held, why", [
    ("A/run.log", "OUT_A ({out}) already holds a run (run.log): use a new RUN_NAME_A or OUT_A"),
    ("A/verdict.json", "OUT_A ({out}) already holds a run (verdict.json)"),
    ("single/single.json", "SINGLE_OUT ({single}) already holds a launch (single.json): it is never written over"),
])
def test_a_directory_that_already_holds_a_run_or_a_launch_is_never_written_over(singlebed, held, why):
    """X10 (#1194 / #1220): the arm's OUT holding a run, or SINGLE_OUT a launch, refuses before anything runs
    (mutation: dropping the guard turns its case RED)."""
    repo, fake, env, tmp = singlebed
    f = tmp / held
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("an earlier run's\n")
    p = _single(repo, env)
    assert p.returncode == 2 and why.format(out=tmp / "A", single=tmp / "single") in p.stderr, p.stderr[-2000:]
    assert _calls(fake) == []
    assert f.read_text() == "an earlier run's\n"


def test_a_launch_whose_record_cannot_leave_the_machine_runs_nothing(singlebed):
    repo, fake, env, tmp = singlebed
    p = _single(repo, env, FAKE_GCS_CP_FAIL="1")
    assert p.returncode == 2 and "the launch's record could not be copied to the evidence bucket" in p.stderr
    assert [c.split()[1] for c in _calls(fake) if " dry=" in c] == ["dry=1"]


def test_a_signal_to_the_launcher_reaches_the_run_which_tears_its_cluster_down(singlebed):
    repo, fake, env, tmp = singlebed
    (fake / f"{NA}.sleep").write_text("60")
    proc = subprocess.Popen(["bash", str(repo / "run606_single.sh")], env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    log = tmp / "single" / "single.log"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and " pid " not in (log.read_text() if log.exists() else ""):
        time.sleep(0.05)
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=60) == 130
    assert f"{NA} signal" in _calls(fake) and any(c.startswith(f"{NA} teardown") for c in _calls(fake))
    assert "signal: stopping run A" in log.read_text()


def test_the_pair_launcher_and_the_single_launcher_share_one_arm_s_steps():
    """runenv / durable_copy / durable_fetch live once (run606_arm.sh), sourced by both launchers."""
    for f in ("run606_pair.sh", "run606_single.sh"):
        src = (BENCH / f).read_text()
        assert 'source "$HERE/run606_arm.sh"' in src and "\nrunenv() {" not in src and "durable_copy() {" not in src
