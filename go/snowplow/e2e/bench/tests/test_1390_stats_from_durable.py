"""#1390 item 2: the pair statistics read a fresh verified download of each arm's durable copy (run606_stats.sh,
through the same custlat CLI), never the VM's own run directory, and they re-run offline from that copy alone
(`python3 -m bench.durable rejudge`, `vm606 fetch-evidence`): a VM that disappears after the arms loses no
verdict. Acceptance (Diego): the VM disappears after the arms finish, and the verdict still completes from the
durable copy."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

from bench import durable, vm606
from tests.test_1390_durable import BUCKET, _three_runs  # noqa: F401
from tests.test_run24_aa_replica_nodes_goenv import (DIG_N1, DIG_N2, NAMES, ROOM_FOR_THREE, SPEC, _copies_out,
                                                    _evaluate_dir, _launch_env, _steady_x, pairbed,  # noqa: F401
                                                    steady_of)  # noqa: F401


def _launch_id(tmp: Path) -> str:
    return json.loads((tmp / "pair" / "replica-N2-N2B.json").read_text())["in_run"]["launch"]


def test_the_statistics_read_the_verified_download_never_the_vms_directory(pairbed, steady_of):  # noqa: F811
    """N1's download does not verify (a flipped byte): its pair is INVALID for want of a durable copy, although
    N1's own run directory is intact on this machine; the replica (N2, N2B) is computed from its copies."""
    p, fake, tmp = _three_runs(pairbed, steady_of, FAKE_GCS_CORRUPT="N1/verdict.json")
    log = (tmp / "pair" / "pair.log").read_text()
    assert "EVIDENCE NOT DURABLE (run N1): its durable copy could not be downloaded and verified" in log, log[-3000:]
    assert f"run N2: durable copy downloaded and verified: {tmp / 'pair.durable' / 'N2'}" in log
    v = json.loads((tmp / "pair" / "paired-verdict-N1-N2.json").read_text())
    assert v["verdict"] == "INVALID" and any("run N1 has no verified durable copy" in r for r in v["invalid_reasons"])
    assert (tmp / "N1" / "verdict.json").is_file()                  # the run's own directory was there all along
    rep = json.loads((tmp / "pair" / "replica-N2-N2B.json").read_text())
    assert rep["in_run"]["usable"] is True
    assert p.returncode == 5                                         # the evidence is not safe


def test_a_vm_lost_after_the_arms_loses_no_verdict(pairbed, steady_of, tmp_path):  # noqa: F811
    """The launcher is killed as its statistics start and every directory on its machine is removed (the VM
    deleted itself); the verdicts are computed offline from the bucket alone, and they are the verdicts an
    uninterrupted launch of the same runs gives."""
    repo, fake, env, tmp = pairbed
    _copies_out(repo)
    _evaluate_dir(fake / f"{NAMES['N1']}.out", DIG_N1)
    _evaluate_dir(fake / f"{NAMES['N2']}.out", DIG_N2, scale=_steady_x(steady_of, 2.5))
    _evaluate_dir(fake / f"{NAMES['N2B']}.out", DIG_N2, scale=_steady_x(steady_of, 0.9))
    e = _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE)
    proc = subprocess.Popen(["bash", str(repo / "run606_pair.sh")], env=e, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    log = tmp / "pair" / "pair.log"
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and "statistics: replica N2:N2B started" not in (
            log.read_text() if log.exists() else ""):
        time.sleep(0.05)
    os.killpg(proc.pid, signal.SIGKILL)                                # the VM is gone, mid-statistics
    proc.wait()
    text = log.read_text()
    assert "statistics: replica N2:N2B started" in text
    launch = next(ln.split("/runs/", 1)[1].split("/", 1)[0] for ln in text.splitlines()
                  if "durable copy verified: gs://" in ln)
    for d in ("N1", "N2", "N2B", "pair", "pair.durable"):                # nothing of the machine survives
        shutil.rmtree(tmp / d, ignore_errors=True)
    out = tmp_path / "offline"
    r = subprocess.run(["python3", "-m", "bench.durable", "rejudge", "--bucket", BUCKET, "--launch", launch,
                        "--dest", str(out)], env=e, capture_output=True, text=True, timeout=300)
    assert r.returncode in (0, 1, 4), r.stdout[-2000:] + r.stderr[-2000:]
    got = json.loads((out / "pair" / "paired-verdict-N1-N2.json").read_text())
    rep = json.loads((out / "pair" / "replica-N2-N2B.json").read_text())
    assert rep["in_run"]["usable"] is True and rep["in_run"]["launch"] == launch
    olog = (out / "pair" / "pair.log").read_text()
    assert "statistics: replica N2:N2B ended exit=0" in olog and "statistics: pair N1:N2 ended" in olog
    # the control: the same runs, launched to the end
    shutil.rmtree(fake / "gcs", ignore_errors=True)
    for d in ("N1", "N2", "N2B", "pair", "pair.durable"):
        shutil.rmtree(tmp / d, ignore_errors=True)
    shutil.rmtree(tmp / "bench-evidence", ignore_errors=True)          # the killed launch's published evidence
    c = subprocess.run(["bash", str(repo / "run606_pair.sh")], env=e, capture_output=True, text=True, timeout=300)
    assert c.returncode == 0, c.stderr[-2000:]
    want = json.loads((tmp / "pair" / "paired-verdict-N1-N2.json").read_text())
    assert got["verdict"] == want["verdict"]
    assert got["checks"] and [(x["check"], x["result"], x.get("value")) for x in got["checks"]] == \
        [(x["check"], x["result"], x.get("value")) for x in want["checks"]]
    assert [x["line"] for x in got["cells"]] == [x["line"] for x in want["cells"]]


def test_vm606_fetch_evidence_re_judges_from_the_bucket(monkeypatch, capsys, tmp_path):
    seen = {}

    def fake_rejudge(bucket, launch, dest, say=print):
        seen.update(bucket=bucket, launch=launch, dest=dest)
        return 0
    monkeypatch.setattr(durable, "rejudge", fake_rejudge)
    assert vm606.main(["fetch-evidence", "--launch", "pair-x-20261113T000000Z-1", "--dest", str(tmp_path)]) == 0
    assert seen == {"bucket": BUCKET, "launch": "pair-x-20261113T000000Z-1", "dest": str(tmp_path)}


def test_a_re_judge_without_the_launchs_record_judges_nothing(tmp_path, monkeypatch):
    from tests.test_1390_durable import _fake_gcloud
    _fake_gcloud(tmp_path)
    (tmp_path / "fake").mkdir()
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE", str(tmp_path / "fake"))
    lines = []
    assert durable.rejudge(BUCKET, "pair-never", str(tmp_path / "d"), say=lines.append) == 2
    assert any("no verified copy of plan" in ln for ln in lines)
    assert not (tmp_path / "d" / "pair").exists()
