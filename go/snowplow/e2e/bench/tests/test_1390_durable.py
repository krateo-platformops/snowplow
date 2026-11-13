"""#1390 item 1: each arm's run directory is copied to the evidence bucket and verified (bench/durable.py)
before any pair statistics start. A fake gcloud plays Cloud Storage (tests/fake_gcs.py); a listing that lost
an object answers rc 0 without it, as a real one would, and must fail the copy."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from bench import durable
from tests import fake_gcs

B = "snowplow-bench-evidence-test"
L = "pair-a-b-20261113T000000Z-1"


def _fake_gcloud(tmp: Path) -> None:
    b = tmp / "bin"
    b.mkdir()
    (b / "gcloud").write_text("#!/usr/bin/env python3\nimport json, os, sys\n"
                              "a = sys.argv[1:]\n"
                              "open(os.path.join(os.environ['FAKE'], 'calls.jsonl'), 'a').write(json.dumps("
                              "{'argv': a, 'pcu': os.environ.get('CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED')})"
                              " + chr(10))\n" + fake_gcs.HANDLER + "gcs(a)\nsys.exit(9)\n")
    (b / "gcloud").chmod(0o755)


@pytest.fixture
def gcs(tmp_path, monkeypatch):
    _fake_gcloud(tmp_path)
    (tmp_path / "fake").mkdir()
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE", str(tmp_path / "fake"))
    for k in [k for k in os.environ if k.startswith("FAKE_GCS")]:
        monkeypatch.delenv(k)
    arm = tmp_path / "runs" / "snowplow-bench-r1390a"
    (arm / "k6").mkdir(parents=True)
    (arm / "k6" / "k6.json.gz").write_bytes(os.urandom(5000))
    (arm / "verdict.json").write_text(json.dumps({"verdict": "PASS"}))
    (arm / ".evidence-due").write_text("")
    (arm / "run.log").write_text("run complete: x\n")
    return tmp_path, arm


def _objects(tmp):
    root = tmp / "fake" / "gcs" / B
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()) if root.exists() else []


def test_an_arm_goes_up_verified_and_comes_back_identical(gcs):
    tmp, arm = gcs
    ok, msg = durable.upload(B, L, str(arm))
    assert ok, msg
    pfx = f"runs/{L}/{arm.name}"
    assert f"durable copy verified: gs://{B}/{pfx}/ (4 files" in msg
    objs = _objects(tmp)
    assert f"{pfx}.durable-manifest.json" in objs and f"{pfx}/.evidence-due" in objs and len(objs) == 5
    calls = [json.loads(x) for x in (tmp / "fake" / "calls.jsonl").read_text().splitlines()]
    # the manifest goes up LAST, after the listing that verified the files
    i_list = next(i for i, c in enumerate(calls) if c["argv"][:3] == ["storage", "objects", "list"])
    i_man = next(i for i, c in enumerate(calls) if c["argv"][:2] == ["storage", "cp"] and
                 c["argv"][-1].endswith(".durable-manifest.json"))
    assert i_list < i_man
    assert all(c["pcu"] == "False" for c in calls if c["argv"][:2] == ["storage", "cp"])   # every object has an MD5
    lst = calls[i_list]["argv"]
    assert "--raw" in lst and "--exhaustive" in lst
    ok, msg = durable.download(B, L, arm.name, str(tmp / "dl"))
    assert ok, msg
    for p in arm.rglob("*"):
        if p.is_file():
            assert (tmp / "dl" / arm.name / p.relative_to(arm)).read_bytes() == p.read_bytes()


@pytest.mark.parametrize("env, says", [
    ({"FAKE_GCS_LIST_DROP": "k6/k6.json.gz"}, "k6/k6.json.gz: not in the bucket"),   # a listing short of one object
    ({"FAKE_GCS_LOSE": "verdict.json"}, "verdict.json: not in the bucket"),           # an upload that lost one
    ({"FAKE_GCS_CP_FAIL": "1"}, "failed (rc=1"),
    ({"FAKE_GCS_LIST_FAIL": "1"}, "could not be listed (rc=1"),
])
def test_a_copy_not_verified_fails_and_writes_no_manifest(gcs, monkeypatch, env, says):
    tmp, arm = gcs
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    ok, msg = durable.upload(B, L, str(arm))
    assert not ok and says in msg, msg
    assert not [o for o in _objects(tmp) if o.endswith(".durable-manifest.json")]


def test_an_object_the_manifest_does_not_name_fails_the_copy(gcs):
    tmp, arm = gcs
    stray = tmp / "fake" / "gcs" / B / "runs" / L / arm.name / "planted.txt"
    stray.parent.mkdir(parents=True)
    stray.write_text("not the run's")
    ok, msg = durable.upload(B, L, str(arm))
    assert not ok and f"runs/{L}/{arm.name}/planted.txt: in the bucket, not in the manifest" in msg, msg


def test_a_download_is_verified_against_the_manifest(gcs, monkeypatch):
    tmp, arm = gcs
    assert durable.upload(B, L, str(arm))[0]
    monkeypatch.setenv("FAKE_GCS_CORRUPT", "k6.json.gz")
    ok, msg = durable.download(B, L, arm.name, str(tmp / "dl1"))
    assert not ok and "k6/k6.json.gz: sha256" in msg, msg
    monkeypatch.delenv("FAKE_GCS_CORRUPT")
    ok, msg = durable.download(B, L, arm.name, str(tmp / "dl1"))
    assert not ok and "already exists" in msg                       # never over an earlier copy
    ok, msg = durable.download(B, L, "snowplow-bench-never-uploaded", str(tmp / "dl2"))
    assert not ok and "no verified copy of snowplow-bench-never-uploaded" in msg


def test_a_copy_whose_manifest_never_landed_is_not_a_copy(gcs, monkeypatch):
    tmp, arm = gcs
    monkeypatch.setenv("FAKE_GCS_LOSE", ".durable-manifest.json")
    ok, msg = durable.upload(B, L, str(arm))
    assert not ok and "manifest" in msg
    monkeypatch.delenv("FAKE_GCS_LOSE")
    ok, msg = durable.download(B, L, arm.name, str(tmp / "dl"))
    assert not ok and "no verified copy" in msg


def test_a_symlink_is_refused_before_anything_is_copied(gcs):
    tmp, arm = gcs
    (arm / "link").symlink_to("/etc/hosts")
    ok, msg = durable.upload(B, L, str(arm))
    assert not ok and "link" in msg and "not a regular file" in msg
    assert _objects(tmp) == []


# ─── the launcher: every arm's directory leaves the machine before the statistics ───

from tests.test_run24_aa_replica_nodes_goenv import (DIG_N1, DIG_N2, NAMES, ROOM_FOR_THREE, SPEC,  # noqa: E402
                                                    _copies_out, _evaluate_dir, _launch, _launch_env, _steady_x,
                                                    pairbed, steady_of)  # noqa: F401

BUCKET = durable.bucket_name("operations-dev-krateo-io")


def _three_runs(pairbed, steady_of, **over):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _copies_out(repo)
    _evaluate_dir(fake / f"{NAMES['N1']}.out", DIG_N1)
    _evaluate_dir(fake / f"{NAMES['N2']}.out", DIG_N2, scale=_steady_x(steady_of, 2.5))
    _evaluate_dir(fake / f"{NAMES['N2B']}.out", DIG_N2, scale=_steady_x(steady_of, 0.9))
    p = _launch(repo, _launch_env(env, tmp, SPEC, "N1:N2 N2:N2B", REPLICA_PAIR="N2:N2B", **ROOM_FOR_THREE, **over))
    return p, fake, tmp


def test_the_launcher_copies_its_record_and_each_arm_before_any_statistics(pairbed, steady_of):  # noqa: F811
    p, fake, tmp = _three_runs(pairbed, steady_of)
    assert p.returncode == 0, p.stderr[-3000:]
    log = (tmp / "pair" / "pair.log").read_text()
    launch = json.loads((tmp / "pair" / "replica-N2-N2B.json").read_text())["in_run"]["launch"]
    root = fake / "gcs" / BUCKET / "runs" / launch
    assert (root / "plan" / "pair.json").is_file() and (root / "plan.durable-manifest.json").is_file()
    for x in ("N1", "N2", "N2B"):
        arm = tmp / x                                            # OUT_<x>
        m = json.loads((root / f"{arm.name}.durable-manifest.json").read_text())
        on_disk = sorted(q.relative_to(arm).as_posix() for q in arm.rglob("*") if q.is_file())
        assert sorted(m["files"]) == on_disk and "verdict.json" in on_disk
        line = f"run {x}: durable copy verified: gs://{BUCKET}/runs/{launch}/{arm.name}/"
        assert line in log, log[-3000:]
        assert log.index(line) < log.index("statistics: replica N2:N2B started")
    assert log.index("durable copy verified: gs://") < log.index(" pid ")         # the record, before any run


def test_an_arm_whose_copy_is_not_verified_is_said_loudly_and_the_launch_is_not_safe(pairbed, steady_of):  # noqa: F811
    """A listing short of one object (rc 0): EVIDENCE NOT DURABLE for that run, and the launch exits 5."""
    p, fake, tmp = _three_runs(pairbed, steady_of, FAKE_GCS_LIST_DROP="N2B/verdict.json")
    log = (tmp / "pair" / "pair.log").read_text()
    line = next((ln for ln in log.splitlines() if "EVIDENCE NOT DURABLE (run N2B):" in ln), "")
    assert "/N2B/ is not " in line and ": verdict.json: not in the bucket" in line, log[-3000:]
    assert "EVIDENCE NOT DURABLE (run N1)" not in log and "EVIDENCE NOT DURABLE (run N2)" not in log
    assert "run N1: durable copy verified" in log
    assert p.returncode == 5, p.stderr[-2000:]


def test_a_launch_whose_record_cannot_be_copied_starts_nothing(pairbed, steady_of):  # noqa: F811
    p, fake, tmp = _three_runs(pairbed, steady_of, FAKE_GCS_CP_FAIL="1")
    assert p.returncode == 2 and "the launch's record could not be copied to the evidence bucket" in p.stderr
    assert not any(" dry=0 " in c for c in (fake / "calls").read_text().splitlines())   # no run started
