"""#1194: a paid run is not finished until its evidence is safe.

The reboot of 2026-11-06 wiped /tmp/snowplow-runs/606 (run 21's verdicts, summaries, logs and every
earlier attempt) and the pull token beside it. These arms hold what replaces that:
  - the evidence is on disk, whole and verified, BEFORE the cluster delete is issued;
  - a copy that fails or hangs is loud, is never success, and never holds gke-down;
  - a directory an earlier run left is never written over;
  - nothing a run must keep defaults to a temporary directory, and the pull token's path may not sit
    in one. The token file is never opened here: only its path is judged.

The launcher arms run the real run606.sh under fake gcloud / kubectl / helm (no cloud, no cluster),
and the real run606_pair.sh over a stand-in run606.sh. tests/conftest.py sets
BENCH_ALLOW_VOLATILE_PATHS, because tmp_path sits in a temporary directory; `_real_rules` removes
it for the arms that judge the refusal itself.
"""

from __future__ import annotations

import errno
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from bench import evidence, ghcr_pull
from tests.test_run15_customer_latency import _launch, _verdicts, pairbed, va  # noqa: F401  (fixtures)
from tests.test_run606_real_sequence import BENCH_DIR, RUN_NAME, _calls, harness  # noqa: F401  (fixture)

needs_permissions = pytest.mark.skipif(os.geteuid() == 0, reason="root reads and writes past file modes")


def _run_dir(tmp: Path, files: dict[str, bytes] | None = None) -> Path:
    d = tmp / "run"
    for rel, data in (files or {"run.log": b"log\n", "verdict.json": b'{"verdict": "PASS"}'}).items():
        (d / rel).parent.mkdir(parents=True, exist_ok=True)
        (d / rel).write_bytes(data)
    return d


def _tree(d: Path) -> dict[str, bytes]:
    return {p.relative_to(d).as_posix(): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}


# ─── where the files live ─────────────────────────────────────────────────

def _real_rules(monkeypatch):
    monkeypatch.delenv(evidence.ALLOW_VOLATILE_ENV, raising=False)


def test_both_trees_default_under_home_and_not_a_temporary_directory(monkeypatch):
    _real_rules(monkeypatch)
    for k in ("BENCH_RUNS_DIR", "BENCH_EVIDENCE_DIR", "XDG_DATA_HOME"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("HOME", "/Users/someone")
    share = Path("/Users/someone/.local/share/snowplow-bench")
    assert evidence.run_dir("r") == share / "runs" / "606" / "r"
    assert evidence.evidence_dir("r") == share / "evidence" / "606" / "r"
    assert evidence.volatile_root_of(evidence.run_dir("r")) is None
    assert evidence.volatile_root_of(evidence.evidence_dir("r")) is None
    monkeypatch.setenv("XDG_DATA_HOME", "/data")
    assert evidence.runs_root() == Path("/data/snowplow-bench/runs")
    monkeypatch.setenv("BENCH_EVIDENCE_DIR", "/Volumes/synced/evidence")
    assert evidence.evidence_dir("r") == Path("/Volumes/synced/evidence/606/r")


@pytest.mark.parametrize("path", ["/tmp/snowplow-runs/606/x", "/tmp", "/var/tmp/x", "/dev/shm/x",
                                  os.path.join(tempfile.gettempdir(), "x"),
                                  # what /tmp resolves to on macOS; on Linux, a directory like any other
                                  *(["/private/tmp/x"] if sys.platform == "darwin" else [])])
def test_a_temporary_directory_is_recognised_by_its_real_path(monkeypatch, path):
    _real_rules(monkeypatch)
    assert evidence.volatile_root_of(path) is not None


@pytest.mark.parametrize("path", ["/Users/someone/x", "/home/someone/.config/snowplow-bench/ghcr-pull.env",
                                  "/tmpfoo/x", "/Volumes/synced/x"])
def test_a_directory_that_only_looks_temporary_is_not(monkeypatch, path):
    _real_rules(monkeypatch)
    assert evidence.volatile_root_of(path) is None


def _sources() -> list[Path]:
    """Every launcher and bench module; the tests name /tmp on purpose."""
    return [p for p in BENCH_DIR.rglob("*") if p.suffix in (".py", ".sh")
            and not {"tests", "__pycache__"} & set(p.relative_to(BENCH_DIR).parts)]


# The lock guards this boot's processes, and evidence.py names the directories it refuses.
_ALLOWED_TMP = {"bench/cli.py": {'_RUN_LOCK_DEFAULT = "/tmp/snowplow-bench.lock"'},
                "bench/evidence.py": {'VOLATILE_DIRS = ("/tmp", "/var/tmp", "/var/folders", "/run", "/dev/shm")'}}


def test_no_bench_source_defaults_a_file_to_a_temporary_directory():
    """A quoted path that begins with /tmp, in every launcher and module."""
    quoted = re.compile(r"""["'][^"'\n]{0,8}/tmp[/"']""")
    sources = _sources()
    assert len(sources) > 20
    found = {}
    for p in sources:
        for line in p.read_text().splitlines():
            if quoted.search(line) and line.strip() not in _ALLOWED_TMP.get(p.relative_to(BENCH_DIR).as_posix(), ()):
                found.setdefault(p.relative_to(BENCH_DIR).as_posix(), []).append(line.strip())
    assert not found
    assert "/tmp/snowplow-runs" not in (BENCH_DIR / "run606.sh").read_text()
    assert "/tmp/snowplow-runs" not in (BENCH_DIR / "run606_pair.sh").read_text()


# ─── the copy ─────────────────────────────────────────────────────────────

def test_publish_copies_every_file_and_records_what_it_read(tmp_path):
    files = {"run.log": b"log\n", "verdict.json": b"{}", "k6/k6.json.gz": bytes(range(256)) * 9000,
             "diagnostics/deep/bundle.txt": b"bundle", "a-file-no-list-ever-named.jsonl": b"new\n", "empty": b""}
    src = _run_dir(tmp_path, files)
    dest = tmp_path / "evidence" / "r"
    m = evidence.publish(src, dest, "id-1", "final", ["run.log", "verdict.json"])
    got = _tree(dest)
    for rel, data in files.items():
        assert got[rel] == data
    assert set(m["files"]) == set(files)
    assert m["bytes"] == sum(len(v) for v in files.values())
    assert m["stage"] == "final" and m["run_id"] == "id-1"
    assert evidence.verify(dest, "final") == []
    assert _tree(src) == files                  # the run directory itself is untouched
    assert not [n for n in got if n.endswith(".part")]


def test_the_password_file_and_a_link_are_named_and_never_copied(tmp_path):
    src = _run_dir(tmp_path)
    (src / ".bench-password").write_text("the bench password")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_text("not the run's")
    (src / "link-file").symlink_to(outside / "secret")
    (src / "link-dir").symlink_to(outside)
    dest = tmp_path / "evidence" / "r"
    m = evidence.publish(src, dest, "id-1", "final")
    assert sorted(m["left_out"]) == [".bench-password", "link-dir", "link-file"]
    assert sorted(_tree(dest)) == sorted(["run.log", "verdict.json", evidence.OWNER, evidence.MANIFEST])


def test_a_published_directory_is_private(tmp_path):
    dest = tmp_path / "evidence" / "r"
    evidence.publish(_run_dir(tmp_path), dest, "id-1", "final")
    assert dest.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize("damage", ["changed", "cut", "gone"])
def test_verify_reads_the_copys_own_bytes(tmp_path, damage):
    dest = tmp_path / "evidence" / "r"
    evidence.publish(_run_dir(tmp_path), dest, "id-1", "final")
    f = dest / "verdict.json"
    data = f.read_bytes()
    if damage == "changed":
        f.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))    # same size: only the hash can see it
    elif damage == "cut":
        f.write_bytes(data[:-1])
    else:
        f.unlink()
    problems = evidence.verify(dest, "final")
    assert len(problems) == 1 and "verdict.json" in problems[0]


def test_verify_refuses_what_never_completed(tmp_path):
    dest = tmp_path / "evidence" / "r"
    assert "no MANIFEST.json" in evidence.verify(dest, "final")[0]
    evidence.publish(_run_dir(tmp_path), dest, "id-1", "before-teardown")
    assert "before-teardown" in evidence.verify(dest, "final")[0]
    assert evidence.verify(dest, "before-teardown") == []
    (dest / evidence.MANIFEST).write_text("{ cut")
    assert "cannot be read" in evidence.verify(dest, "final")[0]
    (dest / evidence.MANIFEST).write_text('{"stage": "final", "files": {}}')   # a manifest of nothing
    assert "no file was published" in evidence.verify(dest, "final")[0]


# ─── a failed copy is loud and is not success ─────────────────────────────

@needs_permissions
def test_a_file_that_cannot_be_read_fails_the_publish_and_every_other_file_is_copied(tmp_path):
    src = _run_dir(tmp_path)
    (src / "aaa-unreadable.log").write_text("x")
    (src / "aaa-unreadable.log").chmod(0)
    dest = tmp_path / "evidence" / "r"
    with pytest.raises(evidence.EvidenceError, match="aaa-unreadable.log"):
        evidence.publish(src, dest, "id-1", "final")
    assert not (dest / evidence.MANIFEST).exists()
    assert (dest / "verdict.json").exists()               # tried past the bad file: run.log sorts after it
    assert evidence.verify(dest, "final")
    (src / "aaa-unreadable.log").chmod(0o600)


@needs_permissions
def test_a_directory_that_cannot_be_listed_fails_the_publish(tmp_path):
    """os.walk skips it silently by default: the k6 stream would be missing from a green publish."""
    src = _run_dir(tmp_path, {"run.log": b"log", "k6/k6.json.gz": b"stream"})
    (src / "k6").chmod(0)
    dest = tmp_path / "evidence" / "r"
    with pytest.raises(evidence.EvidenceError, match="k6"):
        evidence.publish(src, dest, "id-1", "final")
    (src / "k6").chmod(0o700)
    assert not (dest / evidence.MANIFEST).exists()


def test_a_destination_that_refuses_a_file_fails_the_publish(tmp_path):
    src = _run_dir(tmp_path)
    dest = tmp_path / "evidence" / "r"
    evidence.publish(src, dest, "id-1", "before-teardown")
    (dest / "verdict.json").unlink()
    (dest / "verdict.json").mkdir()                       # the rename onto it fails
    (src / "verdict.json").write_bytes(b'{"verdict": "FAIL"}')
    with pytest.raises(evidence.EvidenceError, match="verdict.json"):
        evidence.publish(src, dest, "id-1", "final")
    assert json.loads((dest / evidence.MANIFEST).read_text())["stage"] == "before-teardown"


def test_a_copy_that_lands_wrong_is_caught_before_a_manifest_says_it_landed(tmp_path, monkeypatch):
    real = evidence._copy

    def cuts(src, dst):
        out = real(src, dst)
        if dst.name == "verdict.json":
            dst.write_bytes(dst.read_bytes()[:-1])
        return out

    monkeypatch.setattr(evidence, "_copy", cuts)
    dest = tmp_path / "evidence" / "r"
    with pytest.raises(evidence.EvidenceError, match="verdict.json"):
        evidence.publish(_run_dir(tmp_path), dest, "id-1", "final")
    assert not (dest / evidence.MANIFEST).exists()


def test_a_disk_full_at_the_claim_leaves_no_claim_and_the_same_command_retries(tmp_path, monkeypatch, capsys):
    """The review's blocker 1: the disk fills at the publish's very first write, the claim. The
    printed retry must work once there is space, and so must the publish after gke-down. A claim
    made by mkdir and then its owner file left an empty, ownerless directory that refused it."""
    real_fsync, calls = os.fsync, []

    def full_once(fd):
        calls.append(fd)
        if len(calls) == 1:
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", full_once)
    src = _run_dir(tmp_path)
    dest = tmp_path / "evidence" / "r"
    args = ["publish", "--src", str(src), "--dest", str(dest), "--run-id", "id-1", "--stage", "before-teardown"]
    assert evidence.main(args) == 1                       # a message, not a traceback
    err = capsys.readouterr().err
    assert "No space left on device" in err and "same command can retry" in err
    assert not os.path.lexists(dest)                      # no half claim
    assert os.listdir(dest.parent) == []                  # and no staging left beside it
    assert evidence.main(args) == 0                       # the same command, the space back
    assert evidence.verify(dest, "before-teardown") == []
    assert evidence.main(args[:-1] + ["final"]) == 0      # and the publish after gke-down
    assert evidence.verify(dest, "final") == []


def test_a_required_file_the_run_does_not_hold_fails_the_publish(tmp_path):
    src = _run_dir(tmp_path, {"run.log": b"log"})
    dest = tmp_path / "evidence" / "r"
    with pytest.raises(evidence.EvidenceError, match="verdict.json: required"):
        evidence.publish(src, dest, "id-1", "final", ["run.log", "verdict.json"])
    assert not (dest / evidence.MANIFEST).exists()


def test_a_missing_run_directory_and_an_overlap_fail(tmp_path):
    with pytest.raises(evidence.EvidenceError, match="does not exist"):
        evidence.publish(tmp_path / "none", tmp_path / "dest", "id-1", "final")
    src = _run_dir(tmp_path)
    for dest in (src, src / "evidence", src.parent):
        with pytest.raises(evidence.EvidenceError, match="overlap"):
            evidence.publish(src, dest, "id-1", "final")


def test_publish_refuses_a_temporary_directory_as_the_safe_copy(tmp_path, monkeypatch):
    _real_rules(monkeypatch)
    dest = Path(tempfile.gettempdir()) / f"bench-1194-never-{os.getpid()}"
    src = Path(tempfile.gettempdir()) / f"bench-1194-src-{os.getpid()}"
    src.mkdir()
    try:
        (src / "run.log").write_text("log")
        with pytest.raises(evidence.EvidenceError, match="which the system clears"):
            evidence.publish(src, dest, "id-1", "final")
        assert not dest.exists()
    finally:
        shutil.rmtree(src)


def test_the_cli_says_it_failed_and_exits_non_zero(tmp_path, capsys):
    src = _run_dir(tmp_path, {"run.log": b"log"})
    dest = tmp_path / "evidence" / "r"
    args = ["publish", "--src", str(src), "--dest", str(dest), "--run-id", "i", "--stage", "final"]
    assert evidence.main(args + ["--require", "verdict.json"]) == 1
    assert "verdict.json: required" in capsys.readouterr().err
    assert evidence.main(["verify", "--dest", str(dest), "--stage", "final"]) == 1
    assert evidence.main(args) == 0
    assert "1 files" in capsys.readouterr().out
    assert evidence.main(["verify", "--dest", str(dest), "--stage", "final"]) == 0


# ─── a directory an earlier run left is not overwritten ───────────────────

def test_a_directory_another_run_claimed_is_left_as_it_was(tmp_path):
    dest = tmp_path / "evidence" / "r"
    evidence.publish(_run_dir(tmp_path), dest, "earlier", "final")
    before = _tree(dest)
    mine = tmp_path / "mine"
    mine.mkdir()
    (mine / "verdict.json").write_bytes(b"this run's verdict")
    (mine / "brand-new.log").write_bytes(b"x")
    with pytest.raises(evidence.EvidenceError, match="never overwritten"):
        evidence.publish(mine, dest, "later", "final")
    assert _tree(dest) == before


def test_a_directory_nobody_claimed_is_left_as_it_was(tmp_path):
    dest = tmp_path / "evidence" / "r"
    dest.mkdir(parents=True)
    (dest / "verdict.json").write_bytes(b"put here by hand")
    with pytest.raises(evidence.EvidenceError, match="never overwritten"):
        evidence.publish(_run_dir(tmp_path), dest, "id-1", "final")
    assert _tree(dest) == {"verdict.json": b"put here by hand"}


def test_the_same_run_publishes_again_and_keeps_what_it_published(tmp_path):
    """gke-down's record and the end of run.log land on the second pass; a file the run removed
    between the two stays in the evidence."""
    src = _run_dir(tmp_path, {"run.log": b"one\n", "verdict.json": b"{}", "removed-later": b"kept"})
    dest = tmp_path / "evidence" / "r"
    evidence.publish(src, dest, "id-1", "before-teardown")
    (src / "run.log").write_bytes(b"one\ntwo\n")
    (src / "teardown.json").write_bytes(b"{}")
    (src / "removed-later").unlink()
    m = evidence.publish(src, dest, "id-1", "final")
    got = _tree(dest)
    assert got["run.log"] == b"one\ntwo\n" and got["teardown.json"] == b"{}" and got["removed-later"] == b"kept"
    assert m["stage"] == "final" and evidence.verify(dest, "final") == []


def test_an_empty_directory_holds_nothing_to_overwrite_and_is_claimed(tmp_path):
    """The module doc: a publish refuses a directory that holds anything without this run's claim,
    and takes an empty one; preflight refuses both, before any cloud call."""
    dest = tmp_path / "evidence" / "r"
    dest.mkdir(parents=True)
    with pytest.raises(evidence.EvidenceError, match="never overwritten"):
        evidence.preflight(dest)
    evidence.publish(_run_dir(tmp_path), dest, "id-1", "final")
    assert evidence.verify(dest, "final") == [] and (dest / evidence.OWNER).read_text() == "id-1"


def _staging(parent: Path, owner: str | None, tag: str) -> Path:
    """A claim's staging directory as a kill between staging and rename leaves it."""
    p = parent / f".r.claim-{tag}"
    p.mkdir(parents=True)
    if owner is not None:
        (p / evidence.OWNER).write_text(owner)
    return p


def test_a_claim_sweeps_the_staging_its_own_run_left_and_no_other(tmp_path):
    """This run's leftover (its cap killed a copy mid-claim) goes on its next claim. Another run's,
    and an empty one nobody can attribute, stay: one could be a claim in flight."""
    parent = tmp_path / "evidence"
    mine = _staging(parent, "id-1", "aaaa")
    theirs = _staging(parent, "id-2", "bbbb")
    empty = _staging(parent, None, "cccc")
    other_name = parent / ".rr.claim-dddd"                 # another destination's, though it names id-1
    other_name.mkdir()
    (other_name / evidence.OWNER).write_text("id-1")
    evidence.publish(_run_dir(tmp_path), parent / "r", "id-1", "before-teardown")
    assert not mine.exists()
    assert theirs.exists() and empty.exists() and other_name.exists()
    assert evidence.verify(parent / "r", "before-teardown") == []


def test_preflight_refuses_what_a_publish_would(tmp_path, monkeypatch):
    dest = tmp_path / "evidence" / "r"
    evidence.preflight(dest)
    assert not dest.exists() and os.listdir(dest.parent) == []      # it claims nothing and leaves nothing
    dest.mkdir()
    with pytest.raises(evidence.EvidenceError, match="never overwritten"):
        evidence.preflight(dest)
    a_file = tmp_path / "a-file"
    a_file.write_text("x")
    with pytest.raises(evidence.EvidenceError, match="cannot be written"):
        evidence.preflight(a_file / "r")
    _real_rules(monkeypatch)
    with pytest.raises(evidence.EvidenceError, match="which the system clears"):
        evidence.preflight(Path(tempfile.gettempdir()) / "bench-1194" / "r")


# ─── the launcher: evidence before the cluster goes ───────────────────────

def _run606(env, **over):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **over), capture_output=True,
                          text=True, timeout=300)


def _at_delete(tmp: Path, dest: Path) -> str:
    """The fake gcloud's first act on `clusters delete`: copy what the evidence directory holds."""
    return f'cp -R "{dest}" "{tmp}/at-delete"'


def test_the_evidence_is_on_disk_and_verified_before_the_delete_is_issued(harness, run_trees):  # noqa: F811
    """The real run606.sh, every step: the fake gcloud reads the disk when the delete arrives."""
    env, state, _, tmp = harness
    dest = run_trees[1] / "606" / RUN_NAME
    p = _run606(env, FAKE_AT_DELETE=_at_delete(tmp, dest))
    assert p.returncode == 0, p.stderr[-3000:]
    out = tmp / "out"
    at_delete = tmp / "at-delete"
    assert evidence.verify(at_delete, "before-teardown") == []
    assert (at_delete / "verdict.json").read_bytes() == (out / "verdict.json").read_bytes()
    held = set(_tree(at_delete))
    for f in ("verdict.json", "run.log", "command-failures.jsonl", "cluster-shape.jsonl"):
        assert f in held, sorted(held)
    assert "teardown.json" not in held                  # gke-down has not run: the delete really was later
    # and after gke-down: its record and the end of the log, verified
    assert evidence.verify(dest, "final") == []
    final = _tree(dest)
    assert "teardown.json" in final and "cluster" in final["run.log"].decode()
    assert len(final["run.log"]) > len(_tree(at_delete)["run.log"])
    log = (out / "run.log").read_text()
    assert log.index("=== evidence (before-teardown)") < log.index("=== teardown ") < log.index("=== evidence (final)")
    assert "evidence safe: " in log and "EVIDENCE NOT SAFE" not in log
    assert (out / ".evidence-due").exists()             # what the pair reads


def test_a_run_that_failed_mid_measure_publishes_what_it_collected(harness, run_trees):  # noqa: F811
    env, state, _, tmp = harness
    dest = run_trees[1] / "606" / RUN_NAME
    p = _run606(env, BENCH_606_FAIL_AT="collect", FAKE_AT_DELETE=_at_delete(tmp, dest))
    assert p.returncode == 1
    assert evidence.verify(tmp / "at-delete", "before-teardown") == []
    assert (tmp / "at-delete" / "verdict.json").stat().st_size > 0        # teardown's partial collect
    assert evidence.verify(dest, "final") == []


def test_a_run_that_made_no_cluster_owes_no_evidence(harness, run_trees):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_FAIL_AT="reap")
    assert p.returncode == 1
    assert not run_trees[1].joinpath("606", RUN_NAME).exists()
    assert not (tmp / "out" / ".evidence-due").exists()


@needs_permissions
def test_a_failed_copy_is_loud_is_not_success_and_the_cluster_still_goes(harness, run_trees):  # noqa: F811
    """The verdict is a PASS or whatever it is; the run's exit is not 0 and it never says safe."""
    env, state, _, tmp = harness
    root = run_trees[1] / "606"
    root.mkdir(parents=True)
    try:
        p = _run606(env, FAKE_AT_CREATE=f'chmod 500 "{root}"')          # the disk went bad mid-run
    finally:
        root.chmod(0o700)
    assert p.returncode == 1, p.stderr[-2000:]
    assert p.stderr.count("EVIDENCE NOT SAFE") == 2                     # to the operator, both stages
    log = (tmp / "out" / "run.log").read_text()
    assert "EVIDENCE NOT SAFE (before-teardown" in log and "EVIDENCE NOT SAFE (final" in log
    assert "this run is NOT finished" in log and "bench.evidence publish" in log
    assert "evidence safe: " not in log
    assert (tmp / "out" / "verdict.json").exists()                      # OUT keeps what could not be copied
    gcloud = [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]
    assert ["container", "clusters", "delete"] in gcloud
    assert "cluster " + RUN_NAME + " deleted (gke-down OK)" in log


@needs_permissions
def test_a_copy_that_fails_before_teardown_and_succeeds_after_is_still_not_success(harness, run_trees):  # noqa: F811
    """The evidence was not safe when the cluster went: that is the run's outcome, even though the
    publish after gke-down then lands it whole. Only the first stage's exit says so."""
    env, state, _, tmp = harness
    root = run_trees[1] / "606"
    root.mkdir(parents=True)
    try:
        p = _run606(env, FAKE_AT_CREATE=f'chmod 500 "{root}"', FAKE_AT_DELETE=f'chmod 700 "{root}"')
    finally:
        root.chmod(0o700)
    assert p.returncode == 1, p.stderr[-2000:]
    log = (tmp / "out" / "run.log").read_text()
    assert "EVIDENCE NOT SAFE (before-teardown" in log and "EVIDENCE NOT SAFE (final" not in log
    assert "evidence safe: " in log                     # the retry after gke-down worked: no half claim
    assert evidence.verify(root / RUN_NAME, "final") == []


@needs_permissions
def test_a_copy_that_succeeds_before_teardown_and_fails_after_is_not_success(harness, run_trees):  # noqa: F811
    """The mirror: the record of the teardown itself could not be kept. Only the second stage's
    exit says so."""
    env, state, _, tmp = harness
    dest = run_trees[1] / "606" / RUN_NAME
    try:
        p = _run606(env, FAKE_AT_DELETE=f'chmod 500 "{dest}"')
    finally:
        if dest.exists():
            dest.chmod(0o700)
    assert p.returncode == 1, p.stderr[-2000:]
    log = (tmp / "out" / "run.log").read_text()
    assert "EVIDENCE NOT SAFE (final" in log and "EVIDENCE NOT SAFE (before-teardown" not in log
    assert "evidence safe: " not in log
    assert evidence.verify(dest, "before-teardown") == []


def test_a_copy_that_never_returns_is_stopped_at_its_cap_and_never_holds_the_delete(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_HOLD_IN="evidence", EVIDENCE_CAP_S="2")       # the hold is 600 s, past _run606's bound
    assert p.returncode == 1
    log = (tmp / "out" / "run.log").read_text()
    assert "EVIDENCE NOT SAFE (before-teardown, exit 124): stopped at its cap of 2s" in log
    assert log.index("EVIDENCE NOT SAFE (before-teardown") < log.index("=== teardown ")
    assert ["container", "clusters", "delete"] in [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]


def test_a_signal_in_the_teardown_does_not_cost_the_evidence(harness, run_trees):  # noqa: F811
    """A second signal cuts collect and diagnostics short. The copy still runs, shielded, and whole."""
    env, state, _, tmp = harness
    log_p = tmp / "out" / "run.log"
    with open(tmp / "console", "w") as fh:
        p = subprocess.Popen(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_SLOW_LOGS="15"),
                             stdout=subprocess.DEVNULL, stderr=fh)

        def wait_for(text):
            t0 = time.time()
            while time.time() - t0 < 240 and p.poll() is None and not (log_p.exists() and text in log_p.read_text()):
                time.sleep(0.1)
            assert p.poll() is None, f"run606 ended before {text!r}"

        wait_for("=== step collect ===")
        time.sleep(1)
        p.send_signal(signal.SIGTERM)
        wait_for("collecting partial results before teardown")
        p.send_signal(signal.SIGTERM)
        p.wait(timeout=280)
    log = log_p.read_text()
    assert "signal during teardown" in log, log[-2000:]
    assert "=== evidence (before-teardown)" in log
    assert "EVIDENCE NOT SAFE" not in log
    assert evidence.verify(run_trees[1] / "606" / RUN_NAME, "final") == []


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGHUP], ids=["TERM", "HUP"])
def test_a_signal_during_the_copy_does_not_stop_the_copy(harness, run_trees, sig):  # noqa: F811
    """A TERM or a HUP to the whole process group (a closed terminal, a `kill` of the group): the
    copy is shielded like gke-down. Not SIGINT: bash already ignores it in a background job, so an
    INT arm passes with the shield gone (the review's mutation)."""
    env, state, _, tmp = harness
    log_p = tmp / "out" / "run.log"
    with open(tmp / "console", "w") as fh:
        p = subprocess.Popen(["bash", str(BENCH_DIR / "run606.sh")], stdout=subprocess.DEVNULL, stderr=fh,
                             env=dict(env, BENCH_606_HOLD_IN="evidence", BENCH_606_HOLD_S="4"), start_new_session=True)
        t0 = time.time()
        while time.time() - t0 < 240 and p.poll() is None and not (
                log_p.exists() and "=== evidence (before-teardown)" in log_p.read_text()):
            time.sleep(0.1)
        assert p.poll() is None
        time.sleep(1)
        os.killpg(p.pid, sig)
        p.wait(timeout=240)
    log = log_p.read_text()
    assert "EVIDENCE NOT SAFE" not in log, log[-2000:]
    assert evidence.verify(run_trees[1] / "606" / RUN_NAME, "final") == []


def test_a_verdict_the_run_reached_and_then_lost_is_not_safe(harness):  # noqa: F811
    """Evaluate finished, so the publish requires verdict.json: an evidence directory without it
    is not a finished run, however many files it holds."""
    env, state, _, tmp = harness
    p = _run606(env, FAKE_VERDICT_LOST="1")
    assert p.returncode == 1, p.stderr[-2000:]
    log = (tmp / "out" / "run.log").read_text()
    assert "verdict.json: required" in log and "EVIDENCE NOT SAFE (before-teardown" in log
    assert "evidence safe: " not in log


# The step a stopped collect is: commands run one after another, so when one dies the next starts.
# `kill` is wrapped to wait GAP_S after each TERM, which widens exactly the gap a parent has between
# its child dying and its own TERM arriving (the review's harness). halt_tree is run606.sh's own,
# read from the script.
_HALT_HARNESS = r"""
set -u
%(halt_tree)s
kill() { builtin kill "$@"; local rc=$?; [[ "$1" == "-TERM" ]] && sleep "$GAP_S"; return $rc; }
%(step)s
( set +e; step ) &
pid=$!
sleep 1                      # the step's first command is running
halt_tree "$pid"
wait "$pid" 2>/dev/null
sleep 1
if pgrep -f "sleep $TOKEN" >/dev/null; then echo SURVIVED; pkill -f "sleep $TOKEN"; else echo CLEAN; fi
"""
_STEPS = {
    "loop": 'step() { while :; do sleep "$TOKEN"; done; }',
    "nested": 'step() { ( while :; do sleep "$TOKEN"; done ); }',
    "python": 'step() { python3 -c "import subprocess, sys\nwhile True: subprocess.run([\'sleep\', sys.argv[1]])" "$TOKEN"; }',
}


@pytest.mark.parametrize("shape", sorted(_STEPS))
def test_halt_tree_stops_a_step_before_it_can_start_its_next_command(shape):
    src = (BENCH_DIR / "run606.sh").read_text()
    m = re.search(r"^halt_tree\(\) \{\n.*?^\}\n", src, re.S | re.M)
    assert m, "run606.sh defines halt_tree"
    token = f"{700 + os.getpid() % 200}.{abs(hash(shape)) % 100000}"
    p = subprocess.run(["bash", "-c", _HALT_HARNESS % {"halt_tree": m.group(0), "step": _STEPS[shape]}],
                       env=dict(os.environ, TOKEN=token, GAP_S="1"), capture_output=True, text=True, timeout=120)
    assert p.stdout.strip() == "CLEAN", (p.stdout, p.stderr[-2000:])


def test_an_out_left_by_an_earlier_run_is_refused_before_any_cloud_call(harness):  # noqa: F811
    env, state, _, tmp = harness
    out = tmp / "out"
    out.mkdir()
    for name in ("verdict.json", "run.log"):
        (out / name).write_bytes(b"the earlier run's")
        before = _tree(out)
        p = _run606(env)
        assert p.returncode == 2 and f"already holds a run ({name})" in p.stderr
        assert _tree(out) == before
        assert not [c for c in _calls(state) if c["tool"] == "gcloud" and c["argv"][:2] == ["container", "clusters"]]
        (out / name).unlink()


def test_evidence_left_by_an_earlier_run_is_refused_before_any_cloud_call(harness, run_trees):  # noqa: F811
    env, state, _, tmp = harness
    dest = run_trees[1] / "606" / RUN_NAME
    dest.mkdir(parents=True)
    (dest / "verdict.json").write_bytes(b"the earlier run's")
    p = _run606(env)
    assert p.returncode == 2 and "never overwritten" in p.stderr
    assert not [c for c in _calls(state) if c["tool"] == "gcloud" and c["argv"][:2] == ["container", "clusters"]]
    assert _tree(dest) == {"verdict.json": b"the earlier run's"}
    # the dry pass refuses it too: the pair's dry passes are what stop a pair before it starts
    p = _run606(env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 2 and "never overwritten" in p.stderr


def test_a_dry_pass_prints_the_publish_and_copies_nothing(harness, run_trees):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr[-2000:]
    assert not run_trees[1].joinpath("606", RUN_NAME).exists()
    assert "python3 -m bench.evidence publish" in p.stderr


def test_a_run_with_no_out_writes_under_home(harness, monkeypatch):  # noqa: F811
    """run606.sh's own default, with nothing in the environment naming a directory."""
    env, state, _, tmp = harness
    for k in ("OUT", "BENCH_RUNS_DIR", "BENCH_EVIDENCE_DIR", "XDG_DATA_HOME"):
        env.pop(k, None)
    p = _run606(env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr[-2000:]
    assert (tmp / "home/.local/share/snowplow-bench/runs/606" / RUN_NAME / "run.log").is_file()


def test_the_budget_counts_the_copy_before_the_delete(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", EVIDENCE_CAP_S="777")
    line = next(ln for ln in p.stderr.splitlines() if "run budget: " in ln)
    terms = dict(re.findall(r"\+ (\w+) (\d+)(?: |$)", line))
    assert terms["evidence"] == "777"
    def total(**over):
        q = _run606(env, BENCH_606_DRY_RUN="1", **over)
        return int(re.search(r"= (\d+) s;", q.stderr).group(1))
    assert total(EVIDENCE_CAP_S="1000") - total(EVIDENCE_CAP_S="400") == 600


# ─── the pull token's path ────────────────────────────────────────────────

def test_the_default_pull_token_path_survives_a_reboot(monkeypatch):
    _real_rules(monkeypatch)
    assert evidence.volatile_root_of(ghcr_pull.DEFAULT_ENV_FILE) is None
    assert ghcr_pull.DEFAULT_ENV_FILE.endswith("/.config/snowplow-bench/ghcr-pull.env")
    src = (BENCH_DIR / "run606.sh").read_text()
    assert 'GHCR_PULL_ENV_FILE:-$HOME/.config/snowplow-bench/ghcr-pull.env' in src
    for f in _sources():
        assert "snowplow-bench-ghcr-pull.env" not in f.read_text(), f


def test_a_pull_token_path_in_a_temporary_directory_is_refused_by_its_path_alone(harness):  # noqa: F811
    """The path run 21 used. The file need not exist, and the refusal comes first: nothing opens it."""
    env, state, _, tmp = harness
    env = {k: v for k, v in env.items() if k != evidence.ALLOW_VOLATILE_ENV}
    p = _run606(env, BENCH_606_DRY_RUN="1", GHCR_PULL_ENV_FILE="/tmp/snowplow-bench-ghcr-pull.env")
    assert p.returncode == 2
    assert "refusing: GHCR_PULL_ENV_FILE (/tmp/snowplow-bench-ghcr-pull.env)" in p.stderr
    assert "=== step" not in p.stderr


def test_an_evidence_directory_in_a_temporary_directory_is_refused(harness):  # noqa: F811
    """BENCH_RUNS_DIR is not judged: a run in /tmp still publishes to the evidence directory."""
    env, state, _, tmp = harness
    env = {k: v for k, v in env.items() if k != evidence.ALLOW_VOLATILE_ENV}
    # the token's own refusal comes first: the suite's fake token, somewhere that is not temporary
    held = Path(tempfile.mkdtemp(dir=BENCH_DIR, prefix=".test-1194-"))
    try:
        fake_token = held / "ghcr-pull.env"
        shutil.copy(os.environ["GHCR_PULL_ENV_FILE"], fake_token)
        fake_token.chmod(0o600)
        p = _run606(env, BENCH_606_DRY_RUN="1", BENCH_EVIDENCE_DIR="/tmp/bench-1194", GHCR_PULL_ENV_FILE=str(fake_token))
    finally:
        shutil.rmtree(held)
    assert p.returncode == 2, p.stderr[-2000:]
    assert "refusing: the evidence directory (/tmp/bench-1194/606/" in p.stderr


# ─── the pair ─────────────────────────────────────────────────────────────

NA, NB = "snowplow-bench-r15a", "snowplow-bench-r15b"


def _owes(repo: Path, also: str = ""):
    """The stand-in run606.sh, as a real one that reached a cluster: it leaves the mark, and `also`
    runs in its place of publishing."""
    s = (repo / "run606.sh").read_text()
    a = 'echo "=== step measure ===" >> "$OUT/run.log"\n'
    assert s.count(a) == 1
    (repo / "run606.sh").write_text(s.replace(a, a + ': > "$OUT/.evidence-due"\n' + also))
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qam", "owes"], cwd=repo,
                   check=True, capture_output=True)


_PUBLISH = ('python3 -m bench.evidence publish --src "$OUT" --dest "$(python3 -m bench.evidence dest "$RUN_NAME")" '
            '--run-id "$RUN_NAME" --stage final\n')


def test_the_pair_publishes_its_record_before_a_run_starts_and_all_of_it_at_the_end(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    dest = run_trees[1] / "606" / f"pair-{NA}-{NB}"
    _owes(repo, f'[[ -f "{dest}/pair.json" ]] && echo "$RUN_NAME saw pair.json" >> "$FAKE/calls"\n' + _PUBLISH)
    p = _launch(repo, env)
    assert p.returncode == 0, p.stderr[-3000:]
    calls = (fake / "calls").read_text()
    assert f"{NA} saw pair.json" in calls and f"{NB} saw pair.json" in calls
    assert evidence.verify(dest, "final") == []
    got = _tree(dest)
    for f in ("pair.json", "pair.log", "paired-verdict-A-B.json", "A.console", "B.console", "dry-A.console"):
        assert f in got, sorted(got)
    assert got["paired-verdict-A-B.json"] == (tmp / "pair" / "paired-verdict-A-B.json").read_bytes()
    log = (tmp / "pair" / "pair.log").read_text()
    assert "EVIDENCE NOT SAFE" not in log and "run A: evidence final: verified" in log


@needs_permissions
def test_a_pair_whose_own_copy_failed_exits_5_whatever_the_verdict_says(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    dest = run_trees[1] / "606" / f"pair-{NA}-{NB}"
    _owes(repo, f'chmod 500 "{dest}"\n' + _PUBLISH)
    try:
        p = _launch(repo, env)
    finally:
        dest.chmod(0o700)
    assert p.returncode == 5, p.stderr[-3000:]
    assert json.loads((tmp / "pair" / "paired-verdict-A-B.json").read_text())["verdict"] == "PASS"
    log = (tmp / "pair" / "pair.log").read_text()
    assert "EVIDENCE NOT SAFE (pair)" in log and "the launch is NOT finished" in log
    assert evidence.verify(dest, "final")


@needs_permissions
def test_a_pair_that_cannot_publish_its_record_starts_nothing(pairbed, va, run_trees):  # noqa: F811
    """The preflight passed; by the time pair.json exists, its evidence directory cannot be written.
    Refused there: no run starts, so no cluster is made for a pair with no record."""
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    root = run_trees[1] / "606"
    s = (repo / "run606.sh").read_text()
    a = 'if [[ "${BENCH_606_DRY_RUN:-0}" == "1" ]]; then\n'
    assert s.count(a) == 1
    (repo / "run606.sh").write_text(s.replace(a, a + f'  chmod 500 "{root}"\n'))
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qam", "full"], cwd=repo,
                   check=True, capture_output=True)
    try:
        p = _launch(repo, env)
    finally:
        root.chmod(0o700)
    assert p.returncode == 2, p.stderr[-3000:]
    assert "the pair's evidence could not be published" in p.stderr and "nothing was created" in p.stderr
    calls = (fake / "calls").read_text()
    assert " dry=1 " in calls and " dry=0 " not in calls


def test_a_pair_whose_run_published_nothing_exits_5(pairbed, va):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    _owes(repo)
    p = _launch(repo, env)
    assert p.returncode == 5, p.stderr[-3000:]
    log = (tmp / "pair" / "pair.log").read_text()
    assert "EVIDENCE NOT SAFE (run A)" in log and "EVIDENCE NOT SAFE (run B)" in log


def test_a_pair_whose_run_copy_is_damaged_exits_5(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    damage = (r'[[ "$RUN_NAME" == "' + NB + r'" ]] && echo x >> "$(python3 -m bench.evidence dest "$RUN_NAME")/run.log"'
              + "\n")
    _owes(repo, _PUBLISH + damage)
    p = _launch(repo, env)
    assert p.returncode == 5, p.stderr[-3000:]
    log = (tmp / "pair" / "pair.log").read_text()
    assert "EVIDENCE NOT SAFE (run B)" in log and "EVIDENCE NOT SAFE (run A)" not in log


def test_a_run_that_reached_no_cluster_owes_the_pair_nothing(pairbed, va):  # noqa: F811
    """The stand-in leaves no mark: the verdict's own exit stands."""
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    p = _launch(repo, env)
    assert p.returncode == 0, p.stderr[-3000:]


def test_a_failed_verdict_keeps_its_exit_when_the_evidence_is_safe(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va, verdict="INVALID")
    _owes(repo, _PUBLISH)
    p = _launch(repo, env)
    assert p.returncode == 3, p.stderr[-3000:]


def test_a_signalled_pair_publishes_what_it_has(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    (fake / f"{NA}.sleep").write_text("60")
    (fake / f"{NB}.sleep").write_text("60")
    proc = subprocess.Popen(["bash", str(repo / "run606_pair.sh")], env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    t0 = time.time()
    while time.time() - t0 < 60 and "run A pid" not in ((tmp / "pair" / "pair.log").read_text()
                                                        if (tmp / "pair" / "pair.log").exists() else ""):
        time.sleep(0.1)
    time.sleep(0.5)
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 130
    dest = run_trees[1] / "606" / f"pair-{NA}-{NB}"
    assert evidence.verify(dest, "interrupted") == []
    assert "signal: stopping every run" in (dest / "pair.log").read_text()


def test_a_pair_directory_left_by_an_earlier_pair_is_refused(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    pair = tmp / "pair"
    pair.mkdir()
    for name in ("pair.json", "paired-verdict-A-B.json"):
        (pair / name).write_bytes(b"the earlier pair's")
        before = _tree(pair)
        p = _launch(repo, env)
        assert p.returncode == 2 and f"already holds a pair ({name})" in p.stderr
        assert _tree(pair) == before and not (fake / "calls").exists()
        (pair / name).unlink()


def test_pair_evidence_left_by_an_earlier_pair_is_refused(pairbed, va, run_trees):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    dest = run_trees[1] / "606" / f"pair-{NA}-{NB}"
    dest.mkdir(parents=True)
    (dest / "paired-verdict.json").write_bytes(b"the earlier pair's")
    for dry in ("1", "0"):
        p = _launch(repo, env, BENCH_606_DRY_RUN=dry)
        assert p.returncode == 2 and "never overwritten" in p.stderr
    assert not (fake / "calls").exists()
    assert _tree(dest) == {"paired-verdict.json": b"the earlier pair's"}


def test_the_pair_and_its_runs_default_to_the_same_directories(pairbed, va, run_trees):  # noqa: F811
    """run606_pair.sh reads each run's OUT: its default must be run606.sh's own."""
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    e = {k: v for k, v in env.items() if k not in ("PAIR_OUT", "OUT_A", "OUT_B")}
    p = _launch(repo, e)
    assert p.returncode == 0, p.stderr[-3000:]
    assert (run_trees[0] / "606" / NA / "verdict.json").is_file()
    assert (run_trees[0] / "606" / f"pair-{NA}-{NB}" / "paired-verdict-A-B.json").is_file()
    src = (BENCH_DIR / "run606.sh").read_text()
    assert 'OUT="${OUT:-$(python3 -m bench.evidence run-dir "$RUN_NAME")}"' in src
