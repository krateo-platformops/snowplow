"""#1194: where a run's files live, and the step that makes a paid run's evidence safe.

The Mac rebooted on 2026-11-06 and macOS wiped /tmp. That took /tmp/snowplow-runs/606, run 21's
verdicts, summaries and logs and every earlier attempt: only the summary on #606 survived. A paid
run is not finished until its evidence is safe.

WHERE. Both trees sit under the operator's home, beside ~/.config/snowplow-bench (the XDG pair of
that directory):

    ${XDG_DATA_HOME:-~/.local/share}/snowplow-bench/runs       BENCH_RUNS_DIR     the runs write here
    ${XDG_DATA_HOME:-~/.local/share}/snowplow-bench/evidence   BENCH_EVIDENCE_DIR the published copies

Home survives a reboot, and neither macOS's temporary-file cleaning nor a `git clean` or a removed
worktree reaches it. It is ONE MACHINE'S DISK: a lost disk or a replaced machine still loses it.
Off-machine storage is Diego's decision; BENCH_EVIDENCE_DIR takes a synced or mounted directory
when he makes it, with no change here.

WHAT. `publish` copies every regular file of the run directory, not a list of names: a list would
silently miss the next artifact a run learns to write. Each file is copied to a temporary name,
fsynced and renamed, then read back from the destination and compared with the sha256 of the bytes
read from the source. MANIFEST.json is written after that, so a manifest says the whole copy landed. Any
file that could not be copied fails the publish, after every other file was tried; the copy that
landed is kept, and no manifest says it is complete.

NEVER OVER AN EARLIER RUN. The destination is claimed by the run's own id. A publish to a directory
another run claimed, or to one nobody claimed, is refused and touches nothing; the same id may
publish again (the launcher does, after gke-down, for teardown.json and the end of run.log).
`preflight` refuses the same thing before any cloud call.

    python3 -m bench.evidence run-dir NAME               # the default run directory of NAME
    python3 -m bench.evidence dest NAME                  # the evidence directory of NAME
    python3 -m bench.evidence durable-path --what W PATH # exit 2 when PATH sits in a temporary dir
    python3 -m bench.evidence preflight --dest D         # exit 2 + why this run could not publish
    python3 -m bench.evidence publish --src S --dest D --run-id I --stage T [--require F]...
    python3 -m bench.evidence verify --dest D --stage T  # exit 1 + what does not match
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
import time
from pathlib import Path

#: the campaign directory the launchers keep their runs under
CAMPAIGN = "606"
MANIFEST = "MANIFEST.json"
#: who claimed this evidence directory (a run id)
OWNER = ".evidence-owner"
#: the bench users' password, written for a moment and removed (s_classes, fw_local_cleanup): a run
#: stopped inside that moment can leave it, and no copy of it is ever made
NEVER_COPIED = frozenset({".bench-password"})
#: where the system clears files by itself. macOS: /tmp on every boot, $TMPDIR (/var/folders) on
#: its own schedule, and /var/tmp ages out; Linux: /tmp, /run, /dev/shm.
VOLATILE_DIRS = ("/tmp", "/var/tmp", "/var/folders", "/run", "/dev/shm")
#: the test suite's own switch (tests/conftest.py): pytest's tmp_path sits in a temporary directory.
#: Nothing else sets it; run606.sh's refusals below are what an operator meets.
ALLOW_VOLATILE_ENV = "BENCH_ALLOW_VOLATILE_PATHS"
CHUNK = 1024 * 1024
#: how many problems one failure message names
MAX_NAMED = 8


class EvidenceError(Exception):
    """The evidence is not safe; the message says why."""


def data_root() -> Path:
    base = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
    return Path(base) / "snowplow-bench"


def runs_root() -> Path:
    return Path(os.environ.get("BENCH_RUNS_DIR") or data_root() / "runs")


def evidence_root() -> Path:
    return Path(os.environ.get("BENCH_EVIDENCE_DIR") or data_root() / "evidence")


def run_dir(name: str) -> Path:
    return runs_root() / CAMPAIGN / name


def evidence_dir(name: str) -> Path:
    return evidence_root() / CAMPAIGN / name


def volatile_roots() -> list[str]:
    """Each temporary directory by its real path: /tmp is /private/tmp on macOS."""
    return sorted({os.path.realpath(d) for d in (*VOLATILE_DIRS, tempfile.gettempdir())})


def volatile_root_of(path) -> str | None:
    """The temporary directory that holds `path`, read through its symlinks; None when none does.
    Only the path is looked at: the file need not exist, and it is never opened."""
    if os.environ.get(ALLOW_VOLATILE_ENV):
        return None
    real = os.path.realpath(path)
    for root in volatile_roots():
        if real == root or real.startswith(root + os.sep):
            return root
    return None


def _refuse_volatile(path, what: str) -> None:
    root = volatile_root_of(path)
    if root is not None:
        raise EvidenceError(f"{what} ({path}) is under {root}, which the system clears (a reboot wiped run 21's "
                            f"evidence and the pull token, #1194): point it at a directory that survives a reboot")


def preflight(dest) -> None:
    """Before any cloud call: this run could publish to `dest`, and `dest` holds no earlier run."""
    dest = Path(dest)
    _refuse_volatile(dest, "the evidence directory")
    if os.path.lexists(dest):
        raise EvidenceError(f"the evidence directory {dest} already exists: an earlier run's evidence is never "
                            f"overwritten; use a new run name")
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=dest.parent, prefix=".evidence-probe-") as fh:
            fh.write(b"probe")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError as e:
        raise EvidenceError(f"the evidence directory {dest} cannot be written: {e}") from e


def _claim(dest: Path, run_id: str) -> None:
    """Take `dest` for `run_id`, or find it already this run's. mkdir is the atomic step: of two
    runs only one creates the directory, and a directory this call did not create must say it is
    this run's."""
    owner = dest / OWNER
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.mkdir(mode=0o700)
    except FileExistsError:
        try:
            held_by = owner.read_text().strip()
        except OSError:
            held_by = None
        if held_by != run_id:
            raise EvidenceError(f"{dest} already exists and holds {'run ' + held_by if held_by else 'no claim by this run'}: "
                                f"an earlier run's evidence is never overwritten; nothing was copied") from None
        return
    except OSError as e:
        raise EvidenceError(f"the evidence directory {dest} cannot be created: {e}") from e
    _write_durably(owner, run_id.encode())


def _write_durably(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".part")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync_dir(path: Path) -> None:
    """A rename is durable once its directory is; a reboot right after a publish keeps it."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _copy(src: Path, dst: Path) -> tuple[int, str]:
    """Copy `src`; returns what was READ from it. A file still growing is not a mismatch: the entry
    describes these bytes, and the next publish takes the rest."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".part")
    digest, size = hashlib.sha256(), 0
    with open(src, "rb") as r, open(tmp, "wb") as w:
        while chunk := r.read(CHUNK):
            digest.update(chunk)
            size += len(chunk)
            w.write(chunk)
        w.flush()
        os.fsync(w.fileno())
    os.replace(tmp, dst)
    return size, digest.hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _files(src: Path, errors: list[str]):
    """(relative path, full path) of everything under `src`. Symlinks are named, never followed. A
    directory that cannot be listed goes to `errors`: os.walk alone would skip it silently."""
    for top, dirs, names in os.walk(src, onerror=lambda e: errors.append(f"{e.filename}: {e.strerror}")):
        dirs.sort()
        for name in sorted(names + [d for d in dirs if os.path.islink(os.path.join(top, d))]):
            full = Path(top) / name
            yield full.relative_to(src).as_posix(), full


def read_manifest(dest) -> dict | None:
    try:
        return json.loads((Path(dest) / MANIFEST).read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:
        raise EvidenceError(f"{dest}: {MANIFEST} cannot be read: {e}") from e


def verify(dest, stage: str) -> list[str]:
    """What is wrong with `dest` (empty: every file the manifest names is there, byte for byte).
    Reads the destination's own bytes: a file the copy lost, cut or changed cannot pass. A stage
    other than `stage` is a publish that did not complete: the last one is what says the run ended."""
    dest = Path(dest)
    try:
        manifest = read_manifest(dest)
    except EvidenceError as e:
        return [str(e)]
    if manifest is None:
        return [f"{dest}: no {MANIFEST}: the run published nothing, or its publish did not finish"]
    problems = []
    if manifest.get("stage") != stage:
        problems.append(f"{dest}: the last publish that completed is '{manifest.get('stage')}', not '{stage}'")
    return problems + _landed(dest, manifest.get("files") or {})


def _landed(dest: Path, entries: dict) -> list[str]:
    """Each entry's bytes, read back from `dest`."""
    if not entries:
        return [f"{dest}: no file was published"]
    problems = []
    for rel, want in sorted(entries.items()):
        try:
            if _sha256(dest / rel) != want["sha256"]:
                problems.append(f"{rel}: its bytes in {dest} differ from what was read from the run")
        except OSError as e:
            problems.append(f"{rel}: {e}")
    return problems


def publish(src, dest, run_id: str, stage: str, require=()) -> dict:
    """Copy the run directory `src` to `dest` and return the manifest. Raises EvidenceError, and
    writes no manifest, unless every file landed, every `require`d one is among them, and the copy
    reads back whole."""
    src, dest = Path(src), Path(dest)
    if not src.is_dir():
        raise EvidenceError(f"the run directory {src} does not exist")
    real_src, real_dest = os.path.realpath(src), os.path.realpath(dest)
    if real_dest == real_src or real_dest.startswith(real_src + os.sep) or real_src.startswith(real_dest + os.sep):
        raise EvidenceError(f"{src} and {dest} overlap: a directory is no copy of itself")
    _refuse_volatile(dest, "the evidence directory")
    _claim(dest, run_id)
    earlier = read_manifest(dest) or {}
    entries, left_out, errors, touched = dict(earlier.get("files") or {}), [], [], set()
    for rel, full in _files(src, errors):
        try:
            st = full.lstat()
            if full.name in NEVER_COPIED or not stat.S_ISREG(st.st_mode):
                left_out.append(rel)
                continue
            known = entries.get(rel)
            if known and (known["source_size"], known["source_mtime_ns"]) == (st.st_size, st.st_mtime_ns):
                continue                # this run already published these bytes
            size, sha = _copy(full, dest / rel)
            entries[rel] = {"size": size, "sha256": sha, "source_size": st.st_size,
                            "source_mtime_ns": st.st_mtime_ns}
            touched.add((dest / rel).parent)
        except OSError as e:
            errors.append(f"{rel}: {e}")
    errors += [f"{rel}: required, and the run directory does not hold it" for rel in require if rel not in entries]
    errors += _landed(dest, entries)
    if errors:
        raise EvidenceError(_named(errors) + f"; what was copied is in {dest}, without a manifest for this stage")
    try:
        for d in touched:
            _fsync_dir(d)
        manifest = {"run_id": run_id, "stage": stage, "source": str(src), "published_at": time.time(),
                    "bytes": sum(e["size"] for e in entries.values()), "left_out": left_out, "files": entries}
        _write_durably(dest / MANIFEST, json.dumps(manifest, indent=1, sort_keys=True).encode())
    except OSError as e:
        raise EvidenceError(f"{dest}: {e}") from e
    return manifest


def _named(problems: list[str]) -> str:
    more = len(problems) - MAX_NAMED
    return "; ".join(problems[:MAX_NAMED]) + (f"; and {more} more" if more > 0 else "")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.evidence")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run-dir").add_argument("name")
    sub.add_parser("dest").add_argument("name")
    dp = sub.add_parser("durable-path")
    dp.add_argument("--what", required=True)
    dp.add_argument("path")
    sub.add_parser("preflight").add_argument("--dest", required=True)
    pub = sub.add_parser("publish")
    pub.add_argument("--src", required=True)
    pub.add_argument("--dest", required=True)
    pub.add_argument("--run-id", required=True)
    pub.add_argument("--stage", required=True)
    pub.add_argument("--require", action="append", default=[])
    ver = sub.add_parser("verify")
    ver.add_argument("--dest", required=True)
    ver.add_argument("--stage", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "run-dir":
        print(run_dir(a.name))
    elif a.cmd == "dest":
        print(evidence_dir(a.name))
    elif a.cmd in ("durable-path", "preflight"):
        try:
            if a.cmd == "preflight":
                preflight(a.dest)
            else:
                _refuse_volatile(a.path, a.what)
        except EvidenceError as e:
            print(f"refusing: {e}", file=sys.stderr)
            return 2
    elif a.cmd == "publish":
        try:
            m = publish(a.src, a.dest, a.run_id, a.stage, a.require)
        except EvidenceError as e:
            print(e, file=sys.stderr)
            return 1
        print(f"evidence {a.stage}: {len(m['files'])} files, {m['bytes']} bytes, verified, in {a.dest}"
              + (f"; left out: {', '.join(m['left_out'])}" if m["left_out"] else ""))
    else:
        problems = verify(a.dest, a.stage)
        if problems:
            print(_named(problems), file=sys.stderr)
            return 1
        print(f"evidence {a.stage}: verified, in {a.dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
