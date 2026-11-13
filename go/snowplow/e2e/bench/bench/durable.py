"""#1390: a launch's evidence leaves the orchestrator VM, per arm, before any pair statistics start.

Run 26e lost its paired verdict: its VM deleted itself at its --termination-time, with every arm's run directory
on its boot disk, while the replica statistics still ran. Diego's ruling (2026-11-13): each arm's run directory
is copied to a Cloud Storage bucket the bench ensures (bench.vm606 ensure_bucket: private, uniform access, public
access prevention enforced, 7-day retention, 30-day lifecycle delete), verified, and the statistics run from a
fresh verified download of that copy, so a lost VM loses no verdict.

WHERE. gs://<bucket>/runs/<launch id>/<run dir name>/<path in the run dir>, the launch id being run606_pair.sh's
PAIR_RUN_ID; the pair's own directory (pair.json first) the same way. Beside each copied directory, its manifest
<run dir name>.durable-manifest.json, written LAST: no manifest, no verified copy.

UPLOAD (as the orchestrator SA: storage.objectCreator and storage.objectViewer on that bucket only, expiring
with the run). The manifest is built first, from the bytes on disk: every regular file under the directory, its
sha256, md5 and size. Then one recursive copy (parallel composite uploads off, so every object carries its MD5),
then ONE listing of the prefix (gcloud storage objects list --raw --exhaustive: the API's own objects, no early
stop on an empty page) compared object for object with the manifest: each file present with its size and MD5,
nothing else there. Only then is the manifest uploaded, and listed back the same way. Any step that fails, any
object missing, extra or different, fails the upload (exit 1), naming it.

DOWNLOAD. The manifest object first (none: not a verified copy), then the directory, then every file's sha256 and
size against the manifest, and nothing on disk the manifest does not name.

    python3 -m bench.durable bucket [PROJECT]                                # the bucket's name
    python3 -m bench.durable upload --bucket B --launch L --src DIR          # exit 1 + why
    python3 -m bench.durable download --bucket B --launch L --name N --dest D  # D/N, verified; exit 1 + why
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

from bench import gke

#: the manifest object's suffix, beside the directory it describes
MANIFEST_SUFFIX = ".durable-manifest.json"
#: every object an upload writes carries an MD5 the listing compares: gcloud's parallel composite uploads
#: (composite objects have no MD5) are off for these calls only
UPLOAD_ENV = {"CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED": "False"}


def bucket_name(project: str) -> str:
    """One bucket per project, its name the project's (bucket names are global)."""
    return f"snowplow-bench-evidence-{project}"


def prefix(launch: str, name: str) -> str:
    return f"runs/{launch}/{name}"


def _digests(p: Path) -> tuple[str, str, int]:
    sha, md5, n = hashlib.sha256(), hashlib.md5(usedforsecurity=False), 0
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            sha.update(chunk)
            md5.update(chunk)
            n += len(chunk)
    return sha.hexdigest(), base64.b64encode(md5.digest()).decode(), n


def manifest(src: str, *, launch: str, bucket: str) -> dict:
    """Every regular file under `src` (dot files too; a symlink is refused: what it points at is not the run's)."""
    root = Path(src)
    files = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for f in sorted(filenames):
            p = Path(dirpath, f)
            rel = p.relative_to(root).as_posix()
            if p.is_symlink() or not p.is_file():
                raise RuntimeError(f"{rel} in {src} is not a regular file: refusing to copy what it points at")
            sha, md5, n = _digests(p)
            files[rel] = {"sha256": sha, "md5": md5, "size": n}
    if not files:
        raise RuntimeError(f"{src} holds no file: nothing to copy")
    return {"kind": "durable_manifest", "launch": launch, "name": root.name, "bucket": bucket,
            "prefix": prefix(launch, root.name), "files": files}


def _gcloud(argv: list[str], *, env: dict | None = None, timeout: float = 3600) -> tuple[int, str, str]:
    err: list[str] = []
    rc, out = gke.execute(argv, dry_run=False, log=lambda _m: None, stderr_out=err,
                          env=dict(os.environ, **env) if env else None, timeout=timeout)
    return rc, out, err[-1] if err else ""


def listed(bucket: str, pfx: str) -> dict[str, dict]:
    """{object name: object} under gs://bucket/pfx/, one exhaustive raw listing; raises when it did not answer."""
    rc, out, err = _gcloud(["gcloud", "storage", "objects", "list", f"gs://{bucket}/{pfx}/**", "--raw", "--exhaustive",
                            "--format", "json"])
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    if not isinstance(got, list):
        raise RuntimeError(f"gs://{bucket}/{pfx}/ could not be listed (rc={rc}; {gke.error_summary(err)})")
    return {str(o.get("name")): o for o in got if isinstance(o, dict)}


def compare(objects: dict[str, dict], m: dict, *, with_manifest: bool) -> list[str]:
    """What of the listing differs from the manifest: each file missing, of another size or MD5, and each object
    the manifest does not name (the manifest object itself only when `with_manifest`)."""
    pfx = m["prefix"]
    off = []
    for rel, f in sorted(m["files"].items()):
        o = objects.get(f"{pfx}/{rel}")
        if o is None:
            off.append(f"{rel}: not in the bucket")
            continue
        if str(o.get("size")) != str(f["size"]):
            off.append(f"{rel}: {o.get('size')} bytes in the bucket, {f['size']} on disk")
        if o.get("md5Hash") != f["md5"]:
            off.append(f"{rel}: MD5 {o.get('md5Hash')} in the bucket, {f['md5']} on disk")
    want = {f"{pfx}/{rel}" for rel in m["files"]}
    if with_manifest:
        want.add(pfx + MANIFEST_SUFFIX)
    off += [f"{name}: in the bucket, not in the manifest" for name in sorted(set(objects) - want)]
    return off


def upload(bucket: str, launch: str, src: str) -> tuple[bool, str]:
    """(verified, the line saying so or why not): the module doc's UPLOAD."""
    src = os.path.abspath(src)
    name = os.path.basename(src)
    pfx = prefix(launch, name)
    try:
        m = manifest(src, launch=launch, bucket=bucket)
    except (OSError, RuntimeError) as e:
        return False, f"the manifest of {src} could not be built ({e})"
    rc, _out, err = _gcloud(["gcloud", "storage", "cp", "--recursive", "--no-clobber", src, f"gs://{bucket}/runs/{launch}/"],
                            env=UPLOAD_ENV)
    if rc != 0:
        return False, f"copying {src} to gs://{bucket}/{pfx}/ failed (rc={rc}; {gke.error_summary(err)})"
    try:
        off = compare(listed(bucket, f"runs/{launch}/{name}"), m, with_manifest=False)
    except RuntimeError as e:
        return False, str(e)
    if off:
        return False, f"gs://{bucket}/{pfx}/ is not {src}: " + "; ".join(off[:20]) + (
            f" (+{len(off) - 20} more)" if len(off) > 20 else "")
    with tempfile.TemporaryDirectory() as td:
        mp = Path(td, name + MANIFEST_SUFFIX)
        mp.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
        _sha, md5, size = _digests(mp)
        rc, _out, err = _gcloud(["gcloud", "storage", "cp", "--no-clobber", str(mp), f"gs://{bucket}/{pfx}{MANIFEST_SUFFIX}"],
                                env=UPLOAD_ENV)
    if rc != 0:
        return False, f"the manifest of {name} could not be written to the bucket (rc={rc}; {gke.error_summary(err)})"
    try:
        objects = listed(bucket, f"runs/{launch}")
    except RuntimeError as e:
        return False, str(e)
    mo = objects.get(pfx + MANIFEST_SUFFIX)
    if mo is None or str(mo.get("size")) != str(size) or mo.get("md5Hash") != md5:
        return False, f"the manifest of {name} is not in the bucket as written"
    off = compare({k: v for k, v in objects.items() if k.startswith(pfx + "/") or k == pfx + MANIFEST_SUFFIX},
                  m, with_manifest=True)
    if off:
        return False, f"gs://{bucket}/{pfx}/ is not {src}: " + "; ".join(off[:20])
    total = sum(f["size"] for f in m["files"].values())
    return True, (f"durable copy verified: gs://{bucket}/{pfx}/ ({len(m['files'])} files, {total} bytes, each "
                  f"listed back with its size and MD5; manifest {pfx}{MANIFEST_SUFFIX})")


def download(bucket: str, launch: str, name: str, dest: str) -> tuple[bool, str]:
    """(verified, the line or why not): the module doc's DOWNLOAD, into dest/name (which must not exist)."""
    pfx = prefix(launch, name)
    out = Path(dest, name)
    if out.exists():
        return False, f"{out} already exists: a download never lands on an earlier copy"
    Path(dest).mkdir(parents=True, exist_ok=True)
    mpath = Path(dest, name + MANIFEST_SUFFIX)
    rc, _o, err = _gcloud(["gcloud", "storage", "cp", f"gs://{bucket}/{pfx}{MANIFEST_SUFFIX}", str(mpath)])
    if rc != 0:
        return False, (f"gs://{bucket}/{pfx}{MANIFEST_SUFFIX} could not be read (rc={rc}; {gke.error_summary(err)}): "
                       f"no verified copy of {name}")
    try:
        m = json.loads(mpath.read_text())
        files = m["files"]
        assert m.get("prefix") == pfx and isinstance(files, dict) and files
    except (OSError, ValueError, KeyError, AssertionError):
        return False, f"the manifest of {name} in the bucket is not a manifest of {pfx}"
    rc, _o, err = _gcloud(["gcloud", "storage", "cp", "--recursive", f"gs://{bucket}/{pfx}", str(dest) + "/"])
    if rc != 0:
        return False, f"downloading gs://{bucket}/{pfx}/ failed (rc={rc}; {gke.error_summary(err)})"
    off = []
    for rel, f in sorted(files.items()):
        p = out / rel
        if not p.is_file():
            off.append(f"{rel}: not downloaded")
            continue
        sha, _md5, n = _digests(p)
        if n != f["size"] or sha != f["sha256"]:
            off.append(f"{rel}: sha256 {sha[:12]}… ({n} bytes) downloaded, the manifest says {f['sha256'][:12]}… "
                       f"({f['size']} bytes)")
    extra = sorted(p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()
                   and p.relative_to(out).as_posix() not in files)
    off += [f"{rel}: downloaded, not in the manifest" for rel in extra]
    if off:
        return False, f"the download of gs://{bucket}/{pfx}/ is not its manifest: " + "; ".join(off[:20])
    return True, f"durable copy downloaded and verified: {out} ({len(files)} files, sha256 each, from gs://{bucket}/{pfx}/)"


#: the launch's own record beside its runs (run606_pair.sh uploads $PAIR_OUT/plan/pair.json before any run starts)
PLAN = "plan"
BENCH_DIR = Path(__file__).resolve().parent.parent


def rejudge(bucket: str, launch: str, dest: str, *, say=print) -> int:
    """#1390 item 2: a launch's statistics, offline, from its durable copy alone: its record (plan/pair.json) and
    every run directory downloaded and verified into `dest`, then run606_stats.sh, the very statistics stage the
    launcher runs, into dest/pair. Returns the statistics' exit (the verdicts ranked), 2 when the record itself
    cannot be read. A run whose copy does not verify has no statistics (its pairs INVALID, saying so)."""
    ok, msg = download(bucket, launch, PLAN, dest)
    say(msg)
    if not ok:
        return 2
    try:
        pj = json.loads(Path(dest, PLAN, "pair.json").read_text())
        runs, pairs = pj["runs"], pj["pairs"]
    except (OSError, ValueError, KeyError) as e:
        say(f"the launch's record pair.json is not readable ({type(e).__name__}): nothing to re-judge")
        return 2
    env = dict(os.environ, PAIR_OUT=str(Path(dest, "pair")), PAIR_RUN_ID=launch, STATS_LABELS=" ".join(runs),
               PAIRS=" ".join(f"{p['baseline']}:{p['candidate']}" for p in pairs),
               REPLICA_PAIR=(f"{pj['replica_pair']['first']}:{pj['replica_pair']['second']}"
                             if pj.get("replica_pair") else ""))
    if Path(dest, "pair").exists():
        say(f"{Path(dest, 'pair')} already exists: a re-judge never writes over earlier verdicts")
        return 2
    for label, r in runs.items():
        name = os.path.basename(str(r.get("out") or r["run_name"]).rstrip("/"))
        ok, msg = download(bucket, launch, name, dest)
        say(f"run {label}: {msg}")
        src = str(Path(dest, name)) if ok else ""
        try:
            done = ok and "run complete: " in Path(dest, name, "run.log").read_text()
        except OSError:
            done = False
        env.update({f"SRC_{label}": src, f"EXPECTED_IMAGE_DIGEST_{label}": str(r.get("digest") or ""),
                    f"DONE_{label}": "1" if done else "0"})
    import subprocess
    return subprocess.run(["bash", str(BENCH_DIR / "run606_stats.sh")], env=env, cwd=str(BENCH_DIR)).returncode


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.durable")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("bucket")
    p.add_argument("project", nargs="?", default=gke.DEFAULT_PROJECT)
    p = sub.add_parser("upload")
    p.add_argument("--bucket", required=True)
    p.add_argument("--launch", required=True)
    p.add_argument("--src", required=True)
    p = sub.add_parser("download")
    for f in ("--bucket", "--launch", "--name", "--dest"):
        p.add_argument(f, required=True)
    p = sub.add_parser("rejudge", help="#1390: a launch's statistics, offline, from its durable copy")
    p.add_argument("--bucket", default=None, help=f"default: the project's ({bucket_name('<project>')})")
    p.add_argument("--project", default=gke.DEFAULT_PROJECT)
    p.add_argument("--launch", required=True, help="the launch id (run606_pair.sh's PAIR_RUN_ID)")
    p.add_argument("--dest", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "bucket":
        print(bucket_name(a.project))
        return 0
    if a.cmd == "rejudge":
        return rejudge(a.bucket or bucket_name(a.project), a.launch, a.dest)
    ok, msg = upload(a.bucket, a.launch, a.src) if a.cmd == "upload" else download(a.bucket, a.launch, a.name, a.dest)
    print(msg, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
