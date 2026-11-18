"""COLLECT's copies, verified against what the pod holds (Diego 2026-11-18, "Fix both bench gaps").

Run 29 N2 lost the end of k6/viewer.jsonl: one `kubectl cp <pod>:/out <dir>` failed 11 s in, its stderr discarded,
no retry, and whatever had been extracted was kept (76.4 MB of ~80 MB, cut mid-record). In the pod the viewer had
finished cleanly (viewer-exit rc 0), so viewer.stream_complete blamed the sidecar. run606's collect_copy now:

  1. asks the pod, before the copy, what it holds: each file's size and the sha256 of exactly that many bytes
     (`script`, run with `sh -c` in the pod: find, wc, head and sha256sum, in every bench image);
  2. copies with kubectl's stderr kept for the run log (identities redacted by kerr_line);
  3. verifies the copy (`verify`): every file the pod held is there, at least as long, and its first <size> bytes
     hash to the pod's sha256 (a stream still appended to while copied is whole; one cut short is not);
  4. retries a failed or short copy within COLLECT_TIMEOUT_S (the bound one copy already had), and
  5. records the copy (`record`, collect-copies.jsonl): an "interrupted" record before its first attempt, its outcome
     after the last, so a collect cap that ends a copy midway leaves it named; evaluate's collect.copies reads each
     copy's LAST record and names every short, failed or interrupted copy (the run is INVALID: its streams are not
     what the pods held).

A copy the pod could not list (`manifest_ok` false) is recorded "unverified": collect.copies is then UNMEASURED,
naming it (reviewer-1480's ruling on #1542: a copy that could not be checked is no evidence it is whole), never
PASS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

#: the in-pod manifest: one line per regular file, "<size> <sha256 of its first <size> bytes> <path>"; a file still
#: being appended to is hashed over exactly the size read, so the pair always agrees
SCRIPT = r'''r="$1"
if [ -d "$r" ]; then
  cd "$r" || exit 3
  list() { find . -type f; }
else
  list() { printf '%s\n' "$r"; }
fi
list | while IFS= read -r f; do
  s=$(wc -c < "$f") || exit 4
  h=$(head -c "$s" "$f" | sha256sum) || exit 5
  printf '%s %s %s\n' "$s" "${h%% *}" "$f"
done'''

VERIFIED, SHORT, FAILED, UNVERIFIED, INTERRUPTED = "verified", "short", "failed", "unverified", "interrupted"


def parse_manifest(text: str) -> list[dict]:
    """The pod's manifest lines as [{"path", "size", "sha256"}]; a line that does not parse is skipped."""
    out = []
    for ln in text.splitlines():
        size, _, rest = ln.strip().partition(" ")
        sha, _, path = rest.partition(" ")
        if size.isdigit() and len(sha) == 64 and path:
            out.append({"path": path, "size": int(size), "sha256": sha})
    return out


def _local_of(entry: dict, local: str, kind: str) -> str:
    if kind == "file":
        return local
    rel = entry["path"][2:] if entry["path"].startswith("./") else entry["path"]
    return os.path.join(local, rel)


def _prefix_sha256(path: str, n: int) -> str:
    h = hashlib.sha256()
    left = n
    with open(path, "rb") as fh:
        while left > 0:
            b = fh.read(min(left, 1 << 20))
            if not b:
                break
            h.update(b)
            left -= len(b)
    return h.hexdigest()


def verify(manifest: list[dict], local: str, kind: str) -> dict:
    """Each file the pod held, checked in the local copy: there, at least its pod size, and its first <size> bytes
    hashing to the pod's sha256. {"ok": bool, "files": [...], "short": [names of the files that are not]}."""
    files, short = [], []
    for e in manifest:
        p = _local_of(e, local, kind)
        rec = {"path": e["path"], "pod_size": e["size"], "pod_sha256": e["sha256"]}
        try:
            got = os.path.getsize(p)
        except OSError:
            rec.update(copied_size=None, ok=False, why="not copied")
        else:
            rec["copied_size"] = got
            if got < e["size"]:
                rec.update(ok=False, why=f"copied {got} of {e['size']} bytes")
            elif _prefix_sha256(p, e["size"]) != e["sha256"]:
                rec.update(ok=False, why=f"its first {e['size']} bytes differ from the pod's (sha256)")
            else:
                rec["ok"] = True
        files.append(rec)
        if not rec["ok"]:
            short.append(f"{e['path']}: {rec['why']}")
    return {"ok": not short, "files": files, "short": short}


def record(what: str, pod: str, remote: str, attempts: int, outcome: str, manifest_ok: bool,
           checked: dict | None, errors: list[str], dest: str | None = None) -> dict:
    """One collect_copy record (collect-copies.jsonl)."""
    return {"kind": "collect_copy", "what": what, "pod": pod, "remote": remote, "dest": dest, "attempts": attempts,
            "outcome": outcome, "manifest_ok": manifest_ok,
            "files": (checked or {}).get("files"), "short": (checked or {}).get("short") or [],
            "errors": [e for e in errors if e][-5:]}


def copies_check(recs: list[dict]) -> tuple[dict | None, list[str]]:
    """evaluate's collect.copies, a validity item over each copy's LAST record (one copy: what, pod, remote, dest):
    a short, failed or interrupted copy means a stream is not what its pod held: FAIL, the run INVALID, each named;
    else an unverified copy (its pod could not list its files) leaves it UNMEASURED, each named; else PASS. (None, [])
    when the run recorded no copies (evidence from before this check). The second value is kept for report warnings
    (none since the ruling: an unverified copy is the check's own outcome)."""
    if not recs:
        return None, []
    last: dict[tuple, dict] = {}
    for r in recs:
        last[(r.get("what"), r.get("pod"), r.get("remote"), r.get("dest"))] = r
    recs = list(last.values())
    bad = [r for r in recs if r.get("outcome") in (SHORT, FAILED, INTERRUPTED)]
    unverified = [r for r in recs if r.get("outcome") == UNVERIFIED]
    value = {"copies": len(recs), "verified": sum(1 for r in recs if r.get("outcome") == VERIFIED),
             "short": sum(1 for r in recs if r.get("outcome") == SHORT),
             "failed": sum(1 for r in recs if r.get("outcome") == FAILED),
             "interrupted": sum(1 for r in recs if r.get("outcome") == INTERRUPTED),
             "unverified": len(unverified)}

    def name(r: dict) -> str:
        detail = "; ".join(r.get("short") or []) or "; ".join(r.get("errors") or []) or "no detail recorded"
        return (f"{r.get('what')} ({r.get('remote')} from {r.get('pod')}): {r.get('outcome')} after "
                f"{r.get('attempts')} attempt(s): {detail}")

    bar = "every collected copy verified against its pod (each file's size and sha256)"
    if bad:
        return {"check": "collect.copies", "value": value, "bar": bar, "result": "FAIL",
                "why": "COLLECT failed: " + " | ".join(name(r) for r in bad)}, []
    if unverified:
        return {"check": "collect.copies", "value": value, "bar": bar, "result": "UNMEASURED",
                "why": "UNMEASURED: " + " | ".join(f"{name(r)}: its pod could not list what it held, so the copy "
                                                   "could not be checked" for r in unverified)}, []
    return {"check": "collect.copies", "value": value, "bar": bar, "result": "PASS"}, []


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.collect_copy")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("script", help="print the in-pod manifest script")
    v = sub.add_parser("verify", help="verify a local copy against the pod's manifest (exit 3: short)")
    v.add_argument("--manifest", required=True)
    v.add_argument("--local", required=True)
    v.add_argument("--kind", choices=("dir", "file"), required=True)
    r = sub.add_parser("record", help="one collect_copy record on stdout")
    for k in ("what", "pod", "remote", "outcome"):
        r.add_argument(f"--{k}", required=True)
    r.add_argument("--attempts", type=int, required=True)
    r.add_argument("--dest", default=None, help="the local path the copy goes to (one copy: what, pod, remote, dest)")
    r.add_argument("--manifest-ok", choices=("0", "1"), required=True)
    r.add_argument("--checked", help="verify's output (absent: not verified)")
    r.add_argument("--error", action="append", default=[], help="one redacted kubectl error line (repeatable)")
    a = ap.parse_args(argv)
    if a.cmd == "script":
        print(SCRIPT)
        return 0
    if a.cmd == "verify":
        with open(a.manifest, errors="replace") as fh:
            got = verify(parse_manifest(fh.read()), a.local, a.kind)
        json.dump(got, sys.stdout)
        sys.stdout.write("\n")
        return 0 if got["ok"] else 3
    checked = None
    if a.checked:
        try:
            with open(a.checked) as fh:
                checked = json.load(fh)
        except (OSError, ValueError):
            checked = None
    json.dump(record(a.what, a.pod, a.remote, a.attempts, a.outcome, a.manifest_ok == "1", checked, a.error,
                     a.dest), sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
