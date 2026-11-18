"""COLLECT's copies verified against what the pod holds (Diego 2026-11-18, "Fix both bench gaps"; run 29 N2).

Run 29 N2's `kubectl cp k6load-…:/out` failed 11 s in with its stderr discarded and no retry; the k6/viewer.jsonl it
had extracted was cut mid-record (76.4 MB of ~80 MB), and viewer.stream_complete blamed the sidecar, which had exited
rc 0 in its pod. run606's collect_copy lists the pod's files (size, sha256 of exactly that many bytes) before the
copy, keeps kubectl's stderr in the run log, verifies the copy, retries a failed or short one within
COLLECT_TIMEOUT_S, and records it; evaluate's collect.copies names a short or failed copy (INVALID).

The shell cases run run606's own collect_copy against a stand-in kubectl whose "pod" is a local directory.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from bench import collect_copy, k6eval
from tests.test_k6_830 import _write_viewer

BENCH = Path(__file__).resolve().parents[1]


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ─── verify: each file the pod held, in the copy ────────────────────────────────────────────────────────────────────

def _manifest(pod: Path) -> list[dict]:
    """What the in-pod script prints for `pod` (a directory), run with the system's sh."""
    p = subprocess.run(["sh", "-c", collect_copy.SCRIPT, "_", str(pod)], capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    return collect_copy.parse_manifest(p.stdout)


def test_the_in_pod_script_lists_each_files_size_and_sha256(tmp_path):
    pod = tmp_path / "pod"
    (pod / "sub").mkdir(parents=True)
    (pod / "viewer.jsonl").write_bytes(b'{"a": 1}\n' * 1000)
    (pod / "sub" / "x.bin").write_bytes(b"\x00\x01")
    got = {e["path"]: e for e in _manifest(pod)}
    assert got["./viewer.jsonl"] == {"path": "./viewer.jsonl", "size": 9000, "sha256": _sha(b'{"a": 1}\n' * 1000)}
    assert got["./sub/x.bin"]["size"] == 2 and got["./sub/x.bin"]["sha256"] == _sha(b"\x00\x01")
    # a single file: its own path
    one = subprocess.run(["sh", "-c", collect_copy.SCRIPT, "_", str(pod / "viewer.jsonl")], capture_output=True,
                         text=True, timeout=60)
    assert collect_copy.parse_manifest(one.stdout)[0]["size"] == 9000


@pytest.mark.parametrize("local,ok,why", [
    (b'{"a": 1}\n' * 1000, True, None),                                    # whole
    (b'{"a": 1}\n' * 1000 + b'{"a": 2}\n', True, None),                    # appended to after the listing: whole
    (b'{"a": 1}\n' * 900 + b'{"a', False, "copied 8103 of 9000 bytes"),    # run 29 N2: cut mid-record
    (b'{"b": 1}\n' * 1000, False, "its first 9000 bytes differ from the pod's (sha256)"),
    (None, False, "not copied"),
], ids=["whole", "grown-while-copied", "cut-short", "other-bytes", "missing"])
def test_a_copy_is_whole_only_if_every_pod_file_is_there_at_least_as_long_with_the_same_bytes(tmp_path, local, ok,
                                                                                               why):
    """(mutations: comparing sizes only passes other-bytes; requiring equal sizes fails grown-while-copied; skipping a
    missing file passes missing)"""
    pod, copy = tmp_path / "pod", tmp_path / "copy"
    pod.mkdir()
    copy.mkdir()
    (pod / "viewer.jsonl").write_bytes(b'{"a": 1}\n' * 1000)
    if local is not None:
        (copy / "viewer.jsonl").write_bytes(local)
    got = collect_copy.verify(_manifest(pod), str(copy), "dir")
    assert got["ok"] is ok, got
    assert got["short"] == ([] if ok else [f"./viewer.jsonl: {why}"]), got


# ─── evaluate's collect.copies ──────────────────────────────────────────────────────────────────────────────────────

def _rec(outcome, **kw):
    return {"kind": "collect_copy", "what": "k6load's results", "pod": "k6load-zbq4x", "remote": "/out",
            "attempts": kw.pop("attempts", 1), "outcome": outcome, "manifest_ok": outcome != "unverified",
            "files": None, "short": kw.pop("short", []), "errors": kw.pop("errors", []), **kw}


def test_collect_copies_names_a_short_copy_and_passes_a_verified_one():
    c, w = collect_copy.copies_check([_rec("verified"), _rec("short", attempts=3,
                                                              short=["./viewer.jsonl: copied 76400000 of 79900000 bytes"])])
    assert c["result"] == "FAIL" and w == [], c
    assert c["why"] == ("COLLECT failed: k6load's results (/out from k6load-zbq4x): short after 3 attempt(s): "
                        "./viewer.jsonl: copied 76400000 of 79900000 bytes"), c
    assert c["value"] == {"copies": 2, "verified": 1, "short": 1, "failed": 0, "unverified": 0}
    assert collect_copy.copies_check([_rec("verified")])[0]["result"] == "PASS"


def test_an_unverified_copy_is_a_report_warning_never_verified_and_no_records_no_check():
    c, w = collect_copy.copies_check([_rec("unverified")])
    assert c["result"] == "PASS" and c["value"]["unverified"] == 1 and c["value"]["verified"] == 0, c
    assert w and "the pod could not list what it held, so this copy is not verified" in w[0], w
    assert collect_copy.copies_check([]) == (None, [])        # evidence from before the check


def _phases(tmp_path: Path) -> Path:
    from tests.test_1422_cluster_operations import _phases_file
    return _phases_file(tmp_path)


def test_through_the_cli_a_short_copy_makes_the_run_invalid_named(tmp_path):
    cc = tmp_path / "collect-copies.jsonl"
    cc.write_text(json.dumps(_rec("short", attempts=2, short=["./viewer.jsonl: copied 10 of 20 bytes"])) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(_phases(tmp_path)), str(cc)],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    v = json.loads(p.stdout)
    c = next(x for x in v["validity"] if x["check"] == "collect.copies")
    assert c["result"] == "FAIL" and "./viewer.jsonl: copied 10 of 20 bytes" in c["why"], c
    assert v["verdict"] == "INVALID" and any("collect.copies" in r for r in v["invalid_reasons"]), v["invalid_reasons"]


# ─── viewer.stream_complete: a truncated copy, not a sidecar that did not finish ────────────────────────────────────

def test_a_cut_copy_of_a_viewer_that_exited_rc_0_says_truncated_copy(tmp_path):
    """run 29 N2: viewer-exit rc 0 in the pod, the collected viewer.jsonl cut mid-record with no viewer_end: the check
    says a truncated copy (COLLECT), not that the sidecar did not finish (mutation: the old wording)."""
    _write_viewer(tmp_path, rc=0, end=False, cut=True)
    c = k6eval.viewer_checks(k6eval.read_viewer_out(str(tmp_path)), None)[0]
    assert c["result"] == "FAIL", c
    assert "no viewer_end in the collected file although the sidecar exited rc 0: a truncated copy (COLLECT)" in c["why"]
    assert "the collected copy is cut mid-record" in c["why"] and "did not finish" not in c["why"].replace(
        "not a sidecar that did not finish", ""), c


def test_a_viewer_end_without_its_final_newline_is_a_truncated_copy(tmp_path):
    _write_viewer(tmp_path, rc=0)
    f = tmp_path / "viewer.jsonl"
    f.write_text(f.read_text().rstrip("\n"))
    c = k6eval.viewer_checks(k6eval.read_viewer_out(str(tmp_path)), None)[0]
    assert c["result"] == "FAIL" and "lacks its final newline although the sidecar exited rc 0" in c["why"], c


def test_a_sidecar_that_did_not_finish_keeps_its_own_wording(tmp_path):
    """No exit record (the sidecar died before its wrapper recorded one): not a copy problem."""
    _write_viewer(tmp_path, rc=None, end=False)
    c = k6eval.viewer_checks(k6eval.read_viewer_out(str(tmp_path)), None)[0]
    assert "no viewer_end: the sidecar did not finish, or the tail is missing" in c["why"], c


# ─── run606's collect_copy, against a stand-in kubectl ──────────────────────────────────────────────────────────────

def _fn(name: str) -> str:
    src = (BENCH / "run606.sh").read_text()
    i = src.index(f"{name}() {{")
    return src[i:src.index("\n}\n", i) + 3]


KUBECTL = r'''#!/usr/bin/env bash
# a stand-in kubectl: the "pod" is $POD_DIR (/out in it). FAKE_CP: ok | fail-once | short-once | always-fail | short
state="$STATE_DIR/cp-count"
n=$(cat "$state" 2>/dev/null || echo 0)
args=("$@")
while [[ "${args[0]}" == --* || "${args[0]}" == -n ]]; do
  [[ "${args[0]}" == -n ]] && args=("${args[@]:2}") || args=("${args[@]:1}")
done
case "${args[0]}" in
  exec)
    i=0; for a in "${args[@]}"; do i=$((i+1)); [[ "$a" == "--" ]] && break; done
    cmd=("${args[@]:$i}")
    cmd=("${cmd[@]//\/out/$POD_DIR}")
    [[ "${FAKE_EXEC:-ok}" == fail ]] && { echo 'error: unable to upgrade connection: container not found' >&2; exit 1; }
    exec "${cmd[@]}" ;;
  cp)
    echo $((n + 1)) > "$state"
    src="${args[-2]#*:}"; dest="${args[-1]}"
    src="$POD_DIR${src#/out}"
    mode="${FAKE_CP:-ok}"
    if [[ "$mode" == always-fail || ( "$mode" == fail-once && "$n" == 0 ) ]]; then
      echo 'error: unexpected EOF (copying k6load-zbq4x:/out)' >&2; exit 1
    fi
    if [[ -d "$src" ]]; then mkdir -p "$dest"; cp -R "$src/." "$dest/"; else cp "$src" "$dest"; fi
    if [[ "$mode" == short || ( "$mode" == short-once && "$n" == 0 ) ]]; then
      f="$dest"; [[ -d "$dest" ]] && f="$dest/viewer.jsonl"
      head -c 100 "$f" > "$f.cut" && mv "$f.cut" "$f"
    fi
    exit 0 ;;
esac
exit 0
'''


def _run_copy(tmp_path: Path, *, mode="ok", exec_mode="ok", timeout_s=12, kind="dir"):
    out, pod, state, fake = tmp_path / "out", tmp_path / "pod", tmp_path / "state", tmp_path / "bin"
    for d in (out, pod, state, fake):
        d.mkdir()
    (pod / "viewer.jsonl").write_bytes(b'{"kind": "viewer_end"}\n' * 200)
    (pod / "viewer-exit.json").write_text('{"rc": 0}')
    (fake / "kubectl").write_text(KUBECTL)
    (fake / "kubectl").chmod(0o755)
    remote, dest = ("/out", out / "k6") if kind == "dir" else ("/out/viewer.jsonl", out / "viewer.jsonl")
    script = ("set -Eeuo pipefail\nlog() { echo \"$*\" >> \"$OUT/run.log\"; }\n"
              "bounded() { shift; \"$@\"; }\nkerr_line() { cat \"$KERR\"; }\n"
              f"K=(kubectl) COLLECT_TIMEOUT_S={timeout_s} KERR=\"$OUT/.kerr\"\n"
              + _fn("collect_copy")
              + f'\nif collect_copy "k6load\'s results" k6load-zbq4x "" {remote} "{dest}" {kind}; then echo copy-ok; '
                f'else echo copy-not-whole; fi\n')
    env = dict(os.environ, OUT=str(out), POD_DIR=str(pod), STATE_DIR=str(state), FAKE_CP=mode, FAKE_EXEC=exec_mode,
               PATH=f"{fake}{os.pathsep}{os.environ['PATH']}")
    p = subprocess.run(["bash", "-c", script], cwd=BENCH, capture_output=True, text=True, timeout=120, env=env)
    assert p.returncode == 0, p.stderr
    recs = [json.loads(x) for x in (out / "collect-copies.jsonl").read_text().splitlines()]
    log = (out / "run.log").read_text() if (out / "run.log").exists() else ""
    return p.stdout.strip(), recs[-1], log, out


def test_a_whole_copy_is_verified_in_one_attempt(tmp_path):
    said, rec, log, out = _run_copy(tmp_path)
    assert said == "copy-ok" and rec["outcome"] == "verified" and rec["attempts"] == 1, rec
    assert (out / "k6" / "viewer.jsonl").stat().st_size == 23 * 200 and not (out / "k6.part").exists()


def test_a_failed_copy_is_retried_with_kubectls_error_in_the_run_log(tmp_path):
    """(mutations: no retry reads copy-not-whole, failed; stderr discarded leaves the log without kubectl's error)"""
    said, rec, log, _ = _run_copy(tmp_path, mode="fail-once")
    assert said == "copy-ok" and rec["outcome"] == "verified" and rec["attempts"] == 2, rec
    assert "error: unexpected EOF (copying k6load-zbq4x:/out)" in log, log
    assert any("kubectl cp rc=1" in e for e in rec["errors"]), rec


def test_a_short_copy_is_retried_and_verified(tmp_path):
    """The first copy comes out cut (run 29 N2's shape), the second whole (mutation: no verification keeps the cut
    one as verified)."""
    said, rec, log, out = _run_copy(tmp_path, mode="short-once")
    assert said == "copy-ok" and rec["outcome"] == "verified" and rec["attempts"] == 2, rec
    assert "is short (attempt 1): ./viewer.jsonl: copied 100 of 4600 bytes" in log, log
    assert (out / "k6" / "viewer.jsonl").stat().st_size == 4600


def test_a_copy_still_short_at_its_bound_is_recorded_short_and_kept(tmp_path):
    """Every attempt short until COLLECT_TIMEOUT_S: recorded "short" with the file named, what was copied kept (as
    before), the step not failed (mutations: a new bound past COLLECT_TIMEOUT_S, or "verified" for it)."""
    said, rec, log, out = _run_copy(tmp_path, mode="short", timeout_s=8)
    assert said == "copy-not-whole" and rec["outcome"] == "short" and rec["attempts"] >= 2, rec
    assert rec["short"] == ["./viewer.jsonl: copied 100 of 4600 bytes"], rec
    assert (out / "k6" / "viewer.jsonl").stat().st_size == 100
    assert "is short after" in log and "within 8s: kept as copied" in log, log


def test_a_copy_that_always_fails_is_recorded_failed_within_its_bound(tmp_path):
    said, rec, log, _ = _run_copy(tmp_path, mode="always-fail", timeout_s=8)
    assert said == "copy-not-whole" and rec["outcome"] == "failed" and rec["attempts"] >= 2, rec


def test_a_pod_that_cannot_list_its_files_leaves_the_copy_unverified(tmp_path):
    said, rec, log, _ = _run_copy(tmp_path, exec_mode="fail")
    assert said == "copy-ok" and rec["outcome"] == "unverified" and rec["manifest_ok"] is False, rec
    assert "could not list what it holds at /out" in log and "container not found" in log, log


def test_a_single_file_is_copied_and_verified_the_same_way(tmp_path):
    said, rec, _, out = _run_copy(tmp_path, kind="file", mode="short-once")
    assert said == "copy-ok" and rec["outcome"] == "verified" and rec["attempts"] == 2, rec
    assert (out / "viewer.jsonl").stat().st_size == 4600
