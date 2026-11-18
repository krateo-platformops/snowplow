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
    c, w = collect_copy.copies_check([_rec("verified", what="the driver's results"), _rec("short", attempts=3,
                                                              short=["./viewer.jsonl: copied 76400000 of 79900000 bytes"])])
    assert c["result"] == "FAIL" and w == [], c
    assert c["why"] == ("COLLECT failed: k6load's results (/out from k6load-zbq4x): short after 3 attempt(s): "
                        "./viewer.jsonl: copied 76400000 of 79900000 bytes"), c
    assert c["value"] == {"copies": 2, "verified": 1, "short": 1, "failed": 0, "interrupted": 0, "unverified": 0}
    assert collect_copy.copies_check([_rec("verified")])[0]["result"] == "PASS"


def test_an_unverified_copy_leaves_the_check_unmeasured_never_pass_and_no_records_no_check():
    """reviewer-1480's ruling (#1542 N1): a copy whose pod could not list its files could not be checked, which is
    no evidence that it is whole: UNMEASURED, the copy named, never PASS (mutation: unverified read as PASS)."""
    c, w = collect_copy.copies_check([_rec("verified"), _rec("unverified", what="netns606's samples")])
    assert c["result"] == "UNMEASURED" and c["value"]["unverified"] == 1 and c["value"]["verified"] == 1, c
    assert "netns606's samples (/out from k6load-zbq4x): unverified" in c["why"], c
    assert "its pod could not list what it held, so the copy could not be checked" in c["why"] and w == [], c
    # a short copy beside it decides: FAIL
    assert collect_copy.copies_check([_rec("unverified"), _rec("short", what="x")])[0]["result"] == "FAIL"
    assert collect_copy.copies_check([]) == (None, [])        # evidence from before the check


def test_a_failed_copy_fails_the_check_named():
    """run 29 N2's own case (#1542 B1): the copy failed outright. collect.copies FAILs naming it, with kubectl's error
    (mutation C7: FAILED dropped from the bad set reads PASS)."""
    c, _ = collect_copy.copies_check([_rec("verified", what="mem606's heap profiles"),
                                      _rec("failed", attempts=4, errors=["attempt 4: kubectl cp rc=1: error: unexpected "
                                                                         "EOF"])])
    assert c["result"] == "FAIL" and c["value"]["failed"] == 1, c
    assert c["why"] == ("COLLECT failed: k6load's results (/out from k6load-zbq4x): failed after 4 attempt(s): "
                        "attempt 4: kubectl cp rc=1: error: unexpected EOF"), c


def test_each_copy_is_judged_by_its_last_record():
    """A copy's "interrupted" record (written before its first attempt) is replaced by its outcome; one with only
    the interrupted record (a collect cap ended it midway) FAILs, named (mutations: reading every record, or
    leaving interrupted out of the bad set)."""
    first = dict(_rec("interrupted", attempts=0), dest="/o/k6")
    done = dict(_rec("verified"), dest="/o/k6")
    assert collect_copy.copies_check([first, done])[0]["result"] == "PASS"
    c, _ = collect_copy.copies_check([first, done, dict(_rec("interrupted", attempts=0, what="k6browser's results"),
                                                        dest="/o/browser")])
    assert c["result"] == "FAIL" and c["value"]["interrupted"] == 1, c
    assert "k6browser's results (/out from k6load-zbq4x): interrupted after 0 attempt(s)" in c["why"], c


def _phases(tmp_path: Path) -> Path:
    from tests.test_1422_cluster_operations import _phases_file
    return _phases_file(tmp_path)


@pytest.mark.parametrize("outcome", ["failed", "interrupted"])
def test_through_the_cli_a_failed_or_interrupted_copy_makes_the_run_invalid_named(tmp_path, outcome):
    cc = tmp_path / "collect-copies.jsonl"
    cc.write_text(json.dumps(_rec(outcome, attempts=3, errors=["attempt 3: kubectl cp rc=1: error: EOF"])) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(_phases(tmp_path)), str(cc)],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    v = json.loads(p.stdout)
    c = next(x for x in v["validity"] if x["check"] == "collect.copies")
    assert c["result"] == "FAIL" and f"{outcome} after 3 attempt(s)" in c["why"], c
    assert v["verdict"] == "INVALID" and any("collect.copies" in r for r in v["invalid_reasons"]), v["invalid_reasons"]


def test_through_the_cli_an_unverified_copy_leaves_the_check_unmeasured(tmp_path):
    cc = tmp_path / "collect-copies.jsonl"
    cc.write_text(json.dumps(_rec("unverified")) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(_phases(tmp_path)), str(cc)],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    v = json.loads(p.stdout)
    c = next(x for x in v["validity"] if x["check"] == "collect.copies")
    assert c["result"] == "UNMEASURED" and "could not be checked" in c["why"], c


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
    echo "$src" >> "$STATE_DIR/cp-started"
    if [[ "$mode" == fail-then-hang && "$n" != 0 ]] || [[ "$mode" == hang-second-copy && "$src" == *netns* ]]; then
      sleep 60; exit 0
    fi
    if [[ "$mode" == always-fail || ( "$mode" == fail-once && "$n" == 0 ) || ( "$mode" == fail-then-hang ) ]]; then
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



def _real_bounded() -> str:
    """run606's own bounded and kill_tree (the attempt bound under test, never a stand-in)."""
    return 'BOUNDED_PIDS=""\n' + _fn("kill_tree") + _fn("bounded")


@pytest.mark.serial  # #1166/#1174: asserts the copy's elapsed time against COLLECT_TIMEOUT_S on the wall clock
def test_each_attempt_is_bounded_by_what_is_left_of_the_copys_window(tmp_path):
    """reviewer-1480's N2: the first attempt fails at once, the second hangs. The second is cut where the copy's
    COLLECT_TIMEOUT_S window ends (it starts ~5 s in, with ~3 s left of 8), not given a whole COLLECT_TIMEOUT_S of its
    own (mutation C3: every attempt bounded by the whole COLLECT_TIMEOUT_S runs ~13 s)."""
    import time
    out, pod, state, fake = tmp_path / "out", tmp_path / "pod", tmp_path / "state", tmp_path / "bin"
    for d in (out, pod, state, fake):
        d.mkdir()
    (pod / "viewer.jsonl").write_bytes(b"x\n" * 100)
    (fake / "kubectl").write_text(KUBECTL)
    (fake / "kubectl").chmod(0o755)
    script = ("set -Eeuo pipefail\nlog() { echo \"$*\" >> \"$OUT/run.log\"; }\nkerr_line() { cat \"$KERR\"; }\n"
              + _real_bounded() + 'K=(kubectl) COLLECT_TIMEOUT_S=8 KERR="$OUT/.kerr"\n' + _fn("collect_copy")
              + f'collect_copy "k6load\'s results" k6load-zbq4x "" /out "{out / "k6"}" dir || echo copy-not-whole\n')
    env = dict(os.environ, OUT=str(out), POD_DIR=str(pod), STATE_DIR=str(state), FAKE_CP="fail-then-hang",
               PATH=f"{fake}{os.pathsep}{os.environ['PATH']}")
    t0 = time.monotonic()
    p = subprocess.run(["bash", "-c", script], cwd=BENCH, capture_output=True, text=True, timeout=120, env=env)
    took = time.monotonic() - t0
    assert p.returncode == 0 and "copy-not-whole" in p.stdout, p.stderr
    assert took < 8 + 3, f"the copy took {took:.1f} s; its attempts and waits fit in COLLECT_TIMEOUT_S=8 (+ the listing)"
    rec = [json.loads(x) for x in (out / "collect-copies.jsonl").read_text().splitlines()][-1]
    assert rec["outcome"] == "failed" and rec["attempts"] == 2, rec


@pytest.mark.serial  # #1166/#1174: waits on the wall clock for copy 2 to start before the cap
def test_a_cap_during_a_later_copy_strands_nothing_finished_and_names_the_cut_copy(tmp_path):
    """reviewer-1480's N4: collect's cap stops it while copy 2 is in flight. Copy 1 is already in place (verified,
    promoted the moment its attempt ended), copy 2 is recorded "interrupted" (its record written before its first
    attempt), and collect.copies names it (mutations: no interrupted record leaves copy 2 unseen; promotion at the end
    of collect strands copy 1)."""
    import signal
    import time
    out, pod, state, fake = tmp_path / "out", tmp_path / "pod", tmp_path / "state", tmp_path / "bin"
    for d in (out, pod, state, fake):
        d.mkdir()
    (pod / "viewer.jsonl").write_bytes(b'{"kind": "viewer_end"}\n' * 200)
    (pod / "netns.jsonl").write_bytes(b"{}\n" * 10)
    (fake / "kubectl").write_text(KUBECTL)
    (fake / "kubectl").chmod(0o755)
    script = ("set -Eeuo pipefail\nlog() { echo \"$*\" >> \"$OUT/run.log\"; }\nkerr_line() { cat \"$KERR\"; }\n"
              + _real_bounded() + 'K=(kubectl) COLLECT_TIMEOUT_S=60 KERR="$OUT/.kerr"\n' + _fn("collect_copy")
              + f'collect_copy "k6load\'s results" k6load-zbq4x "" /out/viewer.jsonl "{out / "viewer.jsonl"}" file\n'
              + f'collect_copy "the netns606 samples" netns606-0 "" /out/netns.jsonl "{out / "netns.jsonl"}" file\n')
    env = dict(os.environ, OUT=str(out), POD_DIR=str(pod), STATE_DIR=str(state), FAKE_CP="hang-second-copy",
               PATH=f"{fake}{os.pathsep}{os.environ['PATH']}")
    p = subprocess.Popen(["bash", "-c", script], cwd=BENCH, env=env, start_new_session=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.monotonic() + 60
    started = state / "cp-started"
    while not (started.exists() and "netns" in started.read_text()):
        assert time.monotonic() < deadline and p.poll() is None, "copy 2 never started"
        time.sleep(0.1)
    time.sleep(0.5)
    os.killpg(p.pid, signal.SIGTERM)                       # the cap, as `capped` stops collect: its whole tree
    p.wait(timeout=30)
    assert (out / "viewer.jsonl").stat().st_size == 23 * 200, "copy 1 is not in place"
    recs = [json.loads(x) for x in (out / "collect-copies.jsonl").read_text().splitlines()]
    c, _ = collect_copy.copies_check(recs)
    assert c["result"] == "FAIL" and c["value"]["verified"] == 1 and c["value"]["interrupted"] == 1, c
    assert "the netns606 samples (/out/netns.jsonl from netns606-0): interrupted" in c["why"], c
