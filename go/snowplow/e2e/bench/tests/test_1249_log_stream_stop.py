"""#1249: stopping the snowplow log stream leaves nothing running.

stop_log_stream signalled the stream loop's children (pkill -TERM -P), then the loop (kill). A loop
already past its stop check could fork its next command in between: `sleep "$LOG_STREAM_RETRY_S"`,
orphaned, outlived the run (CI's run 42413356862 on #1246: two teardown errors, `sleep 5`, seen on
Linux only, where the reaper reads /bin/sleep's environment). It now stops the loop with halt_tree,
which freezes each process before its children are signalled (#1220).

The arm runs run606.sh's OWN halt_tree and stop_log_stream (read from the script) on a loop of the
stream loop's shape, caught past its stop check inside the step before its sleep (the real loop's
`printf ... "$(date +%s)"`). `pkill` and `kill` are wrapped to wait after signalling: that widens
exactly the gap between the two signals, so the old stop loses the race every time, not one run in
many. No cluster, no kubectl.
"""

from __future__ import annotations

import os
import re
import subprocess

import pytest

from tests.test_run606_real_sequence import BENCH_DIR

_HARNESS = r"""
set -u
%(halt_tree)s
%(stop_log_stream)s
S="$OUT/.log-stream"
( set +e
  while [[ ! -e "$S-stop" ]]; do
    true                              # the stream (kubectl logs -f | scrub) ended on its own
    [[ -e "$S-stop" ]] && break       # its stop check: not stopped yet
    x="$(sleep 2)"                    # printf '{"bench_stream":"reconnect",...}' "$(date +%%s)"
    sleep "$TOKEN"                    # sleep "$LOG_STREAM_RETRY_S"
  done ) >/dev/null 2>&1 &
echo "$!" > "$S.pid"
LOG_STREAM_PIDFILE="$S.pid"
sleep 0.7                             # the loop is inside its $(...), past the stop check
pkill() { command pkill "$@"; local rc=$?; sleep "$GAP_S"; return $rc; }
kill() { builtin kill "$@"; local rc=$?; [[ "$1" == "-TERM" ]] && sleep "$GAP_S"; return $rc; }
stop_log_stream
sleep 1
if pgrep -f "sleep $TOKEN" >/dev/null; then echo SURVIVED; command pkill -f "sleep $TOKEN"; else echo CLEAN; fi
"""


def _fn(src: str, name: str) -> str:
    m = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", src, re.S | re.M)
    assert m, f"run606.sh defines {name}"
    return m.group(0)


def test_stopping_the_log_stream_leaves_no_command_of_its_loop_running(tmp_path):
    src = (BENCH_DIR / "run606.sh").read_text()
    script = _HARNESS % {"halt_tree": _fn(src, "halt_tree"), "stop_log_stream": _fn(src, "stop_log_stream")}
    token = f"{700 + os.getpid() % 200}.{abs(hash(str(tmp_path))) % 100000}"
    p = subprocess.run(["bash", "-c", script], env=dict(os.environ, OUT=str(tmp_path), TOKEN=token, GAP_S="1"),
                       capture_output=True, text=True, timeout=120)
    assert p.stdout.strip() == "CLEAN", (p.stdout, p.stderr[-2000:])
    assert (tmp_path / ".log-stream-stop").exists() and not (tmp_path / ".log-stream.pid").exists()


def test_stop_log_stream_stops_its_loop_with_halt_tree():
    """What the arm above runs is what run606.sh's teardown and collect call."""
    src = (BENCH_DIR / "run606.sh").read_text()
    body = _fn(src, "stop_log_stream")
    assert 'halt_tree "$pid"' in body and "pkill" not in body
