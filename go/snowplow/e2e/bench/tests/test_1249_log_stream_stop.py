"""#1249: stopping the snowplow log stream leaves nothing running.

stop_log_stream signalled the stream loop's children (pkill -TERM -P), then the loop (kill). A loop
already past its stop check could fork its next command in between: `sleep "$LOG_STREAM_RETRY_S"`,
orphaned, outlived the run (CI's run 42413356862 on #1246). #1249 stopped it with halt_tree.

#1297 moved the stream in-cluster: bench/log_stream.sh runs in the observer pod, and the observer's
supervisor (bench/incluster.py Segment.halt) stops it by creating its stop file, then TERMing (and,
past a grace, KILLing) the loop's whole PROCESS GROUP: every command the loop forked is in that
group, whenever it was forked, so none is left. The arm runs the observer's own stream segment on a
fake kubectl whose `logs -f` ends at once (the loop then sits in its reconnect `sleep`), and checks
that nothing of the loop survives the stop. No cluster.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

from tests.test_run606_real_sequence import BENCH_DIR

FAKE = """#!/bin/sh
case "$*" in
  *" get pods "*) echo "2026-10-21T05:00:00Z snowplow-abc " ;;
  *" logs "*) echo '2026-10-21T06:00:00.5Z {"msg": "x"}' ;;
esac
exit 0
"""


def test_stopping_the_log_stream_leaves_no_command_of_its_loop_running(tmp_path):
    fbin = tmp_path / "bin"
    fbin.mkdir()
    (fbin / "kubectl").write_text(FAKE)
    (fbin / "kubectl").chmod(0o755)
    token = f"{700 + os.getpid() % 200}.{abs(hash(str(tmp_path))) % 100000}"
    out = tmp_path / "out"
    out.mkdir()
    driver = ("import sys; from bench import incluster\n"
              "segs = [s for s in incluster.observe_segments(sys.argv[1], 1, 'app=snowplow') if s.name == 'stream']\n"
              "sys.exit(incluster.supervise(sys.argv[1], segs, 0, poll_s=0.1))\n")
    env = dict(os.environ, PATH=f"{fbin}{os.pathsep}{os.environ['PATH']}", PYTHONPATH=str(BENCH_DIR),
               PW="pw", BENCH_USER_PREFIX="scaleuser-", SNOWPLOW_POD_SELECTOR="app=snowplow",
               LOG_STREAM_RETRY_S=token)
    p = subprocess.Popen([sys.executable, "-c", driver, str(out)], env=env, cwd=BENCH_DIR)
    try:
        deadline = time.time() + 30
        while time.time() < deadline and subprocess.run(["pgrep", "-f", f"sleep {token}"],
                                                        capture_output=True).returncode != 0:
            time.sleep(0.2)
        assert subprocess.run(["pgrep", "-f", f"sleep {token}"], capture_output=True).returncode == 0, \
            "the loop never reached its reconnect sleep"
        (out / "stop").touch()
        p.wait(timeout=60)
        time.sleep(0.5)
        left = subprocess.run(["pgrep", "-f", f"sleep {token}"], capture_output=True).returncode == 0
    finally:
        if p.poll() is None:
            p.kill()
        subprocess.run(["pkill", "-f", f"sleep {token}"], capture_output=True)
    assert not left, "a command of the stream loop outlived its stop"
    assert (out / ".log-stream-stop").exists() and (out / "exited").exists()
    seg = json.loads((out / "observe606.json").read_text())["segments"][0]
    assert seg["segment"] == "stream" and seg["stopped"]
    assert '"bench_stream":"reconnect"' in (out / "snowplow-stream.log").read_text()


def test_the_stream_is_stopped_by_its_process_group():
    """What the arm above runs is what the observer runs: the loop in a session of its own, its
    stop file first, then its group."""
    from bench import incluster
    seg = next(s for s in incluster.observe_segments("/out", 15, "x") if s.name == "stream")
    assert seg.on_stop == ".log-stream-stop" and seg.argv[-1].endswith("bench/log_stream.sh")
    src = (BENCH_DIR / "bench" / "incluster.py").read_text()
    assert "start_new_session=True" in src and "os.killpg(p.pid, sig)" in src
    # the loop is no longer on the operator's machine
    assert "stop_log_stream" not in (BENCH_DIR / "run606.sh").read_text()
