"""#1170: every process a test starts is killed and reaped when the test ends: pass, fail, a
subprocess timeout or Ctrl-C. The session fails if a process outlives every test.

Each arm runs a small inner pytest session (with tests/procreap.py, as pytest.ini loads it)
whose test leaves a process behind the way the bench's own leaks did: a child that backgrounds
a grandchild and exits (run606.sh's samplers, re-parented to init), or a child in a session of
its own (run606, started with start_new_session). Afterwards the arm looks for that process by
a unique token in its ARGV, with a plain `ps`, not through the owner tags the plugin uses, so
a plugin that tags nothing is caught as well as one that kills nothing.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from pathlib import Path

import pytest

BENCH_DIR = Path(__file__).resolve().parent.parent

#: the inner tests' building blocks: an orphan that outlives the process that started it
INNER_HEAD = '''
import os, subprocess, sys, time
import pytest
TOKEN = os.environ["REAP_TOKEN"]
PY = sys.executable
SLEEPER = [PY, "-c", "import time; time.sleep(300)", TOKEN]


def orphan(**kw):
    """bash backgrounds the sleeper and exits: the sleeper is re-parented to init."""
    cmd = " ".join(f"'{a}'" for a in SLEEPER) + " >/dev/null 2>&1 &"
    subprocess.run(["bash", "-c", cmd], check=True, **kw)
'''


def _alive(token: str) -> list[str]:
    out = subprocess.run(["ps", "-A", "-ww", "-o", "pid=,command="], capture_output=True, text=True).stdout
    return [ln.strip() for ln in out.splitlines() if token in ln and "ps -A" not in ln]


def _kill(token: str) -> None:
    for ln in _alive(token):
        try:
            os.kill(int(ln.split()[0]), signal.SIGKILL)
        except (ProcessLookupError, ValueError):
            pass


def _inner(tmp_path: Path, body: str, *, start=False, extra=()):
    """Write the inner test file; run it (or start it, for the Ctrl-C arm) in a session of
    its own, with tags of its own."""
    token = f"reap1170-{uuid.uuid4().hex}"
    f = tmp_path / "test_inner.py"
    f.write_text(INNER_HEAD + textwrap.dedent(body))
    env = {k: v for k, v in os.environ.items()
           if k not in ("BENCH_TEST_SESSION", "BENCH_TEST_OWNER") and not k.startswith("PYTEST_")}
    env.update(PYTHONPATH=str(BENCH_DIR), REAP_TOKEN=token, BENCH_TEST_SESSION=f"inner-{token}",
               READY=str(tmp_path / "ready"))
    argv = [sys.executable, "-m", "pytest", "-p", "tests.procreap", "-p", "no:cacheprovider",
            "-q", "-rA", *extra, str(f)]
    if start:
        return token, subprocess.Popen(argv, cwd=tmp_path, env=env, start_new_session=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return token, subprocess.run(argv, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)


@pytest.mark.parametrize("extra", [(), ("-n", "2")], ids=["serial", "xdist"])
def test_a_process_a_passing_test_leaves_behind_is_killed_when_the_test_ends(tmp_path, extra):
    token, p = _inner(tmp_path, '''
        def test_leaks():
            orphan()
            deadline = time.monotonic() + 10
            while not any(TOKEN in l for l in subprocess.run(["ps", "-A", "-ww", "-o", "command="],
                          capture_output=True, text=True).stdout.splitlines() if "ps -A" not in l):
                assert time.monotonic() < deadline, "the orphan never started"
                time.sleep(0.05)
    ''', extra=extra)
    try:
        assert p.returncode == 0, p.stdout + p.stderr
        assert _alive(token) == [], "the orphan outlived its test"
        assert "outlived every test" not in p.stdout                   # the test's end, not the session's
        # shown, never absorbed: the summary names the test and the process
        assert "processes a test left running" in p.stdout and "test_inner.py::test_leaks" in p.stdout
        assert token in p.stdout
        # the command line only: a process's environment can hold credentials
        listed = [ln for ln in p.stdout.splitlines() if ln.startswith("    pid ")]
        assert listed and not [ln for ln in listed if re.search(r"\s[A-Z][A-Z0-9_]*=", ln)], listed
    finally:
        _kill(token)


def test_a_test_whose_subprocess_times_out_leaves_no_process_behind(tmp_path):
    """subprocess.run's timeout kills the child it started, never the grandchild."""
    token, p = _inner(tmp_path, '''
        def test_times_out():
            subprocess.run(["bash", "-c", " ".join(f"'{a}'" for a in SLEEPER) + " & wait"], timeout=1)

        def test_next():                    # the NEXT test already finds none of it running
            ps = subprocess.run(["ps", "-A", "-ww", "-o", "command="], capture_output=True, text=True).stdout
            assert not [l for l in ps.splitlines() if TOKEN in l and "ps -A" not in l]
    ''')
    try:
        assert p.returncode == 1 and "TimeoutExpired" in p.stdout, p.stdout + p.stderr
        assert "1 failed, 1 passed" in p.stdout, p.stdout              # test_next saw no leftover
        assert "outlived every test" not in p.stdout                   # the test's end, not the session's
        assert _alive(token) == [], "the timed-out command's grandchild outlived its test"
    finally:
        _kill(token)


def test_a_test_interrupted_with_ctrl_c_leaves_no_process_behind(tmp_path):
    """Ctrl-C reaches the terminal's process group; run606 and the fakes it starts sit in a
    session of their own, so the signal never reaches them: the test's end must."""
    token, p = _inner(tmp_path, '''
        def test_interrupted():
            subprocess.Popen(SLEEPER, start_new_session=True)
            open(os.environ["READY"], "w").close()
            time.sleep(120)
    ''', start=True)
    try:
        deadline = time.monotonic() + 60
        while not (tmp_path / "ready").exists():
            assert p.poll() is None and time.monotonic() < deadline, p.stdout.read() if p.poll() is not None else ""
            time.sleep(0.05)
        assert _alive(token), "the inner test's process is not running"
        os.killpg(p.pid, signal.SIGINT)
        out, _ = p.communicate(timeout=60)
        assert p.returncode == 2 and "KeyboardInterrupt" in out, out          # pytest: interrupted
        assert _alive(token) == [], "a process of the interrupted test outlived it"
        assert "outlived every test" not in out                        # the test's end, not the session's
    finally:
        if p.poll() is None:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
        _kill(token)


def test_a_child_started_with_its_own_env_is_still_the_tests(tmp_path):
    """env= replaces the inherited environment: the tags are added to it."""
    token, p = _inner(tmp_path, '''
        def test_own_env():
            orphan(env={"PATH": os.environ["PATH"]})
    ''')
    try:
        assert p.returncode == 0, p.stdout + p.stderr
        assert _alive(token) == [], "a child started with env= escaped its test"
        assert "outlived every test" not in p.stdout                   # the test's end, not the session's
    finally:
        _kill(token)


def test_the_interpreters_resource_tracker_is_not_a_tests_leak(tmp_path):
    """multiprocessing's resource tracker (bench/ghcr_pull.py's spawn context starts it) lives
    as long as the test process, by design: it is the process's, not the test's that first
    used it, so no test's end may kill it (a killed tracker is relaunched, warning that
    resources might leak)."""
    token, p = _inner(tmp_path, '''
        import multiprocessing

        def _child():
            pass

        @pytest.mark.parametrize("i", [1, 2])
        def test_spawns(i):
            pr = multiprocessing.get_context("spawn").Process(target=_child)
            pr.start()
            pr.join(60)
            assert pr.exitcode == 0
    ''')
    try:
        assert p.returncode == 0, p.stdout + p.stderr
        assert "processes a test left running" not in p.stdout, p.stdout
        assert "resource_tracker" not in p.stdout + p.stderr, p.stdout + p.stderr
    finally:
        _kill(token)


@pytest.mark.parametrize("extra", [(), ("-n", "2")], ids=["serial", "xdist"])
def test_a_process_that_outlives_every_test_fails_the_session(tmp_path, extra):
    """A module-scoped fixture is set up before any test's tag exists, so no test owns what
    it starts: the session check fails the run, names the process, and stops it."""
    token, p = _inner(tmp_path, '''
        @pytest.fixture(scope="module")
        def server():
            subprocess.Popen(SLEEPER, start_new_session=True)
            yield

        def test_uses_the_server(server):
            pass
    ''', extra=extra)
    try:
        assert p.returncode == 1, p.stdout + p.stderr
        assert "outlived every test and fixture" in p.stdout and token in p.stdout, p.stdout
        assert "1 passed" in p.stdout                       # the test itself passed: the SESSION fails
        # under xdist the controller runs the check, after its workers have stopped
        assert _alive(token) == []
    finally:
        _kill(token)
