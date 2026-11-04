"""Every process a test starts belongs to that test, and is killed and reaped when the test
ends: pass, fail, a subprocess timeout or Ctrl-C (#1170). Loaded as a plugin (pytest.ini
`-p tests.procreap`), so the controller of a pytest-xdist run loads it too.

How a process is the test's. A process can leave the tree it was started in: run606.sh
backgrounds its samplers and exits, and the orphan is re-parented to init. It can leave its
process group too: run606 is started in a session of its own (start_new_session), and so are
the tests' fake gcloud and kubectl. What it keeps is its environment. Each test therefore gets
an owner tag, BENCH_TEST_OWNER, in os.environ, which every child inherits. A child started
with an explicit env= gets the tag added (subprocess.Popen below). The session tag
BENCH_TEST_SESSION is set once, by the controller, and every xdist worker inherits it.

When a test ends, every live process that carries its owner tag is killed (SIGKILL: an orphan
gets no say in it) and, if it is the test process's own child, reaped. What was killed is
recorded on the test's report and listed in the terminal summary: a leak is shown, never
absorbed. At the end of the session the controller lists every live process that still carries
the session tag. Anything there escaped its test, for example a process started by a module-
or session-scoped fixture that never stopped it. The check prints each one, kills it, and
FAILS the run.

Scope: a process that drops its environment (env -i, or an exec with a fresh environment of
its own) is invisible to both checks. Nothing the bench starts does that.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import time
import uuid

import pytest

SESSION_VAR = "BENCH_TEST_SESSION"
OWNER_VAR = "BENCH_TEST_OWNER"



def _start_the_interpreters_own_helpers():
    """multiprocessing's resource tracker is the interpreter's own helper: started once, on
    first use, and left running until this process exits (bench/ghcr_pull.py's spawn context
    starts it). Started here, before any tag is set, it belongs to the test process, not to
    whichever test first needed it; killed at that test's end, it would only be relaunched
    (with a warning that resources might leak)."""
    try:
        import multiprocessing.resource_tracker as rt
    except ImportError:                 # pragma: no cover
        return
    saved = {k: os.environ.pop(k) for k in (SESSION_VAR, OWNER_VAR) if k in os.environ}
    try:
        rt.ensure_running()
    finally:
        os.environ.update(saved)


_start_the_interpreters_own_helpers()

# the controller (or the serial run) sets it at plugin import, before any xdist worker exists:
# every worker inherits the same session tag
os.environ.setdefault(SESSION_VAR, uuid.uuid4().hex[:16])
SESSION = os.environ[SESSION_VAR]

_spawns = [0]
_real_popen_init = subprocess.Popen.__init__


def _tagging_popen_init(self, *args, **kwargs):
    """A child started with env= gets the session and owner tags it would have inherited."""
    env = kwargs.get("env")
    if env is not None:
        tags = {k: os.environ[k] for k in (SESSION_VAR, OWNER_VAR) if k in os.environ}
        if any(isinstance(k, bytes) for k in env):
            tags = {k.encode(): v.encode() for k, v in tags.items()}
        missing = {k: v for k, v in tags.items() if k not in env}
        if missing:
            kwargs["env"] = {**env, **missing}
    _spawns[0] += 1
    _real_popen_init(self, *args, **kwargs)


subprocess.Popen.__init__ = _tagging_popen_init

try:                                    # multiprocessing's "spawn" context forks outside Popen
    import multiprocessing.util as _mp_util
    _real_spawnv = _mp_util.spawnv_passfds

    def _counting_spawnv(*a, **k):
        _spawns[0] += 1
        return _real_spawnv(*a, **k)

    _mp_util.spawnv_passfds = _counting_spawnv
except (ImportError, AttributeError):   # pragma: no cover
    pass


def tagged(var: str, value: str) -> list[tuple[int, int, str]]:
    """Live processes whose environment holds var=value exactly: (pid, ppid, command)."""
    want = f"{var}={value}"
    me = os.getpid()
    out = []
    if os.path.isdir("/proc/self"):                     # Linux: read each environ directly
        for d in os.listdir("/proc"):
            if not d.isdigit() or int(d) == me:
                continue
            try:
                with open(f"/proc/{d}/environ", "rb") as fh:
                    if want.encode() not in fh.read().split(b"\0"):
                        continue
                with open(f"/proc/{d}/stat", "rb") as fh:
                    stat = fh.read()
                with open(f"/proc/{d}/cmdline", "rb") as fh:
                    cmd = fh.read().replace(b"\0", b" ").decode(errors="replace").strip()
            except OSError:                             # gone, or not ours to read
                continue
            fields = stat[stat.rfind(b")") + 2:].split()
            if fields[0] == b"Z":                       # a zombie has run its course
                continue
            out.append((int(d), int(fields[1]), cmd))
        return out
    # macOS: ps -E appends each process's environment to its command line
    p = subprocess.Popen(["ps", "-A", "-E", "-ww", "-o", "pid=,ppid=,stat=,command="],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, errors="replace")
    listing, _ = p.communicate()
    pat = re.compile(r"(?:^|\s)" + re.escape(want) + r"(?:\s|$)")
    for line in listing.splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4 or not parts[0].isdigit():
            continue
        pid, ppid, stat, rest = int(parts[0]), int(parts[1]), parts[2], parts[3]
        if pid in (me, p.pid) or stat.startswith("Z") or not pat.search(rest):
            continue
        out.append((pid, ppid))
    if not out:
        return []
    # the command lines WITHOUT the environment (it can hold credentials): a second, plain ps
    q = subprocess.run(["ps", "-ww", "-o", "pid=,command=", "-p", ",".join(str(pid) for pid, _ in out)],
                       capture_output=True, text=True, errors="replace")
    cmds = {}
    for line in q.stdout.splitlines():
        head, _, cmd = line.strip().partition(" ")
        if head.isdigit():
            cmds[int(head)] = cmd.strip()
    return [(pid, ppid, cmds.get(pid, "?")) for pid, ppid in out]


def kill_tagged(var: str, value: str, rounds: int = 50) -> list[tuple[int, str]]:
    """SIGKILL every live process tagged var=value until none is left (a dying parent can
    fork once more before it goes); reap the ones that are this process's own children."""
    killed: dict[int, str] = {}
    for _ in range(rounds):
        procs = tagged(var, value)
        if not procs:
            break
        for pid, ppid, cmd in procs:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
            killed.setdefault(pid, cmd)
            if ppid == os.getpid():
                try:
                    os.waitpid(pid, 0)
                except ChildProcessError:               # its Popen reaped it first
                    pass
        time.sleep(0.02)
    return sorted(killed.items())


_owner_seq = [0]


@pytest.fixture(autouse=True)
def _bench_test_owns_its_processes(request, monkeypatch):
    """The test's owner tag; at its end, every process still carrying it is killed and reaped."""
    _owner_seq[0] += 1
    owner = f"{SESSION}.{os.getpid()}.{_owner_seq[0]}"
    monkeypatch.setenv(OWNER_VAR, owner)
    spawned = _spawns[0]
    yield owner
    if _spawns[0] == spawned:           # it started no process: nothing can carry its tag
        return
    killed = kill_tagged(OWNER_VAR, owner)
    if killed:
        request.node.user_properties.append(
            ("processes_killed_at_test_end", [f"pid {pid}: {cmd[:400]}" for pid, cmd in killed]))


_killed_report: list[tuple[str, list[str]]] = []


def pytest_runtest_logreport(report):
    if report.when == "teardown":
        for name, value in report.user_properties:
            if name == "processes_killed_at_test_end":
                _killed_report.append((report.nodeid, value))


def pytest_terminal_summary(terminalreporter):
    if not _killed_report:
        return
    tr = terminalreporter
    tr.section("processes a test left running (killed at its end, #1170)")
    for nodeid, procs in _killed_report:
        tr.write_line(nodeid)
        for p in procs:
            tr.write_line(f"    {p}")


@pytest.hookimpl(trylast=True)          # after xdist has stopped its workers
def pytest_sessionfinish(session):
    if hasattr(session.config, "workerinput"):     # an xdist worker: its controller checks
        return
    left = kill_tagged(SESSION_VAR, SESSION)
    if not left:
        return
    tr = session.config.pluginmanager.get_plugin("terminalreporter")
    lines = [f"{len(left)} process(es) started by this test session outlived every test and "
             f"fixture (killed now; #1170):"] + [f"    pid {pid}: {cmd[:400]}" for pid, cmd in left]
    for ln in lines:
        if tr is not None:
            tr.write_line(ln, red=True)
        else:                                           # pragma: no cover
            print(ln)
    session.exitstatus = pytest.ExitCode.TESTS_FAILED
