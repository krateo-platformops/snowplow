"""Start a paid run fully detached from the session that launches it.

    cd go/snowplow/e2e/bench
    RUNS=... PAIRS=... RUN_NAME_R=... (every variable the run needs) \\
      python3 -m bench.detach --log LOG --pidfile PIDFILE -- bash run606_pair.sh

Run 22's launcher died with the Claude Code session that started it: the session's restart sent
SIGTERM to its process tree, and the launcher's teardown did not complete. macOS has no
setsid(1). This is the supported launch: it forks; the child calls setsid() (a session and a
process group of its own, no controlling terminal), ignores SIGHUP, takes /dev/null as stdin and
the log (appended) as stdout and stderr, closes every other descriptor it inherited, writes its
pid to the pidfile, and execs the command, so the pid IS the command's (run606_pair.sh's shell,
whose INT/TERM trap stops every run and lets each tear its cluster down). The parent prints that
pid once the exec has succeeded and exits, so the child is re-parented away from the launching
tree at once: a signal to the launcher's process group, or a walk of its descendants, does not
reach it. SIGHUP stays ignored in the command (an ignored signal is inherited across exec, and
bash cannot trap one ignored at entry): a hangup is no stop.

Stopping a detached run: `kill -TERM "$(cat PIDFILE)"` (run606_pair.sh forwards it to every
run). The pidfile is refused while the pid it names is alive, so one pidfile never stands for
two launches.

Exit: 0 once the command is running; 1 when it could not be started (the reason on stderr).
"""

from __future__ import annotations

import argparse
import os
import signal
import sys


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _held(pidfile: str) -> int | None:
    """The live pid an existing pidfile names, if any."""
    try:
        with open(pidfile) as fh:
            pid = int(fh.read().strip() or "0")
    except (OSError, ValueError):
        return None
    return pid if pid > 0 and _alive(pid) else None


def _write_pidfile(pidfile: str, pid: int) -> None:
    tmp = f"{pidfile}.tmp-{pid}"
    with open(tmp, "w") as fh:
        fh.write(f"{pid}\n")
    os.replace(tmp, pidfile)


def detach(argv: list[str], log: str, pidfile: str) -> int:
    """Start argv detached; return its pid (raises OSError when it could not be started)."""
    held = _held(pidfile)
    if held is not None:
        raise OSError(f"the pidfile {pidfile} names pid {held}, which is alive: one pidfile, one launch")
    # the log is opened by the parent, so a path that cannot be written refuses before any fork
    log_fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    null_fd = os.open(os.devnull, os.O_RDONLY)
    # the exec's report: closed on exec (CLOEXEC, Python's default); an exec that fails writes why
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:                                         # the child: never returns
        try:
            os.close(r)
            os.setsid()
            signal.signal(signal.SIGHUP, signal.SIG_IGN)
            os.dup2(null_fd, 0)
            os.dup2(log_fd, 1)
            os.dup2(log_fd, 2)
            # every other descriptor the launcher had open (a session's pipes would wait on this
            # process): the ones that are open, listed, never a guessed range
            for fd in sorted({int(x) for x in os.listdir("/dev/fd") if x.isdigit()}):
                if fd > 2 and fd != w:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            _write_pidfile(pidfile, os.getpid())
            os.execvp(argv[0], argv)
        except BaseException as e:  # noqa: BLE001 — whatever stopped the exec is the parent's answer
            try:
                os.write(w, f"{type(e).__name__}: {e}".encode(errors="replace"))
            finally:
                os._exit(127)
    os.close(w)
    os.close(log_fd)
    os.close(null_fd)
    chunks = []
    while True:
        b = os.read(r, 4096)
        if not b:
            break
        chunks.append(b)
    os.close(r)
    if chunks:
        os.waitpid(pid, 0)
        raise OSError(f"could not start {argv[0]}: {b''.join(chunks).decode(errors='replace')}")
    return pid


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    cmd: list[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, cmd = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(prog="detach", description="start a command fully detached (see the module doc)")
    ap.add_argument("--log", required=True, help="the command's stdout and stderr, appended")
    ap.add_argument("--pidfile", required=True, help="where the command's pid is written")
    a = ap.parse_args(argv)
    if not cmd:
        ap.error("the command goes after --")
    try:
        pid = detach(cmd, a.log, a.pidfile)
    except OSError as e:
        print(f"detach: {e}", file=sys.stderr)
        return 1
    print(pid, flush=True)
    print(f"detached: pid {pid} (its own session and process group); log {a.log}; pidfile {a.pidfile}; "
          f"stop it with: kill -TERM {pid}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
