"""run606's periodic samples of the bench cluster (#919).

    python3 -m bench.sampler --kind top|apf --every-s S --out FILE -- kubectl ...

Runs the given command every S seconds and appends ONE record per sample to
FILE (a `.gz` FILE gets one gzip member per record: the APF scrape keeps
thousands of series per sample). Each sample is BOUNDED by its own period: a
sample still unanswered when the next one is due has failed. The command's
whole process group is killed then, the sample is recorded as failed
({"failed": true, "rc", "timed_out", "bound_s", "error"}: kubectl's first
error line, redacted by bench/identity.py) and the loop carries on. Run 16 B:
one hung `kubectl top` (no request timeout) stopped the CPU samples for the
last 3.5 h of the run, the stall included, and nothing said so.

Kinds (what an answered sample keeps):
  top  {"raw": kubectl top's rows on one line} (load606 evaluate and custlat
       read `raw` as before);
  apf  {"replica": process_start_time_seconds, "series": {...}}: every
       `apiserver_flowcontrol_*` series of the API server's /metrics. A
       regional control plane has several replicas behind one endpoint; each
       scrape lands on one of them, and process_start_time_seconds says
       which (bench/apf.py differences counters only within one replica).

`t` is when the sample ended (as the shell loop wrote it before), `t_start`
when it began. SIGTERM stops the loop: the sample in flight is killed and not
recorded (a stop is not a failed sample).
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import signal
import subprocess
import sys
import time
from typing import Callable

from bench import identity

APF_PREFIX = "apiserver_flowcontrol_"
REPLICA_SERIES = "process_start_time_seconds"


class _Stopping(Exception):
    pass


class Runner:
    """Runs one command at a time in its own process group, so a bound or a
    stop kills everything it started (kubectl's credential plugin too)."""

    def __init__(self):
        self.proc: subprocess.Popen | None = None
        self.stopping = False
        self.writing = False

    def stop(self, *_):
        """SIGTERM / SIGINT: kill the sample in flight and end the loop at
        once; a record being written is finished first."""
        self.stopping = True
        self._kill()
        if not self.writing:
            raise _Stopping

    def _kill(self):
        p = self.proc
        if p is not None and p.poll() is None:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    def run(self, argv: list[str], bound_s: float) -> tuple[int | None, bytes, bytes, bool]:
        """(rc, stdout, stderr, timed_out); rc None when the bound ended it."""
        if self.stopping:
            raise _Stopping
        self.proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            out, err = self.proc.communicate(timeout=bound_s)
            rc, timed_out = self.proc.returncode, False
        except subprocess.TimeoutExpired:
            self._kill()
            out, err = self.proc.communicate()
            rc, timed_out = None, True
        finally:
            self._kill()
            self.proc = None
        if self.stopping:
            raise _Stopping
        return rc, out, err, timed_out


def parse_top(text: str) -> dict:
    """kubectl top's rows on one line, as run606's shell loop wrote them."""
    return {"raw": " ".join(text.split())}


def parse_apf(text: str) -> dict:
    """The API server's flow-control series (Prometheus text format), each
    `name{labels}` -> value, and the replica that answered."""
    series: dict[str, float] = {}
    replica = None
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if not (line.startswith(APF_PREFIX) or line.startswith(REPLICA_SERIES)):
            continue
        # name{labels} value [timestamp]; label values may hold spaces
        cut = line.rfind("}")
        head, rest = (line[:cut + 1], line[cut + 1:]) if cut >= 0 else line.split(" ", 1)
        try:
            value = float(rest.split()[0])
        except (IndexError, ValueError):
            continue
        head = head.strip()
        if head == REPLICA_SERIES:
            replica = value
        elif head.startswith(APF_PREFIX):
            series[head] = value
    return {"replica": replica, "series": series}


# top_boot: kubectl top through snowplow's restart (#922 F3: boot is its own
# phase), apart from the measure's `top` so no boot sample reads as storm
PARSERS: dict[str, Callable[[str], dict]] = {"top": parse_top, "top_boot": parse_top, "apf": parse_apf}


def _first_line(b: bytes) -> str | None:
    for line in (b or b"").decode("utf-8", "replace").splitlines():
        if line.strip():
            return identity.redact_identity(line.strip())
    return None


def sample(kind: str, argv: list[str], bound_s: float, runner: Runner,
           clock: Callable[[], float] = time.time) -> dict:
    """One bounded sample: the kind's answered record, or a failed one."""
    t0 = clock()
    try:
        rc, out, err, timed_out = runner.run(argv, bound_s)
    except OSError as e:                      # the command could not start
        rc, out, err, timed_out = None, b"", f"{type(e).__name__}: {e}".encode(), False
    t1 = clock()
    base = {"kind": kind, "t": round(t1, 3), "t_start": round(t0, 3)}
    if rc == 0:
        return {**base, **PARSERS[kind](out.decode("utf-8", "replace"))}
    return {**base, "failed": True, "rc": rc, "timed_out": timed_out, "bound_s": bound_s,
            "error": ((f"no answer within the sample's bound ({bound_s:g} s): killed"
                       if timed_out else None) or _first_line(err))}


def write(path: str, rec: dict) -> None:
    line = (json.dumps(rec, separators=(",", ":")) + "\n").encode()
    if path.endswith(".gz"):
        with gzip.open(path, "ab") as fh:          # one gzip member per record
            fh.write(line)
    else:
        with open(path, "ab") as fh:
            fh.write(line)


def loop(kind: str, argv: list[str], every_s: float, out: str, runner: Runner,
         clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
         samples: int | None = None) -> int:
    """Sample every `every_s` until stopped (or `samples` samples). Each
    sample's bound is its period: it ends before the next one is due."""
    due = clock()
    n = 0
    try:
        while (samples is None or n < samples) and not runner.stopping:
            rec = sample(kind, argv, every_s, runner, clock)
            runner.writing = True
            try:
                write(out, rec)
            finally:
                runner.writing = False
            n += 1
            due += every_s
            now = clock()
            due = max(due, now)                    # late: the next one now, no burst to catch up
            if not runner.stopping and (samples is None or n < samples):
                sleep(due - now)
    except _Stopping:
        pass
    return 0


def summary(recs: list[dict], kind: str) -> dict:
    """One kind's samples in a run: answered, failed (by error), and the
    longest stretch no sample answered — from the last answered sample before
    a failure to the next answered one (or the last failure, when none
    followed). A run with no sample of the kind says so: missing, not zero."""
    rows = sorted((r for r in recs if r.get("kind") == kind and r.get("t") is not None),
                  key=lambda r: r["t"])
    failed = [r for r in rows if r.get("failed")]
    errors: dict[str, int] = {}
    for r in failed:
        e = r.get("error") or f"rc={r.get('rc')}"
        errors[e] = errors.get(e, 0) + 1
    longest, last_ok, open_since = 0.0, None, None
    for r in rows:
        if r.get("failed"):
            if open_since is None:
                open_since = last_ok if last_ok is not None else r.get("t_start", r["t"])
            longest = max(longest, r["t"] - open_since)
        else:
            if open_since is not None:
                longest = max(longest, r["t"] - open_since)
            open_since, last_ok = None, r["t"]
    return {"answered": len(rows) - len(failed), "failed": len(failed), "failed_by_error": errors,
            "longest_unanswered_s": round(longest, 1),
            **({} if rows else {"note": f"no {kind} sample in the inputs: missing, not zero"})}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.sampler")
    p.add_argument("--kind", choices=sorted(PARSERS), required=True)
    p.add_argument("--every-s", type=float, required=True,
                   help="the period; also each sample's bound")
    p.add_argument("--out", required=True)
    p.add_argument("--samples", type=int, default=None, help="stop after N samples")
    p.add_argument("cmd", nargs=argparse.REMAINDER)
    a = p.parse_args(argv)
    cmd = a.cmd[1:] if a.cmd[:1] == ["--"] else a.cmd
    if not cmd or a.every_s <= 0:
        p.error("a command after -- and --every-s > 0 are required")
    runner = Runner()
    signal.signal(signal.SIGTERM, runner.stop)
    signal.signal(signal.SIGINT, runner.stop)
    return loop(a.kind, cmd, a.every_s, a.out, runner, samples=a.samples)


if __name__ == "__main__":
    sys.exit(main())
