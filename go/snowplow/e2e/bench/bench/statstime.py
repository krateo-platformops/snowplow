"""#1390: how long a launch's pair statistics take, recorded per step, and the input each step read.

Run 26e's replica ran for hours past the arms and the VM deleted itself (and the evidence) under it. Each
statistics step run606_pair.sh runs (`custlat replica`, each `custlat pair`) is wrapped by its stat_step: start
and end lines in the pair log, the step's own progress lines between them (custlat.progress), and one record
in $PAIR_OUT/statistics.jsonl:

    {"kind": "statistics_step", "step": "replica N1:N1B", "command": "replica", "started": <epoch>,
     "ended": <epoch>, "seconds": <float>, "exit": <rc>, "input_bytes": <int>, "k6_seconds": <float|null>,
     "host": {"cpus": <int>, "machine": <str>}}

`input_bytes` is what the step reads that grows with the run: every regular file under each arm's k6/ and
browser/ (k6's streams; the rest of a run dir is a few MB). `k6_seconds`: the arms' k6 spans summed, from
their verdicts (report.customer_latency.k6_span), so a later launch scales by its own planned length.

    python3 -m bench.statstime input-bytes DIR...         # stdout: the bytes
    python3 -m bench.statstime record --file F --step S --command C --started T0 --ended T1 --exit RC DIR...
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

#: the parts of an arm's run directory the statistics read that grow with the run
INPUT_DIRS = ("k6", "browser")


def input_bytes(dirs) -> int:
    total = 0
    for d in dirs:
        for sub in INPUT_DIRS:
            for p in sorted(Path(d, sub).rglob("*")):
                if p.is_file() and not p.is_symlink():
                    total += p.stat().st_size
    return total


def k6_seconds(dirs) -> float | None:
    """The arms' k6 spans summed (their verdicts' report.customer_latency.k6_span); None when one is unknown."""
    tot = 0.0
    for d in dirs:
        try:
            v = json.loads(Path(d, "verdict.json").read_text())
            a, b = ((v.get("report") or {}).get("customer_latency") or {}).get("k6_span") or (None, None)
            tot += float(b) - float(a)
        except (OSError, ValueError, TypeError):
            return None
    return round(tot, 1)


def host() -> dict:
    return {"cpus": os.cpu_count(), "machine": os.uname().machine}


def record(path: str, *, step: str, command: str, started: float, ended: float, rc: int, dirs) -> dict:
    rec = {"kind": "statistics_step", "step": step, "command": command, "started": started, "ended": ended,
           "seconds": round(ended - started, 1), "exit": rc, "input_bytes": input_bytes(dirs),
           "k6_seconds": k6_seconds(dirs), "host": host()}
    with open(path, "a") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
    return rec


# ─── the statistics budget a launch needs, measured (#1390 item 3) ──────────
#
# A launch's VM must outlive its arms AND its statistics. The budget is measured, never a fixed knob: from every
# recorded statistics step (statistics.jsonl, written by each launch; the measurement committed beside the bench,
# statistics_measured.jsonl, until a launch on this code has recorded its own):
#   - the slowest rate seen, seconds per byte of input (a step that read nothing is no rate);
#   - the input a step reads per k6-second of its two arms, the largest seen;
# and from the launch's own plan: the k6 seconds of each arm (each run's DRIVE_S, from its dry pass) and its
# statistics steps (the replica and every pair, each reading its two arms). The budget is
#   steps x (2 arms x k6 seconds x bytes per k6-second) x seconds per byte.

MEASURED = Path(__file__).resolve().parent.parent / "statistics_measured.jsonl"


def load_records(paths) -> list[dict]:
    out = []
    for p in paths:
        try:
            for line in Path(p).read_text().splitlines():
                r = json.loads(line)
                if isinstance(r, dict) and r.get("kind") == "statistics_step" and r.get("exit") == 0:
                    out.append(r)
        except (OSError, ValueError):
            continue
    return out


def budget(records: list[dict], *, k6_seconds_per_arm: float, steps: int) -> tuple[float | None, str]:
    """(seconds, how) the launch's statistics are budgeted, or (None, why it is unknown)."""
    rates = [r["seconds"] / r["input_bytes"] for r in records
             if isinstance(r.get("input_bytes"), int) and r["input_bytes"] > 0 and r.get("seconds", 0) > 0]
    density = [r["input_bytes"] / r["k6_seconds"] for r in records
               if isinstance(r.get("input_bytes"), int) and r["input_bytes"] > 0 and (r.get("k6_seconds") or 0) > 0]
    if not rates or not density:
        return None, ("no recorded statistics step measures both a duration and its input (statistics.jsonl): the "
                      "statistics budget is unknown")
    rate, dens = max(rates), max(density)
    per_step = 2 * k6_seconds_per_arm * dens
    secs = steps * per_step * rate
    return secs, (f"statistics budget {secs:.0f} s: {steps} step(s) x {per_step / 1e6:.0f} MB of input (2 arms x "
                  f"{k6_seconds_per_arm:.0f} k6-seconds x {dens / 1e3:.1f} kB per k6-second, the most recorded) x "
                  f"{rate * 1e9:.1f} s per GB (the slowest of {len(rates)} recorded step(s))")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.statstime")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("input-bytes")
    p.add_argument("dirs", nargs="+")
    p = sub.add_parser("record")
    for f in ("--file", "--step", "--command"):
        p.add_argument(f, required=True)
    p.add_argument("--started", type=float, required=True)
    p.add_argument("--ended", type=float, required=True)
    p.add_argument("--exit", type=int, required=True, dest="rc")
    p.add_argument("dirs", nargs="+")
    p = sub.add_parser("budget", help="#1390: the statistics budget of a launch, from every recorded step")
    p.add_argument("--k6-seconds", type=float, required=True, help="each arm's planned k6 seconds (the most)")
    p.add_argument("--steps", type=int, required=True, help="the launch's statistics steps (replica + pairs)")
    p.add_argument("records", nargs="*", help="statistics.jsonl files (the committed measurement always counts)")
    a = ap.parse_args(argv)
    if a.cmd == "budget":
        secs, how = budget(load_records([MEASURED, *a.records]), k6_seconds_per_arm=a.k6_seconds, steps=a.steps)
        print(how, file=sys.stdout if secs is not None else sys.stderr)
        return 0 if secs is not None else 2
    if a.cmd == "input-bytes":
        print(input_bytes(a.dirs))
        return 0
    rec = record(a.file, step=a.step, command=a.command, started=a.started, ended=a.ended, rc=a.rc, dirs=a.dirs)
    print(json.dumps(rec, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
