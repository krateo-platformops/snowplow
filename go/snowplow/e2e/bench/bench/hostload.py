"""The orchestrator's own host load (#1297): recorded, gated, and said with the verdict.

Paid run 24 failed its data step on all three arms because the operator's laptop sat at a load of
~90 on 8 CPUs (parallel agent test suites), not because of the clusters, and nothing in the evidence
said so: the bench recorded no host load. Since #1297 the laptop only orchestrates (the data load and
every timed sampler run in-cluster, bench/incluster.py), and this module makes its load an instrument:

  record    every --every-s (15 s), one {"kind": "host_load"} record in host-load.jsonl beside the
            evidence: the 1/5/15-minute load averages and the CPU count, for the whole run;
  check     run606.sh's preflight, before any cloud call: a 1-minute load above the CPU count refuses
            the run, saying why (the run would measure the laptop, not snowplow); the reading is the
            file's first record ({"kind": "host_load_preflight"}). A dry run reads and says it, and
            refuses nothing (it spends and measures nothing);
  report    evaluate's host_load_report_only (INFO, never judged): the preflight reading, the samples,
            the highest load against the CPU count, the share of samples over it, and the host's load
            during every API outage run606's own polls recorded (api_unreachable: those polls are the
            orchestrator's, so a loaded host is the first thing to rule out there).

BENCH_606_HOST_LOAD="<load1> <load5> <load15> <cpus>" stands in for the host's reading (a test hook,
like BENCH_606_FAIL_AT): the refusal and the report are tested through the CLI with it.

STDLIB only.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

__all__ = ["reading", "check", "record_loop", "report", "main"]

KIND = "host_load"
PREFLIGHT_KIND = "host_load_preflight"
TEST_HOOK = "BENCH_606_HOST_LOAD"


def reading(env=None) -> dict:
    """The host's load averages and CPU count, now."""
    env = os.environ if env is None else env
    hook = (env.get(TEST_HOOK) or "").split()
    if hook:
        if len(hook) != 4:
            raise ValueError(f"{TEST_HOOK} wants '<load1> <load5> <load15> <cpus>', got {env[TEST_HOOK]!r}")
        l1, l5, l15 = (float(x) for x in hook[:3])
        cpus, source = int(hook[3]), TEST_HOOK
    else:
        l1, l5, l15 = os.getloadavg()
        cpus, source = os.cpu_count() or 0, "os.getloadavg"
    return {"t": round(time.time(), 3), "load1": round(l1, 2), "load5": round(l5, 2),
            "load15": round(l15, 2), "cpus": cpus, "source": source}


def check(r: dict) -> tuple[bool, str]:
    """(ok, why): a 1-minute load above the CPU count is refused."""
    cpus = r.get("cpus") or 0
    if cpus <= 0:
        return False, "the host's CPU count could not be read: its load cannot be judged"
    if r["load1"] > cpus:
        return False, (f"the orchestrator's 1-minute load {r['load1']:g} exceeds its {cpus} CPUs "
                       f"(5-min {r['load5']:g}, 15-min {r['load15']:g}): the run would measure this "
                       f"machine, not snowplow (run 24: load ~90 on 8 CPUs failed the data step on every "
                       f"arm). Wait until the host is quiet (no parallel test suites, kind clusters or VMs)")
    return True, (f"the orchestrator's 1-minute load {r['load1']:g} is within its {cpus} CPUs "
                  f"(5-min {r['load5']:g}, 15-min {r['load15']:g})")


def _append(path: str, rec: dict) -> None:
    with open(path, "a") as fh:
        fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        fh.flush()


class _Stop(Exception):
    pass


def record_loop(out: str, every_s: float, samples: int | None = None,
                clock=time.time, sleep=time.sleep, read=reading) -> int:
    """One record every `every_s` until SIGTERM / SIGINT (or `samples` records). A late sample is
    taken at once, never a burst to catch up: its own time says how late it was."""
    def _stop(*_):
        raise _Stop

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    n, due = 0, clock()
    try:
        while samples is None or n < samples:
            _append(out, {"kind": KIND, **read()})
            n += 1
            due = max(due + every_s, clock())
            if samples is None or n < samples:
                sleep(max(0.0, due - clock()))
    except _Stop:
        pass
    return 0


def report(recs: list[dict]) -> dict:
    """evaluate's host_load_report_only: INFO lines, never judged. No record reads UNKNOWN."""
    # the last preflight: a dry pass into the same OUT leaves its own reading before the run's
    pre = next((r for r in reversed(recs) if r.get("kind") == PREFLIGHT_KIND), None)
    rows = sorted((r for r in recs if r.get("kind") == KIND and r.get("t") is not None), key=lambda r: r["t"])
    if pre is None and not rows:
        why = "no host_load record: the run recorded no host load (a run before #1297)"
        return {"result": "INFO", "state": "UNKNOWN", "why": why,
                "lines": [f"INFO orchestrator host load: UNKNOWN ({why})"]}
    lines = []
    if pre is not None:
        lines.append(f"INFO orchestrator host load at preflight: {pre.get('why')}"
                     + ("" if pre.get("ok") else " (REFUSED)" if not pre.get("dry") else " (dry run: not refused)"))
    out = {"result": "INFO", "state": "read", "preflight": pre, "samples": len(rows)}
    if rows:
        over = [r for r in rows if r.get("cpus") and r["load1"] > r["cpus"]]
        top = max(rows, key=lambda r: (r["load1"] / r["cpus"]) if r.get("cpus") else 0)
        gaps = [b["t"] - a["t"] for a, b in zip(rows, rows[1:])]
        out.update({"max_load1": top["load1"], "cpus": top.get("cpus"), "max_at": top["t"],
                    "samples_over_cpus": len(over), "longest_gap_s": round(max(gaps), 1) if gaps else None})
        lines.append(f"INFO orchestrator host load: {len(rows)} samples, highest 1-min load {top['load1']:g} on "
                     f"{top.get('cpus')} CPUs, {len(over)} sample(s) over the CPU count"
                     + (f", longest gap between samples {max(gaps):.0f} s" if gaps else ""))
        outages = []
        for o in (r for r in recs if r.get("kind") == "api_unreachable"):
            t0, t1 = o.get("t0"), o.get("t1")
            inside = [r for r in rows if t0 is not None and t1 is not None and t0 - 15 <= r["t"] <= t1 + 15]
            peak = max((r["load1"] for r in inside), default=None)
            outages.append({"t0": t0, "t1": t1, "host_load1_max": peak,
                            "cpus": inside[0].get("cpus") if inside else None})
            lines.append(f"INFO host load during the API outage run606's polls saw from {t0} to {t1}: "
                         + (f"1-min load up to {peak:g} on {inside[0].get('cpus')} CPUs" if inside
                            else "no host sample in that window"))
        out["api_outages"] = outages
    out["lines"] = lines
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.hostload")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="preflight: refuse (exit 2) a 1-min load above the CPU count")
    c.add_argument("--out", required=True, help="host-load.jsonl: the reading is appended to it")
    c.add_argument("--dry", action="store_true", help="say the reading, refuse nothing")
    r = sub.add_parser("record", help="one host_load record every --every-s, until SIGTERM")
    r.add_argument("--out", required=True)
    r.add_argument("--every-s", type=float, default=15.0)
    r.add_argument("--samples", type=int, default=None)
    a = p.parse_args(argv)
    if a.cmd == "record":
        return record_loop(a.out, a.every_s, a.samples)
    rd = reading()
    ok, why = check(rd)
    if not ok and not a.dry:
        # a refusal writes nothing: the run never started, and its OUT stays free for a relaunch
        print(f"refusing: {why}", file=sys.stderr)
        return 2
    _append(a.out, {"kind": PREFLIGHT_KIND, **rd, "ok": ok, "dry": bool(a.dry), "why": why})
    if ok:
        print(why)
        return 0
    print(f"DRY: would refuse: {why}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
