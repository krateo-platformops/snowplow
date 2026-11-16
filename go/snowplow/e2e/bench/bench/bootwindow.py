"""The boot-window rehearsal's report (BENCH_606_REHEARSAL=bootwindow): what the boot606 probe saw.

Runs 26j and 27 (2026-11-16, every GOGC=50 arm): the measure's first snowplow read (/debug/vars, the
runtime606 Job) landed 6-11 s after the rebooted pod's Ready and got no byte for 30 s. The rehearsal
probes the same window to say what holds it: from the loadgen node, every 5 s, concurrently, /readyz
(anonymous), /debug/pprof/goroutine?debug=1 and /debug/vars (the bench's debug reader) and the portal's
boot root (/call, the preflight user), from before the reboot to 180 s past the new pod's Ready
(load606 BootProbe; its records in <out>/bootwindow/probes.jsonl, a goroutine dump saved whenever a
probe passed 5 s without a byte). This module reads them, with the new pod's Ready (pod-boot.jsonl),
the reboot's start (boot.jsonl) and the observer's `kubectl top` of the pod through the boot
(top-boot.jsonl), into <out>/bootwindow.json and ONE block of lines.

Per route, in the window [Ready, Ready + window]: how many probes, their answers, how many STALLED
(past the stall bound with no byte: a late first byte, or no answer that took longer than the bound),
how many FAILED FAST (no answer within the bound: refused, reset, a name that does not resolve), the
longest time without a byte, and when the stalls began and ended relative to Ready. A route with
either is NOT SERVED, which is what the reading key reads (the owner's, 2026-11-16):
  every route stalls                       network or endpoint churn (the Service's endpoints, kube-proxy)
  /readyz fast, the gated routes stall     the auth/refresh gate (every authenticated request waits on it)
  only /debug/vars stalls                  the expvar render (snowplow's /debug/vars handler itself)
  none stalls                              no stall in the window
  any other pattern                        named as it is, with no reading
Beside it: the GC activity /debug/vars reported when it answered (NumGC, PauseTotalNs, GCCPUFraction,
the effective GOGC) and the pod's CPU from the observer's samples, so a GC-driven stall reads apart.
Never a body, never an identity: the records hold neither, and this module prints only their fields.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

ROUTES = ("readyz", "goroutine", "vars", "call")
SAID = {"readyz": "/readyz (anonymous)", "goroutine": "/debug/pprof/goroutine?debug=1 (debug reader)",
        "vars": "/debug/vars (debug reader)", "call": "the boot root /call (preflight user)"}
GATED = ("goroutine", "vars", "call")


def _rows(path: str) -> list[dict]:
    try:
        with open(path) as fh:
            return [json.loads(ln) for ln in fh if ln.lstrip().startswith("{")]
    except (OSError, ValueError):
        return []


def _last(path: str, kind: str) -> dict | None:
    return next((r for r in reversed(_rows(path)) if r.get("kind") == kind), None)


def no_byte_s(p: dict) -> float:
    """How long the probe went without a byte of an answer (s): to its first byte, or its whole time."""
    ms = p.get("first_byte_ms")
    return round((ms if ms is not None else p.get("elapsed_ms") or 0.0) / 1000.0, 1)


def stalled(p: dict, stall_s: float) -> bool:
    """Past the stall bound with no byte: a late first byte, or no answer that took longer than the bound."""
    fb = p.get("first_byte_ms")
    return fb > stall_s * 1000.0 if fb is not None else (p.get("elapsed_ms") or 0.0) > stall_s * 1000.0


def failed(p: dict, stall_s: float) -> bool:
    """No HTTP answer, and quickly (refused, reset, a name that does not resolve): not served, not a stall."""
    return p.get("first_byte_ms") is None and not p.get("status") and not stalled(p, stall_s)


def route_summary(probes: list[dict], ready: float, stall_s: float) -> dict:
    """One route's probes started in the window: answers by status, stalls, fast failures, the longest
    wait for a byte, and the stalls' (and failures') span relative to Ready (s)."""
    st = [p for p in probes if stalled(p, stall_s) or failed(p, stall_s)]
    statuses: dict = {}
    for p in probes:
        statuses[str(p.get("status"))] = statuses.get(str(p.get("status")), 0) + 1
    out = {"probes": len(probes), "statuses": statuses, "stalled": sum(1 for p in probes if stalled(p, stall_s)),
           "failed_fast": sum(1 for p in probes if failed(p, stall_s)), "not_served": len(st),
           "max_no_byte_s": max((no_byte_s(p) for p in probes), default=None),
           "error_classes": sorted({p["error_class"] for p in probes if p.get("error_class")})}
    if st:
        out["stall_from_ready_s"] = round(min(p["t"] for p in st) - ready, 1)
        out["stall_until_ready_s"] = round(max(p["t"] + no_byte_s(p) for p in st) - ready, 1)
        out["stalled_at"] = sorted({p.get("stalled_at") or "first_byte (late)" for p in st})
    return out


def reading(routes: dict) -> str:
    """The owner's key, over which routes were not served in the window (a stall, or no answer at all)."""
    s = {r for r, v in routes.items() if v.get("not_served")}
    if not all(routes.get(r, {}).get("probes") for r in ROUTES):
        return "NO READING: a route has no probe in the window"
    if not s:
        return "no stall in the window"
    if s == set(ROUTES):
        return "every route stalls: network or endpoint churn"
    if "readyz" not in s and s == set(GATED):
        return "/readyz fast, the gated routes stall: the auth/refresh gate"
    if s == {"vars"}:
        return "only /debug/vars stalls: the expvar render"
    return "a pattern the key does not name: stalled " + ", ".join(SAID[r] for r in ROUTES if r in s)


def gc_summary(probes: list[dict]) -> dict:
    """GC activity across the /debug/vars answers in the window (first to last answer)."""
    g = [p["gc"] for p in sorted(probes, key=lambda p: p["t"]) if isinstance(p.get("gc"), dict)]
    if not g:
        return {"answers": 0}
    f, last = g[0], g[-1]

    def d(k):
        return (last[k] - f[k]) if isinstance(last.get(k), (int, float)) and isinstance(f.get(k), (int, float)) else None
    fr = [x["gc_cpu_fraction"] for x in g if isinstance(x.get("gc_cpu_fraction"), (int, float))]
    heap = [x["heap_alloc"] for x in g if isinstance(x.get("heap_alloc"), (int, float))]
    pause = d("pause_total_ns")
    return {"answers": len(g), "gogc": sorted({x.get("gogc") for x in g}, key=str),
            "num_gc_first": f.get("num_gc"), "num_gc_delta": d("num_gc"),
            "pause_ms_delta": None if pause is None else round(pause / 1e6, 1),
            "gc_cpu_fraction_max": max(fr) if fr else None,
            "heap_alloc_mib": [round(min(heap) / 2 ** 20), round(max(heap) / 2 ** 20)] if heap else None,
            "prewarm_complete": [x.get("prewarm_complete") for x in (f, last)]}


_TOP = re.compile(r"(\S+)\s+(\d+)m\s+(\d+)Mi")


def cpu_summary(top_rows: list[dict], pod: str | None, lo: float, hi: float) -> dict:
    """The pod's CPU (millicores) in the observer's `kubectl top` samples between lo and hi (epoch s)."""
    vals = []
    for r in top_rows:
        if not (lo <= (r.get("t") or 0) <= hi):
            continue
        for name, m, _ in _TOP.findall(r.get("raw") or ""):
            if name == pod:
                vals.append(int(m))
    if not vals:
        return {"samples": 0}
    s = sorted(vals)
    return {"samples": len(s), "max_m": s[-1], "median_m": s[len(s) // 2]}


def report(out_dir: str, declared: str = "", window_s: float | None = None) -> tuple[dict, list[str]]:
    bw = os.path.join(out_dir, "bootwindow")
    rows = _rows(os.path.join(bw, "probes.jsonl"))
    start = next((r for r in rows if r.get("kind") == "boot_probe_start"), {})
    end = next((r for r in rows if r.get("kind") == "boot_probe_end"), None)
    probes = [r for r in rows if r.get("kind") == "probe"]
    dumps = [r for r in rows if r.get("kind") == "dump"]
    pod = _last(os.path.join(out_dir, "pod-boot.jsonl"), "snowplow_pod") or {}
    ready = pod.get("ready_at")
    boot = _last(os.path.join(out_dir, "boot.jsonl"), "boot") or {}
    window_s = float(window_s if window_s is not None else start.get("window_s") or 180.0)
    stall_s = float(start.get("stall_s") or 5.0)
    rec: dict = {"kind": "bootwindow", "declared": declared, "ready_at": ready, "pod": pod.get("pod"),
                 "t_restart": boot.get("t_restart"), "window_s": window_s, "stall_s": stall_s,
                 "probe_end": end, "ok": False}
    lines = ["=== bootwindow report ===", f"snowplow env declared: {declared or '(none)'}"]
    if not probes or not isinstance(ready, (int, float)):
        rec["why"] = ("the probe left no records (bootwindow/probes.jsonl)" if not probes else
                      "the new pod's Ready is not known (pod-boot.jsonl)")
        lines += [f"NO REPORT: {rec['why']}", "=== end of bootwindow report ==="]
        return rec, lines
    t_restart = boot.get("t_restart")
    lines.append(f"the new pod: Ready {round(ready - t_restart, 1) if t_restart else '?'} s after the restart; "
                 f"window: Ready to Ready+{window_s:g} s; every probe a new connection, bounded at "
                 f"{start.get('timeout_s')} s; a stall = past {stall_s:g} s with no byte")
    inwin = [p for p in probes if ready <= p["t"] <= ready + window_s]
    routes = {r: route_summary([p for p in inwin if p.get("route") == r], ready, stall_s) for r in ROUTES}
    rec["routes"] = routes
    for r in ROUTES:
        v = routes[r]
        said = (f"{'STALLED' if v['stalled'] else 'NOT SERVED'}: {v['stalled']} of {v['probes']} probes stalled "
                f"(past {stall_s:g} s without a byte), {v['failed_fast']} failed fast with no answer; from "
                f"Ready+{v['stall_from_ready_s']} s to Ready+{v['stall_until_ready_s']} s, the longest "
                f"{v['max_no_byte_s']} s without a byte ({', '.join(v['stalled_at'])}"
                f"{'; ' + ', '.join(v['error_classes']) if v['error_classes'] else ''})"
                if v["not_served"] else
                f"no stall: {v['probes']} probes, the longest {v['max_no_byte_s']} s to a first byte")
        lines.append(f"  {SAID[r]}: {said}; answers {json.dumps(v['statuses'], sort_keys=True)}")
    pre = [p for p in probes if p["t"] < ready]
    pre_st = {r: sum(1 for p in pre if p.get("route") == r and (stalled(p, stall_s) or failed(p, stall_s))) for r in ROUTES}
    lines.append(f"before Ready (the old pod, then the rollout): {len(pre)} probes, not served per route "
                 f"{json.dumps(pre_st)}")
    rec["reading"] = reading(routes)
    lines.append(f"READING: {rec['reading']}")
    rec["gc"] = gc = gc_summary([p for p in inwin if p.get("route") == "vars"])
    lines.append("GC in the window (/debug/vars answers): " + (
        "none answered" if not gc["answers"] else
        f"{gc['answers']} answers, effective GOGC {gc['gogc']}, NumGC +{gc['num_gc_delta']} (from {gc['num_gc_first']}), "
        f"GC pauses +{gc['pause_ms_delta']} ms, GCCPUFraction max {gc['gc_cpu_fraction_max']}, heap "
        f"{gc['heap_alloc_mib']} MiB, prewarm complete first/last {gc['prewarm_complete']}"))
    rec["cpu"] = cpu = cpu_summary(_rows(os.path.join(out_dir, "top-boot.jsonl")), pod.get("pod"), ready,
                                   ready + window_s)
    lines.append("the pod's CPU in the window (the observer's kubectl top): " + (
        f"{cpu['samples']} samples, max {cpu['max_m']}m, median {cpu['median_m']}m" if cpu["samples"]
        else "no sample of it"))
    rec["dumps"] = [{k: d.get(k) for k in ("n", "t", "status", "first_byte_ms", "elapsed_ms", "error_class", "file",
                                           "bytes", "triggers")} for d in dumps]
    saved = [d for d in dumps if d.get("file")]
    lines.append(f"goroutine dumps: {len(dumps)} taken, {len(saved)} saved"
                 + "".join(f"\n    #{d['n']} at Ready{d['t'] - ready:+.1f} s: "
                           + (f"saved bootwindow/{d['file']} ({d['bytes']} bytes)" if d.get("file") else
                              f"NOT saved ({d.get('status')}: {d.get('error_class') or 'no body'})")
                           + "; for " + ", ".join(f"{t['route']} (tick {t['tick']}, {t['waited_s']} s)"
                                                  for t in d.get("triggers") or [])
                           for d in dumps))
    complete = all(routes[r]["probes"] for r in ROUTES) and end is not None and end.get("reason") == "window"
    rec["ok"] = complete
    if not complete:
        rec["why"] = ("the probe did not cover the whole window (its end: "
                      f"{(end or {}).get('reason', 'none recorded')}) or a route has no probe in it")
        lines.append(f"INCOMPLETE: {rec['why']}")
    lines.append("=== end of bootwindow report ===")
    return rec, lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.bootwindow")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("report", help="the bootwindow block from the run's files; exit 1 when the window is not covered")
    r.add_argument("--out-dir", required=True)
    r.add_argument("--declared", default="")
    r.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    rec, lines = report(a.out_dir, a.declared)
    with open(a.out + ".part", "w") as fh:
        json.dump(rec, fh, indent=1)
    os.replace(a.out + ".part", a.out)
    print("\n".join(lines))
    return 0 if rec["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
