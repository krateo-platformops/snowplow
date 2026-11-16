"""The boot window of a measuring run (report-only): what answered while the Go runtime read waited.

Runs 26j and 27 (2026-11-16): every failed Go runtime read (/debug/vars as the debug reader, the runtime606
Job, the measure's first snowplow read) hit a pod Kubernetes had marked Ready, through the ClusterIP
Service, the old pod gone; the 4 GOGC=50 boots gave no byte for 30 s, the 2 GOGC=100 boots answered; the
CPU was not saturated. The owner's ruling: no separate rehearsal; every measuring run's own read carries
the probe (load606 BootWindow), so the relaunched GC run answers the question itself. While the read
runs, every 5 s, concurrently, /readyz (anonymous) and the portal's boot root (/call, the preflight user),
until the read ended and one round more; ONE goroutine dump (/debug/pprof/goroutine?debug=2, the debug
reader) when a request of the window passes 5 s without a byte. All of it is in the Job's log
($OUT/go-runtime.jsonl), the dump as gzip+base64 chunks.

This module reads that log (and the new pod's Ready from the before-record, pod-before.jsonl) into
$OUT/boot-window.json, writes the dump to its own file, and says ONE block. Per route (/readyz, the boot
root, and /debug/vars: the read's own attempts): requests, answers, how many STALLED (past the stall bound
without a byte: a late first byte, or no answer that took longer), how many FAILED FAST (no answer within
the bound: refused, reset, a name that does not resolve), the longest time without a byte, and the span
of the stalls relative to Ready. The reading key (the owner's, 2026-11-16), over the routes not served:
  every route                              network or endpoint churn
  /readyz fast, the gated routes not       the auth/refresh gate
  only /debug/vars                         the expvar render
  none                                     no stall in the window
  any other pattern                        named as it is, with no reading
REPORT-ONLY: nothing here changes a verdict (the Go runtime gate is snowplow_env's). Never a body other
than the dump (a file, never printed), never an identity: the records hold neither.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import sys
import time

ROUTES = ("readyz", "call", "vars")
SAID = {"readyz": "/readyz (anonymous)", "call": "the boot root /call (preflight user)",
        "vars": "/debug/vars (debug reader, the Go runtime read)"}
GATED = ("call", "vars")
KIND = "boot_window"


def _rows(path: str | None) -> list[dict]:
    try:
        with open(path) as fh:
            return [r for r in (json.loads(ln) for ln in fh if ln.lstrip().startswith("{")) if isinstance(r, dict)]
    except (OSError, TypeError, ValueError):
        return []


def no_byte_s(p: dict) -> float:
    """How long the request went without a byte of an answer (s): to its first byte, or its whole time."""
    ms = p.get("first_byte_ms")
    return round((ms if ms is not None else p.get("elapsed_ms") or 0.0) / 1000.0, 1)


def stalled(p: dict, stall_s: float) -> bool:
    """Past the stall bound with no byte: a late first byte, or no answer that took longer than the bound."""
    fb = p.get("first_byte_ms")
    return fb > stall_s * 1000.0 if fb is not None else (p.get("elapsed_ms") or 0.0) > stall_s * 1000.0


def failed(p: dict, stall_s: float) -> bool:
    """No HTTP answer, and quickly (refused, reset, a name that does not resolve): not served, no stall."""
    return p.get("first_byte_ms") is None and not p.get("status") and not stalled(p, stall_s)


def route_summary(reqs: list[dict], ref: float, stall_s: float) -> dict:
    """One route's requests: answers by status, stalls, fast failures, the longest wait for a byte, and
    the span of the requests not served, in s from `ref` (the new pod's Ready, else the window's start)."""
    bad = [p for p in reqs if stalled(p, stall_s) or failed(p, stall_s)]
    statuses: dict = {}
    for p in reqs:
        statuses[str(p.get("status"))] = statuses.get(str(p.get("status")), 0) + 1
    out = {"requests": len(reqs), "statuses": statuses, "stalled": sum(1 for p in reqs if stalled(p, stall_s)),
           "failed_fast": sum(1 for p in reqs if failed(p, stall_s)), "not_served": len(bad),
           "max_no_byte_s": max((no_byte_s(p) for p in reqs), default=None),
           "error_classes": sorted({p["error_class"] for p in reqs if p.get("error_class")})}
    if bad:
        a, b = min(p["t"] for p in bad), max(p["t"] + no_byte_s(p) for p in bad)
        out.update(from_s=round(a - ref, 1), until_s=round(b - ref, 1), stall_s=round(b - a, 1),
                   stalled_at=sorted({p.get("stalled_at") or "first_byte (late)" for p in bad}))
    return out


def reading(routes: dict) -> str:
    """The owner's key, over the routes not served in the window."""
    s = {r for r, v in routes.items() if v.get("not_served")}
    if not all(routes.get(r, {}).get("requests") for r in ROUTES):
        return "NO READING: a route has no request in the window (" + ", ".join(
            SAID[r] for r in ROUTES if not routes.get(r, {}).get("requests")) + ")"
    if not s:
        return "no stall in the window"
    if s == set(ROUTES):
        return "every route not served: network or endpoint churn"
    if "readyz" not in s and s == set(GATED):
        return "/readyz fast, the gated routes not served: the auth/refresh gate"
    if s == {"vars"}:
        return "only /debug/vars not served: the expvar render"
    return "a pattern the key does not name: not served " + ", ".join(SAID[r] for r in ROUTES if r in s)


def write_dump(rows: list[dict], path: str) -> dict | None:
    """The window's one dump: its record, and its body (gzip) written to `path` when every chunk came."""
    d = next((r for r in rows if r.get("kind") == "boot_dump"), None)
    if d is None:
        return None
    out = {k: d.get(k) for k in ("t", "trigger", "status", "elapsed_ms", "connect_ms", "first_byte_ms",
                                 "error_class", "stalled_at", "bytes", "chunks", "skipped")}
    chunks = sorted((r for r in rows if r.get("kind") == "boot_dump_chunk"), key=lambda r: r.get("i", 0))
    out["path"] = None
    if d.get("chunks"):
        if [c.get("i") for c in chunks] != list(range(d["chunks"])):
            out["why"] = f"{len(chunks)} of its {d['chunks']} chunks are in the log"
            return out
        try:
            data = base64.b64decode("".join(c["data"] for c in chunks), validate=True)
        except (binascii.Error, KeyError, TypeError) as e:
            out["why"] = f"its chunks do not decode ({type(e).__name__})"
            return out
        with open(path + ".part", "wb") as fh:
            fh.write(data)
        os.replace(path + ".part", path)
        out["path"] = path
    return out


def _utc(t) -> str:
    return (time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int((t % 1) * 1000):03d}Z"
            if isinstance(t, (int, float)) else "UNKNOWN")


def _off(t, ref, name: str) -> str:
    return f"{name} {t - ref:+.1f} s" if isinstance(t, (int, float)) and isinstance(ref, (int, float)) else f"{name} ?"


def summarize(effective: str, pod: str | None, dump_path: str, load_start: str | None = None) -> dict:
    rows = _rows(effective)
    start = next((r for r in rows if r.get("kind") == "boot_window_start"), None)
    rec: dict = {"kind": KIND, "source": effective}
    if start is None:
        rec["why"] = "the runtime606 Job's log holds no boot window (no Job, or one that predates it)"
        rec["lines"] = [f"INFO boot window: NO RECORD ({rec['why']})"]
        return rec
    stall_s = float(start.get("stall_s") or 5.0)
    ready = next((r.get("ready_at") for r in reversed(_rows(pod)) if r.get("kind") == "snowplow_pod"), None)
    ref, refname = (ready, "Ready") if isinstance(ready, (int, float)) else (start["t"], "the window's start")
    reqs = {"readyz": [r for r in rows if r.get("kind") == "boot_probe" and r.get("route") == "readyz"],
            "call": [r for r in rows if r.get("kind") == "boot_probe" and r.get("route") == "call"],
            "vars": [r for r in rows if r.get("kind") == "go_runtime_attempt"]}
    routes = {r: route_summary(v, ref, stall_s) for r, v in reqs.items()}
    gc = next(((r.get("read") or {}).get("gc") for r in rows if r.get("kind") == "go_runtime"), None)
    end = next((r for r in rows if r.get("kind") == "boot_window_end"), None)
    dump = write_dump(rows, dump_path)
    # the load's start (run606 load-start.json): the window now overlaps the load's first seconds (the owner's
    # ruling, 2026-11-16: the load starts right after Ready, the read and its probes run alongside it)
    try:
        with open(load_start) as fh:
            ls = json.load(fh)
        applied = ls.get("applied_at") if isinstance(ls, dict) and ls.get("kind") == "load_start" else None
    except (OSError, TypeError, ValueError):
        applied = None
    t_end = (end or {}).get("t") or max((r.get("t") or 0) for r in rows)
    rec.update(ready_at=ready, ref=refname, window_start_from_ref_s=round(start["t"] - ref, 1), stall_bound_s=stall_s,
               routes=routes, reading=reading(routes), gc=gc, dump=dump, end=end,
               window={"start": start["t"], "end": t_end, "start_utc": _utc(start["t"]), "end_utc": _utc(t_end),
                       "load_applied_at": applied,
                       "overlap_with_load_s": (round(max(0.0, t_end - max(applied, start["t"])), 1)
                                               if isinstance(applied, (int, float)) else None)})
    lines = [f"INFO boot window (report-only): from {refname}{'+' if start['t'] >= ref else ''}"
             f"{rec['window_start_from_ref_s']} s, every {start.get('every_s')} s, each request a new connection "
             f"bounded at {start.get('timeout_s')} s; a stall = past {stall_s:g} s without a byte"
             + ("" if end else "; the window's end is not in the log (the step's wait ended first)"),
             f"INFO boot window: probes from {_utc(start['t'])} ({_off(start['t'], ready, 'Ready')}, "
             f"{_off(start['t'], applied, 'load start')}) to {_utc(t_end)} ({_off(t_end, ready, 'Ready')}, "
             f"{_off(t_end, applied, 'load start')}): "
             + (f"{rec['window']['overlap_with_load_s']} s of them overlap the load (two small requests every "
                f"{start.get('every_s')} s, the read's own attempts beside them)" if applied is not None else
                "no load start recorded (no load in this run)")]
    for r in ROUTES:
        v = routes[r]
        said = (f"NOT SERVED: {v['stalled']} of {v['requests']} stalled, {v['failed_fast']} failed fast; for "
                f"{v['stall_s']} s ({refname}{v['from_s']:+} s to {refname}{v['until_s']:+} s), the longest "
                f"{v['max_no_byte_s']} s without a byte ({', '.join(v['stalled_at'])}"
                f"{'; ' + ', '.join(v['error_classes']) if v['error_classes'] else ''})"
                if v.get("not_served") else
                f"served: {v['requests']} request(s), the longest {v['max_no_byte_s']} s to a first byte")
        lines.append(f"INFO boot window: {SAID[r]}: {said}; answers {json.dumps(v['statuses'], sort_keys=True)}")
    lines.append(f"INFO boot window: READING: {rec['reading']}")
    if applied is not None and any(v.get("not_served") for v in routes.values()):
        from bench.snowplow_env import STALL_SHARED_NODE       # #1494 (4): where the stall is reported
        lines.append(f"INFO boot window: {STALL_SHARED_NODE}")
    if isinstance(gc, dict):
        lines.append(f"INFO boot window: /debug/vars, once answered: GOGC {gc.get('gogc')}, NumGC {gc.get('num_gc')}, "
                     f"PauseTotalNs {gc.get('pause_total_ns')}, GCCPUFraction {gc.get('gc_cpu_fraction')}, "
                     f"prewarm complete {gc.get('prewarm_complete')}")
    if dump is None:
        lines.append("INFO boot window: goroutine dump: none (no request passed the stall bound)")
    else:
        t = dump.get("trigger") or {}
        what = (f"saved {dump['path']} ({dump.get('bytes')} bytes)" if dump.get("path") else
                f"SKIPPED ({dump['skipped']})" if dump.get("skipped") else
                f"NOT SAVED: it answered {dump.get('status')}"
                + (f", {dump.get('error_class')} after {dump.get('elapsed_ms')} ms ({dump.get('stalled_at')}): the dump "
                   f"stalled too" if not dump.get("status") else "") + (f"; {dump['why']}" if dump.get("why") else ""))
        lines.append(f"INFO boot window: goroutine dump (taken when {SAID.get(t.get('route'), t.get('route'))} was "
                     f"{t.get('waited_s')} s without a byte): {what}")
        dt = dump.get("t")
        lines.append(f"INFO boot window: the goroutine dump (debug=2: it stops the world while it walks every "
                     f"goroutine) ran at {_utc(dt)} ({_off(dt, ready, 'Ready')}, {_off(dt, applied, 'load start')}) "
                     f"for {dump.get('elapsed_ms')} ms")
    rec["lines"] = lines
    return rec


# ─── what the boot window overlapped (#1494 item 4): the measured phases and the gate's ─────
#
# The load starts right after Ready (the owner's ruling, 2026-11-16), so in a stalled arm the boot window's
# probes run into the load (up to the read's deadline, Ready + its budget) and the goroutine dump (debug=2:
# it stops the world while it walks every goroutine, ~7.7k of them at 50K) can land inside a measured phase.
# Marked here, per phase, from the arm's own record (snowplow-env.json's boot_window: its probes' span and
# its dump's start and duration): evaluate's measured phases (boot, storm, burst, each churn step) and the
# gate's phases (custlat.pooled of the customers' windows: what the pair's cells are computed over). An
# overlap is the same test as a cluster operation's (bench/clusterops.py overlaps). Report-only.

def spans_of(bw: dict | None) -> tuple[dict | None, dict | None, str]:
    """(the probes' span {start_t, end_t}, the dump's {start_t, end_t} or None, why neither is known)."""
    if not isinstance(bw, dict) or bw.get("kind") != KIND:
        return None, None, "the run has no boot-window record (no Go runtime read, or one that predates it)"
    w = bw.get("window") or {}
    if not isinstance(w.get("start"), (int, float)) or not isinstance(w.get("end"), (int, float)):
        return None, None, "the boot-window record holds no probe span (a record that predates #1494's)"
    probes = {"id": "the boot-window probes", "start_t": float(w["start"]), "end_t": float(w["end"])}
    d = bw.get("dump") if isinstance(bw.get("dump"), dict) else None
    dump = None
    if d and isinstance(d.get("t"), (int, float)) and not d.get("skipped"):
        dump = {"id": "the goroutine dump", "start_t": float(d["t"]),
                "end_t": float(d["t"]) + float(d.get("elapsed_ms") or 0) / 1000.0}
    return probes, dump, ""


def overlap_report(bw: dict | None, phases: list, gate: list) -> dict:
    """report["boot_window_overlap_report_only"] of one run: per measured phase (name, start, end) and per gate
    phase (its windows, pooled by name), whether the probes and the dump overlapped it."""
    from bench import clusterops
    from bench.snowplow_env import STALL_SHARED_NODE
    probes, dump, why = spans_of(bw)
    out: dict = {"state": "UNKNOWN" if why else "READ", "why": why or None, "probes": probes, "dump": dump,
                 "phases": [], "gate": {}, "lines": []}
    if why:
        out["lines"].append(f"INFO boot window overlap: UNKNOWN ({why})")
        return out
    stalled = any((r or {}).get("not_served") for r in (bw.get("routes") or {}).values())

    def marks(t0, t1) -> dict:
        if t0 is None or t1 is None:
            return {"probes": None, "dump": None}
        return {"probes": clusterops.overlaps(probes, t0, t1), "dump": bool(dump) and clusterops.overlaps(dump, t0, t1)}
    for name, t0, t1 in phases:
        out["phases"].append(dict(phase=name, t0=t0, t1=t1, **marks(t0, t1)))
    for name, t0, t1 in gate:
        m = marks(t0, t1)
        g = out["gate"].setdefault(name, {"probes": False, "dump": False})
        for k in ("probes", "dump"):
            g[k] = g[k] or bool(m[k])
    hit = [p for p in out["phases"] if p["probes"] or p["dump"]]
    ghit = [n for n, g in out["gate"].items() if g["probes"] or g["dump"]]
    out["lines"].append(
        f"INFO boot window overlap: the probes {clusterops.stamp(probes['start_t'])}..{clusterops.stamp(probes['end_t'])}"
        + (f", the goroutine dump {clusterops.stamp(dump['start_t'])} for {round(dump['end_t'] - dump['start_t'], 3)} s"
           if dump else ", no goroutine dump")
        + (f"; overlapped measured phase(s) {', '.join(p['phase'] for p in hit)}" if hit else
           "; no measured phase overlapped")
        + (f"; gate phase(s) {', '.join(ghit)}" if ghit else "; no gate phase overlapped"))
    for p in hit:
        what = " and ".join(x for x, on in (("the probes", p["probes"]), ("the goroutine dump", p["dump"])) if on)
        out["lines"].append(f"INFO boot window overlap, phase {p['phase']} [{clusterops.stamp(p['t0'])}.."
                            f"{clusterops.stamp(p['t1'])}]: OVERLAPPED by {what}")
    if stalled:
        out["lines"].append(f"INFO boot window overlap: a route was not served; {STALL_SHARED_NODE}")
    return out


def _arm_overlap(v: dict | None) -> dict:
    sec = ((v or {}).get("report") or {}).get("boot_window_overlap_report_only") if isinstance(v, dict) else None
    return sec if isinstance(sec, dict) else {"state": "UNKNOWN", "why": "the run's verdict.json has no boot-window "
                                              "overlap section (an evaluate before #1494)", "phases": [], "gate": {}}


def _said(sec: dict, row: dict | None) -> str:
    if sec.get("state") != "READ":
        return f"UNKNOWN ({sec.get('why')})"
    if row is None:
        return "phase not measured"
    on = [x for x, k in (("probes", "probes"), ("dump", "dump")) if row.get(k)]
    return ("OVERLAPPED by the " + " and the ".join(on)) if on else "not overlapped"


def pair_overlap(va: dict | None, vb: dict | None, labels: tuple[str, str]) -> dict:
    """The pair report's boot-window marks: per measured and gate phase, each arm's, and the gate phases where
    either arm's probes or dump overlapped (the pair's cells over those phases are marked by cell_mark)."""
    sa, sb = _arm_overlap(va), _arm_overlap(vb)
    out: dict = {"result": "INFO", "a": sa.get("state"), "b": sb.get("state"), "gate_marked": {}, "lines": []}
    pa = {p["phase"]: p for p in sa.get("phases") or []}
    pb = {p["phase"]: p for p in sb.get("phases") or []}
    for name in [*pa, *(n for n in pb if n not in pa)]:
        ta, tb = _said(sa, pa.get(name)), _said(sb, pb.get(name))
        if "OVERLAPPED" in ta + tb or "UNKNOWN" in ta + tb:
            out["lines"].append(f"INFO boot window overlap, phase {name}: {labels[0]}: {ta}; {labels[1]}: {tb}")
    for name in [*(sa.get("gate") or {}), *(n for n in (sb.get("gate") or {}) if n not in (sa.get("gate") or {}))]:
        ga, gb = (sa.get("gate") or {}).get(name), (sb.get("gate") or {}).get(name)
        ta, tb = _said(sa, ga), _said(sb, gb)
        if "OVERLAPPED" in ta + tb:
            out["gate_marked"][name] = f"{labels[0]}: {ta}; {labels[1]}: {tb}"
            out["lines"].append(f"INFO boot window overlap, gate phase {name}: {labels[0]}: {ta}; {labels[1]}: {tb}")
    if not out["lines"]:
        out["lines"].append("INFO boot window overlap: " + (
            "no phase of either arm overlapped a probe window or the goroutine dump"
            if sa.get("state") == "READ" and sb.get("state") == "READ" else
            f"UNKNOWN: {labels[0]}: {sa.get('why') or 'read'}; {labels[1]}: {sb.get('why') or 'read'}"))
    return out


def cell_mark(cell: dict, marked: dict) -> dict:
    """A pair cell whose gate phase either arm's probes or dump overlapped: marked in its record and its line."""
    m = marked.get(cell.get("phase"))
    if m and isinstance(cell.get("line"), str):
        cell = dict(cell, boot_window=m, line=f"{cell['line']} [boot window overlapped this phase: {m}]")
    return cell


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.bootwindow")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("said", help="the boot window's block from the runtime606 Job's log (always exit 0)")
    s.add_argument("--effective", required=True, help="$OUT/go-runtime.jsonl")
    s.add_argument("--pod", default=None, help="$OUT/pod-before.jsonl: the new pod's Ready")
    s.add_argument("--load-start", default=None, help="$OUT/load-start.json: when the drivers were applied")
    s.add_argument("--dump", required=True, help="where the goroutine dump (gzip) is written")
    s.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    rec = summarize(a.effective, a.pod, a.dump, a.load_start)
    with open(a.out + ".part", "w") as fh:
        json.dump(rec, fh, indent=1)
    os.replace(a.out + ".part", a.out)
    print("\n".join(rec["lines"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
