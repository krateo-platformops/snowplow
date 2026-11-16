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


def summarize(effective: str, pod: str | None, dump_path: str) -> dict:
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
    rec.update(ready_at=ready, ref=refname, window_start_from_ref_s=round(start["t"] - ref, 1), stall_bound_s=stall_s,
               routes=routes, reading=reading(routes), gc=gc, dump=dump, end=end)
    lines = [f"INFO boot window (report-only): from {refname}{'+' if start['t'] >= ref else ''}"
             f"{rec['window_start_from_ref_s']} s, every {start.get('every_s')} s, each request a new connection "
             f"bounded at {start.get('timeout_s')} s; a stall = past {stall_s:g} s without a byte"
             + ("" if end else "; the window's end is not in the log (the step's wait ended first)")]
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
    rec["lines"] = lines
    return rec


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.bootwindow")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("said", help="the boot window's block from the runtime606 Job's log (always exit 0)")
    s.add_argument("--effective", required=True, help="$OUT/go-runtime.jsonl")
    s.add_argument("--pod", default=None, help="$OUT/pod-before.jsonl: the new pod's Ready")
    s.add_argument("--dump", required=True, help="where the goroutine dump (gzip) is written")
    s.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    rec = summarize(a.effective, a.pod, a.dump)
    with open(a.out + ".part", "w") as fh:
        json.dump(rec, fh, indent=1)
    os.replace(a.out + ".part", a.out)
    print("\n".join(rec["lines"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
