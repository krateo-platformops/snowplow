"""Run 24: the Go runtime settings snowplow was measured with. Report only: evaluate shows them as
INFO lines with the verdict and never judges them.

Run 23 compared one snowplow on Go 1.26 with the same code on Go 1.27.3, and nothing in either
run's evidence said what GOGC, GOMEMLIMIT or GOMAXPROCS the process ran with. Two views:

  the container's environment: every GO* key (GOGC, GOMEMLIMIT, GOMAXPROCS, GODEBUG, any other)
      of the live snowplow container, read in the same reading, from the same env sources and by
      the same rules as tracing's OTEL_* keys (bench/tracing.py: env over envFrom, a later source
      over an earlier one, a ConfigMap changed after the container started proves nothing, #1276's
      pre-install reading for an absent optional one). ONLY those names and their values are
      kept: never another key, never a Secret's name or value. A key a Secret could set, or a
      source whose content at the container's start is unknowable, reads UNKNOWN. A GO* key is a
      name of "GO" and upper-case letters or digits (no underscore: Go's own, GOGC ... GOTRACEBACK);
      GOOGLE_* and the like are another tool's and are never read;
  the runtime's own view, from mem606's /debug/vars samples: memstats NextGC (its peak) and
      GCCPUFraction (the last sample: it is cumulative since the process started) and NumGC.
      snowplow publishes no GOGC, GOMEMLIMIT or GOMAXPROCS value on /debug/vars (memstats has
      none; no runtime/metrics expvar: load606.SCHED_LATENCY_KEY says the same): said, never a
      number.

A key no source sets is unset, and the runtime's default applies: GOGC 100, GOMEMLIMIT no limit
(math.MaxInt64), GOMAXPROCS the runtime's own choice (since Go 1.25 the container's cgroup CPU
limit when it is lower than the node's CPUs).
"""

from __future__ import annotations

import json
import re

UNKNOWN = "UNKNOWN"
#: resolved in every reading whether or not a source names them
GO_KEYS = ("GOGC", "GOMEMLIMIT", "GOMAXPROCS")
GO_DEFAULTS = {"GOGC": "the runtime's default, 100",
               "GOMEMLIMIT": "the runtime's default, no limit",
               "GOMAXPROCS": "the runtime's default (Go >= 1.25: the cgroup CPU limit when lower than the node's CPUs)"}
_GO_NAME = re.compile(r"^GO[A-Z0-9]+$")


def is_go_key(name) -> bool:
    return isinstance(name, str) and bool(_GO_NAME.match(name)) and not name.startswith("GOOGLE")


def go_env(pod: dict, container: dict, cms: dict | None, started, resolve, configmaps_why, pre) -> dict:
    """The GO* keys of one reading (tracing.reading calls it with its own resolver)."""
    from bench import tracing
    sources, names = tracing.env_sources(pod, container, cms, started, is_go_key, "go",
                                         configmaps_why=configmaps_why, pre=pre)
    eff = {k: resolve(k) for k in sorted(names | set(GO_KEYS))}
    secret = any(s.get("kind") == "secretRef" for s in sources)
    return {"state": "read", "effective": eff, "env_sources": sources,
            **({"secret_source": "an envFrom Secret could set further GO* keys (never read)"} if secret else {})}


def _key_text(k: str, r: dict) -> str:
    if r.get("state") == UNKNOWN:
        return f"{k} UNKNOWN ({r.get('why')})"
    if r.get("state") == "set":
        return f"{k}={r.get('value')} ({r.get('from')})"
    return f"{k} unset ({GO_DEFAULTS.get(k, 'the runtime default')})"


def reading_text(g: dict | None) -> str:
    if not g:
        return "UNKNOWN (the reading carries no GO* keys: a reading before run 24)"
    if g.get("state") == UNKNOWN:
        return f"UNKNOWN ({g.get('why')})"
    eff = g.get("effective") or {}
    order = [k for k in GO_KEYS if k in eff] + sorted(k for k in eff if k not in GO_KEYS)
    s = ", ".join(_key_text(k, eff[k]) for k in order)
    return s + (f"; {g['secret_source']}" if g.get("secret_source") else "")


def runtime_view(mem: list[dict] | None) -> dict:
    """The runtime's own numbers from mem606's samples (kind mem)."""
    rows = sorted((m for m in mem or [] if m.get("kind") == "mem"), key=lambda m: m.get("t") or 0)
    nxt = [m["next_gc_mb"] for m in rows if m.get("next_gc_mb") is not None]
    frac = [m["gc_cpu_fraction"] for m in rows if m.get("gc_cpu_fraction") is not None]
    ngc = [m["num_gc"] for m in rows if m.get("num_gc") is not None]
    return {"samples": len(rows), "next_gc_mb_max": max(nxt) if nxt else None,
            "gc_cpu_fraction_last": frac[-1] if frac else None, "num_gc_last": ngc[-1] if ngc else None,
            "settings_on_debug_vars": "NOT_EXPOSED",
            "why": "snowplow's /debug/vars publishes no GOGC, GOMEMLIMIT or GOMAXPROCS (memstats has none, "
                   "and there is no runtime/metrics expvar)"}


PHASES = ("before", "after")


def report(tracing_path: str | None, mem: list[dict] | None) -> dict:
    """evaluate's go_runtime_report_only: INFO lines, never a judged result."""
    doc, why = None, None
    if not tracing_path:
        why = "evaluate was given no tracing.json"
    else:
        try:
            with open(tracing_path) as fh:
                doc = json.load(fh)
        except FileNotFoundError:
            why = "no tracing.json: the run recorded no reading"
        except (OSError, ValueError) as e:
            why = f"tracing.json is unreadable ({type(e).__name__})"
    if doc is not None and (not isinstance(doc, dict) or doc.get("kind") != "tracing"):
        doc, why = None, "tracing.json is not a tracing record"
    view = runtime_view(mem)
    vline = (f"INFO Go runtime, its own view ({view['samples']} /debug/vars samples): NextGC max "
             f"{view['next_gc_mb_max']} MB, GCCPUFraction {view['gc_cpu_fraction_last']} (last), NumGC "
             f"{view['num_gc_last']}; GOGC / GOMEMLIMIT / GOMAXPROCS values: not exposed ({view['why']})")
    if doc is None:
        return {"result": "INFO", "state": UNKNOWN, "why": why, "runtime_view": view,
                "lines": [f"INFO Go runtime env: UNKNOWN ({why})", vline]}
    readings = {ph: ((doc.get("readings") or {}).get(ph) or {}).get("go_env") for ph in PHASES}
    lines = [f"INFO Go runtime env {ph} the measure: " + reading_text(readings[ph]) for ph in PHASES]
    b, a = readings["before"], readings["after"]
    changed = None
    if b and a and b.get("state") == a.get("state") == "read":
        keys = sorted(set(b.get("effective") or {}) | set(a.get("effective") or {}))
        changed = [k for k in keys if (b["effective"].get(k) or {}).get("value") != (a["effective"].get(k) or {}).get("value")
                   or (b["effective"].get(k) or {}).get("state") != (a["effective"].get(k) or {}).get("state")]
        if changed:
            lines.append("INFO Go runtime env changed during the measure: " + ", ".join(changed))
    lines.append(vline)
    known = [r for r in readings.values() if r and r.get("state") == "read"]
    return {"result": "INFO", "state": "read" if known else UNKNOWN, "readings": readings,
            "changed": changed, "runtime_view": view, "lines": lines}
