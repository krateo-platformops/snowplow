"""Run 23: was snowplow tracing while it was measured? The tracing state of the measured pod, as
evidence ($OUT/tracing.json). Report only: evaluate shows it with the verdict and never judges it.

    python3 -m bench.tracing read --phase before|after --out OUT/tracing.json \\
        [--namespace krateo-system] [--selector app.kubernetes.io/name=snowplow] [--timeout-s 300] -- kubectl ...

Before run 23 nothing in a run's evidence said whether tracing was on. The chart turns it on
(OTEL_ENABLED "true", OTEL_TRACES_SAMPLER "parentbased_always_on", through configmap/snowplow and
envFrom; the exporter at http://$(HOST_IP):4318, the node-local collector agent's hostPort), and
Observability needs snowplow's cost of spanning every request at bench rate. A run that measured
an image with tracing off, or tracing on with nothing to receive the spans, must say so.

Each reading (run606 takes one with each snowplow pod record: `before` the measure, `after` it, at
collect) holds three things, read with kubectl only (nothing probes the network):

  the effective OTEL_* keys of the live pod's snowplow container: every env source the container
      names (its envFrom ConfigMaps and env entries, in Kubernetes' precedence: an env entry over
      envFrom, a later envFrom source over an earlier one), the ConfigMaps read from the namespace's
      list. Only OTEL_* names and values are kept, never another key. A Secret is never read and
      never named: an OTEL_* key a Secret could set is UNKNOWN. The container's environment was
      fixed when it STARTED, so a ConfigMap missing now, unreadable, or changed after that start
      (its managedFields times; kubectl --show-managed-fields) proves nothing about what the
      process runs with: what it would set is UNKNOWN. A key no source sets is unset, and then
      snowplow's own default applies (internal/tracing: OTEL_TRACING_ENABLED, defaulting to
      OTEL_ENABLED, defaulting to false; plumbing env.Bool: strconv.ParseBool after TrimSpace, an
      unparsable value is the default; no OTEL_TRACES_SAMPLER = the SDK's parentbased_always_on);
  whether an OTLP receiver is there: when the exporter's endpoint is the node's own IP (HOST_IP is
      status.hostIP), a pod on the snowplow pod's node with that port as a hostPort (or a
      hostNetwork pod with it as a containerPort), Running and Ready. Its owner (a DaemonSet, for
      the collector agent) is recorded. Any other endpoint is UNKNOWN: kubectl cannot tell what
      answers there;
  snowplow's span-export counters: there are none (SPAN_EXPORT below says where that was read).

UNKNOWN is never "off": no reading, an unreadable pod, an absent or unreadable ConfigMap, a
Secret source, or more than one live snowplow pod each read UNKNOWN, with the reason.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime

from bench import identity
from bench.sampler import Runner

ON, OFF, UNKNOWN = "ON", "OFF", "UNKNOWN"
PRESENT, NOT_READY, ABSENT = "PRESENT", "NOT_READY", "ABSENT"
SET, UNSET = "set", "unset"

#: the keys snowplow's tracing reads (internal/tracing/tracing.go; the SDK's sampler env), resolved
#: in every reading whether or not a source names them; any other OTEL_* key a source sets is kept too
TRACING_KEYS = ("OTEL_ENABLED", "OTEL_TRACING_ENABLED", "OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG",
                "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
SDK_DEFAULT_SAMPLER = "parentbased_always_on"
#: the otlptracehttp default port when an endpoint names none (http and https alike)
OTLP_HTTP_PORT = 4318
#: Go's strconv.ParseBool
_TRUE = {"1", "t", "T", "TRUE", "true", "True"}
_FALSE = {"0", "f", "F", "FALSE", "false", "False"}

#: snowplow's span-export counters on /debug/vars or /metrics, and their in-run deltas: NONE EXIST.
#: Read in snowplow main at 57aad668 (images 1.12.66 and 1.12.67): internal/tracing and
#: internal/otelresource publish no expvar, no expvar key in the module names otel, span, trace or
#: export, and snowplow serves no /metrics (no promhttp). The OTel SDK's own exporter metric
#: (otel.sdk.exporter.span.exported, otlptracehttp v1.45 internal/observ) exists only with
#: OTEL_GO_X_OBSERVABILITY=true and goes to the global MeterProvider: exported over OTLP to the
#: collector, never readable from the pod. A reading's effective keys show that flag when a source
#: sets it. Nothing here is a number: a build that publishes a counter adds its key here.
SPAN_EXPORT = {
    "state": "NOT_EXPOSED",
    "counters": [],
    "delta": None,
    "why": ("snowplow publishes no span-export counter: no /debug/vars key (internal/tracing and "
            "internal/otelresource publish none) and no /metrics endpoint; the OTel SDK's "
            "otel.sdk.exporter.span.exported is gated by OTEL_GO_X_OBSERVABILITY and exported over OTLP "
            "only, never readable from the pod"),
    "checked_in": "snowplow main 57aad668db90a2cd3119e2e94574049d0e01c34e (go.opentelemetry.io/otel v1.45.0)",
}

_VAR_REF = re.compile(r"\$\(([A-Za-z_][A-Za-z0-9_]*)\)")


def _is_otel(name) -> bool:
    return isinstance(name, str) and name.startswith("OTEL_")


def _ts(s) -> datetime | None:
    if not isinstance(s, str) or not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_bool(v: str):
    v = v.strip()
    return True if v in _TRUE else False if v in _FALSE else None


# ─── one reading ─────────────────────────────────────────────────────────────

def _live_pod(pods: dict | None) -> tuple[dict | None, dict]:
    """The one live snowplow pod (no deletionTimestamp), or why there is none to read."""
    if pods is None:
        return None, {"state": UNKNOWN, "why": "the snowplow pods were not read"}
    live = [p for p in pods.get("items") or [] if not (p.get("metadata") or {}).get("deletionTimestamp")]
    if len(live) != 1:
        return None, {"state": UNKNOWN, "live_pods": [(p.get("metadata") or {}).get("name") for p in live],
                      "why": ("no live snowplow pod" if not live else
                              f"{len(live)} live snowplow pods: one reading cannot say which served the run")}
    return live[0], {}


def _container(pod: dict) -> tuple[dict | None, dict | None]:
    cs = (pod.get("spec") or {}).get("containers") or []
    pick = cs if len(cs) == 1 else [c for c in cs if c.get("name") == "snowplow"]
    if len(pick) != 1:
        return None, None
    st = next((s for s in (pod.get("status") or {}).get("containerStatuses") or []
               if s.get("name") == pick[0].get("name")), None)
    return pick[0], st


def _cm_index(configmaps: dict | None) -> dict | None:
    if configmaps is None:
        return None
    return {(c.get("metadata") or {}).get("name"): c for c in configmaps.get("items") or []}


def _cm_state(name: str, cms: dict | None, started: datetime | None) -> tuple[str, dict | None]:
    """A referenced ConfigMap: read (and unchanged since the container started), or why its data
    proves nothing about the process's environment."""
    if cms is None:
        return "unreadable", None
    cm = cms.get(name)
    if cm is None:
        return "absent", None
    md = cm.get("metadata") or {}
    times = [t for t in (_ts(m.get("time")) for m in md.get("managedFields") or []) if t is not None]
    created = _ts(md.get("creationTimestamp"))
    if created is not None:
        times.append(created)
    if started is None or not md.get("managedFields") or not times:
        return "change_time_unknown", cm
    return ("changed_after_start" if max(times) > started else "read"), cm


_CM_WHY = {"unreadable": "the namespace's ConfigMaps were not read",
           "absent": "it is not there now: what it held when the container started is unknown",
           "changed_after_start": "it changed after the container started: the process runs with what it held then",
           "change_time_unknown": ("when it last changed is unknown (no managedFields times, or the container's "
                                   "start time is unknown): it may have changed after the container started")}


def _field(pod: dict, path: str):
    """The downward-API fields an env entry can name, as the kubelet resolves them."""
    md, st, sp = pod.get("metadata") or {}, pod.get("status") or {}, pod.get("spec") or {}
    simple = {"status.hostIP": st.get("hostIP"), "status.podIP": st.get("podIP"),
              "spec.nodeName": sp.get("nodeName"), "spec.serviceAccountName": sp.get("serviceAccountName"),
              "metadata.name": md.get("name"), "metadata.namespace": md.get("namespace"),
              "metadata.uid": md.get("uid")}
    if path in simple:
        return simple[path]
    m = re.fullmatch(r"metadata\.(labels|annotations)\['([^']+)'\]", path or "")
    return ((md.get(m.group(1)) or {}).get(m.group(2))) if m else None


def _resolver(pod: dict, container: dict, cms: dict | None, started: datetime | None):
    """resolve(name) -> {"state": set|unset|UNKNOWN, "value", "from", "why"}, in the kubelet's order:
    the container's env entries (the last one of a name wins) over its envFrom sources (a later
    source over an earlier one); an optional ConfigMap key that is missing sets nothing."""
    env = container.get("env") or []
    env_from = container.get("envFrom") or []

    def from_env_from(name: str) -> dict:
        for src in reversed(env_from):
            prefix = src.get("prefix") or ""
            if prefix and not name.startswith(prefix):
                continue
            if "secretRef" in src:
                return {"state": UNKNOWN, "why": "an envFrom Secret could set it (Secrets are never read)"}
            ref = src.get("configMapRef")
            if not isinstance(ref, dict):
                return {"state": UNKNOWN, "why": "an envFrom source of an unknown kind"}
            st, cm = _cm_state(ref.get("name"), cms, started)
            label = f"envFrom configMapRef {ref.get('name')}" + (f" (prefix {prefix})" if prefix else "")
            if st != "read":
                return {"state": UNKNOWN, "why": f"{label}: {_CM_WHY[st]}"}
            data = cm.get("data") or {}
            if name[len(prefix):] in data:
                return {"state": SET, "value": data[name[len(prefix):]], "from": label}
        return {"state": UNSET}

    def resolve(name: str, depth: int = 0) -> dict:
        for i in range(len(env) - 1, -1, -1):
            e = env[i]
            if e.get("name") != name:
                continue
            vf = e.get("valueFrom")
            if vf is None:
                return {"state": SET, "value": e.get("value") or "", "from": "env"}
            if "secretKeyRef" in vf:
                return {"state": UNKNOWN, "why": "an env entry takes it from a Secret (Secrets are never read)"}
            if "fieldRef" in vf:
                path = (vf.get("fieldRef") or {}).get("fieldPath")
                v = _field(pod, path)
                return ({"state": SET, "value": v, "from": f"env fieldRef {path}"} if v is not None else
                        {"state": UNKNOWN, "why": f"env fieldRef {path}: not in the pod's reading"})
            ref = vf.get("configMapKeyRef")
            if isinstance(ref, dict):
                st, cm = _cm_state(ref.get("name"), cms, started)
                label = f"env configMapKeyRef {ref.get('name')}/{ref.get('key')}"
                if st != "read":
                    return {"state": UNKNOWN, "why": f"{label}: {_CM_WHY[st]}"}
                data = cm.get("data") or {}
                if ref.get("key") in data:
                    return {"state": SET, "value": data[ref.get("key")], "from": label}
                if ref.get("optional"):
                    return from_env_from(name)
                return {"state": UNKNOWN, "why": f"{label}: the key is not there now, and not optional"}
            return {"state": UNKNOWN, "why": "an env entry of an unknown kind"}
        return from_env_from(name)

    return resolve


def _expand(value: str, resolve, before: str) -> tuple[str | None, str | None]:
    """$(VAR) as the kubelet expands it in an env entry: VAR must be defined; else it stays literal."""
    missing = []

    def sub(m):
        r = resolve(m.group(1))
        if r["state"] == SET:
            return str(r["value"])
        if r["state"] == UNKNOWN:
            missing.append(m.group(1))
        return m.group(0)
    out = _VAR_REF.sub(sub, value)
    return (None, f"{before} names $({missing[0]}), which is UNKNOWN") if missing else (out, None)


def _endpoint(eff: dict, resolve, host_ip) -> dict:
    """The traces endpoint as snowplow configures it: OTEL_EXPORTER_OTLP_ENDPOINT when set (Setup
    passes it as WithEndpointURL, over the SDK's own env), else the SDK reads
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, else its default localhost:4318."""
    for key in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
        r = eff[key]
        if r["state"] == UNKNOWN:
            return {"state": UNKNOWN, "key": key, "why": r["why"]}
        if r["state"] == SET:
            raw = str(r["value"])
            expanded, why = _expand(raw, resolve, key)
            if expanded is None:
                return {"state": UNKNOWN, "key": key, "raw": raw, "why": why}
            m = re.match(r"^(?:([a-z][a-z0-9+.-]*)://)?(\[[^\]]*\]|[^/:]*)(?::(\d+))?", expanded)
            host, port = (m.group(2), int(m.group(3)) if m.group(3) else OTLP_HTTP_PORT) if m else (None, None)
            return {"state": SET, "key": key, "raw": raw, "expanded": expanded, "host": host, "port": port,
                    "node_local": bool(host and host_ip and host == host_ip)}
    return {"state": UNSET, "key": None, "host": "localhost", "port": OTLP_HTTP_PORT, "node_local": False,
            "why": "no endpoint key is set: the SDK's default, localhost:4318"}


def _receiver(endpoint: dict, node: str | None, node_pods: dict | None, node_pods_why: str | None) -> dict:
    if endpoint["state"] == UNKNOWN:
        return {"state": UNKNOWN, "why": f"the endpoint is UNKNOWN ({endpoint['why']})"}
    if not endpoint.get("node_local"):
        return {"state": UNKNOWN, "why": (f"the endpoint ({endpoint.get('expanded') or endpoint.get('host')}) is "
                                          f"not the node's IP: kubectl cannot tell what answers there")}
    if node_pods is None:
        return {"state": UNKNOWN, "node": node, "why": node_pods_why or f"the pods on node {node} were not read"}
    port, found = endpoint["port"], []
    for p in node_pods.get("items") or []:
        md, sp, st = p.get("metadata") or {}, p.get("spec") or {}, p.get("status") or {}
        if md.get("deletionTimestamp") or sp.get("nodeName") not in (None, node):
            continue
        hit = None
        for c in sp.get("containers") or []:
            for cp in c.get("ports") or []:
                if (cp.get("protocol") or "TCP") != "TCP":
                    continue
                if cp.get("hostPort") == port:
                    hit = "hostPort"
                elif sp.get("hostNetwork") and cp.get("containerPort") == port:
                    hit = "hostNetwork"
        if hit is None:
            continue
        owner = next(iter(md.get("ownerReferences") or []), {})
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions") or [])
        found.append({"namespace": md.get("namespace"), "pod": md.get("name"), "owner_kind": owner.get("kind"),
                      "owner": owner.get("name"), "phase": st.get("phase"), "ready": ready, "via": hit})
    up = [f for f in found if f["phase"] == "Running" and f["ready"]]
    state = PRESENT if up else NOT_READY if found else ABSENT
    return {"state": state, "node": node, "port": port, "pods": found,
            **({} if up else {"why": (f"no pod on node {node} listens on hostPort {port}" if not found else
                                      f"the pod(s) on hostPort {port} are not Running and Ready")})}


def reading(phase: str, pods: dict | None, configmaps: dict | None, node_pods: dict | None,
            *, pods_why: str | None = None, configmaps_why: str | None = None,
            node_pods_why: str | None = None, t: float | None = None) -> dict:
    """One reading from what kubectl returned (None = that read failed, its *_why says how)."""
    out: dict = {"phase": phase, "t": time.time() if t is None else t}
    pod, why = _live_pod(pods)
    if pod is None:
        why = dict(why, why=pods_why or why["why"]) if pods is None else why
        return {**out, "pod": why, "tracing": {"state": UNKNOWN, "why": why["why"]},
                "sampler": {"state": UNKNOWN, "why": why["why"]}, "endpoint": {"state": UNKNOWN, "why": why["why"]},
                "receiver": {"state": UNKNOWN, "why": why["why"]}, "effective": {}, "env_sources": []}
    container, cstatus = _container(pod)
    md, sp, st = pod.get("metadata") or {}, pod.get("spec") or {}, pod.get("status") or {}
    started_s = (((cstatus or {}).get("state") or {}).get("running") or {}).get("startedAt")
    out["pod"] = {"state": "read", "name": md.get("name"), "node": sp.get("nodeName"), "host_ip": st.get("hostIP"),
                  "container": (container or {}).get("name"), "started_at": started_s,
                  "restarts": (cstatus or {}).get("restartCount")}
    if container is None:
        w = "the pod has no single snowplow container to read"
        return {**out, "tracing": {"state": UNKNOWN, "why": w}, "sampler": {"state": UNKNOWN, "why": w},
                "endpoint": {"state": UNKNOWN, "why": w}, "receiver": {"state": UNKNOWN, "why": w},
                "effective": {}, "env_sources": []}
    cms = _cm_index(configmaps)
    started = _ts(started_s)
    resolve = _resolver(pod, container, cms, started)

    # every env source, OTEL_* only, in the container's order; a Secret source is counted, never named
    sources, names = [], set(TRACING_KEYS)
    for src in container.get("envFrom") or []:
        if "secretRef" in src:
            sources.append({"source": "envFrom", "kind": "secretRef", "state": "not_read"})
            continue
        ref = src.get("configMapRef") or {}
        state, cm = _cm_state(ref.get("name"), cms, started)
        prefix = src.get("prefix") or ""
        otel = {prefix + k: v for k, v in ((cm or {}).get("data") or {}).items() if _is_otel(prefix + k)}
        names |= set(otel)
        sources.append({"source": "envFrom", "kind": "configMapRef", "configmap": ref.get("name"),
                        "prefix": prefix or None, "optional": bool(ref.get("optional")), "state": state,
                        **({"configmaps_why": configmaps_why} if state == "unreadable" and configmaps_why else {}),
                        "otel": otel})
    env_otel = {}
    for e in container.get("env") or []:
        if not _is_otel(e.get("name")):
            continue
        names.add(e["name"])
        vf = e.get("valueFrom")
        if vf is None:
            env_otel[e["name"]] = {"value": e.get("value") or ""}
        elif "secretKeyRef" in vf:
            env_otel[e["name"]] = {"secretKeyRef": True, "state": "not_read"}
        elif "configMapKeyRef" in vf:
            ref = vf["configMapKeyRef"]
            env_otel[e["name"]] = {"configMapKeyRef": {"configmap": ref.get("name"), "key": ref.get("key")}}
        elif "fieldRef" in vf:
            env_otel[e["name"]] = {"fieldRef": (vf.get("fieldRef") or {}).get("fieldPath")}
        else:
            env_otel[e["name"]] = {"unknown_kind": True}
    sources.append({"source": "env", "otel": env_otel})
    eff = {k: resolve(k) for k in sorted(names)}

    # the trace gate, as snowplow resolves it: OTEL_TRACING_ENABLED, default OTEL_ENABLED, default false
    tracing = None
    for key in ("OTEL_TRACING_ENABLED", "OTEL_ENABLED"):
        r = eff[key]
        if r["state"] == UNKNOWN:
            tracing = {"state": UNKNOWN, "why": f"{key}: {r['why']}"}
            break
        if r["state"] == SET:
            b = _parse_bool(str(r["value"]))
            if b is not None:
                tracing = {"state": ON if b else OFF, "why": f"{key}={r['value']}, from {r['from']}"}
                break
    if tracing is None:
        tracing = {"state": OFF, "why": "neither OTEL_TRACING_ENABLED nor OTEL_ENABLED is set to a boolean: "
                                        "snowplow's default, off"}
    s, sa = eff["OTEL_TRACES_SAMPLER"], eff["OTEL_TRACES_SAMPLER_ARG"]
    if s["state"] == UNKNOWN:
        sampler = {"state": UNKNOWN, "why": s["why"]}
    elif s["state"] == SET:
        sampler = {"state": SET, "value": s["value"], "from": s["from"],
                   "arg": sa.get("value") if sa["state"] == SET else None if sa["state"] == UNSET else UNKNOWN}
    else:
        sampler = {"state": UNSET, "value": SDK_DEFAULT_SAMPLER, "why": "OTEL_TRACES_SAMPLER unset: the SDK's default"}
    endpoint = _endpoint(eff, resolve, st.get("hostIP"))
    receiver = _receiver(endpoint, sp.get("nodeName"), node_pods, node_pods_why)
    return {**out, "tracing": tracing, "sampler": sampler, "endpoint": endpoint, "receiver": receiver,
            "effective": eff, "env_sources": sources}


def reading_line(r: dict) -> str:
    """One line: the gate, the sampler, the endpoint, the receiver."""
    tr, sm, ep, rc = r["tracing"], r["sampler"], r["endpoint"], r["receiver"]
    s = f"tracing {tr['state']} ({tr['why']})"
    if sm["state"] == UNKNOWN:
        s += f", sampler UNKNOWN ({sm['why']})"
    else:
        arg = sm.get("arg")
        s += f", sampler {sm['value']}" + (f" arg {arg}" if arg not in (None, "") else "") + \
             (f" ({sm['from']})" if sm["state"] == SET else " (unset: the SDK's default)")
    if ep["state"] == UNKNOWN:
        s += f", endpoint UNKNOWN ({ep['why']})"
    elif ep["state"] == SET:
        s += f", endpoint {ep['raw']}" + (f" = {ep['expanded']}" if ep["expanded"] != ep["raw"] else "") + \
             (" (the node's IP)" if ep["node_local"] else "")
    else:
        s += f", endpoint unset ({ep['why']})"
    if rc["state"] == UNKNOWN:
        s += f", OTLP receiver UNKNOWN ({rc['why']})"
    else:
        who = "; ".join(f"{p['owner_kind'] or 'no owner'} {p['owner'] or p['pod']} in {p['namespace']} "
                        f"({p['phase']}, {'Ready' if p['ready'] else 'not Ready'}, {p['via']} {rc['port']})"
                        for p in rc["pods"])
        s += f", OTLP receiver {rc['state']} on the node" + (f": {who}" if who else f" ({rc['why']})")
    return s


# ─── the file and the report ────────────────────────────────────────────────

def record(path: str, r: dict) -> dict:
    """Add one reading to tracing.json (written whole, then renamed into place)."""
    doc = None
    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        pass
    if not isinstance(doc, dict) or doc.get("kind") != "tracing":
        doc = {"kind": "tracing", "schema": 1, "readings": {}}
    doc["readings"][r["phase"]] = r
    doc["span_export"] = SPAN_EXPORT
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True, default=str)
    os.replace(tmp, path)
    return doc


PHASES = ("before", "after")


def report(path: str | None) -> dict:
    """evaluate's tracing_report_only: INFO lines, never a judged result."""
    doc, why = None, None
    if not path:
        why = "evaluate was given no tracing.json"
    else:
        try:
            with open(path) as fh:
                doc = json.load(fh)
        except FileNotFoundError:
            why = "no tracing.json: the run recorded no reading"
        except (OSError, ValueError) as e:
            why = f"tracing.json is unreadable ({type(e).__name__})"
    if doc is not None and (not isinstance(doc, dict) or doc.get("kind") != "tracing"):
        doc, why = None, "tracing.json is not a tracing record"
    if doc is None:
        return {"result": "INFO", "state": UNKNOWN, "why": why, "span_export": SPAN_EXPORT,
                "lines": [f"INFO tracing: UNKNOWN ({why})", _span_line(SPAN_EXPORT)]}
    readings = doc.get("readings") or {}
    lines = []
    for ph in PHASES:
        r = readings.get(ph)
        lines.append(f"INFO tracing {ph} the measure: " + (reading_line(r) if r else
                                                            "UNKNOWN (no reading at this point)"))
    b, a = readings.get("before"), readings.get("after")
    changed = None
    if b and a:
        keys = (("tracing", "state"), ("sampler", "value"), ("sampler", "arg"), ("endpoint", "expanded"),
                ("receiver", "state"))
        changed = [f"{x}.{y}" for x, y in keys if (b.get(x) or {}).get(y) != (a.get(x) or {}).get(y)]
        if changed:
            lines.append("INFO tracing changed during the measure: " + ", ".join(changed))
    span = doc.get("span_export") or SPAN_EXPORT
    lines.append(_span_line(span))
    states = {((readings.get(ph) or {}).get("tracing") or {}).get("state") or UNKNOWN for ph in PHASES}
    return {"result": "INFO", "state": states.pop() if len(states) == 1 else UNKNOWN,
            "readings": {ph: readings.get(ph) for ph in PHASES}, "changed": changed,
            "span_export": span, "lines": lines}


def _span_line(span: dict) -> str:
    if span.get("state") == "NOT_EXPOSED":
        return f"INFO tracing spans exported: not known ({span['why']})"
    return f"INFO tracing spans exported: {span.get('delta')} ({', '.join(span.get('counters') or [])})"


# ─── the reads ──────────────────────────────────────────────────────────────

def _read(runner: Runner, kubectl: list[str], args: list[str], deadline: float) -> tuple[dict | None, str | None]:
    """(the JSON kubectl answered, None) or (None, why: its error code only, never its text). Every
    read of a reading is bounded by what is left of its ONE deadline (kubectl's own request timeout
    and the runner's wall clock, which kills its process group)."""
    left = math.ceil(deadline - time.monotonic())
    if left < 1:
        return None, f"kubectl {' '.join(args[:4])} was not sent: the reading's bound was spent"
    rc, out, err, timed_out = runner.run(kubectl + [f"--request-timeout={left}s"] + args, left)
    if timed_out:
        return None, f"kubectl {' '.join(args[:4])} did not answer within {left} s"
    if rc != 0:
        return None, f"kubectl exit {rc}: {identity.error_summary(err.decode(errors='replace'))}"
    try:
        doc = json.loads(out)
    except ValueError:
        return None, "kubectl's answer is not JSON"
    return (doc, None) if isinstance(doc, dict) else (None, "kubectl's answer is not a JSON object")


def read_live(phase: str, kubectl: list[str], namespace: str, selector: str, timeout_s: float,
              runner: Runner | None = None) -> dict:
    runner = runner or Runner()
    deadline = time.monotonic() + timeout_s
    pods, pods_why = _read(runner, kubectl, ["-n", namespace, "get", "pods", "-l", selector, "-o", "json"], deadline)
    cms, cms_why = _read(runner, kubectl, ["-n", namespace, "get", "configmaps", "--show-managed-fields",
                                           "-o", "json"], deadline)
    pod, _ = _live_pod(pods)
    node = ((pod or {}).get("spec") or {}).get("nodeName")
    node_pods, node_why = None, None
    if node:
        node_pods, node_why = _read(runner, kubectl, ["get", "pods", "-A", f"--field-selector=spec.nodeName={node}",
                                                      "-o", "json"], deadline)
    elif pod is not None:
        node_why = "the snowplow pod names no node"
    # the namespace's ConfigMaps stay in memory: only OTEL_* keys reach the reading
    return reading(phase, pods, cms, node_pods, pods_why=pods_why, configmaps_why=cms_why, node_pods_why=node_why)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    kubectl = []
    if "--" in argv:
        i = argv.index("--")
        argv, kubectl = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(prog="tracing")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rd = sub.add_parser("read", help="one reading of the live snowplow pod into tracing.json (-- kubectl ...)")
    rd.add_argument("--phase", required=True, choices=PHASES)
    rd.add_argument("--out", required=True)
    rd.add_argument("--namespace", default="krateo-system")
    rd.add_argument("--selector", default="app.kubernetes.io/name=snowplow")
    rd.add_argument("--timeout-s", type=float, default=300.0, help="the reading's ONE bound: its kubectl reads together")
    rp = sub.add_parser("report", help="evaluate's report-only section, as JSON")
    rp.add_argument("file")
    a = ap.parse_args(argv)
    if a.cmd == "report":
        print(json.dumps(report(a.file), indent=1, default=str))
        return 0
    if not kubectl:
        ap.error("read: the kubectl command goes after --")
    r = read_live(a.phase, kubectl, a.namespace, a.selector, a.timeout_s)
    record(a.out, r)
    print(reading_line(r))
    return 0


if __name__ == "__main__":
    sys.exit(main())
