"""The portal-origin gate's window and its verdict (#1005).

run606's wait_portal_origin waits for the frontend's config.json
(frontend-config-vars) to name ONE origin for snowplow, authn and sse-proxy,
at 057's paths (load606 portal-origin). The URLs take two hops:
  1. the installer's composition-dynamic-controller (cdc) renders the
     Frontend composition's spec.config from a Helm `lookup` of the platform
     gateway's LoadBalancer address, on every resync (its Observe renders the
     chart);
  2. the frontend's own cdc renders frontend-config-vars from that spec.
So the URLs appear within about two resync intervals after the gateway has
its address.

THE WINDOW is the gate's own, never what the platform step's earlier waits
left. It allows up to ENDPOINT_WAIT_S for the gateway's address, then 2 x the
installer cdc's EFFECTIVE resync interval + one poll, counted from the latest
reading that has the address (it restarts if the address disappears). The
interval is READ from the live cluster, by the cdc's own semantics
(resync_interval):
  - its -resync-interval flag (Go flag: -x=v, --x=v, -x v; the last one
    wins). A malformed value makes the cdc exit at flag parsing, so the gate
    refuses ("cannot start").
  - else COMPOSITION_CONTROLLER_RESYNC_INTERVAL as the container sees it: its
    own `env` (the last entry wins; valueFrom configMapKeyRef resolved)
    shadows `envFrom` (the last source wins; a source's `prefix` applies).
    core-provider creates one ConfigMap per cdc: 60s at installer 0.3.468.
    A value that is set but unparseable gives plumbing env.Duration's
    default, the compiled 3m.
  - else the compiled default (3m: core-provider cdc main.go:85,
    flag.Duration("resync-interval", env.Duration(..., 3m))).
A read that fails is an error with kubectl's message, never "sets no ...".
More than one installers-*-controller Deployment is ambiguous and refused.

THE VERDICT of a gate that runs out is decided from the LAST poll's readings,
all read-only:
  - gateway: the platform gateway Service's address
    (address | no_address | absent | unread);
  - rendered: what the installer RENDERED, the Frontend composition's
    spec.config URL keys (same_origin | not_same_origin | absent | unread);
  - cdc: the installer cdc Deployment's Available condition, which holds
    while its Observe runs every resync (True | False | None = unread).
A helm revision says nothing here: the cdc writes one only when a reconcile
changes a child (#1006 review F1).

    python3 -m bench.origin_gate resync --deployments DEPLOY.json --configmaps CM.json
    python3 -m bench.origin_gate reading --t T --gateway-json F --gateway-rc N --frontends-json F --frontends-rc N \\
        --deployments-json F
    python3 -m bench.origin_gate verdict --readings WAIT.jsonl --resync RESYNC.json --why WHY
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

#: the cdc's compiled default resync (core-provider cdc main.go:85: 3m)
COMPILED_DEFAULT_S = 180.0
#: the cdc that renders installer-platform: core-provider names a cdc's
#: Deployment `<resource>-<version>-controller`, and the Installer's resource is `installers`
CDC_RESOURCE = "installers"
ENV = "COMPOSITION_CONTROLLER_RESYNC_INTERVAL"
FLAG = "resync-interval"
_DUR = re.compile(r"(\d+(?:\.\d+)?)(h|ms|m|s|us|µs|ns)")
_UNIT = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}


def parse_duration(s) -> float | None:
    """A Go duration ("60s", "3m", "1m30s") in seconds; None if it is not one."""
    s = str(s if s is not None else "").strip()
    if not s:
        return None
    pos, total = 0, 0.0
    for m in _DUR.finditer(s):
        if m.start() != pos:
            return None
        total += float(m.group(1)) * _UNIT[m.group(2)]
        pos = m.end()
    return total if pos == len(s) and total > 0 else None


def _flag_value(args: list) -> tuple[bool, str | None]:
    """(present, its last value) of the -resync-interval flag, Go flag style."""
    present, value, i = False, None, 0
    args = [str(a) for a in args or []]
    while i < len(args):
        m = re.match(rf"^--?{FLAG}(?:=(.*))?$", args[i])
        if m:
            present = True
            if m.group(1) is not None:
                value = m.group(1)
            else:
                value = args[i + 1] if i + 1 < len(args) else None
                i += 1
        i += 1
    return present, value


def _container_env(c: dict, configmaps: dict) -> tuple[str | None, str]:
    """(value, where) of ENV as the container sees it: env (last wins,
    valueFrom configMapKeyRef resolved) shadows envFrom (last source wins,
    prefix applied). (None, ...) when it is not set."""
    for e in reversed(c.get("env") or []):
        if e.get("name") != ENV:
            continue
        if "value" in e:
            return str(e.get("value") or ""), "env"
        ref = ((e.get("valueFrom") or {}).get("configMapKeyRef") or {})
        if ref:
            data = configmaps.get(ref.get("name"))
            if data is None:
                raise LookupError(f"env {ENV} valueFrom ConfigMap {ref.get('name')} was not read")
            return str(data.get(ref.get("key"), "")), f"env valueFrom ConfigMap {ref.get('name')}"
        raise LookupError(f"env {ENV} comes from a source the gate does not read ({sorted(e.get('valueFrom') or {})})")
    for ef in reversed(c.get("envFrom") or []):
        name = (ef.get("configMapRef") or {}).get("name")
        if not name:
            continue
        prefix = ef.get("prefix") or ""
        if not ENV.startswith(prefix):
            continue
        data = configmaps.get(name)
        if data is None:
            raise LookupError(f"envFrom ConfigMap {name} was not read")
        key = ENV[len(prefix):]
        if key in data:
            return str(data[key]), f"envFrom ConfigMap {name}" + (f" (prefix {prefix})" if prefix else "")
    return None, ""


def resync_interval(deployments: dict | None, configmaps: dict | None, read_error: str = "") -> dict:
    """{seconds, source, deployment, refuse}: the installer cdc's EFFECTIVE
    resync interval. `refuse`: why the gate cannot go on (ambiguous cdc, or a
    cdc that cannot start)."""
    if deployments is None:
        return {"seconds": COMPILED_DEFAULT_S, "deployment": None, "refuse": None,
                "source": f"UNREAD (the Deployments were not read: {read_error or 'unknown error'}); "
                          f"sized for the cdc's compiled default"}
    cdcs = [d for d in deployments.get("items") or []
            if ((d.get("metadata") or {}).get("name") or "").startswith(f"{CDC_RESOURCE}-")
            and d["metadata"]["name"].endswith("-controller")]
    if len(cdcs) > 1:
        names = sorted(d["metadata"]["name"] for d in cdcs)
        return {"seconds": None, "deployment": None, "source": f"{len(cdcs)} installer cdcs: {names}",
                "refuse": f"ambiguous: {len(cdcs)} {CDC_RESOURCE}-*-controller Deployments ({names}); "
                          f"which one renders installer-platform is not known"}
    if not cdcs:
        return {"seconds": COMPILED_DEFAULT_S, "deployment": None, "refuse": None,
                "source": f"the cdc's compiled default (no {CDC_RESOURCE}-*-controller Deployment found)"}
    d = cdcs[0]
    name = d["metadata"]["name"]
    cms = None if configmaps is None else {(c.get("metadata") or {}).get("name"): c.get("data") or {}
                                           for c in configmaps.get("items") or []}
    containers = (((d.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
    for c in containers:
        present, v = _flag_value(c.get("args"))
        if present:
            sec = parse_duration(v)
            if sec is None:
                return {"seconds": None, "deployment": name, "source": f"{name} arg -{FLAG}={v}",
                        "refuse": f"{name}: -{FLAG}={v!r} is not a duration: the cdc exits at flag parsing, "
                                  f"nothing renders installer-platform"}
            return {"seconds": sec, "deployment": name, "refuse": None, "source": f"{name} arg -{FLAG}={v}"}
    for c in containers:
        try:
            v, where = _container_env(c, cms if cms is not None else _Unread())
        except LookupError as e:
            return {"seconds": COMPILED_DEFAULT_S, "deployment": name, "refuse": None,
                    "source": f"UNREAD ({name}: {e}{'; ConfigMaps: ' + read_error if read_error else ''}); "
                              f"sized for the cdc's compiled default"}
        if v is not None:
            sec = parse_duration(v)
            if sec is None:
                return {"seconds": COMPILED_DEFAULT_S, "deployment": name, "refuse": None,
                        "source": f"{name} {where} {ENV}={v!r} is unparseable: the cdc's compiled default "
                                  f"(plumbing env.Duration)"}
            return {"seconds": sec, "deployment": name, "refuse": None, "source": f"{name} {where} {ENV}={v}"}
    return {"seconds": COMPILED_DEFAULT_S, "deployment": name, "refuse": None,
            "source": f"the cdc's compiled default ({name} sets no -{FLAG} and no {ENV})"}


class _Unread(dict):
    def get(self, k, default=None):
        return None


def deadline(t0: float, t_addr: float | None, resync_s: float, poll_s: float, lb_wait_s: float) -> float:
    """The gate's own deadline: until the gateway has an address, LB
    provisioning's (t0 + lb_wait_s); from the latest reading that has one, two
    resyncs of the cdc and the poll that reads their result."""
    if t_addr is None:
        return t0 + lb_wait_s
    return t_addr + 2 * resync_s + poll_s


def _json(path: str):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def reading(t: float, gateway_json: str, gateway_rc: int, frontends_json: str, frontends_rc: int,
            deployments_json: str, err: dict | None = None) -> dict:
    """One poll's readings, each one READ or saying it was not (#1006 F3)."""
    from bench import load606
    err = err or {}
    g = _json(gateway_json) if gateway_rc == 0 else None
    if gateway_rc != 0:
        gw = "absent" if "NotFound" in (err.get("gateway") or "") else "unread"
    elif not isinstance(g, dict):
        gw = "unread"
    else:
        ing = ((g.get("status") or {}).get("loadBalancer") or {}).get("ingress") or []
        gw = "address" if any(i.get("ip") or i.get("hostname") for i in ing) else "no_address"
    f = _json(frontends_json) if frontends_rc == 0 else None
    if not isinstance(f, dict):
        rendered = "unread"
    elif not f.get("items"):
        rendered = "absent"
    else:
        cfg = (f["items"][0].get("spec") or {}).get("config") or {}
        ok = load606.portal_origin(json.dumps({"api": cfg}))["origin"] is not None
        rendered = "same_origin" if ok else "not_same_origin"
    d = _json(deployments_json)
    cdc = None
    if isinstance(d, dict):
        cdcs = [x for x in d.get("items") or [] if ((x.get("metadata") or {}).get("name") or "").startswith(
            f"{CDC_RESOURCE}-") and x["metadata"]["name"].endswith("-controller")]
        if len(cdcs) == 1:
            conds = (cdcs[0].get("status") or {}).get("conditions") or []
            cdc = any(c.get("type") == "Available" and c.get("status") == "True" for c in conds)
    return {"t": t, "gateway": gw, "rendered": rendered, "cdc_available": cdc,
            "errors": {k: v[:300] for k, v in err.items() if v}}


def verdict(readings: list[dict], resync: dict, why: str) -> tuple[str, str]:
    """(kind, message) for a gate that ran out, from its LAST poll."""
    tail = f"config.json: {why}; installer cdc resync {resync.get('seconds')}s ({resync.get('source')})"
    if not readings:
        return "no_reading", f"the gate took no reading; {tail}"
    last = readings[-1]
    errs = "; ".join(f"{k}: {v}" for k, v in (last.get("errors") or {}).items())
    gw = last["gateway"]
    if gw == "unread":
        return "gateway_unread", f"the platform gateway Service could not be read ({errs or 'no answer'}); {tail}"
    if gw == "absent":
        return "gateway_absent", f"the platform gateway Service does not exist; {tail}"
    if gw == "no_address":
        span = last["t"] - readings[0]["t"]
        return "lb_pending", f"waiting on GCP: the platform gateway Service has no LoadBalancer address after {span:.0f}s; {tail}"
    # how long the address has held: since the first reading of the trailing run of readings with
    # it (an address that disappeared and came back restarts the count, #1006 F7)
    first = len(readings) - 1
    while first > 0 and readings[first - 1]["gateway"] == "address":
        first -= 1
    since = last["t"] - readings[first]["t"]
    r = last["rendered"]
    if r == "unread":
        return "rendered_unread", (f"the gateway has an address; the Frontend composition could not be read "
                                   f"({errs or 'no answer'}); {tail}")
    if r == "same_origin":
        return "frontend_not_propagated", (f"the installer RENDERED the one origin into the Frontend composition, "
                                           f"but frontend-config-vars does not carry it after {since:.0f}s with the "
                                           f"address: the frontend's own rendering lags or fails; {tail}")
    if last.get("cdc_available") is None:
        return "installer_cdc_unread", (f"the gateway has an address; the Frontend composition is {r}; the "
                                        f"installer cdc's availability could not be read; {tail}")
    if last["cdc_available"] is False:
        return "installer_cdc_down", (f"the gateway has an address, but the installer cdc is not Available: "
                                      f"nothing renders installer-platform (Frontend composition {r}); {tail}")
    return "installer_defect", (f"INSTALLER DEFECT: the installer cdc is Available and the gateway has had its "
                                f"address for {since:.0f}s (2 x its resync), yet the Frontend composition it "
                                f"renders is {r} (not one origin at /content, /auth, /events); {tail}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.origin_gate")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("resync")
    r.add_argument("--deployments", required=True, help="kubectl get deploy -o json (krateo-system)")
    r.add_argument("--deployments-rc", type=int, default=0)
    r.add_argument("--configmaps", required=True, help="kubectl get configmap -o json (krateo-system)")
    r.add_argument("--configmaps-rc", type=int, default=0)
    r.add_argument("--errors", default="", help="a file holding the reads' kubectl errors")
    dl = sub.add_parser("deadline")
    for f in ("--t0", "--resync", "--poll", "--lb-wait"):
        dl.add_argument(f, type=float, required=True)
    dl.add_argument("--t-addr", default="")
    rd = sub.add_parser("reading")
    rd.add_argument("--t", type=float, required=True)
    for f in ("--gateway-json", "--frontends-json", "--deployments-json"):
        rd.add_argument(f, required=True)
    rd.add_argument("--gateway-rc", type=int, required=True)
    rd.add_argument("--frontends-rc", type=int, required=True)
    rd.add_argument("--gateway-err", default="")
    rd.add_argument("--frontends-err", default="")
    v = sub.add_parser("verdict")
    v.add_argument("--readings", required=True)
    v.add_argument("--resync", required=True)
    v.add_argument("--why", required=True)
    nw = sub.add_parser("now", help="the gate's clock: wall time, or BENCH_GATE_CLOCK_STEP_S per call (tests)")
    nw.add_argument("--state", required=True)
    a = ap.parse_args(argv)

    def text(path):
        try:
            with open(path) as fh:
                return fh.read().strip()
        except OSError:
            return ""
    if a.cmd == "now":
        step = os.environ.get("BENCH_GATE_CLOCK_STEP_S")
        if not step:
            import time
            print(f"{time.time():.3f}")
            return 0
        n = int(text(a.state) or 0) + 1
        with open(a.state, "w") as fh:
            fh.write(str(n))
        print(f"{1_800_000_000 + n * float(step):.3f}")
        return 0
    if a.cmd == "deadline":
        t_addr = float(a.t_addr) if a.t_addr else None
        print(f"{deadline(a.t0, t_addr, a.resync, a.poll, a.lb_wait):.3f}")
        return 0
    if a.cmd == "reading":
        print(json.dumps(reading(a.t, a.gateway_json, a.gateway_rc, a.frontends_json, a.frontends_rc,
                                 a.deployments_json, {"gateway": text(a.gateway_err),
                                                      "frontends": text(a.frontends_err)})))
        return 0
    if a.cmd == "resync":
        errors = text(a.errors)
        deps = _json(a.deployments) if a.deployments_rc == 0 else None
        cms = _json(a.configmaps) if a.configmaps_rc == 0 else None
        print(json.dumps(resync_interval(deps, cms, errors)))
        return 0
    readings = []
    try:
        with open(a.readings) as fh:
            readings = [json.loads(ln) for ln in fh if ln.strip()]
    except OSError:
        pass
    kind, msg = verdict(readings, _json(a.resync) or {"seconds": None, "source": "unknown"}, a.why)
    print(json.dumps({"kind": kind, "message": msg}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
