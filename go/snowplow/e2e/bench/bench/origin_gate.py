"""The portal-origin gate's window and its verdict (#1005).

run606's wait_portal_origin waits for the frontend's config.json to name ONE
origin for snowplow, authn and sse-proxy, at 057's paths (load606
portal-origin). The installer writes those URLs from a Helm `lookup` of the
platform gateway Service's LoadBalancer address, re-rendered on every resync
of the installer's composition-dynamic-controller (cdc): its Observe renders
the chart. So the URLs appear within about two resync intervals after the
gateway has its address.

THE WINDOW is the gate's own (never what the platform step's earlier waits
left): up to ENDPOINT_WAIT_S for the gateway's address, then 2 x the cdc's
EFFECTIVE resync interval (+ one poll) from the first reading that has it.
The interval is READ from the live cluster, never assumed (resync_interval):

  1. the cdc container's `-resync-interval` arg;
  2. else COMPOSITION_CONTROLLER_RESYNC_INTERVAL in its env, or in a
     ConfigMap it takes through envFrom (core-provider creates one per cdc,
     `<resource>-<version>-configmap`: 60s at installer 0.3.468);
  3. else the cdc's compiled default (3m: core-provider's chart,
     assets/cdc/deployment.yaml, "the CDC uses its own built-in default
     (1 worker, 3m resync)").

THE VERDICT of a gate that ran out (verdict): two read-only readings taken on
every poll say which failure it is:
  - the platform gateway Service's .status.loadBalancer.ingress: empty = still
    waiting on GCP;
  - `helm history installer-platform` 's revision: advancing = the installer
    is reconciling, frozen = nothing reconciles it.
Reconciling WITH an address, yet config.json still not one origin: an
installer defect, and the message says so.

    python3 -m bench.origin_gate resync --deployments DEPLOY.json --configmaps CM.json
    python3 -m bench.origin_gate verdict --readings WAIT.jsonl --resync RESYNC.json --why WHY
"""

from __future__ import annotations

import argparse
import json
import re
import sys

#: the cdc's compiled default resync (core-provider assets/cdc/deployment.yaml: "3m")
COMPILED_DEFAULT_S = 180.0
#: the cdc that renders installer-platform: core-provider names a cdc's
#: Deployment `<resource>-<version>-controller`, and the Installer's resource is `installers`
CDC_RESOURCE = "installers"
ENV = "COMPOSITION_CONTROLLER_RESYNC_INTERVAL"
_DUR = re.compile(r"(\d+(?:\.\d+)?)(h|ms|m|s|us|µs|ns)")
_UNIT = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}


def parse_duration(s) -> float | None:
    """A Go duration ("60s", "3m", "1m30s") in seconds; None if it is not one."""
    s = str(s or "").strip()
    if not s:
        return None
    pos, total = 0, 0.0
    for m in _DUR.finditer(s):
        if m.start() != pos:
            return None
        total += float(m.group(1)) * _UNIT[m.group(2)]
        pos = m.end()
    return total if pos == len(s) and total > 0 else None


def _cdc(deployments: dict) -> dict | None:
    for d in (deployments or {}).get("items") or []:
        name = (d.get("metadata") or {}).get("name") or ""
        if name.startswith(f"{CDC_RESOURCE}-") and name.endswith("-controller"):
            return d
    return None


def resync_interval(deployments: dict, configmaps: dict) -> dict:
    """{seconds, source, deployment}: the installer cdc's EFFECTIVE resync
    interval, by the cdc's own precedence: its -resync-interval arg, else
    the env (its own, then each envFrom ConfigMap's), else the compiled
    default."""
    d = _cdc(deployments)
    if d is None:
        return {"seconds": COMPILED_DEFAULT_S, "deployment": None,
                "source": f"the cdc's compiled default (no {CDC_RESOURCE}-*-controller Deployment found)"}
    name = d["metadata"]["name"]
    cms = {(c.get("metadata") or {}).get("name"): c.get("data") or {}
           for c in (configmaps or {}).get("items") or []}
    for c in ((d.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers") or []:
        for a in c.get("args") or []:
            m = re.match(r"^--?resync-interval=(.+)$", str(a))
            if m and (sec := parse_duration(m.group(1))) is not None:
                return {"seconds": sec, "deployment": name, "source": f"{name} arg -resync-interval={m.group(1)}"}
        for e in c.get("env") or []:
            if e.get("name") == ENV and (sec := parse_duration(e.get("value"))) is not None:
                return {"seconds": sec, "deployment": name, "source": f"{name} env {ENV}={e['value']}"}
        for ef in c.get("envFrom") or []:
            ref = (ef.get("configMapRef") or {}).get("name")
            v = cms.get(ref, {}).get(ENV)
            if ref and (sec := parse_duration(v)) is not None:
                return {"seconds": sec, "deployment": name, "source": f"{name} envFrom ConfigMap {ref} {ENV}={v}"}
    return {"seconds": COMPILED_DEFAULT_S, "deployment": name,
            "source": f"the cdc's compiled default ({name} sets no -resync-interval and no {ENV})"}


def verdict(readings: list[dict], resync: dict, why: str) -> tuple[str, str]:
    """(kind, message) for a gate that ran out. `readings`: one per poll,
    {t, address: bool, revision: int | None}."""
    addr = [r for r in readings if r.get("address")]
    revs = [r["revision"] for r in readings if isinstance(r.get("revision"), int)]
    span = (readings[-1]["t"] - readings[0]["t"]) if readings else 0
    tail = f"config.json: {why}; resync {resync['seconds']:g}s ({resync['source']})"
    if not addr:
        return "lb_pending", (f"waiting on GCP: the platform gateway Service has no LoadBalancer address after "
                              f"{span:.0f}s; {tail}")
    after = [r["revision"] for r in readings if r["t"] >= addr[0]["t"] and isinstance(r.get("revision"), int)]
    if len(after) < 2:
        return "revision_unknown", (f"the gateway has an address, but installer-platform's helm revision was not "
                                    f"read twice after it (revisions seen: {revs}); {tail}")
    if after[-1] > after[0]:
        return "installer_defect", (f"INSTALLER DEFECT: installer-platform reconciled (revision {after[0]} -> "
                                    f"{after[-1]}) with the gateway's address present, yet config.json is not one "
                                    f"origin at /content, /auth, /events; {tail}")
    return "not_reconciling", (f"nothing reconciles the installer: installer-platform stayed at revision "
                               f"{after[0]} for {readings[-1]['t'] - addr[0]['t']:.0f}s after the gateway had its "
                               f"address; {tail}")


def deadline(t0: float, t_addr: float | None, resync_s: float, poll_s: float, lb_wait_s: float) -> float:
    """The gate's own deadline: until the gateway has an address, LB
    provisioning's (t0 + lb_wait_s); from the first reading with one, two
    resyncs of the cdc and the poll that reads their result."""
    if t_addr is None:
        return t0 + lb_wait_s
    return t_addr + 2 * resync_s + poll_s


def reading(t: float, ingress: str, history: str) -> dict:
    """One poll's readings: the gateway has an address (its ingress, as
    kubectl's jsonpath printed it), and installer-platform's revision (from
    `helm history --max 1 -o json`; None when helm did not answer)."""
    rev = None
    try:
        h = json.loads(history or "null")
        if isinstance(h, list) and h and isinstance(h[-1].get("revision"), int):
            rev = h[-1]["revision"]
    except (ValueError, AttributeError):
        pass
    return {"t": t, "address": bool((ingress or "").strip()), "revision": rev}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.origin_gate")
    sub = ap.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("deadline")
    for f in ("--t0", "--resync", "--poll", "--lb-wait"):
        dl.add_argument(f, type=float, required=True)
    dl.add_argument("--t-addr", default="")
    rd = sub.add_parser("reading")
    rd.add_argument("--t", type=float, required=True)
    rd.add_argument("--ingress", default="")
    rd.add_argument("--history", default="")
    r = sub.add_parser("resync")
    r.add_argument("--deployments", required=True, help="kubectl get deploy -o json (krateo-system)")
    r.add_argument("--configmaps", required=True, help="kubectl get configmap -o json (krateo-system)")
    v = sub.add_parser("verdict")
    v.add_argument("--readings", required=True)
    v.add_argument("--resync", required=True)
    v.add_argument("--why", required=True)
    a = ap.parse_args(argv)

    def load(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return {}
    if a.cmd == "deadline":
        t_addr = float(a.t_addr) if a.t_addr else None
        print(f"{deadline(a.t0, t_addr, a.resync, a.poll, a.lb_wait):.3f}")
        return 0
    if a.cmd == "reading":
        print(json.dumps(reading(a.t, a.ingress, a.history)))
        return 0
    if a.cmd == "resync":
        print(json.dumps(resync_interval(load(a.deployments), load(a.configmaps))))
        return 0
    readings = []
    try:
        with open(a.readings) as fh:
            readings = [json.loads(ln) for ln in fh if ln.strip()]
    except OSError:
        pass
    kind, msg = verdict(readings, load(a.resync) or {"seconds": COMPILED_DEFAULT_S, "source": "unknown"}, a.why)
    print(json.dumps({"kind": kind, "message": msg}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
