"""#1026: the LoadBalancer Services a bench run's install creates, derived
from what the run installs: the installer chart the source step PULLED and
the values the run installs it with ($OUT/installer-values.yaml).

Why the number matters: GKE's service controller reserves a regional static
address (STATIC_ADDRESSES) for every LoadBalancer Service while it builds its
load balancer. Run 19 attempt 3 found the region's 8 all RESERVED by leaked
addresses of earlier runs and waited 45 min for the gateway's. gke-up (and
run606_pair.sh for both runs) refuses a run whose LoadBalancer Services do
not fit the region's STATIC_ADDRESSES headroom; this module says how many
that is.

The rule is the installer's own exposure contract, read from the pulled chart
(templates/compositions.yaml, values.yaml `exposure`), applied to the chart's
pins (files/component-pins.yaml) and the run's values:

  - a component is installed when inst.featureEnabled says so: no feature,
    a feature in inst.coreFeatures, or a feature the values turn on (the
    chart's values.yaml under the run's); registerOnly pins never are;
  - exposure.type LoadBalancer: every installed `expose: true` component but
    agentgateway-policies gets `service.type: LoadBalancer` (one Service
    each, its svcMatch); NodePort and Gateway flip none to LoadBalancer
    (Gateway forces them to ClusterIP);
  - agentgateway-policies, when installed, makes a Gateway whose proxy
    Service the agentgateway controller provisions as a LoadBalancer (its
    default); with exposure.internalDataPlane that one is ClusterIP and the
    SEPARATE platform Gateway's Service `<svcMatch>-platform` is the
    LoadBalancer instead: one either way;
  - componentValues override the injection ("a componentValues override
    always wins"): every `type: LoadBalancer` in an installed component's
    componentValues is one more LoadBalancer Service, and a
    `service.type` other than LoadBalancer there un-flips an exposed one.

What this rule cannot see — a component chart that makes a LoadBalancer
Service by its own default, or a chart-specific switch (krateo-observability's
hyperdxLoadBalancer) — is caught by the run itself: the teardown records the
LoadBalancer Services the cluster really had next to this number
(gke-down --lb-need), and says UNDER-COUNTED when they were more.

    python3 -m bench.lb_need --installer-pulled D --values V [--out F]

prints the number on stdout, one line per Service on stderr; exit 2 (and
why) when the chart cannot be read.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

GATEWAY_COMPONENT = "agentgateway-policies"


def _load(path: str):
    import yaml
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def _merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in (over or {}).items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _lb_types(values, path=()) -> list[str]:
    """The dotted path of every `type: LoadBalancer` in a values tree."""
    out = []
    if isinstance(values, dict):
        if values.get("type") == "LoadBalancer":
            out.append(".".join(path) or "<top>")
        for k, v in values.items():
            out += _lb_types(v, path + (str(k),))
    elif isinstance(values, list):
        for i, v in enumerate(values):
            out += _lb_types(v, path + (str(i),))
    return out


def planned(installer_pulled: str, values_file: str) -> list[dict]:
    """[{component, service, why}] — one row per LoadBalancer Service the
    install creates, by the installer's exposure contract (module doc)."""
    from bench import diag606
    root = os.path.join(installer_pulled, "installer")
    core = set(diag606.core_features(installer_pulled))
    pins = _load(os.path.join(root, "files", "component-pins.yaml")).get("components") or []
    try:
        chart_values = _load(os.path.join(root, "values.yaml"))
    except OSError:
        chart_values = {}
    run_values = _load(values_file)
    vals = _merge(chart_values, run_values)
    features = dict(vals.get("features") or {})
    exposure = dict(vals.get("exposure") or {})
    etype = exposure.get("type") or "LoadBalancer"     # the chart's default (values.yaml)
    cvals = dict(vals.get("componentValues") or {})
    rows = []
    for c in pins:
        if not isinstance(c, dict) or c.get("registerOnly"):
            continue
        f = c.get("feature") or ""
        if f and f not in core and not features.get(f, False):
            continue
        name = str(c.get("name"))
        svc = c.get("svcMatch") or name
        injected: dict = {}
        if c.get("expose") and name != GATEWAY_COMPONENT and not c.get("serviceValuesPath"):
            injected = {"service": {"type": etype if etype in ("LoadBalancer", "NodePort") else "ClusterIP"}}
        elif c.get("expose") and name != GATEWAY_COMPONENT and etype in ("LoadBalancer", "NodePort"):
            nested: dict = {"type": etype}
            for part in reversed(str(c["serviceValuesPath"]).split(".")):
                nested = {part: nested}
            injected = nested
        eff = _merge(injected, cvals.get(name) or {})
        for path in _lb_types(eff):
            why = ("exposure.type LoadBalancer" if path in _lb_types(injected) and
                   path not in _lb_types(cvals.get(name) or {}) else f"componentValues.{name}.{path}")
            rows.append({"component": name, "service": svc if path == "service" else f"{svc} ({path})",
                         "why": why})
        if name == GATEWAY_COMPONENT:
            platform = bool(exposure.get("internalDataPlane"))
            rows.append({"component": name, "service": f"{svc}-platform" if platform else svc,
                         "why": "the platform Gateway's proxy Service (exposure.internalDataPlane)" if platform
                         else "the gateway's proxy Service (the agentgateway controller's default type)"})
    return rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.lb_need")
    p.add_argument("--installer-pulled", required=True)
    p.add_argument("--values", required=True)
    p.add_argument("--out", default=None, help="also write {need, services} here")
    a = p.parse_args(argv)
    try:
        rows = planned(a.installer_pulled, a.values)
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"the LoadBalancer Services this install creates could not be derived: {type(e).__name__}: {e}",
              file=sys.stderr)
        return 2
    for r in rows:
        print(f"LoadBalancer Service: {r['service']} ({r['component']}: {r['why']})", file=sys.stderr)
    print(f"this install creates {len(rows)} LoadBalancer Service(s), each needs a regional static address "
          f"while GKE builds its load balancer", file=sys.stderr)
    if a.out:
        with open(a.out, "w") as fh:
            json.dump({"need": len(rows), "services": rows}, fh, indent=1)
    print(len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
