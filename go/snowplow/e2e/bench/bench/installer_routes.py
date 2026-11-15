"""The portal's route table, as the installer chart a run installs renders it: which path of the ONE
portal origin each component is served at, and which key of the frontend's config.json must carry it.

Why: the portal-origin gate held the table as constants ("/content", "/auth", "/events": krateo-057's,
at installer 0.3.616). Installer 0.3.639 moved authn's path-mode prefix from /auth to /authn (its
/auth prefix swallowed the frontend's OIDC callback route /auth?kind=oidc), and the first paid
rehearsal on 0.3.641 waited out the gate and then called the installer defective: the frontend's
config named authn at /authn, exactly as the chart routes it, and the bench expected /auth.

The table is the chart's, never the bench's:
  - the chart's pins declare, per component, `gatewayPath` (its path-mode prefix), `gatewayRewrite`,
    `svcMatch` / `exposePort` (its backend) and `configKeys` (the frontend config keys that carry its
    browser URL). The installer's templates build BOTH things from them: the routes
    (agentgateway-policies' spec.platformRoutes: pathPrefix = gatewayPath) and the frontend's URL
    keys (compositions.yaml: origin + gatewayPath, one per configKey);
  - which components are on is the chart's own rule too: its `inst.compEnabled` define, rendered with
    the run's values by `helm template` (a probe template added to an in-memory copy of the pulled
    chart, as bench.ghcr_pull reads the private-image list). No cluster: the table is known in the
    dry pass, before anything is created;
  - a chart that carries the `inst.platformRoutes` define (0.3.639 on: "so it can be TESTED") has it
    rendered as well, and its route prefixes must be the pins': a chart whose own route table and
    whose pins disagree would route a component at one path and tell the frontend another, and is
    refused here, naming both.

Read from the published charts (2026-11-15): authn is /auth up to 0.3.636 and /authn from 0.3.639;
snowplow /content, sse-proxy /events and the frontend / are the same in 0.3.616 and 0.3.641.

Refused (fail closed), with the reason: a chart that cannot be rendered; one with no
`inst.compEnabled` define (0.3.545: which components are on cannot be asked of it); values that are
not path mode (exposure.type Gateway, gatewayMode path: the bench's portal is one origin); a table
with no route or no frontend key.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys

KIND = "portal_routes"
PROBE_KEY = "table"
ENABLED_DEFINE = "inst.compEnabled"
ROUTES_DEFINE = "inst.platformRoutes"
#: the component whose browser URL the bench's browser and probes call the portal's API through
SNOWPLOW_COMPONENT = "snowplow"


class Refused(ValueError):
    """Why the chart's route table cannot be read."""


def _has_define(root: str, name: str) -> bool:
    tdir = os.path.join(root, "templates")
    needle = f'define "{name}"'
    for f in sorted(os.listdir(tdir)) if os.path.isdir(tdir) else []:
        if f.endswith((".tpl", ".yaml", ".yml")):
            with open(os.path.join(tdir, f), encoding="utf-8", errors="replace") as fh:
                if needle in fh.read():
                    return True
    return False


def _probe(name: str, with_routes: bool) -> str:
    """One ConfigMap whose value is JSON: the pinned components that have a gatewayPath and that the
    chart's own inst.compEnabled says are on, and (when the chart has the define) its own
    inst.platformRoutes for them, at the values' platform port."""
    routes = ('{{- $port := (.Values.exposure.platformPort | default 80) -}}\n'
              '{{- $rendered = (include "' + ROUTES_DEFINE + '" (list $ $all $port false "") | fromYaml).routes '
              '| default list -}}\n') if with_routes else ""
    return ('{{- $all := (include "inst.componentsYaml" $ | fromYaml).components -}}\n'
            '{{- $on := list -}}\n{{- $rendered := list -}}\n'
            '{{- range $c := $all -}}\n'
            '{{- if and $c.gatewayPath (not $c.registerOnly) (eq (include "' + ENABLED_DEFINE
            + '" (list $ $c)) "true") -}}\n{{- $on = append $on $c -}}\n{{- end -}}\n{{- end -}}\n'
            + routes +
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\ndata:\n  " + PROBE_KEY
            + ': {{ dict "components" $on "platformRoutes" $rendered | toJson | quote }}\n')


def _norm(path) -> str:
    """A path prefix as a URL's path compares with it: no trailing slash ("/" is "")."""
    return str(path or "").rstrip("/")


def table(installer_pulled: str, values_file: str, helm: str = "helm") -> dict:
    """The run's portal route table (module doc). Raises Refused."""
    import yaml

    from bench import ghcr_pull, installer_pins
    root = os.path.join(installer_pulled, "installer")
    if not os.path.isfile(os.path.join(root, "Chart.yaml")):
        raise Refused(f"no pulled installer chart at {root}")
    try:
        with open(values_file) as fh:
            values = yaml.safe_load(fh) or {}
        with open(os.path.join(root, "values.yaml")) as fh:
            chart_values = yaml.safe_load(fh) or {}
    except (OSError, yaml.YAMLError) as e:
        raise Refused(f"the run's or the chart's values could not be read ({e})") from None
    exposure = dict(chart_values.get("exposure") or {}, **(values.get("exposure") or {}))
    if exposure.get("type") != "Gateway" or (exposure.get("gatewayMode") or "host") != "path":
        raise Refused(f"the run's values are not path mode (exposure.type={exposure.get('type')!r}, gatewayMode="
                      f"{exposure.get('gatewayMode')!r}): the bench's portal is ONE origin with a path per "
                      f"component (exposure.type Gateway, gatewayMode path)")
    if not _has_define(root, ENABLED_DEFINE):
        raise Refused(f"the installer chart has no {ENABLED_DEFINE} define: which of its components are on cannot "
                      f"be asked of it")
    try:
        _, pins_file = installer_pins.load(root)
    except installer_pins.Refused as e:
        raise Refused(str(e)) from None
    with_routes = _has_define(root, ROUTES_DEFINE)
    probe = f"templates/zz-bench-routes-{secrets.token_hex(4)}.yaml"
    try:
        docs = ghcr_pull.render(ghcr_pull._chart_with(root, {probe: _probe("bench-portal-routes", with_routes)}),
                                ghcr_pull.INSTALLER_RELEASE, values, helm,
                                args=("--kube-version", ghcr_pull.KUBE_VERSION, "--show-only", probe))
        raw = docs[0]["data"][PROBE_KEY] if len(docs) == 1 else None
        doc = json.loads(raw)
    except (ghcr_pull._Refused, OSError) as e:
        raise Refused(f"the installer chart could not be rendered with the run's values: {e}") from None
    except (KeyError, TypeError, ValueError, IndexError):
        raise Refused("the installer chart's render holds no route table (the probe did not render)") from None
    routes, paths = [], {}
    for c in doc.get("components") or []:
        r = {"component": str(c.get("name")), "path": str(c.get("gatewayPath")),
             "rewrite": bool(c.get("gatewayRewrite")), "service": c.get("svcMatch") or c.get("name"),
             "port": c.get("exposePort"), "keys": [str(k) for k in c.get("configKeys") or []]}
        routes.append(r)
        for k in r["keys"]:
            if k in paths and paths[k] != _norm(r["path"]):
                raise Refused(f"the installer's pins give the frontend key {k} two paths ({paths[k] or '/'} and "
                              f"{r['path']})")
            paths[k] = _norm(r["path"])
    if with_routes:
        mine = {r["component"]: _norm(r["path"]) for r in routes}
        theirs = {str(r.get("name")): _norm(r.get("pathPrefix")) for r in doc.get("platformRoutes") or []}
        if mine != theirs:
            diff = sorted(n for n in set(mine) | set(theirs) if mine.get(n) != theirs.get(n))
            raise Refused(
                f"the installer chart's own route table ({ROUTES_DEFINE}) and its pins ({pins_file}: gatewayPath) "
                "disagree: " + "; ".join(f"{n}: routes at {theirs.get(n, 'no route')!r}, pin says "
                                         f"{mine.get(n, 'no gatewayPath')!r}" for n in diff)
                + ": the gateway would route a component at one path and the frontend be told another")
    # THE NEGATIVE (what caught the real bug on the installer's side: /auth, with rewrite, swallowed the
    # frontend's own OIDC callback /auth?kind=oidc): no component's prefix may match a path another
    # component is routed at. A Gateway PathPrefix matches whole path elements (/auth matches /auth and
    # /auth/x, never /authn), and "/" is the catch-all every other prefix is carved out of, so the check
    # is among the non-root prefixes. What the chart cannot say: which paths under "/" the frontend's own
    # router uses (its SPA routes, /auth?kind=oidc among them, are the frontend's code, in no pin), so
    # a prefix that shadows one of THOSE is not something this table can show.
    named = [r for r in routes if _norm(r["path"])]
    for a in named:
        for b in named:
            if a is not b and (_norm(b["path"]) + "/").startswith(_norm(a["path"]) + "/"):
                raise Refused(f"the installer chart routes {a['component']} at {a['path']}, a prefix that also "
                              f"matches {b['path']}, where it routes {b['component']}: one of the two is "
                              f"unreachable ({pins_file}: gatewayPath)")
    if not routes or not paths:
        raise Refused(f"the installer chart renders no portal route with a frontend key for the run's values "
                      f"({len(routes)} route(s), {len(paths)} key(s)): the portal's features are off, or its pins "
                      f"carry no gatewayPath / configKeys")
    snow = next((r for r in routes if r["component"] == SNOWPLOW_COMPONENT and r["keys"]), None)
    if snow is None:
        raise Refused(f"the installer chart routes no {SNOWPLOW_COMPONENT} component with a frontend key: the "
                      f"portal's API has no browser URL")
    return {"kind": KIND, "paths": paths, "routes": routes, "snowplow_key": snow["keys"][0],
            "source": f"the pulled installer chart's {pins_file} (gatewayPath, configKeys), its components on by its "
                      f"own {ENABLED_DEFINE} for the run's values"
                      + (f", and equal to its own {ROUTES_DEFINE}" if with_routes else
                         f" (the chart has no {ROUTES_DEFINE} define: its routes are built inline from the same "
                         f"pins)")}


def said(t: dict) -> str:
    """The table in one line: each route, with the frontend keys that carry it."""
    return "; ".join(f"{r['path']} -> {r['component']} ({r['service']}:{r['port']}"
                     + (f", {', '.join(r['keys'])}" if r["keys"] else "") + ")" for r in t["routes"])


def paths_text(paths: dict) -> str:
    """The expected path of each frontend key, for a message."""
    return ", ".join(f"{k} at {v or '/'}" for k, v in sorted(paths.items()))


def load(path: str) -> dict:
    """A table written by `table` (run606's $OUT/portal-routes.json). Raises Refused."""
    try:
        with open(path) as fh:
            t = json.load(fh)
    except (OSError, ValueError) as e:
        raise Refused(f"the run's portal route table could not be read ({path}: {e})") from None
    if not isinstance(t, dict) or t.get("kind") != KIND or not isinstance(t.get("paths"), dict) or not t["paths"]:
        raise Refused(f"{path} is not a portal route table")
    return t


def main(argv: list[str] | None = None) -> int:
    """`python3 -m bench.installer_routes table --installer-pulled D --values F --out FILE`: the table,
    written and said in one line; exit 1 with the reason when the chart does not give one."""
    ap = argparse.ArgumentParser(prog="python3 -m bench.installer_routes")
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("table")
    t.add_argument("--installer-pulled", required=True)
    t.add_argument("--values", required=True)
    t.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    try:
        tab = table(a.installer_pulled, a.values)
    except Refused as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 1
    tmp = a.out + ".part"
    with open(tmp, "w") as fh:
        json.dump(tab, fh, indent=1)
        fh.write("\n")
    os.replace(tmp, a.out)
    print(f"portal routes, from the installer chart: {said(tab)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
