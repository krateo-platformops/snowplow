"""The installer's composition render, on the laptop and in every dry pass, before any cluster exists (#1588
follow-up): every component the run's Installer CR enables, rendered by the installer's own composition template
with the CR's EFFECTIVE spec, so a `fail` / `required` guard the composition templates reach fires here, not on a paid
cluster (a guard inside inst.emitDecision / inst.depsReady, which the bypass replaces, would not: 0.3.703 has none).

Why: installer 0.3.703 (its guards are byte-identical since 0.3.683) refuses its whole composition render while
features.internalTls is on and internalTls.certManagerNamespace or componentValues.internal-ca-bundle
.target.namespaces is empty (compositions.yaml:964/975/1005). An offline render never reached those guards:
the composition pass emits a component only once the cluster serves its CRD and its dependencies are Ready
(inst.emitDecision, a `lookup` that `helm template` answers empty), so offline it emits none.

The effective spec is what the CR holds once s_install is done: the CR the installer's own bootstrap writes on a
fresh install, its installer-self-cr ConfigMap's installer.yaml (self-bootstrap.yaml), rendered from the run's values
exactly as s_install installs them. A CR that does not hold the run's values is refused, every key but INSTALL_ONLY's
(0.3.703 and older drop internalTls: installer#1084, fixed by #1145 in 0.3.704), and so is a certManagerNamespace that
is not where the verified cert-manager chart's controller reads cluster CA Secrets (its --cluster-resource-namespace).
After the install the platform step holds the LIVE CR to this render: the components it declares must be exactly the
ones rendered here (bench/diag606.py platform_state `expected`).

THE BYPASS, CONFINED TO THIS CHECK: the chart is rendered from an in-memory copy (ghcr_pull._chart_with; the
pulled directory, which s_install installs from, is never written) with one added template that redefines
inst.emitDecision as "emit every enabled component at its pinned version" (BYPASS_TEMPLATE). Its name sorts
before the chart's own _helpers.tpl, so helm loads it last and its define wins (measured on 0.3.703: under a
"zz-" name the chart's own define won and 0 of 66 rendered). Whether it took is never assumed: a probe
template lists the components the CR enables (inst.compEnabled, registerOnly left out as the template leaves
them out), and the check refuses unless exactly those rendered ("rendered N of N enabled").

NOT CONFIRMED by this check: values the installer derives from the live cluster (Service IPs, the node-IP
ConfigMap, existing Secrets) read empty here, so a guard on one of those cannot fire offline.

    python3 -m bench.installer_render check --installer-pulled D --values V --cert-manager-preflight P --out R
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import secrets
import sys

#: the template that redefines the emission gate, in this check's in-memory copy only. Under "00-" its define
#: wins over the chart's own (_helpers.tpl); under "zz-" the chart's won (measured, helm 3, installer 0.3.703)
BYPASS_TEMPLATE = "templates/00-bench-render-check.tpl"
BYPASS = ('{{- define "inst.emitDecision" -}}{{- $top := index . 0 -}}{{- $c := index . 1 -}}'
          '{{- if eq (include "inst.compEnabled" (list $top $c)) "true" -}}'
          '{{- printf "v%s" ($c.version | toString | replace "." "-") -}}{{- end -}}{{- end -}}\n')
#: the probe: the components the CR enables, as the composition template reads them (inst.componentsYaml,
#: registerOnly skipped, inst.compEnabled)
PROBE_KEY = "enabled"
PROBE = ('apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: bench-render-check-enabled\ndata:\n  ' + PROBE_KEY
         + ': {{ $top := . }}{{ $out := list }}{{ range $c := (include "inst.componentsYaml" $ | fromYaml).components }}'
         '{{ if and (not $c.registerOnly) (eq (include "inst.compEnabled" (list $top $c)) "true") }}'
         '{{ $out = append $out $c.name }}{{ end }}{{ end }}{{ $out | toJson | quote }}\n')
COMPOSITIONS = "templates/compositions.yaml"
SELF_BOOTSTRAP = "templates/self-bootstrap.yaml"
SELF_CR = ("installer-self-cr", "installer.yaml")
#: the registry user the CR carries: the run pipes the real one into helm at install (never in a file); this
#: check renders with a placeholder, as nothing it checks depends on it
REGISTRY_USER_PLACEHOLDER = "bench-render-check"


class Refused(ValueError):
    """Why the run's installer would not render, or why that cannot be read."""


def _render(installer_pulled: str, values: dict, extra: dict, show: list[str], helm: str) -> list:
    from bench import ghcr_pull
    root = os.path.join(installer_pulled, "installer")
    if not os.path.isfile(os.path.join(root, "Chart.yaml")):
        raise Refused(f"no pulled installer chart at {root}")
    data = ghcr_pull._chart_with(root, extra)
    args = ["--kube-version", ghcr_pull.KUBE_VERSION]
    for s in show:
        args += ["--show-only", s]
    try:
        return ghcr_pull.render(data, ghcr_pull.INSTALLER_RELEASE, values, helm, args=tuple(args))
    except ghcr_pull._Refused as e:
        raise Refused(str(e)) from None


def self_cr(installer_pulled: str, values: dict, helm: str = "helm") -> dict:
    """The Installer CR the installer's bootstrap applies on a fresh install of these values (its
    installer-self-cr ConfigMap's installer.yaml)."""
    import yaml
    docs = _render(installer_pulled, values, {}, [SELF_BOOTSTRAP], helm)
    cms = [d for d in docs if isinstance(d, dict) and d.get("kind") == "ConfigMap"
           and (d.get("metadata") or {}).get("name") == SELF_CR[0]]
    raw = (cms[0].get("data") or {}).get(SELF_CR[1]) if len(cms) == 1 else None
    if not isinstance(raw, str):
        raise Refused(f"the installer's {SELF_BOOTSTRAP} renders no {SELF_CR[0]} ConfigMap with {SELF_CR[1]}: the CR "
                      f"a fresh install writes is not read")
    cr = yaml.safe_load(raw)
    if not (isinstance(cr, dict) and isinstance(cr.get("spec"), dict) and cr.get("apiVersion")):
        raise Refused(f"the installer's {SELF_CR[0]} {SELF_CR[1]} is not a CR with a spec")
    return cr


#: the run's values the installer reads at install only, never from its CR: the bootstrap's own switch (off in the CR by
#: design) and the engine subchart's values
INSTALL_ONLY = ("bootstrap", "core-provider")


def effective_spec(values: dict, cr: dict, cluster_resource_namespace: str) -> dict:
    """The CR's spec once s_install is done: the fresh-install CR's. Refused when it does not hold the run's values
    (every key but INSTALL_ONLY's: features, componentValues, internalTls, ...), or, with features.internalTls on, when
    its certManagerNamespace is not cert-manager's --cluster-resource-namespace (an empty one is left to the
    installer's own guard, which names it)."""
    spec = cr["spec"]
    lost = [x for k, v in (values or {}).items() if k not in INSTALL_ONLY for x in not_held({k: v}, spec, "spec")]
    if lost:
        raise Refused(f"the CR a fresh install of this installer ({cr['apiVersion']}) writes does not hold the run's "
                      f"{'; '.join(lost)}")
    ns = (spec.get("internalTls") or {}).get("certManagerNamespace")
    if (spec.get("features") or {}).get("internalTls") and ns:
        if not cluster_resource_namespace:
            raise Refused("features.internalTls is on and cert-manager's --cluster-resource-namespace is not known")
        if ns != cluster_resource_namespace:
            raise Refused(f"internalTls.certManagerNamespace is {ns!r}, and cert-manager reads cluster CA Secrets from "
                          f"{cluster_resource_namespace!r} (its --cluster-resource-namespace)")
    return spec


def not_held(want: dict, have, path: str) -> list[str]:
    """The leaves of `want` that `have` does not hold, each as "path=wanted (the CR: what it holds)". An empty map
    wants nothing of its own."""
    out = []
    for k, v in want.items():
        h = have.get(k) if isinstance(have, dict) else None
        out += not_held(v, h, f"{path}.{k}") if isinstance(v, dict) else (
            [] if h == v else [f"{path}.{k}={v!r} (the CR: {h!r})"])
    return out


def check(installer_pulled: str, values: dict, cluster_resource_namespace: str, helm: str = "helm") -> dict:
    """The record of the composition render of the run's effective spec, or Refused naming why: the values'
    internalTls not in the CR or not where cert-manager reads (effective_spec), a guard of the installer that fails
    (helm's own message, which names the template line), or a component the CR enables that does not render."""
    vals = copy.deepcopy(values or {})
    vals.setdefault("registryAuth", {}).setdefault("username", REGISTRY_USER_PLACEHOLDER)
    cr = self_cr(installer_pulled, vals, helm)
    spec = effective_spec(values, cr, cluster_resource_namespace)
    probe = f"templates/zz-bench-render-check-{secrets.token_hex(4)}.yaml"
    try:
        docs = _render(installer_pulled, spec, {BYPASS_TEMPLATE: BYPASS, probe: PROBE}, [COMPOSITIONS, probe], helm)
    except Refused as e:
        if f"could not find template {COMPOSITIONS}" in str(e):
            raise Refused(f"rendered 0 components: the composition template rendered nothing with the effective "
                          f"spec ({e})") from None
        raise Refused(f"the installer refuses to render its components with the run's effective spec: {e}") from None
    enabled_cm = [d for d in docs if isinstance(d, dict) and d.get("kind") == "ConfigMap"
                  and (d.get("metadata") or {}).get("name") == "bench-render-check-enabled"]
    try:
        enabled = json.loads((enabled_cm[0].get("data") or {})[PROBE_KEY]) if len(enabled_cm) == 1 else None
    except (KeyError, ValueError):
        enabled = None
    if not isinstance(enabled, list) or not enabled:
        raise Refused("the probe of the components the CR enables renders no list: what should render is unknown")
    comps = [d for d in docs if isinstance(d, dict) and str(d.get("apiVersion", "")).startswith("composition.krateo.io/")]
    rendered = sorted((d.get("metadata") or {}).get("name") for d in comps)
    missing, extra = sorted(set(enabled) - set(rendered)), sorted(set(rendered) - set(enabled))
    if missing or extra or len(rendered) != len(set(rendered)):
        raise Refused(f"rendered {len(rendered)} of {len(enabled)} enabled components"
                      + (f"; not rendered: {', '.join(missing)}" if missing else "")
                      + (f"; rendered though not enabled: {', '.join(extra)}" if extra else ""))
    return {"kind": "installer_render_check", "ok": True, "cr_api_version": cr["apiVersion"],
            "internal_tls": spec.get("internalTls"), "cluster_resource_namespace": cluster_resource_namespace,
            "enabled": sorted(enabled), "rendered": rendered,
            "components": [{"name": (d.get("metadata") or {}).get("name"), "kind": d.get("kind"),
                            "namespace": (d.get("metadata") or {}).get("namespace"), "spec": d.get("spec")}
                           for d in comps]}


def lines(rec: dict) -> list[str]:
    return [f"installer render: rendered {len(rec['rendered'])} of {len(rec['enabled'])} enabled components with "
            f"the run's effective spec (the CR its values write; the installer's cluster-state emission gate "
            f"bypassed in this check only), every guard passed: {', '.join(rec['rendered'])}",
            f"installer internalTls in that CR: {json.dumps(rec['internal_tls'], sort_keys=True)}; cert-manager's "
            f"--cluster-resource-namespace: {rec['cluster_resource_namespace']!r}"]


def rendered_names(path: str) -> list[str]:
    """The components a run's composition render rendered, from its record (installer-render.json). Refused unless the
    record says the render passed and names at least one."""
    with open(path, encoding="utf-8") as fh:
        rec = json.load(fh)
    passed = isinstance(rec, dict) and rec.get("kind") == "installer_render_check" and rec.get("ok") is True
    names = rec.get("rendered") if passed else None
    if not isinstance(names, list) or not names:
        raise Refused(f"{path} is not the record of a composition render that passed")
    return names


def main(argv: list[str] | None = None) -> int:
    import yaml
    ap = argparse.ArgumentParser(prog="bench.installer_render")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="the installer's composition render of the run's effective spec")
    c.add_argument("--installer-pulled", required=True)
    c.add_argument("--values", required=True)
    c.add_argument("--cert-manager-preflight", required=True, help="bench.cert_manager's preflight record")
    c.add_argument("--helm", default="helm")
    c.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    try:
        with open(a.values, encoding="utf-8") as fh:
            values = yaml.safe_load(fh) or {}
        with open(a.cert_manager_preflight, encoding="utf-8") as fh:
            crn = json.load(fh).get("cluster_resource_namespace") or ""
        try:
            rec = check(a.installer_pulled, values, crn, a.helm)
        except Refused as e:
            with open(a.out, "w", encoding="utf-8") as fh:
                json.dump({"kind": "installer_render_check", "ok": False, "cluster_resource_namespace": crn,
                           "why": str(e)}, fh)
            raise
        with open(a.out, "w", encoding="utf-8") as fh:
            json.dump(rec, fh, indent=1, sort_keys=True)
        print("\n".join(lines(rec)))
    except (Refused, OSError, ValueError) as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
