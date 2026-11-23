"""The installer's composition render, on the laptop and in every dry pass, before any cluster exists (#1588
follow-up): every component the run's Installer CR enables, rendered by the installer's own composition template
with the CR's EFFECTIVE spec, so any `fail` / `required` guard of the installer fires here, not on a paid cluster.

Why: installer 0.3.703 (its guards are byte-identical since 0.3.683) refuses its whole composition render while
features.internalTls is on and internalTls.certManagerNamespace or componentValues.internal-ca-bundle
.target.namespaces is empty (compositions.yaml:964/975/1005). An offline render never reached those guards:
the composition pass emits a component only once the cluster serves its CRD and its dependencies are Ready
(inst.emitDecision, a `lookup` that `helm template` answers empty), so offline it emits none.

The effective spec is what the CR holds once s_install is done:
  1. the CR the installer's own bootstrap writes on a fresh install: its installer-self-cr ConfigMap's
     installer.yaml (self-bootstrap.yaml), rendered from the run's values exactly as s_install installs them;
  2. merged with the run's CR patch (installer_cr_patch): features.internalTls and
     internalTls.certManagerNamespace, which the installer never writes to the CR (installer#1084: its fresh
     -install pick omits internalTls), set in ONE merge patch as the installer's values.yaml:351 prescribes.
A top-level internalTls in the run's values is refused: the installer drops it, so it could only mislead.

THE BYPASS, CONFINED TO THIS CHECK: the chart is rendered from an in-memory copy (ghcr_pull._chart_with; the
pulled directory, which s_install installs from, is never written) with one added template that redefines
inst.emitDecision as "emit every enabled component at its pinned version" (BYPASS_TEMPLATE). Its name sorts
before the chart's own _helpers.tpl, so helm loads it last and its define wins (measured on 0.3.703: under a
"zz-" name the chart's own define won and 0 of 66 rendered). Whether it took is never assumed: a probe
template lists the components the CR enables (inst.compEnabled, registerOnly left out as the template leaves
them out), and the check refuses unless exactly those rendered ("rendered N of N enabled").

NOT CONFIRMED by this check: values the installer derives from the live cluster (Service IPs, the node-IP
ConfigMap, existing Secrets) read empty here, so a guard on one of those cannot fire offline.

    python3 -m bench.installer_render patch --cert-manager-preflight P --out installer-cr-patch.json
    python3 -m bench.installer_render check --installer-pulled D --values V --patch installer-cr-patch.json --out R
    kubectl get installers.<ver>.composition.krateo.io installer -o json | python3 -m bench.installer_render readback --patch P
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
#: the features the installer's CR takes from the run's CR patch, not from its values (installer#1084), and why
CR_PATCH_FEATURES = {
    "internalTls": "its internalTls.certManagerNamespace is CR-only (installer#1084: the fresh-install pick omits "
                   "it) and must be set in the SAME patch as the feature (installer values.yaml:351)",
}
CR_KIND, CR_NAME = "installers", "installer"


class Refused(ValueError):
    """Why the run's installer would not render, or why that cannot be read."""


def _merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in over.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else copy.deepcopy(v)
    return out


def cr_patch(features: dict, cluster_resource_namespace: str) -> dict:
    """The run's ONE merge patch of the Installer CR, after helm install: every CR_PATCH_FEATURES feature the
    bench's features turn on, and, with internalTls, the namespace cert-manager RUNS in (its controller's
    --cluster-resource-namespace, as the bench's cert-manager step renders it: bench/cert_manager.py). {} when
    nothing is to be patched."""
    spec: dict = {}
    on = {f: True for f in CR_PATCH_FEATURES if features.get(f)}
    if on:
        spec["features"] = on
    if on.get("internalTls"):
        if not cluster_resource_namespace:
            raise Refused("features.internalTls is on and cert-manager's --cluster-resource-namespace is not known")
        spec["internalTls"] = {"certManagerNamespace": cluster_resource_namespace}
    return {"spec": spec} if spec else {}


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


def effective_spec(values: dict, patch: dict, cr: dict) -> dict:
    """The CR's spec once s_install is done: the fresh-install CR, the run's CR patch merged into it."""
    if "internalTls" in (values or {}):
        raise Refused("the run's values set a top-level internalTls, which the installer never writes to its CR "
                      "(installer#1084: CR-only); the bench writes it with the CR patch")
    return _merge(cr["spec"], (patch or {}).get("spec") or {})


def check(installer_pulled: str, values: dict, patch: dict, helm: str = "helm") -> dict:
    """The record of the composition render of the run's effective spec, or Refused naming why: a guard of the
    installer that fails (helm's own message, which names the template line), or a component the CR enables
    that does not render."""
    vals = copy.deepcopy(values or {})
    vals.setdefault("registryAuth", {}).setdefault("username", REGISTRY_USER_PLACEHOLDER)
    cr = self_cr(installer_pulled, vals, helm)
    spec = effective_spec(values, patch, cr)
    probe = f"templates/zz-bench-render-check-{secrets.token_hex(4)}.yaml"
    try:
        docs = _render(installer_pulled, spec, {BYPASS_TEMPLATE: BYPASS, probe: PROBE}, [COMPOSITIONS, probe], helm)
    except Refused as e:
        if f"could not find template {COMPOSITIONS}" in str(e):
            raise Refused(f"rendered 0 components: the composition template rendered nothing with the effective "
                          f"spec ({e})") from None
        raise Refused(f"the installer refuses to render its components with the run's effective spec (its values "
                      f"and CR patch): {e}") from None
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
            "patch": patch, "enabled": sorted(enabled), "rendered": rendered,
            "components": [{"name": (d.get("metadata") or {}).get("name"), "kind": d.get("kind"),
                            "namespace": (d.get("metadata") or {}).get("namespace"), "spec": d.get("spec")}
                           for d in comps]}


def lines(rec: dict) -> list[str]:
    return [f"installer render: rendered {len(rec['rendered'])} of {len(rec['enabled'])} enabled components with "
            f"the run's effective spec (its values and CR patch; the installer's cluster-state emission gate "
            f"bypassed in this check only), every guard passed: {', '.join(rec['rendered'])}",
            f"installer CR patch (after helm install, one merge patch): {json.dumps(rec['patch'], sort_keys=True)}"]


def readback(cr: dict, patch: dict) -> list[str]:
    """What the live CR does not hold of the run's patch (each leaf), empty when it holds all of it."""
    out = []

    def walk(want, have, path):
        for k, v in want.items():
            h = have.get(k) if isinstance(have, dict) else None
            if isinstance(v, dict):
                walk(v, h, f"{path}.{k}")
            elif h != v:
                out.append(f"{path}.{k} is {h!r}, the patch wrote {v!r}")
    walk((patch or {}).get("spec") or {}, (cr or {}).get("spec") or {}, "spec")
    return out


def main(argv: list[str] | None = None) -> int:
    import yaml
    ap = argparse.ArgumentParser(prog="bench.installer_render")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("patch", help="the run's Installer CR merge patch")
    p.add_argument("--cert-manager-preflight", required=True, help="bench.cert_manager's preflight record")
    p.add_argument("--out", required=True)
    c = sub.add_parser("check", help="the installer's composition render of the run's effective spec")
    c.add_argument("--installer-pulled", required=True)
    c.add_argument("--values", required=True)
    c.add_argument("--patch", required=True)
    c.add_argument("--helm", default="helm")
    c.add_argument("--out", required=True)
    r = sub.add_parser("readback", help="the live Installer CR (kubectl -o json) on stdin must hold the patch")
    r.add_argument("--patch", required=True)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "patch":
            from bench import installer_features
            with open(a.cert_manager_preflight, encoding="utf-8") as fh:
                crn = json.load(fh).get("cluster_resource_namespace") or ""
            doc = cr_patch(installer_features.bench_features(), crn)
            with open(a.out, "w", encoding="utf-8") as fh:
                json.dump(doc, fh, sort_keys=True)
            print(f"installer CR patch: {json.dumps(doc, sort_keys=True)}")
        elif a.cmd == "check":
            with open(a.values, encoding="utf-8") as fh:
                values = yaml.safe_load(fh) or {}
            with open(a.patch, encoding="utf-8") as fh:
                patch = json.load(fh)
            try:
                rec = check(a.installer_pulled, values, patch, a.helm)
            except Refused as e:
                with open(a.out, "w", encoding="utf-8") as fh:
                    json.dump({"kind": "installer_render_check", "ok": False, "patch": patch, "why": str(e)}, fh)
                raise
            with open(a.out, "w", encoding="utf-8") as fh:
                json.dump(rec, fh, indent=1, sort_keys=True)
            print("\n".join(lines(rec)))
        else:
            with open(a.patch, encoding="utf-8") as fh:
                patch = json.load(fh)
            gaps = readback(json.load(sys.stdin), patch)
            if gaps:
                raise Refused("the Installer CR does not hold the run's patch: " + "; ".join(gaps))
            print(f"installer CR read back: it holds the run's patch {json.dumps(patch, sort_keys=True)}")
    except (Refused, OSError, ValueError) as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
