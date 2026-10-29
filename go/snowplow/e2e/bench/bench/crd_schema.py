"""Every manifest the run applies, validated offline against the CRDs of the
exact components the installer version pins, BEFORE any cluster exists, in the
dry pass too.

Run 17 attempt 4 paid for two GKE clusters for about 40 minutes to learn that
all 670 fresh-class Users lacked `spec.avatarURL`, which authn-crds 0.27.3
requires. The dry pass could not see it. Now it can:

1. CRD SOURCES: the CRDs the installer version would install, read from
   - its vendored dependency charts (`charts/`, already in the pulled installer);
   - every component its `files/component-pins.yaml` declares a CRD
     component (pin `kind` ending in `Crds`: AuthnCrds, SnowplowCrds, ...),
     pulled at the PINNED version from the pin's `repo` (else the installer's
     `ociRepo`), as the installer's own CompositionDefinitions resolve them
     (templates/definitions.yaml).
   CRD files are read as the chart ships them. A file that is templated
   (`{{`) AND declares a CustomResourceDefinition is recorded unread: it
   would need the installer's values to render.
2. DOCUMENTS: what the run applies of a kind that is not built in, rendered by
   the renderers the run applies them from (data_manifests.documents,
   fresh_classes.manifest).
3. VALIDATION against the served version's openAPIV3Schema, as the API
   server does for a CREATE with kubectl's default `--validate=strict`:
   nulls in non-nullable fields pruned, defaults applied, then `type`,
   `required`, `enum`, `properties` (an undeclared field is refused unless
   `x-kubernetes-preserve-unknown-fields`), `additionalProperties`, `items`,
   lengths, bounds, `pattern`, `format` int32/int64/date-time,
   `x-kubernetes-int-or-string`, list-type set/map uniqueness and
   `allOf`/`anyOf`/`oneOf`/`not`.

What it does NOT do, and says per kind instead of approximating:
- CEL (`x-kubernetes-validations`): no CEL evaluator here. A rule at a node
  a document reaches makes that kind NOT FAITHFULLY VALIDATED; the rules are
  named. Its structural refusals still fail the check (they are certain).
  A transition rule (`oldSelf`) is evaluated only on UPDATE, never on the
  create this fresh cluster sees, so it does not count.
- a keyword or `format` outside the list above, at a node a document reaches:
  the same.
- a kind whose CRD is GENERATED in-cluster (the 50K compositions: core-provider
  writes their CRD from the CompositionDefinition's chart): not validated;
  its source is named.
- built-in kinds (Secret, RoleBinding, ...): the API server's own types, not
  CRDs; ObjectMeta (`metadata`) is the API server's own validation too.

A missing CRD (no pinned chart defines the kind at a served version) FAILS
the check: a document nothing validates is never a pass.

STDLIB + PyYAML; the documents come from bench modules (no cluster call).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile

import yaml

__all__ = ["crd_sources", "read_crds", "index_crds", "validate", "check", "main"]

CRD_PIN_KIND_SUFFIX = "Crds"   # the installer's declaration of a CRD component (component-pins.yaml `kind`)

# the keywords validate() implements (descriptions and markers carry no rule)
_IMPLEMENTED = {
    "type", "required", "properties", "additionalProperties", "items", "enum", "nullable", "default",
    "minLength", "maxLength", "pattern", "format", "minimum", "maximum", "exclusiveMinimum",
    "exclusiveMaximum", "multipleOf", "minItems", "maxItems", "minProperties", "maxProperties",
    "allOf", "anyOf", "oneOf", "not", "x-kubernetes-preserve-unknown-fields",
    "x-kubernetes-int-or-string", "x-kubernetes-list-type", "x-kubernetes-list-map-keys",
    "x-kubernetes-map-type", "x-kubernetes-validations",
    "description", "title", "example", "externalDocs",
}
_FORMATS = {"int32", "int64", "date-time"}
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$")
_CRD_DOC = re.compile(r"^kind:\s*CustomResourceDefinition\s*$", re.M)


# ─── 1. the CRDs the installer version pins ────────────────────────────────

def _load(path: str) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def crd_sources(installer_dir: str) -> list[dict]:
    """The installer's vendored dependencies and its pinned CRD components."""
    meta = _load(os.path.join(installer_dir, "Chart.yaml"))
    values = _load(os.path.join(installer_dir, "values.yaml"))
    pins = _load(os.path.join(installer_dir, "files", "component-pins.yaml")).get("components") or []
    charts_dir = os.path.join(installer_dir, "charts")
    vendored = sorted(os.listdir(charts_dir)) if os.path.isdir(charts_dir) else []
    out = []
    for d in meta.get("dependencies") or []:
        name, ver = d.get("name"), str(d.get("version"))
        hit = next((v for v in vendored if v == name or v == f"{name}-{ver}.tgz"), None)
        out.append({"component": name, "chart": name, "version": ver, "from": "vendored",
                    "path": os.path.join(charts_dir, hit) if hit else None})
    for p in pins:
        if not str(p.get("kind") or "").endswith(CRD_PIN_KIND_SUFFIX):
            continue
        chart = p.get("chart") or p["name"]
        repo = (p.get("repo") or values.get("ociRepo") or "").rstrip("/")
        out.append({"component": p["name"], "chart": chart, "version": str(p["version"]), "from": "pin",
                    "ref": f"{repo}/{chart}" if not p.get("chartRepo") else chart,
                    "repo": repo if p.get("chartRepo") else None})
    return out


def pull(src: dict, workdir: str, run=subprocess.run) -> dict:
    """helm pull --untar at the pinned version into its own directory; the
    pulled Chart.yaml must carry that version."""
    dest = os.path.join(workdir, f"{src['chart']}-{src['version']}")
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest)
    argv = ["helm", "pull", src["ref"], "--version", src["version"], "--untar", "-d", dest]
    if src.get("repo"):
        argv += ["--repo", src["repo"]]
    p = run(argv, capture_output=True, text=True)
    if p.returncode != 0:
        return dict(src, error=f"helm pull failed: {(p.stderr or '').strip()[:300]}")
    sub = [d for d in os.listdir(dest) if os.path.isdir(os.path.join(dest, d))]
    if len(sub) != 1:
        return dict(src, error=f"unexpected pull layout: {sub}")
    path = os.path.join(dest, sub[0])
    got = str(_load(os.path.join(path, "Chart.yaml")).get("version"))
    if got != src["version"]:
        return dict(src, error=f"pulled version {got!r} != pinned {src['version']!r}")
    return dict(src, path=path)


def _chart_files(path: str):
    """(name, text) of every YAML file under crds/ and templates/ of a chart
    directory or a packaged .tgz."""
    want = re.compile(r"(^|/)(crds|templates)/.+\.ya?ml$")
    if os.path.isdir(path):
        for root, _, files in os.walk(path):
            for f in sorted(files):
                full = os.path.join(root, f)
                rel = os.path.relpath(full, path)
                if want.search(rel) and "/charts/" not in "/" + rel:
                    with open(full) as fh:
                        yield rel, fh.read()
        return
    with tarfile.open(path) as tf:
        for m in tf.getmembers():
            rel = m.name.split("/", 1)[1] if "/" in m.name else m.name
            if m.isfile() and want.search(rel) and "/charts/" not in "/" + rel:
                yield rel, io.TextIOWrapper(tf.extractfile(m)).read()


def read_crds(src: dict) -> dict:
    """The CRDs a chart ships, or why they could not be read."""
    if src.get("error") or not src.get("path"):
        return dict(src, crds=[], error=src.get("error") or "not vendored in the installer chart")
    crds, unread = [], []
    try:
        for rel, text in _chart_files(src["path"]):
            if not _CRD_DOC.search(text):
                continue
            if "{{" in text:
                unread.append(rel)
                continue
            crds += [d for d in yaml.safe_load_all(text) if isinstance(d, dict)
                     and d.get("kind") == "CustomResourceDefinition"]
    except (OSError, tarfile.TarError, yaml.YAMLError, UnicodeDecodeError) as e:
        return dict(src, crds=[], error="cannot read the chart: " + " ".join(str(e).split()))
    res = dict(src, crds=crds, templated_unread=unread)
    if not crds and not unread and src.get("from") == "pin":
        # a declared CRD component whose CRDs are not plain files (rendered from files/ or
        # shipped in subcharts): its kinds are not indexed, so a document of one is MISSING
        res["error"] = "no CRD as a plain file under crds/ or templates/ (rendered from files/ or subcharts): not read"
    return res


def index_crds(sources: list[dict]) -> dict:
    """{(group, version, kind): {schema, chart}} for every SERVED version."""
    idx = {}
    for s in sources:
        for c in s.get("crds") or []:
            spec = c.get("spec") or {}
            for v in spec.get("versions") or []:
                if not v.get("served"):
                    continue
                key = (spec.get("group"), v.get("name"), (spec.get("names") or {}).get("kind"))
                idx[key] = {"schema": ((v.get("schema") or {}).get("openAPIV3Schema") or {}),
                            "chart": f"{s['chart']} {s['version']}"}
    return idx


# ─── 3. validation (the API server's, for a create) ─────────────────────────

def _type_of(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "integer"
    if isinstance(v, float):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    return "object" if isinstance(v, dict) else type(v).__name__


def _is(v, t: str) -> bool:
    k = _type_of(v)
    return k == t or (t == "number" and k == "integer")


class _Ctx:
    def __init__(self):
        self.cel: list[str] = []          # "<path>: <rule>" a document reaches
        self.unsupported: list[str] = []  # "<path>: <keyword>"


def _walk(v, s: dict, path: str, errs: list, ctx: _Ctx, structural: bool = True) -> None:
    for k in s:
        if k not in _IMPLEMENTED:
            ctx.unsupported.append(f"{path or '.'}: {k}")
    if s.get("format") and s["format"] not in _FORMATS:
        ctx.unsupported.append(f"{path or '.'}: format {s['format']}")
    for r in s.get("x-kubernetes-validations") or []:
        if "oldSelf" not in str(r.get("rule", "")):
            ctx.cel.append(f"{path or '.'}: {r.get('rule')}")
    if s.get("x-kubernetes-int-or-string"):
        if _type_of(v) not in ("integer", "string"):
            errs.append(f'{path}: Invalid value: "{_type_of(v)}": {path} in body must be of type integer-or-string')
            return
    elif s.get("type") and not _is(v, s["type"]):
        errs.append(f'{path}: Invalid value: "{_type_of(v)}": {path} in body must be of type {s["type"]}')
        return
    if "enum" in s and v not in s["enum"]:
        errs.append(f"{path}: Unsupported value: {json.dumps(v)}")
    if isinstance(v, str):
        if "minLength" in s and len(v) < s["minLength"]:
            errs.append(f"{path}: Invalid value: should be at least {s['minLength']} chars long")
        if "maxLength" in s and len(v) > s["maxLength"]:
            errs.append(f"{path}: Too long: may not be more than {s['maxLength']} bytes")
        if "pattern" in s and not re.search(s["pattern"], v):
            errs.append(f"{path}: Invalid value: should match '{s['pattern']}'")
        if s.get("format") == "date-time" and not _RFC3339.match(v):
            errs.append(f"{path}: Invalid value: must be a date-time")
    if _type_of(v) in ("integer", "number"):
        if s.get("format") == "int32" and not -2**31 <= v < 2**31:
            errs.append(f"{path}: Invalid value: must be an int32")
        if s.get("format") == "int64" and not -2**63 <= v < 2**63:
            errs.append(f"{path}: Invalid value: must be an int64")
        if "minimum" in s and (v <= s["minimum"] if s.get("exclusiveMinimum") else v < s["minimum"]):
            errs.append(f"{path}: Invalid value: should be greater than or equal to {s['minimum']}")
        if "maximum" in s and (v >= s["maximum"] if s.get("exclusiveMaximum") else v > s["maximum"]):
            errs.append(f"{path}: Invalid value: should be less than or equal to {s['maximum']}")
        if s.get("multipleOf") and (v / s["multipleOf"]) % 1:
            errs.append(f"{path}: Invalid value: should be a multiple of {s['multipleOf']}")
    if isinstance(v, list):
        if "minItems" in s and len(v) < s["minItems"]:
            errs.append(f"{path}: Invalid value: should have at least {s['minItems']} items")
        if "maxItems" in s and len(v) > s["maxItems"]:
            errs.append(f"{path}: Too many: {len(v)}: must have at most {s['maxItems']} items")
        lt = s.get("x-kubernetes-list-type")
        if lt == "set":
            seen = [json.dumps(x, sort_keys=True) for x in v]
            for dup in sorted({x for x in seen if seen.count(x) > 1}):
                errs.append(f"{path}: Duplicate value: {dup}")
        if lt == "map":
            keys = [tuple(json.dumps((x or {}).get(k), sort_keys=True) for k in s.get("x-kubernetes-list-map-keys") or [])
                    for x in v]
            for dup in sorted({k for k in keys if keys.count(k) > 1}):
                errs.append(f"{path}: Duplicate value: map key {list(dup)}")
        if isinstance(s.get("items"), dict):
            for i, x in enumerate(v):
                _walk(x, s["items"], f"{path}[{i}]", errs, ctx, structural)
    if isinstance(v, dict):
        props = s.get("properties") or {}
        for k in [k for k, x in v.items() if x is None and not (props.get(k) or {}).get("nullable")]:
            del v[k]                               # a null in a non-nullable field is pruned
        for k, ps in props.items():
            if k not in v and "default" in ps:
                v[k] = copy.deepcopy(ps["default"])  # defaulted before validation
        for k in s.get("required") or []:
            if k not in v:
                errs.append(f"{path + '.' if path else ''}{k}: Required value")
        if "minProperties" in s and len(v) < s["minProperties"]:
            errs.append(f"{path}: Invalid value: should have at least {s['minProperties']} properties")
        if "maxProperties" in s and len(v) > s["maxProperties"]:
            errs.append(f"{path}: Too many: must have at most {s['maxProperties']} properties")
        ap = s.get("additionalProperties")
        for k, x in v.items():
            sub = f"{path + '.' if path else ''}{k}"
            if k in props:
                if not (path == "" and k == "metadata"):    # ObjectMeta: the API server's own validation
                    _walk(x, props[k], sub, errs, ctx, structural)
            elif isinstance(ap, dict):
                _walk(x, ap, sub, errs, ctx, structural)
            elif structural and not s.get("x-kubernetes-preserve-unknown-fields") and ap is not True:
                errs.append(f"{sub}: field not declared in schema")
    for sub_s in s.get("allOf") or []:
        _walk(v, sub_s, path, errs, ctx, False)
    if s.get("anyOf"):
        if not any(not _sub_errs(v, x, path, ctx) for x in s["anyOf"]):
            errs.append(f"{path}: Invalid value: must validate at least one schema (anyOf)")
    if s.get("oneOf"):
        n = sum(1 for x in s["oneOf"] if not _sub_errs(v, x, path, ctx))
        if n != 1:
            errs.append(f"{path}: Invalid value: must validate one and only one schema (oneOf)")
    if isinstance(s.get("not"), dict) and not _sub_errs(v, s["not"], path, ctx):
        errs.append(f"{path}: Invalid value: must not validate the schema (not)")


def _sub_errs(v, s, path, ctx) -> list:
    e: list = []
    _walk(copy.deepcopy(v), s, path, e, ctx, False)   # value validation only: pruning follows the main schema
    return e


def validate(doc: dict, schema: dict) -> tuple[list[str], list[str], list[str]]:
    """(refusals, CEL rules reached, unsupported keywords reached) for a create of `doc`."""
    errs: list[str] = []
    ctx = _Ctx()
    _walk(copy.deepcopy(doc), schema, "", errs, ctx)
    return errs, sorted(set(ctx.cel)), sorted(set(ctx.unsupported))


# ─── the check ──────────────────────────────────────────────────────────────

_BUILTIN_GROUP = re.compile(r"^$|\.k8s\.io$|^(apps|batch|policy|autoscaling)$")


def _gvk(doc: dict) -> tuple[str, str, str]:
    api = str(doc.get("apiVersion") or "")
    g, _, v = api.rpartition("/")
    return g, v, str(doc.get("kind") or "")


def _generated_by(docs: list[tuple[str, dict]]) -> dict:
    """{(group, version, kind): source} of the composition kinds core-provider
    GENERATES from the CompositionDefinitions the run applies: group
    composition.krateo.io, version v<chart version, dots as dashes>, kind the
    chart's name in CamelCase (as core-provider names them)."""
    out = {}
    for _, d in docs:
        if _gvk(d)[:1] == ("core.krateo.io",) and d.get("kind") == "CompositionDefinition":
            ch = ((d.get("spec") or {}).get("chart") or {})
            name, ver = str(ch.get("repo") or ""), str(ch.get("version") or "")
            kind = "".join(p[:1].upper() + p[1:] for p in name.split("-"))
            out[("composition.krateo.io", "v" + ver.replace(".", "-"), kind)] = (
                f"core-provider generates it in-cluster from the CompositionDefinition's chart "
                f"({name} {ver} from {ch.get('url')}); validating it offline would mean re-implementing "
                f"core-provider's generator, an approximation")
    return out


def check(docs: list[tuple[str, dict]], sources: list[dict]) -> dict:
    idx = index_crds(sources)
    generated = _generated_by(docs)
    kinds: dict = {}
    builtin: dict = {}
    for what, d in docs:
        g, v, k = _gvk(d)
        if _BUILTIN_GROUP.search(g):
            builtin[f"{k}.{g or 'core'}/{v}"] = builtin.get(f"{k}.{g or 'core'}/{v}", 0) + 1
            continue
        key = f"{k}.{g}/{v}"
        e = kinds.setdefault(key, {"kind": key, "applied_by": [], "documents": 0, "refused": 0,
                                   "violations": {}, "cel_not_evaluated": [], "unsupported": []})
        e["documents"] += 1
        if what not in e["applied_by"]:
            e["applied_by"].append(what)
        if (g, v, k) in generated:
            e["status"], e["why"] = "not validated (generated CRD)", generated[(g, v, k)]
            continue
        crd = idx.get((g, v, k))
        if crd is None:
            e["status"] = "MISSING"
            e["why"] = "no CRD chart the installer pins defines this kind at a served version"
            continue
        e["crd"] = crd["chart"]
        errs, cel, uns = validate(d, crd["schema"])
        if errs:
            e["refused"] += 1
            for x in errs:
                e["violations"][x] = e["violations"].get(x, 0) + 1
        e["cel_not_evaluated"] = sorted(set(e["cel_not_evaluated"]) | set(cel))
        e["unsupported"] = sorted(set(e["unsupported"]) | set(uns))
    for e in kinds.values():
        if "status" in e:
            continue
        if e["refused"]:
            e["status"] = "REFUSED"
        elif e["cel_not_evaluated"] or e["unsupported"]:
            e["status"] = "not faithfully validated"
        else:
            e["status"] = "valid"
    ok = not any(e["status"] in ("REFUSED", "MISSING") for e in kinds.values())
    return {"kind": "crd_check", "ok": ok, "kinds": sorted(kinds.values(), key=lambda e: e["kind"]),
            "builtin_not_validated": builtin,
            "sources": [{x: s.get(x) for x in ("component", "chart", "version", "from", "error",
                                                "templated_unread")} | {"crds": len(s.get("crds") or [])}
                        for s in sources]}


def report_lines(res: dict) -> list[str]:
    out = []
    pulled = [f"{s['chart']} {s['version']}" for s in res["sources"] if s["crds"]]
    out.append(f"crd check: {len(pulled)} CRD chart(s) of the pinned installer read: {', '.join(pulled)}")
    for s in res["sources"]:
        if s.get("error") or s.get("templated_unread"):
            out.append(f"crd check: {s['chart']} {s['version']} not read: "
                       f"{s.get('error') or 'templated CRD file(s) ' + ', '.join(s['templated_unread'])}")
    for e in res["kinds"]:
        by = " + ".join(e["applied_by"])
        head = f"{e['kind']} ({e['documents']} document(s): {by})"
        if e["status"] == "valid":
            out.append(f"crd check: {head}: valid against {e['crd']}")
        elif e["status"] == "REFUSED":
            for v, n in e["violations"].items():
                out.append(f"crd check FAILED: {head}: the API server would refuse {n} x {v} ({e['crd']})")
        elif e["status"] == "MISSING":
            out.append(f"crd check FAILED: {head}: {e['why']}")
        elif e["status"].startswith("not validated"):
            out.append(f"crd check: {head}: NOT validated offline: {e['why']}")
        if e["cel_not_evaluated"] or e["unsupported"]:
            what = ([f"{len(e['cel_not_evaluated'])} CEL rule(s) (no CEL evaluator here): "
                     + "; ".join(e["cel_not_evaluated"])] if e["cel_not_evaluated"] else []) + \
                   ([f"keyword(s) not implemented: {'; '.join(e['unsupported'])}"] if e["unsupported"] else [])
            out.append(f"crd check: {head}: NOT faithfully validated against {e['crd']}: "
                       f"structural rules checked{', and none refused' if not e['refused'] else ''}; "
                       f"not checked: {' | '.join(what)}")
    if res["builtin_not_validated"]:
        out.append("crd check: built-in kinds are the API server's own types, not CRDs (not validated here): "
                   + ", ".join(f"{n} x {k}" for k, n in sorted(res["builtin_not_validated"].items())))
    out.append(f"crd check: {'OK' if res['ok'] else 'FAILED'}")
    return out


def run_documents(a) -> list[tuple[str, dict]]:
    """What this run applies, rendered offline (the run's own parameters)."""
    from bench import data_manifests, fresh_classes
    from bench.load606 import SCALEUSER_PASSWORD
    docs = data_manifests.documents(namespaces=a.namespaces, users=a.users,
                             fresh_modes=tuple(a.fresh_modes or ("annotation",)))
    if a.fresh_classes:
        # the class COUNT depends on the preflight's route table (known on the cluster);
        # every class renders the same objects, so one route stands in (the most classes)
        p = fresh_classes.plan(sessions=a.sessions, routes=1, think_s=a.browser_think_s,
                               duration_s=a.duration_s, namespaces=a.namespaces, user_prefix=a.user_prefix)
        text = fresh_classes.manifest(p, SCALEUSER_PASSWORD)
        docs += [("the fresh identity classes", json.loads(x)) for x in text.split("\n---\n") if x.strip()]
    return docs


def _installer_dir(pulled: str) -> str:
    sub = [d for d in os.listdir(pulled) if os.path.isdir(os.path.join(pulled, d))]
    if len(sub) != 1:
        raise SystemExit(f"crd check: unexpected installer pull layout in {pulled}: {sub}")
    return os.path.join(pulled, sub[0])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="bench.crd_schema")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="validate what the run applies against the pinned CRDs")
    c.add_argument("--installer-pulled", required=True, help="the directory oci-preflight pulled the installer into")
    c.add_argument("--workdir", required=True, help="where the pinned CRD charts are pulled")
    c.add_argument("--out", required=True)
    c.add_argument("--users", type=int, required=True)
    c.add_argument("--namespaces", type=int, required=True)
    c.add_argument("--fresh-modes", nargs="*", default=None)
    c.add_argument("--fresh-classes", action="store_true")
    c.add_argument("--sessions", type=int, default=0)
    c.add_argument("--browser-think-s", type=float, default=0)
    c.add_argument("--duration-s", type=float, default=0)
    c.add_argument("--user-prefix", default="scaleuser-")
    a = ap.parse_args(argv)
    docs = run_documents(a)
    srcs = crd_sources(_installer_dir(a.installer_pulled))
    os.makedirs(a.workdir, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        srcs = list(ex.map(lambda s: pull(s, a.workdir) if s["from"] == "pin" else s, srcs))
    srcs = [read_crds(s) for s in srcs]
    res = check(docs, srcs)
    with open(a.out, "w") as fh:
        json.dump(res, fh, indent=2)
    for ln in report_lines(res):
        print(ln, file=sys.stderr, flush=True)
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
