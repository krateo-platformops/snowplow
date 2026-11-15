"""Who may read the portal: the bench's base group must be granted the portal's shell, and the pages
it is driven through must still grant it, checked from the charts before any cluster exists.

Why (run 26j, 2026-11-15, three paid clusters stopped before load): every bench user is in the base
group (bench/rbac_model.py BASE_GROUP), and the bench relied on the portal chart granting that group
the shell in its release namespace (the layout, the theme, the header, the sidebar Menu and the
shared RESTActions). That grant came with the portal's demo user, which the installer turned on by a
default of its values schema up to 0.3.625 and no longer does from 0.3.630: on 0.3.641 nothing bound
the group, the boot root answered 403 for every bench user, and the run found out at its preflight,
42 minutes into three clusters.

The bench now declares the grant (bench/installer_values.py: componentValues.<portal>.readerGroups,
the portal chart's own route for a group that should see the shell), and THIS check holds, offline:

  1. the installer's values schema takes the key. A schema that does not declare it (installer
     0.3.422: componentValues.portal is additionalProperties=false without readerGroups) refuses
     the run here, by name: the bench does not send a key the chart would reject, and it has no
     other declared way to grant the shell;
  2. the installer chart, rendered with the run's values, carries the group in the Installer CR it
     applies (componentValues.<portal>.readerGroups of its components patch);
  3. the pinned portal chart, rendered with the values the installer gives it (its own
     componentValues for the portal, overridden by the run's), binds the group, in the release
     namespace, to what the shell needs;
  4. each chart the installer pins as depending on the portal and that declares reader groups of its
     own (the page charts) is rendered the same way: which pages grant the group and which do not is
     reported, and a run where none does is refused (its users would get an empty menu).

WHAT "NEEDS" MEANS is the chart's, never restated here: the same chart is rendered a second time
with `readerGroups` set to one probe group that exists nowhere else, and what the chart binds THAT
group to (its reader template's Role rules, per namespace) is what a reader needs. The base group is
granted when, in every such namespace, the Roles bound to it cover each of those rules (a rule is
covered by API group, resource and verb; "*" covers any). So a chart that grants the group through
another template (the demo grants of an installer that still turns them on) passes too, and one
that binds it in another namespace, or without one of the resources, does not.

WHAT THIS CANNOT SHOW (no cluster): that the installer's composition passes the value on to the live
Portal object (its templates read the cluster, `helm template` renders no composition); what the
live menu then lists, and whether one of its pages is a composite page. The rehearsal's preflight
reads those from a real cluster.

    python3 -m bench.portal_readers check --installer-pulled D --values F --ghcr-env-file E --out FILE
        [--budget-file B]      exit 0 granted; 1 refused (the reason on stderr); 4 the registry gave no answer
"""

from __future__ import annotations

import argparse
import concurrent.futures
import io
import itertools
import json
import os
import secrets
import sys
import tarfile

KIND = "portal_readers"
#: the values key of a chart that grants named groups read of what it ships (the portal chart's shell,
#: each page chart's page): the key the bench declares for the portal, and the one a page chart's own
#: values carry
READERS_KEY = "readerGroups"
EXIT_NO_ANSWER = 4
RBAC_GROUP = "rbac.authorization.k8s.io"


class Refused(ValueError):
    """Why the run's readers cannot be shown to hold."""


class NoAnswer(Exception):
    """The registry gave no answer for a chart within the registry reads' bound."""


# ─── the installer's schema ─────────────────────────────────────────────────

def schema_accepts(root: str, component: str, key: str = READERS_KEY) -> tuple[bool, str]:
    """(does the installer chart at `root` take componentValues.<component>.<key>, why). A chart with
    no values schema refuses nothing. With one, each level on the way must declare the next or take
    undeclared keys (additionalProperties not false), exactly as helm validates the values."""
    path = os.path.join(root, "values.schema.json")
    if not os.path.isfile(path):
        return True, "the installer chart has no values schema: nothing refuses the key"
    try:
        with open(path) as fh:
            node = json.load(fh)
    except (OSError, ValueError) as e:
        raise Refused(f"the installer chart's values schema could not be read ({path}: {e})") from None
    at = "the values"
    for step in ("componentValues", component, key):
        if not isinstance(node, dict):
            return True, f"{at} is not constrained by the schema"
        props = node.get("properties") if isinstance(node.get("properties"), dict) else {}
        if step in props:
            node, at = props[step], f"{at}.{step}" if at != "the values" else step
            continue
        if node.get("additionalProperties") is False:
            return False, (f"its values schema declares no `{step}` under {at} and takes no undeclared key there "
                           f"(additionalProperties: false)")
        return True, f"the schema does not declare `{step}` under {at} and takes undeclared keys there"
    return True, f"its values schema declares componentValues.{component}.{key}"


# ─── what a render grants ───────────────────────────────────────────────────

def _triples(rule: dict):
    return itertools.product(rule.get("apiGroups") or [], rule.get("resources") or [], rule.get("verbs") or [])


def _allows(rule: dict, group: str, resource: str, verb: str, names: list | None) -> bool:
    def has(xs, x):
        return "*" in (xs or []) or x in (xs or [])
    if not (has(rule.get("apiGroups"), group) and has(rule.get("resources"), resource)
            and has(rule.get("verbs"), verb)):
        return False
    mine = rule.get("resourceNames") or []
    return not mine or (bool(names) and set(names) <= set(mine))


def missing(need: list[dict], have: list[dict]) -> list[str]:
    """What of `need` (Role rules) the rules `have` do not allow, as `verb resource.group` lines."""
    out = []
    for n in need:
        for g, r, v in _triples(n):
            if not any(_allows(h, g, r, v, n.get("resourceNames")) for h in have):
                out.append(f"{v} {r}" + (f".{g}" if g else ""))
    return sorted(set(out))


def bound(docs: list, group: str, release_ns: str) -> dict[str, list[dict]]:
    """What the rendered `docs` bind the group `group` to: {namespace: [Role rule, ...]}, a
    ClusterRoleBinding's under "*" (every namespace). A namespaced object with no namespace of its
    own is in the release namespace, as helm installs it. A binding whose role is not in the render
    grants nothing that can be read here, and is left out."""
    roles, cluster_roles = {}, {}
    for d in docs:
        if not isinstance(d, dict):
            continue
        meta = d.get("metadata") or {}
        if d.get("kind") == "Role":
            roles[(meta.get("namespace") or release_ns, meta.get("name"))] = d.get("rules") or []
        elif d.get("kind") == "ClusterRole":
            cluster_roles[meta.get("name")] = d.get("rules") or []
    out: dict[str, list[dict]] = {}
    for d in docs:
        if not isinstance(d, dict) or d.get("kind") not in ("RoleBinding", "ClusterRoleBinding"):
            continue
        if not any(isinstance(s, dict) and s.get("kind") == "Group" and s.get("name") == group
                   and (s.get("apiGroup") or RBAC_GROUP) == RBAC_GROUP for s in d.get("subjects") or []):
            continue
        meta, ref = d.get("metadata") or {}, d.get("roleRef") or {}
        ns = "*" if d["kind"] == "ClusterRoleBinding" else (meta.get("namespace") or release_ns)
        rules = (cluster_roles.get(ref.get("name")) if ref.get("kind") == "ClusterRole" else
                 roles.get((ns, ref.get("name"))) if d["kind"] == "RoleBinding" else None)
        if rules:
            out.setdefault(ns, []).extend(rules)
    return out


def grant(render, values: dict, group: str, release_ns: str) -> dict:
    """Whether a chart grants `group` what one of its readers needs (module doc). `render(values)` is
    the chart's `helm template` with those values. {"need": {namespace: rules}, "granted": bool,
    "missing": {namespace: [what]}}; `need` empty: the chart binds a reader group to nothing."""
    probe = f"bench-reader-probe-{secrets.token_hex(4)}"
    need = bound(render(dict(values, **{READERS_KEY: [probe]})), probe, release_ns)
    have = bound(render(values), group, release_ns)
    miss = {ns: m for ns, rules in sorted(need.items())
            if (m := missing(rules, have.get(ns, []) + have.get("*", [])))}
    return {"need": need, "granted": bool(need) and not miss, "missing": miss}


def need_text(need: dict) -> str:
    return "; ".join(f"in {ns}: " + ", ".join(sorted({f"{v} {r}" + (f".{g}" if g else "")
                                                         for rule in rules for g, r, v in _triples(rule)}))
                     for ns, rules in sorted(need.items()))


def missing_text(miss: dict) -> str:
    return "; ".join(f"in {ns}: {', '.join(m)}" for ns, m in sorted(miss.items()))


# ─── the charts ─────────────────────────────────────────────────────────────

def components(installer_pulled: str, values_file: str, portal: str) -> tuple[dict | None, list[dict]]:
    """(the portal's pin, the pins that depend on it), each as bench.ghcr_pull.private_pins gives a
    component the installer turns on for the run's values: name, chart, repo, version, and the values
    the installer renders it with. The portal's is None when the installer pins none, or its feature
    is off."""
    from bench import ghcr_pull, installer_pins
    root = os.path.join(installer_pulled, "installer")
    try:
        pins, _ = installer_pins.load(root, parse=ghcr_pull._load_yaml)
    except installer_pins.Refused as e:
        raise Refused(str(e)) from None
    comps = [c for c in pins.get("components") or [] if isinstance(c, dict) and c.get("name")]
    names = {portal} | {c["name"] for c in comps if portal in (c.get("deps") or [])}
    try:
        on = [p for p in ghcr_pull.private_pins(installer_pulled, values_file, include=names) if p["name"] in names]
    except (ghcr_pull._Refused, ValueError, OSError) as e:
        raise Refused(f"the installer's pins could not be read for the run's values: {e}") from None
    return next((p for p in on if p["name"] == portal), None), [p for p in on if p["name"] != portal]


def _top_values(data: bytes, what: str) -> dict:
    """The chart archive's own values.yaml (its top directory's), {} when it has none."""
    from bench import ghcr_pull
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            tops = sorted((m for m in tf.getmembers() if m.isfile() and m.name.count("/") == 1
                           and m.name.endswith("/values.yaml")), key=lambda m: m.name)
            if not tops:
                return {}
            raw = tf.extractfile(tops[0]).read(ghcr_pull.CHART_FILE_MAX + 1)
    except (tarfile.TarError, OSError) as e:
        raise Refused(f"{what}: its archive could not be read ({e})") from None
    if len(raw) > ghcr_pull.CHART_FILE_MAX:
        raise Refused(f"{what}: its values.yaml is larger than helm's own limit")
    try:
        doc = ghcr_pull._load_yaml(raw, f"{what} values.yaml: ")
    except Exception as e:  # noqa: BLE001 - a YAML error or the reader's own nesting refusal
        raise Refused(f"{what}: its values.yaml could not be read ({str(e).splitlines()[0][:200]})") from None
    return doc if isinstance(doc, dict) else {}


def fetch(pin: dict, creds: tuple[str, str], budget) -> bytes:
    """The pinned chart's archive, fetched in-process with the ghcr pull token (as the registry step
    reads every chart). Raises NoAnswer (asked again while the reads' bound holds another fetch) or
    Refused (the registry's own answer)."""
    from bench import ghcr_pull
    ref = f"{str(pin['repo']).rstrip('/')}/{pin['chart']}"
    while True:
        got = ghcr_pull.pull_chart(ref, pin["version"], *creds, budget=budget)
        if not isinstance(got, str):
            break
        if not got.no_answer:
            raise Refused(f"the pinned chart could not be fetched: {got}")
        if budget is None or not budget.holds(ghcr_pull.FETCH_CHART_BOUND_S):
            raise NoAnswer(f"no answer from the registry for {got}")
    data, meta = got
    if str((meta or {}).get("version")) != pin["version"]:
        raise Refused(f"{ref}: pulled version {str((meta or {}).get('version'))!r} != pinned {pin['version']!r}")
    return data


def _renderer(data: bytes, pin: dict, helm: str):
    from bench import ghcr_pull

    def render(values: dict) -> list:
        try:
            return ghcr_pull.render(data, pin["name"], values, helm)
        except ghcr_pull._Refused as e:
            raise Refused(f"{pin['chart']} {pin['version']} could not be rendered: {e}") from None
    return render


def rendered_groups(installer_pulled: str, values_file: str, portal: str, helm: str = "helm") -> list:
    """componentValues.<portal>.readerGroups of the Installer CR the pulled installer chart applies
    with the run's values (its components patch, as bench.snowplow_env reads the declared env)."""
    import yaml

    from bench import ghcr_pull, snowplow_env
    root = os.path.join(installer_pulled, "installer")
    try:
        with open(values_file) as fh:
            values = yaml.safe_load(fh) or {}
        docs = ghcr_pull.render(ghcr_pull._chart_with(root, {}), ghcr_pull.INSTALLER_RELEASE, values, helm,
                                args=("--kube-version", ghcr_pull.KUBE_VERSION))
        cv = snowplow_env.rendered_component_values(docs)
    except (snowplow_env.Refused, ghcr_pull._Refused, OSError, yaml.YAMLError) as e:
        raise Refused(f"the installer chart could not be rendered with this run's values: {e}") from None
    got = (cv.get(portal) or {}).get(READERS_KEY) if isinstance(cv.get(portal), dict) else None
    return list(got) if isinstance(got, list) else []


def check(installer_pulled: str, values_file: str, creds: tuple[str, str], budget=None, helm: str = "helm") -> dict:
    """The run's record (module doc). Raises Refused with the reason, NoAnswer when the registry gave
    none; a record is returned only when the group is granted the shell and at least one page (when
    the installer pins pages)."""
    from bench import ghcr_pull, installer_values
    portal, group = installer_values.PORTAL_COMPONENT, installer_values.READER_GROUP
    root = os.path.join(installer_pulled, "installer")
    ok, why = schema_accepts(root, portal)
    if not ok:
        raise Refused(f"the installer chart does not take componentValues.{portal}.{READERS_KEY}: {why}. The bench "
                      f"grants its base group `{group}` the portal's shell through that key and sends no key the "
                      f"chart rejects: this installer is older than the bench supports")
    rec = {"kind": KIND, "group": group, "key": f"componentValues.{portal}.{READERS_KEY}", "schema": why}
    sent = rendered_groups(installer_pulled, values_file, portal, helm)
    if group not in sent:
        raise Refused(f"the Installer CR the chart applies with this run's values does not carry `{group}` in "
                      f"componentValues.{portal}.{READERS_KEY} (it renders {sent!r}): the installer would not pass "
                      f"the grant on")
    shell, deps = components(installer_pulled, values_file, portal)
    if shell is None:
        raise Refused(f"the installer pins no `{portal}` component that is on for this run's values: there is no "
                      f"portal chart to grant `{group}` the shell")
    if budget is not None:
        budget.extend(ghcr_pull.FETCH_CHART_BOUND_S * (1 + len(deps)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {p["name"]: ex.submit(fetch, p, creds, budget) for p in [shell, *deps]}
        charts, no_answer = {}, []
        for name, f in futs.items():
            try:
                charts[name] = f.result()
            except NoAnswer as e:
                no_answer.append(str(e))
    if no_answer:
        raise NoAnswer("; ".join(sorted(no_answer)))
    ns = ghcr_pull.SECRET_NS                                   # the namespace bench.ghcr_pull.render releases into
    g = grant(_renderer(charts[shell["name"]], shell, helm), shell["values"], group, ns)
    rec["shell"] = {"component": shell["name"], "chart": shell["chart"], "version": shell["version"],
                    "release_namespace": ns, **g}
    what = f"the portal chart ({shell['chart']} {shell['version']})"
    if not g["need"]:
        raise Refused(f"{what} binds a group named in its `{READERS_KEY}` to nothing: what a reader of the shell "
                      f"needs cannot be derived from it, and the bench's grant would be inert")
    if ns not in g["need"]:
        raise Refused(f"{what} binds a reader group in {sorted(g['need'])}, not in its release namespace {ns}: the "
                      f"shell's objects are read there")
    if not g["granted"]:
        raise Refused(f"{what}, rendered with the values the installer gives it, does not grant the bench's base "
                      f"group `{group}` what a reader of the shell needs ({need_text(g['need'])}); missing "
                      f"{missing_text(g['missing'])}: every bench user's boot call would answer 403")
    pages, others = [], []
    for p in deps:
        own = _top_values(charts[p["name"]], f"{p['chart']} {p['version']}")
        if READERS_KEY not in own:
            others.append(p["name"])
            continue
        pg = grant(_renderer(charts[p["name"]], p, helm), p["values"], group, ns)
        pages.append({"component": p["name"], "chart": p["chart"], "version": p["version"], **pg})
    rec["pages"] = sorted(pages, key=lambda x: x["component"])
    rec["not_pages"] = sorted(others)
    rec["granting"] = [x["component"] for x in rec["pages"] if x["granted"]]
    rec["not_granting"] = [x["component"] for x in rec["pages"] if not x["granted"]]
    if pages and not rec["granting"]:
        raise Refused(f"none of the {len(pages)} page chart(s) this installer pins grants the bench's base group "
                      f"`{group}` ({', '.join(rec['not_granting'])}): its users would get a menu with no page")
    rec["ok"] = True
    return rec


def said(rec: dict) -> list[str]:
    s = rec["shell"]
    lines = [f"portal readers: `{rec['group']}` declared in {rec['key']} ({rec['schema']}); the portal chart "
             f"{s['chart']} {s['version']} grants it the shell ({need_text(s['need'])})"]
    if rec["pages"]:
        lines.append(f"portal readers: {len(rec['granting'])} of {len(rec['pages'])} page chart(s) grant "
                     f"`{rec['group']}`: {', '.join(rec['granting'])}; not granting it: "
                     f"{', '.join(rec['not_granting']) or 'none'}")
    else:
        lines.append("portal readers: this installer pins no page chart with reader groups of its own (its pages "
                     "are the portal chart's)")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.portal_readers")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("--installer-pulled", required=True)
    c.add_argument("--values", required=True)
    c.add_argument("--ghcr-env-file", required=True,
                   help="the ghcr pull token file (bench/ghcr_pull.py), by path; never printed")
    c.add_argument("--budget-file", default=None, help="the run's one bound of its pre-cluster registry reads")
    c.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    from bench import ghcr_pull, noanswer
    why = ghcr_pull.check(a.ghcr_env_file)
    if why:
        print(f"refusing: the ghcr pull token: {why}", file=sys.stderr)
        return 2
    budget = noanswer.Budget.shared(a.budget_file) if a.budget_file else None
    rec, code = None, 0
    try:
        rec = check(a.installer_pulled, a.values, ghcr_pull.creds(a.ghcr_env_file), budget)
    except Refused as e:
        rec, code = {"kind": KIND, "ok": False, "why": str(e)}, 1
    except NoAnswer as e:
        rec, code = {"kind": KIND, "ok": False, "no_answer": True, "why": str(e)}, EXIT_NO_ANSWER
    tmp = a.out + ".part"
    with open(tmp, "w") as fh:
        json.dump(rec, fh, indent=1)
        fh.write("\n")
    os.replace(tmp, a.out)
    if code:
        print(f"refusing: {rec['why']}", file=sys.stderr)
        return code
    for ln in said(rec):
        print(ln)
    return 0


if __name__ == "__main__":
    sys.exit(main())
