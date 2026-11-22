"""#1572: krateo-057's componentValues SWITCHES and SIZES, recorded, and written into the bench's values.

The owner's ruling (2026-11-22): "Switches + sizes, recorded." 057's Installer CR carries
spec.componentValues, and some of them change which workloads and images run: its kagent turns the
per-user tool server on (perUserRbac.enabled) and the upstream tools server off
(kagentapp.kagent-tools.enabled), which the bench's ghcr preflight needs to find kagent's injected pull
secret on a pod. Copying those two by hand would be a special case; this module records every switch
and every size 057 sets, read the same way as its features (bench/installer_features.py, one reading
of the one Installer CR), and the bench's values derive from the record.

WHAT IS RECORDED (component_reading), decided by the installer chart's own values schema at the CR's
version (values.schema.json componentValues), never by a value:
  - a SWITCH: a leaf the schema types `boolean`, or a string whose schema enum is within
    {"true", "false"} (e.g. snowplow.env.OTEL_LOGS_ENABLED);
  - a SIZE: a number or a Kubernetes quantity under a `resources` map, or at a key named for a size
    (SIZE_KEYS: replicaCount, replicas, min/maxReplicas, targetCPUUtilizationPercentage, storage,
    instances).
Everything else is NOT recorded, and the record names it by its path and why, never its value:
endpoints, URLs, hostnames, issuers, groups, identities, credential references (none of them is a
switch or a size), lists and empty maps, and every key the schema does not name: under a closed map
(additionalProperties: false) "not settable through the installer" (no values file can carry it);
under an open one, a free-form map (installer 0.3.697: agentiko's agents by name, frontend.config,
model-configs.models, a ClickHouse map keyed by user name, ...), whose keys may name identities. Under
a key the schema does not name nothing is descended into: the record names the declared parent and
how many keys it holds, so a free-form key (a user's name, say) can never enter it. Both are named as
fidelity gaps by every check.

THE DEVIATIONS (DEVIATIONS below): recorded switches the bench does not copy, each with why and the
owner's ruling, said by every check: a switch that creates an identity, grants a named identity
rights, or opens or reaches an outside service. The bench writes no value for them (the chart's or
the installer's default stands, as before #1572). portal.readerGroups is written by the bench itself
(bench/installer_values.py: its base group), never 057's.

Imports nothing from bench but installer_features' Refused; stdlib + PyYAML.
"""

from __future__ import annotations

import os
import re

RECORD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "installer_component_values_057.json")
RECORD_KIND = "installer_component_values_057"
SIZE_KEYS = frozenset({"replicaCount", "replicas", "minReplicas", "maxReplicas", "targetCPUUtilizationPercentage",
                       "storage", "instances"})
_QTY = re.compile(r"^[0-9]+(\.[0-9]+)?(m|k|Ki|Mi|Gi|Ti|Pi|M|G|T|P)?$")
SWITCH_STRINGS = frozenset({"true", "false"})
#: a Kubernetes resource name (cpu, memory, ephemeral-storage, nvidia.com/gpu)
_RESOURCE_NAME = re.compile(r"^([a-z0-9.-]+/)?[a-z][a-z0-9-]*$")

NOT_A_SWITCH_OR_SIZE = "neither a switch nor a size (the owner's ruling, 2026-11-22: switches and sizes are recorded)"
A_LIST = "a list (neither a switch nor a size)"
AN_EMPTY_MAP = "an empty map (neither a switch nor a size)"
NOT_SETTABLE = "not settable through the installer: its values schema rejects {n} key(s) here (a fidelity gap)"
FREE_FORM = ("a free-form map: {n} key(s) the schema does not name, not recorded (a free-form key may name an "
             "identity; a fidelity gap)")

_RULING = "the owner (Diego), 2026-11-22: not copied"
#: recorded switches the bench does not copy: {path: (why, ruling)}
DEVIATIONS: dict[str, tuple[str, str]] = {
    "portal.enableCyberjokerUser": ("creates the cyberjoker demo user (basic auth)", _RULING),
    "portal.enableAdminUser": ("creates the admin user (basic auth, cluster-admin)", _RULING),
    "portal.enableIncidentResponse": ("grants the sre-incident-responder identity cluster rights (ClusterRoleBinding)",
                                      _RULING),
    "agentgateway-policies.stt.enabled": ("a speech-to-text route to an outside model provider (Vertex); needs the "
                                          "TLS platform listener the bench has not", _RULING),
    "agentgateway-policies.tts.enabled": ("a text-to-speech route to an outside service (Google Cloud TTS); needs the "
                                          "TLS platform listener the bench has not", _RULING),
    "agentgateway-policies.mcpEntry.enabled": ("opens an internet-facing /mcp route (needs a TLS listener)", _RULING),
    "agentgateway-policies.cors.enabled": ("admits outside browser origins", _RULING),
    "agentgateway-policies.cors.allowCredentials": ("admits outside browser origins with credentials", _RULING),
    "agentgateway-policies.uiTokenHop.impersonation.enabled": ("impersonates a named identity", _RULING),
    "cert-manager-issuers.acmeDns01.enabled": ("an ACME issuer through Let's Encrypt and a DNS provider (outside)",
                                               _RULING),
    "krateo-observability.hyperdxLoadBalancer.enabled": ("an outside LoadBalancer for HyperDX", _RULING),
    "frontend.ingress.enabled": ("an outside Ingress", _RULING),
    "frontend.serviceAccount.create": ("creates a ServiceAccount identity", _RULING),
    "frontend.serviceAccount.automount": ("mounts that identity's token", _RULING),
    "gcp-configconnector.configConnector.enabled": ("reaches GCP with a service-account key (configConnector is a "
                                                    "feature deviation too)", _RULING),
    "keycloak-config-kog.auth.bootstrap.enabled": ("bootstraps Keycloak identities", _RULING),
    "zendesk-kog.plugin.webhookReceiver.enabled": ("receives Zendesk's webhooks (outside)", _RULING),
    "zendesk-kog.restDefinitions.request.enabled": ("calls Zendesk (outside)", _RULING),
    "zendesk-kog.restDefinitions.ticket.enabled": ("calls Zendesk (outside)", _RULING),
    "zendesk-kog.restDefinitions.webhook.enabled": ("registers a Zendesk webhook (outside)", _RULING),
    "zendesk-kog.snowplow.restActions.enabled": ("snowplow RESTActions that call Zendesk (outside)", _RULING),
}
#: what the bench writes itself, never 057's (bench/installer_values.py), said by every check
BENCH_OWN = {
    "portal.readerGroups": "the bench's base group (bench/rbac_model.py): its users are in it; 057's groups are "
                           "057's identities (the owner (Diego), 2026-11-22)",
}


class Refused(ValueError):
    """Why a reading, a record or a merge cannot be used, naming the path."""


# ─── reading 057's componentValues against the installer's schema ──────────

def _children(node: dict | None) -> dict:
    return ((node or {}).get("properties") or {}) if isinstance(node, dict) else {}


def _is_switch(node: dict, value) -> bool:
    t = node.get("type")
    if isinstance(value, bool):
        return t == "boolean" or (isinstance(t, list) and "boolean" in t)
    enum = node.get("enum")
    return (isinstance(value, str) and value in SWITCH_STRINGS and isinstance(enum, list) and bool(enum)
            and set(enum) <= SWITCH_STRINGS)


def _is_size(segments: list[str], value) -> bool:
    if isinstance(value, bool):
        return False
    sized = "resources" in segments[:-1] or segments[-1] in SIZE_KEYS
    return sized and (isinstance(value, (int, float)) or (isinstance(value, str) and bool(_QTY.fullmatch(value))))


# ─── the key allow-list (the owner's ruling, 2026-11-22: "copy them via a key allow-list") ──
#
# A free-form map's keys are recorded ONLY when the installer's values schema says what they name, and
# what they name is a product agent or component: its `description` says it is "keyed by agent name" or
# "keyed by component name" (installer 0.3.697: componentValues.agentiko.agents, "Agents to deploy, keyed
# by agent name."), and its values have a schema of their own (additionalProperties: {...}) whose
# switches and sizes are then recorded as everywhere else. A key must also be a product name (a DNS
# label). Nothing is inferred from a key's text: every other free-form map, above all one keyed by user
# name (a ClickHouse users map), is "a free-form map: N keys, not recorded", a named fidelity gap.

PRODUCT_KEYED = re.compile(r"\bkeyed by (?:the )?(?:agent|component) name\b", re.I)
_PRODUCT_NAME = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")


def product_keyed_map(node) -> bool:
    """The schema says this map's keys name a product agent or component (the allow-list above)."""
    return (isinstance(node, dict) and isinstance(node.get("additionalProperties"), dict)
            and bool(PRODUCT_KEYED.search(str(node.get("description") or ""))))


def component_reading(component_values, schema: dict) -> tuple[dict, dict]:
    """(recorded {path: switch or size}, not_recorded {path: why}) of an Installer CR's
    spec.componentValues, by the installer chart's values `schema`."""
    if not isinstance(component_values, dict):
        raise Refused("the Installer's spec.componentValues is not a map")
    comps = _children(((schema.get("properties") or {}).get("componentValues")))
    recorded: dict = {}
    not_recorded: dict = {}
    undeclared_top = sum(1 for c in component_values if c not in comps)
    if undeclared_top:
        not_recorded["(componentValues)"] = NOT_SETTABLE.format(n=undeclared_top)

    def walk(value, node, segments):
        path = ".".join(segments)
        if isinstance(value, dict):
            if not value:
                not_recorded[path] = AN_EMPTY_MAP
                return
            kids = _children(node)
            n = 0
            # a Kubernetes ResourceList (resources.limits / resources.requests): its keys are resource names
            # (cpu, memory, ...), never identities, and its values sizes
            resource_list = len(segments) >= 2 and segments[-2] == "resources" and segments[-1] in ("limits", "requests")
            product_keyed = product_keyed_map(node)
            for k, v in value.items():
                if k in kids:
                    walk(v, kids[k], segments + [k])
                elif resource_list and _RESOURCE_NAME.fullmatch(k) and _is_size(segments + [k], v):
                    recorded[".".join(segments + [k])] = v
                elif product_keyed and _PRODUCT_NAME.fullmatch(k):
                    walk(v, node["additionalProperties"], segments + [k])
                else:
                    n += 1
            if n:
                # a key the schema does not name: rejected under a closed map (additionalProperties: false),
                # free-form under an open one (absent, true, or a schema for the values only)
                closed = isinstance(node, dict) and node.get("additionalProperties") is False
                not_recorded[path] = (NOT_SETTABLE if closed else FREE_FORM).format(n=n)
        elif isinstance(value, list):
            not_recorded[path] = A_LIST
        elif _is_switch(node, value) or _is_size(segments, value):
            recorded[path] = value
        else:
            not_recorded[path] = NOT_A_SWITCH_OR_SIZE

    for comp in sorted(c for c in component_values if c in comps):
        walk(component_values[comp], comps[comp], [comp])
    return dict(sorted(recorded.items())), dict(sorted(not_recorded.items()))


def schema_node(schema: dict, segments: list[str]) -> dict | None:
    """The installer chart's values schema node for componentValues.<segments>, or None."""
    node = (schema.get("properties") or {}).get("componentValues")
    for seg in segments:
        nxt = _children(node).get(seg)
        if nxt is None and product_keyed_map(node) and _PRODUCT_NAME.fullmatch(seg):
            nxt = node["additionalProperties"]          # a product name under an allow-listed map
        if nxt is None:
            return None
        node = nxt
    return node


def declared(schema: dict, path: str) -> bool:
    """The installer chart's values schema declares componentValues.<path> (a resource name under a
    declared resources.limits / resources.requests map counts: a ResourceList names its keys freely)."""
    segs = path.split(".")
    if schema_node(schema, segs) is not None:
        return True
    parent = schema_node(schema, segs[:-1])
    return (parent is not None and len(segs) >= 3 and segs[-3] == "resources" and segs[-2] in ("limits", "requests")
            and parent.get("additionalProperties") is not False)


# ─── the record ────────────────────────────────────────────────────────────

def validate(rec) -> dict:
    if not isinstance(rec, dict) or rec.get("kind") != RECORD_KIND:
        raise Refused(f"not an {RECORD_KIND} record")
    read = rec.get("read")
    if not isinstance(read, dict) or not read.get("date") or not read.get("installer_version"):
        raise Refused("the record names no reading (read.date, read.installer_version)")
    recorded, not_recorded = rec.get("recorded"), rec.get("not_recorded")
    if not isinstance(recorded, dict) or not isinstance(not_recorded, dict):
        raise Refused("the record has no recorded / not_recorded maps")
    for path, v in recorded.items():
        segs = path.split(".")
        ok = (isinstance(v, bool) or (isinstance(v, str) and v in SWITCH_STRINGS)
              or _is_size(segs, v))
        if not ok:
            raise Refused(f"recorded {path}: neither a switch nor a size")
    for path, why in not_recorded.items():
        if not isinstance(why, str) or not why:
            raise Refused(f"not_recorded {path}: no reason")
    if set(recorded) & set(not_recorded):
        raise Refused("a path is both recorded and not recorded")
    return rec


def load(path: str = RECORD_PATH) -> dict:
    import json
    with open(path, encoding="utf-8") as fh:
        return validate(json.load(fh))


def bench_paths(rec: dict | None = None, deviations: dict | None = None) -> dict:
    """{path: value} the bench writes from the record: every recorded switch and size, but the deviations."""
    rec = load() if rec is None else validate(rec)
    deviations = DEVIATIONS if deviations is None else deviations
    for p, (why, ruling) in deviations.items():
        if not (why and ruling):
            raise Refused(f"deviation {p}: it names no reason or no ruling")
    return {p: v for p, v in rec["recorded"].items() if p not in deviations}


def nest(paths: dict) -> dict:
    out: dict = {}
    for path, v in sorted(paths.items()):
        cur = out
        segs = path.split(".")
        for s in segs[:-1]:
            nxt = cur.setdefault(s, {})
            if not isinstance(nxt, dict):
                raise Refused(f"{path}: {s} is both a value and a map")
            cur = nxt
        cur[segs[-1]] = v
    return out


def merge(recorded: dict, own: dict) -> dict:
    """The componentValues a run writes: the record's (nested) under the bench's own; a path both set
    is refused (the bench wins only where a deviation or BENCH_OWN names the path)."""
    import copy
    out = copy.deepcopy(recorded)

    def put(into, frm, prefix):
        for k, v in frm.items():
            p = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict) and isinstance(into.get(k), dict):
                put(into[k], v, p)
            elif k in into:
                if p not in DEVIATIONS and p not in BENCH_OWN:
                    raise Refused(f"componentValues.{p}: the bench sets it and 057's record does too "
                                  f"(a deviation must name it)")
                into[k] = copy.deepcopy(v)
            else:
                into[k] = copy.deepcopy(v)
    put(out, own, "")
    return out


def lines(rec: dict | None = None) -> list[str]:
    """What every check says: what is written, what is not and why."""
    rec = load() if rec is None else validate(rec)
    written = bench_paths(rec)
    gaps = {p: w for p, w in rec["not_recorded"].items() if w.startswith(("not settable", "a free-form map"))}
    out = [f"component values: {len(written)} switch(es) and size(s) written as 057 (installer "
           f"{rec['read']['installer_version']}, read {rec['read']['date']}) sets them; "
           f"{len(rec['not_recorded'])} path(s) not recorded (no endpoint, group, identity or credential is ever copied)"]
    out += [f"the bench does not copy componentValues.{p} ({ruling}): {why}"
            for p, (why, ruling) in sorted(DEVIATIONS.items()) if p in rec["recorded"]]
    out += [f"the bench writes componentValues.{p} itself: {why}" for p, why in sorted(BENCH_OWN.items())]
    out += [f"fidelity gap, componentValues.{p}: {why}" for p, why in sorted(gaps.items())]
    return out


def record(listed, schema: dict, date: str, version: str) -> dict:
    items = (listed or {}).get("items") if isinstance(listed, dict) else None
    if not isinstance(items, list) or len(items) != 1:
        raise Refused("the reading is not exactly one Installer")
    cv = ((items[0] or {}).get("spec") or {}).get("componentValues") or {}
    recorded, not_recorded = component_reading(cv, schema)
    return validate({
        "kind": RECORD_KIND,
        "read": {"date": date, "installer_version": version,
                 "object": "installers.composition.krateo.io (the one Installer CR), spec.componentValues",
                 "rule": "switches and sizes by installer " + version + "'s values schema; every other path by "
                         "path and reason only, never its value"},
        "recorded": recorded,
        "not_recorded": not_recorded,
    })


def drift(rec: dict, listed, schema: dict) -> list[str]:
    items = (listed or {}).get("items") or [{}]
    cv = ((items[0] or {}).get("spec") or {}).get("componentValues") or {}
    recorded, not_recorded = component_reading(cv, schema)
    out = []
    for p in sorted(set(recorded) | set(rec["recorded"])):
        a, b = rec["recorded"].get(p, "absent"), recorded.get(p, "absent")
        if a != b:
            out.append(f"componentValues.{p}: {a} in the record, {b} on 057 now")
    for p in sorted(set(not_recorded) ^ set(rec["not_recorded"])):
        out.append(f"componentValues.{p}: {'no longer' if p in rec['not_recorded'] else 'now'} a not-recorded path")
    return out


def check_chart(schema: dict, version: str, rec: dict | None = None) -> None:
    """Refused unless the chart's schema declares every path the bench writes from the record."""
    rec = load() if rec is None else validate(rec)
    missing = sorted(p for p in bench_paths(rec) if not declared(schema, p))
    if missing:
        raise Refused(f"installer {version} cannot express {len(missing)} of 057's recorded component value(s): "
                      f"{', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}")
