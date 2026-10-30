"""#606 on-cluster progress + failure diagnostics. STDLIB ONLY, no bench imports.

Shakedown #2 (2026-10-14) waited 45 minutes blind on
`kubectl wait installers/installer --for=condition=Ready`, timed out, and tore
the cluster down with nothing captured — so nobody can say WHY the Installer
never went Ready. Two commands fix that:

wait-installer   Poll the Installer CR and every composition.krateo.io leaf CR
                 every `interval` seconds and print which components are Ready
                 and which are not (with their condition reason/message), so a
                 stall shows WHERE. Exit 0 on Ready, 1 on timeout.
collect          Snapshot the cluster into <out>/: Installer CR, all
                 CompositionDefinitions and leaf CRs with Ready/Synced
                 conditions + messages, pods (status, restarts, last-state
                 reason), recent events sorted by time, PVCs, nodes, helm
                 releases, and the last 200 log lines of every failing pod.

Safety: every kubectl/helm call carries an EXPLICIT --kubeconfig/--context
(never the ambient one); Secrets are never read; everything written passes
through `redact()` (JWTs, bearer tokens, token/password-looking values).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from typing import Callable

__all__ = ["redact", "summarize_conditions", "pod_summary", "failing_pods",
           "installer_progress", "Kube", "collect", "wait_installer",
           "evaluate483", "verify483", "chart_inspector_errors",
           "platform_state", "platform_line", "parse_not_ready_children", "declared_components",
           "core_features", "create_state", "definition_record", "definitions_snapshot",
           "controller_logs", "controller_images", "platform_report", "wedge_window",
           "component_windows", "PROVIDER_RUNTIME_RELEASES",
           "allow_list_from_env", "DEFAULT_ALLOW_NOT_READY", "REQUIRED_READY",
           "main"]

COMP_GROUP = "composition.krateo.io"
LOG_TAIL = 200
MAX_FAILING_POD_LOGS = 40
MAX_ITEMS_PER_KIND = 300      # the 50K bench kind must not explode the bundle

_REDACTIONS = [
    (re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+"), "<redacted-jwt>"),
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"), r"\1<redacted>"),
    (re.compile(r"(?i)(gh[pousr]_)[A-Za-z0-9]{20,}"), r"\1<redacted>"),
]

# key=value / "key": "value" credentials. The value is replaced IN PLACE with
# its quoting preserved and never past a quote, so redacted JSON stays JSON
# (a value inside a JSON string, e.g. a condition message "…token=abc…", must
# not swallow the string's closing quote).
_KV = re.compile(
    r'(?i)(?P<key>"?\b(?:access_?token|api_?key|client_?secret|password|passwd|secret|token)"?\s*[:=]\s*)'
    r'(?:(?P<q>")(?P<qv>(?:[^"\\]|\\.)*)"|(?P<v>[^\s,}"\'\\]+))')


def _kv_sub(m: re.Match) -> str:
    if m.group("q"):
        return f'{m.group("key")}"<redacted>"'
    return f'{m.group("key")}<redacted>'


#: the keys under which an object carries a REGISTRY credential: the installer
#: copies its registryAuth (username included) into the Installer CR's spec
#: (installer 0.3.468 self-bootstrap.yaml:191, `pick .Values ... "registryAuth"`),
#: and stamps every CompositionDefinition's spec.chart.credentials from it
#: (_helpers.tpl inst.chartExtras). Its `username` is the pull account's
#: identity: redacted by KEY wherever such an object is written (#1007 review F1).
CREDENTIAL_PARENTS = ("registryAuth", "credentials")


def redact_credentials(obj, under: bool = False):
    """`obj` with every `username` under a registryAuth / credentials key (at
    any depth: registryAuth.registries[].username too) replaced."""
    if isinstance(obj, dict):
        return {k: ("<redacted>" if under and k == "username" and v else
                     redact_credentials(v, under or k in CREDENTIAL_PARENTS)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact_credentials(v, under) for v in obj]
    return obj


def _pull_account() -> tuple[str, ...]:
    """The ghcr pull account's username and token (bench/ghcr_pull.py), read
    from the operator's file, never from argv: literals every bundle and log
    this module writes redacts, wherever they appear (a controller log line
    naming the account included). () when there is no usable file."""
    from bench import ghcr_pull
    path = os.environ.get("GHCR_PULL_ENV_FILE") or ghcr_pull.DEFAULT_ENV_FILE
    if ghcr_pull.check(path):
        return ()
    return tuple(x for x in ghcr_pull.creds(path) if x)


def redact(text: str, extra: tuple[str, ...] = ()) -> str:
    for rx, rep in _REDACTIONS:
        text = rx.sub(rep, text)
    text = _KV.sub(_kv_sub, text)
    for s in extra:
        if s:
            text = text.replace(s, "<redacted>")
    return text


# ─── pure summaries ─────────────────────────────────────────────────────────


def summarize_conditions(obj: dict, resource: str | None = None) -> dict:
    """One object's readiness, in TWO shapes (both documented in README.txt):

    - flattened:  .Ready.status / .Ready.reason / .Ready.message (and .Synced)
    - raw:        .status.conditions[] exactly as the API returned them, so a
                  query written against the API shape also works on the bundle
                  (the installer session's `.status.conditions[]` query
                  silently matched nothing against the flattened-only shape).
    `id` is <resource>/<namespace>/<name>, the form the Installer CR's
    Ready.message uses to name its not-ready children.
    """
    md = obj.get("metadata") or {}
    conds = ((obj.get("status") or {}).get("conditions")) or []
    out = {"kind": obj.get("kind"), "namespace": md.get("namespace"),
           "name": md.get("name")}
    if resource:
        out["id"] = f"{resource}/{md.get('namespace')}/{md.get('name')}"
    raw = []
    for c in conds:
        # VERBATIM (run 3's chart-inspector error was cut at 500 chars, just
        # before the part that names the missing CRD); the cap only guards
        # against a pathological multi-MB message.
        cc = dict(c)
        cc["message"] = (c.get("message") or "")[:20000]
        raw.append(cc)
        if c.get("type") in ("Ready", "Synced"):
            out[c["type"]] = {"status": c.get("status"), "reason": c.get("reason"),
                              "message": cc["message"]}
    out["status"] = {"conditions": raw}
    return out


BUNDLE_README = """\
#606 bench diagnostics bundle — FILE SHAPES (all JSON is redacted; no Secrets)

Readiness objects (composition-crs.json .installer[] / .components[],
compositiondefinitions.json [], focus-483.json .components.*.*) carry BOTH:
  flattened   .Ready.status  .Ready.reason  .Ready.message   (same for .Synced)
  raw         .status.conditions[]   exactly as the API returned them
  plus        .kind .namespace .name and .id = <resource>/<namespace>/<name>
              (the form the Installer CR's Ready.message uses for children)

Example queries (jq):
  jq -r '.components[] | select(.Ready.status!="True") | .id' composition-crs.json
  jq -r '.components[] | select(any(.status.conditions[]; .type=="Synced" and .status=="False")) | .id' composition-crs.json
  jq -r '.[] | select(.Synced.status=="False") | "\\(.id): \\(.Synced.message)"' compositiondefinitions.json
  jq -r '.installer[0].Ready.message' composition-crs.json

Other files:
  installer-cr.json            raw Installer CR(s) (the API object, unflattened)
  platform-state.json          ok, installer_message, CR/CD tallies, required_ready,
                               declared_created (component CRs created / declared
                               by the Installer CR's spec.components, feature on),
                               allowed_not_ready (allow-listed, reason matched),
                               unexpected_not_ready (declared/<Kind>/<name> = declared,
                               not created yet), allow_list
                               composition-crs.json .installer[].declares lists them
  progress.txt                 the one-line platform summary
  chart-inspector-errors.txt   every condition message mentioning chart-inspector, VERBATIM
  focus-483.json               alert_crd_present + CD/CR summaries of alert-provider-crd,
                               alert-provider, krateo-observability
  pods.json / pods-failing.json  phase, node, unschedulable message; per container:
                               ready, restarts, waiting, terminated, last_reason, last_exit, image
  events.json                  last 1000 events, oldest first: time,type,reason,object,count,message
  pvcs.json  nodes.json  helm-list.json
  services.json                namespace, name, type, ports, lb_ingress (empty = LB pending)
  logs/<ns>__<pod>__<container>__{current,previous}.log   last 200 lines, failing pods only

Method notes (learned the hard way; read before changing how the bench installs):
  * LOCAL-CHART INSTALLS CANNOT WORK for this installer (run 4, 2026-10-14). The
    installer's self-CompositionDefinition pulls its OWN chart from the registry at
    .Chart.Version (installer chart templates/self-bootstrap.yaml:215), and the cdc
    ServiceAccount and CR version are derived from .Chart.Version as well
    (self-bootstrap.yaml:71, :72; ordered-teardown.yaml:30). A locally stamped
    version (e.g. 0.0.0-pr483) does not exist in the registry, so the install never
    converges. Fixes must be merged and RELEASED, then installed by INSTALLER_VERSION;
    run606.sh refuses INSTALLER_GIT_REF / INSTALLER_CHART_DIR.
  * "chart-inspector clean" is NOT evidence an install is progressing (run 4): with
    no chart-inspector error anywhere, the install still could not converge. Judge
    progress by the Installer CR's Ready.message children and the CR/CD tallies in
    platform-state.json, never by the absence of an error.
  * The umbrella Installer is NEVER Ready on a credential-free bench (github-provider-kog
    stays 401 on the private krateo-blueprints registry, installer#484). No bench step
    waits on it; run606 waits on the snowplow/frontend/authn Deployments instead.
  * External LoadBalancer IPs are not needed (run 5: IN_USE_ADDRESSES quota, 14 global,
    left snowplow/frontend LBs pending). The load driver and the freshness probe run
    in-cluster against Service DNS (ENDPOINT_MODE=incluster, the default).
"""


# ─── platform readiness: BOTH layers + an explicit allow-list ───────────────

# Known, expected and unrelated to the component under test: the bench is
# credential-free by ruling (2026-10-14), and github-provider-kog's chart lives
# in the PRIVATE ghcr.io/krateo-blueprints registry. Allowed ONLY for this
# exact child AND only while its failure carries this reason; the same child
# failing for any other reason, or any other child failing, fails the run.
# Override with BENCH_ALLOW_NOT_READY="id=reason-substring;id=…" (";"-separated).
# The reason substring is taken from run 3's REAL Synced message:
#   "... failed to resolve reference ghcr.io/krateo-blueprints/charts/
#    github-provider-kog:0.3.2: HEAD ...: GET \"https://ghcr.io/token?...\":
#    response status code 401: unauthorized"
DEFAULT_ALLOW_NOT_READY = (
    ("compositiondefinitions/krateo-system/github-provider-kog",
     "response status code 401"),
)
# The 50K measurement needs these component CRs Ready (by name).
REQUIRED_READY = ("snowplow", "frontend", "authn", "portal")

_CHILDREN_RX = re.compile(r"managed children are not ready:\s*(.+)$", re.S)


_CORE_FEATURES_RX = re.compile(
    r'\{\{-?\s*define\s+"inst\.coreFeatures"\s*-?\}\}(.*?)\{\{-?\s*end\s*-?\}\}', re.S)


def core_features(chart: str) -> tuple[str, ...]:
    """The features the installer chart turns on whatever spec.features says:
    the body of its `inst.coreFeatures` define (templates/_helpers.tpl), which
    inst.featureEnabled reads first. Empty in 0.3.459
    (`{{- define "inst.coreFeatures" -}}{{- end -}}`), read here from the
    chart the source step PULLED, so an installer that adds a core feature
    cannot slip past the declared-components rule. `chart` is the chart
    directory or the pull workdir holding exactly one. Raises ValueError when
    the define is missing or is not a static space-separated list: the rule
    this gate mirrors is then not the chart's."""
    if not os.path.isdir(chart):
        raise ValueError(f"{chart}: no pulled installer chart there (the source step pulls it): "
                         "the installer's core features cannot be read")
    if not os.path.isfile(os.path.join(chart, "Chart.yaml")):
        subs = [d for d in sorted(os.listdir(chart))
                if os.path.isfile(os.path.join(chart, d, "Chart.yaml"))]
        if len(subs) != 1:
            raise ValueError(f"{chart}: not a chart and not a pull holding one chart ({subs})")
        chart = os.path.join(chart, subs[0])
    tdir = os.path.join(chart, "templates")
    bodies = []
    for name in sorted(os.listdir(tdir)) if os.path.isdir(tdir) else []:
        if name.endswith((".tpl", ".yaml")):
            with open(os.path.join(tdir, name)) as fh:
                bodies += _CORE_FEATURES_RX.findall(fh.read())
    if len(bodies) != 1:
        raise ValueError(f"{chart}: {len(bodies)} inst.coreFeatures defines (want 1): "
                         "the installer's feature rule is not the one this gate mirrors")
    body = bodies[0].strip()
    if "{{" in body:
        raise ValueError(f"{chart}: inst.coreFeatures is not a static list ({body[:120]!r})")
    return tuple(body.split())


def declared_components(installer: dict, core: tuple[str, ...] = ()) -> list[dict] | None:
    """The component CRs this Installer CR WILL create: the entries of its
    spec.components that are not registerOnly and whose feature is on
    (no feature = on; a `core` feature = on, see core_features; otherwise
    spec.features[<feature>] is true). This is
    the installer chart's own rule: templates/compositions.yaml Pass B emits a
    component when inst.featureEnabled (templates/_helpers.tpl) holds, once
    its CRD is served and its deps are Ready, "re-evaluated every reconcile;
    the next layer unlocks as deps go Ready". So the CRs that EXIST at a
    moment are not the platform: a layer whose deps have only just gone
    Ready does not exist yet, while the Installer CR already reads
    Ready=True "Composition is up-to-date" (run 17, installer 0.3.459: 24 of
    the 38 declared CRs existed, all Ready; the 13 pages and portal-agents,
    each deps: [portal], came 14-30 s later, and the sidebar menu built from
    those pages was empty). None when spec.components cannot be read."""
    spec = installer.get("spec") or {}
    comps = spec.get("components")
    if not isinstance(comps, list):
        return None
    feats = spec.get("features") or {}
    out = []
    for c in comps:
        if not isinstance(c, dict) or not c.get("name") or not c.get("kind") or c.get("registerOnly"):
            continue
        f = c.get("feature")
        if f and f not in core and not feats.get(f):
            continue
        out.append({"name": c["name"], "kind": c["kind"], "deps": list(c.get("deps") or [])})
    return out


def allow_list_from_env(env=os.environ) -> tuple[tuple[str, str], ...]:
    raw = env.get("BENCH_ALLOW_NOT_READY")
    if raw is None:
        return DEFAULT_ALLOW_NOT_READY
    out = []
    for part in raw.split(";"):
        part = part.strip()
        if part:
            ident, _, reason = part.partition("=")
            out.append((ident.strip(), reason.strip()))
    return tuple(out)


def parse_not_ready_children(message: str) -> list[str]:
    """Child ids named by the Installer CR's Ready.message, e.g.
    '2 of 41 managed children are not ready: compositiondefinitions/ns/a, x/ns/b'."""
    m = _CHILDREN_RX.search(message or "")
    if not m:
        return []
    return [c.strip().rstrip(".") for c in m.group(1).split(",") if c.strip()]


def _messages(s: dict | None) -> str:
    return "\n".join((c.get("message") or "") for c in
                     ((s or {}).get("status") or {}).get("conditions") or [])


def platform_state(installer: list[dict], leaves: list[dict], cds: list[dict],
                   allow=DEFAULT_ALLOW_NOT_READY, required=REQUIRED_READY) -> dict:
    """Readiness across BOTH layers (CompositionDefinitions and component CRs).

    ok ⇔ an Installer CR exists, every component it DECLARES (its
    `declares`, see declared_components) exists, every not-ready child is on
    the allow-list WITH its expected reason, and every `required` component
    CR is Ready. A declared component with no CR yet is not-ready; it is
    excused only when its CompositionDefinition is itself an ALLOWED not-ready
    child, i.e. not Ready for its allow-listed reason (a definition that never
    serves its CRD cannot create the CR; a Ready one, or one failing for any
    other reason, excuses nothing). An Installer
    CR whose declaration cannot be read is not-ready: which components it
    will still create is then unknown.
    """
    inst = installer[0] if installer else {}
    inst_msg = (inst.get("Ready") or {}).get("message") or ""
    by_id = {s.get("id"): s for s in leaves + cds if s.get("id")}
    not_ready = set(parse_not_ready_children(inst_msg))
    not_ready |= {s["id"] for s in leaves + cds
                  if s.get("id") and _cond(s, "Ready") != "True"}
    allow_map = dict(allow)
    allowed, unexpected = [], []
    for ident in sorted(not_ready):
        s = by_id.get(ident)
        want = allow_map.get(ident)
        msgs = _messages(s)
        if want is not None and s is not None and want.lower() in msgs.lower():
            allowed.append({"id": ident, "expected_reason": want,
                            "message": msgs[:2000]})
        else:
            unexpected.append({"id": ident,
                               "why": ("not on the allow-list" if want is None else
                                       "allow-listed, but the expected reason "
                                       f"{want!r} is not in its conditions"),
                               "reason": (s or {}).get("Ready", {}).get("reason"),
                               "message": msgs[:2000]})
    declares = inst.get("declares") if installer else None
    present = {(s.get("kind"), s.get("name")) for s in leaves}
    if installer:
        if declares is None:
            unexpected.append({"id": inst.get("id") or "installer",
                               "why": "its spec.components cannot be read: which components "
                                      "it will still create is unknown",
                               "reason": "DeclarationUnreadable", "message": ""})
        for c in declares or []:
            if (c["kind"], c["name"]) in present:
                continue
            cd = f"compositiondefinitions/{inst.get('namespace')}/{c['name']}"
            row = {"id": f"declared/{c['kind']}/{c['name']}", "reason": "NotCreated",
                   "message": (f"declared by the Installer CR (feature on), no {c['kind']} CR yet"
                               + (f"; deps: {', '.join(c['deps'])}" if c["deps"] else ""))}
            excused = next((a for a in allowed if a["id"] == cd), None)
            if excused is not None:
                allowed.append({**row, "expected_reason": f"its definition {cd} is an allowed not-ready "
                                                          f"child ({excused['expected_reason']})"})
            else:
                unexpected.append({**row, "why": "declared, not created yet"})
    by_name = {s.get("name"): s for s in leaves}
    req = {n: _cond(by_name.get(n), "Ready") == "True" for n in required}
    ok = bool(installer) and not unexpected and all(req.values())
    return {"ok": ok, "installer_ready": _cond(inst, "Ready"),
            "installer_message": inst_msg,
            "components_ready": f"{sum(_cond(s, 'Ready') == 'True' for s in leaves)}/{len(leaves)}",
            "definitions_ready": f"{sum(_cond(s, 'Ready') == 'True' for s in cds)}/{len(cds)}",
            "declared_created": (None if declares is None else
                                 f"{sum((c['kind'], c['name']) in present for c in declares)}/{len(declares)}"),
            "required_ready": req, "allowed_not_ready": allowed,
            "unexpected_not_ready": unexpected,
            "allow_list": [{"id": i, "expected_reason": r} for i, r in allow]}


def platform_line(st: dict) -> str:
    line = (f"installer Ready={st['installer_ready'] or 'absent'} "
            f"msg={st['installer_message'][:300]!r}; CRs ready {st['components_ready']}; "
            f"declared CRs created {st.get('declared_created') or 'UNKNOWN'}; "
            f"CDs ready {st['definitions_ready']}; required "
            + ",".join(f"{n}={'ok' if v else 'NO'}" for n, v in st["required_ready"].items()))
    if st["allowed_not_ready"]:
        line += "; ALLOWED(known): " + ", ".join(
            f"{a['id']}[{a['expected_reason']}]" for a in st["allowed_not_ready"])
    if st["unexpected_not_ready"]:
        line += "; NOT READY: " + ", ".join(
            f"{u['id']}({u['reason']}: {u['message'][:120]})" for u in st["unexpected_not_ready"])
    return line


# ─── #483 (installer#482: Alert CRD gate) ───────────────────────────────────

ALERT_CRD = "alerts.observability.krateo.io"
FOCUS_COMPONENTS = ("alert-provider-crd", "alert-provider", "krateo-observability")


def _cond(s: dict | None, t: str) -> str | None:
    return ((s or {}).get(t) or {}).get("status")


def evaluate483(installer: list[dict], leaves: list[dict], cds: list[dict],
                alert_crd_present: bool, alerts: list[dict], *,
                allow=DEFAULT_ALLOW_NOT_READY, required=REQUIRED_READY) -> dict:
    """Pure: the run-4 assertions, narrowed to the component under test.

    Items 1-4 gate; item 5 is recorded only; item 6 gates that no child
    OUTSIDE the allow-list is not-ready (both layers), so the allow-list can
    excuse only the known-unrelated github-provider-kog 401.
    """
    by_name = {s.get("name"): s for s in leaves}
    cd_names = {s.get("name") for s in cds}
    obs = by_name.get("krateo-observability")
    st = platform_state(installer, leaves, cds, allow, required)
    checks = [
        {"item": 1, "check": "KrateoObservability/krateo-observability Ready=True and Synced=True",
         "pass": (obs is not None and _cond(obs, "Ready") == "True"
                  and _cond(obs, "Synced") == "True"),
         "detail": {"ready": _cond(obs, "Ready"), "synced": _cond(obs, "Synced"),
                    "kind": (obs or {}).get("kind")}},
        {"item": 2, "check": f"crd {ALERT_CRD} present",
         "pass": bool(alert_crd_present)},
        {"item": 3, "check": "alert-provider-crd in CompositionDefinitions",
         "pass": "alert-provider-crd" in cd_names},
        {"item": 4, "check": "alert-provider ABSENT (present = gate too wide)",
         "pass": "alert-provider" not in cd_names and "alert-provider" not in by_name,
         "detail": {"in_cds": "alert-provider" in cd_names,
                    "as_component": "alert-provider" in by_name}},
        {"item": 6, "check": "no not-ready child outside the allow-list (CDs + CRs)",
         "pass": not st["unexpected_not_ready"],
         "detail": {"unexpected": st["unexpected_not_ready"]}},
    ]
    alert_rows = [{"name": (a.get("metadata") or {}).get("name"),
                   "namespace": (a.get("metadata") or {}).get("namespace"),
                   "has_status": bool(a.get("status"))} for a in alerts]
    names = {r["name"] for r in alert_rows}
    info = {"item": 5, "check": "default Alert CRs exist with no status (recorded, not gating)",
            "sre_pod_crashloop_present": "sre-pod-crashloop" in names,
            "alerts": alert_rows,
            "as_expected": ("sre-pod-crashloop" in names
                            and not any(r["has_status"] for r in alert_rows))}
    # Item 4 is the HEADLINE (run 5): the #483 gate must install the CRD
    # without pulling in alert-provider. It is listed first everywhere.
    checks.sort(key=lambda c: (c["item"] != 4, c["item"]))
    headline = next(c for c in checks if c["item"] == 4)
    return {"verdict": "PASS" if all(c["pass"] for c in checks) else "FAIL",
            "headline": {"item": 4, "check": headline["check"],
                         "result": "PASS" if headline["pass"] else "FAIL"},
            "checks": checks, "recorded": info,
            "known_unrelated_allowed": st["allowed_not_ready"],
            "allow_list": st["allow_list"],
            "installer_message": st["installer_message"]}


def _alert_crd_present(k: "Kube") -> bool:
    rc, _, _ = k.kubectl("get", "crd", ALERT_CRD, "-o", "name")
    return rc == 0


def verify483(k: "Kube", allow=None, required=REQUIRED_READY, core: tuple[str, ...] = ()) -> dict:
    allow = allow_list_from_env() if allow is None else allow
    inst, leaves = _leaf_summaries(k, core=core)
    cds = _cd_summaries(k)
    present = _alert_crd_present(k)
    alerts = k.items(ALERT_CRD, "-A") if present else []
    return evaluate483(inst, leaves, cds, present, alerts,
                       allow=allow, required=required)


def chart_inspector_errors(summaries: list[dict]) -> list[str]:
    out = []
    for s in summaries:
        for t in ("Ready", "Synced"):
            msg = (s.get(t) or {}).get("message") or ""
            if "chart-inspector" in msg:
                out.append(f"{s.get('kind')}/{s.get('namespace')}/{s.get('name')} "
                           f"{t}={(s.get(t) or {}).get('status')}:\n{msg}\n")
    return out


def _ready(summary: dict) -> bool:
    return (summary.get("Ready") or {}).get("status") == "True"


def pod_summary(pod: dict) -> dict:
    md, st = pod.get("metadata") or {}, pod.get("status") or {}
    ctrs = []
    for cs in (st.get("initContainerStatuses") or []) + (st.get("containerStatuses") or []):
        state = cs.get("state") or {}
        waiting = (state.get("waiting") or {}).get("reason")
        term = (state.get("terminated") or {}).get("reason")
        last = (cs.get("lastState") or {}).get("terminated") or {}
        ctrs.append({"name": cs.get("name"), "ready": cs.get("ready"),
                     "restarts": cs.get("restartCount", 0),
                     "waiting": waiting, "terminated": term,
                     "last_reason": last.get("reason"),
                     "last_exit": last.get("exitCode"),
                     "image": cs.get("image")})
    return {"namespace": md.get("namespace"), "name": md.get("name"),
            "phase": st.get("phase"), "node": (pod.get("spec") or {}).get("nodeName"),
            "reason": st.get("reason"),
            "unschedulable": next((c.get("message") for c in st.get("conditions") or []
                                   if c.get("type") == "PodScheduled"
                                   and c.get("status") == "False"), None),
            "containers": ctrs}


def failing_pods(summaries: list[dict]) -> list[dict]:
    bad = []
    for p in summaries:
        if p["phase"] == "Succeeded":
            continue
        if (p["phase"] != "Running" or p["unschedulable"]
                or any(not c["ready"] or c["restarts"] or c["waiting"]
                       for c in p["containers"])):
            bad.append(p)
    return bad


def installer_progress(installer: list[dict], leaves: list[dict]) -> tuple[bool, str]:
    """(installer_ready, one progress line) from summarized CRs."""
    inst_ready = bool(installer) and all(_ready(s) for s in installer)
    ready = sorted(s["name"] for s in leaves if _ready(s))
    pending = sorted(leaves, key=lambda s: s["name"] or "")
    pend = [f"{s['name']}({(s.get('Ready') or {}).get('reason') or 'no-status'}"
            f"{': ' + (s.get('Ready') or {}).get('message', '')[:120] if (s.get('Ready') or {}).get('message') else ''})"
            for s in pending if not _ready(s)]
    inst = installer[0] if installer else {}
    line = (f"installer Ready={(inst.get('Ready') or {}).get('status', 'absent')}"
            f" reason={(inst.get('Ready') or {}).get('reason')}; "
            f"components ready {len(ready)}/{len(leaves)}"
            + (f"; NOT ready: {', '.join(pend)}" if pend else ""))
    return inst_ready, line


# ─── cluster access ─────────────────────────────────────────────────────────


class Kube:
    def __init__(self, kubeconfig: str, context: str,
                 run: Callable = subprocess.run):
        if not kubeconfig or not context:
            raise ValueError("explicit kubeconfig and context are required")
        if "krateo-057" in context:
            raise ValueError("krateo-057 is never a bench target (#606)")
        self.kc, self.ctx, self._run = kubeconfig, context, run

    def kubectl(self, *args, timeout=120) -> tuple[int, str, str]:
        if any(a in ("secret", "secrets") or str(a).startswith("secret")
               for a in args[:3]):
            raise ValueError("diag606 never reads Secrets")
        p = self._run(["kubectl", f"--kubeconfig={self.kc}", f"--context={self.ctx}",
                       *args], capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr

    def helm(self, *args, timeout=120) -> tuple[int, str, str]:
        p = self._run(["helm", "--kubeconfig", self.kc, "--kube-context", self.ctx,
                       *args], capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr

    def items(self, *args) -> list[dict]:
        rc, out, _ = self.kubectl("get", *args, "-o", "json")
        if rc != 0:
            return []
        try:
            return json.loads(out).get("items", [])
        except ValueError:
            return []

    def composition_kinds(self) -> list[str]:
        out = []
        for crd in self.items("crd"):
            spec = crd.get("spec") or {}
            if spec.get("group") == COMP_GROUP:
                out.append(f"{spec['names']['plural']}.{COMP_GROUP}")
        return sorted(out)


def _leaf_summaries(k: Kube, skip_installer=True, core: tuple[str, ...] = ()) -> tuple[list[dict], list[dict]]:
    """(installer CRs, platform component CRs). Objects in bench-* namespaces
    are bench DATA (the 50K held compositions), never platform children."""
    inst, leaves = [], []
    for kind in k.composition_kinds():
        resource = kind.split(".", 1)[0]
        objs = [o for o in k.items(kind, "-A")
                if not ((o.get("metadata") or {}).get("namespace") or "").startswith("bench-")]
        sums = [summarize_conditions(o, resource) for o in objs[:MAX_ITEMS_PER_KIND]]
        if kind.startswith("installers."):
            for s, o in zip(sums, objs):
                s["declares"] = declared_components(o, core)
            inst.extend(sums)
        else:
            leaves.extend(sums)
    return inst, leaves


def _cd_summaries(k: Kube) -> list[dict]:
    return [summarize_conditions(o, "compositiondefinitions")
            for o in k.items("compositiondefinitions.core.krateo.io", "-A")]


#: a container's waiting reasons that mean its image cannot be pulled
IMAGE_PULL_REASONS = ("ErrImagePull", "ImagePullBackOff")


def image_pull_failures(k: Kube) -> list[dict]:
    """Every container (init containers included) waiting on an image it
    cannot pull: its pod, image, waiting reason and message, the image-pull
    Secrets the pod references, and what the kubelet said about retrieving
    them (FailedToRetrieveImagePullSecret: the Secret is missing). Read from
    pods and events only: Secrets are never read."""
    missing = {}
    for ev in k.items("events", "-A", "--field-selector", "reason=FailedToRetrieveImagePullSecret"):
        io = ev.get("involvedObject") or {}
        if ev.get("reason") == "FailedToRetrieveImagePullSecret" and io.get("kind") == "Pod":
            missing[(io.get("namespace"), io.get("name"))] = ev.get("message") or ""
    out = []
    for pod in k.items("pods", "-A"):
        md, spec, st = pod.get("metadata") or {}, pod.get("spec") or {}, pod.get("status") or {}
        images = {c.get("name"): c.get("image") for c in (spec.get("initContainers") or []) + (spec.get("containers") or [])}
        for cs in (st.get("initContainerStatuses") or []) + (st.get("containerStatuses") or []):
            w = (cs.get("state") or {}).get("waiting") or {}
            if w.get("reason") not in IMAGE_PULL_REASONS:
                continue
            key = (md.get("namespace"), md.get("name"))
            out.append({"pod": f"{key[0]}/{key[1]}", "container": cs.get("name"),
                        "image": cs.get("image") or images.get(cs.get("name")),
                        "reason": w.get("reason"), "message": redact(w.get("message") or ""),
                        "pull_secrets": [x.get("name") for x in spec.get("imagePullSecrets") or []],
                        "pull_secret_missing": redact(missing[key]) if key in missing else None})
    return out


def image_pull_line(f: dict) -> str:
    secrets = ", ".join(f["pull_secrets"]) or "none"
    tail = f"; kubelet: {f['pull_secret_missing']}" if f["pull_secret_missing"] else ""
    return (f"{f['pod']} [{f['container']}] {f['image']}: {f['reason']} "
            f"(image-pull Secrets referenced: {secrets}{tail})")


def wait_installer(k: Kube, *, timeout_s: float, interval_s: float = 60,
                   allow=None, required=REQUIRED_READY, core: tuple[str, ...] = (),
                   log: Callable[[str], None] = print,
                   clock=time.monotonic, sleep=time.sleep, reason: list | None = None) -> bool:
    """Poll until every child (CDs + CRs) is Ready except the allow-listed
    known-unrelated ones, and every `required` component is Ready. Every
    poll also reads the pods whose image cannot be pulled and names each at
    the poll it appears; on a timeout `reason` gets the failure's NAME: those
    image pulls first (with their image and the pull Secrets they reference),
    then what was not Ready."""
    allow = allow_list_from_env() if allow is None else allow
    log("[platform] allow-list (known, unrelated): "
        + (", ".join(f"{i}[{r}]" for i, r in allow) or "(empty)"))
    deadline = clock() + timeout_s
    seen: set = set()
    while True:
        inst, leaves = _leaf_summaries(k, core=core)
        st = platform_state(inst, leaves, _cd_summaries(k), allow, required)
        log(f"[platform] {platform_line(st)}")
        pulls = image_pull_failures(k)
        for f in pulls:
            if (f["pod"], f["container"], f["reason"]) not in seen:
                seen.add((f["pod"], f["container"], f["reason"]))
                log(f"[platform] IMAGE PULL FAILING: {image_pull_line(f)}")
        if st["ok"]:
            log("[platform] verdict READY"
                + (" (with allow-listed known-unrelated: "
                   + ", ".join(a["id"] for a in st["allowed_not_ready"]) + ")"
                   if st["allowed_not_ready"] else ""))
            return True
        if clock() >= deadline:
            log(f"[platform] TIMEOUT after {timeout_s:.0f}s")
            why = (f"image pull: {'; '.join(image_pull_line(f) for f in pulls)}; then " if pulls else "")
            why += f"not Ready after {timeout_s:.0f}s: {platform_line(st)}"
            log(f"[platform] FAILED: {why}")
            if reason is not None:
                reason.append(why)
            return False
        sleep(interval_s)


def collect(k: Kube, out_dir: str, *, extra_secrets: tuple[str, ...] = (),
            core: tuple[str, ...] = (), log: Callable[[str], None] = print) -> dict:
    """Write the diagnostics bundle. Never raises: a probe that fails records
    its error, so one broken call cannot cost the rest of the bundle. The
    pull account's literals are redacted whoever the caller is."""
    extra_secrets = tuple(extra_secrets) + _pull_account()
    os.makedirs(out_dir, exist_ok=True)
    written: dict[str, str] = {}

    def w(name: str, data):
        txt = data if isinstance(data, str) else json.dumps(redact_credentials(data), indent=2, default=str)
        path = os.path.join(out_dir, name)
        with open(path, "w") as fh:
            fh.write(redact(txt, extra_secrets))
        written[name] = path

    def safe(name, fn):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            w(name + ".error.txt", f"{type(e).__name__}: {e}")

    cd_box: list[dict] = []

    def readme():
        w("README.txt", BUNDLE_README)

    def cds():
        sums = _cd_summaries(k)
        cd_box.extend(sums)
        w("compositiondefinitions.json", sums)

    def installer_and_leaves():
        inst_raw = k.items(f"installers.{COMP_GROUP}", "-A")
        w("installer-cr.json", inst_raw)
        inst, leaves = _leaf_summaries(k, core=core)
        w("composition-crs.json", {"installer": inst, "components": leaves})
        st = platform_state(inst, leaves, cd_box, allow_list_from_env())
        w("platform-state.json", st)
        w("progress.txt", platform_line(st) + "\n")

    def focus():
        # Run-4 focus: the chart-inspector error VERBATIM, and how far the
        # #482/#483 Alert-CRD chain got (did alert-provider-crd reach Ready?).
        inst, leaves = _leaf_summaries(k)
        errs = chart_inspector_errors(inst + leaves + cd_box)
        w("chart-inspector-errors.txt", "\n".join(errs) or "(none)\n")
        by_leaf = {s.get("name"): s for s in leaves}
        by_cd = {s.get("name"): s for s in cd_box}
        w("focus-483.json", {
            "alert_crd_present": _alert_crd_present(k),
            "components": {n: {"composition_definition": by_cd.get(n),
                               "component_cr": by_leaf.get(n)}
                           for n in FOCUS_COMPONENTS}})

    pods_box: list[dict] = []

    def pods():
        sums = [pod_summary(p) for p in k.items("pods", "-A")]
        pods_box.extend(sums)
        w("pods.json", sums)
        w("pods-failing.json", failing_pods(sums))

    def events():
        ev = k.items("events", "-A", "--sort-by=.lastTimestamp")
        rows = [{"time": e.get("lastTimestamp") or e.get("eventTime"),
                 "type": e.get("type"), "reason": e.get("reason"),
                 "object": f"{(e.get('involvedObject') or {}).get('kind')}/"
                           f"{(e.get('involvedObject') or {}).get('namespace')}/"
                           f"{(e.get('involvedObject') or {}).get('name')}",
                 "count": e.get("count"), "message": (e.get("message") or "")[:500]}
                for e in ev[-1000:]]
        w("events.json", rows)

    def pvcs():
        w("pvcs.json", [{"namespace": (p.get("metadata") or {}).get("namespace"),
                         "name": (p.get("metadata") or {}).get("name"),
                         "phase": (p.get("status") or {}).get("phase"),
                         "class": (p.get("spec") or {}).get("storageClassName"),
                         "request": (((p.get("spec") or {}).get("resources") or {})
                                     .get("requests") or {}).get("storage")}
                        for p in k.items("pvc", "-A")])

    def services():
        # run 5: snowplow/frontend LBs were pending (IN_USE_ADDRESSES quota)
        # and the bundle could only show it through events.
        w("services.json", [{
            "namespace": (s.get("metadata") or {}).get("namespace"),
            "name": (s.get("metadata") or {}).get("name"),
            "type": (s.get("spec") or {}).get("type"),
            "ports": [p.get("port") for p in (s.get("spec") or {}).get("ports") or []],
            "lb_ingress": ((s.get("status") or {}).get("loadBalancer") or {}).get("ingress")}
            for s in k.items("svc", "-A")])

    def nodes():
        w("nodes.json", [{"name": (n.get("metadata") or {}).get("name"),
                          "labels": {kk: vv for kk, vv in ((n.get("metadata") or {}).get("labels") or {}).items()
                                     if kk.startswith(("bench.", "cloud.google.com/gke-nodepool"))},
                          "conditions": [{"type": c.get("type"), "status": c.get("status")}
                                         for c in (n.get("status") or {}).get("conditions") or []],
                          "allocatable": (n.get("status") or {}).get("allocatable")}
                         for n in k.items("nodes")])

    def helm_list():
        rc, out, err = k.helm("list", "-A", "-a", "-o", "json")
        w("helm-list.json", out if rc == 0 else f"rc={rc} {err}")

    def logs():
        bad = failing_pods(pods_box)[:MAX_FAILING_POD_LOGS]
        os.makedirs(os.path.join(out_dir, "logs"), exist_ok=True)
        for p in bad:
            for c in p["containers"]:
                for prev in ((False, True) if c["restarts"] else (False,)):
                    args = ["logs", "-n", p["namespace"], p["name"], "-c", c["name"],
                            f"--tail={LOG_TAIL}"] + (["--previous"] if prev else [])
                    rc, out, err = k.kubectl(*args, timeout=60)
                    tag = "previous" if prev else "current"
                    w(f"logs/{p['namespace']}__{p['name']}__{c['name']}__{tag}.log",
                      out if rc == 0 else f"rc={rc} {err}")

    for name, fn in (("readme", readme), ("cds", cds),
                     ("installer", installer_and_leaves),
                     ("focus", focus), ("pods", pods),
                     ("events", events), ("pvcs", pvcs), ("services", services),
                     ("nodes", nodes),
                     ("helm", helm_list), ("logs", logs)):
        safe(name, fn)
    log(f"[diagnostics] {len(written)} files -> {out_dir}")
    return written


# ─── RestDefinitions / CompositionDefinitions: the create handshake ─────────
#
# A run lost a whole attempt to a wedged platform component, and the bundle
# kept neither controller logs nor the managed objects' annotations: the
# cause was unprovable afterwards. Recorded at the end of the platform step and
# at every teardown, so the bench can say what it OBSERVED (it fixes nothing).

CREATE_PENDING = "krateo.io/external-create-pending"
CREATE_SUCCEEDED = "krateo.io/external-create-succeeded"
CREATE_FAILED = "krateo.io/external-create-failed"
# What a pending-without-terminal means depends on the provider-runtime the
# controller CARRIES (read from its image, controller_images), per release
# (krateo-platformops/provider-runtime, each tag's pkg/reconciler/reconciler.go,
# read 2026-10-28):
#   v1.4.0 .. v1.5.3 (897399b "give an unresolved create a bounded path back"):
#     an unresolved create keeps its pending marker and is re-observed every
#     defaultCreateRecoveryReobserveInterval (15 s, :40) until
#     defaultCreateRecoveryGracePeriod (2 m, :38) has elapsed, then retried.
#     The measured recovery (2m0.1s) lands on the first re-observe after the
#     grace period: a wedge once older than grace + 2 x re-observe.
#   v1.0.0 .. v1.3.1: no recovery path. v1.3.0 refuses an incomplete create
#     for good (:766-773); v1.3.1 asks Observe, and one it cannot see is still
#     never retried. Each reconcile runs under r.timeout + reconcileGracePeriod
#     (1 m + 30 s, :29-30, applied at v1.3.0 :662): a pending marker older than
#     that has no reconcile left that could finish it.
#   any other version (newer, a pseudo-version, not read): UNKNOWN, never a
#   default window.
_RECOVERY = {"recovery": True, "grace_s": 120.0, "reobserve_s": 15.0,
             "source": "pkg/reconciler/reconciler.go:38 defaultCreateRecoveryGracePeriod = 2 * time.Minute, "
                       ":40 defaultCreateRecoveryReobserveInterval = 15 * time.Second"}
_NO_RECOVERY = {"recovery": False, "reconcile_timeout_s": 60.0, "reconcile_grace_s": 30.0,
                "source": "pkg/reconciler/reconciler.go:29 reconcileGracePeriod = 30 * time.Second, "
                          ":30 reconcileTimeout = 1 * time.Minute (each reconcile's bound); an incomplete "
                          "create is never retried"}
PROVIDER_RUNTIME_RELEASES = {
    **{v: dict(_NO_RECOVERY, commit=c) for v, c in (
        ("v1.0.0", "5e5132d9"), ("v1.1.0", "c308f606"), ("v1.2.0", "b23717e5"), ("v1.2.1", "ac67ea6e"),
        ("v1.2.2", "d9ca4ac0"), ("v1.3.0", "81781579"), ("v1.3.1", "e60fb883"))},
    **{v: dict(_RECOVERY, commit=c) for v, c in (
        ("v1.4.0", "897399ba"), ("v1.5.0", "34efb047"), ("v1.5.1", "7a8b8d46"), ("v1.5.2", "7f81abec"),
        ("v1.5.3", "855ed1e2"))},
}
DEFINITION_KINDS = (("restdefinitions.ogen.krateo.io", "restdefinitions", "oasgen-provider"),
                    ("compositiondefinitions.core.krateo.io", "compositiondefinitions", "core-provider"))
# the two controllers, by app.kubernetes.io/name EXACTLY (not core-provider-chart-inspector,
# not the agents)
CONTROLLERS = ("oasgen-provider", "core-provider")
# provider-runtime's module path, under both org spellings it has had
PROVIDER_RUNTIME = ("github.com/krateo-platformops/provider-runtime",
                    "github.com/krateoplatformops/provider-runtime")


def wedge_window(version: str | None, why: str | None = None) -> dict:
    """How long a pending-without-terminal may last under this provider-runtime
    before it is WEDGED (see PROVIDER_RUNTIME_RELEASES). `wedge_after_s` None:
    the version is unknown, and so is the window — never a default one."""
    rel = PROVIDER_RUNTIME_RELEASES.get(version or "")
    if rel is None:
        return {"version": version, "wedge_after_s": None, "recovery": None,
                "why": why or (f"provider-runtime {version} is not a release this bench has read the recovery "
                               f"rules of" if version else "the provider-runtime version is unknown")}
    bound = (rel["grace_s"] + 2 * rel["reobserve_s"] if rel["recovery"]
             else rel["reconcile_timeout_s"] + rel["reconcile_grace_s"])
    return {"version": version, "wedge_after_s": bound, "recovery": rel["recovery"],
            "source": f"provider-runtime {version} ({rel['commit']}) {rel['source']}"}


def _ts(v) -> float | None:
    if not isinstance(v, str):
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def create_state(annotations: dict | None, now: float, window: dict) -> dict:
    """The create handshake as the object's own annotations tell it, at `now`,
    judged by `window` (wedge_window of the controller's provider-runtime).

    A pending marker newer than BOTH terminal markers (strictly, as
    meta.ExternalCreateIncomplete: a marker at the same second resolved it) is
    WEDGED once older than the window by its own timestamp, `pending` inside
    it, and UNKNOWN when the window is (the runtime's version unknown). Under a
    runtime with no recovery path a WEDGED one says so. cleared: superseded by
    a later succeeded; create-failed: by a later failed; no-pending: no
    pending marker; unreadable: a marker that is not a timestamp."""
    a = annotations or {}
    raw = {k: a.get(f"krateo.io/external-create-{k}") for k in ("pending", "succeeded", "failed")}
    out = {"annotations": {k: v for k, v in raw.items() if v is not None}}
    if raw["pending"] is None:
        return {**out, "state": "no-pending"}
    t = {k: _ts(v) for k, v in raw.items() if v is not None}
    if any(v is None for v in t.values()):
        return {**out, "state": "unreadable"}
    pending = t["pending"]
    terminal = {k: t[k] for k in ("succeeded", "failed") if k in t}
    if all(pending > v for v in terminal.values()):
        age = round(now - pending, 1)
        bound = window.get("wedge_after_s")
        if bound is None:
            return {**out, "pending_age_s": age, "state": "UNKNOWN",
                    "why": f"no window to judge it by: {window.get('why')}"}
        if age <= bound:
            return {**out, "pending_age_s": age, "state": "pending"}
        return {**out, "pending_age_s": age, "state": "WEDGED",
                **({} if window.get("recovery") else
                   {"note": f"no recovery path in this runtime (provider-runtime {window.get('version')})"})}
    last = max(terminal, key=lambda k: (terminal[k], k == "succeeded"))
    return {**out, "state": "cleared" if last == "succeeded" else "create-failed"}


def definition_record(obj: dict, resource: str, now: float, window: dict) -> dict:
    """One RestDefinition / CompositionDefinition as the bench saw it."""
    s = summarize_conditions(obj, resource)
    st = obj.get("status") or {}
    rec = {"id": s.get("id"), "kind": obj.get("kind"), "namespace": s.get("namespace"), "name": s.get("name"),
           **create_state((obj.get("metadata") or {}).get("annotations"), now, window),
           "ready": s.get("Ready"), "synced": s.get("Synced")}
    managed = st.get("managed")
    if isinstance(managed, dict):              # CompositionDefinition: status.managed{group,kind}, apiVersion
        rec["managed"] = {"group": managed.get("group"), "kind": managed.get("kind")}
    if isinstance(st.get("apiVersion"), str):
        rec["apiVersion"] = st["apiVersion"]
    if st.get("resource") is not None:         # RestDefinition: status.resource{apiVersion,kind}
        rec["resource"] = st["resource"]
    return rec


STATES = ("WEDGED", "UNKNOWN", "pending", "cleared", "create-failed", "no-pending", "unreadable")


def component_windows(images: list[dict]) -> dict[str, dict]:
    """Per controller, the window of the provider-runtime its pods carry. Its
    pods disagreeing, or none read, is UNKNOWN (with why)."""
    out = {}
    for comp in CONTROLLERS:
        rows = [i for i in images if i.get("component") == comp]
        versions = sorted({(i.get("provider_runtime") or {}).get("version") for i in rows} - {None})
        whys = sorted({(i.get("provider_runtime") or {}).get("why") for i in rows} - {None})
        if not rows:
            out[comp] = wedge_window(None, f"no {comp} pod was found")
        elif len(versions) == 1 and not whys:
            out[comp] = wedge_window(versions[0])
        elif len(versions) > 1:
            out[comp] = wedge_window(None, f"its pods carry different provider-runtimes: {', '.join(versions)}")
        else:
            out[comp] = wedge_window(None, "; ".join(whys) or "no pod's provider-runtime was read")
    return out


def definitions_snapshot(k: "Kube", *, at: str, windows: dict[str, dict],
                         now: Callable[[], float] = time.time) -> dict:
    """Every RestDefinition and CompositionDefinition, each judged by the
    window of the controller that reconciles it."""
    t = now()
    objects, unlisted = [], []
    for kind, resource, comp in DEFINITION_KINDS:
        rc, out, err = k.kubectl("get", kind, "-A", "-o", "json")
        try:
            items = json.loads(out).get("items", []) if rc == 0 else None
        except ValueError:
            items = None
        if items is None:
            unlisted.append({"kind": kind, "rc": rc, "error": _first_line(err)})
            continue
        w = windows.get(comp) or wedge_window(None, f"no window for {comp}")
        objects += [{**definition_record(o, resource, t, w), "controller": comp} for o in items]
    counts = {st: sum(1 for o in objects if o["state"] == st) for st in STATES}
    return {"kind": "platform_definitions", "at": at, "t": t, "context": k.ctx, "windows": windows,
            "counts": counts, "unlisted": unlisted, "objects": objects}


def _first_line(text: str) -> str:
    return next((ln for ln in (text or "").splitlines() if ln.strip()), "")[:300]


def _redact_log(text: str, extra: tuple[str, ...] = ()) -> str:
    """A log as every bench log is kept: credentials (redact) AND identities
    (bench/identity.py: users, e-mails, Secret names, CNs). identity imports
    nothing from bench, so this module stays free of bench's import-time
    context guard."""
    from bench import identity
    return "\n".join(identity.redact_identity(ln) for ln in redact(text, extra).split("\n"))


def _redact_obj(obj, extra: tuple[str, ...] = ()):
    """Every string of a record through _redact_log (a condition message, an
    error line can name a user): the record is written AND printed from this."""
    if isinstance(obj, str):
        return _redact_log(obj, extra)
    if isinstance(obj, dict):
        return {key: _redact_obj(v, extra) for key, v in redact_credentials(obj).items()}
    if isinstance(obj, list):
        return [_redact_obj(v, extra) for v in obj]
    return obj


def controller_pods(k: "Kube") -> list[dict]:
    rc, out, _ = k.kubectl("get", "pods", "-A", "-l",
                           f"app.kubernetes.io/name in ({','.join(CONTROLLERS)})", "-o", "json")
    try:
        return json.loads(out).get("items", []) if rc == 0 else []
    except ValueError:
        return []


def controller_logs(k: "Kube", pods: list[dict], out_dir: str, *, extra: tuple[str, ...] = ()) -> list[dict]:
    """Every container log of the controllers, IN FULL (no --tail: the line
    after a failure is never cut), with timestamps; the previous container's
    too when it restarted. Redacted like every bench log, current AND previous."""
    os.makedirs(out_dir, exist_ok=True)
    out = []
    for p in pods:
        md = p.get("metadata") or {}
        restarts = {c.get("name"): c.get("restartCount") or 0
                    for c in (p.get("status") or {}).get("containerStatuses") or []}
        for c in (p.get("spec") or {}).get("containers") or []:
            for prev in ((False, True) if restarts.get(c["name"]) else (False,)):
                args = ["logs", "-n", md.get("namespace"), md.get("name"), "-c", c["name"], "--timestamps"]
                rc, text, err = k.kubectl(*(args + (["--previous"] if prev else [])), timeout=120)
                name = (f"{md.get('namespace')}__{md.get('name')}__{c['name']}__"
                        f"{'previous' if prev else 'current'}.log")
                path = os.path.join(out_dir, name)
                body = _redact_log(text if rc == 0 else f"rc={rc} {_first_line(err)}", extra)
                with open(path, "w") as fh:
                    fh.write(body)
                out.append({"component": (md.get("labels") or {}).get("app.kubernetes.io/name"),
                            "pod": md.get("name"), "container": c["name"], "previous": prev, "rc": rc,
                            "lines": len(text.splitlines()) if rc == 0 else 0, "file": path})
    return out


def controller_images(pods: list[dict], *, read=None, known: dict[str, dict] | None = None) -> list[dict]:
    """Which provider-runtime each controller carries, read from the image
    digest the pod runs (bench/imagemod.py), never assumed from a pin.
    `known`: image digest -> a read already made this run (the platform_end
    record), reused for the same digest."""
    if read is None:
        from bench import imagemod
        read = imagemod.image_deps
    out, cache = [], dict(known or {})
    for p in pods:
        md = p.get("metadata") or {}
        spec = {c["name"]: c for c in (p.get("spec") or {}).get("containers") or []}
        for cs in (p.get("status") or {}).get("containerStatuses") or []:
            ref = (cs.get("imageID") or "").replace("docker-pullable://", "") or cs.get("image")
            row = {"component": (md.get("labels") or {}).get("app.kubernetes.io/name"), "pod": md.get("name"),
                   "container": cs.get("name"), "image": cs.get("image"), "image_id": ref}
            if ref not in cache:
                try:
                    got = read(ref, PROVIDER_RUNTIME, command=(spec.get(cs.get("name")) or {}).get("command"))
                    got = dict(got, version=(got.get("replaced_by") or {}).get("version") or got.get("version"))
                except Exception as e:  # noqa: BLE001 — evidence: why it could not be read
                    got = {"version": None, "why": f"{type(e).__name__}: {e}"[:300]}
                cache[ref] = {k2: got.get(k2) for k2 in ("module", "version", "process_binary", "why")
                              if got.get(k2) is not None}
            row["provider_runtime"] = cache[ref]
            out.append(row)
    return out


def definitions_line(rec: dict) -> str:
    """The run log's summary, built from the REDACTED record (the CLI passes it)."""
    c = rec["counts"]
    line = (f"[platform] definitions at {rec['at']}: {len(rec['objects'])} object(s); WEDGED {c['WEDGED']} "
            f"(expected 0); UNKNOWN {c['UNKNOWN']}; pending in the window {c['pending']}; cleared {c['cleared']}; "
            f"create-failed {c['create-failed']}")
    for comp, w in (rec.get("windows") or {}).items():
        line += (f"\n[platform]   {comp}: provider-runtime {w.get('version') or 'UNKNOWN'}: "
                 + (f"WEDGED after {w['wedge_after_s']:g} s"
                    + ("" if w.get("recovery") else " (no recovery path in this runtime)")
                    if w.get("wedge_after_s") is not None else f"NO WINDOW ({w.get('why')})"))
    for o in rec["objects"]:
        if o["state"] in ("WEDGED", "UNKNOWN"):
            line += (f"\n[platform]   {o['state']} {o['id']}: pending {o['annotations'].get('pending')} "
                     f"({o['pending_age_s']:g} s old), Ready={(o.get('ready') or {}).get('status')} "
                     f"({(o.get('ready') or {}).get('reason')}), Synced={(o.get('synced') or {}).get('status')} "
                     f"({(o.get('synced') or {}).get('reason')})"
                     + (f"; {o['note']}" if o.get("note") else "") + (f"; {o['why']}" if o.get("why") else ""))
    for u in rec["unlisted"]:
        line += f"\n[platform]   could not list {u['kind']} (rc={u['rc']}): {u['error']}"
    return line


def platform_report(snaps: list[dict], caps: list[dict] = ()) -> dict:
    """The verdict's platform section: per snapshot (platform_end, teardown)
    the counts; for the cluster, WEDGED (expected 0), UNKNOWN, cleared, the
    windows and provider-runtimes (with why when one was not read), and any
    snapshot cut short by its cap."""
    if not snaps:
        return {"status": "absent", "cap_hits": list(caps),
                "note": "no platform_definitions record (a run before the bench recorded them, or every "
                        "snapshot was cut short)"}
    snaps = sorted(snaps, key=lambda r: r.get("t") or 0)
    last = snaps[-1]
    images = next((r.get("controller_images") for r in snaps if r.get("controller_images")), None)
    return {"status": "present", "context": last.get("context"),
            "wedged": max(r["counts"]["WEDGED"] for r in snaps), "expected_wedged": 0,
            "unknown": max(r["counts"]["UNKNOWN"] for r in snaps),
            "cleared": last["counts"]["cleared"], "windows": last.get("windows") or {},
            "cap_hits": list(caps),
            "snapshots": [{"at": r["at"], "t": r["t"], "counts": r["counts"], "unlisted": r.get("unlisted") or [],
                           "flagged": [{"id": o["id"], "state": o["state"], "pending": o["annotations"].get("pending"),
                                        "pending_age_s": o.get("pending_age_s"),
                                        **({"note": o["note"]} if o.get("note") else {}),
                                        **({"why": o["why"]} if o.get("why") else {})}
                                       for o in r["objects"] if o["state"] in ("WEDGED", "UNKNOWN")]}
                          for r in snaps],
            "provider_runtime": [{"component": i["component"], "image": i.get("image"),
                                  "version": (i.get("provider_runtime") or {}).get("version"),
                                  **({"why": i["provider_runtime"]["why"]}
                                     if (i.get("provider_runtime") or {}).get("why") else {})}
                                 for i in images or []]}


def _known_images(path: str) -> dict[str, dict]:
    """This run's earlier provider-runtime reads (platform-definitions.jsonl), by image digest."""
    out: dict[str, dict] = {}
    try:
        with open(path) as fh:
            for ln in fh:
                try:
                    r = json.loads(ln)
                except ValueError:
                    continue
                for i in r.get("controller_images") or []:
                    if i.get("image_id") and (i.get("provider_runtime") or {}).get("version"):
                        out[i["image_id"]] = i["provider_runtime"]
    except OSError:
        pass
    return out


# ─── CLI ────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.diag606")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("wait-installer", "collect", "verify483", "definitions"):
        s = sub.add_parser(name)
        s.add_argument("--kubeconfig", required=True)
        s.add_argument("--context", required=True)
        if name == "wait-installer":
            s.add_argument("--timeout-s", type=float, default=2700)
            s.add_argument("--interval-s", type=float, default=60)
            s.add_argument("--reason-out", help="file the failure's named reason is written to")
        else:
            s.add_argument("--out", required=True)
        if name == "definitions":
            s.add_argument("--at", required=True, choices=("platform_end", "teardown"))
            s.add_argument("--logs-dir", required=True)
            s.add_argument("--redact", action="append", default=[])
            continue
        if name == "collect":
            s.add_argument("--redact", action="append", default=[],
                           help="extra literal to redact (repeatable)")
        # the PULLED installer chart (the source step's workdir): its
        # inst.coreFeatures. Required where the platform is GATED; the
        # diagnostics bundle reads it when it exists (a run can fail before
        # the pull)
        s.add_argument("--installer-chart", required=name != "collect",
                       help="the pulled installer chart, or the pull workdir holding it")
    a = p.parse_args(argv)
    if a.cmd == "definitions":
        k = Kube(a.kubeconfig, a.context)
        pods = controller_pods(k)
        # the provider-runtime each controller carries decides its objects' window; a digest already
        # read this run (the platform_end record) is not read again
        images = controller_images(pods, known=_known_images(a.out))
        rec = definitions_snapshot(k, at=a.at, windows=component_windows(images))
        rec["controller_images"] = images
        extra = tuple(a.redact) + _pull_account()
        rec["controller_logs"] = controller_logs(k, pods, a.logs_dir, extra=extra)
        rec = _redact_obj(rec, extra)      # written AND printed redacted
        with open(a.out, "a") as fh:
            fh.write(json.dumps(rec, default=str) + "\n")
        print(definitions_line(rec), flush=True)
        return 0                                   # evidence, not a gate
    try:
        core = core_features(a.installer_chart) if a.installer_chart and (
            a.cmd != "collect" or os.path.isdir(a.installer_chart)) else ()
    except ValueError as e:
        print(f"refusing: {e}", file=sys.stderr, flush=True)
        if a.cmd != "collect":
            return 2
        core = ()
    k = Kube(a.kubeconfig, a.context)
    if a.cmd == "verify483":
        res = verify483(k, core=core)
        with open(a.out, "w") as fh:
            fh.write(redact(json.dumps(redact_credentials(res), indent=2, default=str), _pull_account()))
        h = res["headline"]
        print(f"[verify483] HEADLINE (4) {h['result']} {h['check']}", flush=True)
        for c in res["checks"]:
            print(f"[verify483] ({c['item']}) {'PASS' if c['pass'] else 'FAIL'} "
                  f"{c['check']}", flush=True)
        r = res["recorded"]
        print(f"[verify483] (5) RECORDED sre-pod-crashloop present="
              f"{r['sre_pod_crashloop_present']} as_expected={r['as_expected']} "
              f"alerts={len(r['alerts'])}", flush=True)
        print("[verify483] allow-list (known, unrelated): "
              + ", ".join(f"{a['id']}[{a['expected_reason']}]" for a in res["allow_list"]),
              flush=True)
        for a in res["known_unrelated_allowed"]:
            print(f"[verify483] KNOWN-UNRELATED not ready (allowed): {a['id']} "
                  f"reason matched {a['expected_reason']!r}", flush=True)
        print(f"[verify483] verdict {res['verdict']} "
              f"(headline: alert-provider ABSENT = {h['result']})", flush=True)
        return 0 if res["verdict"] == "PASS" else 1
    if a.cmd == "wait-installer":
        print(f"[platform] installer core features (inst.coreFeatures): {' '.join(core) or '(none)'}",
              flush=True)
        why: list = []
        ok = wait_installer(k, timeout_s=a.timeout_s, interval_s=a.interval_s, core=core,
                            log=lambda m: print(m, flush=True), reason=why)
        if not ok and a.reason_out:
            with open(a.reason_out, "w") as fh:
                fh.write((why[0] if why else "wait-installer failed") + "\n")
        return 0 if ok else 1
    collect(k, a.out, extra_secrets=tuple(a.redact), core=core,
            log=lambda m: print(m, file=sys.stderr, flush=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
