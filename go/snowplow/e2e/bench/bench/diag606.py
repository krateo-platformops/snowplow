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
           "platform_state", "platform_line", "parse_not_ready_children",
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
                               allowed_not_ready (allow-listed, reason matched),
                               unexpected_not_ready, allow_list
  progress.txt                 the one-line platform summary
  chart-inspector-errors.txt   every condition message mentioning chart-inspector, VERBATIM
  focus-483.json               alert_crd_present + CD/CR summaries of alert-provider-crd,
                               alert-provider, krateo-observability
  pods.json / pods-failing.json  phase, node, unschedulable message; per container:
                               ready, restarts, waiting, terminated, last_reason, last_exit, image
  events.json                  last 1000 events, oldest first: time,type,reason,object,count,message
  pvcs.json  nodes.json  helm-list.json
  logs/<ns>__<pod>__<container>__{current,previous}.log   last 200 lines, failing pods only
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

    ok ⇔ an Installer CR exists, every not-ready child is on the allow-list
    WITH its expected reason, and every `required` component CR is Ready.
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
    by_name = {s.get("name"): s for s in leaves}
    req = {n: _cond(by_name.get(n), "Ready") == "True" for n in required}
    ok = bool(installer) and not unexpected and all(req.values())
    return {"ok": ok, "installer_ready": _cond(inst, "Ready"),
            "installer_message": inst_msg,
            "components_ready": f"{sum(_cond(s, 'Ready') == 'True' for s in leaves)}/{len(leaves)}",
            "definitions_ready": f"{sum(_cond(s, 'Ready') == 'True' for s in cds)}/{len(cds)}",
            "required_ready": req, "allowed_not_ready": allowed,
            "unexpected_not_ready": unexpected,
            "allow_list": [{"id": i, "expected_reason": r} for i, r in allow]}


def platform_line(st: dict) -> str:
    line = (f"installer Ready={st['installer_ready'] or 'absent'} "
            f"msg={st['installer_message'][:300]!r}; CRs ready {st['components_ready']}; "
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
    return {"verdict": "PASS" if all(c["pass"] for c in checks) else "FAIL",
            "checks": checks, "recorded": info,
            "known_unrelated_allowed": st["allowed_not_ready"],
            "allow_list": st["allow_list"],
            "installer_message": st["installer_message"]}


def _alert_crd_present(k: "Kube") -> bool:
    rc, _, _ = k.kubectl("get", "crd", ALERT_CRD, "-o", "name")
    return rc == 0


def verify483(k: "Kube", allow=None, required=REQUIRED_READY) -> dict:
    allow = allow_list_from_env() if allow is None else allow
    inst, leaves = _leaf_summaries(k)
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


def _leaf_summaries(k: Kube, skip_installer=True) -> tuple[list[dict], list[dict]]:
    """(installer CRs, platform component CRs). Objects in bench-* namespaces
    are bench DATA (the 50K held compositions), never platform children."""
    inst, leaves = [], []
    for kind in k.composition_kinds():
        resource = kind.split(".", 1)[0]
        objs = [o for o in k.items(kind, "-A")
                if not ((o.get("metadata") or {}).get("namespace") or "").startswith("bench-")]
        sums = [summarize_conditions(o, resource) for o in objs[:MAX_ITEMS_PER_KIND]]
        if kind.startswith("installers."):
            inst.extend(sums)
        else:
            leaves.extend(sums)
    return inst, leaves


def _cd_summaries(k: Kube) -> list[dict]:
    return [summarize_conditions(o, "compositiondefinitions")
            for o in k.items("compositiondefinitions.core.krateo.io", "-A")]


def wait_installer(k: Kube, *, timeout_s: float, interval_s: float = 60,
                   allow=None, required=REQUIRED_READY,
                   log: Callable[[str], None] = print,
                   clock=time.monotonic, sleep=time.sleep) -> bool:
    """Poll until every child (CDs + CRs) is Ready except the allow-listed
    known-unrelated ones, and every `required` component is Ready."""
    allow = allow_list_from_env() if allow is None else allow
    log("[platform] allow-list (known, unrelated): "
        + (", ".join(f"{i}[{r}]" for i, r in allow) or "(empty)"))
    deadline = clock() + timeout_s
    while True:
        inst, leaves = _leaf_summaries(k)
        st = platform_state(inst, leaves, _cd_summaries(k), allow, required)
        log(f"[platform] {platform_line(st)}")
        if st["ok"]:
            log("[platform] verdict READY"
                + (" (with allow-listed known-unrelated: "
                   + ", ".join(a["id"] for a in st["allowed_not_ready"]) + ")"
                   if st["allowed_not_ready"] else ""))
            return True
        if clock() >= deadline:
            log(f"[platform] TIMEOUT after {timeout_s:.0f}s")
            return False
        sleep(interval_s)


def collect(k: Kube, out_dir: str, *, extra_secrets: tuple[str, ...] = (),
            log: Callable[[str], None] = print) -> dict:
    """Write the diagnostics bundle. Never raises: a probe that fails records
    its error, so one broken call cannot cost the rest of the bundle."""
    os.makedirs(out_dir, exist_ok=True)
    written: dict[str, str] = {}

    def w(name: str, data):
        txt = data if isinstance(data, str) else json.dumps(data, indent=2, default=str)
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
        inst, leaves = _leaf_summaries(k)
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
                     ("events", events), ("pvcs", pvcs), ("nodes", nodes),
                     ("helm", helm_list), ("logs", logs)):
        safe(name, fn)
    log(f"[diagnostics] {len(written)} files -> {out_dir}")
    return written


# ─── CLI ────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.diag606")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("wait-installer", "collect", "verify483"):
        s = sub.add_parser(name)
        s.add_argument("--kubeconfig", required=True)
        s.add_argument("--context", required=True)
        if name == "wait-installer":
            s.add_argument("--timeout-s", type=float, default=2700)
            s.add_argument("--interval-s", type=float, default=60)
        else:
            s.add_argument("--out", required=True)
        if name == "collect":
            s.add_argument("--redact", action="append", default=[],
                           help="extra literal to redact (repeatable)")
    a = p.parse_args(argv)
    k = Kube(a.kubeconfig, a.context)
    if a.cmd == "verify483":
        res = verify483(k)
        with open(a.out, "w") as fh:
            fh.write(redact(json.dumps(res, indent=2, default=str)))
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
        print(f"[verify483] verdict {res['verdict']}", flush=True)
        return 0 if res["verdict"] == "PASS" else 1
    if a.cmd == "wait-installer":
        return 0 if wait_installer(k, timeout_s=a.timeout_s,
                                   interval_s=a.interval_s,
                                   log=lambda m: print(m, flush=True)) else 1
    collect(k, a.out, extra_secrets=tuple(a.redact),
            log=lambda m: print(m, file=sys.stderr, flush=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
