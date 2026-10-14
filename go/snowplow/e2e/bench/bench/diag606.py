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
           "installer_progress", "Kube", "collect", "wait_installer", "main"]

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


def summarize_conditions(obj: dict) -> dict:
    md = obj.get("metadata") or {}
    conds = ((obj.get("status") or {}).get("conditions")) or []
    out = {"kind": obj.get("kind"), "namespace": md.get("namespace"),
           "name": md.get("name")}
    for c in conds:
        if c.get("type") in ("Ready", "Synced"):
            out[c["type"]] = {"status": c.get("status"), "reason": c.get("reason"),
                              "message": (c.get("message") or "")[:500]}
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
    inst, leaves = [], []
    for kind in k.composition_kinds():
        objs = k.items(kind, "-A")
        sums = [summarize_conditions(o) for o in objs[:MAX_ITEMS_PER_KIND]]
        if kind.startswith("installers."):
            inst.extend(sums)
        else:
            leaves.extend(sums)
    return inst, leaves


def wait_installer(k: Kube, *, timeout_s: float, interval_s: float = 60,
                   log: Callable[[str], None] = print,
                   clock=time.monotonic, sleep=time.sleep) -> bool:
    deadline = clock() + timeout_s
    while True:
        inst, leaves = _leaf_summaries(k)
        ok, line = installer_progress(inst, leaves)
        log(f"[platform] {line}")
        if ok:
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

    def installer_and_leaves():
        inst_raw = k.items(f"installers.{COMP_GROUP}", "-A")
        w("installer-cr.json", inst_raw)
        inst, leaves = _leaf_summaries(k)
        w("composition-crs.json", {"installer": inst, "components": leaves})
        ok, line = installer_progress(inst, leaves)
        w("progress.txt", line + "\n")

    def cds():
        w("compositiondefinitions.json",
          [summarize_conditions(o) for o in k.items("compositiondefinitions.core.krateo.io", "-A")])

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

    for name, fn in (("installer", installer_and_leaves), ("cds", cds), ("pods", pods),
                     ("events", events), ("pvcs", pvcs), ("nodes", nodes),
                     ("helm", helm_list), ("logs", logs)):
        safe(name, fn)
    log(f"[diagnostics] {len(written)} files -> {out_dir}")
    return written


# ─── CLI ────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.diag606")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("wait-installer", "collect"):
        s = sub.add_parser(name)
        s.add_argument("--kubeconfig", required=True)
        s.add_argument("--context", required=True)
        if name == "wait-installer":
            s.add_argument("--timeout-s", type=float, default=2700)
            s.add_argument("--interval-s", type=float, default=60)
        else:
            s.add_argument("--out", required=True)
            s.add_argument("--redact", action="append", default=[],
                           help="extra literal to redact (repeatable)")
    a = p.parse_args(argv)
    k = Kube(a.kubeconfig, a.context)
    if a.cmd == "wait-installer":
        return 0 if wait_installer(k, timeout_s=a.timeout_s,
                                   interval_s=a.interval_s,
                                   log=lambda m: print(m, flush=True)) else 1
    collect(k, a.out, extra_secrets=tuple(a.redact),
            log=lambda m: print(m, file=sys.stderr, flush=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
