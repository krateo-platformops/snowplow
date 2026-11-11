"""#1297 parity: data606 run IN-CLUSTER creates exactly what data606 run from the laptop created.

tests/kind_1297_data606_parity.sh runs data606 twice on two fresh kind clusters, built the same way:
arm A from this machine (`python3 -m bench data606 ...`, as run 23 did), arm B through run606.sh's own
s_data (the data606 Job on the loadgen node, bench/incluster.py). After each arm it takes a snapshot
of the cluster with this module; PASS is an empty diff of the two snapshots and equal counts per kind.

What a snapshot holds
---------------------
Every object of the kinds data606 writes (KINDS) that is NOT in the arm's baseline, a snapshot of the
same kinds taken after the cluster's setup and before data606 ran: so the snapshot is exactly what
the data step created, and whatever an arm leaves behind beyond data606's own objects (the in-cluster
Job's ServiceAccount, ClusterRole, Role or binding, which run606 must delete) is in it too and fails
the diff. Plus the composition controller Deployment's replicas (the hold scales it to 0; the
Deployment itself is the fixture's, created before data606 holds it).

Excluded, and said: the namespace of the in-cluster Jobs (bench-loadgen, the Namespace object and
everything in it: run606 creates it for every in-cluster Job, data606's and the drivers', and keeps it;
the arm checks that nothing but serviceaccount/default and configmap/kube-root-ca.crt is left in it),
Deployments (only the controller's replicas are compared), and the
objects Kubernetes creates in every new namespace (ConfigMap kube-root-ca.crt, ServiceAccount default:
not of a compared kind).

Each object is normalized (server-set fields stripped: uid, resourceVersion, generation,
creationTimestamp, managedFields, selfLink, deletionTimestamp, status, ownerReferences' uid) and
hashed (sha256 of its canonical JSON). A Secret's values are replaced by their sha256 before anything
else touches the object: no Secret value is kept, written or printed.

The run606.sh extraction (`functions`) is what the bench's other tests of run606.sh do (a function's
text from `name() {` to the `}` at column 0, or its one line): arm B runs the script's own s_data,
not a copy.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

__all__ = ["KINDS", "normalize", "object_hash", "build", "diff", "functions", "assignments", "main"]

# (resource for kubectl get, the kind it lists, namespaced)
KINDS = (
    ("namespaces", "Namespace", False),
    ("compositiondefinitions.core.krateo.io", "CompositionDefinition", True),
    ("githubscaffoldingwithcompositionpages.composition.krateo.io", "GithubScaffoldingWithCompositionPage", True),
    ("roles.rbac.authorization.k8s.io", "Role", True),
    ("rolebindings.rbac.authorization.k8s.io", "RoleBinding", True),
    ("clusterroles.rbac.authorization.k8s.io", "ClusterRole", False),
    ("clusterrolebindings.rbac.authorization.k8s.io", "ClusterRoleBinding", False),
    ("users.basic.authn.krateo.io", "User", True),
    ("secrets", "Secret", True),
    ("restactions.templates.krateo.io", "RESTAction", True),
)
EXCLUDED_NAMESPACES = ("bench-loadgen",)
CONTROLLER = ("bench-ns-01", "githubscaffoldingwithcompositionpages-v1-2-2-controller")

_META_DROP = ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "selfLink",
              "deletionTimestamp", "deletionGracePeriodSeconds")


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def normalize(obj: dict) -> dict:
    """The object without what the API server sets; a Secret's values as their sha256."""
    o = copy.deepcopy(obj)
    if o.get("kind") == "Secret":
        for field in ("data", "stringData"):
            if isinstance(o.get(field), dict):
                o[field] = {k: "sha256:" + _sha(str(v).encode()) for k, v in o[field].items()}
    meta = o.setdefault("metadata", {})
    for k in _META_DROP:
        meta.pop(k, None)
    for ref in meta.get("ownerReferences") or []:
        ref.pop("uid", None)
    o.pop("status", None)
    return o


def object_hash(normalized: dict) -> str:
    return _sha(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode())


def key(kind: str, obj: dict) -> str:
    m = obj.get("metadata") or {}
    return f"{kind}/{m.get('namespace') or '-'}/{m.get('name')}"


def build(listings: dict[str, dict], controller: dict | None, baseline: dict | None) -> dict:
    """A snapshot from each kind's `kubectl get -o json` listing ({kind: List}), the controller
    Deployment (None: absent) and the baseline snapshot (None: this IS the baseline)."""
    objects: dict[str, dict] = {}
    base = set((baseline or {}).get("objects", {}))
    for kind, listing in listings.items():
        for item in listing.get("items") or []:
            meta = item.get("metadata") or {}
            # the harness namespace: neither what is in it nor the Namespace object itself (run606
            # keeps it for the drivers; the arm checks what is left in it)
            if meta.get("namespace") in EXCLUDED_NAMESPACES or (kind == "Namespace"
                                                                and meta.get("name") in EXCLUDED_NAMESPACES):
                continue
            item.setdefault("kind", kind)
            k = key(kind, item)
            if k in base:
                continue
            n = normalize(item)
            objects[k] = {"sha256": object_hash(n), "object": n}
    counts: dict[str, int] = {}
    for k in objects:
        counts[k.split("/", 1)[0]] = counts.get(k.split("/", 1)[0], 0) + 1
    ctl = None if controller is None else {"replicas": (controller.get("spec") or {}).get("replicas")}
    return {"kind": "data606_parity_snapshot", "baseline": baseline is None, "counts": dict(sorted(counts.items())),
            "controller": ctl, "objects": dict(sorted(objects.items()))}


def _paths(a, b, at: str = "") -> list[str]:
    """The JSON paths where a and b differ (paths only: never a value)."""
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            out += _paths(a.get(k), b.get(k), f"{at}.{k}") if k in a and k in b else [f"{at}.{k}"]
        return out
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [p for i, (x, y) in enumerate(zip(a, b)) for p in _paths(x, y, f"{at}[{i}]")]
    return [] if a == b else [at or "."]


def diff(a: dict, b: dict) -> dict:
    """Arm A's snapshot against arm B's: PASS only with no object missing, extra or different, the
    same count per kind and the same controller replicas."""
    oa, ob = a["objects"], b["objects"]
    only_a = sorted(set(oa) - set(ob))
    only_b = sorted(set(ob) - set(oa))
    changed = {k: _paths(oa[k]["object"], ob[k]["object"]) for k in sorted(set(oa) & set(ob))
               if oa[k]["sha256"] != ob[k]["sha256"]}
    kinds = sorted(set(a["counts"]) | set(b["counts"]))
    counts = {k: [a["counts"].get(k, 0), b["counts"].get(k, 0)] for k in kinds}
    counts_equal = all(x == y for x, y in counts.values())
    ok = not only_a and not only_b and not changed and counts_equal and a["controller"] == b["controller"]
    return {"kind": "data606_parity_diff", "verdict": "PASS" if ok else "FAIL",
            "objects": [len(oa), len(ob)], "counts": counts, "counts_equal": counts_equal,
            "controller": [a["controller"], b["controller"]],
            "only_in_a": only_a, "only_in_b": only_b, "different": changed}


# ─── reading a cluster ───────────────────────────────────────────────────────────────────────────

def _kubectl(k: list[str], *args: str) -> tuple[int, str, str]:
    p = subprocess.run([*k, *args], capture_output=True, text=True, timeout=300)
    return p.returncode, p.stdout, p.stderr


def read_cluster(k: list[str]) -> tuple[dict[str, dict], dict | None]:
    """Each compared kind's listing and the controller Deployment. A kind whose CRD is not installed
    lists as empty; any other failure to read fails the snapshot (never read as zero)."""
    listings = {}
    for resource, kind, namespaced in KINDS:
        if kind == "Secret":
            continue
        rc, out, err = _kubectl(k, "get", resource, *(["-A"] if namespaced else []), "-o", "json")
        if rc != 0:
            if "the server doesn't have a resource type" in err:
                listings[kind] = {"items": []}
                continue
            raise RuntimeError(f"cannot list {resource}: {err.strip()[:300]}")
        listings[kind] = json.loads(out)
    # Secrets: only in the namespaces data606 writes them in (krateo-system, bench-*), one read each;
    # no other namespace's Secrets are read at all
    secret_ns = sorted(n["metadata"]["name"] for n in listings["Namespace"]["items"]
                       if n["metadata"]["name"] == "krateo-system"
                       or (n["metadata"]["name"].startswith("bench-")
                           and n["metadata"]["name"] not in EXCLUDED_NAMESPACES))
    listings["Secret"] = {"items": []}
    for ns in secret_ns:
        rc, out, err = _kubectl(k, "-n", ns, "get", "secrets", "-o", "json")
        if rc != 0:
            raise RuntimeError(f"cannot list secrets in {ns}: {err.strip()[:300]}")
        listings["Secret"]["items"] += json.loads(out).get("items") or []
    ns, name = CONTROLLER
    rc, out, err = _kubectl(k, "-n", ns, "get", "deployment", name, "-o", "json", "--ignore-not-found")
    if rc != 0:
        raise RuntimeError(f"cannot read deployment {ns}/{name}: {err.strip()[:300]}")
    return listings, (json.loads(out) if out.strip() else None)


# ─── run606.sh, as its other tests read it ───────────────────────────────────────────────────────

# What arm B extracts from run606.sh: s_data and every run606 function it reaches, and every top-level
# run606 variable they read (with run606's own defaults). ONE list, here: the arm passes it with
# `functions --arm-b`, and tests/test_1297_data606_parity.py checks it against run606.sh itself
# (arm_b_coverage: a variable or function s_data's path reads that is not carried fails the suite, so
# the next one added to run606.sh cannot silently break the arm, as INC_POLL_S did at f7aa9df3).
ARM_B_VARS = ["COLLECT_TIMEOUT_S", "SCALE", "USERS", "NAMESPACES", "LOAD_DRIVER",
              "HOLD_PROBE_WAIT_S", "ENDPOINT_MODE", "PREFLIGHT_TIMEOUT_S", "PREFLIGHT_POLL_S", "TOP_POLL_S",
              "FRESH_MODES", "DATA_TIMEOUT_S", "COLLECT_CAP_S", "PW", "INC", "INC_POLL_S", "BENCH_USER_PREFIX"]
ARM_B_FUNCTIONS = ["log", "run", "kapply", "redact_pw", "kill_tree", "halt_tree", "bounded", "kerr_line",
                   "record_failure", "incluster_pod", "job_pod_why", "incluster_started", "incluster_placement",
                   "incluster_delete", "loadgen_room", "data606_wait", "data606_finish", "data606_gone", "s_data"]
# set by the arm's own preamble (armB.sh), not read from run606.sh: the run dir, the script's dir, the
# kube context it pins, kubectl's error file, the step
ARM_B_PREAMBLE = {"OUT", "HERE", "DRY", "KCFG", "KCTX", "K", "KERR", "CUR_STEP"}
# reached from s_data but not run by the arm, each with its reason
ARM_B_NOT_RUN = {"fresh_classes_create": "k6 driver only (the arm runs LOAD_DRIVER=load606): the fresh "
                                         "identity classes are not data606's objects"}


def _top_level_globals(src: str) -> set[str]:
    """Every variable run606.sh assigns at top level (outside any function)."""
    out, depth = set(), 0
    for ln in src.splitlines():
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\(\) \{", ln):
            depth = 0 if ln.rstrip().endswith("}") else 1
            continue
        if depth:
            if ln == "}":
                depth = 0
            continue
        for m in re.finditer(r"(?:^|[\s;(]|then |else )([A-Z][A-Z0-9_]*)(?:\+)?=", ln):
            out.add(m.group(1))
        m = re.match(r"^\s*(?:export|declare(?: -\w+)?|readonly)\s+(.*)", ln)
        if m:
            out |= {x.split("=")[0] for x in m.group(1).split() if re.match(r"^[A-Z][A-Z0-9_]*(=|$)", x)}
    return out


def _defined_functions(src: str) -> set[str]:
    return set(re.findall(r"^([A-Za-z_][A-Za-z0-9_]*)\(\) \{", src, re.M))


def _blank_plain_strings(text: str) -> str:
    """The shell text with every quoted string that runs no command (no `$(`) blanked: a function
    named in log text is not a call. A small scanner (double quotes with backslash escapes, single
    quotes outside them), not a regex: a regex pairs one string's closing quote with the next one's
    opening quote and blanks the code between them."""
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if c in "\"'":
            j, q = i + 1, c
            while j < n and text[j] != q:
                j += 2 if (q == '"' and text[j] == "\\") else 1
            s = text[i:j + 1]
            out.append(s if (q == '"' and "$(" in s) else q + q)
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def arm_b_coverage(src: str) -> dict:
    """What the arm's extraction misses: every run606 top-level global the extracted functions read
    (`$X`, `${X...}`, a bare X in `$(( ))` or `[[ ]]`) that neither ARM_B_VARS nor the preamble
    carries, and every run606 function they call that is neither extracted nor in ARM_B_NOT_RUN.
    Both lists empty: the arm runs what run606 runs."""
    text = functions(src, ARM_B_FUNCTIONS)
    body = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    globals_ = _top_level_globals(src)
    locals_ = set()
    for m in re.finditer(r"\blocal\s+([^\n;]*)", body):
        locals_ |= {x.split("=")[0] for x in m.group(1).split()}
    # read: $X / ${X...} anywhere, and a bare X inside an arithmetic $(( )) / (( ))
    read = set(re.findall(r"\$\{?([A-Z][A-Z0-9_]*)", body))
    for m in re.finditer(r"\(\((.*?)\)\)", body, re.S):
        read |= set(re.findall(r"\b([A-Z][A-Z0-9_]*)\b", m.group(1)))
    # assigned by the extracted functions themselves before they read it (bounded's BOUNDED_PIDS)
    own = set(re.findall(r"(?:^|[\s;])([A-Z][A-Z0-9_]*)=", body, re.M))
    vars_missing = sorted((read & globals_) - set(ARM_B_VARS) - ARM_B_PREAMBLE - locals_ - own)
    # called: a function's name as a word, outside a string with no expansion in it (log text)
    code = re.sub(r"(^|\s)#.*$", "", _blank_plain_strings(body), flags=re.M)   # and trailing comments
    called = {f for f in _defined_functions(src) if re.search(rf"(^|[\s;|&(!]){re.escape(f)}(\s|;|$|\))", code, re.M)}
    funcs_missing = sorted(called - set(ARM_B_FUNCTIONS) - set(ARM_B_NOT_RUN))
    return {"vars_missing": vars_missing, "functions_missing": funcs_missing}


def functions(src: str, names: list[str]) -> str:
    """Each named function's own text from run606.sh (a missing one fails: the arm would run less)."""
    out = []
    for n in names:
        m = (re.search(rf"^{re.escape(n)}\(\) \{{\n.*?^\}}\n", src, re.S | re.M)
             or re.search(rf"^{re.escape(n)}\(\) \{{ .*\}}\n", src, re.M))
        if not m:
            raise SystemExit(f"run606.sh defines no function {n}")
        out.append(m.group(0))
    return "".join(out)


def assignments(src: str, names: list[str]) -> str:
    """Each named variable's top-level assignment line(s) from run606.sh, in the script's order:
    its own defaults (`X="${X:-...}"`), so the arm runs with them unless the caller sets X."""
    lines = []
    for ln in src.splitlines():
        for n in names:
            if re.match(rf"^(if .*then )?{re.escape(n)}=", ln) or re.match(rf"^else {re.escape(n)}=", ln):
                lines.append(ln)
                break
    missing = [n for n in names if not any(re.search(rf"(^|[ ;]){re.escape(n)}=", ln) for ln in lines)]
    if missing:
        raise SystemExit(f"run606.sh assigns none of {missing} at top level")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.data606_parity")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot", help="the cluster's data606 objects (minus --baseline), hashed")
    s.add_argument("--kubeconfig", required=True)
    s.add_argument("--context", required=True)
    s.add_argument("--baseline", help="the arm's snapshot before data606 (omitted: take the baseline)")
    s.add_argument("--out", required=True)
    d = sub.add_parser("diff", help="arm A's snapshot against arm B's; exit 1 unless PASS")
    d.add_argument("a")
    d.add_argument("b")
    d.add_argument("--out", required=True)
    f = sub.add_parser("functions", help="run606.sh's own functions and default assignments, as bash")
    f.add_argument("--script", required=True)
    f.add_argument("--vars", nargs="*", default=[])
    f.add_argument("--arm-b", action="store_true",
                   help="ARM_B_VARS and ARM_B_FUNCTIONS (refused when arm_b_coverage finds a gap)")
    f.add_argument("names", nargs="*")
    a = p.parse_args(argv)
    if a.cmd == "snapshot":
        k = ["kubectl", f"--kubeconfig={a.kubeconfig}", f"--context={a.context}", "--request-timeout=120s"]
        listings, ctl = read_cluster(k)
        base = json.load(open(a.baseline)) if a.baseline else None
        snap = build(listings, ctl, base)
        Path(a.out).write_text(json.dumps(snap, indent=1, sort_keys=True) + "\n")
        print(json.dumps({"snapshot": a.out, "baseline": snap["baseline"], "objects": len(snap["objects"]),
                          "counts": snap["counts"], "controller": snap["controller"]}))
    elif a.cmd == "diff":
        r = diff(json.load(open(a.a)), json.load(open(a.b)))
        Path(a.out).write_text(json.dumps(r, indent=1) + "\n")
        print(json.dumps({k: r[k] for k in ("verdict", "objects", "counts", "counts_equal", "controller")}))
        for k in ("only_in_a", "only_in_b"):
            for x in r[k][:20]:
                print(f"  {k}: {x}")
        for x, paths in list(r["different"].items())[:20]:
            print(f"  different: {x} at {', '.join(paths[:8])}")
        return 0 if r["verdict"] == "PASS" else 1
    elif a.cmd == "functions":
        src = Path(a.script).read_text()
        if a.arm_b:
            gap = arm_b_coverage(src)
            if gap["vars_missing"] or gap["functions_missing"]:
                raise SystemExit(f"arm B would not run what run606 runs: {gap}")
            a.vars, a.names = ARM_B_VARS, ARM_B_FUNCTIONS
        sys.stdout.write(assignments(src, a.vars) if a.vars else "")
        sys.stdout.write(functions(src, a.names))
    return 0


if __name__ == "__main__":
    sys.exit(main())
