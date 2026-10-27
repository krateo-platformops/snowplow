"""Which API Priority and Fairness flow schema serves whom: asked of the API
server, never re-derived (#922 F3; #924 review M2, M3).

Every API server response carries X-Kubernetes-PF-FlowSchema-UID and
X-Kubernetes-PF-PriorityLevel-UID: the schema and level APF classified THAT
request into (APF runs before authorization, so a refused request carries
them too). At env, run606 makes one LIST as each identity whose API traffic
the figures attribute, through kubectl's own impersonation (`--as`,
`--as-group`; kubectl -v=8 prints the response headers):

  snowplow_sa     snowplow's ServiceAccount (from deploy/snowplow);
  snowplow_users  a bench user with that user's own groups (bench/rbac_model):
                  snowplow reads as each user (#615), and APF classifies those
                  requests by the user and its groups;
  bench_drivers   the bench's own in-cluster drivers' ServiceAccounts
                  (bench-loadgen: fresh606's churn writes, its probe, ...).

Then it gives each identity a FlowSchema of its own (render), at the SAME
priority level its requests landed in and a matchingPrecedence one below the
schema that took them, so APF evaluates it first: same level, same queuing
(one flow per user, as the stock schemas distinguish), but snowplow's
requests are no longer pooled with every other ServiceAccount, user or the
bench's own load. A second probe (verify) proves each identity now lands in
its own schema. The figures read those schemas: snowplow_sa's are
snowplow's ServiceAccount's alone, snowplow_users' the requests snowplow
sends as the bench's users (only snowplow uses their credentials), and the
bench's drivers are apart. The record keeps names only: no user name, no
schema subjects.

    python3 -m bench.flowcontrol [opts] probe --out F -- kubectl ...
    python3 -m bench.flowcontrol [opts] render --probe F
    python3 -m bench.flowcontrol [opts] verify --probe F -- kubectl ...
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time

from bench import rbac_model

IDENTITIES = ("snowplow_sa", "snowplow_users", "bench_drivers")
BENCH_SCHEMA = {"snowplow_sa": "bench-snowplow-sa", "snowplow_users": "bench-snowplow-users",
                "bench_drivers": "bench-drivers"}
DRIVERS_NAMESPACE = "bench-loadgen"     # bench/load606_job.py's namespace for every in-cluster driver
DRIVER_SA = "fresh606"                  # the churn writer (load606_job.render_fresh)
SNOWPLOW_NAMESPACE = "krateo-system"
_UID = r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
_FS = re.compile(r"(?i)x-kubernetes-pf-flowschema-uid[\"':=\s]+" + _UID)
_PL = re.compile(r"(?i)x-kubernetes-pf-prioritylevel-uid[\"':=\s]+" + _UID)
_STATUS = re.compile(r"(?i)response status[\"':=\s]+(\d{3})|\bstatus=\"?(\d{3})\b")


def parse_pf(verbose: str) -> dict:
    """The APF headers (and the HTTP status) kubectl -v=8 printed."""
    fs, pl, st = _FS.search(verbose), _PL.search(verbose), _STATUS.search(verbose)
    return {"fs_uid": fs.group(1) if fs else None, "pl_uid": pl.group(1) if pl else None,
            "status": int(st.group(1) or st.group(2)) if st else None}


def impersonation(sa: str, user: str, groups: list[str]) -> dict[str, list[str]]:
    """kubectl's impersonation flags per identity. A ServiceAccount
    impersonated without groups gets its own (system:serviceaccounts,
    system:serviceaccounts:<ns>) from the API server."""
    return {"snowplow_sa": [f"--as=system:serviceaccount:{SNOWPLOW_NAMESPACE}:{sa}"],
            "snowplow_users": [f"--as={user}"] + [f"--as-group={g}" for g in groups],
            "bench_drivers": [f"--as=system:serviceaccount:{DRIVERS_NAMESPACE}:{DRIVER_SA}"]}


def _kubectl(kubectl: list[str], args: list[str], timeout_s: float) -> subprocess.CompletedProcess:
    return subprocess.run(kubectl + [f"--request-timeout={int(timeout_s)}s"] + args, capture_output=True,
                          text=True, timeout=timeout_s)


def probe_one(kubectl: list[str], flags: list[str], timeout_s: float) -> dict:
    """One LIST as the identity; its response's APF headers."""
    p = _kubectl(kubectl, flags + ["-v=8", "get", "--raw",
                                   f"/api/v1/namespaces/{SNOWPLOW_NAMESPACE}/configmaps?limit=1"], timeout_s)
    return parse_pf(p.stderr)


def names(kubectl: list[str], timeout_s: float) -> dict:
    """{uid: {name, kind, precedence, priority_level}} of every flow schema
    and priority level."""
    out = {}
    for res, kind in (("flowschemas.flowcontrol.apiserver.k8s.io", "flowschema"),
                      ("prioritylevelconfigurations.flowcontrol.apiserver.k8s.io", "prioritylevel")):
        p = _kubectl(kubectl, ["get", res, "-o", "json"], timeout_s)
        if p.returncode != 0:
            raise RuntimeError(f"kubectl get {res}: rc={p.returncode}")
        for it in json.loads(p.stdout).get("items") or []:
            md, spec = it.get("metadata") or {}, it.get("spec") or {}
            out[md.get("uid")] = {"name": md.get("name"), "kind": kind,
                                  "precedence": spec.get("matchingPrecedence"),
                                  "priority_level": (spec.get("priorityLevelConfiguration") or {}).get("name")}
    return out


def classify(kubectl: list[str], flags: dict[str, list[str]], timeout_s: float) -> dict:
    """Each identity's flow schema and priority level, by name."""
    seen = {i: probe_one(kubectl, f, timeout_s) for i, f in flags.items()}
    by_uid = names(kubectl, timeout_s)
    out = {}
    for i, pf in seen.items():
        fs, pl = by_uid.get(pf["fs_uid"]) or {}, by_uid.get(pf["pl_uid"]) or {}
        out[i] = {"flow_schema": fs.get("name"), "precedence": fs.get("precedence"),
                  "priority_level": pl.get("name"), "status": pf["status"],
                  **({} if fs and pl else {"error": "no APF headers in the response" if not pf["fs_uid"]
                                           else "a header's UID names no flow schema / priority level"})}
    return out


def render(original: dict, sa: str) -> str:
    """The bench's FlowSchemas: one per identity, at the priority level its
    requests landed in, one precedence below the schema that took them."""
    subjects = {
        "snowplow_sa": {"kind": "ServiceAccount", "serviceAccount": {"namespace": SNOWPLOW_NAMESPACE, "name": sa}},
        "snowplow_users": {"kind": "Group", "group": {"name": rbac_model.BASE_GROUP}},
        "bench_drivers": {"kind": "Group", "group": {"name": f"system:serviceaccounts:{DRIVERS_NAMESPACE}"}},
    }
    docs = []
    for i in IDENTITIES:
        o = original[i]
        if not o.get("flow_schema") or not o.get("priority_level") or o.get("precedence") is None:
            raise ValueError(f"{i}: not classified ({o.get('error')})")
        docs.append(json.dumps({
            "apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "FlowSchema",
            "metadata": {"name": BENCH_SCHEMA[i], "labels": {"purpose": "snowplow-bench"}},
            "spec": {"matchingPrecedence": max(1, int(o["precedence"]) - 1),
                     "priorityLevelConfiguration": {"name": o["priority_level"]},
                     "distinguisherMethod": {"type": "ByUser"},
                     "rules": [{"subjects": [subjects[i]],
                                "resourceRules": [{"verbs": ["*"], "apiGroups": ["*"], "resources": ["*"],
                                                   "clusterScope": True, "namespaces": ["*"]}],
                                "nonResourceRules": [{"verbs": ["*"], "nonResourceURLs": ["*"]}]}]}}))
    return "\n---\n".join(docs) + "\n"


def _identity_flags(a, kubectl) -> tuple[str, dict]:
    p = _kubectl(kubectl, ["-n", SNOWPLOW_NAMESPACE, "get", "deploy/snowplow", "-o", "json"], a.timeout_s)
    if p.returncode != 0:
        raise RuntimeError(f"kubectl get deploy/snowplow: rc={p.returncode}")
    spec = ((json.loads(p.stdout).get("spec") or {}).get("template") or {}).get("spec") or {}
    sa = spec.get("serviceAccountName") or "default"
    model = rbac_model.RbacModel(users=a.users, namespaces=a.namespaces, admins=min(10, a.users))
    return sa, impersonation(sa, f"{a.user_prefix}{1:04d}", rbac_model.user_groups(1, model))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.flowcontrol")
    ap.add_argument("--users", type=int, required=True)
    ap.add_argument("--namespaces", type=int, required=True)
    ap.add_argument("--user-prefix", required=True)
    ap.add_argument("--timeout-s", type=float, required=True, help="each kubectl read's bound; verify's whole wait")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pr = sub.add_parser("probe")
    pr.add_argument("--out", required=True)
    pr.add_argument("kubectl", nargs=argparse.REMAINDER)
    rd = sub.add_parser("render")
    rd.add_argument("--probe", required=True)
    vf = sub.add_parser("verify")
    vf.add_argument("--probe", required=True)
    vf.add_argument("kubectl", nargs=argparse.REMAINDER)
    a = ap.parse_args(argv)
    kubectl = [x for x in getattr(a, "kubectl", [])[1:]] if getattr(a, "kubectl", [])[:1] == ["--"] \
        else getattr(a, "kubectl", [])
    try:
        if a.cmd == "probe":
            sa, flags = _identity_flags(a, kubectl)
            # an answer without the headers (the API server did not answer, a
            # proxy in between) is asked again, within the one bound
            deadline = time.time() + a.timeout_s
            while True:
                original = classify(kubectl, flags, a.timeout_s)
                if not any(o.get("error") for o in original.values()) or time.time() >= deadline:
                    break
                time.sleep(min(5.0, max(0.0, deadline - time.time())))
            with open(a.out, "w") as fh:
                json.dump({"snowplow_sa": sa, "original": original}, fh)
            bad = {i: o.get("error") for i, o in original.items() if o.get("error")}
            if bad:
                print(f"not classified: {bad}", file=sys.stderr)
                return 1
            return 0
        with open(a.probe) as fh:
            pre = json.load(fh)
        if a.cmd == "render":
            sys.stdout.write(render(pre["original"], pre["snowplow_sa"]))
            return 0
        # verify: the API server's own controller picks new schemas up within
        # seconds; probe until each identity lands in its bench schema, within
        # the one bound
        sa, flags = _identity_flags(a, kubectl)
        deadline = time.time() + a.timeout_s
        while True:
            measured = classify(kubectl, flags, a.timeout_s)
            ok = all(measured[i].get("flow_schema") == BENCH_SCHEMA[i] for i in IDENTITIES)
            if ok or time.time() >= deadline:
                break
            time.sleep(min(5.0, max(0.0, deadline - time.time())))
        print(json.dumps({"kind": "flowcontrol_config", "verified": ok,
                          "identities": {i: {"original": {k: pre["original"][i].get(k) for k in
                                                          ("flow_schema", "priority_level")},
                                             "measured": {k: measured[i].get(k) for k in
                                                          ("flow_schema", "priority_level")}}
                                         for i in IDENTITIES},
                          "snowplow_sa": f"{SNOWPLOW_NAMESPACE}/{sa}"}))
        return 0 if ok else 1
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, KeyError) as e:
        print(f"flowcontrol {a.cmd}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
