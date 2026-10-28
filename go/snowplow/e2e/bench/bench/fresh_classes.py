"""Fresh identity classes through the run, for first-visit page loads (#935).

The paired bench measured a "first visit" (the cold case for an identity
class) on only 5 samples per route: one per real-Chrome session. Owner ruling
(2026-10-27): the 5 sessions keep running and, through the run, each switches
to a FRESH, never-seen identity class — a new bench user with its own
binding set — then visits every route for the first time again.

WHAT MAKES A CLASS FRESH. Snowplow's identity class is the digest of every
binding whose subjects match the requester (rbac IdentityClassOf), and it
learns a user's class from the `<user>-clientconfig` Secret authn writes at
LOGIN (cache/learned_identities.go). A fresh class here is a new user with a
bench user's group shape ([devs, its home team]) PLUS a RoleBinding naming
that user alone (its team's own Role, in its home namespace): no other
requester matches that binding, so no other class has this binding set. The
prewarm enumerator's representative for a User-subject binding is the user
without its groups — a different binding set — so nothing warms the class
before its own login. The bench creates them at the data step (never logged
in) and deletes them at collect; the cluster's deletion is the backstop.

WHEN A SESSION SWITCHES. A class's purpose is one FIRST visit of every
route; the run's warm bars still need WARM visits. So a session switches when
the current class has visited every route twice (k6/pageload.js switchDue):
two route cycles at the browser's think time,

    cycle_s = 2 x len(routes) x BROWSER_THINK_S       (+ the pages' own time)

and a session needs ceil(browser duration_s / cycle_s) classes, the first
being its initial user (the last one stays to the end). Per route, a phase of
length P gets about sessions x P / cycle_s first-visit samples (evaluate
reports the real n beside every figure).

PROOF OF COLD (browser.js, from snowplow's own signals): right before the
fresh login and right after its first page settled, the session reads
snowplow's /debug/vars: snowplow_learned_classes_registered must move — a
class snowplow had not learned registered at this login — and
snowplow_learned_classes_seeded says whether seeding for it ran during the
first visit. evaluate counts a class cold-proven only when the registrations
in its window (with any overlapping windows of other sessions) cover it.

    python3 -m bench.fresh_classes plan --preflight-file F --sessions N \\
        --browser-think-s S --duration-s D --namespaces M --user-prefix P \\
        --password-file F --out PLAN.json --manifest OUT.yaml
    python3 -m bench.fresh_classes cleanup --plan PLAN.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys

from bench import rbac_model

LABEL = "bench.krateo.io/fresh-class"
AUTHN_NS = "krateo-system"          # bench/lifecycle.py PHASE7_AUTHN_NS: where authn's Users live
ROLE = "bench-team-reader"          # bench/rbac_model.namespace_rbac_yaml: each team's Role


def cycle_s(routes: int, think_s: float) -> float:
    """Two route cycles: every route once first, once warm."""
    if routes < 1 or think_s <= 0:
        raise ValueError("a route cycle needs at least one route and a think time > 0")
    return 2 * routes * think_s


def classes_per_session(routes: int, think_s: float, duration_s: float) -> int:
    """Classes one session goes through, its initial user's included."""
    return max(1, math.ceil(duration_s / cycle_s(routes, think_s)))


def plan(*, sessions: int, routes: int, think_s: float, duration_s: float, namespaces: int,
         user_prefix: str) -> dict:
    """Each session's FRESH users (its initial user excluded), their groups
    and home namespace. Names carry the bench's user prefix, which every log
    scrub denies."""
    n = classes_per_session(routes, think_s, duration_s) - 1
    out, k = [], 0
    for s in range(1, sessions + 1):
        fresh = []
        for c in range(1, n + 1):
            team = k % namespaces + 1
            k += 1
            fresh.append({"user": f"{user_prefix}fresh-{s}-{c:03d}",
                          "groups": [rbac_model.BASE_GROUP, rbac_model.team_group(team)],
                          "namespace": f"bench-ns-{team:02d}"})
        out.append({"session": f"s{s}", "fresh": fresh})
    return {"cycle_s": cycle_s(routes, think_s), "classes_per_session": n + 1, "sessions": out,
            "formula": "cycle_s = 2 x len(routes) x BROWSER_THINK_S (every route once first, once warm); "
                       "classes per session = ceil(duration_s / cycle_s), the initial user's included"}


def manifest(p: dict, password: str) -> str:
    """The authn Users, their password Secrets, and the RoleBinding that
    names each user alone."""
    docs = []
    for s in p["sessions"]:
        for f in s["fresh"]:
            u = f["user"]
            meta = {"labels": {LABEL: "true", "app": "scaletest"}}
            docs.append({"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/basic-auth",
                         "metadata": dict(meta, name=f"{u}-password", namespace=AUTHN_NS),
                         "stringData": {"password": password}})
            docs.append({"apiVersion": "basic.authn.krateo.io/v1alpha1", "kind": "User",
                         "metadata": dict(meta, name=u, namespace=AUTHN_NS),
                         "spec": {"displayName": "Fresh class", "groups": f["groups"],
                                  "passwordRef": {"namespace": AUTHN_NS, "name": f"{u}-password",
                                                  "key": "password"}}})
            docs.append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                         "metadata": dict(meta, name=f"{u}-reader", namespace=f["namespace"]),
                         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": ROLE},
                         "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": u}]})
    return "\n---\n".join(json.dumps(d) for d in docs) + "\n" if docs else ""


def cleanup_targets(p: dict) -> list[list[str]]:
    """kubectl delete argument lists: every object the bench made, by its
    label (three calls), and each user's `<user>-clientconfig` Secret authn
    wrote at its login (unlabelled: by name, one call)."""
    sel = f"{LABEL}=true"
    users = [f["user"] for s in p["sessions"] for f in s["fresh"]]
    out = [["-n", AUTHN_NS, "delete", "--ignore-not-found", "users.basic.authn.krateo.io", "-l", sel],
           ["-n", AUTHN_NS, "delete", "--ignore-not-found", "secret", "-l", sel],
           ["delete", "--ignore-not-found", "rolebinding", "--all-namespaces", "-l", sel]]
    if users:
        out.append(["-n", AUTHN_NS, "delete", "--ignore-not-found", "secret"]
                   + [f"{u}-clientconfig" for u in users])
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.fresh_classes")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("plan")
    pl.add_argument("--preflight-file", required=True, help="the run's preflight.jsonl (its route table)")
    pl.add_argument("--sessions", type=int, required=True)
    pl.add_argument("--browser-think-s", type=float, required=True)
    pl.add_argument("--duration-s", type=float, required=True)
    pl.add_argument("--namespaces", type=int, required=True)
    pl.add_argument("--user-prefix", required=True)
    pl.add_argument("--password-file", required=True, help="a file holding the bench users' password")
    pl.add_argument("--out", required=True)
    pl.add_argument("--manifest", required=True)
    cl = sub.add_parser("cleanup", help="one kubectl delete argument list per line, space-separated")
    cl.add_argument("--plan", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "cleanup":
        with open(a.plan) as fh:
            for args in cleanup_targets(json.load(fh)):
                print(" ".join(args))         # no argument carries a space
        return 0
    from bench import load606_job
    routes = load606_job.preflight_routes(a.preflight_file)
    p = plan(sessions=a.sessions, routes=len(routes), think_s=a.browser_think_s, duration_s=a.duration_s,
             namespaces=a.namespaces, user_prefix=a.user_prefix)
    with open(a.password_file) as fh:
        password = fh.read().strip()
    with open(a.out, "w") as fh:
        json.dump(p, fh)
    with open(a.manifest, "w") as fh:
        fh.write(manifest(p, password))
    print(f"fresh classes: {p['classes_per_session'] - 1} per session x {a.sessions} sessions "
          f"(cycle {p['cycle_s']:g} s over {len(routes)} routes)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
