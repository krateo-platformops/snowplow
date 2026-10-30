"""The portal menu classes k6 drives, and the identity each one's preflight
signs in as (run 17 attempt 5; #999 review N2).

The portal filters its sidebar by RBAC, so a menu is per menu class
(rbac_model.menu_class: a user's groups less those whose grants the bench
confines to its data namespaces). Each class k6 drives gets its own route
table: the preflight606 walk (run606 s_classes), signed in as a DEDICATED
identity whose groups are exactly the class's kept groups. That identity is
in the class by menu_class's own definition, and no k6 user shares its
identity class: every k6 user also carries a team group. So the walk does not
teach snowplow the identity class of a user the run measures. Snowplow learns
a class from the `<user>-clientconfig` Secret authn writes at login, and it
keeps that across a restart (cache/learned_identities.go), so the reboot
would not undo it. Created after the data step, deleted at collect with
the fresh classes; the cluster's deletion is the backstop.

    python3 -m bench.menu_classes plan --users N --namespaces M --browser-sessions S \\
        --burst-users B [--dry-run] --user-prefix P --password-file F --out PLAN.json --manifest OUT.yaml
    python3 -m bench.menu_classes list --plan PLAN.json      # LABEL<TAB>USER per class
    python3 -m bench.menu_classes cleanup --plan PLAN.json   # kubectl delete argument lists
"""

from __future__ import annotations

import argparse
import json
import sys

from bench import authn_user

LABEL = "bench.krateo.io/menu-class"
AUTHN_NS = "krateo-system"          # bench/lifecycle.py PHASE7_AUTHN_NS: where authn's Users live


def plan(labels: list[str], user_prefix: str) -> dict:
    """One identity per menu class, in label order: its user and its groups
    (the class's kept groups, nothing else)."""
    return {"classes": [{"class": c, "user": f"{user_prefix}menuclass-{k}", "groups": c.split("+")}
                        for k, c in enumerate(sorted(labels), 1)]}


def manifest(p: dict, password: str) -> str:
    """Each identity's authn User and its password Secret (the ONE authn user
    rendering, bench/authn_user.py). No binding of its own: its groups' grants
    are the bench's (data606) and the platform's."""
    labels = {LABEL: "true", "app": "scaletest"}
    docs = []
    for k, c in enumerate(p["classes"], 1):
        docs.append(authn_user.password_secret(c["user"], AUTHN_NS, password, labels))
        docs.append(authn_user.user(c["user"], AUTHN_NS, display_name="Menu class", groups=c["groups"],
                                    avatar=authn_user.avatar_url(k), labels=labels))
    return "\n---\n".join(json.dumps(d) for d in docs) + "\n" if docs else ""


def cleanup_targets(p: dict) -> list[list[str]]:
    """kubectl delete argument lists: the Users and Secrets by label, and each
    identity's `<user>-clientconfig` Secret authn wrote at its login (by name)."""
    sel = f"{LABEL}=true"
    users = [c["user"] for c in p["classes"]]
    out = [["-n", AUTHN_NS, "delete", "--ignore-not-found", "users.basic.authn.krateo.io", "-l", sel],
           ["-n", AUTHN_NS, "delete", "--ignore-not-found", "secret", "-l", sel]]
    if users:
        out.append(["-n", AUTHN_NS, "delete", "--ignore-not-found", "secret"] + [f"{u}-clientconfig" for u in users])
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.menu_classes")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("plan")
    for f, t in (("--users", int), ("--namespaces", int), ("--browser-sessions", int), ("--burst-users", int)):
        pl.add_argument(f, type=t, required=True)
    pl.add_argument("--dry-run", action="store_true")
    pl.add_argument("--user-prefix", required=True)
    pl.add_argument("--password-file", required=True, help="a file holding the bench users' password")
    pl.add_argument("--out", required=True)
    pl.add_argument("--manifest", required=True)
    for name in ("list", "cleanup"):
        sub.add_parser(name).add_argument("--plan", required=True)
    a = ap.parse_args(argv)
    if a.cmd in ("list", "cleanup"):
        with open(a.plan) as fh:
            p = json.load(fh)
        if a.cmd == "list":
            for c in p["classes"]:
                print(f"{c['class']}\t{c['user']}")
        else:
            for args in cleanup_targets(p):
                print(" ".join(args))         # no argument carries a space
        return 0
    from bench import load606_job
    p = plan(load606_job.menu_classes_driven(a.users, a.namespaces, a.browser_sessions, a.burst_users,
                                             a.dry_run), a.user_prefix)
    with open(a.password_file) as fh:
        password = fh.read().strip()
    with open(a.out, "w") as fh:
        json.dump(p, fh)
    with open(a.manifest, "w") as fh:
        fh.write(manifest(p, password))
    return 0


if __name__ == "__main__":
    sys.exit(main())
