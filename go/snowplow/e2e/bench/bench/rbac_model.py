"""Realistic RBAC shape for the #606 1000-user bench.

The pre-#606 generator (`lifecycle.create_synthetic_users`) put every
synthetic user in ONE group (`devs`). That collapses 1000 users into a single
binding class, which is the one population shape the cache handles best and
production never has. The opposite extreme (one RoleBinding per user) is also
unrealistic, and was measured unviable for prewarm (~132K cells).

This model sits between them and is deterministic, so two runs build the same
cluster:

  - one TEAM per bench namespace (`bench-team-NN` owns `bench-ns-NN`), bound
    by a RoleBinding to a namespaced reader Role;
  - every user belongs to exactly one home team, round-robin;
  - a fixed share of users also belongs to a SECOND team (cross-team members),
    which creates multi-binding identities;
  - a small set of platform admins gets a cluster-wide read ClusterRoleBinding;
  - every user keeps the portal's base group (`devs`) so the portal navigation
    widgets resolve exactly as they do for a real end user.

`identity_classes()` reports how many distinct group sets the model produces,
so the run proof can state the population shape it measured.
"""

from __future__ import annotations

import dataclasses
import functools

__all__ = [
    "RbacModel",
    "team_group",
    "user_groups",
    "identity_classes",
    "namespace_rbac_yaml",
    "admin_rbac_yaml",
    "READER_RULES",
    "team_namespace",
    "grant_scope",
    "menu_class",
]

BASE_GROUP = "devs"
ADMIN_GROUP = "bench-platform-admins"

# What a namespace team may read: its compositions, the per-composition
# widgets and the RESTActions the panels resolve. Same rule set the S8 stage
# grants cyberjoker (phases.S8_ROLE_RULES), so the two populations exercise
# the same resolve paths.
READER_RULES = (
    (("composition.krateo.io",), ("*",), ("get", "list", "watch")),
    (("widgets.templates.krateo.io",), ("*",), ("get", "list")),
    (("templates.krateo.io",), ("restactions",), ("get", "list")),
)


@dataclasses.dataclass(frozen=True)
class RbacModel:
    users: int = 1000
    namespaces: int = 50
    # Every Nth user is also in a second team (N=10 → 10% cross-team).
    cross_team_every: int = 10
    # The first K users are platform admins.
    admins: int = 10

    def __post_init__(self):
        if self.users < 1 or self.namespaces < 1:
            raise ValueError("users and namespaces must be >= 1")
        if self.admins > self.users:
            raise ValueError("admins cannot exceed users")


def bench_model(users: int, namespaces: int) -> "RbacModel":
    """The #606 bench's model, as data606 creates its users: the first
    min(10, users) users are platform admins. One definition (data606 and
    bench/flowcontrol.py's probe read it)."""
    return RbacModel(users=users, namespaces=namespaces, admins=min(10, users))


def team_group(ns_index: int) -> str:
    return f"bench-team-{ns_index:02d}"


def team_namespace(ns_index: int) -> str:
    """The bench data namespace team NN owns (its compositions, nothing of the portal's)."""
    return f"bench-ns-{ns_index:02d}"


def rbac_yaml(namespaces: int) -> list[str]:
    """Every RBAC document the data step applies (data606), in its order:
    each team's Role + RoleBinding, then the admins' ClusterRole + binding."""
    return [namespace_rbac_yaml(n) for n in range(1, namespaces + 1)] + [admin_rbac_yaml()]


@functools.lru_cache(maxsize=None)
def grant_scopes(namespaces: int) -> dict[str, tuple[str, ...]]:
    """{group: the namespaces the bench's RBAC binds it in ("*": a
    ClusterRoleBinding)}, READ from the documents the data step applies
    (rbac_yaml), never paired by name (#999 review N3)."""
    import yaml
    out: dict[str, set[str]] = {}
    for text in rbac_yaml(namespaces):
        for d in yaml.safe_load_all(text):
            if not isinstance(d, dict) or d.get("kind") not in ("RoleBinding", "ClusterRoleBinding"):
                continue
            where = d["metadata"]["namespace"] if d["kind"] == "RoleBinding" else "*"
            for sub in d.get("subjects") or []:
                if sub.get("kind") == "Group":
                    out.setdefault(sub["name"], set()).add(where)
    return {g: tuple(sorted(ns)) for g, ns in out.items()}


def grant_scope(group: str, m: RbacModel) -> tuple[str, ...] | None:
    """Where the bench's RBAC lets `group` read (grant_scopes); None for a
    group the bench does not bind at all (the portal's base group: its
    grants are the platform's own)."""
    return grant_scopes(m.namespaces).get(group)


def menu_class(groups, m: RbacModel) -> str:
    """The identity class of a user's portal MENU (run 17 attempt 5: the
    portal filters its sidebar by RBAC, so admins see pages devs do not).
    Its groups, less every group whose grants this model confines to bench
    data namespaces: those hold compositions, never a portal page, so they
    cannot change which pages a menu offers. A group the bench does not bind
    (the platform grants it) and a cluster-wide one are kept. The label is
    the kept groups, sorted, '+'-joined."""
    data = {team_namespace(n) for n in range(1, m.namespaces + 1)}
    kept = sorted(g for g in groups
                  if (s := grant_scope(g, m)) is None or not set(s) <= data)
    return "+".join(kept)


def _home_team(i: int, m: RbacModel) -> int:
    return ((i - 1) % m.namespaces) + 1


def _second_team(i: int, m: RbacModel) -> int | None:
    if m.cross_team_every <= 0 or i % m.cross_team_every != 0 or m.namespaces < 2:
        return None
    home = _home_team(i, m)
    # A stride coprime-ish with the namespace count spreads pairs evenly.
    other = ((home - 1 + 1 + (i // m.cross_team_every) % (m.namespaces - 1))
             % m.namespaces) + 1
    return other if other != home else None


def user_groups(i: int, m: RbacModel) -> list[str]:
    """Groups for synthetic user number i (1-based), deterministic."""
    if not 1 <= i <= m.users:
        raise ValueError(f"user index {i} outside 1..{m.users}")
    groups = [BASE_GROUP, team_group(_home_team(i, m))]
    second = _second_team(i, m)
    if second is not None:
        groups.append(team_group(second))
    if i <= m.admins:
        groups.append(ADMIN_GROUP)
    return groups


def identity_classes(m: RbacModel) -> dict:
    """The population shape: distinct group sets and their sizes."""
    sizes: dict[tuple[str, ...], int] = {}
    for i in range(1, m.users + 1):
        key = tuple(sorted(user_groups(i, m)))
        sizes[key] = sizes.get(key, 0) + 1
    multi = sum(n for k, n in sizes.items()
                if sum(1 for g in k if g.startswith("bench-team-")) > 1)
    return {
        "users": m.users,
        "distinct_group_sets": len(sizes),
        "largest_class": max(sizes.values()),
        "smallest_class": min(sizes.values()),
        "multi_team_users": multi,
        "admins": m.admins,
    }


def _rules_yaml(rules) -> str:
    out = []
    for groups, resources, verbs in rules:
        out.append(
            "- apiGroups: [" + ", ".join(f'"{g}"' for g in groups) + "]\n"
            "  resources: [" + ", ".join(f'"{r}"' for r in resources) + "]\n"
            "  verbs: [" + ", ".join(f'"{v}"' for v in verbs) + "]")
    return "\n".join(out)


def namespace_rbac_yaml(ns_index: int) -> str:
    """Role + RoleBinding giving team NN read access to bench-ns-NN."""
    ns = team_namespace(ns_index)
    grp = team_group(ns_index)
    return f"""\
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: bench-team-reader
  namespace: {ns}
  labels:
    app: scaletest
rules:
{_rules_yaml(READER_RULES)}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: bench-team-reader
  namespace: {ns}
  labels:
    app: scaletest
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: bench-team-reader
subjects:
- apiGroup: rbac.authorization.k8s.io
  kind: Group
  name: {grp}
"""


def admin_rbac_yaml() -> str:
    """ClusterRole + ClusterRoleBinding: cluster-wide read for the admins."""
    return f"""\
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: bench-platform-admin-reader
  labels:
    app: scaletest
rules:
{_rules_yaml(READER_RULES)}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: bench-platform-admin-reader
  labels:
    app: scaletest
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: bench-platform-admin-reader
subjects:
- apiGroup: rbac.authorization.k8s.io
  kind: Group
  name: {ADMIN_GROUP}
"""
