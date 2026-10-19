"""#606 data generation: 50K compositions + 1000 users with realistic RBAC.

Runs against the TEMPORARY bench cluster only (the kubectl wrapper pins
BENCH_GKE_CONTEXT, which `cluster.canonical_gke_context` refuses for 057).

Composition fixture — option A (CONTROLLER HELD)
------------------------------------------------
The only composition fixture in the harness is the marketplace
`github-scaffolding-with-composition-page` blueprint, whose chart creates a
REAL GitHub repository per composition (toRepo.initialize: true) plus an
ArgoCD Application. At 50K that is 50K external repos, so the composition
controller is held BEFORE any composition exists: the CompositionDefinition is
annotated paused and its controller Deployment scaled to 0. The 50K CRs are
then pure API objects: snowplow informs, lists and serves them; nothing
reconciles them. Event load comes from the harness's ChurnDriver at a chosen
rate, which is the independent variable the #603 law needs anyway.

What option A does NOT represent (state it with every result): the
per-composition children the chart would render (widgets, Argo apps,
per-composition RBAC) and the controller's own write churn. The July 2026
"BenchApp" fixture (23 in-cluster children per composition, no external
calls) covered that and is not in any repository; rebuilding it is option B
in the run plan.

`assert_no_external_children` is the falsifier for the hold: after a probe
batch it requires zero Repo / Application objects for the bench compositions,
and the run aborts before stamping 50K if the hold leaked.
"""

from __future__ import annotations

import functools
import json
import time

from bench import cluster, lifecycle, rbac_model
from bench.load606 import list_dep_fixture_yaml, list_dep_status_fixture_yaml

__all__ = ["hold_composition_controller", "assert_no_external_children",
           "generate", "add_parsers"]

PAUSED_ANNOTATION = "krateo.io/paused=true"
# The served version of the bench composition kind (cluster.composition_yaml
# stamps apiVersion composition.krateo.io/v1-2-2).
COMP_VERSION = "v1-2-2"


def hold_composition_controller(ns: str = "bench-ns-01") -> None:
    """Pause the bench CompositionDefinition and scale its controller to 0."""
    cluster.kubectl("annotate", "--overwrite", "compositiondefinitions.core.krateo.io",
                    cluster.COMPDEF_NAME, "-n", ns, PAUSED_ANNOTATION)
    cluster.kubectl("scale", f"deployment/{cluster.COMP_CONTROLLER_DEPLOY}",
                    "-n", ns, "--replicas=0")


def assert_no_external_children() -> dict:
    """Zero GitHub Repo CRs and zero ArgoCD Applications for bench comps.

    A kind that is not installed counts as zero (it cannot hold children); a
    kind that IS installed but cannot be listed fails closed.
    """
    found = {}
    for kind in ("repoes.git.krateo.io", "applications.argoproj.io"):
        if not cluster._crd_exists(kind):
            found[kind] = 0
            continue
        rc, out, _ = cluster.kubectl("get", kind, "-A", "-o",
                                     "custom-columns=NAME:.metadata.name",
                                     "--no-headers", timeout_secs=300)
        if rc != 0:
            raise RuntimeError(f"cannot list {kind}; refusing to assume zero")
        found[kind] = sum(1 for ln in out.splitlines()
                          if ln.strip().startswith("bench-app-"))
    if any(found.values()):
        raise RuntimeError(f"controller hold LEAKED: {found} — abort before "
                           f"stamping 50K (external side effects)")
    return found


def generate(*, scale: int = 50000, namespaces: int = 50, users: int = 1000,
             probe_batch: int = 50, hold_probe_wait_s: float = 60,
             login: bool = True, fresh_modes: tuple = ("annotation",)) -> dict:
    if scale % namespaces:
        raise ValueError("scale must divide evenly across namespaces")
    per_ns = scale // namespaces
    t0 = time.time()
    proof: dict = {"scale": scale, "namespaces": namespaces, "users": users}

    lifecycle.create_bench_namespaces(1, namespaces)
    if not lifecycle.wait_for_bench_namespaces(namespaces, timeout=600):
        raise RuntimeError(f"bench namespaces not Active ({namespaces})")
    if not cluster.deploy_compositiondefinition("bench-ns-01"):
        raise RuntimeError("CompositionDefinition not Ready in 300s")
    hold_composition_controller("bench-ns-01")

    # Probe batch first; the hold must be proven before 50K are stamped.
    lifecycle.deploy_compositions_parallel(1, 1, probe_batch)
    time.sleep(hold_probe_wait_s)
    proof["hold_probe"] = assert_no_external_children()

    lifecycle.deploy_compositions_parallel(1, namespaces, per_ns)
    proof["compositions"] = cluster.count_compositions()
    proof["hold_final"] = assert_no_external_children()

    model = rbac_model.RbacModel(users=users, namespaces=namespaces,
                                 admins=min(10, users))
    docs = [rbac_model.namespace_rbac_yaml(n) for n in range(1, namespaces + 1)]
    docs.append(rbac_model.admin_rbac_yaml())
    rc, _, err = cluster.kubectl("apply", "--server-side", "-f", "-",
                                 input_data="---\n".join(docs),
                                 timeout_secs=300)
    if rc != 0:
        raise RuntimeError(f"RBAC apply failed: {err[:300]}")
    proof["rbac_shape"] = rbac_model.identity_classes(model)

    count, _ = lifecycle.create_synthetic_users(
        1, users, groups_for=functools.partial(rbac_model.user_groups, m=model),
        login=login)
    if login:
        proof["users_logged_in"] = count
        if count < users:
            raise RuntimeError(f"only {count}/{users} users logged in")
    else:
        proof["users_registered"] = count
        proof["logins"] = "deferred-to-in-cluster-driver"
        if count < users:
            raise RuntimeError(f"only {count}/{users} users registered")

    rc, _, err = cluster.kubectl(
        "apply", "--server-side", "-f", "-",
        input_data=list_dep_fixture_yaml(
            "bench-ns-01", cluster.COMP_GVR, COMP_VERSION, cluster.COMP_RES),
        timeout_secs=60)
    if rc != 0:
        raise RuntimeError(f"freshness fixture apply failed: {err[:300]}")
    if "status" in fresh_modes:
        # Run 9: the status-mode probe RA, applied only when status churn runs
        # (an annotation-only run carries exactly the run-8 fixture set).
        rc, _, err = cluster.kubectl(
            "apply", "--server-side", "-f", "-",
            input_data=list_dep_status_fixture_yaml(
                "bench-ns-01", cluster.COMP_GVR, COMP_VERSION, cluster.COMP_RES),
            timeout_secs=60)
        if rc != 0:
            raise RuntimeError(f"status freshness fixture apply failed: {err[:300]}")
        proof["fresh_modes"] = list(fresh_modes)
    proof["elapsed_s"] = round(time.time() - t0, 1)
    return proof


def add_parsers(sub) -> None:
    p = sub.add_parser("data606", help="#606: 50K compositions + 1000 users "
                                       "with team RBAC (controller held)")
    p.add_argument("--scale", type=int, default=50000)
    p.add_argument("--namespaces", type=int, default=50)
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--hold-probe-wait-s", type=float, default=60,
                   help="wait after the probe batch before checking the hold")
    p.add_argument("--no-login", action="store_true",
                   help="register users only; the in-cluster driver logs them in "
                        "(in-cluster endpoint mode: authn is not reachable from here)")
    p.add_argument("--out", required=True)
    p.add_argument("--fresh-modes", nargs="*", default=None,
                   help="freshness churn modes; 'status' also applies the status probe RA")

    def _run(a):
        from bench.cli import _gke_context_guard
        _gke_context_guard()
        proof = generate(scale=a.scale, namespaces=a.namespaces, users=a.users,
                         probe_batch=min(50, a.scale // a.namespaces),
                         hold_probe_wait_s=a.hold_probe_wait_s,
                         login=not a.no_login,
                         fresh_modes=tuple(a.fresh_modes or ("annotation",)))
        with open(a.out, "w") as fh:
            json.dump(proof, fh, indent=2, default=str)
        print(json.dumps(proof, indent=2, default=str))
        return 0

    p.set_defaults(func=_run)

    q = sub.add_parser("data606-preflight-user", help="#606: register the ONE preflight "
                                              "user (scaleuser-0000), no login")

    def _one(a):
        from bench.cli import _gke_context_guard
        _gke_context_guard()
        n, _ = lifecycle.create_synthetic_users(
            0, 0, groups_for=lambda i: ["devs", rbac_model.team_group(1)], login=False)
        if n != 1:
            raise RuntimeError("preflight user not registered")
        return 0

    q.set_defaults(func=_one)
