"""The data step's manifests, rendered with no cluster and no kubectl call:
the composition fixture, its CompositionDefinition, and the scale users.

bench/cluster.py runs a kubectl context check at import (gke_context_guard);
the schema step (bench/crd_schema.py) renders these BEFORE any cluster exists,
so they live here, and cluster.py, lifecycle.py and data606.py apply them from
here. STDLIB + PyYAML; bench imports are only modules without import-time
side effects.
"""

from __future__ import annotations

from bench import authn_user, rbac_model

__all__ = ["COMPDEF_NAME", "COMP_GVR", "COMP_RES", "COMP_VERSION", "AUTHN_NS", "composition_yaml",
           "compositiondefinition_yaml", "scaleuser_name", "synthetic_user_docs", "documents"]

COMPDEF_NAME = "github-scaffolding-with-composition-page"
COMP_GVR = "composition.krateo.io"
COMP_RES = "githubscaffoldingwithcompositionpages"
COMP_VERSION = "v1-2-2"
AUTHN_NS = "krateo-system"          # where authn's Users live


def composition_yaml(ns, name):
    """Return the YAML body for a single bench composition."""
    return f"""\
apiVersion: composition.krateo.io/v1-2-2
kind: GithubScaffoldingWithCompositionPage
metadata:
  name: {name}
  namespace: {ns}
spec:
  argocd:
    namespace: krateo-system
    application:
      project: default
      source:
        path: chart/
      destination:
        server: https://kubernetes.default.svc
        namespace: {name}
      syncEnabled: false
      syncPolicy:
        automated:
          prune: true
          selfHeal: true
  app:
    service:
      type: NodePort
      port: 31180
  git:
    unsupportedCapabilities: true
    insecure: true
    fromRepo:
      scmUrl: https://github.com
      org: krateoplatformops-blueprints
      name: github-scaffolding-with-composition-page
      branch: main
      path: skeleton/
      credentials:
        authMethod: generic
        secretRef:
          namespace: krateo-system
          name: github-repo-creds
          key: token
    toRepo:
      scmUrl: https://github.com
      org: krateoplatformops-test
      name: {name}
      branch: main
      path: /
      credentials:
        authMethod: generic
        secretRef:
          namespace: krateo-system
          name: github-repo-creds
          key: token
      private: false
      initialize: true
      deletionPolicy: Delete
      verbose: false
      configurationRef:
        name: repo-config
        namespace: demo-system"""


def compositiondefinition_yaml(ns="bench-ns-01"):
    """The bench CompositionDefinition (bench/crd_schema.py validates it, and
    derives the composition kind core-provider generates from it)."""
    return f"""\
apiVersion: core.krateo.io/v1alpha1
kind: CompositionDefinition
metadata:
  name: {COMPDEF_NAME}
  namespace: {ns}
spec:
  chart:
    repo: {COMPDEF_NAME}
    url: https://marketplace.krateo.io
    version: 1.2.2
"""


def scaleuser_name(i):
    return f"scaleuser-{i:04d}"


def synthetic_user_docs(i, groups, password):
    """Scale user i's password Secret and authn User, as the data step applies
    them (the ONE authn user rendering, bench/authn_user.py)."""
    name = scaleuser_name(i)
    labels = {"app": "scaletest"}
    return [authn_user.password_secret(name, AUTHN_NS, password, labels),
            authn_user.user(name, AUTHN_NS, display_name=f"Scale User {i:04d}",
                            groups=list(groups), avatar=authn_user.avatar_url(i), labels=labels)]


def documents(*, namespaces: int = 50, users: int = 1000,
              fresh_modes: tuple = ("annotation",)) -> list[tuple[str, dict]]:
    """Every object of a NON-built-in kind the data step applies (data606's
    generate() and data606-preflight-user), rendered by the renderers they
    apply from, labelled by what applies it. bench/crd_schema.py validates them
    against the CRDs the installer version pins, before any cluster exists.

    Built-in objects (namespaces, RBAC) are not rendered: no CRD governs
    them. One composition per namespace: they differ only in name and
    namespace (composition_yaml), and their kind's CRD is generated in-cluster
    from the CompositionDefinition (crd_schema says so)."""
    import yaml
    from bench.load606 import SCALEUSER_PASSWORD, list_dep_fixture_yaml, list_dep_status_fixture_yaml
    out: list[tuple[str, dict]] = []

    def add(what, text):
        out.extend((what, d) for d in yaml.safe_load_all(text) if d)

    add("the CompositionDefinition", compositiondefinition_yaml("bench-ns-01"))
    for n in range(1, namespaces + 1):
        add("the compositions", composition_yaml(f"bench-ns-{n:02d}", f"bench-app-{n:02d}-01"))
    model = rbac_model.bench_model(users, namespaces)
    for d in synthetic_user_docs(0, ["devs", rbac_model.team_group(1)], SCALEUSER_PASSWORD):
        out.append(("the preflight identity", d))
    for i in range(1, users + 1):
        for d in synthetic_user_docs(i, rbac_model.user_groups(i, m=model), SCALEUSER_PASSWORD):
            out.append(("the data step's users", d))
    add("the freshness fixture", list_dep_fixture_yaml("bench-ns-01", COMP_GVR, COMP_VERSION, COMP_RES))
    if "status" in fresh_modes:
        add("the status freshness fixture",
            list_dep_status_fixture_yaml("bench-ns-01", COMP_GVR, COMP_VERSION, COMP_RES))
    return out
