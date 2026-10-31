"""PR #896 review (adopted from the reviewer's scratch test): the PVC release
must never mutate anything when the bench kubeconfig is missing, empty, or
holds only a non-bench context (even when the ambient ~/.kube/config /
KUBECONFIG has a current-context).

REAL kubectl is used (through a logging shim); every kubeconfig points at
https://127.0.0.1:1 so nothing can be reached. gcloud is a fake. Dropping
--context or --kubeconfig from gke._kubectl turns this RED (the positive
control stops releasing, or a non-bench context resolves).
"""
from __future__ import annotations

import json
import os
import shutil
import textwrap

import pytest

from bench import gke
from tests import test_gke_pvc_disks as T

REAL_KUBECTL = shutil.which("kubectl")
NAME = T.NAME
CTX = None  # set per spec


def _cfg(contexts, current):
    return textwrap.dedent(f"""\
        apiVersion: v1
        kind: Config
        clusters:
        - cluster: {{server: "https://127.0.0.1:1"}}
          name: fake
        contexts:
        """) + "".join(f"- context: {{cluster: fake, user: fake}}\n  name: {c}\n" for c in contexts) + textwrap.dedent(f"""\
        current-context: {current}
        users:
        - name: fake
          user: {{token: x}}
        """)


@pytest.fixture
def realkubectl(T_fakes, tmp_path, monkeypatch):
    if not REAL_KUBECTL:
        pytest.skip("no kubectl")
    shim = T_fakes / "bin" / "kubectl"
    # The shim asks REAL kubectl which context these exact flags resolve to.
    # If one resolves, it plays a REACHABLE cluster (serves PVs/pods), so a
    # code path that lands on a non-bench context WOULD issue its deletes.
    pvs = json.dumps({"items": T.PVS})
    pods = json.dumps({"items": [{"metadata": {"name": "mongodb-0", "namespace": "krateo-system"},
                                  "spec": {"volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "x"}}]}}]})
    (T_fakes / "pvs_k.json").write_text(pvs)
    (T_fakes / "pods_k.json").write_text(pods)
    shim.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        printf '%s\\n' "$*" >> "{T_fakes}/state/kubectl_real.log"
        g=(); r=()
        for x in "$@"; do case "$x" in --kubeconfig=*|--context=*) g+=("$x");; --request-timeout=*) ;; *) r+=("$x");; esac; done
        ctx=$("{REAL_KUBECTL}" "${{g[@]}}" config view --minify -o 'jsonpath={{.current-context}}' 2>/dev/null) || exec "{REAL_KUBECTL}" "$@"
        [ -n "$ctx" ] || exec "{REAL_KUBECTL}" "$@"
        echo "RESOLVED $ctx :: ${{r[*]}}" >> "{T_fakes}/state/resolved.log"
        case "${{r[0]}} ${{r[1]}}" in
          "get pv") cat "{T_fakes}/pvs_k.json";;
          "get pods") cat "{T_fakes}/pods_k.json";;
          "apply --server-side") cat > "{T_fakes}/state/guard_k";;     # #954: the no-new-claims policy
          "create --dry-run=server")                                     # refused once it is in force
            [ -f "{T_fakes}/state/guard_k" ] || exit 0
            echo "ValidatingAdmissionPolicy 'bench-teardown-no-new-claims' with binding" \
                 "'bench-teardown-no-new-claims' denied request" >&2; exit 1;;
          "config view") exec "{REAL_KUBECTL}" "$@";;    # reads the kubeconfig file only
        esac
        exit 0
        """))
    shim.chmod(0o755)
    home = T_fakes / "home"
    (home / ".kube").mkdir(parents=True, exist_ok=True)
    # the ambient config: a non-bench current-context
    (home / ".kube" / "config").write_text(_cfg(["krateo-057-ambient"], "krateo-057-ambient"))
    monkeypatch.setenv("KUBECONFIG", str(home / ".kube" / "config"))
    return T_fakes


T_fakes = T.fakes


def _mutations(tmp):
    log = tmp / "state" / "kubectl_real.log"
    lines = log.read_text().splitlines() if log.exists() else []
    return lines, [l for l in lines if l.split()[3:4] and l.split()[3] in ("delete", "patch", "create", "apply")]


@pytest.mark.parametrize("shape", ["missing", "empty", "non-bench-context-only"])
def test_release_never_mutates_without_the_bench_context(realkubectl, shape):
    spec = gke.default_spec(NAME, kubeconfig_dir=str(realkubectl / "kc"))
    os.makedirs(os.path.dirname(spec.kubeconfig), exist_ok=True)
    if shape == "empty":
        open(spec.kubeconfig, "w").close()
    elif shape == "non-bench-context-only":
        open(spec.kubeconfig, "w").write(_cfg(["other"], "other"))
    log = []
    gke.teardown(spec, dry_run=False, log=log.append, budget_s=60, sleep=lambda s: None)
    lines, muts = _mutations(realkubectl)
    assert lines, "kubectl was never called"
    rl = realkubectl / "state" / "resolved.log"
    assert not rl.exists(), "a non-bench context was resolved: " + rl.read_text()
    # the only kubectl calls are reads, which fail: the LoadBalancer release's endpoint check
    # (#1026: `config view`, before any Service is read or changed) and the PV read; nothing
    # mutating is ever issued
    assert all(l.split()[3:5] in (["get", "pv"], ["config", "view"]) for l in lines), lines
    assert any(l.split()[3:5] == ["get", "pv"] for l in lines), lines
    assert not muts, muts
    assert any("could not list" in m for m in log), log
    # the backstop still deletes only this cluster's disks, by its id label (#954)
    c = T._calls(realkubectl)
    i = T._first(c, ["gcloud", "compute", "disks", "delete"])
    assert c[i][4:c[i].index("--zone")] == ["pvc-ours-attached", "pvc-ours-leftover", "pvc-reprovisioned-id-only"]


def test_control_the_bench_context_present_does_release(realkubectl):
    """Positive control: the shim really plays a reachable cluster, so the
    negative arms above are a detector."""
    spec = gke.default_spec(NAME, kubeconfig_dir=str(realkubectl / "kc"))
    os.makedirs(os.path.dirname(spec.kubeconfig), exist_ok=True)
    open(spec.kubeconfig, "w").write(_cfg([gke.kube_context(spec)], "other-does-not-matter"))
    gke.teardown(spec, dry_run=False, log=lambda m: None, budget_s=60, clock=iter(range(0, 10**6, 40)).__next__,
                 sleep=lambda s: None)
    _, muts = _mutations(realkubectl)
    resolved = (realkubectl / "state" / "resolved.log").read_text()
    assert all(f"RESOLVED {gke.kube_context(spec)} ::" in l for l in resolved.splitlines())
    assert any("delete pvc --all" in m for m in muts)


def test_every_kubectl_call_pins_kubeconfig_and_context(T_fakes):
    """Every kubectl argv the teardown builds carries a non-empty
    --kubeconfig AND the bench --context (never the ambient current-context)."""
    spec = gke.default_spec(NAME)
    base = gke._kubectl(spec)
    assert base[1] == f"--kubeconfig={spec.kubeconfig}" and spec.kubeconfig
    assert base[2] == f"--context=gke_{spec.project}_{spec.region}_{NAME}"   # regional (#899)
