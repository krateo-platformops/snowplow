"""installer#614: cert-manager before the installer, exactly as krateo-057 has it.

Paired run 21 failed as infrastructure: installer 0.3.506 ships cert-manager
only under features.ingress (off on the bench), the ClickHouse and MongoDB
operators need cert-manager.io, so they and their 5 dependents were never
created (33 of 40 declared components). 057 has cert-manager only because it
was installed there by hand: helm release cert-manager, chart
cert-manager-v1.21.1, namespace cert-manager, user values crds.enabled=true.

The arms: the pinned chart is the real jetstack archive and its sha256 is
verified before any cluster exists; a tampered archive refuses the run for
free; the install runs before the installer's, with exactly that chart,
version, release, namespace and values, then waits for its Deployments
Available and its CRDs Established (both derived from the verified chart);
the run's record is what helm reads back, compared with 057's reading; the
dry pass creates nothing.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess

import pytest
import yaml

from bench import cert_manager as cm
from tests.test_run606_real_sequence import (BENCH_DIR, CERT_MANAGER_TGZ, FAKE_DIGEST, RUN_NAME,  # noqa: F401
                                             _calls, fake_registry, harness)

CHART_FILE = "cert-manager-v1.21.1.tgz"
DEPLOYMENTS = ["cert-manager", "cert-manager-cainjector", "cert-manager-webhook"]
CM_IO_CRDS = {"certificaterequests.cert-manager.io", "certificates.cert-manager.io",
              "clusterissuers.cert-manager.io", "issuers.cert-manager.io"}


# ─── the pins: 057's reading, the real archive ──────────────────────────────

def test_the_pins_are_057s_reading():
    assert (cm.RELEASE, cm.NAMESPACE, cm.CHART_NAME, cm.CHART_VERSION) == (
        "cert-manager", "cert-manager", "cert-manager", "v1.21.1")
    assert cm.VALUES == {"crds": {"enabled": True}}          # and nothing else
    assert cm.AT_057 == {"release": "cert-manager", "namespace": "cert-manager",
                         "chart": "cert-manager-v1.21.1", "app_version": "v1.21.1",
                         "values": {"crds": {"enabled": True}}}
    assert cm.CHART_REF == "oci://quay.io/jetstack/charts/cert-manager"
    assert "installer#614" in cm.REASON and "installer ships cert-manager itself" in cm.REASON


def test_the_fixture_is_the_pinned_archive():
    """The suite serves the REAL archive, so the pinned digest is what it checks."""
    import hashlib
    assert hashlib.sha256(CERT_MANAGER_TGZ.read_bytes()).hexdigest() == cm.CHART_SHA256


# ─── preflight: pull, verify, render ────────────────────────────────────────

def _fake_run(tamper=False, pull_rc=0, extra_file=None):
    calls = []

    def run(argv, **kw):
        calls.append(argv)
        if argv[:2] == ["helm", "pull"]:
            d = argv[argv.index("-d") + 1]
            data = CERT_MANAGER_TGZ.read_bytes()
            if tamper:
                data = data[:-1] + bytes([data[-1] ^ 1])
            if pull_rc == 0:
                (open(os.path.join(d, CHART_FILE), "wb")).write(data)
                if extra_file:
                    open(os.path.join(d, extra_file), "wb").write(data)
            return subprocess.CompletedProcess(argv, pull_rc, "", "Error: boom" if pull_rc else "")
        return subprocess.run(argv, **kw)        # helm template: the real helm
    return run, calls


@pytest.mark.skipif(shutil.which("helm") is None, reason="renders with the real helm")
def test_preflight_verifies_the_archive_and_derives_what_to_wait_for(tmp_path):
    run, calls = _fake_run()
    res = cm.preflight(str(tmp_path), run=run)
    assert res["ok"], res["failures"]
    assert calls[0] == ["helm", "pull", "oci://quay.io/jetstack/charts/cert-manager",
                        "--version", "v1.21.1", "-d", str(tmp_path)]
    assert res["pulled_sha256"] == cm.CHART_SHA256 and res["app_version"] == "v1.21.1"
    # the values file holds exactly crds.enabled=true, and the render used it
    assert yaml.safe_load((tmp_path / "values.yaml").read_text()) == {"crds": {"enabled": True}}
    assert calls[1] == ["helm", "template", "cert-manager", str(tmp_path / CHART_FILE),
                        "--namespace", "cert-manager", "-f", str(tmp_path / "values.yaml")]
    assert res["deployments"] == DEPLOYMENTS
    assert CM_IO_CRDS <= set(res["crds"]) and len(res["crds"]) == 6     # + the 2 acme.cert-manager.io
    assert res["chart_file"] == str(tmp_path / CHART_FILE)
    assert "krateo-057" in res["mirrors"] and "installer#614" in res["reason"]


def test_preflight_refuses_a_tampered_archive_and_removes_it(tmp_path):
    run, calls = _fake_run(tamper=True)
    res = cm.preflight(str(tmp_path), run=run)
    assert not res["ok"] and "sha256" in res["failures"][0] and cm.CHART_SHA256 in res["failures"][0]
    assert not (tmp_path / CHART_FILE).exists()                    # never installable by mistake
    assert [c[:2] for c in calls] == [["helm", "pull"]]            # nothing rendered


def test_preflight_refuses_a_failed_pull_and_an_unexpected_layout(tmp_path):
    run, _ = _fake_run(pull_rc=1)
    res = cm.preflight(str(tmp_path / "a"), run=run)
    assert not res["ok"] and "helm pull" in res["failures"][0]
    run, _ = _fake_run(extra_file="cert-manager-v1.20.0.tgz")
    res = cm.preflight(str(tmp_path / "b"), run=run)
    assert not res["ok"] and "unexpected pull layout" in res["failures"][0]


def test_a_previous_runs_archive_never_counts(tmp_path):
    (tmp_path / "cert-manager-v1.20.0.tgz").write_bytes(b"old")
    run, _ = _fake_run(pull_rc=1)
    assert not cm.preflight(str(tmp_path), run=run)["ok"]
    assert not (tmp_path / "cert-manager-v1.20.0.tgz").exists()


def test_rendered_waits_need_a_cert_manager_io_crd():
    deps, crds = cm.rendered_waits("kind: Deployment\nmetadata: {name: d}\n---\n"
                                   "kind: CustomResourceDefinition\nmetadata: {name: orders.acme.cert-manager.io}\n")
    assert deps == ["d"] and crds == ["orders.acme.cert-manager.io"]


# ─── the run's record: helm's readback against 057's reading ────────────────

PRE = {"kind": "cluster_prerequisite", "name": "cert-manager", "chart_ref": cm.CHART_REF,
       "version": "v1.21.1", "chart_sha256": cm.CHART_SHA256, "app_version": "v1.21.1",
       "mirrors": cm.MIRRORS, "reason": cm.REASON, "deployments": DEPLOYMENTS, "crds": sorted(CM_IO_CRDS)}
LISTED = {"name": "cert-manager", "namespace": "cert-manager", "status": "deployed",
          "chart": "cert-manager-v1.21.1", "app_version": "v1.21.1"}


def test_readback_that_matches_057_is_recorded_as_mirroring_it():
    rec = cm.readback(PRE, [LISTED], {"crds": {"enabled": True}})
    assert rec["mirrors_057"] and rec["mismatches"] == []
    assert rec["kind"] == "cluster_prerequisite" and rec["version"] == "v1.21.1"
    assert rec["installed"]["chart"] == "cert-manager-v1.21.1" and rec["installed"]["status"] == "deployed"


@pytest.mark.parametrize("listed,values,field", [
    (dict(LISTED, chart="cert-manager-v1.20.0"), {"crds": {"enabled": True}}, "chart"),
    (dict(LISTED, app_version="v1.20.0"), {"crds": {"enabled": True}}, "app_version"),
    (dict(LISTED, namespace="krateo-system"), {"crds": {"enabled": True}}, "namespace"),
    (LISTED, {"crds": {"enabled": True}, "replicaCount": 2}, "values"),
    (LISTED, None, "values"),
    (dict(LISTED, status="failed"), {"crds": {"enabled": True}}, "status"),
    (dict(LISTED, name="other"), {"crds": {"enabled": True}}, "release"),
])
def test_readback_that_differs_from_057_is_not_mirroring_it(listed, values, field):
    rec = cm.readback(PRE, [listed], values)
    assert not rec["mirrors_057"] and any(m.startswith(field + ":") for m in rec["mismatches"])


# ─── run606: the real sequence ──────────────────────────────────────────────

def _run(harness, **env_extra):  # noqa: F811
    env, state, ambient, tmp = harness
    env = dict(env, **env_extra)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True,
                       text=True, timeout=300)
    return p, state, tmp


def test_run606_installs_cert_manager_as_057_has_it_before_the_installer(harness):  # noqa: F811
    p, state, tmp = _run(harness)
    out = tmp / "out"
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    calls = _calls(state)

    def strip(argv):          # run606's H=(helm --kubeconfig .. --kube-context ..)
        return [x for i, x in enumerate(argv) if x not in ("--kubeconfig", "--kube-context")
                and (i == 0 or argv[i - 1] not in ("--kubeconfig", "--kube-context"))]
    helm = [(i, strip(c["argv"])) for i, c in enumerate(calls) if c["tool"] == "helm"]
    i_cm, cm_argv = next((i, a) for i, a in helm if a[:2] == ["install", "cert-manager"])
    i_inst = next(i for i, a in helm if a[:2] == ["install", "installer"])
    # the verified archive (never a second pull), 057's release, namespace and values
    assert cm_argv == ["install", "cert-manager", str(out / "cert-manager" / CHART_FILE), "-n", "cert-manager",
                       "--create-namespace", "-f", str(out / "cert-manager" / "values.yaml"),
                       "--wait", "--timeout", "300s"]
    assert yaml.safe_load((out / "cert-manager" / "values.yaml").read_text()) == {"crds": {"enabled": True}}
    assert len([a for _, a in helm if a[:1] == ["pull"] and "jetstack" in a[1]]) == 1
    # the pull is before any cluster exists
    i_pull = next(i for i, a in helm if a[:1] == ["pull"] and "jetstack" in a[1])
    i_create = next(i for i, c in enumerate(calls) if c["tool"] == "gcloud" and c["argv"][:3] == [
        "container", "clusters", "create"])
    assert i_pull < i_create < i_cm < i_inst
    # its Deployments Available, then its CRDs Established, both before the installer
    waits = [(i, c["argv"]) for i, c in enumerate(calls) if c["tool"] == "kubectl" and "wait" in c["argv"]]
    i_dep, dep = next((i, a) for i, a in waits if "--for=condition=Available" in a and "deployment/cert-manager" in a)
    i_crd, crd = next((i, a) for i, a in waits if "--for=condition=Established" in a
                      and "crd/certificates.cert-manager.io" in a)
    assert i_cm < i_dep < i_crd < i_inst
    assert [x for x in dep if x.startswith("deployment/")] == [f"deployment/{d}" for d in DEPLOYMENTS]
    assert CM_IO_CRDS <= {x[len("crd/"):] for x in crd if x.startswith("crd/")}
    assert "cert-manager" == dep[dep.index("-n") + 1]
    # the run's record: the version, and that it mirrors 057's hand install, read back
    rec = [json.loads(x) for x in (out / "cert-manager.jsonl").read_text().splitlines()]
    assert len(rec) == 1 and rec[0]["mirrors_057"] and rec[0]["version"] == "v1.21.1"
    assert rec[0]["installed"] == {"release": "cert-manager", "namespace": "cert-manager",
                                   "chart": "cert-manager-v1.21.1", "app_version": "v1.21.1",
                                   "values": {"crds": {"enabled": True}}, "status": "deployed"}
    assert "installer#614" in rec[0]["reason"] and "krateo-057" in rec[0]["mirrors"]
    # and the verdict carries it
    v = json.loads((out / "verdict.json").read_text())
    assert [r["version"] for r in v["report"]["cluster_prerequisites"]] == ["v1.21.1"]
    assert not any("cluster prerequisites" in w for w in v["report"].get("report_warnings") or [])
    log = (out / "run.log").read_text()
    assert log.index("=== step psi ===") < log.index("=== step cert-manager ===") < log.index("=== step install ===")
    assert "mirrors krateo-057's hand install (installer#614)" in log


def test_a_tampered_chart_refuses_the_run_before_any_cluster(harness):  # noqa: F811
    p, state, tmp = _run(harness, FAKE_CERT_MANAGER_TAMPER="1")
    assert p.returncode != 0
    calls = _calls(state)
    assert not any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "create"] for c in calls)
    assert not any(c["tool"] == "kubectl" for c in calls)
    assert "cert-manager source FAILED" in p.stderr and "cert-manager preflight REFUSED" in p.stderr
    rec = [json.loads(x) for x in (tmp / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert [r["step"] for r in rec] == ["cert-manager-source"]
    assert not (tmp / "out" / "cert-manager" / CHART_FILE).exists()


def test_a_release_that_does_not_mirror_057_fails_before_the_installer(harness):  # noqa: F811
    p, state, tmp = _run(harness, FAKE_CERT_MANAGER_CHART="cert-manager-v1.20.0")
    assert p.returncode != 0
    assert "does NOT mirror krateo-057's hand install: chart: 'cert-manager-v1.20.0'" in p.stderr
    assert "=== step install ===" not in p.stderr
    helm = [c["argv"] for c in _calls(state) if c["tool"] == "helm"]
    assert not any("installer" in a and "install" in a for a in helm)
    rec = [json.loads(x) for x in (tmp / "out" / "cert-manager.jsonl").read_text().splitlines()]
    assert not rec[0]["mirrors_057"]


def test_every_cluster_call_of_the_step_is_bounded():
    """The install (helm --timeout) and the waits (kubectl --timeout) carry their
    own bound; the two readback reads run under `bounded` with what is left of
    the step's one bound (a hung read would otherwise hold the run)."""
    src = (BENCH_DIR / "run606.sh").read_text()
    body = src[src.index("s_cert_manager() {"):src.index("s_install() {")].replace("\\\n", " ")
    calls = [ln.strip() for ln in body.splitlines() if '"${H[@]}"' in ln or '"${K[@]}"' in ln]
    assert len(calls) == 5, calls
    for c in calls:
        if c.startswith("run "):
            assert "--timeout" in c, c
        else:
            assert c.startswith('bounded "$left" '), c


# ─── the dry pass creates nothing ───────────────────────────────────────────

def test_the_dry_pass_verifies_the_chart_and_creates_nothing(tmp_path):
    env = dict(os.environ, **fake_registry(tmp_path))
    env.update(HOME=str(tmp_path), OUT=str(tmp_path / "out"), RUN_NAME=RUN_NAME, INSTALLER_VERSION="0.3.422",
               SNOWPLOW_TAG="1.12.46", THINK_S="0", BENCH_606_DRY_RUN="1", EXPECTED_IMAGE_DIGEST=FAKE_DIGEST,
               PYTHONPATH=str(BENCH_DIR))
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    assert p.returncode == 0, p.stderr[-3000:]
    # the fake registry's helm saw only reads: pulls and offline renders, no install, nothing in a cluster
    calls = _calls(tmp_path / "registry-state")
    assert calls and all(c["tool"] == "helm" for c in calls)
    assert {c["argv"][0] for c in calls} <= {"show", "pull", "template"}
    assert not (tmp_path / "registry-state" / "cert-manager-installed").exists()
    log = (tmp_path / "out" / "run.log").read_text()
    assert log.index("=== step cert-manager-source ===") < log.index("=== step up ===")
    assert log.index("=== step cert-manager ===") < log.index("=== step install ===")
    i = log.index("=== step cert-manager ===")
    seg = log[i:log.index("=== step install ===")]
    assert f"DRY: helm --kubeconfig" in seg and f"install cert-manager {tmp_path}/out/cert-manager/{CHART_FILE}" in seg
    assert "DRY: kubectl" in seg and "--for=condition=Available" in seg and "--for=condition=Established" in seg
    assert "RUN:" not in seg
    assert not (tmp_path / "out" / "cert-manager.jsonl").exists()
    # the bound is a term of the run's budget
    assert "+ cert-manager 300 + install 900" in p.stderr
