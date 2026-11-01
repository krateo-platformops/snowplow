"""Every object the bench applies fits the Kubernetes API's size limits.

Run 15 (2026-10-24, bench 0b989a7b) died at its preflight step: `kubectl
apply` of the load606 ConfigMap — the bench code shipped to the in-cluster
Jobs, ~250 KB after #877 — was refused: "metadata.annotations: Too long: may
not be more than 262144 bytes". Client-side apply copies the whole object
into the kubectl.kubernetes.io/last-applied-configuration annotation. The
dry passes never apply, so they could not see it; this test renders what
run606.sh applies (its DRY pass writes loadgen-job.yaml; the preflight
manifest is rendered with the same arguments) and checks each object against
the API's limits, with the annotation each apply SITE would add.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from bench import gke

BENCH = Path(__file__).resolve().parents[1]
FAKE_DIGEST = "sha256:" + "0" * 64
# k8s.io/apimachinery/pkg/api/validation: TotalAnnotationSizeLimitB
ANNOTATIONS_LIMIT = 256 * 1024
# k8s.io/kubernetes/pkg/apis/core/validation: a ConfigMap's data + binaryData
CONFIGMAP_DATA_LIMIT = 1024 * 1024
# etcd's default --max-request-bytes: one object, serialized
OBJECT_LIMIT = int(1.5 * 1024 * 1024)
LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"


def _objects(text: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(text) if d]


def _client_side_annotation(obj: dict) -> int:
    """The bytes client-side apply would add: the object itself, as JSON."""
    return len(json.dumps(obj, separators=(",", ":")).encode())


def _annotation_bytes(obj: dict) -> int:
    ann = (obj.get("metadata") or {}).get("annotations") or {}
    return sum(len(k.encode()) + len(str(v).encode()) for k, v in ann.items())


def check(obj: dict, *, client_side: bool) -> list[str]:
    """The limits `obj` breaks when applied (client_side: with the
    last-applied annotation kubectl would add)."""
    name = f"{obj.get('kind')}/{(obj.get('metadata') or {}).get('name')}"
    bad = []
    ann = _annotation_bytes(obj) + (len(LAST_APPLIED) + _client_side_annotation(obj) if client_side else 0)
    if ann > ANNOTATIONS_LIMIT:
        bad.append(f"{name}: annotations {ann} B > {ANNOTATIONS_LIMIT} B"
                   + (" (client-side apply's last-applied copy)" if client_side else ""))
    if obj.get("kind") == "ConfigMap":
        data = sum(len(k.encode()) + len(v.encode()) for k, v in (obj.get("data") or {}).items())
        data += sum(len(k.encode()) + len(v.encode()) for k, v in (obj.get("binaryData") or {}).items())
        if data > CONFIGMAP_DATA_LIMIT:
            bad.append(f"{name}: data {data} B > {CONFIGMAP_DATA_LIMIT} B")
    size = _client_side_annotation(obj) * (2 if client_side else 1)
    if size > OBJECT_LIMIT:
        bad.append(f"{name}: {size} B serialized > {OBJECT_LIMIT} B")
    return bad


def _run606_dry(tmp_path: Path, **env) -> Path:
    from tests.test_run606_real_sequence import fake_registry   # the dry pass reads the registry
    out = tmp_path / "out"
    e = dict(os.environ, **fake_registry(tmp_path))
    e.update(HOME=str(tmp_path), OUT=str(out), RUN_NAME="snowplow-bench-sizes",
             INSTALLER_VERSION="0.3.422", SNOWPLOW_TAG="1.12.46", BENCH_606_DRY_RUN="1",
             EXPECTED_IMAGE_DIGEST=FAKE_DIGEST, PYTHONPATH=str(BENCH), **env)
    p = subprocess.run(["bash", str(BENCH / "run606.sh")], env=e, capture_output=True, text=True,
                       timeout=300, check=False)
    assert p.returncode == 0, p.stderr[-2000:]
    return out


def _preflight_manifest() -> str:
    """run606.sh s_preflight's render, with its argument shape."""
    p = subprocess.run([sys.executable, "-m", "bench.load606_job", "--preflight-only",
                        "--snowplow-url", "http://snowplow.krateo-system.svc:8081",
                        "--authn-url", "http://authn.krateo-system.svc:8082", "--password", "x",
                        "--init", ("/call?resource=layouts&apiVersion=widgets.templates.krateo.io/v1beta1"
                                   "&name=app-shell&namespace=krateo-system"),
                        "--frontend-ns", "krateo-system", "--builders-ns", "krateo-system"],
                       cwd=BENCH, env=dict(os.environ, PYTHONPATH=str(BENCH)),
                       capture_output=True, text=True, check=True)
    return p.stdout


def _kapply() -> str:
    src = (BENCH / "run606.sh").read_text()
    m = re.search(r"^kapply\(\) \{(.*)\}$", src, re.MULTILINE)
    assert m, "run606.sh has no kapply()"
    return m.group(1)


def test_run606_applies_everything_through_one_server_side_apply():
    """Every kubectl apply in run606.sh is kapply's, and kapply is
    server-side (no last-applied copy)."""
    src = (BENCH / "run606.sh").read_text()
    applies = [ln.strip() for ln in src.splitlines()
               if re.search(r"\bapply\b", ln) and not ln.lstrip().startswith("#")]
    assert applies == [ln for ln in applies if ln.startswith("kapply()")], applies
    sites = [ln.strip() for ln in src.splitlines() if ln.strip().startswith('kapply "')]
    # #924 M3: the bench's own FlowSchemas (env); #1042's pre-drive PSI Job; the preflight Job; #935's
    # fresh identity classes (data; its output names users: discarded); run 19's
    # menu-class identities (classes; the same); the load. Each class's preflight
    # Job goes through kapply inside run_preflight_job.
    assert sites == ['kapply "$OUT/bench-flowschemas.yaml"', 'kapply "$OUT/psi-job.yaml"',
                     'kapply "$OUT/preflight-job.yaml"',
                     'kapply "$OUT/fresh-classes.yaml" >/dev/null 2>"$KERR" || record_failure "fresh classes FAILED: applying their manifest"',
                     'kapply "$OUT/menu-classes.yaml" >/dev/null 2>"$KERR" \\',
                     'kapply "$OUT/loadgen-job.yaml"']
    assert '[[ "$job" == preflight606 ]] || kapply "${out%.jsonl}-job.yaml"' in src
    assert "--server-side" in _kapply()


def test_the_fresh_classes_fit_the_api():
    from bench import fresh_classes
    p = fresh_classes.plan(sessions=5, routes=8, think_s=15, duration_s=9000, namespaces=50, user_prefix="scaleuser-")
    objs = _objects(fresh_classes.manifest(p, "pw"))
    assert len(objs) == 3 * 5 * (p["classes_per_session"] - 1)
    assert all(check(o, client_side=False) == [] for o in objs)


def test_the_bench_flowschemas_fit_the_api():
    from bench import flowcontrol
    orig = {i: {"flow_schema": "service-accounts", "precedence": 9000, "priority_level": "workload-low"}
            for i in flowcontrol.IDENTITIES}
    objs = _objects(flowcontrol.render(orig, "snowplow"))
    assert [o["metadata"]["name"] for o in objs] == list(flowcontrol.BENCH_SCHEMA.values())
    assert all(check(o, client_side=False) == [] for o in objs)


@pytest.mark.parametrize("driver", ["k6", "load606"])
def test_every_object_run606_applies_fits_the_api(tmp_path, driver):
    out = _run606_dry(tmp_path, LOAD_DRIVER=driver)
    client_side = "--server-side" not in _kapply()
    manifests = {"preflight": _preflight_manifest(), "loadgen": (out / "loadgen-job.yaml").read_text()}
    seen = []
    for site, text in manifests.items():
        objs = _objects(text)
        assert objs, site
        for o in objs:
            seen.append((site, o["kind"], o["metadata"]["name"]))
            assert check(o, client_side=client_side) == [], (site, o["kind"], o["metadata"]["name"])
    # the bench code itself ships: the load606 ConfigMap, at both sites
    assert ("preflight", "ConfigMap", "load606") in seen and ("loadgen", "ConfigMap", "load606") in seen


def test_the_self_destruct_manifest_fits_the_api():
    """gke-up applies it client-side: the copy is counted."""
    for o in _objects(gke.self_destruct_manifest(gke.default_spec("snowplow-bench-sizes"))):
        assert check(o, client_side=True) == []


def test_the_check_sees_run15s_failure():
    """The load606 ConfigMap applied client-side, as run 15 did: refused."""
    cm = next(o for o in _objects(_preflight_manifest()) if o["kind"] == "ConfigMap")
    bad = check(cm, client_side=True)
    assert len(bad) == 1 and "annotations" in bad[0] and "client-side" in bad[0], bad
    assert check(cm, client_side=False) == []
    big = {"kind": "ConfigMap", "metadata": {"name": "x"}, "data": {"a": "y" * (CONFIGMAP_DATA_LIMIT + 1)}}
    assert any("data" in b for b in check(big, client_side=False))
