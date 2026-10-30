"""Run 5's `env` failure: snowplow/frontend LoadBalancers stayed pending (GCE
IN_USE_ADDRESSES quota, 14 global) and gke-env died at once. The fix:

- every step that talks HTTP to snowplow/authn runs IN-CLUSTER (load606 and
  fresh606 Jobs) against Service DNS — no external IP needed;
- ENDPOINT_MODE=external (LB IPs) is refused since run 19: the portal rides
  one gateway origin and its Services are ClusterIP;
- no step waits on the umbrella Installer Ready (never true on a
  credential-free bench: github-provider-kog 401, installer#484) — only the
  snowplow/frontend/authn Deployments.
"""

from __future__ import annotations

import json
import subprocess

import pytest
import yaml

from bench import gke, load606_job
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import (  # noqa: F401  (fixture import)
    BENCH_DIR, _calls, harness)


def _svcs(mode):
    def s(n, port):
        return {"metadata": {"name": n, "namespace": "krateo-system"},
                "spec": {"type": "ClusterIP" if mode == "clusterip" else "LoadBalancer",
                         "ports": [{"port": port}]},
                "status": {"loadBalancer": {"ingress": [{"ip": "1.2.3.4"}] if mode == "lb" else []}}}
    return {"items": [s("snowplow", 8081), s("authn", 8082), s("frontend", 8080)]}


def test_resolve_incluster_uses_service_dns_and_ports():
    eps = gke.resolve_incluster(_svcs("clusterip"))
    assert eps == {"SNOWPLOW_URL": "http://snowplow.krateo-system.svc:8081",
                   "AUTHN_URL": "http://authn.krateo-system.svc:8082",
                   "FRONTEND_URL": "http://frontend.krateo-system.svc:8080"}
    with pytest.raises(RuntimeError, match="SNOWPLOW_URL"):
        gke.resolve_incluster({"items": _svcs("clusterip")["items"][1:]})


def test_lb_progress_pending_fatal_found():
    ev = [{"involvedObject": {"kind": "Service", "name": "snowplow"},
           "reason": "SyncLoadBalancerFailed",
           "message": "Quota 'IN_USE_ADDRESSES' exceeded.  Limit: 14.0 globally."}]
    found, pending, fatal = gke.lb_progress(_svcs("pending"), ev)
    assert not found and not fatal
    assert any("snowplow LoadBalancer pending" in p and "IN_USE_ADDRESSES" in p
               for p in pending)
    found, pending, fatal = gke.lb_progress(_svcs("clusterip"))
    assert fatal and "is type ClusterIP" in fatal[0] and "ENDPOINT_MODE=incluster" in fatal[0]
    found, pending, fatal = gke.lb_progress(_svcs("lb"))
    assert found["SNOWPLOW_URL"] == "http://1.2.3.4:8081" and not pending and not fatal


def test_fresh_job_runs_incluster_with_least_privilege():
    m = load606_job.render_fresh(
        snowplow_url="http://snowplow.krateo-system.svc:8081",
        authn_url="http://authn.krateo-system.svc:8082", user="scaleuser-0001",
        password="pw", rates=["1", "10", "50"], samples=60, gap_s=5,
        probe_name="bench-app-01-01", namespaces=50,
        comp_group="composition.krateo.io", comp_version="v1-2-2",
        comp_plural="githubscaffoldingwithcompositionpages", delay_s=60)
    docs = [d for d in yaml.safe_load_all(m) if d]
    role = next(d for d in docs if d["kind"] == "ClusterRole")
    assert role["rules"] == [{"apiGroups": ["composition.krateo.io"],
                              "resources": ["githubscaffoldingwithcompositionpages"],
                              "verbs": ["get", "patch"]}]
    job = next(d for d in docs if d["kind"] == "Job")
    spec = job["spec"]["template"]["spec"]
    assert spec["serviceAccountName"] == "fresh606"
    assert spec["nodeSelector"] == {"bench.krateo.io/pool": "loadgen"}
    script = spec["containers"][0]["args"][0]
    assert "--mutator incluster" in script and "--rates 1 10 50" in script
    assert "http://snowplow.krateo-system.svc:8081" in script
    assert script.startswith("set -eu\nsleep 60\n")


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_clusterip_only_services_still_reach_data_and_measure(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_SVC="clusterip", FAKE_483="pass", FAKE_KOG="1")
    p = _run(env)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    assert "endpoint mode: incluster" in out
    assert "[env] SNOWPLOW_URL=http://snowplow.krateo-system.svc:8081" in out
    assert "=== step data ===" in p.stderr and "=== step measure ===" in p.stderr
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert "--snowplow-url \"http://snowplow.krateo-system.svc:8081\"" in job
    assert "--authn-url \"http://authn.krateo-system.svc:8082\"" in job
    assert "name: fresh606" in job
    calls = _calls(state)

    # waits only on the component Deployments, never on the umbrella Installer
    waits = [c["argv"] for c in calls if c["tool"] == "kubectl" and "wait" in c["argv"]]
    assert any("deploy/snowplow" in w and "deploy/frontend" in w and "deploy/authn" in w
               and "condition=Available" in " ".join(w) for w in waits)
    assert not any(any("installer" in x for x in w) for w in waits)
    assert any("job/fresh606" in w for w in waits)
    # the Installer CR was NOT Ready (kog 401) the whole time, and the run passed
    assert "installer Ready=False" in out


def test_external_mode_is_refused_before_any_cloud_call(harness):
    """Run 19: the portal rides ONE gateway origin (exposure Gateway, path
    mode, as on 057), whose component Services are ClusterIP, so the
    per-component LoadBalancer IPs external mode read never exist. Refused
    at once, before any cloud call, never a run that waits and fails."""
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(ENDPOINT_MODE="external")
    p = _run(env)
    assert p.returncode == 2
    assert "refusing: ENDPOINT_MODE=external" in p.stderr and "ENDPOINT_MODE=incluster" in p.stderr
    assert "=== step" not in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]


def test_no_bench_step_waits_on_the_umbrella_installer():
    src = (BENCH_DIR / "run606.sh").read_text()
    assert "installers.composition.krateo.io/installer" not in src
    assert "--for=condition=Ready" not in src
