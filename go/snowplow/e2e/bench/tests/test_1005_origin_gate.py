"""#1005: the portal-origin gate's own window, and a stop that says which
failure it is.

The installer writes the portal's same-origin URLs from a lookup of the
platform gateway's LoadBalancer address, re-rendered at every resync of its
composition-dynamic-controller. The gate waits, by its OWN window, up to
ENDPOINT_WAIT_S for the gateway's address, then 2 x the cdc's EFFECTIVE resync
interval + one poll. The interval is read from the cluster: the cdc's
-resync-interval arg, else its env / envFrom ConfigMap, else the compiled
default. The window is never what the platform's earlier waits left. A gate
that runs out names its failure from two read-only readings per poll (the
gateway's ingress, installer-platform's helm revision): waiting on GCP,
nothing reconciling, or an installer defect.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import origin_gate
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

ENV = origin_gate.ENV


def _deploy(args=(), env=(), cms=("installers-v0-3-468-configmap",), name="installers-v0-3-468-controller"):
    c = {"args": list(args), "env": [{"name": k, "value": v} for k, v in env],
         "envFrom": [{"configMapRef": {"name": n}} for n in cms]}
    return {"items": [{"metadata": {"name": "snowplow"}, "spec": {"template": {"spec": {"containers": [{}]}}}},
                      {"metadata": {"name": name}, "spec": {"template": {"spec": {"containers": [c]}}}}]}


def _cms(value=None, name="installers-v0-3-468-configmap"):
    return {"items": [{"metadata": {"name": name}, "data": {ENV: value} if value else {}}]}


# ─── the interval, by the cdc's own precedence ──────────────────────────────

def test_the_arg_wins_over_env_and_configmap():
    r = origin_gate.resync_interval(_deploy(args=["-resync-interval=90s"], env=[(ENV, "2m")]), _cms("60s"))
    assert r["seconds"] == 90 and "arg -resync-interval=90s" in r["source"]


def test_then_the_containers_own_env():
    r = origin_gate.resync_interval(_deploy(env=[(ENV, "2m")]), _cms("60s"))
    assert r["seconds"] == 120 and f"env {ENV}=2m" in r["source"]


def test_then_the_envfrom_configmap_as_on_a_fresh_install():
    r = origin_gate.resync_interval(_deploy(), _cms("60s"))
    assert r["seconds"] == 60 and "envFrom ConfigMap installers-v0-3-468-configmap" in r["source"]


def test_else_the_compiled_default():
    r = origin_gate.resync_interval(_deploy(), _cms(None))
    assert r["seconds"] == 180 and "compiled default" in r["source"]
    r = origin_gate.resync_interval({"items": []}, {})
    assert r["seconds"] == 180 and "no installers-*-controller Deployment" in r["source"]


def test_an_unparseable_value_falls_through_to_the_next_source():
    r = origin_gate.resync_interval(_deploy(args=["-resync-interval=soon"], env=[(ENV, "")]), _cms("1m30s"))
    assert r["seconds"] == 90 and "envFrom" in r["source"]
    assert [origin_gate.parse_duration(x) for x in ("60s", "3m", "1m30s", "1h", "500ms", "", "3", "x", "-1s")] == \
        [60, 180, 90, 3600, 0.5, None, None, None, None]


# ─── the window ─────────────────────────────────────────────────────────────

def test_the_window_is_two_resyncs_from_the_gateways_address():
    assert origin_gate.deadline(100.0, None, 60, 5, 600) == 700                 # waiting on the address
    assert origin_gate.deadline(100.0, 400.0, 60, 5, 600) == 400 + 2 * 60 + 5   # from the address, not t0


# ─── the verdict ────────────────────────────────────────────────────────────

R = {"seconds": 60, "source": "x"}


def _r(t, address, rev):
    return {"t": t, "address": address, "revision": rev}


def test_no_address_is_waiting_on_gcp():
    k, m = origin_gate.verdict([_r(0, False, 4), _r(600, False, 9)], R, "why")
    assert k == "lb_pending" and "waiting on GCP" in m and "600s" in m


def test_a_frozen_revision_after_the_address_is_nothing_reconciling():
    k, m = origin_gate.verdict([_r(0, False, 4), _r(10, True, 7), _r(130, True, 7)], R, "why")
    assert k == "not_reconciling" and "revision 7 for 120s" in m


def test_a_moving_revision_with_the_address_is_an_installer_defect():
    k, m = origin_gate.verdict([_r(10, True, 7), _r(70, True, 8), _r(130, True, 9)], R, "3 origins")
    assert k == "installer_defect" and "INSTALLER DEFECT" in m and "7 -> 9" in m and "3 origins" in m


def test_an_unread_revision_says_so():
    k, m = origin_gate.verdict([_r(10, True, None), _r(130, True, None)], R, "why")
    assert k == "revision_unknown"


def test_a_reading_parses_helms_history():
    assert origin_gate.reading(5, "10.0.0.9", json.dumps([{"revision": 12}])) == {"t": 5, "address": True,
                                                                                  "revision": 12}
    assert origin_gate.reading(5, "", "") == {"t": 5, "address": False, "revision": None}


# ─── the gate in run606's real sequence ─────────────────────────────────────

def _run(harness, **kw):  # noqa: F811
    env, state, ambient, tmp = harness
    env = dict(env, **kw)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    return p, p.stdout + p.stderr, tmp / "out"


@pytest.mark.parametrize("cdc,source", [
    ("args:45s", "installers-v0-3-422-controller arg -resync-interval=45s"),
    ("env:2m", f"installers-v0-3-422-controller env {ENV}=2m"),
    ("configmap:60s", f"installers-v0-3-422-controller envFrom ConfigMap installers-v0-3-422-configmap {ENV}=60s"),
    ("none", "the cdc's compiled default (no installers-*-controller Deployment found)"),
])
def test_the_gate_reads_and_records_the_cdcs_resync_from_each_source(harness, cdc, source):  # noqa: F811
    p, out, d = _run(harness, FAKE_CDC_RESYNC=cdc)
    assert p.returncode == 0, out[-3000:]
    secs = {"args:45s": 45, "env:2m": 120, "configmap:60s": 60, "none": 180}[cdc]
    assert f"the installer cdc resyncs every {secs}s ({source}); window: up to 600s" in out
    assert json.loads((d / "portal-origin-resync.json").read_text())["seconds"] == secs


def test_no_gateway_address_stops_saying_waiting_on_gcp(harness):  # noqa: F811
    p, out, d = _run(harness, FAKE_PORTAL="split", FAKE_GATEWAY="none", ENDPOINT_WAIT_S="2")
    assert p.returncode != 0 and "portal origin FAILED: lb_pending: waiting on GCP" in out
    assert "=== step env ===" not in out


def test_a_frozen_installer_stops_saying_nothing_reconciles(harness):  # noqa: F811
    p, out, d = _run(harness, FAKE_PORTAL="split", FAKE_HELM_REVISION="frozen", FAKE_CDC_RESYNC="args:1s")
    assert p.returncode != 0 and "portal origin FAILED: not_reconciling: nothing reconciles the installer" in out
    readings = [json.loads(x) for x in (d / "portal-origin-wait.jsonl").read_text().splitlines()]
    assert len(readings) >= 2 and {r["revision"] for r in readings} == {40} and all(r["address"] for r in readings)


def test_a_squeezed_platform_budget_still_leaves_the_gate_its_window(harness):  # noqa: F811
    """The platform's earlier waits used their whole budget (PLATFORM_TIMEOUT_S
    = DEPLOY_WAIT_S = 1: a deadline derived from them is past before the gate
    starts), yet the gate gets its own 2 x resync after the address, and the
    URLs the installer writes on its 4th read are seen."""
    p, out, d = _run(harness, FAKE_PORTAL="after:4", FAKE_CDC_RESYNC="args:3s",
                     PLATFORM_TIMEOUT_S="1", DEPLOY_WAIT_S="1")
    assert p.returncode == 0, out[-3000:]
    assert out.count("portal not same-origin yet") == 4 and "portal origin (config.json): http://10.0.0.9" in out
