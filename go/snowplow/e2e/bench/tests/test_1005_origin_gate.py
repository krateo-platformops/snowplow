"""#1005 / #1006: the portal-origin gate's own window, and a stop that says
which failure it is.

The URLs take two hops. The installer's composition-dynamic-controller
renders the Frontend composition's spec.config from a lookup of the platform
gateway's address, on every resync. Then the frontend's cdc renders
frontend-config-vars from it.

The gate waits by its OWN window: up to ENDPOINT_WAIT_S for the gateway's
address, then 2 x the installer cdc's EFFECTIVE resync + one poll, from the
latest reading with the address. The resync is read from the cluster by the
cdc's own semantics:
  - Go flag parsing;
  - env shadows envFrom;
  - an unparseable env value gives the compiled default;
  - a malformed flag means the cdc cannot start, and the gate refuses;
  - a failed read says UNREAD.

A gate that runs out decides its failure from the last poll's three
read-only readings: the gateway's address, what the installer RENDERED, and
the cdc's availability. It never uses a helm revision: the cdc writes one
only when a child changes (#1006 review F1).
"""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import origin_gate
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

ENV = origin_gate.ENV
CM = "installers-v0-3-468-configmap"


def _deploy(*containers, name="installers-v0-3-468-controller", extra=()):
    items = [{"metadata": {"name": "snowplow"}, "spec": {"template": {"spec": {"containers": [{}]}}}}]
    items.append({"metadata": {"name": name}, "spec": {"template": {"spec": {"containers": list(containers)}}}})
    items += [{"metadata": {"name": n}, "spec": {"template": {"spec": {"containers": [{}]}}}} for n in extra]
    return {"items": items}


def _c(args=(), env=(), env_from=(CM,), prefixes=None):
    return {"args": list(args), "env": list(env),
            "envFrom": [dict({"configMapRef": {"name": n}}, **({"prefix": prefixes[i]} if prefixes else {}))
                        for i, n in enumerate(env_from)]}


def _cms(**data):
    """ConfigMaps by name -> data; a DECOY carries the key but is referenced by nobody."""
    out = [{"metadata": {"name": n}, "data": d} for n, d in data.items()]
    out.append({"metadata": {"name": "decoy-configmap"}, "data": {ENV: "7s"}})
    return {"items": out}


def _r(deps, cms, err=""):
    return origin_gate.resync_interval(deps, cms, err)


# ─── the interval, by the cdc's own semantics ───────────────────────────────

def test_the_flag_wins_and_the_last_one_counts_in_every_go_flag_form():
    for args, want in ((["-resync-interval=90s"], 90), (["--resync-interval=2m"], 120),
                       (["-resync-interval", "45s"], 45), (["-resync-interval=10s", "-resync-interval=20s"], 20)):
        r = _r(_deploy(_c(args=args, env=[{"name": ENV, "value": "5m"}])), _cms(**{CM: {ENV: "60s"}}))
        assert r["seconds"] == want and r["refuse"] is None and "arg" in r["source"], (args, r)


def test_a_malformed_flag_means_the_cdc_cannot_start_and_the_gate_refuses():
    r = _r(_deploy(_c(args=["-resync-interval=soon"])), _cms(**{CM: {ENV: "60s"}}))
    assert r["seconds"] is None and "cdc exits at flag parsing" in r["refuse"]


def test_env_shadows_envfrom_and_its_last_entry_wins():
    r = _r(_deploy(_c(env=[{"name": ENV, "value": "1m"}, {"name": ENV, "value": "2m"}])), _cms(**{CM: {ENV: "60s"}}))
    assert r["seconds"] == 120 and f"env {ENV}=2m" in r["source"]


def test_an_unparseable_env_value_is_the_compiled_default_not_the_next_source():
    """#1006 F2: plumbing env.Duration returns its default for a value that does
    not parse, so an env "" or "soon" shadows the ConfigMap's 60s with 3m."""
    for bad in ("", "soon"):
        r = _r(_deploy(_c(env=[{"name": ENV, "value": bad}])), _cms(**{CM: {ENV: "60s"}}))
        assert r["seconds"] == 180 and "unparseable: the cdc's compiled default" in r["source"], bad
    r = _r(_deploy(_c()), _cms(**{CM: {ENV: "1 m"}}))
    assert r["seconds"] == 180 and "envFrom ConfigMap" in r["source"] and "unparseable" in r["source"]


def test_env_value_from_a_configmap_key_is_resolved():
    e = {"name": ENV, "valueFrom": {"configMapKeyRef": {"name": "tuning", "key": "resync"}}}
    r = _r(_deploy(_c(env=[e])), _cms(**{CM: {ENV: "60s"}, "tuning": {"resync": "4m"}}))
    assert r["seconds"] == 240 and "valueFrom ConfigMap tuning" in r["source"]


def test_the_envfrom_configmap_as_on_a_fresh_install_and_never_a_decoy():
    """#1006 F5 (M2): the ConfigMap is the one the Deployment references, not
    any ConfigMap that carries the key."""
    r = _r(_deploy(_c()), _cms(**{CM: {ENV: "60s"}}))
    assert r["seconds"] == 60 and f"envFrom ConfigMap {CM}" in r["source"]
    r = _r(_deploy(_c()), _cms(**{CM: {}}))
    assert r["seconds"] == 180 and "compiled default" in r["source"]      # the decoy's 7s is never read


def test_the_last_envfrom_source_wins_and_a_prefix_applies():
    r = _r(_deploy(_c(env_from=(CM, "late"))), _cms(**{CM: {ENV: "60s"}, "late": {ENV: "90s"}}))
    assert r["seconds"] == 90 and "envFrom ConfigMap late" in r["source"]
    r = _r(_deploy(_c(env_from=(CM,), prefixes=["COMPOSITION_"])),
           _cms(**{CM: {"CONTROLLER_RESYNC_INTERVAL": "75s", ENV: "1s"}}))
    assert r["seconds"] == 75 and "prefix COMPOSITION_" in r["source"]


def test_else_the_compiled_default():
    r = _r(_deploy(_c()), _cms(**{CM: {}}))
    assert r["seconds"] == 180 and "sets no -resync-interval" in r["source"]
    r = _r({"items": []}, {"items": []})
    assert r["seconds"] == 180 and "no installers-*-controller Deployment" in r["source"]


def test_a_read_that_failed_is_unread_never_absent():
    r = _r(None, None, "Unable to connect to the server")
    assert r["seconds"] == 180 and r["source"].startswith("UNREAD") and "Unable to connect" in r["source"]
    r = _r(_deploy(_c()), None, "configmaps is forbidden")
    assert r["source"].startswith("UNREAD") and "was not read" in r["source"] and "forbidden" in r["source"]


def test_more_than_one_installer_cdc_is_refused():
    r = _r(_deploy(_c(args=["-resync-interval=10m"]), extra=["installers-v0-3-467-controller"]), _cms())
    assert r["seconds"] is None and r["refuse"].startswith("ambiguous: 2")


def test_parse_duration():
    assert [origin_gate.parse_duration(x) for x in ("60s", "3m", "1m30s", "1h", "500ms", "", "3", "x", "-1s", None)] == \
        [60, 180, 90, 3600, 0.5, None, None, None, None, None]


# ─── the window ─────────────────────────────────────────────────────────────

def test_the_window_is_two_resyncs_from_the_gateways_address():
    assert origin_gate.deadline(100.0, None, 60, 5, 600) == 700                 # waiting on the address
    assert origin_gate.deadline(100.0, 400.0, 60, 5, 600) == 400 + 2 * 60 + 5   # from the address, not t0


# ─── the verdict, from the last poll ────────────────────────────────────────

R = {"seconds": 60, "source": "x"}


def _p(t, gw="address", rendered="not_same_origin", cdc=True, errors=None):
    return {"t": t, "gateway": gw, "rendered": rendered, "cdc_available": cdc, "errors": errors or {}}


@pytest.mark.parametrize("last,kind,words", [
    (_p(9, gw="unread", errors={"gateway": "TLS handshake timeout"}), "gateway_unread", "TLS handshake timeout"),
    (_p(9, gw="absent"), "gateway_absent", "does not exist"),
    (_p(9, gw="no_address"), "lb_pending", "waiting on GCP"),
    (_p(9, rendered="unread"), "rendered_unread", "could not be read"),
    (_p(9, rendered="same_origin"), "frontend_not_propagated", "the installer RENDERED the one origin"),
    (_p(9, cdc=None), "installer_cdc_unread", "could not be read"),
    (_p(9, cdc=False), "installer_cdc_down", "not Available"),
    (_p(9), "installer_defect", "INSTALLER DEFECT"),
    (_p(9, rendered="absent"), "installer_defect", "absent"),
])
def test_each_failure_has_its_own_name(last, kind, words):
    k, m = origin_gate.verdict([_p(0), last], R, "3 origins")
    assert k == kind and words in m and "3 origins" in m


def test_a_missing_reading_is_never_waiting_on_gcp():
    """#1006 F3: a Service that cannot be read, or does not exist, is not GCP."""
    for gw in ("unread", "absent"):
        assert origin_gate.verdict([_p(0, gw=gw)], R, "w")[0] != "lb_pending"


def test_the_last_reading_decides_and_an_address_that_came_back_restarts_the_count():
    """#1006 F7: an address that was there and then lost is no_address; one that
    came back counts from its return."""
    k, _ = origin_gate.verdict([_p(0), _p(60), _p(120, gw="no_address")], R, "w")
    assert k == "lb_pending"
    k, m = origin_gate.verdict([_p(0), _p(60, gw="no_address"), _p(100), _p(130)], R, "w")
    assert k == "installer_defect" and "for 30s" in m


def test_a_reading_is_built_from_what_kubectl_answered(tmp_path):
    svc, fe, dep = tmp_path / "svc.json", tmp_path / "fe.json", tmp_path / "dep.json"
    svc.write_text(json.dumps({"status": {"loadBalancer": {"ingress": [{"ip": "10.0.0.9"}]}}}))
    fe.write_text(json.dumps({"items": [{"spec": {"config": {
        k: "http://10.0.0.9" + p for k, p in (("SNOWPLOW_API_BASE_URL", "/content"), ("AUTHN_API_BASE_URL", "/auth"),
                                             ("EVENTS_API_BASE_URL", "/events"), ("EVENTS_PUSH_API_BASE_URL", "/events"))}}}]}))
    d = _deploy(_c())
    d["items"][1]["status"] = {"conditions": [{"type": "Available", "status": "True"}]}
    dep.write_text(json.dumps(d))
    r = origin_gate.reading(5, str(svc), 0, str(fe), 0, str(dep))
    assert (r["gateway"], r["rendered"], r["cdc_available"]) == ("address", "same_origin", True)
    r = origin_gate.reading(5, str(svc), 1, str(fe), 1, str(tmp_path / "none"),
                            {"gateway": 'Error from server (NotFound): services "x" not found', "frontends": "denied"})
    assert (r["gateway"], r["rendered"], r["cdc_available"]) == ("absent", "unread", None)
    assert origin_gate.reading(5, str(svc), 1, str(fe), 0, str(dep), {"gateway": "timeout"})["gateway"] == "unread"


# ─── the gate in run606's real sequence ─────────────────────────────────────

def _run(harness, **kw):  # noqa: F811
    env, state, ambient, tmp = harness
    env = dict(env, **kw)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    return p, p.stdout + p.stderr, tmp / "out"


@pytest.mark.parametrize("cdc,secs,source", [
    ("args:45s", 45, "installers-v0-3-422-controller arg -resync-interval=45s"),
    ("env:2m", 120, f"installers-v0-3-422-controller env {ENV}=2m"),
    ("configmap:60s", 60, f"installers-v0-3-422-controller envFrom ConfigMap installers-v0-3-422-configmap {ENV}=60s"),
    ("none", 180, "the cdc's compiled default (no installers-*-controller Deployment found)"),
])
def test_the_gate_reads_and_records_the_cdcs_resync_from_each_source(harness, cdc, secs, source):  # noqa: F811
    p, out, d = _run(harness, FAKE_CDC_RESYNC=cdc)
    assert p.returncode == 0, out[-3000:]
    assert f"the installer cdc resyncs every {secs}s ({source}); window: up to 600s" in out
    assert json.loads((d / "portal-origin-resync.json").read_text())["seconds"] == secs
    assert "WARNING: the cdc's resync" not in out


def test_a_resync_past_the_budget_term_warns_within_the_ttl_slack(harness):  # noqa: F811
    """#1006 F5 (M22) / F8: 10m reads past the budget's 180s term; the derived
    TTL's slack holds its window: said, and the run goes on."""
    p, out, d = _run(harness, FAKE_CDC_RESYNC="args:10m")
    assert p.returncode == 0, out[-3000:]
    assert "WARNING: the cdc's resync (600s) exceeds the run budget's 180s" in out


def test_a_resync_past_what_the_ttl_holds_stops_the_run(harness):  # noqa: F811
    """F8 decided: a window the TTL cannot hold would let the reaper take the
    cluster mid-run: refused at the gate, before data."""
    p, out, d = _run(harness, FAKE_CDC_RESYNC="args:5h")
    assert p.returncode != 0 and "exceeds the run budget's 180s term by more than the TTL's" in out
    assert "=== step env ===" not in out


@pytest.mark.parametrize("env,words", [
    ({"FAKE_CDC_COUNT": "2"}, "ambiguous: 2 installers-*-controller Deployments"),
    ({"FAKE_CDC_RESYNC": "args:soon"}, "the cdc exits at flag parsing"),
])
def test_a_resync_the_gate_cannot_honour_stops_it_at_once(harness, env, words):  # noqa: F811
    p, out, d = _run(harness, FAKE_PORTAL="split", **env)
    assert p.returncode != 0 and "the installer cdc's resync cannot be honoured" in out and words in out
    assert "portal not same-origin yet" not in out                     # no wait at all


@pytest.mark.parametrize("env,kind", [
    ({"FAKE_GATEWAY": "none", "ENDPOINT_WAIT_S": "30"}, "lb_pending: waiting on GCP"),
    ({"FAKE_GATEWAY": "absent", "ENDPOINT_WAIT_S": "30"}, "gateway_absent"),
    ({"FAKE_GATEWAY": "unread", "ENDPOINT_WAIT_S": "30"}, "gateway_unread"),
    ({"FAKE_RENDERED": "same"}, "frontend_not_propagated"),
    ({"FAKE_RENDERED": "split", "FAKE_CDC_AVAILABLE": "False"}, "installer_cdc_down"),
    ({"FAKE_RENDERED": "split"}, "installer_defect: INSTALLER DEFECT"),
])
def test_a_gate_that_runs_out_names_its_failure(harness, env, kind):  # noqa: F811
    """Each failure through the real sequence, on the gate's injected clock
    (10 s per reading; resync 20 s: a window of 4 readings after the address)."""
    p, out, d = _run(harness, FAKE_PORTAL="split", FAKE_CDC_RESYNC="args:20s", BENCH_GATE_CLOCK_STEP_S="10",
                     PLATFORM_POLL_S="0", **env)
    assert p.returncode != 0 and f"portal origin FAILED: {kind}" in out
    readings = [json.loads(x) for x in (d / "portal-origin-wait.jsonl").read_text().splitlines()]
    assert readings and all(r["t"] > 1_800_000_000 for r in readings)


def test_an_address_that_flaps_restarts_the_window(harness):  # noqa: F811
    """F7: address, gone, address, gone ...: the window restarts at every
    return, so the gate stops on the LB bound, saying no address."""
    p, out, d = _run(harness, FAKE_PORTAL="split", FAKE_GATEWAY="flap", FAKE_CDC_RESYNC="args:20s",
                     BENCH_GATE_CLOCK_STEP_S="10", PLATFORM_POLL_S="0", ENDPOINT_WAIT_S="90")
    assert p.returncode != 0 and "portal origin FAILED: lb_pending" in out
    gws = [json.loads(x)["gateway"] for x in (d / "portal-origin-wait.jsonl").read_text().splitlines()]
    assert gws[:4] == ["address", "no_address", "address", "no_address"] and len(gws) >= 8


def test_a_squeezed_platform_budget_still_leaves_the_gate_its_window(harness):  # noqa: F811
    """#1006 F4: on the gate's injected clock, not the wall. The platform's
    earlier waits used their whole budget (PLATFORM_TIMEOUT_S = DEPLOY_WAIT_S
    = 1), yet the gate gets its own 2 x 20 s after the address (10 s per
    reading): the URLs frontend-config-vars carries from its 4th read are
    seen."""
    p, out, d = _run(harness, FAKE_PORTAL="after:3", FAKE_CDC_RESYNC="args:20s", BENCH_GATE_CLOCK_STEP_S="10",
                     PLATFORM_POLL_S="0", PLATFORM_TIMEOUT_S="1", DEPLOY_WAIT_S="1")
    assert p.returncode == 0, out[-3000:]
    assert out.count("portal not same-origin yet") == 3 and "portal origin (config.json): http://10.0.0.9" in out
