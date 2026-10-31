"""#1026: run 19 attempt 3 waited 45 min at the portal origin gate. The
gateway's LoadBalancer Service never got an address: "Quota
'STATIC_ADDRESSES' exceeded. Limit: 8.0 in region europe-west4". All 8
regional addresses were RESERVED, unused, by LoadBalancer Services of bench
clusters deleted before their Services.

  1. ORDER: before the cluster delete, every LoadBalancer Service is deleted
     (no new one may be created meanwhile) and the teardown waits, bounded,
     until the Services are gone and GCE lists none of their forwarding rules
     or addresses;
  2. OWN NAMES ONLY: the address names are recorded from this cluster's own
     Services ("a" + UID without dashes, 32 chars) while it lives; after the
     delete only an address on that list is deleted — never one matched by
     description or pattern, whatever gcloud's filter returns;
  3. USERS: an address with users is never deleted (said loudly);
  4. PREFLIGHT: gke-up (and the pair, for both runs) refuses a run whose
     LoadBalancer Services — derived from what it installs, bench/lb_need.py —
     do not fit the region's STATIC_ADDRESSES headroom;
  5. the teardown record carries leaked_addresses next to the disks.

No cloud call: the fake gcloud / kubectl of tests/test_gke_pvc_disks.py play
GKE. tests/fixtures/installer_0_3_468_exposure_run19.json is installer
0.3.468's exposure-relevant pins and values as run 19 pulled them, with the
LoadBalancer Services run 19 attempt 2's cluster really had.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

from bench import gke, lb_need
from tests.test_gke_pvc_disks import NAME, _calls, _first, _ticks, fakes  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

FIX = Path(__file__).resolve().parent / "fixtures"
R = "europe-west4"
REGION_URL = f"https://www.googleapis.com/compute/v1/projects/{gke.DEFAULT_PROJECT}/regions/{R}"
# run 19 attempt 2's event (diagnostics/events.json): GKE's own name for the gateway's address
RUN19_EVENT = ("Error syncing load balancer: failed to ensure load balancer: failed to ensure a static IP for "
               "load balancer (a7497f861f97649d0b468ac1a4bc6f2d(krateo-system/krateo-agent-gateway-platform)): "
               "error creating gce static IP address: googleapi: Error 403: QUOTA_EXCEEDED - Quota "
               "'STATIC_ADDRESSES' exceeded.  Limit: 8.0 in region europe-west4.")

UID_GW = "7497f861-f976-49d0-b468-ac1a4bc6f2d3"           # consistent with the event's name
UID_FE = "0c2f7a10-5b8e-4d6a-9f31-27c4e8a1b9d0"
ADDR_GW, ADDR_FE = "a7497f861f97649d0b468ac1a4bc6f2d", "a0c2f7a105b8e4d6a9f3127c4e8a1b9d"


def _svc(ns, name, uid, typ="LoadBalancer", **spec):
    return {"metadata": {"namespace": ns, "name": name, "uid": uid},
            "spec": {"type": typ, "ports": [{"port": 80}], **spec}}


LB_SVCS = [_svc("krateo-system", "krateo-agent-gateway-platform", UID_GW),
           _svc("krateo-system", "frontend", UID_FE),
           _svc("krateo-system", "snowplow", "11111111-2222-4333-8444-555555555555", typ="ClusterIP")]


def _addr(name, *, users=None, region=REGION_URL, service="krateo-system/frontend"):
    return {"name": name, "region": region, "status": "IN_USE" if users else "RESERVED", "addressType": "EXTERNAL",
            "description": json.dumps({"kubernetes.io/service-name": service}),
            **({"users": users} if users else {})}


FWD_USER = f"{REGION_URL}/forwardingRules/{ADDR_GW}"
# what the region lists once the cluster went under its Services (and what else is there)
ADDRESSES = [
    _addr(ADDR_FE),                                                     # ours, unused: deleted
    _addr(ADDR_GW, users=[FWD_USER], service="krateo-system/krateo-agent-gateway-platform"),  # ours, in use
    # another cluster's leak: the SAME description, RESERVED, unused — not on our list
    _addr("a9e48242a1552458b9711863c4c0b474", service="krateo-system/frontend"),
    # a name sharing our prefix (a pattern would take it)
    _addr(ADDR_FE[:31] + "f"),
    # our name in another region: not this cluster's
    _addr(ADDR_FE, region=REGION_URL.replace(R, "europe-west3")),
]


@pytest.fixture
def lbfakes(fakes, monkeypatch):  # noqa: F811
    (fakes / "addresses.json").write_text(json.dumps(ADDRESSES))
    (fakes / "fwd.json").write_text(json.dumps([{"name": ADDR_GW, "region": REGION_URL},
                                                {"name": ADDR_FE, "region": REGION_URL}]))
    monkeypatch.setenv("FAKE_LB_SVCS", json.dumps(LB_SVCS))
    monkeypatch.setenv("FAKE_ADDRESSES", str(fakes / "addresses.json"))
    monkeypatch.setenv("FAKE_FWD_RULES", str(fakes / "fwd.json"))
    return fakes


def _deleted_addresses(c):
    out = []
    for x in c:
        if x[:3] == ["gcloud", "compute", "addresses"] and x[3] == "delete":
            out += x[4:x.index("--region")]
    return out


# ─── the naming rule ────────────────────────────────────────────────────────

def test_the_address_name_is_gkes_default_load_balancer_name():
    """cloudprovider.DefaultLoadBalancerName: "a" + UID without dashes, cut to
    32. Run 19 attempt 2's SyncLoadBalancerFailed names the gateway's static
    IP a7497f86…; any UID that starts like it gives exactly that name (the
    32nd hex digit is cut)."""
    assert ADDR_GW in RUN19_EVENT
    for last in "0123456789abcdef":
        assert gke.lb_address_name(UID_GW[:-1] + last) == ADDR_GW
    assert len(gke.lb_address_name(UID_FE)) == 32 and gke.lb_address_name(UID_FE).startswith("a0c2f7a10")
    svcs = gke.lb_services({"items": LB_SVCS + [
        dict(_svc("x", "rbs", "22222222-2222-4222-8222-222222222222"),
             metadata={"namespace": "x", "name": "rbs", "uid": "22222222-2222-4222-8222-222222222222",
                       "annotations": {"cloud.google.com/l4-rbs": "enabled"}}),
        _svc("x", "classed", "33333333-3333-4333-8333-333333333333", loadBalancerClass="example.com/lb")]})
    by = {s["service"]: s["address"] for s in svcs}
    assert by["krateo-system/krateo-agent-gateway-platform"] == ADDR_GW
    assert "krateo-system/snowplow" not in by                       # ClusterIP: no address
    assert by["x/classed"] is None                                  # another controller names it
    assert by["x/rbs"] is None                                      # the RBS NetLB names it k8s2-...


# ─── 1. ORDER ───────────────────────────────────────────────────────────────

def test_the_load_balancers_are_released_before_the_cluster_goes(lbfakes, monkeypatch):
    monkeypatch.setenv("FAKE_FWD_GONE_AFTER", "2")         # the controller takes two polls to release them
    (lbfakes / "addresses.json").write_text("[]")          # released: nothing is left after the delete
    rec, log = {}, []
    assert gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=600, clock=_ticks(1),
                        sleep=lambda s: None, record=rec, lb_need=2) == []
    c = _calls(lbfakes)
    i_guard = _first(c, ["kubectl", "apply", "--server-side"])
    i_probe = _first(c, ["kubectl", "create", "--dry-run=server"])
    i_list = _first(c, ["kubectl", "get", "svc", "--all-namespaces"])
    i_del_gw = _first(c, ["kubectl", "delete", "svc", "-n", "krateo-system", "krateo-agent-gateway-platform"])
    i_del_fe = _first(c, ["kubectl", "delete", "svc", "-n", "krateo-system", "frontend"])
    fwd = [i for i, x in enumerate(c) if x[:4] == ["gcloud", "compute", "forwarding-rules", "list"]]
    i_cluster = _first(c, ["gcloud", "container", "clusters", "delete"])
    # nothing may make a new LoadBalancer Service, THEN the record, THEN the deletes
    assert i_guard < i_probe < i_list < min(i_del_gw, i_del_fe)
    # GCE was asked until it listed none of their forwarding rules — all before the cluster delete
    assert len(fwd) == 3 and max(i_del_gw, i_del_fe) < fwd[0] and fwd[-1] < i_cluster
    pre = [i for i, x in enumerate(c) if x[:4] == ["gcloud", "compute", "addresses", "list"] and i < i_cluster]
    assert pre and pre[-1] > fwd[-1]                                  # and none of their addresses either
    assert not any(x[:3] == ["kubectl", "delete", "svc"] and "snowplow" in x for x in c)   # ClusterIP: untouched
    assert any("the 2 LoadBalancer Service(s) are gone and GCE lists none of their forwarding rules or addresses"
               in m for m in log)
    assert rec["lb_release"] == "released"
    assert [s["address"] for s in rec["lb_services"]] == [ADDR_FE, ADDR_GW]
    assert rec["lb_need_check"].startswith("OK: 2 LoadBalancer Service(s)")
    applied = (lbfakes / "state" / "lb_guard_applied").read_text()
    assert 'operations: ["CREATE", "UPDATE"]' in applied and 'resources: ["services"]' in applied
    assert "oldObject.spec.type == 'LoadBalancer'" in applied     # the cleanup finalizer's UPDATE still passes
    # after the delete: the recorded names, verified gone
    assert rec["leaked_addresses"] == [] and rec["address_problems"] == []
    assert any(f"none of the 2 recorded address(es) of {NAME} is left" in m for m in log)


def test_a_release_that_never_completes_is_bounded_and_the_backstop_still_runs(lbfakes, monkeypatch):
    monkeypatch.setenv("FAKE_SVC_STUCK", "1")              # the Services never go
    rec, log = {}, []
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=log.append, budget_s=60, clock=_ticks(),
                 sleep=lambda s: None, record=rec)
    c = _calls(lbfakes)
    assert rec["lb_release"].startswith("not released: krateo-system/frontend")
    assert any("still there at the budget's end" in m for m in log)
    assert _first(c, ["gcloud", "container", "clusters", "delete"]) < _first(c, ["gcloud", "compute", "addresses",
                                                                                 "delete"])


def test_no_load_balancer_is_deleted_unless_new_ones_are_refused(lbfakes, monkeypatch, capsys):
    """The agentgateway controller re-creates its Gateway's Service (a new UID,
    a new address name). Without the guard in force nothing is deleted; the
    recorded names still go to the backstop."""
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    rec = {}
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60, clock=_ticks(),
                 sleep=lambda s: None, record=rec)
    c = _calls(lbfakes)
    assert not any(x[:3] == ["kubectl", "delete", "svc"] for x in c)
    assert "LB release REFUSED" in capsys.readouterr().err
    assert rec["lb_release"].startswith("refused") and len(rec["lb_services"]) == 2


def test_the_guard_keeps_a_controller_from_re_creating_its_service(lbfakes, monkeypatch):
    new = _svc("krateo-system", "krateo-agent-gateway-platform", "44444444-4444-4444-8444-444444444444")
    monkeypatch.setenv("FAKE_LB_RECREATE", json.dumps(new))
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=600, clock=_ticks(1),
                 sleep=lambda s: None)
    assert not (lbfakes / "state" / "svcs_recreated").exists()
    # the reproduction: the old order (no guard) lets it come back under a new name
    (lbfakes / "state" / "lb_guard_applied").unlink()
    (lbfakes / "state" / "svcs_deleted").unlink()
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    monkeypatch.setattr(gke, "_block_new", lambda k, **kw: True)
    gke.release_lb_services(gke.default_spec(NAME), {"endpoint": "127.0.0.1"}, log=lambda m: None,
                            deadline=0, clock=lambda: 1, sleep=lambda s: None)
    assert (lbfakes / "state" / "svcs_recreated").exists()


# ─── 2. OWN NAMES ONLY, 3. USERS ────────────────────────────────────────────

@pytest.mark.parametrize("loose", ["", "1"])
def test_the_sweep_deletes_only_this_clusters_recorded_unused_addresses(lbfakes, monkeypatch, capsys, loose):
    """The cluster went under its Services (the release was refused). Of the
    region's RESERVED addresses only the recorded, unused one goes: not
    another cluster's with the same description, not a name sharing our
    prefix, not our name in another region — even when gcloud's filter
    returns the whole region (`loose`)."""
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    monkeypatch.setenv("FAKE_LOOSE_FILTER", loose)
    rec = {}
    problems = gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60,
                            clock=_ticks(), sleep=lambda s: None, record=rec)
    c = _calls(lbfakes)
    assert _deleted_addresses(c) == [ADDR_FE]
    d = next(x for x in c if x[:4] == ["gcloud", "compute", "addresses", "delete"])
    assert d[d.index("--region") + 1] == R
    lst = [x for x in c if x[:4] == ["gcloud", "compute", "addresses", "list"]][-1]
    assert lst[lst.index("--filter") + 1] == f"name=({ADDR_FE} {ADDR_GW})" and "description" not in " ".join(lst)
    assert {(a["name"], a["deleted"]) for a in rec["leaked_addresses"]} == {(ADDR_FE, True), (ADDR_GW, False)}
    # 3: the one with users is said, never deleted: the backstop failed
    assert problems and any(f"{ADDR_GW} (krateo-system/krateo-agent-gateway-platform) is still used by" in p
                            for p in problems)
    assert f"{gke.ADDRESS_BACKSTOP_FAILED} for {NAME}" in capsys.readouterr().err


def test_an_address_with_users_is_never_deleted(lbfakes, monkeypatch):
    (lbfakes / "addresses.json").write_text(json.dumps([_addr(ADDR_GW, users=[FWD_USER]),
                                                        _addr(ADDR_FE, users=[FWD_USER + "x"])]))
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    rec = {}
    problems = gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60,
                            clock=_ticks(), sleep=lambda s: None, record=rec)
    c = _calls(lbfakes)
    assert not any(x[:4] == ["gcloud", "compute", "addresses", "delete"] for x in c)
    assert len([p for p in problems if "is still used by" in p]) == 2
    assert all(not a["deleted"] and a["users"] for a in rec["leaked_addresses"])


@pytest.mark.parametrize("why, env, want", [
    ("the cluster's Services were never read", {"FAKE_SERVER": "34.90.12.7"}, "were never read"),
    ("the addresses cannot be listed", {"FAKE_ADDR_LIST_RC": "1", "FAKE_LB_GUARD_IGNORED": "1"},
     "listing snowplow-bench-r16x's addresses failed"),
    ("a delete leaves the address", {"FAKE_ADDR_DELETE_NOOP": "1", "FAKE_LB_GUARD_IGNORED": "1"},
     "left them there"),
])
def test_an_address_backstop_that_could_not_do_its_job_is_loud(lbfakes, monkeypatch, capsys, why, env, want):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    rec = {}
    problems = gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60,
                            clock=_ticks(), sleep=lambda s: None, record=rec)
    assert any(want in p for p in problems), (why, problems)
    assert f"{gke.ADDRESS_BACKSTOP_FAILED} for {NAME}" in capsys.readouterr().err
    assert rec["address_problems"]


# ─── 4. PREFLIGHT ───────────────────────────────────────────────────────────

def test_the_static_address_fit_reads_the_regions_quota():
    q = json.loads((FIX / "region_europe_west4_quotas_20261025.json").read_text())
    ok, msg = gke.address_quota_fit(q, 5)
    assert ok and "STATIC_ADDRESSES in europe-west4: 3 of 8 used, 5 free; the run creates 5" in msg
    ok, msg = gke.address_quota_fit(q, 6)
    assert not ok
    # run 19 attempt 3's region: 8 of 8 RESERVED by leaks
    full = {"name": R, "quotas": [{"metric": "STATIC_ADDRESSES", "limit": 8.0, "usage": 8.0}]}
    assert not gke.address_quota_fit(full, 1)[0]
    assert not gke.address_quota_fit({"name": R, "quotas": []}, 0)[0]          # unread: never fits


def test_gke_up_refuses_a_run_whose_load_balancers_do_not_fit(fakes, capsys):  # noqa: F811
    q = json.loads((fakes / "quotas.json").read_text())
    next(x for x in q["quotas"] if x["metric"] == "STATIC_ADDRESSES")["usage"] = 8.0
    (fakes / "quotas.json").write_text(json.dumps(q))
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    err = capsys.readouterr().err
    assert "refusing: STATIC_ADDRESSES in europe-west4: 8 of 8 used, 0 free; the run creates 1" in err
    assert "nothing was created" in err
    assert not any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))


def test_gke_up_refuses_when_the_need_is_unknown(fakes, capsys):  # noqa: F811
    assert gke.main(["gke-up", "--name", NAME, "--yes"]) == 2
    assert "was not given (--address-need, from bench.lb_need)" in capsys.readouterr().err
    assert not any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))


def test_the_pair_refuses_when_both_runs_load_balancers_do_not_fit(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    from tests.test_run15_customer_latency import NA, NB
    (fake / f"{NA}.lbneed").write_text("2")
    (fake / f"{NB}.lbneed").write_text("2")
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_STATIC_USAGE="5")        # 3 free, the pair needs 4
    assert p.returncode == 2 and "refusing: STATIC_ADDRESSES in europe-west4: 5 of 8 used, 3 free; the run " \
                                 "creates 4" in p.stderr, p.stderr[-1500:]
    assert "DRY: both runs would start now" not in p.stderr
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_STATIC_USAGE="4")
    assert p.returncode == 0 and "4 free; the run creates 4" in p.stderr, p.stderr[-1500:]
    # a dry pass that did not derive its number: unknown, refused
    (fake / f"{NB}.lbneed").write_text("none")
    p = _launch(repo, env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 2 and "run B's dry pass did not derive its LoadBalancer Services" in p.stderr


def _chart(tmp: Path, fix: dict) -> Path:
    root = tmp / "installer-pulled" / "installer"
    (root / "files").mkdir(parents=True)
    (root / "templates").mkdir()
    (root / "Chart.yaml").write_text(yaml.safe_dump({"apiVersion": "v2", "name": "installer",
                                                     "version": fix["chart_version"]}))
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": fix["pins"]}))
    (root / "values.yaml").write_text(yaml.safe_dump(fix["chart_values"]))
    (root / "templates" / "_helpers.tpl").write_text(fix["core_features_define"] + "\n")
    return tmp / "installer-pulled"


RUN19 = json.loads((FIX / "installer_0_3_468_exposure_run19.json").read_text())


def _values(tmp: Path, **over) -> Path:
    v = json.loads(json.dumps(RUN19["run_values"]))
    for k, x in over.items():
        v[k] = x if not isinstance(x, dict) else {**(v.get(k) or {}), **x}
    p = tmp / f"values-{len(list(tmp.glob('values-*')))}.yaml"
    p.write_text(yaml.safe_dump(v))
    return p


def test_run19s_install_creates_the_one_load_balancer_its_cluster_had(tmp_path):
    """The derivation against the cluster: installer 0.3.468 with run 19's
    values made exactly krateo-agent-gateway-platform (diagnostics)."""
    rows = lb_need.planned(str(_chart(tmp_path, RUN19)), str(_values(tmp_path)))
    assert [f"krateo-system/{r['service']}" for r in rows] == RUN19["observed_lb_services"] == [
        "krateo-system/krateo-agent-gateway-platform"]


@pytest.mark.parametrize("over, want", [
    # the exposure of runs <= 17: one per exposed portal component — the issue's leaked set
    ({"exposure": {"type": "LoadBalancer"}, "features": {"agentGateway": False}},
     ["authn", "snowplow", "frontend", "sse-proxy"]),
    ({"exposure": {"type": "LoadBalancer", "internalDataPlane": False}},
     ["authn", "snowplow", "frontend", "sse-proxy", "krateo-agent-gateway"]),
    ({"exposure": {"type": "NodePort", "internalDataPlane": False}}, ["krateo-agent-gateway"]),
    ({"exposure": {"type": "Gateway"}, "features": {"agentGateway": False}}, []),
    # componentValues win: a flip undone, a LoadBalancer added
    ({"exposure": {"type": "LoadBalancer"}, "features": {"agentGateway": False},
      "componentValues": {"frontend": {"service": {"type": "ClusterIP"}}}}, ["authn", "snowplow", "sse-proxy"]),
    ({"componentValues": {"krateo-observability": {"service": {"type": "LoadBalancer"}}}},
     ["krateo-clickstack", "krateo-agent-gateway-platform"]),
    # a feature off installs nothing of it
    ({"exposure": {"type": "LoadBalancer"}, "features": {"agentGateway": False, "portal": False}}, []),
])
def test_the_need_follows_what_the_run_installs(tmp_path, over, want):
    rows = lb_need.planned(str(_chart(tmp_path, RUN19)), str(_values(tmp_path, **over)))
    assert [r["service"] for r in rows] == want


def test_the_cli_prints_the_need_and_refuses_an_unreadable_chart(tmp_path, capsys):
    out = tmp_path / "lb-need.json"
    assert lb_need.main(["--installer-pulled", str(_chart(tmp_path, RUN19)), "--values", str(_values(tmp_path)),
                         "--out", str(out)]) == 0
    io = capsys.readouterr()
    assert io.out.strip() == "1" and json.loads(out.read_text())["need"] == 1
    assert "LoadBalancer Service: krateo-agent-gateway-platform" in io.err
    assert lb_need.main(["--installer-pulled", str(tmp_path / "nothing"), "--values", str(out)]) == 2
    assert "could not be derived" in capsys.readouterr().err


# ─── 5. run606: the derivation reaches gke-up; the teardown record ─────────

def _gcloud(state):
    return [c["argv"] for c in (json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines())
            if c["tool"] == "gcloud"]


def test_run606_passes_the_derived_need_and_keeps_the_teardown_record(harness):  # noqa: F811
    env, state, _, tmp = harness
    lbs = [_svc("krateo-system", "krateo-agent-gateway-platform", UID_GW),
           _svc("krateo-system", "frontend", UID_FE)]
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_LB_SVCS=json.dumps(lbs)),
                       capture_output=True, text=True, timeout=600)
    assert p.returncode == 0, p.stderr[-3000:]
    out = tmp / "out"
    assert json.loads((out / "lb-need.json").read_text())["need"] == 1
    assert "this install creates 1 LoadBalancer Service(s)" in p.stderr
    gcl = _gcloud(state)
    up = next(a for a in gcl if a[:3] == ["container", "clusters", "create"])
    assert up                                                            # it was created ...
    rec = json.loads((out / "teardown.json").read_text())
    assert rec["lb_need"] == 1 and rec["lb_release"] == "released"
    assert rec["leaked_addresses"] == [] and rec["address_problems"] == [] and rec["deleted_disks"] == []
    # the cluster had 2 where the preflight derived 1: said, in the record and the log
    assert rec["lb_need_check"].startswith("UNDER-COUNTED: 2 LoadBalancer Service(s)")
    assert "LoadBalancer Services against the preflight: UNDER-COUNTED" in p.stderr
    assert "teardown record: LoadBalancer Services 2 (release: released); leaked_addresses none" in p.stderr
    # the Services went before the cluster
    svc_del = [c for c in (json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines())
               if c["tool"] == "kubectl" and "delete" in c["argv"] and "svc" in c["argv"]]
    assert len(svc_del) == 2


def test_run606_refuses_before_any_cluster_when_its_load_balancers_do_not_fit(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_STATIC_USAGE="8"),
                       capture_output=True, text=True, timeout=600)
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    line = next(ln for ln in log.splitlines() if "up FAILED: gke-up exited 2" in ln)
    assert "STATIC_ADDRESSES in europe-west4: 8 of 8 used, 0 free; the run creates 1" in line
    assert not any(a[:3] == ["container", "clusters", "create"] for a in _gcloud(state))


def test_gke_down_writes_the_teardown_record(lbfakes, monkeypatch):
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    out = lbfakes / "teardown.json"
    assert gke.main(["gke-down", "--name", NAME, "--budget-s", "1", "--yes", "--record", str(out),
                     "--lb-need", "2"]) == gke.EXIT_BACKSTOP_FAILED
    rec = json.loads(out.read_text())
    assert rec["outcome"] == "deleted, backstop failed" and rec["lb_need"] == 2
    assert {a["name"] for a in rec["leaked_addresses"]} == {ADDR_FE, ADDR_GW}
    assert "deleted_disks" in rec and "disk_problems" in rec


def test_the_dry_run_teardown_names_the_release_and_the_sweep(monkeypatch, capsys):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("ran")))
    gke.teardown(gke.default_spec(NAME), dry_run=True)
    out = capsys.readouterr().out
    assert out.index("delete svc") < out.index("DRY-RUN: gcloud container clusters delete") < out.index(
        "DRY-RUN: gcloud compute addresses list")


# ─── #1037 review: the rules the PR states, each with its own arm ──────────

def test_the_record_never_says_deleted_for_an_address_still_listed(lbfakes, monkeypatch):
    """F1 (R3): `deleted` comes from the list AFTER the delete, never from the
    delete call: a delete that leaves the address, or a verifying list that
    fails, both record deleted=False (the run log prints it as is)."""
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    monkeypatch.setenv("FAKE_ADDR_DELETE_NOOP", "1")                # rc 0, the address stays
    rec = {}
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60, clock=_ticks(),
                 sleep=lambda s: None, record=rec)
    assert _deleted_addresses(_calls(lbfakes)) == [ADDR_FE]          # asked, rc 0 — and still listed
    assert {a["name"]: a["deleted"] for a in rec["leaked_addresses"]}[ADDR_FE] is False
    assert any("left them there" in p for p in rec["address_problems"])


def test_the_record_never_says_deleted_when_the_verifying_list_fails(lbfakes, monkeypatch):
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    monkeypatch.setenv("FAKE_ADDR_RELIST_RC", "1")                  # the delete went, the re-list failed
    rec = {}
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60, clock=_ticks(),
                 sleep=lambda s: None, record=rec)
    assert _deleted_addresses(_calls(lbfakes)) == [ADDR_FE]
    assert {a["name"]: a["deleted"] for a in rec["leaked_addresses"]}[ADDR_FE] is False
    assert any("whether they are gone is unknown" in p for p in rec["address_problems"])


class _Clock:
    """A clock that moves `step` per read; `at` = the reading when each argv ran."""
    def __init__(self, step):
        self.t, self.step, self.at = 0.0, step, []

    def __call__(self):
        self.t += self.step
        return self.t


def test_the_load_balancer_release_stops_at_a_quarter_of_the_budget(lbfakes, monkeypatch):
    """F2 (R10): the Services never go; the release gives up at start + B/4,
    so the PVC release starts by then and the cluster delete by B/2 — the
    gone-poll and both backstops keep the rest."""
    monkeypatch.setenv("FAKE_SVC_STUCK", "1")
    clock, B = _Clock(5), 400.0
    real = gke.execute

    def spy(argv, **kw):
        clock.at.append((argv, clock.t))
        return real(argv, **kw)
    monkeypatch.setattr(gke, "execute", spy)
    rec = {}
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=B, clock=clock,
                 sleep=lambda s: None, record=rec)
    t_pv = next(t for argv, t in clock.at if argv[-4:] == ["get", "pv", "-o", "json"])
    t_del = next(t for argv, t in clock.at if argv[:4] == ["gcloud", "container", "clusters", "delete"])
    poll = 4 * clock.step                       # one release poll reads the clock at most a few times
    assert rec["lb_release"].startswith("not released")
    assert t_pv <= B / 4 + poll, (t_pv, B / 4)
    assert t_del <= B / 2 + poll, (t_del, B / 2)


def test_a_gce_list_that_does_not_answer_is_never_released(lbfakes, monkeypatch):
    """F3 (R4): the Services are gone but the forwarding-rule list fails:
    unknown is not "released" — the release keeps polling until its deadline
    and says so."""
    (lbfakes / "addresses.json").write_text("[]")
    monkeypatch.setenv("FAKE_FWD_LIST_RC", "1")
    rec = {}
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=400, clock=_ticks(1),
                 sleep=lambda s: None, record=rec)
    c = _calls(lbfakes)
    i_cluster = _first(c, ["gcloud", "container", "clusters", "delete"])
    fwd = [i for i, x in enumerate(c) if x[:4] == ["gcloud", "compute", "forwarding-rules", "list"]]
    assert rec["lb_release"] == "not released: GCE unlisted"
    assert len(fwd) > 1 and fwd[-1] < i_cluster                     # it kept asking, before the delete


def test_the_address_sweep_waits_for_the_cluster_to_be_gone(lbfakes, monkeypatch):
    """F4 (R9): a cluster still being deleted when the budget ends: no address
    is deleted (its controller may still own them); the record says it was
    skipped and how to list them once the cluster is gone."""
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    monkeypatch.setenv("FAKE_NEVER_GONE", "1")
    rec = {}
    problems = gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=60,
                            clock=_ticks(), sleep=lambda s: None, record=rec)
    c = _calls(lbfakes)
    assert not any(x[:4] == ["gcloud", "compute", "addresses", "delete"] for x in c)
    assert not any(x[:4] == ["gcloud", "compute", "addresses", "list"] for x in c)
    want = (f"once it is gone list them with gcloud compute addresses list --project {gke.DEFAULT_PROJECT} "
            f"--regions {R} --filter name=({ADDR_FE} {ADDR_GW})")
    assert any(p.startswith("skipped:") and want in p for p in rec["address_problems"]), rec["address_problems"]
    assert rec["leaked_addresses"] is None and problems


def test_a_hung_gcloud_call_is_bounded(lbfakes, monkeypatch):
    """N1: every gcloud call this teardown adds carries a timeout (what is
    left until its deadline, at least GCLOUD_CALL_MIN_S); a hung list is a
    failed list, never an empty one."""
    monkeypatch.setenv("FAKE_LB_GUARD_IGNORED", "1")
    monkeypatch.setenv("FAKE_GCE_LIST_SLEEP", "20")
    monkeypatch.setattr(gke, "GCLOUD_CALL_MIN_S", 0.5)
    import time as _t
    t0 = _t.monotonic()
    rec = {}
    gke.teardown(gke.default_spec(NAME), dry_run=False, log=lambda m: None, budget_s=1, sleep=lambda s: None,
                 record=rec)
    assert _t.monotonic() - t0 < 15
    assert any(f"failed (rc={gke.GCLOUD_TIMED_OUT})" in p for p in rec["address_problems"]), rec


def test_the_exposure_type_defaults_to_the_installers_load_balancer(tmp_path):
    """R16: no exposure.type in the chart's values nor the run's: the
    installer's default (values.yaml: LoadBalancer) applies, so every exposed
    component counts."""
    fix = json.loads(json.dumps(RUN19))
    fix["chart_values"]["exposure"] = {}
    v = json.loads(json.dumps(RUN19["run_values"]))
    v.pop("exposure")
    v["features"]["agentGateway"] = False
    vf = tmp_path / "v.yaml"
    vf.write_text(yaml.safe_dump(v))
    rows = lb_need.planned(str(_chart(tmp_path, fix)), str(vf))
    assert [r["service"] for r in rows] == ["authn", "snowplow", "frontend", "sse-proxy"]


def test_gke_down_keeps_the_record_when_the_teardown_raises(lbfakes, monkeypatch):
    """R19: what the teardown learnt before it failed (here: the LoadBalancer
    Services and their address names) is in the record, with the failure."""
    def boom(*a, **kw):
        raise RuntimeError("the PVC release fell over")
    monkeypatch.setattr(gke, "release_pvc_disks", boom)
    out = lbfakes / "teardown.json"
    assert gke.main(["gke-down", "--name", NAME, "--budget-s", "1", "--yes", "--record", str(out)]) == 1
    rec = json.loads(out.read_text())
    assert rec["outcome"] == "failed: RuntimeError"
    assert sorted(s["address"] for s in rec["lb_services"]) == [ADDR_FE, ADDR_GW]
