"""#606 harness: temporary GKE lifecycle, 057 refusal, RBAC model, load
driver, freshness probe, refresh economics, verdict, loadgen Job, run606.sh.

Hermetic: no gcloud, no kubectl, no cluster. The only network use is a local
http.server on 127.0.0.1 for the HttpClient round-trip arm.
"""

from __future__ import annotations

import gzip
import http.server
import json
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path

import pytest
import yaml

from bench import cluster, gke, load606, load606_job, rbac_model

BENCH_DIR = Path(__file__).resolve().parent.parent
NOW = 1_800_000_000


# ─── gke: spec, commands, labels ────────────────────────────────────────────


def _spec(**kw):
    return gke.default_spec("snowplow-bench-1014-1200", now=NOW, owner="Diego B",
                            kubeconfig_dir="/tmp/kc", **kw)


def test_spec_labels_carry_ttl_owner_and_bench_identity():
    s = _spec(ttl_hours=6)
    labels = s.labels()
    assert labels["ttl-expires"] == str(NOW + 6 * 3600)
    assert labels["owner"] == "diego-b"
    for k, v in gke.BENCH_LABELS.items():
        assert labels[k] == v
    assert s.kubeconfig == "/tmp/kc/snowplow-bench-1014-1200.kubeconfig"


def test_create_commands_shape():
    s = _spec()
    cmds = gke.create_commands(s)
    create = cmds[0]
    assert create[:5] == ["gcloud", "container", "clusters", "create", s.name]
    assert create[create.index("--cluster-version") + 1] == "1.36"
    labels = create[create.index("--labels") + 1]
    assert f"ttl-expires={s.ttl_expires}" in labels
    assert "purpose=snowplow-bench" in labels
    pools = {c[4]: c for c in cmds if c[2] == "node-pools"}
    assert set(pools) == {"snowplow", "loadgen"}
    assert "dedicated=snowplow:NoSchedule" in pools["snowplow"]
    assert "dedicated=loadgen:NoSchedule" in pools["loadgen"]
    # Quota shape (europe-west4, read 2026-10-14): E2_CPUS=24 and ~133 GB SSD
    # free would reject e2 pools and pd-balanced disks; 057 is on REGULAR.
    flat = [a for c in cmds for a in c]
    assert not any(a.startswith("e2-") for a in flat)
    assert "pd-balanced" not in flat and "pd-standard" in flat
    assert create[create.index("--release-channel") + 1] == "regular"
    # credentials are the LAST step (dedicated kubeconfig set by the caller)
    assert cmds[-1][:4] == ["gcloud", "container", "clusters", "get-credentials"]


@pytest.mark.parametrize("name", ["krateo-057", "krateo-installer-release",
                                  "krateo-installer-test", "cluster-1",
                                  "snowplow-bench-UPPER"])
def test_names_outside_the_bench_are_refused_everywhere(name):
    with pytest.raises(gke.NotDeletable):
        gke.delete_command(name)
    with pytest.raises(gke.NotDeletable):
        gke.default_spec(name, now=NOW)


def _describe(name="snowplow-bench-1014-1200", **labels):
    base = dict(gke.BENCH_LABELS, **{"ttl-expires": str(NOW)})
    base.update(labels)
    return {"name": name, "zone": "europe-west4-a",
            "resourceLabels": {k: v for k, v in base.items() if v is not None}}


def test_assert_deletable_requires_every_bench_label():
    gke.assert_deletable(_describe())
    with pytest.raises(gke.NotDeletable):
        gke.assert_deletable(_describe(purpose=None))
    with pytest.raises(gke.NotDeletable):
        gke.assert_deletable(_describe(issue="607"))
    with pytest.raises(gke.NotDeletable):
        gke.assert_deletable(_describe(**{"ttl-expires": None}))
    # labels copied onto 057 still do not make it deletable
    with pytest.raises(gke.NotDeletable):
        gke.assert_deletable(_describe(name="krateo-057"))


def test_reaper_selects_only_expired_bench_clusters():
    clusters = [
        _describe("snowplow-bench-a1", **{"ttl-expires": str(NOW - 1)}),
        _describe("snowplow-bench-a2", **{"ttl-expires": str(NOW + 3600)}),
        _describe("snowplow-bench-a3", **{"ttl-expires": "garbage"}),
        _describe("snowplow-bench-a4", purpose=None,
                  **{"ttl-expires": str(NOW - 1)}),
        _describe("krateo-057", **{"ttl-expires": str(NOW - 1)}),
    ]
    cmds = gke.reap_commands(clusters, now=NOW)
    assert [c[4] for c in cmds] == ["snowplow-bench-a1"]
    assert "--quiet" in cmds[0]


def test_resolve_endpoints_reads_lb_ips_and_refuses_missing():
    def svc(name, ip, port):
        return {"metadata": {"name": name}, "spec": {"ports": [{"port": port}]},
                "status": {"loadBalancer": {"ingress": [{"ip": ip}] if ip else []}}}
    full = {"items": [svc("snowplow", "1.1.1.1", 8081), svc("authn", "2.2.2.2", 8082),
                      svc("frontend-krateo-frontend", "3.3.3.3", 8080),
                      svc("kagent-ui", "4.4.4.4", 80)]}
    assert gke.resolve_endpoints(full) == {
        "SNOWPLOW_URL": "http://1.1.1.1:8081",
        "AUTHN_URL": "http://2.2.2.2:8082",
        "FRONTEND_URL": "http://3.3.3.3:8080"}
    pending = {"items": [svc("snowplow", None, 8081), svc("authn", "2.2.2.2", 8082)]}
    with pytest.raises(RuntimeError, match="SNOWPLOW_URL"):
        gke.resolve_endpoints(pending)


def test_self_destruct_checks_ttl_and_labels_before_deleting():
    s = _spec()
    m = gke.self_destruct_manifest(s)
    docs = list(yaml.safe_load_all(m))
    script = docs[1]["spec"]["jobTemplate"]["spec"]["template"]["spec"][
        "containers"][0]["args"][0]
    assert f'-ge {s.ttl_expires} ] || exit 0' in script
    assert "purpose=snowplow-bench" in script
    assert script.index("refusing") < script.index("clusters delete")


def test_run_with_teardown_runs_on_return_exception_and_signal():
    s = _spec()
    calls = []
    assert gke.run_with_teardown(s, lambda: 0, teardown=calls.append) == 0
    with pytest.raises(ValueError):
        gke.run_with_teardown(s, lambda: (_ for _ in ()).throw(ValueError("x")),
                              teardown=calls.append)

    def body():
        os.kill(os.getpid(), signal.SIGTERM)
        return 0
    with pytest.raises(KeyboardInterrupt):
        gke.run_with_teardown(s, body, teardown=calls.append)
    assert len(calls) == 3
    # handlers restored
    assert signal.getsignal(signal.SIGTERM) is not None


def test_teardown_failure_is_reported_not_masked(capsys):
    s = _spec()

    def bad(_):
        raise RuntimeError("gcloud down")
    # A clean body with a failed teardown is NOT a success: the cluster bills.
    assert gke.run_with_teardown(s, lambda: 0, teardown=bad) == 1
    err = capsys.readouterr().err
    assert "TEARDOWN FAILED" in err and "clusters delete" in err


def test_gke_up_without_yes_is_a_dry_run(monkeypatch, capsys):
    def boom(*a, **k):
        raise AssertionError("dry run must not execute anything")
    monkeypatch.setattr(subprocess, "run", boom)
    rc = gke.main(["gke-up", "--name", "snowplow-bench-1014-1200"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "DRY-RUN: gcloud container clusters create snowplow-bench-1014-1200" in out
    assert "ttl-expires=" in out


# ─── 057 refusal in the context pin ─────────────────────────────────────────

CTX_057 = "gke_operations-dev-krateo-io_europe-west3-a_krateo-057"


def test_pinned_context_refuses_057(monkeypatch):
    monkeypatch.setenv("BENCH_GKE_CONTEXT", CTX_057)
    with pytest.raises(SystemExit) as e:
        cluster.canonical_gke_context()
    assert e.value.code == 3


def test_guard_refuses_057_even_when_pin_and_current_context_agree(monkeypatch):
    # Pin == current-context == 057: the pre-#606 guard PASSED this (the two
    # matched). Only the 057 refusal can fail it.
    monkeypatch.delenv("BENCH_ALLOW_NON_GKE", raising=False)
    monkeypatch.setenv("BENCH_GKE_CONTEXT", CTX_057)

    class _P:
        returncode = 0
        stdout = (CTX_057 + "\n").encode()
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _P())
    with pytest.raises(SystemExit) as e:
        cluster.gke_context_guard()
    assert e.value.code == 3


def test_forbidden_context_matches_the_cluster_segment_only():
    assert cluster.forbidden_context(CTX_057)
    assert cluster.forbidden_context("krateo-057")
    assert not cluster.forbidden_context(
        "gke_operations-dev-krateo-io_europe-west4-a_snowplow-bench-1014-1200")
    assert not cluster.forbidden_context("gke_p_z_krateo-0570")


# ─── RBAC model ─────────────────────────────────────────────────────────────


def test_rbac_model_shape_is_between_one_class_and_one_per_user():
    m = rbac_model.RbacModel()
    shape = rbac_model.identity_classes(m)
    assert shape["users"] == 1000
    assert 50 < shape["distinct_group_sets"] < 1000
    assert shape["multi_team_users"] == 100
    assert shape["admins"] == 10
    homes = [rbac_model.user_groups(i, m)[1] for i in range(1, 1001)]
    assert all(homes.count(rbac_model.team_group(n)) == 20 for n in range(1, 51))
    assert all(rbac_model.user_groups(i, m)[0] == "devs" for i in range(1, 1001))
    assert rbac_model.user_groups(7, m) == rbac_model.user_groups(7, m)
    with pytest.raises(ValueError):
        rbac_model.user_groups(1001, m)


def test_rbac_yaml_binds_team_group_in_its_namespace():
    docs = list(yaml.safe_load_all(rbac_model.namespace_rbac_yaml(7)))
    assert docs[0]["metadata"]["namespace"] == "bench-ns-07"
    assert docs[1]["subjects"][0]["name"] == "bench-team-07"
    adm = list(yaml.safe_load_all(rbac_model.admin_rbac_yaml()))
    assert adm[1]["kind"] == "ClusterRoleBinding"
    assert adm[1]["subjects"][0]["name"] == rbac_model.ADMIN_GROUP


def test_synthetic_users_take_groups_from_the_model(monkeypatch):
    from bench import lifecycle
    applied = []
    monkeypatch.setattr(lifecycle, "kubectl",
                        lambda *a, **k: (applied.append(k.get("input_data")) or (0, "", "")))
    monkeypatch.setattr(lifecycle.time, "sleep", lambda s: None)
    monkeypatch.setattr(lifecycle.urllib.request, "urlopen",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("no authn")))
    m = rbac_model.RbacModel(users=20, namespaces=5, admins=1)
    import functools
    lifecycle.create_synthetic_users(1, 20, groups_for=functools.partial(
        rbac_model.user_groups, m=m))
    users = [d for d in yaml.safe_load_all("---\n".join(applied))
             if d and d.get("kind") == "User"]
    assert len(users) == 20
    assert users[0]["spec"]["groups"] == rbac_model.user_groups(1, m)
    applied.clear()
    lifecycle.create_synthetic_users(1, 2)
    users = [d for d in yaml.safe_load_all("---\n".join(applied))
             if d and d.get("kind") == "User"]
    assert users[0]["spec"]["groups"] == ["devs"]


# ─── data606: the controller-hold falsifier ─────────────────────────────────


def _patch_children(monkeypatch, listing, installed=True, rc=0):
    from bench import data606
    monkeypatch.setattr(data606.cluster, "_crd_exists", lambda k: installed)
    monkeypatch.setattr(data606.cluster, "kubectl",
                        lambda *a, **k: (rc, listing, "" if rc == 0 else "boom"))
    return data606


def test_hold_falsifier_passes_with_zero_bench_children(monkeypatch):
    d = _patch_children(monkeypatch, "argocd-root\nsome-system-repo\n")
    assert d.assert_no_external_children() == {
        "repoes.git.krateo.io": 0, "applications.argoproj.io": 0}


def test_hold_falsifier_aborts_on_a_leaked_child(monkeypatch):
    d = _patch_children(monkeypatch, "bench-app-01-01\n")
    with pytest.raises(RuntimeError, match="LEAKED"):
        d.assert_no_external_children()


def test_hold_falsifier_fails_closed_when_listing_fails(monkeypatch):
    d = _patch_children(monkeypatch, "", rc=1)
    with pytest.raises(RuntimeError, match="refusing to assume zero"):
        d.assert_no_external_children()


# ─── load606: SPA walk ──────────────────────────────────────────────────────


def _widget(*paths, **extra):
    items = [{"path": p, "verb": "GET", "allowed": True} for p in paths]
    items += extra.get("more", [])
    return {"status": {"resourcesRefs": {"items": items}}}


def test_extract_refs_follows_only_allowed_get_call_refs():
    doc = _widget("/call?a", more=[
        {"path": "/call?b", "verb": "POST", "allowed": True},
        {"path": "/call?c", "verb": "GET", "allowed": False},
        {"path": "/call?d", "verb": "GET", "allowed": True, "rendered": {"x": 1}},
        {"path": "https://elsewhere", "verb": "GET", "allowed": True}])
    assert load606.extract_refs(doc) == ["/call?a"]
    assert load606.extract_refs({"status": {}}) == []
    assert load606.extract_refs([1, 2]) == []


class _FakeClient:
    def __init__(self, tree, status=None):
        self.tree, self.status, self.calls = tree, status or {}, []
        self.lock = threading.Lock()

    def get(self, path, token):
        with self.lock:
            self.calls.append(path)
        st = self.status.get(path, 200)
        body = json.dumps(self.tree.get(path, {})).encode() if st == 200 else b""
        return load606.Response(st, body, 1.0)


def test_walk_page_walks_levels_dedupes_and_counts_errors():
    import concurrent.futures as cf
    tree = {"/call?root": _widget("/call?a", "/call?b"),
            "/call?a": _widget("/call?leaf", "/call?b"),
            "/call?b": _widget("/call?leaf"),
            "/call?leaf": {}}
    c = _FakeClient(tree, status={"/call?b": 500})
    with cf.ThreadPoolExecutor(4) as pool:
        wall, calls, errors, nbytes, slowest, samples = load606.walk_page(
            c, "t", "/call?root", pool=pool)
    assert sorted(c.calls) == sorted(["/call?root", "/call?a", "/call?b", "/call?leaf"])
    assert calls == 4 and errors == 1 and samples and "500" in samples[0]


# The Driver's navigation model (first load vs route click) is covered in
# tests/test_navmodel.py against a fake portal tree.


def test_http_client_round_trip_with_gzip():
    body = json.dumps(_widget("/call?x")).encode()

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.headers.get("Authorization") != "Bearer tok":
                self.send_response(401); self.end_headers(); return
            z = gzip.compress(body)
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(z)))
            if self.path.endswith("ra"):      # a RESTAction /call carries its stage outcomes (#904)
                # snowplow's own value (dispatchers/issue904_stored_outcomes_test.go:52)
                self.send_header("X-Snowplow-Stage-Outcomes", '[{"name":"sa","ok":false,"reason":"Forbidden"}]')
            self.end_headers()
            self.wfile.write(z)

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        c = load606.HttpClient(f"http://127.0.0.1:{srv.server_address[1]}")
        r = c.get("/call?root", "tok")
        assert r.status == 200 and json.loads(r.body) == json.loads(body)
        assert r.stage_outcomes is None
        assert c.get("/call?ra", "tok").stage_outcomes == '[{"name":"sa","ok":false,"reason":"Forbidden"}]'
        assert c.get("/call?root", None).status == 401
    finally:
        srv.shutdown()


# ─── load606: freshness + churn ─────────────────────────────────────────────


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_freshness_probe_measures_from_mutation_return():
    clk = _Clock()
    state = {"marker": None, "visible_at": None}

    def mutate(m):
        state["marker"], state["visible_at"] = m, clk.t + 0.35
        return load606.WriteResult(True)

    class C:
        def get(self, path, token):
            vis = state["visible_at"] is not None and clk.t >= state["visible_at"]
            return load606.Response(200, (state["marker"] if vis else "old").encode(), 1)

    recs = load606.freshness_probe(C(), "t", "/call?ra", mutate, samples=3,
                                   poll_ms=100, gap_s=1, clock=clk.now,
                                   sleep=clk.sleep)
    assert all(r["converged"] for r in recs)
    assert all(abs(r["latency_ms"] - 400) < 1 for r in recs)  # 100ms poll grid


def test_freshness_probe_records_never_converged_samples():
    clk = _Clock()

    class C:
        def get(self, path, token):
            return load606.Response(200, b"never", 1)
    recs = load606.freshness_probe(C(), "t", "/p", lambda m: load606.WriteResult(True), samples=2,
                                   budget_s=1, gap_s=0, clock=clk.now,
                                   sleep=clk.sleep)
    assert [r["converged"] for r in recs] == [False, False]
    assert all(r["latency_ms"] is None for r in recs)


def test_churn_driver_reaches_its_rate():
    seen = []
    c = load606.ChurnDriver(list(range(5)), 50, lambda t, m: seen.append(t) or load606.WriteResult(True))
    c.start()
    import time as _t
    _t.sleep(0.5)
    res = c.stop()
    assert res["failed"] == 0 and 15 <= res["issued"] <= 40
    assert set(seen) == set(range(5))


def test_list_dep_fixture_has_no_query_string_and_projects_the_marker():
    y = load606.list_dep_fixture_yaml("bench-ns-03", "composition.krateo.io",
                                      "v1-2-2", "things")
    doc = yaml.safe_load(y)
    api = doc["spec"]["api"][0]
    assert api["path"] == "/apis/composition.krateo.io/v1-2-2/namespaces/bench-ns-03/things"
    assert "?" not in api["path"]
    assert load606.FRESH_ANNOTATION in api["filter"]


# ─── load606: economics + verdict ───────────────────────────────────────────


def _vars(completed, enq, list_dep, exact, events, q=0, p95=200):
    return load606.summarize_vars({
        "snowplow_refresher_completed_total": completed,
        "snowplow_refresher_enqueue_total": enq,
        "snowplow_refresher_dropped_total": 0,
        "snowplow_refresher_queue_depth": q,
        "snowplow_refresher_dirty_to_fresh_ms_p95": p95,
        "snowplow_deps_dirty_mark_attribution": {
            "by_bucket": [
                {"path": "x", "cause": "y", "class": "list_dep", "count": list_dep},
                {"path": "x", "cause": "z", "class": "exact_dep", "count": exact}],
            "events_total": events, "unattributed": 0},
        "snowplow_resolved_cache": {"hit_total": 1, "miss_total": 2},
    })


def test_summarize_vars_keeps_absent_counters_as_none():
    s = load606.summarize_vars({})
    assert s["refresher_completed"] is None and s["marks_by_class"] is None


def test_economics_uses_driver_calls_as_read_denominator():
    e = load606.economics(_vars(100, 1000, 900, 100, 10),
                          _vars(1100, 6000, 9900, 100, 410), customer_calls=500)
    assert e["completions_per_read"] == 2.0
    assert e["list_dep_share"] == 1.0
    assert e["marks_per_event"] == 22.5
    assert load606.economics({}, {}, 10)["completions_per_read"] is None


def _navs(first_ms, warm_ms, n_warm=300):
    out = [{"kind": "nav", "user": f"u{i}", "page": "p", "visit": 1,
            "wall_ms": first_ms, "calls": 10, "errors": 0, "sched_lag_ms": 5}
           for i in range(100)]
    out += [{"kind": "nav", "user": f"u{i % 100}", "page": "p", "visit": 2,
             "wall_ms": warm_ms, "calls": 10, "errors": 0, "sched_lag_ms": 5}
            for i in range(n_warm)]
    return out


def _fresh(ms, n=40, converged=True):
    return [{"kind": "fresh", "label": "list_dep", "converged": converged,
             "latency_ms": ms if converged else None} for _ in range(n)]


def test_verdict_pass_fail_invalid():
    vs = [_vars(0, 0, 0, 0, 0), _vars(10, 50, 90, 10, 4)]
    r = load606.evaluate(_navs(300, 200), _fresh(400), vs)
    assert r["verdict"] == "PASS", r["checks"]
    assert r["economics"]["completions_per_read"] == round(10 / 4000, 3)

    # first navigation slower than the warm bar → zero-cold-navs FAILS
    r = load606.evaluate(_navs(800, 200), _fresh(400), vs)
    assert r["verdict"] == "FAIL"
    failed = {c["check"] for c in r["checks"] if c["result"] == "FAIL"}
    assert "zero_cold_navs.first_nav_p95_ms" in failed

    # one unconverged freshness sample fails freshness outright
    r = load606.evaluate(_navs(300, 200), _fresh(400) + _fresh(0, 1, False), vs)
    assert r["verdict"] == "FAIL"

    # no freshness data at all is a FAIL, not a silent pass
    assert load606.evaluate(_navs(300, 200), [], vs)["verdict"] == "FAIL"

    # too few freshness samples / too few warm navs → INVALID wins
    assert load606.evaluate(_navs(300, 200), _fresh(400, n=5), vs)["verdict"] == "INVALID"
    assert load606.evaluate(_navs(300, 200, n_warm=10), _fresh(400), vs)["verdict"] == "INVALID"


def test_read_jsonl_skips_and_counts_stderr_noise(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('{"kind": "nav"}\nlogin failed for u\n\n{"kind": "fresh"}\n')
    recs = load606._read_jsonl(str(p))
    assert [r["kind"] for r in recs] == ["nav", "fresh", "skipped"]
    assert recs[-1]["lines"] == 1


# ─── loadgen Job ────────────────────────────────────────────────────────────


def test_shard_ranges_cover_every_user_once():
    assert load606_job.shard_ranges(1000, 2) == [(1, 500), (501, 1000)]
    r = load606_job.shard_ranges(1001, 3)
    assert r[0][0] == 1 and r[-1][1] == 1001
    assert sum(b - a + 1 for a, b in r) == 1001
    with pytest.raises(ValueError):
        load606_job.shard_ranges(2, 3)


def test_loadgen_job_renders_pinned_to_loadgen_pool_with_source():
    src = load606_job.LOAD606_SRC.read_text()
    m = load606_job.render(users=1000, shards=2, think_s=60, duration_s=1800,
                           snowplow_url="http://s:8081", authn_url="http://a:8082",
                           password="pw",
                           init="/call?resource=layouts&name=app-shell&x=1")
    ns, cm, job = list(yaml.safe_load_all(m))
    assert cm["data"]["load606.py"].rstrip("\n") == src.rstrip("\n")
    spec = job["spec"]["template"]["spec"]
    assert spec["nodeSelector"] == {"bench.krateo.io/pool": "loadgen"}
    assert spec["tolerations"][0]["value"] == "loadgen"
    assert job["spec"]["completionMode"] == "Indexed"
    script = spec["containers"][0]["args"][0]
    assert "0) S=1; E=500;;" in script and "1) S=501; E=1000;;" in script
    assert '--init "/call?resource=layouts&name=app-shell&x=1"' in script
    assert "--page" not in script            # no hard-coded page roots any more


# ─── run606.sh: go gate + teardown ──────────────────────────────────────────


def _run606(tmp_path, **env):
    base = dict(os.environ, HOME=str(tmp_path), OUT=str(tmp_path / "out"),
                RUN_NAME="snowplow-bench-1014-1200", INSTALLER_VERSION="0.0.0",
                SNOWPLOW_TAG="0.0.0", THINK_S="0", PYTHONPATH=str(BENCH_DIR),
                EXPECTED_IMAGE_DIGEST="sha256:" + "ab" * 32)
    base.pop("BENCH_606_GO", None)
    base.update(env)
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=base,
                          capture_output=True, text=True, timeout=120)


def test_run606_refuses_without_todays_go(tmp_path):
    p = _run606(tmp_path)
    assert p.returncode == 2
    assert "BENCH_606_GO" in p.stderr
    assert "gke-up" not in p.stderr + p.stdout
    p = _run606(tmp_path, BENCH_606_GO="19700101-snowplow-bench-1014-1200")
    assert p.returncode == 2


def test_run606_dry_run_completes_and_tears_down(tmp_path):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr
    # No bash-level error anywhere (e.g. backticks in a heredoc executing as
    # command substitution — found by the 2026-10-14 shakedown dry run).
    assert "run606.sh: " not in p.stderr, p.stderr
    vals = yaml.safe_load((tmp_path / "out" / "installer-values.yaml").read_text())
    assert "krateo-core-provider" not in vals
    assert vals["core-provider"]["resources"]["limits"]["memory"] == "4Gi"
    assert vals["vertexAI"] == {"enabled": False}
    assert vals["componentValues"]["snowplow"]["nodeSelector"] == {
        "bench.krateo.io/pool": "snowplow"}
    assert "teardown snowplow-bench-1014-1200 (exit 0)" in p.stderr
    assert "DRY-RUN: gcloud container clusters delete snowplow-bench-1014-1200" in p.stdout


@pytest.mark.parametrize("step", ["install", "data", "measure", "evaluate"])
def test_run606_tears_down_when_a_step_fails(tmp_path, step):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", BENCH_606_FAIL_AT=step)
    assert p.returncode != 0
    assert f"injected failure at {step}" in p.stderr
    assert "teardown snowplow-bench-1014-1200" in p.stderr
    assert "DRY-RUN: gcloud container clusters delete" in p.stdout


def test_run606_no_teardown_when_nothing_was_created(tmp_path):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", BENCH_606_FAIL_AT="reap")
    assert p.returncode != 0
    assert "teardown" not in p.stderr
