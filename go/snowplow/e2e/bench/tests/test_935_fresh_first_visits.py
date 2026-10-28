"""#935 (owner ruling 2026-10-27): first-visit page loads measured with fresh
identity classes through the run, not on 5 samples per route.

- bench/fresh_classes.py plans one fresh class per two route cycles per
  Chrome session (cycle_s = 2 x len(routes) x BROWSER_THINK_S), each a new
  user with a bench user's groups and a RoleBinding naming it alone; run606
  creates them at the data step (never logged in) and deletes them at collect.
- k6/browser.js switches classes (switchDue) in a new browser context and
  reads snowplow's learned-class counters around each fresh sign-in.
- evaluate proves each class cold from those counters and reports, per route
  and phase, the proven classes' first visits: n, p50, p95, max; custlat pair
  gives each as B/A.

Every report goes through the evaluate / pair CLIs.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time

import pytest
import yaml

from bench import fresh_classes, k6_job, rbac_model
from tests.test_k6_830 import _point, _run606_k6, _write_out
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401  (fixture)

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


# ─── the plan: how many classes, what makes each one fresh ──────────────────

def test_the_cadence_is_two_route_cycles_and_derives_from_the_run():
    # 8 routes at a 15 s think time: one class per 240 s; a 3,600 s browser run needs 15
    assert fresh_classes.cycle_s(8, 15) == 240
    assert fresh_classes.classes_per_session(8, 15, 3600) == 15
    assert fresh_classes.classes_per_session(8, 15, 100) == 1          # shorter than a cycle: the initial user only
    with pytest.raises(ValueError):
        fresh_classes.cycle_s(0, 15)


def test_each_fresh_class_is_a_new_user_whose_own_binding_no_one_else_has():
    p = fresh_classes.plan(sessions=2, routes=4, think_s=15, duration_s=600, namespaces=3, user_prefix="scaleuser-")
    assert p["classes_per_session"] == 5 and "2 x len(routes) x BROWSER_THINK_S" in p["formula"]
    users = [f["user"] for s in p["sessions"] for f in s["fresh"]]
    assert len(users) == 8 == len(set(users)) and all(u.startswith("scaleuser-fresh-") for u in users)
    f = p["sessions"][0]["fresh"][0]
    assert f["groups"] == [rbac_model.BASE_GROUP, rbac_model.team_group(1)] and f["namespace"] == "bench-ns-01"
    docs = list(yaml.safe_load_all(fresh_classes.manifest(p, "pw")))
    kinds = {}
    for d in docs:
        kinds.setdefault(d["kind"], []).append(d)
        assert d["metadata"]["labels"][fresh_classes.LABEL] == "true"
    assert {k: len(v) for k, v in kinds.items()} == {"Secret": 8, "User": 8, "RoleBinding": 8}
    rb = kinds["RoleBinding"][0]
    # the binding names the user ALONE: its binding set is its own
    assert rb["subjects"] == [{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": users[0]}]
    assert rb["roleRef"]["name"] == fresh_classes.ROLE and rb["metadata"]["namespace"] == "bench-ns-01"
    assert kinds["User"][0]["spec"]["groups"] == f["groups"]
    assert fresh_classes.manifest(fresh_classes.plan(sessions=2, routes=4, think_s=15, duration_s=60, namespaces=3,
                                                     user_prefix="x-"), "pw") == ""


def test_cleanup_deletes_by_label_and_each_users_clientconfig():
    p = fresh_classes.plan(sessions=1, routes=2, think_s=10, duration_s=120, namespaces=2, user_prefix="scaleuser-")
    t = fresh_classes.cleanup_targets(p)
    assert t[:3] == [
        ["-n", "krateo-system", "delete", "--ignore-not-found", "users.basic.authn.krateo.io", "-l",
         "bench.krateo.io/fresh-class=true"],
        ["-n", "krateo-system", "delete", "--ignore-not-found", "secret", "-l", "bench.krateo.io/fresh-class=true"],
        ["delete", "--ignore-not-found", "rolebinding", "--all-namespaces", "-l", "bench.krateo.io/fresh-class=true"]]
    assert t[3][-2:] == ["scaleuser-fresh-1-001-clientconfig", "scaleuser-fresh-1-002-clientconfig"]


def test_the_browser_config_carries_each_sessions_fresh_users():
    m = rbac_model.RbacModel(users=20, namespaces=2)
    sessions = k6_job.browser_sessions(m, 2)
    p = fresh_classes.plan(sessions=2, routes=2, think_s=10, duration_s=120, namespaces=2, user_prefix="scaleuser-")
    cfg = k6_job.browser_config(sessions=sessions, routes=["/a", "/b"], password="pw", think_s=10,
                                duration_s=120, fresh=p)
    # 2 routes x 10 s: a 40 s class cycle; 120 s: 3 classes, the initial user and 2 fresh
    assert cfg["sessions"][1]["fresh"] == ["scaleuser-fresh-2-001", "scaleuser-fresh-2-002"]
    assert cfg["snowplowUrl"] == "http://localhost:8081"


# ─── browser.js's pure parts (node) ──────────────────────────────────────────

_DRIVER = r"""
import * as pl from './pageload.mjs'
const routes = ['/a', '/b', '/c']
process.stdout.write(JSON.stringify({
  due: [pl.switchDue({ '/a': 2, '/b': 2, '/c': 1 }, routes, false), pl.switchDue({ '/a': 2, '/b': 2, '/c': 2 }, routes, false),
        pl.switchDue({ '/a': 9, '/b': 9, '/c': 9 }, routes, true)],
  tags: [pl.freshClassTags({ registered: 40, seeded: 7 }, { registered: 41, seeded: 8 }, 1793000000.7),
         pl.freshClassTags(null, { registered: 41 }, 5)],
}))
"""


@needs_node
def test_a_class_switches_after_every_route_was_visited_twice(tmp_path):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_DRIVER)
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs")], capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    out = json.loads(p.stdout)
    assert out["due"] == [False, True, False]                       # the last class never switches
    assert out["tags"][0] == {"registered_before": "40", "registered_after": "41", "seeded_before": "7",
                              "seeded_after": "8", "t_before": "1793000000"}
    assert out["tags"][1]["registered_before"] == "" and out["tags"][1]["seeded_after"] == ""


def test_browser_js_runs_each_class_in_its_own_context_and_reads_the_counters_around_its_sign_in():
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    body = src[src.index("async function runClass"):]
    assert body.index("const context = await browser.newContext()") < body.index("await gotoLogin(page)")
    assert body.index("learnedCounts(prevToken)") < body.index("press('Enter')") < body.index("learnedCounts(token)")
    assert "while (Date.now() < deadline && !switchDue(visits, CFG.routes, last))" in body
    assert "const users = [s.user].concat(s.fresh || [])" in src
    assert "new Counter('browser_fresh_class')" in src and "browser_fresh_class|" in src


# ─── evaluate: proven cold, per route and phase, n beside every figure ──────

def _tp(metric, value, t, **tags):
    pt = _point(metric, value, **tags)
    pt["data"]["time"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + "Z"
    return pt


def _fc(t_after, session, cls, rb, ra, sb="5", sa="5", t_before=None):
    return _tp("browser_fresh_class", 1, t_after, session=session, **{"class": cls}, identity="fresh",
               registered_before=rb, registered_after=ra, seeded_before=sb, seeded_after=sa,
               t_before=str(t_before if t_before is not None else t_after - 20))


def _load(t, ms, session, cls, route, visit, identity="fresh"):
    return _tp("browser_page_load_ms", ms, t, session=session, **{"class": cls}, identity=identity, route=route,
               visit=visit, method="click", skeleton="yes", timeout="no")


T = 1_793_000_000


def _browser_points(scale=1.0):
    return [
        _load(T, 900, "s1", "0", "/dashboard", "first", identity="initial"),     # the initial user: not fresh
        # s1 class 1: alone in its window, the counter moved: proven
        _fc(T + 100, "s1", "1", "40", "41", sb="7", sa="8"),
        _load(T + 100, 2000 * scale, "s1", "1", "/dashboard", "first"),
        _load(T + 120, 1500 * scale, "s1", "1", "/compositions", "first"),
        _load(T + 140, 300, "s1", "1", "/dashboard", "warm"),                       # warm: not a first visit
        # s1 class 2 and s2 class 1 overlap: two sign-ins, the counter moved by 2: both proven
        _fc(T + 400, "s1", "2", "41", "43", t_before=T + 380),
        _fc(T + 410, "s2", "1", "41", "43", t_before=T + 385),
        _load(T + 400, 3000 * scale, "s1", "2", "/dashboard", "first"),
        _load(T + 410, 4000 * scale, "s2", "1", "/dashboard", "first"),
        # s2 class 2: the counter did not move: not proven, left out
        _fc(T + 700, "s2", "2", "43", "43"),
        _load(T + 700, 99999, "s2", "2", "/dashboard", "first"),
        # s3 class 1: a read failed: not proven
        _fc(T + 900, "s3", "1", "", "44"),
        _load(T + 900, 88888, "s3", "1", "/dashboard", "first")]


def _evaluate(d, scale=1.0, churn=None):
    _write_out(d / "browser", "browser", _browser_points(scale), None, 0)
    (d / "preflight.jsonl").write_text(json.dumps({"kind": "preflight"}) + "\n")
    files = [str(d / "preflight.jsonl")]
    if churn:                                        # fresh606's churn record: a churn phase
        (d / "fresh-fresh606-0.jsonl").write_text(json.dumps(churn) + "\n")
        files.append(str(d / "fresh-fresh606-0.jsonl"))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", *files,
                        "--browser-out", str(d / "browser"), "--browser-sessions", "3"],
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=300)
    assert p.stdout.strip().startswith("{"), p.stderr[-3000:]
    return json.loads(p.stdout)


def test_fresh_first_visits_per_phase_with_the_churn_steps_pooled(tmp_path):
    v = _evaluate(tmp_path, churn={"kind": "churn", "label": "list_dep@10", "issued": 1, "failed": 0,
                                   "t0": T + 300, "t1": T + 500, "target_rate": 10, "achieved_rate": 10})
    d = v["report"]["acceptance_figures_report_only"]["fresh_first_visits"]["by_route"]["/dashboard"]
    assert set(d) == {"steady", "churn:list_dep@10", "churn", "all"}
    assert d["churn:list_dep@10"] == d["churn"] == {"n": 2, "p50_ms": 3500.0, "p95_ms": 3950.0, "max_ms": 4000.0}
    assert d["steady"]["n"] == 1 and d["all"]["n"] == 3


def test_fresh_first_visits_per_route_proven_cold_through_evaluate(tmp_path):
    v = _evaluate(tmp_path)
    r = v["report"]["acceptance_figures_report_only"]["fresh_first_visits"]
    assert r["status"] == "present" and (r["classes"], r["cold_proven"]) == (5, 3)
    assert sorted((x["session"], x["class"]) for x in r["not_proven"]) == [("s2", "2"), ("s3", "1")]
    assert r["seed_ran_during_first_visit"] == 1
    assert (r["registered_between_windows"], r["between_windows_read"]) == (0, 2) and "warning" not in r
    d = r["by_route"]["/dashboard"]["all"]
    # three proven first visits of /dashboard: 2000, 3000, 4000 (never the initial user's, the warm one,
    # or an unproven class's)
    assert d == {"n": 3, "p50_ms": 3000.0, "p95_ms": 3900.0, "max_ms": 4000.0}
    assert r["by_route"]["/compositions"]["all"]["n"] == 1
    assert set(r["by_route"]["/dashboard"]) == {"steady", "all"}    # no phase plan in this run: steady


def _cls(session, t0, t1, rb, ra):
    return {"session": session, "class": "1", "t0": t0, "t1": t1, "registered_before": rb, "registered_after": ra,
            "seeded_before": 0, "seeded_after": 0}


def test_two_overlapping_sign_ins_with_one_registration_prove_neither():
    """#939 review R2: the overlap rule pinned — one registration cannot
    prove two overlapping sign-ins (the reviewer's Q1: any movement)."""
    from bench import figures
    cs = [_cls("s1", 100, 120, 40, 41), _cls("s2", 110, 130, 40, 41)]
    figures.prove_cold(cs)
    assert [c["cold_proven"] for c in cs] == [False, False]
    assert all("1 class(es) registered for 2 sign-in(s)" in c["why"] for c in cs)
    cs = [_cls("s1", 100, 120, 40, 41), _cls("s2", 110, 130, 40, 42)]       # two registrations: both
    figures.prove_cold(cs)
    assert [c["cold_proven"] for c in cs] == [True, True]


def test_a_neighbour_whose_read_failed_still_counts_in_the_window():
    """#939 review R1: class A reads fine but is NOT fresh (registers
    nothing); class C overlaps it, is fresh (registers 1) and its read
    failed. C's registration must not prove A: C stays in the group, which
    needs two registrations; both are unproven."""
    from bench import figures
    a, c = _cls("s1", 100, 120, 40, 41), _cls("s2", 105, 125, None, None)
    figures.prove_cold([a, c])
    assert (a["cold_proven"], c["cold_proven"]) == (False, False)
    assert "1 class(es) registered for 2 sign-in(s)" in a["why"] and "read failed" in c["why"]


def test_registrations_between_the_windows_falsify_nothing_else_registers():
    from bench import figures
    quiet = [_cls("s1", 100, 120, 40, 41), _cls("s2", 200, 220, 41, 42)]
    assert figures.prove_cold(quiet) == {"registered_between_windows": 0, "gaps_read": 1}
    noisy = [_cls("s1", 100, 120, 40, 41), _cls("s2", 200, 220, 44, 45)]  # 3 registered by something else
    assert figures.prove_cold(noisy) == {"registered_between_windows": 3, "gaps_read": 1}


def test_an_eviction_between_windows_does_not_hide_a_foreign_registration():
    """#939 delta L7: the learned-class counter is a gauge. Gap 1 FALLS by
    2 (an eviction), gap 2 RISES by 2 (something else registered). A signed
    sum reads 0 and the falsifier goes quiet; only the rises count, and the
    report still warns."""
    from bench import figures
    cs = [_cls("s1", 100, 120, 40, 41), _cls("s2", 200, 220, 39, 40), _cls("s3", 300, 320, 42, 43)]
    assert figures.prove_cold(cs) == {"registered_between_windows": 2, "gaps_read": 2}
    o = {"points": {"browser_fresh_class": [(1, {"session": c["session"], "class": c["class"], "t_before": c["t0"],
                                                 "registered_before": c["registered_before"],
                                                 "registered_after": c["registered_after"],
                                                 "seeded_before": 0, "seeded_after": 0}) for c in cs],
                    "browser_page_load_ms": []},
         "times": {"browser_fresh_class": [c["t1"] for c in cs], "browser_page_load_ms": []}}
    r = figures.fresh_first_visits(o, [], lambda at, phases: "steady")
    assert r["registered_between_windows"] == 2 and r["between_windows_read"] == 2
    assert "2 class(es) registered between the fresh windows" in r["warning"]


def test_a_covered_group_does_not_prove_its_failed_read_class():
    """#939 delta L8 (the reviewer's Q7): three overlapping sign-ins and
    three registrations cover the group, so the two whose reads came back
    are proven; the one whose read failed counted toward the three, but
    nothing shows IT registered, so it stays unproven."""
    from bench import figures
    a, c, b = _cls("s1", 100, 120, 40, 41), _cls("s2", 105, 125, None, None), _cls("s3", 110, 130, 41, 43)
    figures.prove_cold([a, c, b])
    assert (a["cold_proven"], c["cold_proven"], b["cold_proven"]) == (True, False, True)
    assert "read failed" in c["why"] and "why" not in a and "why" not in b


def test_the_pair_gives_each_fresh_first_visit_figure_as_b_over_a(tmp_path):
    for x, scale in (("a", 1.0), ("b", 1.5)):
        d = tmp_path / x
        d.mkdir()
        (d / "verdict.json").write_text(json.dumps(_evaluate(d, scale)))
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "pair", "--a", str(tmp_path / "a" / "verdict.json"),
                        "--b", str(tmp_path / "b" / "verdict.json")], cwd=BENCH_DIR, capture_output=True,
                       text=True, timeout=300)
    r = json.loads(p.stdout)["acceptance_ratios_report_only"]["ratios"]
    assert r["fresh_first_visits/by_route//dashboard/all/p95_ms"]["b_over_a"] == 1.5
    assert r["fresh_first_visits/by_route//dashboard/all/n"] == {"a": 3, "b": 3, "b_over_a": 1.0}


# ─── run606: created at data, handed to the browser, cleaned at collect ─────

def test_dry_run_shows_the_fresh_classes_step(tmp_path):
    from tests.test_606 import _run606
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", LOAD_DRIVER="k6")
    assert p.returncode == 0, p.stderr[-2000:]
    assert "DRY: python3 -m bench.fresh_classes plan --sessions 5 --browser-think-s 15" in p.stderr


def test_run606_creates_hands_on_and_cleans_up_the_fresh_classes(harness):  # noqa: F811
    p, calls, out = _run606_k6(harness, DRIVE_S="200", BROWSER_THINK_S="1")
    log = p.stdout + p.stderr
    assert p.returncode == 0, log[-4000:]
    plan = json.loads((out / "fresh-classes.json").read_text())
    users = [f["user"] for s in plan["sessions"] for f in s["fresh"]]
    assert len(plan["sessions"]) == 3 and users
    applied = [c["argv"] for c in calls if c["tool"] == "kubectl" and "apply" in c["argv"]
               and any(x.endswith("fresh-classes.yaml") for x in c["argv"])]
    assert len(applied) == 1 and "--server-side" in applied[0]
    # handed to the browser: its config carries each session's fresh users
    cm = [d for d in yaml.safe_load_all((out / "loadgen-job.yaml").read_text()) if d and d["kind"] == "ConfigMap"]
    bcfg = next(json.loads(v) for d in cm for k, v in (d.get("data") or {}).items() if k == "browser-config.json")
    assert [s["fresh"] for s in bcfg["sessions"]] == [[f["user"] for f in s["fresh"]] for s in plan["sessions"]]
    # cleaned up at collect: by label, and every user's clientconfig Secret by name
    deletes = [c["argv"] for c in calls if c["tool"] == "kubectl" and "delete" in c["argv"]
               and ("bench.krateo.io/fresh-class=true" in c["argv"] or any(x.endswith("-clientconfig") for x in c["argv"]))]
    assert len(deletes) == 4 and all(f"{u}-clientconfig" in deletes[-1] for u in users)
    assert "fresh classes cleaned up" in log
    # no fresh user's name in what run606 prints
    assert not any(u in log for u in users)
    # #939 review: no manifest run606 applied keeps the password on disk
    from bench.load606 import SCALEUSER_PASSWORD
    for name in ("fresh-classes.yaml", "loadgen-job.yaml", "preflight-job.yaml"):
        text = (out / name).read_text()
        assert SCALEUSER_PASSWORD not in text and "<redacted>" in text, name
    leaks = [str(p.relative_to(out)) for p in out.rglob("*") if p.is_file()
             and SCALEUSER_PASSWORD in p.read_text(errors="replace")]
    assert leaks == [], leaks
