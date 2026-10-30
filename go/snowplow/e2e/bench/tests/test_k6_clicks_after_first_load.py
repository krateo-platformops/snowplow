"""Run 11: 36 % of the storm's calls (23.4K of 64.6K) were k6 route clicks of
users whose first dashboard load had not finished — the navigate iterations
started at t=THINK_S whatever the first load was doing; a real browser can't.
navigate.js now drives each user as the SPA does: its route clicks start only
after its first load completed, each THINK_S after the previous page ended.
The first loads still arrive on their open schedule (late ones are counted),
and every click the open schedule would have started during the first load
is counted as deferred.

The pinned k6 runs the real navigate.js in docker with spa.js stubbed to one
request per page against a stand-in snowplow whose first load takes longer
than the think time."""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
from collections import defaultdict

from bench import k6_job, k6eval
from tests.test_k6_830 import INIT, NS, needs_docker
from tests.test_run606_real_sequence import BENCH_DIR

FIX = BENCH_DIR / "tests" / "fixtures"
ROUTES = [{"path": "/a", "endpoint": "/route?p=/a"}, {"path": "/b", "endpoint": "/route?p=/b"}]
THINK_S, FIRST_S, ROUTE_S = 2.0, 3.0, 0.2


def _k6_navigate(tmp_path, *, users=6, burst=2, duration_s=12, burst_at_s=4):
    net = f"k6nav-{os.getpid()}-{tmp_path.name[-12:]}"
    fx = f"{net}-snow"
    k6dir, out, log = tmp_path / "k6", tmp_path / "out", tmp_path / "log"
    for d in (k6dir, out, log):
        d.mkdir()
        d.chmod(0o777)
    shutil.copy(BENCH_DIR / "k6" / "navigate.js", k6dir / "navigate.js")
    shutil.copy(FIX / "k6_spa_stub.js", k6dir / "spa.js")
    cfg = k6_job.k6_config(snowplow_url=f"http://{fx}:8081", init=INIT, frontend_ns=NS,
                           builders_ns=NS, routes=ROUTES, menu_classes={"devs": ROUTES}, class_of=lambda i: "devs", users=users, burst_users=burst,
                           think_s=THINK_S, duration_s=duration_s, burst_at_s=burst_at_s,
                           composite_routes=["/b"] if burst else [], viewers=False)
    (k6dir / "config.json").write_text(json.dumps(cfg))
    (k6dir / "tokens.json").write_text(json.dumps([f"tk-{i}" for i in range(1, users + 1)]))
    subprocess.run(["docker", "network", "create", net], capture_output=True, check=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", fx, "--network", net,
                        "-v", f"{FIX}:/f:ro", "-v", f"{log}:/log", "python:3.12-slim",
                        "python3", "-u", "/f/k6_nav_fixture.py", "8081", str(FIRST_S),
                        str(ROUTE_S), "/log/req.jsonl"], capture_output=True, check=True)
        # the stand-in must listen before k6's first arrival (t=0)
        subprocess.run(["docker", "exec", fx, "python3", "-c",
                        "import socket, time\n"
                        "for _ in range(200):\n"
                        "    try:\n"
                        "        socket.create_connection(('127.0.0.1', 8081), 1).close(); break\n"
                        "    except OSError:\n"
                        "        time.sleep(0.05)\n"], capture_output=True, check=True)
        p = subprocess.run(["docker", "run", "--rm", "--network", net,
                            "-v", f"{k6dir}:/opt/k6:ro", "-v", f"{out}:/out",
                            "-e", "K6_CONFIG=/opt/k6/config.json",
                            "-e", "TOKENS_FILE=/opt/k6/tokens.json",
                            "-e", "SUMMARY_FILE=/out/k6-summary.json",
                            k6_job.K6_IMAGE, "run", "--quiet",
                            "--out", "json=/out/k6.json.gz", "/opt/k6/navigate.js"],
                           capture_output=True, text=True, timeout=300)
    finally:
        subprocess.run(["docker", "rm", "-f", fx], capture_output=True)
        subprocess.run(["docker", "network", "rm", net], capture_output=True)
    reqs = defaultdict(list)
    for ln in (log / "req.jsonl").read_text().splitlines():
        r = json.loads(ln)
        reqs[int(r["auth"].rsplit("-", 1)[1])].append(r)
    return p, cfg, reqs, out


@needs_docker
def test_route_clicks_start_only_after_the_users_first_load(tmp_path):
    p, cfg, reqs, out = _k6_navigate(tmp_path)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    users = cfg["navUserIds"] + cfg["burstUserIds"]
    assert sorted(reqs) == users                      # every user arrived
    for u in users:
        rs = sorted(reqs[u], key=lambda r: r["t0"])
        first = [r for r in rs if r["path"] == "/first"]
        clicks = [r for r in rs if r["path"].startswith("/route")]
        assert len(first) == 1 and clicks, (u, rs)
        # no click while the first load runs; each THINK_S after the page before
        assert clicks[0]["t0"] >= first[0]["t1"] + THINK_S - 0.05, (u, rs)
        for a, b in zip(clicks, clicks[1:]):
            assert b["t0"] >= a["t1"] + THINK_S - 0.05, (u, rs)
    s = json.loads((out / "k6-summary.json").read_text())
    m = s["metrics"]
    assert m["arrivals{scenario:navigate}"]["count"] == len(cfg["navUserIds"])
    assert m["arrivals{scenario:composite_burst}"]["count"] == len(cfg["burstUserIds"])
    assert m["arrivals_late{scenario:navigate}"]["count"] == 0
    # FIRST_S > THINK_S: each navigating user's open schedule had a click
    # slot inside its first load; it is counted, not hidden
    assert m["clicks_deferred{scenario:navigate}"]["count"] >= len(cfg["navUserIds"])
    o = k6eval.read_k6_out(str(out), "k6")
    checks = {c["check"]: c for c in k6eval.k6_checks(o)}
    for sc in ("navigate", "composite_burst"):
        assert checks[f"k6.late_arrivals[{sc}]"]["result"] == "PASS", checks
        assert checks[f"k6.late_clicks[{sc}]"]["result"] == "PASS", checks
    # every click the fixture saw is one counted click
    nav_clicks = sum(1 for u in cfg["navUserIds"] for r in reqs[u] if r["path"].startswith("/route"))
    assert m["clicks{scenario:navigate}"]["count"] == nav_clicks
    assert checks["k6.composite_burst_complete"]["result"] == "PASS", checks
    rep = k6eval.k6_report(o)
    assert rep["clicks_deferred"]["navigate"] >= len(cfg["navUserIds"])
    with gzip.open(out / "k6.json.gz", "rt") as fh:
        frames = [json.loads(x)["data"]["tags"]["frame"] for x in fh
                  if '"bench_frame"' in x and '"Point"' in x]
    assert frames == ["start", "end"]


# ─── the arrival checks, on summaries ───────────────────────────────────────


def _summary(arrived, late, *, nav=4, burst=2, burst_arrived=2, burst_first=2,
             clicks=40, clicks_late=0, clicks_open=50):
    m = {"page_loads": {"count": 0}, "http_req_failed": {"rate": 0.0},
         "route_table_mismatch": {"count": 0},
         "iterations{scenario:navigate}": {"count": arrived},
         "iterations{scenario:composite_burst}": {"count": burst_arrived},
         "arrivals{scenario:navigate}": {"count": arrived},
         "arrivals_late{scenario:navigate}": {"count": late},
         "arrivals{scenario:composite_burst}": {"count": burst_arrived},
         "arrivals_late{scenario:composite_burst}": {"count": 0},
         "first_loads{scenario:composite_burst}": {"count": burst_first},
         "clicks{scenario:navigate}": {"count": clicks},
         "clicks_late{scenario:navigate}": {"count": clicks_late},
         "clicks_open_schedule{scenario:navigate}": {"count": clicks_open},
         "clicks{scenario:composite_burst}": {"count": 6},
         "clicks_late{scenario:composite_burst}": {"count": 0},
         "clicks_open_schedule{scenario:composite_burst}": {"count": 8},
         # the burst users click too: their page loads are more than their deep links
         "page_loads{scenario:composite_burst}": {"count": burst_first + 7}}
    return {"kind": "k6_summary", "config": {"navUsers": nav, "burstUsers": burst}, "metrics": m}


def _checks(summary):
    o = {"summary": summary, "stream": True, "points": {}, "frames": {"start": 1, "end": 1},
         "truncated": False, "exit": {"rc": 0}}
    return {c["check"]: c for c in k6eval.k6_checks(o)}


def test_every_user_arrived_on_time_passes():
    c = _checks(_summary(4, 0))
    assert c["k6.late_arrivals[navigate]"]["result"] == "PASS"
    assert c["k6.late_arrivals[composite_burst]"]["result"] == "PASS"
    assert c["k6.composite_burst_complete"]["result"] == "PASS"
    assert c["k6.late_clicks[navigate]"]["result"] == "PASS"
    assert c["k6.late_clicks[navigate]"]["value"] == 0.0
    # per-vu sessions never drop: the arrival check replaces the dropped one
    assert not any(k.startswith("k6.dropped_iterations") for k in c)


def test_late_first_loads_are_invalid():
    c = _checks(_summary(4, 1))["k6.late_arrivals[navigate]"]
    assert c["result"] == "FAIL" and c["value"] == 0.25
    assert "1 of 4 navigate first loads started more than 1 s after their arrival slot" in c["why"]


def test_a_user_who_never_arrived_is_invalid():
    c = _checks(_summary(3, 0))["k6.late_arrivals[navigate]"]
    assert c["result"] == "FAIL" and "only 3 of 4 navigate users arrived" in c["why"]


def test_the_burst_counts_its_deep_links_not_its_clicks():
    c = _checks(_summary(4, 0, burst_first=1))["k6.composite_burst_complete"]
    assert c["result"] == "FAIL" and c["value"] == 1


def test_late_clicks_are_invalid():
    """#839 gate: a starved k6 clicking late lowers the offered load; it is
    counted and gated like a late arrival."""
    c = _checks(_summary(4, 0, clicks=40, clicks_late=1))["k6.late_clicks[navigate]"]
    assert c["result"] == "FAIL" and c["value"] == 0.025
    assert "1 of 40 navigate route clicks started more than 1 s after their intended time" in c["why"]


def test_no_click_at_all_is_invalid():
    c = _checks(_summary(4, 0, clicks=0, clicks_open=50))["k6.late_clicks[navigate]"]
    assert c["result"] == "FAIL" and "no navigate route click ran" in c["why"]


def test_the_report_gives_the_achieved_click_rate():
    o = {"summary": _summary(4, 0, clicks=40, clicks_late=2, clicks_open=50), "points": {}}
    rep = k6eval.k6_report(o)
    assert rep["clicks"]["navigate"] == {"done": 40, "late": 2, "open_schedule": 50,
                                         "achieved_share_of_open_schedule": 0.8}
