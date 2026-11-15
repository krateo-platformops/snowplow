"""The portal-origin gate holds the live frontend config against the route table of the installer chart
the run installs (bench/installer_routes.py), never against paths fixed in the bench.

The first paid rehearsal on installer 0.3.641 failed 26 minutes into a cluster: the frontend's config
named authn at /authn, as that chart routes it (0.3.639 moved it from /auth, whose prefix swallowed the
frontend's OIDC callback), the bench expected /auth, waited out its window and blamed the installer.

The fixtures are the PUBLISHED charts (0.3.616: authn at /auth; 0.3.641: /authn), rendered with real helm.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tarfile

import pytest
import yaml

from bench import installer_routes, installer_values, load606, origin_gate
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

BENCH = BENCH_DIR
CHARTS = BENCH / "tests" / "fixtures" / "installer_charts"
SHA256 = {"0.3.545": "ff89684302ec33852f390279f2cbd9d069fc65b39ce1ad566b5bf6907653efc1",
          "0.3.616": "614280d447301d7df1a7779e8618eeb079ccb99e7d28ef4bc3e8d466d0c9f285",
          "0.3.641": "d6692a9f3910d29b7b2ea7bf4fafa1d7499bab5686281259ce31fa25ecbc309b"}
AUTHN = {"0.3.616": "/auth", "0.3.641": "/authn"}
pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="renders with the real helm")


def _pull(tmp, version):
    src = CHARTS / f"installer-{version}.tgz"
    assert hashlib.sha256(src.read_bytes()).hexdigest() == SHA256[version]
    d = tmp / f"pulled-{version}"
    with tarfile.open(src) as tf:
        tf.extractall(d, filter="data")
    return d


@pytest.fixture(scope="module")
def pulled(tmp_path_factory):
    return {v: _pull(tmp_path_factory.mktemp("charts"), v) for v in SHA256}


@pytest.fixture(scope="module")
def tables(pulled, tmp_path_factory):
    """{version: the table, through the CLI run606's portal-routes step runs}."""
    d = tmp_path_factory.mktemp("tables")
    vals = d / "installer-values.yaml"
    vals.write_text(installer_values.document("1.12.70"))
    out = {}
    for v in AUTHN:
        f = d / f"portal-routes-{v}.json"
        p = subprocess.run([sys.executable, "-m", "bench.installer_routes", "table", "--installer-pulled",
                            str(pulled[v]), "--values", str(vals), "--out", str(f)],
                           cwd=BENCH, capture_output=True, text=True, timeout=300)
        assert p.returncode == 0, p.stderr
        out[v] = (json.loads(f.read_text()), f, p.stdout)
    return out


def _values(tmp_path, **exposure):
    v = yaml.safe_load(installer_values.document("1.12.70"))
    v["exposure"].update(exposure)
    f = tmp_path / "v.yaml"
    f.write_text(yaml.safe_dump(v))
    return str(f)


# ─── the table is the chart's ───────────────────────────────────────────────

@pytest.mark.parametrize("version", list(AUTHN))
def test_the_table_is_what_the_published_chart_routes(tables, version):
    t, _, said = tables[version]
    assert t["paths"] == {"AUTHN_API_BASE_URL": AUTHN[version], "SNOWPLOW_API_BASE_URL": "/content",
                          "EVENTS_API_BASE_URL": "/events", "EVENTS_PUSH_API_BASE_URL": "/events"}
    assert t["snowplow_key"] == "SNOWPLOW_API_BASE_URL"
    assert [(r["component"], r["path"], r["rewrite"], r["service"], r["port"]) for r in t["routes"]] == [
        ("authn", AUTHN[version], True, "authn", 8082), ("snowplow", "/content", True, "snowplow", 8081),
        ("frontend", "/", False, "frontend", 8080), ("krateo-sse-proxy", "/events", True, "sse-proxy", 8080)]
    assert f"portal routes, from the installer chart: {AUTHN[version]} -> authn (authn:8082, AUTHN_API_BASE_URL)" in said
    # 0.3.639 on carries its own route-table define, rendered too and equal; before it the routes are inline
    assert ("equal to its own inst.platformRoutes" in t["source"]) == (version == "0.3.641")
    assert ("has no inst.platformRoutes define" in t["source"]) == (version == "0.3.616")
    assert {"0.3.616": "files/component-pins.yaml", "0.3.641": "files/component-pins.min.yaml"}[version] in t["source"]


def test_the_only_route_that_moved_between_the_two_charts_is_authns(tables):
    a, b = ({r["component"]: r for r in tables[v][0]["routes"]} for v in ("0.3.616", "0.3.641"))
    assert set(a) == set(b)
    assert {n: (a[n]["path"], b[n]["path"]) for n in a if a[n] != b[n]} == {"authn": ("/auth", "/authn")}
    for n in a:                                   # nothing else of a route differs: backend, port, rewrite, keys
        assert {k: v for k, v in a[n].items() if k != "path"} == {k: v for k, v in b[n].items() if k != "path"}


def _copy(pulled, tmp_path, version="0.3.641"):
    d = tmp_path / "chart"
    shutil.copytree(pulled[version], d)
    return d


def _edit(path, old, new, count=1):
    s = path.read_text()
    assert s.count(old) >= 1, old
    path.write_text(s.replace(old, new, count))


def _refused(chart, values) -> str:
    with pytest.raises(installer_routes.Refused) as e:
        installer_routes.table(str(chart), values)
    return str(e.value)


def test_a_table_the_chart_does_not_give_is_refused(pulled, tmp_path):
    # not path mode: the portal is not one origin with a path per component
    assert "the run's values are not path mode (exposure.type='Gateway', gatewayMode='host')" in _refused(
        pulled["0.3.641"], _values(tmp_path, gatewayMode="host"))
    assert "not path mode (exposure.type='LoadBalancer'" in _refused(pulled["0.3.641"], _values(tmp_path, type="LoadBalancer"))
    vals = _values(tmp_path)
    # a chart that cannot say which components are on
    bare = _copy(pulled, tmp_path)
    helpers = bare / "installer" / "templates" / "_helpers.tpl"
    helpers.write_text(helpers.read_text().replace('define "inst.compEnabled"', 'define "inst.compOn"')
                       .replace('define "inst.featureEnabled"', 'define "inst.featureOn"'))
    assert "has neither an inst.compEnabled nor an inst.featureEnabled define" in _refused(bare, vals)
    assert "no pulled installer chart at" in _refused(tmp_path / "nowhere", vals)
    # the portal's feature off: no route to hold a config against
    off = yaml.safe_load(installer_values.document("1.12.70"))
    off.setdefault("features", {})["portal"] = False
    (tmp_path / "off.yaml").write_text(yaml.safe_dump(off))
    assert "renders no portal route with a frontend key" in _refused(pulled["0.3.641"], str(tmp_path / "off.yaml"))


def test_a_chart_from_before_comp_enabled_is_asked_by_its_own_feature_rule(pulled, tmp_path):
    """0.3.545 (like 0.3.422, which several dry-pass tests pull from the real registry) has no
    inst.compEnabled: its route table gates on inst.featureEnabled of the pin's feature."""
    t = installer_routes.table(str(pulled["0.3.545"]), _values(tmp_path))
    assert t["paths"]["AUTHN_API_BASE_URL"] == "/auth" and "its own inst.featureEnabled" in t["source"]
    assert [r["component"] for r in t["routes"]] == ["authn", "snowplow", "frontend", "krateo-sse-proxy"]
    off = yaml.safe_load(installer_values.document("1.12.70"))
    off.setdefault("features", {})["portal"] = False
    (tmp_path / "off.yaml").write_text(yaml.safe_dump(off))
    assert "renders no portal route with a frontend key" in _refused(pulled["0.3.545"], str(tmp_path / "off.yaml"))


def test_a_chart_with_comp_enabled_is_asked_by_it_not_by_the_feature_alone(pulled, tmp_path):
    """inst.compEnabled also turns a component on through its `alsoFeatures`; the older rule
    (inst.featureEnabled of the pin's one feature) would call this authn off and drop its route."""
    chart = _copy(pulled, tmp_path)
    f = chart / "installer" / "files" / "component-pins.min.yaml"
    doc = yaml.safe_load(f.read_text())
    authn = next(c for c in doc["components"] if c["name"] == "authn")
    authn["feature"], authn["alsoFeatures"] = "specialistAgents", ["portal"]      # off, and on through portal
    f.write_text(yaml.safe_dump(doc))
    t = installer_routes.table(str(chart), _values(tmp_path))
    assert t["paths"]["AUTHN_API_BASE_URL"] == "/authn" and "its own inst.compEnabled" in t["source"]


def _pins_of(chart):
    f = chart / "installer" / "files" / "component-pins.min.yaml"
    return f, yaml.safe_load(f.read_text())


def test_a_frontend_key_given_two_paths_by_the_pins_is_refused(pulled, tmp_path):
    chart = _copy(pulled, tmp_path)
    f, doc = _pins_of(chart)
    next(c for c in doc["components"] if c["name"] == "krateo-sse-proxy")["configKeys"] = [
        "EVENTS_API_BASE_URL", "AUTHN_API_BASE_URL"]
    f.write_text(yaml.safe_dump(doc))
    assert "give the frontend key AUTHN_API_BASE_URL two paths (/authn and /events)" in _refused(chart, _values(tmp_path))


@pytest.mark.parametrize("change", ["no key", "no path"])
def test_a_table_with_no_snowplow_route_is_refused(pulled, tmp_path, change):
    """The browser and the probes reach the portal's API through snowplow's URL: a chart that routes no
    snowplow with a frontend key gives them nothing to call."""
    chart = _copy(pulled, tmp_path)
    f, doc = _pins_of(chart)
    snow = next(c for c in doc["components"] if c["name"] == "snowplow")
    snow.pop("configKeys" if change == "no key" else "gatewayPath")
    f.write_text(yaml.safe_dump(doc))
    assert "routes no snowplow component with a frontend key" in _refused(chart, _values(tmp_path))


def test_a_chart_whose_own_route_table_and_pins_disagree_is_refused_naming_both(pulled, tmp_path):
    chart = _copy(pulled, tmp_path)
    _edit(chart / "installer" / "templates" / "_helpers.tpl", '"pathPrefix" $d.gatewayPath', '"pathPrefix" "/auth"')
    why = _refused(chart, _values(tmp_path))
    assert "the installer chart's own route table (inst.platformRoutes) and its pins " \
           "(files/component-pins.min.yaml: gatewayPath) disagree" in why
    assert "authn: routes at '/auth', pin says '/authn'" in why


@pytest.mark.parametrize("path, clash", [("/authn/content", "authn at /authn, a prefix that also matches /authn/content"),
                                         ("/authn", "at /authn, a prefix that also matches /authn")])
def test_a_prefix_that_swallows_another_components_path_is_refused(pulled, tmp_path, path, clash):
    """The negative the installer's own fix rested on: no component's prefix matches a path another
    component is routed at. /auth does not match /authn (a prefix matches whole path elements)."""
    chart = _copy(pulled, tmp_path)
    pins = chart / "installer" / "files" / "component-pins.min.yaml"
    doc = yaml.safe_load(pins.read_text())
    next(c for c in doc["components"] if c["name"] == "snowplow")["gatewayPath"] = path
    pins.write_text(yaml.safe_dump(doc))
    assert clash in _refused(chart, _values(tmp_path))


def test_auth_and_authn_side_by_side_are_two_routes(pulled, tmp_path):
    """/auth is not a prefix of /authn: a chart may route both (the check is by path element)."""
    chart = _copy(pulled, tmp_path)
    pins = chart / "installer" / "files" / "component-pins.min.yaml"
    doc = yaml.safe_load(pins.read_text())
    next(c for c in doc["components"] if c["name"] == "snowplow")["gatewayPath"] = "/auth"
    pins.write_text(yaml.safe_dump(doc))
    t = installer_routes.table(str(chart), _values(tmp_path))
    assert t["paths"]["SNOWPLOW_API_BASE_URL"] == "/auth" and t["paths"]["AUTHN_API_BASE_URL"] == "/authn"


# ─── the gate holds the live config against the chart's table ───────────────

# the frontend's config.json of the real rehearsal cluster on installer 0.3.641 (2026-11-15), its URL keys
LIVE_641 = {"api": {"AUTHN_API_BASE_URL": "http://34.7.89.19:80/authn",
                    "SNOWPLOW_API_BASE_URL": "http://34.7.89.19:80/content",
                    "EVENTS_API_BASE_URL": "http://34.7.89.19:80/events",
                    "EVENTS_PUSH_API_BASE_URL": "http://34.7.89.19:80/events"}}


def _cli(config, routes_file):
    return subprocess.run([sys.executable, "-m", "bench.load606", "portal-origin", "--routes", str(routes_file)],
                          input=json.dumps(config), cwd=BENCH, capture_output=True, text=True, timeout=60)


def test_the_rehearsals_real_config_passes_against_its_own_charts_table(tables):
    p = _cli(LIVE_641, tables["0.3.641"][1])
    assert p.returncode == 0, p.stderr
    assert "export PORTAL_ORIGIN=http://34.7.89.19:80" in p.stdout
    assert "export PORTAL_SNOWPLOW_URL=http://34.7.89.19:80/content" in p.stdout


@pytest.mark.parametrize("version, live", [("0.3.616", "/authn"), ("0.3.641", "/auth")])
def test_a_config_whose_path_is_not_the_charts_says_which_two_disagree(tables, version, live):
    cfg = {"api": dict(LIVE_641["api"], AUTHN_API_BASE_URL=f"http://34.7.89.19:80{live}")}
    p = _cli(cfg, tables[version][1])
    assert p.returncode == 1 and p.stdout == ""
    assert (f"AUTHN_API_BASE_URL='http://34.7.89.19:80{live}': path '{live}', and the installer chart routes it at "
            f"'{AUTHN[version]}'") in p.stderr
    r = load606.portal_origin(json.dumps(cfg), tables[version][0])
    assert r["wrong"] == "paths"
    # not yet rendered, and several origins, are their own kinds: the gate waits on those
    assert load606.portal_origin(json.dumps({"api": {}}), tables[version][0])["wrong"] == "incomplete"
    split = {"api": dict(cfg["api"], EVENTS_API_BASE_URL="http://localhost:8083/events")}
    assert load606.portal_origin(json.dumps(split), tables[version][0])["wrong"] == "origins"


def test_no_path_is_fixed_in_the_gate():
    """The table is given: neither the gate nor the config check carries a route path of its own."""
    for mod in ("load606.py", "origin_gate.py"):
        src = (BENCH / "bench" / mod).read_text()
        code = "\n".join(ln.split("#", 1)[0] for ln in src.splitlines())
        for path in ('"/auth"', '"/authn"', '"/content"', '"/events"'):
            assert path not in code, (mod, path)
    assert not hasattr(load606, "PORTAL_PATHS")


def _gate_files(tmp_path, cfg):
    svc, fe, dep = tmp_path / "svc.json", tmp_path / "fe.json", tmp_path / "dep.json"
    svc.write_text(json.dumps({"status": {"loadBalancer": {"ingress": [{"ip": "34.7.89.19"}]}}}))
    fe.write_text(json.dumps({"items": [{"spec": {"config": cfg}}]}))
    dep.write_text(json.dumps({"items": [{"metadata": {"name": "installers-v0-3-641-controller"},
                                          "status": {"conditions": [{"type": "Available", "status": "True"}]}}]}))
    return svc, fe, dep


def test_the_gates_reading_and_verdict_name_the_charts_paths(tables, tmp_path):
    """Through the gate's CLI, as run606 calls it: the 0.3.641 cluster's config against 0.3.616's table (what
    the bench did with its fixed paths) is `wrong_paths`, and the verdict says which two things disagree."""
    svc, fe, dep = _gate_files(tmp_path, LIVE_641["api"])
    out = {}
    for v in AUTHN:
        p = subprocess.run([sys.executable, "-m", "bench.origin_gate", "reading", "--t", "5", "--gateway-json", str(svc),
                            "--gateway-rc", "0", "--frontends-json", str(fe), "--frontends-rc", "0",
                            "--deployments-json", str(dep), "--routes", str(tables[v][1])],
                           cwd=BENCH, capture_output=True, text=True, timeout=60)
        assert p.returncode == 0, p.stderr
        out[v] = json.loads(p.stdout)
    assert out["0.3.641"]["rendered"] == "same_origin" and out["0.3.616"]["rendered"] == "wrong_paths"
    (tmp_path / "wait.jsonl").write_text(json.dumps(out["0.3.616"]) + "\n")
    (tmp_path / "resync.json").write_text(json.dumps({"seconds": 60, "source": "x"}))
    p = subprocess.run([sys.executable, "-m", "bench.origin_gate", "verdict", "--readings", str(tmp_path / "wait.jsonl"),
                        "--resync", str(tmp_path / "resync.json"), "--why", "w", "--routes", str(tables["0.3.616"][1])],
                       cwd=BENCH, capture_output=True, text=True, timeout=60)
    v = json.loads(p.stdout)
    assert v["kind"] == "installer_defect"
    assert "the Frontend composition the installer renders and the installer chart's own route table disagree" in v["message"]
    assert "path '/authn', and the installer chart routes it at '/auth'" in v["message"]
    assert "The gateway routes AUTHN_API_BASE_URL at /auth, EVENTS_API_BASE_URL at /events" in v["message"]
    # a config that is simply not rendered yet names the chart's paths too, and no path of the bench's own
    k, m = origin_gate.verdict([dict(out["0.3.641"], rendered="not_same_origin", rendered_why="3 origins")],
                               {"seconds": 60, "source": "x"}, "w", tables["0.3.641"][0])
    assert k == "installer_defect" and "not one origin with AUTHN_API_BASE_URL at /authn," in m and "/auth," not in m
    # the gate refuses to run without the table
    p = subprocess.run([sys.executable, "-m", "bench.origin_gate", "verdict", "--readings", str(tmp_path / "wait.jsonl"),
                        "--resync", str(tmp_path / "resync.json"), "--why", "w", "--routes", str(tmp_path / "none.json")],
                       cwd=BENCH, capture_output=True, text=True, timeout=60)
    assert p.returncode == 2 and "portal route table could not be read" in p.stderr


# ─── the real run: run606.sh against the fakes ──────────────────────────────

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def test_the_dry_pass_derives_the_table_before_any_cluster_and_uses_its_paths(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", FAKE_AUTHN_PATH="/authn", FAKE_INSTALLER_PINS="min")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert log.index("=== step portal-routes ===") < log.index("=== step reap ===") < log.index("=== step up ===")
    assert "portal routes, from the installer chart: / -> frontend (frontend:8080); /authn -> authn (authn:8082, " \
           "AUTHN_API_BASE_URL); /content -> snowplow" in log
    assert json.loads((out / "portal-routes.json").read_text())["paths"]["AUTHN_API_BASE_URL"] == "/authn"
    assert f"portal-origin --routes {out}/portal-routes.json" in log
    assert "PORTAL_SNOWPLOW_URL=http://dry-run-gateway/content" in (out / "portal-origin.env").read_text()


@pytest.mark.parametrize("path", ["/auth", "/authn"])
def test_a_run_goes_through_the_gate_at_whatever_path_its_chart_routes_authn(harness, path):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_AUTHN_PATH=path, **({"FAKE_INSTALLER_PINS": "min"} if path == "/authn" else {}))
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    assert f"{path} -> authn (authn:8082, AUTHN_API_BASE_URL)" in log
    assert "portal origin (config.json): http://10.0.0.9; snowplow at http://10.0.0.9/content" in log
    assert "portal not same-origin" not in log


def test_a_platform_that_names_a_path_its_chart_does_not_route_fails_at_once_naming_both(harness):  # noqa: F811
    """The chart routes authn at /authn and the live frontend config says /auth: the installer's two
    renderings disagree. The gate does not wait out its window on it."""
    env, state, _, tmp = harness
    p = _run606(env, FAKE_AUTHN_PATH="/authn", FAKE_LIVE_AUTHN_PATH="/auth")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "run complete" not in log, log[-3000:]
    assert "portal not same-origin, and waiting cannot fix it: portal not same-origin: AUTHN_API_BASE_URL=" \
           "'http://10.0.0.9/auth': path '/auth', and the installer chart routes it at '/authn'" in log
    assert log.count("portal not same-origin yet") == 0                       # no poll was waited out
    assert "portal origin FAILED: installer_defect: INSTALLER DEFECT: the Frontend composition the installer renders " \
           "and the installer chart's own route table disagree" in log
    assert "=== step env ===" not in log


def test_a_chart_with_no_route_table_stops_the_run_before_any_cluster(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", FAKE_AUTHN_PATH="/content")       # two components at one prefix
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "portal-routes FAILED" in log, log[-2000:]
    assert "a prefix that also matches /content" in log + p.stderr
    assert "=== step reap ===" not in log and "=== step up ===" not in log
