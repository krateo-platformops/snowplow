"""The bench's base group reads the portal: declared in the run's installer values, held against the
pinned charts before any cluster exists (bench/portal_readers.py), and shown by the rehearsal's
preflight on a real cluster.

Run 26j (2026-11-15) stopped before load on three paid clusters: on installer 0.3.641 nothing granted
the bench's base group the portal's shell (the grant had come with the portal's demo user, on by a
default of the installer's values schema up to 0.3.625), the boot root answered 403 for every bench
user, the preflight logged "preflight OK" and the run stopped one step later on a message about
composite pages.

The installer fixtures are the PUBLISHED charts (0.3.545, 0.3.616, 0.3.641), rendered with real helm;
the portal and page charts are the suite's fake registry's, in the real charts' shape (conftest).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile

import pytest
import yaml

from bench import accept1126, ghcr_pull, installer_routes, installer_values, portal_readers, rbac_model, snowplow_env
from tests.conftest import FAKE_PAGE_READERS, FAKE_PORTAL_DEMO, FAKE_PORTAL_SHELL_READERS
from tests.test_installer_routes import CHARTS, SHA256
from tests.test_run606_real_sequence import BENCH_DIR, SCALEUSER_PASSWORD, harness  # noqa: F401  (fixture)

BENCH = BENCH_DIR
GROUP = rbac_model.BASE_GROUP
NS = ghcr_pull.SECRET_NS
INIT = "/call?resource=layouts&apiVersion=widgets.templates.krateo.io/v1beta1&name=app-shell&namespace=krateo-system"
needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="renders with the real helm")
DIG = "sha256:" + "ab" * 32
DIG2 = "sha256:" + "cd" * 32


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


def _values(tmp_path, doc=None):
    f = tmp_path / "installer-values.yaml"
    f.write_text(doc if doc is not None else installer_values.document("1.12.70"))
    return str(f)


def _creds():
    return ghcr_pull.creds(os.environ["GHCR_PULL_ENV_FILE"])


# ─── declared: the run's values carry the group, from the one definition ────

def test_every_runs_values_grant_the_base_group_the_shell_and_name_it_once():
    for doc in (installer_values.document("1.12.70"),
                installer_values.document("1.12.70", "1.8.44", {"GOGC": "100"})):
        v = yaml.safe_load(doc)
        assert v["componentValues"]["portal"] == {"readerGroups": [rbac_model.BASE_GROUP]}
        assert "enableCyberjokerUser" not in doc            # no demo user
    # the group is bench/rbac_model.py's, never a second literal in the module that writes the values
    src = (BENCH / "bench" / "installer_values.py").read_text()
    assert rbac_model.BASE_GROUP == "devs" and not re.search(r"\bdevs\b", src)
    assert installer_values.READER_GROUP is rbac_model.BASE_GROUP


def test_the_group_written_is_the_models_whatever_it_is(monkeypatch):
    monkeypatch.setattr(installer_values, "READER_GROUP", "bench-everyone")
    assert yaml.safe_load(installer_values.document("1.12.70"))["componentValues"]["portal"]["readerGroups"] == \
        ["bench-everyone"]


# ─── the installer's schema takes the key, or the run is refused by name ────

@pytest.mark.parametrize("version", ["0.3.545", "0.3.616", "0.3.641"])
def test_the_published_installers_take_the_key(pulled, version):
    ok, why = portal_readers.schema_accepts(str(pulled[version] / "installer"), "portal")
    assert ok and why == "its values schema declares componentValues.portal.readerGroups"


def _schema_chart(tmp_path, portal_props, closed=True):
    root = tmp_path / "installer"
    root.mkdir(parents=True)
    (root / "values.schema.json").write_text(json.dumps({"type": "object", "additionalProperties": False, "properties": {
        "componentValues": {"type": "object", "additionalProperties": False, "properties": {
            "portal": {"type": "object", "additionalProperties": not closed, "properties": portal_props}}}}}))
    return str(root)


def test_a_schema_without_the_key_refuses_it_by_name(tmp_path):
    # installer 0.3.422's form: componentValues.portal is a closed map without readerGroups
    ok, why = portal_readers.schema_accepts(_schema_chart(tmp_path, {"enableCyberjokerUser": {"type": "boolean"}}),
                                            "portal")
    assert not ok and why == ("its values schema declares no `readerGroups` under componentValues.portal and takes "
                              "no undeclared key there (additionalProperties: false)")
    # an open map takes it; a chart with no schema refuses nothing; a schema with no portal map at all is closed
    assert portal_readers.schema_accepts(_schema_chart(tmp_path / "open", {}, closed=False), "portal")[0]
    (tmp_path / "none" / "installer").mkdir(parents=True)
    assert portal_readers.schema_accepts(str(tmp_path / "none" / "installer"), "portal") == \
        (True, "the installer chart has no values schema: nothing refuses the key")
    ok, why = portal_readers.schema_accepts(_schema_chart(tmp_path / "other", {"readerGroups": {}}), "frontend")
    assert not ok and "declares no `frontend` under componentValues" in why


# ─── what a chart grants: its own reader rules, per namespace ───────────────

def _tgz(name, files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for fn, doc in {"Chart.yaml": {"apiVersion": "v2", "name": name, "version": "1.0.0"}, **files}.items():
            data = (doc if isinstance(doc, str) else yaml.safe_dump(doc)).encode()
            ti = tarfile.TarInfo(f"{name}/{fn}")
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def _grant(templates, values, chart_values=None):
    data = _tgz("portal", {"values.yaml": chart_values or {"readerGroups": [], "enableCyberjokerUser": False},
                           **{f"templates/{k}": v for k, v in templates.items()}})
    pin = {"name": "portal", "chart": "portal", "version": "1.0.0"}
    return portal_readers.grant(portal_readers._renderer(data, pin, "helm"), values, GROUP, NS)


def _binding(name, ns, group=GROUP, kind="Role"):
    scope = "" if kind == "ClusterRole" else f"\n  namespace: {ns}"
    return (f"---\napiVersion: rbac.authorization.k8s.io/v1\nkind: {kind}Binding\n"
            f"metadata:\n  name: {name}{scope}\nroleRef: {{apiGroup: rbac.authorization.k8s.io, kind: {kind}, "
              f"name: {name}}}\nsubjects:\n- {{kind: Group, name: {group}, apiGroup: rbac.authorization.k8s.io}}\n")


def _role(name, ns, rules, kind="Role"):
    scope = "" if kind == "ClusterRole" else f"\n  namespace: {ns}"
    return (f"---\napiVersion: rbac.authorization.k8s.io/v1\nkind: {kind}\nmetadata:\n  name: {name}{scope}\n"
            f"rules: {json.dumps(rules)}\n")


WIDGETS = {"apiGroups": ["widgets.templates.krateo.io"], "resources": ["*"], "verbs": ["get", "list"]}
RESTACTIONS = {"apiGroups": ["templates.krateo.io"], "resources": ["restactions"], "verbs": ["get"]}
SHELL = {"rbac.shell-readers.yaml": FAKE_PORTAL_SHELL_READERS}


@needs_helm
def test_a_group_named_in_the_reader_groups_is_granted_what_the_chart_gives_a_reader():
    g = _grant(SHELL, {"readerGroups": [GROUP]})
    assert g["granted"] and g["missing"] == {}
    # what a reader needs is the chart's own reader rules, in the release namespace: nothing restated here
    assert portal_readers.need_text(g["need"]) == (
        f"in {NS}: get *.widgets.templates.krateo.io, get restactions.templates.krateo.io, "
        "list *.widgets.templates.krateo.io")


@needs_helm
def test_a_group_that_is_not_bound_is_not_granted():
    # run 26j: the reader groups do not name the group, and nothing else binds it
    g = _grant(SHELL, {"readerGroups": ["admins"]})
    assert not g["granted"] and g["missing"] == {NS: [
        "get *.widgets.templates.krateo.io", "get restactions.templates.krateo.io",
        "list *.widgets.templates.krateo.io"]}
    assert not _grant(SHELL, {})["granted"]


@needs_helm
def test_a_group_bound_in_another_namespace_is_not_granted_the_shell():
    # the demo grants of a chart that binds the group in its demo namespace only
    demo = _role("demo", "demo-system", [WIDGETS, RESTACTIONS]) + _binding("demo", "demo-system")
    g = _grant({**SHELL, "demo.yaml": demo}, {"readerGroups": ["admins"]})
    assert not g["granted"] and list(g["missing"]) == [NS] and len(g["missing"][NS]) == 3


@needs_helm
def test_a_role_without_restactions_does_not_grant_the_shell():
    part = _role("widgets-only", NS, [WIDGETS]) + _binding("widgets-only", NS)
    g = _grant({**SHELL, "part.yaml": part}, {"readerGroups": ["admins"]})
    assert not g["granted"] and g["missing"] == {NS: ["get restactions.templates.krateo.io"]}
    # a Role narrowed to named objects does not cover a reader's rule either
    named = _role("named", NS, [WIDGETS, dict(RESTACTIONS, resourceNames=["sidebar-nav"])]) + _binding("named", NS)
    g = _grant({**SHELL, "named.yaml": named}, {"readerGroups": ["admins"]})
    assert not g["granted"] and g["missing"] == {NS: ["get restactions.templates.krateo.io"]}


@needs_helm
def test_a_grant_through_another_template_or_a_cluster_role_counts():
    # the demo user's grants, while an installer still turns them on: two Roles in the release namespace
    g = _grant({**SHELL, "demo.yaml": FAKE_PORTAL_DEMO}, {"readerGroups": [], "enableCyberjokerUser": True})
    assert g["granted"]
    wide = _role("wide", "", [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}], kind="ClusterRole") \
        + _binding("wide", "", kind="ClusterRole")
    assert _grant({**SHELL, "wide.yaml": wide}, {"readerGroups": []})["granted"]
    # a binding of ANOTHER group grants this one nothing
    other = _role("other", NS, [WIDGETS, RESTACTIONS]) + _binding("other", NS, group="admins")
    assert not _grant({**SHELL, "other.yaml": other}, {"readerGroups": []})["granted"]


@needs_helm
def test_a_chart_with_no_reader_template_gives_nothing_to_derive_a_need_from():
    g = _grant({"demo.yaml": FAKE_PORTAL_DEMO}, {"readerGroups": [GROUP], "enableCyberjokerUser": True})
    assert g["need"] == {} and not g["granted"]


# ─── the whole check, against the published installers and the registry ─────

@needs_helm
@pytest.mark.parametrize("version", ["0.3.616", "0.3.641"])
def test_the_published_installers_grant_the_group_the_shell_and_say_which_pages_do(pulled, tmp_path, version):
    out = tmp_path / "portal-readers.json"
    p = subprocess.run([sys.executable, "-m", "bench.portal_readers", "check", "--installer-pulled",
                        str(pulled[version]), "--values", _values(tmp_path), "--ghcr-env-file",
                        os.environ["GHCR_PULL_ENV_FILE"], "--out", str(out)],
                       cwd=BENCH, capture_output=True, text=True, timeout=600)
    assert p.returncode == 0, p.stderr
    rec = json.loads(out.read_text())
    pins = {c["name"]: c for c in yaml.safe_load(next((pulled[version] / "installer" / "files").glob(
        "component-pins*.yaml")).read_text())["components"]}
    assert rec["ok"] and rec["group"] == GROUP and rec["key"] == "componentValues.portal.readerGroups"
    assert rec["shell"]["version"] == pins["portal"]["version"] and rec["shell"]["release_namespace"] == NS
    assert rec["shell"]["granted"] and set(rec["shell"]["need"]) == {NS}
    # every chart the installer pins as depending on the portal, and turns on, was looked at: a page
    # chart (it declares reader groups of its own) or not
    deps = {n for n, c in pins.items() if "portal" in (c.get("deps") or [])}
    looked = {x["component"] for x in rec["pages"]} | set(rec["not_pages"])
    # (the three it leaves out are off for the bench's values: the service desk, and two of the agents')
    assert looked == deps - {"service-desk-page", "builder-gate", "krateo-capability-roles"}
    assert len([n for n in looked if n.endswith("-page")]) >= 13 and "portal-agents" in looked
    assert rec["granting"] == ["dashboard-page"] and rec["not_granting"] == ["settings-page"]
    assert f"portal readers: 1 of 2 page chart(s) grant `{GROUP}`: dashboard-page; not granting it: settings-page" \
        in p.stdout
    assert f"the portal chart portal {pins['portal']['version']} grants it the shell (in {NS}: get " in p.stdout


@needs_helm
def test_a_portal_chart_with_no_reader_groups_is_refused(pulled, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_PORTAL_CHART", "no-readers")
    with pytest.raises(portal_readers.Refused, match="binds a group named in its `readerGroups` to nothing"):
        portal_readers.check(str(pulled["0.3.641"]), _values(tmp_path), _creds())


@needs_helm
def test_a_run_whose_values_do_not_reach_the_installers_cr_is_refused(pulled, tmp_path):
    doc = yaml.safe_load(installer_values.document("1.12.70"))
    del doc["componentValues"]["portal"]
    with pytest.raises(portal_readers.Refused, match=r"does not carry `devs` in componentValues.portal.readerGroups "
                                                     r"\(it renders \[\]\)"):
        portal_readers.check(str(pulled["0.3.641"]), _values(tmp_path, yaml.safe_dump(doc)), _creds())


@needs_helm
def test_an_installer_that_overrides_the_reader_groups_away_is_refused_at_the_portal_chart(pulled, tmp_path,
                                                                                           monkeypatch):
    # the Installer CR carries the group, and the values the portal chart is rendered with do not
    # (stood in for by the chart's inputs: the group's grant must come out of the RENDER, not out of
    # the values document)
    real = portal_readers.components

    def without(installer_pulled, values_file, portal):
        shell, deps = real(installer_pulled, values_file, portal)
        return dict(shell, values=dict(shell["values"], readerGroups=["admins"])), deps
    monkeypatch.setattr(portal_readers, "components", without)
    with pytest.raises(portal_readers.Refused, match="does not grant the bench's base group `devs` what a reader of "
                                                     "the shell needs .*every bench user's boot call would answer 403"):
        portal_readers.check(str(pulled["0.3.641"]), _values(tmp_path), _creds())


@needs_helm
def test_a_run_no_page_grants_is_refused(pulled, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_PAGE_READERS_ONLY", "admins")
    with pytest.raises(portal_readers.Refused, match=r"none of the 2 page chart\(s\) this installer pins grants the "
                                                     r"bench's base group `devs` \(dashboard-page, settings-page\)"):
        portal_readers.check(str(pulled["0.3.641"]), _values(tmp_path), _creds())


@needs_helm
def test_a_registry_that_gives_no_answer_is_not_a_refusal(pulled, tmp_path, monkeypatch):
    def none(pin, creds, budget):
        raise portal_readers.NoAnswer(f"no answer from the registry for {pin['chart']}")
    monkeypatch.setattr(portal_readers, "fetch", none)
    with pytest.raises(portal_readers.NoAnswer):
        portal_readers.check(str(pulled["0.3.641"]), _values(tmp_path), _creds())


# ─── the real run: run606.sh against the fakes ──────────────────────────────

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def _gcloud(state):
    f = state / "calls.jsonl"
    return [json.loads(x)["argv"][:3] for x in f.read_text().splitlines()
            if json.loads(x)["tool"] == "gcloud"] if f.exists() else []


@needs_helm
def test_run606_declares_the_group_and_holds_it_against_the_charts_before_any_cluster(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert yaml.safe_load((out / "installer-values.yaml").read_text())["componentValues"]["portal"] == \
        {"readerGroups": [GROUP]}
    rec = json.loads((out / "portal-readers.json").read_text())
    assert rec["ok"] and rec["granting"] == ["dashboard-page"] and rec["not_granting"] == ["settings-page"]
    assert f"portal readers: `{GROUP}` declared in componentValues.portal.readerGroups" in log
    # at the source stage: before the login contract, the CRD check and the registry step
    assert log.index("portal readers: ") < log.index("=== step login-contract ===")
    # a dry pass keeps no password in what it wrote (the class identities' manifest was never sent
    # anywhere, so nothing had redacted it)
    for f in out.rglob("*"):
        if f.is_file():
            assert SCALEUSER_PASSWORD not in f.read_text(errors="replace"), f


@needs_helm
def test_a_dry_k6_pass_keeps_no_password_in_the_class_identities(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", LOAD_DRIVER="k6", USERS="20", THINK_S="1", COMPOSITE_BURST_USERS="5",
                BROWSER_SESSIONS="3")
    out = tmp / "out"
    assert (out / "menu-classes.yaml").exists(), p.stderr[-3000:]
    text = (out / "menu-classes.yaml").read_text()
    assert SCALEUSER_PASSWORD not in text and '"password": "<redacted>"' in text
    for f in out.rglob("*"):
        if f.is_file():
            assert SCALEUSER_PASSWORD not in f.read_text(errors="replace"), f


@needs_helm
@pytest.mark.parametrize("knob, why", [
    ({"FAKE_INSTALLER_SCHEMA": "no-reader-groups"},
     "source FAILED: portal readers: the installer chart does not take componentValues.portal.readerGroups: its "
     "values schema declares no `readerGroups` under componentValues.portal and takes no undeclared key there"),
    ({"FAKE_PORTAL_CHART": "no-readers"},
     "source FAILED: portal readers: the portal chart (portal 1.8.118) binds a group named in its `readerGroups` to "
     "nothing"),
    ({"FAKE_PAGE_READERS_ONLY": "admins"},
     "source FAILED: portal readers: none of the 2 page chart(s) this installer pins grants the bench's base group"),
    ({"FAKE_PORTAL_PIN": "none"},
     "source FAILED: portal readers: the installer pins no `portal` component that is on for this run's values"),
])
def test_run606_refuses_a_run_whose_users_could_not_read_the_portal_before_anything_is_created(
        harness, monkeypatch, knob, why):  # noqa: F811
    env, state, _, tmp = harness
    for k, v in knob.items():
        monkeypatch.setenv(k, v)                 # the suite's registry reads its knobs in THIS process
    p = _run606(env, **knob)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and why in log, log[-3000:]
    assert "=== step login-contract ===" not in log and "=== step up ===" not in log
    assert ["container", "clusters", "create"] not in _gcloud(state)
    assert not json.loads((tmp / "out" / "portal-readers.json").read_text())["ok"]


@needs_helm
def test_run606_stops_at_the_preflight_on_a_boot_root_that_is_not_200_and_names_it(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_PREFLIGHT="forbidden", COMPOSITE_BURST_USERS="200")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0
    assert (f"PREFLIGHT REFUSED: {INIT} -> 403: the portal's boot root did not answer 200 for this login (its groups "
            "are not granted the portal's shell; an empty body)") in log, log[-3000:]
    # the real cause, once: not "preflight OK", and not the message about composite pages one step later
    assert "] preflight OK" not in log and "has no composite page" not in log
    assert "=== step data ===" not in log


@needs_helm
def test_a_product_error_that_is_not_the_boot_root_is_recorded_and_the_run_goes_on(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert "PRODUCT ERROR (recorded, not fatal) /agents/new -> 500" in p.stderr
    assert ("preflight OK (the run: the boot root answered 200; a product error of any other call, if any, is "
            "recorded, not fatal)") in log


# ─── the rehearsal: the image, and the preflight ────────────────────────────

@needs_helm
def test_the_rehearsal_checks_the_image_and_runs_the_preflight_and_reports_both(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", COMPOSITE_BURST_USERS="200")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-3000:]
    block = log[log.index("=== rehearsal report ==="):log.index("=== end of rehearsal report ===")]
    for said in (f"the snowplow pod's image against the expected digest {DIG}: MATCH",
                 f"snowplow: image 'ghcr.io/krateo-platformops/snowplow:1.12.50', imageID "
                 f"'ghcr.io/krateo-platformops/snowplow@{DIG}', digest {DIG}",
                 f"portal readers, from the charts: `{GROUP}` declared in componentValues.portal.readerGroups",
                 f"portal readers, from the charts: 1 of 2 page chart(s) grant `{GROUP}`: dashboard-page",
                 f"preflight (as the bench's preflight user): OK; boot root {INIT} answered 200; 3 route(s) in the "
                 "menu; composite page(s): /observability, /compositions",
                 "  /dashboard: 200, ", "product error (recorded, not fatal) /agents/new -> 500"):
        assert said in block, (said, block)
    # the image check is the measuring run's own record, and the preflight its own step
    rec = json.loads((out / "image-digest.jsonl").read_text())
    assert rec["kind"] == "image_digest" and rec["result"] == "MATCH" and rec["expected"] == DIG
    assert "image digest MATCH: every live snowplow pod runs " + DIG in log
    assert "composite pages for the cold burst: /observability /compositions" in log
    # the go runtime read comes first, then the image, then the preflight; and still no load
    assert log.index("snowplow env verified from the running process") < log.index("image digest MATCH") < log.index("portal boot root")
    assert "=== step data ===" not in log and "=== step measure ===" not in log
    assert (state / "users").read_text().split() == ["scaleuser-0000"]


@needs_helm
def test_a_rehearsal_whose_pod_runs_another_image_fails_and_names_both_digests(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_IMAGE_DIGEST=DIG2)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "rehearsal complete" not in log
    assert f"VOID: a snowplow pod does not run {DIG}" in log
    block = log[log.index("=== rehearsal report ==="):log.index("=== end of rehearsal report ===")]
    assert f"the snowplow pod's image against the expected digest {DIG}: VOID (digest mismatch: " in block
    assert f"imageID 'ghcr.io/krateo-platformops/snowplow@{DIG2}', digest {DIG2}" in block
    # every check still ran: the report shows the preflight too
    assert "preflight (as the bench's preflight user): OK" in block


@needs_helm
@pytest.mark.parametrize("extra, said, line", [
    ({"FAKE_PREFLIGHT": "forbidden"},
     f"preflight (as the bench's preflight user): REFUSED; boot root {INIT} answered 403; 0 route(s) in the menu",
     f"PREFLIGHT REFUSED: {INIT} -> 403"),
    ({"FAKE_PREFLIGHT": "noroutes"},
     f"preflight (as the bench's preflight user): REFUSED; boot root {INIT} answered 200; 0 route(s) in the menu",
     "PREFLIGHT REFUSED: (sidebar menu) -> None: the shell rendered no sidebar Menu with routes"),
    ({"FAKE_PREFLIGHT": "nocomposite", "COMPOSITE_BURST_USERS": "200"},
     "3 route(s) in the menu; composite page(s): NONE",
     "PREFLIGHT REFUSED: COMPOSITE_BURST_USERS=200 but the live menu has no composite page"),
])
def test_a_rehearsal_whose_preflight_does_not_pass_fails_and_says_why(harness, extra, said, line):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", **extra)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "rehearsal complete" not in log, log[-3000:]
    assert line in log
    block = log[log.index("=== rehearsal report ==="):log.index("=== end of rehearsal report ===")]
    assert said in block, block
    # the cluster is deleted all the same
    assert ["container", "clusters", "delete"] in _gcloud(state)


def test_a_rehearsal_report_says_what_it_has_no_record_of(tmp_path):
    lines = snowplow_env.rehearsal_lines(str(tmp_path))
    for said in ("the snowplow pod's image: NO RECORD (image-digest.jsonl): the digest check did not run",
                 "portal readers, from the charts: NO RECORD (portal-readers.json)",
                 "preflight (as the bench's preflight user): NO RECORD (preflight.jsonl): the preflight Job gave no "
                 "result"):
        assert said in lines
    (tmp_path / "portal-readers.json").write_text(json.dumps({"kind": "portal_readers", "ok": False, "why": "x"}))
    assert "portal readers, from the charts: REFUSED: x" in snowplow_env.rehearsal_lines(str(tmp_path))


# ─── the acceptance run's prefixes are the route table's ────────────────────

@needs_helm
@pytest.mark.parametrize("version, authn", [("0.3.616", "/auth"), ("0.3.641", "/authn")])
def test_the_acceptance_run_logs_in_and_reads_at_the_charts_paths(pulled, tmp_path, monkeypatch, version, authn):
    f = tmp_path / "portal-routes.json"
    p = subprocess.run([sys.executable, "-m", "bench.installer_routes", "table", "--installer-pulled",
                        str(pulled[version]), "--values", _values(tmp_path), "--out", str(f)],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, p.stderr
    monkeypatch.setattr(accept1126, "_ROUTES", None)
    accept1126.use_routes(installer_routes.load(str(f)))
    assert accept1126.route_prefix(installer_routes.AUTHN_COMPONENT) == authn
    assert accept1126.route_prefix(installer_routes.SNOWPLOW_COMPONENT) == "/content"
    seen = []

    class R:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"accessToken": "t"}'

    def urlopen(req, **kw):
        seen.append(req.full_url)
        return R()
    monkeypatch.setattr(accept1126.urllib.request, "urlopen", urlopen)
    monkeypatch.setenv("HARNESS_USER", "u")
    monkeypatch.setenv("HARNESS_PASSWORD", "p")
    assert accept1126.login("https://p", accept1126._RequestLog()) == "t"
    assert seen == [f"https://p{authn}/basic/login"]


def test_the_acceptance_run_refuses_without_a_route_table_and_holds_no_path_of_its_own(tmp_path, monkeypatch):
    monkeypatch.setattr(accept1126, "_ROUTES", None)
    with pytest.raises(accept1126.PreflightFailed, match="no portal route table: pass --routes"):
        accept1126.route_prefix(installer_routes.SNOWPLOW_COMPONENT)
    accept1126.use_routes({"kind": "portal_routes", "paths": {"X": "/x"}, "routes": [{"component": "frontend",
                                                                                     "path": "/"}]})
    with pytest.raises(accept1126.PreflightFailed, match="routes no `authn` component"):
        accept1126.route_prefix(installer_routes.AUTHN_COMPONENT)
    # a table that is not one refuses the run before anything
    bad = tmp_path / "routes.json"
    bad.write_text("{}")
    assert accept1126.run(tmp_path / "run", "https://p", routes=str(bad)) == 2
    # the CLI takes no run without the table, and the module fixes no prefix
    src = (BENCH / "bench" / "accept1126.py").read_text()
    assert not re.search(r'"/(auth|authn|content)"', src) and 'p.add_argument("--routes", required=True' in src
