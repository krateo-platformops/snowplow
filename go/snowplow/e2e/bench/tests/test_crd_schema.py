"""The dry pass validates every manifest the run applies against the CRDs the
installer version pins, before any cluster exists (bench/crd_schema.py,
run606.sh s_schema). Run 17 attempt 4's 670 avatar-less Users would have
failed it for free.

The CRD charts are the pinned ones, vendored (tests/fixtures/crd_charts:
authn-crds 0.27.3, snowplow-crds 1.12.56 and core-provider-crds 2.13.40, as
installer 0.3.468 pins them), served by the harness's fake registry.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
import yaml

from bench import authn_user, crd_schema, data_manifests
from tests.test_606 import _run606
from tests.test_run606_real_sequence import BENCH_DIR, CRD_CHARTS, harness  # noqa: F401  (fixture)


def _crd(chart, kind):
    for f in sorted((CRD_CHARTS / chart / "templates").iterdir()):
        d = yaml.safe_load(f.read_text())
        if d["spec"]["names"]["kind"] == kind:
            return d
    raise KeyError(kind)


def _schema(chart, kind):
    return _crd(chart, kind)["spec"]["versions"][0]["schema"]["openAPIV3Schema"]


USER = _schema("authn-crds", "User")


def _user(**drop):
    u = authn_user.user("u", "krateo-system", display_name="d", groups=["devs"],
                        avatar=authn_user.avatar_url(1), labels={})
    for k in drop:
        u["spec"].pop(k)
    return u


# ─── the validator: the API server's rules for a create ────────────────────

def test_attempt_4s_user_is_refused_for_the_field_the_api_server_named():
    errs, cel, uns = crd_schema.validate(_user(avatarURL=1), USER)
    assert errs == ["spec.avatarURL: Required value"] and not cel and not uns
    assert crd_schema.validate(_user(), USER) == ([], [], [])


def test_strict_validation_refuses_an_undeclared_field_and_a_wrong_type():
    u = _user()
    u["spec"]["nickname"] = "x"
    u["spec"]["groups"] = "devs"
    errs, _, _ = crd_schema.validate(u, USER)
    assert "spec.nickname: field not declared in schema" in errs
    assert any(e.startswith('spec.groups: Invalid value: "string"') and "must be of type array" in e for e in errs)


def test_metadata_is_the_api_servers_own_validation_not_the_crds():
    u = _user()
    u["metadata"]["annotations"] = {"a": "b"}
    assert crd_schema.validate(u, USER)[0] == []


SCHEMA = {"type": "object", "properties": {
    "spec": {"type": "object", "required": ["mode", "port"], "properties": {
        "mode": {"type": "string", "enum": ["a", "b"], "default": "a"},
        "port": {"x-kubernetes-int-or-string": True, "anyOf": [{"type": "integer"}, {"type": "string"}]},
        "tags": {"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string"}},
        "free": {"type": "object", "x-kubernetes-preserve-unknown-fields": True},
        "note": {"type": "string", "nullable": False},
        "size": {"type": "integer", "format": "int32", "minimum": 1},
        "rule": {"type": "object", "x-kubernetes-validations": [{"rule": "self.x > 0"}],
                 "properties": {"x": {"type": "integer"}}},
        "move": {"type": "object", "x-kubernetes-validations": [{"rule": "self == oldSelf"}],
                 "properties": {"x": {"type": "integer"}}},
        "odd": {"type": "string", "format": "uuid"}}}}}


def _v(spec):
    return crd_schema.validate({"spec": spec}, SCHEMA)


def test_defaults_apply_before_required_and_nulls_are_pruned():
    errs, _, _ = _v({"port": 8080, "note": None})
    assert errs == []                                  # mode defaulted; note's null pruned


def test_value_rules():
    errs, _, _ = _v({"mode": "c", "port": True, "tags": ["x", "x"], "size": 0})
    assert "spec.mode: Unsupported value: \"c\"" in errs
    assert any(e.startswith("spec.port: Invalid value") for e in errs)
    assert "spec.tags: Duplicate value: \"x\"" in errs
    assert any(e.startswith("spec.size: Invalid value: should be greater than or equal to 1") for e in errs)
    assert _v({"port": "http", "free": {"anything": {"at": 1}}})[0] == []


def test_a_cel_rule_reached_is_named_a_transition_rule_is_not_and_neither_refuses():
    errs, cel, uns = _v({"port": 1, "rule": {"x": 0}, "move": {"x": 1}})
    assert errs == [] and cel == ["spec.rule: self.x > 0"] and uns == []
    assert _v({"port": 1})[1] == []                    # a rule at a node the document lacks is never evaluated


def test_a_keyword_it_does_not_implement_is_named_where_reached():
    assert _v({"port": 1, "odd": "u"})[2] == ["spec.odd: format uuid"]
    assert _v({"port": 1})[2] == []


# ─── the CRD sources: what the installer version pins ──────────────────────

def _installer(tmp_path, pins, deps=()):
    d = tmp_path / "installer"
    (d / "files").mkdir(parents=True)
    (d / "charts").mkdir()
    (d / "Chart.yaml").write_text(json.dumps({"name": "installer", "version": "0.3.468",
                                              "dependencies": [{"name": n, "version": v} for n, v in deps]}))
    (d / "values.yaml").write_text("ociRepo: oci://ghcr.io/krateo-platformops/charts\n")
    (d / "files" / "component-pins.yaml").write_text(json.dumps({"components": pins}))
    return d


def test_the_sources_are_the_vendored_dependencies_and_every_pinned_crd_component(tmp_path):
    d = _installer(tmp_path, [
        {"name": "authn-crd", "kind": "AuthnCrds", "chart": "authn-crds", "version": "0.27.3"},
        {"name": "authn", "kind": "Authn", "chart": "authn", "version": "0.27.3"},
        {"name": "gateway-api-crds", "kind": "GatewayAPICrds", "version": "0.10.0",
         "repo": "oci://ghcr.io/krateo-blueprints/charts"}], deps=[("core-provider-crds", "2.13.40")])
    (d / "charts" / "core-provider-crds").mkdir()
    got = crd_schema.crd_sources(str(d))
    assert [(s["chart"], s["version"], s["from"], s.get("ref")) for s in got] == [
        ("core-provider-crds", "2.13.40", "vendored", None),
        ("authn-crds", "0.27.3", "pin", "oci://ghcr.io/krateo-platformops/charts/authn-crds"),
        ("gateway-api-crds", "0.10.0", "pin", "oci://ghcr.io/krateo-blueprints/charts/gateway-api-crds")]
    assert got[0]["path"] == str(d / "charts" / "core-provider-crds")


def test_a_pull_must_bring_the_pinned_version(tmp_path):
    def run(argv, **kw):
        dest = argv[argv.index("-d") + 1]
        (tmp_path / dest / "authn-crds").mkdir(parents=True)
        (tmp_path / dest / "authn-crds" / "Chart.yaml").write_text("name: authn-crds\nversion: 0.27.2\n")
        return subprocess.CompletedProcess(argv, 0, "", "")
    got = crd_schema.pull({"chart": "authn-crds", "version": "0.27.3", "ref": "r"}, str(tmp_path / "w"), run=run)
    assert got["error"] == "pulled version '0.27.2' != pinned '0.27.3'"


def _sources():
    return [crd_schema.read_crds({"chart": c, "version": v, "from": "pin", "path": str(CRD_CHARTS / c)})
            for c, v in (("authn-crds", "0.27.3"), ("snowplow-crds", "1.12.56"), ("core-provider-crds", "2.13.40"))]


# ─── the check ─────────────────────────────────────────────────────────────

def test_the_data_step_documents_are_valid_or_named_against_the_pinned_crds():
    res = crd_schema.check(data_manifests.documents(namespaces=2, users=3, fresh_modes=("annotation", "status")),
                           _sources())
    st = {e["kind"]: e["status"] for e in res["kinds"]}
    assert res["ok"], res
    assert st == {"CompositionDefinition.core.krateo.io/v1alpha1": "valid",
                  "GithubScaffoldingWithCompositionPage.composition.krateo.io/v1-2-2": "not validated (generated CRD)",
                  "RESTAction.templates.krateo.io/v1": "not faithfully validated",
                  "User.basic.authn.krateo.io/v1alpha1": "valid"}
    ra = next(e for e in res["kinds"] if e["kind"].startswith("RESTAction"))
    assert len(ra["cel_not_evaluated"]) == 3 and all(c.startswith("spec.api[0]: ") for c in ra["cel_not_evaluated"])


def test_a_kind_no_pinned_chart_defines_fails_the_check():
    docs = [("x", {"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Paragraph", "spec": {}})]
    res = crd_schema.check(docs, _sources())
    assert not res["ok"] and res["kinds"][0]["status"] == "MISSING"
    assert any(ln.startswith("crd check FAILED: Paragraph.widgets.templates.krateo.io/v1beta1")
               for ln in crd_schema.report_lines(res))


def test_the_refusal_line_counts_and_names_no_object():
    docs = [("the fresh identity classes", _user(avatarURL=1) | {"metadata": {"name": f"scaleuser-fresh-1-{i:03d}"}})
            for i in range(670)]
    res = crd_schema.check(docs, _sources())
    lines = crd_schema.report_lines(res)
    assert not res["ok"]
    assert ("crd check FAILED: User.basic.authn.krateo.io/v1alpha1 (670 document(s): the fresh identity classes): "
            "the API server would refuse 670 x spec.avatarURL: Required value (authn-crds 0.27.3)") in lines
    assert not any("scaleuser" in ln for ln in lines)


# ─── run606: the dry pass, before any cluster exists ───────────────────────

def test_the_dry_pass_refuses_a_field_the_pinned_crd_requires_before_any_cluster(tmp_path):
    """The registry serves an authn-crds whose User requires one more field:
    the dry pass fails at the schema step, and nothing after it runs."""
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", LOAD_DRIVER="k6", FAKE_CRD_REQUIRE="User:nickname")
    assert p.returncode != 0
    err = p.stderr
    assert "crd check FAILED: User.basic.authn.krateo.io/v1alpha1" in err
    assert "x spec.nickname: Required value (authn-crds 0.27.3)" in err
    assert "the fresh identity classes" in err.split("crd check FAILED: User", 1)[1].split("\n", 1)[0]
    assert "schema FAILED: the manifests of this run do not satisfy the CRDs installer 0.0.0 pins" in err
    assert "=== step reap ===" not in err and "gke-up" not in p.stdout + err
    recs = [json.loads(x) for x in (tmp_path / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert [r["step"] for r in recs] == ["schema"]
    assert all("crd check FAILED" in x for x in recs[0]["stderr"])


def test_the_dry_pass_validates_what_the_run_applies(tmp_path):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", LOAD_DRIVER="k6")
    assert p.returncode == 0, p.stderr[-3000:]
    res = json.loads((tmp_path / "out" / "crd-check.json").read_text())
    user = next(e for e in res["kinds"] if e["kind"] == "User.basic.authn.krateo.io/v1alpha1")
    assert user["status"] == "valid" and "the fresh identity classes" in user["applied_by"]
    assert "crd check: OK" in p.stderr


def test_every_non_builtin_kind_the_run_applies_is_in_the_check(harness):  # noqa: F811
    """The falsifier for the renderers' list: the harness's API server records
    every object applied; each kind not built in is one the check covered."""
    env, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    applied = {tuple(json.loads(x)) for x in (state / "applied-kinds.jsonl").read_text().splitlines()}
    custom = {f"{k}.{api.rpartition('/')[0]}/{api.rpartition('/')[2]}" for api, k in applied
              if not crd_schema._BUILTIN_GROUP.search(api.rpartition("/")[0])}
    checked = {e["kind"] for e in json.loads((tmp / "out" / "crd-check.json").read_text())["kinds"]}
    assert custom and custom <= checked, custom - checked


@pytest.mark.parametrize("chart", ["authn-crds", "snowplow-crds", "core-provider-crds"])
def test_the_vendored_crd_charts_are_the_versions_installer_0_3_468_pins(chart):
    want = {"authn-crds": "0.27.3", "snowplow-crds": "1.12.56", "core-provider-crds": "2.13.40"}[chart]
    assert yaml.safe_load((CRD_CHARTS / chart / "Chart.yaml").read_text())["version"] == want


def test_the_schema_step_renders_without_a_kubectl_call():
    """It runs before any cluster exists: nothing it imports may run kubectl
    at import (bench/cluster.py's context guard does). The harness's
    ambient-context test sees such a call; this one names the module."""
    code = ("import sys; from bench import crd_schema, data_manifests, fresh_classes; "
            "data_manifests.documents(namespaces=1, users=1); "
            "print(sorted(m for m in sys.modules if m.startswith('bench.')))")
    p = subprocess.run([sys.executable, "-c", code], cwd=BENCH_DIR, capture_output=True, text=True,
                       env={"PATH": "/nonexistent", "PYTHONPATH": str(BENCH_DIR)}, timeout=120)
    assert p.returncode == 0, p.stderr
    assert "bench.cluster" not in p.stdout and "bench.lifecycle" not in p.stdout, p.stdout
