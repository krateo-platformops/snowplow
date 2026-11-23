"""The bench's installer features are krateo-057's, from ONE record (bench/installer_features.py).

The owner's ruling (2026-11-22): "the bench's install must match 057's installer features". Run 30's
values set coreAgents and specialistAgents off by hand while 057 runs both: the bench served no
kagent.dev agents/remotemcpservers nor review.krateo.io proposals, the dashboard-inbox RESTAction's
steps answered 404, and snowplow declined to cache four dashboard widgets (a bench-only tail).

What is held here, each with its falsifier (break the derivation and the test fails):
  - the committed record IS the reading of 2026-11-23 (feature keys and booleans of 057's Installer
    CR, installer 0.3.703, READ_VERSION): a record edited by hand, one feature flipped, fails;
  - the run's values write every feature of the record, each at 057's value or at a named deviation's:
    a record that changes, changes the values (they are derived, not a second list);
  - the chart check refuses an installer that cannot express 057's features (0.3.641: no internalTls)
    or declares one the record does not name; the record/drift tools keep nothing but booleans;
  - what a feature turns on is the installer's inst.compEnabled: `feature` OR `alsoFeatures`.
"""

from __future__ import annotations

import copy
import io
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

from bench import diag606, installer_component_values, installer_features, installer_values

BENCH_DIR = Path(__file__).resolve().parent.parent

# ─── the reading of 2026-11-22 (written out from the read, not from the code under test) ─────────
# kubectl --context <krateo-057> get installers.composition.krateo.io -A -o json: one Installer,
# krateo-system/installer, apiVersion composition.krateo.io/v0-3-698; its spec.features:
READ_20261122_CR = {
    "accessReview": False, "agentGateway": True, "auditWorm": True, "capabilityRoles": False,
    "codegenAgents": False, "configConnector": True, "coreAgents": True, "coreProvider": True,
    "externalDns": False, "githubProviderKogConfig": False, "identityLifecycle": False, "ingress": False,
    "internalTls": True, "keycloak": False, "oasgenProvider": True, "openstack": False, "portal": True,
    "publicPages": False, "selfservice": False, "serviceDesk": True, "showback": False, "sloRollup": True,
    "specialistAgents": True, "tenantOnboarding": False,
}
# installer 0.3.703 (helm pull, digest sha256:f37134378bb1a4a5821d117d5651e0e431c7c1f0ac30bcd6431c5e9f422554ed):
# the features its values.schema.json declares that the CR leaves out, at their values.yaml defaults
READ_20261122_DEFAULTS = {"cloudnativePg": False, "repoGraph": False, "structureGraph": False}
# the installer 057 ran at the reading (re-taken 2026-11-23 09:45 Rome on 0.3.703, a version-only move;
# readings: 0.3.697 and 0.3.698 on 2026-11-22, 0.3.700 at 01:02 on 2026-11-23; the features never moved): every
# test below names it and the reading's date through these pins
READ_VERSION = "0.3.703"
READ_DATE = "2026-11-23"
READ_CR_VERSION = "v" + READ_VERSION.replace(".", "-")


def _is_the_reading(rec: dict) -> bool:
    return (rec["read"]["date"] == READ_DATE and rec["read"]["installer_version"] == READ_VERSION
            and rec["cr_features"] == READ_20261122_CR and rec["chart_defaults"] == READ_20261122_DEFAULTS
            and rec["features"] == {**READ_20261122_CR, **READ_20261122_DEFAULTS})


def test_the_committed_record_is_the_reading_of_2026_11_22():
    assert _is_the_reading(installer_features.load())


@pytest.mark.parametrize("feature", sorted({**READ_20261122_CR, **READ_20261122_DEFAULTS}))
def test_falsifier_a_record_with_one_feature_flipped_is_not_the_reading(feature):
    """Falsifier of the drift guard: the record edited by hand, any one feature, is caught."""
    rec = copy.deepcopy(installer_features.load())
    part = "cr_features" if feature in rec["cr_features"] else "chart_defaults"
    rec[part][feature] = not rec[part][feature]
    rec["features"][feature] = not rec["features"][feature]
    installer_features.validate(rec)            # still a well-formed record ...
    assert not _is_the_reading(rec)             # ... but not 057's


def test_the_record_holds_nothing_but_feature_booleans():
    raw = json.loads(Path(installer_features.RECORD_PATH).read_text())
    assert set(raw) == {"kind", "read", "cr_features", "chart_defaults", "features"}
    assert all(isinstance(v, bool) for part in ("cr_features", "chart_defaults", "features") for v in raw[part].values())
    assert set(raw["read"]) == {"date", "installer_version", "object", "chart"}


def test_a_malformed_record_is_refused():
    rec = installer_features.load()
    for broken in (
            {**rec, "kind": "other"},
            {**rec, "cr_features": {**rec["cr_features"], "coreAgents": "true"}},     # a string, not a bool
            {**rec, "features": {**rec["features"], "coreAgents": False}},            # not CR + defaults
            {**rec, "chart_defaults": {**rec["chart_defaults"], "portal": True}},     # read AND defaulted
            {**rec, "read": {**rec["read"], "date": "yesterday"}}):
        with pytest.raises(installer_features.Refused):
            installer_features.validate(broken)


# ─── the run's values ──────────────────────────────────────────────────────

def _doc_features(doc: str) -> dict:
    return yaml.safe_load(doc)["features"]


def test_the_values_write_every_feature_at_057s_value_or_a_named_deviation():
    rec = installer_features.load()
    got = _doc_features(installer_values.document("1.12.75"))
    assert set(got) == set(rec["features"])                   # every one written: no chart default decides one
    for f, v in got.items():
        want = installer_features.DEVIATIONS[f][0] if f in installer_features.DEVIATIONS else rec["features"][f]
        assert v is want, f
    # the run-30 tail: kagent-crds (coreAgents) and nightly-review-crd (specialistAgents) are installed
    assert got["coreAgents"] is True and got["specialistAgents"] is True


def test_falsifier_the_values_follow_the_record_they_are_not_a_second_list(monkeypatch):
    """Break the derivation's input: a record with coreAgents off and showback on must change the values."""
    rec = copy.deepcopy(installer_features.load())
    for f in ("coreAgents", "showback"):
        rec["cr_features"][f] = not rec["cr_features"][f]
        rec["features"][f] = not rec["features"][f]
    monkeypatch.setattr(installer_features, "load", lambda path=installer_features.RECORD_PATH: rec)
    got = _doc_features(installer_values.document("1.12.75"))
    assert got["coreAgents"] is False and got["showback"] is True


def test_falsifier_a_deviation_changes_only_its_feature(monkeypatch):
    rec = installer_features.load()
    monkeypatch.setattr(installer_features, "DEVIATIONS", {"auditWorm": (False, "why", "ruling")})
    got = _doc_features(installer_values.document("1.12.75"))
    assert got == {**rec["features"], "auditWorm": False}


def test_every_deviation_names_why_and_a_ruling_and_differs_from_057():
    rec = installer_features.load()
    for f, (value, why, ruling) in installer_features.DEVIATIONS.items():
        assert f in rec["features"] and value is not rec["features"][f] and why and ruling
    for bad in ({"nope": (False, "w", "r")},                      # no such feature
                {"coreAgents": (True, "w", "r")},                 # 057's own value: not a deviation
                {"coreAgents": (False, "", "r")},                 # no reason
                {"coreAgents": (False, "w", "")}):                # no ruling
        with pytest.raises(installer_features.Refused):
            installer_features.bench_features(rec, bad)


# ─── the chart check ───────────────────────────────────────────────────────

def _chart(tmp_path: Path, version: str, features: dict, krateo_ns: str = "krateo-system") -> Path:
    """A pulled installer chart, as installer_src leaves it (<dir>/installer): its version, its schema's
    features and their values.yaml defaults."""
    root = tmp_path / version / "installer"
    root.mkdir(parents=True)
    (root / "Chart.yaml").write_text(f"apiVersion: v2\nname: installer\nversion: {version}\n")
    # #1572: the schema also declares every componentValues path the bench writes from 057's component-values
    # record, as installer 0.3.697's schema declares them (the chart check refuses a chart that cannot express one)
    cv: dict = {}
    cvr = installer_component_values.load()
    # #1575: and the schema facts the record keeps (installer 0.3.700 declares the collector agent's tolerations)
    facts = [p for p, declared in sorted(cvr["installer_schema"].items()) if declared]
    for path in sorted(cvr["recorded"]) + facts:
        node, segs = cv, path.split(".")
        for seg in segs[:-1]:
            node = node.setdefault(seg, {"type": "object"}).setdefault("properties", {})
        node.setdefault(segs[-1], {})
    (root / "values.schema.json").write_text(json.dumps(
        {"properties": {"componentValues": {"type": "object", "properties": cv},
                        "features": {"type": "object", "additionalProperties": False,
                                     "properties": {f: {"type": "boolean"} for f in features}}}}))
    # as every published installer chart: namespaces.krateo (the bench's KRATEO_NAMESPACE is held to it)
    (root / "values.yaml").write_text(yaml.safe_dump({"features": {f: v for f, v in features.items() if v is not None},
                                                      "namespaces": {"krateo": krateo_ns}}))
    return root.parent


ALL = {**READ_20261122_CR, **READ_20261122_DEFAULTS}


def test_a_chart_that_declares_exactly_057s_features_passes_and_says_the_deviations(tmp_path):
    lines = installer_features.check_chart(str(_chart(tmp_path, READ_VERSION, {f: False for f in ALL})))
    assert lines[0].startswith(f"installer features: 27 written, as 057 (installer {READ_VERSION}, read {READ_DATE})")
    dev = installer_features.deviation_lines()
    # the feature deviations, then the component values' own lines (#1572: switches and sizes, their deviations
    # and the not-recorded paths)
    assert lines[1:1 + len(dev)] == dev
    assert lines[1 + len(dev):] == installer_component_values.lines() + installer_values.top_level_lines()
    assert all(f"on {f}:" in "\n".join(lines) for f in installer_features.DEVIATIONS)


def test_falsifier_a_chart_that_cannot_express_a_057_feature_is_refused(tmp_path):
    """installer 0.3.641's schema has no internalTls (057 runs it on) nor publicPages."""
    older = {f: False for f in ALL if f not in ("internalTls", "publicPages")}
    with pytest.raises(installer_features.Refused, match=r"installer 0\.3\.641 cannot express 2 feature\(s\).*"
                                                         r"internalTls=True, publicPages=False"):
        installer_features.check_chart(str(_chart(tmp_path, "0.3.641", older)))


def test_falsifier_a_chart_feature_the_record_does_not_name_is_refused(tmp_path):
    newer = {**{f: False for f in ALL}, "someNewFeature": True}
    with pytest.raises(installer_features.Refused, match=r"declares 1 feature\(s\) the record .* someNewFeature"):
        installer_features.check_chart(str(_chart(tmp_path, "0.3.700", newer)))


def test_the_suite_holds_the_archive_of_the_recorded_installer(tmp_path):
    """The one archive a run's values render against (tests/installer_archives.py) is the published installer
    the records were read on, and the real chart check passes on it: when 057's record is re-taken on a new
    installer, this fails until that installer's archive is added."""
    from tests import installer_archives as ia
    assert ia.RECORDED == installer_features.load()["read"]["installer_version"] == READ_VERSION
    assert ia.RECORDED == installer_component_values.load()["read"]["installer_version"]
    lines = installer_features.check_chart(str(ia.pull(tmp_path / "p", ia.RECORDED)))
    assert lines[0].startswith(f"installer features: 27 written, as 057 (installer {READ_VERSION}")


# the published archives before the record, and the features of 057's record each cannot express
CANNOT_EXPRESS = {
    "0.3.545": (7, "capabilityRoles=False, cloudnativePg=False, internalTls=True, keycloak=False, openstack=False, "
                   "publicPages=False, tenantOnboarding=False"),
    "0.3.616": (3, "internalTls=True, publicPages=False, tenantOnboarding=False"),
    "0.3.641": (2, "internalTls=True, publicPages=False")}


@pytest.mark.parametrize("version", sorted(CANNOT_EXPRESS))
def test_every_published_archive_before_the_record_is_refused_by_the_chart_check(tmp_path, version):
    """The rule (#1575 review): the values come from the record of 057 at one installer, so an installer that
    cannot express them is refused by the chart check, with its message, before any value is written. No
    version is special: each archive is refused for the features its own schema lacks."""
    from tests import installer_archives as ia
    assert set(CANNOT_EXPRESS) == set(ia.BEFORE_THE_RECORD)
    n, named = CANNOT_EXPRESS[version]
    with pytest.raises(installer_features.Refused, match=rf"installer {re.escape(version)} cannot express {n} "
                                                         rf"feature\(s\) 057 \(installer {re.escape(READ_VERSION)}, "
                                                         rf"read {READ_DATE}\) runs: {re.escape(named)}; "
                                                         rf"install an installer that declares them"):
        installer_features.check_chart(str(ia.pull(tmp_path / "p", version)))


def _values_file(tmp_path: Path, mutate=None) -> Path:
    """The values document a run installs with (bench.installer_values), parsed, mutated, written back."""
    doc = yaml.safe_load(installer_values.document("1.12.74"))
    if mutate:
        mutate(doc)
    f = tmp_path / f"values-{len(list(tmp_path.glob('values-*')))}.yaml"
    f.write_text(yaml.safe_dump(doc))
    return f


def test_the_check_cli_writes_its_record_and_refuses_with_exit_2(tmp_path, capsys):
    ok, out = _chart(tmp_path, READ_VERSION, {f: False for f in ALL}), tmp_path / "ok.json"
    vals = _values_file(tmp_path)
    assert installer_features.main(["check", "--installer-pulled", str(ok), "--values", str(vals),
                                    "--out", str(out)]) == 0
    rec = json.loads(out.read_text())
    assert rec["ok"] and rec["features"] == installer_features.bench_features()
    bad, out2 = _chart(tmp_path, "0.3.641", {f: False for f in ALL if f != "internalTls"}), tmp_path / "bad.json"
    assert installer_features.main(["check", "--installer-pulled", str(bad), "--values", str(vals),
                                    "--out", str(out2)]) == 2
    assert "refusing: installer 0.3.641 cannot express" in capsys.readouterr().err
    assert json.loads(out2.read_text())["ok"] is False


# ─── #1588 follow-up: what the installer refuses to render, refused before any cluster ──

def test_the_run_writes_the_ca_bundle_target_from_the_namespace_it_installs_krateo_into():
    """The value is the bench's own topology (installer_values.KRATEO_NAMESPACE), the namespace s_install
    installs into and the chart's namespaces.krateo; the document's registry pull Secret's namespace is the
    same definition; run606.sh's installer release namespace is it too."""
    doc = yaml.safe_load(installer_values.document("1.12.74"))
    assert doc["componentValues"]["internal-ca-bundle"] == {"target": {"namespaces": [installer_values.KRATEO_NAMESPACE]}}
    assert doc["registryAuth"]["passwordRef"]["namespace"] == installer_values.KRATEO_NAMESPACE
    src = (BENCH_DIR / "run606.sh").read_text()
    installs = [ln for ln in src.splitlines() if "-n krateo-system --create-namespace -f \"$OUT/installer-values.yaml\"" in ln]
    assert len(installs) == 2 and installer_values.KRATEO_NAMESPACE == "krateo-system"
    assert "internal-ca-bundle.target.namespaces" in installer_component_values.BENCH_OWN
    # the top-level values the bench writes off are off in the document it writes
    for path in installer_values.TOP_LEVEL_OFF:
        node = doc
        for seg in path.split("."):
            node = node[seg]
        assert node is False, path


def test_s_cert_manager_reads_its_namespace_from_the_one_definition():
    """The cert-manager step installs into bench/cert_manager.py NAMESPACE, read from its preflight record: no
    literal namespace in the step (the installer's internalTls.certManagerNamespace comes from the same one)."""
    src = (BENCH_DIR / "run606.sh").read_text()
    body = src[src.index("s_cert_manager() {"):]
    body = body[:body.index("\n}\n")]
    assert 'ns="$(cm_field namespace)"' in body and "-n cert-manager" not in body
    assert body.count('-n "$ns"') == 5           # [[ -n "$ns" ]], then install, wait, helm list, helm get values


@pytest.mark.parametrize("case,mutate", [
    ("absent", lambda d: d["componentValues"].pop("internal-ca-bundle")),
    ("empty_list", lambda d: d["componentValues"]["internal-ca-bundle"]["target"].__setitem__("namespaces", [])),
    ("empty_name", lambda d: d["componentValues"]["internal-ca-bundle"]["target"].__setitem__("namespaces", [""])),
])
def test_falsifier_internal_tls_on_with_an_empty_ca_bundle_target_is_refused_before_any_cluster(tmp_path, capsys,
                                                                                                  case, mutate):
    """installer 0.3.703 compositions.yaml:1005: features.internalTls on and target.namespaces empty refuses the
    whole composition render (no component installs); the check refuses the run first, by the CLI s_source runs."""
    assert installer_features.bench_features()["internalTls"] is True
    chart = _chart(tmp_path, READ_VERSION, {f: False for f in ALL})
    bad = _values_file(tmp_path, mutate)
    with pytest.raises(installer_features.Refused, match=r"features\.internalTls is on and the run's values leave "
                                                         r"componentValues\.internal-ca-bundle\.target\.namespaces empty"):
        installer_features.check_values(yaml.safe_load(bad.read_text()))
    assert installer_features.main(["check", "--installer-pulled", str(chart), "--values", str(bad)]) == 2
    assert "refusing: features.internalTls is on and the run's values leave" in capsys.readouterr().err
    # the same document with the value passes, and says it
    good = yaml.safe_load(_values_file(tmp_path).read_text())
    assert installer_features.check_values(good) == [
        f"internal TLS: componentValues.internal-ca-bundle.target.namespaces = {installer_values.KRATEO_NAMESPACE}, "
        f"the namespace the bench installs krateo into"]


def test_with_internal_tls_off_an_empty_ca_bundle_target_is_not_refused(tmp_path):
    bad = yaml.safe_load(_values_file(tmp_path, lambda d: d["componentValues"].pop("internal-ca-bundle")).read_text())
    off = {**installer_features.DEVIATIONS, "internalTls": (False, "a test's deviation", "a test's ruling")}
    assert installer_features.check_values(bad, deviations=off) == [
        "internal TLS: features.internalTls is off, so nothing of it is written"]


def test_falsifier_a_chart_that_installs_krateo_elsewhere_is_refused(tmp_path):
    chart = _chart(tmp_path, READ_VERSION, {f: False for f in ALL}, krateo_ns="krateo")
    with pytest.raises(installer_features.Refused, match=r"installs krateo into namespaces\.krateo='krateo' by default, "
                                                         r"and the bench installs it into 'krateo-system'"):
        installer_features.check_chart(str(chart))


# ─── a reading of 057: record and drift ────────────────────────────────────

SECRETISH = "do-not-keep-me"


def _listed(features: dict, version=READ_CR_VERSION, n=1) -> dict:
    it = {"apiVersion": f"composition.krateo.io/{version}", "kind": "Installer",
          "metadata": {"name": "installer", "namespace": "krateo-system"},
          "spec": {"features": dict(features), "registryAuth": {"username": SECRETISH},
                   "componentValues": {"x": {"token": SECRETISH}}}}
    return {"items": [copy.deepcopy(it) for _ in range(n)]}


def test_record_keeps_the_crs_feature_booleans_and_the_charts_defaults_only(tmp_path):
    chart = _chart(tmp_path, READ_VERSION, {**{f: False for f in READ_20261122_CR}, **READ_20261122_DEFAULTS})
    rec = installer_features.record(_listed(READ_20261122_CR), str(chart), READ_DATE)
    assert _is_the_reading(rec)
    assert SECRETISH not in json.dumps(rec)


def test_record_refuses_a_chart_of_another_version_or_a_feature_it_does_not_declare(tmp_path):
    chart = _chart(tmp_path, "0.3.641", {f: False for f in ALL})
    with pytest.raises(installer_features.Refused, match=f"pull {READ_VERSION}"):
        installer_features.record(_listed(READ_20261122_CR), str(chart), READ_DATE)
    chart = _chart(tmp_path, READ_VERSION, {f: False for f in ALL if f != "sloRollup"})
    with pytest.raises(installer_features.Refused, match="sloRollup"):
        installer_features.record(_listed(READ_20261122_CR), str(chart), READ_DATE)


def test_drift_is_empty_for_the_reading_and_names_what_moved():
    rec = installer_features.load()
    assert installer_features.drift(rec, _listed(READ_20261122_CR)) == []
    moved = installer_features.drift(rec, _listed({**READ_20261122_CR, "showback": True}, version="v0-3-799"))
    assert moved == [f"057 runs installer 0.3.799, the record was read on {READ_VERSION}",
                     "showback: False in the record, True on 057 now"]
    gone = dict(READ_20261122_CR)
    del gone["portal"]
    assert installer_features.drift(rec, _listed(gone)) == ["portal: True in the record, absent on 057 now"]


@pytest.mark.parametrize("listed", [
    {"items": []}, _listed(READ_20261122_CR, n=2), _listed(READ_20261122_CR, version="v1"),
    _listed({**READ_20261122_CR, "coreAgents": "yes"}), {"kind": "List"}])
def test_a_reading_that_is_not_one_installer_with_boolean_features_is_refused(listed):
    with pytest.raises(installer_features.Refused):
        installer_features.cr_reading(listed)


def test_drift_cli_exits_1_on_a_moved_feature(monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_listed({**READ_20261122_CR, "coreAgents": False}))))
    assert installer_features.main(["drift"]) == 1
    out = capsys.readouterr().out
    assert "coreAgents: True in the record, False on 057 now" in out and SECRETISH not in out
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_listed(READ_20261122_CR))))
    assert installer_features.main(["drift"]) == 0


# ─── what a feature turns on: inst.compEnabled ─────────────────────────────

@pytest.mark.parametrize("component,features,core,on", [
    ({"name": "a"}, {}, (), True),                                                         # no feature
    ({"name": "a", "feature": "coreAgents"}, {"coreAgents": True}, (), True),
    ({"name": "a", "feature": "coreAgents"}, {"coreAgents": False}, (), False),
    ({"name": "a", "feature": "coreAgents"}, {}, ("coreAgents",), True),                   # a core feature
    # installer 0.3.697: cert-manager is `feature: ingress, alsoFeatures: [internalTls]`
    ({"name": "cert-manager", "feature": "ingress", "alsoFeatures": ["internalTls"]},
     {"ingress": False, "internalTls": True}, (), True),
    ({"name": "cert-manager", "feature": "ingress", "alsoFeatures": ["internalTls"]},
     {"ingress": False, "internalTls": False}, (), False),
])
def test_component_enabled_is_the_installers_comp_enabled(component, features, core, on):
    assert installer_features.component_enabled(component, features, core) is on


def test_falsifier_diag606_declares_a_component_on_by_its_also_features():
    """Until this change diag606 read `feature` alone: cert-manager (ingress off, internalTls on) was
    left out of the platform it waits for."""
    cr = {"spec": {"features": {"ingress": False, "internalTls": True},
                   "components": [{"name": "cert-manager", "kind": "CertManager", "feature": "ingress",
                                   "alsoFeatures": ["internalTls"]},
                                  {"name": "keycloak", "kind": "Keycloak", "feature": "keycloak"}]}}
    assert [c["name"] for c in diag606.declared_components(cr)] == ["cert-manager"]


# ─── the wiring: run606's source stage and vm606's laptop chart check ──────

def test_run606s_source_stage_checks_the_features_before_anything_else_reads_the_chart():
    src = (BENCH_DIR / "run606.sh").read_text()
    body = src[src.index("s_source() {"):]
    body = body[:body.index("\n}\n")]
    assert body.index("bench.installer_src oci-preflight") < body.index("installer_features_check || return 1") \
        < body.index("portal_readers")
    fn = src[src.index("installer_features_check() {"):]
    fn = fn[:fn.index("\n}\n")]
    assert 'python3 -m bench.installer_features check --installer-pulled "$OUT/installer-pulled"' in fn
    assert 'record_failure "source FAILED: installer features:' in fn


def test_vm606s_chart_check_refuses_a_chart_that_cannot_express_057s_features(tmp_path, monkeypatch):
    from bench import installer_src, vm606
    from tests.test_1395_private_render import REAL_CHART_CHECK

    def preflight(chart, version, workdir, requirements=(), **kw):
        root = Path(workdir) / "installer"
        root.mkdir(parents=True)
        src = _chart(tmp_path, version, {f: False for f in ALL if f != "internalTls"}) / "installer"
        for p in src.iterdir():
            (root / p.name).write_bytes(p.read_bytes())
        return {"ok": True}
    monkeypatch.setattr(installer_src, "oci_preflight", preflight)
    env = {"INSTALLER_VERSION": READ_VERSION, "RUNS": "N1", "SNOWPLOW_TAG_N1": "1.12.75"}
    v = re.escape(READ_VERSION)
    with pytest.raises(vm606.Refused, match=rf"installer {v}: installer {v} cannot express 1 feature\(s\)"):
        REAL_CHART_CHECK(env, ["N1"])


def test_falsifier_vm606s_chart_check_refuses_a_kit_on_another_installer_than_the_records(tmp_path, monkeypatch):
    """The owner's ruling (2026-11-22): the kit's INSTALLER_VERSION is 057's live installer, read at the
    kit's build; a kit on another version than the one 057's features were recorded on is refused."""
    from bench import installer_src, vm606
    from tests.test_1395_private_render import REAL_CHART_CHECK

    def preflight(*a, **k):
        raise AssertionError("the version is checked before any pull")
    monkeypatch.setattr(installer_src, "oci_preflight", preflight)
    env = {"INSTALLER_VERSION": "0.3.641", "RUNS": "N1", "SNOWPLOW_TAG_N1": "1.12.75"}
    with pytest.raises(vm606.Refused, match=r"the kit installs installer 0\.3\.641, but 057's features were "
                                            rf"recorded on {re.escape(READ_VERSION)} \(read {READ_DATE}\)"):
        REAL_CHART_CHECK(env, ["N1"])
    assert installer_features.check_kit_version(READ_VERSION) == [
        f"installer {READ_VERSION}: the version 057's features were recorded on ({READ_DATE})"]


# ─── the live installer version: read from 057, never typed ────────────────

def _cd(url="oci://ghcr.io/krateo-platformops/charts/installer", version=READ_VERSION):
    return {"apiVersion": "core.krateo.io/v1alpha1", "kind": "CompositionDefinition",
            "metadata": {"name": "installer", "namespace": "krateo-system"},
            "spec": {"chart": {"url": url, "version": version, "credentials": {"username": SECRETISH}}}}


def test_the_live_installer_version_is_the_installer_compositiondefinitions_chart_version():
    assert installer_features.live_installer_version(_cd()) == READ_VERSION
    for bad in (_cd(url="oci://ghcr.io/krateo-agentiko/charts/installer-agent"), _cd(version=READ_VERSION + "-rc1"),
                _cd(version=""), {"kind": "Installer"}, None):
        with pytest.raises(installer_features.Refused):
            installer_features.live_installer_version(bad)


def test_the_installer_version_cli_prints_it_and_records_nothing_else(tmp_path, monkeypatch, capsys):
    out = tmp_path / "installer-version.json"
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_cd())))
    assert installer_features.main(["installer-version", "--out", str(out), "--date", "2026-11-22"]) == 0
    assert capsys.readouterr().out == READ_VERSION + "\n"
    rec = json.loads(out.read_text())
    assert rec["installer_version"] == READ_VERSION and SECRETISH not in out.read_text()
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_cd(version="latest"))))
    assert installer_features.main(["installer-version"]) == 2


def test_the_deviations_are_the_owners_rulings_and_name_their_fidelity_gap():
    assert set(installer_features.DEVIATIONS) == {"configConnector", "serviceDesk"}
    for f, (value, why, ruling) in installer_features.DEVIATIONS.items():
        assert value is False and ruling == "the owner (Diego), 2026-11-22: OFF" and "Fidelity gap:" in why
    assert "~274 cnrm.cloud.google.com CRDs" in installer_features.DEVIATIONS["configConnector"][1]


# ─── end to end: run606.sh against the harness's fakes ─────────────────────

from tests.test_run606_real_sequence import harness  # noqa: E402,F401  (fixture)


def _run606(env, **extra):
    import subprocess
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def test_run606_writes_057s_features_and_says_the_deviations(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1")
    log = (tmp / "out" / "run.log").read_text()
    assert f"installer features: 27 written, as 057 (installer {READ_VERSION}, read {READ_DATE}) runs them" in log, \
        (p.stderr + log)[-3000:]
    assert all(f"the bench differs from 057 on {f}:" in log for f in installer_features.DEVIATIONS)
    assert json.loads((tmp / "out" / "installer-features.json").read_text())["ok"] is True
    assert _doc_features((tmp / "out" / "installer-values.yaml").read_text()) == installer_features.bench_features()


def test_falsifier_run606_refuses_an_installer_that_cannot_express_057s_features(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", FAKE_INSTALLER_FEATURES="drop:internalTls")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0
    assert ("source FAILED: installer features: installer 0.3.422 cannot express 1 feature(s) 057 (installer "
            f"{READ_VERSION}, read {READ_DATE}) runs: internalTls=True") in log, log[-3000:]
    assert "=== step up ===" not in log
    assert json.loads((tmp / "out" / "installer-features.json").read_text())["ok"] is False
