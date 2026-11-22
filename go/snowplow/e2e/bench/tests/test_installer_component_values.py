"""#1572: the bench writes krateo-057's componentValues switches and sizes, from ONE record
(bench/installer_component_values.py; the owner's ruling, 2026-11-22: "Switches + sizes, recorded").

Each check has its falsifier: break the derivation and the test fails.
  - the committed record IS the reading of 2026-11-22 (its sha256 and counts): a hand edit fails;
  - what is recorded is decided by the installer's schema: a switch, a size; every other path is named
    with why, never its value, and nothing under an undeclared key (a user's name) is ever stored;
  - the values carry every recorded switch and size but the named deviations, under the bench's own;
    a record that changes changes the values; a path both the bench and the record set is refused;
  - the chart check refuses an installer that cannot express a recorded path.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
import yaml

from bench import installer_component_values as icv
from bench import installer_features, installer_values

# the reading of 2026-11-22 (installer 0.3.697): the record's bytes, and what the owner's ruling turns on
KAGENT_SWITCHES = {"kagent.perUserRbac.enabled": True, "kagent.kagentapp.kagent-tools.enabled": False}


def _sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


PINNED = "ab03da9e626a938b761ac87193f12848e0945938d862744fb15267481d789255"   # sha256 of bench/installer_component_values_057.json as recorded 2026-11-22


def test_the_committed_record_is_the_reading_of_2026_11_22():
    rec = icv.load()
    assert rec["read"]["date"] == "2026-11-22" and rec["read"]["installer_version"] == "0.3.697"
    assert len(rec["recorded"]) == 57 and len(rec["not_recorded"]) == 134
    assert {p: rec["recorded"][p] for p in KAGENT_SWITCHES} == KAGENT_SWITCHES
    assert rec["recorded"]["kagent.kagentapp.database.postgres.bundled.storage"] == "8Gi"
    assert rec["recorded"]["snowplow.env.OTEL_LOGS_ENABLED"] == "true"
    assert _sha(icv.RECORD_PATH) == PINNED


def test_falsifier_a_record_edited_by_hand_is_not_the_reading(tmp_path):
    rec = json.loads(Path(icv.RECORD_PATH).read_text())
    rec["recorded"]["kagent.perUserRbac.enabled"] = False
    p = tmp_path / "r.json"
    p.write_text(json.dumps(rec, indent=1) + "\n")
    icv.validate(json.loads(p.read_text()))          # still well formed ...
    assert _sha(p) != PINNED                         # ... not 057's


def test_the_record_stores_only_switches_and_sizes_and_reasons():
    rec = icv.load()
    for p, v in rec["recorded"].items():
        assert isinstance(v, bool) or v in ("true", "false") or icv._is_size(p.split("."), v), p
    reasons = {icv.NOT_A_SWITCH_OR_SIZE, icv.A_LIST, icv.AN_EMPTY_MAP}
    for p, why in rec["not_recorded"].items():
        assert why in reasons or (why.startswith(("not settable through the installer", "a free-form map"))
                                  and "key(s)" in why), p


# ─── the reading: schema-decided, privacy-safe ─────────────────────────────

SCHEMA = {"properties": {"componentValues": {"properties": {
    "comp": {"type": "object", "properties": {
        "on": {"type": "boolean"},
        "env": {"type": "object", "properties": {"FLAG": {"type": "string", "enum": ["true", "false"]},
                                                 "URL": {"type": "string"}}},
        "resources": {"type": "object", "properties": {"limits": {"type": "object", "properties": {
            "memory": {"type": "string"}}}, "requests": {"type": "object", "additionalProperties": {"type": "string"}}}},
        "closed": {"type": "object", "additionalProperties": False, "properties": {}},
        "replicaCount": {"type": "integer"},
        "endpoint": {"type": "string"},
        "groups": {"type": "array"},
        "secretRef": {"type": "object", "properties": {"name": {"type": "string"}}},
        "users": {"type": "object", "properties": {}},
        "empty": {"type": "object"}}}}}}}
PLANTED = "planted-never-stored"
USER = "someone-s-name"


def _cv():
    return {"comp": {"on": True, "env": {"FLAG": "true", "URL": PLANTED},
                     "resources": {"limits": {"memory": "1Gi"}, "requests": {"cpu": "10m", USER + " Name": "1"}},
                     "closed": {"rejected": True},
                     "replicaCount": 3, "endpoint": PLANTED, "groups": [PLANTED], "secretRef": {"name": PLANTED},
                     "users": {USER: {"password_sha256_hex": PLANTED}}, "empty": {}},
            "undeclared-component": {"x": PLANTED}}


def test_the_reading_records_switches_and_sizes_by_the_schema():
    rec, nr = icv.component_reading(_cv(), SCHEMA)
    # a ResourceList's keys are resource names (cpu): recorded as sizes; a key that is no resource name is not
    assert rec == {"comp.on": True, "comp.env.FLAG": "true", "comp.resources.limits.memory": "1Gi",
                   "comp.resources.requests.cpu": "10m", "comp.replicaCount": 3}
    assert nr == {"(componentValues)": icv.NOT_SETTABLE.format(n=1), "comp.closed": icv.NOT_SETTABLE.format(n=1),
                  "comp.empty": icv.AN_EMPTY_MAP, "comp.endpoint": icv.NOT_A_SWITCH_OR_SIZE,
                  "comp.env.URL": icv.NOT_A_SWITCH_OR_SIZE, "comp.groups": icv.A_LIST,
                  "comp.resources.requests": icv.FREE_FORM.format(n=1),
                  "comp.secretRef.name": icv.NOT_A_SWITCH_OR_SIZE, "comp.users": icv.FREE_FORM.format(n=1)}


def test_privacy_no_class_c_or_d_value_and_no_free_form_key_ever_enters_a_record():
    listed = {"items": [{"spec": {"componentValues": _cv()}}]}
    text = json.dumps(icv.record(listed, SCHEMA, "2026-11-22", "0.3.697"))
    assert PLANTED not in text and USER not in text and "undeclared-component" not in text


# ─── the key allow-list: product names only, by what the schema says ───────

def _keyed(description: str) -> dict:
    return {"properties": {"componentValues": {"properties": {"comp": {"type": "object", "properties": {
        "byName": {"type": "object", "description": description,
                   "additionalProperties": {"type": "object", "properties": {
                       "enabled": {"type": "boolean"}, "secretName": {"type": "string"}}}}}}}}}}


def test_the_allow_list_records_a_product_keyed_maps_switches_as_agentikos_schema_says():
    """installer 0.3.697: componentValues.agentiko.agents, "Agents to deploy, keyed by agent name.\""""
    schema = _keyed("Agents to deploy, keyed by agent name.")
    cv = {"comp": {"byName": {"helm": {"enabled": True, "secretName": PLANTED}, "Not A Name": {"enabled": True}}}}
    rec, nr = icv.component_reading(cv, schema)
    assert rec == {"comp.byName.helm.enabled": True}
    assert nr == {"comp.byName": icv.FREE_FORM.format(n=1), "comp.byName.helm.secretName": icv.NOT_A_SWITCH_OR_SIZE}
    assert icv.declared(schema, "comp.byName.helm.enabled")
    # the committed record: agentiko's two agents, as 057 sets them
    r = icv.load()["recorded"]
    assert {p: r[p] for p in ("agentiko.agents.helm.enabled", "agentiko.agents.k8s.enabled")} == \
        {"agentiko.agents.helm.enabled": True, "agentiko.agents.k8s.enabled": True}


@pytest.mark.parametrize("description", ["Users keyed by user name.", "ClickHouse users.", "", "keyed by agent"])
def test_privacy_a_user_keyed_or_unsaid_free_form_map_never_has_a_key_recorded(description):
    """The falsifier of the allow-list: accept any free-form map, and a user's name enters the record."""
    schema = _keyed(description)
    cv = {"comp": {"byName": {USER: {"enabled": True}}}}
    rec, nr = icv.component_reading(cv, schema)
    assert rec == {} and nr == {"comp.byName": icv.FREE_FORM.format(n=1)}
    assert USER not in json.dumps(icv.record({"items": [{"spec": {"componentValues": cv}}]}, schema, "d", "v"))
    # the committed record: the ClickHouse users map is a gap, never a key
    assert icv.load()["not_recorded"][
        "krateo-observability.clickstack.clickhouse.cluster.spec.settings.extraUsersConfig.users"].startswith(
        "a free-form map: 1 key(s)")


def test_falsifier_a_schema_that_declares_a_free_form_key_would_store_it():
    """The privacy property rests on the schema: a key is stored only when the schema declares it."""
    schema = copy.deepcopy(SCHEMA)
    schema["properties"]["componentValues"]["properties"]["comp"]["properties"]["users"]["properties"][USER] = {
        "type": "object", "properties": {"on": {"type": "boolean"}}}
    cv = {"comp": {"users": {USER: {"on": True}}}}
    assert icv.component_reading(cv, schema)[0] == {f"comp.users.{USER}.on": True}


# ─── the values ────────────────────────────────────────────────────────────

def _cv_of(doc: str) -> dict:
    return yaml.safe_load(doc)["componentValues"]


def _get(d, path):
    for s in path.split("."):
        if not isinstance(d, dict) or s not in d:
            return KeyError
        d = d[s]
    return d


def test_the_values_carry_every_recorded_switch_and_size_but_the_deviations():
    cv = _cv_of(installer_values.document("1.12.75", "1.8.61", {"GOGC": "100"}))
    rec = icv.load()
    for p, v in rec["recorded"].items():
        if p in icv.DEVIATIONS:
            assert _get(cv, p) is KeyError, p
        else:
            assert _get(cv, p) == v, p
    # the run-30 blocker: kagent's per-user tool server on, its upstream tools server off, as on 057
    assert {p: _get(cv, p) for p in KAGENT_SWITCHES} == KAGENT_SWITCHES
    # the bench's own, under it: its group, its snowplow, its declared env beside 057's audit-log switch
    assert cv["portal"]["readerGroups"] == [installer_values.READER_GROUP]
    assert cv["snowplow"]["env"] == {"GOGC": "100", "OTEL_LOGS_ENABLED": "true"}
    assert cv["snowplow"]["image"]["tag"] == "1.12.75" and cv["frontend"]["image"]["tag"] == "1.8.61"
    assert "enableCyberjokerUser" not in cv["portal"] and "stt" not in cv["agentgateway-policies"]


def test_falsifier_the_values_follow_the_record(monkeypatch):
    rec = copy.deepcopy(icv.load())
    rec["recorded"]["kagent.perUserRbac.enabled"] = False
    rec["recorded"]["frontend.replicaCount"] = 2
    monkeypatch.setattr(icv, "load", lambda path=icv.RECORD_PATH: rec)
    cv = _cv_of(installer_values.document("1.12.75"))
    assert cv["kagent"]["perUserRbac"]["enabled"] is False and cv["frontend"]["replicaCount"] == 2


def test_falsifier_a_deviation_keeps_a_switch_out(monkeypatch):
    monkeypatch.setattr(icv, "DEVIATIONS", {**icv.DEVIATIONS, "kagent.perUserRbac.enabled": ("why", "ruling")})
    assert "perUserRbac" not in _cv_of(installer_values.document("1.12.75"))["kagent"]


def test_a_path_both_the_bench_and_the_record_set_is_refused_unless_named():
    rec = {"snowplow": {"env": {"OTEL_LOGS_ENABLED": "true"}}}
    with pytest.raises(icv.Refused, match=r"componentValues\.snowplow\.env\.OTEL_LOGS_ENABLED: the bench sets it"):
        icv.merge(rec, {"snowplow": {"env": {"OTEL_LOGS_ENABLED": "false"}}})
    assert icv.merge({"portal": {"readerGroups": ["x"]}}, {"portal": {"readerGroups": ["y"]}}) == \
        {"portal": {"readerGroups": ["y"]}}                       # BENCH_OWN: the bench's own group


def test_the_declared_snowplow_env_cannot_collide_with_057s_audit_log_switch():
    """SNOWPLOW_ENV declares Go runtime keys only (bench/snowplow_env.py), and the record's snowplow env
    switch is not one: the two never meet, and a collision would be refused above."""
    from bench import snowplow_env
    with pytest.raises(snowplow_env.Refused):
        snowplow_env.parse("OTEL_LOGS_ENABLED=false")


def test_every_deviation_says_why_and_the_owners_ruling_and_every_check_says_them():
    lines = icv.lines()
    rec = icv.load()
    for p, (why, ruling) in icv.DEVIATIONS.items():
        assert why and ruling == "the owner (Diego), 2026-11-22: not copied"
        if p in rec["recorded"]:
            assert any(f"does not copy componentValues.{p} " in ln for ln in lines), p
    assert any("writes componentValues.portal.readerGroups itself" in ln for ln in lines)
    gaps = [ln for ln in lines if ln.startswith("fidelity gap, componentValues.")]
    assert len(gaps) == sum(1 for w in rec["not_recorded"].values()
                            if w.startswith(("not settable", "a free-form map"))) == 8
    # the switches that create an identity or reach outside, as the owner named them
    for p in ("portal.enableCyberjokerUser", "agentgateway-policies.stt.enabled", "agentgateway-policies.tts.enabled"):
        assert p in icv.DEVIATIONS


def test_falsifier_the_chart_check_refuses_an_installer_that_cannot_express_a_recorded_path():
    schema = {"properties": {"componentValues": {"properties": {}}}}
    with pytest.raises(icv.Refused, match=r"installer 0\.3\.641 cannot express \d+ of 057's recorded component"):
        icv.check_chart(schema, "0.3.641")
    full = {"properties": {"componentValues": {"properties": {}}}}
    for p in icv.bench_paths():
        node = full["properties"]["componentValues"]
        for s in p.split("."):
            node = node.setdefault("properties", {}).setdefault(s, {})
    icv.check_chart(full, "0.3.697")


def test_drift_names_what_moved():
    rec = icv.load()
    listed = {"items": [{"spec": {"componentValues": {"kagent": {"perUserRbac": {"enabled": False}}}}}]}
    schema = {"properties": {"componentValues": {"properties": {"kagent": {"properties": {"perUserRbac": {
        "properties": {"enabled": {"type": "boolean"}}}}}}}}}
    moved = icv.drift(rec, listed, schema)
    assert "componentValues.kagent.perUserRbac.enabled: True in the record, False on 057 now" in moved
