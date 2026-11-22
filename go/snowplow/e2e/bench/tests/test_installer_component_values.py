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

# the reading of 2026-11-22 (installer 0.3.698): the record's bytes, and what the owner's ruling turns on
KAGENT_SWITCHES = {"kagent.perUserRbac.enabled": True, "kagent.kagentapp.kagent-tools.enabled": False}


def _sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# sha256 of bench/installer_component_values_057.json as re-taken 2026-11-22 14:05 Rome on installer 0.3.698
# (#1575 review: the first reading was on 0.3.697; no recorded switch or size moved, one not-recorded path
# came: zendesk-organisations.zendesk.baseUrl), re-taken at 15:29 Rome to keep installer_schema (the agent's
# toleration: not declared by 0.3.698)
PINNED = "37904656cc3e7cd58833b6fdabed8e97dadc4a073f1f254d44f340484c83d0fd"


def test_the_committed_record_is_the_reading_of_2026_11_22():
    rec = icv.load()
    assert rec["read"]["date"] == "2026-11-22" and rec["read"]["installer_version"] == "0.3.698"
    assert len(rec["recorded"]) == 57 and len(rec["not_recorded"]) == 135
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


# ─── a ResourceList's keys: Kubernetes resource names only (#1575 review B2) ─

#: the reviewer's probe: an open resources.limits map holding a name-shaped bare key
B2_PROBE = {"comp": {"resources": {"limits": {"alice": "1"}}}}
B2_SCHEMA = {"properties": {"componentValues": {"properties": {"comp": {"type": "object", "properties": {
    "resources": {"type": "object", "properties": {
        "limits": {"type": "object", "additionalProperties": {"type": "string"}},
        "requests": {"type": "object", "additionalProperties": {"type": "string"}}}}}}}}}}


def test_privacy_a_bare_resource_key_that_is_no_kubernetes_resource_is_never_recorded():
    """The reviewer's probe: c.resources.limits = {"alice": "1"} was recorded as c.resources.limits.alice."""
    rec, nr = icv.component_reading(B2_PROBE, B2_SCHEMA)
    assert rec == {} and nr == {"comp.resources.limits": icv.FREE_FORM.format(n=1)}
    assert "alice" not in json.dumps(icv.record({"items": [{"spec": {"componentValues": B2_PROBE}}]}, B2_SCHEMA,
                                                "2026-11-22", "0.3.698"))
    assert not icv.declared(B2_SCHEMA, "comp.resources.limits.alice")


@pytest.mark.parametrize("key,name,recorded", [
    ("cpu", True, True), ("memory", True, True), ("ephemeral-storage", True, True), ("hugepages-2Mi", True, True),
    ("hugepages-1Gi", True, True), ("vendor/foo", True, True),
    # a resource name the dot-joined record cannot hold (nest() would split it): a named gap, never recorded
    ("nvidia.com/gpu", True, False), ("example.com/foo", True, False),
    ("alice", False, False), ("gpu", False, False), ("storage", False, False), ("pods", False, False),
    ("hugepages-", False, False), ("hugepages-big", False, False), ("Alice", False, False), ("/gpu", False, False),
    ("example.com/", False, False), ("a b", False, False)])
def test_a_resource_key_is_admitted_only_as_kubernetes_names_one(key, name, recorded):
    """Bare: the standard set and hugepages-<size>; every other resource must carry its domain and a '/'."""
    assert icv.resource_name(key) is name and icv.recordable_resource(key) is recorded
    cv = {"comp": {"resources": {"requests": {key: "1"}}}}
    rec, nr = icv.component_reading(cv, B2_SCHEMA)
    assert rec == ({f"comp.resources.requests.{key}": "1"} if recorded else {})
    assert nr == ({} if recorded else {"comp.resources.requests": icv.FREE_FORM.format(n=1)})
    assert icv.declared(B2_SCHEMA, f"comp.resources.requests.{key}") is recorded


def test_falsifier_the_pre_review_resource_rule_records_the_probe(monkeypatch):
    """Put back the rule the review found (any bare lowercase word) and the probe's key enters the record:
    the privacy test above is what catches it."""
    import re
    old = re.compile(r"^([a-z0-9.-]+/)?[a-z][a-z0-9-]*$")
    monkeypatch.setattr(icv, "resource_name", lambda k: bool(old.fullmatch(k)))
    assert icv.component_reading(B2_PROBE, B2_SCHEMA)[0] == {"comp.resources.limits.alice": "1"}


def test_a_record_holding_a_resource_key_that_is_no_resource_name_is_refused_without_naming_it():
    rec = {"kind": icv.RECORD_KIND, "read": {"date": "2026-11-22", "installer_version": "0.3.698"},
           "recorded": {"comp.resources.limits.alice": "1"}, "not_recorded": {},
           "installer_schema": {icv.RECEIVER_TOLERATION: False}}
    with pytest.raises(icv.Refused, match=r"recorded comp\.resources\.limits: a key that is no Kubernetes resource "
                                          r"name") as e:
        icv.validate(rec)
    assert "alice" not in str(e.value)
    # a dotted extended resource reads, split, as two keys under the ResourceList: refused too
    rec["recorded"] = {"comp.resources.requests.nvidia.com/gpu": "1"}
    with pytest.raises(icv.Refused, match=r"recorded comp\.resources\.requests: a key that is no Kubernetes"):
        icv.validate(rec)
    rec["recorded"] = {"comp.resources.limits.memory": "1Gi", "comp.resources.requests.vendor/gpu": "1",
                       "comp.resources.limits.hugepages-2Mi": "2Mi"}
    icv.validate(rec)


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
    devs = icv.deviations()
    for p, v in rec["recorded"].items():
        if p in devs:
            assert _get(cv, p) is KeyError, p
        else:
            assert _get(cv, p) == v, p
    # the identity / outside-service switches OFF on 057 are written off (the owner's ruling, 2026-11-22)
    assert cv["agentgateway-policies"]["cors"] == {"enabled": False, "allowCredentials": False}
    assert cv["frontend"]["ingress"] == {"enabled": False}
    assert cv["krateo-observability"]["hyperdxLoadBalancer"] == {"enabled": False}
    # the run-30 blocker: kagent's per-user tool server on, its upstream tools server off, as on 057
    assert {p: _get(cv, p) for p in KAGENT_SWITCHES} == KAGENT_SWITCHES
    # the bench's own, under it: its group, its snowplow, its declared env beside 057's audit-log switch
    assert cv["portal"]["readerGroups"] == [installer_values.READER_GROUP]
    # 057's OTEL_LOGS_ENABLED is not copied: the bench has no OTLP receiver on snowplow's node (#1575)
    assert cv["snowplow"]["env"] == {"GOGC": "100"}
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
    """Name a switch that is ON on 057 an identity / outside-service switch, and it is no longer written."""
    assert _cv_of(installer_values.document("1.12.75"))["frontend"]["previewSandbox"] == {"enabled": True}
    monkeypatch.setattr(icv, "IDENTITY_OR_OUTSIDE", {**icv.IDENTITY_OR_OUTSIDE, "frontend.previewSandbox.enabled": "why"})
    assert "previewSandbox" not in _cv_of(installer_values.document("1.12.75"))["frontend"]


def test_an_identity_or_outside_switch_is_copied_off_and_never_copied_on():
    """The owner's ruling (2026-11-22): OFF on 057, written off; ON on 057, a deviation. Read from the record."""
    rec = icv.load()
    off = icv.copied_off(rec)
    on = {p: d for p, d in icv.deviations(rec).items() if p in icv.IDENTITY_OR_OUTSIDE}
    assert set(off) | set(on) == set(icv.IDENTITY_OR_OUTSIDE) & set(rec["recorded"]) and not set(off) & set(on)
    assert all(icv.is_off(rec["recorded"][p]) for p in off) and not any(icv.is_off(rec["recorded"][p]) for p in on)
    assert sorted(off) == [
        "agentgateway-policies.cors.allowCredentials", "agentgateway-policies.cors.enabled",
        "agentgateway-policies.uiTokenHop.impersonation.enabled", "cert-manager-issuers.acmeDns01.enabled",
        "frontend.ingress.enabled", "krateo-observability.hyperdxLoadBalancer.enabled",
        "zendesk-kog.plugin.webhookReceiver.enabled", "zendesk-kog.restDefinitions.ticket.enabled",
        "zendesk-kog.restDefinitions.webhook.enabled", "zendesk-kog.snowplow.restActions.enabled"]
    assert sorted(on) == [
        "agentgateway-policies.mcpEntry.enabled", "agentgateway-policies.stt.enabled",
        "agentgateway-policies.tts.enabled", "frontend.serviceAccount.automount", "frontend.serviceAccount.create",
        "gcp-configconnector.configConnector.enabled", "keycloak-config-kog.auth.bootstrap.enabled",
        "portal.enableAdminUser", "portal.enableCyberjokerUser", "portal.enableIncidentResponse",
        "zendesk-kog.restDefinitions.request.enabled"]
    written = icv.bench_paths(rec)
    assert {p: written[p] for p in off} == {p: False for p in off} and not set(on) & set(written)


def test_kagent_per_user_rbac_is_copied_with_the_reason_it_is_outside_the_ruling_said_by_every_check():
    """The owner (Diego), 2026-11-22: perUserRbac swaps a cluster-admin-bound ServiceAccount for a powerless one
    and acts with each caller's token: no identity, no rights. Copied, and every check says why."""
    rec = icv.load()
    assert set(icv.REVIEWED_COPIED) == {"kagent.perUserRbac.enabled"}
    assert not set(icv.REVIEWED_COPIED) & set(icv.IDENTITY_OR_OUTSIDE)
    assert icv.bench_paths(rec)["kagent.perUserRbac.enabled"] is True
    line = [ln for ln in icv.lines(rec) if ln.startswith("the bench copies componentValues.kagent.perUserRbac.enabled")]
    assert len(line) == 1 and "no per-user identity and grants no rights" in line[0] \
        and "frontend.serviceAccount.create stays a deviation" in line[0]


def test_falsifier_a_switch_both_classified_and_reviewed_is_refused(monkeypatch):
    monkeypatch.setattr(icv, "IDENTITY_OR_OUTSIDE", {**icv.IDENTITY_OR_OUTSIDE, "kagent.perUserRbac.enabled": "why"})
    with pytest.raises(icv.Refused, match=r"kagent\.perUserRbac\.enabled: both an identity or outside-service switch"):
        icv.bench_paths()


def test_falsifier_057_turning_an_identity_switch_on_makes_it_a_deviation_not_a_copy(monkeypatch):
    """If 057 later turns cors on, the bench must not follow: the rule reads the record, it is no list of
    'off' switches."""
    rec = copy.deepcopy(icv.load())
    rec["recorded"]["agentgateway-policies.cors.enabled"] = True
    monkeypatch.setattr(icv, "load", lambda path=icv.RECORD_PATH: rec)
    assert "agentgateway-policies.cors.enabled" in icv.deviations()
    assert _cv_of(installer_values.document("1.12.75"))["agentgateway-policies"]["cors"] == {"allowCredentials": False}
    # and an identity switch with no reason is refused, not silently copied
    monkeypatch.setattr(icv, "IDENTITY_OR_OUTSIDE", {**icv.IDENTITY_OR_OUTSIDE, "portal.enableAdminUser": ""})
    with pytest.raises(icv.Refused, match=r"portal\.enableAdminUser: an identity or outside-service switch"):
        icv.bench_paths()


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
    for p, (why, ruling) in icv.deviations(rec).items():
        want = icv.RULING_RECEIVER if p in icv.SNOWPLOW_NODE_EXPORTS else "the owner (Diego), 2026-11-22: ON on 057, not copied"
        assert why and ruling == want
        assert sum(f"does not copy componentValues.{p} ({ruling}): {why}" in ln for ln in lines) == 1, p
    for p, why in icv.copied_off(rec).items():
        assert sum(f"writes componentValues.{p} off, as 057 has it (the owner (Diego), 2026-11-22: OFF on 057, "
                   f"copied off): {why}" in ln for ln in lines) == 1, p
    assert any("writes componentValues.portal.readerGroups itself" in ln for ln in lines)
    gaps = [ln for ln in lines if ln.startswith("fidelity gap, componentValues.")]
    assert len(gaps) == sum(1 for w in rec["not_recorded"].values()
                            if w.startswith(("not settable", "a free-form map"))) == 8
    # the switches that create an identity or reach outside, as the owner named them
    for p in ("portal.enableCyberjokerUser", "agentgateway-policies.stt.enabled", "agentgateway-policies.tts.enabled"):
        assert p in icv.deviations(rec)


def test_falsifier_the_chart_check_refuses_an_installer_that_cannot_express_a_recorded_path():
    schema = {"properties": {"componentValues": {"properties": {}}}}
    with pytest.raises(icv.Refused, match=r"installer 0\.3\.641 cannot express \d+ of 057's recorded component"):
        icv.check_chart(schema, "0.3.641")
    full = {"properties": {"componentValues": {"properties": {}}}}
    for p in icv.bench_paths():
        node = full["properties"]["componentValues"]
        for s in p.split("."):
            node = node.setdefault("properties", {}).setdefault(s, {})
    icv.check_chart(full, "0.3.698")


def test_drift_names_what_moved():
    rec = icv.load()
    listed = {"items": [{"spec": {"componentValues": {"kagent": {"perUserRbac": {"enabled": False}}}}}]}
    schema = {"properties": {"componentValues": {"properties": {"kagent": {"properties": {"perUserRbac": {
        "properties": {"enabled": {"type": "boolean"}}}}}}}}}
    moved = icv.drift(rec, listed, schema)
    assert "componentValues.kagent.perUserRbac.enabled: True in the record, False on 057 now" in moved


# ─── #1575: the OTLP receiver on snowplow's node, decided by the installer's schema ─

def _rec_with_receiver(settable: bool) -> dict:
    rec = copy.deepcopy(icv.load())
    rec["installer_schema"] = {icv.RECEIVER_TOLERATION: settable}
    return rec


def _schema_with(paths, receiver: bool) -> dict:
    full = {"properties": {"componentValues": {"properties": {}}}}
    for p in list(paths) + ([icv.RECEIVER_TOLERATION] if receiver else []):
        node = full["properties"]["componentValues"]
        for seg in p.split("."):
            node = node.setdefault("properties", {}).setdefault(seg, {})
    return full


def test_the_record_keeps_that_the_recorded_installer_cannot_express_the_agents_toleration(tmp_path):
    """Read from installer 0.3.698's own values schema (the archive the record was taken against)."""
    from bench import installer_features
    from tests import installer_archives as ia
    rec = icv.load()
    assert rec["installer_schema"] == {icv.RECEIVER_TOLERATION: False}
    schema = installer_features.chart_schema(str(ia.pull(tmp_path / "p", ia.RECORDED)))[1]
    assert icv.receiver_settable(schema) is False and not icv.receiver_on_snowplow_node(rec)
    assert icv.receiver_settable(_schema_with([], True)) is True
    assert icv.receiver_settable(_schema_with([], False)) is False


def test_without_the_toleration_in_the_schema_the_deviation_is_said_and_otel_logs_is_not_copied():
    """The branch in force (installer 0.3.698): no toleration written, OTEL_LOGS_ENABLED a deviation with the reason."""
    rec = icv.load()
    why, ruling = icv.deviations(rec)["snowplow.env.OTEL_LOGS_ENABLED"]
    assert why == ("no OTLP receiver on the bench's snowplow node: installer 0.3.698 cannot express the daemonset "
                   "toleration (componentValues.otel-collector-daemonset.opentelemetry-collector.tolerations is not in "
                   "its values schema; 057 has a receiver there)") and ruling == icv.RULING_RECEIVER
    cv = _cv_of(installer_values.document("1.12.75"))
    assert "otel-collector-daemonset" not in cv and "OTEL_LOGS_ENABLED" not in cv["snowplow"].get("env", {})
    lines = icv.lines(rec)
    assert sum(ln == f"the bench differs from 057 ({icv.RULING_RECEIVER}): {why}" for ln in lines) == 1
    assert any(ln.startswith("the bench does not copy componentValues.snowplow.env.OTEL_LOGS_ENABLED ") for ln in lines)
    # the other OTLP switches export from nodes the agent covers: copied, as 057 has them
    for p in ("alert-provider.otel.enabled", "krateo-sse-proxy.otel.enabled", "oasgen-provider.rdc.otel.nodeLocalEndpoint"):
        assert icv.bench_paths(rec)[p] is True, p


def test_falsifier_with_the_toleration_in_the_schema_the_bench_writes_it_and_copies_otel_logs(monkeypatch):
    """The branch an installer that declares the path takes: the bench's own toleration, 057's OTEL_LOGS_ENABLED."""
    rec = _rec_with_receiver(True)
    monkeypatch.setattr(icv, "load", lambda path=icv.RECORD_PATH: rec)
    assert "snowplow.env.OTEL_LOGS_ENABLED" not in icv.deviations()
    cv = _cv_of(installer_values.document("1.12.75", "", {"GOGC": "100"}))
    assert cv["otel-collector-daemonset"] == {"opentelemetry-collector": {"tolerations": [
        {"key": "dedicated", "operator": "Equal", "value": "snowplow", "effect": "NoSchedule"}]}}
    assert cv["snowplow"]["env"] == {"GOGC": "100", "OTEL_LOGS_ENABLED": "true"}
    assert cv["snowplow"]["tolerations"] == cv["otel-collector-daemonset"]["opentelemetry-collector"]["tolerations"]
    lines = icv.lines()
    assert any(ln.startswith(f"the bench writes componentValues.{icv.RECEIVER_TOLERATION} itself: its snowplow pool's "
                             "toleration") for ln in lines)
    assert not any("no OTLP receiver" in ln for ln in lines)


def test_the_toleration_is_the_bench_pools_taint():
    from bench import gke
    snow = next(np for np in gke.default_spec("snowplow-bench-x", now=0).extra_pools if np.name == "snowplow")
    t = installer_values.SNOWPLOW_POOL_TOLERATION
    assert snow.taint == (t["key"], t["value"], t["effect"]) and t["operator"] == "Equal"


@pytest.mark.parametrize("schema_has,record_has", [(True, False), (False, True)])
def test_falsifier_a_chart_whose_schema_disagrees_with_the_records_fact_is_refused(schema_has, record_has):
    rec = _rec_with_receiver(record_has)
    with pytest.raises(icv.Refused, match=rf"installer 9\.9\.9's values schema {'declares' if schema_has else 'does not declare'} "
                                          rf"componentValues\.otel-collector-daemonset\.opentelemetry-collector\.tolerations, "
                                          rf"and the record .* says it {'does' if record_has else 'does not'}: record 057 again"):
        icv.check_chart(_schema_with(icv.bench_paths(rec), schema_has), "9.9.9", rec)
    icv.check_chart(_schema_with(icv.bench_paths(rec), record_has), "9.9.9", rec)       # agreeing: passes


def test_drift_names_a_schema_fact_that_moved():
    rec = icv.load()
    listed = {"items": [{"spec": {"componentValues": {}}}]}
    moved = icv.drift(rec, listed, _schema_with([], True))
    assert f"installer schema: componentValues.{icv.RECEIVER_TOLERATION} declared by this chart, not declared in the " \
           f"record" in moved


def test_a_record_without_the_schema_facts_is_refused():
    rec = copy.deepcopy(icv.load())
    del rec["installer_schema"]
    with pytest.raises(icv.Refused, match="the record keeps no installer_schema facts"):
        icv.validate(rec)
    rec["installer_schema"] = {icv.RECEIVER_TOLERATION: "no"}
    with pytest.raises(icv.Refused, match="the record keeps no installer_schema facts"):
        icv.validate(rec)


# the receiver gate (bench/tracing.py receiver-gate, run606's env step)

TOLERATED = {"componentValues": {"otel-collector-daemonset": {"opentelemetry-collector": {"tolerations": [
    {"key": "dedicated", "operator": "Equal", "value": "snowplow", "effect": "NoSchedule"}]}}}}


@pytest.mark.parametrize("values,reading,ok,said", [
    ({"componentValues": {}}, None, True, "not required"),
    (TOLERATED, {"receiver": {"state": "PRESENT", "port": 4318}}, True, "PRESENT (port 4318)"),
    (TOLERATED, {"receiver": {"state": "ABSENT", "why": "no pod on node n listens on hostPort 4318"}}, False,
     "no OTLP receiver on snowplow's node although the run's values give the node-local agent the snowplow pool's "
     "toleration: ABSENT (no pod on node n listens on hostPort 4318)"),
    (TOLERATED, {"receiver": {"state": "NOT_READY", "why": "x"}}, False, "NOT_READY"),
    (TOLERATED, {"receiver": {"state": "UNKNOWN", "why": "y"}}, False, "UNKNOWN"),
    (TOLERATED, None, False, "UNKNOWN (no reading)"),
])
def test_the_receiver_gate_requires_an_agent_only_where_the_values_give_it_the_pool(values, reading, ok, said):
    from bench import tracing
    got_ok, line = tracing.receiver_gate(values, reading)
    assert got_ok is ok and said in line, line


def test_the_receiver_gate_cli_reads_nothing_when_not_required(tmp_path):
    """The branch in force: the run's own values (no agent toleration), no kubectl command, a record, rc 0."""
    import subprocess
    import sys
    v, out = tmp_path / "installer-values.yaml", tmp_path / "otlp-receiver.json"
    v.write_text(installer_values.document("1.12.75"))
    p = subprocess.run([sys.executable, "-m", "bench.tracing", "receiver-gate", "--values", str(v), "--out", str(out)],
                       cwd=Path(icv.__file__).resolve().parent.parent, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0 and "not required" in p.stdout, p.stderr
    assert json.loads(out.read_text()) == {"kind": "otlp_receiver", "required": False, "ok": True,
                                           "line": p.stdout.strip(), "receiver": None}
    v.write_text(yaml.safe_dump(TOLERATED))
    p = subprocess.run([sys.executable, "-m", "bench.tracing", "receiver-gate", "--values", str(v), "--out", str(out)],
                       cwd=Path(icv.__file__).resolve().parent.parent, capture_output=True, text=True, timeout=60)
    assert p.returncode == 2 and "the run's values require a receiver" in p.stderr


def test_run606_parses():
    """f0105d9a: an apostrophe inside "${line:-...}" opened a quote and the whole script stopped parsing
    (CI: 301 failed). bash -n on the script catches that class before any harness run."""
    import subprocess
    p = subprocess.run(["bash", "-n", str(Path(icv.__file__).resolve().parent.parent / "run606.sh")],
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr


def test_run606s_env_step_holds_the_receiver_gate_before_any_load():
    src = (Path(icv.__file__).resolve().parent.parent / "run606.sh").read_text()
    body = src[src.index("s_env() {"):]
    body = body[:body.index("\n}\n")]
    assert body.rstrip().endswith("otlp_receiver_gate || return 1")
    fn = src[src.index("otlp_receiver_gate() {"):]
    fn = fn[:fn.index("\n}\n")]
    assert 'python3 -m bench.tracing receiver-gate --values "$OUT/installer-values.yaml"' in fn
    assert 'record_failure "env FAILED: ' in fn
    assert src.index("\nstep env       s_env\n") < src.index("\nstep measure   s_measure\n")
