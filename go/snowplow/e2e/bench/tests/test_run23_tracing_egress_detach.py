"""Run 23 (R = snowplow 1.12.66 vs N = 1.12.67): three things the paired run's evidence needs.

1. Was snowplow tracing (bench/tracing.py, $OUT/tracing.json)? The measured pod's effective OTEL_*
   keys, its sampler, whether an OTLP receiver listens on its node at the endpoint's port, and
   the span-export counters (snowplow has none: said, never a number). INFO, never judged. The
   arms: a chart-shaped pod with its ConfigMap present, absent and partial; no reading, an
   unreadable one, or a Secret source reads UNKNOWN, never "off".
2. Connection reuse per egress and protocol (#1189, the Go 1.27 arm's reading): mem606 captures
   snowplow_egress_connections and snowplow_endpoint_retries_total, check-prefixes enforces
   them, and evaluate reports opened, handovers and the reuse share 1 - opened / handovers per
   arm. An image without the counters reads ABSENT, never 0.
3. A paid launch survives the launching session (bench/detach.py): the child outlives a SIGTERM
   and a SIGHUP to its parent's process group; a child started the plain way, the control, does
   not, so the signal reached what it had to.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

import pytest

from bench import custlat, load606
from bench import tracing as tr
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

STARTED = "2026-11-08T10:00:00Z"
SECRET_NAME = "tenant-x-clientconfig"         # a Secret's name: it must never reach the evidence


# ─── fixtures: what kubectl answers ─────────────────────────────────────────

def _pod(env_from=None, env=None, *, node="n1", host_ip="10.0.0.5", started=STARTED, name="snowplow-abc"):
    return {"metadata": {"name": name, "namespace": "krateo-system"},
            "spec": {"nodeName": node, "containers": [{
                "name": "snowplow",
                "envFrom": [{"configMapRef": {"name": "snowplow"}}] if env_from is None else env_from,
                "env": ([{"name": "HOST_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}},
                         {"name": "LOG_LEVEL", "value": "warn"},
                         {"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://$(HOST_IP):4318"}]
                        if env is None else env)}]},
            "status": {"hostIP": host_ip, "containerStatuses": [
                {"name": "snowplow", "restartCount": 0, "state": {"running": {"startedAt": started}}}]}}


def _pods(*pods):
    return {"items": list(pods) or [_pod()]}


CHART_DATA = {"OTEL_ENABLED": "true", "OTEL_TRACES_SAMPLER": "parentbased_always_on",
              "OTEL_EXPORTER_OTLP_ENDPOINT": "http://$(HOST_IP):4318", "OTEL_LOGS_ENABLED": "false",
              "LOG_LEVEL": "warn", "URL_AUTHN": "http://authn.krateo-system:8082"}


def _cms(data=None, *, name="snowplow", changed="2026-11-08T09:00:00Z", managed=True):
    md = {"name": name, "creationTimestamp": "2026-11-08T08:00:00Z"}
    if managed:
        md["managedFields"] = [{"manager": "helm", "time": changed}]
    return {"items": [{"metadata": md, "data": CHART_DATA if data is None else data},
                      {"metadata": {"name": "other", "managedFields": [{"time": changed}]},
                       "data": {"OTEL_ENABLED": "false"}}]}


def _agent(*, ready=True, node="n1", port=4318, phase="Running"):
    return {"metadata": {"name": "otel-collector-daemonset-agent-x1", "namespace": "clickstack",
                         "ownerReferences": [{"kind": "DaemonSet", "name": "otel-collector-daemonset-agent"}]},
            "spec": {"nodeName": node, "containers": [{"name": "otc", "ports": [
                {"containerPort": 4318, "hostPort": port, "protocol": "TCP"}]}]},
            "status": {"phase": phase, "conditions": [{"type": "Ready", "status": "True" if ready else "False"}]}}


def _node(*pods):
    return {"items": [{"metadata": {"name": "snowplow-abc", "namespace": "krateo-system"},
                       "spec": {"nodeName": "n1"}, "status": {"phase": "Running"}}] + list(pods)}


# ─── 1. tracing: the configmap present, absent, partial ─────────────────────

def test_present_the_chart_shaped_pod_reads_on_with_its_sampler_and_the_node_agent():
    r = tr.reading("before", _pods(), _cms(), _node(_agent()))
    assert r["tracing"]["state"] == tr.ON and "OTEL_ENABLED=true" in r["tracing"]["why"]
    assert r["sampler"] == {"state": "set", "value": "parentbased_always_on",
                            "from": "envFrom configMapRef snowplow", "arg": None}
    # the env entry overrides the ConfigMap's key and expands $(HOST_IP) to the node's IP
    assert r["endpoint"]["raw"] == "http://$(HOST_IP):4318" and r["endpoint"]["key"] == \
        "OTEL_EXPORTER_OTLP_ENDPOINT"
    assert r["effective"]["OTEL_EXPORTER_OTLP_ENDPOINT"]["from"] == "env"
    assert (r["endpoint"]["expanded"], r["endpoint"]["port"], r["endpoint"]["node_local"]) == (
        "http://10.0.0.5:4318", 4318, True)
    rc = r["receiver"]
    assert rc["state"] == tr.PRESENT and rc["node"] == "n1"
    assert rc["pods"] == [{"namespace": "clickstack", "pod": "otel-collector-daemonset-agent-x1",
                           "owner_kind": "DaemonSet", "owner": "otel-collector-daemonset-agent",
                           "phase": "Running", "ready": True, "via": "hostPort"}]
    line = tr.reading_line(r)
    assert line.startswith("tracing ON") and "sampler parentbased_always_on" in line
    assert "OTLP receiver PRESENT on the node: DaemonSet otel-collector-daemonset-agent" in line


def test_only_otel_keys_are_kept_never_another_key_or_value():
    r = tr.reading("before", _pods(), _cms(), _node(_agent()))
    text = json.dumps(r)
    for k, v in (("LOG_LEVEL", "warn"), ("URL_AUTHN", "http://authn.krateo-system:8082"), ("HOST_IP", None)):
        assert f'"{k}"' not in text, k
        assert v is None or v not in text, v
    src = r["env_sources"]
    assert src[0]["otel"] == {k: v for k, v in CHART_DATA.items() if k.startswith("OTEL_")}
    assert src[1] == {"source": "env", "otel": {"OTEL_EXPORTER_OTLP_ENDPOINT": {"value": "http://$(HOST_IP):4318"}}}
    # every OTEL_* key a source sets is resolved, not only the ones tracing reads
    assert r["effective"]["OTEL_LOGS_ENABLED"] == {"state": "set", "value": "false",
                                                   "from": "envFrom configMapRef snowplow"}


def test_absent_configmap_reads_unknown_never_off():
    r = tr.reading("before", _pods(), {"items": []}, _node(_agent()))
    assert r["tracing"]["state"] == tr.UNKNOWN and r["sampler"]["state"] == tr.UNKNOWN
    assert "not there now" in r["tracing"]["why"]
    assert r["env_sources"][0]["state"] == "absent"
    # the env entry's own endpoint is known: it overrides whatever the ConfigMap held
    assert r["endpoint"]["state"] == "set" and r["receiver"]["state"] == tr.PRESENT
    # and so is an optional ConfigMap: what it held at the container's start is unknown
    opt = _pod(env_from=[{"configMapRef": {"name": "snowplow", "optional": True}}])
    assert tr.reading("before", _pods(opt), {"items": []}, _node())["tracing"]["state"] == tr.UNKNOWN


def test_unreadable_configmaps_or_pod_or_no_reading_read_unknown():
    r = tr.reading("before", _pods(), None, _node(_agent()), configmaps_why="kubectl exit 1: Error from server (Forbidden)")
    assert r["tracing"]["state"] == tr.UNKNOWN and r["env_sources"][0]["state"] == "unreadable"
    assert r["env_sources"][0]["configmaps_why"].startswith("kubectl exit 1")
    r = tr.reading("after", None, None, None, pods_why="kubectl list pods did not answer within 300 s")
    assert {r[k]["state"] for k in ("tracing", "sampler", "endpoint", "receiver")} == {tr.UNKNOWN}
    assert r["pod"]["why"] == "kubectl list pods did not answer within 300 s"
    two = tr.reading("before", _pods(_pod(), _pod(name="snowplow-def")), _cms(), _node())
    assert two["tracing"]["state"] == tr.UNKNOWN and "2 live snowplow pods" in two["tracing"]["why"]
    # a Terminating leftover is not the live pod
    old = _pod(name="snowplow-old")
    old["metadata"]["deletionTimestamp"] = "2026-11-08T09:59:00Z"
    assert tr.reading("before", _pods(old, _pod()), _cms(), _node(_agent()))["tracing"]["state"] == tr.ON
    none = tr.reading("before", {"items": []}, _cms(), None)
    assert none["tracing"]["state"] == tr.UNKNOWN and none["pod"]["why"] == "no live snowplow pod"


def test_partial_configmap_unset_keys_take_snowplows_defaults_from_a_complete_reading():
    # OTEL_ENABLED only: on, and the SDK's default sampler (said as unset, not as set)
    r = tr.reading("before", _pods(), _cms({"OTEL_ENABLED": "true"}), _node(_agent()))
    assert r["tracing"]["state"] == tr.ON
    assert r["sampler"] == {"state": "unset", "value": "parentbased_always_on",
                            "why": "OTEL_TRACES_SAMPLER unset: the SDK's default"}
    assert "(unset: the SDK's default)" in tr.reading_line(r)
    # a sampler but no switch, every source read: snowplow's own default, off, with its reason
    r = tr.reading("before", _pods(), _cms({"OTEL_TRACES_SAMPLER": "traceidratio",
                                            "OTEL_TRACES_SAMPLER_ARG": "0.05"}), _node(_agent()))
    assert r["tracing"]["state"] == tr.OFF and "snowplow's default" in r["tracing"]["why"]
    assert (r["sampler"]["value"], r["sampler"]["arg"]) == ("traceidratio", "0.05")
    # an unparsable switch is env.Bool's default (ParseBool fails): the master decides
    r = tr.reading("before", _pods(), _cms({"OTEL_TRACING_ENABLED": "yes", "OTEL_ENABLED": "1"}), _node())
    assert r["tracing"]["state"] == tr.ON and r["tracing"]["why"].startswith("OTEL_ENABLED=1")


def test_the_per_signal_switch_overrides_the_master():
    r = tr.reading("before", _pods(), _cms(dict(CHART_DATA, OTEL_TRACING_ENABLED="false")), _node(_agent()))
    assert r["tracing"]["state"] == tr.OFF and r["tracing"]["why"].startswith("OTEL_TRACING_ENABLED=false")


def test_a_configmap_changed_after_the_container_started_proves_nothing():
    r = tr.reading("before", _pods(), _cms(changed="2026-11-08T10:05:00Z"), _node(_agent()))
    assert r["tracing"]["state"] == tr.UNKNOWN and "changed after the container started" in r["tracing"]["why"]
    r = tr.reading("before", _pods(), _cms(managed=False), _node(_agent()))
    assert r["tracing"]["state"] == tr.UNKNOWN and r["env_sources"][0]["state"] == "change_time_unknown"


def test_a_secret_source_is_unknown_and_never_named():
    pod = _pod(env_from=[{"configMapRef": {"name": "snowplow"}}, {"secretRef": {"name": SECRET_NAME}}])
    r = tr.reading("before", _pods(pod), _cms(), _node(_agent()))
    assert r["tracing"]["state"] == tr.UNKNOWN and "Secret" in r["tracing"]["why"]
    assert r["effective"]["OTEL_EXPORTER_OTLP_ENDPOINT"]["state"] == "set"     # env wins over every envFrom
    assert {"source": "envFrom", "kind": "secretRef", "state": "not_read"} in r["env_sources"]
    env = [{"name": "OTEL_TRACES_SAMPLER", "valueFrom": {"secretKeyRef": {"name": SECRET_NAME, "key": "s"}}}]
    r2 = tr.reading("before", _pods(_pod(env=env)), _cms(), _node())
    assert r2["sampler"]["state"] == tr.UNKNOWN and r2["tracing"]["state"] == tr.ON
    for doc in (r, r2):
        assert SECRET_NAME not in json.dumps(doc)


def test_the_receiver_absent_not_ready_or_not_on_the_node_ip():
    assert tr.reading("b", _pods(), _cms(), _node())["receiver"]["state"] == tr.ABSENT
    assert tr.reading("b", _pods(), _cms(), _node(_agent(ready=False)))["receiver"]["state"] == tr.NOT_READY
    assert tr.reading("b", _pods(), _cms(), _node(_agent(port=4317)))["receiver"]["state"] == tr.ABSENT
    r = tr.reading("b", _pods(), _cms(), None, node_pods_why="kubectl exit 1: Error from server (Forbidden)")
    assert r["receiver"]["state"] == tr.UNKNOWN and "Forbidden" in r["receiver"]["why"]
    svc = _pod(env=[{"name": "OTEL_EXPORTER_OTLP_ENDPOINT",
                     "value": "http://otel-collector.krateo-system.svc.cluster.local:4318"}])
    r = tr.reading("b", _pods(svc), _cms({"OTEL_ENABLED": "true"}), _node(_agent()))
    assert r["receiver"]["state"] == tr.UNKNOWN and "not the node's IP" in r["receiver"]["why"]
    # no endpoint key anywhere: the SDK's localhost:4318, which kubectl cannot see
    r = tr.reading("b", _pods(_pod(env=[])), _cms({"OTEL_ENABLED": "true"}), _node(_agent()))
    assert r["endpoint"]["state"] == "unset" and r["receiver"]["state"] == tr.UNKNOWN
    # a hostNetwork pod listening on the port is a receiver too
    hn = _agent()
    hn["spec"]["hostNetwork"] = True
    del hn["spec"]["containers"][0]["ports"][0]["hostPort"]
    assert tr.reading("b", _pods(), _cms(), _node(hn))["receiver"]["pods"][0]["via"] == "hostNetwork"


def test_the_report_is_info_with_both_readings_and_says_spans_exported_are_not_known(tmp_path):
    f = tmp_path / "tracing.json"
    tr.record(str(f), tr.reading("before", _pods(), _cms(), _node(_agent())))
    tr.record(str(f), tr.reading("after", _pods(), _cms(), _node(_agent(ready=False))))
    doc = json.loads(f.read_text())
    assert set(doc["readings"]) == {"before", "after"} and doc["span_export"]["state"] == "NOT_EXPOSED"
    assert doc["span_export"]["delta"] is None and doc["span_export"]["counters"] == []
    rep = tr.report(str(f))
    assert rep["result"] == "INFO" and rep["state"] == tr.ON and rep["changed"] == ["receiver.state"]
    assert rep["lines"][0].startswith("INFO tracing before the measure: tracing ON")
    assert rep["lines"][1].startswith("INFO tracing after the measure: tracing ON")
    assert rep["lines"][2] == "INFO tracing changed during the measure: receiver.state"
    assert rep["lines"][3].startswith("INFO tracing spans exported: not known (snowplow publishes no")
    assert all(ln.startswith("INFO ") for ln in rep["lines"])


def test_no_tracing_file_or_one_reading_reports_unknown(tmp_path):
    rep = tr.report(str(tmp_path / "tracing.json"))
    assert rep["state"] == tr.UNKNOWN and rep["lines"][0] == (
        "INFO tracing: UNKNOWN (no tracing.json: the run recorded no reading)")
    assert tr.report(None)["state"] == tr.UNKNOWN
    (tmp_path / "bad.json").write_text("{")
    assert "unreadable" in tr.report(str(tmp_path / "bad.json"))["why"]
    f = tmp_path / "one.json"
    tr.record(str(f), tr.reading("before", _pods(), _cms(), _node(_agent())))
    rep = tr.report(str(f))
    assert rep["state"] == tr.UNKNOWN and "after the measure: UNKNOWN (no reading at this point)" in rep["lines"][1]


_FAKE_KUBECTL = r"""
import json, os, sys
a = sys.argv[1:]
with open(os.environ["CALLS"], "a") as fh:
    fh.write(json.dumps(a) + "\n")
mode = os.environ.get("MODE", "ok")
if "pods" in a and "-l" in a:
    if mode == "pods_forbidden":
        print('Error from server (Forbidden): pods is forbidden: User "alice@example.com" cannot list',
              file=sys.stderr); sys.exit(1)
    print(os.environ["PODS"]); sys.exit(0)
if "configmaps" in a:
    print(os.environ["CMS"]); sys.exit(0)
if "pods" in a and "-A" in a:
    print(os.environ["NODE"]); sys.exit(0)
sys.exit(3)
"""


def _cli(tmp_path, phase, mode="ok"):
    k = tmp_path / "kubectl.py"
    k.write_text(_FAKE_KUBECTL)
    env = dict(os.environ, CALLS=str(tmp_path / "calls.jsonl"), MODE=mode, PODS=json.dumps(_pods()),
               CMS=json.dumps(_cms()), NODE=json.dumps(_node(_agent())), PYTHONPATH=str(BENCH_DIR))
    return subprocess.run([sys.executable, "-m", "bench.tracing", "read", "--phase", phase,
                           "--out", str(tmp_path / "tracing.json"), "--timeout-s", "60", "--",
                           sys.executable, str(k), "--context=bench"],
                          cwd=BENCH_DIR, env=env, capture_output=True, text=True, timeout=120)


def test_the_cli_reads_with_kubectl_only_and_merges_both_phases(tmp_path):
    p = _cli(tmp_path, "before")
    assert p.returncode == 0, p.stderr
    assert p.stdout.startswith("tracing ON") and "OTLP receiver PRESENT" in p.stdout
    assert _cli(tmp_path, "after").returncode == 0
    doc = json.loads((tmp_path / "tracing.json").read_text())
    assert set(doc["readings"]) == {"before", "after"}
    calls = [json.loads(ln) for ln in (tmp_path / "calls.jsonl").read_text().splitlines()]
    assert len(calls) == 6 and all(c[0] == "--context=bench" for c in calls)
    assert all(any(x.startswith("--request-timeout=") and x != "--request-timeout=0s" for x in c) for c in calls)
    assert [c for c in calls if "configmaps" in c][0][-3:] == ["--show-managed-fields", "-o", "json"]
    assert ["get", "pods", "-A", "--field-selector=spec.nodeName=n1", "-o", "json"] == calls[2][-6:]
    assert not any("secret" in x.lower() for c in calls for x in c)       # a Secret is never read


def test_the_cli_keeps_the_error_code_not_the_error_text(tmp_path):
    p = _cli(tmp_path, "before", mode="pods_forbidden")
    assert p.returncode == 0, p.stderr
    doc = json.loads((tmp_path / "tracing.json").read_text())
    r = doc["readings"]["before"]
    assert r["tracing"]["state"] == tr.UNKNOWN and "Error from server (Forbidden)" in r["pod"]["why"]
    assert "alice" not in (tmp_path / "tracing.json").read_text() and "alice" not in p.stdout


def test_custlat_pair_prints_each_arms_lines_and_says_unknown_for_an_older_verdict():
    va = {"report": {"tracing_report_only": {"lines": ["INFO tracing before the measure: tracing ON (x)"]}}}
    out = custlat.info_lines_pair(va, {"report": {}}, "tracing_report_only", "UNKNOWN", ("R", "N"))
    assert out["lines"][0] == "R (baseline): INFO tracing before the measure: tracing ON (x)"
    assert out["lines"][1].startswith("N (candidate): INFO tracing_report_only: UNKNOWN (the run's verdict.json")


def test_the_dry_run_shows_both_readings_and_evaluate_reads_tracing_json(tmp_path):
    from tests.test_606 import _run606
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr[-2000:]
    reads = [ln for ln in p.stderr.splitlines() if "DRY: python3 -m bench.tracing read" in ln]
    # the measure takes its reading; collect (which a dry pass skips) takes the other, after its pod record
    assert len(reads) == 1 and "--phase before" in reads[0]
    src = (BENCH_DIR / "run606.sh").read_text()
    assert "  snowplow_pod_record before\n  tracing_reading before\n" in src
    assert "  snowplow_pod_record after\n  tracing_reading after\n" in src
    ev = next(ln for ln in p.stderr.splitlines() if "DRY: python3 -m bench.load606 evaluate" in ln)
    assert f"--tracing {tmp_path / 'out' / 'tracing.json'}" in ev


def test_the_real_sequence_records_both_readings_into_the_verdict(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=600)
    assert "run complete: " in p.stderr, p.stderr[-3000:]
    doc = json.loads((tmp / "out" / "tracing.json").read_text())
    for ph in ("before", "after"):
        r = doc["readings"][ph]
        assert (r["tracing"]["state"], r["sampler"]["value"], r["receiver"]["state"]) == (
            tr.ON, "parentbased_always_on", tr.PRESENT), r
        assert "DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS" not in json.dumps(r)     # a non-OTEL key of the ConfigMap
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    rep = v["report"]["tracing_report_only"]
    assert rep["result"] == "INFO" and rep["state"] == tr.ON
    log = (tmp / "out" / "run.log").read_text()
    assert "tracing (before): tracing ON" in log and "tracing (after): tracing ON" in log
    assert "INFO tracing before the measure: tracing ON" in log
    assert "INFO tracing spans exported: not known" in log
    # the fake snowplow publishes no egress counters: ABSENT, never 0
    eg = v["report"]["egress_connections_report_only"]
    assert eg["connections"]["status"] == "ABSENT" and "INFO egress connections: ABSENT" in log
    assert not [c for c in _kubectl_verbs(state) if "secret" in " ".join(c).lower() and c[:1] == ["get"]]


def _kubectl_verbs(state):
    from tests.test_run606_real_sequence import _calls
    return [c["argv"] for c in _calls(state) if c["tool"] == "kubectl"]


# ─── 2. egress connections (#1189) ──────────────────────────────────────────

EC, ER = load606.EGRESS_KEYS["connections"], load606.EGRESS_KEYS["retries"]


def _mem(t, conns=None, retries=None, num_gc=10):
    extra = {}
    if conns is not None:
        extra[EC] = conns
    if retries is not None:
        extra[ER] = retries
    return {"kind": "mem", "t": t, "num_gc": num_gc, "extra": extra}


def _cells(stage_h1, stage_h2, authn_h1):
    return {"stage": {"http1": dict(zip(("opened", "handovers"), stage_h1)),
                      "http2": dict(zip(("opened", "handovers"), stage_h2))},
            "authn": {"http1": dict(zip(("opened", "handovers"), authn_h1))}}


def test_mem606_captures_both_and_check_prefixes_enforces_them():
    assert EC in load606.RUN8_VARS_PREFIXES and "snowplow_endpoint_retries" in load606.RUN8_VARS_PREFIXES
    assert load606.prefixes_missing(load606.RUN8_VARS_PREFIXES) == []
    cut = [p for p in load606.RUN8_VARS_PREFIXES if p not in (EC, "snowplow_endpoint_retries")]
    assert set(load606.prefixes_missing(cut)) == {EC, ER}
    p = subprocess.run([sys.executable, "-m", "bench.load606", "check-prefixes", *cut], cwd=BENCH_DIR,
                       capture_output=True, text=True)
    assert p.returncode == 2 and EC in p.stderr and ER in p.stderr


def test_the_report_is_per_egress_and_protocol_with_the_reuse_share():
    mem = [_mem(0, _cells((10, 100), (2, 50), (1, 1)), {"stage": {"status_5xx": 3, "status_429": 0}}),
           _mem(60, _cells((30, 300), (2, 450), (1, 1)), {"stage": {"status_5xx": 7, "status_429": 2}})]
    r = load606.egress_connections_report(mem, [])
    assert r["result"] == "INFO"
    allc = r["connections"]["by_phase"]["all"]["cells"]
    assert allc["stage/http1"] == {"opened": 20, "handovers": 200, "reuse_share": 0.9}
    assert allc["stage/http2"] == {"opened": 0, "handovers": 400, "reuse_share": 1.0}
    assert allc["authn/http1"] == {"opened": 0, "handovers": 0, "reuse_share": None}
    assert r["retries"]["by_phase"]["all"]["retries"] == {"stage/status_429": 2, "stage/status_5xx": 4}
    lines = r["lines"]
    assert "INFO egress connections stage/http1 (whole measure, 60 s): opened 20, handovers 200, reuse share 90.00%" \
        in lines
    assert any(ln.startswith("INFO egress connections authn/http1") and "n/a (no hand-over)" in ln for ln in lines)
    assert "INFO endpoint retries (whole measure): stage/status_429 2, stage/status_5xx 4" in lines


def test_a_restart_counts_the_pair_after_it_from_zero():
    mem = [_mem(0, _cells((50, 500), (0, 0), (0, 0)), num_gc=40),
           _mem(30, _cells((5, 20), (0, 0), (0, 0)), num_gc=2)]       # the counters began again
    r = load606.egress_connections_report(mem, [])
    assert r["connections"]["by_phase"]["all"]["cells"]["stage/http1"] == {
        "opened": 5, "handovers": 20, "reuse_share": 0.75}
    assert r["connections"]["restarts"] == 1 and any("1 restart(s)" in ln for ln in r["lines"])


def test_an_image_without_the_counters_is_absent_never_zero():
    r = load606.egress_connections_report([_mem(0), _mem(15)], [])
    assert r["connections"]["status"] == "ABSENT" and r["retries"]["status"] == "ABSENT"
    assert "absent, not 0" in r["connections"]["why"]
    assert r["lines"][0].startswith("INFO egress connections: ABSENT (snowplow_egress_connections is in none "
                                    "of the 2 mem606 samples")
    assert not any(" 0," in ln or "share" in ln for ln in r["lines"])
    va = {"report": {"egress_connections_report_only": r}}
    pair = custlat.info_lines_pair(va, {}, "egress_connections_report_only", "ABSENT", ("R", "N"))
    assert pair["lines"][0].startswith("R (baseline): INFO egress connections: ABSENT")
    assert pair["lines"][-1].startswith("N (candidate): INFO egress_connections_report_only: ABSENT")


# ─── 3. detach: a paid launch survives its session ──────────────────────────

_SLEEPER = r"""
import os, sys, time
time.sleep(float(sys.argv[2]))
open(sys.argv[1], "w").write(str(os.getpid()))
"""


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_for(cond, bound_s):
    deadline = time.monotonic() + bound_s
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return cond()


def test_the_detached_child_survives_a_sigterm_and_a_sighup_to_its_launchers_group(tmp_path):
    sleeper = tmp_path / "sleeper.py"
    sleeper.write_text(_SLEEPER)
    done, plain_done = tmp_path / "done", tmp_path / "plain-done"
    pidfile, log = tmp_path / "run.pid", tmp_path / "run.log"
    # the launching session: a shell in a process group of its own (as a Claude Code session's tool
    # shell is), which starts the run through bench.detach, plus a control child started the plain
    # way, and then waits
    script = (f"{sys.executable} -m bench.detach --log {log} --pidfile {pidfile} -- "
              f"{sys.executable} {sleeper} {done} 3 > {tmp_path / 'detach.out'} || exit 9\n"
              f"{sys.executable} {sleeper} {plain_done} 3 &\n"
              f"echo $! > {tmp_path / 'plain.pid'}\n"
              "wait\n")
    launcher = subprocess.Popen(["bash", "-c", script], cwd=BENCH_DIR, start_new_session=True,
                                env=dict(os.environ, PYTHONPATH=str(BENCH_DIR)))
    try:
        assert _wait_for(lambda: (tmp_path / "plain.pid").exists() and pidfile.exists(), 30), "never launched"
        pid = int(pidfile.read_text())
        plain = int((tmp_path / "plain.pid").read_text())
        assert (tmp_path / "detach.out").read_text().strip() == str(pid)          # it printed the pid
        assert os.getsid(pid) == pid and os.getpgid(pid) == pid                    # its own session and group
        assert os.getpgid(plain) == launcher.pid != os.getpgid(pid)
        # the session restarts: its process group gets SIGHUP and SIGTERM
        os.killpg(launcher.pid, signal.SIGHUP)
        os.killpg(launcher.pid, signal.SIGTERM)
        launcher.wait(timeout=30)
        # the control died with the group (the signal reached what it had to) ...
        assert _wait_for(lambda: not _alive(plain), 10) and not plain_done.exists()
        # ... the detached child lives on, re-parented away from the launcher; a hangup sent to it
        # directly is ignored too (SIG_IGN survives the exec), and it finishes its work
        assert _alive(pid)
        os.kill(pid, signal.SIGHUP)
        time.sleep(0.3)
        assert _alive(pid)
        ppid = int(subprocess.run(["ps", "-o", "ppid=", "-p", str(pid)], capture_output=True,
                                  text=True).stdout.strip() or "0")
        assert ppid not in (0, launcher.pid)
        assert _wait_for(lambda: done.exists(), 30) and done.read_text() == str(pid)
        assert _wait_for(lambda: not _alive(pid), 10)
    finally:
        for f in ("run.pid", "plain.pid"):
            try:
                os.kill(int((tmp_path / f).read_text()), signal.SIGKILL)
            except (OSError, ValueError):
                pass
        if launcher.poll() is None:
            os.killpg(launcher.pid, signal.SIGKILL)
            launcher.wait()


def test_detach_refuses_a_live_pidfile_and_reports_a_failed_exec(tmp_path):
    pidfile = tmp_path / "run.pid"
    pidfile.write_text(f"{os.getpid()}\n")                     # alive: this test's own process
    env = dict(os.environ, PYTHONPATH=str(BENCH_DIR))
    p = subprocess.run([sys.executable, "-m", "bench.detach", "--log", str(tmp_path / "l"), "--pidfile",
                        str(pidfile), "--", "true"], cwd=BENCH_DIR, env=env, capture_output=True, text=True)
    assert p.returncode == 1 and "is alive: one pidfile, one launch" in p.stderr
    pidfile.unlink()
    p = subprocess.run([sys.executable, "-m", "bench.detach", "--log", str(tmp_path / "l"), "--pidfile",
                        str(pidfile), "--", str(tmp_path / "no-such-command")], cwd=BENCH_DIR, env=env,
                       capture_output=True, text=True)
    assert p.returncode == 1 and "could not start" in p.stderr and "FileNotFoundError" in p.stderr
    p = subprocess.run([sys.executable, "-m", "bench.detach", "--log", str(tmp_path / "l"), "--pidfile",
                        str(pidfile)], cwd=BENCH_DIR, env=env, capture_output=True, text=True)
    assert p.returncode == 2 and "the command goes after --" in p.stderr


def test_the_pair_launcher_documents_the_detached_launch():
    head = (BENCH_DIR / "run606_pair.sh").read_text().split("set -Eeuo pipefail")[0]
    assert "python3 -m bench.detach --log <LOG> --pidfile <PIDFILE>" in head and "-- bash run606_pair.sh" in head
