"""A run's declared snowplow environment, per arm (bench/snowplow_env.py): one image at GOGC=50 against
the same image at the Go default.

  - declared: SNOWPLOW_ENV="GOGC=100", explicit values only, Go runtime keys only;
  - applied through the installer's values and nothing else, and rendered by the pulled chart before
    anything is created (real helm, the published installer 0.3.641 archive: the 50 -> 100 override, and
    that no declaration renders the installer's own 50);
  - proven from the running pod before any load (the run's own reading of the container's env and the
    ConfigMaps it reads): a mismatch fails the arm;
  - agreed between the arms of a launch: the same declared keys or the launch is refused; a key no arm
    declared that differs between them fails the arm before load;
  - said by the pair in one line: same image, different env.

Every report is read through the CLI the run uses.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile

import pytest

from bench import installer_values, snowplow_env
from bench import tracing as tr
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run22_three_runs import ROOM_FOR_THREE, _launch_env
from tests.test_run606_real_sequence import BENCH_DIR, RUN_NAME, harness  # noqa: F401  (fixture)

BENCH = BENCH_DIR
CHARTS = BENCH / "tests" / "fixtures" / "installer_charts"
# the published chart, as `helm pull oci://ghcr.io/krateo-platformops/charts/installer --version 0.3.641`
# gave it on 2026-11-14 (the registry's manifest digest: sha256:eb3799df...160ff; the archive's own sha256 below)
INSTALLER = "0.3.641"
needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="renders with the real helm")
DIG = "sha256:" + "ab" * 32
DIG2 = "sha256:" + "cd" * 32


def _cli(*args, **kw):
    return subprocess.run([sys.executable, "-m", "bench.snowplow_env", *args], cwd=BENCH, capture_output=True,
                          text=True, timeout=300, **kw)


# ─── declared ───────────────────────────────────────────────────────────────

def test_a_declaration_is_explicit_go_runtime_values():
    assert snowplow_env.parse("GOGC=100") == {"GOGC": "100"}
    assert snowplow_env.parse(" GOMEMLIMIT=7GiB  GOGC=50 ") == {"GOGC": "50", "GOMEMLIMIT": "7GiB"}
    assert snowplow_env.parse("") == {} and snowplow_env.parse(None) == {}
    # the Go default is declared by its value, and every line that names it says so
    assert snowplow_env.declared_text({"GOGC": "100"}) == "GOGC=100 (the Go default, set explicitly)"
    assert snowplow_env.declared_text({"GOGC": "50"}) == "GOGC=50"


@pytest.mark.parametrize("text, why", [
    ("GOGC", "'GOGC' is not KEY=VALUE"),
    ("GOGC=", "GOGC's value '' is empty"),                       # there is no "unset"
    ("GOGC=50 GOGC=100", "names GOGC twice"),
    ("LOG_LEVEL=debug", "LOG_LEVEL is not a Go runtime key"),    # nothing reads it back from the pod
    ("GOOGLE_APPLICATION_CREDENTIALS=x", "is not a Go runtime key"),
    ('GOGC="50"', "holds a character outside"),
])
def test_a_declaration_the_bench_cannot_honour_is_refused(text, why):
    p = _cli("parse", "--declared", text)
    assert p.returncode == 2 and f"refusing: SNOWPLOW_ENV" in p.stderr and why in p.stderr


def test_the_values_carry_the_declaration_and_nothing_changes_without_one():
    plain = installer_values.document("1.12.70")
    assert installer_values.document("1.12.70", "", {}) == plain and "env:" not in plain
    doc = installer_values.document("1.12.70", "1.8.41", {"GOGC": "100", "GOMEMLIMIT": "7GiB"})
    import yaml
    v = yaml.safe_load(doc)
    assert v["componentValues"]["snowplow"]["env"] == {"GOGC": "100", "GOMEMLIMIT": "7GiB"}     # strings
    assert v["componentValues"]["snowplow"]["image"]["tag"] == "1.12.70"
    assert v["componentValues"]["frontend"]["image"]["tag"] == "1.8.41"
    without = yaml.safe_load(installer_values.document("1.12.70", "1.8.41"))
    del v["componentValues"]["snowplow"]["env"]
    assert v == without                                           # the declaration adds its keys, only


# ─── rendered: the published installer 0.3.641, real helm ───────────────────

@pytest.fixture(scope="module")
def pulled(tmp_path_factory):
    src = CHARTS / f"installer-{INSTALLER}.tgz"
    assert hashlib.sha256(src.read_bytes()).hexdigest() == \
        "d6692a9f3910d29b7b2ea7bf4fafa1d7499bab5686281259ce31fa25ecbc309b"
    d = tmp_path_factory.mktemp("pulled")
    with tarfile.open(src) as tf:
        tf.extractall(d, filter="data")
    return d


def _render(pulled, tmp, declared, in_values=None):
    """render-check, as run606's snowplow-env step runs it: values written by bench.installer_values."""
    vals = tmp / "installer-values.yaml"
    p = subprocess.run([sys.executable, "-m", "bench.installer_values", "--snowplow-tag", "1.12.70",
                        "--snowplow-env", declared if in_values is None else in_values, "--out", str(vals)],
                       cwd=BENCH, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    out = tmp / "snowplow-env-render.json"
    p = _cli("render-check", "--installer-pulled", str(pulled), "--values", str(vals), "--declared", declared,
             "--out", str(out))
    return p, json.loads(out.read_text())


@needs_helm
def test_installer_0_3_641_renders_the_override_and_its_own_50_without_one(pulled, tmp_path):
    p, rec = _render(pulled, tmp_path, "GOGC=100")
    assert p.returncode == 0 and rec["ok"] is True and rec["rendered"] == {"GOGC": "100"}, p.stdout + p.stderr
    assert 'rendered: componentValues.snowplow.env of the Installer CR the chart applies: {"GOGC": "100"}' in p.stdout
    p, rec = _render(pulled, tmp_path, "GOGC=50")
    assert p.returncode == 0 and rec["rendered"] == {"GOGC": "50"}
    # the negative: with no declaration the installer's own default is what a fresh install gets,
    # which is why a run cannot "unset" the key and must declare 100 to get the Go default
    p, rec = _render(pulled, tmp_path, "")
    assert p.returncode == 0 and rec["rendered"] == {"GOGC": "50"} and rec["declared"] == {}


@needs_helm
def test_a_declaration_that_does_not_reach_the_installers_cr_is_refused(pulled, tmp_path):
    """The values do not carry it (the arm would inherit the installer's 50)."""
    p, rec = _render(pulled, tmp_path, "GOGC=100", in_values="")
    assert p.returncode == 1 and rec["ok"] is False
    assert "NOT rendered as declared: GOGC renders as '50', not '100'" in p.stdout
    p, rec = _render(pulled, tmp_path, "GOGC=100 GOMAXPROCS=4", in_values="GOGC=100")
    assert p.returncode == 1 and "GOMAXPROCS is not in the rendered componentValues.snowplow.env" in p.stdout


def test_a_chart_that_cannot_be_rendered_is_refused_not_passed(tmp_path):
    (tmp_path / "v.yaml").write_text(installer_values.document("1.12.70", "", {"GOGC": "100"}))
    p = _cli("render-check", "--installer-pulled", str(tmp_path / "nowhere"), "--values", str(tmp_path / "v.yaml"),
             "--declared", "GOGC=100", "--out", str(tmp_path / "r.json"))
    assert p.returncode == 1 and "could not be rendered" in p.stdout


# ─── observed: the running pod, as the run reads it ─────────────────────────

STARTED = "2026-11-14T09:00:00Z"


def _reading(tmp, cm=None, env=(), cm_written="2026-11-14T08:00:00Z", secret=False):
    """tracing.json as run606's `tracing read --phase before` writes it, from the API's own shapes: the
    live snowplow pod (its env list, envFrom its ConfigMap) and the namespace's ConfigMaps."""
    c = {"name": "snowplow", "env": [{"name": k, "value": v} for k, v in env],
         "envFrom": [{"configMapRef": {"name": "snowplow"}}] + ([{"secretRef": {"name": "s"}}] if secret else [])}
    pods = {"items": [{"metadata": {"name": "snowplow-abc"}, "spec": {"nodeName": "n1", "containers": [c]},
                       "status": {"hostIP": "10.0.0.5", "containerStatuses": [{
                           "name": "snowplow", "ready": True, "state": {"running": {"startedAt": STARTED}}}],
                           "conditions": [{"type": "Ready", "status": "True"}]}}]}
    cms = {"items": [{"metadata": {"name": "snowplow", "creationTimestamp": "2026-01-01T00:00:00Z",
                                   "managedFields": [{"manager": "helm", "time": cm_written}]},
                      "data": dict(cm or {})}]}
    path = tmp / "tracing.json"
    tr.record(str(path), tr.reading("before", pods, cms, {"items": []}))
    return path


GIB = 2 ** 30


def _eff(d, declared="", **over):
    """go-runtime.jsonl as the runtime606 Job prints it (load606 go-runtime): what the process runs with.
    By default the declared GOGC (the runtime's 100 with none) and a 7 GiB limit derived from an 8 GiB
    container, as snowplow 1.12.70 runs. `over`: other fields; ok=False, why=...: a reading that failed."""
    gogc = snowplow_env.gogc_value(snowplow_env.parse(declared).get("GOGC", "100"))
    eff = {"gogc": gogc, "memory_limit_bytes": 7 * GIB, "memory_limit_source": "cgroup_v2",
           "cgroup_memory_limit_bytes": 8 * GIB}
    ok, why = over.pop("ok", True), over.pop("why", None)
    eff.update(over)
    f = d / "go-runtime.jsonl"
    f.write_text(json.dumps({"kind": "go_runtime", "ok": ok, "effective": eff if ok else None, "why": why}) + "\n")
    return str(f)


def _check(tmp, declared, eff=None, **kw):
    out = tmp / "snowplow-env.json"
    p = _cli("check", "--declared", declared, "--tracing", str(_reading(tmp, **kw)), "--effective",
             _eff(tmp, declared, **(eff or {})), "--arm", "N2", "--out", str(out))
    return p, json.loads(out.read_text())


def test_the_pod_has_the_declared_value_and_the_undeclared_keys_are_recorded(tmp_path):
    p, rec = _check(tmp_path, "GOGC=100", cm={"GOGC": "100", "GOMEMLIMIT": "7GiB", "LOG_LEVEL": "warn"})
    assert p.returncode == 0 and rec["verified"] is True and rec["arm"] == "N2", p.stdout
    assert rec["observed"]["GOGC"]["state"] == "set" and rec["observed"]["GOGC"]["value"] == "100"
    # what no one declared is recorded with its value (the arms are compared on it), and nothing but Go keys
    assert {k: (r["state"], r["value"]) for k, r in rec["undeclared"].items()} == {
        "GOMEMLIMIT": ("set", "7GiB"), "GOMAXPROCS": ("unset", None)}
    assert "LOG_LEVEL" not in json.dumps(rec)
    assert rec["effective"] == {"gogc": 100, "memory_limit_bytes": 7 * GIB, "memory_limit_source": "cgroup_v2",
                                "cgroup_memory_limit_bytes": 8 * GIB}
    assert ("verified from the running process: the process runs with GOGC 100, memory limit 7.00 GiB (7516192768 bytes) "
            "(source cgroup_v2; the container's limit 8.00 GiB (8589934592 bytes)); the pod's env has every declared "
            "key as declared (GOGC=100 (the Go default, set explicitly)); undeclared in the env: GOMAXPROCS unset, "
            "GOMEMLIMIT=7GiB") in p.stdout


@pytest.mark.parametrize("kw, why", [
    (dict(cm={"GOGC": "50"}), "declared GOGC=100, the pod has GOGC=50"),              # the arm inherited
    (dict(cm={}), "declared GOGC=100, the pod has GOGC unset"),
    (dict(cm={"GOGC": ""}), "declared GOGC=100, the pod has GOGC="),                   # an empty string is not 100
    (dict(cm={"GOGC": "100"}, env=(("GOGC", "50"),)), "declared GOGC=100, the pod has GOGC=50"),   # env over envFrom
    (dict(cm={"GOGC": "100"}, cm_written="2026-11-14T09:30:00Z"), "declared GOGC=100, the pod has GOGC UNKNOWN"),
    (dict(cm={"GOGC": "100"}, secret=True), "an envFrom Secret could set further GO* keys"),
])
def test_a_pod_that_does_not_have_the_declaration_fails_the_check(tmp_path, kw, why):
    """Read from the container's real sources: its env list wins over its ConfigMap, and a ConfigMap
    written after the container started proves nothing about what the process has."""
    p, rec = _check(tmp_path, "GOGC=100", **kw)
    assert p.returncode == 1 and rec["verified"] is False, p.stdout
    assert why in rec["why"] and "NOT as declared: " in p.stdout


def test_no_reading_of_the_pod_is_not_a_pass(tmp_path):
    out = tmp_path / "snowplow-env.json"
    p = _cli("check", "--declared", "GOGC=100", "--tracing", str(tmp_path / "none.json"), "--effective",
             _eff(tmp_path, "GOGC=100"), "--out", str(out))
    assert p.returncode == 1 and json.loads(out.read_text())["verified"] is False
    assert "the run took no reading of the pod" in p.stdout


def test_a_run_that_declares_nothing_still_records_what_its_pod_has(tmp_path):
    p, rec = _check(tmp_path, "", cm={"GOGC": "50"})
    assert p.returncode == 0 and rec["declared"] == {} and rec["undeclared"]["GOGC"]["value"] == "50"


# ─── the settings the PROCESS runs with ─────────────────────────────────────

EFFECTIVE_WRONG = {
    # the env says 100 and the process runs with 50: the env is not the fact
    "the process's GOGC is not the declared one": (
        "GOGC=100", dict(gogc=50), "declared GOGC=100, the process runs with GOGC 50"),
    "GC off, declared 100": ("GOGC=100", dict(gogc=-1), "declared GOGC=100, the process runs with GOGC off"),
    "the declared memory limit is not the process's": (
        "GOGC=100 GOMEMLIMIT=6GiB", dict(memory_limit_bytes=7 * GIB),
        "declared GOMEMLIMIT=6GiB, the process runs with a memory limit of 7.00 GiB (7516192768 bytes)"),
    "the settings could not be read (the image does not publish them)": (
        "GOGC=100", dict(ok=False, why="/debug/vars holds no snowplow_go_runtime"),
        "the settings the process runs with could not be read: /debug/vars holds no snowplow_go_runtime"),
    "the login failed": ("GOGC=100", dict(ok=False, why="the login failed (HTTPError)"),
                         "could not be read: the login failed (HTTPError)"),
}


@pytest.mark.parametrize("case", list(EFFECTIVE_WRONG))
def test_a_process_that_does_not_run_with_the_declaration_fails_the_check(tmp_path, case):
    """The pod's env has the declaration in every case: what fails the arm is the process's own answer."""
    declared, eff, why = EFFECTIVE_WRONG[case]
    cm = {k: v for k, v in snowplow_env.parse(declared).items()}
    p, rec = _check(tmp_path, declared, eff=eff, cm=cm)
    assert p.returncode == 1 and rec["verified"] is False, p.stdout
    assert why in rec["why"] and "NOT as declared: " in p.stdout


def test_no_reading_of_the_process_is_not_a_pass(tmp_path):
    out = tmp_path / "snowplow-env.json"
    args = ["check", "--declared", "GOGC=100", "--tracing", str(_reading(tmp_path, cm={"GOGC": "100"})), "--out", str(out)]
    for extra, why in (([], "no reading of the process's settings was taken"),
                       (["--effective", str(tmp_path / "none.jsonl")], "is not there"),
                       (["--effective", str(tmp_path / "empty.jsonl")], "printed no reading")):
        (tmp_path / "empty.jsonl").write_text("Traceback (most recent call last):\n")
        p = _cli(*args, *extra)
        rec = json.loads(out.read_text())
        assert p.returncode == 1 and rec["verified"] is False and rec["effective"] is None
        assert "the settings the process runs with could not be read" in rec["why"] and why in rec["why"]


def test_declared_values_are_compared_as_the_runtime_holds_them():
    assert snowplow_env.gogc_value("off") == -1 and snowplow_env.gogc_value("50") == 50
    assert snowplow_env.memlimit_bytes("7GiB") == 7 * GIB and snowplow_env.memlimit_bytes("1048576") == 2 ** 20
    assert snowplow_env.memlimit_bytes("512MiB") == 512 * 2 ** 20 and snowplow_env.memlimit_bytes("off") == 2 ** 63 - 1
    assert snowplow_env.memlimit_bytes("7G") is None                # Go takes no such unit: never guessed
    assert snowplow_env.size_text(2 ** 63 - 1) == "no limit"


def test_a_declared_memory_limit_the_process_has_passes(tmp_path):
    p, rec = _check(tmp_path, "GOGC=50 GOMEMLIMIT=6GiB", eff=dict(memory_limit_bytes=6 * GIB, memory_limit_source="env"),
                    cm={"GOGC": "50", "GOMEMLIMIT": "6GiB"})
    assert p.returncode == 0 and rec["verified"] is True, p.stdout


def test_the_runtime_job_reads_what_snowplow_publishes():
    """load606's go-runtime record, from /debug/vars as snowplow 1.12.70 answers it (the key and its four
    fields are internal/memlimit/expvar.go's)."""
    from bench import load606
    body = json.dumps({"snowplow_go_runtime": {"gogc": 100, "memory_limit_bytes": 7 * GIB,
                                               "memory_limit_source": "cgroup_v2",
                                               "cgroup_memory_limit_bytes": 8 * GIB}, "memstats": {}})
    rec = load606.go_runtime_record(200, body)
    assert rec["ok"] is True and rec["effective"]["memory_limit_source"] == "cgroup_v2"
    for status, b, why in ((401, "", "/debug/vars answered 401"), (200, "<html>", "answered no JSON"),
                           (200, json.dumps({"memstats": {}}), "holds no snowplow_go_runtime"),
                           (200, json.dumps({"snowplow_go_runtime": {"gogc": 100}}), "lacks memory_limit_bytes")):
        r = load606.go_runtime_record(status, b)
        assert r["ok"] is False and r["effective"] is None and why in r["why"]
    full = json.loads(body)["snowplow_go_runtime"]
    for field in load606.GO_RUNTIME_FIELDS:           # a reading without any one of the four is no reading
        less = {k: v for k, v in full.items() if k != field}
        r = load606.go_runtime_record(200, json.dumps({"snowplow_go_runtime": less}))
        assert r["ok"] is False and r["effective"] is None and r["why"] == f"snowplow_go_runtime lacks {field}"
    assert load606.GO_RUNTIME_FIELDS == ("gogc", "memory_limit_bytes", "memory_limit_source",
                                         "cgroup_memory_limit_bytes")
    from bench import load606_job
    job = load606_job.render_go_runtime(snowplow_url="http://s", authn_url="http://a", user="u", password="pw",
                                        deadline=1794812999, preflight_user="p", init="/call?x")
    assert "name: runtime606" in job and "load606.py go-runtime" in job and "bench.krateo.io/pool: loadgen" in job
    assert "--deadline 1794812999" in job      # the step's own deadline, never one of the Job's making


# ─── agreed between the arms of a pair, AFTER the measure (owner's ruling 2026-11-16) ───
#
# No arm waits for another before its load: each arm's reading is recorded, and the pair (custlat pair and
# replica) compares them after the measure (snowplow_env.agreement). One that differs makes the pair INVALID.

def _arm_record(shared, tmp, arm, declared, cm, eff=None):
    d = tmp / arm
    d.mkdir(exist_ok=True)
    out = d / "snowplow-env.json"
    p = _cli("check", "--declared", declared, "--tracing", str(_reading(d, cm=cm)), "--effective",
             _eff(d, declared, **(eff or {})), "--arm", arm, "--out", str(out))
    assert p.returncode == 0, p.stdout
    shared.mkdir(exist_ok=True)
    shutil.copy(out, shared / f"{arm}.json")


def _agreement(shared, a, b):
    return snowplow_env.agreement(json.loads((shared / f"{a}.json").read_text()),
                                  json.loads((shared / f"{b}.json").read_text()), (a, b))


def test_arms_that_differ_only_in_what_they_declared_agree(tmp_path):
    shared = tmp_path / "shared"
    for arm, gogc in (("N1", "50"), ("N1B", "50"), ("N2", "100")):
        _arm_record(shared, tmp_path, arm, f"GOGC={gogc}", {"GOGC": gogc, "GOMEMLIMIT": "7GiB"})
    assert _agreement(shared, "N1", "N2") == [] and _agreement(shared, "N1", "N1B") == []


def test_an_undeclared_key_that_differs_between_arms_makes_their_pair_invalid(tmp_path):
    shared = tmp_path / "shared"
    _arm_record(shared, tmp_path, "N1", "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "7GiB"})
    _arm_record(shared, tmp_path, "N2", "GOGC=100", {"GOGC": "100", "GOMEMLIMIT": "6GiB"})
    assert "GOMEMLIMIT is declared on no run and differs between them: N1: GOMEMLIMIT=7GiB, N2: GOMEMLIMIT=6GiB" \
        in _agreement(shared, "N1", "N2")
    # set on one arm and unset on the other differs too
    _arm_record(shared, tmp_path, "N2", "GOGC=100", {"GOGC": "100"})
    assert any("N1: GOMEMLIMIT=7GiB, N2: GOMEMLIMIT unset" in d for d in _agreement(shared, "N1", "N2"))


@pytest.mark.parametrize("eff, why", [
    (dict(memory_limit_bytes=6 * GIB), "the memory limit the process runs with differs between the runs: N1: "
                                       "7.00 GiB (7516192768 bytes), N2: 6.00 GiB (6442450944 bytes)"),
    (dict(memory_limit_source="env"), "where the process's memory limit comes from differs between the runs: N1: "
                                      "cgroup_v2, N2: env"),
])
def test_processes_with_different_memory_limits_disagree(tmp_path, eff, why):
    """Nothing in either pod's env differs: only what the processes run with."""
    shared = tmp_path / "shared"
    _arm_record(shared, tmp_path, "N1", "GOGC=50", {"GOGC": "50"})
    _arm_record(shared, tmp_path, "N2", "GOGC=100", {"GOGC": "100"}, eff=eff)
    assert why in _agreement(shared, "N1", "N2")


def test_an_undeclared_gogc_must_be_the_same_and_a_declared_one_may_differ(tmp_path):
    shared = tmp_path / "shared"
    _arm_record(shared, tmp_path, "N1", "", {}, eff=dict(gogc=100))
    _arm_record(shared, tmp_path, "N2", "", {}, eff=dict(gogc=50))
    assert "the GOGC the process runs with (declared on no run) differs between the runs: N1: 100, N2: 50" \
        in _agreement(shared, "N1", "N2")
    declared = tmp_path / "declared"
    _arm_record(declared, tmp_path, "N1", "GOGC=50", {"GOGC": "50"})
    _arm_record(declared, tmp_path, "N2", "GOGC=100", {"GOGC": "100"})
    assert _agreement(declared, "N1", "N2") == []


def test_an_arm_with_no_verified_reading_is_not_compared_it_is_invalid_on_its_own(tmp_path):
    shared = tmp_path / "shared"
    _arm_record(shared, tmp_path, "N1", "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "7GiB"})
    rec = json.loads((shared / "N1.json").read_text())
    assert snowplow_env.agreement(rec, dict(rec, verified=False, undeclared={}), ("N1", "N2")) == []
    assert snowplow_env.agreement(rec, None, ("N1", "N2")) == []
    assert snowplow_env.validity({"state": "NOT_VERIFIED", "why": "x"})["result"] == "FAIL"


def test_no_run_waits_for_another_before_its_load():
    src = (BENCH / "run606.sh").read_text() + (BENCH / "run606_pair.sh").read_text()
    for gone in ("ENV_AGREE_WAIT_S", "SNOWPLOW_ENV_DIR", "SNOWPLOW_ENV_ARMS", ".gone", "snowplow_env agree"):
        assert gone not in src, gone


# ─── the launcher: the same keys on every run, and what a pair may compare ──

SPEC = {"N1": ("snowplow-bench-gc1", DIG), "N1B": ("snowplow-bench-gc1b", DIG), "N2": ("snowplow-bench-gc2", DIG)}
GC = {"SNOWPLOW_ENV_N1": "GOGC=50", "SNOWPLOW_ENV_N1B": "GOGC=50", "SNOWPLOW_ENV_N2": "GOGC=100"}


def _gc_launch(pairbed, **over):  # noqa: F811
    repo, fake, env, tmp = pairbed
    e = _launch_env(env, tmp, SPEC, "N1:N1B N1:N2", REPLICA_PAIR="N1:N1B", BENCH_606_DRY_RUN="1", **ROOM_FOR_THREE)
    e.update(GC)
    e.update(over)
    return _launch(repo, {k: v for k, v in e.items() if v is not None}), fake


def test_one_image_under_two_declared_envs_is_a_pair(pairbed):  # noqa: F811
    p, _ = _gc_launch(pairbed)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "N1=snowplow-bench-gc1 (tag-n1, snowplow env GOGC=50)" in p.stderr
    assert "N2=snowplow-bench-gc2 (tag-n2, snowplow env GOGC=100)" in p.stderr


@pytest.mark.parametrize("over, refusal", [
    ({"SNOWPLOW_ENV_N2": None},
     "the runs do not declare the same snowplow env keys (N1: GOGC; N1B: GOGC; N2: none)"),
    ({"SNOWPLOW_ENV_N2": "GOGC=100 GOMEMLIMIT=7GiB"},
     "the runs do not declare the same snowplow env keys (N1: GOGC; N1B: GOGC; N2: GOGC GOMEMLIMIT)"),
    ({"SNOWPLOW_ENV_N2": "GOGC=50"},
     "pair N1:N2: the two digests are the same image and the two runs declare the same snowplow env: nothing to "
     "compare"),
    ({"SNOWPLOW_ENV_N1B": "GOGC=100"},
     "REPLICA_PAIR N1:N1B: an A/A replica runs ONE snowplow env on both arms"),
    ({"SNOWPLOW_ENV_N2": "GOGC="}, "SNOWPLOW_ENV: GOGC's value '' is empty"),
    ({"SNOWPLOW_ENV": "GOGC=100"}, "SNOWPLOW_ENV is per run"),
])
def test_the_launch_is_refused_before_anything_starts(pairbed, over, refusal):  # noqa: F811
    p, fake = _gc_launch(pairbed, **over)
    assert p.returncode == 2 and f"refusing: {refusal}" in p.stderr, p.stderr[-2000:]
    assert not (fake / "calls").exists()                                       # not even a dry pass


def test_the_launcher_gives_each_run_its_declaration_and_its_label(pairbed):  # noqa: F811
    src = (BENCH / "run606_pair.sh").read_text()
    assert 'RUNENV+=(SNOWPLOW_ENV="${!se:-}" BENCH_ARM="$1")' in src


# ─── the pair's one line ────────────────────────────────────────────────────

def _verdict(d, digest, declared=None, cm=None, eff=None):
    """A run's verdict.json holding what the pair reads: the expected digest, and the snowplow-env
    record evaluate reports (the record made by `check` from a real reading)."""
    d.mkdir()
    report = {"image_digest": {"expected": digest}}
    if declared is not None:
        out = d / "snowplow-env.json"
        _cli("check", "--declared", declared, "--tracing", str(_reading(d, cm=cm)), "--effective",
             _eff(d, declared, **(eff or {})), "--out", str(out))
        report["snowplow_env"] = snowplow_env.report(str(out))
    (d / "verdict.json").write_text(json.dumps({"verdict": "PASS", "report": report}))
    return d


def _pair(da, db, *extra):
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "pair", "--a", str(da / "verdict.json"), "--b",
                        str(db / "verdict.json"), "--a-label", "N1", "--b-label", "N2", *extra],
                       cwd=BENCH, capture_output=True, text=True, timeout=300)
    return json.loads(p.stdout)["arms"]


def test_the_pair_says_same_image_different_env_in_one_line(tmp_path):
    da = _verdict(tmp_path / "a", DIG, "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "7GiB"})
    db = _verdict(tmp_path / "b", DIG, "GOGC=100", {"GOGC": "100", "GOMEMLIMIT": "7GiB"})
    arms = _pair(da, db, "--not-comparable", "run 26i: installer 0.3.616 there, 0.3.641 here")
    assert arms["same_image"] is True and arms["env_differs"] is True and arms["undeclared_differ"] == []
    assert arms["effective_differs"] == ["gogc"]
    assert arms["lines"] == [
        f"INFO arms: the same image ({DIG}); their processes ran with different GC settings: GOGC 50 on N1 "
        f"(baseline), GOGC 100 on N2 (candidate); memory limit 7.00 GiB (7516192768 bytes) from cgroup_v2 on both (they differ "
        f"in GC settings only, not in image); each read from the running process alongside the load's start (declared: N1 "
        f"(baseline) GOGC=50; N2 (candidate) GOGC=100 (the Go default, set explicitly))",
        "INFO not comparable with run 26i: installer 0.3.616 there, 0.3.641 here"]


def test_the_pair_says_image_only_both_and_unknown_in_their_own_words(tmp_path):
    a = _verdict(tmp_path / "a", DIG, "GOGC=50", {"GOGC": "50"})
    same_env = _verdict(tmp_path / "b", DIG2, "GOGC=50", {"GOGC": "50"})
    (line,) = _pair(a, same_env)["lines"]
    assert (f"different images (N1 (baseline): {DIG}; N2 (candidate): {DIG2}); their processes ran with the same GC "
            f"settings: GOGC 50 on both; memory limit 7.00 GiB (7516192768 bytes) from cgroup_v2 on both (they differ in image "
            f"only)") in line
    both = _verdict(tmp_path / "c", DIG2, "GOGC=100", {"GOGC": "100"})
    (line,) = _pair(a, both)["lines"]
    assert "different images" in line and "different GC settings: GOGC 50 on N1 (baseline), GOGC 100 on N2" in line
    assert "only" not in line
    # an arm whose pod did not have the declaration, or with no record: UNKNOWN, never "the same"
    bad = _verdict(tmp_path / "d", DIG, "GOGC=100", {"GOGC": "50"})
    old = _verdict(tmp_path / "e", DIG)
    for other in (bad, old):
        arms = _pair(a, other)
        assert arms["env_differs"] is None
        assert arms["lines"] == [f"INFO arms: the same image ({DIG}); snowplow's GC settings UNKNOWN: no record "
                                 f"verified from the running process for N2 (candidate)"]
    # an undeclared key that differs is said, and, compared after the measure, makes the pair INVALID (named)
    und = _verdict(tmp_path / "f", DIG, "GOGC=100", {"GOGC": "100", "GOMEMLIMIT": "6GiB"})
    a2 = _verdict(tmp_path / "g", DIG, "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "7GiB"})
    line, invalid = _pair(a2, und)["lines"]
    assert "UNDECLARED env keys differ: GOMEMLIMIT is declared on no run and differs between them" in line
    assert invalid.startswith("INVALID arms: snowplow env, compared after the measure from each arm's reading: "
                              "GOMEMLIMIT is declared on no run and differs between them")
    # the same env on both arms, and processes that run with different memory limits: said from the processes
    m1 = _verdict(tmp_path / "h", DIG, "GOGC=50", {"GOGC": "50"})
    m2 = _verdict(tmp_path / "i", DIG, "GOGC=50", {"GOGC": "50"}, eff=dict(memory_limit_bytes=6 * GIB))
    arms = _pair(m1, m2)
    assert arms["env_differs"] is False and arms["effective_differs"] == ["memory_limit_bytes"]
    assert ("their processes ran with different GC settings: GOGC 50 on both; memory limit 7.00 GiB (7516192768 bytes) from "
            "cgroup_v2 on N1 (baseline), 6.00 GiB (6442450944 bytes) from cgroup_v2 on N2 (candidate)") in arms["lines"][0]


def test_evaluate_reports_the_record_and_says_unknown_without_one(tmp_path):
    f = tmp_path / "fresh-x.jsonl"
    f.write_text(json.dumps({"kind": "churn", "label": "x@1", "t0": 1.0, "t1": 2.0, "target_rate": 1}) + "\n")
    out = tmp_path / "snowplow-env.json"
    _cli("check", "--declared", "GOGC=100", "--tracing", str(_reading(tmp_path, cm={"GOGC": "100"})), "--effective",
         _eff(tmp_path, "GOGC=100"), "--out", str(out))
    for path, state, said in ((out, "VERIFIED", "INFO snowplow Go runtime, effective (read from the running process "
                                                "alongside the load's start): GOGC 100, memory limit 7.00 GiB (7516192768 bytes)"),
                              (tmp_path / "none.json", "UNKNOWN", "INFO snowplow env: UNKNOWN (the run has no "
                                                                  "snowplow-env record")):
        p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f), "--snowplow-env", str(path)],
                           cwd=BENCH, capture_output=True, text=True, timeout=300)
        sec = json.loads(p.stdout)["report"]["snowplow_env"]
        assert sec["state"] == state and said in sec["lines"][0] and said in p.stderr


# ─── the real run: run606.sh against the fakes ──────────────────────────────

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


@needs_helm
def test_run606_installs_the_declaration_and_proves_it_from_the_pod_before_load(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    import yaml
    vals = yaml.safe_load((out / "installer-values.yaml").read_text())
    assert vals["componentValues"]["snowplow"]["env"] == {"GOGC": "100"}
    assert json.loads((out / "snowplow-env-render.json").read_text())["rendered"] == {"GOGC": "100"}
    rec = json.loads((out / "snowplow-env.json").read_text())
    assert rec["verified"] is True and rec["declared"] == {"GOGC": "100"}
    assert rec["observed"]["GOGC"] == {"state": "set", "value": "100", "from": rec["observed"]["GOGC"]["from"]}
    # rendered before any cluster, proven before any load
    i = {k: log.index(k) for k in ("snowplow env declared: GOGC=100 (the Go default, set explicitly); rendered:",
                                   "=== step up ===", "snowplow env verified from the running process:", "driver done")}
    assert sorted(i.values()) == list(i.values()), i
    v = json.loads((out / "verdict.json").read_text())
    assert v["report"]["snowplow_env"]["state"] == "VERIFIED"
    # never a patch of the Deployment or of its ConfigMap
    calls = [json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines()]
    kube = [c["argv"] for c in calls if c["tool"] == "kubectl"]
    assert not any(a[:1] == ["patch"] or "set" in a[:2] for a in kube if "snowplow" in " ".join(a) and "GOGC" in " ".join(a))
    assert not any("GOGC" in " ".join(a) for a in kube)


@needs_helm
@pytest.mark.parametrize("extra, where, why", [
    # the platform's ConfigMap never got it (the arm would run at the installer's 50): the load runs (it
    # starts right after Ready, the owner's ruling 2026-11-16) and the run is INVALID, named
    ({"FAKE_SNOWPLOW_GO_ENV": '{"GOGC": "50"}'}, "measure",
     "snowplow_env.verified: snowplow env NOT as declared (the Go runtime read's deadline is the new pod's Ready + "
     "its budget): declared GOGC=100, the process runs with GOGC 50; declared GOGC=100, the pod has GOGC=50"),
    # an installer that does not carry the declaration into its CR: before any cluster
    ({"FAKE_INSTALLER_HOLDS_GOGC": "50"}, "snowplow-env",
     "snowplow-env FAILED: the installer does not render the declared snowplow env (GOGC=100): NOT rendered as "
     "declared: GOGC renders as '50', not '100'"),
])
def test_run606_refuses_an_arm_that_would_not_run_what_it_declared(harness, extra, where, why):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", **extra)
    log = (tmp / "out" / "run.log").read_text()
    if where == "measure":
        v = json.loads((tmp / "out" / "verdict.json").read_text())
        assert p.returncode == 0 and "run complete" in log and v["verdict"] == "INVALID", log[-3000:]
        assert why in v["invalid_reasons"] and f"INVALID: {why}" in log
    else:
        assert p.returncode != 0 and why in log, log[-3000:]
        assert "run complete" not in log and "driver done" not in log
    gcl = [json.loads(x)["argv"][:3] for x in (state / "calls.jsonl").read_text().splitlines()
           if json.loads(x)["tool"] == "gcloud"]
    created = ["container", "clusters", "create"] in gcl
    assert created == (where == "measure")                # the render refuses before anything is created


@needs_helm
def test_the_rehearsal_installs_reads_the_pod_prints_one_block_and_tears_down_without_load(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-3000:]
    block = log[log.index("=== rehearsal report ==="):log.index("=== end of rehearsal report ===")]
    for said in ("maintenance exclusion: VERIFIED: bench-run-ttl (no_upgrades, ",
                 'read back, as GKE prints it: {"endTime": ', "the cluster's createTime, as GKE prints it: '",
                 "the step took ", "the update was not refused (no operation was in flight)",
                 "cluster operations: readable; control CREATE_CLUSTER operation-",
                 "CREATE_NODE_POOL on nodePool/snowplow", "UPDATE_CLUSTER ",
                 'snowplow env, rendered by the installer chart: as declared: componentValues.snowplow.env of the '
                 'Installer CR the chart applies: {"GOGC": "100"}',
                 "the snowplow process's effective GC settings, read before any load (/debug/vars snowplow_go_runtime, "
                 "as the bench's debug reader): GOGC 100, memory limit 7.00 GiB (7516192768 bytes) (source cgroup_v2; the "
                 "container's limit 8.00 GiB (8589934592 bytes)); raw: {",
                 "the pod's Go runtime env, read before any load: GOGC=100 (",
                 "comparison with the declaration: VERIFIED: declared GOGC=100 (the Go default, set explicitly)"):
        assert said in block, (said, block)
    # no data, no drivers, no verdict; the cluster is deleted
    # the reading's user is the bench's debug reader, created by the rehearsal before the Job logs in; the
    # preflight user is the rehearsal's preflight's (tests/test_debug_reader.py)
    users = (state / "users").read_text().split()
    assert users == ["scaleuser-debug-reader", "scaleuser-0000"]
    for step in ("=== step data ===", "=== step measure ===", "=== step evaluate ===", "run complete"):
        assert step not in log
    assert not (out / "verdict.json").exists()
    gcl = [json.loads(x)["argv"][:3] for x in (state / "calls.jsonl").read_text().splitlines()
           if json.loads(x)["tool"] == "gcloud"]
    assert ["container", "clusters", "delete"] in gcl


@needs_helm
def test_a_rehearsal_whose_pod_is_not_as_declared_says_so_and_exits_non_zero(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_SNOWPLOW_GO_ENV='{"GOGC": "50"}')
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "rehearsal complete" not in log
    assert "comparison with the declaration: NOT AS DECLARED: declared GOGC=100 (the Go default, set explicitly); " \
           "declared GOGC=100, the process runs with GOGC 50; declared GOGC=100, the pod has GOGC=50" in log


def test_run606_refuses_a_bad_declaration_or_rehearsal_mode_at_once(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=")
    assert p.returncode == 2 and "refusing: SNOWPLOW_ENV: GOGC's value '' is empty" in p.stderr
    p = _run606(env, BENCH_606_REHEARSAL="all")
    assert p.returncode == 2 and "BENCH_606_REHEARSAL must be 'env' or unset" in p.stderr
    assert not (state / "calls.jsonl").exists()


# ─── the real run: the process's own answer decides ─────────────────────────

@needs_helm
@pytest.mark.parametrize("extra, why", [
    # the pod's env has GOGC=100 (the install declared it) and the process runs with 50
    ({"FAKE_GO_RUNTIME": '{"gogc": 50}'}, "declared GOGC=100, the process runs with GOGC 50"),
    # an image that does not publish its settings: nothing to hold the declaration against
    ({"FAKE_GO_RUNTIME_FAIL": "nokey"},
     "the settings the process runs with could not be read: /debug/vars holds no snowplow_go_runtime"),
    # the read never answered within its deadline (the new pod's Ready + GO_RUNTIME_S)
    ({"FAKE_GO_RUNTIME_FAIL": "noanswer"},
     "the settings the process runs with could not be read: /debug/vars: no answer to 8 attempt(s) over 275.0 s"),
])
def test_a_run_whose_process_does_not_run_with_the_declaration_is_invalid_and_its_load_ran(harness, extra, why):  # noqa: F811
    """The owner's ruling (2026-11-16): the load starts right after Ready and the read runs alongside it;
    the run counts only if the read answered in time as declared."""
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", **extra)
    log = (tmp / "out" / "run.log").read_text()
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    assert p.returncode == 0 and "run complete" in log and "driver done" in log, log[-3000:]
    assert v["verdict"] == "INVALID"
    [r] = [r for r in v["invalid_reasons"] if r.startswith("snowplow_env.verified: ")]
    assert why in r, r


@needs_helm
def test_run606_reads_the_process_before_load_and_keeps_the_password_out(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    rec = json.loads((out / "snowplow-env.json").read_text())
    assert rec["effective"] == {"gogc": 100, "memory_limit_bytes": 7 * GIB, "memory_limit_source": "cgroup_v2",
                                "cgroup_memory_limit_bytes": 8 * GIB}
    lines = [json.loads(x) for x in (out / "go-runtime.jsonl").read_text().splitlines()]
    assert [x["kind"] for x in lines if x["kind"].startswith("go_runtime")] == ["go_runtime_attempt", "go_runtime"]
    assert lines[-1]["kind"] == "go_runtime" and lines[-1]["ok"] is True
    # the Job ran before any driver, and the bench user's password is nowhere it was written
    calls = [json.loads(x)["argv"] for x in (state / "calls.jsonl").read_text().splitlines()
             if json.loads(x)["tool"] == "kubectl"]
    i_job = next(i for i, a in enumerate(calls) if "job/runtime606" in a and "logs" in a)
    i_load = next(i for i, a in enumerate(calls) if "apply" in a and any("loadgen-job.yaml" in x for x in a))
    assert i_load < i_job        # the load starts right after Ready; the read runs alongside it
    pw = env["FAKE_PW"]
    for f in ("go-runtime-job.yaml", "run.log", "go-runtime.jsonl", "snowplow-env.json"):
        assert pw not in (out / f).read_text(), f
    assert "name: runtime606" in (out / "go-runtime-job.yaml").read_text()
    v = json.loads((out / "verdict.json").read_text())
    assert v["report"]["snowplow_env"]["lines"][0].startswith(
        "INFO snowplow Go runtime, effective (read from the running process alongside the load's start): GOGC 100, memory "
        "limit 7.00 GiB (7516192768 bytes)")
    # the Job's wait is a term of the plan: named in the line, counted in the total, and the total with
    # its margin fits the cluster's TTL
    import re
    m = re.search(r"run budget: (.*) = (\d+) s; fits TTL_HOURS=\d+ \((\d+) s; .* minus (\d+) s", log)
    line, total, ttl_s, margin = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
    assert " + go runtime 300 + measure " in line
    flat = " ".join(re.sub(r"\([^()]*\)", "", line).split())
    terms = [int(n) * int(k) if n else int(k) for n, k in re.findall(r"(?:(\d+)x)?(\d+)(?= \+|$)", flat)]
    assert len(terms) == flat.count(" + ") + 1 and sum(terms) == total, (flat, total)
    assert total + margin <= ttl_s


