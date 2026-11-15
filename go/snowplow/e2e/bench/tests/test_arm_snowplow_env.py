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


def _check(tmp, declared, **kw):
    out = tmp / "snowplow-env.json"
    p = _cli("check", "--declared", declared, "--tracing", str(_reading(tmp, **kw)), "--arm", "N2", "--out", str(out))
    return p, json.loads(out.read_text())


def test_the_pod_has_the_declared_value_and_the_undeclared_keys_are_recorded(tmp_path):
    p, rec = _check(tmp_path, "GOGC=100", cm={"GOGC": "100", "GOMEMLIMIT": "7GiB", "LOG_LEVEL": "warn"})
    assert p.returncode == 0 and rec["verified"] is True and rec["arm"] == "N2", p.stdout
    assert rec["observed"]["GOGC"]["state"] == "set" and rec["observed"]["GOGC"]["value"] == "100"
    # what no one declared is recorded with its value (the arms are compared on it), and nothing but Go keys
    assert {k: (r["state"], r["value"]) for k, r in rec["undeclared"].items()} == {
        "GOMEMLIMIT": ("set", "7GiB"), "GOMAXPROCS": ("unset", None)}
    assert "LOG_LEVEL" not in json.dumps(rec)
    assert "verified from the pod: the pod has every declared key as declared (GOGC=100 (the Go default, set " \
           "explicitly)); undeclared: GOMAXPROCS unset, GOMEMLIMIT=7GiB" in p.stdout


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
    p = _cli("check", "--declared", "GOGC=100", "--tracing", str(tmp_path / "none.json"), "--out", str(out))
    assert p.returncode == 1 and json.loads(out.read_text())["verified"] is False
    assert "the run took no reading of the pod" in p.stdout


def test_a_run_that_declares_nothing_still_records_what_its_pod_has(tmp_path):
    p, rec = _check(tmp_path, "", cm={"GOGC": "50"})
    assert p.returncode == 0 and rec["declared"] == {} and rec["undeclared"]["GOGC"]["value"] == "50"


# ─── agreed between the arms of a launch ────────────────────────────────────

def _arm_record(shared, tmp, arm, declared, cm):
    d = tmp / arm
    d.mkdir(exist_ok=True)
    out = d / "snowplow-env.json"
    p = _cli("check", "--declared", declared, "--tracing", str(_reading(d, cm=cm)), "--arm", arm, "--out", str(out))
    assert p.returncode == 0, p.stdout
    shared.mkdir(exist_ok=True)
    shutil.copy(out, shared / f"{arm}.json")


def _agree(shared, tmp, arm, arms, wait="0"):
    out = tmp / f"agree-{arm}.json"
    p = _cli("agree", "--dir", str(shared), "--arm", arm, "--arms", *arms, "--wait-s", wait, "--out", str(out))
    return p, json.loads(out.read_text())


def test_arms_that_differ_only_in_what_they_declared_agree(tmp_path):
    shared = tmp_path / "shared"
    for arm, gogc in (("N1", "50"), ("N1B", "50"), ("N2", "100")):
        _arm_record(shared, tmp_path, arm, f"GOGC={gogc}", {"GOGC": gogc, "GOMEMLIMIT": "7GiB"})
    p, res = _agree(shared, tmp_path, "N2", ["N1", "N1B", "N2"])
    assert p.returncode == 0 and res["ok"] is True and res["compared"] == ["N1", "N1B"] and res["not_compared"] == []
    assert "undeclared Go runtime keys: the same on N2 and N1, N1B" in p.stdout


def test_an_undeclared_key_that_differs_between_arms_fails_before_load(tmp_path):
    shared = tmp_path / "shared"
    _arm_record(shared, tmp_path, "N1", "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "7GiB"})
    _arm_record(shared, tmp_path, "N2", "GOGC=100", {"GOGC": "100", "GOMEMLIMIT": "6GiB"})
    for arm in ("N1", "N2"):
        p, res = _agree(shared, tmp_path, arm, ["N1", "N2"])
        assert p.returncode == 1 and res["ok"] is False
        assert "GOMEMLIMIT is declared on no run and differs between them: N1: GOMEMLIMIT=7GiB, N2: GOMEMLIMIT=6GiB" \
            in p.stdout
    # set on one arm and unset on the other differs too
    _arm_record(shared, tmp_path, "N2", "GOGC=100", {"GOGC": "100"})
    p, _ = _agree(shared, tmp_path, "N1", ["N1", "N2"])
    assert p.returncode == 1 and "N1: GOMEMLIMIT=7GiB, N2: GOMEMLIMIT unset" in p.stdout


def test_an_arm_with_no_reading_is_waited_for_then_named_not_assumed_equal(tmp_path):
    shared = tmp_path / "shared"
    _arm_record(shared, tmp_path, "N1", "GOGC=50", {"GOGC": "50"})
    now, slept = [0.0], []

    def sleep(s):
        slept.append(s)
        now[0] += s
    res = snowplow_env.agree(str(shared), "N1", ["N1", "N2"], 600, clock=lambda: now[0], sleep=sleep, poll_s=5)
    assert sum(slept) == 600 and max(slept) <= 5                              # its whole bound, and no more
    assert res["ok"] is True and res["compared"] == [] and res["not_compared"] == ["N2"]
    assert "NOT compared with N2 (no reading within 600 s, or the run ended without one)" in res["line"]
    # a run the launcher saw end is not waited for; through the CLI
    (shared / "N2.gone").write_text("")
    slept.clear()
    assert snowplow_env.agree(str(shared), "N1", ["N1", "N2"], 600, clock=lambda: now[0], sleep=sleep)["ok"] is True
    assert slept == []
    p, res = _agree(shared, tmp_path, "N1", ["N1", "N2"], wait="600")
    assert p.returncode == 0 and res["not_compared"] == ["N2"]
    # a reading that arrives during the wait is compared
    (shared / "N2.gone").unlink()

    def arrives(s):
        sleep(s)
        if now[0] >= 900 and not (shared / "N2.json").exists():
            _arm_record(shared, tmp_path, "N2", "GOGC=100", {"GOGC": "100", "GOMEMLIMIT": "7GiB"})
    res = snowplow_env.agree(str(shared), "N1", ["N1", "N2"], 600, clock=lambda: now[0], sleep=arrives)
    assert res["ok"] is False and res["compared"] == ["N2"] and "GOMEMLIMIT" in res["why"]


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


def test_the_launcher_gives_each_run_its_declaration_and_the_shared_readings(pairbed):  # noqa: F811
    src = (BENCH / "run606_pair.sh").read_text()
    assert 'RUNENV+=(SNOWPLOW_ENV="${!se:-}" BENCH_ARM="$1" SNOWPLOW_ENV_DIR="$PAIR_OUT/snowplow-env"' in src
    assert '[[ -f "$PAIR_OUT/snowplow-env/$x.json" ]] || : > "$PAIR_OUT/snowplow-env/$x.gone"' in src


# ─── the pair's one line ────────────────────────────────────────────────────

def _verdict(d, digest, declared=None, cm=None):
    """A run's verdict.json holding what the pair reads: the expected digest, and the snowplow-env
    record evaluate reports (the record made by `check` from a real reading)."""
    d.mkdir()
    report = {"image_digest": {"expected": digest}}
    if declared is not None:
        out = d / "snowplow-env.json"
        _cli("check", "--declared", declared, "--tracing", str(_reading(d, cm=cm)), "--out", str(out))
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
    assert arms["lines"] == [
        f"INFO arms: the same image ({DIG}); they differ in snowplow env: GOGC=50 on N1 (baseline), GOGC=100 (the "
        f"Go default, set explicitly) on N2 (candidate) (in env only, not in image); each read from the running pod "
        f"before the measure",
        "INFO not comparable with run 26i: installer 0.3.616 there, 0.3.641 here"]


def test_the_pair_says_image_only_both_and_unknown_in_their_own_words(tmp_path):
    a = _verdict(tmp_path / "a", DIG, "GOGC=50", {"GOGC": "50"})
    same_env = _verdict(tmp_path / "b", DIG2, "GOGC=50", {"GOGC": "50"})
    (line,) = _pair(a, same_env)["lines"]
    assert f"different images (N1 (baseline): {DIG}; N2 (candidate): {DIG2}); the same declared snowplow env " \
           f"(GOGC=50) (in image only)" in line
    both = _verdict(tmp_path / "c", DIG2, "GOGC=100", {"GOGC": "100"})
    (line,) = _pair(a, both)["lines"]
    assert "different images" in line and "they differ in snowplow env: GOGC=50 on N1 (baseline)" in line
    assert "only" not in line
    # an arm whose pod did not have the declaration, or with no record: UNKNOWN, never "the same"
    bad = _verdict(tmp_path / "d", DIG, "GOGC=100", {"GOGC": "50"})
    old = _verdict(tmp_path / "e", DIG)
    for other in (bad, old):
        arms = _pair(a, other)
        assert arms["env_differs"] is None
        assert arms["lines"] == [f"INFO arms: the same image ({DIG}); snowplow env UNKNOWN: no record verified from "
                                 f"the pod for N2 (candidate)"]
    # an undeclared key that differs is said (the arms' own gate fails the run before load; this is its record)
    und = _verdict(tmp_path / "f", DIG, "GOGC=100", {"GOGC": "100", "GOMEMLIMIT": "6GiB"})
    a2 = _verdict(tmp_path / "g", DIG, "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "7GiB"})
    (line,) = _pair(a2, und)["lines"]
    assert "UNDECLARED keys differ: GOMEMLIMIT is declared on no run and differs between them" in line


def test_evaluate_reports_the_record_and_says_unknown_without_one(tmp_path):
    f = tmp_path / "fresh-x.jsonl"
    f.write_text(json.dumps({"kind": "churn", "label": "x@1", "t0": 1.0, "t1": 2.0, "target_rate": 1}) + "\n")
    out = tmp_path / "snowplow-env.json"
    _cli("check", "--declared", "GOGC=100", "--tracing", str(_reading(tmp_path, cm={"GOGC": "100"})), "--out", str(out))
    for path, state, said in ((out, "VERIFIED", "INFO snowplow env declared: GOGC=100 (the Go default, set "
                                                "explicitly); read from the running pod before the measure: as "
                                                "declared"),
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
                                   "=== step up ===", "snowplow env verified from the pod:", "driver done")}
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
    # the platform's ConfigMap never got it (the arm would run at the installer's 50): before any load
    ({"FAKE_SNOWPLOW_GO_ENV": '{"GOGC": "50"}'}, "measure",
     "measure FAILED before any load: snowplow env NOT as declared: declared GOGC=100, the pod has GOGC=50"),
    # an installer that does not carry the declaration into its CR: before any cluster
    ({"FAKE_INSTALLER_HOLDS_GOGC": "50"}, "snowplow-env",
     "snowplow-env FAILED: the installer does not render the declared snowplow env (GOGC=100): NOT rendered as "
     "declared: GOGC renders as '50', not '100'"),
])
def test_run606_refuses_an_arm_that_would_not_run_what_it_declared(harness, extra, where, why):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", **extra)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and why in log, log[-3000:]
    assert "run complete" not in log and "driver done" not in log
    gcl = [json.loads(x)["argv"][:3] for x in (state / "calls.jsonl").read_text().splitlines()
           if json.loads(x)["tool"] == "gcloud"]
    created = ["container", "clusters", "create"] in gcl
    assert created == (where == "measure")                # the render refuses before anything is created


@needs_helm
def test_run606_fails_before_load_when_an_undeclared_key_differs_from_another_arms(harness):  # noqa: F811
    """Two arms of one launch: the other arm's reading (GOMEMLIMIT 6GiB) is already in the shared
    directory; this arm's pod has 7GiB, which no one declared."""
    env, state, _, tmp = harness
    shared = tmp / "shared"
    other = tmp / "other"
    other.mkdir()
    _arm_record(shared, other, "N1", "GOGC=50", {"GOGC": "50", "GOMEMLIMIT": "6GiB"})
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_ARM="N2", SNOWPLOW_ENV_DIR=str(shared), SNOWPLOW_ENV_ARMS="N1 N2",
                FAKE_SNOWPLOW_GO_ENV='{"GOGC": "100", "GOMEMLIMIT": "7GiB"}')
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0, log[-3000:]
    assert "measure FAILED before any load: undeclared Go runtime keys: DIFFER: GOMEMLIMIT is declared on no run " \
           "and differs between them: N1: GOMEMLIMIT=6GiB, N2: GOMEMLIMIT=7GiB" in log
    assert "driver done" not in log and (shared / "N2.json").exists()
    # the wait for the other runs is a term of the plan
    assert " + env agreement 600 + measure " in log


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
                 "the pod's Go runtime env, read before any load: GOGC=100 (",
                 "comparison with the declaration: VERIFIED: declared GOGC=100 (the Go default, set explicitly)"):
        assert said in block, (said, block)
    # no data, no drivers, no verdict; the cluster is deleted
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
           "declared GOGC=100, the pod has GOGC=50" in log


def test_run606_refuses_a_bad_declaration_or_rehearsal_mode_at_once(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=")
    assert p.returncode == 2 and "refusing: SNOWPLOW_ENV: GOGC's value '' is empty" in p.stderr
    p = _run606(env, BENCH_606_REHEARSAL="all")
    assert p.returncode == 2 and "BENCH_606_REHEARSAL must be 'env' or unset" in p.stderr
    assert not (state / "calls.jsonl").exists()
