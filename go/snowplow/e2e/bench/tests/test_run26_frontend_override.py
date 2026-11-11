"""Run 26: snowplow 1.12.70 vs 1.12.69 with FRONTEND 1.8.38 on both arms.

The installer (0.3.545) pins frontend 1.8.34. The bench overrides the
frontend image per run exactly as it overrides snowplow's:
  - FRONTEND_TAG + EXPECTED_FRONTEND_DIGEST (both or neither), rendered as
    componentValues.frontend.image.tag: the frontend chart's own key (1.8.34
    values.yaml `image.tag`, empty = its appVersion; deployment.yaml joins
    image.registry / image.repository : image.tag through krateo.image);
  - the registry preflight renders the frontend chart with that value and
    resolves the image it renders (`ghcr_pull preflight --override`);
  - after the reboot and BEFORE any load, every live frontend pod's imageID
    carries EXPECTED_FRONTEND_DIGEST, or the run is VOID (the snowplow gate's
    shape, recorded as `frontend_image_digest`);
  - the verdict records it (report.frontend_image, check frontend.image_digest),
    the pair launcher refuses runs whose frontends differ, and records each
    run's frontend in pair.json, its runs line and every paired verdict.
Unset: the installer's pinned frontend runs and nothing changes (byte-identical
installer values, the same evaluate arguments and verdict).
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest
import yaml

from bench import custlat, load606
from tests.test_606 import _run606
from tests.test_run606_real_sequence import (  # noqa: F401
    BENCH_DIR, FAKE_DIGEST, OTHER_DIGEST, _calls, harness)
from tests.test_run606_pr483 import _env483
from tests.test_run15_customer_latency import (  # noqa: F401
    DIG_A, DIG_B, REPLICA, _ab, _launch, pairbed, va)
from tests.test_run15_customer_latency import _calls as _pair_calls

FD = "sha256:" + "1a" * 32          # the frontend's expected digest
FD_OTHER = "sha256:" + "2b" * 32
FTAG = "1.8.38"
SP_DIG = "sha256:" + "ab" * 32      # the snowplow digest test_606._run606 launches with
BLOCK = f'\n  frontend:\n    image:\n      tag: "{FTAG}"'


def _fpod(name, digest, terminating=False):
    meta = {"name": name}
    if terminating:
        meta["deletionTimestamp"] = "2026-11-11T20:00:00Z"
    return {"metadata": meta, "status": {"containerStatuses": [{
        "name": "frontend", "image": f"ghcr.io/krateo-platformops/frontend:{FTAG}",
        "imageID": f"ghcr.io/krateo-platformops/frontend@{digest}"}]}}


# ─── the gate and the verdict (load606) ─────────────────────────────────────


def test_the_frontend_gate_is_the_snowplow_gate_under_its_own_kind():
    ok = load606.image_digest_gate({"items": [_fpod("f", FD)]}, FD, kind="frontend_image_digest", what="frontend")
    assert ok["kind"] == "frontend_image_digest" and ok["result"] == "MATCH"
    bad = load606.image_digest_gate({"items": [_fpod("f", FD_OTHER)]}, FD, kind="frontend_image_digest",
                                    what="frontend")
    assert bad["result"] == "VOID" and "digest mismatch: f/frontend" in bad["why"]
    none = load606.image_digest_gate({"items": []}, FD, kind="frontend_image_digest", what="frontend")
    assert none["result"] == "VOID" and none["why"] == "no live frontend pod"
    left = load606.image_digest_gate({"items": [_fpod("old", FD_OTHER, True), _fpod("new", FD)]}, FD,
                                     require_settled=True, kind="frontend_image_digest", what="frontend")
    assert left["result"] == "UNSETTLED"
    # the default is the snowplow gate, unchanged
    assert load606.image_digest_gate({"items": []}, FD)["kind"] == "image_digest"
    assert load606.image_digest_gate({"items": []}, FD)["why"] == "no live snowplow pod"


def test_the_gate_cli_writes_the_kind_it_is_given():
    body = json.dumps({"items": [_fpod("f", FD_OTHER)]})
    p = subprocess.run(["python3", "-m", "bench.load606", "digest-gate", "--expected", FD,
                        "--kind", "frontend_image_digest", "--what", "frontend"],
                       input=body, cwd=BENCH_DIR, capture_output=True, text=True)
    assert p.returncode == 3 and json.loads(p.stdout)["kind"] == "frontend_image_digest"


def _fgate(result="MATCH", expected=FD, digest=FD):
    return {"kind": "frontend_image_digest", "expected": expected, "result": result,
            "why": None if result == "MATCH" else "digest mismatch: f/frontend: x",
            "pods": [{"pod": "f", "digest": digest, "terminating": False}]}


def test_without_an_override_the_verdict_is_unchanged():
    base = load606.evaluate([], [], [])
    same = load606.evaluate([], [], [], frontend_tag=None, expected_frontend_digest=None, frontend_digests=[])
    assert json.dumps(base, sort_keys=True, default=str) == json.dumps(same, sort_keys=True, default=str)
    assert "frontend_image" not in base["report"]
    assert not any(c["check"].startswith("frontend.") for c in base["validity"])


@pytest.mark.parametrize("gates,ok,why", [
    ([_fgate()], True, None),
    ([], False, "gate did not run"),
    ([_fgate("VOID", digest=FD_OTHER)], False, "digest mismatch"),
    ([_fgate(expected=FD_OTHER)], False, "VOID"),
    # the snowplow gate's record is never read as the frontend's
    ([dict(_fgate(), kind="image_digest")], False, "gate did not run"),
])
def test_with_an_override_the_verdict_is_invalid_unless_the_frontend_digest_held(gates, ok, why):
    r = load606.evaluate([], [], [], frontend_tag=FTAG, expected_frontend_digest=FD, frontend_digests=gates)
    c = next(v for v in r["validity"] if v["check"] == "frontend.image_digest")
    assert (c["result"] == "PASS") is ok, c
    if not ok:
        assert r["verdict"] == "INVALID" and why in c["why"]
    fi = r["report"]["frontend_image"]
    assert fi["override_tag"] == FTAG and fi["expected"] == FD
    if ok:
        assert fi["observed"] == [FD] and fi["result"] == "MATCH"


# ─── the paired verdict (custlat) ───────────────────────────────────────────


def _fe(v, tag=FTAG, digest=FD, result="MATCH"):
    v["report"]["frontend_image"] = {"override_tag": tag, "expected": digest, "observed": [digest],
                                     "result": result, "gate": None}
    return v


def _failed(p):
    return {c["check"] for c in p["validity"] if c["result"] == "FAIL"}


def test_the_pair_without_overrides_is_unchanged(va):  # noqa: F811
    a, b = _ab(va)
    p = custlat.pair(a, b, replica=REPLICA)
    assert "frontend" not in p
    assert not any(c["check"].endswith(".frontends") for c in p["validity"])


def test_the_pair_names_both_frontends_and_compares_only_one(va):  # noqa: F811
    a, b = _ab(va)
    p = custlat.pair(_fe(a), _fe(b), replica=REPLICA)
    assert not _failed(p) and p["verdict"] == "PASS"
    assert p["frontend"]["line"] == (f"INFO frontend: A frontend {FTAG} (override), expected {FD}, observed {FD} "
                                     f"(MATCH); B frontend {FTAG} (override), expected {FD}, observed {FD} (MATCH)")
    for b2, what in ((_fe(_ab(va)[1], tag="1.8.34"), "tag"), (_fe(_ab(va)[1], digest=FD_OTHER), "digest"),
                     (_ab(va)[1], "B without override"), (_fe(_ab(va)[1], result="VOID"), "B's gate VOID")):
        p = custlat.pair(_fe(_ab(va)[0]), b2, replica=REPLICA)
        assert _failed(p) == {"customer_latency.pair.frontends"} and p["verdict"] == "INVALID", what
    assert "the installer's pinned frontend (no override)" in custlat.pair(
        _fe(_ab(va)[0]), _ab(va)[1], replica=REPLICA)["frontend"]["line"]


# ─── the registry preflight (ghcr_pull) ─────────────────────────────────────


def _installer(tmp_path, frontend=True):
    root = tmp_path / "installer"
    (root / "files").mkdir(parents=True)
    (root / "templates").mkdir()
    (root / "Chart.yaml").write_text("apiVersion: v2\nname: installer\nversion: 0.3.545\n")
    (root / "templates" / "_helpers.tpl").write_text('{{- define "inst.coreFeatures" -}}{{- end -}}\n')
    comps = [{"name": "snowplow", "version": "1.12.66", "feature": "portal"}]
    if frontend:   # as 0.3.545 pins it: no repo of its own (the installer's ociRepo)
        comps.append({"name": "frontend", "kind": "Frontend", "chart": "frontend", "version": "1.8.34",
                      "feature": "portal"})
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": comps}))
    (root / "values.yaml").write_text(yaml.safe_dump({"ociRepo": "oci://ghcr.io/krateo-platformops/charts",
                                                      "features": {"portal": True}}))
    v = tmp_path / "v.yaml"
    v.write_text(yaml.safe_dump({"componentValues": {"frontend": {"image": {"tag": FTAG}}}}))
    return v


def _preflight(tmp_path, v, *extra):
    out = tmp_path / "pf.json"
    p = subprocess.run(["python3", "-m", "bench.ghcr_pull", "preflight", "--env-file", os.environ["GHCR_PULL_ENV_FILE"],
                        "--installer-pulled", str(tmp_path), "--values", str(v), "--out", str(out),
                        "--helm", os.environ["FAKE_HELM_TEMPLATE_BIN"], *extra],
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=120)
    return p, (json.loads(out.read_text()) if out.exists() else None)


def test_the_preflight_renders_and_resolves_the_overridden_frontend_image(tmp_path):
    v = _installer(tmp_path)
    p, pf = _preflight(tmp_path, v, "--override", f"frontend={FTAG}")
    assert p.returncode == 0, p.stderr[-2000:]
    fe = next(c for c in pf["charts"] if c["chart"].endswith("/frontend"))
    assert fe["image_override"] == FTAG and fe["error"] is None and fe["render"].startswith("ok")
    img = next(i for i in pf["images"] if i["image"] == f"ghcr.io/krateo-platformops/frontend:{FTAG}")
    assert img["charts"] == ["frontend"] and img["error"] is None
    assert f"[ghcr] image ghcr.io/krateo-platformops/frontend:{FTAG} (frontend): OK" in p.stderr


def test_without_an_override_the_preflight_is_unchanged(tmp_path):
    v = _installer(tmp_path)
    p, pf = _preflight(tmp_path, v)
    assert p.returncode == 0, p.stderr[-2000:]
    assert pf["charts"] == [] and pf["images"] == []        # frontend: no repo of its own, not private
    assert "image_override" not in json.dumps(pf)


def test_an_override_the_render_does_not_carry_is_refused(tmp_path):
    v = _installer(tmp_path)
    p, pf = _preflight(tmp_path, v, "--override", "frontend=1.8.99")   # the values say 1.8.38
    assert p.returncode == 3
    assert "the run overrides its image (tag 1.8.99), yet no ghcr.io image it renders carries that tag" in p.stderr
    assert f"ghcr.io/krateo-platformops/frontend:{FTAG}" in p.stderr


def test_an_override_of_a_component_this_install_does_not_run_is_refused(tmp_path):
    v = _installer(tmp_path, frontend=False)
    p, _ = _preflight(tmp_path, v, "--override", f"frontend={FTAG}")
    assert p.returncode == 2 and "overrides the image of frontend, which this install does not enable" in p.stderr
    for bad in ("frontend", "=1.8.38", "frontend="):
        p, _ = _preflight(tmp_path, v, "--override", bad)
        assert p.returncode == 2 and "COMPONENT=TAG" in p.stderr, bad


# ─── run606.sh: inputs, values, the dry pass ────────────────────────────────


def _dry(tmp_path, **env):
    return _run606(tmp_path, BENCH_606_DRY_RUN="1", FAKE_FRONTEND_PIN="1", **env)


def test_unset_the_installer_values_are_byte_identical_and_nothing_is_added(tmp_path):
    a, b = tmp_path / "unset", tmp_path / "set"
    a.mkdir()
    b.mkdir()
    pu = _dry(a)
    assert pu.returncode == 0, pu.stderr[-3000:]
    ps = _dry(b, FRONTEND_TAG=FTAG, EXPECTED_FRONTEND_DIGEST=FD)
    assert ps.returncode == 0, ps.stderr[-3000:]
    unset = (a / "out" / "installer-values.yaml").read_text()
    with_fe = (b / "out" / "installer-values.yaml").read_text()
    # the override is the frontend block under componentValues, and nothing else
    assert with_fe.count(BLOCK) == 1 and with_fe.replace(BLOCK, "", 1) == unset
    assert sorted(yaml.safe_load(unset)["componentValues"]) == ["snowplow"]
    vals = yaml.safe_load(with_fe)
    assert vals["componentValues"]["frontend"] == {"image": {"tag": FTAG}}
    assert vals["componentValues"]["snowplow"] == yaml.safe_load(unset)["componentValues"]["snowplow"]
    # unset: no frontend gate, no frontend evaluate arguments, no override log
    assert "frontend pods imageID" not in pu.stderr and "--frontend-tag" not in pu.stderr
    assert "frontend image override" not in pu.stderr
    # set: the gate before any load, the evaluate arguments
    assert f"DRY: frontend pods imageID == {FD} (else VOID)" in ps.stderr
    assert (ps.stderr.index(f"DRY: snowplow pods imageID == {SP_DIG}")
            < ps.stderr.index("DRY: frontend pods imageID"))
    assert f"--frontend-tag {FTAG} --expect-frontend-digest {FD}" in ps.stderr
    assert f"frontend image override: componentValues.frontend.image.tag={FTAG}" in ps.stderr


@pytest.mark.parametrize("env,why", [
    ({"FRONTEND_TAG": FTAG}, "FRONTEND_TAG and EXPECTED_FRONTEND_DIGEST are set together or not at all"),
    ({"EXPECTED_FRONTEND_DIGEST": FD}, "FRONTEND_TAG and EXPECTED_FRONTEND_DIGEST are set together or not at all"),
    ({"FRONTEND_TAG": FTAG, "EXPECTED_FRONTEND_DIGEST": "sha256:abc"}, "EXPECTED_FRONTEND_DIGEST must be sha256"),
    ({"FRONTEND_TAG": '1.8.38"\n  x: y', "EXPECTED_FRONTEND_DIGEST": FD}, "FRONTEND_TAG must be an image tag"),
])
def test_run606_refuses_a_half_or_malformed_frontend_override(tmp_path, env, why):
    p = _dry(tmp_path, **env)
    assert p.returncode == 2 and why in p.stderr, p.stderr[-1000:]
    assert not (tmp_path / "out" / "installer-values.yaml").exists()


# ─── run606.sh: the gate before any load (the fake cluster) ─────────────────


def _fenv(env):
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", FAKE_FRONTEND_PIN="1",
               FRONTEND_TAG=FTAG, EXPECTED_FRONTEND_DIGEST=FD)
    return env


def test_run606_a_frontend_on_another_digest_aborts_void_before_any_load(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env = dict(_fenv(env), FAKE_FRONTEND_DIGEST=FD_OTHER)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    assert p.returncode != 0
    assert f"image digest MATCH: every live snowplow pod runs {FAKE_DIGEST}" in p.stderr
    assert f"VOID: a frontend pod does not run {FD}" in p.stderr
    assert "=== step measure ===" not in p.stderr
    applied = [c["argv"] for c in _calls(state) if c["tool"] == "kubectl" and c["argv"][:1] == ["apply"]]
    assert not any("loadgen-job.yaml" in " ".join(a) for a in applied)
    rec = json.loads((tmp / "out" / "frontend-digest.jsonl").read_text())
    assert rec["kind"] == "frontend_image_digest" and rec["result"] == "VOID"
    assert [r["digest"] for r in rec["pods"]] == [FD_OTHER]
    vals = yaml.safe_load((tmp / "out" / "installer-values.yaml").read_text())
    assert vals["componentValues"]["frontend"] == {"image": {"tag": FTAG}}
    pf = json.loads((tmp / "out" / "ghcr-pull-preflight.json").read_text())
    assert any(i["image"] == f"ghcr.io/krateo-platformops/frontend:{FTAG}" for i in pf["images"])


def test_run606_a_matching_frontend_is_recorded_in_the_verdict(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env = dict(_fenv(env), FAKE_FRONTEND_DIGEST=FD)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    assert f"image digest MATCH: every live frontend pod runs {FD}" in p.stderr
    assert p.stderr.index("image digest MATCH: every live frontend pod") < p.stderr.index("=== step measure ===")
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    c = next(x for x in v["validity"] if x["check"] == "frontend.image_digest")
    assert c["result"] == "PASS" and c["value"] == FD
    fi = v["report"]["frontend_image"]
    assert fi["override_tag"] == FTAG and fi["expected"] == FD and fi["observed"] == [FD]


# ─── run606_pair.sh ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("over,why", [
    ({"FRONTEND_TAG_A": FTAG, "EXPECTED_FRONTEND_DIGEST_A": FD,
      "FRONTEND_TAG_B": "1.8.34", "EXPECTED_FRONTEND_DIGEST_B": FD_OTHER}, "FRONTEND_TAG_B (1.8.34) and FRONTEND_TAG_A"),
    ({"FRONTEND_TAG_A": FTAG, "EXPECTED_FRONTEND_DIGEST_A": FD}, "FRONTEND_TAG_B (unset) and FRONTEND_TAG_A (1.8.38)"),
    ({"FRONTEND_TAG_A": FTAG, "EXPECTED_FRONTEND_DIGEST_A": FD,
      "FRONTEND_TAG_B": FTAG, "EXPECTED_FRONTEND_DIGEST_B": FD_OTHER}, "EXPECTED_FRONTEND_DIGEST_B"),
    ({"FRONTEND_TAG_A": FTAG, "FRONTEND_TAG_B": FTAG, "EXPECTED_FRONTEND_DIGEST_B": FD},
     "FRONTEND_TAG_A and EXPECTED_FRONTEND_DIGEST_A are set together or not at all"),
    ({"FRONTEND_TAG": FTAG}, "FRONTEND_TAG is per run"),
    ({"EXPECTED_FRONTEND_DIGEST": FD}, "EXPECTED_FRONTEND_DIGEST is per run"),
])
def test_the_pair_refuses_runs_whose_frontends_differ(pairbed, over, why):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, **over)
    assert p.returncode == 2 and why in p.stderr, p.stderr[-2000:]
    assert not (fake / "calls").exists()          # refused before any run's dry pass


def _echo_frontend(repo):
    """The stand-in run606.sh also records the frontend it was given (one commit, as the launcher needs)."""
    f = repo / "run606.sh"
    f.write_text(f.read_text().replace(' pid=$$" >> "$FAKE/calls"',
                                       ' fe=${FRONTEND_TAG:-none}/${EXPECTED_FRONTEND_DIGEST:-none} pid=$$" >> "$FAKE/calls"', 1))
    for c in (["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "fe"]):
        subprocess.run(["git", *c], cwd=repo, check=True, capture_output=True)


def _verdicts(fake, va, fe):  # noqa: F811
    from tests.test_run15_customer_latency import NA, NB
    a, b = _ab(va)
    if fe:
        _fe(a), _fe(b)
    (fake / f"{NA}.verdict.json").write_text(json.dumps(a))
    (fake / f"{NB}.verdict.json").write_text(json.dumps(b))


def test_the_pair_passes_one_frontend_to_every_run_and_records_it(pairbed, va):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _echo_frontend(repo)
    _verdicts(fake, va, fe=True)
    p = _launch(repo, env, FRONTEND_TAG_A=FTAG, EXPECTED_FRONTEND_DIGEST_A=FD,
                FRONTEND_TAG_B=FTAG, EXPECTED_FRONTEND_DIGEST_B=FD)
    assert p.returncode == 0, p.stderr[-3000:]
    calls = [c for c in _pair_calls(fake) if " dry=" in c]
    assert len(calls) == 4 and all(f" fe={FTAG}/{FD} " in c for c in calls)     # the dry passes and the runs
    pj = json.loads((tmp / "pair" / "pair.json").read_text())
    for x in ("A", "B"):
        assert pj["runs"][x]["frontend_tag"] == FTAG and pj["runs"][x]["frontend_digest"] == FD
    log = (tmp / "pair" / "pair.log").read_text()
    assert f"(1.12.54, frontend {FTAG} {FD})" in log and f"(1.12.55, frontend {FTAG} {FD})" in log
    assert f"A vs B: INFO frontend: A frontend {FTAG} (override)" in log
    v = json.loads((tmp / "pair" / "paired-verdict-A-B.json").read_text())
    assert v["verdict"] == "PASS" and v["frontend"]["a"]["override_tag"] == FTAG


def test_the_pair_without_frontends_is_unchanged(pairbed, va):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _echo_frontend(repo)
    _verdicts(fake, va, fe=False)
    p = _launch(repo, env)
    assert p.returncode == 0, p.stderr[-3000:]
    calls = [c for c in _pair_calls(fake) if " dry=" in c]
    assert len(calls) == 4 and all(" fe=none/none " in c for c in calls)
    pj = json.loads((tmp / "pair" / "pair.json").read_text())
    for x in ("A", "B"):
        assert sorted(pj["runs"][x]) == ["digest", "out", "run_name", "tag"]
    log = (tmp / "pair" / "pair.log").read_text()
    assert "INFO frontend" not in log and ", frontend " not in log
    assert "frontend" not in json.loads((tmp / "pair" / "paired-verdict-A-B.json").read_text())

