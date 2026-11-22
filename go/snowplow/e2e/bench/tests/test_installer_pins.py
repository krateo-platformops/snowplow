"""The installer's component pins have ONE reader (bench/installer_pins.py), which reads the file the
installer chart itself loads.

A real dry pass on installer 0.3.641 was refused at the source step: "files/component-pins.yaml
missing". From 0.3.640 the published chart ships only the generated files/component-pins.min.yaml
(its inst.componentsYaml loads that one); every bench reader opened the old name. The chart fixtures
here are the PUBLISHED archives (0.3.616: the old form; 0.3.641: the new one), and each reader is
driven through them, the source step included.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile

import pytest
import yaml

from bench import crd_schema, ghcr_pull, installer_pins, installer_src, installer_values, lb_need, login_contract
from tests import chart_era
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run22_three_runs import ROOM_FOR_THREE, _launch_env
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

BENCH = BENCH_DIR
CHARTS = BENCH / "tests" / "fixtures" / "installer_charts"
# the published archives' own sha256 (tests/test_1395_private_render.py, tests/test_arm_snowplow_env.py)
SHA256 = {"0.3.616": "614280d447301d7df1a7779e8618eeb079ccb99e7d28ef4bc3e8d466d0c9f285",
          "0.3.641": "d6692a9f3910d29b7b2ea7bf4fafa1d7499bab5686281259ce31fa25ecbc309b"}
PINS_FILE = {"0.3.616": "component-pins.yaml", "0.3.641": "component-pins.min.yaml"}
needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="renders with the real helm")


@pytest.fixture(scope="module")
def pulled(tmp_path_factory):
    """{version: the pull directory, as `helm pull --untar -d <dir>` leaves it (<dir>/installer)}."""
    out = {}
    for v, digest in SHA256.items():
        src = CHARTS / f"installer-{v}.tgz"
        assert hashlib.sha256(src.read_bytes()).hexdigest() == digest
        d = tmp_path_factory.mktemp(f"pulled-{v}")
        with tarfile.open(src) as tf:
            tf.extractall(d, filter="data")
        out[v] = d
    return out


def _values(tmp_path, tag="1.12.70"):
    f = tmp_path / "installer-values.yaml"
    f.write_text(chart_era.for_archives(installer_values.document(tag)))   # #1572: tests/chart_era.py
    return f


# ─── which file: the one the chart itself loads ─────────────────────────────

@pytest.mark.parametrize("version", list(SHA256))
def test_the_pins_are_read_from_the_file_the_published_chart_loads(pulled, version):
    chart = str(pulled[version] / "installer")
    assert installer_pins.shipped(chart) == [PINS_FILE[version]]
    assert installer_pins.loaded_by_templates(chart) == [PINS_FILE[version]]       # its own .Files.Get
    doc, name = installer_pins.load(chart)
    assert name == f"files/{PINS_FILE[version]}"
    comps = {c["name"]: c for c in doc["components"]}
    assert len(comps) == {"0.3.616": 86, "0.3.641": 89}[version]
    assert comps["snowplow"]["version"] == {"0.3.616": "1.12.67", "0.3.641": "1.12.70"}[version]


def test_the_min_file_carries_every_field_the_bench_reads(pulled):
    """The generated data-only copy drops comments, not fields: every key an entry of 0.3.616's
    annotated file has is a key of 0.3.641's entries, the ones the bench reads among them."""
    keys = {v: {k for c in installer_pins.components(str(pulled[v] / "installer")) for k in c} for v in SHA256}
    assert keys["0.3.616"] <= keys["0.3.641"]
    assert {"name", "kind", "chart", "version", "repo", "feature", "alsoFeatures", "deps", "tier", "expose",
            "exposePort", "svcMatch"} <= keys["0.3.641"]


# ─── every reader, through the one function, on both charts ─────────────────

@pytest.mark.parametrize("version", list(SHA256))
def test_the_source_preflight_reads_the_pins(pulled, version):
    res = installer_src.preflight(str(pulled[version] / "installer"), requirements=())
    assert res["ok"] is True and res["failures"] == [] and res["pins_file"] == f"files/{PINS_FILE[version]}"
    # a requirement is checked against those pins
    bad = installer_src.preflight(str(pulled[version] / "installer"), requirements=[("snowplow", "version", "==", "0")])
    assert bad["ok"] is False and "snowplow.version == " in bad["failures"][0]


@pytest.mark.parametrize("version", list(SHA256))
def test_the_crd_sources_read_the_pins(pulled, version):
    srcs = {s["chart"]: s for s in crd_schema.crd_sources(str(pulled[version] / "installer"))}
    assert srcs["snowplow-crds"]["version"] == {"0.3.616": "1.12.65", "0.3.641": "1.12.70"}[version]
    assert "authn-crds" in srcs


@pytest.mark.parametrize("version", list(SHA256))
def test_the_registry_preflights_pins_and_the_login_contracts_frontend_read_the_pins(pulled, tmp_path, version):
    values = str(_values(tmp_path))
    pins = ghcr_pull.private_pins(str(pulled[version]), values, include={"frontend", "snowplow"})
    by = {p["name"]: p for p in pins}
    assert by["snowplow"]["version"] == {"0.3.616": "1.12.67", "0.3.641": "1.12.70"}[version]
    fe = login_contract.frontend_tag(str(pulled[version]), values)
    assert re.fullmatch(r"\d+\.\d+\.\d+", fe["tag"]) and fe["tag"] == str(by["frontend"]["version"])


@pytest.mark.parametrize("version", list(SHA256))
def test_the_load_balancer_plan_reads_the_pins(pulled, tmp_path, version):
    rows = lb_need.planned(str(pulled[version]), str(_values(tmp_path)))
    assert rows and all(r["component"] and r["service"] for r in rows)


@needs_helm
@pytest.mark.parametrize("version", list(SHA256))
def test_the_chart_renders_its_private_image_list_with_real_helm(pulled, tmp_path, version):
    """What the chart renders comes from the same file: helm loads it through the chart's own helper."""
    values = yaml.safe_load(_values(tmp_path).read_text())
    paths = ghcr_pull.private_image_paths(str(pulled[version]), values)
    names = {c["name"] for c in installer_pins.components(str(pulled[version] / "installer"))}
    assert paths and set(paths) <= names


def test_the_cli_of_the_source_step_passes_on_0_3_641(pulled):
    """`python3 -m bench.installer_src preflight`'s own check, as run606's source step reaches it."""
    p = subprocess.run([sys.executable, "-c",
                        "import json, sys; from bench import installer_src; "
                        "r = installer_src.preflight(sys.argv[1], requirements=()); print(json.dumps(r)); "
                        "sys.exit(0 if r['ok'] else 1)", str(pulled["0.3.641"] / "installer")],
                       cwd=BENCH, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stdout + p.stderr
    assert json.loads(p.stdout)["pins_file"] == "files/component-pins.min.yaml"


# ─── refused, naming the files; never chosen by a name's order ──────────────

HELPER = '{{- define "inst.componentsYaml" -}}{{ .Files.Get "files/%s" }}{{- end -}}\n'
PINS = {"components": [{"name": "snowplow", "kind": "Snowplow", "version": "1.0.0"}]}


def _chart(tmp_path, files: dict, loads: tuple = ()):
    c = tmp_path / "installer"
    (c / "templates").mkdir(parents=True)
    (c / "files").mkdir()
    (c / "Chart.yaml").write_text("apiVersion: v2\nname: installer\nversion: 9.9.9\n")
    (c / "templates" / "_helpers.tpl").write_text("".join(HELPER % n for n in loads) or "{{/* no pins */}}\n")
    for name, doc in files.items():
        (c / "files" / name).write_text(doc if isinstance(doc, str) else yaml.safe_dump(doc))
    return str(c)


OTHER = {"components": [{"name": "snowplow", "kind": "Snowplow", "version": "2.0.0"}]}
REFUSED = {
    "the chart ships no pins file": (
        dict(files={"retired-components.yaml": {"retired": []}}, loads=("component-pins.min.yaml",)),
        "templates load files/component-pins.min.yaml, which the chart does not ship; it ships no pins file"),
    "no template names one and none is shipped": (
        dict(files={}), "it ships no pins file in", ),
    "the templates name the old file, the chart ships only the new one": (
        dict(files={"component-pins.min.yaml": PINS}, loads=("component-pins.yaml",)),
        "templates load files/component-pins.yaml, which the chart does not ship; it ships component-pins.min.yaml"),
    "two files and no template says which": (
        dict(files={"component-pins.yaml": PINS, "component-pins.min.yaml": PINS}),
        "it ships component-pins.min.yaml, component-pins.yaml in"),
    "the templates load two pins files": (
        dict(files={"component-pins.yaml": PINS, "component-pins.min.yaml": PINS},
             loads=("component-pins.yaml", "component-pins.min.yaml")),
        "templates load more than one pins file (component-pins.min.yaml, component-pins.yaml)"),
    "both shipped, and they disagree": (
        dict(files={"component-pins.yaml": OTHER, "component-pins.min.yaml": PINS}, loads=("component-pins.min.yaml",)),
        "loads files/component-pins.min.yaml and also ships component-pins.yaml, whose components are not the same"),
    "the file is not a pins document": (
        dict(files={"component-pins.min.yaml": "just: text\n"}, loads=("component-pins.min.yaml",)),
        "files/component-pins.min.yaml is not a map with a `components` list"),
}


@pytest.mark.parametrize("case", list(REFUSED))
def test_a_chart_whose_pins_file_is_not_decided_is_refused_by_name(tmp_path, case):
    kw, why = REFUSED[case]
    chart = _chart(tmp_path, **kw)
    with pytest.raises(installer_pins.Refused) as e:
        installer_pins.load(chart)
    assert why in str(e.value), str(e.value)
    # and the source preflight says the same, as a failure of the chart
    res = installer_src.preflight(chart, requirements=())
    assert res["ok"] is False and any(why in f for f in res["failures"]) and res["pins_file"] is None


def test_both_files_shipped_and_equal_reads_the_one_the_chart_loads(tmp_path):
    chart = _chart(tmp_path, files={"component-pins.yaml": PINS, "component-pins.min.yaml": PINS},
                   loads=("component-pins.min.yaml",))
    assert installer_pins.load(chart) == (PINS, "files/component-pins.min.yaml")


def test_one_file_and_no_template_naming_it_is_that_file(tmp_path):
    assert installer_pins.load(_chart(tmp_path, files={"component-pins.yaml": PINS}))[1] == "files/component-pins.yaml"


def test_no_bench_module_opens_a_pins_file_by_its_own_name():
    """Every reader goes through bench/installer_pins.py: no other module joins a path to a pins file."""
    opened = re.compile(r"""["']component-pins[^"']*\.ya?ml["']""")
    hits = {p.name: opened.findall(p.read_text()) for p in (BENCH / "bench").glob("*.py")
            if p.name != "installer_pins.py" and opened.search(p.read_text())}
    assert hits == {}, hits
    for mod in ("installer_src.py", "crd_schema.py", "ghcr_pull.py", "lb_need.py"):
        assert "installer_pins." in (BENCH / "bench" / mod).read_text(), mod


# ─── the real run: run606's source step on a chart of 0.3.640's form ────────

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


@needs_helm
@pytest.mark.parametrize("form", ["min", "annotated"])
def test_run606s_dry_pass_goes_through_every_step_on_either_form_of_the_chart(harness, form):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", **({"FAKE_INSTALLER_PINS": "min"} if form == "min" else {}))
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    pulled_files = sorted(os.listdir(tmp / "out" / "installer-pulled" / "installer" / "files"))
    assert pulled_files == (["component-pins.min.yaml"] if form == "min" else ["component-pins.yaml"])
    assert "installer registry preflight REFUSED" not in log and "(registry preflight OK" in log
    steps = re.findall(r"=== step (\S+) ===", log)
    assert steps[:10] == ["apis", "source", "login-contract", "cert-manager-source", "schema", "registry",
                          "snowplow-env", "portal-routes", "reap", "up"] and steps[-1] == "evaluate"


# ─── from the review of the per-arm env change ──────────────────────────────

SPEC = {"N1": ("snowplow-bench-pa", "sha256:" + "ab" * 32), "N2": ("snowplow-bench-pb", "sha256:" + "cd" * 32)}


def test_a_launch_refuses_the_rehearsal_mode(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    e = _launch_env(env, tmp, SPEC, "N1:N2", BENCH_606_DRY_RUN="1", BENCH_606_REHEARSAL="env", **ROOM_FOR_THREE)
    p = _launch(repo, e)
    assert p.returncode == 2, p.stderr[-1500:]
    assert "refusing: BENCH_606_REHEARSAL is set (env): a launch never carries it" in p.stderr
    assert not (fake / "calls").exists()                                       # not even a dry pass
    # and the VM launcher's env file check refuses it as it refuses every test hook
    from bench import vm606
    assert vm606.TEST_HOOKS.match("BENCH_606_REHEARSAL")


def _budget(env, **extra):
    p = _run606(env, BENCH_606_DRY_RUN="1", **extra)
    m = re.search(r"run budget: (.*) = (\d+) s; fits TTL_HOURS=(\d+) ", p.stderr)
    assert p.returncode == 0 and m, p.stderr[-1500:]
    return m.group(1), int(m.group(2))


def test_a_run_of_a_launch_waits_for_no_other_run_and_its_plan_has_no_such_term(harness):  # noqa: F811
    """The owner's ruling (2026-11-16): every arm's load starts right after its pod's Ready; the arms' readings
    are compared after the measure (custlat pair and replica), so a run of a launch plans no wait for the
    others: the same total as a run alone."""
    env, state, _, tmp = harness
    alone_line, alone = _budget(env)
    line, total = _budget(env, BENCH_ARM="N1")
    assert "env agreement" not in line and total == alone
    for text, tot in ((alone_line, alone), (line, total)):
        flat = " ".join(re.sub(r"\([^()]*\)", "", text).split())               # a term's breakdown is not a term
        terms = [int(n) * int(k) if n else int(k) for n, k in re.findall(r"(?:(\d+)x)?(\d+)(?= \+|$)", flat)]
        assert len(terms) == flat.count(" + ") + 1, flat                       # every term was read
        assert sum(terms) == tot, (flat, terms, tot)
