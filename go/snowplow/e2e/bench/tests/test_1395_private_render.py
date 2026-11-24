"""#1395: the installer's private-image component list (inst.privateImageComponents) is RENDERED
from the installer chart the run installs (bench.ghcr_pull.private_image_paths), never read by a
regex over the template source; and the laptop renders it (bench.vm606.chart_check) before any
paid step.

Run 26f (installer 0.3.616) was refused at N1's dry pass, ON THE VM: "inst.privateImageComponents
is not one static list: its rule cannot be read". The define was still one static list, but
installer #829 (58b233f, "pin kagent 0.1.22") had put a `{{/* ... */}}` comment inside it.

The charts here are the REAL published installer archives (helm pull
oci://ghcr.io/krateo-platformops/charts/installer; tests/installer_archives.py), rendered by the real
helm, as the suite renders every chart. The expected lists are written out from the installer's
own source at each tag (chart/templates/_helpers.tpl), not taken from the code under test. Every render
with a run's values is on the archive of the installer krateo-057's record was read on (#1572: an older
installer cannot express the record's features and is refused before any render; 0.3.545 and 0.3.616
stay for the history they hold, read from their source).
No cloud call: the laptop-side checks run against the fake gcloud of tests/fake_vm606.py, and the
chart pulls against a fake `helm pull` that serves these archives.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import shlex
import shutil
import tarfile
from pathlib import Path

import pytest
import yaml

from bench import ghcr_pull, gke, installer_values, rbac_model, vm606
from tests import fake_vm606
from tests import installer_archives as ia
from tests.test_gke_pvc_disks import fakes as pvc_fakes  # noqa: F401
from tests.test_run606_real_sequence import harness  # noqa: F401
from tests.test_vm606 import ENV, RUN, _run  # noqa: F401
from tests.test_vm606 import fake as vmfake  # noqa: F401

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="renders with the real helm")

BENCH_DIR = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"
CHARTS = ia.CHARTS
#: the archive a run's values render against: the installer krateo-057's record was read on
RECORDED = ia.RECORDED
REAL_CHART_CHECK = vm606.chart_check       # tests/test_vm606.py's fixture records it instead

# installer 0.3.545's chart/templates/_helpers.tpl, inst.privateImageComponents: 7 entries
AT_0_3_545 = {
    "repo-mcp-server": [["imagePullSecrets"]],
    "structure-graph-mcp-server": [["imagePullSecrets"]],
    "config-refs-mcp-server": [["imagePullSecrets"]],
    "core-provider-agent": [["mcpServers", "chartGate", "imagePullSecrets"]],
    "agentgateway-policies": [["certReplayHop", "image", "pullSecrets"], ["uiTokenHop", "image", "pullSecrets"]],
    "alert-provider": [["imagePullSecrets"]],
}
# installer 0.3.616's: the same, plus kagent's per-user tool server (installer #829), behind a comment
AT_0_3_616 = {**AT_0_3_545, "kagent": [["perUserRbac", "toolImagePullSecrets"]]}
# installer 0.3.698's: the same, plus zendesk-kog's wrapper plugin (behind a comment too); 0.3.700's and 0.3.701's (the record's): the same
AT_0_3_698 = {**AT_0_3_616, "zendesk-kog": [["plugin", "imagePullSecrets"]]}
AT_0_3_703 = dict(AT_0_3_698)
AT_RECORDED = {"0.3.703": AT_0_3_703}[RECORDED]


def _values(snowplow_tag="1.12.69", frontend_tag="1.8.41", **over) -> dict:
    """A run's installer values, as run606.sh writes them, with the username document s_install pipes."""
    v = ghcr_pull._load_yaml(installer_values.document(snowplow_tag, frontend_tag)) or {}
    return ghcr_pull._merge(v, {"registryAuth": {"username": "bench-user"}, **over})


def _pulled(tmp_path, version: str) -> Path:
    """The archive, verified, unpacked as `helm pull --untar -d <dir>` leaves it: <dir>/installer."""
    return ia.pull(tmp_path / f"pulled-{version}", version)


def _helpers(pulled: Path) -> Path:
    return pulled / "installer" / "templates" / "_helpers.tpl"


def _define(text: str) -> str:
    m = re.search(r'\{\{- define "inst\.privateImageComponents" -\}\}.*?\n\{\{- end -\}\}', text, re.S)
    assert m, "the fixture's define is where the installer's source has it"
    return m.group(0)


def _with_define(pulled: Path, body: str) -> None:
    """The define's whole body replaced by `body` (a test of what the render does with other forms)."""
    t = _helpers(pulled).read_text()
    d = _define(t)
    _helpers(pulled).write_text(t.replace(d, '{{- define "inst.privateImageComponents" -}}\n' + body
                                          + "\n{{- end -}}"))


# ─── the list, rendered from the real charts ───────────────────────────────

def test_the_installers_list_is_read_from_its_rendered_chart(tmp_path):
    pulled = _pulled(tmp_path, RECORDED)
    assert ghcr_pull.private_image_paths(str(pulled), _values()) == AT_RECORDED


def test_the_recorded_installers_list_is_its_source_and_the_comment_form_too():
    """Its define, like 0.3.616's (run 26f's form), carries template comments inside the list; the render reads
    it all the same (above)."""
    with tarfile.open(ia.archive(RECORDED)) as tf:
        body = _define(tf.extractfile("installer/templates/_helpers.tpl").read().decode())
    assert "{{/*" in body.split("\n", 1)[1]
    assert sorted(ln[len("- name: "):] for ln in body.splitlines() if ln.startswith("- name: ")) == sorted(
        n for n, paths in AT_RECORDED.items() for _ in paths)


def test_0_3_616_is_the_form_run_26f_was_refused_on():
    """The fixture is the run 26f form: a template comment inside the define, which a regex over the
    source read as an unknown rule ("{{" in the body)."""
    with tarfile.open(ia.archive("0.3.616")) as tf:
        text = tf.extractfile("installer/templates/_helpers.tpl").read().decode()
    body = _define(text)
    assert "{{/*" in body.split("\n", 1)[1] and "kagent" in body
    with tarfile.open(ia.archive("0.3.545")) as tf:
        old = _define(tf.extractfile("installer/templates/_helpers.tpl").read().decode())
    assert "{{/*" not in old.split("\n", 1)[1]


def test_the_entries_the_history_added_are_in_the_render():
    """0.3.616 added kagent's per-user tool server (installer #829), 0.3.698 zendesk-kog's plugin: both are in
    the recorded installer's render, nothing taught about either (the render above)."""
    assert set(AT_0_3_616) - set(AT_0_3_545) == {"kagent"} and set(AT_0_3_698) - set(AT_0_3_616) == {"zendesk-kog"}
    assert {k: AT_RECORDED[k] for k in AT_0_3_545} == AT_0_3_545


def test_a_next_entry_is_read_by_construction(tmp_path):
    """The next installer tag adds a component: an entry appended to the list is in the render, with nothing
    taught about it."""
    pulled = _pulled(tmp_path, RECORDED)
    t = _helpers(pulled).read_text()
    d = _define(t)
    _helpers(pulled).write_text(t.replace(d, d.replace(
        "\n{{- end -}}", "\n- name: next-component\n  path:\n  - plugin\n  - imagePullSecrets\n{{- end -}}")))
    got = ghcr_pull.private_image_paths(str(pulled), _values())
    assert got == {**AT_RECORDED, "next-component": [["plugin", "imagePullSecrets"]]}
    assert sum(map(len, got.values())) == sum(map(len, AT_RECORDED.values())) + 1


def test_the_list_is_rendered_with_the_runs_values(tmp_path):
    """A define that is a rule over the values renders what the run's values make of it."""
    pulled = _pulled(tmp_path, RECORDED)
    t = _helpers(pulled).read_text()
    d = _define(t)
    _helpers(pulled).write_text(t.replace(d, d.replace(
        "\n{{- end -}}", "\n{{- if .Values.features.agentGateway }}\n- name: gated\n  path: [x, pullSecrets]\n"
                         "{{- end }}\n{{- end -}}")))
    on = ghcr_pull.private_image_paths(str(pulled), _values())
    off = ghcr_pull.private_image_paths(str(pulled), _values(features={"agentGateway": False}))
    assert on == {**AT_RECORDED, "gated": [["x", "pullSecrets"]]} and off == AT_RECORDED


def test_the_chart_s_install_installs_is_never_written(tmp_path):
    pulled = _pulled(tmp_path, RECORDED)

    def tree():
        return {str(p.relative_to(pulled)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(pulled.rglob("*")) if p.is_file()}
    before = tree()
    ghcr_pull.private_image_paths(str(pulled), _values())
    assert tree() == before


def test_the_render_is_the_install_release_namespace_and_cluster_version(tmp_path):
    seen = tmp_path / "argv"
    helm = tmp_path / "helm"
    helm.write_text(f"#!/bin/sh\necho \"$@\" > {seen}\nexec {shlex.quote(shutil.which('helm'))} \"$@\"\n")
    helm.chmod(0o755)
    ghcr_pull.private_image_paths(str(_pulled(tmp_path, RECORDED)), _values(), str(helm))
    argv = seen.read_text().split()
    assert argv[:2] == ["template", "installer"] and argv[argv.index("--namespace") + 1] == "krateo-system"
    assert argv[argv.index("--kube-version") + 1] == gke.DEFAULT_CLUSTER_VERSION
    assert re.fullmatch(r"templates/zz-bench-probe-[0-9a-f]{8}\.yaml", argv[argv.index("--show-only") + 1])


# ─── a render that cannot answer refuses, with the reason ──────────────────

@pytest.mark.parametrize("body,why", [
    (None, 'helm template failed: Error: '),                  # the define gone (renamed): its cause below
    ("components: [", "the define does not render as YAML: "),
    ("components: []", "the define renders no `components` list, or an empty one"),
    ("other: []", "the define renders no `components` list, or an empty one"),
    ("- a\n- b", "the define does not render as YAML: "),      # fromYaml of a list is no map
    ("components:\n- name: x", 'its entry 0 is not a name and a non-empty path of keys: {"name": "x"}'),
    ("components:\n- name: x\n  path: []", "its entry 0 is not a name and a non-empty path of keys"),
    ("components:\n- path: [a]", "its entry 0 is not a name and a non-empty path of keys"),
    ("components:\n- name: x\n  path: [a, '']", "its entry 0 is not a name and a non-empty path of keys"),
    ('{{ fail "the installer refuses" }}', "helm template failed: Error: execution error at "),
])
def test_a_list_that_cannot_be_read_refuses(tmp_path, body, why):
    pulled = _pulled(tmp_path, RECORDED)
    if body is None:
        t = _helpers(pulled).read_text()
        _helpers(pulled).write_text(t.replace('define "inst.privateImageComponents"', 'define "inst.renamedList"'))
    else:
        _with_define(pulled, body)
    with pytest.raises(ghcr_pull._Refused) as e:
        ghcr_pull.private_image_paths(str(pulled), _values())
    assert str(e.value).startswith("the installer's private-image components (inst.privateImageComponents) cannot "
                                   "be read from its rendered chart: ") and why in str(e.value), str(e.value)
    if body is None:
        assert 'no template "inst.privateImageComponents"' in str(e.value)


def test_no_helm_and_no_chart_refuse_too(tmp_path):
    pulled = _pulled(tmp_path, RECORDED)
    with pytest.raises(ghcr_pull._Refused, match="helm template could not run"):
        ghcr_pull.private_image_paths(str(pulled), _values(), str(tmp_path / "no-helm"))
    with pytest.raises(ghcr_pull._Refused, match="no pulled installer chart at "):
        ghcr_pull.private_image_paths(str(tmp_path / "nothing"), _values())


def test_a_render_with_no_answer_refuses(tmp_path, monkeypatch):
    monkeypatch.setattr(ghcr_pull, "RENDER_BOUND_S", 1.0)
    helm = tmp_path / "helm"
    helm.write_text("#!/bin/sh\nexec sleep 30\n")
    helm.chmod(0o755)
    with pytest.raises(ghcr_pull._Refused, match="helm template did not finish within its 1 s bound"):
        ghcr_pull.private_image_paths(str(_pulled(tmp_path, RECORDED)), _values(), str(helm))


def _preflight_argv(tmp_path, pulled: Path) -> list[str]:
    v = tmp_path / "installer-values.yaml"
    v.write_text(installer_values.document("1.12.69", "1.8.41"))
    env = tmp_path / "ghcr-pull.env"
    env.write_text("GHCR_PULL_USERNAME=bench-user\nGHCR_PULL_TOKEN=opaque-never-printed\n")
    env.chmod(0o600)
    return ["preflight", "--env-file", str(env), "--installer-pulled", str(pulled), "--values", str(v),
            "--out", str(tmp_path / "pf.json")]


def test_the_preflight_refuses_before_any_registry_read(tmp_path, monkeypatch, capsys):
    pulled = _pulled(tmp_path, RECORDED)
    _with_define(pulled, "components: []")

    def no_read(*a, **k):
        raise AssertionError("a registry read before the installer's list was read")
    monkeypatch.setattr(ghcr_pull, "_OPEN", no_read)
    rc = ghcr_pull.main(_preflight_argv(tmp_path, pulled))
    err = capsys.readouterr().err
    assert rc == 2 and err.startswith("refusing: the installer's private-image components "
                                      "(inst.privateImageComponents) cannot be read from its rendered chart: "), err
    assert not (tmp_path / "pf.json").exists()


def test_the_preflight_renders_with_the_username_and_never_says_it(tmp_path, capsys):
    """The run's values include the username s_install pipes; a reason that would carry it says
    it as <registry user>, never the token."""
    pulled = _pulled(tmp_path, RECORDED)
    helm = tmp_path / "helm"
    helm.write_text("#!/bin/sh\necho \"Error: values: $(grep username)\" >&2\nexit 1\n")
    helm.chmod(0o755)
    rc = ghcr_pull.main(_preflight_argv(tmp_path, pulled) + ["--helm", str(helm)])
    err = capsys.readouterr().err
    assert rc == 2 and "<registry user>" in err and "bench-user" not in err and "opaque" not in err, err


# ─── one source for the run's values ───────────────────────────────────────

@pytest.mark.parametrize("snowplow,frontend,golden", [
    ("1.12.69", "", "installer_values_heredoc_1.12.69.yaml"),
    ("1.12.70", "1.8.41", "installer_values_heredoc_1.12.70_frontend_1.8.41.yaml")])
def test_the_values_are_the_heredocs_byte_for_byte(tmp_path, snowplow, frontend, golden):
    """The goldens are what run606.sh's heredoc wrote (before #1395) for these tags; the document is
    that, byte for byte, but for two blocks:
      - features: krateo-057's (bench/installer_features.py, #1572) where the heredoc's three
        hand-picked lines were;
      - componentValues: the heredoc's own entries (snowplow's image, node and tolerations; the
        frontend's image), plus the portal's reader group (run 26j) and the internal CA bundle's target
        namespace (the namespace the bench installs krateo into), under krateo-057's recorded switches
        and sizes (bench/installer_component_values.py, #1572), written as one YAML map after a comment."""
    from bench import installer_component_values as icv
    from bench import installer_features
    want = (FIXTURES / golden).read_text()
    got = installer_values.document(snowplow, frontend)
    k, m = got.index("# The installer's features are krateo-057's"), got.index("vertexAI:")
    c, j = got.index("# componentValues: krateo-057's recorded switches and sizes"), got.index("registryAuth:")
    heredoc = "features:\n  agentGateway: true\n  coreAgents: false\n  specialistAgents: false\n"
    # the features the values write: 057's, and internalTls's own value with them (bench/installer_values.py)
    feats = installer_features.bench_features()
    assert got[k:m].split("\n", 2)[2] == installer_features.values_block(feats) + installer_values.internal_tls_block(feats)
    wc, wj = want.index("componentValues:\n"), want.index("registryAuth:")
    # every byte outside the two blocks is the heredoc's
    assert got[:k] + heredoc + got[m:c] + want[wc:wj] + got[j:] == want
    # the componentValues block: the heredoc's entries and the reader group, over 057's record
    block = got[c:j]
    assert all(ln.startswith("#") for ln in block[:block.index("componentValues:\n")].splitlines())
    own = {**yaml.safe_load(want)["componentValues"], "portal": {"readerGroups": [rbac_model.BASE_GROUP]}}
    if icv.receiver_on_snowplow_node():
        # #1575: the bench's own topology, where the installer can express it: the node-local OTLP agent
        # tolerates the snowplow pool, with the toleration the heredoc gave snowplow
        own["otel-collector-daemonset"] = {"opentelemetry-collector": {"tolerations": own["snowplow"]["tolerations"]}}
    # #1588 follow-up: features.internalTls's CA bundle targets the namespace the bench installs krateo into
    own["internal-ca-bundle"] = {"target": {"namespaces": [installer_values.KRATEO_NAMESPACE]}}
    assert yaml.safe_load(block)["componentValues"] == icv.merge(icv.nest(icv.bench_paths()), own)
    out = tmp_path / "v.yaml"
    assert installer_values.main(["--snowplow-tag", snowplow, "--frontend-tag", frontend, "--out", str(out)]) == 0
    assert out.read_text() == got


def test_run606_writes_its_values_through_the_one_source():
    src = (BENCH_DIR / "run606.sh").read_text()
    body = src[src.index("write_installer_values() {"):]
    body = body[:body.index("\n}\n")]
    assert "python3 -m bench.installer_values --snowplow-tag \"$SNOWPLOW_TAG\" --frontend-tag \"$FRONTEND_TAG\"" in body
    assert "<<" not in body and "componentValues" not in body


def test_vm606s_installer_chart_is_run606s_default():
    src = (BENCH_DIR / "run606.sh").read_text()
    assert f'INSTALLER_CHART="${{INSTALLER_CHART:-{vm606.INSTALLER_CHART}}}"' in src


# ─── the laptop: before any paid step ──────────────────────────────────────

FAKE_PULL_HELM = """#!/usr/bin/env python3
import os, sys, tarfile
REAL = {real!r}
a = sys.argv[1:]
ver = a[a.index("--version") + 1] if "--version" in a else ""
if a[:1] == ["pull"] and a[1:2] == ["oci://quay.io/jetstack/charts/cert-manager"]:
    # the bench's pinned cert-manager chart, the REAL archive (bench/cert_manager.py verifies its sha256)
    import shutil
    shutil.copy(os.environ["FAKE_CERT_MANAGER_TGZ"], os.path.join(a[a.index("-d") + 1], "cert-manager-" + ver + ".tgz"))
    sys.exit(0)
src = os.path.join(os.environ["FAKE_INSTALLER_CHARTS"], "installer-" + ver + ".tgz")
if a[:2] == ["show", "chart"] or a[:1] == ["pull"]:
    if not os.path.isfile(src):
        print("Error: installer:" + ver + ": not found", file=sys.stderr)
        sys.exit(1)
    if a[0] == "show":
        print("apiVersion: v2" + chr(10) + "name: installer" + chr(10) + "version: " + ver)
        sys.exit(0)
    with tarfile.open(src) as tf:
        tf.extractall(a[a.index("-d") + 1], filter="data")
    sys.exit(0)
os.execv(REAL, [REAL, *a])
"""


@pytest.fixture
def laptop_helm(tmp_path, monkeypatch):
    """`helm` on PATH: pull and show serve the archives in the returned directory (the real ones, and
    any a test adds), template is the real helm. Returns that directory."""
    d = tmp_path / "registry"
    d.mkdir()
    for v in ia.SHA256:
        shutil.copy(ia.archive(v), d)
    b = tmp_path / "laptop-helm"
    b.mkdir()
    (b / "helm").write_text(FAKE_PULL_HELM.format(real=shutil.which("helm")))
    (b / "helm").chmod(0o755)
    monkeypatch.setenv("FAKE_INSTALLER_CHARTS", str(d))
    monkeypatch.setenv("FAKE_CERT_MANAGER_TGZ", str(Path(__file__).resolve().parent / "fixtures" / "cert-manager-v1.21.1.tgz"))
    monkeypatch.setenv("PATH", f"{b}{os.pathsep}{os.environ['PATH']}")
    return d


def _broken(tmp_path, registry: Path, version: str, body: str) -> None:
    """installer-<version>.tgz in the fake registry: the recorded installer with the define's body replaced."""
    pulled = _pulled(tmp_path / version, RECORDED)
    _with_define(pulled, body)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(pulled / "installer", arcname="installer")
    (registry / f"installer-{version}.tgz").write_bytes(buf.getvalue())


def _env(**over) -> dict:
    """A launch env on the installer 057's record was read on: vm606's chart check refuses any other kit
    version before it pulls anything (#1572, tests/test_installer_features.py)."""
    return {**ENV, "INSTALLER_VERSION": RECORDED, "FRONTEND_TAG_N1": "1.8.41", "FRONTEND_TAG_N2": "1.8.41", **over}


WANT_RECORDED = (f"private-image components (rendered on the laptop, {sum(map(len, AT_RECORDED.values()))} entries): "
                 + ", ".join(sorted(AT_RECORDED)))


def _chart_lines(lines: list[str]) -> list[str]:
    """chart_check's private-list lines (its first lines are the kit version's and the features' own)."""
    return [ln for ln in lines if "private-image components" in ln]


def test_the_laptop_renders_every_runs_list(laptop_helm):
    lines = REAL_CHART_CHECK(_env(), ["N1", "N2"])
    assert _chart_lines(lines) == [f"installer {RECORDED}, run N1's values: {WANT_RECORDED}",
                                   f"installer {RECORDED}, run N2's values: {WANT_RECORDED}"]
    # #1588 follow-up: every run's effective spec rendered by the installer's composition template, every enabled
    # component, every guard passed; and the internalTls of the CR it was rendered with
    rendered = [ln for ln in lines if "installer render: rendered " in ln]
    assert [ln.split(":", 1)[0] for ln in rendered] == ["run N1", "run N2"], lines
    for ln in rendered:
        n = re.search(r"rendered (\d+) of (\d+) enabled components", ln)
        assert n and n.group(1) == n.group(2) and int(n.group(1)) > 0, ln
        assert "trust-manager" in ln and "internal-ca-bundle" in ln and "cert-manager-issuers" in ln, ln
    assert sum('installer internalTls in that CR: {"certManagerNamespace": "cert-manager"}; cert-manager\'s '
               "--cluster-resource-namespace: 'cert-manager'" in ln for ln in lines) == 2, lines


def test_falsifier_the_laptop_refuses_a_run_whose_values_leave_the_ca_bundle_target_empty(laptop_helm, monkeypatch):
    """#1588 follow-up: installer 0.3.703 refuses its whole composition render with features.internalTls on and
    componentValues.internal-ca-bundle.target.namespaces empty; the laptop refuses the launch first."""
    real = installer_values.document

    def without(*a, **k):
        d = yaml.safe_load(real(*a, **k))
        d["componentValues"].pop("internal-ca-bundle")
        return yaml.safe_dump(d)
    monkeypatch.setattr(installer_values, "document", without)
    with pytest.raises(vm606.Refused) as e:
        REAL_CHART_CHECK(_env(), ["N1", "N2"])
    assert str(e.value).startswith(f"installer {RECORDED}, run N1's values: the installer refuses to render its "
                                   "components with the run's effective spec"), str(e.value)
    assert ("compositions.yaml:1005:4): features.internalTls is on but componentValues.internal-ca-bundle.target"
            ".namespaces is empty") in str(e.value), str(e.value)


@pytest.mark.parametrize("body,why", [
    ("components: []", "the define renders no `components` list, or an empty one"),
    ("{{ fail \"no\" }}", "helm template failed: Error: execution error at "),
])
def test_the_laptop_refuses_a_list_it_cannot_read(tmp_path, laptop_helm, body, why):
    _broken(tmp_path, laptop_helm, RECORDED, body)
    with pytest.raises(vm606.Refused) as e:
        REAL_CHART_CHECK(_env(), ["N1", "N2"])
    assert str(e.value).startswith(f"installer {RECORDED}, run N1's values: the installer's private-image components "
                                   "(inst.privateImageComponents) cannot be read from its rendered chart: ")
    assert why in str(e.value), str(e.value)


def test_the_laptop_refuses_a_chart_it_cannot_pull(laptop_helm):
    (laptop_helm / f"installer-{RECORDED}.tgz").unlink()
    with pytest.raises(vm606.Refused, match=r"the installer chart oci://ghcr\.io/krateo-platformops/charts/installer "
                                            rf"{re.escape(RECORDED)} could not be pulled on the laptop: .*not found"):
        REAL_CHART_CHECK(_env(), ["N1", "N2"])


def _env_file(tmp_path, env: dict) -> str:
    p = tmp_path / "run.env"
    p.write_text("".join(f"{k}={shlex.quote(v)}\n" for k, v in env.items()))
    return str(p)


def test_up_refuses_before_any_gcp_call_when_the_list_cannot_be_read(vmfake, tmp_path, laptop_helm, capsys,
                                                                     monkeypatch):
    """Run 26f, again: the mismatch refuses on the laptop, and no VM, grant, rule or bucket exists."""
    monkeypatch.setattr(vm606, "chart_check", REAL_CHART_CHECK)
    _broken(tmp_path, laptop_helm, RECORDED, "components: []")
    for yes in ([], ["--yes"]):
        rc, out, err = _run(capsys, "up", "--env-file", _env_file(tmp_path, _env()),
                            "--run", RUN, "--ttl-hours", "15", "--poll-s", "0", *yes)
        assert rc == 2 and err.startswith(f"refusing: installer {RECORDED}, run N1's values: the installer's "
                                          "private-image components (inst.privateImageComponents) cannot be read"), err
        assert fake_vm606.calls(vmfake) == []
        assert not (vm606.state_dir(RUN) / "state.json").exists()


def test_up_checks_the_chart_first_then_goes_on(vmfake, tmp_path, laptop_helm, capsys, monkeypatch):
    monkeypatch.setattr(vm606, "chart_check", REAL_CHART_CHECK)
    rc, out, err = _run(capsys, "up", "--env-file", _env_file(tmp_path, _env()), "--run", RUN, "--ttl-hours", "15")
    assert rc == 0, err
    assert out.index(f"run N2's values: {WANT_RECORDED}") < out.index("DRY")
    assert fake_vm606.calls(vmfake)


def test_up_needs_the_env_file(vmfake, capsys):
    with pytest.raises(SystemExit):
        vm606.main(["up", "--run", RUN, "--ttl-hours", "15"])
    assert "--env-file" in capsys.readouterr().err


def test_launch_refuses_before_any_ssh_when_the_list_cannot_be_read(vmfake, tmp_path, laptop_helm, capsys,
                                                                    monkeypatch):
    monkeypatch.setattr(vm606, "chart_check", REAL_CHART_CHECK)
    _broken(tmp_path, laptop_helm, RECORDED, "components: []")
    pin = tmp_path / "pin.json"
    pin.write_text("{}")
    rc, out, err = _run(capsys, "launch", "--run", RUN, "--bench-sha", "a" * 40, "--bench-zone", "europe-west1-b",
                        "--env-file", _env_file(tmp_path, _env()), "--pin-manifest", str(pin),
                        "--pin-sha256", hashlib.sha256(b"{}").hexdigest(), "--yes")
    assert rc == 2 and "cannot be read from its rendered chart" in err, err
    assert fake_vm606.calls(vmfake) == []


# ─── #1396's two mirrors, on this chart with run 26g's values (the #1395 review) ───

def _render(chart: Path, values: dict, *args, extra: dict | None = None) -> list:
    data = ghcr_pull._chart_with(str(chart), extra or {})
    return ghcr_pull.render(data, "installer", values, args=("--kube-version", gke.DEFAULT_CLUSTER_VERSION, *args))


def test_the_core_features_reader_reads_the_recorded_installer_as_the_chart_renders_it(tmp_path):
    """diag606.core_features (still a reader of the source, #1396) against the chart's own render."""
    from bench import diag606
    pulled = _pulled(tmp_path, RECORDED)
    probe = ('apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: p\ndata:\n'
             '  core: {{ include "inst.coreFeatures" . | quote }}\n')
    docs = _render(pulled / "installer", _values(), "--show-only", "templates/zz-core.yaml",
                   extra={"templates/zz-core.yaml": probe})
    rendered = tuple(x for x in docs[0]["data"]["core"].split(" ") if x)
    assert diag606.core_features(str(pulled)) == rendered == ()


@pytest.mark.parametrize("snowplow", ["1.12.69", "1.12.70"])   # run 26g: N1 and N1B, N2
def test_the_bench_enable_rule_is_the_installers(tmp_path, snowplow):
    """The installer's enable rule (inst.compEnabled) reads `feature` OR `alsoFeatures`; until #1572 the
    bench's rule (ghcr_pull.private_pins, lb_need) read `feature` alone (#1396). On the recorded installer
    with a run's values (057's features) the difference is real: internalTls turns cert-manager and its
    issuers on through `alsoFeatures` while `ingress` is off. The components the installer registers
    (definitions.yaml in composition mode, as self-bootstrap.yaml hands it the run's values) are exactly
    the bench rule's, and a rule without `alsoFeatures` would miss those two."""
    import yaml
    from bench import diag606
    pulled = _pulled(tmp_path, RECORDED)
    root = pulled / "installer"
    run = _values(snowplow)
    assert run["features"]["internalTls"] is True             # else the two components below prove nothing
    values = ghcr_pull._merge(run, {"bootstrap": {"coreProvider": {"enabled": False}}})

    def registered(chart):
        return {d["metadata"]["name"] for d in _render(chart, values, "--show-only", "templates/definitions.yaml")
                if d.get("kind") == "CompositionDefinition"}
    pins = yaml.safe_load((root / "files" / "component-pins.min.yaml").read_text())
    # installer 0.3.701's files/component-pins.min.yaml (as 0.3.698's and 0.3.700's), its entries with alsoFeatures
    assert [(c["name"], c["alsoFeatures"]) for c in pins["components"] if c.get("alsoFeatures")] == [
        ("cert-manager", ["internalTls"]), ("cert-manager-issuers", ["internalTls"]),
        ("cloudnative-pg", ["cloudnativePg"]), ("external-secrets", ["serviceDesk"])]
    stock = registered(root)
    stripped = tmp_path / "stripped" / "installer"
    shutil.copytree(root, stripped)
    bare = yaml.safe_load((root / "files" / "component-pins.min.yaml").read_text())
    for c in bare["components"]:
        c.pop("alsoFeatures", None)
    (stripped / "files" / "component-pins.min.yaml").write_text(yaml.safe_dump(bare))
    assert stock - registered(stripped) == {"cert-manager", "cert-manager-issuers"}
    assert registered(stripped) < stock
    feats = {**yaml.safe_load((root / "values.yaml").read_text())["features"], **run["features"]}
    core = set(diag606.core_features(str(pulled)))
    # #1572: the bench's rule is the installer's inst.compEnabled (bench/installer_features.py)
    rule = {c["name"] for c in pins["components"] if installer_features.component_enabled(c, feats, core)}
    assert rule == stock
    # with coreAgents on (#1572), its private-image components are registered too
    assert sorted(set(ghcr_pull.private_image_paths(str(pulled), run)) & stock) == [
        "agentgateway-policies", "alert-provider", "config-refs-mcp-server", "core-provider-agent", "kagent",
        "repo-mcp-server"]


# ─── helm's reason, in every line layout (#1395 review) ─────────────────────
# CI run 43429175943 rendered with the runner image's newer helm: its reason for the missing define
# was cut to "Error: installer/templates/zz-bench-probe-98ab0cc0.yaml:6:29", the location line of a
# multi-line error, and the cause was lost. Fixtures:
#   v3.19.0 (the VM's, vm606.HELM_VERSION): captured from that helm on this probe (v3.22.1 prints
#     the same bytes);
#   v4.3.2: the layout helm v4.3.2 writes, from its source (pkg/engine/engine.go
#     reformatExecErrorMsg / TraceableError.String: location, then the indented executing line and
#     message, then the chained no-template error; pkg/cmd/template.go appends the --debug hint), its
#     first line the one CI printed.
STDERR = FIXTURES / "helm_stderr"
CAUSE = 'no template "inst.privateImageComponents" associated with template "gotpl"'


@pytest.mark.parametrize("layout", ["v3.19.0_no_template.txt", "v4.3.2_no_template.txt"])
def test_the_reason_keeps_helms_cause_in_every_layout(layout):
    why = ghcr_pull.helm_reason((STDERR / layout).read_text(), 1)
    assert why.startswith("Error: ") and why.endswith(CAUSE) and "\n" not in why and "--debug" not in why, why
    assert "zz-bench-probe-0123abcd.yaml:6:29" in why


@pytest.mark.parametrize("tail,want", [
    ("Error: one line\n", "Error: one line"),
    ("walk.go:75: found symbolic link in path\nError: the reason\n  and its cause\n\nUse --debug flag to x\n",
     "Error: the reason and its cause"),
    ("xxxx partial line of a cut tail\nError: first\nError: second\n", "Error: first Error: second"),
    ("no error line at all\nsecond line\n", "no error line at all second line"),
    ("\n\nUse --debug flag to render out invalid YAML\n", "exit 7"),
    ("", "exit 7"),
])
def test_the_reason_is_everything_from_the_first_error_line(tail, want):
    assert ghcr_pull.helm_reason(tail, 7) == want


def test_the_reason_is_capped():
    assert len(ghcr_pull.helm_reason("Error: " + "y" * 10_000, 1)) == ghcr_pull.REASON_MAX


@pytest.mark.parametrize("layout", ["v3.19.0_no_template.txt", "v4.3.2_no_template.txt"])
def test_a_render_refusal_names_the_cause_whatever_helm_lays_it_out(tmp_path, layout):
    helm = tmp_path / "helm"
    helm.write_text(f"#!/bin/sh\ncat > /dev/null\ncat {shlex.quote(str(STDERR / layout))} >&2\nexit 1\n")
    helm.chmod(0o755)
    with pytest.raises(ghcr_pull._Refused) as e:
        ghcr_pull.private_image_paths(str(_pulled(tmp_path, RECORDED)), _values(), str(helm))
    assert str(e.value).endswith(CAUSE) and "helm template failed: Error: " in str(e.value), str(e.value)


def test_ci_renders_with_the_helm_the_vm_pins():
    wf = (BENCH_DIR.parent.parent.parent.parent / ".github" / "workflows" / "bench-tests.yaml").read_text()
    step = wf[wf.index("- name: The helm the VM pins, first on PATH"):wf.index("- name: The suite's helm is the VM's")]
    assert "from bench import vm606; print(vm606.HELM_VERSION, vm606.HELM_SHA256)" in step
    assert 'sha256sum -c -' in step and '>> "$GITHUB_PATH"' in step
    assert wf.index("The helm the VM pins") < wf.index("- name: Bench suite, in parallel")


# ─── M9: the laptop renders with each run's own values ─────────────────────

def test_the_laptop_renders_with_each_runs_snowplow_and_frontend_tags(tmp_path, laptop_helm):
    """A list that is a rule over componentValues shows what the laptop rendered with: each run's
    SNOWPLOW_TAG_<L> and FRONTEND_TAG_<L> (bench.installer_values), and no frontend override for a
    run without one. The VM's preflight renders with the same document (run606.sh)."""
    pulled = _pulled(tmp_path / "v", RECORDED)
    t = _helpers(pulled).read_text()
    d = _define(t)
    _helpers(pulled).write_text(t.replace(d, d.replace("\n{{- end -}}", (
        "\n- name: snowplow-{{ .Values.componentValues.snowplow.image.tag }}\n  path: [p]\n"
        '{{- with dig "frontend" "image" "tag" "" .Values.componentValues }}\n- name: frontend-{{ . }}\n'
        "  path: [p]\n{{- end }}\n"
        "{{- end -}}"))))
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(pulled / "installer", arcname="installer")
    (laptop_helm / f"installer-{RECORDED}.tgz").write_bytes(buf.getvalue())
    env = _env(SNOWPLOW_TAG_N1="1.12.69", SNOWPLOW_TAG_N2="1.12.70", FRONTEND_TAG_N1="1.8.41", FRONTEND_TAG_N2="1.8.41")
    entries = sum(map(len, AT_RECORDED.values()))
    n1, n2 = _chart_lines(REAL_CHART_CHECK(env, ["N1", "N2"]))
    assert f"{entries + 2} entries" in n1 and "snowplow-1.12.69" in n1 and "frontend-1.8.41" in n1, n1
    assert "snowplow-1.12.70" in n2 and "frontend-1.8.41" in n2 and "snowplow-1.12.69" not in n2, n2
    env = {k: v for k, v in env.items() if not k.startswith("FRONTEND_TAG_")}
    n1, _ = _chart_lines(REAL_CHART_CHECK(env, ["N1", "N2"]))
    assert f"{entries + 1} entries" in n1 and "frontend-" not in n1, n1
