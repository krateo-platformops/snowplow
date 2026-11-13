"""Run 26g's forensics: three bench defects, each fixed and armed here.

1. Every arm created its GKE cluster, then failed ~10 min later in gke-up: the TTL proof asked
   cloudresourcemanager.googleapis.com as the GKE node SA, and that API was not enabled in the project (since run
   26h the proof calls the GKE API instead, and only the laptop's `gcloud projects` calls still need it). Now
   every Google API the run calls (laptop, orchestrator, node SA), DERIVED from the code (bench/gcp_apis.py), is
   checked enabled before anything is created: in vm606 `up` (dry or not) and in run606's first step. An empty or
   failed services answer refuses (gcloud lists can exit 0 with nothing).
2. The proof's refusal said only "HTTP 403, PERMISSION_DENIED": the real reason, SERVICE_DISABLED, was in
   error.details[]. bench.gke.ttl_proof_answer (run 26h: the GKE-API proof, each probe's answer read by
   probe_answer) now names each details[] reason (and the API it is about), never the free-text message.
3. The schema step's CRD check pulled ghcr.io charts anonymously ("kagent-crds 0.1.6 not read: helm pull failed:
   ... 401"). A ghcr.io CRD chart is now fetched in-process with the ghcr pull token, by path, as the registry step
   reads every chart. (Whether an unreadable chart refuses the run: the existing contract names it and fails only
   when a document's kind is then MISSING; kept, reported.)
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
from pathlib import Path

import pytest

from bench import crd_schema, gcp_apis, gke, vm606
from tests import fake_region
from tests.conftest import FAKE_GHCR_TOKEN, FAKE_GHCR_USER
from tests.test_606 import _run606
from tests.test_gke_pvc_disks import NAME, fakes  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)
from tests.test_vm606 import RUN, _run, _up_env, _verbs, fake  # noqa: F401  (fixture)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
SERVICE_DISABLED = FIXTURES / "crm_service_disabled_403.json"
CRM = "cloudresourcemanager.googleapis.com"
INSIDE, CONTROL = gke.ttl_probe_names("abcdef0123")


def _proof_logs(code: int, body: dict) -> str:
    """The proof pod's log when every one of its probes (both permissions, both names) answered `body` at `code`."""
    return "".join(f"{gke.TTL_PROOF_MARK}{perm} {name} {code} {json.dumps(body)}\n"
                   for perm in gke.TTL_PROBE_METHODS for name in (INSIDE, CONTROL))


def _services(env: dict):
    """A `run` for gcp_apis.enabled: the suite's fake services answer (tests/fake_region.py) under `env`."""
    calls = []

    def run(argv):
        calls.append(argv)
        assert argv[:4] == ["gcloud", "services", "list", "--enabled"]
        return fake_region.services_answer(env)
    run.calls = calls
    return run


# ─── 1. every Google API the run calls, derived from the code, enabled ───────

def test_the_apis_are_derived_from_every_caller():
    """Each caller's APIs come from its own code: the node SA's from the manifests bench.gke renders for its pods
    and NODE_SA_REST, the laptop's from the operator modules' gcloud calls and flags, the orchestrator's from the
    rest; no gcloud call is left without an API."""
    need = gcp_apis.required()
    assert need["unmapped"] == []
    apis = need["apis"]
    assert {f"{h}.googleapis.com" for _f, h in gcp_apis.NODE_SA_REST.values()} <= set(apis)
    # run 26h: the TTL proof calls the GKE API as the node SA; cloudresourcemanager only for the operator's
    # `gcloud projects` calls (the project, its IAM policy), never for the node SA any more
    assert gcp_apis.NODE_SA_REST == {"TTL_PROOF_API": ("gke.py", "container")}
    assert any(u.startswith("GKE node SA: REST container.googleapis.com (the TTL proof Job")
               for u in apis["container.googleapis.com"]), apis["container.googleapis.com"]
    assert apis[CRM] and all(u.startswith("laptop (operator): gcloud projects (bench/") for u in apis[CRM]), apis[CRM]
    assert any(u.startswith("GKE node SA: gcloud container (the ttl-self-destruct CronJob")
               for u in apis["container.googleapis.com"])
    assert any(u.startswith("laptop (operator): --tunnel-through-iap (bench/gcevm.py:")
               for u in apis["iap.googleapis.com"])
    assert any(u.startswith("laptop (operator): enable-oslogin (bench/vm606.py:") for u in apis["oslogin.googleapis.com"])
    assert any(u.startswith("orchestrator: REST cloudquotas.googleapis.com (bench/gke.py:")
               for u in apis["cloudquotas.googleapis.com"])
    assert any(u.startswith("orchestrator: gcloud storage (bench/durable.py:") for u in apis["storage.googleapis.com"])
    assert any(u.startswith("orchestrator: gcloud services (bench/gcp_apis.py:")
               for u in apis["serviceusage.googleapis.com"])
    assert "auth.googleapis.com" not in apis                # www.googleapis.com/auth/...: an OAuth scope


def test_vm606_reads_its_node_sa_list_and_its_operator_modules_from_the_one_source():
    assert vm606.NODE_SA_REST is gcp_apis.NODE_SA_REST and vm606.OPERATOR_SIDE is gcp_apis.OPERATOR_SIDE


def test_a_new_call_in_the_code_is_counted_or_refused(tmp_path):
    """A REST host added anywhere becomes a required API; a gcloud group the map does not know refuses the check
    by name (the derivation is never silently short)."""
    d = tmp_path / "copy"
    shutil.copytree(BENCH_DIR / "bench", d / "bench", ignore=shutil.ignore_patterns("__pycache__"))
    with open(d / "bench" / "nodes.py", "a") as fh:
        fh.write('\nPUBSUB = "https://pubsub.googleapis.com/v1"\n')
    assert "orchestrator: REST pubsub.googleapis.com (bench/nodes.py:" in " ".join(
        gcp_apis.required(d)["apis"]["pubsub.googleapis.com"])
    with open(d / "bench" / "testvm.py", "a") as fh:
        fh.write('\nX = ["gcloud", "logging", "read"]\n')
    with pytest.raises(gcp_apis.Refused, match=r"gcloud logging \(bench/testvm.py:\d+\): no API mapped"):
        gcp_apis.check("p", dry=True, bench_dir=d)


def test_a_node_sa_pods_calls_are_read_from_its_manifests():
    got = gcp_apis.required(manifests=[("a pod", "tok=$(gcloud auth print-access-token)\n"
                                                  "curl https://pubsub.googleapis.com/v1/x\ngcloud logging read\n")])
    assert got["apis"]["pubsub.googleapis.com"] == ["GKE node SA: REST pubsub.googleapis.com (a pod)"]
    assert got["unmapped"] == ["gcloud logging (a pod): no API mapped (GCLOUD_API)"]


def test_the_clusters_logging_and_monitoring_apis_are_read_from_its_create_argv():
    """bench.gke creates the cluster with --logging SYSTEM --monitoring SYSTEM: its nodes then write to Cloud Logging
    and Cloud Monitoring as the node SA, so both APIs are required, read from the argv gke builds (one source)."""
    apis = gcp_apis.required()["apis"]
    for flag, api in (("--logging", "logging.googleapis.com"), ("--monitoring", "monitoring.googleapis.com")):
        assert apis[api] == [f"GKE node SA: container clusters create {flag} SYSTEM (bench.gke.create_commands)"]
    # read from the argv, never a list: a flag set to NONE (either spelling) or left out requires nothing
    got = gcp_apis.required(creates=[["gcloud", "container", "clusters", "create", "c", "--logging=NONE",
                                      "--monitoring", "SYSTEM,API_SERVER"]])["apis"]
    assert "logging.googleapis.com" not in got
    assert got["monitoring.googleapis.com"] == [
        "GKE node SA: container clusters create --monitoring SYSTEM,API_SERVER (bench.gke.create_commands)"]
    got = gcp_apis.required(creates=[["gcloud", "container", "clusters", "create", "c", "--monitoring", "NONE"]])
    assert not {"logging.googleapis.com", "monitoring.googleapis.com"} & set(got["apis"])


def test_a_disabled_logging_api_refuses_naming_the_node_sa():
    with pytest.raises(gcp_apis.Refused) as e:
        gcp_apis.check("operations-dev-krateo-io",
                       run=_services({"FAKE_SERVICES_DISABLED": "logging.googleapis.com"}))
    assert ("logging.googleapis.com is not enabled (called by GKE node SA: container clusters create --logging "
            "SYSTEM (bench.gke.create_commands))") in str(e.value)


def test_with_the_projects_services_of_run_26g_the_check_refuses_naming_cloudresourcemanager():
    """The project's listing on 2026-11-13 (tests/fixtures/services_enabled_20261113.json): every API the run calls
    is enabled but cloudresourcemanager (then the TTL proof's; since run 26h the operator's `gcloud projects`
    calls'), and the refusal names it, who calls it and how to enable it."""
    with pytest.raises(gcp_apis.Refused) as e:
        gcp_apis.check("operations-dev-krateo-io", run=_services({"FAKE_SERVICES": "captured"}))
    msg = str(e.value)
    n = len(gcp_apis.required()["apis"])
    assert msg.startswith(f"1 of the {n} Google APIs the run calls is not enabled in operations-dev-krateo-io")
    assert f"{CRM} is not enabled (called by laptop (operator): gcloud projects (bench/" in msg
    assert f"gcloud services enable {CRM} --project operations-dev-krateo-io" in msg
    assert msg.count("is not enabled (called by") == 1


def test_every_api_enabled_passes_and_names_them():
    run = _services({})
    lines = gcp_apis.check("operations-dev-krateo-io", run=run)
    assert lines[0].startswith("Google APIs: each of the ") and CRM in lines[0] and len(run.calls) == 1


@pytest.mark.parametrize("answer, says", [
    ((0, "[]\n", ""), "it answered no enabled service (an empty answer is a failed read, never 'nothing disabled')"),
    ((0, "", ""), "its answer is not a JSON list"),
    ((0, json.dumps([{"config": {"name": CRM}, "state": "DISABLED"}]), ""), "it answered no enabled service"),
    (fake_region.services_answer({"FAKE_SERVICES": "fail"}), "rc=1; "),
    ((124, "", "no answer within its 120 s bound"), "rc=124; "),
])
def test_an_empty_or_failed_answer_refuses_never_reads_as_nothing_disabled(answer, says):
    with pytest.raises(gcp_apis.Refused) as e:
        gcp_apis.check("operations-dev-krateo-io", run=lambda argv: answer)
    assert says in str(e.value) and "could not be read" in str(e.value)
    assert "@" not in str(e.value)                       # gcloud's error names the account: never repeated


def test_the_read_sets_the_partial_error_property_false(tmp_path, monkeypatch):
    """The one gcloud the check runs (a fake that prints its environment and the empty answer, rc 0)."""
    b = tmp_path / "bin"
    b.mkdir()
    (b / "gcloud").write_text("#!/bin/sh\necho \"$CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR\" > \"$OUTF\"\necho '[]'\n")
    (b / "gcloud").chmod(0o755)
    monkeypatch.setenv("PATH", f"{b}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("OUTF", str(tmp_path / "env"))
    with pytest.raises(gcp_apis.Refused, match="answered no enabled service"):
        gcp_apis.check("p")
    assert (tmp_path / "env").read_text().strip() == "false"


class _NoRefusal(BaseException):
    """up did not return within its bound (a BaseException, so no `except Exception` in up swallows it)."""


#: how long `vm606 up` may take to refuse at its first check: it refuses in about a second against the fakes. An up
#: that got past the check goes on to make and wait for a VM (a reviewer's mutation hung there for 34 minutes):
#: that fails here, fast, never as a hang.
UP_REFUSES_WITHIN_S = 60


def _up_bounded(capsys, *argv):
    """_run(capsys, "up", *argv), failed by SIGALRM if it has not returned within UP_REFUSES_WITHIN_S."""
    def boom(*_):
        raise _NoRefusal(f"vm606 up did not return within {UP_REFUSES_WITHIN_S} s: it went past the API check")
    old = signal.signal(signal.SIGALRM, boom)
    signal.alarm(UP_REFUSES_WITHIN_S)
    try:
        return _run(capsys, "up", *argv)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


@pytest.mark.parametrize("yes", [True, False])
def test_vm606_up_refuses_before_anything_while_an_api_is_disabled(fake, capsys, monkeypatch, yes):  # noqa: F811
    monkeypatch.setenv("FAKE_SERVICES", "captured")
    rc, _out, err = _up_bounded(capsys, "--env-file", _up_env(), "--run", RUN, "--ttl-hours", "15",
                                *(["--yes"] if yes else []))
    assert rc == 2 and f"{CRM} is not enabled (called by laptop (operator)" in err, err
    assert not [v for v in _verbs(fake) if "add-iam-policy-binding" in v or "create" in v or "delete" in v]


def test_vm606_up_refuses_an_empty_services_answer(fake, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_SERVICES", "empty")
    rc, _out, err = _up_bounded(capsys, "--env-file", _up_env(), "--run", RUN, "--ttl-hours", "15", "--yes")
    assert rc == 2 and "could not be read" in err and "never 'nothing disabled'" in err
    assert not [v for v in _verbs(fake) if "create" in v or "add-iam-policy-binding" in v]


def test_plan_iam_grants_the_orchestrator_the_services_read():
    need = vm606.plan(vm606.scan(BENCH_DIR))
    assert any(u.startswith("gcloud services list  bench/gcp_apis.py:")
               for u in need["roles/serviceusage.serviceUsageConsumer"])


def test_run606_refuses_at_its_first_step_while_an_api_is_disabled(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_SERVICES="captured"),
                       capture_output=True, text=True, timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and f"apis FAILED: 1 of the " in log and f"{CRM} is not enabled" in log, log[-3000:]
    assert "=== step source ===" not in log and "=== step up ===" not in log
    gcl = [c["argv"] for c in map(json.loads, (state / "calls.jsonl").read_text().splitlines()) if c["tool"] == "gcloud"]
    assert gcl and all(a[:3] == ["services", "list", "--enabled"] for a in gcl), gcl
    recs = [json.loads(x) for x in (tmp / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert [r["step"] for r in recs] == ["apis"]


def test_run606s_dry_pass_derives_the_apis_and_reads_nothing_from_gcp(tmp_path):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr[-3000:]
    log = (tmp_path / "out" / "run.log").read_text()
    assert "=== step apis ===" in log and "DRY: gcloud services list --enabled --project operations-dev-krateo-io" in log
    assert log.index("=== step apis ===") < log.index("=== step source ===")


# ─── 2. the TTL proof's refusal names Google's reason ───────────────────────

def test_a_service_disabled_answer_is_named_by_its_reason():
    """Run 26g's own answer (Google's SERVICE_DISABLED 403, as captured), given to every probe of the GKE-API proof
    (run 26h): refused, named by its code, status and reason, never by its free text."""
    body = json.loads(SERVICE_DISABLED.read_text())
    ok, why = gke.ttl_proof_answer(_proof_logs(403, body))
    assert not ok
    desc = "HTTP 403 (403 PERMISSION_DENIED; reason: SERVICE_DISABLED (service cloudresourcemanager.googleapis.com))"
    for perm in gke.TTL_PROBE_METHODS:
        assert (f"{perm} is not granted to the node SA, not even on snowplow-bench-* clusters ({INSIDE} answered "
                f"{desc}; {CONTROL} answered {desc})") in why, why
    assert "357378285144" not in why and "has not been used" not in why     # the free text never


@pytest.mark.parametrize("details, says", [
    ([{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "IAM_PERMISSION_DENIED",
       "domain": "iam.googleapis.com", "metadata": {"permission": "resourcemanager.projects.testIamPermissions"}}],
     "HTTP 403 (403 PERMISSION_DENIED; reason: IAM_PERMISSION_DENIED)"),
    ([{"reason": "denied for x@example.invalid"}, {"reason": "SERVICE_DISABLED",
                                                    "metadata": {"service": "x@example.invalid"}}],
     "HTTP 403 (403 PERMISSION_DENIED; reason: SERVICE_DISABLED)"),
    ("not a list", "HTTP 403 (403 PERMISSION_DENIED)"),
    ([], "HTTP 403 (403 PERMISSION_DENIED)"),
])
def test_only_a_reasons_code_and_an_api_host_get_through(details, says):
    ok, why = gke.ttl_proof_answer(_proof_logs(403, {"error": {
        "code": 403, "status": "PERMISSION_DENIED", "message": "for x@example.invalid", "details": details}}))
    assert not ok and says in why and "@" not in why, why


def test_gke_up_says_the_reason_when_the_proof_is_refused(fakes, monkeypatch, capsys):  # noqa: F811
    monkeypatch.setenv("FAKE_TTL_PROOF", "body")
    monkeypatch.setenv("FAKE_TTL_PROOF_BODY", str(SERVICE_DISABLED))
    rc = gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--ttl-proof-out", str(fakes / "p.json"), "--yes"])
    err = capsys.readouterr().err
    assert rc == gke.EXIT_TTL_BACKSTOP_MISSING
    assert "reason: SERVICE_DISABLED (service cloudresourcemanager.googleapis.com)" in err, err


# ─── 3. the CRD charts on ghcr.io are read with the ghcr pull token ─────────

def _no_helm(argv, **kw):
    raise AssertionError(f"helm ran for a ghcr.io chart: {argv}")


def _src(org: str = "krateo-platformops") -> dict:
    return {"component": "authn-crds", "chart": "authn-crds", "version": "0.27.3", "from": "pin",
            "ref": f"oci://ghcr.io/{org}/charts/authn-crds", "repo": None}


def test_a_ghcr_crd_chart_is_fetched_with_the_token_never_by_an_anonymous_helm_pull(tmp_path):
    got = crd_schema.pull(_src(), str(tmp_path), run=_no_helm, creds=(FAKE_GHCR_USER, FAKE_GHCR_TOKEN))
    assert not got.get("error") and got["path"].endswith("authn-crds-0.27.3.tgz"), got
    crds = crd_schema.read_crds(got)
    assert [c["spec"]["names"]["kind"] for c in crds["crds"]] == ["User"]


def test_a_private_chart_needs_the_token_that_reads_it(tmp_path):
    """krateo-agentiko is private (kagent-crds' org): a token without its scope is answered 401 there, and no
    credential at all is never a pull."""
    got = crd_schema.pull(_src("krateo-agentiko"), str(tmp_path / "a"), run=_no_helm,
                          creds=(FAKE_GHCR_USER, "scope401"))
    assert got["error"] == "ghcr pull failed: oci://ghcr.io/krateo-agentiko/charts/authn-crds:0.27.3: HTTP 401 "\
                           "Unauthorized" and not got.get("no_answer"), got
    got = crd_schema.pull(_src("krateo-agentiko"), str(tmp_path / "b"), run=_no_helm, creds=None)
    assert "no ghcr pull credential was given (--ghcr-env-file): not pulled" in got["error"]
    got = crd_schema.pull(_src("krateo-agentiko"), str(tmp_path / "c"), run=_no_helm,
                          creds=(FAKE_GHCR_USER, FAKE_GHCR_TOKEN))
    assert not got.get("error"), got


def test_a_ghcr_fetch_with_no_answer_is_asked_again_within_the_runs_bound(tmp_path, monkeypatch):
    from tests.test_ghcr_pull import _flaky, _timeout
    from tests.test_run26_registry_reads_no_answer import _savings
    _savings(monkeypatch)
    seen = _flaky(monkeypatch, lambda r: r.get_method() == "GET" and "/authn-crds/manifests/" in r.full_url,
                  times=1, exc=_timeout)
    got = crd_schema.pull_all([_src()], str(tmp_path / "w"), str(tmp_path / "b.json"), run=_no_helm,
                              creds=(FAKE_GHCR_USER, FAKE_GHCR_TOKEN))
    assert seen["n"] == 1 and got[0].get("path") and not got[0].get("error"), got


def _installer(tmp_path) -> Path:
    """An installer chart, as oci-preflight pulls it, pinning the vendored CRD charts on ghcr.io (its ociRepo)."""
    import yaml
    d = tmp_path / "pulled" / "installer"
    (d / "files").mkdir(parents=True)
    (d / "Chart.yaml").write_text(yaml.safe_dump({"apiVersion": "v2", "name": "installer", "version": "0.3.425"}))
    (d / "values.yaml").write_text("ociRepo: oci://ghcr.io/krateo-platformops/charts\n")
    (d / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": [
        {"name": "authn-crds", "kind": "AuthnCrds", "version": "0.27.3"},
        {"name": "snowplow-crds", "kind": "SnowplowCrds", "version": "1.12.56"},
        {"name": "core-provider-crds", "kind": "CoreProviderCrds", "version": "2.13.40"}]}))
    return d.parent


def test_the_check_reads_the_token_by_path_and_never_prints_it(tmp_path):
    pulled = _installer(tmp_path)
    argv = ["check", "--installer-pulled", str(pulled), "--workdir", str(tmp_path / "w"), "--out",
            str(tmp_path / "c.json"), "--users", "2", "--namespaces", "2"]
    p = subprocess.run(["python3", "-m", "bench.crd_schema", *argv, "--ghcr-env-file", os.environ["GHCR_PULL_ENV_FILE"]],
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=300)
    res = json.loads((tmp_path / "c.json").read_text())
    ghcr = [s for s in res["sources"] if s["from"] == "pin"]
    assert ghcr and all(s["crds"] and not s["error"] for s in ghcr), (ghcr, p.stderr[-2000:])
    for text in (p.stdout, p.stderr, (tmp_path / "c.json").read_text()):
        assert FAKE_GHCR_TOKEN not in text
    bad = tmp_path / "loose.env"
    bad.write_text(f"GHCR_PULL_USERNAME={FAKE_GHCR_USER}\nGHCR_PULL_TOKEN={FAKE_GHCR_TOKEN}\n")
    bad.chmod(0o644)
    p = subprocess.run(["python3", "-m", "bench.crd_schema", *argv, "--ghcr-env-file", str(bad)],
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=120)
    assert p.returncode == 2 and "refusing: the ghcr pull token: " in p.stderr and FAKE_GHCR_TOKEN not in p.stderr


def test_run606s_schema_step_pulls_no_ghcr_chart_with_helm(tmp_path):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", LOAD_DRIVER="k6")
    assert p.returncode == 0, p.stderr[-3000:]
    res = json.loads((tmp_path / "out" / "crd-check.json").read_text())
    assert all(s["crds"] and not s["error"] for s in res["sources"] if s["from"] == "pin"), res["sources"]
    calls = tmp_path / "registry-state" / "calls.jsonl"
    helm = [c["argv"] for c in map(json.loads, calls.read_text().splitlines())] if calls.exists() else []
    assert not [a for a in helm if a[:1] == ["pull"] and a[1].startswith("oci://ghcr.io/") and "-crds" in a[1]], helm
