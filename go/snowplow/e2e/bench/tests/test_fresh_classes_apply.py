"""Run 17 attempt 4 failed as tooling at the data step. All 670 fresh identity
class Users were refused by the API server (`spec.avatarURL: Required value`).
The run log kept only `fresh classes FAILED: [07:53:11] RUN: kubectl ...`, so
the cause lived in run606's .kubectl-last-error file and nowhere else.

1. The cause: the bench had TWO authn user renderings, and fresh_classes'
   lacked spec.avatarURL, which the User CRD requires (authn-crds 0.27.3,
   vendored in tests/fixtures/crd_charts/authn-crds/). Now there is
   one (bench/authn_user.py), and every User the bench renders is validated
   against that CRD here. The harness's fake API server validates applied
   Users against it too.
2. The evidence gap: kerr_line kept the FIRST line of the captured stderr,
   and run606's own `RUN:` line shares that stderr and came first. Now every
   line the command wrote is kept, redacted, with identical lines counted.
   record_failure says it in the log and keeps it for the verdict
   (command-failures.jsonl, report.command_failures).
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
import yaml

from bench import authn_user, fresh_classes, identity, lifecycle
from tests.test_935_fresh_first_visits import _run606_k6
from tests.test_driver_stuck_api_blip import kerr_line
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

CRD = yaml.safe_load((BENCH_DIR / "tests" / "fixtures" / "crd_charts" / "authn-crds" / "templates" / "basic.authn.krateo.io_users.yaml").read_text())
SCHEMA = CRD["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
TYPES = {"string": str, "object": dict, "array": list, "boolean": bool, "integer": int}


def violations(obj, schema, path="") -> list[str]:
    """The subset of OpenAPI v3 the API server checks that these objects can
    break: required properties, types, array items, nested objects."""
    out = []
    t = schema.get("type")
    if t in TYPES and not isinstance(obj, TYPES[t]):
        return [f"{path or '.'}: not {t}"]
    if t == "object":
        for k in schema.get("required") or []:
            if k not in obj:
                out.append(f"{path}.{k}: Required value")
        for k, sub in (schema.get("properties") or {}).items():
            if k in obj:
                out += violations(obj[k], sub, f"{path}.{k}")
    if t == "array" and "items" in schema:
        for i, v in enumerate(obj):
            out += violations(v, schema["items"], f"{path}[{i}]")
    return out


def _fresh_docs():
    p = fresh_classes.plan(sessions=2, routes=4, think_s=15, duration_s=600, namespaces=3, user_prefix="u-")
    return [d for d in yaml.safe_load_all(fresh_classes.manifest(p, "pw")) if d]


def _lifecycle_docs(monkeypatch):
    applied = []
    monkeypatch.setattr(lifecycle, "kubectl", lambda *a, **k: (applied.append(k.get("input_data")) or (0, "", "")))
    lifecycle.create_synthetic_users(1, 3, login=False)
    return [d for d in yaml.safe_load_all("---\n".join(applied)) if d]


def test_the_vendored_crd_requires_what_the_api_server_refused():
    assert CRD["metadata"]["name"] == "users.basic.authn.krateo.io"
    assert SCHEMA["properties"]["spec"]["required"] == ["avatarURL", "displayName", "passwordRef"]


def test_every_fresh_class_user_satisfies_the_user_crd():
    users = [d for d in _fresh_docs() if d["kind"] == "User"]
    assert users and all(violations(u, SCHEMA) == [] for u in users), [violations(u, SCHEMA) for u in users][:2]
    # run 17 attempt 4's manifest, as it was: refused exactly as the API server did
    old = dict(users[0], spec={k: v for k, v in users[0]["spec"].items() if k != "avatarURL"})
    assert violations(old, SCHEMA) == [".spec.avatarURL: Required value"]


def test_every_data_step_user_satisfies_the_user_crd(monkeypatch):
    users = [d for d in _lifecycle_docs(monkeypatch) if d["kind"] == "User"]
    assert len(users) == 3 and all(violations(u, SCHEMA) == [] for u in users)


def test_the_two_paths_render_users_the_same_way(monkeypatch):
    """One rendering: the same documents' shape from both paths."""
    def shape(d):
        return (d["apiVersion"], d["kind"], tuple(sorted(d.get("spec") or d.get("stringData") or {})),
                tuple(sorted(d["metadata"])))
    fresh = {shape(d) for d in _fresh_docs() if d["kind"] in ("User", "Secret")}
    data = {shape(d) for d in _lifecycle_docs(monkeypatch) if d["kind"] in ("User", "Secret")}
    assert fresh == data
    assert authn_user.avatar_url(1) == "https://i.pravatar.cc/256?img=2"     # the data step's, unchanged


# ─── the evidence: every line the command wrote, redacted, counted ──────────

ATTEMPT4 = ("[07:53:11] RUN: kubectl --kubeconfig=k --context=c apply --server-side --force-conflicts "
            "--field-manager=bench606 -f /out/fresh-classes.yaml\n"
            + "".join(f'Error from server (Invalid): User.basic.authn.krateo.io "scaleuser-fresh-{s}-{i:03d}" '
                      f"is invalid: spec.avatarURL: Required value\n" for s in range(1, 6) for i in range(1, 135)))
LINE = ('670 x Error from server (Invalid): User.basic.authn.krateo.io "<redacted>" is invalid: '
        "spec.avatarURL: Required value")


def test_the_summary_keeps_every_error_not_the_benchs_own_run_line():
    assert identity.summarize(ATTEMPT4) == [LINE]
    mixed = ATTEMPT4 + 'Error from server (AlreadyExists): rolebindings.rbac.authorization.k8s.io "scaleuser-fresh-1-001-reader" already exists\n'
    assert identity.summarize(mixed) == [LINE, 'Error from server (AlreadyExists): rolebindings.rbac.authorization.k8s.io '
                                               '"<redacted>" already exists']
    assert identity.summarize("[07:53:11] RUN: kubectl apply\n") == []
    assert identity.summarize("") == []


def test_kerr_line_says_the_cause_not_the_command(tmp_path):
    got = kerr_line(tmp_path, ATTEMPT4.rstrip("\n"))
    assert got == LINE
    assert "scaleuser-fresh" not in got and "RUN:" not in got


def test_run606_records_a_refused_apply_in_the_log_and_for_the_verdict(harness):  # noqa: F811
    """The harness's API server refuses a User missing a required field (here
    one more than authn's CRD requires): the data step fails, its log line
    and command-failures.jsonl carry the refusal, redacted."""
    p, calls, out = _run606_k6(harness, DRIVE_S="200", BROWSER_THINK_S="1", FAKE_USER_REQUIRE="nickname")
    log = p.stdout + p.stderr
    assert p.returncode != 0
    line = next(ln for ln in log.splitlines() if "fresh classes FAILED: applying their manifest:" in ln)
    assert ('x Error from server (Invalid): User.basic.authn.krateo.io "<redacted>" is invalid: spec.nickname: '
            "Required value") in line
    assert "RUN: kubectl" not in line.split("FAILED:", 1)[1]
    assert "scaleuser-fresh" not in log
    recs = [json.loads(x) for x in (out / "command-failures.jsonl").read_text().splitlines()]
    assert len(recs) == 1 and recs[0]["step"] == "data"
    assert recs[0]["what"] == "fresh classes FAILED: applying their manifest"
    assert recs[0]["stderr"][0].endswith('User.basic.authn.krateo.io "<redacted>" is invalid: spec.nickname: Required value')


def test_the_fresh_classes_apply_passes_the_validating_api_server(harness):  # noqa: F811
    """With the one rendering, the harness's validating API server accepts the
    fresh classes (attempt 4's manifest would have failed right here)."""
    p, calls, out = _run606_k6(harness, DRIVE_S="200", BROWSER_THINK_S="1")
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    # present from the start and empty: "none failed" is recorded, not inferred from a missing file
    assert (out / "command-failures.jsonl").read_text() == ""


def test_the_verdict_carries_the_command_failures(tmp_path):
    f = tmp_path / "command-failures.jsonl"
    rec = {"kind": "command_failure", "step": "measure", "what": "measure FAILED: reading job/k6load failed",
           "t": 1.0, "stderr": ["Error from server (Forbidden): jobs.batch \"<redacted>\" is forbidden"]}
    f.write_text(json.dumps(rec) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f)], cwd=BENCH_DIR,
                       capture_output=True, text=True, timeout=120)
    assert json.loads(p.stdout)["report"]["command_failures"] == [rec]


@pytest.mark.parametrize("line", [
    'Error from server (NotFound): secrets "scaleuser-0001-clientconfig" not found',
    'Error from server (Invalid): User.basic.authn.krateo.io "scaleuser-fresh-1-001" is invalid: spec.x: Required value',
    'Error from server (AlreadyExists): rolebindings.rbac.authorization.k8s.io "scaleuser-fresh-1-001-reader" already exists',
    'Error from server (Forbidden): users.basic.authn.krateo.io "scaleuser-0001" is forbidden: User "x" cannot get',
])
def test_an_api_error_never_names_the_object(line):
    assert "scaleuser" not in identity.redact_identity(line)
