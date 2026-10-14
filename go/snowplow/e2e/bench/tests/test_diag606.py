"""diag606: redaction, progress line, failing-pod selection, safety rails."""

from __future__ import annotations

import json

import pytest

from bench import diag606
from tests import _planted


def test_redact_keeps_json_valid_and_removes_credentials():
    tok, pw, key, bear, jwt = (_planted.opaque(), _planted.opaque(),
                               _planted.opaque(), _planted.bearer(),
                               _planted.jwt())
    assert jwt.count(".") == 2 and jwt.startswith("eyJ")   # real JWT shape
    doc = {"message": f"helm failed: token={tok}, retry",
           "password": pw, "api_key": key,
           "passwordRef": {"name": "admin-password"},
           "log": f"Authorization: Bearer {bear}",
           "jwt": jwt}
    out = diag606.redact(json.dumps(doc, indent=2), extra=(key,))
    back = json.loads(out)                      # still valid JSON
    for leaked in (tok, pw, key, bear, jwt, jwt.split(".")[2]):
        assert leaked not in out
    assert back["passwordRef"] == {"name": "admin-password"}   # a ref, not a secret
    assert back["message"].startswith("helm failed: token=<redacted>")


def test_installer_progress_names_what_is_stuck():
    def s(name, ok, reason="R", msg=""):
        return {"name": name, "Ready": {"status": "True" if ok else "False",
                                        "reason": reason, "message": msg}}
    ok, line = diag606.installer_progress(
        [s("installer", False, "Creating")],
        [s("authn", True), s("krateo-observability", False, "ReconcileError", "pvc pending")])
    assert not ok
    assert "components ready 1/2" in line
    assert "krateo-observability(ReconcileError: pvc pending)" in line
    ok, _ = diag606.installer_progress([s("installer", True)], [s("authn", True)])
    assert ok
    ok, line = diag606.installer_progress([], [])
    assert not ok and "Ready=absent" in line


def test_failing_pods_selects_crash_pending_and_unschedulable():
    pods = [diag606.pod_summary(p) for p in [
        {"metadata": {"name": "ok", "namespace": "n"}, "status": {"phase": "Running",
         "containerStatuses": [{"name": "c", "ready": True, "restartCount": 0, "state": {}}]}},
        {"metadata": {"name": "done", "namespace": "n"}, "status": {"phase": "Succeeded"}},
        {"metadata": {"name": "pend", "namespace": "n"}, "status": {"phase": "Pending",
         "conditions": [{"type": "PodScheduled", "status": "False",
                         "message": "0/5 nodes: Insufficient cpu"}]}},
        {"metadata": {"name": "crash", "namespace": "n"}, "status": {"phase": "Running",
         "containerStatuses": [{"name": "c", "ready": False, "restartCount": 2,
                                "state": {"waiting": {"reason": "CrashLoopBackOff"}}}]}}]]
    bad = {p["name"]: p for p in diag606.failing_pods(pods)}
    assert set(bad) == {"pend", "crash"}
    assert bad["pend"]["unschedulable"].startswith("0/5 nodes")


def test_kube_refuses_missing_pin_057_and_secrets():
    with pytest.raises(ValueError):
        diag606.Kube("", "ctx")
    with pytest.raises(ValueError):
        diag606.Kube("/k", "gke_p_z_krateo-057")
    k = diag606.Kube("/k", "gke_p_z_snowplow-bench-x", run=lambda *a, **kw: None)
    with pytest.raises(ValueError):
        k.kubectl("get", "secrets", "-A")


def test_wait_installer_polls_until_ready_and_logs_each_poll():
    states = iter([False, False, True])
    seen = []

    class K:
        def composition_kinds(self):
            return ["installers.composition.krateo.io", "authns.composition.krateo.io"]

        def items(self, kind, *a):
            ok = self._ok
            return [{"kind": "X", "metadata": {"name": kind.split(".")[0]},
                     "status": {"conditions": [{"type": "Ready",
                                                "status": "True" if ok else "False"}]}}]

    k = K()

    def sleep(_):
        pass

    def log(m):
        seen.append(m)

    orig = diag606._leaf_summaries

    def leaf(kk):
        kk._ok = next(states)
        return orig(kk)
    diag606._leaf_summaries = leaf
    try:
        assert diag606.wait_installer(k, timeout_s=100, interval_s=0, log=log,
                                      clock=lambda: 0, sleep=sleep)
    finally:
        diag606._leaf_summaries = orig
    assert len(seen) == 3 and "components ready 1/1" in seen[-1]
