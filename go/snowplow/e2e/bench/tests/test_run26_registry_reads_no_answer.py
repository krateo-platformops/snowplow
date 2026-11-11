"""Run 26 (bench 4da02a99) died on ONE arm, N1B, at 20:04:11Z, before any measuring:

    cert-manager preflight REFUSED: helm pull oci://quay.io/jetstack/charts/cert-manager --version v1.21.1
    failed: Error: failed to perform "Fetch" on source: Get "https://cdn01.quay.io/quayio-production-s3/
    sha256/af/...?X-Amz-...      <- cut at 300 characters, inside the signed URL: the cause was lost

N1 and N2 passed the same pull. #1335 B's classification and bounded retry (bench/noanswer.py) now
serve every pre-cluster registry read that can refuse a run: cert-manager's pull, the installer's
show + pull, the CRD charts' pulls, and the ghcr preflight, spending ONE bound together; a failure
keeps its cause, its signed query strings and credentials redacted. And the installer is installed
from the copy s_source verified, never read from the registry again once the cluster exists.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import cert_manager as cm
from bench import crd_schema, installer_src, noanswer

SIGNED = ("https://cdn01.quay.io/quayio-production-s3/sha256/af/af0c3e7b2f?X-Amz-Algorithm=AWS4-HMAC-SHA256"
          "&X-Amz-Credential=AKIAEXAMPLE%2F20261111%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261111T200410Z"
          "&X-Amz-Expires=600&X-Amz-SignedHeaders=host&X-Amz-Signature=" + "ab" * 32)


def _helm_error(tail: str) -> str:
    """helm's own shape for run 26's failure, with `tail` the cause after the URL."""
    return f'Error: failed to perform "Fetch" on source: Get "{SIGNED}": {tail}\n'


class _Clock:
    """A clock that moves only when slept (the attempts' pacing) or ticked by a call."""

    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += max(0.0, s)


def _pulls(tails, chart_bytes=None):
    """A `run` for cert_manager.preflight: each helm pull fails with the next of `tails` (helm's
    error) until they run out, then succeeds with the real pinned archive; helm template is real."""
    from tests.test_614_cert_manager import CERT_MANAGER_TGZ, CHART_FILE
    import os
    calls = []
    left = list(tails)

    def run(argv, **kw):
        calls.append(argv)
        if argv[:2] == ["helm", "pull"]:
            if left:
                return subprocess.CompletedProcess(argv, 1, "", _helm_error(left.pop(0)))
            d = argv[argv.index("-d") + 1]
            open(os.path.join(d, CHART_FILE), "wb").write(chart_bytes or CERT_MANAGER_TGZ.read_bytes())
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.run(argv, **kw)
    return run, calls


# ─── the cause survives, the credentials do not ─────────────────────────────

@pytest.mark.parametrize("tail", ["context deadline exceeded", "read tcp 10.0.0.2:5432->52.1.2.3:443: read: "
                                  "connection reset by peer", "unexpected status code 503: Service Unavailable"])
def test_run_26s_error_keeps_its_cause_and_never_its_signature(tail):
    got = noanswer.cause(_helm_error(tail))
    assert got.endswith(f': {tail}'), got
    assert "X-Amz" not in got and "AKIAEXAMPLE" not in got and "ab" * 32 not in got
    assert got.startswith('Error: failed to perform "Fetch" on source: Get "https://cdn01.quay.io/'
                          'quayio-production-s3/sha256/af/af0c3e7b2f?<redacted>"')


def test_credentials_are_redacted_wherever_they_are():
    t = noanswer.redact("Get https://user:s3cret@registry.example/v2/x?token=abc: Authorization: Bearer eyJhbGciOi"
                        " and Basic dXNlcjpwYXNz; signature=deadbeef X-Amz-Signature=cafe")
    for leak in ("s3cret", "user:", "token=abc", "eyJhbGciOi", "dXNlcjpwYXNz", "deadbeef", "cafe"):
        assert leak not in t, (leak, t)


def test_a_long_error_is_shortened_from_its_start_never_its_cause():
    got = noanswer.cause("x" * 10_000 + ": context deadline exceeded")
    assert got.endswith("context deadline exceeded") and len(got) <= noanswer.CAUSE_MAX + 3


@pytest.mark.parametrize("tail,no_answer", [
    ("context deadline exceeded", True), ("dial tcp: i/o timeout", True),
    ("net/http: TLS handshake timeout", True), ("read: connection reset by peer", True), ("unexpected EOF", True),
    ("dial tcp: lookup cdn01.quay.io: Temporary failure in name resolution", True),
    ("unexpected status code 503: Service Unavailable", True), ("response status code 429: Too Many Requests", True),
    ("http2: server sent GOAWAY and closed the connection", True),
    ("401 Unauthorized", False), ("403 Forbidden", False), ("manifest unknown", False),
    ("dial tcp 1.2.3.4:443: connect: connection refused", False), ("dial tcp: lookup nope.invalid: no such host", False),
    ("x509: certificate signed by unknown authority", False),
])
def test_helms_error_text_is_read_as_no_answer_or_an_answer(tail, no_answer):
    assert (noanswer.unanswered_text(noanswer.cause(_helm_error(tail))) is not None) is no_answer


# ─── cert-manager's pull: retried, or refused ───────────────────────────────

@pytest.mark.skipif(__import__("shutil").which("helm") is None, reason="renders with the real helm")
def test_run_26s_pull_with_no_answer_is_asked_again_and_verifies():
    """Run 26 N1B's shape, the cause a CDN timeout: the pull is asked again within the bound (the
    installer step left its share for it), and the chart verifies."""
    clock = _Clock()
    budget = noanswer.Budget(clock=clock, sleep=clock.sleep)
    budget.extend(2 * noanswer.helm_read_bound())           # the installer source step's share, answered fast
    run, calls = _pulls(["context deadline exceeded"])
    res = cm.preflight(__import__("tempfile").mkdtemp(), run=run, budget=budget)
    assert res["ok"], res["failures"]
    assert [c[:2] for c in calls].count(["helm", "pull"]) == 2 and not res.get("no_answer")


def test_a_pull_the_registry_answers_is_refused_at_once_with_its_cause(tmp_path):
    clock = _Clock()
    budget = noanswer.Budget(clock=clock, sleep=clock.sleep)
    budget.extend(10 * noanswer.helm_read_bound())
    run, calls = _pulls(["unexpected status code 403: Forbidden"])
    res = cm.preflight(str(tmp_path), run=run, budget=budget)
    assert not res["ok"] and not res.get("no_answer") and len(calls) == 1
    assert res["failures"] == [f"helm pull {cm.CHART_REF} --version {cm.CHART_VERSION} failed: "
                               f"{noanswer.cause(_helm_error('unexpected status code 403: Forbidden'))}"]
    assert "X-Amz" not in res["failures"][0] and res["failures"][0].endswith("403: Forbidden")


def test_no_answer_until_the_bound_is_spent_is_said_as_no_answer(tmp_path):
    """Never answered: asked again only while the bound holds a whole attempt, each attempt counted at
    its whole bound; then NO ANSWER (exit 4 from the CLI), naming the read and the cause."""
    clock = _Clock()
    budget = noanswer.Budget(clock=clock, sleep=clock.sleep)
    budget.extend(2 * noanswer.helm_read_bound())
    run, calls = _pulls(["read: connection reset by peer"] * 50)
    t0 = clock()
    res = cm.preflight(str(tmp_path), run=run, budget=budget)
    assert res["no_answer"] and not res["ok"]
    n = [c[:2] for c in calls].count(["helm", "pull"])
    assert n == 3                                            # 3 x 120 s within the 2 x 120 + 120 s bound
    assert clock() - t0 <= budget.total
    why = res["failures"][0]
    assert why.startswith(f"NO ANSWER from the helm pull of {cm.CHART_REF} --version {cm.CHART_VERSION} (")
    assert "connection reset by peer" in why and f"in {n} attempt(s)" in why and "X-Amz" not in why


def test_the_cli_exits_4_for_no_answer_and_2_for_an_answer(tmp_path, monkeypatch, capsys):
    """run606 tells them apart by the exit: 4 is NO ANSWER, 2 the chart's refusal."""
    orig = cm.preflight
    monkeypatch.setattr(noanswer.Budget, "shared", classmethod(lambda cls, path, sleep=None: _paced(cls)))
    for tails, want, line in ((["context deadline exceeded"] * 50, 4, "cert-manager preflight: NO ANSWER"),
                              (["unexpected status code 404: Not Found"], 2, "cert-manager preflight REFUSED")):
        run, _ = _pulls(tails)
        monkeypatch.setattr(cm, "preflight", lambda w, budget=None, run=run: orig(w, run=run, budget=budget))
        rc = cm.main(["preflight", "--workdir", str(tmp_path / str(want)), "--out", str(tmp_path / f"{want}.json"),
                      "--budget-file", str(tmp_path / "b.json")])
        err = capsys.readouterr().err
        assert rc == want and line in err and "X-Amz" not in err, err


def _paced(cls):
    clock = _Clock()
    return cls(clock=clock, sleep=clock.sleep)


# ─── the installer's show + pull, the CRD charts' pulls ─────────────────────

def test_the_installers_reads_and_the_crd_pulls_go_through_the_same_classification(tmp_path):
    clock = _Clock()
    budget = noanswer.Budget(clock=clock, sleep=clock.sleep)

    def never(argv, **kw):
        return subprocess.CompletedProcess(argv, 1, "", _helm_error("context deadline exceeded"))
    res = installer_src.oci_preflight("oci://ghcr.io/x/installer", "0.3.1", str(tmp_path / "i"), run=never,
                                      budget=budget)
    assert res["no_answer"] and res["failures"][0].startswith("NO ANSWER from the helm show chart oci://ghcr.io/"
                                                              "x/installer --version 0.3.1 (")

    def refused(argv, **kw):
        return subprocess.CompletedProcess(argv, 1, "", "Error: 403 Forbidden\n")
    res = installer_src.oci_preflight("oci://ghcr.io/x/installer", "0.3.1", str(tmp_path / "j"), run=refused,
                                      budget=noanswer.Budget(clock=clock, sleep=clock.sleep))
    assert not res.get("no_answer") and res["failures"] == [
        "helm show chart oci://ghcr.io/x/installer --version 0.3.1 failed: Error: 403 Forbidden"]
    got = crd_schema.pull({"chart": "authn-crds", "version": "0.27.3", "ref": "oci://ghcr.io/x/authn-crds"},
                          str(tmp_path / "w"), run=never, budget=None)
    assert got["no_answer"] and got["error"].startswith("no answer from the helm pull of oci://ghcr.io/x/authn-crds")


def test_the_registry_reads_of_a_run_spend_one_bound_across_their_steps(tmp_path):
    """Budget.shared: each step adds its share to the run's one bound, in a file in OUT."""
    path = str(tmp_path / ".registry-read-bound.json")
    a = noanswer.Budget.shared(path)
    a.extend(240)
    b = noanswer.Budget.shared(path)                 # the next step, another process
    assert b.total == 240 and abs(b.deadline - a.deadline) < 1e-6
    b.extend(120)
    assert json.load(open(path))["total"] == 360 and b.holds(300)
