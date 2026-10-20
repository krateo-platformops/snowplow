"""Run 9 attempt 3: one transient apiserver drop must not cost a user batch.

Attempt 3 (2026-10-20) deployed all 50K compositions, then data606 died with
"only 950/1000 users registered": batch 301-350 hit "Unable to connect to the
server: http2: client connection lost" once and was never retried. These arms
pin the retry contract for every bulk apply in the data step: re-apply the
SAME YAML on 5/10/20 s backoff (4 attempts), fail only when every attempt
fails. In-process only; kubectl and sleep are stubbed.
"""

from __future__ import annotations

import pytest

from bench import data606, lifecycle

TRANSIENT = ("Unable to connect to the server: http2: client connection lost")


class FlakyKubectl:
    """Fails the apply whose YAML contains `marker` `fail_times` times."""

    def __init__(self, marker=None, fail_times=1):
        self.marker = marker
        self.fail_left = fail_times
        self.applies = []  # input_data of every apply call, in order

    def __call__(self, *args, **kw):
        data = kw.get("input_data") or ""
        if args[:1] == ("apply",):
            self.applies.append(data)
            if (self.marker is None or self.marker in data) and self.fail_left > 0:
                self.fail_left -= 1
                return 1, "", TRANSIENT
        return 0, "", ""


@pytest.fixture
def sleeps(monkeypatch):
    got = []
    monkeypatch.setattr(lifecycle.time, "sleep", got.append)
    return got


def _no_login(monkeypatch):
    monkeypatch.setattr(lifecycle.urllib.request, "urlopen",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("no authn")))


def test_user_batch_survives_one_transient_failure(monkeypatch, sleeps):
    # scaleuser-0301 opens batch 301-350, the batch run 9 attempt 3 lost.
    k = FlakyKubectl(marker="name: scaleuser-0301\n", fail_times=1)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    _no_login(monkeypatch)
    n, _ = lifecycle.create_synthetic_users(1, 400, login=False)
    assert n == 400
    batch = [d for d in k.applies if "name: scaleuser-0301\n" in d]
    assert len(batch) == 2 and batch[0] == batch[1], "must re-apply the SAME YAML"
    assert sleeps == [5]
    assert len(k.applies) == 8 + 1  # 8 batches of 50, one retried


def test_user_batch_failing_every_attempt_is_not_counted(monkeypatch, sleeps):
    k = FlakyKubectl(marker="name: scaleuser-0301\n", fail_times=99)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    _no_login(monkeypatch)
    n, _ = lifecycle.create_synthetic_users(1, 400, login=False)
    assert n == 350
    assert sleeps == [5, 10, 20]
    assert sum("name: scaleuser-0301\n" in d for d in k.applies) == 4


def test_user_batch_three_failures_then_success_is_counted(monkeypatch, sleeps):
    k = FlakyKubectl(marker="name: scaleuser-0301\n", fail_times=3)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    _no_login(monkeypatch)
    n, _ = lifecycle.create_synthetic_users(1, 400, login=False)
    assert n == 400
    assert sleeps == [5, 10, 20]


def test_composition_batch_survives_one_transient_failure(monkeypatch, sleeps):
    k = FlakyKubectl(marker="name: bench-app-03-01\n", fail_times=1)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    assert lifecycle.deploy_compositions_parallel(1, 5, 3, workers=2) == []
    assert sleeps == [5]
    assert len(k.applies) == 5 + 1


def test_composition_batch_failing_every_attempt_is_returned(monkeypatch, sleeps):
    k = FlakyKubectl(marker="name: bench-app-03-01\n", fail_times=99)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    failed = lifecycle.deploy_compositions_parallel(1, 5, 3, workers=2)
    assert failed == ["bench-ns-03[1-3]"]
    assert sleeps == [5, 10, 20]


def test_namespace_chunk_retries_and_reports(monkeypatch, sleeps):
    k = FlakyKubectl(fail_times=1)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    assert lifecycle.create_bench_namespaces(1, 3) == []
    assert sleeps == [5]
    k = FlakyKubectl(fail_times=99)
    monkeypatch.setattr(lifecycle, "kubectl", k)
    assert lifecycle.create_bench_namespaces(1, 3) == ["bench-ns-01..03"]


def test_data606_fails_the_step_on_a_composition_batch_lost_after_retries(
        monkeypatch, sleeps):
    """Before this fix a lost composition batch only logged a WARNING and the
    run went on with a silent shortfall."""
    monkeypatch.setattr(lifecycle, "kubectl", FlakyKubectl(fail_times=0))
    monkeypatch.setattr(data606.cluster, "kubectl", FlakyKubectl(fail_times=0))
    monkeypatch.setattr(lifecycle, "wait_for_bench_namespaces", lambda n, timeout: True)
    monkeypatch.setattr(data606.cluster, "deploy_compositiondefinition", lambda ns: True)
    monkeypatch.setattr(data606, "hold_composition_controller", lambda ns: None)
    monkeypatch.setattr(data606, "assert_no_external_children", lambda: {})
    monkeypatch.setattr(data606.time, "sleep", lambda s: None)
    calls = []

    def deploy(ns_start, ns_end, per_ns):
        calls.append((ns_start, ns_end, per_ns))
        return [] if len(calls) == 1 else ["bench-ns-07[1-200]"]

    monkeypatch.setattr(lifecycle, "deploy_compositions_parallel", deploy)
    with pytest.raises(RuntimeError, match="composition batches failed after retries"):
        data606.generate(scale=100, namespaces=10, users=5, probe_batch=1,
                         hold_probe_wait_s=0, login=False)
    assert calls == [(1, 1, 1), (1, 10, 10)]
