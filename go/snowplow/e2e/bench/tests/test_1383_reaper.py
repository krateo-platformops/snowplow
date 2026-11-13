"""#1383 gap 5: gke-reap (the first step of every run, and the layer after the TTL CronJob) listed with a label
--filter, ignored each delete's exit code and never checked a cluster went. Now: one unfiltered list that fails
closed (gcloud container clusters list answers rc 0 when locations did not respond, and only warns: the
compute partial-error property does not govern it), each delete's rc read, each delete verified by its exact-name
describe answering NotFound.
"""

from __future__ import annotations

import json

import pytest

from bench import gke
from tests.test_gke_pvc_disks import NAME, _calls, fakes  # noqa: F401  (fixture)

BASE = dict(gke.BENCH_LABELS, owner="t")


def _cluster(name, ttl, location="europe-west4", **labels):
    return {"name": name, "location": location, "status": "RUNNING",
            "resourceLabels": dict(BASE, **{"ttl-expires": str(ttl)}, **labels)}


EXPIRED = _cluster("snowplow-bench-r1383-old", 1000)
CLUSTERS = [EXPIRED,
            _cluster("snowplow-bench-r1383-alive", 9999999999),
            _cluster("krateo-057", 1000),                                     # labels copied onto 057: never
            _cluster("snowplow-bench-unlabelled", 1000, purpose="other")]


@pytest.fixture
def reapfakes(fakes, monkeypatch):  # noqa: F811
    monkeypatch.setenv("FAKE_CLUSTERS", json.dumps(CLUSTERS))
    monkeypatch.setattr(gke, "_POLL_S", 0.01)
    return fakes


def _deletes(tmp):
    return [c[4] for c in _calls(tmp) if c[1:4] == ["container", "clusters", "delete"]]


def test_the_reaper_deletes_each_expired_bench_cluster_and_verifies_it_gone(reapfakes, capsys):
    assert gke.main(["gke-reap", "--yes"]) == 0
    assert _deletes(reapfakes) == [EXPIRED["name"]]
    c = _calls(reapfakes)
    lst = next(x for x in c if x[1:4] == ["container", "clusters", "list"])
    assert "--filter" not in lst                                          # no client-side label filter
    i = next(i for i, x in enumerate(c) if x[1:4] == ["container", "clusters", "delete"])
    assert any(x[1:5] == ["container", "clusters", "describe", EXPIRED["name"]] for x in c[i + 1:])
    assert f"reaped: {EXPIRED['name']} (past its TTL): deleted, its describe answers NotFound" in capsys.readouterr().out


def test_a_list_with_locations_that_did_not_respond_fails_closed(reapfakes, monkeypatch, capsys):
    """GKE names the locations it could not read (missingZones); gcloud prints the rest, rc 0, and warns."""
    monkeypatch.setenv("FAKE_CLUSTERS", "[]")
    monkeypatch.setenv("FAKE_MISSING_ZONES", "europe-west4-a, europe-west4")
    assert gke.main(["gke-reap", "--yes"]) == 1
    err = capsys.readouterr().err
    assert "did not respond" in err and "whether any bench cluster is past its TTL is unknown" in err
    assert "no expired bench clusters" not in err


def test_a_refused_delete_fails_the_reaper(reapfakes, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_CLUSTER_DELETE_RC", "1")
    assert gke.main(["gke-reap", "--yes"]) == 1
    assert f"{EXPIRED['name']}: its delete failed (rc=1)" in capsys.readouterr().err


def test_a_cluster_that_never_reads_gone_is_not_reaped(reapfakes, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_NEVER_GONE", "1")
    assert gke.main(["gke-reap", "--yes", "--budget-s", "0.05"]) == 1
    err = capsys.readouterr().err
    assert f"{EXPIRED['name']}: delete requested, NOT confirmed gone" in err
    assert "reaped:" not in err


def test_a_dry_reap_deletes_nothing(reapfakes, capsys):
    assert gke.main(["gke-reap"]) == 0
    assert _deletes(reapfakes) == []
    assert f"DRY-RUN: gcloud container clusters delete {EXPIRED['name']}" in capsys.readouterr().out


def test_the_cluster_quota_check_reads_the_same_fail_closed_list(reapfakes, monkeypatch):
    """#985's count of the region's clusters is the same list: an incomplete one is an unknown count, never room."""
    monkeypatch.setenv("FAKE_CLUSTERS", "[]")                 # what answered: none, so it would read as room
    monkeypatch.setenv("FAKE_MISSING_ZONES", "europe-west4-b")
    ok, msg = gke.cluster_quota_check(gke.DEFAULT_PROJECT, gke.DEFAULT_ZONE, 1)
    assert not ok and "could not be listed" in msg


# ─── #1387 review: a delete already running is a delete in progress, not a failed one ───

OP = "operation-1794560000000-5a7d2c1e-91b3-4c2f-8e0d-6f1b2a3c4d5e"


def test_a_cluster_its_cronjob_is_already_deleting_is_reaped_once_it_reads_gone(reapfakes, monkeypatch, capsys):
    """GKE's answer to a delete while the cluster's own ttl-self-destruct CronJob (or a partner launch's reap) is
    deleting it: "Cluster is running incompatible operation operation-...", rc 1. The reaper waits for the
    describe's NotFound instead of reporting a failed delete."""
    monkeypatch.setenv("FAKE_CLUSTER_DELETE_BUSY", OP)
    assert gke.main(["gke-reap", "--yes"]) == 0
    out = capsys.readouterr()
    assert f"{EXPIRED['name']}: {OP} is already running on it (a delete in progress" in out.out
    assert f"reaped: {EXPIRED['name']} (past its TTL): deleted, its describe answers NotFound" in out.out
    assert "its delete failed" not in out.err


def test_an_operation_that_never_ends_the_cluster_is_still_not_reaped(reapfakes, monkeypatch, capsys):
    """The running operation may be no delete (an upgrade): the cluster stays, and the poll says so."""
    monkeypatch.setenv("FAKE_CLUSTER_DELETE_BUSY", OP)
    monkeypatch.setenv("FAKE_NEVER_GONE", "1")
    assert gke.main(["gke-reap", "--yes", "--budget-s", "0.05"]) == 1
    assert f"{EXPIRED['name']}: delete requested, NOT confirmed gone" in capsys.readouterr().err
