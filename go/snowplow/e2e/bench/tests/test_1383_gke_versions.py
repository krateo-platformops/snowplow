"""#1383 gap 7: gke-up asks GKE for a minor (1.36) and GKE picks the patch, per cluster; the run's shape record
did not say which, so a pair could compare two platforms unknowingly. observed_shape now records the control
plane's and the nodes' versions, and the paired verdict is INVALID unless both clusters run the same ones.
"""

from __future__ import annotations

import copy

from bench import custlat, gke

V = "1.36.4-gke.1391002"
DESCRIBE = {"name": "snowplow-bench-r1383", "location": "europe-west4", "locations": ["europe-west4-a"],
            "currentNodeCount": 5, "currentMasterVersion": V, "currentNodeVersion": V,
            "nodePools": [{"name": n, "config": {"machineType": "n2-standard-8"}, "initialNodeCount": 1,
                           "locations": ["europe-west4-a"], "version": V} for n in ("default-pool", "snowplow")]}


def test_the_shape_record_carries_the_versions_gke_gave():
    rec = gke.observed_shape(DESCRIBE)
    assert rec["master_version"] == V and rec["node_version"] == V
    assert [p["version"] for p in rec["node_pools"]] == [V, V]
    assert gke.shape_versions(rec) == {"master": V, "nodes": [V]}


def test_versions_not_recorded_are_unknown_never_equal():
    assert gke.shape_versions(None) is None
    assert gke.shape_versions(gke.cluster_shape(gke.default_spec("snowplow-bench-r1383"))) is None   # a dry run
    old = gke.observed_shape({k: v for k, v in DESCRIBE.items() if k != "currentMasterVersion"})
    assert gke.shape_versions(old) is None
    half = copy.deepcopy(DESCRIBE)
    half["nodePools"][1].pop("version")
    assert gke.shape_versions(gke.observed_shape(half)) is None


def _verdict(dig: str, describe: dict | None) -> dict:
    return {"verdict": "FAIL", "report": {
        "image_digest": {"expected": dig},
        "cluster_shape": gke.observed_shape(describe) if describe else None,
        "customer_latency": {"gate_phases": {}, "phases": {}, "k6_span": [0, 100], "schedule": {},
                             "windows": [], "readiness": {"excluded": [], "restarts": {
                                 "restarted": False, "count": 0, "in_samples": 0, "unlocated": []}},
                             "k6_errors": {"requests": 1000, "failed": 0, "rate": 0.0}}}}


def _gv(p):
    return next(c for c in p["validity"] if c["check"] == "customer_latency.pair.gke_versions")


DA, DB = "sha256:" + "a" * 64, "sha256:" + "b" * 64


def test_the_pair_compares_both_clusters_versions():
    same = custlat.pair(_verdict(DA, DESCRIBE), _verdict(DB, DESCRIBE))
    assert _gv(same)["result"] == "PASS" and not any("gke_versions" in r for r in same["invalid_reasons"])
    other = copy.deepcopy(DESCRIBE)
    other["currentMasterVersion"] = "1.36.5-gke.1000"
    p = custlat.pair(_verdict(DA, DESCRIBE), _verdict(DB, other))
    assert _gv(p)["result"] == "FAIL" and p["verdict"] == "INVALID"
    assert _gv(p)["value"] == {"a": {"master": V, "nodes": [V]}, "b": {"master": "1.36.5-gke.1000", "nodes": [V]}}
    nodes = copy.deepcopy(DESCRIBE)
    nodes["nodePools"][0]["version"] = "1.36.3-gke.1"
    assert custlat.pair(_verdict(DA, DESCRIBE), _verdict(DB, nodes))["verdict"] == "INVALID"


def test_a_pair_whose_versions_are_unrecorded_is_invalid():
    p = custlat.pair(_verdict(DA, DESCRIBE), _verdict(DB, None))
    assert _gv(p)["result"] == "FAIL" and p["verdict"] == "INVALID"
    assert any(r.startswith("customer_latency.pair.gke_versions:") for r in p["invalid_reasons"])
