"""Bench clusters get a REGIONAL control plane (Diego, run 16 attempt 3).

Attempt 2: B's zonal control plane went unreachable for 5+ minutes at
07:10:07Z, from inside the cluster too ("No valid leases found", TLS handshake
timeouts), with no GKE operation logged. A regional cluster runs 3
control-plane replicas. The nodes do not move: --node-locations keeps every
pool in europe-west4-a, and --num-nodes is per node location, so with the
one zone each pool keeps its count (no extra node, same CPU / instance
quota). Every gcloud call on the cluster names the region; the kube context
is gke_<project>_europe-west4_<name>; the run's record says
"control plane: regional" (runs 14-16 have none: zonal).
Fakes only: no cluster is created.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import gke
from tests.test_run606_real_sequence import BENCH_DIR, KCTX, RUN_NAME, _calls, harness  # noqa: F401

NAME = "snowplow-bench-r17x"
SPEC = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16")


def _flag(argv, f):
    return argv[argv.index(f) + 1] if f in argv else None


def test_the_spec_keeps_its_zone_for_the_nodes_and_derives_the_region():
    assert (SPEC.zone, SPEC.region) == ("europe-west4-a", "europe-west4")
    assert gke.kube_context(SPEC) == f"gke_operations-dev-krateo-io_europe-west4_{NAME}"
    assert gke.region_of("europe-west4") == "europe-west4"


def test_every_cluster_command_names_the_region_and_nodes_stay_in_one_zone():
    cmds = gke.create_commands(SPEC)
    for argv in cmds + [gke.delete_command(SPEC), gke.describe_command(SPEC),
                        gke.operation_wait_command("operation-1", SPEC)]:
        assert _flag(argv, "--region") == "europe-west4" and "--zone" not in argv, argv
    create, *pools, creds = cmds
    assert create[:5] == ["gcloud", "container", "clusters", "create", NAME]
    assert creds[:4] == ["gcloud", "container", "clusters", "get-credentials"]
    for argv in [create, *pools]:
        assert _flag(argv, "--node-locations") == "europe-west4-a", argv     # ONE zone, every pool
    # same machine types, same node counts as the zonal clusters of runs 14-16
    got = {"default-pool": (_flag(create, "--machine-type"), _flag(create, "--num-nodes"))}
    got.update({p[4]: (_flag(p, "--machine-type"), _flag(p, "--num-nodes")) for p in pools})
    assert got == {"default-pool": ("n2-standard-8", "3"), "snowplow": ("n2-standard-8", "1"),
                   "loadgen": ("n2-standard-16", "1")}


def test_the_self_destruct_cronjob_deletes_the_regional_cluster():
    m = gke.self_destruct_manifest(SPEC)
    assert f"gcloud container clusters describe {NAME} --project operations-dev-krateo-io --region europe-west4" in m
    assert f"gcloud container clusters delete {NAME} --project operations-dev-krateo-io --region europe-west4" in m
    assert "--zone" not in m


def test_reap_names_each_clusters_own_location():
    """A regional bench cluster by its region; a zonal one made before run 17
    by its zone; nothing that is not a bench cluster."""
    base = dict(gke.BENCH_LABELS, **{"ttl-expires": "1", "owner": "t"})
    clusters = [
        {"name": "snowplow-bench-r17a", "location": "europe-west4", "zone": "europe-west4", "resourceLabels": base},
        {"name": "snowplow-bench-r16a", "location": "europe-west4-a", "zone": "europe-west4-a",
         "resourceLabels": base},
        {"name": "krateo-installer-release", "location": "europe-west4", "resourceLabels": base},
        {"name": "snowplow-bench-alive", "location": "europe-west4",
         "resourceLabels": dict(base, **{"ttl-expires": "9999999999"})},
    ]
    cmds = {c[4]: c for c in gke.reap_commands(clusters, now=1000)}
    assert set(cmds) == {"snowplow-bench-r17a", "snowplow-bench-r16a"}
    assert _flag(cmds["snowplow-bench-r17a"], "--region") == "europe-west4"
    assert _flag(cmds["snowplow-bench-r16a"], "--zone") == "europe-west4-a"


def test_gke_up_dry_run_says_regional(capsys):
    assert gke.main(["gke-up", "--name", NAME]) == 0
    out = capsys.readouterr().out
    assert "control plane: regional (europe-west4); every node in europe-west4-a" in out
    assert f"context=gke_operations-dev-krateo-io_europe-west4_{NAME}" in out
    assert "--region europe-west4 --node-locations europe-west4-a" in out


def test_the_shape_record():
    rec = gke.cluster_shape(SPEC)
    assert rec["kind"] == "cluster_shape" and rec["control_plane"] == "regional"
    assert rec["region"] == "europe-west4" and rec["node_locations"] == ["europe-west4-a"]
    assert [p["nodes"] for p in rec["node_pools"]] == [3, 1, 1]


def test_run606_drives_the_regional_cluster_and_records_its_shape(harness):  # noqa: F811
    """run606 against the fakes: gke-up / gke-down get the node zone (the
    region is derived), every kubectl / helm call pins the regional context,
    and the verdict carries the shape record."""
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=600)
    assert p.returncode == 0, p.stderr[-2000:]
    assert KCTX == f"gke_operations-dev-krateo-io_europe-west4_{RUN_NAME}"
    calls = _calls(state)
    assert all(c["context"] in (KCTX, None) for c in calls if c["tool"] in ("kubectl", "helm"))
    gcl = [c["argv"] for c in calls if c["tool"] == "gcloud"]
    on_cluster = [a for a in gcl if a[:2] == ["container", "clusters"] and a[2] != "list"]
    assert on_cluster and all(_flag(a, "--region") == "europe-west4" and "--zone" not in a for a in on_cluster)
    log = (tmp / "out" / "run.log").read_text()
    assert f"control plane: regional (europe-west4); every node in europe-west4-a; context {KCTX}" in log
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    assert v["report"]["cluster_shape"]["control_plane"] == "regional"


@pytest.mark.parametrize("old", ["gke_operations-dev-krateo-io_europe-west4-a_"])
def test_no_zonal_context_is_built_anywhere(old):
    src = (BENCH_DIR / "run606.sh").read_text()
    assert 'KCTX="gke_${PROJECT}_${REGION}_${RUN_NAME}"' in src and 'REGION="${ZONE%-*}"' in src
    assert old not in gke.kube_context(SPEC)
