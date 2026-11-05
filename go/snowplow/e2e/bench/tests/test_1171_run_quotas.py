"""#1171: run 21 attempt 2 failed 30 min in. Cluster A's gateway LoadBalancer
stayed pending: "Quota 'IN_USE_ADDRESSES' exceeded" (limit 16 in
europe-west4). The launcher's pre-check had said "STATIC_ADDRESSES in
europe-west4: 0 of 8 used, 8 free": the wrong quota. Every node's external IP
and every LoadBalancer IP counts against IN_USE_ADDRESSES.

  1. ONE TABLE (bench/gke.py RUN_QUOTAS) of every regional quota the run
     consumes, each with a pre-check; the need of each is derived from the
     footprint of the spec gke-up creates and the install's LoadBalancer
     Services, never a constant;
  2. THE TABLE AGAINST WHAT THE SCRIPTS CREATE: every flag of the argv
     create_commands builds is classified here, the footprint is recomputed
     here from that argv, and every gcloud create in the bench's sources is
     one the footprint parses — a newly consumed resource fails a test;
  3. THE ISSUE'S ARMS: 13 of 16 in use and a pair that needs 12 is refused
     with both numbers; 4 of 16 starts; the need follows the pool sizes.

No cloud call: the fake gcloud answers the region's full 2026-10-25 quota
listing (tests/fake_region.py), steered per quota.
"""

from __future__ import annotations

import dataclasses
import json
import re
import subprocess

import pytest

from bench import gke
from tests import fake_region
from tests.test_gke_pvc_disks import NAME, _calls, _pair_calls, fakes, quota_line  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

R = "europe-west4"
REGION = fake_region.quotas({})                        # the 10-25 listing, IN_USE_ADDRESSES 0 used
LISTED = {q["metric"] for q in json.loads(fake_region.FIXTURE.read_text())["quotas"]}

# ─── 2. the table against what the scripts create ──────────────────────────

# Every flag create_commands puts on a gcloud argv, and what of it counts against a
# regional quota. A flag that is not here fails test_every_create_flag_is_classified:
# classify it (e.g. --enable-private-nodes: no node external IPs; --local-ssd-count,
# --accelerator: quotas RUN_QUOTAS has no row for) and teach the footprint before it ships.
CONSUMES = {
    "--num-nodes": "nodes: INSTANCES, IN_USE_ADDRESSES (each public node's external IP)",
    "--node-locations": "x nodes per location; one MIG, instance group and template per pool per location",
    "--machine-type": "<FAMILY>_CPUS",
    "--disk-type": "DISKS_TOTAL_GB (pd-standard) or SSD_TOTAL_GB (pd-balanced, pd-ssd)",
    "--disk-size": "the boot disk GB of each node",
}
CONSUMES_NOTHING = {"--project", "--region", "--cluster", "--cluster-version", "--release-channel", "--labels",
                    "--scopes", "--enable-ip-alias", "--logging", "--monitoring", "--enable-autorepair",
                    "--node-taints", "--node-labels", "--quiet"}


def _opt(argv, name):
    return argv[argv.index(name) + 1] if name in argv else None


def _footprint_from_argv(cmds: list[list[str]], lb_services: int) -> gke.Footprint:
    """The footprint recomputed from the gcloud argv alone (this test's own
    reading of gcloud's flags, not bench.gke's)."""
    fp = gke.Footprint(lb_services=lb_services, pvc_gb=gke.BENCH_PVC_FOOTPRINT_GB)
    for a in cmds:
        if a[1:4] == ["container", "clusters", "create"]:
            fp.clusters += 1
        if a[1:4] not in (["container", "clusters", "create"], ["container", "node-pools", "create"]):
            continue
        locs = _opt(a, "--node-locations").split(",")
        n = int(_opt(a, "--num-nodes")) * len(locs)
        mt = _opt(a, "--machine-type")
        family, vcpus = mt.split("-")[0], int(mt.split("-")[2])
        fp.nodes += n
        fp.node_external_ips += 0 if "--enable-private-nodes" in a else n
        fp.vcpus[family] = fp.vcpus.get(family, 0) + n * vcpus
        dt = _opt(a, "--disk-type")
        fp.boot_disk_gb[dt] = fp.boot_disk_gb.get(dt, 0) + n * int(_opt(a, "--disk-size"))
        fp.node_groups += len(locs)
    return fp


def _specs():
    base = gke.default_spec(NAME)
    big = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16")
    resized = dataclasses.replace(
        base, platform_pool=dataclasses.replace(base.platform_pool, count=4),
        extra_pools=(dataclasses.replace(base.extra_pools[0], count=2, disk_type="pd-balanced"),
                     base.extra_pools[1]))
    return [base, big, resized]


@pytest.mark.parametrize("spec", _specs(), ids=["default", "k6-loadgen", "resized"])
def test_every_create_flag_is_classified(spec):
    for a in gke.create_commands(spec):
        unknown = sorted({t for t in a if t.startswith("--")} - set(CONSUMES) - CONSUMES_NOTHING)
        assert not unknown, (f"{' '.join(a[:4])}: {unknown} not classified: does it consume a regional quota? "
                             f"(RUN_QUOTAS, gke.footprint)")


@pytest.mark.parametrize("spec", _specs(), ids=["default", "k6-loadgen", "resized"])
@pytest.mark.parametrize("lb", [0, 1, 4])
def test_the_footprint_is_what_the_create_argv_makes(spec, lb):
    assert gke.footprint(spec, lb_services=lb) == _footprint_from_argv(gke.create_commands(spec), lb)


def test_every_gcloud_create_in_the_bench_is_one_the_footprint_reads():
    """The bench's only quota-consuming gcloud calls are the cluster and its
    node pools (the install's LoadBalancer Services and PVC disks are the
    footprint's lb_services and pvc_gb). A new `gcloud ... create` (an
    address, a disk, a firewall rule) fails here until the table counts it."""
    py = re.compile(r'"gcloud",\s*"([a-z-]+)",\s*"([a-z-]+)",\s*"(create|reserve|insert)"')
    sh = re.compile(r"\bgcloud\s+([a-z-]+)\s+([a-z-]+)\s+(create|reserve|insert)\b")
    found = set()
    for f in [*sorted((BENCH_DIR / "bench").glob("*.py")), BENCH_DIR / "run606.sh", BENCH_DIR / "run606_pair.sh"]:
        text = f.read_text()
        found |= {(f.name, *m.groups()) for m in py.finditer(text)}
        found |= {(f.name, *m.groups()) for m in sh.finditer(text)}
    assert found == {("gke.py", "container", "clusters", "create"), ("gke.py", "container", "node-pools", "create")}
    # and those are exactly the argv create_commands builds (credentials is a read)
    made = {tuple(a[1:4]) for a in gke.create_commands(gke.default_spec(NAME))}
    assert made == {("container", "clusters", "create"), ("container", "node-pools", "create"),
                    ("container", "clusters", "get-credentials")}


def _needs(fp) -> dict[str, int]:
    return {m: n for row in gke.RUN_QUOTAS for m, n, _ in row.needs(fp)}


def test_the_table_reads_every_part_of_the_footprint_and_each_row_reads_one():
    fp = gke.footprint(gke.default_spec(NAME), lb_services=1)
    base = _needs(fp)
    read_by_some_row = set()
    rows_reading = {row.quota: False for row in gke.RUN_QUOTAS}
    for f in dataclasses.fields(gke.Footprint):
        v = getattr(fp, f.name)
        bumped = dataclasses.replace(fp, **{f.name: {k: x + 1 for k, x in v.items()} if isinstance(v, dict)
                                            else v + 1})
        for row in gke.RUN_QUOTAS:
            if [n for _, n, _ in row.needs(bumped)] != [n for _, n, _ in row.needs(fp)]:
                read_by_some_row.add(f.name)
                rows_reading[row.quota] = True
    assert read_by_some_row == {f.name for f in dataclasses.fields(gke.Footprint)}
    assert all(rows_reading.values()), rows_reading
    # every regional row names a quota the region really lists (the 10-25 reading)
    region_metrics = {m for row in gke.RUN_QUOTAS if row.source == "region" for m, _, _ in row.needs(fp)}
    assert region_metrics <= LISTED, region_metrics - LISTED
    assert set(base) == region_metrics | {gke.CLUSTER_QUOTA_ID}


def test_the_table_holds_every_quota_the_issue_names():
    quotas = {row.quota for row in gke.RUN_QUOTAS}
    assert {"SSD_TOTAL_GB", "ClustersPerRegion", "STATIC_ADDRESSES", "IN_USE_ADDRESSES", "<FAMILY>_CPUS",
            "EXTERNAL_NETWORK_LB_FORWARDING_RULES"} <= quotas


def test_a_machine_family_or_disk_type_the_table_does_not_know_is_refused():
    base = gke.default_spec(NAME)
    for pool in (dataclasses.replace(base.platform_pool, machine_type="e2-medium"),
                 dataclasses.replace(base.platform_pool, disk_type="hyperdisk-balanced")):
        with pytest.raises(ValueError):
            gke.footprint(dataclasses.replace(base, platform_pool=pool), lb_services=1)
    # a family whose quota the region does not list: unknown, never assumed to fit
    fp = gke.footprint(dataclasses.replace(base, platform_pool=dataclasses.replace(
        base.platform_pool, machine_type="zz9-standard-8")), lb_services=1)
    _, refusals = gke.region_fit(REGION, fp)
    assert "ZZ9_CPUS not in the region's quotas: whether the run fits is unknown" in refusals


def test_a_footprint_file_must_be_one():
    fp = gke.footprint(gke.default_spec(NAME), lb_services=2)
    assert gke.Footprint.from_json(json.loads(json.dumps(fp.to_json()))) == fp
    for bad in ({}, {**fp.to_json(), "nodes": -1}, {**fp.to_json(), "nodes": "5"},
                {k: v for k, v in fp.to_json().items() if k != "lb_services"}, {**fp.to_json(), "extra": 1}):
        with pytest.raises(ValueError):
            gke.Footprint.from_json(bad)


# ─── 3. the issue's arms ────────────────────────────────────────────────────

def _region(**usage) -> dict:
    return fake_region.quotas({f"FAKE_USAGE_{m}": str(v) for m, v in usage.items()})


def _pair(lb=1) -> gke.Footprint:
    spec = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16")
    return gke.footprint(spec, lb_services=lb) + gke.footprint(spec, lb_services=lb)


def test_a_pair_needs_twelve_in_use_addresses_its_nodes_and_load_balancers():
    """Run 21's pair: 2 x (5 nodes + 1 LoadBalancer)."""
    ok, msg = quota_line(_region(IN_USE_ADDRESSES=13), _pair(), "IN_USE_ADDRESSES")
    assert not ok and msg.startswith("IN_USE_ADDRESSES in europe-west4: 13 of 16 used, 3 free; the run needs 12: "
                                     "10 node external IP(s) + 2 LoadBalancer IP(s). Every node's external IP")
    ok, msg = quota_line(_region(IN_USE_ADDRESSES=4), _pair(), "IN_USE_ADDRESSES")
    assert ok and "4 of 16 used, 12 free; the run needs 12" in msg


def test_the_need_follows_the_node_pool_sizes():
    base = gke.default_spec(NAME)
    n = _needs(gke.footprint(base, lb_services=1))
    assert (n["IN_USE_ADDRESSES"], n["INSTANCES"], n["N2_CPUS"], n["DISKS_TOTAL_GB"]) == (6, 5, 40, 500)
    bigger = dataclasses.replace(base, platform_pool=dataclasses.replace(base.platform_pool, count=4))
    n = _needs(gke.footprint(bigger, lb_services=1))
    assert (n["IN_USE_ADDRESSES"], n["INSTANCES"], n["N2_CPUS"], n["DISKS_TOTAL_GB"]) == (7, 6, 48, 600)
    smaller = dataclasses.replace(base, extra_pools=base.extra_pools[:1])
    n = _needs(gke.footprint(smaller, lb_services=1))
    assert (n["IN_USE_ADDRESSES"], n["INSTANCE_GROUP_MANAGERS"]) == (5, 2)
    # 12 free: the default pair fits, one more platform node in each cluster does not
    region = _region(IN_USE_ADDRESSES=4)
    assert quota_line(region, _pair(), "IN_USE_ADDRESSES")[0]
    spec = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16")
    spec = dataclasses.replace(spec, platform_pool=dataclasses.replace(spec.platform_pool, count=4))
    ok, msg = quota_line(region, gke.footprint(spec, lb_services=1) + gke.footprint(spec, lb_services=1),
                         "IN_USE_ADDRESSES")
    assert not ok and "12 free; the run needs 14: 12 node external IP(s) + 2 LoadBalancer IP(s)" in msg


def test_the_loadgen_machine_type_moves_the_cpu_need_through_the_cli(tmp_path, capsys):
    for mt, want in (("n2-standard-8", 40), ("n2-standard-16", 48)):
        out = tmp_path / f"{mt}.json"
        assert gke.main(["gke-footprint", "--name", NAME, "--loadgen-machine-type", mt, "--address-need", "1",
                         "--out", str(out)]) == 0
        assert json.loads(out.read_text())["vcpus"] == {"n2": want}
    assert "footprint: IN_USE_ADDRESSES: the run needs 6: 5 node external IP(s) + 1 LoadBalancer IP(s)" \
        in capsys.readouterr().err


def test_gke_up_refuses_a_run_whose_nodes_do_not_fit_in_use_addresses(fakes, capsys):  # noqa: F811
    q = json.loads((fakes / "quotas.json").read_text())
    next(x for x in q["quotas"] if x["metric"] == "IN_USE_ADDRESSES")["usage"] = 11.0
    (fakes / "quotas.json").write_text(json.dumps(q))
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    err = capsys.readouterr().err
    assert ("refusing: IN_USE_ADDRESSES in europe-west4: 11 of 16 used, 5 free; the run needs 6: 5 node external "
            "IP(s) + 1 LoadBalancer IP(s)") in err and "nothing was created" in err
    assert not any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))
    next(x for x in q["quotas"] if x["metric"] == "IN_USE_ADDRESSES")["usage"] = 10.0
    (fakes / "quotas.json").write_text(json.dumps(q))
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 0
    assert "IN_USE_ADDRESSES in europe-west4: 10 of 16 used, 6 free; the run needs 6" in capsys.readouterr().out
    assert any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))


def test_the_pair_refuses_at_13_of_16_and_starts_at_4(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, FAKE_USAGE_IN_USE_ADDRESSES="13")
    assert p.returncode == 2, p.stderr[-1500:]
    assert ("refusing: IN_USE_ADDRESSES in europe-west4: 13 of 16 used, 3 free; the run needs 12: 10 node external "
            "IP(s) + 2 LoadBalancer IP(s)") in p.stderr
    assert "refusing: refusing:" not in p.stderr
    assert not [c for c in _pair_calls(fake) if " dry=0 " in c]          # neither run started
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_USAGE_IN_USE_ADDRESSES="4")
    assert p.returncode == 0, p.stderr[-1500:]
    assert "IN_USE_ADDRESSES in europe-west4: 4 of 16 used, 12 free; the run needs 12" in p.stderr
    assert "DRY: both runs would start now" in p.stderr


def test_the_pair_names_every_quota_that_does_not_fit(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_USAGE_IN_USE_ADDRESSES="13", FAKE_USAGE_INSTANCES="20")
    assert p.returncode == 2
    lines = [ln for ln in p.stderr.splitlines() if ln.startswith("refusing: ")]
    assert any(ln.startswith("refusing: IN_USE_ADDRESSES in europe-west4: 13 of 16 used") for ln in lines)
    assert any(ln.startswith("refusing: INSTANCES in europe-west4: 20 of 24 used, 4 free; the run needs 10")
               for ln in lines)


def test_run606_refuses_before_any_cluster_when_its_nodes_do_not_fit(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, FAKE_USAGE_IN_USE_ADDRESSES="13"),
                       capture_output=True, text=True, timeout=600)
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    line = next(ln for ln in log.splitlines() if "up FAILED: gke-up exited 2" in ln)
    assert "IN_USE_ADDRESSES in europe-west4: 13 of 16 used, 3 free; the run needs 6" in line
    fp = json.loads((tmp / "out" / "footprint.json").read_text())       # derived before gke-up, kept
    assert (fp["nodes"], fp["node_external_ips"], fp["lb_services"]) == (5, 5, 1)
    calls = [json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines()]
    assert not any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "create"] for c in calls)


def test_run606_derives_the_footprint_with_gke_ups_own_flags():
    """gke-footprint and gke-up get the same spec flags (the parsers share
    them: footprint_args), so the footprint is the cluster gke-up creates."""
    text = (BENCH_DIR / "run606.sh").read_text()

    def flags(cmd):
        m = re.search(r"python3 -m bench\.gke " + cmd + r" (.*?)(?:\|\||2>)", text, re.S)
        toks = m.group(1).replace("\\\n", " ").split()
        return {t: toks[i + 1] for i, t in enumerate(toks) if t.startswith("--") and i + 1 < len(toks)
                and t in ("--name", "--project", "--zone", "--loadgen-machine-type", "--address-need")}

    up, fp = flags("gke-up"), flags("gke-footprint")
    assert up == fp and set(up) == {"--name", "--project", "--zone", "--loadgen-machine-type", "--address-need"}
