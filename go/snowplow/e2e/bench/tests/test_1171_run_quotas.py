"""#1171: run 21 attempt 2 failed 30 min in. Cluster A's gateway LoadBalancer
stayed pending: "Quota 'IN_USE_ADDRESSES' exceeded" (limit 16 in
europe-west4). The launcher's pre-check had said "STATIC_ADDRESSES in
europe-west4: 0 of 8 used, 8 free": the wrong quota. Every node's external IP
and every LoadBalancer IP counts against IN_USE_ADDRESSES.

#1173: and the usage `regions describe` reports for IN_USE_ADDRESSES leaves
the forwarding rules' IPs out (three readings), so what is used is counted
from what holds the region's addresses: VMs, external forwarding rules,
addresses held by neither.

  1. ONE TABLE (bench/gke.py RUN_QUOTAS) of every quota the run consumes,
     each with a pre-check; the need of each is derived from the footprint of
     the spec gke-up creates and the install's LoadBalancer Services, never a
     constant;
  2. THE TABLE AGAINST WHAT THE SCRIPTS CREATE: every flag of the argv
     create_commands builds is classified here, the footprint is recomputed
     here from that argv, and every gcloud create in the bench's sources is
     one the footprint parses: a newly consumed resource fails a test;
  3. THE ISSUES' ARMS: attempt 2's region (3 node IPs + 7 forwarding rules,
     reported 3) and a pair that needs 12 is refused; 13 of 16 refused, 4 of 16
     starts; the need follows the pool sizes and the GKE version.

No cloud call: the fake gcloud plays the live API (tests/fake_region.py): the
region's full 2026-10-25 quota listing, its resources as stated by each arm,
and IN_USE_ADDRESSES reported as the live API reports it (VM IPs only).
"""

from __future__ import annotations

import dataclasses
import json
import re
import subprocess

import pytest

from bench import gke
from tests import fake_region
from tests.fake_region import address, forwarding_rule, region_env, vm
from tests.test_gke_pvc_disks import NAME, _calls, _pair_calls, fakes, quota_line  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

R = "europe-west4"
LISTED = {q["metric"] for q in json.loads(fake_region.FIXTURE.read_text())["quotas"]}

# ─── 2. the table against what the scripts create ──────────────────────────

# Every flag create_commands puts on a gcloud argv, and what of it counts against a
# quota. A flag that is not here fails test_every_create_flag_is_classified:
# classify it (e.g. --enable-private-nodes: no node external IPs; --local-ssd-count,
# --accelerator: quotas RUN_QUOTAS has no row for) and teach the footprint before it ships.
CONSUMES = {
    "--num-nodes": "nodes: INSTANCES, IN_USE_ADDRESSES (each public node's external IP)",
    "--node-locations": "x nodes per location; one MIG, instance group and template per pool per location; "
                        "from GKE 1.37 one NEG per LoadBalancer Service per location",
    "--machine-type": "<FAMILY>_CPUS, CPUS_ALL_REGIONS",
    "--disk-type": "DISKS_TOTAL_GB (pd-standard) or SSD_TOTAL_GB (pd-balanced, pd-ssd)",
    "--disk-size": "the boot disk GB of each node",
    "--cluster-version": "the LoadBalancer kind: from 1.37 a regional backend service and GCE_VM_IP NEGs per Service",
}
CONSUMES_NOTHING = {"--project", "--region", "--cluster", "--release-channel", "--labels", "--scopes",
                    "--enable-ip-alias", "--logging", "--monitoring", "--enable-autorepair", "--node-taints",
                    "--node-labels", "--quiet"}


def _opt(argv, name):
    return argv[argv.index(name) + 1] if name in argv else None


def _footprint_from_argv(cmds: list[list[str]], lb_services: int) -> gke.Footprint:
    """The footprint recomputed from the gcloud argv alone (this test's own
    reading of gcloud's flags and of GKE's docs, not bench.gke's)."""
    fp = gke.Footprint(lb_services=lb_services, pvc_gb=gke.BENCH_PVC_FOOTPRINT_GB)
    for a in cmds:
        if a[1:4] not in (["container", "clusters", "create"], ["container", "node-pools", "create"]):
            continue
        locs = _opt(a, "--node-locations").split(",")
        if a[1:4] == ["container", "clusters", "create"]:
            fp.clusters += 1
            major, minor = (int(x) for x in _opt(a, "--cluster-version").split(".")[:2])
            if (major, minor) >= (1, 37):
                fp.lb_backend_services += lb_services
                fp.lb_negs += lb_services * len(locs)
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
    newer = gke.default_spec(NAME, cluster_version="1.37")
    return [base, big, resized, newer]


SPEC_IDS = ["default", "k6-loadgen", "resized", "gke-1.37"]


@pytest.mark.parametrize("spec", _specs(), ids=SPEC_IDS)
def test_every_create_flag_is_classified(spec):
    for a in gke.create_commands(spec):
        unknown = sorted({t for t in a if t.startswith("--")} - set(CONSUMES) - CONSUMES_NOTHING)
        assert not unknown, (f"{' '.join(a[:4])}: {unknown} not classified: does it consume a quota? "
                             f"(RUN_QUOTAS, gke.footprint)")


@pytest.mark.parametrize("spec", _specs(), ids=SPEC_IDS)
@pytest.mark.parametrize("lb", [0, 1, 4])
def test_the_footprint_is_what_the_create_argv_makes(spec, lb):
    assert gke.footprint(spec, lb_services=lb) == _footprint_from_argv(gke.create_commands(spec), lb)


def test_every_gcloud_create_in_the_bench_is_one_the_footprint_reads():
    """The bench's only quota-consuming gcloud calls are the cluster and its
    node pools (the install's LoadBalancer Services and PVC disks are the
    footprint's lb_* and pvc_gb). A new `gcloud ... create` (an address, a
    disk, a firewall rule) fails here until the table counts it."""
    py = re.compile(r'"gcloud",\s*"([a-z-]+)",\s*"([a-z-]+)",\s*"(create|reserve|insert)"')
    sh = re.compile(r"\bgcloud\s+([a-z-]+)\s+([a-z-]+)\s+(create|reserve|insert)\b")
    found = set()
    for f in [*sorted((BENCH_DIR / "bench").glob("*.py")), BENCH_DIR / "run606.sh", BENCH_DIR / "run606_pair.sh"]:
        text = f.read_text()
        found |= {(f.name, *m.groups()) for m in py.finditer(text)}
        found |= {(f.name, *m.groups()) for m in sh.finditer(text)}
    # bench/vm606.py's plan-iam PRINTS the orchestrator SA's create for a human (never run; an SA uses no
    # regional quota). Its VM (`compute instances create`, an argv without "gcloud" beside it) exists
    # before the launch, so the run's pre-check reads its share as usage, never as footprint.
    assert found == {("gke.py", "container", "clusters", "create"), ("gke.py", "container", "node-pools", "create"),
                     ("vm606.py", "iam", "service-accounts", "create")}
    # and those are exactly the argv create_commands builds (credentials is a read)
    made = {tuple(a[1:4]) for a in gke.create_commands(gke.default_spec(NAME))}
    assert made == {("container", "clusters", "create"), ("container", "node-pools", "create"),
                    ("container", "clusters", "get-credentials")}


def _needs(fp) -> dict[str, int]:
    return {m: n for row in gke.RUN_QUOTAS for m, n, _ in row.needs(fp)}


def test_the_table_reads_every_part_of_the_footprint_and_each_row_reads_one():
    fp = gke.footprint(gke.default_spec(NAME, cluster_version="1.37"), lb_services=1)
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
    assert set(base) == region_metrics | {gke.CLUSTER_QUOTA_ID, "CPUS_ALL_REGIONS", "FIREWALLS"}


def test_the_table_holds_every_quota_the_issues_name():
    quotas = {row.quota for row in gke.RUN_QUOTAS}
    assert {"SSD_TOTAL_GB", "ClustersPerRegion", "STATIC_ADDRESSES", "IN_USE_ADDRESSES", "<FAMILY>_CPUS",
            "EXTERNAL_NETWORK_LB_FORWARDING_RULES", "REGIONAL_EXTERNAL_NETWORK_LB_BACKEND_SERVICES",
            "NETWORK_ENDPOINT_GROUPS", "CPUS_ALL_REGIONS", "FIREWALLS"} <= quotas


def test_a_machine_family_disk_type_or_version_the_table_does_not_know_is_refused():
    base = gke.default_spec(NAME)
    for spec in (dataclasses.replace(base, platform_pool=dataclasses.replace(base.platform_pool,
                                                                               machine_type="e2-medium")),
                 dataclasses.replace(base, platform_pool=dataclasses.replace(base.platform_pool,
                                                                               disk_type="hyperdisk-balanced")),
                 dataclasses.replace(base, cluster_version="latest")):
        with pytest.raises(ValueError):
            gke.footprint(spec, lb_services=1)
    # a family whose quota the region does not list: unknown, never assumed to fit
    fp = gke.footprint(dataclasses.replace(base, platform_pool=dataclasses.replace(
        base.platform_pool, machine_type="zz9-standard-8")), lb_services=1)
    ok, msg = quota_line(fake_region.quotas({}), fp, "ZZ9_CPUS")
    assert not ok and msg == "ZZ9_CPUS not in the quotas of europe-west4: whether the run fits is unknown"


def test_a_footprint_file_must_be_one():
    fp = gke.footprint(gke.default_spec(NAME), lb_services=2)
    assert gke.Footprint.from_json(json.loads(json.dumps(fp.to_json()))) == fp
    for bad in ({}, {**fp.to_json(), "nodes": -1}, {**fp.to_json(), "nodes": "5"},
                {k: v for k, v in fp.to_json().items() if k != "lb_services"}, {**fp.to_json(), "extra": 1}):
        with pytest.raises(ValueError):
            gke.Footprint.from_json(bad)


# ─── 3. IN_USE_ADDRESSES: what is used, counted from what holds the addresses (#1173) ─

def _pair(lb=1, version=gke.DEFAULT_CLUSTER_VERSION) -> gke.Footprint:
    spec = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16", cluster_version=version)
    return gke.footprint(spec, lb_services=lb) + gke.footprint(spec, lb_services=lb)


def _in_use(env: dict, fp=None) -> tuple[bool, str]:
    return quota_line(fake_region.quotas(env), fp or _pair(), "IN_USE_ADDRESSES", env)


def test_run21_attempt2s_region_is_refused_though_it_reports_3_used():
    """#1171's measurement: before the pair the region held krateo-installer-
    release, 3 node IPs and 7 external forwarding rules; GCE reported 3 used."""
    env = region_env(vms=3, forwarding_rules=7)
    reported = next(q for q in fake_region.quotas(env)["quotas"] if q["metric"] == "IN_USE_ADDRESSES")
    assert reported["usage"] == 3                               # what the live API reports
    ok, msg = _in_use(env)
    assert not ok, msg
    assert msg.startswith("IN_USE_ADDRESSES in europe-west4: 10 of 16 used (listed: 3 VM external IP(s) + 7 "
                          "forwarding-rule use(s) + 0 address(es) held by neither = 10; GCE reports 3), 6 free; "
                          "the run needs 12: 10 node external IP(s) + 2 LoadBalancer IP(s)")


def test_13_of_16_refuses_a_pair_that_needs_12_and_4_of_16_starts_it():
    ok, msg = _in_use(region_env(vms=13))
    assert not ok and "13 of 16 used (listed: 13 VM external IP(s) + 0 forwarding-rule use(s) + 0 address(es) " \
                      "held by neither = 13; GCE reports 13), 3 free; the run needs 12" in msg
    ok, msg = _in_use(region_env(vms=4))
    assert ok and "4 of 16 used" in msg and "12 free; the run needs 12" in msg
    ok, msg = _in_use(region_env(vms=2, forwarding_rules=2))
    assert ok and "4 of 16 used (listed: 2 VM external IP(s) + 2 forwarding-rule use(s)" in msg


def test_every_holder_counts_once_and_only_the_regions_external_ones():
    holders = {
        "FAKE_REGION_INSTANCES": json.dumps([vm("two-nics", external_ips=2), vm("elsewhere", zone="europe-west3-a"),
                                             {**vm("internal-only"), "networkInterfaces": [{"networkIP": "10.0.0.9"}]}]),
        "FAKE_REGION_FORWARDING_RULES": json.dumps([forwarding_rule("ext"), forwarding_rule("ext-managed",
                                                                                             scheme="EXTERNAL_MANAGED"),
                                                    forwarding_rule("int", scheme="INTERNAL"),
                                                    forwarding_rule("west3", region="europe-west3")]),
        "FAKE_REGION_ADDRESSES": json.dumps([
            address("reserved"),                                                      # unattached: counts
            address("router", users=[f"{fake_region.API}/regions/{R}/routers/nat"]),  # held by neither: counts
            address("by-vm", users=[f"{fake_region.API}/zones/{R}-a/instances/two-nics"]),         # the VM's
            address("by-fr", users=[f"{fake_region.API}/regions/{R}/forwardingRules/ext"]),        # the rule's
            address("internal", address_type="INTERNAL"), address("west3", region="europe-west3")]),
    }
    used, why = gke.in_use_addresses({k: fake_region.region_list(k, holders) for k in fake_region.LISTS}, R)
    assert (used, why) == (6, "2 VM external IP(s) + 2 forwarding-rule use(s) + 2 address(es) held by neither")


def test_a_leaked_reservation_is_counted_and_tips_a_pair_that_would_fit():
    """The docs do not say whether a reserved, unattached address counts: it
    does (#1171 ruling). 4 VM IPs leave exactly the 12 a pair needs; one
    leaked reservation more and the pair is refused."""
    ok, msg = _in_use(region_env(vms=4, addresses=[address("a" + "0" * 31)]))
    assert not ok and "5 of 16 used (listed: 4 VM external IP(s) + 0 forwarding-rule use(s) + 1 address(es) held " \
                      "by neither = 5; GCE reports 4), 11 free; the run needs 12" in msg


def test_used_is_the_larger_of_the_listed_and_the_reported():
    """The listed count can miss a holder GCE counts; the reported one misses
    the forwarding rules: neither is known complete."""
    region = fake_region.quotas({})
    next(q for q in region["quotas"] if q["metric"] == "IN_USE_ADDRESSES")["usage"] = 9.0
    ok, msg = quota_line(region, _pair(), "IN_USE_ADDRESSES", region_env(vms=2))
    assert not ok and "9 of 16 used (listed: 2 VM external IP(s) + 0 forwarding-rule use(s) + 0 address(es) held " \
                      "by neither = 2; GCE reports 9; GCE reports 7 more than the resources counted: something " \
                      "holds an address this check does not list), 7 free" in msg
    # when the count covers the reported usage (the observed case), no such note
    ok, msg = _in_use(region_env(vms=3, forwarding_rules=7))
    assert "more than the resources counted" not in msg


def test_a_list_that_cannot_be_read_is_unknown():
    _, refusals = gke.region_fit({"region": fake_region.quotas({}), "project": fake_region.project_quotas({})},
                                 _pair(), {"instances": [], "forwarding-rules": None, "addresses": []})
    assert ("IN_USE_ADDRESSES: the region's forwarding-rules were not listed: what is used cannot be counted, "
            "whether the run fits is unknown") in refusals


# #1172 B2: every read fails closed, through the code that makes the read: a gcloud that
# fails (a missing permission, a transient error) is a refusal naming that read, never an
# empty region (the forwarding-rule term at 0 is revision 1's numbers, run 21 again)
READ_REFUSALS = {
    "instances": "IN_USE_ADDRESSES: gcloud compute instances list failed (rc=1): what is used cannot be counted, "
                 "whether the run fits is unknown",
    "forwarding-rules": "IN_USE_ADDRESSES: gcloud compute forwarding-rules list failed (rc=1): what is used cannot be "
                        "counted, whether the run fits is unknown",
    "addresses": "IN_USE_ADDRESSES: gcloud compute addresses list failed (rc=1): what is used cannot be counted, "
                 "whether the run fits is unknown",
    "project-info": "could not read the project's quotas (gcloud compute project-info describe failed, rc=1): "
                    "whether the run fits is unknown",
}


@pytest.mark.parametrize("read", fake_region.READS)
def test_gke_up_refuses_when_a_read_fails(fakes, monkeypatch, capsys, read):  # noqa: F811
    for k, v in region_env(vms=0).items():                     # an empty region: every quota fits, but the read
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("FAKE_FAIL_READ", read)
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    err = capsys.readouterr().err
    assert f"refusing: {READ_REFUSALS[read]}; nothing was created" in err, err
    c = _calls(fakes)
    assert any(x[:4] == (["gcloud", "compute", "project-info", "describe"] if read == "project-info"
                         else ["gcloud", "compute", read, "list"]) for x in c)   # the read was made, and failed
    assert not any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in c)


PARTIAL = "CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR"


def _fp_pair():
    spec = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16")
    return gke.footprint(spec, lb_services=1) + gke.footprint(spec, lb_services=1)


def test_without_the_property_real_gclouds_failed_list_reads_attempt_2s_region_as_fit(fakes, monkeypatch):  # noqa: F811
    """#1172 B3, the hazard the property closes: real gcloud's failed list
    exits 0 with [] (the SDK's MultiScopeLister only warns while
    compute/allow_partial_error is true, its default). Without the property
    on the call, attempt 2's region (3 VMs + 7 forwarding rules, reported 3)
    with its forwarding-rule list failing reads FIT: run 21 again. (#1383:
    the property is the executor's, gke.COMPUTE_LIST_ENV; emptied here.)"""
    for k, v in region_env(vms=3, forwarding_rules=7).items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("FAKE_FAIL_READ", "forwarding-rules")
    monkeypatch.delenv(PARTIAL, raising=False)

    with monkeypatch.context() as m:
        m.setattr(gke, "COMPUTE_LIST_ENV", {})
        fits, refusals = gke.quota_check(gke.DEFAULT_PROJECT, gke.DEFAULT_ZONE, _fp_pair())
    assert not refusals, refusals
    assert any(m.startswith("IN_USE_ADDRESSES in europe-west4: 3 of 16 used (listed: 3 VM external IP(s) + 0 "
                            "forwarding-rule use(s)") for m in fits), fits
    # the same read with the property, as the pre-check makes it: refused, naming the read
    fits, refusals = gke.quota_check(gke.DEFAULT_PROJECT, gke.DEFAULT_ZONE, _fp_pair())
    assert READ_REFUSALS["forwarding-rules"] in refusals, refusals


def test_the_pre_checks_lists_and_only_they_carry_the_property(fakes, monkeypatch):  # noqa: F811
    """The property is on each of the three lists' own call (gcloud's
    environment form of compute/allow_partial_error), never set globally:
    #1383, execute() sets it on every compute list it runs."""
    monkeypatch.delenv(PARTIAL, raising=False)
    seen = []
    real_run = gke.subprocess.run

    def spy(argv, **kw):
        seen.append((argv[1:4], (kw.get("env") or {}).get(PARTIAL)))
        return real_run(argv, **kw)

    monkeypatch.setattr(gke.subprocess, "run", spy)
    gke.quota_check(gke.DEFAULT_PROJECT, gke.DEFAULT_ZONE, _fp_pair())
    def is_list(a):
        return a[:1] == ["compute"] and a[2:3] == ["list"]

    lists = {tuple(a): v for a, v in seen if is_list(a)}
    assert lists == {("compute", "addresses", "list"): "false", ("compute", "forwarding-rules", "list"): "false",
                     ("compute", "instances", "list"): "false"}
    others = [(a, v) for a, v in seen if not is_list(a)]
    assert others and all(v is None for _, v in others), seen      # the describes and the #985 reads: untouched
    import os
    assert PARTIAL not in os.environ


@pytest.mark.parametrize("read", fake_region.READS)
def test_the_pair_refuses_when_a_read_fails(pairbed, read):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, FAKE_FAIL_READ=read, **region_env(vms=0))
    assert p.returncode == 2, p.stderr[-1500:]
    assert f"refusing: {READ_REFUSALS[read]}" in p.stderr, p.stderr[-1500:]
    assert not [c for c in _pair_calls(fake) if " dry=0 " in c]          # neither run started


def test_the_fake_never_takes_an_in_use_number():
    with pytest.raises(SystemExit):
        fake_region.quotas({"FAKE_USAGE_IN_USE_ADDRESSES": "13"})


# ─── the need follows the spec: pool sizes, GKE version, all-regions CPUs ───

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
    env = region_env(vms=4)
    assert _in_use(env)[0]
    spec = gke.default_spec(NAME, loadgen_machine_type="n2-standard-16")
    spec = dataclasses.replace(spec, platform_pool=dataclasses.replace(spec.platform_pool, count=4))
    ok, msg = _in_use(env, gke.footprint(spec, lb_services=1) + gke.footprint(spec, lb_services=1))
    assert not ok and "12 free; the run needs 14: 12 node external IP(s) + 2 LoadBalancer IP(s)" in msg


def test_gke_1_37_adds_a_backend_service_and_a_neg_per_zone_for_each_load_balancer():
    old, new = _needs(_pair(lb=2, version="1.36")), _needs(_pair(lb=2, version="1.37"))
    assert (old["REGIONAL_EXTERNAL_NETWORK_LB_BACKEND_SERVICES"], old["NETWORK_ENDPOINT_GROUPS"]) == (0, 0)
    assert (new["REGIONAL_EXTERNAL_NETWORK_LB_BACKEND_SERVICES"], new["NETWORK_ENDPOINT_GROUPS"]) == (4, 4)
    assert {k: v for k, v in new.items() if old[k] != v} == {"REGIONAL_EXTERNAL_NETWORK_LB_BACKEND_SERVICES": 4,
                                                             "NETWORK_ENDPOINT_GROUPS": 4}
    region = fake_region.quotas({"FAKE_USAGE_NETWORK_ENDPOINT_GROUPS": "98"})
    assert quota_line(region, _pair(lb=2, version="1.36"), "NETWORK_ENDPOINT_GROUPS")[0]
    ok, msg = quota_line(region, _pair(lb=2, version="1.37.1-gke.100"), "NETWORK_ENDPOINT_GROUPS")
    assert not ok and msg.startswith("NETWORK_ENDPOINT_GROUPS in europe-west4: 98 of 100 used, 2 free; the run needs 4")


def test_the_projects_all_regions_cpus_count_when_the_project_has_that_quota(fakes, monkeypatch, capsys):  # noqa: F811
    ok, msg = quota_line(fake_region.quotas({}), _pair(), "CPUS_ALL_REGIONS")
    assert ok and msg.startswith("CPUS_ALL_REGIONS: operations-dev-krateo-io has no such quota")
    proj = {"FAKE_PROJECT_QUOTAS": json.dumps([{"metric": "CPUS_ALL_REGIONS", "limit": 128.0, "usage": 40.0}])}
    ok, msg = quota_line(fake_region.quotas({}), _pair(), "CPUS_ALL_REGIONS", proj)
    assert not ok and msg == ("CPUS_ALL_REGIONS in operations-dev-krateo-io: 40 of 128 used, 88 free; the run needs "
                              "96 vCPUs (every node, any family)")
    # through gke-up: one run (40 vCPUs) fits 88 free; at 100 used it does not
    monkeypatch.setenv("FAKE_PROJECT_QUOTAS", proj["FAKE_PROJECT_QUOTAS"])
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 0
    assert "CPUS_ALL_REGIONS in operations-dev-krateo-io: 40 of 128 used, 88 free; the run needs 40" \
        in capsys.readouterr().out
    monkeypatch.setenv("FAKE_PROJECT_QUOTAS", json.dumps([{"metric": "CPUS_ALL_REGIONS", "limit": 128.0,
                                                            "usage": 100.0}]))
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    assert "refusing: CPUS_ALL_REGIONS in operations-dev-krateo-io: 100 of 128 used, 28 free; the run needs 40" \
        in capsys.readouterr().err


def test_the_loadgen_machine_type_and_version_move_the_need_through_the_cli(tmp_path, capsys):
    for mt, ver, want_cpu, want_neg in (("n2-standard-8", "1.36", 40, 0), ("n2-standard-16", "1.37", 48, 1)):
        out = tmp_path / f"{mt}-{ver}.json"
        assert gke.main(["gke-footprint", "--name", NAME, "--loadgen-machine-type", mt, "--cluster-version", ver,
                         "--address-need", "1", "--out", str(out)]) == 0
        fp = json.loads(out.read_text())
        assert (fp["vcpus"], fp["lb_negs"]) == ({"n2": want_cpu}, want_neg)
    assert "footprint: IN_USE_ADDRESSES: the run needs 6: 5 node external IP(s) + 1 LoadBalancer IP(s)" \
        in capsys.readouterr().err


# ─── through gke-up, the pair launcher and run606.sh ───────────────────────

def test_gke_up_counts_the_regions_forwarding_rules(fakes, monkeypatch, capsys):  # noqa: F811
    for k, v in region_env(vms=3, forwarding_rules=8).items():
        monkeypatch.setenv(k, v)
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    err = capsys.readouterr().err
    assert ("refusing: IN_USE_ADDRESSES in europe-west4: 11 of 16 used (listed: 3 VM external IP(s) + 8 "
            "forwarding-rule use(s) + 0 address(es) held by neither = 11; GCE reports 3), 5 free; the run needs 6: "
            "5 node external IP(s) + 1 LoadBalancer IP(s)") in err and "nothing was created" in err
    assert not any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))
    for k, v in region_env(vms=3, forwarding_rules=7).items():
        monkeypatch.setenv(k, v)
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 0
    assert "IN_USE_ADDRESSES in europe-west4: 10 of 16 used (listed: 3 VM external IP(s) + 7 forwarding-rule " \
           "use(s) + 0 address(es) held by neither = 10; GCE reports 3), 6 free; the run needs 6" \
        in capsys.readouterr().out
    assert any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))
    # the reads, before the create, all read-only
    c = _calls(fakes)
    first_create = next(i for i, x in enumerate(c) if x[:4] == ["gcloud", "container", "clusters", "create"])
    for kind in ("instances", "forwarding-rules", "addresses"):
        assert any(x[:4] == ["gcloud", "compute", kind, "list"] for x in c[:first_create]), kind
    assert any(x[:4] == ["gcloud", "compute", "project-info", "describe"] for x in c[:first_create])


def test_the_pair_refuses_attempt_2s_region_and_starts_at_4_of_16(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, **region_env(vms=3, forwarding_rules=7))
    assert p.returncode == 2, p.stderr[-1500:]
    assert ("refusing: IN_USE_ADDRESSES in europe-west4: 10 of 16 used (listed: 3 VM external IP(s) + 7 "
            "forwarding-rule use(s) + 0 address(es) held by neither = 10; GCE reports 3), 6 free; the run needs 12: "
            "10 node external IP(s) + 2 LoadBalancer IP(s)") in p.stderr
    assert "refusing: refusing:" not in p.stderr
    assert not [c for c in _pair_calls(fake) if " dry=0 " in c]          # neither run started
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", **region_env(vms=4))
    assert p.returncode == 0, p.stderr[-1500:]
    assert "IN_USE_ADDRESSES in europe-west4: 4 of 16 used (listed: 4 VM external IP(s)" in p.stderr
    assert "12 free; the run needs 12" in p.stderr and "DRY: all 2 runs would start now" in p.stderr
    # both numbers, listed and reported, in the pair's log: each launch is #1173's falsifier
    assert "GCE reports 4)" in (tmp / "pair" / "pair.log").read_text()


def test_the_pair_names_every_quota_that_does_not_fit(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_USAGE_INSTANCES="20", **region_env(vms=13))
    assert p.returncode == 2
    lines = [ln for ln in p.stderr.splitlines() if ln.startswith("refusing: ")]
    assert any(ln.startswith("refusing: IN_USE_ADDRESSES in europe-west4: 13 of 16 used") for ln in lines)
    assert any(ln.startswith("refusing: INSTANCES in europe-west4: 20 of 24 used, 4 free; the run needs 10")
               for ln in lines)


def test_run606_refuses_before_any_cluster_when_the_regions_forwarding_rules_leave_no_room(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **region_env(vms=3, forwarding_rules=8)),
                       capture_output=True, text=True, timeout=600)
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    line = next(ln for ln in log.splitlines() if "up FAILED: gke-up exited 2" in ln)
    assert "IN_USE_ADDRESSES in europe-west4: 11 of 16 used (listed: 3 VM external IP(s) + 8 forwarding-rule " \
           "use(s) + 0 address(es) held by neither = 11; GCE reports 3), 5 free; the run needs 6" in line
    fp = json.loads((tmp / "out" / "footprint.json").read_text())       # derived before gke-up, kept
    assert (fp["nodes"], fp["node_external_ips"], fp["lb_services"]) == (5, 5, 1)
    calls = [json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines()]
    assert not any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "create"] for c in calls)


def test_run606_logs_both_in_use_numbers_when_the_run_fits(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **region_env(vms=3, forwarding_rules=2)),
                       capture_output=True, text=True, timeout=600)
    assert p.returncode == 0, p.stderr[-3000:]
    log = (tmp / "out" / "run.log").read_text()
    assert "IN_USE_ADDRESSES in europe-west4: 5 of 16 used (listed: 3 VM external IP(s) + 2 forwarding-rule " \
           "use(s) + 0 address(es) held by neither = 5; GCE reports 3), 11 free; the run needs 6" in log


def test_run606_derives_the_footprint_with_gke_ups_own_flags():
    """gke-footprint and gke-up get the same spec flags (the parsers share
    them: footprint_args), so the footprint is the cluster gke-up creates."""
    text = (BENCH_DIR / "run606.sh").read_text()

    def flags(cmd):
        m = re.search(r"python3 -m bench\.gke " + cmd + r" (.*?)(?:\|\||2>)", text, re.S)
        toks = m.group(1).replace("\\\n", " ").split()
        return {t: toks[i + 1] for i, t in enumerate(toks) if t.startswith("--") and i + 1 < len(toks)
                and t in ("--name", "--project", "--zone", "--loadgen-machine-type", "--address-need",
                          "--cluster-version")}

    up, fp = flags("gke-up"), flags("gke-footprint")
    assert up == fp and {"--name", "--project", "--zone", "--loadgen-machine-type", "--address-need"} <= set(up)
