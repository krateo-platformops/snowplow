"""#1089: run 21 records what run 20 could only infer.

A  The 30 s stage dials (#1054), likely a TCP connect whose SYN was never
   answered: the snowplow pod's TCPSynRetrans / RetransSegs (read NODE-SIDE
   from its network namespace: the image has no shell), the snowplow node's
   conntrack insert_failed / drop, and the API server's request codes, per
   phase, report only, in verdict.json (bench/netns606.py, bench/netpath.py).
B  Browser connection queueing on /dashboard first visits: each /call's
   negotiated protocol (a tag), its Resource Timing offsets from its
   startTime, its start from its load's (metadata, never a URL), and per
   route: the protocol set, requestStart − fetchStart on reused connections,
   its share of the gap between Chrome's network events and Resource Timing,
   and the waterfall (k6/pageload.js, k6/browser.js, bench/calltiming.py).

Fixtures are run-20-shaped: run 20b's own phase layout (boot, the storm, the
burst inside the churn steps, the six churn steps, steady; its times), its
/dashboard first visit's 57 calls a load on HTTP/1.1. Every report is checked
through the evaluate CLI (what run606 runs). Missing data reads UNMEASURED,
never 0.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from bench import calltiming, load606, netns606, netpath, sampler
from tests.test_919_evidence_gaps import NODE, _evaluate, needs_node
from tests.test_k6_830 import _point, _write_out
from tests.test_run606_real_sequence import BENCH_DIR

# ─── run 20b's phase layout (snowplow-bench-run20b-871a4e09: boot.jsonl,
# pod-before.jsonl, mem606's mem_start / first load_detected / mem_end,
# fresh606's churn records) ──────────────────────────────────────────────────
T_RESTART = 1793517266.397013
CREATED, STARTED, READY = 1793517267, 1793517268, 1793517754
BEFORE_T = 1793517760.23469
MEM_START = 1793517763.009779
LOAD_SEEN = 1793517782.3410144
MEM_END = 1793534082.8476672
PLAN = {"profile_s": 30, "storm_s": 600.0, "every_s": 300.0, "burst_at_s": 900.0, "burst_pre_s": 60.0,
        "burst_post_s": 240.0}
CHURNS = [("list_dep@1", 1793517852.354, 1793518237.293, 1.0), ("list_dep@10", 1793518237.293, 1793518594.331, 10.0),
          ("list_dep@50", 1793518594.336, 1793518968.361, 50.0),
          ("list_dep_status@1", 1793518968.478, 1793519346.033, 1.0),
          ("list_dep_status@10", 1793519346.033, 1793519716.904, 10.0),
          ("list_dep_status@50", 1793519716.904, 1793520091.63, 50.0)]
STORM_END = LOAD_SEEN + PLAN["storm_s"]
BURST = (LOAD_SEEN + PLAN["burst_at_s"] - PLAN["burst_pre_s"], LOAD_SEEN + PLAN["burst_at_s"] + PLAN["burst_post_s"])
NEW_UID = "0b5e4c1a-7d2f-4e8b-9a61-2c3d4e5f6a7b"     # the pod the bench recorded (after the reboot)
OLD_UID = "9f8e7d6c-5b4a-4321-8fed-cba987654321"     # the pod before the reboot: never snowplow's here
OTHER_UID = "11111111-2222-4333-8444-555555555555"   # a DaemonSet pod on the same node


def run20b_records(uid=True) -> dict[str, list[dict]]:
    pod = {"kind": "snowplow_pod", "pod": "snowplow-x", "restarts": 0, "ready": True, "created_at": CREATED,
           "started_at": STARTED, "ready_at": READY, **({"uid": NEW_UID} if uid else {})}
    return {
        "boot.jsonl": [{"kind": "boot", "t_restart": T_RESTART}],
        "pod-before.jsonl": [{**pod, "phase": "before", "t": BEFORE_T}],
        "pod-after.jsonl": [{**pod, "phase": "after", "t": MEM_END - 5}],
        "mem-mem606-x.jsonl": [{"kind": "mem_start", "t": MEM_START, "cpu_plan": PLAN},
                               {"kind": "mem", "t": LOAD_SEEN, "load_detected": True},
                               {"kind": "mem_end", "t": MEM_END}],
        "fresh-fresh606-x.jsonl": [{"kind": "churn", "label": lb, "t0": a, "t1": b, "target_rate": r, "issued": 1,
                                    "failed": 0} for lb, a, b, r in CHURNS]}


def grid(t0=T_RESTART - 120, t1=MEM_END, every=15.0):
    t = t0
    while t <= t1:
        yield t
        t += every


# injected events (time, count): mid-phase, so no pair carrying one crosses an edge
SYN_EVENTS = [(CREATED + 100, 2), (BURST[0] + 60, 7), (CHURNS[5][1] + 100, 3), (MEM_END - 3000, 1)]
CT_EVENTS = [(BURST[0] + 90, 4)]
CODE_EVENTS = {"503": [(BURST[0] + 120, 5)], "429": [(LOAD_SEEN + 200, 3)]}


def upto(events, t):
    return sum(n for at, n in events if at <= t)


def netns_rec(t, early_drop=False, failed=False):
    if failed:
        return {"kind": "netns", "t": t, "failed": True, "error": "OSError: boom"}
    spaces = [{"netns": "4026531840", "host": True, "pod_uids": [OTHER_UID],          # hostNetwork DaemonSet
               "tcp": {"TCPSynRetrans": 900, "RetransSegs": 900, "ActiveOpens": 900, "AttemptFails": 9}}]
    if t < T_RESTART + 20:                      # the old pod: huge, fast counters that must never leak in
        k = int(t - T_RESTART + 1000)
        spaces.append({"netns": "4026531111", "host": False, "pod_uids": [OLD_UID],
                       "tcp": {"TCPSynRetrans": 100 * k, "RetransSegs": 500 * k, "ActiveOpens": 1000 * k,
                               "AttemptFails": k}})
    if t >= CREATED:
        syn = upto(SYN_EVENTS, t)
        spaces.append({"netns": "4026532222", "host": False, "pod_uids": [NEW_UID],
                       "tcp": {"TCPSynRetrans": syn, "RetransSegs": 5 * syn, "ActiveOpens": int(t - CREATED),
                               "AttemptFails": 0}})
    ct = {"insert_failed": upto(CT_EVENTS, t), "drop": 0, **({"early_drop": 0} if early_drop else {})}
    return {"kind": "netns", "t": t, "t_start": t - 0.05, "processes": 300, "processes_unreadable": 0,
            "namespaces": spaces,
            "node": {"conntrack": ct, "nf_conntrack_count": 1000 + int(t) % 7, "nf_conntrack_max": 262144,
                     "boot_id": "b-1", "dataplane": {"kube_proxy": True, "cilium_agent": False}}}


def apf_rec(t, i, codes=True):
    rep = 1.0e9 if i % 2 == 0 else 2.0e9        # two control-plane replicas, in turn
    by = {"200": 1000.0 * i + (5e6 if rep == 2.0e9 else 0)}
    if codes:
        for code, ev in CODE_EVENTS.items():
            n = sum(c for at, c in ev if at <= t and (code != "429" or rep == 2.0e9) and (code != "503" or rep == 1.0e9))
            if n:                                   # a counter vector exports a code from its first increment
                by[code] = float(n)
    return {"kind": "apf", "t": t, "t_start": t - 1, "replica": rep, "series": {},
            **({"requests_by_code": by} if codes else {})}


def evaluate20b(tmp_path, *, netns=True, uid=True, codes=True, apf=True, early_drop=False):
    files = run20b_records(uid=uid)
    if netns:
        files["netns.jsonl"] = [{"kind": "netns_start", "t": T_RESTART - 121, "every_s": 15}] + \
            [netns_rec(t, early_drop) for t in grid()]
    if apf:
        files["apf.jsonl.gz"] = [apf_rec(t, i, codes) for i, t in enumerate(grid())]
    return _evaluate(tmp_path, files)


# ─── A1. the node-side sampler, on a fake /proc ─────────────────────────────

NETSTAT = ("TcpExt: SyncookiesSent SyncookiesRecv TCPTimeouts TCPSynRetrans TCPOrigDataSent\n"
           "TcpExt: 0 0 12 {syn} 999\n"
           "IpExt: InNoRoutes InOctets\nIpExt: 0 123456\n")
SNMP = ("Ip: Forwarding DefaultTTL\nIp: 1 64\n"
        "Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab "
        "InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors\n"
        "Tcp: 1 200 120000 -1 {opens} 10 {fails} 0 4 5000 6000 {retrans} 0 3 0\n"
        "Udp: InDatagrams NoPorts\nUdp: 1 2\n")
# kernel 5.15+: no per-CPU `entries`-only columns removed; hex per CPU; no early_drop column here
CONNTRACK = ("entries  clashres found     new invalid ignore delete chainlength insert insert_failed drop "
             "icmp_error  expect_new expect_create expect_delete search_restart\n"
             "000003e8  00000000 00000000  00000000 0000001a 00000000 00000000 00000000 00000000 0000000a 00000002 "
             "00000000  00000000 00000000 00000000 00000004\n"
             "000003e8  00000000 00000000  00000000 00000003 00000000 00000000 00000000 00000000 00000010 00000001 "
             "00000000  00000000 00000000 00000000 00000000\n")


def fake_proc(root: Path, *, conntrack=True, netstat_readable=True) -> Path:
    proc = root / "proc"

    def pid(n, ns, cgroup, comm, syn=0, opens=0, fails=0, retrans=0):
        d = proc / str(n)
        (d / "ns").mkdir(parents=True)
        os.symlink(f"net:[{ns}]", d / "ns" / "net")
        (d / "cgroup").write_text(cgroup)
        (d / "comm").write_text(comm + "\n")
        (d / "net").mkdir()
        if netstat_readable or n != 100:
            (d / "net" / "netstat").write_text(NETSTAT.format(syn=syn))
        (d / "net" / "snmp").write_text(SNMP.format(opens=opens, fails=fails, retrans=retrans))
    pid(1, "4026531840", "0::/init.scope\n", "systemd", syn=50, opens=500, fails=5, retrans=60)
    pid(40, "4026531840", "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod"
                          "11111111_2222_4333_8444_555555555555.slice/cri-containerd-aa.scope\n", "kube-proxy")
    # snowplow: systemd driver (underscores), its pause and its server, one netns
    sp = ("0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod0b5e4c1a_7d2f_4e8b_9a61_2c3d4e5f6a7b"
          ".slice/cri-containerd-{c}.scope\n")
    pid(100, "4026532222", sp.format(c="pause"), "pause", syn=7, opens=1200, fails=3, retrans=41)
    pid(101, "4026532222", sp.format(c="server"), "server", syn=7, opens=1200, fails=3, retrans=41)
    # another pod, cgroupfs driver (dashes)
    pid(200, "4026532333", "12:memory:/kubepods/burstable/pod9f8e7d6c-5b4a-4321-8fed-cba987654321/bb\n", "app",
        syn=1, opens=2, fails=0, retrans=0)
    (proc / "self").mkdir()                       # not a pid: skipped
    if conntrack:
        (proc / "net" / "stat").mkdir(parents=True)
        (proc / "net" / "stat" / "nf_conntrack").write_text(CONNTRACK)
    (proc / "sys" / "net" / "netfilter").mkdir(parents=True)
    (proc / "sys" / "net" / "netfilter" / "nf_conntrack_count").write_text("1000\n")
    (proc / "sys" / "net" / "netfilter" / "nf_conntrack_max").write_text("262144\n")
    (proc / "sys" / "kernel" / "random").mkdir(parents=True)
    (proc / "sys" / "kernel" / "random" / "boot_id").write_text("b-1\n")
    return proc


def test_the_sampler_reads_each_pod_namespace_and_the_node_from_proc(tmp_path):
    r = netns606.sample(str(fake_proc(tmp_path)), clock=lambda: 1000.0)
    by = {ns["netns"]: ns for ns in r["namespaces"]}
    assert set(by) == {"4026531840", "4026532222", "4026532333"}
    snow = by["4026532222"]
    assert snow["pod_uids"] == [NEW_UID] and not snow["host"]
    assert snow["tcp"] == {"TCPSynRetrans": 7, "RetransSegs": 41, "ActiveOpens": 1200, "AttemptFails": 3}
    assert by["4026532333"]["pod_uids"] == ["9f8e7d6c-5b4a-4321-8fed-cba987654321"]        # cgroupfs form
    host = by["4026531840"]
    assert host["host"] and host["pod_uids"] == ["11111111-2222-4333-8444-555555555555"]   # hostNetwork pod
    # summed over the CPUs, in hex: insert_failed 0x0a + 0x10, drop 2 + 1; early_drop: no such column
    assert r["node"]["conntrack"] == {"insert_failed": 26, "drop": 3}
    assert (r["node"]["nf_conntrack_count"], r["node"]["nf_conntrack_max"], r["node"]["boot_id"]) == \
        (1000, 262144, "b-1")
    assert r["node"]["dataplane"] == {"kube_proxy": True, "cilium_agent": False}
    assert r["processes"] == 5


def test_an_unreadable_file_is_recorded_never_a_zero(tmp_path):
    r = netns606.sample(str(fake_proc(tmp_path, conntrack=False, netstat_readable=False)), clock=lambda: 1.0)
    snow = next(ns for ns in r["namespaces"] if ns["netns"] == "4026532222")
    # pid 100 (the first in the namespace) has no netstat: that field is absent, the error named
    assert "TCPSynRetrans" not in snow["tcp"] and snow["tcp"]["RetransSegs"] == 41
    assert snow["errors"]["netstat"].startswith("FileNotFoundError")
    assert r["node"]["conntrack"] is None and r["node"]["conntrack_error"].startswith("FileNotFoundError")


def test_the_loop_writes_whole_lines_and_stops_on_the_stop_file(tmp_path):
    proc = fake_proc(tmp_path)
    out, stop = tmp_path / "netns.jsonl", tmp_path / "stop"
    ticks = iter(range(1000, 2000))

    def sleep(_s):
        if next(ticks) >= 1003:
            stop.write_text("")
    netns606.loop(str(out), 15, str(stop), str(proc), clock=lambda: 5.0, sleep=sleep)
    recs = [json.loads(ln) for ln in out.read_text().splitlines()]
    assert recs[0]["kind"] == "netns_start" and recs[-1]["kind"] == "netns_end"
    assert [r["kind"] for r in recs[1:-1]] == ["netns"] * recs[-1]["samples"] and recs[-1]["samples"] == 4


def test_the_manifest_is_node_side_pinned_to_snowplow_and_ships_the_sampler(tmp_path):
    yaml = pytest.importorskip("yaml")
    docs = list(yaml.safe_load_all(netns606.render(every_s=15)))
    ns, cm, job = docs
    assert ns["kind"] == "Namespace" and cm["kind"] == "ConfigMap" and job["kind"] == "Job"
    assert cm["data"]["netns606.py"].rstrip("\n") == (BENCH_DIR / "bench" / "netns606.py").read_text().rstrip("\n")
    spec = job["spec"]["template"]["spec"]
    assert spec["hostPID"] is True and spec["hostNetwork"] is True
    assert spec["nodeSelector"] == {"bench.krateo.io/pool": "snowplow"}
    assert {"key": "dedicated", "operator": "Equal", "value": "snowplow", "effect": "NoSchedule"} in spec["tolerations"]
    c = spec["containers"][0]
    assert c["securityContext"] == {"privileged": True}
    assert c["command"][:3] == ["python3", "/opt/netns606/netns606.py", "loop"] and "15" in c["command"]
    # sidecar-free: nothing of it lands in the snowplow pod or its namespace
    assert job["metadata"]["namespace"] == "bench-loadgen"


def test_the_shipped_sampler_runs_alone(tmp_path):
    """The pod runs the file by itself (no bench package): stdlib only."""
    src = tmp_path / "alone" / "netns606.py"
    src.parent.mkdir()
    src.write_text((BENCH_DIR / "bench" / "netns606.py").read_text())
    proc = fake_proc(tmp_path)
    (tmp_path / "stop").write_text("")       # stop at once: start / end frames only
    p = subprocess.run(["python3", "-I", str(src), "loop", "--proc", str(proc), "--out", str(tmp_path / "o.jsonl"),
                        "--stop-file", str(tmp_path / "stop"), "--every-s", "15"],
                       capture_output=True, text=True, timeout=60, cwd=src.parent)
    assert p.returncode == 0, p.stderr
    p = subprocess.run(["python3", "-I", str(src), "loop", "--proc", str(proc), "--out", str(tmp_path / "o2.jsonl"),
                        "--stop-file", str(tmp_path / "nostop"), "--samples", "1"],
                       capture_output=True, text=True, timeout=60, cwd=src.parent)
    assert p.returncode == 0, p.stderr
    assert json.loads((tmp_path / "o2.jsonl").read_text().splitlines()[1])["kind"] == "netns"


def test_run606_starts_the_sampler_with_the_platform_and_collects_its_file():
    src = (BENCH_DIR / "run606.sh").read_text()
    env = src[src.index("s_env() {"):]
    env = env[:env.index("\n}\n")]                                           # s_env's own body
    # a call in s_env (the platform is up, before the reboot): the boot is covered
    assert env.index("\n  start_apf_loop\n") < env.index("\n  start_netns_sampler\n")
    assert src.index("\nstep env ") < src.index("\nstep reboot ")
    start = src[src.index("start_netns_sampler() {"):]
    start = start[:start.index("\n}\n")]
    assert "bench.netns606 render --every-s \"$TOP_POLL_S\"" in start and "kapply" in start
    assert "|| log" in start                                               # report only: never fails the run
    coll = src[src.index("s_collect() {"):src.index("mem_stop() {")]
    assert "job-name=netns606" in coll and ':/out/netns.jsonl" "$OUT/netns.jsonl"' in coll
    ev = src[src.index("s_evaluate() {"):]
    # a glob, so nullglob drops it when no file was collected (a literal path is passed as is, and
    # evaluate stops on a file that is not there: test_run606_real_sequence runs that case)
    assert '"$OUT"/netns*.jsonl' in ev[:ev.index("python3 -m bench.load606 evaluate")]


# ─── A2. the API server's codes, from the APF scrape ────────────────────────

def test_parse_apf_sums_request_total_by_code_only():
    text = ("process_start_time_seconds 1.5e9\n"
            'apiserver_request_total{code="200",component="apiserver",resource="pods",verb="LIST"} 10\n'
            'apiserver_request_total{code="200",component="apiserver",resource="secrets",verb="GET"} 5\n'
            'apiserver_request_total{code="503",component="apiserver",resource="pods",verb="LIST"} 2\n'
            'apiserver_request_total{code="429",resource="x y",verb="WATCH"} 1 1700000000\n'
            'apiserver_request_total_other{code="500"} 99\n'
            'apiserver_flowcontrol_dispatched_requests_total{priority_level="a"} 3\n')
    r = sampler.parse_apf(text)
    assert r["requests_by_code"] == {"200": 15.0, "503": 2.0, "429": 1.0}
    assert r["series"] == {'apiserver_flowcontrol_dispatched_requests_total{priority_level="a"}': 3.0}
    assert sampler.parse_apf("process_start_time_seconds 1\n")["requests_by_code"] is None   # not read, not {}


# ─── A3. per phase, through evaluate, on run 20b's layout ───────────────────

def test_the_tcp_path_per_phase_on_run20b_shape(tmp_path):
    v = evaluate20b(tmp_path)
    r = v["report"]["tcp_path_report_only"]
    tcp = r["snowplow_pod_tcp"]
    assert tcp["status"] == "present" and tcp["pod_uids"] == [NEW_UID]
    ph = tcp["by_phase"]
    # the new pod's samples start inside the boot (run 20b: Ready 6 s before the measure, no sample between)
    assert list(ph)[:3] == ["boot", "storm", "burst"]
    assert ph["boot"]["counters"]["TCPSynRetrans"] == 2
    assert ph["storm"]["counters"]["TCPSynRetrans"] == 0                    # measured: a real 0
    assert ph["burst"]["counters"] == {"TCPSynRetrans": 7, "RetransSegs": 35, "ActiveOpens":
                                       ph["burst"]["counters"]["ActiveOpens"], "AttemptFails": 0}
    assert ph["churn:list_dep_status@50"]["counters"]["TCPSynRetrans"] == 3
    assert ph["steady"]["counters"]["TCPSynRetrans"] == 1
    assert ph["all"]["counters"]["TCPSynRetrans"] == 13                     # every pair, spanning included
    # the old pod's 100-per-second counters and the hostNetwork pod's never enter
    assert ph["all"]["counters"]["RetransSegs"] == 65
    # the burst lies inside the churn steps: pairs across its edges are spanning, never booked
    assert any("burst" in k for k in tcp["spanning"])
    ct = r["node_conntrack"]
    assert ct["status"] == "present" and ct["dataplane"] == {"kube_proxy": True, "cilium_agent": False}
    assert ct["by_phase"]["burst"]["counters"] == {"insert_failed": 4, "drop": 0, "early_drop": "UNMEASURED"}
    assert ct["by_phase"]["storm"]["counters"]["insert_failed"] == 0
    assert ct["by_phase"]["burst"]["nf_conntrack_max_max"] == 262144
    codes = r["apiserver_request_codes"]
    assert codes["status"] == "present" and codes["sources"] == 2
    assert codes["by_phase"]["burst"]["5xx"] == 5 and codes["by_phase"]["burst"]["429"] == 0
    assert codes["by_phase"]["storm"]["429"] == 3 and codes["by_phase"]["storm"]["5xx"] == 0
    # replica B's counters are 5 M higher: differencing across replicas would read millions; within
    # each replica the 200s grow by 1,000 a record, so the run's total is exactly
    recs = [apf_rec(t, i) for i, t in enumerate(grid())]
    want = sum(max(r["requests_by_code"]["200"] for r in recs if r["replica"] == rep)
               - min(r["requests_by_code"]["200"] for r in recs if r["replica"] == rep) for rep in (1.0e9, 2.0e9))
    assert codes["by_phase"]["all"]["total"] == want + 5 + 3
    assert not any(w.startswith("tcp_path") for w in v["report"]["report_warnings"])


def test_the_pod_record_keeps_the_uid_netpath_picks_the_namespace_by():
    pods = {"items": [{"metadata": {"name": "snowplow-x", "uid": NEW_UID, "creationTimestamp": "2026-11-01T07:14:27Z"},
                       "spec": {"containers": [{"resources": {}}]},
                       "status": {"containerStatuses": [{"restartCount": 0, "ready": True, "imageID": "x@sha256:" + "a" * 64,
                                                         "state": {"running": {"startedAt": "2026-11-01T07:14:28Z"}}}]}}]}
    rec = load606.pod_record(pods, "before")
    assert rec["uid"] == NEW_UID and netpath.snowplow_uids([rec]) == [NEW_UID]


def test_a_phase_without_a_pair_is_unmeasured_never_zero():
    clock = netpath.PhaseClock([("burst", 100, 110)], load606._phase_of, None)
    rows = [(95, "a", {"TCPSynRetrans": 1}, {}), (105, "a", {"TCPSynRetrans": 3}, {}),
            (120, "a", {"TCPSynRetrans": 3}, {})]
    r = netpath.by_phase(rows, clock, netpath.TCP_FIELDS)
    assert r["by_phase"]["burst"]["counters"] == {f: "UNMEASURED" for f in netpath.TCP_FIELDS}
    assert r["by_phase"]["burst"]["pairs"] == 0
    assert r["spanning"]["steady..burst"]["counters"]["TCPSynRetrans"] == 2
    # a field neither sample carried is UNMEASURED beside the measured ones
    assert r["spanning"]["steady..burst"]["counters"]["RetransSegs"] == "UNMEASURED"


def test_a_counter_that_goes_back_is_a_reset_and_sources_are_never_crossed():
    clock = netpath.PhaseClock([], load606._phase_of, None)
    rows = [(0, "ns1", {"TCPSynRetrans": 50}, {}), (15, "ns2", {"TCPSynRetrans": 2}, {}),
            (30, "ns1", {"TCPSynRetrans": 51}, {}), (45, "ns2", {"TCPSynRetrans": 1}, {}),   # ns2 went back
            (60, "ns2", {"TCPSynRetrans": 4}, {})]
    r = netpath.by_phase(rows, clock, ("TCPSynRetrans",))
    assert r["counter_resets_skipped"] == 1 and r["sources"] == 2
    assert r["by_phase"]["steady"]["counters"]["TCPSynRetrans"] == 1 + 3


@pytest.mark.parametrize("what, why", [
    ("netns", "no netns606 sample"),
    ("uid", "no snowplow_pod record carries the pod's UID"),
])
def test_missing_pod_evidence_reads_unmeasured_with_its_reason(tmp_path, what, why):
    v = evaluate20b(tmp_path, netns=what != "netns", uid=what != "uid")
    tcp = v["report"]["tcp_path_report_only"]["snowplow_pod_tcp"]
    assert tcp["status"] == "UNMEASURED" and tcp["why"].startswith(why)
    assert "by_phase" not in tcp                                     # no figure, so no 0 to misread
    assert any(w.startswith("tcp_path snowplow_pod_tcp: UNMEASURED") for w in v["report"]["report_warnings"])


def test_a_pod_the_sampler_never_saw_is_unmeasured():
    clock = netpath.PhaseClock([], load606._phase_of, None)
    r = netpath.pod_tcp([netns_rec(T_RESTART - 60)], [NEW_UID], clock)     # before the new pod existed
    assert r["status"] == "UNMEASURED" and "no sampled network namespace" in r["why"]


def test_dataplane_v2_is_said_its_conntrack_is_not_nf_conntrack():
    clock = netpath.PhaseClock([], load606._phase_of, None)
    recs = [netns_rec(t) for t in (0.0, 15.0)]
    for r in recs:
        r["node"]["dataplane"] = {"kube_proxy": False, "cilium_agent": True}
    out = netpath.node_conntrack(recs, clock)
    assert "Cilium's BPF conntrack" in out["dataplane_note"]


def test_run20_apf_without_codes_is_unmeasured(tmp_path):
    """Run 20's own apf stream shape: answered scrapes, no requests_by_code."""
    v = evaluate20b(tmp_path, codes=False)
    c = v["report"]["tcp_path_report_only"]["apiserver_request_codes"]
    assert c["status"] == "UNMEASURED" and "requests_by_code" in c["why"]


RUN20_APF = Path("/tmp/snowplow-runs/606/snowplow-bench-run20b-871a4e09/apf.jsonl.gz")


@pytest.mark.skipif(not RUN20_APF.exists(), reason="run 20b's collected files are not on this machine")
def test_run20b_real_apf_stream_reads_unmeasured_codes():
    clock = netpath.PhaseClock([], load606._phase_of, None)
    r = netpath.apiserver_codes(load606.iter_jsonl(str(RUN20_APF)), clock)
    assert r["status"] == "UNMEASURED" and "none of the" in r["why"]


# ─── B1. each /call's Resource Timing, in the page (node) ───────────────────

_TIMING_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const cases = JSON.parse(readFileSync(process.argv[2], 'utf8'))
let deliver = null
globalThis.window = { location: { href: 'http://gw/dashboard', pathname: '/dashboard' },
                      fetch: () => Promise.resolve({ ok: true }), performance: { timeOrigin: cases.origin } }
globalThis.history = { pushState () {}, replaceState () {} }
globalThis.document = { addEventListener () {}, querySelectorAll: () => [] }
globalThis.MutationObserver = class { observe () {} }
window.PerformanceObserver = class {
  constructor (cb) { deliver = (es) => cb({ getEntries: () => es }) }
  observe () {}
}
window.addEventListener = () => {}
;(0, eval)(pl.OBSERVER)
deliver(cases.entries)
process.stdout.write(JSON.stringify({
  calls: window.__benchCalls,
  load: pl.callOps(window.__benchCalls, { route: '/dashboard', visit: 'first', during: 'load' }, cases.loadT0, '3:7'),
  between: pl.callOps(window.__benchCalls, { route: '/dashboard', during: 'between' }),
  old: pl.callOps([{ resource: 'r', name: 'n', namespace: '', ms: 1, status: 200 }], {}),
  start: [pl.loadStart(null, 5), pl.loadStart({ t0: 9 }, 5)],
  source: pl.timingOf.toString(),
}))
"""
ORIGIN = 1793517678000.0
SECRET = "extras=%7B%22username%22%3A%22alice%22%7D"


def _entry(start, fetch, cs, ce, rq, rs, re_, proto="http/1.1", name="tables", dur=None):
    return {"name": f"http://gw/content/call?resource={name}&name=n1&namespace=ns&{SECRET}",
            "startTime": start, "fetchStart": fetch, "connectStart": cs, "connectEnd": ce, "requestStart": rq,
            "responseStart": rs, "responseEnd": re_, "duration": (re_ - start) if dur is None else dur,
            "responseStatus": 200, "nextHopProtocol": proto}


def _timing_node(tmp_path, entries, load_t0=ORIGIN + 1000):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_TIMING_DRIVER)
    (tmp_path / "cases.json").write_text(json.dumps({"entries": entries, "origin": ORIGIN, "loadT0": load_t0}))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "cases.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


@needs_node
def test_each_call_keeps_its_protocol_and_offsets_from_its_start_never_its_url(tmp_path):
    out = _timing_node(tmp_path, [
        _entry(1010.0, 1010.2, 1010.2, 1010.2, 1250.2, 1265.0, 1270.0),            # reused: waited 240 ms
        _entry(1012.0, 1012.1, 1012.3, 1013.9, 1014.0, 1030.0, 1031.0, name="flexes"),   # a new connection
        _entry(1020.0, 1020.0, 0, 0, 0, 0, 1050.0, proto="", name="cards"),          # cross-origin, no TAO
        {"name": "http://gw/static/app.js", "startTime": 5, "duration": 3}])
    c0, c1, c2 = out["calls"]
    assert (c0["protocol"], c0["start"], c0["fetch"], c0["connectStart"], c0["connectEnd"], c0["requestStart"],
            c0["responseStart"], c0["responseEnd"]) == ("http/1.1", ORIGIN + 1010.0, 0.2, 0.2, 0.2, 240.2, 255.0, 260.0)
    assert (c1["connectStart"], c1["connectEnd"]) == (0.3, 1.9)
    # not exposed (0 in the entry): null, never an offset of -startTime
    assert (c2["protocol"], c2["connectStart"], c2["requestStart"], c2["responseStart"], c2["fetch"]) == \
        ("", None, None, None, 0.0)
    name, v, tags, timing = out["load"][0]
    assert name == "browser_call_ms" and tags["call_protocol"] == "http/1.1" and tags["call_resource"] == "tables"
    m = json.loads(timing)
    assert m == {"fetch": 0.2, "connect_start": 0.2, "connect_end": 0.2, "request_start": 240.2,
                 "response_start": 255.0, "response_end": 260.0, "from_load_ms": 10.0, "load_id": "3:7"}
    assert calltiming.queue_ms(m) == 240.0
    assert calltiming.queue_ms(json.loads(out["load"][1][3])) is None             # its own connect
    between = json.loads(out["between"][0][3])
    assert "from_load_ms" not in between and "load_id" not in between            # not a load's call
    assert out["old"][0][3] is None and out["old"][0][2]["call_protocol"] == ""   # a call with no timing
    assert out["start"] == [5, 9]
    blob = json.dumps(out)
    assert "alice" not in blob and "extras" not in blob and "/call?" not in blob
    assert not out["source"].lstrip().startswith("export")                       # shipped into the page


def test_browser_js_puts_timing_and_the_load_id_in_metadata_never_tags():
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    drain = src[src.index("async function drainCalls"):]
    drain = drain[:drain.index("\n}\n")]
    assert "md.call_timing = timing" in drain and "delete md.call_timing" in drain
    assert drain.index("md.call_timing = timing") < drain.rindex("CALL.add(value, t)") < \
        drain.index("delete md.call_timing")
    loop = src[src.index("while (Date.now() < deadline && !switchDue("):]
    loop = loop[:loop.index("  } finally {")]
    assert loop.index("const loadId = beginLoad()") < loop.index("await item.click()")
    assert loop.index("loadStart(r, actionAt), loadId)") < loop.index("endLoad()")
    login = src[src.index("const loginAt = Date.now()"):src.index("while (Date.now() < deadline")]
    assert login.index("beginLoad()") < login.index("press('Enter')") < login.index("endLoad()")
    assert "exec.vu.metrics.metadata.load_id = id" in src
    # no url / name system tag still: the extras (the user) never reach a point
    import re
    assert re.search(r"systemTags: \[([^\]]*)\]", src).group(1).count("'url'") == 0


# ─── B2. the per-route report, through evaluate, run-20-shaped ──────────────

def _dashboard_first_loads(n_loads=3, calls=57, conns=4, service=20.0, chrome_extra=None, proto="http/1.1"):
    """Run 20b's /dashboard first visit: 57 /calls a load, all issued together
    on HTTP/1.1 with `conns` free connections: call i waits (i // conns) x
    `service` ms for one. Chrome's samples of the load: one Fetch per call,
    its CDP time `chrome_extra` ms past Resource Timing's responseStart."""
    pts, meta = [], []
    for L in range(n_loads):
        lid = f"1:{L + 1}"
        tags = dict(session="s1", **{"class": "0"}, route="/dashboard", visit="first", during="load")
        pts.append(_point("browser_page_load_ms", 1302.0 + L, session="s1", route="/dashboard", visit="first",
                          method="click", skeleton="yes", timeout="no", t0=str(int(ORIGIN) + L)))
        meta.append(("browser_page_load_ms", {"load_id": lid}))
        for i in range(calls):
            q = (i // conns) * service
            m = {"fetch": 0.1, "connect_start": 0.1, "connect_end": 0.1, "request_start": 0.1 + q,
                 "response_start": 0.1 + q + 15.0, "response_end": 0.1 + q + 20.0,
                 "from_load_ms": 5.0 + i * 0.5, "load_id": lid}
            if L == 0 and i < conns:               # the first load opens its connections
                m["connect_start"], m["connect_end"] = 0.2, 1.2
            pts.append(_point("browser_call_ms", m["response_end"], **tags, call_resource=f"w{i % 15}",
                              call_name=f"n{i}", call_namespace="ns", call_status="200", call_protocol=proto))
            meta.append(("browser_call_ms", {"call_timing": json.dumps(m)}))
            if chrome_extra is not None:
                pts.append(_point("browser_http_req_duration", m["response_start"] + chrome_extra,
                                  resource_type="Fetch", status="200", method="GET"))
                meta.append(("browser_http_req_duration", {"load_id": lid}))
    for p, (_, md) in zip(pts, meta):
        p["data"]["metadata"] = md
    return pts


def _browser_eval(tmp_path, pts):
    _write_out(tmp_path / "browser", "browser", pts, None, 0)
    return _evaluate(tmp_path, {"preflight.jsonl": [{"kind": "preflight"}]},
                     "--browser-out", str(tmp_path / "browser"), "--browser-sessions", "1")


def test_dashboard_first_visit_queueing_on_http11_through_evaluate(tmp_path):
    v = _browser_eval(tmp_path, _dashboard_first_loads(chrome_extra=50.0))
    r = v["report"]["browser"]["call_timing_report_only"]
    assert r["status"] == "present" and r["protocols"] == {"http/1.1": 171}
    d = r["by_route_visit"]["/dashboard first"]
    assert d["calls"] == 171 and d["reused_connection"] == 171 - 4 and d["new_connection"] == 4
    # per load: 20 x (4 x (0 + ... + 13) + 14) = 7,560 ms of waiting; the first load's 4 connects excluded
    assert d["queue_ms"]["sum_ms"] == pytest.approx(7560 * 3, abs=0.5)
    assert d["queue_ms"]["max_ms"] == 280.0
    g = d["chrome_gap"]
    assert g["loads"] == 3 and g["gap_ms"] == pytest.approx(3 * 57 * 50.0, abs=1)
    assert g["queue_share"] == pytest.approx(7560 * 3 / (3 * 57 * 50.0), abs=0.001)
    w = d["waterfall"]
    assert w["loads"] == 3 and w["first_call_start_ms"]["p50_ms"] == 5.0
    assert w["load_wall_ms"]["n"] == 3 and w["last_call_end_ms"]["max_ms"] == pytest.approx(5 + 56 * 0.5 + 300.1, abs=0.2)
    assert [c["call"] for c in w["calls"]][:2] == ["w0/ns/n0", "w1/ns/n1"]      # start order
    assert "alice" not in json.dumps(r)


def test_queueing_vanishes_with_enough_connections(tmp_path):
    """The same loads with a connection per call (what one HTTP/2 connection
    gives): queue 0 — measured, a real 0."""
    v = _browser_eval(tmp_path, _dashboard_first_loads(conns=57, chrome_extra=1.0, proto="h2"))
    d = v["report"]["browser"]["call_timing_report_only"]["by_route_visit"]["/dashboard first"]
    assert d["queue_ms"]["max_ms"] == 0.0 and d["protocols"] == {"h2": 171}


def test_a_gap_with_no_chrome_samples_is_unmeasured(tmp_path):
    v = _browser_eval(tmp_path, _dashboard_first_loads(chrome_extra=None))
    g = v["report"]["browser"]["call_timing_report_only"]["by_route_visit"]["/dashboard first"]["chrome_gap"]
    assert g["status"] == "UNMEASURED"


def test_run20_shaped_calls_without_timing_read_unmeasured(tmp_path):
    """Run 20's browser_call_ms: tags only (no call_protocol, no metadata)."""
    tags = dict(session="s1", route="/dashboard", visit="first", during="load", call_resource="cards",
                call_name="c", call_namespace="ns", call_status="200")
    v = _browser_eval(tmp_path, [_point("browser_call_ms", 27.7, **tags)] * 57)
    r = v["report"]["browser"]["call_timing_report_only"]
    assert r["status"] == "UNMEASURED" and "before #1089" in r["why"]


def test_a_gap_that_is_not_positive_has_no_share():
    loads = {"1:1": [({"response_start": 30.0}, {}, 10.0)]}
    g = calltiming._gap(loads, {"1:1": [20.0]})
    assert g["gap_ms"] == -10.0 and str(g["queue_share"]).startswith("UNMEASURED")
