"""#1089: the TCP path under snowplow, per phase — REPORT ONLY.

Run 20 inferred, without evidence, that snowplow's 30 s stage dials (#1054)
were TCP connects whose SYN was never answered. Three sources say whether:

  snowplow_pod_tcp        the snowplow pod's network namespace (netns606, read
                          node-side): TcpExt.TCPSynRetrans, Tcp.RetransSegs,
                          Tcp.ActiveOpens, Tcp.AttemptFails
  node_conntrack          the snowplow node's nf_conntrack insert_failed / drop /
                          early_drop (and nf_conntrack_count against _max), and the
                          dataplane seen there (kube-proxy / cilium-agent)
  apiserver_request_codes apiserver_request_total summed by code (the APF
                          sampler's /metrics scrape): retried 5xx / 429. Every
                          client's requests, not only snowplow's.

Counters are differenced between two consecutive samples of the SAME source
(the pod's network namespace; the node's boot; the API server replica), never
across one (a new namespace, a reboot, another replica): a value that went
back is a reset, skipped and counted. A difference counts in a phase only when
both its samples are in it (bench/apf.PhaseClock); one that crosses a phase
boundary is reported under `spanning`, never booked to either side. A phase
with no such pair, or a field no sample carried, reads UNMEASURED — never 0.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from bench.apf import PhaseClock

UNMEASURED = "UNMEASURED"
TCP_FIELDS = ("TCPSynRetrans", "RetransSegs", "ActiveOpens", "AttemptFails")
CONNTRACK_FIELDS = ("insert_failed", "drop", "early_drop")
NOTE = ("report only: counters differenced between consecutive samples of one source (netns / node boot / "
        "API-server replica), booked to a phase only when both samples are in it (else `spanning`); a phase "
        "with no such pair, or a field no sample carried, is UNMEASURED, never 0")


class _Acc:
    def __init__(self):
        self.samples = 0
        self.pairs = 0
        self.pair_seconds = 0.0
        self.counters: dict[str, float] = defaultdict(float)
        self.seen: set[str] = set()                # fields some pair differenced
        self.gauges: dict[str, list[float]] = defaultdict(list)

    def merge(self, o: "_Acc"):
        self.samples += o.samples
        self.pairs += o.pairs
        self.pair_seconds += o.pair_seconds
        for k, v in o.counters.items():
            self.counters[k] += v
        self.seen |= o.seen
        for k, v in o.gauges.items():
            self.gauges[k] += v


def _out(a: _Acc, fields: tuple[str, ...] | None, gauges: tuple[str, ...] = ()) -> dict:
    keys = fields if fields is not None else tuple(sorted(a.seen))
    d = {"samples": a.samples, "pairs": a.pairs, "pair_seconds": round(a.pair_seconds, 1)}
    if not a.pairs:
        d["counters"] = (UNMEASURED + ": no two samples of one source inside the phase") if fields is None \
            else {k: UNMEASURED for k in keys}
    else:
        d["counters"] = {k: (int(a.counters[k]) if k in a.seen else UNMEASURED) for k in keys}
    for g in gauges:
        vals = a.gauges.get(g)
        d[f"{g}_max"] = max(vals) if vals else UNMEASURED
    return d


def by_phase(samples: Iterable[tuple[float, object, dict, dict]], clock: PhaseClock,
             fields: tuple[str, ...] | None, gauges: tuple[str, ...] = (), absent_is_zero: bool = False) -> dict:
    """samples: (t, source, {counter: value}, {gauge: value}) in time order.
    `fields`: the counters every phase names (None: whatever was seen).
    `absent_is_zero`: a counter a sample lacks counts as 0 there (a Prometheus
    counter vector exports a series only from its first increment); otherwise
    a counter missing from either sample of a pair is not differenced."""
    acc: dict[str, _Acc] = defaultdict(_Acc)
    spanning: dict[str, _Acc] = defaultdict(_Acc)
    last: dict = {}
    resets = 0
    sources = set()
    for t, src, c, g in samples:
        ph = clock.phase(t)
        a = acc[ph]
        a.samples += 1
        for k, v in (g or {}).items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                a.gauges[k].append(v)
        sources.add(src)
        prev = last.get(src)
        last[src] = (t, c)
        if prev is None:
            continue
        keys = set(c) | (set(prev[1]) if absent_is_zero else set())
        if not absent_is_zero:
            keys &= set(prev[1])
        d = {k: c.get(k, 0) - prev[1].get(k, 0) for k in keys}
        if any(v < 0 for v in d.values()):
            resets += 1
            continue
        seq = clock.crossed(prev[0], t)
        q = a if len(seq) == 1 else spanning["..".join(seq)]
        q.pairs += 1
        q.pair_seconds += t - prev[0]
        for k, v in d.items():
            q.counters[k] += v
            q.seen.add(k)
    out = {"sources": len(sources), "counter_resets_skipped": resets,
           "by_phase": {n: _out(acc[n], fields, gauges) for n in clock.order(acc)},
           "spanning": {k: _out(v, fields, gauges) for k, v in sorted(spanning.items())}}
    churn = [n for n in acc if n.startswith("churn:")]
    for name, members, spans in (("churn", churn, [k for k in spanning if all(
            x.startswith("churn:") for x in k.split(".."))]), ("all", list(acc), list(spanning))):
        if members:
            s = _Acc()
            for n in members:
                s.merge(acc[n])
            for k in spans:
                s.merge(spanning[k])
            out["by_phase"][name] = _out(s, fields, gauges)
    return out


def snowplow_uids(pod_records: list[dict]) -> list[str]:
    """The snowplow pod UIDs the bench recorded (snowplow_pod before / after)."""
    return sorted({r["uid"] for r in pod_records if r.get("kind") == "snowplow_pod" and r.get("uid")})


def _absent(why: str) -> dict:
    return {"status": UNMEASURED, "why": why}


def pod_tcp(netns: list[dict], uids: list[str], clock: PhaseClock) -> dict:
    ok = [r for r in netns if not r.get("failed")]
    if not netns:
        return _absent("no netns606 sample: the node-side sampler did not run (a run before #1089) "
                       "or $OUT/netns.jsonl was not collected")
    if not ok:
        return {**_absent(f"all {len(netns)} netns606 samples failed"),
                "errors": sorted({str(r.get("error")) for r in netns})[:5]}
    if not uids:
        return _absent("no snowplow_pod record carries the pod's UID (a run before #1089): snowplow's "
                       "network namespace cannot be told from the others on its node")
    want = set(uids)
    rows, errors = [], defaultdict(int)
    for r in ok:
        for ns in r.get("namespaces") or []:
            hit = want & set(ns.get("pod_uids") or [])
            if not hit or ns.get("host"):
                continue
            for f, e in (ns.get("errors") or {}).items():
                errors[f"{f}: {e}"] += 1
            rows.append((r["t"], (ns.get("netns"), min(hit)), ns.get("tcp") or {}, {}))
    if not rows:
        return {**_absent(f"no sampled network namespace belongs to the snowplow pod(s) "
                          f"({len(uids)} UID(s) recorded): it ran on another node, or the sampler never saw it"),
                "samples": len(ok)}
    rep = by_phase(rows, clock, TCP_FIELDS)
    return {"status": "present", "source": "netns606: /proc/<pid>/net/{netstat,snmp} of the snowplow pod's "
                                           "network namespace, read node-side (hostPID)",
            "pod_uids": uids, "samples": len(rows), "read_errors": dict(errors), **rep}


def node_conntrack(netns: list[dict], clock: PhaseClock) -> dict:
    ok = [r for r in netns if not r.get("failed")]
    if not ok:
        return _absent("no answered netns606 sample: the snowplow node's conntrack was not read")
    rows, errors = [], defaultdict(int)
    dp = {"kube_proxy": False, "cilium_agent": False}
    for r in ok:
        n = r.get("node") or {}
        for k in dp:
            dp[k] = dp[k] or bool((n.get("dataplane") or {}).get(k))
        if n.get("conntrack") is None:
            errors[str(n.get("conntrack_error"))] += 1
            continue
        g = {k: n.get(k) for k in ("nf_conntrack_count", "nf_conntrack_max")}
        rows.append((r["t"], n.get("boot_id"), n["conntrack"], g))
    if not rows:
        return {**_absent("no netns606 sample read /proc/net/stat/nf_conntrack"), "errors": dict(errors),
                "dataplane": dp}
    rep = by_phase(rows, clock, CONNTRACK_FIELDS, gauges=("nf_conntrack_count", "nf_conntrack_max"))
    note = None
    if dp["cilium_agent"]:
        note = ("cilium-agent runs on the node (GKE Dataplane V2): Service NAT uses Cilium's BPF conntrack, "
                "which nf_conntrack does not count — its drops are not in these figures")
    elif not dp["kube_proxy"]:
        note = "neither kube-proxy nor cilium-agent was seen on the node: which conntrack NATs Services is unknown"
    return {"status": "present", "source": "netns606: the node's /proc/net/stat/nf_conntrack (summed over CPUs)",
            "dataplane": dp, **({"dataplane_note": note} if note else {}),
            "unread_samples": dict(errors), **rep}


def apiserver_codes(apf_recs: Iterable[dict], clock: PhaseClock) -> dict:
    rows, scrapes, without = [], 0, 0

    def gen():
        nonlocal scrapes, without
        for r in apf_recs:
            if r.get("kind") != "apf" or r.get("failed") or r.get("t") is None:
                continue
            scrapes += 1
            codes = r.get("requests_by_code")
            if codes is None or r.get("replica") is None:
                without += 1
                continue
            yield (r["t"], r["replica"], codes, {})
    rep = by_phase(gen(), clock, None, absent_is_zero=True)
    if not scrapes:
        return _absent("no answered apf scrape: the API server's /metrics was not sampled")
    if scrapes == without:
        return _absent(f"none of the {scrapes} apf scrapes carries requests_by_code (a run before #1089, or "
                       f"no replica attributed): the API server's codes are not known")
    for ph in list(rep["by_phase"].values()) + list(rep["spanning"].values()):
        c = ph["counters"]
        if isinstance(c, dict):
            ph["5xx"] = sum(v for k, v in c.items() if k.startswith("5"))
            ph["429"] = c.get("429", 0)
            ph["total"] = sum(c.values())
        else:
            ph["5xx"] = ph["429"] = ph["total"] = UNMEASURED
    return {"status": "present", "source": "apiserver_request_total summed by code, from the APF sampler's "
                                           "/metrics scrape; every client's requests, not only snowplow's",
            "scrapes": scrapes, "scrapes_without_codes": without, **rep}


def sampler_health(netns: list[dict]) -> dict:
    """What the unprivileged sampler could read (#1104 review B1): processes
    whose network namespace it could not resolve (about 0 expected: its
    CAP_SYS_PTRACE + AppArmor Unconfined reach every process), whether it saw
    the host's namespace, and the samples that failed whole."""
    ok = [r for r in netns if not r.get("failed")]
    if not netns:
        return _absent("no netns606 sample")
    un = [r["processes_unreadable"] for r in ok if _isnum(r.get("processes_unreadable"))]
    return {"samples": len(netns), "failed": len(netns) - len(ok),
            "processes_max": max((r.get("processes") or 0 for r in ok), default=None),
            "processes_unreadable_max": max(un) if un else UNMEASURED,
            "host_namespace_seen": any(ns.get("host") for r in ok for ns in r.get("namespaces") or [])}


def _isnum(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def report(recs: list[dict], apf_recs: Iterable[dict], phases: list[tuple[str, float, float]], phase_of,
           measure_t0: float | None, boot: tuple[float, float] | None = None) -> dict:
    """verdict.json's tcp_path_report_only. `recs`: evaluate's records (the
    netns606 samples among them, kind netns; the snowplow_pod records);
    `apf_recs`: the APF stream (read once more, record by record)."""
    clock = PhaseClock(phases, phase_of, measure_t0, boot)
    netns = sorted((r for r in recs if r.get("kind") == "netns" and r.get("t") is not None), key=lambda r: r["t"])
    return {"note": NOTE,
            "sampler": sampler_health(netns),
            "snowplow_pod_tcp": pod_tcp(netns, snowplow_uids(recs), clock),
            "node_conntrack": node_conntrack(netns, clock),
            "apiserver_request_codes": apiserver_codes(apf_recs, clock)}
