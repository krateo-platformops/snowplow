"""The API server's flow control (APF), per phase — report only (#919).

run606 scrapes every `apiserver_flowcontrol_*` series of the bench cluster's
API server for the whole run (env -> collect; bench/sampler.py, kind "apf",
$OUT/apf.jsonl.gz). evaluate reports, per phase and per priority level and
flow schema:

  wait        apiserver_flowcontrol_request_wait_duration_seconds: requests
              that waited in APF's queues (by its `execute` label: "true" ran,
              "false" did not), their count, mean and p50 / p95 / p99 (the
              histogram's buckets, interpolated as Prometheus does);
  rejected    apiserver_flowcontrol_rejected_requests_total by reason;
  dispatched  apiserver_flowcontrol_dispatched_requests_total;
  inqueue_requests / executing_requests / executing_seats
              apiserver_flowcontrol_current_*: max and mean over the samples;
  nominal_limit_seats  (per priority level) the limit those are judged by.

The next paired run judges #920 (snowplow's client-side 5/s limit removed) by
the API server's load: this says whether snowplow's own LISTs waited in, or
were rejected by, the API server's queues.

A regional control plane has several replicas behind one endpoint, and each
scrape lands on one of them: process_start_time_seconds (the record's
`replica`) says which. Counters and histograms are differenced only between
two scrapes of the SAME replica (a new start time is a new replica or a
restart: never differenced across). A difference counts in a phase only when
both its scrapes are in that phase; one that crosses a phase boundary is
reported under `spanning` (from..to), never booked to either side, and a
phase with no such pair says its counters are NOT MEASURED (never 0). A gauge
is one replica's value at one moment (a 15 s sample: its max is the max of
the samples, not the true peak). A counter vector exports a series only
after its first increment: rejected_requests_total absent from a replica's
scrapes means none rejected since that replica started, said as such.
"""

from __future__ import annotations

import functools
import re
from collections import defaultdict
from typing import Iterable

PREFIX = "apiserver_flowcontrol_"
WAIT = PREFIX + "request_wait_duration_seconds"
REJECTED = PREFIX + "rejected_requests_total"
DISPATCHED = PREFIX + "dispatched_requests_total"
GAUGES = {"inqueue_requests": PREFIX + "current_inqueue_requests",
          "executing_requests": PREFIX + "current_executing_requests",
          "executing_seats": PREFIX + "current_executing_seats"}
# per priority level: its limit, and the high watermark of its seat demand
# over each adjustment period (the peak a 15 s scrape alone would miss)
PL_GAUGES = {"nominal_limit_seats": PREFIX + "nominal_limit_seats",
             "demand_seats_high_watermark": PREFIX + "demand_seats_high_watermark"}
# derived per scrape: executing + in-queue requests of one flow schema (or
# level) at that moment
INFLIGHT = "inflight_requests"
QUANTILES = (50, 95, 99)
BEFORE = "before_measure"
BOOT = "boot"
REPORT_ABSENT = "absent"

_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


@functools.lru_cache(maxsize=None)          # a run repeats a few thousand keys millions of times
def parse_key(key: str) -> tuple[str, dict]:
    """`name{a="x",b="y"}` -> (name, {a: x, b: y})."""
    i = key.find("{")
    if i < 0:
        return key, {}
    labels = {k: v.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")
              for k, v in _LABEL.findall(key[i + 1:key.rfind("}")])}
    return key[:i], labels


def _le(v: str) -> float:
    return float("inf") if v == "+Inf" else float(v)


def quantile(q: float, buckets: dict[float, float]) -> float | None:
    """Prometheus' histogram_quantile over cumulative buckets {le: count}."""
    les = sorted(buckets)
    if not les or not buckets[les[-1]]:
        return None
    total = buckets[les[-1]]
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le in les:
        c = buckets[le]
        if c >= rank:
            if le == float("inf"):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return prev_le


class _Phase:
    def __init__(self):
        self.scrapes = 0
        self.failed = 0
        self.replicas: set = set()
        self.pairs = 0
        self.pair_seconds = 0.0
        self.counters: dict[str, float] = defaultdict(float)     # series key -> delta
        self.gauges: dict[tuple, list[float]] = defaultdict(list)  # (out name, pl, fs) -> values

    def add_gauges(self, series: dict):
        per_pl: dict[tuple, float] = defaultdict(float)
        inflight: dict[tuple, float] = defaultdict(float)
        for key, v in series.items():
            name, lb = parse_key(key)
            for out, fam in list(GAUGES.items()) + list(PL_GAUGES.items()):
                if name == fam:
                    pl, fs = lb.get("priority_level", ""), lb.get("flow_schema", "")
                    self.gauges[(out, pl, fs)].append(v)
                    if fs:
                        per_pl[(out, pl)] += v
                        if out in ("inqueue_requests", "executing_requests"):
                            inflight[(pl, fs)] += v
                            inflight[(pl, None)] += v
        for (out, pl), v in per_pl.items():
            self.gauges[(out, pl, None)].append(v)       # the level's total at that moment
        for (pl, fs), v in inflight.items():
            self.gauges[(INFLIGHT, pl, fs)].append(v)

    def merge(self, other: "_Phase"):
        self.scrapes += other.scrapes
        self.failed += other.failed
        self.replicas |= other.replicas
        self.pairs += other.pairs
        self.pair_seconds += other.pair_seconds
        for k, v in other.counters.items():
            self.counters[k] += v
        for k, v in other.gauges.items():
            self.gauges[k] += v


def _stats(vals: list[float]) -> dict:
    return {"max": max(vals), "mean": round(sum(vals) / len(vals), 3), "samples": len(vals)}


def _phase_out(ph: _Phase) -> dict:
    levels: dict[str, dict] = {}

    def node(pl, fs):
        lv = levels.setdefault(pl, {"flow_schemas": {}})
        return lv if fs is None else lv["flow_schemas"].setdefault(fs, {})

    hist: dict[tuple, dict] = defaultdict(lambda: {"buckets": defaultdict(float), "sum": 0.0, "count": 0.0})
    rejected_seen = False
    for key, d in ph.counters.items():
        name, lb = parse_key(key)
        pl, fs = lb.get("priority_level", ""), lb.get("flow_schema", "")
        if name.startswith(WAIT + "_"):
            ex = lb.get("execute", "")
            for tgt in ((pl, fs, ex), (pl, None, ex)):
                h = hist[tgt]
                if name == WAIT + "_bucket":
                    h["buckets"][_le(lb.get("le", "+Inf"))] += d
                elif name == WAIT + "_sum":
                    h["sum"] += d
                elif name == WAIT + "_count":
                    h["count"] += d
        elif name == REJECTED:
            rejected_seen = True
            for n in (node(pl, fs), node(pl, None)):
                r = n.setdefault("rejected", {})
                reason = lb.get("reason", "")
                r[reason] = r.get(reason, 0) + int(d)
        elif name == DISPATCHED:
            for n in (node(pl, fs), node(pl, None)):
                n["dispatched"] = n.get("dispatched", 0) + int(d)
    for (pl, fs, ex), h in hist.items():
        n = h["count"]
        w = {"requests": int(n), "mean_s": round(h["sum"] / n, 6) if n else None}
        for q in QUANTILES:
            v = quantile(q / 100, dict(h["buckets"])) if n else None
            w[f"p{q}_s"] = round(v, 6) if v is not None else None
        node(pl, fs).setdefault("wait", {})["executed" if ex == "true" else
                                             "not_executed" if ex == "false" else ex] = w
    for (out, pl, fs), vals in ph.gauges.items():
        if vals:
            node(pl, fs if fs else None)[out] = _stats(vals) if out not in PL_GAUGES else max(vals)
    return {"scrapes": ph.scrapes, "failed_scrapes": ph.failed, "replicas": len(ph.replicas),
            "same_replica_pairs": ph.pairs, "pair_seconds": round(ph.pair_seconds, 1),
            "counters": ("measured" if ph.pairs else
                         "NOT MEASURED: no two scrapes of one replica inside the phase (rejections, "
                         "waits, dispatched unknown, never 0)"),
            "rejected_series_seen": rejected_seen,
            "priority_levels": {k: levels[k] for k in sorted(levels)}}


def report(recs: Iterable[dict], phases: list[tuple[str, float, float]], phase_of,
           measure_t0: float | None, boot: tuple[float, float] | None = None) -> dict:
    """The APF report over the run's `apf` records, in the order the sampler
    wrote them (one stream: time order), consumed ONE AT A TIME — a run's
    scrapes hold millions of series in all; only each replica's last scrape
    (its reported families) is kept. `phases` / `phase_of`:
    load606.run_phases and its _phase_of; `measure_t0`: when the measure
    began (samples before it are BEFORE, not the storm whose window opens at
    -inf); `boot`: snowplow's boot window (its restart to its new pod's
    Ready), its own phase (#922 F3).
    """
    def phase(t):
        if boot is not None and boot[0] <= t < boot[1]:
            return BOOT
        return BEFORE if measure_t0 is not None and t < measure_t0 else phase_of(t, phases)
    # every phase edge: where the phase a time falls in can change
    edges = sorted({e for _, a0, a1 in phases for e in (a0, a1) if e not in (float("inf"), float("-inf"))}
                   | ({measure_t0} if measure_t0 is not None else set()) | (set(boot) if boot else set()))

    def crossed(t0, t1):
        """The phases from t0 to t1, in order (each edge in (t0, t1] may
        change it): one name when no phase boundary lies between."""
        seq = [phase(t0)]
        for e in edges:
            if t0 < e <= t1 and phase(e) != seq[-1]:
                seq.append(phase(e))
        if phase(t1) != seq[-1]:
            seq.append(phase(t1))
        return seq
    acc: dict[str, _Phase] = defaultdict(_Phase)
    spanning: dict[str, _Phase] = defaultdict(_Phase)
    last: dict = {}                       # replica -> (t, its reported series)
    answered, resets, unattributed, truncated = 0, 0, 0, None
    replicas: set = set()
    errors: dict[str, int] = defaultdict(int)
    for r in recs:
        if r.get("kind") == "skipped" and r.get("truncated"):
            truncated = r["truncated"]
        if r.get("kind") != "apf" or r.get("t") is None:
            continue
        p = acc[phase(r["t"])]
        if r.get("failed"):
            p.failed += 1
            errors[r.get("error") or f"rc={r.get('rc')}"] += 1
            continue
        answered += 1
        series = {k: v for k, v in (r.get("series") or {}).items() if _reported(k)}
        rep = r.get("replica")
        p.scrapes += 1
        p.replicas.add(rep)
        p.add_gauges(series)
        if rep is None:
            unattributed += 1             # which replica answered is unknown: never differenced
            continue
        replicas.add(rep)
        prev = last.get(rep)
        last[rep] = (r["t"], series)
        if prev is None:
            continue
        d = {k: v - prev[1].get(k, 0.0) for k, v in series.items() if _cumulative(k)}
        if any(v < 0 for v in d.values()):
            resets += 1                   # a counter went back: not differenced
            continue
        # a difference counts in a phase only when no phase boundary lies
        # between its two scrapes; one that crosses any (a neighbour, or a
        # whole phase between two stretches of another: burst inside a churn
        # step) is reported as spanning (the phases it crossed, in order),
        # never booked to any of them
        seq = crossed(prev[0], r["t"])
        q = p if len(seq) == 1 else spanning["..".join(seq)]
        q.pairs += 1
        q.pair_seconds += r["t"] - prev[0]
        for k, v in d.items():
            if v:
                q.counters[k] += v
    if not acc:
        return {"status": REPORT_ABSENT,
                "note": "no apf sample in the inputs: the API server's flow-control metrics are MISSING "
                        "(run606 before #919, or the sampler did not run), not zero"}
    failed = sum(p.failed for p in acc.values())
    order = [n for n in (BEFORE, BOOT) if n in acc] + [n for n, _, _ in phases] + ["steady"]
    by_phase = {n: _phase_out(acc[n]) for n in order if n in acc}
    churn = [n for n in acc if n.startswith("churn:")]
    # pooled: the churn steps (with the differences spanning only churn steps)
    # and the whole run (every difference)
    for name, members, spans in (("churn", churn, [k for k in spanning if all(
            x.startswith("churn:") for x in k.split(".."))]), ("all", list(acc), list(spanning))):
        if members:
            s = _Phase()
            for n in members:
                s.merge(acc[n])
            for k in spans:
                s.merge(spanning[k])
            by_phase[name] = _phase_out(s)
    return {"status": "present",
            "samples": {"answered": answered, "failed": failed, "failed_by_error": dict(errors)},
            "replicas": sorted(replicas),
            "unattributed_scrapes": unattributed,
            "counter_resets_skipped": resets,
            "measure_t0": measure_t0,
            "boot": list(boot) if boot else None,
            # differences whose two scrapes are in different phases: in no phase
            "spanning": {k: _phase_out(v) for k, v in sorted(spanning.items())},
            **({"stream_cut": truncated} if truncated else {}),
            "how": "counters / histograms differenced between consecutive scrapes of the same replica "
                   "(process_start_time_seconds); a difference counts in a phase only when both its scrapes "
                   "are in it, else it is reported under `spanning` (from..to); a phase with no such pair "
                   "has its counters NOT MEASURED (never 0); gauges: one "
                   "replica's value per scrape; rejected_requests_total absent = none rejected since that "
                   "replica started (a counter vector exports a series on its first increment); a gauge series "
                   "is exported from its first use, so its mean is over the scrapes that carried it",
            "by_phase": by_phase}


def _cumulative(key: str) -> bool:
    name = key.split("{", 1)[0]
    return name.endswith(("_total", "_bucket", "_sum", "_count"))


_REPORTED = (WAIT + "_", REJECTED + "{", DISPATCHED + "{",
             *(g + "{" for g in list(GAUGES.values()) + list(PL_GAUGES.values())))


def _reported(key: str) -> bool:
    """A series of a family this report reads (the sampler keeps every
    apiserver_flowcontrol_* series; the rest stay in apf.jsonl.gz)."""
    return key.startswith(_REPORTED)
