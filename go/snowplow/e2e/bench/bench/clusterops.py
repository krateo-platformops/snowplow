"""#1422: the GKE operations of a run's own cluster, recorded with the run and marked on its phases.

Run 26i (2026-11-14): GKE resized the control plane of the bench clusters by itself (operation type
RESIZE_CLUSTER, "initiated by GKE", node count unchanged), at different times per arm: twice on N2
(09:23:45Z to 09:32:18Z, 09:32:45Z to 09:41:18Z), once on N1 (from 11:12:21Z), not at all on its replica
N1B by then. The bench recorded none of it, so the report could not say whether an arm's measured phase
ran while its control plane was being replaced.

Three parts, one module:

  - `read()` / `record` (run606's collect): `gcloud container operations list` for the cluster's region,
    kept to the operations whose target is this cluster or one of its node pools (exact name, exact
    location, from the operation's own targetLink), each as its id, type, status, start and end. The
    listing is a read that works whether or not the cluster still exists (GKE keeps a deleted cluster's
    operations in the list). A listing that fails is recorded as UNREADABLE with its reason, and its
    `operations` is null: never an empty list. No client-side --filter: gcloud applies one after the
    fact and answers [] with rc 0 for a filter that matches nothing, which here would read as "none".
  - `report()` (bench.load606 evaluate, report["cluster_operations_report_only"]): for every measured
    phase evaluate knows the start and end of, one of three outcomes that never share a wording:
    NONE ("no operation overlapped"), OVERLAPPED ("overlapped by: <type> <start>..<end>") and UNKNOWN
    ("operations could not be read: <why>"). An operation overlaps a phase when it started before the
    phase ended AND ended after the phase started; one still running at the read (no end time) overlaps
    everything after its start.
  - `pair()` (bench.custlat pair and replica --in-run): the two arms' outcomes per phase, side by side,
    naming the phases where ONE arm's measurement overlapped an operation and the other's did not.

THE POSITIVE CONTROL. A listing that answers cleanly can still be the wrong one, or a short one: an empty
list, another region's, one that holds only other clusters' operations, one without this cluster's
beginning. Each would read "no operation overlapped". So a record is readable only when the listing
PROVES it is this cluster's: it must hold the operation the bench itself caused, the cluster's own
CREATE_CLUSTER, by exact target (name and region) AND by its start, within CONTROL_SKEW_S of the
creation time GKE reported for this cluster when gke-up read it back (Cluster.createTime, kept in
maintenance-exclusion.json as `cluster_created`). The creation time tells this cluster from an
earlier one of the same name. The record names the operation that matched (`control`). Without it
the record is UNREADABLE, saying which control was missing. Operations that started before the
control's are an earlier cluster's of the same name, and are not this run's: they are left out.

The owner's ruling of 2026-11-16 (#1422: "warm up until settled, then measure") adds two parts:

  - `settle()` (run606's settle step, before the measured reboot): the arm drives its real load on the
    loaded cluster (the warm-up driver Job) until GKE's control plane has SETTLED: nothing runs on the
    cluster, and nothing ran in the last two of GKE's automatic checks under that load. Settled is
    decided on a readable listing only (an unreadable one is never "settled"). A deadline DERIVED from
    the operations GKE has run on bench clusters (`cadence()`, gke_operations_evidence.json) bounds it;
    not settled by then, the arm is INVALID by environment, named, and is not measured.
  - `validity()` (evaluate): any operation of the cluster that overlaps the measure (the reboot to the
    drivers' last record, and each measured phase in it) makes the arm INVALID by environment, named;
    an overlap that cannot be decided (no record, an unreadable one, a measure whose own bounds are not
    recorded) makes it INVALID too (undecided), never valid. An INVALID arm voids its pair (custlat).

What GKE's checks look like (RUN, every cluster-operations.json of runs 26j/27c, and run 26i's listing):
GKE starts a RESIZE_CLUSTER by itself; it re-checks a cadence after the previous one started (300 s
on run 27c N2: 15:40:24, 15:45:25, 15:50:25Z), later when the previous one is still running, and a
check that changes nothing ends in about 1 s. A check that decides to do nothing at all leaves no
operation: run 27c N1B shows one resize and nothing after it for hours. So "the last two checks were
no-ops" is read from the listing as: no operation started or ran in the last two cadence periods, and
none runs now. A ~1 s RESIZE_CLUSTER inside that window is not taken for a no-op: the listing holds no
field saying it changed nothing, its duration is the only sign, and no bound on that duration has a
derivation (run 27c N2 holds a 31 s one). Being strict costs one window after a burst's last check.

Shapes (each read from the real thing, never guessed):
  - an Operation as `gcloud container operations list --format json` prints it: read 2026-11-14 from
    the region of run 26i's clusters (SDK 561). Keys: name, operationType, status, startTime, endTime
    (absent while it runs), targetLink, selfLink, zone (the REGION for a regional cluster), progress
    (CREATE_CLUSTER only). Times are RFC 3339 with up to nine fractional digits and a Z.
    targetLink is the GKE API's own URL of the target, ending /projects/<project NUMBER>/locations/
    <location>/clusters/<name>[/nodePools/<pool>]: the record keeps the target's kind and pool, never the link
    (the project number is not in the run's evidence otherwise).
  - Operation.Status and Operation.Type: the GKE API reference,
    https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.operations
    (RESIZE_CLUSTER: "The control plane is being resized. This operation type is initiated by GKE.").
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from itertools import pairwise
from typing import Callable

from bench import gke
from bench.identity import error_summary, redact_identity

KIND = "cluster_operations"
SECTION = "cluster_operations_report_only"
# the three outcomes of a phase; a report line carries exactly one of the three wordings below
NONE, OVERLAPPED, UNKNOWN = "NONE", "OVERLAPPED", "UNKNOWN"
SAID_NONE = "no operation overlapped"
SAID_OVERLAPPED = "overlapped by: "
SAID_UNKNOWN = "operations could not be read: "
UNREADABLE_PREFIX = "could not be read: "
# one listing answers in seconds (225 operations of the region: under 3 s on 2026-11-14); a call past
# this is no answer, and is recorded as one
LIST_TIMEOUT_S = 120.0
CONTROL_TYPE = "CREATE_CLUSTER"
# how far the cluster's CREATE_CLUSTER may start from the createTime GKE reports for the cluster. Run 26i
# (2026-11-14): createTime 08:54:10Z, its CREATE_CLUSTER's startTime 08:54:14.07Z, 4 s apart. Two clusters
# of one name are never this close: the earlier one had to finish being created (about 6 minutes, same run)
# and be deleted before the name was free.
CONTROL_SKEW_S = 120.0
CREATED_KEY = "cluster_created"

# the one form GKE gave every target of the 225 operations read on 2026-11-14 (a regional listing)
_TARGET = re.compile(r"/projects/[^/]+/locations/([^/]+)/clusters/([^/]+)(?:/nodePools/([^/]+))?$")
# gcloud's own log lines on stderr: a listing that exits 0 with one did not answer cleanly (a partial
# result is announced this way, as `container clusters list` announces its missing zones)
_GCLOUD_COMPLAINT = re.compile(r"^(ERROR|WARNING):", re.M)


def epoch(ts) -> float | None:
    """An RFC 3339 time (any number of fractional digits, Z or an offset) as unix seconds; None for
    anything else (an absent endTime: the operation has not ended)."""
    return gke.rfc3339_epoch(ts)


def stamp(t: float | None) -> str:
    return "?" if t is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def list_command(spec: gke.ClusterSpec) -> list[str]:
    """Every operation GKE lists for the cluster's region (a read; the cluster need not exist)."""
    return ["gcloud", "container", "operations", "list", "--project", spec.project, "--region", spec.region,
            "--format", "json"]


class Ambiguous(ValueError):
    """An operation whose target cannot be told to be this cluster's or not."""


def target_of(op: dict, spec: gke.ClusterSpec) -> str | None:
    """"cluster" or "nodePool/<pool>" when `op` targets THIS cluster (exact name, in its region) or one
    of its node pools; None for another target. Raises Ambiguous for an operation that cannot be placed:
    no target link, a link that names this cluster in any other form than
    .../locations/<region>/clusters/<name>[/nodePools/<pool>] (a zonal form, a trailing slash), or this
    cluster's name in another location (the listing asked for is the region's). Dropping such an
    operation would read as "it did not happen"."""
    link = op.get("targetLink")
    if not isinstance(link, str) or not link:
        raise Ambiguous("it has no target link")
    m = _TARGET.search(link)
    if m is None:
        if spec.name in link.split("/"):
            raise Ambiguous(f"its target names {spec.name} in a form that is not "
                            f".../locations/{spec.region}/clusters/{spec.name}")
        return None
    if m.group(2) != spec.name:
        return None
    if m.group(1) != spec.region:
        raise Ambiguous(f"its target is {spec.name} in {m.group(1)}, and the listing asked for is {spec.region}'s")
    return f"nodePool/{m.group(3)}" if m.group(3) else "cluster"


def operation(op: dict, target: str) -> dict:
    """One operation as the run records it: nothing of it but its id, type, status, target and times."""
    return {"id": op.get("name"), "type": op.get("operationType"), "status": op.get("status"), "target": target,
            "start": op.get("startTime"), "end": op.get("endTime"),
            "start_t": epoch(op.get("startTime")), "end_t": epoch(op.get("endTime"))}


def _unreadable(spec: gke.ClusterSpec, now: float, why: str) -> dict:
    return {"kind": KIND, "cluster": spec.name, "region": spec.region, "read_at": now, "readable": False,
            "unreadable": UNREADABLE_PREFIX + why, "operations": None,
            "source": " ".join(list_command(spec)[:4])}


def created_from(path: str | None) -> tuple[float | None, str]:
    """(the cluster's creation time as GKE reported it to gke-up, "") or (None, why it is not known),
    from the record gke-up wrote when it read the cluster back (maintenance-exclusion.json)."""
    if not path:
        return None, "the cluster's creation time was not given (--created-from)"
    try:
        with open(path) as fh:
            rec = json.load(fh)
    except OSError as e:
        return None, f"the cluster's creation time is not recorded ({path}: {e.strerror})"
    except ValueError:
        return None, f"the cluster's creation time is not recorded ({path} is not JSON)"
    t = epoch(rec.get(CREATED_KEY)) if isinstance(rec, dict) else None
    if t is None:
        return None, f"the cluster's creation time is not recorded ({path} holds no readable {CREATED_KEY})"
    return t, ""


def control_of(ops: list[dict], created: float) -> dict | None:
    """The cluster's own CREATE_CLUSTER among its operations: the one that started within CONTROL_SKEW_S
    of the creation time GKE reported for the cluster (the nearest, should two qualify)."""
    near = [o for o in ops if o["type"] == CONTROL_TYPE and o["target"] == "cluster" and o["start_t"] is not None
            and abs(o["start_t"] - created) <= CONTROL_SKEW_S]
    return min(near, key=lambda o: abs(o["start_t"] - created)) if near else None


def read(spec: gke.ClusterSpec, *, created: float | None, created_why: str = "", run=None,
         now: Callable[[], float] = time.time, timeout: float = LIST_TIMEOUT_S) -> dict:
    """The cluster's operations record. Fails closed: a listing that exits non-zero, times out, answers
    no JSON list, or exits 0 with a gcloud ERROR / WARNING on its stderr is UNREADABLE, with the reason
    (the code only for an error: gcloud's text names the operator's account), and `operations` null.
    So is one that does not hold the positive control (module doc): this cluster's own CREATE_CLUSTER,
    by exact target and by `created`, the creation time GKE reported for it (None: not known, and then
    no listing can be proven to be this cluster's)."""
    run = run or gke.execute
    argv = list_command(spec)
    err: list[str] = []
    try:
        # the compute partial-error property off, as on every compute list (gke.COMPUTE_LIST_ENV): a lister
        # that swallows a failed request answers rc 0 with [] while it is true. This listing is the container
        # API's, so its own fail-open forms are closed here: a complaint on stderr (GKE's missing zones) is
        # UNREADABLE, and a listing that lacks the cluster's own CREATE_CLUSTER (the positive control) is too.
        rc, out = run(argv, dry_run=False, log=lambda _m: None, stderr_out=err, timeout=timeout,
                      env=dict(os.environ, **gke.COMPUTE_LIST_ENV))
    except subprocess.TimeoutExpired:
        return _unreadable(spec, now(), f"{' '.join(argv[:4])} did not answer within {timeout:g} s")
    text = err[-1] if err else ""
    if rc != 0:
        return _unreadable(spec, now(), f"{' '.join(argv[:4])} failed (rc={rc}, {error_summary(text)})")
    complaint = next((ln.strip() for ln in text.splitlines() if _GCLOUD_COMPLAINT.match(ln.strip())), None)
    if complaint is not None:
        return _unreadable(spec, now(), f"{' '.join(argv[:4])} exited 0 with a complaint on its stderr, so its "
                                        f"list may be partial: {redact_identity(complaint)}")
    try:
        got = json.loads(out)
    except ValueError:
        got = None
    if not isinstance(got, list) or not all(isinstance(o, dict) for o in got):
        return _unreadable(spec, now(), f"{' '.join(argv[:4])} exited 0 and answered no JSON list of operations")
    ops, unplaced = [], []
    for o in got:
        try:
            t = target_of(o, spec)
        except Ambiguous as e:
            unplaced.append(f"{o.get('name')} ({o.get('operationType')}): {e}")
            continue
        if t is not None:
            ops.append(operation(o, t))
    if unplaced:
        return _unreadable(spec, now(), f"{len(unplaced)} operation(s) of the listing cannot be told to be "
                                        f"{spec.name}'s or not: {'; '.join(unplaced[:5])}")
    ops.sort(key=lambda o: (o["start_t"] is None, o["start_t"] or 0.0, str(o["id"])))
    held = f"the listing holds {len(got)} operation(s), {len(ops)} of them of {spec.name} in {spec.region}"
    if created is None:
        return _unreadable(spec, now(), f"{created_why or 'the creation time of the cluster is not known'}: "
                                        f"the listing cannot be proven to be this cluster's ({held})")
    control = control_of(ops, created)
    if control is None:
        others = [stamp(o["start_t"]) for o in ops if o["type"] == CONTROL_TYPE and o["target"] == "cluster"]
        return _unreadable(
            spec, now(),
            f"the control is missing: no {CONTROL_TYPE} of {spec.name} in {spec.region} that started within "
            f"{CONTROL_SKEW_S:g} s of the cluster's creation ({stamp(created)}) is in the listing, so it is not "
            f"proven to be this cluster's whole listing ({held}"
            + (f"; its {CONTROL_TYPE} operation(s) started {', '.join(others)}: another cluster of the same name"
               if others else "") + ")")
    # nothing of THIS cluster precedes its own creation: an earlier operation is an earlier cluster's of the name
    ops = [o for o in ops if o["start_t"] is None or o["start_t"] >= control["start_t"]]
    return {"kind": KIND, "cluster": spec.name, "region": spec.region, "read_at": now(), "readable": True,
            "unreadable": None, "operations": ops, "listed_in_region": len(got),
            "control": {"operation": control["id"], "type": CONTROL_TYPE, "start": control["start"],
                        "cluster_created": stamp(created),
                        "proves": f"the listing holds this cluster's own {CONTROL_TYPE} (exact target, started "
                                  f"within {CONTROL_SKEW_S:g} s of the creation time GKE reported for it)"},
            "source": f"{' '.join(argv[:4])} --region {spec.region}: the operations whose target is this cluster "
                      f"or one of its node pools"}


def dry_record(spec: gke.ClusterSpec) -> dict:
    return _unreadable(spec, time.time(), "dry run: nothing was read")


# ─── the mark: which operations overlapped which phase ──────────────────────

def overlaps(op: dict, t0: float, t1: float) -> bool:
    """`op` ran during [t0, t1]: it started before the phase ended and ended after the phase started.
    One that ended exactly when the phase started did not; one with no end time (still running at the
    read) overlaps everything after its start."""
    end = op.get("end_t")
    return op["start_t"] < t1 and (end is None or end > t0)


def op_text(op: dict) -> str:
    end = stamp(op["end_t"]) if op.get("end_t") is not None else "not ended at the read"
    where = "" if op.get("target") in (None, "cluster") else f" on {op['target']}"
    return f"{op.get('type')}{where} {stamp(op.get('start_t'))}..{end} ({op.get('status')}, {op.get('id')})"


def load(path: str | None) -> tuple[dict | None, str]:
    """(the record, "") or (None, why there is none to read)."""
    if not path:
        return None, "no cluster-operations record was given to evaluate"
    try:
        with open(path) as fh:
            rec = json.load(fh)
    except OSError as e:
        return None, f"the run has no cluster-operations record ({path}: {e.strerror})"
    except ValueError:
        return None, f"the run's cluster-operations record is not JSON ({path})"
    if not isinstance(rec, dict) or rec.get("kind") != KIND:
        return None, f"{path} is not a {KIND} record"
    return rec, ""


def report(path: str | None, phases: list[tuple[str, float | None, float | None]], blocks=None) -> dict:
    """report["cluster_operations_report_only"] of one run: the record's operations and, per measured
    phase (name, start, end: unix seconds), its outcome. UNKNOWN for every phase when the operations
    could not be read (no record, an UNREADABLE one, an operation with no start time), and for one
    phase when its own start or end is not recorded. `blocks`: (k6 span, gate windows, block seconds,
    custlat's split of an interval by phase), for the per-block marks (block_marks)."""
    rec, why = load(path)
    if rec is not None and not rec.get("readable"):
        why = str(rec.get("unreadable") or "the record says neither readable nor why not")
        why = why.removeprefix(UNREADABLE_PREFIX)
    ops = (rec or {}).get("operations") if not why else None
    if not why and not isinstance(ops, list):
        why = "the record is marked readable and holds no list of operations"
    if not why:
        timeless = [str(o.get("id")) for o in ops if o.get("start_t") is None]
        if timeless:
            why = f"no start time for operation(s) {', '.join(timeless)}: what they overlapped cannot be decided"
    out: dict = {"state": "UNREADABLE" if why else "READ", "why": why or None,
                 "cluster": (rec or {}).get("cluster"), "read_at": (rec or {}).get("read_at"),
                 "operations": ops if not why else None, "phases": [], "lines": [],
                 "not_covered": "clock: operations carry GKE's times, phases the bench's own; the two are "
                                "compared as they are (operations last minutes)"}
    if why:
        out["lines"].append(f"INFO cluster operations: {SAID_UNKNOWN}{why}")
    else:
        kinds: dict[str, int] = {}
        for o in ops:
            kinds[str(o.get("type"))] = kinds.get(str(o.get("type")), 0) + 1
        out["lines"].append(
            f"INFO cluster operations of {out['cluster']} (read {stamp(out['read_at'])}): {len(ops)} recorded"
            + (": " + ", ".join(f"{k} x{n}" for k, n in sorted(kinds.items())) if ops else ""))
    for name, t0, t1 in phases:
        row: dict = {"phase": name, "t0": t0, "t1": t1, "by": []}
        if why:
            row["overlap"], row["why"] = UNKNOWN, why
            said = SAID_UNKNOWN + why
        elif t0 is None or t1 is None:
            row["overlap"], row["why"] = UNKNOWN, "the phase's own start or end is not recorded"
            said = f"not decided: {row['why']}"
        else:
            row["by"] = [o for o in ops if overlaps(o, t0, t1)]
            row["overlap"] = OVERLAPPED if row["by"] else NONE
            said = SAID_OVERLAPPED + "; ".join(op_text(o) for o in row["by"]) if row["by"] else SAID_NONE
        out["phases"].append(row)
        out["lines"].append(f"INFO cluster operations, phase {name} [{stamp(t0)}..{stamp(t1)}]: {said}")
    if blocks is not None:
        span, wins, block_s, split = blocks
        out["blocks"] = block_marks(ops if not why else None, why or None, span, wins, block_s, split)
        out["lines"] += block_lines(out["blocks"], ops if not why else None)
    return out


def block_marks(ops: list[dict] | None, why: str | None, span, wins, block_s: float, seconds_by_phase) -> dict:
    """The latency blocks the pair statistics use (bench/custlat.py blocks: BLOCK_S blocks on a grid from
    the customers' first call to their last), each marked NONE / OVERLAPPED / UNKNOWN, counted per gate
    phase (a block counts in every phase it holds time of, as custlat counts it), and the stretches of
    consecutive overlapped blocks with the operations over them. `seconds_by_phase(a, b)`: custlat's
    split of [a, b) by phase. UNKNOWN for every block when the operations could not be read."""
    if not span:
        return {"state": "NO_BLOCKS", "why": "the run has no customer calls (no k6 span): no latency block"}
    t0, t1 = float(span[0]), float(span[1])
    nb = int((t1 - t0) // block_s) + 1
    out: dict = {"block_s": block_s, "span": [t0, t1], "blocks": nb, "by_phase": {}, "stretches": []}
    if why:
        return dict(out, state=UNKNOWN, why=why, overlapped=None)
    hit, cur = 0, None
    for k in range(nb):
        a = t0 + k * block_s
        b = min(a + block_s, t1) if k < nb - 1 else max(t1, a)
        by = [o["id"] for o in ops if o["start_t"] < max(b, a + 1e-9) and (o.get("end_t") is None or o["end_t"] > a)]
        for p, secs in seconds_by_phase(a, max(b, a + 1e-9)).items():
            if secs > 0:
                row = out["by_phase"].setdefault(p, {"blocks": 0, "overlapped": 0})
                row["blocks"] += 1
                row["overlapped"] += bool(by)
        if not by:
            cur = None
            continue
        hit += 1
        if cur is not None and cur["by"] == by:
            cur["t1"], cur["blocks"] = b, cur["blocks"] + 1
        else:
            cur = {"t0": a, "t1": b, "blocks": 1, "by": by}
            out["stretches"].append(cur)
    return dict(out, state="READ", overlapped=hit)


def block_lines(marks: dict, ops: list[dict] | None) -> list[str]:
    head = "INFO cluster operations, latency blocks"
    if marks.get("state") == "NO_BLOCKS":
        return [f"{head}: none to mark ({marks['why']})"]
    if marks.get("state") != "READ":
        return [f"{head} ({marks['blocks']} of {marks['block_s']:g} s): {SAID_UNKNOWN}{marks.get('why')}"]
    names = {o["id"]: o for o in ops or []}
    per = ", ".join(f"{p} {r['overlapped']} of {r['blocks']}" for p, r in marks["by_phase"].items())
    out = [f"{head}: {marks['overlapped']} of {marks['blocks']} blocks of {marks['block_s']:g} s overlapped by an "
           f"operation ({per})" if marks["overlapped"] else
           f"{head}: {SAID_NONE} any of the {marks['blocks']} blocks of {marks['block_s']:g} s ({per})"]
    for st in marks["stretches"]:
        out.append(f"{head} [{stamp(st['t0'])}..{stamp(st['t1'])}], {st['blocks']} block(s): {SAID_OVERLAPPED}"
                   + "; ".join(op_text(names[i]) for i in st["by"]))
    return out


def warnings(section: dict) -> list[str]:
    """What evaluate adds to report_warnings: an unreadable record, and each overlapped phase."""
    if section.get("state") != "READ":
        return [f"cluster operations: {SAID_UNKNOWN}{section.get('why')}: whether an operation overlapped a "
                f"measured phase is UNKNOWN"]
    bl = section.get("blocks") or {}
    return [f"cluster operations: phase {p['phase']} {SAID_OVERLAPPED}{'; '.join(op_text(o) for o in p['by'])}"
            for p in section.get("phases") or [] if p.get("overlap") == OVERLAPPED] + (
        [f"cluster operations: {bl['overlapped']} of {bl['blocks']} latency blocks overlapped by an operation"]
        if bl.get("overlapped") else [])


# ─── two arms side by side ──────────────────────────────────────────────────

def _arm(v: dict | None) -> tuple[dict[str, dict], str]:
    """(phase -> its row, why the arm has none)."""
    sec = ((v or {}).get("report") or {}).get(SECTION) if isinstance(v, dict) else None
    if not isinstance(sec, dict):
        return {}, ("the run has no readable verdict.json" if not isinstance(v, dict) else
                    f"the run's verdict.json has no {SECTION} (an evaluate before #1422)")
    why = "" if sec.get("state") == "READ" else str(sec.get("why") or "the section does not say why")
    return {p["phase"]: p for p in sec.get("phases") or [] if isinstance(p, dict) and p.get("phase")}, why


def _said(row: dict | None, absent: str) -> tuple[str, str]:
    """(outcome, its wording) of one arm's phase."""
    if row is None:
        return UNKNOWN, (SAID_UNKNOWN + absent) if absent else "phase not measured"
    if row.get("overlap") == OVERLAPPED:
        return OVERLAPPED, SAID_OVERLAPPED + "; ".join(op_text(o) for o in row.get("by") or [])
    if row.get("overlap") == NONE:
        return NONE, SAID_NONE
    why = str(row.get("why") or "not said")
    return UNKNOWN, why if why.startswith("the phase's own") else SAID_UNKNOWN + why


def pair(va: dict | None, vb: dict | None, labels: tuple[str, str]) -> dict:
    """The two runs' phases side by side (labels: each run's name and role, as its lines say it).
    `asymmetric`: the phases one arm measured under a cluster operation and the other did not: what
    the pair compares there is not the same cluster state on both sides. Where an arm's operations
    could not be read, whether the arms differ is UNKNOWN, and is said so: never symmetric."""
    (pa, wa), (pb, wb) = _arm(va), _arm(vb)
    out: dict = {"result": "INFO", "phases": [], "asymmetric": [], "unknown": [], "lines": []}
    for name in [*pa, *(n for n in pb if n not in pa)]:
        (sa, ta), (sb, tb) = _said(pa.get(name), wa), _said(pb.get(name), wb)
        row = {"phase": name, "a": sa, "b": sb,
               "asymmetric": None if UNKNOWN in (sa, sb) else sa != sb}
        out["phases"].append(row)
        head = f"INFO cluster operations, phase {name}:"
        both = f"{labels[0]}: {ta}; {labels[1]}: {tb}"
        if row["asymmetric"] is None:
            out["unknown"].append(name)
            out["lines"].append(f"{head} whether the arms differ is UNKNOWN: {both}")
        elif row["asymmetric"]:
            out["asymmetric"].append(name)
            out["lines"].append(f"{head} ASYMMETRIC, one arm only measured under a cluster operation: {both}")
        elif sa == NONE:
            out["lines"].append(f"{head} {SAID_NONE} on either arm")
        else:
            out["lines"].append(f"{head} both arms overlapped: {both}")
    if not out["phases"]:
        out["lines"].append(f"INFO cluster operations: no phase to compare: {labels[0]}: "
                            f"{wa or 'no measured phase'}; {labels[1]}: {wb or 'no measured phase'}")
    # the latency blocks the pair's cells are computed from: how many of each arm's overlapped an operation
    ba, bb = _blocks(va), _blocks(vb)
    out["blocks"] = {"a": ba, "b": bb}
    names = [*(ba.get("by_phase") or {}), *(n for n in (bb.get("by_phase") or {}) if n not in (ba.get("by_phase") or {}))]
    for name in names or [None]:
        ta, tb = _blocks_said(ba, name), _blocks_said(bb, name)
        out["lines"].append(f"INFO cluster operations, latency blocks{', phase ' + name if name else ''}: "
                            f"{labels[0]}: {ta}; {labels[1]}: {tb}")
    return out


def _blocks(v: dict | None) -> dict:
    sec = ((v or {}).get("report") or {}).get(SECTION) if isinstance(v, dict) else None
    bl = sec.get("blocks") if isinstance(sec, dict) else None
    if isinstance(bl, dict):
        return bl
    return {"state": UNKNOWN, "why": ("the run has no readable verdict.json" if not isinstance(v, dict) else
                                      f"the run's verdict.json marks no latency block (an evaluate before #1422)")}


def _blocks_said(bl: dict, phase: str | None) -> str:
    if bl.get("state") == "NO_BLOCKS":
        return "no latency block"
    if bl.get("state") != "READ":
        return SAID_UNKNOWN + str(bl.get("why"))
    row = (bl.get("by_phase") or {}).get(phase) if phase else {"blocks": bl["blocks"], "overlapped": bl["overlapped"]}
    if row is None:
        return "no block in this phase"
    return f"{row['overlapped']} of {row['blocks']} blocks overlapped by an operation"


# ─── the settle (#1422, the owner's ruling of 2026-11-16) ───────────────────

#: the operations GKE ran by itself on bench clusters, as their runs recorded them: the settle's window,
#: poll and deadline are derived from these, never set by hand
EVIDENCE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gke_operations_evidence.json")
#: GKE's control-plane check: the operation type it starts by itself, a no-op or a resize
CHECK_TYPE = "RESIZE_CLUSTER"
SETTLE_KIND = "control_plane_settle"
SETTLED, WAITING, UNSETTLED = "SETTLED", "WAITING", "UNSETTLED"
NOT_READ, NOT_STARTED, LOAD_ENDED = "UNREADABLE", "LOAD_NOT_STARTED", "LOAD_ENDED"
SETTLE_CHECK = "environment.control_plane_settled"
OVERLAP_CHECK = "environment.cluster_operations"
BY_ENV = "INVALID by environment: "
UNDECIDED = "INVALID by environment (undecided): "
# exit codes of `settle`: 0 settled; 3 not settled by the deadline (INVALID by environment, its verdict
# written); 2 the warm-up load did not run (tooling: it never started, or it ended before the settle)
EXIT_SETTLED, EXIT_TOOLING, EXIT_UNSETTLED = 0, 2, 3


def load_evidence(path: str | None = None) -> tuple[dict, str]:
    """(the evidence document, its sha256): which evidence a settle derived its bounds from is recorded."""
    with open(path or EVIDENCE_FILE, "rb") as fh:
        raw = fh.read()
    doc = json.loads(raw)
    if not isinstance(doc, dict) or not isinstance(doc.get("clusters"), list):
        raise ValueError(f"{path or EVIDENCE_FILE} holds no clusters' operations")
    return doc, hashlib.sha256(raw).hexdigest()


def cadence(evidence: dict) -> dict:
    """GKE's check cadence and its longest burst, from the operations it ran (each cluster's RESIZE_CLUSTER,
    in start order), and the settle's terms derived from them:

      period_s   the check cadence: the longest start-to-start interval between two consecutive checks of one
                 cluster where the earlier one ended within the SHORTEST such interval seen (the next check
                 then came on the cadence alone, not held back by a running resize) and the interval is under
                 two of the shortest (one slot on, no silent check between them). Runs 26j/27c: 300.0 s,
                 300.2 s, 300.3 s; the intervals after a 494-564 s resize (540-600 s) are not cadence.
      burst_s    the longest burst: a chain of checks each starting within one period of the previous one's
                 end, from its first start to its last end (run 26j N1B (8ef7e3ec): two resizes and the
                 check between them, 06:49:22-07:07:17Z, 1075 s).
      window_s   2 x period_s: the span in which two checks ran, the ruling's "last two checks".
      deadline_s window_s + burst_s + window_s, from the warm-up load's start: a clean first window settles
                 at once; a burst under way at the start, or starting before that window closed, ends within
                 burst_s and is followed by its own clean window. A burst longer than any seen, or a second
                 one, is past the evidence: the arm is then INVALID by environment, named, and its record
                 belongs in the evidence.
      poll_s     period_s / 5: the listing is read five times per check period.
    Raises ValueError when the evidence cannot give a period (no two consecutive checks of one cluster)."""
    per: list[tuple[str, list[tuple[float, float]]]] = []
    for c in evidence.get("clusters") or []:
        ops = []
        for o in c.get("operations") or []:
            if o.get("type") != CHECK_TYPE:
                continue
            a, b = epoch(o.get("start")), epoch(o.get("end"))
            if a is None or b is None or b < a:
                raise ValueError(f"the evidence's {c.get('cluster')} holds a {CHECK_TYPE} without a start and an "
                                 f"end in order ({o.get('start')}..{o.get('end')})")
            ops.append((a, b))
        per.append((str(c.get("cluster")), sorted(ops)))
    gaps = [(b[0] - a[0], a, b, name) for name, ops in per for a, b in pairwise(ops)]
    if not gaps:
        raise ValueError("the evidence holds no two consecutive checks of one cluster: no cadence can be derived")
    shortest = min(g for g, *_ in gaps)
    # the next LISTED check one cadence slot on: a check that changes nothing at all leaves no operation, so two
    # listed checks two or more slots apart (silent ones between them) say nothing of the cadence
    cad = [(g, name, a) for g, a, b, name in gaps if a[1] - a[0] <= shortest and g < 2 * shortest]
    period, p_cluster, p_op = max(cad)
    bursts: list[tuple[float, str, float, float]] = []
    for name, ops in per:
        chain: list[tuple[float, float]] = []
        for op in ops:
            if chain and op[0] - chain[-1][1] > period:
                bursts.append((chain[-1][1] - chain[0][0], name, chain[0][0], chain[-1][1]))
                chain = []
            chain.append(op)
        if chain:
            bursts.append((chain[-1][1] - chain[0][0], name, chain[0][0], chain[-1][1]))
    burst, b_cluster, b0, b1 = max(bursts)
    window = 2 * period
    return {"period_s": round(period, 3), "burst_s": round(burst, 3), "window_s": round(window, 3),
            "deadline_s": round(window + burst + window, 3), "poll_s": round(period / 5, 3),
            "from": {"period": f"{p_cluster}: the check started {stamp(p_op[0])} ({p_op[1] - p_op[0]:.1f} s) and the "
                               f"next {period:.1f} s later",
                     "burst": f"{b_cluster}: {stamp(b0)}..{stamp(b1)}",
                     "clusters": len(per), "checks": sum(len(ops) for _, ops in per)}}


def settle_decision(rec: dict, *, load_since: float | None, now: float, window: float) -> tuple[str, str]:
    """(state, why) of the cluster at `now`, from one listing `rec` (read()):
    UNREADABLE  the listing could not be read, or holds an operation with no start: never settled;
    WAITING     an operation runs, or one started or ran within the last `window` s, or the warm-up load has
                not yet run for the whole window (`load_since`: when it started);
    SETTLED     none of these: two checks came and went under the load with nothing to do."""
    if not rec.get("readable"):
        return NOT_READ, str(rec.get("unreadable") or "the listing says neither readable nor why not")
    ops = rec.get("operations")
    if not isinstance(ops, list):
        return NOT_READ, "the listing is marked readable and holds no list of operations"
    timeless = [str(o.get("id")) for o in ops if o.get("start_t") is None]
    if timeless:
        return NOT_READ, f"no start time for operation(s) {', '.join(timeless)}"
    running = [o for o in ops if o.get("end_t") is None]
    if running:
        return WAITING, "running: " + "; ".join(op_text(o) for o in running)
    w0 = now - window
    if load_since is None or load_since > w0:
        ran = 0.0 if load_since is None else max(0.0, now - load_since)
        return WAITING, (f"the warm-up load has run {ran:.0f} s of the {window:.0f} s in which GKE's last two checks "
                         f"must judge it")
    recent = [o for o in ops if overlaps(o, w0, now)]
    if recent:
        return WAITING, (f"within the last {window:.0f} s (GKE's last two checks): "
                         + "; ".join(op_text(o) for o in recent))
    return SETTLED, (f"nothing runs on the cluster, and nothing ran in the last {window:.0f} s (GKE's last two "
                     f"checks) under the warm-up load")


def pod_state(pods: dict, container: str) -> tuple[str, float | None, str]:
    """("RUNNING", the driver container's start, "") / ("WAITING", None, why) / ("ENDED", None, why), from
    `kubectl get pods -l job-name=<warm-up Job> -o json`: the warm-up load is applied only while its driver
    container runs."""
    items = (pods or {}).get("items") or []
    if not items:
        return "WAITING", None, "no warm-up pod is listed yet"
    if len(items) > 1:
        return "ENDED", None, f"{len(items)} warm-up pods are listed: the Job made another (one ended)"
    p = items[0]
    if (p.get("metadata") or {}).get("deletionTimestamp"):
        return "ENDED", None, "the warm-up pod is being deleted"
    phase = (p.get("status") or {}).get("phase")
    if phase in ("Succeeded", "Failed"):
        return "ENDED", None, f"the warm-up pod is {phase}"
    cs = next((c for c in (p.get("status") or {}).get("containerStatuses") or [] if c.get("name") == container), None)
    st = (cs or {}).get("state") or {}
    if "terminated" in st:
        t = st["terminated"]
        return "ENDED", None, f"its {container} container ended (exit {t.get('exitCode')}, {t.get('reason')})"
    if "running" in st:
        at = epoch((st["running"] or {}).get("startedAt"))
        if at is None:
            return "WAITING", None, f"its {container} container runs with no readable start time"
        if (cs or {}).get("restartCount"):
            return "ENDED", None, f"its {container} container restarted ({cs['restartCount']}): the load was cut"
        return "RUNNING", at, ""
    return "WAITING", None, f"the warm-up pod is {phase or 'unlisted'}, its {container} container not yet running"


def _read_pods(kubectl: list[str], job: str, timeout: float) -> tuple[dict | None, str]:
    argv = [*kubectl, f"--request-timeout={int(timeout)}s", "-n", "bench-loadgen", "get", "pods",
            "-l", f"job-name={job}", "-o", "json"]
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout + 10)
    except subprocess.TimeoutExpired:
        return None, f"listing the warm-up pod did not answer within {timeout:g} s"
    if p.returncode != 0:
        return None, f"listing the warm-up pod failed (rc={p.returncode}, {error_summary(p.stderr)})"
    try:
        return json.loads(p.stdout), ""
    except ValueError:
        return None, "listing the warm-up pod answered no JSON"


def _write(path: str, doc: dict) -> None:
    with open(path + ".part", "w") as fh:
        json.dump(doc, fh, indent=1, default=str)
        fh.write("\n")
    os.replace(path + ".part", path)


def settle_verdict(rec: dict) -> dict:
    """The verdict of an arm whose control plane did not settle by its deadline: INVALID by environment,
    named, not measured (custlat reads it as any arm's verdict: it voids the arm's pair)."""
    chk = {"check": SETTLE_CHECK, "value": rec.get("state"), "bar": SETTLED, "result": "FAIL",
           "why": BY_ENV + str(rec.get("why"))}
    return {"verdict": "INVALID", "invalid_reasons": [f"{SETTLE_CHECK}: {chk['why']}"], "checks": [],
            "validity": [chk], "economics": None, "thresholds": {},
            "report": {"control_plane_settle": rec, "measured": False,
                       "report_warnings": ["the arm was not measured: GKE's control plane did not settle"]}}


def settle(spec: gke.ClusterSpec, *, created: float | None, created_why: str = "", kubectl: list[str],
           job: str, container: str, start_within: float, out: str, verdict_out: str | None = None,
           evidence_path: str | None = None, run=None, read_pods=None, now: Callable[[], float] = time.time,
           sleep: Callable[[float], None] = time.sleep, say: Callable[[str], None] = print) -> int:
    """run606's settle step, after its warm-up Job was applied: wait (within `start_within` s) for the
    warm-up driver container to run, then read the cluster's operations every poll_s until settle_decision
    says SETTLED, or the deadline (the load's start + deadline_s) passes. The record (`out`, rewritten at each
    poll) holds every poll, the evidence the terms came from, and the operations last read."""
    read_pods = read_pods or (lambda: _read_pods(kubectl, job, 60.0))
    evidence, sha = load_evidence(evidence_path)
    cad = cadence(evidence)
    rec: dict = {"kind": SETTLE_KIND, "cluster": spec.name, "region": spec.region, "state": WAITING, "why": None,
                 "load": {"job": job, "container": container, "started_at": None},
                 "evidence": {"file": os.path.basename(evidence_path or EVIDENCE_FILE), "sha256": sha, **cad},
                 "deadline_at": None, "settled_at": None, "polls": [], "operations": None}

    def finish(state: str, why: str, code: int) -> int:
        rec.update(state=state, why=why)
        _write(out, rec)
        if code == EXIT_UNSETTLED and verdict_out:
            _write(verdict_out, settle_verdict(rec))
        say(f"control plane {state}: {why}")
        return code

    t_apply = now()
    while True:
        pods, why = read_pods()
        st, at, pwhy = pod_state(pods, container) if pods is not None else ("WAITING", None, why)
        if st == "RUNNING":
            break
        if st == "ENDED" or now() >= t_apply + start_within:
            return finish(NOT_STARTED, f"the warm-up load did not start within {start_within:g} s: {pwhy}",
                          EXIT_TOOLING)
        sleep(max(0.5, min(cad["poll_s"], t_apply + start_within - now())))
    rec["load"]["started_at"] = stamp(at)
    deadline = at + cad["deadline_s"]
    rec["deadline_at"] = stamp(deadline)
    say(f"warm-up load running since {stamp(at)}: settled when nothing ran on {spec.name} in the last "
        f"{cad['window_s']:g} s (two of GKE's {cad['period_s']:g} s checks) and nothing runs; deadline "
        f"{stamp(deadline)} (+{cad['deadline_s']:g} s: two windows and the longest burst seen, {cad['burst_s']:g} s)")
    last_why = ""
    while True:
        t = now()
        op_rec = read(spec, created=created, created_why=created_why, run=run, now=now)
        state, why = settle_decision(op_rec, load_since=at, now=t, window=cad["window_s"])
        pods, pwhy = read_pods()
        pst, _, pwhy = pod_state(pods, container) if pods is not None else ("WAITING", None, pwhy)
        if pods is not None and pst != "RUNNING":
            rec["polls"].append({"t": stamp(t), "state": state, "why": why, "load": pwhy})
            return finish(LOAD_ENDED, f"the warm-up load stopped before the control plane settled: {pwhy}",
                          EXIT_TOOLING)
        if op_rec.get("readable"):
            rec["operations"] = op_rec["operations"]
        rec["polls"].append({"t": stamp(t), "state": state, "why": why,
                             **({} if pods is not None else {"load": f"not read: {pwhy}"})})
        _write(out, rec)
        if why != last_why:
            say(f"control plane {state} at {stamp(t)}: {why}")
            last_why = why
        # the load's own state must have been READ running at this poll for the window to count
        if state == SETTLED and pods is not None:
            rec["settled_at"] = stamp(t)
            return finish(SETTLED, f"{why} (read at {stamp(t)}, {t - at:.0f} s after the load started)",
                          EXIT_SETTLED)
        if t >= deadline:
            return finish(UNSETTLED,
                          f"GKE's control plane did not settle within {cad['deadline_s']:g} s of the warm-up load's "
                          f"start ({stamp(at)}..{stamp(deadline)}; two windows of {cad['window_s']:g} s and the "
                          f"longest burst in the evidence, {cad['burst_s']:g} s); at the last read: {why}. The arm "
                          f"is not measured", EXIT_UNSETTLED)
        sleep(cad["poll_s"])


def settle_check(path: str | None) -> dict:
    """evaluate's environment.control_plane_settled: the settle record says SETTLED, or the arm is INVALID by
    environment (undecided when there is no record)."""
    try:
        with open(path or "") as fh:
            rec = json.load(fh)
    except (OSError, ValueError):
        rec = None
    if not isinstance(rec, dict) or rec.get("kind") != SETTLE_KIND:
        return {"check": SETTLE_CHECK, "value": None, "bar": SETTLED, "result": "FAIL",
                "why": UNDECIDED + f"the run has no settle record ({path}): whether GKE's control plane had settled "
                                   f"before the measured reboot is unknown"}
    if rec.get("state") != SETTLED:
        return {"check": SETTLE_CHECK, "value": rec.get("state"), "bar": SETTLED, "result": "FAIL",
                "why": BY_ENV + f"the control plane was {rec.get('state')} before the measure: {rec.get('why')}"}
    return {"check": SETTLE_CHECK, "value": SETTLED, "bar": SETTLED, "result": "PASS",
            "settled_at": rec.get("settled_at"), "load_started_at": (rec.get("load") or {}).get("started_at")}


def overlap_check(section: dict) -> dict:
    """evaluate's environment.cluster_operations, from report()'s section: FAIL (INVALID by environment) when
    any operation of the cluster overlapped the measure or one of its phases, or when that cannot be
    decided; PASS when every phase reads NONE."""
    phases = section.get("phases") or []
    hit = [p for p in phases if p.get("overlap") == OVERLAPPED]
    unknown = [p for p in phases if p.get("overlap") not in (OVERLAPPED, NONE)]
    base = {"check": OVERLAP_CHECK, "bar": "no operation of the cluster overlaps the measure"}
    if section.get("state") != "READ":
        return dict(base, value=None, result="FAIL",
                    why=UNDECIDED + f"the cluster's operations could not be read ({section.get('why')}): whether one "
                                    f"overlapped the measure is unknown")
    if not phases:
        return dict(base, value=None, result="FAIL", why=UNDECIDED + "the run records no measured phase to judge")
    if hit:
        ops = {o["id"]: o for p in hit for o in p.get("by") or []}
        return dict(base, value=[p["phase"] for p in hit], result="FAIL",
                    why=BY_ENV + f"GKE's {'; '.join(op_text(o) for o in ops.values())} overlapped the measured "
                                 f"phase(s) {', '.join(p['phase'] for p in hit)}")
    if unknown:
        return dict(base, value=None, result="FAIL",
                    why=UNDECIDED + "; ".join(f"phase {p['phase']}: {p.get('why')}" for p in unknown))
    return dict(base, value=[], result="PASS")


# ─── CLI (run606's collect) ─────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    """`python3 -m bench.clusterops record --name N --project P --zone Z --created-from F --out FILE [--yes]`: the
    cluster's operations record, written whatever the read answered (UNREADABLE is a record too);
    one line on stdout saying what was recorded. Exit 0: the record is evidence, not a reason to end
    the run. Without --yes nothing is read (a dry run's record says so)."""
    ap = argparse.ArgumentParser(prog="python3 -m bench.clusterops")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--name", required=True)
    r.add_argument("--project", default=gke.DEFAULT_PROJECT)
    r.add_argument("--zone", default=gke.DEFAULT_ZONE)
    r.add_argument("--out", required=True)
    r.add_argument("--created-from", default=None,
                   help="gke-up's maintenance-exclusion.json: the creation time GKE reported for the cluster "
                        "(cluster_created), which the listing's CREATE_CLUSTER must match; without it the "
                        "record is 'could not be read'")
    r.add_argument("--yes", action="store_true", help="actually run gcloud (default: dry run)")
    s = sub.add_parser("settle", help="#1422: wait, under the warm-up load, until GKE's control plane settled")
    s.add_argument("--name", required=True)
    s.add_argument("--project", default=gke.DEFAULT_PROJECT)
    s.add_argument("--zone", default=gke.DEFAULT_ZONE)
    s.add_argument("--created-from", default=None)
    s.add_argument("--job", required=True, help="the warm-up Job (bench-loadgen)")
    s.add_argument("--container", required=True, help="its driver container: the load runs while it runs")
    s.add_argument("--start-within", type=float, required=True, help="s for the driver container to start")
    s.add_argument("--out", required=True, help="the settle record (settle.json)")
    s.add_argument("--verdict-out", default=None, help="not settled: the arm's INVALID verdict is written here")
    s.add_argument("kubectl", nargs="+", help="after --: the run's kubectl argv (its kubeconfig and context)")
    t = sub.add_parser("terms", help="#1422: the settle's terms derived from the evidence, as KEY=VALUE lines")
    t.add_argument("--evidence", default=None)
    a = ap.parse_args(argv)
    if a.cmd == "terms":
        cad = cadence(load_evidence(a.evidence)[0])
        for k in ("period_s", "burst_s", "window_s", "deadline_s", "poll_s"):
            print(f"SETTLE_{k[:-2].upper()}_S={cad[k]:g}")
        return 0
    spec = gke.default_spec(a.name, project=a.project, zone=a.zone)
    if a.cmd == "settle":
        created, why = created_from(a.created_from)
        return settle(spec, created=created, created_why=why, kubectl=a.kubectl, job=a.job, container=a.container,
                      start_within=a.start_within, out=a.out, verdict_out=a.verdict_out)
    if not a.yes:
        print(f"DRY-RUN: {' '.join(list_command(spec))}")
        rec = dry_record(spec)
    else:
        created, why = created_from(a.created_from)
        rec = read(spec, created=created, created_why=why)
    with open(a.out, "w") as fh:
        json.dump(rec, fh, indent=1)
        fh.write("\n")
    if rec["readable"]:
        ops = rec["operations"]
        print(f"cluster operations of {spec.name} (its {CONTROL_TYPE} {rec['control']['operation']} is in the "
              f"listing): {len(ops)} recorded"
              + (": " + "; ".join(op_text(o) for o in ops) if ops else ""))
    else:
        print(f"cluster operations of {spec.name}: {rec['unreadable']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
