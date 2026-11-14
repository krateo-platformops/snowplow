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

Report only: nothing here changes a verdict. Whether an overlap moves a metric is decided from run
26i's own evidence (N1 against N1B), before any re-measure rule exists (owner, 2026-11-14).

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
import json
import re
import subprocess
import sys
import time
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

_TARGET = re.compile(r"/(?:locations|zones)/([^/]+)/clusters/([^/]+)(?:/nodePools/([^/]+))?$")
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


def target_of(op: dict, spec: gke.ClusterSpec) -> str | None:
    """"cluster" or "nodePool/<pool>" when `op` targets THIS cluster (exact name, in its region) or one
    of its node pools; None for any other target."""
    m = _TARGET.search(str(op.get("targetLink") or ""))
    if m is None or m.group(1) != spec.region or m.group(2) != spec.name:
        return None
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
        rc, out = run(argv, dry_run=False, log=lambda _m: None, stderr_out=err, timeout=timeout)
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
    ops = sorted((operation(o, t) for o in got if (t := target_of(o, spec)) is not None),
                 key=lambda o: (o["start_t"] is None, o["start_t"] or 0.0, str(o["id"])))
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


def report(path: str | None, phases: list[tuple[str, float | None, float | None]]) -> dict:
    """report["cluster_operations_report_only"] of one run: the record's operations and, per measured
    phase (name, start, end: unix seconds), its outcome. UNKNOWN for every phase when the operations
    could not be read (no record, an UNREADABLE one, an operation with no start time), and for one
    phase when its own start or end is not recorded."""
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
    return out


def warnings(section: dict) -> list[str]:
    """What evaluate adds to report_warnings: an unreadable record, and each overlapped phase."""
    if section.get("state") != "READ":
        return [f"cluster operations: {SAID_UNKNOWN}{section.get('why')}: whether an operation overlapped a "
                f"measured phase is UNKNOWN"]
    return [f"cluster operations: phase {p['phase']} {SAID_OVERLAPPED}{'; '.join(op_text(o) for o in p['by'])}"
            for p in section.get("phases") or [] if p.get("overlap") == OVERLAPPED]


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
    return out


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
    a = ap.parse_args(argv)
    spec = gke.default_spec(a.name, project=a.project, zone=a.zone)
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
