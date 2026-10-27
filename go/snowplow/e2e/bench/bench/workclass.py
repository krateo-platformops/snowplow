"""#871 (owner ruling 2026-10-25, "measure first"): how much storm CPU each
kind of background work takes, read from snowplow's own work-class label.

snowplow (krateo-platformops/snowplow#871 work-class PR) runs every unit of
work under the pprof label

    snowplow_work = customer | refresh | refresh_viewed | prewarm | drain |
                    seed | comparator | fill_bg

and every goroutine a unit starts inherits it. It also counts the units, per
class, in the expvar snowplow_work_class ({class: {started, completed}}).

Before the label, run 15's background share could only be attributed by the
ROOT frame of each stack (the pre-build check, issuecomment-6396882049): a
refresher refill runs its stages on errgroup goroutines and a prewarm walk's
4-wide stage work lands in the same errgroup bucket, so both read as "other".
This module reads the label instead:

  * cpu_report — mem606's CPU profiles (cpu-NN-*.pb.gz, copied to the run's
    heap/ dir), per profiling window (storm / burst / periodic): CPU seconds
    and share per class. A sample with no label is split by its root frame
    (GC workers; the customer request's HTTP goroutine before its dispatch is
    labelled — middleware, auth; anything else).
  * units_report — the snowplow_work_class counters per run phase, from
    mem606's samples.

REPORT ONLY. A profile set where NO sample carries the label is an image
without it (status label_absent): the shares are missing, never zero.

The profile decoder reads only the profile.proto fields it needs (sample
types, samples, locations, functions, string table); it has no dependency.
"""

from __future__ import annotations

import gzip
import os
import zlib
from typing import Iterable

KEY = "snowplow_work"
EXPVAR_KEY = "snowplow_work_class"
CLASSES = ("customer", "refresh", "refresh_viewed", "prewarm", "drain", "seed",
           "comparator", "fill_bg")
BACKGROUND = ("refresh", "refresh_viewed", "prewarm", "drain", "seed", "comparator", "fill_bg")
# Unlabelled samples, by root frame.
UNLABELLED_GC = "unlabelled:gc"
UNLABELLED_HTTP = "unlabelled:http"
UNLABELLED_OTHER = "unlabelled:other"
GC_ROOTS = ("runtime.gcBgMarkWorker", "runtime.bgsweep", "runtime.bgscavenge")
HTTP_ROOT = "net/http.(*conn).serve"

STATUS_PRESENT = "present"
STATUS_LABEL_ABSENT = "label_absent"
STATUS_NO_PROFILES = "no_profiles"
STATUS_ABSENT = "absent_in_all_samples"


# ─── profile.proto, the fields this report reads ────────────────────────────

def _varint(b: bytes, i: int) -> tuple[int, int]:
    v = shift = 0
    while True:
        c = b[i]
        v |= (c & 0x7F) << shift
        i += 1
        if not c & 0x80:
            return v, i
        shift += 7


def _fields(b: bytes):
    """(field, wire type, int value | bytes) for each field of a message."""
    i = 0
    while i < len(b):
        key, i = _varint(b, i)
        f, w = key >> 3, key & 7
        if w == 0:
            v, i = _varint(b, i)
            yield f, w, v
        elif w == 2:
            n, i = _varint(b, i)
            yield f, w, b[i:i + n]
            i += n
        elif w == 1:
            i += 8
        elif w == 5:
            i += 4
        else:
            raise ValueError(f"unsupported wire type {w}")


def _ints(w: int, v) -> list[int]:
    """A repeated integer field, packed (bytes) or not (one int)."""
    if w == 0:
        return [v]
    out, i = [], 0
    while i < len(v):
        x, i = _varint(v, i)
        out.append(x)
    return out


def _signed(x: int) -> int:
    return x - (1 << 64) if x >= 1 << 63 else x


def decode(body: bytes) -> list[tuple[int, dict, list[str]]]:
    """A CPU profile's samples: (cpu nanoseconds, {label key: value}, function
    names leaf first). Raises ValueError when it is not a CPU profile."""
    b = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
    types, samples, strs = [], [], []
    loc_fns: dict[int, list[int]] = {}
    fn_name: dict[int, int] = {}
    for f, w, v in _fields(b):
        if f == 1:                                   # sample_type: ValueType
            t = dict((ff, vv) for ff, _, vv in _fields(v))
            types.append((t.get(1, 0), t.get(2, 0)))
        elif f == 2:                                 # sample
            locs, vals, labels = [], [], []
            for ff, ww, vv in _fields(v):
                if ff == 1:
                    locs += _ints(ww, vv)
                elif ff == 2:
                    vals += [_signed(x) for x in _ints(ww, vv)]
                elif ff == 3:
                    lab = dict((k, x) for k, _, x in _fields(vv))
                    labels.append((lab.get(1, 0), lab.get(2, 0)))
            samples.append((locs, vals, labels))
        elif f == 4:                                 # location
            lid, fns = 0, []
            for ff, _, vv in _fields(v):
                if ff == 1:
                    lid = vv
                elif ff == 4:                        # line: function_id
                    fns += [x for k, _, x in _fields(vv) if k == 1]
            loc_fns[lid] = fns
        elif f == 5:                                 # function
            d = dict((ff, vv) for ff, _, vv in _fields(v) if ff in (1, 2))
            fn_name[d.get(1, 0)] = d.get(2, 0)
        elif f == 6:                                 # string_table
            strs.append(v.decode("utf-8", "replace"))

    def s(i: int) -> str:
        return strs[i] if 0 <= i < len(strs) else ""
    idx = next((k for k, (t, u) in enumerate(types) if s(t) == "cpu" and s(u) == "nanoseconds"), None)
    if idx is None:
        raise ValueError("not a CPU profile (no cpu/nanoseconds sample type)")
    out = []
    for locs, vals, labels in samples:
        # a location's lines run innermost first: leaf → root, in order
        frames = [s(fn_name.get(fid, 0)) for lid in locs for fid in loc_fns.get(lid, [])]
        out.append((vals[idx] if idx < len(vals) else 0,
                    {s(k): s(v) for k, v in labels}, frames))
    return out


def classify(labels: dict, frames: list[str]) -> str:
    """A sample's class: its snowplow_work label, else its root frame's
    unlabelled bucket."""
    c = labels.get(KEY)
    if c:
        return c
    root = frames[-1] if frames else ""
    if root.startswith(GC_ROOTS):
        return UNLABELLED_GC
    if root == HTTP_ROOT:
        return UNLABELLED_HTTP
    return UNLABELLED_OTHER


def attribute(bodies: Iterable[bytes]) -> dict:
    """CPU seconds per class over a set of profiles, with each class's share,
    the background share (every class but customer), and whether any sample
    carried the label at all."""
    cpu: dict[str, int] = {}
    labelled = False
    n = 0
    for body in bodies:
        n += 1
        for ns, labels, frames in decode(body):
            c = classify(labels, frames)
            labelled |= KEY in labels
            cpu[c] = cpu.get(c, 0) + ns
    total = sum(cpu.values())

    def share(x):
        return round(x / total, 4) if total else None
    by_class = {c: {"cpu_s": round(v / 1e9, 2), "share": share(v)}
                for c, v in sorted(cpu.items(), key=lambda kv: -kv[1])}
    bg = sum(v for c, v in cpu.items() if c in BACKGROUND)
    return {"profiles": n, "cpu_s": round(total / 1e9, 2), "label_seen": labelled,
            "by_class": by_class,
            "background_share": share(bg) if labelled else None,
            "customer_share": share(cpu.get("customer", 0)) if labelled else None,
            "unlabelled_share": share(sum(v for c, v in cpu.items() if c.startswith("unlabelled:")))}


# ─── reports ────────────────────────────────────────────────────────────────

def cpu_report(win: dict | None, mem: list[dict], profile_dir: str | None) -> dict:
    """Per profiling window, the CPU attribution of the profiles mem606 wrote
    there (`win` = load606.cpu_profile_windows(mem)); `profile_dir` is where
    they were copied (the run's heap/ dir)."""
    if win is None or not win.get("profiles"):
        return {"status": STATUS_NO_PROFILES, "note": "mem606 recorded no CPU profile"}
    if not profile_dir:
        return {"status": STATUS_NO_PROFILES,
                "note": "no --profile-dir: the profiles' files were not handed to evaluate"}
    secs = {r["profile"]: r.get("prof_duration_s") or r.get("seconds")
            for r in mem if r.get("kind") == "cpu_profile" and r.get("profile")}
    by_window: dict[str, list[str]] = {}
    for p in win["profiles"]:
        by_window.setdefault(p["window"] or "periodic", []).append(p["profile"])
    missing, unreadable, out = [], [], {}
    for w, names in by_window.items():
        bodies, span = [], 0.0
        for name in names:
            path = os.path.join(profile_dir, name)
            try:
                with open(path, "rb") as fh:
                    body = fh.read()
                decode(body)
            except FileNotFoundError:
                missing.append(name)
                continue
            # a truncated or corrupt profile (bad gzip header: OSError; corrupt
            # deflate: zlib.error; truncated: EOFError; bad proto: ValueError /
            # IndexError) is listed, never fatal: evaluate must still write
            # verdict.json
            except (OSError, ValueError, IndexError, EOFError, zlib.error) as e:
                unreadable.append({"profile": name, "error": str(e)})
                continue
            bodies.append(body)
            span += float(secs.get(name) or 0)
        a = attribute(bodies)
        a["profiled_s"] = round(span, 1)
        a["cores"] = round(a["cpu_s"] / span, 2) if span else None
        out[w] = a
    read = sum(a["profiles"] for a in out.values())
    if not read:
        return {"status": STATUS_NO_PROFILES, "missing_profiles": missing,
                "unreadable_profiles": unreadable,
                "note": f"none of mem606's {len(missing) + len(unreadable)} CPU profile(s) could be "
                        f"read under {profile_dir} ({len(missing)} missing, {len(unreadable)} unreadable)"}
    seen = any(a["label_seen"] for a in out.values())
    r = {"status": STATUS_PRESENT if seen else STATUS_LABEL_ABSENT, "key": KEY,
         "by_window": out, "missing_profiles": missing, "unreadable_profiles": unreadable,
         "note": ("report only. share = the class's CPU seconds over all sampled CPU in the "
                  "window; background = refresh + refresh_viewed + prewarm + drain + seed + "
                  "comparator + fill_bg. unlabelled:http is a customer request before its "
                  "dispatch is labelled (middleware, auth); unlabelled:gc is the GC workers")}
    if not seen:
        r["note"] = (f"no sample of {sum(a['profiles'] for a in out.values())} profile(s) carried "
                     f"{KEY}: an image without the #871 label — the shares are MISSING, not zero")
    return r


def _counters(m: dict) -> dict | None:
    v = (m.get("extra") or {}).get(EXPVAR_KEY)
    if not isinstance(v, dict):
        return None
    out = {}
    for c, d in v.items():
        if isinstance(d, dict):
            for k in ("started", "completed"):
                x = d.get(k)
                out[f"{k}/{c}"] = x if isinstance(x, (int, float)) and not isinstance(x, bool) else None
    return out


def units_report(mem: list[dict], fresh: list[dict] | None, phase_counter_report) -> dict:
    """The snowplow_work_class counters per run phase: units started and
    completed per class (`phase_counter_report` = load606._phase_counter_report)."""
    r = phase_counter_report(mem, fresh, EXPVAR_KEY, _counters)
    if r["status"] != STATUS_PRESENT:
        return r
    r.pop("last")
    r.pop("phases")
    out = {}
    for name, p in r["by_phase"].items():
        c = p["counters"]
        out[name] = {"seconds": p["seconds"], "pairs": p["pairs"],
                     "started": {k.split("/", 1)[1]: v for k, v in c.items() if k.startswith("started/")},
                     "completed": {k.split("/", 1)[1]: v for k, v in c.items() if k.startswith("completed/")}}
    return {"status": STATUS_PRESENT, "by_phase": out, "samples": r["samples"],
            "restarts": r["restarts"],
            "note": ("report only. A unit nested in another (a seed target in its prewarm scope, "
                     "a comparator pass in its refresh dequeue) is counted in both classes")}
