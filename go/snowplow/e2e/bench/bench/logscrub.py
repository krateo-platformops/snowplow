"""Metadata-only scrubber for snowplow's JSON log lines (#822).

Every snowplow log line the bench collects goes through here before it
reaches $OUT. A line keeps:
  - KEY NAMES only from an allowlist (KEY_NAMES); any other key name is
    replaced by `k:<12 hex>` (a key can itself be identity: `{"<user>": ...}`);
  - string VALUES only for the few code-defined metadata fields in KEEP
    (time, level, msg, subsystem, event, class / gvr / restaction
    coordinates, ...), and only when the value matches no deny pattern;
  - every number and boolean;
and every other string (names, paths, namespaces, errors, causes, reasons,
user fields) is replaced by `h:<12 hex>` of its sha256, so lines still
correlate without their content. A nested value is hashed whole. A non-JSON
line becomes {"unparsed": true, "len": n, "h": ...}. Free text is never
trusted: `cause` / `reason` carry raw error text (snowplow
internal/handlers/list.go:97 `pd.Error()`), and a kept field such as `msg` is
hashed as soon as it looks like it names a subject, a credential or a
key=value pair (DENY), a `--deny` literal (the test password, the bench's
user prefix) or a `--deny-pattern`.

    kubectl logs --timestamps ... | python3 -m bench.logscrub --kubelet-timestamps \
        [--state-file F] [--last-time-file F] [--deny S ...] [--deny-pattern RE ...]

--kubelet-timestamps: each input line starts with the kubelet's RFC3339 time
(`kubectl logs --timestamps`); it is kept as `kts` and is what a reconnecting
stream resumes from (`--since-time`, which is INCLUSIVE). --state-file holds
the newest kts and the hashes of the lines already emitted AT it, so the
resent line(s) of a resume are dropped, never duplicated; --last-time-file
holds that kts alone, for the shell.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys

KEEP = frozenset({
    "time", "level", "msg", "subsystem", "event", "kind", "class", "hint",
    "dispatch", "gvr", "group", "version", "resource", "restaction_namespace",
    "restaction_name", "wire_shape", "previous", "phase", "tier", "outcome", "state",
    "traceId", "spanId",
})
# Key names that may appear as written (their string values are still hashed
# unless the key is also in KEEP). Every other key name is hashed.
KEY_NAMES = KEEP | frozenset({
    "kts", "name", "namespace", "path", "host", "error", "err", "cause", "reason",
    "user", "username", "groups", "subject", "key", "l1_key", "sa_dial",
    "first_observation", "snap_seq", "armed_keys", "requested", "armed",
    "skipped_informer_miss", "skipped_other", "count", "bytes", "status", "code",
    "attempt", "seq", "gen", "elapsed_ms", "duration_ms", "wall_ms", "ms", "items",
    "unparsed", "len", "h",
})
# A KEEP value is hashed anyway when it looks like it names an identity, a
# credential or carries a key=value / quoted fragment (concatenated messages).
DENY = re.compile(
    r"scaleuser-\d+|clientconf|\buser(name)?s?\b|subject|serviceaccount|system:|token|"
    r"secret|passw|authoriz|bearer|cookie|credential|eyJ[A-Za-z0-9_-]{10,}|@|=|\"",
    re.IGNORECASE)
TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z (.*)$", re.DOTALL)


def _norm_ts(m: "re.Match") -> str:
    """RFC3339Nano with the fraction padded to 9 digits: the kubelet trims
    trailing zeros, which would break a string comparison."""
    frac = (m.group(2) or ".")[1:]
    return f"{m.group(1)}.{frac:0<9}Z"


def _h(v) -> str:
    raw = v if isinstance(v, str) else json.dumps(v, sort_keys=True, default=str)
    return "h:" + hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:12]


class Scrubber:
    def __init__(self, deny_literals=(), deny_patterns=()):
        self.deny_literals = tuple(d for d in deny_literals if d)
        self.deny_patterns = tuple(re.compile(p, re.IGNORECASE) for p in deny_patterns if p)

    def _given(self, v: str) -> bool:
        return (any(d.lower() in v.lower() for d in self.deny_literals)
                or any(p.search(v) for p in self.deny_patterns))

    def denied(self, v: str) -> bool:
        return bool(DENY.search(v)) or self._given(v)

    def key(self, k: str) -> str:
        # KEY_NAMES are code constants ("user" names a field, it is not one);
        # only the caller's own literals / patterns can veto one.
        return k if k in KEY_NAMES and not self._given(k) else "k:" + _h(k)[2:]

    def scrub(self, rec: dict) -> dict:
        out = {}
        for k, v in rec.items():
            if isinstance(v, bool) or isinstance(v, (int, float)) or v is None:
                val = v
            elif isinstance(v, str) and k in KEEP and not self.denied(v):
                val = v
            else:
                val = _h(v)
            out[self.key(k)] = val
        return out

    def line(self, raw: str) -> str:
        try:
            rec = json.loads(raw)
        except ValueError:
            rec = None
        if not isinstance(rec, dict):
            return json.dumps({"unparsed": True, "len": len(raw), "h": _h(raw)})
        return json.dumps(self.scrub(rec))


# Kept for callers of the earlier function API.
def scrub(rec: dict, deny_literals: tuple[str, ...] = ()) -> dict:
    return Scrubber(deny_literals).scrub(rec)


def scrub_line(line: str, deny_literals: tuple[str, ...] = ()) -> tuple[str, str | None]:
    """(scrubbed JSON line, the record's `time` or None)."""
    s = line.rstrip("\n")
    out = Scrubber(deny_literals).line(s)
    try:
        t = json.loads(s).get("time")
    except (ValueError, AttributeError):
        t = None
    return out, t if isinstance(t, str) else None


def _load_state(path):
    try:
        with open(path) as fh:
            st = json.load(fh)
        return st.get("kts"), set(st.get("seen") or [])
    except (OSError, ValueError, AttributeError):
        return None, set()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="logscrub")
    p.add_argument("--kubelet-timestamps", action="store_true")
    p.add_argument("--state-file", default=None)
    p.add_argument("--last-time-file", default=None)
    p.add_argument("--deny", action="append", default=[],
                   help="a literal that must never appear verbatim (repeatable)")
    p.add_argument("--deny-pattern", action="append", default=[],
                   help="a regex whose match hashes a kept value (repeatable)")
    a = p.parse_args(argv)
    sc = Scrubber(a.deny, a.deny_pattern)
    last, seen = _load_state(a.state_file) if a.state_file else (None, set())
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        kts = None
        if a.kubelet_timestamps:
            m = TS.match(line)
            if m:
                kts, line = _norm_ts(m), m.group(3)
        if kts is not None:
            hl = hashlib.sha256(line.encode("utf-8", "replace")).hexdigest()[:16]
            if last is not None and (kts < last or (kts == last and hl in seen)):
                continue                       # resent by an inclusive --since-time
            if kts != last:
                last, seen = kts, set()
            seen.add(hl)
        out = sc.line(line)
        if kts is not None:
            out = json.dumps({"kts": kts, **json.loads(out)})
        sys.stdout.write(out + "\n")
        sys.stdout.flush()
        if kts is not None:
            for path, body in ((a.state_file, json.dumps({"kts": last, "seen": sorted(seen)})),
                               (a.last_time_file, last)):
                if path:
                    try:
                        with open(path, "w") as fh:
                            fh.write(body)
                    except OSError:
                        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
