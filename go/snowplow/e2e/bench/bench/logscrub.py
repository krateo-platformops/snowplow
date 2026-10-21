"""Metadata-only scrubber for snowplow's JSON log lines (#822).

Every snowplow log line the bench collects goes through here before it
reaches $OUT. A line keeps:
  - the values of an ALLOWLIST of fields that are code-defined metadata
    (time, level, msg, subsystem, event, class / gvr / restaction coordinates,
    ...), unless the value matches a deny pattern;
  - every number and boolean;
and every other string (names, paths, namespaces, errors, user fields) is
replaced by `h:<12 hex>` of its sha256, so lines still correlate without their
content. A nested value is hashed whole. A non-JSON line becomes
{"unparsed": true, "len": n, "h": ...}. No body, username or token survives:
names can embed a username (a `<user>-clientconfig` Secret) and errors can
carry response bodies, so neither is ever kept verbatim.

    kubectl logs ... | python3 -m bench.logscrub [--last-time-file F] [--deny S ...]

--last-time-file records the newest `time` seen, so a reconnecting stream
resumes from it (`kubectl logs --since-time`).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys

KEEP = frozenset({
    "time", "level", "msg", "subsystem", "event", "kind", "class", "hint", "cause",
    "reason", "dispatch", "gvr", "group", "version", "resource", "restaction_namespace",
    "restaction_name", "wire_shape", "previous", "phase", "tier", "outcome", "state",
    "traceId", "spanId",
})
# A kept value is hashed anyway when it looks like identity or a credential.
DENY = re.compile(r"scaleuser-\d+|clientconfig|bearer\s|eyJ[A-Za-z0-9_-]{10,}|password",
                  re.IGNORECASE)


def _h(v) -> str:
    raw = v if isinstance(v, str) else json.dumps(v, sort_keys=True, default=str)
    return "h:" + hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:12]


def scrub(rec: dict, deny_literals: tuple[str, ...] = ()) -> dict:
    out = {}
    for k, v in rec.items():
        if isinstance(v, bool) or isinstance(v, (int, float)) or v is None:
            out[k] = v
        elif (isinstance(v, str) and k in KEEP and not DENY.search(v)
              and not any(d and d in v for d in deny_literals)):
            out[k] = v
        else:
            out[k] = _h(v)
    return out


def scrub_line(line: str, deny_literals: tuple[str, ...] = ()) -> tuple[str, str | None]:
    """(scrubbed JSON line, the record's `time` or None)."""
    s = line.rstrip("\n")
    try:
        rec = json.loads(s)
    except ValueError:
        rec = None
    if not isinstance(rec, dict):
        return json.dumps({"unparsed": True, "len": len(s), "h": _h(s)}), None
    t = rec.get("time") if isinstance(rec.get("time"), str) else None
    return json.dumps(scrub(rec, deny_literals)), t


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="logscrub")
    p.add_argument("--last-time-file", default=None)
    p.add_argument("--deny", action="append", default=[],
                   help="a literal that must never appear verbatim (repeatable)")
    a = p.parse_args(argv)
    deny = tuple(a.deny)
    last = None
    for line in sys.stdin:
        if not line.strip():
            continue
        out, t = scrub_line(line, deny)
        sys.stdout.write(out + "\n")
        sys.stdout.flush()
        if t and a.last_time_file and t != last:
            last = t
            try:
                with open(a.last_time_file, "w") as fh:
                    fh.write(t)
            except OSError:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
