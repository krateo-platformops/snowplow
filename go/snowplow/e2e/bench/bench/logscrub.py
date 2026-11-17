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
correlate without their content. A nested value is hashed whole. Free text is
never trusted: `cause` / `reason` carry raw error text (snowplow
internal/handlers/list.go:97 `pd.Error()`), and a kept field such as `msg` is
hashed as soon as it looks like it names a subject, a credential or a
key=value pair (DENY), a `--deny` literal (the test password, the bench's
user prefix) or a `--deny-pattern`; a kept one also passes bench/identity.py,
the bench's one identity redaction.

A klog line (client-go's own log: `W1027 00:02:54.123456   1 request.go:697]
...`; #919, run 16's burst of 21 at 00:02:54Z was unreadable) keeps its
header — severity, klog time, thread, file:line — and its body becomes a
record the JSON path scrubs, with the SAME function (Scrubber.scrub): a
structured line (`"message" key="value" key2=3`, klog's InfoS / ErrorS)
gives {"msg": message, key: value, ...}; any other body is {"msg": body}.
So the message is judged WHOLE, exactly as a JSON line's msg (kept only
when nothing in it is denied, else hashed whole), and each key / value
through KEY_NAMES / KEEP: {"format": "klog", "severity", "klog_time",
"thread", "source", "msg", ...}. Any other non-JSON line becomes
{"unparsed": true, "len": n, "h": ...}.

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

from bench import identity

KEEP = frozenset({
    "time", "level", "msg", "subsystem", "event", "kind", "class", "hint",
    "dispatch", "gvr", "group", "version", "resource", "restaction_namespace",
    "restaction_name", "wire_shape", "previous", "phase", "tier", "outcome", "state",
    "traceId", "spanId",
    # client-go's own structured fields (its throttling line: "Waited before
    # sending request" delay= reason= verb= URL=; the URL is hashed)
    "delay", "verb",
})
# Key names that may appear as written (their string values are still hashed
# unless the key is also in KEEP). Every other key name is hashed.
KEY_NAMES = KEEP | frozenset({
    "kts", "name", "namespace", "path", "host", "error", "err", "cause", "reason",
    "user", "username", "groups", "subject", "key", "l1_key", "sa_dial",
    "first_observation", "snap_seq", "armed_keys", "requested", "armed",
    "skipped_informer_miss", "skipped_other", "count", "bytes", "status", "code",
    "attempt", "seq", "gen", "elapsed_ms", "duration_ms", "wall_ms", "ms", "items",
    "unparsed", "len", "h", "URL",
})
# A KEEP value is hashed anyway when it looks like it names an identity, a
# credential or carries a key=value / quoted fragment (concatenated messages).
DENY = re.compile(
    r"scaleuser-\d+|clientconf|\buser(name)?s?\b|subject|serviceaccount|system:|token|"
    r"secret|passw|authoriz|bearer|cookie|credential|eyJ[A-Za-z0-9_-]{10,}|@|=|\"",
    re.IGNORECASE)
# #1516: snowplow's slowcall records ("slowcall.request", and "slowcall.goroutines", the snapshot taken while a
# customer request is past its bound) read as hashes, so a stall's blocked frames could not be read from a run's
# evidence (run 28 N1, #1515). slowcall restricts them itself (internal/slowcall/slowcall.go:28-40, verified in
# snapshot.go / monitor.go at main 24253666): `stacks` holds, per aggregated stack, its count, the pprof labels
# on an allowlist (snowplow_work and the fill's apistage_fill_gvr / apistage_fill_reason: a closed class, a GVR,
# a closed reason) and frames as `function file-basename:line` (frameOf, keptLabels, renderGroup), never an
# argument or a pc; `requests` holds, per in-flight slow request, its ServeMux route, work class, ages, open phases
# and waits (a phase, a mechanism, the waited key only as its redact digest `wait:<hex>`, a GVR, a dial branch and
# verb, a leader's work class, attempts and status: writeRequestLine). So these records' KEY names (each a code
# constant of slowcall) are written as they are, and those two text attributes are kept LINE BY LINE where a line
# has exactly that grammar and names no identity (DENY's identity patterns, the --deny literals and patterns, and
# bench/identity.py changing nothing); any other line is hashed alone, so one odd frame never hides the others and
# never leaks. Every other value of these records is judged as any line's is (KEEP, else hashed).
SLOWCALL_MSGS = frozenset({"slowcall.request", "slowcall.goroutines"})
SLOWCALL_TEXT = frozenset({"stacks", "requests"})
_SLOWCALL_PHASES = ("auth", "object_fetch", "rbac_check", "key_mint", "l1_get", "content_read", "flight_wait",
                    "lock_wait", "fill_admission", "stage_dial", "informer_dispatch", "informer_sync_wait",
                    "refresher_yield", "jq", "refilter", "parse", "encode", "compress", "write")
_SLOWCALL_INFO = ("kind", "ms", "outcome", "phase", "key", "gvr", "verb", "leader_class", "leader_work", "branch",
                  "attempts", "status", "last_cause", "retry_wait_ms", "conn_wait_ms", "dns_ms", "connect_ms",
                  "tls_ms", "ttfb_ms", "reused", "http_429", "http_5xx", "http_network_errors", "attempt_detail",
                  "host")
SLOWCALL_KEYS = frozenset({
    # snapshotAttrs (snapshot.go)
    "subsystem", "inflight", "slow_inflight", "slow_customers", "requests_omitted", "oldest_age_ms", "route",
    "yield_parked", "capture_ms", "goroutines", "request_goroutines", "background_goroutines", "other_goroutines",
    "stacks_kept", "stacks_omitted", "truncated", "requests", "stacks",
    # recordAttrs (monitor.go)
    "method", "class", "wall_ms", "bound_ms", "outcome", "budget", "phase", "phase_sum_ms", "unattributed_ms",
    "http_attempts", "http_429", "http_5xx", "http_network_errors", "http_reused", "retry_wait_ms", "conn_wait_ms",
    "connect_ms", "ttfb_ms", "unscoped_http_n", "unscoped_http_ms", "leads",
    # the CPU window (cpu.go windowAttrs)
    "cpu_window_ms", "cpu_window_partial", "process_cpu_ms", "process_cores", "cgroup_cpu_ms", "cfs_periods",
    "cfs_throttled_periods", "cfs_throttled_ms",
}) | {f"{p}{s}" for p in _SLOWCALL_PHASES for s in ("_ms", "_n", "_max_ms", "_open")} | {
    f"{pre}{k}" for pre in ("wait_", "dial_", "lead_") for k in _SLOWCALL_INFO}
_STACK_LABELS = ("snowplow_work", "apistage_fill_gvr", "apistage_fill_reason")
_LABEL = r"(?:%s)=[A-Za-z0-9_./-]*" % "|".join(_STACK_LABELS)
_STACKS_LINE = re.compile(
    r"^(?:== (?:request|background) stacks ==|\d+(?: \{" + _LABEL + r"(?:," + _LABEL + r")*\})?"
    r"|  [A-Za-z0-9_./()*\[\],{}-]+(?: [A-Za-z0-9_.-]+\.(?:go|s):\d+)?)$")
_TOKEN = r"[A-Za-z0-9_:./{}*-]+"
_REQUESTS_LINE = re.compile(r"^[A-Z]+ /" + r"[A-Za-z0-9_./{}*-]*" + r" class:[A-Za-z0-9_-]+ age_ms:\d+"
                            r"(?: " + _TOKEN + r")*$")
# DENY's identity alternatives alone (its "=" and '"' are a concatenated message's marks, not identities, and the
# grammar above admits no quote at all)
DENY_IDENTITY = re.compile(
    r"scaleuser-\d+|clientconf|\buser(name)?s?\b|subject|serviceaccount|system:|token|"
    r"secret|passw|authoriz|bearer|cookie|credential|eyJ[A-Za-z0-9_-]{10,}|@",
    re.IGNORECASE)
TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z (.*)$", re.DOTALL)
# klog's header (k8s.io/klog/v2): Lmmdd hh:mm:ss.uuuuuu threadid file.go:line]
# body. The file:line is a Go source file's base name: anything else is not a
# klog line.
KLOG = re.compile(r"^([IWEF])(\d{4} \d\d:\d\d:\d\d\.\d{6})\s+(\d+) ([A-Za-z0-9_.-]+\.go:\d+)\] ?(.*)$",
                  re.DOTALL)
# a structured klog body: its quoted message, then key=value pairs (a value
# quoted, Go-style, or bare)
KLOG_MSG = re.compile(r'^"((?:[^"\\]|\\.)*)"')
KLOG_KV = re.compile(r'\s+([A-Za-z_][A-Za-z0-9_.-]*)=("(?:[^"\\]|\\.)*"|[^\s"]+)')


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

    def kept(self, v: str) -> str:
        """A string the JSON path keeps (a KEEP field, not denied): through
        bench/identity.py all the same."""
        return identity.redact_identity(v)

    def klog(self, m: "re.Match") -> dict:
        """A klog line: its header kept, its body scrubbed as a JSON line's
        record is (scrub), the message judged whole."""
        sev, ts, thread, src, body = m.groups()
        return {"format": "klog", "severity": sev, "klog_time": ts, "thread": int(thread), "source": src,
                **self.scrub(klog_record(body))}

    def key(self, k: str) -> str:
        # KEY_NAMES are code constants ("user" names a field, it is not one);
        # only the caller's own literals / patterns can veto one.
        return k if k in KEY_NAMES and not self._given(k) else "k:" + _h(k)[2:]

    def slowcall_text(self, k: str, v: str) -> str:
        """#1516: a slowcall record's `stacks` / `requests`, line by line: a line of slowcall's own grammar that
        names no identity is kept, any other line is hashed alone."""
        grammar = _STACKS_LINE if k == "stacks" else _REQUESTS_LINE
        out = []
        for ln in v.split("\n"):
            ok = (ln == "" or (grammar.match(ln) is not None and not DENY_IDENTITY.search(ln)
                               and not self._given(ln) and identity.redact_identity(ln) == ln))
            out.append(ln if ok else _h(ln))
        return "\n".join(out)

    def scrub(self, rec: dict) -> dict:
        out = {}
        slowcall = rec.get("msg") in SLOWCALL_MSGS and rec.get("subsystem") == "slowcall"
        for k, v in rec.items():
            if isinstance(v, bool) or isinstance(v, (int, float)) or v is None:
                val = v
            elif slowcall and k in SLOWCALL_TEXT and isinstance(v, str):
                val = self.slowcall_text(k, v)
            elif isinstance(v, str) and k in KEEP and not self.denied(v):
                val = self.kept(v)
            else:
                val = _h(v)
            out[k if slowcall and k in SLOWCALL_KEYS and not self._given(k) else self.key(k)] = val
        return out

    def line(self, raw: str) -> str:
        try:
            rec = json.loads(raw)
        except ValueError:
            rec = None
        if not isinstance(rec, dict):
            m = KLOG.match(raw)
            if m:
                return json.dumps(self.klog(m))
            return json.dumps({"unparsed": True, "len": len(raw), "h": _h(raw)})
        return json.dumps(self.scrub(rec))


def _klog_value(v: str):
    """A klog value: a quoted one unquoted (Go's %q, JSON-compatible for the
    escapes klog writes), a bare number as a number, else the text as is."""
    if v.startswith('"'):
        try:
            return json.loads(v)
        except ValueError:
            return v[1:-1]
    for conv in (int, float):
        try:
            return conv(v)
        except ValueError:
            pass
    return v


def klog_record(body: str) -> dict:
    """A klog body as the record the JSON path scrubs: a structured body
    (`"message" k="v" k2=3`, the whole of it parsed) is {"msg": message,
    k: v, k2: 3}; any other body, printf-style, is {"msg": body}."""
    m = KLOG_MSG.match(body)
    if m:
        rec, pos = {"msg": _klog_value(m.group(0))}, m.end()
        for kv in KLOG_KV.finditer(body, pos):
            if kv.start() != pos:
                break
            rec.setdefault(kv.group(1), _klog_value(kv.group(2)))
            pos = kv.end()
        if body[pos:].strip() == "":
            return rec
    return {"msg": body}


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
