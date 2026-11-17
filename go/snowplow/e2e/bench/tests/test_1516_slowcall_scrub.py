"""#1516: snowplow's slowcall records read in a run's evidence. The log scrubber hashed every whole string, so the
goroutine snapshots taken at run 28 N1's stalls (#1515) named no frame. slowcall restricts `stacks` and `requests`
itself (internal/slowcall/slowcall.go:28-40: function names, file basenames, line numbers, allowlisted pprof
labels; routes as ServeMux patterns, a waited key only as its redact digest), so the scrubber keeps, in these
records only, slowcall's own key names, and each line of those two attributes that has slowcall's grammar and
names no identity. Every case goes through the CLI run606 streams snowplow's log through.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from bench import logscrub
from tests.test_run606_real_sequence import BENCH_DIR

# a snapshot as snapshot.go's snapshotAttrs logs it (renderGroup / writeRequestLine shapes)
STACKS = ("== request stacks ==\n"
          "41 {snowplow_work=customer}\n"
          "  sync.runtime_notifyListWait sema.go:597\n"
          "  github.com/krateo-platformops/snowplow/internal/cache.(*flight).wait flight.go:212\n"
          "  net/http.(*conn).serve server.go:2102\n"
          "== background stacks ==\n"
          "3 {apistage_fill_gvr=composition.krateo.io/v1-2-2/githubscaffoldingwithcompositionpages,snowplow_work=refresher}\n"
          "  github.com/krateo-platformops/snowplow/internal/refresher.(*worker).run worker.go:88\n")
REQUESTS = ("GET /call class:customer age_ms:8173 open:stage_dial:1 wait{stage_dial dial wait:3f9a0c1d2e4b "
            "composition.krateo.io/v1-2-2/githubscaffoldingwithcompositionpages informer get attempts:2 status:0 "
            "age_ms:8102}\n"
            "GET /call class:customer age_ms:8170 open:flight_wait:1 wait{flight_wait content_flight wait:77aa00bb11cc "
            "leader:customer age_ms:8090}\n")


def _snapshot(**over):
    rec = {"time": "2026-11-16T21:13:35.1Z", "level": "WARN", "msg": "slowcall.goroutines", "subsystem": "slowcall",
           "inflight": 52, "slow_inflight": 49, "slow_customers": 49, "requests_omitted": 0, "oldest_age_ms": 8173,
           "route": "GET /call", "yield_parked": 0, "capture_ms": 4, "goroutines": 1210, "request_goroutines": 52,
           "background_goroutines": 9, "other_goroutines": 1149, "stacks_kept": 2, "stacks_omitted": 0,
           "truncated": False, "wait_kind": "dial", "wait_ms": 8102, "wait_phase": "stage_dial",
           "wait_key": "wait:3f9a0c1d2e4b", "cpu_window_ms": 8200, "process_cpu_ms": 900, "requests": REQUESTS,
           "stacks": STACKS}
    rec.update(over)
    return rec


def _scrub(*recs, deny=("benchuser",)):
    p = subprocess.run([sys.executable, "-m", "bench.logscrub", *(x for d in deny for x in ("--deny", d))],
                       input="".join(json.dumps(r) + "\n" for r in recs), cwd=BENCH_DIR, capture_output=True,
                       text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    return [json.loads(x) for x in p.stdout.splitlines()]


def test_a_stall_snapshot_names_its_blocked_frames():
    (out,) = _scrub(_snapshot())
    # slowcall's own key names, as written; its numbers as they are
    for k in ("inflight", "slow_customers", "oldest_age_ms", "stacks_kept", "wait_ms", "cpu_window_ms", "requests",
              "stacks", "wait_kind", "wait_key"):
        assert k in out, (k, sorted(out))
    assert out["oldest_age_ms"] == 8173 and out["stacks_kept"] == 2
    # the stacks and requests, whole, in clear: the blocked frames are named
    assert out["stacks"] == STACKS and out["requests"] == REQUESTS
    assert "internal/cache.(*flight).wait flight.go:212" in out["stacks"]
    # every other string value is judged as before: wait_key (a digest) and route are not KEEP fields
    assert out["wait_key"].startswith("h:") and out["route"].startswith("h:")


@pytest.mark.parametrize("attr,line,why", [
    # the grammar: a pprof label outside slowcall's allowlist (a free string some code chose)
    ("stacks", "7 {tenant=acme}", "a label key outside the allowlist"),
    # DENY's identity patterns: a service account named in a request line
    ("requests", "GET /call class:customer age_ms:9 wait{auth review system:serviceaccount:team:bot age_ms:9}",
     "an identity pattern"),
    # the --deny literals (run606: the bench's user prefix, the test password)
    ("requests", "GET /call class:customer age_ms:9 wait{auth review benchuser-0042 age_ms:9}", "a --deny literal"),
    # bench/identity.py: an external IdP's pool, which no DENY pattern names
    ("requests", "GET /call class:customer age_ms:9 wait{auth review workforcePools/acme-pool age_ms:9}",
     "bench/identity.py"),
    # an e-mail in a frame's place
    ("stacks", "  alice@example.com x.go:1", "the grammar and an identity pattern"),
])
def test_a_line_that_names_an_identity_is_still_hashed_and_the_others_stay_readable(attr, line, why):
    """The allowlist cannot leak: each guard alone hashes the line (mutation: dropping that guard turns its case
    RED), and only that line."""
    base = STACKS if attr == "stacks" else REQUESTS
    (out,) = _scrub(_snapshot(**{attr: base + line + "\n"}))
    got = out[attr].split("\n")
    assert line not in out[attr], why
    assert got[-2].startswith("h:") and len(got[-2]) == 14, got[-2]
    assert "\n".join(got[:-2]) + "\n" == base                      # the other lines, untouched


def test_only_slowcalls_own_records_are_allowlisted():
    """A record that is not slowcall's (another msg, or no slowcall subsystem) keeps today's rule: an unknown key
    name is hashed and its text is hashed whole."""
    other, fake = _scrub({"msg": "something.else", "stacks": STACKS, "inflight": 3},
                         _snapshot(subsystem="other"))
    assert "stacks" not in other and "inflight" not in other
    assert any(v == logscrub._h(STACKS) for v in other.values())
    assert "stacks" not in fake and "inflight" not in fake
    assert not any(isinstance(v, str) and "flight.go" in v for v in fake.values())

