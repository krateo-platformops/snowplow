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

# a snapshot exactly as snowplow renders it (main 5dc927ff): renderGroup's "<count> {<keptLabels>}" header (the
# allowlisted labels sorted, each "k=v"; the fill's GVR label is gvr.String(), apistage.go:1005), its frames
# "fn file:line" (frameOf), and writeRequestLine's request lines, whose waits name a GVR as
# schema.GroupVersionResource.String() ("<group>/<version>, Resource=<resource>"; the GVR strings of slowcall's
# own tests, slowcall_1054_test.go:185 and review2_1059_test.go:376, and the core group's "/v1, Resource=...")
STACKS = ("== request stacks ==\n"
          "41 {snowplow_work=customer}\n"
          "  sync.runtime_notifyListWait sema.go:597\n"
          "  github.com/krateo-platformops/snowplow/internal/cache.(*flight).wait flight.go:212\n"
          "  net/http.(*conn).serve server.go:2102\n"
          "== background stacks ==\n"
          "3 {apistage_fill_gvr=composition.krateo.io/v1, Resource=fsas,apistage_fill_reason=miss,"
          "snowplow_work=refresh}\n"
          "  github.com/krateo-platformops/snowplow/internal/refresher.(*worker).run worker.go:88\n"
          "2 {apistage_fill_gvr=/v1, Resource=configmaps,snowplow_work=customer}\n"
          "  github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api.dispatch internal_dispatch.go:477\n")
REQUESTS = ("GET /call class:customer age_ms:8173 open:stage_dial:1 wait{stage_dial dial g/v1, Resource=long "
            "internal_rest_config list attempts:2 status:0 age_ms:8102}\n"
            "GET /call class:customer age_ms:8170 open:flight_wait:1 wait{flight_wait content_flight wait:77aa00bb11cc "
            "composition.krateo.io/v1, Resource=fsas leader:refresh age_ms:8090}\n"
            "GET /call class:background age_ms:6000 lead{flight_wait content_flight wait:0a1b2c3d4e5f "
            "/v1, Resource=configmaps age_ms:5990}\n")


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
    # the lines that say what a stalled request waited on, in the GVR shape snowplow renders: a fill-labelled
    # stack header, a dial and a flight wait naming their GVR, a lead on the core group
    assert "3 {apistage_fill_gvr=composition.krateo.io/v1, Resource=fsas,apistage_fill_reason=miss," in out["stacks"]
    assert "2 {apistage_fill_gvr=/v1, Resource=configmaps,snowplow_work=customer}" in out["stacks"]
    assert "wait{stage_dial dial g/v1, Resource=long internal_rest_config list attempts:2" in out["requests"]
    assert "content_flight wait:77aa00bb11cc composition.krateo.io/v1, Resource=fsas leader:refresh" in out["requests"]
    assert "/v1, Resource=configmaps age_ms:5990}" in out["requests"]
    assert not any(ln.startswith("h:") for ln in out["stacks"].split("\n") + out["requests"].split("\n"))
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
    # a GVR is admitted as its own production, and a RESTAction can template its stage path from request data:
    # a requester-chosen GVR still goes through every identity guard
    ("requests", "GET /call class:customer age_ms:9 wait{flight_wait content_flight scaleuser-0042.io/v1, "
                 "Resource=things age_ms:9}", "an identity pattern in an admitted GVR"),
    ("stacks", "4 {apistage_fill_gvr=benchuser-7.example/v1, Resource=things}", "a --deny literal in an admitted GVR"),
    ("requests", "GET /call class:customer age_ms:9 wait{flight_wait content_flight g/v1, Resource=workforcePools/"
                 "acme age_ms:9}", "a GVR shape that is not one (its resource holds a '/')"),
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
    other, fake, same_subsystem = _scrub({"msg": "something.else", "stacks": STACKS, "inflight": 3},
                                         _snapshot(subsystem="other"), _snapshot(msg="slowcall.other"))
    assert "stacks" not in other and "inflight" not in other
    assert any(v == logscrub._h(STACKS) for v in other.values())
    for rec in (fake, same_subsystem):     # slowcall's subsystem without one of its two messages is not one of them
        assert "stacks" not in rec and "inflight" not in rec, rec
        assert not any(isinstance(v, str) and "flight.go" in v for v in rec.values())


def test_a_key_name_slowcall_never_writes_is_hashed_even_in_its_records():
    """Only slowcall's own key names are written as they are: any other key of a slowcall record is hashed."""
    (out,) = _scrub(_snapshot(tenant_acme_ms=5))
    assert "tenant_acme_ms" not in out and 5 in out.values()


def test_a_slowcall_key_name_that_carries_a_deny_literal_is_hashed():
    """A --deny literal vetoes even one of slowcall's own key names (run606 passes the bench's user prefix and the
    test password)."""
    (out,) = _scrub(_snapshot(), deny=("wait_kind",))
    assert "wait_kind" not in out and "inflight" in out

