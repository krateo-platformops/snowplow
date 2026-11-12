"""#1383: the vm606 paid bench's teardown and verification gaps, where a check could report a
success it never measured, or cleanup depended on a person.

No cloud call: a fake gcloud / kubectl on PATH plays GCE and GKE. Where gcloud fails OPEN, the
fake fails open as real gcloud does (rc 0, "[]" and a WARNING on stderr), never as a fake that
exits 1, which would hide the very defect under test.
"""

from __future__ import annotations

import json
import os
import re
import textwrap
from pathlib import Path

import pytest

from bench import gcevm, gke

BENCH = Path(__file__).resolve().parents[1]
NAME = "snowplow-bench-r1383"
PROJECT = gke.DEFAULT_PROJECT
ZONE = gke.DEFAULT_ZONE
REGION = gke.region_of(ZONE)
PARTIAL = "CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR"

# Real gcloud (SDK 561, googlecloudsdk/api_lib/compute/lister.py) on a compute list whose requests
# failed: with compute/allow_partial_error true (its default) rc 0, "[]" and a WARNING; with the
# property false, rc 1 and an ERROR. FAKE_PARTIAL=1 makes every compute list such a list.
FAKE_GCLOUD = textwrap.dedent("""\
    #!/usr/bin/env python3
    import json, os, sys
    a = sys.argv[1:]
    with open(os.path.join(os.environ["FAKE_STATE"], "calls.jsonl"), "a") as fh:
        fh.write(json.dumps({"argv": a, "partial_error": os.environ.get("%(partial)s")}) + "\\n")
    words = []
    for t in a:
        if t.startswith("-"):
            break
        words.append(t)
    if words[:1] == ["compute"] and "list" in words[1:] and os.environ.get("FAKE_PARTIAL"):
        why = " - Required 'compute.%(perm)s.list' permission for 'projects/p'\\n"
        if (os.environ.get("%(partial)s") or "").lower() == "false":
            sys.stdout.write("[]\\n")
            sys.stderr.write("ERROR: (gcloud.compute." + words[1] + ".list) Some requests did not succeed:\\n" + why)
            sys.exit(1)
        sys.stdout.write("[]\\n")
        sys.stderr.write("WARNING: Some requests did not succeed.\\n" + why)
        sys.exit(0)
    if words[:1] == ["compute"] and "list" in words[1:]:
        print(os.environ.get("FAKE_LIST", "[]")); sys.exit(0)
    sys.exit(0)
    """) % {"partial": PARTIAL, "perm": "disks"}


@pytest.fixture
def fake(tmp_path, monkeypatch):
    b = tmp_path / "bin"
    b.mkdir()
    (b / "gcloud").write_text(FAKE_GCLOUD)
    (b / "gcloud").chmod(0o755)
    st = tmp_path / "state"
    st.mkdir()
    monkeypatch.setenv("PATH", f"{b}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_STATE", str(st))
    monkeypatch.delenv(PARTIAL, raising=False)
    return tmp_path


def _calls(tmp):
    p = tmp / "state" / "calls.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


def _spec():
    return gke.default_spec(NAME)


# ─── gap 1: every compute list fails closed, one definition ─────────────────

def test_a_partial_error_fails_every_teardown_compute_list(fake, monkeypatch):
    """The disk backstop, the LB release and the address backstop list through execute(): with a
    partial error (rc 0 + [] from real gcloud without the property) none reads "nothing left"."""
    monkeypatch.setenv("FAKE_PARTIAL", "1")
    spec = _spec()
    log: list[str] = []
    with pytest.raises(RuntimeError, match="listing .*disks failed"):
        gke.disk_backstop(spec, cluster_id="abc", recorded=[], log=log.append, deadline=1000.0,
                          clock=lambda: 0.0, sleep=lambda s: None)
    assert not any("no disk of" in m for m in log)
    svcs = [{"service": "krateo-system/gw", "uid": "u-1", "address": "au1"}]
    leaked, problems = gke.address_backstop(spec, services=svcs, log=log.append, deadline=1000.0,
                                            clock=lambda: 0.0)
    assert leaked == [] and any("listing" in p and "addresses failed" in p for p in problems), problems
    assert gke._gce_lb_left(spec, ["au1"], log=log.append, deadline=1000.0, clock=lambda: 0.0) is None
    lists = [c for c in _calls(fake) if c["argv"][:1] == ["compute"]]
    assert lists and all(c["partial_error"] == "false" for c in lists), lists


def test_a_partial_error_fails_the_vm_tools_lists(fake, monkeypatch):
    """bench.gcevm's executor applies the same one definition (vm606 / testvm)."""
    monkeypatch.setenv("FAKE_PARTIAL", "1")
    with gcevm.call_bound(30), pytest.raises(gcevm.Refused, match="could not be listed"):
        gcevm.list_json(["compute", "firewall-rules", "list", "--project", PROJECT, "--format", "json"], "the rules")
    assert all(c["partial_error"] == "false" for c in _calls(fake))


def test_a_compute_list_without_the_property_would_fail_open(fake, monkeypatch):
    """The hazard itself, so the fake is known to model it: the same list run without the
    property answers rc 0 and [] (what every check above would have read as "none left")."""
    monkeypatch.setenv("FAKE_PARTIAL", "1")
    monkeypatch.setattr(gke, "COMPUTE_LIST_ENV", {})
    rc, out = gke.execute(gke._gce_list_command("addresses", _spec(), ["au1"]), dry_run=False, log=lambda m: None)
    assert (rc, json.loads(out)) == (0, [])


def test_the_list_env_has_one_definition():
    """No per-site copy: the property's name is written once in the bench code, in bench/gke.py."""
    hits = [(p.name, n) for p in sorted((BENCH / "bench").glob("*.py"))
            for n, line in enumerate(p.read_text().splitlines(), 1) if PARTIAL in line]
    assert [h[0] for h in hits] == ["gke.py"], hits
    assert gke.COMPUTE_LIST_ENV == {PARTIAL: "false"}
    assert not hasattr(gke, "PRECHECK_LIST_ENV") and not hasattr(gcevm, "LIST_ENV")


@pytest.mark.parametrize("argv, listing", [
    (gke.disks_list_command(_spec(), cluster_id="x", names=["d"]), True),
    (gke._gce_list_command("forwarding-rules", _spec(), ["a"]), True),
    (gke.region_list_command("instances", PROJECT, REGION), True),
    (["compute", "firewall-rules", "list", "--project", PROJECT], True),
    (["compute", "operations", "list", "--regions", REGION], True),
    (["gcloud", "compute", "instances", "describe", "vm", "--zone", ZONE], False),
    (["compute", "ssh", "vm", "--zone", ZONE, "--", "ls list"], False),
    (gke.all_clusters_command(PROJECT), False),     # a container list: its own fail-closed read (gap 5)
])
def test_the_executors_know_a_compute_list(argv, listing):
    assert gke.is_compute_list(argv) is listing
    env = gke.gcloud_env(argv, {"X": "1"})
    assert (env.get(PARTIAL) == "false") is listing


def test_the_property_wins_over_a_callers_own_value():
    env = gke.gcloud_env(["gcloud", "compute", "disks", "list"], {PARTIAL: "true"})
    assert env[PARTIAL] == "false"
