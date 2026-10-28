"""gke-down while another operation runs on the cluster (run 15, B).

GKE refused B's delete: "Cluster is running incompatible operation
operation-…" and run606 gave up ("TEARDOWN FAILED — delete by hand"). The
delete now waits for THAT operation (`gcloud container operations wait`) and
retries, bounded by the teardown budget; any other failure still raises at
once. A stand-in gcloud on PATH plays GKE.
"""

from __future__ import annotations

import os
import textwrap
import time
from pathlib import Path

import pytest

from bench import gke

FAKE_GCLOUD = textwrap.dedent("""\
    #!/usr/bin/env python3
    import json, os, sys, time
    a = sys.argv[1:]
    st = os.environ["FAKE_STATE"]
    def log(x):
        open(os.path.join(st, "calls"), "a").write(x + "\\n")
    def opt(n):
        return a[a.index(n) + 1] if n in a else None
    if a[:3] == ["container", "clusters", "describe"]:
        if os.path.exists(os.path.join(st, "deleted")):          # #895: the delete went through
            log("gone")
            sys.stderr.write("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, "
                             "message=Not found: clusters/" + a[3] + ".\\n")
            sys.exit(1)
        log("describe")
        print(json.dumps({"name": a[3], "id": "d8cac43bc0ac45d28fe8bdd1ccbe101a68fc42887613486d883c5066a9fc7796", "resourceLabels": {
            "purpose": "snowplow-bench", "issue": "606", "created-by": "bench-gke", "owner": "t",
            "ttl-expires": str(int(time.time()) + 3600)}}))
        sys.exit(0)
    if a[:3] == ["container", "clusters", "delete"]:
        ops = os.environ.get("FAKE_BLOCKING_OPS", "").split()
        n = len([l for l in open(os.path.join(st, "calls")).read().splitlines() if l.startswith("delete")]) \\
            if os.path.exists(os.path.join(st, "calls")) else 0
        log("delete")
        if os.environ.get("FAKE_DELETE_OTHER_ERROR"):
            sys.stderr.write("ERROR: (gcloud.container.clusters.delete) permission denied\\n"); sys.exit(1)
        if n < len(ops):
            sys.stderr.write("ERROR: (gcloud.container.clusters.delete) Some requests did not succeed:\\n"
                             f" - ResponseError: code=400, message=Cluster is running incompatible operation {ops[n]}.\\n")
            sys.exit(1)
        open(os.path.join(st, "deleted"), "w").write("1")
        sys.exit(0)
    if a[:3] == ["compute", "disks", "list"]:
        log("disks-list")
        print("[]")
        sys.exit(0)
    if a[:3] == ["container", "operations", "wait"]:
        log(f"wait {a[3]} {opt('--project')} {opt('--region')}")
        time.sleep(float(os.environ.get("FAKE_WAIT_S", "0")))
        sys.exit(int(os.environ.get("FAKE_WAIT_RC", "0")))
    sys.exit(2)
    """)


@pytest.fixture
def fakegke(tmp_path, monkeypatch):
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "gcloud").write_text(FAKE_GCLOUD)
    (bin_ / "gcloud").chmod(0o755)
    (bin_ / "kubectl").write_text(FAKE_KUBECTL)
    (bin_ / "kubectl").chmod(0o755)
    st = tmp_path / "state"
    st.mkdir()
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_STATE", str(st))
    return st


# no PV in the cluster (#895's PV release and disk backstop: test_gke_pvc_disks)
FAKE_KUBECTL = '#!/usr/bin/env python3\nimport json, os\nopen(os.path.join(os.environ["FAKE_STATE"], "calls"), "a").write("kubectl\\n")\nprint(json.dumps({"items": []}))\n'


def _calls(st: Path):
    """The cluster operations, in order (the PV release, the gone-poll and the
    disk backstop around them are #895's, tested in test_gke_pvc_disks)."""
    p = st / "calls"
    return [c for c in (p.read_text().splitlines() if p.exists() else [])
            if c not in ("kubectl", "gone", "disks-list")]


SPEC = gke.default_spec("snowplow-bench-r15b")
OP1, OP2 = "operation-1792823484322-b2555fe0", "operation-1792823999000-c0ffee00"


def test_a_delete_blocked_by_an_operation_waits_for_it_then_deletes(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_BLOCKING_OPS", OP1)
    gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60)
    assert _calls(fakegke) == ["describe", "delete", f"wait {OP1} {SPEC.project} {SPEC.region}", "delete"]


def test_each_new_blocking_operation_is_waited_for(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_BLOCKING_OPS", f"{OP1} {OP2}")
    gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60)
    assert _calls(fakegke) == ["describe", "delete", f"wait {OP1} {SPEC.project} {SPEC.region}", "delete",
                               f"wait {OP2} {SPEC.project} {SPEC.region}", "delete"]


def test_a_failed_wait_still_retries_the_delete(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_BLOCKING_OPS", OP1)
    monkeypatch.setenv("FAKE_WAIT_RC", "1")
    gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60)
    assert _calls(fakegke)[-1] == "delete" and _calls(fakegke).count("delete") == 2


def test_any_other_delete_failure_raises_at_once(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_DELETE_OTHER_ERROR", "1")
    with pytest.raises(RuntimeError, match="delete snowplow-bench-r15b failed"):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=60)
    assert _calls(fakegke) == ["describe", "delete"]


def test_the_wait_is_bounded_by_the_budget(fakegke, monkeypatch):
    """An operation that outlasts the budget: the wait is cut there, and the
    teardown fails loudly (run606 prints "delete by hand")."""
    monkeypatch.setenv("FAKE_BLOCKING_OPS", OP1)
    monkeypatch.setenv("FAKE_WAIT_S", "30")
    t0 = time.monotonic()
    with pytest.raises(RuntimeError, match=f"{OP1} still running when the budget ran out"):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=1.5)
    assert time.monotonic() - t0 < 15


def test_an_operation_still_blocking_when_the_budget_is_spent_fails(fakegke, monkeypatch):
    monkeypatch.setenv("FAKE_BLOCKING_OPS", " ".join([OP1] * 50))
    ticks = iter(range(0, 10_000, 40))           # each clock read: 40 s later
    with pytest.raises(RuntimeError, match="still blocked by"):
        gke.teardown(SPEC, dry_run=False, log=lambda _: None, budget_s=100,
                     clock=lambda: next(ticks))
    assert _calls(fakegke).count("delete") <= 3


def test_gke_down_passes_the_budget(fakegke, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_BLOCKING_OPS", OP1)
    assert gke.main(["gke-down", "--name", SPEC.name, "--budget-s", "60", "--yes"]) == 0
    assert f"wait {OP1} {SPEC.project} {SPEC.region}" in _calls(fakegke)
    # and honours it: an operation that outlasts --budget-s fails gke-down, fast
    (fakegke / "calls").unlink()
    (fakegke / "deleted").unlink()
    monkeypatch.setenv("FAKE_WAIT_S", "30")
    t0 = time.monotonic()
    assert gke.main(["gke-down", "--name", SPEC.name, "--budget-s", "1.5", "--yes"]) == 1
    assert time.monotonic() - t0 < 15
    assert "still running when the budget ran out" in capsys.readouterr().err


def test_run606_teardown_gives_gke_down_its_budget():
    src = (Path(__file__).resolve().parents[1] / "run606.sh").read_text()
    line = next(ln for ln in src.splitlines() if "bench.gke gke-down" in ln)
    assert '--budget-s "$DOWN_BUDGET_S"' in line
    # DOWN_BUDGET_S is already the run budget's gke-down term
    assert 'DOWN_BUDGET_S="${DOWN_BUDGET_S:-900}"' in src
