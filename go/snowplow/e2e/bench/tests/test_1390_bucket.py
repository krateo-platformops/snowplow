"""#1390 item 1, the bucket side: `vm606 up` (and `vm606 bucket`) ensures the evidence bucket, creating it when
absent, then READS IT BACK and verifies every property Diego set (region, uniform access, public access
prevention enforced, 7-day retention not locked, 30-day lifecycle delete), refusing on any mismatch; the
orchestrator SA gets storage.objectCreator and storage.objectViewer on that bucket only, under the run's
expiring condition, and plan-iam refuses any other storage role. The fake gcloud plays the bucket
(tests/fake_vm606.py)."""

from __future__ import annotations

import json

import pytest

from bench import vm606
from tests import fake_vm606
from tests.test_vm606 import BUCKET, PROJECT, RUN, SA, _run, _up, fake  # noqa: F401  (fixture)

NAME = BUCKET[len("gs://"):]


def _bucket(fake) -> dict | None:  # noqa: F811
    p = fake / "buckets" / f"{NAME}.json"
    return json.loads(p.read_text()) if p.exists() else None


def test_up_creates_the_bucket_and_reads_back_every_property(fake, capsys, monkeypatch):  # noqa: F811
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 0, err
    b = _bucket(fake)
    assert b["location"] == "EUROPE-WEST4" and b["iamConfiguration"] == {
        "uniformBucketLevelAccess": {"enabled": True}, "publicAccessPrevention": "enforced"}
    assert b["retentionPolicy"]["retentionPeriod"] == str(7 * 86400) and not b["retentionPolicy"].get("isLocked")
    assert b["lifecycle"] == {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}
    assert (f"the evidence bucket {BUCKET} created, read back: europe-west4, uniform access, public access "
            f"prevention enforced, retention 7 days (not locked), lifecycle delete at 30 days") in out
    verbs = [" ".join(c["argv"][:3]) for c in fake_vm606.calls(fake)]
    i_create = verbs.index("storage buckets create")
    assert verbs.index("storage buckets describe", i_create) > i_create             # read back after the create
    assert i_create < min(i for i, v in enumerate(verbs) if "add-iam-policy-binding" in v)
    # the grant: both storage roles on that bucket only, under the run's expiring condition
    pol = fake_vm606.policy(fake, f"bucket-{NAME}")["bindings"]
    assert [b["role"] for b in pol] == ["roles/storage.objectCreator", "roles/storage.objectViewer"]
    assert all(b["members"] == [SA] and b["condition"]["title"] == f"snowplow-bench-{RUN}"
               and b["condition"]["expression"].startswith("request.time < timestamp(") for b in pol)
    proj = fake_vm606.policy(fake, f"project-{PROJECT}")["bindings"]
    assert not [b for b in proj if b["role"].startswith("roles/storage.")]


@pytest.mark.parametrize("drift, named", [
    ({"location": "US"}, "location: \"us\", #1390 requires \"europe-west4\""),
    ({"iamConfiguration": {"uniformBucketLevelAccess": {"enabled": False}, "publicAccessPrevention": "enforced"}},
     "uniform bucket-level access: false"),
    ({"iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}, "publicAccessPrevention": "inherited"}},
     "public access prevention: \"inherited\""),
    ({"retentionPolicy": {"retentionPeriod": "86400"}}, "retention period: \"86400\""),
    ({"retentionPolicy": {"retentionPeriod": "604800", "isLocked": True}}, "retention locked: true"),
    ({"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 3650}}]}}, "lifecycle:"),
    ({"lifecycle": None}, "lifecycle: null"),
])
def test_a_bucket_not_as_required_refuses_before_any_grant(fake, capsys, monkeypatch, drift, named):  # noqa: F811
    """Created that way (GCS made something else) or already there that way: refused, nothing granted, the
    bucket left as it is."""
    monkeypatch.setenv("FAKE_BUCKET_ON_CREATE", json.dumps(drift))
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 2 and f"the evidence bucket {BUCKET} is not as #1390 requires" in err and named in err, err
    verbs = [" ".join(c["argv"][:3]) for c in fake_vm606.calls(fake)]
    assert not [v for v in verbs if "add-iam-policy-binding" in v or v == "compute instances create"]
    before = _bucket(fake)
    monkeypatch.delenv("FAKE_BUCKET_ON_CREATE")
    rc, out, err = _run(capsys, "bucket", "--yes")                  # an existing bucket: verified, never changed
    assert rc == 2 and named in err and _bucket(fake) == before
    assert "storage buckets create" not in [" ".join(c["argv"][:3]) for c in fake_vm606.calls(fake)][len(verbs):]


def _in_use(fake):  # noqa: F811
    fake_vm606.put_rule(fake, RUN, f"{vm606.vm_name(RUN)}-iap-ssh", expires="2026-11-20T00:00:00Z")


def _unconditional(fake):  # noqa: F811
    fake_vm606.put_policy(fake, f"project-{PROJECT}", [{"role": "roles/compute.viewer", "members": [SA]}])


@pytest.mark.parametrize("refusal, says, env", [
    (_in_use, f"run {RUN} is in use", {}),
    (_unconditional, "holds unconditional binding(s)", {}),
    (None, "the VM does not fit", {"FAKE_LIMIT_E2_CPUS": "4"}),
])
def test_a_refused_up_leaves_no_bucket(fake, capsys, monkeypatch, refusal, says, env):  # noqa: F811
    """Each refusal of up comes before the bucket: a refused up creates nothing, the bucket included."""
    if refusal:
        refusal(fake)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 2 and says in err, err
    assert _bucket(fake) is None
    assert "storage buckets create" not in [" ".join(c["argv"][:3]) for c in fake_vm606.calls(fake)]


def test_a_dry_bucket_creates_nothing(fake, capsys):  # noqa: F811
    rc, out, err = _run(capsys, "bucket")
    assert rc == 0 and f"DRY: would create the evidence bucket {BUCKET}" in out
    assert f"DRY-RUN: gcloud storage buckets create {BUCKET} --project {PROJECT} --location europe-west4" in out
    assert _bucket(fake) is None


@pytest.mark.parametrize("roles", [
    ("roles/storage.objectUser",), ("roles/storage.objectAdmin",), ("roles/storage.admin",),
    ("roles/storage.objectCreator", "roles/storage.legacyBucketWriter"),
])
def test_plan_iam_refuses_any_other_storage_role(fake, capsys, monkeypatch, roles):  # noqa: F811
    """objectUser / objectAdmin can delete or overwrite evidence; any storage role grant_targets does not put on
    the bucket would land on the project: plan-iam and up refuse the plan."""
    monkeypatch.setitem(vm606.CALL_ROLES, ("storage", "cp"), roles)
    rc, out, err = _run(capsys, "plan-iam")
    assert rc == 1 and "the orchestrator SA holds no storage role but roles/storage.objectCreator and " \
                       "roles/storage.objectViewer, on the evidence bucket only" in err, err
    rc, out, err = _up(capsys, monkeypatch)
    assert rc == 2 and "holds no storage role but" in err
    assert not [c for c in fake_vm606.calls(fake) if "add-iam-policy-binding" in " ".join(c["argv"][:3])]
