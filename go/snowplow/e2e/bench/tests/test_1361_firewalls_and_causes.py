"""#1361: the first live `bench.testvm up` runs (2026-11-11, #1338) refused twice, and the refusals did not say why.

  - tvsmoke1 (23:37Z): "firewall rule testvm-tvsmoke1-iap-ssh could not be created (rc=1; no error code; the error
    text is withheld (it may name the account))". GCE had answered QUOTA_EXCEEDED, "Quota 'FIREWALLS' exceeded. Limit:
    100.0 globally": the project-global FIREWALLS quota was 99 of 100, and no preflight read it.
  - tvsmoke2 (23:51Z): "instances create failed (rc=1; no error code; ...)". The audit log holds the answer: insert
    refused at once, INVALID_ARGUMENT "The resource 'projects/<p>/regions/europe-west3/subnetworks/default' is not
    ready", while another tenant's GKE create patched that subnetwork (23:50:49-23:51:08Z).

So, at the ONE place every gcloud refusal of testvm and vm606 goes through (bench.gcevm.gcloud_cause): the cause is
kept and only the identity dropped, by #1354's redaction (bench.noanswer.cause); the project's FIREWALLS is read
before anything is made, its need derived from what the run makes (bench.gke RUN_QUOTAS, the vm's SSH rules); and a
create GCE calls "not ready" waits for the operation that makes it so (bench.gcevm.create). The testvm and vm606 arms
are in tests/test_testvm.py and tests/test_vm606.py; this file holds the shared parts.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from bench import gcevm, gke, identity, lb_need, noanswer
from tests import fake_region, fake_vm606
from tests.test_1026_lb_addresses import RUN19, _chart, _values
from tests.test_gke_pvc_disks import NAME, _calls, fakes, quota_line  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_testvm import CAUSES

FIX = Path(__file__).resolve().parent / "fixtures"
LIVE = json.loads((FIX / "firewalls_bench_20261112.json").read_text())
PROJECT = fake_region.PROJECT


def _text(key: str) -> str:
    return fake_vm606.GCLOUD_FAILURES[key].format(cmd="instances.create", project=PROJECT, zone="europe-west3-b")


# ─── 1. the cause survives the redaction; the identity does not ────────────

@pytest.mark.parametrize("key", sorted(CAUSES))
def test_gcloud_cause_keeps_each_cause_and_drops_the_account(key):
    got = gcevm.gcloud_cause(_text(key))
    for word in CAUSES[key]:
        assert word in got, (word, got)
    _no_identity(got)
    assert "@" not in got
    # the code-only summary it replaces had no word for any of them (the instrument with no vocabulary)
    assert identity.error_summary(_text(key)).startswith("no error code")


# #1361 review B1: an account that is no e-mail, in each form gcloud and the IAM API name one; every line through the
# bench's one redaction (bench.identity, so every caller of it), the subject an IdP's own opaque id
SUBJECT = "0f3c2a91-77aa-4c1e-9d2b-3e1a5b6c7d8e"
PRINCIPAL = f"principal://iam.googleapis.com/locations/global/workforcePools/acme-pool/subject/{SUBJECT}"
NON_EMAIL = [
    f"Your current active account [{PRINCIPAL}] does not have any valid credentials",
    f"{PRINCIPAL} does not have compute.instances.create access to the Google Cloud project.",
    f"Required 'compute.firewalls.create' permission for 'projects/p/global/firewalls/x' for principal '{PRINCIPAL}'",
    f'member "{PRINCIPAL}" is not allowed',
    f"principalSet://iam.googleapis.com/locations/global/workforcePools/acme-pool/group/{SUBJECT}: denied",
    f"granted to locations/global/workforcePools/acme-pool/subject/{SUBJECT} (no scheme)",
    f"[{SUBJECT}] does not have permission to access projects instance [p]",
    f"Your current active account [{SUBJECT}] does not have any valid credentials",
    f"Reauthentication for account [{SUBJECT}] failed; run gcloud auth login",
    f"for account '{SUBJECT}'",
]


@pytest.mark.parametrize("line", NON_EMAIL)
def test_an_identity_that_is_no_email_is_redacted_everywhere(line):
    for out in (identity.redact_identity(line), identity.redact_text(line), gcevm.gcloud_cause(line),
                identity.error_summary(line), " | ".join(identity.summarize(line))):
        assert SUBJECT not in out and "acme-pool" not in out and "principal://" not in out, out


def test_a_wrapped_authenticated_as_sentence_goes_whole():
    text = ("ERROR: (gcloud.compute.networks.describe) HTTPError 404: The resource 'projects/p/global/networks/n' was "
            f"not found. This command is authenticated as\n{PRINCIPAL} which is the active account specified by the\n"
            "[core/account] property.\n - next line stays\n")
    assert identity.redact_text(text) == ("ERROR: (gcloud.compute.networks.describe) HTTPError 404: The resource "
                                          "'projects/p/global/networks/n' was not found.\n - next line stays")
    # and line by line (redact_identity's callers), the account on the next line still goes
    assert all(SUBJECT not in identity.redact_identity(ln) for ln in text.splitlines())
    assert identity.redact_identity(f"authenticated as\n{SUBJECT}") == "authenticated as <redacted>"


def _no_identity(text: str) -> None:
    for leak in ("PLACEHOLDER", "@example.invalid", "authenticated as", "principal://", "workforcePools/P"):
        assert leak not in text, (leak, text)


# ─── the not-ready wait: one deadline, every call within what is left of it (review m2) ─

class _Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, s):
        self.now += s


NOT_READY_ERR = ("ERROR: (gcloud.compute.instances.create) Could not fetch resource:\n - The resource "
                 f"'projects/{PROJECT}/regions/europe-west3/subnetworks/default' is not ready\n")
OP = {"name": "op-1", "operationType": "compute.subnetworks.patch", "status": "RUNNING",
      "targetLink": f"https://www.googleapis.com/compute/beta/projects/{PROJECT}/regions/europe-west3/subnetworks/default"}


def _scripted(monkeypatch, durations: dict, done_after: int | None):
    """gcevm's gcloud, played: each call takes durations[verb] seconds of the fake clock and is recorded with the
    timeout it was given and the time left when it started; describe says DONE on its `done_after`-th call."""
    clock, calls, n = _Clock(), [], {"describe": 0}
    monkeypatch.setattr(gcevm, "time", clock)

    def gcloud(args, *, dry=False, env=None, input_data=None, timeout=None, quiet=False):
        verb = args[2] if args[:2] == ["compute", "operations"] else "create"
        calls.append({"verb": verb, "timeout": timeout, "at": clock.now})
        clock.now += durations[verb]
        if verb == "create":
            return 1, "", NOT_READY_ERR
        if verb == "list":
            return 0, json.dumps([OP]), ""
        n["describe"] += 1
        return 0, json.dumps({**OP, "status": "DONE" if n["describe"] == done_after else "RUNNING"}), ""

    monkeypatch.setattr(gcevm, "gcloud", gcloud)
    return clock, calls


def test_every_call_of_the_wait_gets_what_is_left_of_one_deadline_and_none_runs_past_it(monkeypatch):
    clock, calls = _scripted(monkeypatch, {"create": 10, "list": 10, "describe": 10}, done_after=None)
    with gcevm.call_bound(100), pytest.raises(gcevm.Refused) as e:
        gcevm.create(["compute", "instances", "create", "x"], dry=False, poll_s=15, what="instance x", hint="HINT")
    msg = str(e.value)
    assert msg.startswith("instance x: the call's bound of 100 s is spent; GCE's last answer: ") and "is not ready" in msg
    assert msg.endswith("(operation op-1 had not ended): nothing was created by this call; HINT")
    assert [c["verb"] for c in calls][:3] == ["create", "list", "describe"]
    for c in calls:
        assert c["timeout"] == pytest.approx(100 - c["at"]) and c["at"] < 100, c       # the list's too: never its own
    assert clock.now <= 100 + 10                                                       # the last call's own length


def test_a_create_made_again_gets_only_what_is_left_no_floor(monkeypatch):
    """The patch ends 0.5 s before the deadline: the create is made again within 0.5 s, not a floor of its own;
    once the deadline passes, the refusal says GCE's last answer and the hint."""
    clock, calls = _scripted(monkeypatch, {"create": 10, "list": 10, "describe": 79.5}, done_after=1)
    with gcevm.call_bound(100), pytest.raises(gcevm.Refused) as e:
        gcevm.create(["compute", "instances", "create", "x"], dry=False, poll_s=15, what="instance x", hint="HINT")
    creates = [c for c in calls if c["verb"] == "create"]
    assert len(creates) == 2 and creates[1]["timeout"] == pytest.approx(0.5)
    assert "the call's bound of 100 s is spent" in str(e.value) and str(e.value).endswith("; HINT")
    assert "subnetworks/default' is not ready" in str(e.value)


def test_a_list_that_fails_inside_the_wait_refuses_with_the_hint(monkeypatch):
    clock, calls = _scripted(monkeypatch, {"create": 1, "list": 1, "describe": 1}, done_after=1)
    real = gcevm.gcloud

    def gcloud(args, **kw):
        if args[:3] == ["compute", "operations", "list"]:
            return 1, "", "ERROR: (gcloud.compute.operations.list) Some requests did not succeed:\n - code=503\n"
        return real(args, **kw)

    monkeypatch.setattr(gcevm, "gcloud", gcloud)
    with gcevm.call_bound(100), pytest.raises(gcevm.Refused) as e:
        gcevm.create(["compute", "instances", "create", "x"], dry=False, poll_s=15, what="instance x", hint="HINT")
    assert str(e.value).startswith("instance x: the operations of --regions europe-west3 could not be listed (rc=1; ")
    assert str(e.value).endswith("; HINT")


def test_the_authenticated_as_sentence_goes_whole_and_the_rest_of_the_line_stays():
    got = gcevm.gcloud_cause("ERROR: (gcloud.compute.disks.describe) HTTPError 404: The resource 'projects/p/zones/z/"
                             "disks/d' was not found. This command is authenticated as first.last@corp.example which "
                             "is the active account specified by the [core/account] property. Retry later.\n")
    assert got == ("ERROR: (gcloud.compute.disks.describe) HTTPError 404: The resource 'projects/p/zones/z/disks/d' "
                   "was not found. Retry later.")


def test_gcloud_cause_is_noanswers_redaction_after_the_identity_one():
    """Reused, not duplicated: a signed URL's query and a bearer token go as bench.noanswer.redact takes them, a long
    text is shortened from its START (gcloud says the cause last)."""
    got = gcevm.gcloud_cause("ERROR: x <https://compute.googleapis.com/x?alt=json&access_token=SECRET> Bearer ya29.abc\n")
    assert "SECRET" not in got and "ya29.abc" not in got and "?<redacted>" in got and "Bearer <redacted>" in got
    long = "ERROR: (gcloud.compute.instances.create) " + "w" * (noanswer.CAUSE_MAX * 2) + "\n - Quota 'X' exceeded.\n"
    got = gcevm.gcloud_cause(long)
    assert got.startswith("...") and got.endswith("Quota 'X' exceeded.") and len(got) == noanswer.CAUSE_MAX + 3
    assert gcevm.gcloud_cause("") == gcevm.gcloud_cause(None) == "no error text"


def test_every_gcloud_refusal_of_testvm_and_vm606_says_the_cause():
    """No gcloud API call's refusal in the two tools (and the module they share) is left on the code-only summary:
    error_summary remains only where an SSH / scp / kubectl failure is summarised (an OS Login refusal names the POSIX
    user; kubectl's names a user)."""
    bench = Path(gcevm.__file__).resolve().parent
    seen = []
    for mod in ("gcevm.py", "testvm.py", "vm606.py"):
        lines = (bench / mod).read_text().splitlines()
        for i, ln in enumerate(lines):
            if "error_summary(err" in ln:
                ctx = "\n".join(lines[max(0, i - 12):i + 1])
                why = re.findall(r"_kubectl\(|\bscp\(|def failed\(|def ssh_stage\(", ctx)
                assert why, (mod, i + 1, ctx)
                seen.append(why[-1])
    assert sorted(set(seen)) == ["_kubectl(", "def failed(", "def ssh_stage(", "scp("]


# ─── 2. FIREWALLS: what the run makes, against the live project ────────────

def _cluster_rules() -> dict[str, list[dict]]:
    by: dict[str, list[dict]] = {}
    for r in LIVE["rules"]:
        for t in r["targetTags"]:
            if t.startswith("gke-snowplow-bench-"):
                by.setdefault(t, []).append(r)
    return by


_PATTERNS = {
    "gke-<cluster>-<hash>-all": r"gke-snowplow-bench-[\w-]+-all", "gke-<cluster>-<hash>-vms": r"gke-snowplow-bench-[\w-]+-vms",
    "k8s-<cluster-id>-node-http-hc": r"k8s-[0-9a-f]{16}-node-http-hc", "k8s-fw-<lb-id>": r"k8s-fw-a[0-9a-f]{31}",
    "k8s-fw-<lb-id>-deny": r"k8s-fw-a[0-9a-f]{31}-deny",
}


def test_every_rule_gke_made_for_a_bench_cluster_is_one_the_table_counts():
    """The nine bench clusters of runs 24-26 (GKE 1.36), read live: every rule targeting a cluster's nodes is of a kind
    GKE_FIREWALLS names, and each cluster held exactly what _firewalls_need says for its LoadBalancer Services."""
    assert set(_PATTERNS) == {n for n, _ in gke.GKE_FIREWALLS}
    clusters = _cluster_rules()
    assert len(clusters) == 9
    for tag, rules in clusters.items():
        kinds = [next((k for k, rx in _PATTERNS.items() if re.fullmatch(rx, r["name"])), None) for r in rules]
        assert None not in kinds, (tag, [r["name"] for r, k in zip(rules, kinds) if k is None])
        lbs = {r["description"]["kubernetes.io/service-name"] for r in rules if r["name"].startswith("k8s-fw-")}
        fp = gke.footprint(gke.default_spec(NAME), lb_services=len(lbs))
        assert [n for m, n, _ in gke._firewalls_need(fp)] == [len(rules)], (tag, sorted(r["name"] for r in rules))
        assert sorted(kinds) == sorted(k for k, w in gke.GKE_FIREWALLS
                                       for _ in range(len(lbs) if w == "LoadBalancer Service" else 1))
    # each orchestrator: its two SSH rules, the ones gcevm.ssh_rules makes
    orch = {}
    for r in LIVE["rules"]:
        for t in r["targetTags"]:
            if t.startswith("snowplow-bench-orch-"):
                orch.setdefault(t, set()).add(r["name"])
    assert orch and all(names == set(gcevm.ssh_rules(t, "")) for t, names in orch.items())


def test_the_load_balancer_count_is_the_installs_own(tmp_path):
    """The LoadBalancer Services come from the install's chart and values (bench/lb_need.py), never a number here:
    run 19's install makes krateo-agent-gateway-platform, the one Service each live bench cluster's k8s-fw pair is
    for."""
    rows = lb_need.planned(str(_chart(tmp_path, RUN19)), str(_values(tmp_path)))
    names = {f"krateo-system/{r['service']}" for r in rows}
    live = {r["description"]["kubernetes.io/service-name"] for rs in _cluster_rules().values() for r in rs
            if r["name"].startswith("k8s-fw-")}
    assert names == live == {"krateo-system/krateo-agent-gateway-platform"}
    fp = gke.footprint(gke.default_spec(NAME), lb_services=len(rows))
    assert gke._firewalls_need(fp)[0][1] == 5


@pytest.mark.parametrize("clusters, lb, want", [(1, 1, 5), (1, 0, 2), (1, 3, 9), (3, 3, 15), (2, 1, 7)])
def test_the_need_follows_the_clusters_and_their_load_balancers(clusters, lb, want):
    """2 GKE rules a cluster, a health-check rule a cluster with a LoadBalancer Service (at most min(clusters, LB
    Services) of them in a summed footprint), 2 rules a LoadBalancer Service."""
    one = gke.footprint(gke.default_spec(NAME), lb_services=0)
    fp = gke.Footprint(clusters=0)
    for _ in range(clusters):
        fp = fp + one
    fp.lb_services = lb
    assert gke._firewalls_need(fp)[0][1] == want


def test_the_firewalls_row_reads_the_project_and_refuses_what_does_not_fit():
    region = fake_region.quotas({})
    one = gke.footprint(gke.default_spec(NAME), lb_services=1)
    ok, msg = quota_line(region, one, "FIREWALLS", {"FAKE_PROJECT_USAGE_FIREWALLS": "95"})
    assert ok and msg == ("FIREWALLS in operations-dev-krateo-io: 95 of 100 used, 5 free; the run needs 5: 2 GKE rules "
                          "x 1 cluster(s) + 1 health-check rule x 1 cluster(s) with a LoadBalancer Service + 2 (allow, "
                          "deny) x 1 LoadBalancer Service(s)")
    ok, msg = quota_line(region, one, "FIREWALLS", {"FAKE_PROJECT_USAGE_FIREWALLS": "96"})
    assert not ok and msg.startswith("FIREWALLS in operations-dev-krateo-io: 96 of 100 used, 4 free; the run needs 5")
    assert gke.FIREWALLS_HINT in msg
    # a project that does not list FIREWALLS: unknown, refused
    ok, msg = quota_line(region, one, "FIREWALLS", {"FAKE_PROJECT_QUOTAS": json.dumps(
        [{"metric": "FIREWALLS", "limit": None, "usage": None}])})
    assert not ok and "whether the run fits is unknown" in msg


def test_gke_up_refuses_before_any_cluster_when_the_projects_firewalls_do_not_fit(fakes, monkeypatch, capsys):  # noqa: F811
    monkeypatch.setenv("FAKE_PROJECT_USAGE_FIREWALLS", "96")
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    err = capsys.readouterr().err
    assert "refusing: FIREWALLS in operations-dev-krateo-io: 96 of 100 used, 4 free; the run needs 5" in err, err
    assert not any(x[:4] == ["gcloud", "container", "clusters", "create"] for x in _calls(fakes))
    monkeypatch.setenv("FAKE_PROJECT_USAGE_FIREWALLS", "95")
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 0
    assert "FIREWALLS in operations-dev-krateo-io: 95 of 100 used, 5 free; the run needs 5" in capsys.readouterr().out


def test_the_pair_refuses_when_both_runs_firewalls_do_not_fit(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_PROJECT_USAGE_FIREWALLS="91")       # 9 free, the pair needs 10
    assert p.returncode == 2 and ("refusing: FIREWALLS in operations-dev-krateo-io: 91 of 100 used, 9 free; the run "
                                  "needs 10: 2 GKE rules x 2 cluster(s)") in p.stderr, p.stderr[-1500:]
    p = _launch(repo, env, BENCH_606_DRY_RUN="1", FAKE_PROJECT_USAGE_FIREWALLS="90")
    assert p.returncode == 0 and "FIREWALLS in operations-dev-krateo-io: 90 of 100 used, 10 free; the run needs 10" \
        in p.stderr, p.stderr[-1500:]


def test_a_vm_reads_its_firewalls_from_the_project_and_its_cpus_from_the_region():
    assert {"FIREWALLS"} <= gke.PROJECT_QUOTAS and "E2_CPUS" not in gke.PROJECT_QUOTAS
    metric, need, what, unit = gcevm.ssh_rules_footprint(gcevm.ssh_rules("t", "d"))
    assert (metric, need, unit) == ("FIREWALLS", 2, "") and what == "its 2 IAP-only SSH rules (t-deny-ssh, t-iap-ssh)"
