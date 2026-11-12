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
    assert "PLACEHOLDER-OPERATOR" not in got and "@" not in got and "authenticated as" not in got, got
    # the code-only summary it replaces had no word for any of them (the instrument with no vocabulary)
    assert identity.error_summary(_text(key)).startswith("no error code")


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
