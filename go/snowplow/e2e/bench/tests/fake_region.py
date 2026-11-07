"""The region and the project a fake gcloud plays (#1171, #1173), modelled on
the live API rather than on an outcome:

  - `compute regions describe`: every quota of europe-west4 as gcloud
    returned them on 2026-10-25 (fixtures/region_europe_west4_quotas_20261025.json),
    so a pre-check that reads one more quota reads it from the real listing.
    Usage is steered per quota, FAKE_USAGE_<METRIC> (and the older names
    FAKE_SSD_USAGE / FAKE_STATIC_USAGE), except IN_USE_ADDRESSES: its usage is
    what the live API reports, the region's VM external IPs ONLY. Three
    readings (2026-10-25, 2026-11-04 22:20Z, 2026-11-05 01:0xZ) show the
    reported number leaving the forwarding rules' IPs out, while GCE refused
    run 21's forwarding rule on that quota. FAKE_USAGE_IN_USE_ADDRESSES is
    refused: an arm states the region's resources, never the number;
  - `compute instances | forwarding-rules | addresses list` (no --filter: the
    pre-check's reads): the region's resources, FAKE_REGION_INSTANCES,
    FAKE_REGION_FORWARDING_RULES, FAKE_REGION_ADDRESSES (JSON lists shaped as
    gcloud prints them; vm(), forwarding_rule() and address() below make rows);
  - `compute project-info describe`: the project's global quotas,
    FAKE_PROJECT_QUOTAS (a JSON list; none by default);
  - FAKE_FAIL_READ: those reads fail as real gcloud fails them (#1172 B2,
    B3; read_failure below): a failed LIST exits 0 with "[]" and a WARNING
    unless the call sets CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR=false.

A fake gcloud is its own process (the run606 harness runs it with `python -I`,
no PYTHONPATH), so it loads this file by path: LOADER is the source to put in
front of its body; it defines region_quotas(name), region_reported(quotas),
region_list(kind) and project_quotas().
"""

from __future__ import annotations

import json
import os
from pathlib import Path

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "region_europe_west4_quotas_20261025.json"
OLDER_NAMES = {"SSD_TOTAL_GB": "FAKE_SSD_USAGE", "STATIC_ADDRESSES": "FAKE_STATIC_USAGE"}
LISTS = {"instances": "FAKE_REGION_INSTANCES", "forwarding-rules": "FAKE_REGION_FORWARDING_RULES",
         "addresses": "FAKE_REGION_ADDRESSES"}
PROJECT = "operations-dev-krateo-io"
API = "https://www.googleapis.com/compute/v1/projects/" + PROJECT


def _env(env):
    return os.environ if env is None else env


def region_list(kind: str, env=None) -> list:
    return json.loads(_env(env).get(LISTS[kind]) or "[]")


def reported_in_use(env=None, region: str = "europe-west4") -> int:
    """What `regions describe` reports for IN_USE_ADDRESSES: the region's VM
    external IPs; no forwarding rule's (the three readings, module doc)."""
    n = 0
    for i in region_list("instances", env):
        if str(i.get("zone", "")).rsplit("/", 1)[-1].rsplit("-", 1)[0] != region:
            continue
        for nic in i.get("networkInterfaces") or []:
            n += sum(1 for ac in nic.get("accessConfigs") or [] if ac.get("natIP"))
    return n


def region_reported(region: dict, env=None) -> dict:
    """A region's quotas with IN_USE_ADDRESSES as the live API reports it."""
    if _env(env).get("FAKE_USAGE_IN_USE_ADDRESSES") is not None:
        raise SystemExit("fake_region: FAKE_USAGE_IN_USE_ADDRESSES is refused: state the region's resources "
                         "(FAKE_REGION_INSTANCES / _FORWARDING_RULES / _ADDRESSES), the API reports the rest")
    for q in region["quotas"]:
        if q["metric"] == "IN_USE_ADDRESSES":
            q["usage"] = float(reported_in_use(env, region["name"]))
    return region


def quotas(env=None, name: str = "europe-west4") -> dict:
    env = _env(env)
    region = json.loads(FIXTURE.read_text())
    region["name"] = name
    for q in region["quotas"]:
        m = q["metric"]
        v = env.get(f"FAKE_USAGE_{m}") or (env.get(OLDER_NAMES[m]) if m in OLDER_NAMES else None)
        if v is not None and m != "IN_USE_ADDRESSES":
            q["usage"] = float(v)
        if env.get(f"FAKE_LIMIT_{m}") is not None:      # run 22: a region with room for three clusters
            q["limit"] = float(env[f"FAKE_LIMIT_{m}"])
    return region_reported(region, env)


def project_quotas(env=None) -> dict:
    return {"name": PROJECT, "quotas": json.loads(_env(env).get("FAKE_PROJECT_QUOTAS") or "[]")}


# FAKE_FAIL_READ: the pre-check reads that fail as gcloud fails them (comma-separated:
# instances, forwarding-rules, addresses, project-info): exit 1, an ERROR line, no JSON
READS = ("instances", "forwarding-rules", "addresses", "project-info")
_PERMISSION = {"instances": "compute.instances.list", "forwarding-rules": "compute.forwardingRules.list",
               "addresses": "compute.addresses.list", "project-info": "compute.projects.get"}


def read_failure(what: str, env=None) -> tuple[int, str, str] | None:
    """(rc, stdout, stderr) of a failed `what` read as real gcloud gives them
    (Google Cloud SDK 561, probed read-only by the #1172 re-review), or None
    when the read answers:
      - a describe: rc 1, an ERROR line, no stdout;
      - a list, by default (compute/allow_partial_error true): rc 0, "[]" and
        "WARNING: Some requests did not succeed." (the SDK's MultiScopeLister
        only warns);
      - a list whose environment sets CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR=false:
        rc 1, "[]" and "ERROR: (gcloud.compute.<kind>.list) Some requests did not
        succeed:"."""
    env = _env(env)
    if what not in [x.strip() for x in (env.get("FAKE_FAIL_READ") or "").split(",")]:
        return None
    why = f" - Required '{_PERMISSION[what]}' permission for 'projects/{PROJECT}'\n"
    if what == "project-info":
        return 1, "", f"ERROR: (gcloud.compute.project-info.describe) Could not fetch resource:\n{why}"
    if (env.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR") or "").strip().lower() == "false":
        return 1, "[]\n", f"ERROR: (gcloud.compute.{what}.list) Some requests did not succeed:\n{why}"
    return 0, "[]\n", f"WARNING: Some requests did not succeed.\n{why}"


# ─── rows, shaped as gcloud prints them ─────────────────────────────────────

def vm(name: str, zone: str = "europe-west4-a", external_ips: int = 1) -> dict:
    return {"name": name, "zone": f"{API}/zones/{zone}",
            "networkInterfaces": [{"networkIP": "10.0.0.2",
                                   "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "natIP": f"34.0.0.{k + 1}"}
                                                     for k in range(external_ips)]}]}


def forwarding_rule(name: str, region: str = "europe-west4", scheme: str = "EXTERNAL") -> dict:
    return {"name": name, "region": f"{API}/regions/{region}", "loadBalancingScheme": scheme,
            "IPAddress": "35.0.0.1", "target": f"{API}/regions/{region}/targetPools/{name}"}


def address(name: str, region: str = "europe-west4", users: list | None = None,
            address_type: str = "EXTERNAL") -> dict:
    return {"name": name, "region": f"{API}/regions/{region}", "addressType": address_type,
            "status": "IN_USE" if users else "RESERVED", **({"users": users} if users else {})}


def region_env(vms: int = 0, forwarding_rules: int = 0, addresses: list | None = None) -> dict:
    """FAKE_REGION_* for a region holding `vms` VMs with one external IP each
    and `forwarding_rules` external forwarding rules."""
    return {"FAKE_REGION_INSTANCES": json.dumps([vm(f"gke-other-{k}") for k in range(vms)]),
            "FAKE_REGION_FORWARDING_RULES": json.dumps([forwarding_rule(f"a{k:031d}") for k in range(forwarding_rules)]),
            "FAKE_REGION_ADDRESSES": json.dumps(addresses or [])}


LOADER = (
    "import importlib.util as _ilu\n"
    f"_frs = _ilu.spec_from_file_location('fake_region', {str(Path(__file__).resolve())!r})\n"
    "_frm = _ilu.module_from_spec(_frs)\n"
    "_frs.loader.exec_module(_frm)\n"
    "def region_quotas(name):\n"
    "    return _frm.quotas(None, name)\n"
    "def region_reported(q):\n"
    "    return _frm.region_reported(q)\n"
    "def region_list(kind):\n"
    "    return _frm.region_list(kind)\n"
    "def project_quotas():\n"
    "    return _frm.project_quotas()\n"
    "def answer_read(what, rows):\n"
    "    import json as _j, sys as _s\n"
    "    failed = _frm.read_failure(what)\n"
    "    if failed:\n"
    "        rc, out, err = failed\n"
    "        _s.stdout.write(out); _s.stderr.write(err); _s.exit(rc)\n"
    "    print(_j.dumps(rows)); _s.exit(0)\n"
)
