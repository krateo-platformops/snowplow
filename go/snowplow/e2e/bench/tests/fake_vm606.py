"""A fake gcloud for bench/vm606.py: GCE instances, disks and firewall rules, IAM policies, IAP SSH and
scp to a VM that is a directory (tests/test_vm606.py). Modelled on gcloud 561's answers, not on an
outcome:

  - the VM: <state>/vm/NAME. Its login user's home is identity-shaped, as OS Login makes it
    (home/IDENTITY, the operator's account as a POSIX name); VM_ROOT (/srv/snowplow-bench) is
    <state>/vm/NAME/srv: the fake rewrites that prefix in what it runs and copies, so the module's
    remote scripts carry no test hook;
  - `compute instances create` (FAKE_CREATE_FAIL=1: a 503) records the argv and makes the instance (labels, tags, scheduling
    terminationTime, a boot disk named after it) and its disk; FAKE_BOOTSTRAP (JSON) is written as
    the VM's bootstrap marker at create, as the startup script would;
  - `compute instances | disks describe NAME --zone Z` / `firewall-rules describe NAME`: the resource
    (an instance or a disk only in its own zone, as GCE answers), or gcloud's 404 naming the resource's
    full path (a disk's names the account). In a project other than operations-dev-krateo-io every
    compute call answers the PARENT's NotFound ("The resource 'projects/<p>' was not found", the same
    words), and in a zone gcloud does not know, "Invalid value for field 'zone'", as gcloud 561 does;
    `projects describe` of another project answers gcloud's permission-or-absent 403 (FAKE_PROJECT_STATE: this
    one's lifecycleState), and `compute zones
    describe` knows ZONES. FAKE_INSTANCE_DESCRIBE_FAIL=403|503: an instance describe fails otherwise
    than NotFound;
  - NO `--filter`: any call that passes one exits 2. gcloud 561's filter parser turns
    `name ~ ^(a|b)$` into `name eq ".*(^).*"` and answers an empty list with rc 0 (#1307 re-review N1,
    --log-http), and `labels.x=y` is applied client-side with matching rules that are changing; a fake
    that applied its own reading of a filter would model the outcome, not gcloud. vm606 selects by
    describe or by exact equality on an unfiltered list;
  - `compute instances list` (unfiltered, every zone): a list whose requests fail (FAKE_LIST_FAILS=1)
    exits 0 with "[]" and a WARNING unless the call's environment sets
    CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR=false, then rc 1 (#1172 B3, tests/fake_region.py);
  - #1383: `compute firewall-rules list` (unfiltered): every rule this fake holds; FAKE_FW_LIST_FAILS=1 fails it
    the same way (rc 0 + "[]" + WARNING without the property, rc 1 with it); FAKE_FW_DELETE_FAIL=<name>: that
    rule's delete is refused (rc 1);
  - `compute instances delete --delete-disks all`: the instance and its disk go (FAKE_KEEP_DISKS=1
    keeps the disk; FAKE_INSTANCE_DELETE_LOST=1: the delete answers rc 0 and nothing goes); `firewall-rules create / delete` (FAKE_FW_DELETE_LOST=1: the delete answers rc 0
    and the rule stays; FAKE_FW_DESCRIBE_503=1: a describe answers 503, not NotFound;
    FAKE_FW_DESCRIBE_NOT_OBJECT=1: it answers rc 0 with `[]`, not a rule);
  - `projects | iam service-accounts get-iam-policy / add-iam-policy-binding /
    remove-iam-policy-binding` with --condition-from-file: a v3 policy per resource; a remove matches
    the condition exactly, as gcloud does. FAKE_IAM_LOST_WRITE=<role>: an add or a remove of that role
    answers rc 0 and is lost (another writer's set-policy replaced it);
  - `compute ssh NAME ... --command CMD` (and scp), for an instance in that --zone only: CMD runs under bash, the script on its stdin, with the
    VM's environment only (HOME = the identity-shaped home, the test's PATH), in a session of its
    own that is hung up (SIGHUP, then SIGTERM to its process group) when the command ends, as sshd
    does. FAKE_SSH_FAIL=4033|4003|4047|4010|4049|publickey answers as gcloud does when IAP / the firewall / OS Login
    refuse, FAKE_SSH_FAIL_TIMES times (default: always). FAKE_TAMPER_AFTER_SSH="<n>:<path under srv>"
    appends a byte to that file when the n-th ssh call ends;
  - `compute ssh ... --command CMD` with any CMD but `bash -s` (bench.gcevm's byte streams, tests/test_testvm.py):
    stdin and stdout are bytes pumped through, as the tunnel does; FAKE_STREAM_STALL="<n>:<bytes>" makes the
    n-th ssh call's tunnel stop moving after that many bytes (#1336's hang), until its caller kills it. Both
    VM roots (vm606's /srv/snowplow-bench, testvm's /srv/testvm, VM_ROOTS) are <state>/vm/NAME/srv;
  - `compute instances create --no-service-account --no-scopes`: an instance with no service account;
    `compute machine-types list --zones Z`: e2-standard and n2-standard 2..32 and e2-highcpu-64 in Z;
  - `compute scp`: the side written NAME:<path> is on the VM; the bytes are copied (this process is
    scp: copying is its job) and the source's mode kept, as scp does for a new file;
    FAKE_SCP_CORRUPT=<basename> flips the last byte of a file of that name in transit; FAKE_SCP_MODE
    (octal) is the mode the copy gets instead of the source's;
  - `projects describe`, `services list --enabled --format json`, `iam service-accounts describe`: the
    project number, the enabled services (IAP among them unless FAKE_IAP_DISABLED=1), the SA unless
    FAKE_NO_SA=1 (the bench node SA unless FAKE_NO_NODE_SA=1; one made by `iam service-accounts create` or
    put_sa as made), IAM's NOT_FOUND as gcloud 561 prints it (read live 2026-11-13);
  - run 26h: `iam roles describe / create ID --project P`: the project's custom roles (none until made, or put_role);
    a describe of an absent one answers IAM's "The role named projects/P/roles/ID was not found" (read live
    2026-11-13); FAKE_ROLE_EXTRA_PERMISSIONS (JSON list): the role IAM makes holds these too;
  - `compute regions describe REGION`: tests/fake_region.py's quotas (FAKE_LIMIT_<METRIC> steers a
    limit); `compute forwarding-rules | addresses list`: none. An instance carries its zone and one
    external IP (natIP), as GCE lists an instance with an ephemeral external IP;
  - #1361: `compute project-info describe`: tests/fake_region.py's project quotas (the 2026-11-12 reading),
    FIREWALLS in use = the project's other rules (FAKE_PROJECT_USAGE_FIREWALLS, default the reading's 68) + the
    rules this fake holds, and a firewall-rules create at the limit answers GCE's QUOTA_EXCEEDED
    (FAKE_PROJECT_INFO_FAIL=1: the describe fails); FAKE_FW_CREATE_FAIL / FAKE_CREATE_FAIL=<a GCLOUD_FAILURES
    key>: the create answers that cause as gcloud prints it; FAKE_NOT_READY=<n>: an instances create answers
    "The resource '.../subnetworks/default' is not ready" (run tvsmoke2) while a subnetwork patch operation runs,
    which `compute operations list` lists and which ends after n `operations describe`s of it (never: no
    operation is listed).

Every call is one JSON line in <FAKE_STATE>/calls.jsonl: its argv and the partial-error property.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests import fake_region

IDENTITY = "jane_doe_example_com"       # an OS Login POSIX name: the operator's account
VM_ROOT = "/srv/snowplow-bench"
# every VM root a tool under test uses (vm606, testvm): each is <state>/vm/NAME/srv on the fake VM
VM_ROOTS = (VM_ROOT, "/srv/testvm")

PROJECT = "operations-dev-krateo-io"
# gcloud 561's answers, read live (#1307 round 4): an absent instance or firewall rule, an absent disk,
# a describe in a project that does not exist (the PARENT's NotFound, in the same words), an unknown zone
ACCOUNT_404 = ("ERROR: (gcloud.compute.{kind}.describe) Could not fetch resource:\n - The resource "
               "'projects/{project}/{where}/{name}' was not found\n")
DISK_404 = ("ERROR: (gcloud.compute.disks.describe) HTTPError 404: The resource 'projects/{project}/{where}/{name}' "
            "was not found. This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the "
            "active account specified by the [core/account] property\n")
PARENT_404 = ("ERROR: (gcloud.compute.{kind}.{verb}) Could not fetch resource:\n - The resource 'projects/{project}' "
              "was not found\n")
PROJECT_403 = ("ERROR: (gcloud.projects.describe) [PLACEHOLDER-OPERATOR@example.invalid] does not have permission to "
               "access projects instance [{project}] (or it may not exist): The caller does not have permission. "
               "This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active account "
               "specified by the [core/account] property\n")
ZONE_400 = ("ERROR: (gcloud.compute.{kind}.{verb}) Could not fetch resource:\n - Invalid value for field 'zone': "
            "'{zone}'. Unknown zone.\n")
DESCRIBE_FAILURES = {
    "403": "ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n - Required 'compute.instances.get' "
           "permission for 'projects/{project}/zones/{zone}/instances/{name}'\n",
    "503": "ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n - The service is currently "
           "unavailable. code=503\n",
}
ZONES = ("europe-west1-b", "europe-west1-c", "europe-west1-d", "europe-west3-a", "europe-west3-b", "europe-west3-c",
         "europe-west4-a", "europe-west4-b", "europe-west4-c")
# gcloud's start-iap-tunnel ProxyCommand fails with IAP's close code ("Error while connecting [<code>:
# '<reason>']", gcloud 561 iap_tunnel_websocket.py), and ssh then reports the closed connection, exit 255
_KEX = "kex_exchange_identification: Connection closed by remote host\nConnection closed by UNKNOWN port 65535\n"


def _iap(code: str, reason: str) -> tuple[int, str]:
    return 255, (f"ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [{code}: '{reason}'].\n" + _KEX)


# ── one builder of what GCE's describe answers, in its shapes (read live, 2026-11-11: a firewall rule, an
# instance and its boot disk): the fake's `create` and put_instance / put_rule both use it ──
BUILDER = r'''
COMPUTE = "https://www.googleapis.com/compute/v1"
SCOPE_URLS = {"cloud-platform": "https://www.googleapis.com/auth/cloud-platform"}
IMAGE_SUFFIX = "-trixie-v20251111"          # an image of the family, as debian-cloud names them


def firewall_doc(name, project, network, direction, action, rules, source_ranges, target_tags, priority,
                 description):
    proto, port = rules.split(":")
    return {"kind": "compute#firewall", "name": name, "network": f"{COMPUTE}/projects/{project}/global/networks/{network}",
            "direction": direction, "priority": int(priority), "sourceRanges": list(source_ranges),
            "targetTags": list(target_tags), "description": description, "disabled": False,
            "logConfig": {"enable": False}, ("denied" if action == "DENY" else "allowed"): [{"IPProtocol": proto,
                                                                                            "ports": [port]}],
            "selfLink": f"{COMPUTE}/projects/{project}/global/firewalls/{name}"}


def instance_docs(name, project, zone, labels, tags, termination_time, action, sa, scopes, metadata, machine,
                  disk_gb, disk_type, image_project, image_family, shielded, network):
    zurl = f"{COMPUTE}/projects/{project}/zones/{zone}"
    net = f"{COMPUTE}/projects/{project}/global/networks/{network}"
    inst = {"kind": "compute#instance", "name": name, "status": "RUNNING", "zone": zurl,
            "machineType": f"{zurl}/machineTypes/{machine}", "labels": dict(labels),
            "tags": {"fingerprint": "Zm9v", "items": list(tags)},
            "networkInterfaces": [{"kind": "compute#networkInterface", "name": "nic0", "network": net,
                                   "subnetwork": f"{COMPUTE}/projects/{project}/regions/{zone[:-2]}/subnetworks/{network}",
                                   "networkIP": "10.164.0.9", "stackType": "IPV4_ONLY",
                                   "accessConfigs": [{"kind": "compute#accessConfig", "name": "external-nat",
                                                      "networkTier": "PREMIUM", "type": "ONE_TO_ONE_NAT",
                                                      "natIP": "34.90.0.9"}]}],
            "serviceAccounts": [{"email": sa, "scopes": [SCOPE_URLS.get(x, x) for x in scopes]}] if sa else [],
            "metadata": {"kind": "compute#metadata", "fingerprint": "Zm9v",
                         "items": [{"key": k, "value": v} for k, v in metadata.items()]},
            "scheduling": {"automaticRestart": False, "onHostMaintenance": "MIGRATE", "preemptible": False,
                           "provisioningModel": "STANDARD", "terminationTime": termination_time,
                           "instanceTerminationAction": action},
            "shieldedInstanceConfig": {"enableSecureBoot": shielded, "enableVtpm": shielded,
                                       "enableIntegrityMonitoring": shielded},
            "disks": [{"kind": "compute#attachedDisk", "boot": True, "autoDelete": True, "mode": "READ_WRITE",
                       "interface": "SCSI", "type": "PERSISTENT", "source": f"{zurl}/disks/{name}",
                       "diskSizeGb": str(disk_gb)}]}
    disk = {"kind": "compute#disk", "name": name, "zone": zurl, "sizeGb": str(disk_gb), "status": "READY",
            "type": f"{zurl}/diskTypes/{disk_type}",
            "sourceImage": f"{COMPUTE}/projects/{image_project}/global/images/{image_family}{IMAGE_SUFFIX}"}
    image = {"kind": "compute#image", "name": f"{image_family}{IMAGE_SUFFIX}", "family": image_family,
             "selfLink": disk["sourceImage"]}
    return inst, disk, image
'''
exec(BUILDER)


SSH_FAILURES = {
    "4033": _iap("4033", "not authorized"),
    "4003": _iap("4003", "failed to connect to backend"),
    "4047": _iap("4047", "Failed to lookup instance"),
    "4010": _iap("4010", "destination read failed"),
    "4049": _iap("4049", "a code IAP's table does not list"),
    # OS Login names the instance by its numeric id (19 digits; this one starts like an IAP code)
    "publickey": (255, f"{IDENTITY}@compute.4096312768531209876: Permission denied (publickey).\n"),
}

# #1361: a create that GCE or gcloud refuses, as gcloud 561 prints it (FAKE_FW_CREATE_FAIL=<key> for a
# firewall-rules create, FAKE_CREATE_FAIL=<key> for an instances create; "1" is the older 503): an operation's
# error ("Could not fetch resource:" and its message, with the reason detail when the API gives one), an HTTP
# error before any operation (its JSON body), an argument gcloud's own parser refuses. Two name the operator's
# account, as gcloud's do. Each key is the cause a refusal must name (tests/test_testvm.py).
GCLOUD_FAILURES = {
    "QUOTA_EXCEEDED": "ERROR: (gcloud.compute.{cmd}) Could not fetch resource:\n - Quota 'FIREWALLS' exceeded.  Limit: "
                      "100.0 globally.\n\tmetric name = compute.googleapis.com/firewalls\n\tlimit name = FIREWALLS-per-"
                      "project\n\treason = QUOTA_EXCEEDED\n",
    "RESOURCE_EXHAUSTED": "ERROR: (gcloud.compute.{cmd}) HttpError accessing <https://compute.googleapis.com/compute/v1/"
                          "projects/{project}/zones/{zone}/instances?alt=json>: response: <{{'status': '429'}}>, "
                          "content <{{\n  \"error\": {{\n    \"code\": 429,\n    \"message\": \"Quota exceeded for "
                          "quota metric 'Queries' and limit 'Queries per minute' of service 'compute.googleapis.com'"
                          "\",\n    \"status\": \"RESOURCE_EXHAUSTED\"\n  }}\n}}\n>\n",
    "ZONE_RESOURCE_POOL_EXHAUSTED": "ERROR: (gcloud.compute.{cmd}) Could not fetch resource:\n - The zone 'projects/"
                                    "{project}/zones/{zone}' does not have enough resources available to fulfill the "
                                    "request.  Try a different zone, or try again later.\n\treason = "
                                    "ZONE_RESOURCE_POOL_EXHAUSTED\n",
    "Invalid value": "ERROR: (gcloud.compute.{cmd}) Could not fetch resource:\n - Invalid value for field "
                     "'resource.scheduling.terminationTime': '2026-11-12T01:51:00Z'. Termination time must be in the "
                     "future.\n",
    "unrecognized arguments": "ERROR: (gcloud.compute.{cmd}) unrecognized arguments: --no-such-flag \n\nTo search "
                              "the help text of gcloud commands, run:\n  gcloud help -- SEARCH_TERMS\n",
    "the account": "ERROR: (gcloud.compute.{cmd}) Could not fetch resource:\n - Required 'compute.instances.create' "
                   "permission for 'projects/{project}/zones/{zone}/instances/x'. This command is authenticated as "
                   "PLACEHOLDER-OPERATOR@example.invalid which is the active account specified by the [core/account] "
                   "property.\n",
    "the account in brackets": "ERROR: (gcloud.compute.{cmd}) [PLACEHOLDER-OPERATOR@example.invalid] does not have "
                               "permission to access projects instance [{project}] (or it may not exist): The caller "
                               "does not have permission\n",
    # #1361 review B1: an external IdP's account (workforce identity federation) is no e-mail: a principal URI whose
    # subject is the IdP's own id (a GUID, an opaque string). gcloud names it in brackets, in quotes, bare, and in an
    # authenticated-as sentence it wraps across lines
    "a workforce account without credentials": (
        "ERROR: (gcloud.compute.{cmd}) Your current active account [principal://iam.googleapis.com/locations/global/"
        "workforcePools/PLACEHOLDER-POOL/subject/PLACEHOLDER-SUBJECT-7d2b3e1a] does not have any valid credentials\n"
        "Please run:\n\n  $ gcloud auth login\n\nto obtain new credentials.\n"),
    "a workforce principal without access": (
        "ERROR: (gcloud.compute.{cmd}) Could not fetch resource:\n - principal://iam.googleapis.com/locations/global/"
        "workforcePools/PLACEHOLDER-POOL/subject/PLACEHOLDER-SUBJECT-7d2b3e1a does not have compute.instances.create "
        "access to the Google Cloud project.\n"),
    "a workforce principal quoted": (
        "ERROR: (gcloud.compute.{cmd}) Could not fetch resource:\n - Required 'compute.firewalls.create' permission "
        "for 'projects/{project}/global/firewalls/x' for principal 'principal://iam.googleapis.com/locations/global/"
        "workforcePools/PLACEHOLDER-POOL/subject/0f3c2a91-PLACEHOLDER-guid'\n"),
    "a wrapped authenticated-as sentence": (
        "ERROR: (gcloud.compute.{cmd}) HTTPError 404: The resource 'projects/{project}/global/networks/nope' was not "
        "found. This command is authenticated as\nprincipal://iam.googleapis.com/locations/global/workforcePools/"
        "PLACEHOLDER-POOL/subject/PLACEHOLDER-SUBJECT-7d2b3e1a which is the active account specified by the\n"
        "[core/account] property.\n"),
}
# a create the fake has answered "not ready" more often than this was made again in a loop (FAKE_NOT_READY)
NOT_READY_CAP = 4

SOURCE = r'''#!/usr/bin/env python3
import json, os, re, shutil, signal, subprocess, sys
from pathlib import Path
a = sys.argv[1:]
st = Path(os.environ["FAKE_STATE"])
for d in ("instances", "disks", "firewalls", "policies"):
    (st / d).mkdir(parents=True, exist_ok=True)
with open(st / "calls.jsonl", "a") as fh:
    fh.write(json.dumps({"argv": a, "partial_error": os.environ.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR")}) + "\n")
def opt(n):
    return a[a.index(n) + 1] if n in a else None
# run 26h: FAKE_RC0_EMPTY="<first 3 words>;...": that call answers rc 0 with "[]" and a WARNING, as a gcloud read
# whose request failed can (the shape gcloud's lists fail open with): a read that trusts rc 0 reads it as an answer
if " ".join(a[:3]) in [x for x in os.environ.get("FAKE_RC0_EMPTY", "").split(";") if x]:
    sys.stderr.write("WARNING: Some requests did not succeed.\n"); print("[]"); sys.exit(0)
# run 26h: FAKE_READ_DENIED="<first 3 words>;...": that call answers IAM's PERMISSION_DENIED (rc 1), which is not
# its NotFound
if " ".join(a[:3]) in [x for x in os.environ.get("FAKE_READ_DENIED", "").split(";") if x]:
    sys.stderr.write(f"ERROR: (gcloud.{a[0]}.{a[1]}) PERMISSION_DENIED: Permission denied on the resource. This "
                     "command is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active account "
                     "specified by the [core/account] property\n"); sys.exit(1)
def load(kind, name):
    p = st / kind / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None
def vm(name):
    return st / "vm" / name
def onvm_text(name, text):
    for root in VM_ROOTS:
        text = text.replace(root, str(vm(name) / "srv"))
    return text
def onvm(name, path):
    return Path(onvm_text(name, path)) if path.startswith(VM_ROOTS) else vm(name) / "home" / IDENTITY / path
def failed_list(kind):
    if os.environ.get("FAKE_LIST_FAILS") != "1":
        return
    why = f" - Required 'compute.{kind}.list' permission for 'projects/operations-dev-krateo-io'\n"
    if (os.environ.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR") or "").lower() == "false":
        print("[]"); sys.stderr.write(f"ERROR: (gcloud.compute.{kind}.list) Some requests did not succeed:\n{why}"); sys.exit(1)
    print("[]"); sys.stderr.write(f"WARNING: Some requests did not succeed.\n{why}"); sys.exit(0)
def notfound(kind, where, name):
    sys.stderr.write((DISK_404 if kind == "disks" else ACCOUNT_404).format(kind=kind, where=where, name=name,
                                                                           project=opt("--project"))); sys.exit(1)
def in_zone(doc):
    return doc is not None and str(doc.get("zone", "")).endswith("/zones/" + str(opt("--zone")))
def instance(name):
    """(the instance NAME in --zone, its file) or (None, None). GCE names an instance per zone: a namesake in
    another zone is the file NAME@ZONE.json (put_instance(..., namesake=True))."""
    for p in (st / "instances" / f"{name}.json", st / "instances" / f"{name}@{opt('--zone')}.json"):
        d = json.loads(p.read_text()) if p.exists() else None
        if in_zone(d):
            return d, p
    return None, None
def parents(kind, verb, zonal):
    """GCE's answer when the project does not exist, or the zone is unknown: before the resource's own."""
    if opt("--project") != PROJECT:
        sys.stderr.write(PARENT_404.format(kind=kind, verb=verb, project=opt("--project"))); sys.exit(1)
    if zonal and opt("--zone") not in ZONES:
        sys.stderr.write(ZONE_400.format(kind=kind, verb=verb, zone=opt("--zone"))); sys.exit(1)
if "--filter" in a:
    sys.stderr.write("fake gcloud: --filter is not modelled (gcloud can mis-parse one into an empty answer, rc 0)\n")
    sys.exit(2)
API = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io/zones/europe-west4-a"
APIP = "https://www.googleapis.com/compute/v1/projects/operations-dev-krateo-io"
# ── project, services, SA, region ──
if a[:3] == ["compute", "regions", "describe"]:
    print(json.dumps(region_quotas(a[3]))); sys.exit(0)
def firewalls_used():
    """#1361: FIREWALLS in use: the project's other rules (the 2026-11-12 reading; FAKE_PROJECT_USAGE_FIREWALLS
    steers it) and every rule this fake holds. (used, limit)"""
    fw = next(x for x in project_quotas()["quotas"] if x["metric"] == "FIREWALLS")
    return fw["usage"] + len(list((st / "firewalls").glob("*.json"))), fw["limit"]
def op_view(o, status):
    return {**{k: v for k, v in o.items() if k not in ("left", "listed_status")}, "status": status}
def scope_flags(verb, allowed):
    """gcloud 561's scope flags: `operations list` takes --regions / --zones / --global, `operations describe`
    --region / --zone / --global; any other is gcloud's own argument error, before any request."""
    given = [f for f in ("--regions", "--zones", "--region", "--zone", "--global") if f in a]
    bad = [f for f in given if f not in allowed]
    if bad or len(given) != 1:
        sys.stderr.write(f"ERROR: (gcloud.compute.operations.{verb}) " + (f"unrecognized arguments: {' '.join(bad)}"
                         if bad else "exactly one scope flag is needed") + "\n"); sys.exit(2)
    return given[0], (opt(given[0]) if given[0] != "--global" else None)
if a[:3] == ["compute", "operations", "list"]:
    failed_list("operations")
    flag, where_ = scope_flags("list", ("--regions", "--zones", "--global"))
    ops = [json.loads(p.read_text()) for p in sorted((st / "operations").glob("*.json"))] \
        if (st / "operations").is_dir() else []
    scope = {"--regions": "regions", "--zones": "zones", "--global": "global"}[flag]
    ops = [o for o in ops if f"/{scope}/" + (f"{where_}/" if where_ else "") in o["selfLink"]]
    # the list's view of an operation may lag its describe's (FAKE_NOT_READY=stale: it keeps saying RUNNING)
    print(json.dumps([op_view(o, o.get("listed_status") or o["status"]) for o in ops])); sys.exit(0)
if a[:3] == ["compute", "operations", "describe"]:
    flag, where_ = scope_flags("describe", ("--region", "--zone", "--global"))
    p = st / "operations" / f"{a[3]}.json"
    o = json.loads(p.read_text()) if p.exists() else None
    scope = {"--region": "regions", "--zone": "zones", "--global": "global"}[flag]
    if o is None or f"/{scope}/" + (f"{where_}/" if where_ else "") not in o["selfLink"]:
        sys.stderr.write(f"ERROR: (gcloud.compute.operations.describe) Could not fetch resource:\n - The resource "
                         f"'{a[3]}' was not found\n"); sys.exit(1)
    if os.environ.get("FAKE_OP_DESCRIBE_FAIL") == "1":
        sys.stderr.write("ERROR: (gcloud.compute.operations.describe) Could not fetch resource:\n - The service is "
                         "currently unavailable. code=503\n"); sys.exit(1)
    o["left"] -= 1
    if o["left"] <= 0:
        o["status"] = "DONE"
    p.write_text(json.dumps(o))
    print(json.dumps(op_view(o, o["status"]))); sys.exit(0)
if a[:3] == ["compute", "project-info", "describe"]:
    if os.environ.get("FAKE_PROJECT_INFO_FAIL") == "1":
        sys.stderr.write("ERROR: (gcloud.compute.project-info.describe) Could not fetch resource:\n - Required "
                         "'compute.projects.get' permission for 'projects/operations-dev-krateo-io'\n"); sys.exit(1)
    q = project_quotas()
    next(x for x in q["quotas"] if x["metric"] == "FIREWALLS")["usage"] = firewalls_used()[0]
    print(json.dumps(q)); sys.exit(0)
if a[:2] == ["compute", "forwarding-rules"] or a[:2] == ["compute", "addresses"]:
    failed_list(a[1]); print("[]"); sys.exit(0)
if a[:2] == ["projects", "describe"]:
    if a[2] != PROJECT:
        sys.stderr.write(PROJECT_403.format(project=a[2])); sys.exit(1)
    number = os.environ.get("FAKE_PROJECT_NUMBER", "357378285144")
    print(json.dumps({"projectId": PROJECT, "projectNumber": number,
                      "lifecycleState": os.environ.get("FAKE_PROJECT_STATE", "ACTIVE")})
          if opt("--format") == "json" else number); sys.exit(0)
if a[:3] == ["compute", "zones", "describe"]:
    parents("zones", "describe", False)
    if a[3] not in ZONES:
        sys.stderr.write(ZONE_400.format(kind="zones", verb="describe", zone=a[3])); sys.exit(1)
    print(json.dumps({"name": a[3], "status": "UP",
                      "region": f"https://www.googleapis.com/compute/v1/projects/{PROJECT}/regions/{a[3][:-2]}"}))
    sys.exit(0)
if a[:3] == ["services", "list", "--enabled"]:
    names = ["compute.googleapis.com", "container.googleapis.com"]
    if os.environ.get("FAKE_IAP_DISABLED") != "1":
        names.append("iap.googleapis.com")
    print(json.dumps([{"config": {"name": n}, "state": "ENABLED"} for n in names])); sys.exit(0)
SA_404 = ("ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account. This command is "
          "authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active account specified by the "
          "[core/account] property\n")
ROLE_404 = ("ERROR: (gcloud.iam.roles.describe) NOT_FOUND: The role named projects/{project}/roles/{role} was not "
            "found. This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the active account "
            "specified by the [core/account] property.\n")
def sa_doc(email):
    """run 26h: an SA this fake created (or put_sa), else the orchestrator SA (unless FAKE_NO_SA=1) or the bench node
    SA (unless FAKE_NO_NODE_SA=1, as before node-sa made it); None: IAM's NOT_FOUND."""
    p = st / "sas" / f"{email}.json"
    if p.exists():
        return json.loads(p.read_text())
    node = email.startswith("snowplow-bench-node@")
    if os.environ.get("FAKE_NO_NODE_SA" if node else "FAKE_NO_SA") == "1":
        return None
    return {"email": email, "name": f"projects/{PROJECT}/serviceAccounts/{email}", "disabled": False}
if a[:3] == ["iam", "service-accounts", "describe"]:
    doc = sa_doc(a[3])
    if doc is None:
        sys.stderr.write(SA_404); sys.exit(1)
    print(json.dumps(doc) if opt("--format") == "json" else doc["email"]); sys.exit(0)
if a[:3] == ["iam", "service-accounts", "create"]:
    email = f"{a[3]}@{opt('--project')}.iam.gserviceaccount.com"
    if sa_doc(email) is not None:
        sys.stderr.write("ERROR: (gcloud.iam.service-accounts.create) Resource in projects [x] is the subject of a "
                         "conflict: Service account already exists within project.\n"); sys.exit(1)
    (st / "sas").mkdir(exist_ok=True)
    (st / "sas" / f"{email}.json").write_text(json.dumps({"email": email, "displayName": opt("--display-name"),
                                                            "description": opt("--description"), "disabled": False}))
    sys.exit(0)
# ── run 26h: the project's custom roles (bench.vm606 node-sa's reaper role) ──
if a[:3] == ["iam", "roles", "describe"]:
    p = st / "roles" / f"{a[3]}.json"
    if not p.exists():
        sys.stderr.write(ROLE_404.format(project=opt("--project"), role=a[3])); sys.exit(1)
    print(p.read_text()); sys.exit(0)
if a[:3] == ["iam", "roles", "create"]:
    (st / "roles").mkdir(exist_ok=True)
    p = st / "roles" / f"{a[3]}.json"
    if p.exists():
        sys.stderr.write("ERROR: (gcloud.iam.roles.create) ALREADY_EXISTS: A role named x already exists.\n")
        sys.exit(1)
    perms = sorted((opt("--permissions") or "").split(","))
    perms += json.loads(os.environ.get("FAKE_ROLE_EXTRA_PERMISSIONS") or "[]")   # what IAM made differs
    p.write_text(json.dumps({"name": f"projects/{opt('--project')}/roles/{a[3]}", "title": opt("--title"),
                             "description": opt("--description"), "includedPermissions": perms,
                             "stage": opt("--stage"), "etag": "BwX="}))
    sys.exit(0)
# ── #1390: the evidence bucket (its API description, as `--raw` gives it) ──
def bucket_404(url):
    sys.stderr.write(f"ERROR: (gcloud.storage.buckets.describe) {url} not found: 404.\n"); sys.exit(1)
if a[:3] == ["storage", "buckets", "describe"]:
    (st / "buckets").mkdir(exist_ok=True)
    bp = st / "buckets" / (a[3][len("gs://"):] + ".json")
    if not bp.exists():
        bucket_404(a[3])
    print(bp.read_text()); sys.exit(0)
if a[:3] == ["storage", "buckets", "create"]:
    (st / "buckets").mkdir(exist_ok=True)
    name = a[3][len("gs://"):]
    bp = st / "buckets" / (name + ".json")
    if bp.exists():
        sys.stderr.write(f"ERROR: (gcloud.storage.buckets.create) HTTPError 409: The requested bucket name is not "
                         f"available.\n"); sys.exit(1)
    rp = opt("--retention-period") or ""
    doc = {"kind": "storage#bucket", "name": name, "location": (opt("--location") or "US").upper(),
           "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": "--uniform-bucket-level-access" in a},
                                "publicAccessPrevention": "enforced" if "--public-access-prevention" in a
                                else "inherited"}}
    if rp:
        doc["retentionPolicy"] = {"retentionPeriod": str(int(rp.rstrip("s"))), "effectiveTime": "2026-11-13T00:00:00Z"}
    if opt("--lifecycle-file"):
        lc = json.loads(Path(opt("--lifecycle-file")).read_text())
        doc["lifecycle"] = {"rule": (lc.get("lifecycle") or lc)["rule"]}
    doc.update(json.loads(os.environ.get("FAKE_BUCKET_ON_CREATE") or "{}"))   # what GCS made differs
    bp.write_text(json.dumps(doc))
    sys.exit(0)
# ── IAM policies ──
iam = None
if a[:3] == ["storage", "buckets", "get-iam-policy"] or (a[:2] == ["storage", "buckets"] and "iam-policy" in a[2]):
    if not (st / "buckets" / (a[3][len("gs://"):] + ".json")).exists():
        bucket_404(a[3])
    iam = (a[2], "bucket-" + a[3][len("gs://"):])
if a[:1] == ["projects"] and "iam-policy" in a[1]:
    iam = (a[1], "project-" + a[2])
if a[:2] == ["iam", "service-accounts"] and "iam-policy" in a[2]:
    iam = (a[2], "sa-" + a[3])
if iam:
    verb, key = iam
    p = st / "policies" / f"{key}.json"
    pol = json.loads(p.read_text()) if p.exists() else {"version": 1, "etag": "BwX=", "bindings": []}
    if verb == "get-iam-policy":
        print(json.dumps(pol)); sys.exit(0)
    member, role = opt("--member"), opt("--role")
    cond = json.loads(Path(opt("--condition-from-file")).read_text()) if opt("--condition-from-file") else None
    if cond is not None:
        cond = {k: cond[k] for k in ("title", "expression", "description") if k in cond}
    if verb != "get-iam-policy" and os.environ.get("FAKE_IAM_LOST_WRITE") == role:
        print(json.dumps(pol)); sys.exit(0)          # answered, then lost to a concurrent policy write
    if verb == "add-iam-policy-binding":
        for b in pol["bindings"]:
            if b["role"] == role and b.get("condition") == cond:
                if member not in b["members"]:
                    b["members"].append(member)
                break
        else:
            pol["bindings"].append({"role": role, "members": [member], **({"condition": cond} if cond else {})})
        pol["version"] = 3 if any("condition" in b for b in pol["bindings"]) else 1
    else:
        hit = [b for b in pol["bindings"] if b["role"] == role and b.get("condition") == cond and member in b["members"]]
        if not hit:
            sys.stderr.write(f"ERROR: (gcloud.{key.split('-')[0]}.remove-iam-policy-binding) Policy binding with the "
                             "specified principal, role, and condition not found!\n"); sys.exit(1)
        hit[0]["members"].remove(member)
        pol["bindings"] = [b for b in pol["bindings"] if b["members"]]
    p.write_text(json.dumps(pol))
    print(json.dumps(pol)); sys.exit(0)
# ── firewall rules ──
def refused_create(var, cmd):
    key = os.environ.get(var)
    if key and key != "1":
        sys.stderr.write(GCLOUD_FAILURES[key].format(cmd=cmd, project=opt("--project"), zone=opt("--zone"))); sys.exit(1)
if a[:3] == ["compute", "firewall-rules", "create"]:
    name = a[3]
    refused_create("FAKE_FW_CREATE_FAIL", "firewall-rules.create")
    used, limit = firewalls_used()
    if used >= limit:                                # GCE's own refusal at the limit (run tvsmoke1's)
        sys.stderr.write(GCLOUD_FAILURES["QUOTA_EXCEEDED"].format(cmd="firewall-rules.create")); sys.exit(1)
    if load("firewalls", name):
        sys.stderr.write(f"ERROR: (gcloud.compute.firewall-rules.create) The resource '{name}' already exists\n"); sys.exit(1)
    doc = firewall_doc(name, opt("--project"), opt("--network"), opt("--direction"), opt("--action"), opt("--rules"),
                       opt("--source-ranges").split(","), opt("--target-tags").split(","), opt("--priority"),
                       opt("--description"))
    doc.update(json.loads(os.environ.get("FAKE_FW_ON_CREATE") or "{}"))
    (st / "firewalls" / f"{name}.json").write_text(json.dumps(doc))
    sys.exit(0)
if a[:3] == ["compute", "firewall-rules", "describe"] and os.environ.get("FAKE_FW_DESCRIBE_NOT_OBJECT") == "1":
    print("[]"); sys.exit(0)
if a[:3] == ["compute", "firewall-rules", "describe"] and os.environ.get("FAKE_FW_DESCRIBE_503") == "1":
    sys.stderr.write("ERROR: (gcloud.compute.firewall-rules.describe) Could not fetch resource:\n - "
                     "The service is currently unavailable. code=503\n"); sys.exit(1)
if a[:3] == ["compute", "firewall-rules", "describe"]:
    parents("firewall-rules", "describe", False)
    d = load("firewalls", a[3])
    if d is None:
        notfound("firewall-rules", "global/firewalls", a[3])
    print(json.dumps(d)); sys.exit(0)
if a[:3] == ["compute", "firewall-rules", "list"]:
    # #1383: the project's rules, unfiltered (FAKE_FW_LIST_FAILS=1: its requests fail, as a list fails: rc 0 + []
    # and a WARNING unless the property is false); FAKE_FW_DELETE_FAIL=<name>: that rule's delete is refused
    if os.environ.get("FAKE_FW_LIST_FAILS") == "1":
        os.environ["FAKE_LIST_FAILS"] = "1"
        failed_list("firewalls")
    print(json.dumps([json.loads(p.read_text()) for p in sorted((st / "firewalls").glob("*.json"))])); sys.exit(0)
if a[:3] == ["compute", "firewall-rules", "delete"] and os.environ.get("FAKE_FW_DELETE_FAIL") == a[3]:
    sys.stderr.write(f"ERROR: (gcloud.compute.firewall-rules.delete) Could not fetch resource:\n - The resource "
                     f"'projects/{opt('--project')}/global/firewalls/{a[3]}' is not ready\n"); sys.exit(1)
if a[:3] == ["compute", "firewall-rules", "delete"]:
    if not load("firewalls", a[3]):
        notfound("firewall-rules", "global/firewalls", a[3])
    if os.environ.get("FAKE_FW_DELETE_LOST") != "1":
        (st / "firewalls" / f"{a[3]}.json").unlink()
    sys.exit(0)
# ── instances, disks ──
if a[:3] == ["compute", "instances", "create"]:
    name = a[3]
    refused_create("FAKE_CREATE_FAIL", "instances.create")
    # #1361 (run tvsmoke2): the zone's subnetwork is being patched by another tenant's operation. The region's
    # operations list also holds an earlier patch of it that ended (DONE), as the live list does (96 that day).
    # FAKE_NOT_READY=<n>: the running one ends after n describes of it, then the create goes through; =never: GCE
    # says not ready and only the ended one is listed; =stale: describe says DONE at once while the list keeps
    # saying RUNNING, and GCE keeps saying not ready (the two reads disagree). After NOT_READY_CAP not-ready answers
    # the fake answers something else, so a create that loops is counted, never hung.
    if os.environ.get("FAKE_NOT_READY"):
        mode = os.environ["FAKE_NOT_READY"]
        region = opt("--zone")[:-2]
        subnet = f"projects/{opt('--project')}/regions/{region}/subnetworks/default"
        ops_dir = st / "operations"
        ops_dir.mkdir(exist_ok=True)
        base = {"operationType": "compute.subnetworks.patch",
                "targetLink": f"https://www.googleapis.com/compute/beta/{subnet}"}
        def op_doc(name, **kw):
            return {"name": name, **base, "selfLink": f"https://www.googleapis.com/compute/v1/projects/"
                                                      f"{opt('--project')}/regions/{region}/operations/{name}", **kw}
        ended = ops_dir / "operation-1794436860115-subnet-patch-ended.json"
        if not ended.exists():
            ended.write_text(json.dumps(op_doc(ended.stem, status="DONE", left=0,
                                               insertTime="2026-11-11T14:41:01.052-08:00")))
        op = ops_dir / "operation-1794441048556-subnet-patch.json"
        if mode != "never" and not op.exists():
            op.write_text(json.dumps(op_doc(op.stem, status="RUNNING", insertTime="2026-11-11T15:50:49.493-08:00",
                                            left=1 if mode == "stale" else int(mode),
                                            **({"listed_status": "RUNNING"} if mode == "stale" else {}))))
        answered = st / "not_ready_answers"
        n = int(answered.read_text()) if answered.exists() else 0
        if mode in ("never", "stale") or json.loads(op.read_text())["status"] != "DONE":
            answered.write_text(str(n + 1))
            if n + 1 > NOT_READY_CAP:
                sys.stderr.write("ERROR: (gcloud.compute.instances.create) fake: the create was made again "
                                 f"{n + 1} times while not ready\n"); sys.exit(1)
            sys.stderr.write(f"ERROR: (gcloud.compute.instances.create) Could not fetch resource:\n - The resource "
                             f"'{subnet}' is not ready\n"); sys.exit(1)
    if os.environ.get("FAKE_CREATE_FAIL") == "1":
        sys.stderr.write("ERROR: (gcloud.compute.instances.create) Could not fetch resource:\n - The service is "
                         "currently unavailable. code=503\n"); sys.exit(1)
    labels = dict(kv.split("=", 1) for kv in opt("--labels").split(","))
    metadata = dict(kv.split("=", 1) for kv in opt("--metadata").split(","))
    for kv in opt("--metadata-from-file").split(","):
        k, path = kv.split("=", 1)
        metadata[k] = Path(path).read_text()
    shielded = all(f in a for f in ("--shielded-secure-boot", "--shielded-vtpm", "--shielded-integrity-monitoring"))
    doc, disk, image = instance_docs(
        name, opt("--project"), opt("--zone"), labels, opt("--tags").split(","), opt("--termination-time"),
        opt("--instance-termination-action"), opt("--service-account"),
        [] if "--no-scopes" in a else opt("--scopes").split(","), metadata,
        opt("--machine-type"), opt("--boot-disk-size").removesuffix("GB"), opt("--boot-disk-type"),
        opt("--image-project"), opt("--image-family"), shielded, opt("--network"))
    doc.update(json.loads(os.environ.get("FAKE_INSTANCE_ON_CREATE") or "{}"))
    (st / "instances" / f"{name}.json").write_text(json.dumps(doc))
    (st / "disks" / f"{name}.json").write_text(json.dumps(disk))
    (st / "images").mkdir(exist_ok=True)
    (st / "images" / f"{image['name']}.json").write_text(json.dumps(image))
    (vm(name) / "home" / IDENTITY).mkdir(parents=True, exist_ok=True)
    (vm(name) / "srv").mkdir(parents=True, exist_ok=True)
    if os.environ.get("FAKE_BOOTSTRAP"):
        m = vm(name) / "srv" / ".bootstrap" / "bootstrap.json"
        m.parent.mkdir(parents=True, exist_ok=True)
        m.write_text(os.environ["FAKE_BOOTSTRAP"])
    sys.exit(0)
if a[:3] == ["compute", "instances", "describe"]:
    parents("instances", "describe", True)
    if os.environ.get("FAKE_INSTANCE_DESCRIBE_FAIL"):
        sys.stderr.write(DESCRIBE_FAILURES[os.environ["FAKE_INSTANCE_DESCRIBE_FAIL"]].format(
            project=opt("--project"), zone=opt("--zone"), name=a[3])); sys.exit(1)
    d, _p = instance(a[3])
    if d is None:
        notfound("instances", f"zones/{opt('--zone')}/instances", a[3])
    print(json.dumps(d)); sys.exit(0)
if a[:3] == ["compute", "images", "describe"]:
    d = load("images", a[3])
    if d is None:
        sys.stderr.write(f"ERROR: (gcloud.compute.images.describe) Could not fetch resource:\n - The resource "
                         f"'projects/{opt('--project')}/global/images/{a[3]}' was not found\n"); sys.exit(1)
    print(json.dumps(d)); sys.exit(0)
if a[:3] == ["compute", "disks", "describe"]:
    parents("disks", "describe", True)
    d = load("disks", a[3])
    if not in_zone(d):
        notfound("disks", f"zones/{opt('--zone')}/disks", a[3])
    print(json.dumps(d)); sys.exit(0)
if a[:3] == ["compute", "machine-types", "list"]:
    failed_list("machineTypes")
    zone = opt("--zones")
    print(json.dumps([{"kind": "compute#machineType", "name": f"{fam}-{n}", "guestCpus": n, "memoryMb": n * 4096,
                       "zone": zone} for fam in ("e2-standard", "n2-standard") for n in (2, 4, 8, 16, 32)]
                     + [{"kind": "compute#machineType", "name": "e2-highcpu-64", "guestCpus": 64, "memoryMb": 65536,
                         "zone": zone}]))
    sys.exit(0)
if a[:3] == ["compute", "instances", "list"]:
    failed_list("instances")
    print(json.dumps([json.loads(p.read_text()) for p in sorted((st / "instances").glob("*.json"))])); sys.exit(0)
if a[:3] == ["compute", "instances", "delete"]:
    name = a[3]
    parents("instances", "delete", True)
    d, path = instance(name)
    if d is None:
        notfound("instances", f"zones/{opt('--zone')}/instances", name)
    if os.environ.get("FAKE_INSTANCE_DELETE_LOST") == "1":
        sys.exit(0)                                    # answered, and the instance is still there
    path.unlink()
    if path.name == f"{name}.json" and opt("--delete-disks") == "all" and os.environ.get("FAKE_KEEP_DISKS") != "1":
        (st / "disks" / f"{name}.json").unlink()
        shutil.rmtree(vm(name), ignore_errors=True)
    sys.exit(0)
# ── ssh, scp ──
if a[:2] == ["compute", "ssh"]:
    name = a[2]
    parents("ssh", "ssh", True)
    if instance(name)[0] is None:                       # an instance in another zone is not found here
        sys.stderr.write(f"ERROR: (gcloud.compute.ssh) Could not fetch resource:\n - The resource 'projects/"
                         f"{opt('--project')}/zones/{opt('--zone')}/instances/{name}' was not found\n"); sys.exit(1)
    n = len([1 for ln in (st / "calls.jsonl").read_text().splitlines() if json.loads(ln)["argv"][:2] == ["compute", "ssh"]])
    fail = os.environ.get("FAKE_SSH_FAIL")
    if fail and n <= int(os.environ.get("FAKE_SSH_FAIL_TIMES", "1000000")):
        rc, text = SSH_FAILURES[fail]
        sys.stderr.write(text); sys.exit(rc)
    home = vm(name) / "home" / IDENTITY
    home.mkdir(parents=True, exist_ok=True)
    (vm(name) / "srv").mkdir(parents=True, exist_ok=True)
    env = {"HOME": str(home), "PATH": os.environ["PATH"], "LANG": "C.UTF-8", "USER": IDENTITY}
    command = onvm_text(name, opt("--command"))
    if command == "bash -s":
        script = onvm_text(name, sys.stdin.read())
        p = subprocess.Popen(["bash", "-c", command], cwd=home, env=env, stdin=subprocess.PIPE,
                             start_new_session=True, text=True)
        p.communicate(script)
    else:
        # a byte stream (an upload on stdin, a download on stdout), pumped as the tunnel does;
        # FAKE_STREAM_STALL="<n>:<bytes>": on the n-th ssh call, the tunnel stops after that many bytes
        stall = os.environ.get("FAKE_STREAM_STALL", "")
        limit = int(stall.split(":")[1]) if stall and int(stall.split(":")[0]) == n else None
        # in this process's group: a caller that kills a stalled transfer's gcloud kills its remote end too
        p = subprocess.Popen(["bash", "-c", command], cwd=home, env=env, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE)
        import threading, time
        def pump(src, dst, close):
            moved = 0
            while True:
                b = src.read1(65536) if hasattr(src, "read1") else src.read(65536)
                if not b:
                    break
                if limit is not None and moved + len(b) > limit:
                    dst.write(b[:max(limit - moved, 0)]); dst.flush()
                    time.sleep(3600)                  # the tunnel stalls: nothing more moves
                try:
                    dst.write(b); dst.flush()
                except BrokenPipeError:
                    break
                moved += len(b)
            if close:
                try:
                    dst.close()
                except BrokenPipeError:
                    pass
        up = threading.Thread(target=pump, args=(sys.stdin.buffer, p.stdin, True), daemon=True)
        down = threading.Thread(target=pump, args=(p.stdout, sys.stdout.buffer, False), daemon=True)
        up.start(); down.start()
        p.wait(); down.join()
    for sig in (signal.SIGHUP, signal.SIGTERM):          # sshd hangs the session up
        try:
            os.killpg(p.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
    tamper = os.environ.get("FAKE_TAMPER_AFTER_SSH", "")
    if tamper and int(tamper.split(":", 1)[0]) == n:
        with open(vm(name) / "srv" / tamper.split(":", 1)[1], "ab") as fh:
            fh.write(b"\n")
    sys.exit(p.returncode)
if a[:2] == ["compute", "scp"]:
    src, dst = a[-2], a[-1]
    def where(x):
        if ":" in x and not x.startswith("/"):
            name, rel = x.split(":", 1)
            parents("scp", "scp", True)
            if instance(name)[0] is None:
                sys.stderr.write(f"ERROR: (gcloud.compute.scp) Could not fetch resource:\n - The resource "
                                 f"'projects/{opt('--project')}/zones/{opt('--zone')}/instances/{name}' was not "
                                 f"found\n"); sys.exit(1)
            return onvm(name, rel)
        return Path(x)
    s, d = where(src), where(dst)
    if not s.is_file() or not d.parent.is_dir():
        sys.stderr.write("scp: No such file or directory\n"); sys.exit(1)
    data = s.read_bytes()
    if os.environ.get("FAKE_SCP_CORRUPT") == s.name and data:
        data = data[:-1] + bytes([data[-1] ^ 1])
    d.write_bytes(data)
    os.chmod(d, int(os.environ["FAKE_SCP_MODE"], 8) if os.environ.get("FAKE_SCP_MODE") else s.stat().st_mode & 0o777)
    sys.exit(0)
# ── GKE (bench.testvm --cluster): <state>/gke/NAME.json is a cluster; get-credentials writes $KUBECONFIG ──
if a[:3] in (["container", "clusters", "describe"], ["container", "clusters", "get-credentials"]):
    d = load("gke", a[3])
    if d is None or (d.get("location") and d["location"] != opt("--location")):
        sys.stderr.write(f"ERROR: (gcloud.container.clusters.{a[2]}) ResponseError: code=404, message=Not found: "
                         f"projects/{opt('--project')}/locations/{opt('--location')}/clusters/{a[3]}.\n"); sys.exit(1)
    if a[2] == "describe":
        print(json.dumps(d)); sys.exit(0)
    Path(os.environ["KUBECONFIG"]).write_text(json.dumps({"apiVersion": "v1", "kind": "Config",
                                                         "current-context": a[3], "operator": "gcloud-auth-plugin"}))
    sys.exit(0)
sys.stderr.write("fake gcloud: unknown call " + " ".join(a) + "\n")
sys.exit(2)
'''


def install(bin_dir: Path, state: Path) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    g = bin_dir / "gcloud"
    consts = "".join(f"{k} = {globals()[k]!r}\n" for k in (
        "PROJECT", "ACCOUNT_404", "DISK_404", "PARENT_404", "PROJECT_403", "ZONE_400", "DESCRIBE_FAILURES", "ZONES",
        "SSH_FAILURES", "IDENTITY", "VM_ROOT", "VM_ROOTS", "GCLOUD_FAILURES", "NOT_READY_CAP")) + BUILDER
    g.write_text(SOURCE.replace("API = ", consts + fake_region.LOADER + "API = ", 1))
    g.chmod(0o755)


def calls(state: Path) -> list[dict]:
    p = state / "calls.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


def put_instance(state: Path, name: str, labels: dict, termination_time: str, zone: str = "europe-west4-a",
                 namesake: bool = False) -> Path:
    """An instance as up's create_command makes it (the same builder as the fake's `create`), for a test that
    starts after `up`; returns the VM's VM_ROOT. namesake: a second instance of that name, in another zone
    (no disk or VM directory of its own)."""
    from bench import vm606
    for d in ("instances", "disks", "images"):
        (state / d).mkdir(parents=True, exist_ok=True)
    doc, disk, image = instance_docs(
        name, PROJECT, zone, labels, [name], termination_time, "DELETE", vm606.sa_email(PROJECT), ["cloud-platform"],
        {"enable-oslogin": "TRUE", "block-project-ssh-keys": "TRUE", "startup-script": vm606.startup_script()},
        vm606.MACHINE_TYPE, vm606.BOOT_DISK_GB, vm606.BOOT_DISK_TYPE, vm606.IMAGE_PROJECT, vm606.IMAGE_FAMILY, True,
        vm606.NETWORK)
    (state / "instances" / (f"{name}@{zone}.json" if namesake else f"{name}.json")).write_text(json.dumps(doc))
    if namesake:
        return state / "vm" / name / "srv"
    (state / "disks" / f"{name}.json").write_text(json.dumps(disk))
    (state / "images" / f"{image['name']}.json").write_text(json.dumps(image))
    (state / "vm" / name / "home" / IDENTITY).mkdir(parents=True, exist_ok=True)
    srv = state / "vm" / name / "srv"
    srv.mkdir(parents=True, exist_ok=True)
    return srv


def bootstrap_done(state: Path, name: str, doc: dict) -> None:
    m = state / "vm" / name / "srv" / ".bootstrap" / "bootstrap.json"
    m.parent.mkdir(parents=True, exist_ok=True)
    m.write_text(json.dumps(doc))


def policy(state: Path, key: str) -> dict:
    """key: project-<id>, sa-<email> or bucket-<name> (bucket-gs://<name> too: grant_policies' form)."""
    key = key.replace("bucket-gs://", "bucket-")
    p = state / "policies" / f"{key}.json"
    return json.loads(p.read_text()) if p.exists() else {"bindings": []}


def put_policy(state: Path, key: str, bindings: list) -> None:
    key = key.replace("bucket-gs://", "bucket-")
    if key.startswith("bucket-"):                      # a bucket policy lives on a bucket that exists
        (state / "buckets").mkdir(parents=True, exist_ok=True)
        bp = state / "buckets" / f"{key[len('bucket-'):]}.json"
        if not bp.exists():
            bp.write_text(json.dumps({"kind": "storage#bucket", "name": key[len("bucket-"):]}))
    (state / "policies").mkdir(parents=True, exist_ok=True)
    (state / "policies" / f"{key}.json").write_text(json.dumps({"version": 3, "etag": "BwX=", "bindings": bindings}))


def put_sa(state: Path, email: str, **fields) -> None:
    """run 26h: an SA as IAM describes it (fields over the defaults: enabled)."""
    (state / "sas").mkdir(parents=True, exist_ok=True)
    (state / "sas" / f"{email}.json").write_text(json.dumps({"email": email, "disabled": False, **fields}))


def put_role(state: Path, role_id: str, permissions: list[str], **fields) -> None:
    """run 26h: a custom role of the project as IAM describes it."""
    (state / "roles").mkdir(parents=True, exist_ok=True)
    (state / "roles" / f"{role_id}.json").write_text(json.dumps(
        {"name": f"projects/{PROJECT}/roles/{role_id}", "includedPermissions": permissions, "stage": "GA", **fields}))


def put_rule(state: Path, run: str, fw: str, expires: str | None = None) -> None:
    """One of the run's SSH rules as up's firewall_create_argv makes it (the fake's `create` builder). Its
    recorded expiry (#1383): `expires`, else the run's record's terminates_at, as up writes both."""
    from bench import vm606
    r = vm606.firewall_rules(run, expires or vm606._state(run).get("terminates_at"))[fw]
    (state / "firewalls").mkdir(parents=True, exist_ok=True)
    (state / "firewalls" / f"{fw}.json").write_text(json.dumps(firewall_doc(
        fw, PROJECT, vm606.NETWORK, "INGRESS", r["action"], "tcp:22", r["sourceRanges"], r["targetTags"], r["priority"],
        r["description"])))
