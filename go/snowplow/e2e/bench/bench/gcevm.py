"""A disposable GCE VM, reached through IAP only: the primitives bench/vm606.py (the #606 orchestrator VM)
and bench/testvm.py (#1338, a VM for any team's heavy test suite) share. Factored out of vm606, not
copied: each rule below is vm606's, stated once.

  - gcloud(): one call, its argv printed (RUN: / DRY-RUN:), its stdout and stderr the caller's: gcloud's
    error text names the operator's account, so only its code ever reaches output (error_summary).
  - SELECTION by exact name. A resource is read by `describe` (its own NotFound is "absent", any other
    failure refuses: not_found() counts a NotFound only when it names the resource's own full path, since
    gcloud words a missing PARENT the same way), or by exact equality over one unfiltered list read with
    CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR=false (a list whose requests failed otherwise exits 0 with "[]").
    No --filter: gcloud 561 can mis-parse one into an empty answer with rc 0 (#1307 re-review N1).
  - PARENTS FIRST: the project (exists, ACTIVE, the record's number) and the zone are confirmed before any
    describe there is read as "absent".
  - SSH THROUGH IAP ONLY: two rules per VM, by its network tag (ssh_rules): DENY tcp:22 from 0.0.0.0/0 at
    priority 1000 (beats the project's default-allow-ssh at 65534) and ALLOW tcp:22 from IAP's
    35.235.240.0/20 at 900.
  - IAP's tunnel close code decides what a failed SSH means (IAP_CODES, from IAP's own table); a code
    waiting can fix is waited out within a bound, any other stops at once, named by its number only.
  - AS MADE: a describe is reduced to the fields a create command sets (FIREWALL_FIELDS; each tool's own
    instance table), and diff() names each field that differs from what the tool RECORDED creating.
  - quota_check(): a VM's footprint against its region's quotas (bench.gke's primitives), failing closed.
  - BOUNDED TRANSFERS (#1336): a byte stream over `gcloud compute ssh` (never `compute scp`: on 2026-11-11
    scp of a 31 MB file through IAP hung twice for 55 min, while an ssh-streamed tar went through), bounded
    by a deadline derived from its byte count and a rate measured by a probe of the same direction just
    before it, and refused by name when no byte moves for longer than the probe took (a stall).
"""

from __future__ import annotations

import contextlib
import contextvars
import datetime as _dt
import hashlib
import json
import os
import re
import selectors
import shlex
import signal
import subprocess
import tempfile
import time
from pathlib import Path

from bench import gke
from bench.identity import error_summary, redact_identity

NETWORK = "default"
IAP_RANGE = "35.235.240.0/20"          # IAP's TCP-forwarding source range
COMPUTE_API = "https://www.googleapis.com/compute/v1"
TCP_22 = [{"IPProtocol": "tcp", "ports": ["22"]}]

# gcloud's `compute ... list` exits 0 with "[]" when its requests failed unless this is false
# (bench.gke.PRECHECK_LIST_ENV, #1172 B3)
LIST_ENV = {"CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR": "false"}


class Refused(Exception):
    """A refusal; the message says why. Nothing after it ran."""


# ─── running gcloud ─────────────────────────────────────────────────────────

def say(msg: str) -> None:
    print(msg, flush=True)


# Every gcloud call is bounded, and the bound is the calling tool's, never a constant here: `timeout=` on the call,
# else the bound the tool set for its scope (call_bound: testvm, its session's declared length; vm606, its own).
_CALL_BOUND: contextvars.ContextVar[float | None] = contextvars.ContextVar("gcevm_call_bound_s", default=None)


@contextlib.contextmanager
def call_bound(seconds: float):
    """Bound every gcloud call made inside this scope (that passes no timeout of its own) by `seconds`."""
    token = _CALL_BOUND.set(float(seconds))
    try:
        yield
    finally:
        _CALL_BOUND.reset(token)


def gcloud(args: list[str], *, dry: bool = False, env: dict | None = None, input_data: str | None = None,
           timeout: float | None = None, quiet: bool = False) -> tuple[int, str, str]:
    """One gcloud call: (rc, stdout, stderr). Dry: printed, not run. Its stdout and stderr are the
    caller's (stderr names the account; an IAM call's stdout is the whole policy): never printed here."""
    if timeout is None:
        timeout = _CALL_BOUND.get()
    if timeout is None:
        raise Refused("a gcloud call with no bound: its tool sets one (gcevm.call_bound)")
    argv = ["gcloud", *args]
    if dry:
        say("DRY-RUN: " + shlex.join(argv))
        return 0, "", ""
    if not quiet:
        say("RUN: " + shlex.join(argv))
    try:
        p = subprocess.run(argv, capture_output=True, text=True, input=input_data, timeout=timeout,
                           env=dict(os.environ, **(env or {})))
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout:.0f} s"
    return p.returncode, p.stdout, p.stderr


def where(project: str, zone: str) -> list[str]:
    return ["--project", project, "--zone", zone]


def ssh_argv(name: str, project: str, zone: str, command: str) -> list[str]:
    """`gcloud compute ssh` through IAP, OS Login, no tty; `command` runs in the login shell."""
    return ["compute", "ssh", name, *where(project, zone), "--tunnel-through-iap", "--quiet", "--ssh-flag=-T",
            "--command", command]


def ssh(name: str, project: str, zone: str, script: str, *, timeout: float | None = None) -> tuple[int, str, str]:
    """Run `script` with bash on the VM (the script on stdin), OS Login through IAP; bounded as gcloud() is."""
    return gcloud(ssh_argv(name, project, zone, "bash -s"), input_data=script, timeout=timeout, quiet=True)


def remote_lines(out: str) -> list[str]:
    return [redact_identity(ln) for ln in (out or "").splitlines()]


def marker(out: str, key: str) -> str | None:
    for ln in (out or "").splitlines():
        if ln.startswith(key + "="):
            return ln[len(key) + 1:]
    return None


# IAP TCP forwarding's tunnel close code, as gcloud prints it: "Error while connecting [<code>: '<reason>']"
# (gcloud 561, api_lib/compute/iap_tunnel_websocket.py: 'Error while connecting [%s].' of the WebSocket's
# '%r: %r' % (close_code, close_reason)). Only the number is ever printed: the code, not gcloud's text.
IAP_TUNNEL_CODE = re.compile(r"Error while connecting \[(4\d{3})\b")
# What each code means, from IAP's own table (IAP FAQ, "Error codes":
# https://docs.cloud.google.com/iap/docs/faq#error-codes), and whether waiting can fix it on a VM up
# just made, with its firewall rules: (stage, waiting can fix it)
IAP_CODES = {
    # "Connection or firewall issue": "Check that your VM process is running and listening on the expected
    # port", and that the firewall allows it: sshd not up yet on a booting VM, or a rule just created
    "4003": ("IAP 4003: IAP reached the VM's network, port 22 did not answer (sshd or the firewall rule not up "
             "yet)", True),
    # "Instance doesn't exist or is stopped": "Make sure your VM is powered on and has fully completed its
    # startup sequence": a VM just created (run 24's first up, 2026-11-11: answered minutes later)
    "4047": ("IAP 4047: IAP does not find the instance running yet (it has not completed its startup)", True),
    # "Connection closed by destination": the remedy is to reset the VM
    "4010": ("IAP 4010: the VM closed the connection (IAP's remedy: reset the VM)", False),
    # "Permission, existence, or VM state issue": the Tunnel User role (iap.tunnelInstances.accessViaIAP)
    "4033": ("IAP 4033: not authorized to tunnel to the VM (iap.tunnelInstances.accessViaIAP)", False),
}


def iap_code(err: str | None) -> str | None:
    m = IAP_TUNNEL_CODE.search(err or "")
    return m.group(1) if m else None


def failed(what: str, rc: int, out: str, err: str, *, prefix: str, tail: int = 20) -> Refused:
    """A failed remote step: its own `<prefix>` lines (or its output's tail), redacted, and gcloud's code."""
    lines = [ln for ln in remote_lines(out) if ln.startswith(prefix)] or remote_lines(out)[-tail:]
    body = ("\n  " + "\n  ".join(lines[-tail:])) if lines else ""
    code = iap_code(err)
    iap = f"; IAP tunnel code {code}" if code else ""
    return Refused(f"{what} failed (rc={rc}; gcloud: {error_summary(err)}{iap}){body}")


# gcloud compute ssh --tunnel-through-iap, a failure without an IAP code: which stage refused, from its
# stderr (never printed: an OS Login refusal names the POSIX user). (pattern, stage, waiting can fix it)
SSH_STAGES = (
    (re.compile(r"Permission denied \(publickey"), "OS Login: the key was refused (compute.osAdminLogin)", False),
    (re.compile(r"Could not fetch resource|was not found|code=404"), "the instance: GCE does not know it", False),
    (re.compile(r"Connection (refused|reset|closed|timed out)|timed out after|kex_exchange"),
     "ssh: the connection failed (the VM may still be booting)", True),
)


def ssh_stage(rc: int, err: str) -> tuple[str, bool]:
    """(stage, waiting can fix it). An IAP tunnel code decides first (ssh's own "connection closed" follows
    every tunnel failure); a code IAP's table does not list is waited out, and named."""
    code = iap_code(err)
    if code:
        return IAP_CODES.get(code, (f"IAP {code}: a tunnel code IAP's table does not list", True))
    for rx, stage, transient in SSH_STAGES:
        if rx.search(err or ""):
            return stage, transient
    return f"ssh: rc={rc}, {error_summary(err)}", True


def wait_for_marker(name: str, project: str, zone: str, script: str, *, ready_timeout_s: float,
                    timeout_s: float, poll_s: float, hint: str, marker_path: str,
                    attempt_s: float | None = None) -> dict:
    """SSH until `script` prints a JSON object (a bootstrap marker). Each failure is read: one waiting cannot
    fix stops at once, naming its stage; a booting VM's (port 22 not yet answering) is waited out until
    ready_timeout_s. Each attempt is bounded by `attempt_s` (vm606's own), else by what is left of timeout_s.
    `hint` says what the operator does next."""
    t0 = time.monotonic()
    reached = False
    while True:
        rc, out, err = ssh(name, project, zone, script,
                           timeout=attempt_s if attempt_s is not None else max(timeout_s - (time.monotonic() - t0),
                                                                               poll_s))
        now = time.monotonic() - t0
        if rc != 0:
            stage, transient = ssh_stage(rc, err)
            if not transient:
                raise Refused(f"SSH to {name} failed at {stage}; the VM is up and billing until its TTL: {hint}")
            if not reached and now >= ready_timeout_s:
                raise Refused(f"SSH to {name} never answered in {ready_timeout_s} s, last at {stage}; the VM "
                              f"is up and billing until its TTL: {hint}")
        else:
            reached = True
            try:
                got = json.loads(out) if out.strip() else None
            except ValueError:
                got = None
            if isinstance(got, dict):
                return got
        if now >= timeout_s:
            raise Refused(f"{name}'s bootstrap did not finish in {timeout_s} s ({marker_path} "
                          f"absent; SSH {'answers' if reached else 'never answered'}; the startup script's log: "
                          f"sudo journalctl -u google-startup-scripts on the VM); {hint}")
        time.sleep(poll_s)


# ─── reading GCE ────────────────────────────────────────────────────────────

def not_found(err: str, path: str) -> bool:
    """GCE's NotFound for `path` ITSELF. gcloud 561 says "The resource '<path>' was not found" for a missing
    instance, disk or firewall rule, and the same words for a missing PARENT ("The resource
    'projects/<p>' was not found" when the project does not exist): only the resource's own full path
    makes a NotFound about it (#1307 round-3 S5)."""
    return f"The resource '{path}' was not found" in (err or "")


def get(args: list[str], path: str, what: str) -> dict | None:
    """One describe: the resource, or None when GCE answers NotFound for `path` itself; any other failure,
    a parent's NotFound among them, refuses (a describe cannot fail open the way a list can)."""
    rc, out, err = gcloud([*args, "--format", "json"], quiet=True)
    if rc != 0:
        if not_found(err, path):
            return None
        raise Refused(f"{what} could not be read (rc={rc}; {error_summary(err)}): refusing")
    try:
        got = json.loads(out)
    except ValueError:
        got = None
    if not isinstance(got, dict):
        raise Refused(f"{what} answered no JSON object: refusing")
    return got


def list_json(args: list[str], what: str) -> list[dict]:
    """One unfiltered list, the partial-error property false: a list, or a refusal (never a failed "[]")."""
    rc, out, err = gcloud(args, env=LIST_ENV, quiet=True)
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    if not isinstance(got, list):
        raise Refused(f"{what} could not be listed (rc={rc}; {error_summary(err)}): refusing")
    return got


def instance(name: str, project: str, zone: str) -> dict | None:
    """The instance `name` in `zone`, or None when GCE answers NotFound for it there (a wrong zone answers
    that too: a down also reads every zone before it calls a VM gone)."""
    return get(["compute", "instances", "describe", name, *where(project, zone)],
               f"projects/{project}/zones/{zone}/instances/{name}", f"instance {name} ({zone})")


def disk(name: str, project: str, zone: str) -> dict | None:
    return get(["compute", "disks", "describe", name, *where(project, zone)],
               f"projects/{project}/zones/{zone}/disks/{name}", f"disk {name}")


def project_doc(project: str) -> dict:
    """The project (`projects describe`): it exists, is ACTIVE and is the one asked. A parent every
    absent verdict in it needs (#1307 round-3 S5); an unknown project refuses here, before anything."""
    rc, out, err = gcloud(["projects", "describe", project, "--format", "json"], quiet=True)
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    if not isinstance(got, dict) or got.get("projectId") != project or not str(got.get("projectNumber", "")).isdigit():
        raise Refused(f"project {project} could not be read (rc={rc}; {error_summary(err)}): unknown, or not "
                      f"visible to this account; nothing in it is read as absent")
    if got.get("lifecycleState") != "ACTIVE":
        raise Refused(f"project {project} is {got.get('lifecycleState')}, not ACTIVE: refusing")
    return got


def parents(project: str, zone: str, recorded_number: str | None = None) -> str:
    """Confirm the run's parents exist before any describe there is read as "absent": the project
    (`projects describe`) and the zone in it (`compute zones describe`: an unknown zone answers "Invalid
    value for field 'zone'"). A recorded project number must be the project's. Returns the number."""
    number = str(project_doc(project)["projectNumber"])
    rc, out, err = gcloud(["compute", "zones", "describe", zone, "--project", project, "--format", "json"], quiet=True)
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    if not isinstance(got, dict) or got.get("name") != zone:
        raise Refused(f"zone {zone} of project {project} could not be read (rc={rc}; {error_summary(err)}): nothing "
                      f"in it is read as absent")
    if recorded_number and str(recorded_number) != number:
        raise Refused(f"the run's record names project number {recorded_number}, {project} is {number}: refusing")
    return number


def require_iap(project: str) -> None:
    """iap.googleapis.com enabled: the VM is reached through it only."""
    rc, out, err = gcloud(["services", "list", "--enabled", "--project", project, "--format", "json"], quiet=True)
    enabled = gke._json(out) if rc == 0 else None
    if not isinstance(enabled, list):
        raise Refused(f"could not read whether IAP is enabled (rc={rc}; {error_summary(err)})")
    if "iap.googleapis.com" not in {(x.get("config") or {}).get("name") for x in enabled}:
        raise Refused(f"iap.googleapis.com is not enabled in {project}: the VM is reached through IAP only "
                      f"(gcloud services enable iap.googleapis.com --project {project})")


def zone_of(inst: dict) -> str:
    return str(inst.get("zone", "")).rsplit("/", 1)[-1]


def named(instances: list[dict]) -> str:
    return ", ".join(sorted(f"{i.get('name')} ({zone_of(i)})" for i in instances))


def instances_claiming(project: str, name: str, run: str) -> list[dict]:
    """Every instance of the project (all zones, one unfiltered list with the partial-error property
    false) that claims `run`: named `name`, or labelled run=<run>, each by exact equality. Either claim
    alone selects it (the run is in use, the VM is not gone); only an instance with both, and the tool's
    owner label, is ever acted on."""
    rows = list_json(["compute", "instances", "list", "--project", project, "--format", "json"],
                     f"the instances of {project}")
    return [i for i in rows if i.get("name") == name or (i.get("labels") or {}).get("run") == run]


def disk_names(inst: dict) -> list[str]:
    return sorted(str(d.get("source", "")).rsplit("/", 1)[-1] for d in inst.get("disks") or [] if d.get("source"))


def time_left_s(inst: dict, now: float) -> float | None:
    t = ((inst.get("scheduling") or {}).get("terminationTime")) or ""
    try:
        end = _dt.datetime.fromisoformat(t.replace("Z", "+00:00"))
    except ValueError:
        return None
    return end.timestamp() - now


def rfc3339(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch(t: str | None) -> float | None:
    try:
        return _dt.datetime.fromisoformat(str(t).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def utc(t) -> str | None:
    try:
        return _dt.datetime.fromisoformat(str(t).replace("Z", "+00:00")).astimezone(_dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None if t is None else str(t)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


# ─── the SSH rules (IAP only) ───────────────────────────────────────────────

def ssh_rules(tag: str, description: str) -> dict[str, dict]:
    """A VM's two SSH rules, by name: what up makes for its network tag and the only rules down deletes."""
    return {f"{tag}-deny-ssh": {"action": "DENY", "sourceRanges": ["0.0.0.0/0"], "priority": 1000,
                                "targetTags": [tag], "description": description},
            f"{tag}-iap-ssh": {"action": "ALLOW", "sourceRanges": [IAP_RANGE], "priority": 900,
                               "targetTags": [tag], "description": description}}


def firewall_create_argv(name: str, rule: dict, project: str) -> list[str]:
    return ["compute", "firewall-rules", "create", name, "--project", project, "--network", NETWORK,
            "--direction", "INGRESS", "--action", rule["action"], "--rules", "tcp:22",
            "--source-ranges", ",".join(rule["sourceRanges"]), "--target-tags", ",".join(rule["targetTags"]),
            "--priority", str(rule["priority"]), "--description", rule["description"]]


# Each describe field of a firewall rule, and the create flags that set it; () is a default up relies on that
# no flag sets (a rule is enabled, and selects by source range and target tag only). Tests check that the flags
# of firewall_create_argv are exactly these (#1331 review: a hand-picked subset let a disabled deny rule pass).
FIREWALL_FIELDS = {
    "name": ("<name>",), "network": ("--project", "--network"), "direction": ("--direction",),
    "allowed/denied": ("--action", "--rules"), "sourceRanges": ("--source-ranges",),
    "targetTags": ("--target-tags",), "priority": ("--priority",), "description": ("--description",),
    "disabled": (), "sourceTags": (), "sourceServiceAccounts": (), "targetServiceAccounts": (),
    "destinationRanges": (),
}


def network_url(project: str) -> str:
    return f"{COMPUTE_API}/projects/{project}/global/networks/{NETWORK}"


def firewall_want(fw: str, rule: dict, project: str) -> dict:
    """The rule as firewall_create_argv makes it, in describe's shape (FIREWALL_FIELDS)."""
    return {"name": fw, "network": network_url(project), "direction": "INGRESS",
            "allowed/denied": {"allowed": TCP_22 if rule["action"] == "ALLOW" else [],
                               "denied": TCP_22 if rule["action"] == "DENY" else []},
            "sourceRanges": sorted(rule["sourceRanges"]), "targetTags": sorted(rule["targetTags"]),
            "priority": rule["priority"], "description": rule["description"], "disabled": False,
            "sourceTags": [], "sourceServiceAccounts": [], "targetServiceAccounts": [], "destinationRanges": []}


def firewall_view(r: dict) -> dict:
    return {"name": r.get("name"), "network": r.get("network"), "direction": r.get("direction"),
            "allowed/denied": {"allowed": r.get("allowed") or [], "denied": r.get("denied") or []},
            "sourceRanges": sorted(r.get("sourceRanges") or []), "targetTags": sorted(r.get("targetTags") or []),
            "priority": r.get("priority"), "description": r.get("description"),
            "disabled": r.get("disabled") is not False,
            **{k: sorted(r.get(k) or []) for k in ("sourceTags", "sourceServiceAccounts", "targetServiceAccounts",
                                                    "destinationRanges")}}


def rule_docs(names, project: str) -> list[dict]:
    """The rules of `names` that exist, each read by its exact name (`describe`: NotFound for that rule is
    "absent", any other failure refuses; never a list filter)."""
    rows = []
    for fw in names:
        r = get(["compute", "firewall-rules", "describe", fw, "--project", project],
                f"projects/{project}/global/firewalls/{fw}", f"firewall rule {fw}")
        if r is not None:
            rows.append(r)
    return rows


def owned_rule(r: dict, want: dict, tool: str, run: str) -> None:
    """A rule of the run's exact name is the run's when its target tag and description are the ones up recorded
    creating: else it is someone else's, and refuses."""
    if sorted(r.get("targetTags") or []) != sorted(want["targetTags"]) or r.get("description") != want["description"]:
        raise Refused(f"firewall rule {r.get('name')!r} is not one {tool} made for run {run} (target tags "
                      f"{short(r.get('targetTags'))}, description {short(r.get('description'))}): refusing; nothing "
                      f"was changed")


# ─── as made: an instance's describe in the shape of its create flags ───────

def boot_disk_and_image(inst: dict, project: str, zone: str) -> tuple[dict | None, dict | None]:
    disks = inst.get("disks") or []
    d = str((disks[0] if disks else {}).get("source", "")).rsplit("/", 1)[-1]
    dk = disk(d, project, zone) if d else None
    image = None
    m = re.search(r"/projects/([^/]+)/global/images/([^/]+)$", str((dk or {}).get("sourceImage", "")))
    if m:
        image = get(["compute", "images", "describe", m[2], "--project", m[1]],
                    f"projects/{m[1]}/global/images/{m[2]}", f"image {m[2]}")
    return dk, image


def instance_view(inst: dict, disk_doc: dict | None, image: dict | None, *, label_keys, account) -> dict:
    """labels: the keys the tool sets (a label grants nothing); metadata: every item (an ssh-keys item, another
    startup script or OS Login switched off each change who gets in); a service account by `account(email)`,
    never its address (output passes no email)."""
    disks = inst.get("disks") or []
    d0 = disks[0] if disks else {}
    labels = inst.get("labels") or {}
    sched = inst.get("scheduling") or {}
    return {"name": inst.get("name"), "zone": inst.get("zone"), "machineType": inst.get("machineType"),
            "boot image": {"project": (re.search(r"/projects/([^/]+)/global/images/", str((disk_doc or {}).get(
                "sourceImage", ""))) or [None, None])[1], "family": (image or {}).get("family")},
            "boot disk": {"disks": len(disks), "boot": d0.get("boot"), "autoDelete": d0.get("autoDelete"),
                          "source": d0.get("source"), "sizeGb": (disk_doc or {}).get("sizeGb"),
                          "type": (disk_doc or {}).get("type")},
            "serviceAccounts": [{"email": account(a.get("email")), "scopes": sorted(a.get("scopes") or [])}
                                for a in inst.get("serviceAccounts") or []],
            "network": [{"network": n.get("network"), "accessConfigs": [c.get("type") for c in n.get("accessConfigs")
                                                                         or []]}
                        for n in inst.get("networkInterfaces") or []],
            "tags": sorted((inst.get("tags") or {}).get("items") or []),
            "labels": {k: labels.get(k) for k in label_keys},
            "metadata": {i.get("key"): (sha256_text(i.get("value") or "") if i.get("key") == "startup-script"
                                        else i.get("value"))
                         for i in (inst.get("metadata") or {}).get("items") or []},
            "terminationTime": utc(sched.get("terminationTime")),
            "instanceTerminationAction": sched.get("instanceTerminationAction"),
            "shieldedInstanceConfig": {k: (inst.get("shieldedInstanceConfig") or {}).get(k)
                                       for k in ("enableSecureBoot", "enableVtpm", "enableIntegrityMonitoring")}}


def short(v) -> str:
    t = redact_identity(json.dumps(v, sort_keys=True))
    return t if len(t) <= 100 else t[:97] + "..."


def diff(what: str, got: dict, want: dict) -> list[str]:
    """Each field that differs, named; a mapping field names its differing keys (never a whole startup script)."""
    out = []
    for k, w in want.items():
        g = got.get(k)
        if g == w:
            continue
        if isinstance(w, dict) and isinstance(g, dict):
            keys = sorted(x for x in {*w, *g} if g.get(x) != w.get(x))
            out.append(f"{what} {k}: " + ", ".join(
                f"{x} {short(g.get(x)) if x != 'startup-script' else '(differs)'}, up made "
                f"{short(w.get(x)) if x != 'startup-script' else 'its own'}" for x in keys))
        else:
            out.append(f"{what} {k}: {short(g)}, up made {short(w)}")
    return out


# ─── quota ──────────────────────────────────────────────────────────────────

def quota_check(project: str, zone: str, footprint) -> tuple[list[str], list[str]]:
    """A VM's footprint ((metric, need, what, unit), ...) against the quotas of the region it sits in, reads
    only, each failing closed (bench.gke's primitives: the region's quotas, its instances / forwarding rules /
    addresses listed with the partial-error property false, in-use addresses counted from their holders)."""
    region = gke.region_of(zone)
    rc, out, err = gcloud(gke.region_quotas_command(project, zone)[1:], quiet=True)
    quotas = gke._json(out) if rc == 0 else None
    if not isinstance(quotas, dict):
        raise Refused(f"the quotas of {region} could not be read (rc={rc}; {error_summary(err)}): whether the VM fits "
                      f"is unknown")
    listed: dict[str, list[dict]] = {}
    for kind in ("instances", "forwarding-rules", "addresses"):
        rc, out, err = gcloud(gke.region_list_command(kind, project, region)[1:], env=LIST_ENV, quiet=True)
        got = gke._json(out) if rc == 0 else None
        if not isinstance(got, list):
            raise Refused(f"the {kind} of {region} could not be listed (rc={rc}; {error_summary(err)}): whether the "
                          f"VM fits is unknown")
        listed[kind] = got
    fits, refusals = [], []
    for metric, need, what, unit in footprint:
        used = gke.in_use_addresses(listed, region) if metric == "IN_USE_ADDRESSES" else None
        ok, msg = gke.quota_fit(quotas, metric, need, f"the VM needs {need}{unit}: {what}", unit, used)
        (fits if ok else refusals).append(msg)
    return fits, refusals


# ─── bounded transfers over ssh (#1336) ─────────────────────────────────────

# The probe that measures a direction's rate: this many bytes through the same `gcloud compute ssh` path the
# transfer takes, right before it. It is the one transfer with no rate to derive a bound from: its caller bounds
# it (testvm: by what is left of the VM's own deadline).
PROBE_BYTES = 4 << 20
# A transfer may take SLACK times what the probe's rate predicts (connection included), and a stall is no byte
# moving for SLACK times the whole probe: multiplicative both, so a k-fold slowdown beyond SLACK is refused,
# never hidden under an additive allowance.
SLACK = 8
CHUNK = 1 << 16


class Rate:
    """One probe of a direction: `seconds` for `nbytes`, connection included (so the rate is a lower bound)."""

    def __init__(self, nbytes: int, seconds: float):
        self.nbytes, self.seconds = nbytes, max(seconds, 1e-3)

    def bound_s(self, nbytes: int) -> float:
        """The transfer's deadline: SLACK x (one connection + nbytes at the probe's rate)."""
        return SLACK * self.seconds * (1 + nbytes / self.nbytes)

    def stall_s(self) -> float:
        return SLACK * self.seconds

    def __repr__(self) -> str:
        return f"{self.nbytes / self.seconds / 1e6:.2f} MB/s over {self.seconds:.2f} s"


def _kill(p: subprocess.Popen) -> None:
    """A refused transfer's gcloud and everything it started (its IAP tunnel helper among them): SIGKILL to the
    group, which nothing can outlive or delay (no grace window to choose)."""
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    p.wait()


def stream(argv: list[str], *, what: str, nbytes: int, bound_s: float, stall_s: float,
           src: str | None = None, dst: str | None = None) -> tuple[int, str, str]:
    """One `gcloud ...` byte stream: `src`'s bytes into its stdin (an upload), or its stdout into `dst` (a
    download), never both. Bounded: refused by name when it runs past `bound_s`, or when no byte moves for
    `stall_s` ("<what> stalled at N of M bytes"); the gcloud process group is killed either way. Returns (rc,
    stdout of an upload, stderr) once gcloud exits; a download's byte count is checked here, its sha256 by the
    caller."""
    full = ["gcloud", *argv]
    shown = [f"<a {len(x)}-byte remote script>" if "\n" in x else x for x in full]
    say(f"RUN: {shlex.join(shown)}  ({what}: {nbytes} bytes, bound {bound_s:.0f} s, stall {stall_s:.0f} s)")
    with tempfile.TemporaryFile() as errf, tempfile.TemporaryFile() as outf:
        src_fh = open(src, "rb") if src else None
        dst_fh = open(dst, "wb") if dst else None
        try:
            p = subprocess.Popen(full, stdin=subprocess.PIPE if src else subprocess.DEVNULL,
                                 stdout=subprocess.PIPE if dst else outf, stderr=errf, start_new_session=True)
            t0 = last = time.monotonic()
            moved = 0
            sel = selectors.DefaultSelector()
            if src:
                os.set_blocking(p.stdin.fileno(), False)
                sel.register(p.stdin, selectors.EVENT_WRITE)
            if dst:
                sel.register(p.stdout, selectors.EVENT_READ)
            pending = b""
            while sel.get_map():
                now = time.monotonic()
                if now - last > stall_s:                 # the more specific verdict first: nothing moves at all
                    _kill(p)
                    raise Refused(f"{what} stalled at {moved} of {nbytes} bytes (no byte moved for {stall_s:.0f} s, "
                                  f"{SLACK}x the probe): refusing")
                if now - t0 > bound_s:
                    _kill(p)
                    raise Refused(f"{what} ran past its bound of {bound_s:.0f} s at {moved} of {nbytes} bytes "
                                  f"(derived from the size and the probe's rate): refusing")
                # wake exactly at the nearer of the two verdicts' times, or at the next byte
                events = sel.select(timeout=max(min(stall_s - (now - last), bound_s - (now - t0)), 0) + 1e-3)
                for key, _ev in events:
                    if key.fileobj is p.stdout:
                        b = os.read(p.stdout.fileno(), CHUNK)
                        if not b:
                            sel.unregister(p.stdout)
                            continue
                        dst_fh.write(b)
                        moved += len(b)
                        last = time.monotonic()
                    else:
                        if not pending:
                            pending = src_fh.read(CHUNK)
                        if not pending:
                            sel.unregister(p.stdin)
                            p.stdin.close()
                            continue
                        try:
                            n = os.write(p.stdin.fileno(), pending)
                        except BlockingIOError:
                            continue
                        except BrokenPipeError:
                            sel.unregister(p.stdin)
                            break
                        pending = pending[n:]
                        moved += n
                        last = time.monotonic()
                if p.poll() is not None and src and p.stdin in [k.fileobj for k in sel.get_map().values()]:
                    sel.unregister(p.stdin)          # gcloud exited before taking every byte: its rc says why
            # every byte moved; gcloud (and the remote side's report) has the rest of the bound
            left = bound_s - (time.monotonic() - t0)
            try:
                rc = p.wait(timeout=max(left, 0.001))
            except subprocess.TimeoutExpired:
                _kill(p)
                raise Refused(f"{what} moved {moved} of {nbytes} bytes and did not finish within its bound of "
                              f"{bound_s:.0f} s: refusing") from None
        finally:
            for fh in (src_fh, dst_fh):
                if fh:
                    fh.close()
        errf.seek(0)
        outf.seek(0)
        err = errf.read().decode(errors="replace")
        out = outf.read().decode(errors="replace")
    if dst and rc == 0 and moved != nbytes:
        raise Refused(f"{what} ended at {moved} of {nbytes} bytes: refusing")
    return rc, out, err


# the remote side of an upload: exactly N bytes from stdin into a file (atomically), then its size and sha256
_RECEIVE = ("set -euo pipefail\numask 077\nd={dst}\nmkdir -p \"$(dirname \"$d\")\"\nhead -c {n} > \"$d.part\"\n"
            "python3 -c 'import hashlib,os,sys; p=sys.argv[1]; h=hashlib.sha256(); f=open(p,\"rb\"); "
            "[h.update(b) for b in iter(lambda: f.read(1<<20), b\"\")]; "
            "print(\"GCEVM_BYTES=%d\" % os.path.getsize(p)); print(\"GCEVM_SHA256=\" + h.hexdigest())' \"$d.part\"\n"
            "mv \"$d.part\" \"$d\"\n")


def _bash_c(script: str) -> str:
    return "bash -c " + shlex.quote(script)


def round_trip(name: str, project: str, zone: str, bound_s: float) -> float:
    """The seconds one SSH session through IAP takes, connection and all, measured now (a no-op, under bound_s)."""
    t0 = time.monotonic()
    rc, out, err = ssh(name, project, zone, "true\n", timeout=bound_s)
    if rc != 0:
        raise failed("the SSH round trip before a transfer", rc, out, err, prefix="GCEVM")
    return time.monotonic() - t0


def probe(name: str, project: str, zone: str, direction: str, scratch: str, bound_s: float) -> Rate:
    """The rate of `direction` ("up" to the VM, "down" from it), measured by PROBE_BYTES through the same ssh
    path, under the caller's `bound_s`. Its stall bound is derived like a transfer's: no byte for SLACK x an SSH
    round trip measured just before it (the probe's own connection costs one) is a stall, refused by name.
    `scratch`: a directory of this run on the VM."""
    stall_s = SLACK * round_trip(name, project, zone, bound_s)
    with tempfile.TemporaryDirectory() as d:
        t0 = time.monotonic()
        if direction == "up":
            src = os.path.join(d, "probe")
            with open(src, "wb") as fh:
                fh.write(b"\0" * PROBE_BYTES)
            cmd = _bash_c(_RECEIVE.format(dst=shlex.quote(f"{scratch}/probe"), n=PROBE_BYTES))
            rc, out, err = stream(ssh_argv(name, project, zone, cmd), what="the upload probe", nbytes=PROBE_BYTES,
                                  bound_s=bound_s, stall_s=stall_s, src=src)
            if rc == 0 and marker(out, "GCEVM_BYTES") != str(PROBE_BYTES):
                raise Refused(f"the upload probe arrived as {marker(out, 'GCEVM_BYTES')} bytes, not {PROBE_BYTES}")
        else:
            cmd = _bash_c(f"head -c {PROBE_BYTES} /dev/zero")
            rc, out, err = stream(ssh_argv(name, project, zone, cmd), what="the download probe", nbytes=PROBE_BYTES,
                                  bound_s=bound_s, stall_s=stall_s, dst=os.path.join(d, "probe"))
        if rc != 0:
            raise failed(f"the {direction}load probe", rc, out, err, prefix="GCEVM")
        rate = Rate(PROBE_BYTES, time.monotonic() - t0)
    say(f"{direction}load probe: {PROBE_BYTES} bytes in {rate.seconds:.2f} s")
    return rate


def upload(name: str, project: str, zone: str, src: str, dst: str, *, what: str, scratch: str,
           probe_bound_s: float) -> str:
    """`src` to `dst` on the VM over ssh, bounded by its size and a fresh probe's rate (the probe under
    `probe_bound_s`); the VM's sha256 of what arrived must be the laptop's. Returns the sha256."""
    nbytes = os.path.getsize(src)
    want = sha256_file(src)
    rate = probe(name, project, zone, "up", scratch, probe_bound_s)
    cmd = _bash_c(_RECEIVE.format(dst=shlex.quote(dst), n=nbytes))
    rc, out, err = stream(ssh_argv(name, project, zone, cmd), what=what, nbytes=nbytes,
                          bound_s=rate.bound_s(nbytes), stall_s=rate.stall_s(), src=src)
    if rc != 0:
        raise failed(what, rc, out, err, prefix="GCEVM")
    got_n, got = marker(out, "GCEVM_BYTES"), marker(out, "GCEVM_SHA256")
    if got_n != str(nbytes) or got != want:
        raise Refused(f"{what} arrived as {got_n} bytes with sha256 {got}; the laptop's is {nbytes} bytes, {want}")
    return got


def download(name: str, project: str, zone: str, src: str, dst: str, *, nbytes: int, sha256: str, what: str,
             probe_bound_s: float) -> None:
    """`src` on the VM (whose size and sha256 the VM reported) to `dst` over ssh (`cat`), bounded by its size and a
    fresh probe's rate (the probe under `probe_bound_s`); refused unless the bytes that arrived have that sha256."""
    rate = probe(name, project, zone, "down", os.path.dirname(src), probe_bound_s)
    rc, out, err = stream(ssh_argv(name, project, zone, _bash_c(f"cat {shlex.quote(src)}")), what=what,
                          nbytes=nbytes, bound_s=rate.bound_s(nbytes), stall_s=rate.stall_s(), dst=dst)
    if rc != 0:
        raise failed(what, rc, out, err, prefix="GCEVM")
    got = sha256_file(dst)
    if got != sha256:
        Path(dst).unlink()
        raise Refused(f"{what} arrived with sha256 {got}, the VM's is {sha256}: nothing was kept")
