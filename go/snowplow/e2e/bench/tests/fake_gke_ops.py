"""What a fake gcloud answers about a cluster's operations and its maintenance policy (#1422),
modelled on the live API and on gcloud's own code rather than on an outcome.

Where each shape comes from:

  - `container operations list --format json`: the Operation objects gcloud printed for the region of
    run 26i's clusters on 2026-11-14 (SDK 561, read-only). Keys, exactly: name
    ("operation-<epoch ms>-<8 hex>-<4>-<4>-<4>-<12>"), operationType, status, startTime, endTime (ABSENT
    while the operation runs: the one RUNNING RESIZE_CLUSTER of that listing had none), selfLink,
    targetLink, zone (the REGION for a regional cluster), and `progress` on CREATE_CLUSTER only. Times
    are RFC 3339 with a Z and 7 to 9 fractional digits ("2026-11-14T08:54:14.0659289Z"). targetLink is
    https://container.googleapis.com/v1/projects/<project NUMBER>/locations/<region>/clusters/<name>,
    with /nodePools/<pool> after it for a node-pool operation. `operation()` builds one. gcloud's
    command (surface/container/operations/list.py) returns ListOperationsResponse.operations and
    prints nothing on stderr when it succeeds (0 bytes on that read).
  - what GKE records by itself: a create is a CREATE_CLUSTER, each node pool a CREATE_NODE_POOL, a
    maintenance-policy update an UPDATE_CLUSTER (the API reference: "SET_MAINTENANCE_POLICY: Unused.
    Updating maintenance policy uses UPDATE_CLUSTER"), a delete a DELETE_CLUSTER; the fake appends one
    to <state>/operations.json per such call, so a listing after them answers what GKE's would.
  - `container clusters describe`'s maintenancePolicy: {"resourceVersion": "e3b0c442"} on a cluster
    with no policy (run 26i's clusters, the same read). With an exclusion: the API reference's
    MaintenancePolicy.window (MaintenanceWindow).maintenanceExclusions, a map of the exclusion's name
    to a TimeWindow {startTime, endTime, maintenanceExclusionOptions: {scope: NO_UPGRADES |
    NO_MINOR_UPGRADES | NO_MINOR_OR_NODE_UPGRADES}}
    (https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters).
    NOT read from a live cluster: no bench cluster has held an exclusion yet. Whether GKE prints the
    scope of a NO_UPGRADES exclusion (the enum's default) or leaves it out is not known, so the fake
    can answer either (FAKE_EXCLUSION_NO_SCOPE) and gke.exclusion_check takes both.
  - `container clusters update --add-maintenance-exclusion-*`: gcloud's flags (command_lib/container/
    flags.py AddMaintenanceExclusionFlags, SDK 561): the name, an optional start, a REQUIRED end (or
    --add-maintenance-exclusion-until-end-of-support), a scope matching
    ^(no_upgrades|no_minor_upgrades|no_minor_or_node_upgrades)$; an exclusion whose name the policy
    already holds is refused (api_adapter.AddMaintenanceExclusion: "A maintenance exclusion named
    {0} already exists."). The wording of the fake's own refusals is not gcloud's, only their exit codes
    and the "running incompatible operation <id>" text (run 15's, which the bench matches) are.

Knobs (environment):
  FAKE_OPERATIONS_EXTRA      a JSON list of Operation objects the region also lists (operation())
  FAKE_OPERATIONS_RC         the listing exits with this code and gcloud's error text
  FAKE_OPERATIONS_KILLED_RC  the listing prints its list and exits with this code, saying nothing (a gcloud
                             that was killed: 143, or timeout(1)'s 124)
  FAKE_OPERATIONS_STDERR     the listing exits 0, prints its list, and this on stderr (a partial result)
  FAKE_OPERATIONS_OUT        the listing exits 0 and prints exactly this instead of its list
  FAKE_EXCLUSION_UPDATE_RC   the update fails with this code and a GKE error
  FAKE_EXCLUSION_BUSY        an operation id: the FIRST update is refused, the cluster running it
  FAKE_EXCLUSION_DROPPED     the update exits 0 and the cluster does not hold the exclusion
  FAKE_EXCLUSION_SCOPE       the scope the cluster holds, whatever was asked (e.g. NO_MINOR_UPGRADES)
  FAKE_EXCLUSION_END_SHIFT_S the cluster's exclusion ends this many seconds off what was asked
  FAKE_EXCLUSION_NO_SCOPE    the cluster's exclusion carries no maintenanceExclusionOptions

A fake gcloud is its own process, so it loads this file by path: LOADER is the source to put in front
of its body; it defines ops_note(state, kind, argv), answer_operations(state, argv),
answer_exclusion_update(state, argv) and maintenance_policy(state, name).
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

PROJECT_NUMBER = "123456789012"          # a placeholder: the live links carry the project's number
API = "https://container.googleapis.com/v1"
NO_POLICY = {"resourceVersion": "e3b0c442"}
_SCOPE = re.compile(r"^(no_upgrades|no_minor_upgrades|no_minor_or_node_upgrades)$")
_TIME = re.compile(r"(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.\d+)?(Z|[+-]\d\d:\d\d)?")


def _nanos(t: float) -> str:
    """GKE's operation times: RFC 3339, Z, nanoseconds with trailing zeros dropped."""
    frac = f"{t - int(t):.9f}"[2:].rstrip("0")
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(t))) + (f".{frac}" if frac else "") + "Z"


def operation(kind: str, cluster: str, *, start: float, end: float | None = None, pool: str | None = None,
              region: str = "europe-west4", status: str | None = None) -> dict:
    """One Operation as gcloud prints it. `end` None: still running (no endTime, status RUNNING)."""
    name = f"operation-{int(start * 1000)}-{uuid.UUID(int=int(start * 1e6) & (2 ** 128 - 1))}"
    target = f"{API}/projects/{PROJECT_NUMBER}/locations/{region}/clusters/{cluster}"
    op = {"name": name, "operationType": kind,
          "selfLink": f"{API}/projects/{PROJECT_NUMBER}/locations/{region}/operations/{name}",
          "startTime": _nanos(start), "status": status or ("DONE" if end is not None else "RUNNING"),
          "targetLink": target + (f"/nodePools/{pool}" if pool else ""), "zone": region}
    if end is not None:
        op["endTime"] = _nanos(end)
    if kind == "CREATE_CLUSTER":
        op["progress"] = {"metrics": [{"intValue": "9", "name": "CLUSTER_CONFIGURING"},
                                      {"intValue": "9", "name": "CLUSTER_CONFIGURING_TOTAL"}]}
    return op


def _opt(a: list[str], name: str):
    return a[a.index(name) + 1] if name in a else None


def _load(path: Path, default):
    return json.loads(path.read_text()) if path.exists() else default


def ops_note(state: str, kind: str, a: list[str]) -> None:
    """GKE's own record of a call that is an operation: `a` is the gcloud argv (clusters create /
    update / delete NAME, node-pools create POOL --cluster NAME)."""
    pool = a[3] if a[:3] == ["container", "node-pools", "create"] else None
    cluster = _opt(a, "--cluster") if pool else a[3]
    region = _opt(a, "--region") or _opt(a, "--location") or "europe-west4"
    path = Path(state) / "operations.json"
    now = time.time()
    ops = _load(path, [])
    ops.append(operation(kind, cluster, start=now, end=now + 0.5, pool=pool, region=region))
    path.write_text(json.dumps(ops))


def answer_operations(state: str, a: list[str]) -> None:
    """`gcloud container operations list --project P --region R --format json`."""
    env = os.environ
    if env.get("FAKE_OPERATIONS_RC"):
        sys.stderr.write("ERROR: (gcloud.container.operations.list) ResponseError: code=503, message=The service is "
                         "currently unavailable. This command is authenticated as "
                         "PLACEHOLDER-OPERATOR@example.invalid which is the active account specified by the "
                         "[core/account] property.\n")
        sys.exit(int(env["FAKE_OPERATIONS_RC"]))
    if "FAKE_OPERATIONS_OUT" in env:
        sys.stdout.write(env["FAKE_OPERATIONS_OUT"])
        sys.stderr.write(env["FAKE_OPERATIONS_STDERR"] + "\n" if env.get("FAKE_OPERATIONS_STDERR") else "")
        sys.exit(0)
    region = _opt(a, "--region") or _opt(a, "--location")
    ops = _load(Path(state) / "operations.json", []) + json.loads(env.get("FAKE_OPERATIONS_EXTRA") or "[]")
    print(json.dumps([o for o in ops if region in (None, o.get("zone"))], indent=2))
    if env.get("FAKE_OPERATIONS_KILLED_RC"):
        sys.stdout.flush()
        sys.exit(int(env["FAKE_OPERATIONS_KILLED_RC"]))
    if env.get("FAKE_OPERATIONS_STDERR"):
        sys.stderr.write(env["FAKE_OPERATIONS_STDERR"] + "\n")
    sys.exit(0)


def _instant(text: str) -> int:
    m = _TIME.fullmatch(text)
    if m is None:
        sys.stderr.write(f"ERROR: (gcloud.container.clusters.update) argument --add-maintenance-exclusion-*: "
                         f"Failed to parse date/time: Unknown string format: {text}\n")
        sys.exit(2)
    y, mo, d, h, mi, s, tz = m.groups()
    t = calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(s)))
    if tz and tz != "Z":
        off = int(tz[1:3]) * 3600 + int(tz[4:6]) * 60
        t -= off if tz[0] == "+" else -off
    return t


def _z(t: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def answer_exclusion_update(state: str, a: list[str]) -> None:
    """`gcloud container clusters update NAME ... --add-maintenance-exclusion-name N -start S -end E
    -scope SCOPE`: validated as gcloud validates it, then held by the cluster (<state>/
    maintenance-<NAME>.json) the way the knobs say."""
    env = os.environ
    name, excl = a[3], _opt(a, "--add-maintenance-exclusion-name")
    start, end = _opt(a, "--add-maintenance-exclusion-start"), _opt(a, "--add-maintenance-exclusion-end")
    scope = _opt(a, "--add-maintenance-exclusion-scope")
    if end is None and "--add-maintenance-exclusion-until-end-of-support" not in a:
        sys.stderr.write("ERROR: (gcloud.container.clusters.update) Exactly one of (--add-maintenance-exclusion-end "
                         "| --add-maintenance-exclusion-until-end-of-support) must be specified.\n")
        sys.exit(2)
    if scope is not None and not _SCOPE.match(scope):
        sys.stderr.write("ERROR: (gcloud.container.clusters.update) argument --add-maintenance-exclusion-scope: Bad "
                         'value [' + scope + ']: Must be in one of "no_upgrades", "no_minor_upgrades" or '
                         '"no_minor_or_node_upgrades"\n')
        sys.exit(2)
    t0, t1 = (_instant(start) if start else int(time.time())), _instant(end)
    busy = Path(state) / "exclusion-busy-answered"
    if env.get("FAKE_EXCLUSION_BUSY") and not busy.exists():
        busy.write_text("1")
        sys.stderr.write("ERROR: (gcloud.container.clusters.update) ResponseError: code=400, message=Cluster is "
                         f"running incompatible operation {env['FAKE_EXCLUSION_BUSY']}.\n")
        sys.exit(1)
    if env.get("FAKE_EXCLUSION_UPDATE_RC"):
        sys.stderr.write("ERROR: (gcloud.container.clusters.update) ResponseError: code=400, message=Maintenance "
                         "exclusion is not valid.\n")
        sys.exit(int(env["FAKE_EXCLUSION_UPDATE_RC"]))
    path = Path(state) / f"maintenance-{name}.json"
    held = _load(path, {})
    excl = excl or "generated-exclusion-" + _z(int(time.time()))
    if excl in held:
        sys.stderr.write(f"ERROR: (gcloud.container.clusters.update) A maintenance exclusion named {excl} already "
                         f"exists.\n")
        sys.exit(1)
    if not env.get("FAKE_EXCLUSION_DROPPED"):
        window = {"startTime": _z(t0), "endTime": _z(t1 + int(env.get("FAKE_EXCLUSION_END_SHIFT_S") or 0))}
        if not env.get("FAKE_EXCLUSION_NO_SCOPE"):
            window["maintenanceExclusionOptions"] = {
                "scope": env.get("FAKE_EXCLUSION_SCOPE") or (scope or "no_upgrades").upper()}
        held[excl] = window
        path.write_text(json.dumps(held))
    ops_note(state, "UPDATE_CLUSTER", a)
    sys.stderr.write(f"Updating {name}...\n.done.\n")
    sys.exit(0)


def maintenance_policy(state: str, name: str) -> dict:
    """Cluster.maintenancePolicy as `clusters describe` answers it."""
    held = _load(Path(state) / f"maintenance-{name}.json", {})
    return dict(NO_POLICY, window={"maintenanceExclusions": held}) if held else dict(NO_POLICY)


LOADER = (
    "import importlib.util as _ilu\n"
    f"_fos = _ilu.spec_from_file_location('fake_gke_ops', {str(Path(__file__).resolve())!r})\n"
    "_fom = _ilu.module_from_spec(_fos)\n"
    "_fos.loader.exec_module(_fom)\n"
    "ops_note = _fom.ops_note\n"
    "answer_operations = _fom.answer_operations\n"
    "answer_exclusion_update = _fom.answer_exclusion_update\n"
    "maintenance_policy = _fom.maintenance_policy\n"
)
