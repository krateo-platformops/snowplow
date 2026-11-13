"""Every Google API the run calls, derived from the code, and each one ENABLED in the run's project, checked
before anything is created (run 26g).

Run 26g: all three arms created their GKE clusters, then failed in gke-up after about 10 minutes each: "TTL
backstop NOT proven: IAM refused the permission test (HTTP 403, PERMISSION_DENIED)". The TTL proof Job then asked
cloudresourcemanager.googleapis.com as the GKE node SA, and that API was not enabled in the project (the error's
reason, SERVICE_DISABLED, was not printed then: bench.gke.error_reasons prints it now). Nothing had checked, before
the clusters existed, that the APIs the run calls are enabled. (Since run 26h the proof calls the GKE API instead,
as the bench's own node SA: NODE_SA_REST.)

WHO CALLS WHAT, read from the code (`required`), never from a list kept by hand:
  - the laptop (the operator: OPERATOR_SIDE, bench.vm606 and the modules it shares) and the orchestrator (every
    other bench module, on the vm606 VM as its SA): every gcloud argv in bench/*.py (a list literal of "gcloud"
    then its command group, the operator modules' wrapper called on a list that starts with the group, and an argv
    list that starts with a group GCLOUD_API maps),
    each mapped by its command group to the API it calls (GCLOUD_API); every REST URL on a googleapis.com host;
    every gcloud flag or VM setting that calls an API of its own (FLAG_API: an IAP tunnel, OS Login);
  - the GKE node SA: what the in-cluster pods that run as it do, read from the manifests bench.gke renders for
    them (the ttl-self-destruct CronJob, the TTL proof Job): their gcloud calls and REST URLs; and every REST
    constant NODE_SA_REST names (the one source of which URLs only the node SA requests; bench.vm606's plan-iam
    reads it too); and what the cluster create argv bench.gke builds makes the nodes send as the node SA
    (CLUSTER_FLAG_API: --logging SYSTEM, --monitoring SYSTEM: GKE's system logging and monitoring agents).
A gcloud command group no entry of GCLOUD_API maps (a call the derivation cannot place) refuses the check by name:
the set is never silently short.

THE CHECK (`check`): one read-only `gcloud services list --enabled` of the project. An answer that is not a
non-empty JSON list of services is a refusal, never "nothing disabled": gcloud's list commands can exit 0 with an
empty answer when their request failed (bench.gke.COMPUTE_LIST_ENV, the partial-error property set false, is on
the call all the same). Each API the run calls that is not ENABLED is named, with who calls it and where, and the
run refuses.

WHERE: bench.vm606 `up` (dry or not, before any grant, rule, bucket or VM: its _project_ready) and run606.sh's
`apis` step, its first, before any cluster (its dry pass derives the set offline and reads nothing from GCP, as
every other GCP read of the dry pass).

    python3 -m bench.gcp_apis required                # every API the run calls, and who calls it where
    python3 -m bench.gcp_apis check --project P       # exit 0, or 2 naming each one not enabled
    python3 -m bench.gcp_apis check --project P --dry # the derivation only; no GCP call
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from bench import gke
from bench.identity import error_summary

BENCH_DIR = Path(__file__).resolve().parent.parent

#: a gcloud command group -> the API it calls (None: gcloud's own, no API: its credential, its version)
GCLOUD_API: dict[str, str | None] = {
    "compute": "compute.googleapis.com",
    "container": "container.googleapis.com",
    "storage": "storage.googleapis.com",
    "projects": "cloudresourcemanager.googleapis.com",
    "iam": "iam.googleapis.com",
    "services": "serviceusage.googleapis.com",
    "auth": None,
    "version": None,
}
#: a gcloud flag or a VM setting (as the code writes it, quoted) -> the API it makes gcloud call: an SSH / scp
#: through an IAP tunnel, and OS Login's key import on an instance with enable-oslogin
FLAG_API = {"--tunnel-through-iap": "iap.googleapis.com", "enable-oslogin": "oslogin.googleapis.com"}
#: a flag of the cluster create argv bench.gke builds -> the API the cluster's nodes then write to as the GKE node SA
#: (GKE's system logging and monitoring agents), unless its value is NONE (the cluster then sends nothing there)
CLUSTER_FLAG_API = {"--logging": "logging.googleapis.com", "--monitoring": "monitoring.googleapis.com"}
#: a URL on the www host of googleapis.com names its API by its first path segment (www.googleapis.com/compute/v1:
#: compute.googleapis.com), except an OAuth scope (www.googleapis.com/auth/...)
WWW_NOT_API = frozenset({"auth"})
#: A REST API the orchestrator never calls itself: a module constant (by name, in its file, on its host) whose URL
#: only a pod in the bench cluster requests, as the GKE node SA. #1383 gap 3 / run 26h: gke-up's TTL proof Job calls
#: the GKE API (clusters get and delete, on two cluster names that do not exist) AS the identity the
#: ttl-self-destruct CronJob runs as. bench.vm606's plan-iam skips it (no orchestrator role); `required` counts it
#: as the node SA's.
NODE_SA_REST = {"TTL_PROOF_API": ("gke.py", "container")}
#: the operator's modules: their calls run on the laptop as the operator, never as the orchestrator SA
OPERATOR_SIDE = ("vm606.py", "gcevm.py", "testvm.py")
#: the one read: how long it may take (a services list answers in seconds; a call past this is no answer)
SERVICES_LIST_BOUND_S = 120.0

LAPTOP, ORCHESTRATOR, NODE_SA = "laptop (operator)", "orchestrator", "GKE node SA"

_ARGV = re.compile(r'"gcloud"\s*,\s*"([^"]+)"')
_WRAPPED = re.compile(r'\b_?gcloud\(\s*\[\s*"(?!gcloud")([^"]+)"')   # (one with "gcloud" first is _ARGV's)
_REST = re.compile(r"https?://([a-z0-9-]+)\.googleapis\.com(?:/([a-z0-9-]+))?")
_SCRIPT = re.compile(r"(?:^|[\s;&|(`$])gcloud\s+([a-z][a-z0-9-]*)")
_NAMED = re.compile(r"\s*([A-Z_][A-Z0-9_]*)\s*=")


class Refused(Exception):
    """The run's APIs could not be confirmed enabled: one is not, or the answer could not be read."""


def _line(src: str, pos: int) -> int:
    return src.count("\n", 0, pos) + 1


def _rest_api(host: str, segment: str | None) -> str | None:
    if host != "www":
        return f"{host}.googleapis.com"
    return None if not segment or segment in WWW_NOT_API else f"{segment}.googleapis.com"


def node_sa_manifests() -> list[tuple[str, str]]:
    """(what, text) of every manifest whose pods run as the GKE node SA, as bench.gke renders them."""
    spec = gke.default_spec(gke.BENCH_NAME_PREFIX + "apis")
    return [("the ttl-self-destruct CronJob (bench.gke.self_destruct_manifest)", gke.self_destruct_manifest(spec)),
            ("the TTL proof Job (bench.gke.ttl_proof_manifest)", gke.ttl_proof_manifest(spec))]


def cluster_creates() -> list[list[str]]:
    """The gcloud argv lists bench.gke runs to create a bench cluster and its node pools (create_commands)."""
    spec = gke.default_spec(gke.BENCH_NAME_PREFIX + "apis")
    # (indexed, not compared to an argv literal: a literal here would read as a call of this module's own to
    # vm606's plan-iam scan and to `required`)
    return [c for c in gke.create_commands(spec) if c[1:2] == ["container"] and c[3:4] == ["create"]]


def required(bench_dir: Path = BENCH_DIR, manifests=None, creates=None) -> dict:
    """{"apis": {api: [who: what (where)]}, "unmapped": [text]}: every Google API the run calls (module doc)."""
    apis: dict[str, set[str]] = {}
    unmapped: list[str] = []

    def add(api: str | None, who: str, what: str) -> None:
        if api:
            apis.setdefault(api, set()).add(f"{who}: {what}")

    def group(g: str, who: str, where: str) -> None:
        if g not in GCLOUD_API:
            unmapped.append(f"gcloud {g} ({where}): no API mapped (GCLOUD_API)")
        else:
            add(GCLOUD_API[g], who, f"gcloud {g} ({where})")

    for f in sorted((bench_dir / "bench").glob("*.py")):
        src = f.read_text()
        rel = f.relative_to(bench_dir)
        who = LAPTOP if f.name in OPERATOR_SIDE else ORCHESTRATOR
        for rx in (_ARGV, _WRAPPED):
            for m in rx.finditer(src):
                group(m.group(1), who, f"{rel}:{_line(src, m.start())}")
        # an argv list built for a gcloud wrapper (bench.vm606 _binding_argv, bench.gcevm ssh_argv / get / list_json
        # calls): a list literal whose first element is a group this map knows, followed by a string
        known = "|".join(re.escape(g) for g in GCLOUD_API)
        for m in re.finditer(r'\[\s*"(' + known + r')"\s*,\s*f?["\']', src):
            group(m.group(1), who, f"{rel}:{_line(src, m.start())}")
        for flag, api in FLAG_API.items():
            # as an argv element ("--flag", ...) or a setting's value ("key=VALUE"), never a mapping's key
            for m in re.finditer(r'["\']' + re.escape(flag) + r'(?:=|["\']\s*[,\]])', src):
                add(api, who, f"{flag} ({rel}:{_line(src, m.start())})")
        for m in _REST.finditer(src):
            host = m.group(1)
            where = f"{rel}:{_line(src, m.start())}"
            named = _NAMED.match(src[src.rfind("\n", 0, m.start()) + 1:m.start()])
            caller = NODE_SA if named and NODE_SA_REST.get(named.group(1)) == (f.name, host) else who
            add(_rest_api(host, m.group(2)), caller, f"REST {host}.googleapis.com ({where})")
    for name, (fname, host) in NODE_SA_REST.items():
        add(f"{host}.googleapis.com", NODE_SA, f"REST {name} (bench/{fname})")
    for what, text in (node_sa_manifests() if manifests is None else manifests):
        for m in _SCRIPT.finditer(text):
            group(m.group(1), NODE_SA, what)
        for m in _REST.finditer(text):
            add(_rest_api(m.group(1), m.group(2)), NODE_SA, f"REST {m.group(1)}.googleapis.com ({what})")
    for argv in (cluster_creates() if creates is None else creates):
        for i, a in enumerate(argv):
            flag, eq, val = a.partition("=")
            if flag in CLUSTER_FLAG_API:
                val = val if eq else (argv[i + 1] if i + 1 < len(argv) else "")
                if val.upper() != "NONE":
                    add(CLUSTER_FLAG_API[flag], NODE_SA,
                        f"{' '.join(argv[1:4])} {flag} {val} (bench.gke.create_commands)")
    return {"apis": {a: sorted(w) for a, w in sorted(apis.items())}, "unmapped": sorted(set(unmapped))}


def services_list_argv(project: str) -> list[str]:
    return ["gcloud", "services", "list", "--enabled", "--project", project, "--format", "json"]


def _gcloud_read(argv: list[str]) -> tuple[int, str, str]:
    """One read-only gcloud call within SERVICES_LIST_BOUND_S: (rc, stdout, stderr), with the partial-error
    property false (a list whose request failed exits non-zero, not 0 with an empty answer)."""
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=SERVICES_LIST_BOUND_S,
                           env=dict(os.environ, **gke.COMPUTE_LIST_ENV))
    except subprocess.TimeoutExpired:
        return 124, "", f"no answer within its {SERVICES_LIST_BOUND_S:g} s bound"
    except OSError as e:
        return 127, "", f"gcloud could not run: {e.strerror}"
    return p.returncode, p.stdout, p.stderr


def enabled(project: str, run=_gcloud_read) -> set[str]:
    """The APIs ENABLED in `project`, from one `gcloud services list --enabled`. Refused unless the answer is a
    non-empty JSON list naming at least one ENABLED service: a failed or empty read never reads as "every API
    enabled" nor as "none disabled"."""
    rc, out, err = run(services_list_argv(project))
    try:
        got = json.loads(out) if rc == 0 else None
    except ValueError:
        got = None
    names = set()
    for x in got if isinstance(got, list) else []:
        if isinstance(x, dict) and x.get("state") == "ENABLED" and isinstance(x.get("config"), dict) \
                and isinstance(x["config"].get("name"), str):
            names.add(x["config"]["name"])
    if not names:
        why = (f"rc={rc}; {error_summary(err)}" if rc != 0 else
               "its answer is not a JSON list" if not isinstance(got, list) else
               "it answered no enabled service (an empty answer is a failed read, never 'nothing disabled')")
        raise Refused(f"which APIs are enabled in {project} could not be read ({why}): whether the run's Google APIs "
                      f"are enabled is unknown")
    return names


def _callers(uses: list[str], limit: int = 3) -> str:
    shown = "; ".join(uses[:limit])
    return shown + (f"; and {len(uses) - limit} more" if len(uses) > limit else "")


def check(project: str, *, run=_gcloud_read, dry: bool = False, bench_dir: Path = BENCH_DIR,
          manifests=None) -> list[str]:
    """Every API the run calls (required) ENABLED in `project` (enabled): the lines saying so, or Refused naming
    each one that is not, who calls it and where. `dry`: the derivation only, no GCP call."""
    need = required(bench_dir, manifests)
    if need["unmapped"]:
        raise Refused("the code makes gcloud calls whose API is not known, so the run's APIs cannot all be checked: "
                      + "; ".join(need["unmapped"]))
    apis = need["apis"]
    if dry:
        return [f"DRY: {' '.join(services_list_argv(project))}: each of the {len(apis)} Google APIs the run calls "
                f"must be enabled in {project}: {', '.join(apis)}"]
    on = enabled(project, run)
    off = [a for a in apis if a not in on]
    if off:
        raise Refused(f"{len(off)} of the {len(apis)} Google APIs the run calls {'is' if len(off) == 1 else 'are'} "
                      f"not enabled in {project}, so the run would fail when it first calls "
                      f"{'it' if len(off) == 1 else 'them'}: "
                      + " | ".join(f"{a} is not enabled (called by {_callers(apis[a])})" for a in off)
                      + f". Nothing was created. Enable {'it' if len(off) == 1 else 'them'} "
                      f"(gcloud services enable {' '.join(off)} --project {project}), then run again")
    return [f"Google APIs: each of the {len(apis)} the run calls is enabled in {project}: {', '.join(apis)}"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.gcp_apis")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("required", help="every Google API the run calls, and who calls it where")
    c = sub.add_parser("check", help="every Google API the run calls, enabled in the project")
    c.add_argument("--project", required=True)
    c.add_argument("--dry", action="store_true", help="the derivation only: no GCP call")
    a = ap.parse_args(argv)
    if a.cmd == "required":
        need = required()
        for api, uses in need["apis"].items():
            print(api)
            for u in uses:
                print(f"  {u}")
        for u in need["unmapped"]:
            print(f"UNMAPPED: {u}")
        return 1 if need["unmapped"] else 0
    try:
        lines = check(a.project, dry=a.dry)
    except Refused as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    for ln in lines:
        print(ln, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
