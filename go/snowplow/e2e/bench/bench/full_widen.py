"""#1004 — #870's full-widen worst case as its own stage of the paired run: a
CRD schema change that alters EVERY object's served bytes at once.

WHAT IT MEASURES. A CRD schema change makes snowplow rebuild that GVR's store:
on 1.12.59 the informer is torn down and relisted (relist), and from 1.12.60
the replacement is built beside the live copy and swapped in (#870,
build_and_swap; two full copies of the GVR coexist until the swap). The stage
creates its own CRD and N objects the size of the run's compositions, makes
snowplow register and serve the GVR, widens the CRD with a DEFAULTED property
(the apiserver applies a default on read, so every object's served bytes
change), and records, across the rebuild:
  - the heap and GC peak (/debug/vars memstats, one read a SAMPLE_S);
  - no whole-type mark (build_and_swap: unanchors[type_event] 0);
  - the build's window ending (the end signal below) and no abort;
  - on relist builds, relist_bridge_timeout_total;
read with accept1126's B1-B7 rows (one definition), by the mechanism the build
PUBLISHES (bench/store_rebuild.mechanism), never by version.

WHERE IT RUNS (run606.sh s_full_widen). After the evaluate step, so the
run's verdict is final and nothing of the stage reaches a measured cell (the
store-rebuild watches are process-wide), and before teardown, on both arms of
a pair. Its one switch is Diego's go, BENCH_1004_GO=<YYYYMMDD>-<RUN_NAME>
(run606_pair.sh: BENCH_1004_GO_A / _B, both or neither). Not a dedicated run:
the question is the rebuild's peak ON TOP of snowplow's working set at the
run's scale, which only a cluster holding the run's 50K compositions, booted
and driven, has; a dedicated run would pay the install, the data and the boot
again for the same state.

WHAT IT CREATES (all removed by `cleanup`, the cluster's deletion the backstop):
  - CRD fullwidens.fullwiden.bench.krateo.io (spec: preserve-unknown-fields;
    widened: + spec.benchWidened, type string, default "widened");
  - N objects, N = the compositions the data step PROVED (data-proof.json),
    N / NAMESPACES in each bench-ns-NN, each one's body composition_yaml's
    for that namespace and name (data_manifests: the bytes of the composition
    beside it), applied in batches of COMPOSITION_APPLY_BATCH,
    COMPOSITION_APPLY_WORKERS at a time, as the data step applies them;
  - RESTAction bench-fullwiden-probe (bench-ns-01): one stage, a GET of ONE
    stage object (the stage's read: it makes snowplow register the GVR through
    the informer dispatch, and its objects are small);
  - its own identity (authn User + password Secret, group bench-fullwiden)
    and a ClusterRole + binding: get/list/watch on the stage's objects, get on
    the probe RESTAction; nothing else;
  - the fullwiden606 Job (bench-loadgen, loadgen pool) and its ConfigMap
    (this file, load606.py, store_rebuild.py).

THE READ. GET /call on the probe RESTAction with extras {"bench1004_read": n},
n new on every read: the extras fold into the RESTAction's L1 key
(cache.ComputeKey), so no read is an RA-level hit. The api stage's CONTENT cell
is keyed by (gvr, namespace, name) and is shared across reads, so a read does
NOT always dispatch the stage's GET: once the cell is filled (only after an
informer serve) a read can be served from it. During a relist the cell is
dirty-marked, not evicted, and served until the refresher's failed refill makes
it known-stale, from where reads fall through (#547's known-stale
read-through): the relist end's "serves again" relies on that read-through.
A read "moved" a fall-through cell when snowplow_apiserver_fallthrough_cells
for the stage's GVR changed between the /debug/vars read just before it and
the one just after.

THE PHASES (in the Job, `watch`), each bounded:
  register  read until the GVR is registered (snowplow_plurals_registered_gvrs)
            and one read returned 200 and moved NONE of the GVR's fall-through
            cells: the live copy serves (registration alone could be a copy
            still listing; #1004 ruling Q4). A cold LIST of N objects: bounded
            by ROLLOUT_TIMEOUT_S (the register bound). Then the snapshot the
            deltas start from, and /out/armed.
  armed     the operator applies the widened CRD, then writes /out/widened;
            bounded by the widen wait (the operator's poll, its apply, its exec).
  settle    from the moment /out/widened is seen (the bound's clock starts at
            the widen, so a post-sync timeout is named as itself), one read +
            /debug/vars a SAMPLE_S until the build's END (rulings Q1/Q2):
              build_and_swap  the stage GVR's own outcome in
                              snowplow_store_swaps_by_gvr moved by >= 1 since
                              armed, and known_stale_keys_open[schema] and
                              known_stale_window_open_seconds_max[schema] are 0;
              relist          Δ(relist_dirtymark_postsync_total +
                              relist_postsync_timeout_total) >= Δschema_relists_fired
                              >= 1, and the latest read returned 200 and moved
                              none of the GVR's not-synced / not-servable cells
                              (servable again, as a customer sees it).
            Bound reached first: recorded as the outcome, never a pass (Q3).
THE BOUNDS (Q3, `bounds`): relist = max(relistPostSyncWait, the live snowplow
Deployment's DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS as its container sees it (env,
valueFrom or envFrom: the chart sets it through envFrom configMapRef; read
with origin_gate._container_env), else its default) + one sample: snowplow's
own give-ups, past which its end cannot come.
build_and_swap has no snowplow bound (every build ends on an event,
store_swap.go): ROLLOUT_TIMEOUT_S, the bench's bound for snowplow listing and
syncing everything it serves at boot; the stage's copy is one GVR of the
run's composition count.

ATTRIBUTION (Q2). snowplow_crd_discovery's counters are process-wide: the rows
are the stage's only when every schema relist between armed and the end was
the stage's CRD's. Both tags log cache.crd_discovery.schema_relist with the
CRD's name; the stage's scrubbed snowplow log (logscrub hashes the name) is
read for it, and the result is refused (FAIL, never PASS) when another CRD's
relist landed in the window, when the log names fewer of the stage's own than
Δschema_relists_fired, or when the log was not read.

STDLIB ONLY at import: the Job imports this module with load606.py and
store_rebuild.py beside it (bench/ in its ConfigMap). The renderers that need
other bench modules import them where they run (the operator side).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.parse
from typing import Callable

GROUP = "fullwiden.bench.krateo.io"
VERSION = "v1alpha1"
PLURAL = "fullwidens"
KIND = "FullWiden"
CRD_NAME = f"{PLURAL}.{GROUP}"
GVR = f"{GROUP}/{VERSION}, Resource={PLURAL}"     # schema.GroupVersionResource.String()
GVR_REGISTERED = f"{GROUP}/{VERSION}/{PLURAL}"    # snowplow_plurals_registered_gvrs' form
WIDEN_FIELD = "benchWidened"
WIDEN_DEFAULT = "widened"
OBJECT_PREFIX = "bench-fullwiden-"
LABEL = "bench.krateo.io/full-widen"
LABELS = {LABEL: "true", "app": "scaletest"}
IDENTITY_GROUP = "bench-fullwiden"
AUTHN_NS = "krateo-system"           # where authn's Users live (data_manifests.AUTHN_NS)
RA_API = "templates.krateo.io/v1"
RA_NAME = "bench-fullwiden-probe"
RA_NS = "bench-ns-01"
RBAC_NAME = "bench-fullwiden-reader"
LOADGEN_NS = "bench-loadgen"
JOB = "fullwiden606"
CONFIGMAP = "fullwiden606"
SRC_DIR = "/opt/fullwiden"
STREAM = "/out/widen.jsonl"
ARMED = "/out/armed"
EXITED = "/out/exited"
WIDENED = "/out/widened"     # written by the operator after the widened CRD is applied
# The Job's read cadence: accept1126's settle (one /debug/vars a second). #918
# measured a 20-35 s rebuild window at 50K: 15 s (mem606's) would see one or two
# samples of it and could miss the heap peak.
SAMPLE_S = 1.0
# 1.12.59 crd_discovery_side_effect.go relistPostSyncWait (a const): the post-sync
# re-fire gives up after it (relist_postsync_timeout_total).
RELIST_POSTSYNC_WAIT_S = 120
# 1.12.59 relist_bridge.go: the bridge's sync wait, from the snowplow env
# (intFromEnv: unset, unparseable or <= 0 read as the default)
RELIST_BRIDGE_ENV = "DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS"
RELIST_BRIDGE_DEFAULT_S = 120
# the /debug/vars families the Job keeps per sample (bench/store_rebuild.py's names)
CRD_F = "snowplow_crd_discovery"
SWAP_F = "snowplow_store_swap"
SWAP_KS_F = "snowplow_store_swap_known_stale"
SWAPS_BY_GVR_F = "snowplow_store_swaps_by_gvr"
VERIFY_F = "snowplow_store_verification"
LIST_INCREMENTAL_F = "snowplow_list_incremental"
FALLTHROUGH_F = "snowplow_apiserver_fallthrough_cells"
REGISTERED_F = "snowplow_plurals_registered_gvrs"
KEPT_FAMILIES = (CRD_F, SWAP_F, SWAP_KS_F, VERIFY_F, LIST_INCREMENTAL_F)
# the fall-through reasons a rebuild window causes (store_rebuild.WINDOW_REASONS)
WINDOW_REASONS = ("informer-fallthrough-not-synced", "informer-fallthrough-not-servable")
MEM_FIELDS = ("HeapInuse", "HeapAlloc", "HeapSys", "Sys", "NumGC", "PauseTotalNs", "GCCPUFraction")

MECHANISMS = ("relist", "build_and_swap")


# ─── what the stage creates (operator side, offline) ─────────────────────────

def object_name(ns_i: int, j: int) -> str:
    return f"{OBJECT_PREFIX}{ns_i:02d}-{j:05d}"


def per_namespace(n: int, namespaces: int) -> int:
    """The objects in each bench namespace; N must split evenly, as the data
    step's compositions do (data606.generate)."""
    if n < 1 or namespaces < 1 or n % namespaces:
        raise ValueError(f"the stage's N ({n}) must be >= 1 and split evenly across {namespaces} namespaces")
    return n // namespaces


def object_yaml(ns: str, name: str) -> str:
    """One stage object: the data step's composition for (ns, name), verbatim
    but its apiVersion and kind (data_manifests.composition_yaml)."""
    from bench import data_manifests
    comp = data_manifests.composition_yaml(ns, name)
    head = (f"apiVersion: {data_manifests.COMP_GVR}/{data_manifests.COMP_VERSION}\n"
            "kind: GithubScaffoldingWithCompositionPage\n")
    if not comp.startswith(head):
        raise ValueError("data_manifests.composition_yaml no longer starts with its apiVersion and kind: "
                         "the stage cannot take its body verbatim")
    return f"apiVersion: {GROUP}/{VERSION}\nkind: {KIND}\n" + comp[len(head):]


def batches(n: int, namespaces: int, batch: int) -> list[tuple[str, list[tuple[str, str]]]]:
    """[(label, [(ns, name)])]: at most `batch` objects of one namespace per apply,
    as the data step batches its compositions (lifecycle.deploy_compositions_parallel)."""
    per = per_namespace(n, namespaces)
    out = []
    for ns_i in range(1, namespaces + 1):
        ns = f"bench-ns-{ns_i:02d}"
        for start in range(1, per + 1, batch):
            end = min(start + batch - 1, per)
            out.append((f"{ns}[{start}-{end}]", [(ns, object_name(ns_i, j)) for j in range(start, end + 1)]))
    return out


def crd(widened: bool) -> dict:
    """The stage's CRD. The only difference between its two forms: the widened
    one declares spec.benchWidened with a DEFAULT, which the apiserver applies
    when it reads an object, so every existing object's served bytes change
    (accept1126's B2 widen); the structural schema (snowplow's fingerprint)
    changes with it."""
    spec = {"type": "object", "x-kubernetes-preserve-unknown-fields": True}
    if widened:
        spec["properties"] = {WIDEN_FIELD: {"type": "string", "default": WIDEN_DEFAULT}}
    return {"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
            "metadata": {"name": CRD_NAME, "labels": dict(LABELS)},
            "spec": {"group": GROUP, "scope": "Namespaced",
                     "names": {"plural": PLURAL, "singular": PLURAL[:-1], "kind": KIND,
                               "listKind": KIND + "List"},
                     "versions": [{"name": VERSION, "served": True, "storage": True,
                                   # apiVersion / kind / metadata declared as controller-gen does
                                   "schema": {"openAPIV3Schema": {"type": "object", "properties": {
                                       "apiVersion": {"type": "string"}, "kind": {"type": "string"},
                                       "metadata": {"type": "object"}, "spec": spec}}}}]}}


def probe_ra(first: tuple[str, str]) -> dict:
    """The stage's read: one RESTAction stage that GETs ONE stage object (its
    name projected), resolved through the informer dispatch, which registers
    the GVR on its first read and serves it once the copy is synced."""
    ns, name = first
    return {"apiVersion": RA_API, "kind": "RESTAction",
            "metadata": {"name": RA_NAME, "namespace": RA_NS, "labels": dict(LABELS)},
            "spec": {"api": [{"name": "probe",
                              "path": f"/apis/{GROUP}/{VERSION}/namespaces/{ns}/{PLURAL}/{name}",
                              "filter": "{name: .probe.metadata.name}"}]}}


def identity_docs(user: str, password: str) -> list[dict]:
    """The stage's own login: an authn User in its own group (the ONE authn
    user rendering, bench/authn_user.py), and a ClusterRole + binding that
    let that group read the stage's objects and the probe RESTAction."""
    from bench import authn_user
    rules = [{"apiGroups": [GROUP], "resources": [PLURAL], "verbs": ["get", "list", "watch"]},
             {"apiGroups": [RA_API.split("/")[0]], "resources": ["restactions"], "verbs": ["get"],
              "resourceNames": [RA_NAME]}]
    meta = {"name": RBAC_NAME, "labels": dict(LABELS)}
    return [authn_user.password_secret(user, AUTHN_NS, password, LABELS),
            authn_user.user(user, AUTHN_NS, display_name="Full widen (#1004)", groups=[IDENTITY_GROUP],
                            avatar=authn_user.avatar_url(0), labels=LABELS),
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": dict(meta),
             "rules": rules},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": dict(meta),
             "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": RBAC_NAME},
             "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": IDENTITY_GROUP}]}]


def job_yaml(*, snowplow_url: str, authn_url: str, user: str, password: str, bounds: dict,
             linger_s: float, widen_wait_s: float, sample_s: float = SAMPLE_S) -> str:
    """The fullwiden606 Job and its ConfigMap (this file, load606.py and
    store_rebuild.py under bench/, so `python3 -m bench.full_widen` imports
    them as on the host). Framed like mem606: the wrapper appends its exit
    record, writes /out/exited and lingers until the collect copies /out."""
    from pathlib import Path
    from bench.load606_job import IMAGE, _indent
    here = Path(__file__).parent
    files = {"full_widen.py": Path(__file__), "load606.py": here / "load606.py",
             "store_rebuild.py": here / "store_rebuild.py"}
    data = "".join(f"  {k}: |\n{_indent(p.read_text(), 4)}\n" for k, p in files.items())
    items = "".join(f"          - key: {k}\n            path: bench/{k}\n" for k in files)
    script = f"""set -eu
rc=0
cd {SRC_DIR}
python3 -u -m bench.full_widen watch \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --user {json.dumps(user)} --password {json.dumps(password)} \\
  --bounds {json.dumps(json.dumps(bounds, sort_keys=True))} --sample-s {sample_s:g} \\
  --widen-wait-s {widen_wait_s:g} || rc=$?
printf '{{"kind": "exit", "rc": %d, "t": %d}}\\n' "$rc" "$(date +%s)" >> {STREAM}
echo "$rc" > {EXITED}
sleep {linger_s:g}
exit "$rc"
"""
    return f"""\
apiVersion: v1
kind: ConfigMap
metadata:
  name: {CONFIGMAP}
  namespace: {LOADGEN_NS}
  labels:
    {LABEL}: "true"
    app: scaletest
data:
{data}---
apiVersion: batch/v1
kind: Job
metadata:
  name: {JOB}
  namespace: {LOADGEN_NS}
  labels:
    {LABEL}: "true"
    app: scaletest
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: {JOB}
    spec:
      restartPolicy: Never
      nodeSelector:
        bench.krateo.io/pool: loadgen
      tolerations:
      - key: dedicated
        operator: Equal
        value: loadgen
        effect: NoSchedule
      containers:
      - name: widen
        image: {IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(script, 10)}
        volumeMounts:
        - name: src
          mountPath: {SRC_DIR}
        - name: out
          mountPath: /out
      volumes:
      - name: src
        configMap:
          name: {CONFIGMAP}
          items:
{items}      - name: out
        emptyDir: {{}}
"""


def documents(namespaces: int, user_prefix: str) -> list[tuple[str, dict]]:
    """What the stage applies of a kind that is not built in, for
    bench/crd_schema.py: one object per namespace (they differ only in name
    and namespace, as the compositions do), the probe RESTAction, the User."""
    import yaml
    from bench.load606 import SCALEUSER_PASSWORD
    out = [("the full-widen stage's objects", yaml.safe_load(object_yaml(f"bench-ns-{i:02d}", object_name(i, 1))))
           for i in range(1, namespaces + 1)]
    out.append(("the full-widen stage's read", probe_ra(("bench-ns-01", object_name(1, 1)))))
    out += [("the full-widen stage's identity", d) for d in identity_docs(identity_user(user_prefix),
                                                                          SCALEUSER_PASSWORD)
            if d["kind"] == "User"]
    return out


def crd_source() -> dict:
    """The stage's own CRD as a bench/crd_schema.py source (its objects are
    created under the narrow form)."""
    return {"component": "the full-widen stage (#1004)", "chart": "bench/full_widen.py", "version": "narrow",
            "from": "bench", "crds": [crd(False)], "templated_unread": []}


def identity_user(user_prefix: str) -> str:
    return f"{user_prefix}fullwiden"


def plan(*, n: int, scale: int, namespaces: int, out_dir: str, user_prefix: str, password: str,
         snowplow_url: str, authn_url: str, bounds: dict, linger_s: float, widen_wait_s: float) -> dict:
    """Write every manifest of the stage into out_dir and return its plan.
    N is the data step's proven count, and must be the SCALE the run budgeted."""
    from bench import data_manifests
    if n != scale:
        raise ValueError(f"the data step proved {n} compositions, the run budgeted SCALE={scale}: "
                         f"the stage's N is what the run installs, and its bounds were derived from SCALE")
    os.makedirs(out_dir, exist_ok=True)
    bs = batches(n, namespaces, data_manifests.COMPOSITION_APPLY_BATCH)
    files = []
    for k, (label, objs) in enumerate(bs, 1):
        f = os.path.join(out_dir, f"objects-{k:04d}.yaml")
        with open(f, "w") as fh:
            fh.write("\n---\n".join(object_yaml(ns, name) for ns, name in objs) + "\n")
        files.append(f)
    first = bs[0][1][0]
    sample = object_yaml(*first)
    user = identity_user(user_prefix)

    def write(name, text):
        with open(os.path.join(out_dir, name), "w") as fh:
            fh.write(text)
    write("crd.yaml", json.dumps(crd(False)) + "\n")
    write("crd-widened.yaml", json.dumps(crd(True)) + "\n")
    write("probe.yaml", "\n---\n".join(json.dumps(d) for d in [probe_ra(first)] + identity_docs(user, password))
          + "\n")
    write("job.yaml", job_yaml(snowplow_url=snowplow_url, authn_url=authn_url, user=user, password=password,
                               bounds=bounds, linger_s=linger_s, widen_wait_s=widen_wait_s))
    p = {"kind": "full_widen_plan", "n": n, "namespaces": namespaces, "per_namespace": n // namespaces,
         "batches": len(bs), "batch": data_manifests.COMPOSITION_APPLY_BATCH,
         "workers": data_manifests.COMPOSITION_APPLY_WORKERS,
         "retry_backoffs_s": list(data_manifests.APPLY_RETRY_BACKOFFS_S),
         "object_bytes": len(sample.encode()), "composition_bytes": len(
             data_manifests.composition_yaml(*first).encode()),
         "crd": CRD_NAME, "gvr": GVR, "first": list(first), "bounds": bounds, "files": files,
         "derived_from": {"n": "data-proof.json compositions (the data step's count)",
                          "body": "data_manifests.composition_yaml, verbatim but apiVersion/kind",
                          "batching": "data_manifests COMPOSITION_APPLY_BATCH / _WORKERS / APPLY_RETRY_BACKOFFS_S "
                                      "(the data step's)"}}
    with open(os.path.join(out_dir, "plan.json"), "w") as fh:
        json.dump(p, fh, indent=1)
    return p


def cleanup_targets(user_prefix: str) -> list[list[str]]:
    """kubectl argument lists, in order: the Job and its ConfigMap, the probe,
    the identity (by label, and the clientconfig Secret authn wrote at its
    login, by name), the RBAC, and last the CRD: its deletion removes the N
    objects (the batch delete), so it is the one waited for (by the caller)."""
    sel = f"{LABEL}=true"
    return [["-n", LOADGEN_NS, "delete", "--ignore-not-found", "--wait=false", "job,configmap", "-l", sel],
            ["-n", RA_NS, "delete", "--ignore-not-found", "restactions.templates.krateo.io", RA_NAME],
            ["-n", AUTHN_NS, "delete", "--ignore-not-found", "users.basic.authn.krateo.io", "-l", sel],
            ["-n", AUTHN_NS, "delete", "--ignore-not-found", "secret", "-l", sel],
            ["-n", AUTHN_NS, "delete", "--ignore-not-found", "secret", f"{identity_user(user_prefix)}-clientconfig"],
            ["delete", "--ignore-not-found", "clusterrolebinding,clusterrole", "-l", sel]]


# ─── the bounds (ruling Q3) ──────────────────────────────────────────────────

def _env_seconds(raw) -> int | None:
    """snowplow's intFromEnv: unset, unparseable or <= 0 read as the default."""
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def snowplow_container(deployment: dict) -> dict:
    """The snowplow container of the snowplow Deployment: the one named as the
    Deployment (the chart names it `snowplow.fullname`, as the Deployment).
    Refused (ValueError) when there is none."""
    name = (deployment.get("metadata") or {}).get("name")
    cs = ((deployment.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers") or []
    hit = [c for c in cs if c.get("name") == name]
    if len(hit) != 1:
        raise ValueError(f"the snowplow Deployment {name!r} has {len(hit)} container(s) named {name!r} "
                         f"(containers: {[c.get('name') for c in cs]}): whose env is snowplow's is not known")
    return hit[0]


def bounds(deployment: dict, configmaps: dict, rollout_timeout_s: float, sample_s: float = SAMPLE_S) -> dict:
    """Each phase's bound and where it comes from. `deployment`: the live
    snowplow Deployment; `configmaps`: its namespace's ConfigMaps (both
    kubectl -o json). The bridge's timeout is read as the snowplow container
    sees it (origin_gate._container_env: env, valueFrom, envFrom). Refused
    (ValueError) when it cannot be read: never a default in its place."""
    from bench import origin_gate
    c = snowplow_container(deployment)
    cms = {(m.get("metadata") or {}).get("name"): m.get("data") or {} for m in configmaps.get("items") or []}
    try:
        raw, where = origin_gate._container_env(c, cms, RELIST_BRIDGE_ENV)
    except LookupError as e:
        raise ValueError(f"{RELIST_BRIDGE_ENV} of the snowplow container cannot be read: {e}") from None
    bridge = _env_seconds(raw)
    if raw is None:
        bridge_src = f"{RELIST_BRIDGE_ENV} not set for the snowplow container: its default {RELIST_BRIDGE_DEFAULT_S} s"
    elif bridge is None:
        bridge_src = (f"{RELIST_BRIDGE_ENV}={raw!r} ({where}) unparseable or <= 0: its default "
                      f"{RELIST_BRIDGE_DEFAULT_S} s (intFromEnv)")
    else:
        bridge_src = f"{RELIST_BRIDGE_ENV}={raw} ({where})"
    bridge = bridge if bridge is not None else RELIST_BRIDGE_DEFAULT_S
    relist = max(RELIST_POSTSYNC_WAIT_S, bridge) + sample_s
    rollout = (f"ROLLOUT_TIMEOUT_S {rollout_timeout_s:g} s, the bench's bound for snowplow listing and syncing "
               f"everything it serves at boot")
    return {"relist": {"s": relist, "source": (f"max(relistPostSyncWait {RELIST_POSTSYNC_WAIT_S} s, the bridge's "
                                               f"{bridge} s: {bridge_src}) + one {sample_s:g} s sample: "
                                               f"snowplow's own give-ups")},
            "build_and_swap": {"s": float(rollout_timeout_s),
                               "source": f"{rollout}: snowplow publishes no bound (every build ends on an event)"},
            "register": {"s": float(rollout_timeout_s), "source": f"{rollout}: the register read waits for a cold "
                                                                  f"LIST of the stage's objects"}}


# ─── the end rules (rulings Q1, Q2, Q4) ──────────────────────────────────────

def _fam(v: dict, family: str, stat: str, label=None):
    f = v.get(family)
    if not isinstance(f, dict) or stat not in f:
        return None
    x = f[stat]
    if label is not None:
        x = x.get(label) if isinstance(x, dict) else None
    return x if isinstance(x, (int, float)) and not isinstance(x, bool) else None


def gvr_cells(v: dict) -> dict | None:
    """The stage GVR's fall-through cells, {"path|reason": n}; None when the
    build does not publish the meter (never an empty reading)."""
    cells = v.get(FALLTHROUGH_F)
    if not isinstance(cells, dict):
        return None
    out = {}
    for cell, n in cells.items():
        parts = str(cell).split("|")
        if len(parts) == 3 and parts[1] == GVR and isinstance(n, (int, float)):
            out[f"{parts[0]}|{parts[2]}"] = n
    return out


def cells_moved(before: dict | None, after: dict | None, reasons=None) -> dict | None:
    """{"path|reason": Δ} for the cells that moved between two reads (only
    `reasons`, when given); None when either read lacks the meter."""
    if before is None or after is None:
        return None
    out = {}
    for k, n in after.items():
        if reasons is not None and k.split("|", 1)[1] not in reasons:
            continue
        d = n - before.get(k, 0)
        if d:
            out[k] = d
    return out


def registered(v: dict) -> bool | None:
    r = v.get(REGISTERED_F)
    if not isinstance(r, dict) or not isinstance(r.get("gvrs"), list):
        return None
    return GVR_REGISTERED in r["gvrs"]


def serves(probe: dict, v: dict) -> tuple[bool, str]:
    """Ruling Q4: the GVR is registered and the read returned 200 and moved
    none of its fall-through cells (any reason)."""
    reg = registered(v)
    if reg is None:
        return False, f"{REGISTERED_F} not published"
    if not reg:
        return False, "the GVR is not registered yet"
    if probe.get("status") != 200:
        return False, f"the read answered {probe.get('status')}"
    if probe.get("moved") is None:
        return False, f"{FALLTHROUGH_F} not published: whether the read fell through is unknown"
    if probe["moved"]:
        return False, f"the read fell through: {probe['moved']}"
    return True, "registered, and a read answered 200 with no fall-through"


def swap_outcomes(v: dict, gvr: str = GVR) -> int | None:
    """Every outcome a GVR's builds ended in (installed, superseded, abort_*),
    summed; 0 for a GVR with no build yet; None when unpublished."""
    by = v.get(SWAPS_BY_GVR_F)
    if not isinstance(by, dict):
        return None
    m = by.get(gvr) or {}
    return sum(n for n in m.values() if isinstance(n, (int, float))) if isinstance(m, dict) else None


def build_ended(mech: str, armed: dict, now: dict, probe: dict) -> tuple[bool, str]:
    """Whether the stage's build has ended, by the mechanism's end signal
    (rulings Q1 / Q2): (ended, why). `armed`: the snapshot taken before the
    widen (the deltas' baseline)."""
    if mech == "build_and_swap":
        a, b = swap_outcomes(armed), swap_outcomes(now)
        ko = _fam(now, SWAP_KS_F, "store_swap_known_stale_keys_open", "schema")
        wm = _fam(now, SWAP_KS_F, "store_swap_known_stale_window_open_seconds_max", "schema")
        if a is None or b is None or ko is None or wm is None:
            return False, "the swap's end signal is not published"
        if b - a < 1:
            return False, f"no build of {GVR} has ended since armed ({SWAPS_BY_GVR_F})"
        if ko or wm:
            return False, f"the schema window is open (keys_open {ko}, window_open_seconds_max {wm})"
        return True, (f"{SWAPS_BY_GVR_F}[{GVR}] moved by {b - a}; known_stale_keys_open[schema] 0 and "
                      f"known_stale_window_open_seconds_max[schema] 0")
    if mech == "relist":
        def d(stat):
            x, y = _fam(armed, CRD_F, stat), _fam(now, CRD_F, stat)
            return None if x is None or y is None else y - x
        fired, post, tout = (d("schema_relists_fired"), d("relist_dirtymark_postsync_total"),
                             d("relist_postsync_timeout_total"))
        if fired is None or post is None or tout is None:
            return False, "the relist's end counters are not published"
        if fired < 1:
            return False, "no schema relist fired since armed"
        if post + tout < fired:
            return False, f"the post-sync re-fire has not caught up ({post} + {tout} timeouts < {fired} relists)"
        moved = probe.get("moved_window")
        if probe.get("status") != 200 or moved is None or moved:
            return False, (f"not servable again: the read answered {probe.get('status')}, "
                           f"window fall-throughs {moved}")
        return True, (f"Δrelist_dirtymark_postsync_total {post} + Δrelist_postsync_timeout_total {tout} >= "
                      f"Δschema_relists_fired {fired}, and a read answered 200 with no not-synced / "
                      f"not-servable fall-through")
    return False, f"unknown mechanism {mech!r}"


# ─── the Job (in-cluster) ────────────────────────────────────────────────────

def trim(v: dict, prev_num_gc: int | None) -> dict:
    """One /debug/vars read, kept: memstats' heap and GC fields (and the GC
    pauses since the previous read, load606.gc_pauses), the rebuild families
    whole, every GVR's build outcomes (bounded by the cluster's type set: the
    report names other GVRs' builds), and of the per-GVR fall-through and
    registration maps only the stage GVR's entry (whether a family is
    published is kept: absent is never zero). METADATA ONLY."""
    from bench import load606
    m = v.get("memstats") or {}
    out = {k: v[k] for k in KEPT_FAMILIES if k in v}
    if isinstance(v.get(SWAPS_BY_GVR_F), dict):
        out[SWAPS_BY_GVR_F] = v[SWAPS_BY_GVR_F]
    if isinstance(v.get(FALLTHROUGH_F), dict):
        out[FALLTHROUGH_F] = {c: n for c, n in v[FALLTHROUGH_F].items() if str(c).split("|")[1:2] == [GVR]}
    if isinstance(v.get(REGISTERED_F), dict):
        out[REGISTERED_F] = {"gvrs": [g for g in v[REGISTERED_F].get("gvrs") or [] if g == GVR_REGISTERED]}
    pauses, lost = load606.gc_pauses(m, prev_num_gc)
    out["memstats"] = {k: m.get(k) for k in MEM_FIELDS} | {"pauses_ms": pauses, "pauses_lost": lost}
    return out


def read_path(seq: int) -> str:
    """The stage's read, its extras new on every read (a new RA-level L1 key)."""
    extras = urllib.parse.quote(json.dumps({"bench1004_read": seq}, separators=(",", ":")))
    return (f"/call?apiVersion={urllib.parse.quote(RA_API, safe='')}&resource=restactions"
            f"&name={RA_NAME}&namespace={RA_NS}&extras={extras}")


class Watch:
    """The Job's loop. `get(path, token) -> (status, body bytes)`, `login() ->
    token`, `emit(record)`, `arm()` (writes /out/armed), `widened() -> bool`
    (the operator wrote /out/widened), `clock()`, `sleep(s)` injected (tests
    drive it with a fake snowplow). `bounds`: {"register", "relist",
    "build_and_swap": {"s", "source"}}; `widen_wait_s`: how long armed may wait
    for the operator's widen."""

    def __init__(self, *, get: Callable, login: Callable, emit: Callable, arm: Callable, widened: Callable,
                 bounds: dict, widen_wait_s: float, sample_s: float = SAMPLE_S, clock: Callable = time.time,
                 sleep: Callable = time.sleep, mechanism: Callable | None = None):
        self.get, self.login, self.emit, self.arm, self.widened = get, login, emit, arm, widened
        self.bounds, self.widen_wait_s, self.sample_s = bounds, widen_wait_s, sample_s
        self.clock, self.sleep, self.mechanism = clock, sleep, mechanism
        self.token, self.seq, self.prev_gc = None, 0, None

    def _vars(self) -> dict | None:
        for _ in range(2):
            if self.token is None:
                try:
                    self.token = self.login()
                except Exception as e:      # noqa: BLE001 — a failed login is a sample, retried
                    self.emit({"kind": "login_failed", "t": self.clock(), "error": type(e).__name__})
                    return None
            st, body = self.get("/debug/vars", self.token)
            if st == 401:
                self.token = None
                continue
            if st != 200:
                return None
            try:
                return json.loads(body)
            except ValueError:
                return None
        return None

    def tick(self, phase: str) -> dict | None:
        """One read with a /debug/vars read either side: the sample record."""
        before = self._vars()
        self.seq += 1
        st, _ = self.get(read_path(self.seq), self.token) if self.token else (0, b"")
        after = self._vars()
        if before is None or after is None:
            self.emit({"kind": "sample_failed", "t": self.clock(), "phase": phase, "read": st})
            return None
        a, b = gvr_cells(before), gvr_cells(after)
        rec = {"kind": "sample", "t": self.clock(), "phase": phase,
               "probe": {"seq": self.seq, "status": st, "moved": cells_moved(a, b),
                         "moved_window": cells_moved(a, b, WINDOW_REASONS)},
               "vars": trim(after, self.prev_gc)}
        self.prev_gc = (after.get("memstats") or {}).get("NumGC")
        self.emit(rec)
        return rec

    def _end(self, outcome: str, rc: int, **kw) -> int:
        self.emit({"kind": "end", "t": self.clock(), "outcome": outcome, **kw})
        return rc

    def run(self) -> int:
        from bench import store_rebuild
        mech_of = self.mechanism or store_rebuild.mechanism
        t0 = self.clock()
        self.emit({"kind": "start", "t": t0, "gvr": GVR, "bounds": self.bounds, "sample_s": self.sample_s,
                   "widen_wait_s": self.widen_wait_s})
        mech, rec = None, None
        reg_deadline = t0 + self.bounds["register"]["s"]
        while True:                                   # register (ruling Q4)
            rec = self.tick("register")
            if rec is not None and mech is None:
                mech, why = mech_of(rec["vars"])
                if mech is None:
                    return self._end("mechanism_unknown", 3, why=why)
                self.emit({"kind": "mechanism", "t": self.clock(), "mechanism": mech, "why": why,
                           "bound_s": self.bounds[mech]["s"], "bound_source": self.bounds[mech]["source"]})
            if rec is not None and serves(rec["probe"], rec["vars"])[0]:
                break
            if self.clock() >= reg_deadline:
                return self._end("not_serving_within_bound", 3, bound_s=self.bounds["register"]["s"],
                                 why=serves(rec["probe"], rec["vars"])[1] if rec else "the last read failed")
            self.sleep(self.sample_s)
        armed_vars, t_armed = rec["vars"], self.clock()
        self.emit({"kind": "armed", "t": t_armed, "mechanism": mech, "why": serves(rec["probe"], rec["vars"])[1]})
        self.arm()
        while not self.widened():                     # armed: the operator's widen
            if self.clock() >= t_armed + self.widen_wait_s:
                return self._end("never_widened", 3, waited_s=round(self.clock() - t_armed, 1))
            self.sleep(self.sample_s)
            self.tick("armed")
        t_widened = self.clock()
        self.emit({"kind": "widened", "t": t_widened})
        deadline = t_widened + self.bounds[mech]["s"]
        detected = None
        armed_fired = _fam(armed_vars, CRD_F, "schema_relists_fired")
        while True:                                   # settle (rulings Q1 / Q2), the clock from the widen
            self.sleep(self.sample_s)
            rec = self.tick("settle")
            if rec is not None:
                if mech_of(rec["vars"])[0] != mech:
                    return self._end("mechanism_changed", 3,
                                     why="the build's rebuild mechanism changed: the pod was replaced")
                fired = _fam(rec["vars"], CRD_F, "schema_relists_fired")
                if detected is None and fired is not None and armed_fired is not None and fired > armed_fired:
                    detected = rec["t"]
                    self.emit({"kind": "detected", "t": detected})
                ok, why = build_ended(mech, armed_vars, rec["vars"], rec["probe"])
                if ok:
                    return self._end("ended", 0, signal=why, detected=detected)
            if self.clock() >= deadline:
                return self._end("bound_reached", 0, detected=detected, bound_s=self.bounds[mech]["s"],
                                 why=build_ended(mech, armed_vars, rec["vars"], rec["probe"])[1] if rec
                                 else "the last read failed")


def _cmd_watch(a) -> int:
    from bench import load606
    client = load606.HttpClient(a.snowplow_url)
    os.makedirs(os.path.dirname(STREAM), exist_ok=True)
    fh = open(STREAM, "a")

    def emit(rec):
        fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        fh.flush()

    def get(path, token):
        r = client.get(path, token)
        return r.status, r.body

    def arm():
        with open(ARMED, "w") as f:
            f.write(str(time.time()))
    return Watch(get=get, login=lambda: load606.login(a.authn_url, a.user, a.password), emit=emit, arm=arm,
                 widened=lambda: os.path.exists(WIDENED), bounds=json.loads(a.bounds),
                 widen_wait_s=a.widen_wait_s, sample_s=a.sample_s).run()


# ─── the evaluation (operator side) ──────────────────────────────────────────

HOW = ("A CRD of the stage's own and N objects the size of the run's compositions, created in batches like the "
       "data step's; the GVR registered by the stage's own read (a RESTAction GET of one object, its extras new "
       "on every read so no read is an RA-level L1 hit; the api stage's content cell is shared, and the relist's "
       "'serves again' relies on #547's known-stale read-through) until a read serves; then the CRD widened with "
       "a defaulted property, so every object's served bytes change. In-cluster, one read and two /debug/vars "
       "reads a sample: the heap and GC from memstats; the rebuild's counters by the mechanism the build "
       "publishes, read with accept1126's B1-B7 rows over armed -> end, attributed to the stage's CRD by the "
       "schema_relist lines of snowplow's log; the end and its bound (its clock from the widen) per the #1004 "
       "rulings. After the run's evaluate step: nothing of it reaches the paired verdict.")

SCHEMA_RELIST_MSG = "cache.crd_discovery.schema_relist"
# what names the GVR a rebuild ran for, per tag (both hash the GVR: its String() carries "=")
GVR_LINES = ("cache.crd_discovery.relist_postsync.dirty_marked", "cache.crd_discovery.relist_postsync.timeout",
             "cache.store.swap.build")


def parse(lines) -> list[dict]:
    out = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except ValueError:
            out.append({"kind": "unparseable"})
    return out


def _epoch(ts) -> float | None:
    """An RFC3339(Nano) time as epoch seconds (snowplow's slog `time`, the
    kubelet's `kts`)."""
    import calendar
    import re
    m = re.match(r"^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)$", str(ts or ""))
    if not m:
        return None
    y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
    t = calendar.timegm((y, mo, d, h, mi, se)) + (float(m.group(7)) if m.group(7) else 0.0)
    z = m.group(8)
    if z != "Z":
        sign = 1 if z[0] == "+" else -1
        t -= sign * (int(z[1:3]) * 3600 + int(z[4:6]) * 60)
    return t


def relist_attribution(log_lines: list[str] | None, t0: float, t1: float, slack: float = SAMPLE_S) -> dict | None:
    """Which CRDs' schema relists (and which GVRs' rebuilds) the scrubbed
    snowplow log shows in [t0 - slack, t1 + slack]; None when the log was not
    read. The scrub hashes the CRD's name and the GVR (logscrub._h), so the
    stage's are recognised by their hash and every other one is named by it."""
    if log_lines is None:
        return None
    from bench import logscrub
    k_crd = "k:" + logscrub._h("crd")[2:]
    ours_crd, ours_gvr = logscrub._h(CRD_NAME), logscrub._h(GVR)
    out = {"window": [round(t0 - slack, 3), round(t1 + slack, 3)], "ours": 0, "other_crds": {},
           "rebuilds": {"ours": 0, "other_gvrs": {}}}
    for ln in log_lines:
        try:
            rec = json.loads(ln)
        except ValueError:
            continue
        if not isinstance(rec, dict) or rec.get("msg") not in (SCHEMA_RELIST_MSG,) + GVR_LINES:
            continue
        t = _epoch(rec.get("time")) or _epoch(rec.get("kts"))
        if t is None or not (t0 - slack <= t <= t1 + slack):
            continue
        if rec["msg"] == SCHEMA_RELIST_MSG:
            c = rec.get(k_crd)
            if c == ours_crd:
                out["ours"] += 1
            else:
                out["other_crds"][str(c)] = out["other_crds"].get(str(c), 0) + 1
        else:
            g = rec.get("gvr")
            if g == ours_gvr:
                out["rebuilds"]["ours"] += 1
            else:
                out["rebuilds"]["other_gvrs"][str(g)] = out["rebuilds"]["other_gvrs"].get(str(g), 0) + 1
    return out


def memory_report(samples: list[dict], t_detected: float | None) -> dict:
    """The heap and GC across the settle (armed -> end), from each sample's
    memstats: peak heap against the armed reading, GC cycles, pauses."""
    ms = [s["vars"].get("memstats") or {} for s in samples]
    mb = lambda x: round(x / 1048576, 1) if isinstance(x, (int, float)) else None   # noqa: E731
    heap = [(mb(m.get("HeapInuse")), s["t"]) for m, s in zip(ms, samples) if m.get("HeapInuse") is not None]
    peak = max(heap, default=(None, None))
    pauses = [p for m in ms[1:] for p in (m.get("pauses_ms") or [])]
    first, last = ms[0], ms[-1]

    def d(k):
        return (last[k] - first[k]) if isinstance(last.get(k), (int, float)) and isinstance(first.get(k), (int, float)) \
            else None
    return {"samples": len(ms),
            "heap_inuse_mb_armed": mb(first.get("HeapInuse")), "heap_inuse_mb_peak": peak[0],
            "heap_inuse_mb_peak_over_armed": (round(peak[0] - mb(first["HeapInuse"]), 1)
                                              if peak[0] is not None and first.get("HeapInuse") is not None else None),
            "heap_inuse_peak_s_after_detected": (round(peak[1] - t_detected, 1)
                                                 if peak[1] is not None and t_detected is not None else None),
            "heap_inuse_mb_end": mb(last.get("HeapInuse")),
            "heap_alloc_mb_peak": max((mb(m.get("HeapAlloc")) for m in ms if m.get("HeapAlloc") is not None),
                                      default=None),
            "sys_mb_peak": max((mb(m.get("Sys")) for m in ms if m.get("Sys") is not None), default=None),
            "gc_cycles": d("NumGC"), "gc_pause_max_ms": max(pauses, default=None),
            "gc_pause_total_ms": round(d("PauseTotalNs") / 1e6, 3) if d("PauseTotalNs") is not None else None,
            "gc_pauses_not_seen": sum(m.get("pauses_lost") or 0 for m in ms[1:]),
            "gc_cpu_fraction_armed": first.get("GCCPUFraction"), "gc_cpu_fraction_end": last.get("GCCPUFraction"),
            "note": "GCCPUFraction is a since-start fraction; pauses from memstats' 256-entry ring per sample"}


def restarts(pods: dict) -> dict:
    """{pod: restarts} of the snowplow pods (kubectl get pods -o json)."""
    return {p["metadata"]["name"]: sum(c.get("restartCount", 0) for c in (p.get("status") or {})
                                           .get("containerStatuses") or [])
            for p in pods.get("items") or []}


def _other_builds(armed: dict, end: dict) -> dict:
    """Other GVRs whose build outcomes moved between two samples (report)."""
    a, b = armed.get(SWAPS_BY_GVR_F), end.get(SWAPS_BY_GVR_F)
    if not isinstance(a, dict) or not isinstance(b, dict):
        return {}
    return {g: swap_outcomes(end, g) - (swap_outcomes(armed, g) or 0) for g in b
            if g != GVR and (swap_outcomes(end, g) or 0) - (swap_outcomes(armed, g) or 0)}


def evaluate(records: list[dict], *, plan_: dict, pods_before: dict, pods_after: dict,
             log_lines: list[str] | None) -> dict:
    """The stage's record: outcome, rows, attribution, memory, how; `passed`
    only when the build ENDED inside its bound, every gating row passed, every
    schema relist in the window was the stage's CRD's, and snowplow was
    neither restarted nor replaced."""
    from bench import accept1126
    reasons = []
    kinds = [r.get("kind") for r in records]
    end = next((r for r in records if r.get("kind") == "end"), None)
    mech_rec = next((r for r in records if r.get("kind") == "mechanism"), None)
    widened = next((r for r in records if r.get("kind") == "widened"), None)
    armed_i = kinds.index("armed") if "armed" in kinds else None
    out = {"kind": "full_widen", "how": HOW, "plan": {k: plan_.get(k) for k in (
        "n", "namespaces", "per_namespace", "batches", "batch", "workers", "object_bytes", "composition_bytes",
        "crd", "gvr", "derived_from")},
        "mechanism": mech_rec and mech_rec.get("mechanism"),
        "bound": mech_rec and {"s": mech_rec.get("bound_s"), "source": mech_rec.get("bound_source"),
                               "clock": "from the widen (/out/widened seen)"},
        "outcome": end.get("outcome") if end else "no_end_record", "end": end,
        "exit": next((r for r in records if r.get("kind") == "exit"), None),
        "sample_failures": kinds.count("sample_failed"), "login_failures": kinds.count("login_failed"),
        "snowplow_restarts": {"before": pods_before, "after": pods_after}}
    if end is None:
        reasons.append("the Job's stream has no end record: the stage did not finish")
    elif end.get("outcome") != "ended":
        reasons.append(f"outcome {end.get('outcome')}: {end.get('why') or ''}".rstrip(": "))
    if pods_before != pods_after:
        reasons.append(f"snowplow restarted or was replaced during the stage ({pods_before} -> {pods_after})")
    settle = [r for r in records[armed_i:] if r.get("kind") == "sample"] if armed_i is not None else []
    armed_s = next((r for r in reversed(records[:armed_i]) if r.get("kind") == "sample"), None) \
        if armed_i is not None else None
    if armed_s is not None and settle and out["mechanism"] in MECHANISMS:
        mech = out["mechanism"]
        t_armed, t_end = records[armed_i]["t"], (end or settle[-1])["t"]
        att = relist_attribution(log_lines, t_armed, t_end)
        out["attribution"] = att
        d = None
        try:
            before = accept1126.snapshot_crd_counters(armed_s["vars"], mech)
            after = accept1126.snapshot_crd_counters(settle[-1]["vars"], mech)
            d = accept1126.delta(before, after)
            endr = accept1126.crd_end_readings(settle[-1]["vars"], mech)
            ok, rows = accept1126.evaluate(accept1126.crd_rows(mech, changed=plan_.get("n") or 0), d | endr)
            out |= {"delta": d, "end_readings": endr, "assertions": rows,
                    "failed_rows": [r["id"] for r in rows if not r["passed"] and not r.get("report_only")],
                    "report_only_failed": [r["id"] for r in rows if not r["passed"] and r.get("report_only")]}
            if mech == "relist":
                out["relist_bridge_timeout_total"] = d.get(f"{CRD_F}.relist_bridge_timeout_total")
            rep = accept1126.CRD_REPORT[mech]
            if rep:
                out["report_only"] = rep(d, endr)
            if not ok:
                reasons.append(f"rows failed: {', '.join(out['failed_rows'])}")
        except accept1126.AssertionsFailed as e:
            reasons.append(f"the rows could not be read: {e}")
        fired = (d or {}).get(f"{CRD_F}.schema_relists_fired")
        if att is None:
            reasons.append("snowplow's log was not read: whether another CRD's schema relist landed in the "
                           "window is unknown, so the rows are unattributable")
        else:
            if att["other_crds"]:
                reasons.append(f"another CRD's schema relist landed between armed and the end "
                               f"({att['other_crds']}): the process-wide rows are unattributable")
            if fired is not None and att["ours"] < fired:
                reasons.append(f"Δschema_relists_fired {fired} but snowplow's log names the stage's CRD "
                               f"{att['ours']} time(s): unattributed relist(s)")
        out["other_gvr_builds"] = _other_builds(armed_s["vars"], settle[-1]["vars"])
        fts = {}
        for s in settle:
            for k, n in (s["probe"].get("moved_window") or {}).items():
                fts[k] = fts.get(k, 0) + n
        out["window_fallthroughs_during_settle"] = {"by_path_reason": fts, "reads": len(settle),
                                                    "note": "report only (#870 K1 claims 0 on build_and_swap)"}
        out["memory"] = memory_report([armed_s] + settle, (end or {}).get("detected"))
        det = (end or {}).get("detected")
        out["timeline_s"] = {"armed_to_widened": round(widened["t"] - t_armed, 1) if widened else None,
                             "widened_to_detected": (round(det - widened["t"], 1) if det and widened else None),
                             "widened_to_end": round(end["t"] - widened["t"], 1) if end and widened else None,
                             "detected_to_end": round(end["t"] - det, 1) if end and det else None}
    elif not reasons:
        reasons.append("no settle samples: nothing was measured")
    out["passed"] = not reasons
    out["fail_reasons"] = reasons
    return out


def report_lines(rec: dict) -> list[str]:
    m = rec.get("memory") or {}
    out = [f"full widen: {'PASS' if rec['passed'] else 'FAIL'}; outcome {rec['outcome']}; mechanism "
           f"{rec.get('mechanism')}; N {rec['plan'].get('n')} objects of {rec['plan'].get('object_bytes')} B "
           f"(composition {rec['plan'].get('composition_bytes')} B)"]
    if rec.get("bound"):
        out.append(f"full widen: bound {rec['bound']['s']} s from the widen ({rec['bound']['source']})")
    if rec.get("end") and rec["end"].get("signal"):
        out.append(f"full widen: ended on {rec['end']['signal']}")
    if m:
        out.append(f"full widen: heap_inuse {m['heap_inuse_mb_armed']} MB at armed, peak {m['heap_inuse_mb_peak']} MB "
                   f"(+{m['heap_inuse_mb_peak_over_armed']}), end {m['heap_inuse_mb_end']} MB; GC {m['gc_cycles']} "
                   f"cycles, max pause {m['gc_pause_max_ms']} ms, total {m['gc_pause_total_ms']} ms")
    if rec.get("timeline_s"):
        out.append(f"full widen: timeline {rec['timeline_s']}")
    if "attribution" in rec:
        out.append(f"full widen: schema relists in the window: {rec['attribution']}")
    for r in rec.get("assertions") or []:
        out.append(f"full widen: {r['id']} {'ok' if r['passed'] else 'FAILED'}"
                   f"{' (report only)' if r.get('report_only') else ''}: {r['assertion']}: {r['actual']}")
    if "relist_bridge_timeout_total" in rec:
        out.append(f"full widen: relist_bridge_timeout_total Δ {rec['relist_bridge_timeout_total']}")
    out += [f"full widen FAILED: {r}" for r in rec["fail_reasons"]]
    return out


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.full_widen")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bounds", help="the live snowplow Deployment and its namespace's ConfigMaps -> each bound")
    b.add_argument("--deployment", required=True)
    b.add_argument("--configmaps", required=True)
    b.add_argument("--rollout-timeout-s", type=float, required=True)
    pl = sub.add_parser("plan", help="write the stage's manifests and plan.json")
    for f in ("--n", "--scale", "--namespaces"):
        pl.add_argument(f, type=int, required=True)
    for f in ("--out-dir", "--user-prefix", "--password-file", "--snowplow-url", "--authn-url", "--bounds"):
        pl.add_argument(f, required=True)
    pl.add_argument("--linger-s", type=float, required=True)
    pl.add_argument("--widen-wait-s", type=float, required=True)
    sub.add_parser("proof-count", help="data-proof.json on stdin -> its composition count")
    sub.add_parser("cleanup-count", help="how many cleanup deletes there are (the run budget counts each)")
    cl = sub.add_parser("cleanup", help="kubectl argument lists, one per line")
    cl.add_argument("--user-prefix", required=True)
    sub.add_parser("restarts", help="kubectl get pods -o json on stdin -> {pod: restarts}")
    ev = sub.add_parser("evaluate")
    for f in ("--stream", "--plan", "--pods-before", "--pods-after", "--out", "--log"):
        ev.add_argument(f, required=True)
    w = sub.add_parser("watch", help="in-cluster: the register, armed and settle phases")
    for f in ("--snowplow-url", "--authn-url", "--user", "--password", "--bounds"):
        w.add_argument(f, required=True)
    w.add_argument("--sample-s", type=float, default=SAMPLE_S)
    w.add_argument("--widen-wait-s", type=float, required=True)
    a = ap.parse_args(argv)
    if a.cmd == "watch":
        return _cmd_watch(a)
    if a.cmd == "bounds":
        with open(a.deployment) as fh:
            dep = json.load(fh)
        with open(a.configmaps) as fh:
            cms = json.load(fh)
        try:
            print(json.dumps(bounds(dep, cms, a.rollout_timeout_s)))
        except ValueError as e:
            print(f"refusing: {e}", file=sys.stderr)
            return 2
        return 0
    if a.cmd == "proof-count":
        n = json.load(sys.stdin).get("compositions")
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            print(f"refusing: data-proof.json carries no composition count ({n!r})", file=sys.stderr)
            return 2
        print(n)
        return 0
    if a.cmd == "cleanup-count":
        print(len(cleanup_targets("x")))
        return 0
    if a.cmd == "plan":
        with open(a.password_file) as fh:
            pw = fh.read()
        try:
            p = plan(n=a.n, scale=a.scale, namespaces=a.namespaces, out_dir=a.out_dir, user_prefix=a.user_prefix,
                     password=pw, snowplow_url=a.snowplow_url, authn_url=a.authn_url,
                     bounds=json.loads(a.bounds), linger_s=a.linger_s, widen_wait_s=a.widen_wait_s)
        except ValueError as e:
            print(f"refusing: {e}", file=sys.stderr)
            return 2
        print(f"full widen plan: {p['n']} objects ({p['per_namespace']} in each of {p['namespaces']} namespaces), "
              f"{p['batches']} batches of <= {p['batch']}, {p['workers']} at a time; {p['object_bytes']} B each "
              f"(the composition's {p['composition_bytes']} B)", file=sys.stderr)
        for f in p["files"]:
            print(f)
        return 0
    if a.cmd == "cleanup":
        for args in cleanup_targets(a.user_prefix):
            print(" ".join(args))
        return 0
    if a.cmd == "restarts":
        print(json.dumps(restarts(json.load(sys.stdin)), sort_keys=True))
        return 0
    if a.cmd == "evaluate":
        with open(a.stream) as fh:
            recs = parse(fh)
        with open(a.plan) as fh:
            p = json.load(fh)
        with open(a.pods_before) as fh:
            pb = json.load(fh)
        with open(a.pods_after) as fh:
            pa = json.load(fh)
        try:
            with open(a.log) as fh:
                log_lines = fh.read().splitlines()
        except OSError:
            log_lines = None
        rec = evaluate(recs, plan_=p, pods_before=pb, pods_after=pa, log_lines=log_lines)
        with open(a.out, "w") as fh:
            json.dump(rec, fh, indent=1)
        for ln in report_lines(rec):
            print(ln, file=sys.stderr)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
