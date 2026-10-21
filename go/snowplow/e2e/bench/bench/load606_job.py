"""Render the in-cluster load-generator manifest for #606.

Namespace + ConfigMap (carrying bench/load606.py verbatim) + an Indexed Job
with one pod per shard, pinned to the tainted `loadgen` pool. Each pod drives
users [start, end] of its shard and writes JSONL to its /out (DRIVE_OUT, read
back with `kubectl cp`); only shard 0 samples /debug/vars (one sampler, not N,
so the sampler's own reads are a known constant).

The password is passed on the command line of a short-lived Job on a
throwaway cluster, for synthetic test users only; it is the same fixed test
password lifecycle._scaleuser_password() already uses.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

__all__ = ["render", "shard_ranges", "main"]

LOAD606_SRC = Path(__file__).with_name("load606.py")
IMAGE = "python:3.12-slim"
# Result files on the pods' /out emptyDir (#817): never stdout, whose container
# log the kubelet rotates under the reader.
DRIVE_OUT = "/out/nav.jsonl"
MEM_OUT = "/out/mem.jsonl"


def shard_ranges(users: int, shards: int) -> list[tuple[int, int]]:
    """Split users 1..N into `shards` contiguous, near-equal ranges."""
    if users < 1 or shards < 1 or shards > users:
        raise ValueError("need 1 <= shards <= users")
    base, extra = divmod(users, shards)
    out, start = [], 1
    for i in range(shards):
        n = base + (1 if i < extra else 0)
        out.append((start, start + n - 1))
        start += n
    return out


def split_burst(burst_users: int, ranges: list[tuple[int, int]]) -> list[int]:
    """The composite burst cohort split over the shards as evenly as their
    sizes allow; each shard's share comes from its OWN user range."""
    shards = len(ranges)
    base, extra = divmod(burst_users, shards)
    out = [base + (1 if i < extra else 0) for i in range(shards)]
    for (a, b), n in zip(ranges, out):
        if n > b - a + 1:
            raise ValueError(f"burst share {n} exceeds the shard's {b - a + 1} users")
    return out


# The loadgen node (bench/gke.py: 1 x n2-standard-8). GKE allocatable on an
# n2-standard-8 is ~7910m CPU / ~29 GiB; HEADROOM keeps the node's daemonsets
# and a margin out of the requests budget.
LOADGEN_ALLOCATABLE_MCPU = 7910
LOADGEN_ALLOCATABLE_MI = 29 * 1024
LOADGEN_HEADROOM_MCPU = 1000
LOADGEN_HEADROOM_MI = 4 * 1024
# A driver pod is ONE python process (GIL-bound, ~1 core at most: measured
# 0.88 cores at saturation, see #825), so it requests the core it can use.
DRIVER_CPU = "1"
DRIVER_MEMORY = "1Gi"
FRESH_CPU, FRESH_MEMORY = "1", "1Gi"
MEM_CPU, MEM_MEMORY = "250m", "512Mi"
LOGIN_CONCURRENCY_TOTAL = 64          # run 10's proven authn concurrency


def _mcpu(q: str) -> int:
    return int(q[:-1]) if q.endswith("m") else int(float(q) * 1000)


def _mi(q: str) -> int:
    return int(q[:-2]) * 1024 if q.endswith("Gi") else int(q[:-2])


def loadgen_fit(shards: int, driver_cpu: str, driver_memory: str) -> dict:
    """The loadgen node must hold every Job's requests (shards drivers +
    fresh606 + mem606) with headroom, or the render refuses: a pod that
    cannot schedule would leave a shard Pending and the run INVALID."""
    cpu = shards * _mcpu(driver_cpu) + _mcpu(FRESH_CPU) + _mcpu(MEM_CPU)
    mem = shards * _mi(driver_memory) + _mi(FRESH_MEMORY) + _mi(MEM_MEMORY)
    cap_cpu = LOADGEN_ALLOCATABLE_MCPU - LOADGEN_HEADROOM_MCPU
    cap_mem = LOADGEN_ALLOCATABLE_MI - LOADGEN_HEADROOM_MI
    if cpu > cap_cpu or mem > cap_mem:
        raise ValueError(f"loadgen node over budget: {shards} drivers x ({driver_cpu}, "
                         f"{driver_memory}) + fresh606 + mem606 = {cpu}m / {mem}Mi requested, "
                         f"budget {cap_cpu}m / {cap_mem}Mi (n2-standard-8 minus headroom)")
    return {"cpu_m": cpu, "memory_mi": mem, "budget_cpu_m": cap_cpu, "budget_memory_mi": cap_mem}


def _indent(text: str, n: int) -> str:
    pad = " " * n
    return "\n".join(pad + ln if ln else ln for ln in text.splitlines())


def render(*, users: int, shards: int, think_s: float, duration_s: float,
           snowplow_url: str, authn_url: str, password: str,
           init: str, frontend_ns: str = "krateo-system",
           builders_ns: str = "krateo-system", src: str | None = None,
           composite_routes: list[str] | tuple = (), burst_users: int = 0,
           burst_at_s: float = 600.0, viewers: bool = True,
           linger_s: float = 3600.0, driver_cpu: str = DRIVER_CPU,
           driver_memory: str = DRIVER_MEMORY) -> str:
    if not init.startswith("/call"):
        raise ValueError("init must be the frontend config.json api.INIT /call path")
    loadgen_fit(shards, driver_cpu, driver_memory)
    src = LOAD606_SRC.read_text() if src is None else src
    ranges = shard_ranges(users, shards)
    bursts = split_burst(burst_users, ranges)
    # Shell case over JOB_COMPLETION_INDEX → this pod's user range and its
    # share of the composite burst cohort.
    case = "\n".join(f"  {i}) S={a}; E={b}; B={n};;"
                     for i, ((a, b), n) in enumerate(zip(ranges, bursts)))
    portal_args = (f"--init {json.dumps(init)} --frontend-ns {json.dumps(frontend_ns)} "
                   f"--builders-ns {json.dumps(builders_ns)}")
    # Cold composite-page burst (#710 ruling 3): ONE cohort of burst_users,
    # split evenly over the shards (each reserves the last B users of its own
    # range), all arriving at burst_at_s after their pod's drive start — the
    # pods start together, so the cohort arrives within the pods' login-time
    # skew, which the verdict reports (composite_burst). Split, not repeated
    # per pod (that would be shards x the burst), and not all in one pod (one
    # GIL-bound process would queue the whole cohort's calls behind itself).
    burst_args = ""
    if burst_users > 0:
        burst_args = (f" --burst-users \"$BURST\" --burst-at-s {burst_at_s:g}"
                      + "".join(f" --composite-route {json.dumps(r)}" for r in composite_routes))
    # The results go to a FILE on the pod's /out, never to stdout (#817): the
    # kubelet rotates container logs at a size cap and `kubectl logs` returns
    # only the current file, so run 9's ~30 min of navigations were gone by
    # collect. When drive exits the wrapper appends its exit code, writes it
    # to /out/done (the pod turns Ready: its results are final; run606 reads
    # the code) and stays up for `kubectl cp` until linger_s.
    script = f"""set -eu
case "$JOB_COMPLETION_INDEX" in
{case}
  *) echo "bad index" >&2; exit 2;;
esac
VARS=0; [ "$JOB_COMPLETION_INDEX" = "0" ] && VARS=15
BURST=$B
# One navigation worker per user (a user has at most one navigation in
# flight): the pool never queues a due navigation (run 10: 128 workers for
# 1000 users during the first-load arrival). Logins: the shards share authn,
# so each pod's adaptive ramp is capped at its share of run 10's 64.
NAVW=$(( E - S + 1 )); [ "$NAVW" -lt 128 ] && NAVW=128
rc=0
python3 -u /opt/load606/load606.py drive \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --password {json.dumps(password)} --user-start "$S" --user-end "$E" \\
  --think-s {think_s} --duration-s {duration_s} --vars-interval "$VARS" \\
  --nav-workers "$NAVW" --login-max-concurrency {max(8, LOGIN_CONCURRENCY_TOTAL // shards)} \\
  {portal_args}{burst_args}{"" if viewers else " --no-viewers"} --out {DRIVE_OUT} || rc=$?
printf '{{"kind": "drive_exit", "rc": %d, "t": %d}}\\n' "$rc" "$(date +%s)" >> {DRIVE_OUT}
echo "drive exited rc=$rc; results in {DRIVE_OUT}; holding {linger_s:g}s for collect"
echo "$rc" > /out/done
sleep {linger_s:g}
exit "$rc"
"""
    return f"""\
apiVersion: v1
kind: Namespace
metadata:
  name: bench-loadgen
  labels:
    app: scaletest
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: load606
  namespace: bench-loadgen
data:
  load606.py: |
{_indent(src, 4)}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: load606
  namespace: bench-loadgen
spec:
  completionMode: Indexed
  completions: {shards}
  parallelism: {shards}
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: load606
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
      - name: driver
        image: {IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(script, 10)}
        resources:
          requests:
            cpu: {json.dumps(driver_cpu)}
            memory: {driver_memory}
        readinessProbe:
          exec:
            command: ["test", "-f", "/out/done"]
          periodSeconds: 10
        volumeMounts:
        - name: src
          mountPath: /opt/load606
        - name: out
          mountPath: /out
      volumes:
      - name: src
        configMap:
          name: load606
      - name: out
        emptyDir: {{}}
"""


def render_preflight(*, snowplow_url: str, authn_url: str, user: str,
                     password: str, init: str, frontend_ns: str,
                     builders_ns: str, src: str | None = None) -> str:
    """Namespace + ConfigMap + the preflight606 Job: one user, seconds-scale
    (`load606 preflight --full-walk`). Exit 2 = refused (config error: an
    unserved type or a 404 on INIT or a menu route); product 5xx are recorded
    in its JSON line, never a refusal."""
    src = LOAD606_SRC.read_text() if src is None else src
    script = f"""set -eu
exec python3 -u /opt/load606/load606.py preflight \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --user {json.dumps(user)} --password {json.dumps(password)} \\
  --init {json.dumps(init)} --frontend-ns {json.dumps(frontend_ns)} \\
  --builders-ns {json.dumps(builders_ns)} --full-walk
"""
    return f"""\
apiVersion: v1
kind: Namespace
metadata:
  name: bench-loadgen
  labels:
    app: scaletest
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: load606
  namespace: bench-loadgen
data:
  load606.py: |
{_indent(src, 4)}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: preflight606
  namespace: bench-loadgen
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: preflight606
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
      - name: preflight
        image: {IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(script, 10)}
        volumeMounts:
        - name: src
          mountPath: /opt/load606
      volumes:
      - name: src
        configMap:
          name: load606
"""


def render_mem(*, snowplow_url: str, authn_url: str, user: str, password: str,
               duration_s: float, interval_s: float = 15.0,
               profile_every_s: float = 120.0,
               vars_prefixes: tuple[str, ...] = (), linger_s: float = 3600.0) -> str:
    """mem606: the standard memory capture for every run (#710) — memstats
    every interval_s and heap profiles at Ready / under load / after any
    restart, kept in the pod's /out until the collect step copies them.
    METADATA ONLY. Uses the load606 ConfigMap that render() creates.

    Framed and crash-proof like the driver (#817): memwatch writes mem_start /
    mem_end and ends early once /out/stop exists (collect asks for the final
    stream); this wrapper — not python — appends mem_exit, writes the code to
    /out/exited and lingers, so a crashed sampler's stream is still copied."""
    script = f"""set -eu
rc=0
python3 -u /opt/load606/load606.py memwatch \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --user {json.dumps(user)} --password {json.dumps(password)} \\
  --duration-s {duration_s:g} --interval-s {interval_s:g} --linger-s 0 --stop-file /out/stop \\
  --profile-every-s {profile_every_s:g} --out-dir /out --out {MEM_OUT}{"".join(f" --vars-prefix {json.dumps(x)}" for x in vars_prefixes)} || rc=$?
printf '{{"kind": "mem_exit", "rc": %d, "t": %d}}\\n' "$rc" "$(date +%s)" >> {MEM_OUT}
echo "$rc" > /out/exited
sleep {linger_s:g}
exit "$rc"
"""
    return f"""\
---
apiVersion: batch/v1
kind: Job
metadata:
  name: mem606
  namespace: bench-loadgen
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: mem606
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
      - name: mem
        image: {IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(script, 10)}
        resources:
          requests:
            cpu: {MEM_CPU}
            memory: {MEM_MEMORY}
        volumeMounts:
        - name: src
          mountPath: /opt/load606
        - name: out
          mountPath: /out
      volumes:
      - name: src
        configMap:
          name: load606
      - name: out
        emptyDir: {{}}
"""


def render_fresh(*, snowplow_url: str, authn_url: str, user: str, password: str,
                 rates: list[str], samples: int, gap_s: float, probe_name: str,
                 namespaces: int, comp_group: str, comp_version: str,
                 comp_plural: str, poll_ms: int = 100,
                 delay_s: float = 0, viewer: bool = True,
                 modes: list[str] | tuple = ("annotation",)) -> str:
    """The freshness probe as an in-cluster Job (fresh606) on the loadgen pool.

    It talks to snowplow/authn by Service DNS (no external IP needed) and
    mutates the probe + churn compositions through the Kubernetes API with its
    own ServiceAccount, whose ClusterRole allows get/patch on the bench
    composition kind ONLY (never Secrets). Uses the load606 ConfigMap that
    render() creates; apply both in one manifest.
    """
    rate_args = " ".join(str(r) for r in rates)
    modes = list(modes or ("annotation",))
    # An annotation-only probe renders exactly as before (run 8); status
    # churn (run 9) adds the phases and the /status write permission.
    mode_args = "" if modes == ["annotation"] else " --modes " + " ".join(modes)
    status_rule = ""
    if "status" in modes:
        status_rule = f"""
- apiGroups: ["{comp_group}"]
  resources: ["{comp_plural}/status"]
  verbs: ["get", "update"]"""
    # The delay lets every virtual user's FIRST navigation land before
    # freshness is sampled (the load Job starts at the same time).
    script = f"""set -eu
sleep {delay_s:g}
exec python3 -u /opt/load606/load606.py fresh --mutator incluster \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --user {json.dumps(user)} --password {json.dumps(password)} \\
  --group {comp_group} --version {comp_version} --plural {comp_plural} \\
  --probe-name {probe_name} --rates {rate_args} --samples {samples} \\
  --gap-s {gap_s} --poll-ms {poll_ms} --churn-namespaces {namespaces} \\
  --wait-for-load{"" if viewer else " --no-viewer"}{mode_args} --out /dev/stdout
"""
    return f"""\
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: fresh606
  namespace: bench-loadgen
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: bench-fresh606-patch
  labels:
    app: scaletest
rules:
- apiGroups: ["{comp_group}"]
  resources: ["{comp_plural}"]
  verbs: ["get", "patch"]{status_rule}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: bench-fresh606-patch
  labels:
    app: scaletest
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: bench-fresh606-patch
subjects:
- kind: ServiceAccount
  name: fresh606
  namespace: bench-loadgen
---
apiVersion: batch/v1
kind: Job
metadata:
  name: fresh606
  namespace: bench-loadgen
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: fresh606
    spec:
      serviceAccountName: fresh606
      restartPolicy: Never
      nodeSelector:
        bench.krateo.io/pool: loadgen
      tolerations:
      - key: dedicated
        operator: Equal
        value: loadgen
        effect: NoSchedule
      containers:
      - name: fresh
        image: {IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(script, 10)}
        resources:
          requests:
            cpu: {json.dumps(FRESH_CPU)}
            memory: {FRESH_MEMORY}
        volumeMounts:
        - name: src
          mountPath: /opt/load606
      volumes:
      - name: src
        configMap:
          name: load606
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="load606_job")
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--shards", type=int, default=2)
    p.add_argument("--think-s", type=float, default=60.0)
    p.add_argument("--duration-s", type=float, default=1800.0)
    p.add_argument("--driver-cpu", default=DRIVER_CPU, help="CPU request per driver pod")
    p.add_argument("--driver-memory", default=DRIVER_MEMORY, help="memory request per driver pod")
    p.add_argument("--linger-s", type=float, default=3600.0,
                   help="how long a finished driver pod stays up for collect (kubectl cp)")
    p.add_argument("--snowplow-url", required=True)
    p.add_argument("--authn-url", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--init", required=True)
    p.add_argument("--composite-route", action="append", default=None)
    p.add_argument("--burst-users", type=int, default=0)
    p.add_argument("--burst-at-s", type=float, default=600.0)
    p.add_argument("--frontend-ns", default="krateo-system")
    p.add_argument("--builders-ns", default="krateo-system")
    p.add_argument("--preflight-only", action="store_true",
                   help="render ONLY the preflight Job (+ namespace/configmap)")
    p.add_argument("--preflight-user", default="scaleuser-0000")
    # Optional in-cluster freshness Job (fresh606), same manifest.
    p.add_argument("--fresh-rates", nargs="*", default=None)
    p.add_argument("--fresh-user", default="scaleuser-0001")
    p.add_argument("--fresh-samples", type=int, default=60)
    p.add_argument("--fresh-gap-s", type=float, default=5.0)
    p.add_argument("--fresh-delay-s", type=float, default=60.0)
    p.add_argument("--mem-vars-prefix", action="append", default=None,
                   help="extra /debug/vars key prefixes for mem606 (repeatable)")
    p.add_argument("--mem-duration-s", type=float, default=0.0,
                   help=">0 also renders the mem606 memory/heap sampler Job")
    p.add_argument("--probe-name", default="bench-app-01-01")
    p.add_argument("--namespaces", type=int, default=50)
    p.add_argument("--comp-group", default="composition.krateo.io")
    p.add_argument("--comp-version", default="v1-2-2")
    p.add_argument("--comp-plural", default="githubscaffoldingwithcompositionpages")
    p.add_argument("--fresh-modes", nargs="*", default=None,
                   help="fresh606 churn modes: annotation and/or status (run 9)")
    p.add_argument("--no-viewers", action="store_true",
                   help="driver and probe open NO /refreshes viewers (#764)")
    a = p.parse_args(argv)
    if a.preflight_only:
        sys.stdout.write(render_preflight(
            snowplow_url=a.snowplow_url, authn_url=a.authn_url,
            user=a.preflight_user, password=a.password, init=a.init,
            frontend_ns=a.frontend_ns, builders_ns=a.builders_ns))
        return 0
    sys.stdout.write(render(users=a.users, shards=a.shards, think_s=a.think_s,
                            duration_s=a.duration_s,
                            snowplow_url=a.snowplow_url,
                            authn_url=a.authn_url, password=a.password,
                            init=a.init, frontend_ns=a.frontend_ns,
                            builders_ns=a.builders_ns,
                            composite_routes=a.composite_route or [],
                            burst_users=a.burst_users, burst_at_s=a.burst_at_s,
                            viewers=not a.no_viewers, linger_s=a.linger_s,
                            driver_cpu=a.driver_cpu, driver_memory=a.driver_memory))
    if a.mem_duration_s > 0:
        sys.stdout.write(render_mem(
            snowplow_url=a.snowplow_url, authn_url=a.authn_url,
            user=a.preflight_user, password=a.password,
            duration_s=a.mem_duration_s,
            vars_prefixes=tuple(a.mem_vars_prefix or ()), linger_s=a.linger_s))
    if a.fresh_rates:
        sys.stdout.write(render_fresh(
            snowplow_url=a.snowplow_url, authn_url=a.authn_url,
            user=a.fresh_user, password=a.password, rates=a.fresh_rates,
            samples=a.fresh_samples, gap_s=a.fresh_gap_s,
            probe_name=a.probe_name, namespaces=a.namespaces,
            comp_group=a.comp_group, comp_version=a.comp_version,
            comp_plural=a.comp_plural, delay_s=a.fresh_delay_s,
            viewer=not a.no_viewers,
            modes=tuple(a.fresh_modes or ("annotation",))))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValueError as e:          # a plan the loadgen node or the users cannot hold
        print(f"refusing the load plan: {e}", file=sys.stderr)
        sys.exit(2)
