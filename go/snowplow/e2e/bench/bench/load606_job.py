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

__all__ = ["main", "render", "shard_ranges"]

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


def render_base(src: str | None = None) -> str:
    """The bench-loadgen Namespace + the load606 ConfigMap (load606.py
    verbatim) every in-cluster Job mounts."""
    src = LOAD606_SRC.read_text() if src is None else src
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
"""


def _indent(text: str, n: int) -> str:
    pad = " " * n
    return "\n".join(pad + ln if ln else ln for ln in text.splitlines())


def render(*, users: int, shards: int, think_s: float, duration_s: float,
           snowplow_url: str, authn_url: str, password: str,
           init: str, frontend_ns: str = "krateo-system",
           builders_ns: str = "krateo-system", src: str | None = None,
           composite_routes: list[str] | tuple = (), burst_users: int = 0,
           burst_at_s: float = 600.0, viewers: bool = True,
           linger_s: float = 3600.0) -> str:
    if not init.startswith("/call"):
        raise ValueError("init must be the frontend config.json api.INIT /call path")
    src = LOAD606_SRC.read_text() if src is None else src
    ranges = shard_ranges(users, shards)
    # Shell case over JOB_COMPLETION_INDEX → this pod's user range.
    case = "\n".join(f"  {i}) S={a}; E={b};;" for i, (a, b) in enumerate(ranges))
    portal_args = (f"--init {json.dumps(init)} --frontend-ns {json.dumps(frontend_ns)} "
                   f"--builders-ns {json.dumps(builders_ns)}")
    # Cold composite-page burst (#710 ruling 3): the last N users of the
    # LAST shard arrive together at burst_at_s, each deep-linking to one of
    # the composite routes the preflight derived from the live menu.
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
BURST=0; [ "$JOB_COMPLETION_INDEX" = "{shards - 1}" ] && BURST={burst_users}
rc=0
python3 -u /opt/load606/load606.py drive \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --password {json.dumps(password)} --user-start "$S" --user-end "$E" \\
  --think-s {think_s} --duration-s {duration_s} --vars-interval "$VARS" \\
  {portal_args}{burst_args}{"" if viewers else " --no-viewers"} --out {DRIVE_OUT} || rc=$?
printf '{{"kind": "drive_exit", "rc": %d, "t": %d}}\\n' "$rc" "$(date +%s)" >> {DRIVE_OUT}
echo "drive exited rc=$rc; results in {DRIVE_OUT}; holding {linger_s:g}s for collect"
echo "$rc" > /out/done
sleep {linger_s:g}
exit "$rc"
"""
    return render_base(src) + f"""---
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
            cpu: "3"
            memory: 4Gi
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
            cpu: "1"
            memory: 1Gi
        volumeMounts:
        - name: src
          mountPath: /opt/load606
      volumes:
      - name: src
        configMap:
          name: load606
"""


def preflight_routes(path: str) -> list[dict]:
    """The run's route table: the preflight's routes (menu order), with the
    content endpoint each one fetches; only routes it found servable."""
    rec = None
    with open(path) as fh:
        for line in fh:
            if line.startswith("{"):
                r = json.loads(line)
                if r.get("kind") == "preflight":
                    rec = r
    if rec is None:
        raise ValueError(f"{path}: no preflight record")
    return [{"path": r["path"], "endpoint": r["endpoint"]} for r in rec.get("routes") or []
            if r.get("endpoint") and r.get("status") == 200]


# The frontend's config.json (installer default, values.schema.json:2552-2556 at
# 0.3.430) points the SPA at localhost:8081 snowplow, :8082 authn and :8083
# events. :8083 is the sse-proxy (the /notifications firehose behind live
# widget refresh, ui/src/hooks/useLiveRefresh.ts:18-21): Service
# krateo-sse-proxy (installer compositions.yaml:790,803 pin the release name to
# the component name; chart krateo-sse-proxy 0.1.15 templates/service.yaml:4 +
# _helpers.tpl:20-31 make it the Service name) on port 8080
# (templates/service.yaml:14, values.yaml:36; exposure.port is unset, so
# compositions.yaml:153 leaves it).
K6_BROWSER_FORWARDS = ("8080=frontend.krateo-system.svc:8080",
                       "8081=snowplow.krateo-system.svc:8081",
                       "8082=authn.krateo-system.svc:8082",
                       "8083=krateo-sse-proxy.krateo-system.svc:8080")


def render_k6_driver(a) -> str:
    """#830: the k6 load Job and the real-Chrome Job, from the CLI args."""
    import os

    from bench import k6_job, rbac_model
    viewers = not a.no_viewers
    k6_job.loadgen_fit(a.loadgen_machine_type, k6_job.k6_plan_requests(viewers=viewers))
    if a.preflight_file and os.path.exists(a.preflight_file):
        routes = preflight_routes(a.preflight_file)
    elif a.dry_run:
        # BENCH_606_DRY_RUN prints the manifests and applies nothing; there is
        # no preflight to read, so the route table is a visible placeholder.
        routes = [{"path": "/dry-run-no-preflight", "endpoint": a.init}]
    else:
        raise ValueError(f"no preflight route table at {a.preflight_file!r}")
    think_s = a.think_s if a.think_s > 0 or not a.dry_run else 1.0   # dry: printed, never run
    burst_users = min(a.burst_users, a.users - 1) if a.dry_run else a.burst_users
    model = rbac_model.RbacModel(users=a.users, namespaces=a.namespaces)
    sessions = k6_job.browser_sessions(model, a.browser_sessions,
                                       last=max(0, a.users - burst_users - 1))
    cfg = k6_job.k6_config(snowplow_url=a.snowplow_url, init=a.init, frontend_ns=a.frontend_ns,
                           builders_ns=a.builders_ns, routes=routes, users=a.users,
                           burst_users=burst_users, think_s=think_s,
                           duration_s=a.duration_s, burst_at_s=a.burst_at_s,
                           composite_routes=a.composite_route or (
                               [routes[0]["path"]] if a.dry_run else []),
                           browser_users=[s["index"] for s in sessions], viewers=viewers)
    bcfg = k6_job.browser_config(sessions=sessions, routes=[r["path"] for r in routes],
                                 password=a.password, think_s=a.browser_think_s,
                                 duration_s=a.duration_s)
    forwards = a.browser_forward or list(K6_BROWSER_FORWARDS)
    return (k6_job.render_k6(config=cfg, authn_url=a.authn_url, password=a.password,
                             linger_s=a.linger_s)
            + k6_job.render_browser(config=bcfg, forwards=forwards, linger_s=a.linger_s))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="load606_job")
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--shards", type=int, default=2)
    p.add_argument("--think-s", type=float, default=60.0)
    p.add_argument("--duration-s", type=float, default=1800.0)
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
    # #830: run 11's load = k6 + real Chrome, instead of the load606 driver.
    p.add_argument("--driver", choices=["k6", "load606"], default="load606")
    p.add_argument("--preflight-file", default=None,
                   help="k6: preflight.jsonl, whose routes are the run's route table")
    p.add_argument("--browser-sessions", type=int, default=5)
    p.add_argument("--loadgen-machine-type", default="n2-standard-16",
                   help="k6: the loadgen node the plan must fit (bench/gke.py LOADGEN_MACHINE_TYPE)")
    p.add_argument("--dry-run", action="store_true",
                   help="k6: no preflight exists (BENCH_606_DRY_RUN); placeholder route table")
    p.add_argument("--browser-think-s", type=float, default=15.0)
    p.add_argument("--browser-forward", action="append", default=None,
                   help="k6: LOCALPORT=host:port forwarded inside the browser pod (repeatable)")
    a = p.parse_args(argv)
    if a.preflight_only:
        sys.stdout.write(render_preflight(
            snowplow_url=a.snowplow_url, authn_url=a.authn_url,
            user=a.preflight_user, password=a.password, init=a.init,
            frontend_ns=a.frontend_ns, builders_ns=a.builders_ns))
        return 0
    if a.driver == "k6":
        sys.stdout.write(render_base() + render_k6_driver(a))
    else:
        sys.stdout.write(render(users=a.users, shards=a.shards, think_s=a.think_s,
                            duration_s=a.duration_s,
                            snowplow_url=a.snowplow_url,
                            authn_url=a.authn_url, password=a.password,
                            init=a.init, frontend_ns=a.frontend_ns,
                            builders_ns=a.builders_ns,
                            composite_routes=a.composite_route or [],
                            burst_users=a.burst_users, burst_at_s=a.burst_at_s,
                            viewers=not a.no_viewers, linger_s=a.linger_s))
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
    sys.exit(main())
