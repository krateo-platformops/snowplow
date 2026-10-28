"""Run 11's load and measurement Jobs (#830): k6 for the 1,000-user
background load, k6 browser for 3-5 real Chrome sessions.

Both are plain Jobs on the loadgen pool. Not the k6 operator: its benefit is a
synchronized multi-pod start, and ONE k6 pod carries 1,000 VUs (k6 is a
multi-core Go runtime, one JS VM per VU — no single-process ceiling like run
10's Python driver, #825). Each pod keeps #818's discipline: results under
/out (never the container log, which the kubelet rotates), a framed stream, a
wrapper that records the exit code and marks /out/done (the pod turns Ready),
then lingers for `kubectl cp`.

Login: the k6 pod's initContainer signs the users in through authn
(load606.py login-tokens, the proven adaptive ramp) into /work/tokens.json —
an emptyDir INSIDE the pod, never under /out, never collected or logged.

Open tabs (#830 ruling, option A): the k6 pod's `viewer` sidecar
(load606.py view-feed, the run 9/10 ViewerHub) tails k6's navigation feed on
the /feed emptyDir and keeps each user's /refreshes stream armed on the
widgets that user's last page load actually answered with.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

from bench import rbac_model

__all__ = [
    "K6_BROWSER_IMAGE",
    "K6_IMAGE",
    "browser_config",
    "browser_sessions",
    "k6_config",
    "render_browser",
    "render_k6",
]

K6_DIR = Path(__file__).resolve().parent.parent / "k6"
# grafana/k6 2.3.0 (released 2026-09-21), pinned by digest.
K6_IMAGE = ("grafana/k6:2.3.0@sha256:"
            "9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34")
K6_BROWSER_IMAGE = ("grafana/k6:2.3.0-with-browser@sha256:"
                    "fd3bc5d244f87e7d05152ede97b99813a381a83b639a4e43e1939b56460544ee")
PY_IMAGE = "python:3.12-slim"
K6_UID = 12345                      # the k6 images' user

K6_CPU, K6_MEMORY = "4", "4Gi"
BROWSER_CPU, BROWSER_MEMORY = "3", "4Gi"
FORWARD_CPU, FORWARD_MEMORY = "100m", "64Mi"
# 1,000 idle SSE reader threads + push refetches (run 10's driver carried them
# inside its 3 CPU / 4Gi beside the navigation)
VIEWER_CPU, VIEWER_MEMORY = "1", "2Gi"

LOGIN_CONCURRENCY = 64              # run 10's proven authn concurrency
# Run 12: navigate.js runs one VU per k6 user (a tab, as the SPA drives it),
# so k6 holds at most `users` VUs. Measured (#831): the pinned k6 with 1,800
# VUs on this script peaks at 1.09 GiB, inside K6_MEMORY.
FRESH_CPU, FRESH_MEMORY = "1", "1Gi"   # load606_job.render_fresh's requests

# GKE node allocatable (cloud.google.com/kubernetes-engine/docs/concepts/
# plan-node-sizes): CPU reserves 6 % of the 1st core, 1 % of the 2nd, 0.5 % of
# the 3rd-4th, 0.25 % above 4; memory reserves 25 % of the first 4 GiB, 20 %
# of the next 4, 10 % of the next 8, 6 % of the next 112, plus a 100 MiB
# eviction threshold. n2-standard-8 = 8 vCPU / 32 GiB, -16 = 16 / 64.
LOADGEN_ALLOCATABLE = {
    "n2-standard-8": {"cpu_m": 7910, "memory_mi": 29023},     # 32768 - 3645 (3.56 GiB) - 100 MiB
    "n2-standard-16": {"cpu_m": 15890, "memory_mi": 59825},   # 65536 - 5611 (5.48 GiB) - 100 MiB
}
# kept free for the node's daemonsets and a margin
LOADGEN_HEADROOM = {"cpu_m": 1000, "memory_mi": 4096}


def _mcpu(q: str) -> int:
    return int(q[:-1]) if q.endswith("m") else int(float(q) * 1000)


def _mi(q: str) -> int:
    return int(q[:-2]) * 1024 if q.endswith("Gi") else int(q[:-2])


def loadgen_fit(machine: str, requests: list[tuple[str, str]]) -> dict:
    """Refuse a plan the loadgen node cannot hold with headroom: a pod that
    cannot schedule would leave its Job Pending and the run INVALID."""
    if machine not in LOADGEN_ALLOCATABLE:
        raise ValueError(f"unknown loadgen machine type {machine!r}: "
                         f"known {sorted(LOADGEN_ALLOCATABLE)}")
    cpu = sum(_mcpu(c) for c, _ in requests)
    mem = sum(_mi(m) for _, m in requests)
    alloc = LOADGEN_ALLOCATABLE[machine]
    cap_cpu = alloc["cpu_m"] - LOADGEN_HEADROOM["cpu_m"]
    cap_mem = alloc["memory_mi"] - LOADGEN_HEADROOM["memory_mi"]
    if cpu > cap_cpu or mem > cap_mem:
        raise ValueError(f"loadgen node over budget on {machine}: {cpu}m / {mem}Mi requested, "
                         f"budget {cap_cpu}m / {cap_mem}Mi (allocatable minus headroom)")
    return {"machine": machine, "cpu_m": cpu, "memory_mi": mem,
            "budget_cpu_m": cap_cpu, "budget_memory_mi": cap_mem}


def k6_plan_requests(k6_cpu: str | None = None, browser_cpu: str | None = None,
                     viewers: bool = True) -> list[tuple[str, str]]:
    """Every container the k6 plan puts on the loadgen node: k6 (+ its viewer
    sidecar), the browser pod (Chrome + the forwarder sidecar), fresh606 and
    mem606."""
    return ([(k6_cpu or K6_CPU, K6_MEMORY)]
            + ([(VIEWER_CPU, VIEWER_MEMORY)] if viewers else [])
            + [(browser_cpu or BROWSER_CPU, BROWSER_MEMORY), (FORWARD_CPU, FORWARD_MEMORY),
               (FRESH_CPU, FRESH_MEMORY), ("250m", "512Mi")])


def browser_sessions(m: rbac_model.RbacModel, n: int = 5,
                     last: int | None = None) -> list[dict]:
    """n users of DISTINCT identity classes, deterministic, among users
    1..`last` (the navigating users: the burst users are the last ones).

    #831 gate: snowplow's identity-bound cells are per identity CLASS (the
    binding set, which the group set decides), so a k6 user of the same class
    warms a browser user's "first" visit. The picks are therefore SINGLETON
    classes, a group set no other user has, whose user k6 then leaves out
    (k6_config browser_users): a platform admin, cross-team users, then other
    singletons. 1,000 users / 50 namespaces have 107 of them. Only when there
    are too few is a shared class taken, and that session is cold=False."""
    last = m.users if last is None else last
    classes: dict[tuple, list[int]] = {}
    for i in range(1, m.users + 1):
        classes.setdefault(tuple(sorted(rbac_model.user_groups(i, m))), []).append(i)

    def key(i):
        return tuple(sorted(rbac_model.user_groups(i, m)))

    def teams(i):
        return len([g for g in rbac_model.user_groups(i, m) if g.startswith("bench-team-")])

    single = [i for i in range(1, last + 1) if len(classes[key(i)]) == 1]
    admins = [i for i in single if i <= m.admins]
    cross = [i for i in single if i > m.admins and teams(i) > 1]
    picks = (admins[:1] + cross + admins[1:] + [i for i in single if i not in admins + cross])[:n]
    seen = {key(i) for i in picks}
    for i in range(1, last + 1):                 # too few singletons: shared classes
        if len(picks) >= n:
            break
        if key(i) not in seen:
            seen.add(key(i))
            picks.append(i)
    return [{"name": f"s{k + 1}", "user": f"scaleuser-{i:04d}", "index": i, "offset": 3 * k,
             "groups": rbac_model.user_groups(i, m), "cold": len(classes[key(i)]) == 1}
            for k, i in enumerate(picks)]


def first_visit_label(sessions: list[dict]) -> str:
    """What a browser "first visit" is, for the report."""
    if sessions and all(s["cold"] for s in sessions):
        return ("cold for the identity class: every session's group set is its own and its "
                "user is not driven by k6 (cells shared across classes may still be warm)")
    return "first for this tab, not cold: k6 users share some sessions' identity classes"


def k6_config(*, snowplow_url: str, init: str, frontend_ns: str, builders_ns: str,
              routes: list[dict], users: int, burst_users: int, think_s: float,
              duration_s: float, burst_at_s: float, composite_routes: list[str],
              browser_users: list[int] = (), viewers: bool = True) -> dict:
    """k6's config. The burst users are the LAST `burst_users` of 1..users;
    the real-Chrome sessions' users (`browser_users`) are left out of k6's
    navigating users, so each identity has exactly one client."""
    if not routes:
        raise ValueError("no route table: preflight found no navigable route")
    if burst_users and not composite_routes:
        raise ValueError("a composite burst needs at least one composite route")
    unknown = set(composite_routes) - {r["path"] for r in routes}
    if unknown:
        # a deep link outside the menu would record failed first loads, not a burst
        raise ValueError(f"composite routes not in the route table: {sorted(unknown)}")
    if not 0 <= burst_users < users:
        raise ValueError("burst users must be fewer than users")
    if think_s <= 0:
        raise ValueError("the think time must be > 0: it sets k6's arrival rate")
    burst_ids = list(range(users - burst_users + 1, users + 1))
    excluded = set(browser_users)
    if excluded & set(burst_ids) or not excluded <= set(range(1, users + 1)):
        raise ValueError("browser users must be navigating users in 1..users, outside the burst")
    nav_ids = [i for i in range(1, users - burst_users + 1) if i not in excluded]
    if not nav_ids:
        raise ValueError("no user left for k6 to drive")
    return {"snowplowUrl": snowplow_url.rstrip("/"), "init": init, "frontendNs": frontend_ns,
            "buildersNs": builders_ns,
            "routes": [{"path": r["path"], "endpoint": r["endpoint"]} for r in routes],
            "users": users, "burstUsers": burst_users, "thinkS": think_s,
            "navUserIds": nav_ids, "burstUserIds": burst_ids,
            "durationS": int(duration_s), "burstAtS": int(burst_at_s),
            "compositeRoutes": list(composite_routes),
            "viewers": viewers}            # write the navigation feed (navigate.js)


def browser_config(*, sessions: list[dict], routes: list[str], password: str,
                   think_s: float, duration_s: float, quiet_ms: int = 1000,
                   page_timeout_ms: int = 60000, signin_timeout_ms: int = 60000,
                   frontend_url: str = "http://localhost:8080",
                   snowplow_url: str = "http://localhost:8081",
                   fresh: dict | None = None) -> dict:
    """`fresh` (#935): bench/fresh_classes.plan — each session's fresh users,
    in order; `snowplow_url`: snowplow as the browser pod reaches it (its
    forward), where a session reads the learned-class counters."""
    by_session = {x["session"]: [f["user"] for f in x["fresh"]] for x in (fresh or {}).get("sessions", [])}
    return {"sessions": [dict({k: s[k] for k in ("name", "user", "offset")}, fresh=by_session.get(s["name"], []))
                         for s in sessions],
            "routes": list(routes), "password": password, "thinkS": think_s,
            "durationS": int(duration_s), "quietMs": quiet_ms,
            "pageTimeoutMs": page_timeout_ms, "signinTimeoutMs": signin_timeout_ms,
            "frontendUrl": frontend_url, "snowplowUrl": snowplow_url,
            "firstVisit": first_visit_label(sessions)}


def _indent(text: str, n: int) -> str:
    pad = " " * n
    return "\n".join(pad + ln if ln else ln for ln in text.splitlines())


CPU_SAMPLE_S = 5


def _cpu_sampler(prefix: str, request: str, limit: str | None) -> str:
    """The container's own CPU (#831 gate M1), from its cgroup: cpu.stat
    usage_usec / throttled_usec (v2; cpuacct.usage on v1), every CPU_SAMPLE_S,
    timed by /proc/uptime (sub-second), into /out/<prefix>-cpu.jsonl, framed
    by cpu_start (request, limit) and cpu_end. POSIX sh: the k6 images are
    Alpine (busybox), the python image Debian (dash)."""
    f = f"/out/{prefix}-cpu.jsonl"
    lim = _mcpu(limit) if limit else "null"
    return f"""cpu_sample() {{
  u=; th=null
  if [ -r /sys/fs/cgroup/cpu.stat ]; then
    while read -r k v; do
      case "$k" in usage_usec) u=$v ;; throttled_usec) th=$v ;; esac
    done < /sys/fs/cgroup/cpu.stat
  elif [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
    read -r n < /sys/fs/cgroup/cpuacct/cpuacct.usage; u=$((n / 1000))
  fi
  read -r up _ < /proc/uptime
  [ -n "$u" ] && printf '{{"kind": "cpu", "t": %s, "usage_usec": %s, "throttled_usec": %s}}\\n' "$up" "$u" "$th" >> {f}
}}
# the limit the kernel enforces (cgroup v2 cpu.max "<quota> <period>" or "max")
cg=null
if [ -r /sys/fs/cgroup/cpu.max ]; then
  read -r q per < /sys/fs/cgroup/cpu.max
  [ "$q" != max ] && cg=$((q * 1000 / per))
fi
printf '{{"kind": "cpu_start", "container": "{prefix}", "request_m": {_mcpu(request)}, "limit_m": {lim}, "cgroup_limit_m": %s, "interval_s": {CPU_SAMPLE_S}}}\\n' "$cg" > {f}
( while :; do cpu_sample; sleep {CPU_SAMPLE_S}; done ) &
sampler=$!
"""


def _wrapper(cmd: str, prefix: str, linger_s: float, done: str = "/out/done",
             cpu: tuple[str, str | None] | None = None) -> str:
    """#818's discipline: run, record the exit code into the stream dir,
    mark `done` (Ready = results final), linger for kubectl cp. `cpu` =
    (request, limit): sample the container's CPU while `cmd` runs."""
    head = _cpu_sampler(prefix, *cpu) if cpu else ""
    tail = (f"""kill "$sampler" 2>/dev/null
cpu_sample
printf '{{"kind": "cpu_end"}}\\n' >> /out/{prefix}-cpu.jsonl
""" if cpu else "")
    return f"""set -u
{head}rc=0
{cmd} || rc=$?
{tail}printf '{{"kind": "{prefix}_exit", "rc": %d, "t": %d}}\\n' "$rc" "$(date +%s)" > /out/{prefix}-exit.json
echo "{prefix} exited rc=$rc; holding {linger_s:g}s for collect"
echo "$rc" > {done}
sleep {linger_s:g}
exit "$rc"
"""


def _configmap(name: str, files: dict[str, str]) -> str:
    data = "\n".join(f"  {k}: |\n{_indent(v, 4)}" for k, v in files.items())
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: bench-loadgen
data:
{data}
"""


_POOL = """      restartPolicy: Never
      nodeSelector:
        bench.krateo.io/pool: loadgen
      tolerations:
      - key: dedicated
        operator: Equal
        value: loadgen
        effect: NoSchedule"""


def render_k6(*, config: dict, authn_url: str, password: str, linger_s: float,
              user_prefix: str = "scaleuser-", cpu: str = K6_CPU,
              memory: str = K6_MEMORY) -> str:
    """ConfigMap k6 (scripts + config) + the k6load Job. Uses the load606
    ConfigMap load606_job.render() creates (the login initContainer)."""
    scripts = {"spa.js": (K6_DIR / "spa.js").read_text(),
               "navigate.js": (K6_DIR / "navigate.js").read_text(),
               "config.json": json.dumps(config, indent=1)}
    login = (f"exec python3 -u /opt/load606/load606.py login-tokens --authn-url {json.dumps(authn_url)} "
             f"--password {json.dumps(password)} --user-prefix {json.dumps(user_prefix)} "
             f"--user-start 1 --user-end {config['users']} --max-concurrency {LOGIN_CONCURRENCY} "
             f"--out /work/tokens.json")
    viewers = bool(config.get("viewers"))
    # the feed: console.log lines, raw, into the emptyDir the viewer tails
    feed = " --log-format raw --console-output /feed/nav.jsonl" if viewers else ""
    run = f"k6 run --quiet{feed} --out json=/out/k6.json.gz /opt/k6/navigate.js"
    view = (f"exec python3 -u /opt/load606/load606.py view-feed "
            f"--snowplow-url {json.dumps(config['snowplowUrl'])} --tokens /work/tokens.json "
            f"--user-prefix {json.dumps(user_prefix)} --feed /feed/nav.jsonl "
            f"--stop-file /out/done --out /out/viewer.jsonl")
    sidecar = f"""
      - name: viewer
        image: {PY_IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(_wrapper(view.replace("exec ", "", 1), "viewer", linger_s, done="/out/viewer-done",
                  cpu=(VIEWER_CPU, VIEWER_CPU)), 10)}
        resources:
          requests:
            cpu: {json.dumps(VIEWER_CPU)}
            memory: {VIEWER_MEMORY}
          limits:
            cpu: {json.dumps(VIEWER_CPU)}
        readinessProbe:
          exec:
            command: ["test", "-f", "/out/viewer-done"]
          periodSeconds: 10
        volumeMounts:
        - name: src
          mountPath: /opt/load606
        - name: work
          mountPath: /work
          readOnly: true
        - name: feed
          mountPath: /feed
          readOnly: true
        - name: out
          mountPath: /out""" if viewers else ""
    return _configmap("k6", scripts) + f"""---
apiVersion: batch/v1
kind: Job
metadata:
  name: k6load
  namespace: bench-loadgen
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: k6load
      annotations:
        # collect's `kubectl exec cat /out/done` / `cp` reach k6's container
        kubectl.kubernetes.io/default-container: k6
    spec:
{_POOL}
      securityContext:
        runAsUser: {K6_UID}
        runAsGroup: {K6_UID}
        fsGroup: {K6_UID}
      initContainers:
      - name: login
        image: {PY_IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(login, 10)}
        volumeMounts:
        - name: src
          mountPath: /opt/load606
        - name: work
          mountPath: /work
      containers:
      - name: k6
        image: {K6_IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(_wrapper(run, "k6", linger_s, cpu=(cpu, cpu)), 10)}
        env:
        - name: K6_CONFIG
          value: /opt/k6/config.json
        - name: TOKENS_FILE
          value: /work/tokens.json
        - name: SUMMARY_FILE
          value: /out/k6-summary.json
        - name: GOMAXPROCS
          value: {json.dumps(str(math.ceil(_mcpu(cpu) / 1000)))}
        resources:
          requests:
            cpu: {json.dumps(cpu)}
            memory: {memory}
          # #831 gate M1: k6 (Go, GOMAXPROCS = node cores otherwise) must not
          # take the CPU Chrome is measuring with
          limits:
            cpu: {json.dumps(cpu)}
        readinessProbe:
          exec:
            command: ["test", "-f", "/out/done"]
          periodSeconds: 10
        volumeMounts:
        - name: k6
          mountPath: /opt/k6
        - name: work
          mountPath: /work
          readOnly: true
        - name: feed
          mountPath: /feed
        - name: out
          mountPath: /out{sidecar}
      volumes:
      - name: src
        configMap:
          name: load606
      - name: k6
        configMap:
          name: k6
      - name: work
        emptyDir: {{}}
      - name: feed
        emptyDir: {{}}
      - name: out
        emptyDir: {{}}
"""


# Run 11 (#831 follow-up): 4 of 5 sessions met net::ERR_CONNECTION_REFUSED on
# localhost:8080 — Chrome was up before the forward sidecar listened (plain
# containers start together). Three layers, each bounded: the forwarder is a
# NATIVE sidecar (initContainers, restartPolicy Always) whose startupProbe
# (forward.py --probe) holds the browser container back until every local
# port accepts; the browser wrapper then waits for an HTTP answer through
# every forward, i.e. from its target too (FORWARD_WAIT_S, else rc 3: INVALID
# with the reason); browser.js retries its first navigation.
FORWARD_WAIT_S = 180
FORWARD_PROBE_PERIOD_S = 2
# one attempt's whole time (#834 review): `wget -T` is a read-IDLE timeout,
# so a port that streams at / kept one attempt open forever. A stream that
# sent its headers has answered; busybox `timeout` ends the attempt.
FORWARD_ATTEMPT_S = 10


def _forward_wait(ports: list[str], wait_s: int) -> str:
    """POSIX sh (busybox): every 127.0.0.1:<port> answers HTTP (any status;
    busybox wget prints `HTTP/` only when a server answered) within wait_s;
    each attempt ends within FORWARD_ATTEMPT_S (+2 s to kill), so the wait
    overruns wait_s by at most one attempt. Writes /out/browser-forward.json
    either way."""
    return f"""wait_forwards() {{
  t0=$(date +%s); end=$((t0 + ${{FORWARD_WAIT_S:-{wait_s}}}))
  for p in {" ".join(ports)}; do
    until timeout -k 2 {FORWARD_ATTEMPT_S} wget -q -T 5 -S -O /dev/null "http://127.0.0.1:$p/" 2>&1 | grep -q 'HTTP/'; do
      if [ "$(date +%s)" -ge "$end" ]; then
        echo "forward 127.0.0.1:$p did not answer within ${{FORWARD_WAIT_S:-{wait_s}}}s: not starting the browser" >&2
        printf '{{"kind": "forward_wait", "ok": false, "port": %s, "waited_s": %s}}\\n' "$p" "$(( $(date +%s) - t0 ))" > /out/browser-forward.json
        return 3
      fi
      sleep 1
    done
  done
  printf '{{"kind": "forward_wait", "ok": true, "waited_s": %s}}\\n' "$(( $(date +%s) - t0 ))" > /out/browser-forward.json
}}
"""


def render_browser(*, config: dict, forwards: list[str], linger_s: float,
                   cpu: str = BROWSER_CPU, memory: str = BROWSER_MEMORY,
                   forward_wait_s: int = FORWARD_WAIT_S) -> str:
    """ConfigMap k6browser (script + config + forwarder) + the k6browser Job:
    Chrome through k6 browser, behind the localhost forwarder (a native
    sidecar Chrome waits for)."""
    files = {"browser.js": (K6_DIR / "browser.js").read_text(),
             "pageload.js": (K6_DIR / "pageload.js").read_text(),
             "forward.py": (K6_DIR / "forward.py").read_text(),
             "browser-config.json": json.dumps(config, indent=1)}
    ports = [f.split("=", 1)[0] for f in forwards]
    run = (_forward_wait(ports, forward_wait_s) + "wait_forwards && "
           "k6 run --quiet --out json=/out/browser.json.gz /opt/k6/browser.js")
    fwd = ", ".join(json.dumps(f) for f in forwards)
    probes = math.ceil(forward_wait_s / FORWARD_PROBE_PERIOD_S)
    return _configmap("k6browser", files) + f"""---
apiVersion: batch/v1
kind: Job
metadata:
  name: k6browser
  namespace: bench-loadgen
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: k6browser
      annotations:
        # collect's `kubectl exec` / `cp` reach the browser's /out, never the
        # forwarder sidecar's
        kubectl.kubernetes.io/default-container: browser
    spec:
{_POOL}
      securityContext:
        fsGroup: {K6_UID}
      initContainers:
      # a native sidecar (restartPolicy Always): it starts first, the browser
      # container only once its startupProbe passed, and it never holds the
      # Job open
      - name: forward
        image: {PY_IMAGE}
        restartPolicy: Always
        command: ["python3", "-u", "/opt/k6/forward.py", {fwd}]
        startupProbe:
          exec:
            command: ["python3", "/opt/k6/forward.py", "--probe", {fwd}]
          periodSeconds: {FORWARD_PROBE_PERIOD_S}
          timeoutSeconds: 10
          failureThreshold: {probes}
        resources:
          requests:
            cpu: {json.dumps(FORWARD_CPU)}
            memory: {FORWARD_MEMORY}
        volumeMounts:
        - name: k6
          mountPath: /opt/k6
      containers:
      - name: browser
        image: {K6_BROWSER_IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(_wrapper(run, "browser", linger_s, cpu=(cpu, None)), 10)}
        env:
        - name: BROWSER_CONFIG
          value: /opt/k6/browser-config.json
        - name: SUMMARY_FILE
          value: /out/browser-summary.json
        - name: K6_BROWSER_HEADLESS
          value: "true"
        - name: K6_BROWSER_ARGS
          value: no-sandbox
        resources:
          requests:
            cpu: {json.dumps(cpu)}
            memory: {memory}
        readinessProbe:
          exec:
            command: ["test", "-f", "/out/done"]
          periodSeconds: 10
        volumeMounts:
        - name: k6
          mountPath: /opt/k6
        - name: out
          mountPath: /out
      volumes:
      - name: k6
        configMap:
          name: k6browser
      - name: out
        emptyDir: {{}}
"""
