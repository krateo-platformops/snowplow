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
"""

from __future__ import annotations

import json
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

LOGIN_CONCURRENCY = 64              # run 10's proven authn concurrency
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


def k6_plan_requests(k6_cpu: str | None = None, browser_cpu: str | None = None) -> list[tuple[str, str]]:
    """Every Job the k6 plan puts on the loadgen node: k6, the browser pod
    (Chrome + the forwarder sidecar), fresh606 and mem606."""
    return [(k6_cpu or K6_CPU, K6_MEMORY), (browser_cpu or BROWSER_CPU, BROWSER_MEMORY),
            (FORWARD_CPU, FORWARD_MEMORY), (FRESH_CPU, FRESH_MEMORY), ("250m", "512Mi")]


def browser_sessions(m: rbac_model.RbacModel, n: int = 5,
                     last: int | None = None) -> list[dict]:
    """n users from DISTINCT identity classes, deterministic: a platform
    admin, a cross-team user, then single-team users of distinct teams.
    Only users 1..`last` (the navigating users: the burst users are the last
    ones, k6_config)."""
    last = m.users if last is None else last
    picks: list[int] = []
    seen: set[tuple] = set()

    def take(i):
        key = tuple(sorted(rbac_model.user_groups(i, m)))
        if key not in seen:
            seen.add(key)
            picks.append(i)

    if m.admins and last >= 1:
        take(1)
    multi = next((i for i in range(m.admins + 1, last + 1)
                  if len([g for g in rbac_model.user_groups(i, m)
                          if g.startswith("bench-team-")]) > 1), None)
    if multi:
        take(multi)
    for i in range(m.admins + 1, last + 1):
        if len(picks) >= n:
            break
        take(i)
    return [{"name": f"s{k + 1}", "user": f"scaleuser-{i:04d}", "index": i, "offset": 3 * k,
             "groups": rbac_model.user_groups(i, m)} for k, i in enumerate(picks[:n])]


def k6_config(*, snowplow_url: str, init: str, frontend_ns: str, builders_ns: str,
              routes: list[dict], users: int, burst_users: int, think_s: float,
              duration_s: float, burst_at_s: float, composite_routes: list[str],
              browser_users: list[int] = ()) -> dict:
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
    nav_rate = len(nav_ids) / think_s
    return {"snowplowUrl": snowplow_url.rstrip("/"), "init": init, "frontendNs": frontend_ns,
            "buildersNs": builders_ns,
            "routes": [{"path": r["path"], "endpoint": r["endpoint"]} for r in routes],
            "users": users, "burstUsers": burst_users, "thinkS": think_s,
            "navUserIds": nav_ids, "burstUserIds": burst_ids,
            "durationS": int(duration_s), "burstAtS": int(burst_at_s),
            "compositeRoutes": list(composite_routes),
            # VUs: in steady state ~rate x page time; the first-load minute
            # can need many more. maxVUs = the navigating users (no more
            # page loads in flight than people), and a drop is COUNTED; k6
            # refuses preAllocatedVUs > maxVUs (the kind proof hit it).
            "preAllocatedVUs": min(len(nav_ids), max(50, int(nav_rate * 10))),
            "maxVUs": len(nav_ids)}


def browser_config(*, sessions: list[dict], routes: list[str], password: str,
                   think_s: float, duration_s: float, quiet_ms: int = 1000,
                   page_timeout_ms: int = 60000,
                   frontend_url: str = "http://localhost:8080") -> dict:
    return {"sessions": [{k: s[k] for k in ("name", "user", "offset")} for s in sessions],
            "routes": list(routes), "password": password, "thinkS": think_s,
            "durationS": int(duration_s), "quietMs": quiet_ms,
            "pageTimeoutMs": page_timeout_ms, "frontendUrl": frontend_url}


def _indent(text: str, n: int) -> str:
    pad = " " * n
    return "\n".join(pad + ln if ln else ln for ln in text.splitlines())


def _wrapper(cmd: str, prefix: str, linger_s: float) -> str:
    """#818's discipline: run, record the exit code into the stream dir,
    mark /out/done (Ready = results final), linger for kubectl cp."""
    return f"""set -u
rc=0
{cmd} || rc=$?
printf '{{"kind": "{prefix}_exit", "rc": %d, "t": %d}}\\n' "$rc" "$(date +%s)" > /out/{prefix}-exit.json
echo "{prefix} exited rc=$rc; holding {linger_s:g}s for collect"
echo "$rc" > /out/done
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
    run = "k6 run --quiet --out json=/out/k6.json.gz /opt/k6/navigate.js"
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
{_indent(_wrapper(run, "k6", linger_s), 10)}
        env:
        - name: K6_CONFIG
          value: /opt/k6/config.json
        - name: TOKENS_FILE
          value: /work/tokens.json
        - name: SUMMARY_FILE
          value: /out/k6-summary.json
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
        - name: work
          mountPath: /work
          readOnly: true
        - name: out
          mountPath: /out
      volumes:
      - name: src
        configMap:
          name: load606
      - name: k6
        configMap:
          name: k6
      - name: work
        emptyDir: {{}}
      - name: out
        emptyDir: {{}}
"""


def render_browser(*, config: dict, forwards: list[str], linger_s: float,
                   cpu: str = BROWSER_CPU, memory: str = BROWSER_MEMORY) -> str:
    """ConfigMap k6browser (script + config + forwarder) + the k6browser Job:
    Chrome through k6 browser, with localhost forwarder sidecar."""
    files = {"browser.js": (K6_DIR / "browser.js").read_text(),
             "forward.py": (K6_DIR / "forward.py").read_text(),
             "browser-config.json": json.dumps(config, indent=1)}
    run = "k6 run --quiet --out json=/out/browser.json.gz /opt/k6/browser.js"
    fwd = ", ".join(json.dumps(f) for f in forwards)
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
      containers:
      - name: browser
        image: {K6_BROWSER_IMAGE}
        command: ["/bin/sh", "-c"]
        args:
        - |
{_indent(_wrapper(run, "browser", linger_s), 10)}
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
      - name: forward
        image: {PY_IMAGE}
        command: ["python3", "-u", "/opt/k6/forward.py", {fwd}]
        resources:
          requests:
            cpu: {json.dumps(FORWARD_CPU)}
            memory: {FORWARD_MEMORY}
        volumeMounts:
        - name: k6
          mountPath: /opt/k6
      volumes:
      - name: k6
        configMap:
          name: k6browser
      - name: out
        emptyDir: {{}}
"""
