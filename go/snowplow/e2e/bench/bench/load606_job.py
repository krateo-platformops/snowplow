"""Render the in-cluster load-generator manifest for #606.

Namespace + ConfigMap (carrying bench/load606.py verbatim) + an Indexed Job
with one pod per shard, pinned to the tainted `loadgen` pool. Each pod drives
users [start, end] of its shard and streams JSONL to stdout; only shard 0
samples /debug/vars (one sampler, not N, so the sampler's own reads are a
known constant).

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


def _indent(text: str, n: int) -> str:
    pad = " " * n
    return "\n".join(pad + ln if ln else ln for ln in text.splitlines())


def render(*, users: int, shards: int, think_s: float, duration_s: float,
           snowplow_url: str, authn_url: str, password: str,
           pages: list[str], src: str | None = None) -> str:
    if not pages:
        raise ValueError("at least one --page is required")
    src = LOAD606_SRC.read_text() if src is None else src
    ranges = shard_ranges(users, shards)
    # Shell case over JOB_COMPLETION_INDEX → this pod's user range.
    case = "\n".join(f"  {i}) S={a}; E={b};;" for i, (a, b) in enumerate(ranges))
    page_args = " ".join(f"--page {json.dumps(p)}" for p in pages)
    script = f"""set -eu
case "$JOB_COMPLETION_INDEX" in
{case}
  *) echo "bad index" >&2; exit 2;;
esac
VARS=0; [ "$JOB_COMPLETION_INDEX" = "0" ] && VARS=15
exec python3 -u /opt/load606/load606.py drive \\
  --snowplow-url {json.dumps(snowplow_url)} --authn-url {json.dumps(authn_url)} \\
  --password {json.dumps(password)} --user-start "$S" --user-end "$E" \\
  --think-s {think_s} --duration-s {duration_s} --vars-interval "$VARS" \\
  {page_args} --out /dev/stdout
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
            cpu: "3"
            memory: 4Gi
        volumeMounts:
        - name: src
          mountPath: /opt/load606
      volumes:
      - name: src
        configMap:
          name: load606
"""


def render_fresh(*, snowplow_url: str, authn_url: str, user: str, password: str,
                 rates: list[str], samples: int, gap_s: float, probe_name: str,
                 namespaces: int, comp_group: str, comp_version: str,
                 comp_plural: str, poll_ms: int = 100,
                 delay_s: float = 0) -> str:
    """The freshness probe as an in-cluster Job (fresh606) on the loadgen pool.

    It talks to snowplow/authn by Service DNS (no external IP needed) and
    mutates the probe + churn compositions through the Kubernetes API with its
    own ServiceAccount, whose ClusterRole allows get/patch on the bench
    composition kind ONLY (never Secrets). Uses the load606 ConfigMap that
    render() creates; apply both in one manifest.
    """
    rate_args = " ".join(str(r) for r in rates)
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
  --out /dev/stdout
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
  verbs: ["get", "patch"]
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


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="load606_job")
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--shards", type=int, default=2)
    p.add_argument("--think-s", type=float, default=60.0)
    p.add_argument("--duration-s", type=float, default=1800.0)
    p.add_argument("--snowplow-url", required=True)
    p.add_argument("--authn-url", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--page", action="append", required=True)
    # Optional in-cluster freshness Job (fresh606), same manifest.
    p.add_argument("--fresh-rates", nargs="*", default=None)
    p.add_argument("--fresh-user", default="scaleuser-0001")
    p.add_argument("--fresh-samples", type=int, default=60)
    p.add_argument("--fresh-gap-s", type=float, default=5.0)
    p.add_argument("--fresh-delay-s", type=float, default=60.0)
    p.add_argument("--probe-name", default="bench-app-01-01")
    p.add_argument("--namespaces", type=int, default=50)
    p.add_argument("--comp-group", default="composition.krateo.io")
    p.add_argument("--comp-version", default="v1-2-2")
    p.add_argument("--comp-plural", default="githubscaffoldingwithcompositionpages")
    a = p.parse_args(argv)
    sys.stdout.write(render(users=a.users, shards=a.shards, think_s=a.think_s,
                            duration_s=a.duration_s,
                            snowplow_url=a.snowplow_url,
                            authn_url=a.authn_url, password=a.password,
                            pages=a.page))
    if a.fresh_rates:
        sys.stdout.write(render_fresh(
            snowplow_url=a.snowplow_url, authn_url=a.authn_url,
            user=a.fresh_user, password=a.password, rates=a.fresh_rates,
            samples=a.fresh_samples, gap_s=a.fresh_gap_s,
            probe_name=a.probe_name, namespaces=a.namespaces,
            comp_group=a.comp_group, comp_version=a.comp_version,
            comp_plural=a.comp_plural, delay_s=a.fresh_delay_s))
    return 0


if __name__ == "__main__":
    sys.exit(main())
