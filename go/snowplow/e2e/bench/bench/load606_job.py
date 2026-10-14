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
    a = p.parse_args(argv)
    sys.stdout.write(render(users=a.users, shards=a.shards, think_s=a.think_s,
                            duration_s=a.duration_s,
                            snowplow_url=a.snowplow_url,
                            authn_url=a.authn_url, password=a.password,
                            pages=a.page))
    return 0


if __name__ == "__main__":
    sys.exit(main())
