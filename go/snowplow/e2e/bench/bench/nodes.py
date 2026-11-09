"""Run 24: the CPU platform of every node a run measured on. Report only: evaluate shows it as
INFO lines with the verdict, and the paired verdict per arm; nothing judges it.

    python3 -m bench.nodes read --phase up|collect --project P --out OUT/cluster-shape-nodes.jsonl \\
        [--timeout-s 300] -- kubectl ...

Run 23's profile analysis found the load generator's node ran 11 % faster on one cluster than on
the other, on the same machine type: a GCE machine type is a family of CPU platforms, and no run
recorded which one each node got. Each reading (run606 takes one after gke-up, `up`, and one at
collect, `collect`: a node GKE replaced in between shows) records, per node:

  from `kubectl get nodes -o json`: the node's name, its GKE node pool
      (cloud.google.com/gke-nodepool: default-pool = the platform pool, snowplow, loadgen), the
      bench pool label, its zone and machine type labels;
  from `gcloud compute instances describe <node> --zone <zone>` (a GKE node is the GCE instance of
      its name): the instance's cpuPlatform (what GCE placed it on, e.g. "Intel Cascade Lake") and
      minCpuPlatform. The describe's output is projected to those fields only
      (--format json(name,zone,machineType,cpuPlatform,minCpuPlatform)): an instance's metadata and
      service account never leave gcloud.

A node whose describe fails, or that no kubectl reading lists, reads UNKNOWN with why (the error
code only, never gcloud's text: it names the operator's account; bench/identity.py). One record
per reading is appended to the file (kind node_platforms); run606's evaluate step reads every
cluster-shape*.jsonl, so the file is in the run's evidence as the cluster's shape is.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time

from bench import identity
from bench.sampler import Runner

UNKNOWN = "UNKNOWN"
KIND = "node_platforms"
POOL_LABEL = "cloud.google.com/gke-nodepool"
BENCH_POOL_LABEL = "bench.krateo.io/pool"
ZONE_LABEL = "topology.kubernetes.io/zone"
TYPE_LABEL = "node.kubernetes.io/instance-type"
DESCRIBE_FIELDS = "json(name,zone,machineType,cpuPlatform,minCpuPlatform)"
PHASES = ("up", "collect")


def describe_command(project: str, zone: str, name: str) -> list[str]:
    return ["gcloud", "compute", "instances", "describe", name, "--project", project, "--zone", zone,
            "--format", DESCRIBE_FIELDS]


def _last(path) -> str | None:
    return path.rsplit("/", 1)[-1] if isinstance(path, str) and path else None


def node_rows(nodes: dict | None) -> list[dict]:
    """What kubectl says of each node: name, pool, zone, machine type (labels only)."""
    out = []
    for n in (nodes or {}).get("items") or []:
        md = n.get("metadata") or {}
        lb = md.get("labels") or {}
        out.append({"node": md.get("name"), "pool": lb.get(POOL_LABEL), "bench_pool": lb.get(BENCH_POOL_LABEL),
                    "zone": lb.get(ZONE_LABEL), "machine_type": lb.get(TYPE_LABEL)})
    return sorted(out, key=lambda r: (str(r["pool"]), str(r["node"])))


def platform(describe: dict | None, why: str | None) -> dict:
    """One instance's platform from its (projected) describe, or why it is UNKNOWN."""
    if not isinstance(describe, dict) or not describe.get("cpuPlatform"):
        return {"cpu_platform": UNKNOWN, "min_cpu_platform": None,
                "why": why or "the describe answered no cpuPlatform"}
    return {"cpu_platform": describe["cpuPlatform"], "min_cpu_platform": describe.get("minCpuPlatform"),
            "instance_machine_type": _last(describe.get("machineType"))}


def reading(phase: str, nodes: dict | None, describes: dict[str, tuple[dict | None, str | None]],
            *, nodes_why: str | None = None, t: float | None = None) -> dict:
    """One record: `describes` maps a node name to (its describe, or None, and why)."""
    rec = {"kind": KIND, "phase": phase, "t": time.time() if t is None else t}
    if nodes is None:
        return {**rec, "state": UNKNOWN, "why": nodes_why or "the nodes were not read", "nodes": []}
    rows = []
    for r in node_rows(nodes):
        d, why = describes.get(r["node"], (None, "not described"))
        rows.append({**r, **platform(d, why)})
    return {**rec, "state": "read" if rows else UNKNOWN, "nodes": rows,
            **({} if rows else {"why": "kubectl listed no node"})}


def _run_json(runner: Runner, argv: list[str], deadline: float, what: str) -> tuple[dict | None, str | None]:
    left = math.ceil(deadline - time.monotonic())
    if left < 1:
        return None, f"{what} was not sent: the reading's bound was spent"
    rc, out, err, timed_out = runner.run(argv, left)
    if timed_out:
        return None, f"{what} did not answer within {left} s"
    if rc != 0:
        return None, f"{what} exit {rc}: {identity.error_summary(err.decode(errors='replace'))}"
    try:
        doc = json.loads(out)
    except ValueError:
        return None, f"{what}'s answer is not JSON"
    return (doc, None) if isinstance(doc, dict) else (None, f"{what}'s answer is not a JSON object")


def read_live(phase: str, kubectl: list[str], project: str, timeout_s: float, runner: Runner | None = None) -> dict:
    """The reading: one kubectl list, one gcloud describe per node, all under ONE bound."""
    runner = runner or Runner()
    deadline = time.monotonic() + timeout_s
    left = max(1, math.ceil(deadline - time.monotonic()))
    nodes, why = _run_json(runner, kubectl + [f"--request-timeout={left}s", "get", "nodes", "-o", "json"],
                           deadline, "kubectl get nodes")
    describes = {}
    for r in node_rows(nodes):
        if not (r["node"] and r["zone"]):
            describes[r["node"]] = (None, "the node names no zone label: its instance cannot be described")
            continue
        describes[r["node"]] = _run_json(runner, describe_command(project, r["zone"], r["node"]), deadline,
                                         "gcloud compute instances describe")
    return reading(phase, nodes, describes, nodes_why=why)


def reading_line(rec: dict) -> str:
    if rec.get("state") != "read":
        return f"node CPU platforms ({rec.get('phase')}): UNKNOWN ({rec.get('why')})"
    by: dict[str, list[str]] = {}
    for n in rec["nodes"]:
        plat = n["cpu_platform"] if n["cpu_platform"] != UNKNOWN else f"UNKNOWN ({n.get('why')})"
        by.setdefault(str(n["pool"]), []).append(f"{n['node']} {plat} ({n['machine_type']}, {n['zone']})")
    return f"node CPU platforms ({rec['phase']}): " + "; ".join(f"{p}: " + ", ".join(v) for p, v in sorted(by.items()))


def report(recs: list[dict]) -> dict:
    """evaluate's nodes_report_only: every reading's line, and which node changed platform (or
    appeared, or went) between them. INFO only."""
    rs = sorted((r for r in recs if r.get("kind") == KIND), key=lambda r: r.get("t") or 0)
    if not rs:
        why = "no node_platforms record: the run recorded no reading (a run before run 24)"
        return {"result": "INFO", "state": UNKNOWN, "why": why, "lines": [f"INFO node CPU platforms: UNKNOWN ({why})"]}
    lines = ["INFO " + reading_line(r) for r in rs]
    by_pool: dict[str, list[str]] = {}
    for n in rs[-1].get("nodes") or []:
        by_pool.setdefault(str(n.get("pool")), []).append(n.get("cpu_platform"))
    changed = []
    if len(rs) > 1:
        first = {n["node"]: n.get("cpu_platform") for n in rs[0].get("nodes") or []}
        last = {n["node"]: n.get("cpu_platform") for n in rs[-1].get("nodes") or []}
        changed = sorted(k for k in set(first) | set(last) if first.get(k) != last.get(k))
        if changed and rs[0].get("state") == rs[-1].get("state") == "read":
            lines.append("INFO node CPU platforms changed between the readings: " + ", ".join(
                f"{k} {first.get(k, '(absent)')} -> {last.get(k, '(absent)')}" for k in changed))
    known = [r for r in rs if r.get("state") == "read"]
    return {"result": "INFO", "state": "read" if known else UNKNOWN, "readings": rs, "by_pool": by_pool,
            "changed": changed, "lines": lines}


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    kubectl = []
    if "--" in argv:
        i = argv.index("--")
        argv, kubectl = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(prog="nodes")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rd = sub.add_parser("read", help="one reading of every node's CPU platform (-- kubectl ...)")
    rd.add_argument("--phase", required=True, choices=PHASES)
    rd.add_argument("--project", required=True)
    rd.add_argument("--out", required=True, help="appended: one node_platforms record per reading")
    rd.add_argument("--timeout-s", type=float, default=300.0, help="the reading's ONE bound: every read together")
    rp = sub.add_parser("report", help="evaluate's report-only section of a records file, as JSON")
    rp.add_argument("file")
    a = ap.parse_args(argv)
    if a.cmd == "report":
        recs = []
        try:
            with open(a.file) as fh:
                recs = [json.loads(ln) for ln in fh if ln.strip()]
        except (OSError, ValueError):
            pass
        print(json.dumps(report(recs), indent=1))
        return 0
    if not kubectl:
        ap.error("read: the kubectl command goes after --")
    rec = read_live(a.phase, kubectl, a.project, a.timeout_s)
    with open(a.out, "a") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
    print(reading_line(rec))
    return 0


if __name__ == "__main__":
    sys.exit(main())
