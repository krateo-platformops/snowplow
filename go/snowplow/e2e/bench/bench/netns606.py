"""#1089: the TCP path under snowplow, read NODE-SIDE every 15 s.

Run 20 inferred that snowplow's 30 s stage dials (#1054) were TCP connects
whose SYN was never answered (Go's dialer gives up at 30 s); nothing in the run
could say. This sampler records what would:

  per pod network namespace on the snowplow node (snowplow's among them):
    TcpExt.TCPSynRetrans   SYNs sent again (a connect not answered in time)
    Tcp.RetransSegs        segments retransmitted
    Tcp.ActiveOpens        connects started (the scale the two above are read against)
    Tcp.AttemptFails       connects that failed
  the node (its own network namespace, where pods' Service traffic is NATed):
    nf_conntrack insert_failed / drop / early_drop (/proc/net/stat/nf_conntrack,
    summed over CPUs), nf_conntrack_count / nf_conntrack_max;
    which dataplane runs there (kube-proxy, or Dataplane V2's cilium-agent:
    there Service NAT uses Cilium's BPF conntrack, which nf_conntrack does not see)

HOW (the snowplow image is distroless: no shell, nothing to exec): ONE pod on
the snowplow node, hostPID + hostNetwork, privileged (readlink of another
user's /proc/<pid>/ns/net needs CAP_SYS_PTRACE), reading /proc only. Every
process's network namespace is /proc/<pid>/ns/net; /proc/<pid>/net/{netstat,
snmp} are that namespace's counters, whoever reads them; the pod a process
belongs to is in its /proc/<pid>/cgroup (kubepods ... pod<UID>). No sidecar,
no ephemeral container, nothing changed on the snowplow pod. The evaluator
(bench/netpath.py) picks snowplow's namespace by the pod UID of the bench's
snowplow_pod records; a namespace no pod UID names (the host's) is reported
as such.

Each sample is one JSON line in /out/netns.jsonl (an emptyDir: the kubelet
rotates container logs, #817), copied at collect. A file that cannot be read
is recorded with its error, never as 0. /out/stop ends the loop.

STDLIB ONLY: the pod runs this file alone (python:3.12-slim).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

TCP_FIELDS = (("TcpExt", "TCPSynRetrans", "netstat"), ("Tcp", "RetransSegs", "snmp"),
              ("Tcp", "ActiveOpens", "snmp"), ("Tcp", "AttemptFails", "snmp"))
CONNTRACK_FIELDS = ("insert_failed", "drop", "early_drop")
DATAPLANE_COMMS = {"kube-proxy": "kube_proxy", "cilium-agent": "cilium_agent"}
EVERY_S = 15.0
OUT = "/out/netns.jsonl"
STOP = "/out/stop"
# a pod's cgroup path names its UID: cgroupfs `/kubepods/burstable/pod<uid>/…`
# (dashes) or systemd `kubepods-burstable-pod<uid with _>.slice` (underscores)
_POD_UID = re.compile(r"pod([0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12})")
_NETNS = re.compile(r"^net:\[(\d+)\]$")


def pairs(text: str) -> dict[str, dict[str, int]]:
    """/proc/net/{netstat,snmp}: header line + value line per section,
    `Sec: Name1 Name2 ...` / `Sec: v1 v2 ...` -> {Sec: {Name: int}}."""
    out: dict[str, dict[str, int]] = {}
    lines = [ln.split() for ln in text.splitlines() if ln.strip()]
    for head, vals in zip(lines[::2], lines[1::2]):
        if not head or not vals or head[0] != vals[0] or not head[0].endswith(":"):
            continue
        sec = head[0][:-1]
        row = {}
        for k, v in zip(head[1:], vals[1:]):
            try:
                row[k] = int(v)
            except ValueError:
                pass
        out[sec] = row
    return out


def pod_uid(cgroup_text: str) -> str | None:
    """The pod UID a process's cgroup names (dashed), or None (a host process)."""
    m = _POD_UID.search(cgroup_text or "")
    return m.group(1).replace("_", "-") if m else None


def conntrack(text: str) -> dict[str, int]:
    """/proc/net/stat/nf_conntrack: a header naming the columns, then one row
    per CPU in hex. Each CONNTRACK_FIELDS column summed over the CPUs (a
    column this kernel does not print is absent, never 0)."""
    lines = [ln.split() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return {}
    head, rows = lines[0], lines[1:]
    out: dict[str, int] = {}
    for f in CONNTRACK_FIELDS:
        if f not in head:
            continue
        i = head.index(f)
        try:
            out[f] = sum(int(r[i], 16) for r in rows if len(r) > i)
        except ValueError:
            continue
    return out


def _read(path: str) -> str:
    with open(path) as fh:
        return fh.read()


def _err(e: Exception) -> str:
    return f"{type(e).__name__}: {e.strerror or e}" if isinstance(e, OSError) else f"{type(e).__name__}: {e}"


def tcp_counters(proc: str, pid: str) -> tuple[dict[str, int], dict[str, str]]:
    """({field: value}, {file: error}) for one process's network namespace."""
    got: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for f in ("netstat", "snmp"):
        try:
            got[f] = pairs(_read(os.path.join(proc, pid, "net", f)))
        except OSError as e:
            errors[f] = _err(e)
    out = {}
    for sec, name, f in TCP_FIELDS:
        v = (got.get(f) or {}).get(sec, {}).get(name)
        if v is not None:
            out[name] = v
    return out, errors


def sample(proc: str = "/proc", clock=time.time) -> dict:
    """One sample of every network namespace on the node, and the node's
    conntrack. `proc`: the (host) procfs root."""
    t0 = clock()
    spaces: dict[str, dict] = {}            # netns inode -> {pid, pod_uids}
    seen_comms: set[str] = set()
    unreadable = 0
    host_ns = None
    try:
        pids = sorted((p for p in os.listdir(proc) if p.isdigit()), key=int)
    except OSError as e:
        return {"kind": "netns", "t": round(clock(), 3), "t_start": round(t0, 3), "failed": True,
                "error": f"{proc}: {_err(e)}"}
    for pid in pids:
        try:
            m = _NETNS.match(os.readlink(os.path.join(proc, pid, "ns", "net")))
        except OSError:
            unreadable += 1                  # gone, or no access: counted, never guessed
            continue
        if not m:
            continue
        ns = m.group(1)
        if pid == "1":
            host_ns = ns
        try:
            comm = _read(os.path.join(proc, pid, "comm")).strip()
            if comm in DATAPLANE_COMMS:
                seen_comms.add(comm)
        except OSError:
            pass
        try:
            uid = pod_uid(_read(os.path.join(proc, pid, "cgroup")))
        except OSError:
            uid = None
        sp = spaces.setdefault(ns, {"pid": pid, "pod_uids": set()})
        if uid:
            sp["pod_uids"].add(uid)
    namespaces = []
    for ns, sp in sorted(spaces.items()):
        tcp, errors = tcp_counters(proc, sp["pid"])
        namespaces.append({"netns": ns, "host": ns == host_ns, "pod_uids": sorted(sp["pod_uids"]),
                           "tcp": tcp, **({"errors": errors} if errors else {})})
    node: dict = {}
    try:
        node["conntrack"] = conntrack(_read(os.path.join(proc, "net", "stat", "nf_conntrack")))
    except OSError as e:
        node["conntrack"] = None
        node["conntrack_error"] = _err(e)
    for k in ("nf_conntrack_count", "nf_conntrack_max"):
        try:
            node[k] = int(_read(os.path.join(proc, "sys", "net", "netfilter", k)).strip())
        except (OSError, ValueError) as e:
            node[k] = None
            node[f"{k}_error"] = _err(e)
    try:
        node["boot_id"] = _read(os.path.join(proc, "sys", "kernel", "random", "boot_id")).strip()
    except OSError:
        node["boot_id"] = None
    node["dataplane"] = {v: k in seen_comms for k, v in DATAPLANE_COMMS.items()}
    return {"kind": "netns", "t": round(clock(), 3), "t_start": round(t0, 3), "processes": len(pids),
            "processes_unreadable": unreadable, "namespaces": namespaces, "node": node}


def loop(out: str, every_s: float, stop: str, proc: str = "/proc", clock=time.time, sleep=time.sleep,
         samples: int | None = None) -> int:
    """Sample every `every_s` until `stop` exists (or `samples` samples); one
    line each, flushed, so a copy at any moment holds whole samples."""
    due, n = clock(), 0
    with open(out, "a") as fh:
        fh.write(json.dumps({"kind": "netns_start", "t": round(clock(), 3), "every_s": every_s}) + "\n")
        fh.flush()
        while (samples is None or n < samples) and not os.path.exists(stop):
            try:
                rec = sample(proc, clock)
            except Exception as e:            # noqa: BLE001 - a sample that broke is recorded, the loop goes on
                rec = {"kind": "netns", "t": round(clock(), 3), "failed": True, "error": _err(e)}
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
            fh.flush()
            n += 1
            due = max(due + every_s, clock())
            if samples is None or n < samples:
                sleep(max(0.0, due - clock()))
        fh.write(json.dumps({"kind": "netns_end", "t": round(clock(), 3), "samples": n}) + "\n")
    return 0


# ─── the manifest (host side) ──────────────────────────────────────────────
NAMESPACE = "bench-loadgen"
JOB = "netns606"
IMAGE = "python:3.12-slim"


def _indent(text: str, n: int) -> str:
    pad = " " * n
    return "\n".join(pad + ln if ln else ln for ln in text.splitlines())


def render(src: str | None = None, every_s: float = EVERY_S) -> str:
    """Namespace + the netns606 ConfigMap (this file verbatim) + its Job,
    pinned to the snowplow pool (its taint tolerated), hostPID + hostNetwork,
    privileged, reading /proc only. Small requests: it shares snowplow's node."""
    src = _read(__file__) if src is None else src
    return f"""apiVersion: v1
kind: Namespace
metadata:
  name: {NAMESPACE}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {JOB}
  namespace: {NAMESPACE}
data:
  netns606.py: |
{_indent(src, 4)}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {JOB}
  namespace: {NAMESPACE}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      hostPID: true
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      nodeSelector:
        bench.krateo.io/pool: snowplow
      tolerations:
      - key: dedicated
        operator: Equal
        value: snowplow
        effect: NoSchedule
      containers:
      - name: netns
        image: {IMAGE}
        command: ["python3", "/opt/netns606/netns606.py", "loop", "--every-s", "{every_s:g}",
                  "--out", "{OUT}", "--stop-file", "{STOP}"]
        securityContext:
          privileged: true
        resources:
          requests: {{cpu: 20m, memory: 32Mi}}
          limits: {{cpu: 200m, memory: 128Mi}}
        volumeMounts:
        - name: src
          mountPath: /opt/netns606
        - name: out
          mountPath: /out
      volumes:
      - name: src
        configMap:
          name: {JOB}
      - name: out
        emptyDir: {{}}
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="netns606")
    sub = p.add_subparsers(dest="cmd", required=True)
    lp = sub.add_parser("loop", help="(in the pod) sample until the stop file exists")
    lp.add_argument("--every-s", type=float, default=EVERY_S)
    lp.add_argument("--out", default=OUT)
    lp.add_argument("--stop-file", default=STOP)
    lp.add_argument("--proc", default="/proc")
    lp.add_argument("--samples", type=int, default=None)
    rp = sub.add_parser("render", help="(host) the Namespace + ConfigMap + Job YAML on stdout")
    rp.add_argument("--every-s", type=float, default=EVERY_S)
    a = p.parse_args(argv)
    if a.cmd == "render":
        sys.stdout.write(render(every_s=a.every_s))
        return 0
    return loop(a.out, a.every_s, a.stop_file, a.proc, samples=a.samples)


if __name__ == "__main__":
    sys.exit(main())
