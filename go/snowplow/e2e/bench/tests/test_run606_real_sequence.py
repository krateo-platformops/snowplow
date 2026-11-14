"""run606.sh — the REAL step sequence (not the dry run) under a WRONG ambient
kube context.

The first shakedown attempt (2026-10-14) died at step 1: `python -m bench
gke-reap` imported bench.cluster, whose import-time guard compared the
operator's ambient current-context (`kind-krateo-4388`) to the dead neon pin
and exited 3. The dry-run arms could not see it — they never execute a step
and conftest sets BENCH_ALLOW_NON_GKE=1 for the whole suite.

This arm executes every step for real, with:
  - fake `gcloud` / `kubectl` / `helm` first on PATH (no cloud, no cluster),
    each recording argv + the kubeconfig it resolved;
  - an ambient KUBECONFIG whose current-context is a deliberately wrong kind
    context, and BENCH_ALLOW_NON_GKE removed;
  - a local HTTP server standing in for authn + snowplow.

It asserts the run completes, tears down, never resolves the ambient
kubeconfig or any context but the bench cluster's, leaves the ambient file
byte-identical, and never prints the synthetic users' password.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import urllib.parse
from pathlib import Path

import pytest

from bench import gke
from bench.load606 import SCALEUSER_PASSWORD
from tests import _planted, fake_gke_api, fake_region

BENCH_DIR = Path(__file__).resolve().parent.parent
RUN_NAME = "snowplow-bench-1014-1300"
PROJECT = "operations-dev-krateo-io"
ZONE = "europe-west4-a"                       # every node's zone
REGION = "europe-west4"                       # the regional control plane's: gcloud and the context name it
KCTX = f"gke_{PROJECT}_{REGION}_{RUN_NAME}"
AMBIENT_CTX = "kind-krateo-4388"
# #895: a successful teardown ends with the cluster delete, the poll that sees
# it gone, and the exact disk backstop for it
TEARDOWN_TAIL = [["container", "clusters", "delete"], ["container", "clusters", "describe"],
                 ["compute", "disks", "list"]]
# The digest the fake snowplow pod runs and the run expects (a test value).
FAKE_DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32

_COMMON = textwrap.dedent("""\
    import json, os, sys, time
    STATE = os.environ["FAKE_STATE"]
    def record(tool, kubeconfig=None, context=None):
        with open(os.path.join(STATE, "calls.jsonl"), "a") as fh:
            fh.write(json.dumps({"tool": tool, "argv": sys.argv[1:],
                                 "kubeconfig": kubeconfig,
                                 "context": context}) + "\\n")
    """)

FAKE_GCLOUD = _COMMON + fake_region.LOADER + textwrap.dedent("""\
    a = sys.argv[1:]
    record("gcloud", os.environ.get("KUBECONFIG"))
    def opt(name):
        return a[a.index(name) + 1] if name in a else None
    if a[:3] == ["auth", "print-access-token"]:                 # #985: the Cloud Quotas read
        print("fake-access-token"); sys.exit(0)
    if a[:3] == ["services", "list", "--enabled"]:              # run 26g: the apis step (tests/fake_region.py)
        answer_services()
    if a[:3] == ["container", "clusters", "list"]:
        print(os.environ.get("FAKE_CLUSTERS", "[]")); sys.exit(0)
    if a[:2] == ["container", "get-server-config"]:             # #1383 gap 8 (tests/fake_region.py)
        print(json.dumps(server_config())); sys.exit(0)
    # FAKE_CREATE_ERROR: the create refused with this stderr (run 18: the
    # per-location limit); no cluster exists, so its describe answers NotFound
    if a[:3] == ["container", "clusters", "create"] and os.environ.get("FAKE_CREATE_ERROR"):
        sys.stderr.write(os.environ["FAKE_CREATE_ERROR"] + "\\n"); sys.exit(1)
    if a[:3] == ["container", "clusters", "describe"] and os.environ.get("FAKE_CREATE_ERROR"):
        sys.stderr.write("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, "
                         "message=Not found: clusters/" + a[3] + ".\\n")
        sys.exit(1)
    if a[:3] == ["container", "clusters", "get-credentials"]:
        name, p, z = a[3], opt("--project"), opt("--region")   # regional control plane
        ctx = f"gke_{p}_{z}_{name}"
        with open(os.environ["KUBECONFIG"], "w") as fh:
            json.dump({"apiVersion": "v1", "kind": "Config",
                       "current-context": ctx,
                       "contexts": [{"name": ctx, "context": {"cluster": ctx}}],
                       "clusters": [{"name": ctx, "cluster": {"server": "https://127.0.0.1:1"}}],
                       "users": []}, fh)
        sys.exit(0)
    if a[:3] == ["compute", "regions", "describe"]:            # #1171: every quota of the region
        print(json.dumps(region_quotas(a[3]))); sys.exit(0)     # (tests/fake_region.py; FAKE_USAGE_<METRIC>)
    if a[:3] == ["compute", "project-info", "describe"]:       # #1171: the project's global quotas
        answer_read("project-info", project_quotas())
    if a[:2] in (["compute", "instances"], ["compute", "forwarding-rules"], ["compute", "addresses"]) \\
            and a[2] == "list" and "--filter" not in a:          # #1173: what holds the region's addresses
        answer_read(a[1], region_list(a[1]))
    if a[:2] in (["compute", "addresses"], ["compute", "forwarding-rules"]):
        # #1026: the region's addresses / forwarding rules (FAKE_ADDRESSES: the addresses GCE lists)
        if a[2] == "list":
            print(os.environ.get("FAKE_ADDRESSES", "[]") if a[1] == "addresses" else "[]")
        sys.exit(0)
    if a[:3] == ["compute", "disks", "list"]:                  # #895: the disk backstop
        if os.environ.get("FAKE_DISKS_FAIL") == "1":
            sys.exit(1)
        print(os.environ.get("FAKE_DISKS", "[]")); sys.exit(0)
    # #1383: FAKE_NEVER_GONE=1: GKE accepted the delete, and its describe still answers
    if a[:3] == ["container", "clusters", "describe"] and os.path.exists(os.path.join(STATE, "deleted")) \\
            and os.environ.get("FAKE_NEVER_GONE") != "1":
        sys.stderr.write("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, "
                         "message=Not found: clusters/" + a[3] + ".\\n")
        sys.exit(1)
    if a[:3] == ["container", "clusters", "describe"]:
        # FAKE_REGIONAL_NOTFOUND=1: the region answers NotFound (gcloud's real
        # text, the account a placeholder); a same-named zonal bench cluster
        # answers in the zone (#900 review: run606's teardown must not go there)
        if os.environ.get("FAKE_REGIONAL_NOTFOUND") == "1" and "--region" in a:
            sys.stderr.write("ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, message=Not "
                             f"found: projects/{opt('--project')}/locations/{opt('--region')}/clusters/{a[3]}. "
                             "This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid which is the "
                             "active account specified by the [core/account] property.\\n")
            sys.exit(1)
        labels = {"purpose": "snowplow-bench", "issue": "606",
                  "created-by": "bench-gke", "owner": "t",
                  "ttl-expires": str(int(time.time()) + 3600)}
        # what GKE reports for the regional cluster gke-up made (#899 F1 records it)
        print(json.dumps({"name": a[3], "id": "d8cac43bc0ac45d28fe8bdd1ccbe101a68fc42887613486d883c5066a9fc7796", "location": opt("--region") or opt("--zone"),
                          "endpoint": "127.0.0.1",    # the server get-credentials writes below
                          "locations": ["europe-west4-a"], "currentNodeCount": 5,
                          # #1383 gap 7: the versions GKE gave it (FAKE_GKE_VERSION: another patch)
                          "currentMasterVersion": os.environ.get("FAKE_GKE_VERSION") or GKE_VERSION,
                          "currentNodeVersion": os.environ.get("FAKE_GKE_VERSION") or GKE_VERSION,
                          "nodePools": [{"name": n, "config": {"machineType": m}, "initialNodeCount": c,
                                         "locations": ["europe-west4-a"],
                                         "version": os.environ.get("FAKE_GKE_VERSION") or GKE_VERSION}
                                        for n, m, c in (("default-pool", "n2-standard-8", 3),
                                                        ("snowplow", "n2-standard-8", 1),
                                                        ("loadgen", "n2-standard-8", 1))],
                          "resourceLabels": labels}))
        sys.exit(0)
    if a[:3] == ["container", "clusters", "delete"]:
        # #1194: FAKE_AT_DELETE=<script>: what the disk holds at the moment the cluster goes
        if os.environ.get("FAKE_AT_DELETE"):
            import subprocess
            subprocess.run(["bash", "-c", os.environ["FAKE_AT_DELETE"]], check=True)
        time.sleep(float(os.environ.get("FAKE_SLOW_DOWN", "0")))   # a delete in flight
        if os.environ.get("FAKE_DOWN_FAIL") == "1":
            sys.exit(1)
        # #1297: the cluster's deletion ends every pod still on it (the fake kubectl's in-cluster
        # Jobs, when a signal cut the teardown's own collect and deletes short): TERMed, as the
        # kubelet would, and waited for
        import glob, signal
        for pf in glob.glob(os.path.join(STATE, "pods", "*", "pid")):
            pid = int(open(pf).read())
            try:
                os.killpg(pid, signal.SIGTERM)
            except OSError:
                continue
            for _ in range(150):
                try:
                    os.killpg(pid, 0)
                except OSError:
                    break
                time.sleep(0.1)
        open(os.path.join(STATE, "deleted"), "w").write("1")
        sys.exit(0)
    if a[:3] == ["container", "clusters", "create"]:
        if os.environ.get("FAKE_AT_CREATE"):                     # #1194: the disk changes mid-run
            import subprocess
            subprocess.run(["bash", "-c", os.environ["FAKE_AT_CREATE"]], check=True)
        time.sleep(float(os.environ.get("FAKE_SLOW_UP", "0")))   # a hung create
    sys.exit(0)   # create / node-pools create / delete
    """)

_CANON = textwrap.dedent("""\
    # The API server answers every quantity in its CANONICAL form (resource.Quantity: binary
    # suffixes kept binary, decimal ones decimal, the largest suffix that keeps the number whole:
    # 8192Mi -> 8Gi, 1000m -> 1, 0.5 -> 500m), never as it was written: runs 24d / 25b's data606
    # placement compared the strings and failed. Every pod this fake lists is answered that way.
    def canon(q):
        import re
        from fractions import Fraction
        m = re.fullmatch(r"([0-9.]+)(?:[eE]([+-]?[0-9]+))?(Ki|Mi|Gi|Ti|Pi|Ei|n|u|m|k|M|G|T|P|E|)", str(q))
        if not m:
            return q
        num, exp, suf = m.groups()
        v = Fraction(num) * (Fraction(10) ** int(exp) if exp else 1)
        binary = ["Ki", "Mi", "Gi", "Ti", "Pi", "Ei"]
        if suf in binary:
            v *= 1024 ** (binary.index(suf) + 1)
            for i in range(len(binary) - 1, -1, -1):
                if v.denominator == 1 and v.numerator % (1024 ** (i + 1)) == 0:
                    return str(v.numerator // (1024 ** (i + 1))) + binary[i]
            return str(v.numerator) if v.denominator == 1 else q
        dec = {"n": -9, "u": -6, "m": -3, "": 0, "k": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}
        v *= Fraction(10) ** dec[suf]
        for e, sf in ((18, "E"), (15, "P"), (12, "T"), (9, "G"), (6, "M"), (3, "k"), (0, ""), (-3, "m"),
                      (-6, "u"), (-9, "n")):
            x = v / Fraction(10) ** e
            if x.denominator == 1:
                return str(x.numerator) + sf
        return q
    def canon_containers(cs):
        out = []
        for c in cs or []:
            c = dict(c)
            r = c.get("resources")
            if r:
                c["resources"] = {k: {n: canon(q) for n, q in (v or {}).items()} for k, v in r.items()}
            out.append(c)
        return out
    """)

FAKE_KUBECTL = _COMMON + _CANON + fake_gke_api.LOADER + textwrap.dedent("""\
    def slow_read(secs):
        # a read that hangs `secs` (it ignores --request-timeout, as an open exec stream does), and says in
        # slow-reads.jsonl how it ended: "cut" by a TERM (after how long), or "completed" (it ran its whole hang)
        if secs <= 0:
            return
        import signal
        t0 = time.monotonic()
        def note(how):
            with open(os.path.join(STATE, "slow-reads.jsonl"), "a") as fh:
                fh.write(json.dumps({"argv": sys.argv[1:], "how": how,
                                     "after_s": round(time.monotonic() - t0, 3)}) + "\\n")
        def cut(*_):
            note("cut")
            os._exit(143)
        signal.signal(signal.SIGTERM, cut)
        time.sleep(secs)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        note("completed")
    raw = sys.argv[1:]
    kc = os.environ.get("KUBECONFIG"); ctx = None; a = []; ns = None; imp = []
    it = iter(raw)
    for x in it:
        if x.startswith("--kubeconfig="): kc = x.split("=", 1)[1]
        elif x.startswith("--context="): ctx = x.split("=", 1)[1]
        elif x in ("-n", "--namespace"): ns = next(it, None)   # anywhere, like kubectl
        elif x.startswith("--request-timeout="): pass          # global flag, anywhere
        elif x.startswith(("--as=", "--as-group=")): imp.append(x)   # impersonation (#922 F3)
        elif x.startswith("-v="): pass                         # verbosity
        else: a.append(x)
    resolved = ctx
    if resolved is None:
        try:
            resolved = json.load(open(kc))["current-context"]
        except Exception:
            resolved = None
    record("kubectl", kc, resolved)
    # #1051: FAKE_VERDICT_SNAPSHOT=<file>: the first call after verdict.json exists copies it
    _snap = os.environ.get("FAKE_VERDICT_SNAPSHOT")
    _vj = os.path.join(os.environ.get("OUT", ""), "verdict.json")
    if _snap and not os.path.exists(_snap) and os.path.exists(_vj) and os.path.getsize(_vj) > 0:
        open(_snap, "wb").write(open(_vj, "rb").read())
    # #1194: FAKE_VERDICT_LOST=1: the verdict is gone by the teardown's first read
    if os.environ.get("FAKE_VERDICT_LOST") == "1" and os.path.exists(_vj) and os.path.getsize(_vj) > 0:
        os.remove(_vj)
    port = os.environ["FAKE_PORT"]
    if a[:2] == ["auth", "can-i"]:
        # #1307 L7: GKE's IAM authorizer's answer to the orchestrator's SelfSubjectAccessReview;
        # FAKE_K8S_AUTHZ=no: the conditional container.admin is not honoured (kubectl: "no", rc 1)
        ans = os.environ.get("FAKE_K8S_AUTHZ", "yes")
        print(ans); sys.exit(0 if ans == "yes" else 1)
    if a[:3] == ["create", "secret", "generic"]:
        # the ghcr pull Secret and the image-pull Secret: their values come on stdin
        with open(os.path.join(STATE, "created-secrets.jsonl"), "a") as fh:
            fh.write(json.dumps({"argv": a, "stdin": sys.stdin.read(), "ns": ns}) + chr(10))
        print(f"secret/{a[3]} created"); sys.exit(0)
    if a[:1] == ["apply"] and "-f" in a:
        # every object applied, by apiVersion and kind (tests/test_crd_schema.py: the
        # schema step's check names every non-built-in kind the run applies)
        import yaml
        f = a[a.index("-f") + 1]
        text = sys.stdin.read() if f == "-" else (open(f).read() if os.path.isfile(f) else "")
        if "name: ttl-delete-proof" in text:          # run 26h: the proof Job, whose probes GKE answers (logs)
            open(os.path.join(STATE, "ttl-proof-job.yaml"), "w").write(text)
        with open(os.path.join(STATE, "applied-kinds.jsonl"), "a") as fh, \\
                open(os.path.join(STATE, "applied-jobs"), "a") as jobs:
            for d in yaml.safe_load_all(text):
                if isinstance(d, dict):
                    fh.write(json.dumps([d.get("apiVersion"), d.get("kind")]) + "\\n")
                    if d.get("kind") == "ValidatingAdmissionPolicy":      # the teardown's guards
                        open(os.path.join(STATE, "vap-" + d["metadata"]["name"]), "w").write("1")
                    if d.get("kind") == "Job":
                        jobs.write((d.get("metadata") or {}).get("name", "") + "\\n")
        # #1383 gap 4: FAKE_TTL_APPLY_FAIL=1: the API server refuses gke-up's TTL CronJob
        if os.environ.get("FAKE_TTL_APPLY_FAIL") == "1" and "ttl-self-destruct" in text:
            sys.stderr.write("Error from server (InternalError): cronjobs.batch is refused\\n"); sys.exit(1)
    # #1383 gap 3 / run 26h: gke-up's TTL proof Job (the CronJob's pod spec calling the GKE API as the node SA on two
    # names that do not exist), GKE answering per tests/fake_gke_api.py: FAKE_TTL_PROOF=granted (default: node-sa's
    # grant) | denied (no grant: 403 for both)
    if a[:3] == ["get", "job", "ttl-delete-proof"]:
        print(json.dumps({"kind": "Job", "status": {"succeeded": 1}})); sys.exit(0)
    if a[:2] == ["get", "pods"] and "job-name=ttl-delete-proof" in a:       # its pod, on a platform node
        print(json.dumps({"items": [{"metadata": {"name": "ttl-delete-proof-x"},
                                     "spec": {"nodeName": "gke-node-proof"}}]})); sys.exit(0)
    if a[:3] == ["get", "node", "gke-node-proof"]:
        print(json.dumps({"metadata": {"name": a[2], "labels": {"bench.krateo.io/pool": "platform"}}})); sys.exit(0)
    if a[:2] == ["logs", "job/ttl-delete-proof"]:
        p = os.path.join(STATE, "ttl-proof-job.yaml")
        sys.stdout.write(ttl_proof_logs(open(p).read() if os.path.exists(p) else "",
                                        os.environ.get("FAKE_TTL_PROOF", "granted")))
        sys.exit(0)
    # #1297: the in-cluster Jobs (bench/incluster.py) RUN here, as their pods would: the container's
    # own command, its /out at STATE/pods/<job>/out, its kubectl this fake (the run's kubeconfig),
    # started when the Job is applied, TERMed (as the kubelet would) when the Job is deleted.
    # FAKE_INC_FAILED=<job>: that Job's pod fails before its program runs (an init container that failed);
    # FAKE_INC_POOL=<pool>: the pods land on a node of that pool; FAKE_INC_LEFT=1: data606's pod is
    # still listed when the drivers are applied; FAKE_INC_PENDING=<job>: that Job's pod is never
    # scheduled (Pending, its PodScheduled condition carrying run 24b's FailedScheduling message).
    INC = ("data606", "observe606")
    def inc_dir(job):
        return os.path.join(STATE, "pods", job)
    def inc_alive(job):
        return (os.path.exists(os.path.join(inc_dir(job), "pid"))
                and not os.path.exists(os.path.join(inc_dir(job), "deleted")))
    if a[:1] == ["apply"] and "-f" in a and os.path.isfile(a[a.index("-f") + 1]):
        import subprocess, yaml
        docs = [d for d in yaml.safe_load_all(open(a[a.index("-f") + 1])) if isinstance(d, dict)]
        secrets = {d["metadata"]["name"]: d.get("stringData") or {} for d in docs if d.get("kind") == "Secret"}
        inc_jobs = [d for d in docs if d.get("kind") == "Job" and (d.get("metadata") or {}).get("name") in INC]
        for d in inc_jobs:
            job = (d.get("metadata") or {}).get("name")
            if d.get("kind") != "Job" or job not in INC:
                continue
            os.makedirs(os.path.join(inc_dir(job), "out"), exist_ok=True)
            json.dump(d, open(os.path.join(inc_dir(job), "job.json"), "w"))
            if job in (os.environ.get("FAKE_INC_FAILED"), os.environ.get("FAKE_INC_PENDING")):
                continue
            c = d["spec"]["template"]["spec"]["containers"][0]
            out = os.path.join(inc_dir(job), "out")
            argv = [out if x == "/out" else x for x in c["command"]]
            argv[argv.index("--linger-s") + 1] = "60"
            env = dict(os.environ, HOME=os.path.join(inc_dir(job)))
            for e in c.get("env", []):
                if e["name"] in ("PATH", "KUBECONFIG", "PYTHONPATH", "HOME"):
                    continue
                ref = (e.get("valueFrom") or {}).get("secretKeyRef")
                env[e["name"]] = secrets[ref["name"]][ref["key"]] if ref else e["value"]
            p = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, start_new_session=True,
                                 stdout=open(os.path.join(inc_dir(job), "stdout.log"), "ab"), stderr=subprocess.STDOUT)
            open(os.path.join(inc_dir(job), "pid"), "w").write(str(p.pid))
        if inc_jobs:
            sys.exit(0)
    # run 24b: a Job whose pod is never scheduled (FAKE_PSI=pending, FAKE_PREFLIGHT=pending): its pod as
    # the API server lists it, Pending with the scheduler's own reason on its PodScheduled condition
    stuck = {"psi606": os.environ.get("FAKE_PSI") == "pending",
             "preflight606": os.environ.get("FAKE_PREFLIGHT") == "pending"}
    if a[:2] == ["get", "pods"] and "json" in a and any(x in ("job-name=" + j for j, on in stuck.items() if on) for x in a):
        job = next(x for x in a if x.startswith("job-name=")).split("=", 1)[1]
        print(json.dumps({"items": [{"metadata": {"name": job + "-0"}, "spec": {}, "status": {
            "phase": "Pending", "conditions": [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                                                "message": "0/5 nodes are available: 1 Insufficient cpu."}]}}]}))
        sys.exit(0)
    inc_sel = next((x.split("=", 1)[1] for x in a if x.startswith("job-name=") and x.split("=", 1)[1] in INC), None)
    if a[:2] == ["get", "pods"] and inc_sel:
        job = inc_sel
        applied = os.path.exists(os.path.join(inc_dir(job), "job.json"))
        listed = applied and (not os.path.exists(os.path.join(inc_dir(job), "deleted"))
                              or (job == "data606" and os.environ.get("FAKE_INC_LEFT") == "1"))
        failed = os.environ.get("FAKE_INC_FAILED") == job
        pending = os.environ.get("FAKE_INC_PENDING") == job
        done = os.path.exists(os.path.join(inc_dir(job), "out", "done"))
        phase = "Failed" if failed else "Pending" if pending else "Running"
        jp = next((x for x in a if x.startswith("jsonpath=")), "")
        if "json" in a:
            spec = json.load(open(os.path.join(inc_dir(job), "job.json")))["spec"]["template"]["spec"] if listed else {}
            # FAKE_INC_OOM=<job>: by collect its container was OOM-killed (the pod Failed)
            st = ({"phase": "Failed", "containerStatuses": [{"name": spec["containers"][0]["name"], "ready": False,
                                                              "restartCount": 0, "state": {"terminated": {
                                                                  "reason": "OOMKilled", "exitCode": 137}}}]}
                  if listed and os.environ.get("FAKE_INC_OOM") == job else {"phase": phase})
            if pending:
                st["conditions"] = [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                                     "message": "0/5 nodes are available: 1 Insufficient memory, 1 node(s) had untolerated taint(s), 3 node(s) didn't match Pod's node affinity/selector."}]
            print(json.dumps({"items": [{"metadata": {"name": job + "-0"},
                                         "spec": {**({} if pending else {"nodeName": "loadgen-0"}),
                                                  "containers": canon_containers(spec.get("containers", []))},
                                         "status": st}] if listed else []}))
            sys.exit(0)
        if "name" in a:
            print("pod/" + job + "-0" if listed else ""); sys.exit(0)
        if ".metadata.name" in jp:
            print(job + "-0 " if listed else ""); sys.exit(0)
        if "containerStatuses" in jp:
            print(f"{phase}/{'true' if done else 'false'}" if listed else ""); sys.exit(0)
        if ".status.phase" in jp:
            print(phase if listed else ""); sys.exit(0)
        sys.exit(0)
    # review M1: the live loadgen node and the pods on it. FAKE_INC_ALLOC="<cpu> <memory>": its
    # allocatable (default an n2-standard-16's, k6_job.LOADGEN_ALLOCATABLE); FAKE_INC_DS="<cpu> <memory>":
    # the requests of the DaemonSet pods already there (default run 23's plan: the 1000m / 4096Mi
    # reserve it kept); the observer's pod counts once it runs
    if a[:2] == ["get", "nodes"] and "bench.krateo.io/pool=loadgen" in a:
        cpu, mem = os.environ.get("FAKE_INC_ALLOC", "15890m 59825Mi").split()
        print(json.dumps({"items": [{"metadata": {"name": "loadgen-0", "labels": {"bench.krateo.io/pool": "loadgen"}},
                                     "status": {"allocatable": {"cpu": cpu, "memory": mem}}}]}))
        sys.exit(0)
    if a[:2] == ["get", "pods"] and "spec.nodeName=loadgen-0" in a:
        cpu, mem = os.environ.get("FAKE_INC_DS", "1000m 4096Mi").split()
        pods = [{"metadata": {"namespace": "kube-system", "name": "gke-daemonsets"},
                 "spec": {"containers": [{"name": "ds", "resources": {"requests": {"cpu": canon(cpu), "memory": canon(mem)}}}]},
                 "status": {"phase": "Running"}}]
        if inc_alive("observe606"):
            spec = json.load(open(os.path.join(inc_dir("observe606"), "job.json")))["spec"]["template"]["spec"]
            pods.append({"metadata": {"namespace": "bench-loadgen", "name": "observe606-0",
                                      "labels": {"job-name": "observe606"}},
                         "spec": {"containers": canon_containers(spec["containers"]),
                                  "initContainers": canon_containers(spec["initContainers"])},
                         "status": {"phase": "Running"}})
        print(json.dumps({"items": pods}))
        sys.exit(0)
    if a[:2] == ["get", "node"] and "loadgen-0" in a:
        print(json.dumps({"metadata": {"name": "loadgen-0", "labels": {
            "bench.krateo.io/pool": os.environ.get("FAKE_INC_POOL", "loadgen"),
            "node.kubernetes.io/instance-type": "n2-standard-16"}}}))
        sys.exit(0)
    inc_pod = next((j for j in INC for x in a if x == j + "-0" or x.startswith(j + "-0:")), None)
    if inc_pod and a[:1] == ["exec"]:
        import subprocess
        cmd = a[a.index("--") + 1:] if "--" in a else a[-2:]
        out = os.path.join(inc_dir(inc_pod), "out")
        cmd = [x.replace("/out", out) for x in cmd]
        sys.exit(subprocess.run(cmd).returncode)
    if inc_pod and a[:1] == ["cp"]:
        import shutil
        shutil.copytree(os.path.join(inc_dir(inc_pod), "out"), a[-1], dirs_exist_ok=True)
        sys.exit(0)
    if inc_pod and a[:1] == ["logs"]:
        print("tools: kubectl (fake) verified; kubeconfig as this pod's ServiceAccount"); sys.exit(0)
    if a[:1] == ["delete"]:
        with open(os.path.join(STATE, "deleted.jsonl"), "a") as fh:
            fh.write(json.dumps({"ns": ns, "names": [x for x in a[1:] if not x.startswith("-")],
                                 "t": time.time()}) + chr(10))
        for job in INC:
            if f"job/{job}" in a and os.path.exists(os.path.join(inc_dir(job), "job.json")):
                open(os.path.join(inc_dir(job), "deleted"), "w").write("1")
                pf = os.path.join(inc_dir(job), "pid")
                if os.path.exists(pf):
                    import signal
                    try:
                        os.killpg(int(open(pf).read()), signal.SIGTERM)
                    except OSError:
                        pass
                    for _ in range(100):        # --cascade=foreground --wait: until the pod is gone
                        try:
                            os.killpg(int(open(pf).read()), 0)
                        except OSError:
                            break
                        time.sleep(0.1)
                    else:
                        try:
                            os.killpg(int(open(pf).read()), signal.SIGKILL)
                        except OSError:
                            pass
    # #1004: the full-widen stage's objects, its Job's /out and the CRD's deletion.
    # FAKE_FW_JOB: ok (default: arms once applied, exits once the widened CRD is
    # applied) | ends_unarmed (exits without arming) | never (never arms);
    # FAKE_FW_SHORT=k: k objects fewer than applied exist; FAKE_FW_WAIT_HANG=s:
    # the CRD's Established wait hangs; FAKE_FW_STREAM: the stream /out holds.
    fw_crd = "fullwidens.fullwiden.bench.krateo.io"
    def fw_note(what):
        with open(os.path.join(STATE, "fw-events"), "a") as fh:
            fh.write(json.dumps([time.time(), what]) + "\\n")
    if a[:1] == ["apply"] and "-f" in a and "/full-widen/" in a[a.index("-f") + 1]:
        fw_note("apply " + os.path.basename(a[a.index("-f") + 1]))
    if a[:1] == ["wait"] and f"crd/{fw_crd}" in a:
        time.sleep(float(os.environ.get("FAKE_FW_WAIT_HANG", "0")))
        sys.exit(0)
    if a[:2] == ["get", fw_crd]:
        kinds = open(os.path.join(STATE, "applied-kinds.jsonl")).read().splitlines()
        n = sum(1 for k in kinds if json.loads(k) == ["fullwiden.bench.krateo.io/v1alpha1", "FullWiden"])
        print("\\n".join(f"bench-ns-01 o{i} 1s" for i in range(max(0, n - int(os.environ.get("FAKE_FW_SHORT", "0"))))))
        sys.exit(0)
    if a[:3] == ["get", "deploy", "snowplow"]:
        # as the chart renders it: the env through envFrom its ConfigMap (FAKE_FW_DEPLOY_ENV: an env list too)
        env = json.loads(os.environ.get("FAKE_FW_DEPLOY_ENV", "[]"))
        print(json.dumps({"metadata": {"name": "snowplow"}, "spec": {"template": {"spec": {"containers": [
            {"name": "snowplow", "envFrom": [{"configMapRef": {"name": "snowplow"}}], "env": env}]}}}}))
        sys.exit(0)
    fw_events = (open(os.path.join(STATE, "fw-events")).read() if os.path.exists(os.path.join(STATE, "fw-events"))
                 else "")
    if a[:2] == ["get", "crd"] and "json" in a and "apply crd.yaml" in fw_events:
        # #1004 F7: every CRD's name, uid and generation, read at the window's two ends.
        # The widen bumps the stage CRD's generation; FAKE_FW_OTHER_CRD_BUMP=1: another
        # CRD's spec changes in the window too; FAKE_FW_CRDS_FAIL=1: the read fails
        if os.environ.get("FAKE_FW_CRDS_FAIL") == "1":
            print("Error from server (Forbidden): customresourcedefinitions is forbidden", file=sys.stderr)
            sys.exit(1)
        widened = "apply crd-widened.yaml" in fw_events
        bump = os.environ.get("FAKE_FW_OTHER_CRD_BUMP") == "1" and widened
        print(json.dumps({"items": [
            {"metadata": {"name": "installers.composition.krateo.io", "uid": "u-inst", "generation": 3 + bump}},
            {"metadata": {"name": "users.basic.authn.krateo.io", "uid": "u-users", "generation": 1}},
            {"metadata": {"name": fw_crd, "uid": "u-fw", "generation": 1 + widened}}]}))
        sys.exit(0)
    if a[:2] == ["get", "pods"] and "job-name=fullwiden606" in a:
        print("fullwiden606-0" if "apply job.yaml" in fw_events else ""); sys.exit(0)
    if a[:1] == ["exec"] and "fullwiden606-0" in a and a[-2:] == ["touch", "/out/widened"]:
        fw_note("exec touch /out/widened"); sys.exit(0)
    if a[:1] == ["exec"] and "fullwiden606-0" in a:
        mode = os.environ.get("FAKE_FW_JOB", "ok")
        armed = mode == "ok" and "apply job.yaml" in fw_events
        exited = (mode == "ok" and "exec touch /out/widened" in fw_events) or mode == "ends_unarmed"
        fw_note("exec " + a[-1] + " " + ("yes" if {"/out/armed": armed, "/out/exited": exited}[a[-1]] else "no"))
        sys.exit(0 if {"/out/armed": armed, "/out/exited": exited}[a[-1]] else 1)
    if a[:1] == ["cp"] and any(x.startswith("fullwiden606-0:") for x in a):
        os.makedirs(a[-1], exist_ok=True)
        src = os.environ.get("FAKE_FW_STREAM")
        open(os.path.join(a[-1], "widen.jsonl"), "w").write(open(src).read() if src else "")
        fw_note("cp"); sys.exit(0)
    if a[:2] == ["delete", "crd"] and fw_crd in a:
        fw_note("delete crd"); sys.exit(0)
    if a[:1] == ["delete"] and any(x.startswith("bench.krateo.io/full-widen") for x in a):
        fw_note("delete " + " ".join(x for x in a if "," in x or x.endswith(".io"))); sys.exit(0)
    # run 16 attempt 2: the API server stops answering for a while mid-measure.
    # FAKE_API_DOWN_POLLS=N: the first N driver-wait reads (the job status and
    # the driver pods) fail as kubectl does then; FAKE_API_DOWN_WAITS=N: the
    # first N fresh606 waits and their probes too. Then the API answers again.
    down_flag = os.path.join(STATE, "api_down")         # an outage in progress: /readyz unanswered too
    def unanswered():
        open(down_flag, "w").write("1")
        # FAKE_UNANSWERED_HANG_S: an unanswered call hangs this long before
        # kubectl gives up (its --request-timeout); when it was SENT is logged
        with open(os.path.join(STATE, "unanswered_sent"), "a") as fh:
            fh.write(f"{time.time()}\\n")
        time.sleep(float(os.environ.get("FAKE_UNANSWERED_HANG_S", "0")))
        print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr)
        sys.exit(1)
    def api_blip(var, counter):
        n_max = int(os.environ.get(var, "0"))
        if not n_max:
            return
        p = os.path.join(STATE, counter)
        n = int(open(p).read()) if os.path.exists(p) else 0
        if n < n_max:
            open(p, "w").write(str(n + 1))
            unanswered()
        if os.path.exists(down_flag):
            os.remove(down_flag)
    collect_started = os.path.join(STATE, "collect_started")
    collect_down = (os.environ.get("FAKE_COLLECT_PODS_UNANSWERED") == "1" and os.path.exists(collect_started))
    if a[:2] == ["get", "--raw=/metrics"]:                      # #919: the APF preflight + sampler
        if os.environ.get("FAKE_METRICS_FORBIDDEN") == "1":
            print('Error from server (Forbidden): User "someone@example.invalid" cannot get path "/metrics"',
                  file=sys.stderr)
            sys.exit(1)
        p = os.path.join(STATE, "metrics_reads")
        n = (int(open(p).read()) if os.path.exists(p) else 0) + 1
        open(p, "w").write(str(n))
        lb = 'flow_schema="global-default",priority_level="global-default"'
        print("# TYPE process_start_time_seconds gauge\\nprocess_start_time_seconds 1.7929e+09")
        print(f"apiserver_flowcontrol_dispatched_requests_total{{{lb}}} {10 * n}")
        print(f"apiserver_flowcontrol_current_inqueue_requests{{{lb}}} {n % 3}")
        for le, c in (("0.005", 9 * n), ("+Inf", 10 * n)):
            print(f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{lb},le="{le}"}} {c}')
        print(f'apiserver_flowcontrol_request_wait_duration_seconds_sum{{execute="true",{lb}}} {0.1 * n}')
        print(f'apiserver_flowcontrol_request_wait_duration_seconds_count{{execute="true",{lb}}} {10 * n}')
        print("apiserver_request_total{code=\\"200\\"} 5")
        sys.exit(0)
    # #922 F3 (#924 M2/M3): APF as the API server answers it. A request's
    # X-Kubernetes-PF-* headers (kubectl -v=8 prints them) name its flow
    # schema; once the bench's FlowSchemas are applied, each identity lands
    # in its own (FAKE_BENCH_FS_IGNORED=1: they never take, verify fails).
    fs_uid = {"service-accounts": "00000000-0000-4000-8000-000000000001",
              "global-default": "00000000-0000-4000-8000-000000000002",
              "bench-snowplow-sa": "00000000-0000-4000-8000-000000000011",
              "bench-snowplow-users": "00000000-0000-4000-8000-000000000012",
              "bench-drivers": "00000000-0000-4000-8000-000000000013"}
    pl_uid = {"workload-low": "00000000-0000-4000-8000-000000000101",
              "global-default": "00000000-0000-4000-8000-000000000102"}
    pl_of = {"service-accounts": "workload-low", "global-default": "global-default",
             "bench-snowplow-sa": "workload-low", "bench-snowplow-users": "global-default",
             "bench-drivers": "workload-low"}
    prec = {"service-accounts": 9000, "global-default": 9900, "bench-snowplow-sa": 8999,
            "bench-snowplow-users": 9899, "bench-drivers": 8999}
    bench_fs = (os.path.exists(os.path.join(STATE, "bench_flowschemas"))
                and os.environ.get("FAKE_BENCH_FS_IGNORED") != "1")
    if a[:1] == ["apply"] and "-f" in a and os.environ.get("FAKE_USER_CRD"):
        # the API server validates an applied User against authn's CRD (its spec.required),
        # as the real one refused all 670 fresh Users of run 17 attempt 4
        import yaml
        crd = yaml.safe_load(open(os.environ["FAKE_USER_CRD"]))
        need = list(crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"].get("required") or [])
        need += [x for x in os.environ.get("FAKE_USER_REQUIRE", "").split(",") if x]
        bad = []
        f = a[a.index("-f") + 1]
        for d in yaml.safe_load_all(open(f)) if os.path.isfile(f) else []:
            if isinstance(d, dict) and d.get("kind") == "User":
                for k in need:
                    if k not in (d.get("spec") or {}):
                        bad.append(f'Error from server (Invalid): User.basic.authn.krateo.io "{d["metadata"]["name"]}" '
                                   f"is invalid: spec.{k}: Required value")
        if bad:
            sys.stderr.write("\\n".join(bad) + "\\n")
            sys.exit(1)
    if a[:1] == ["apply"] and any(x.endswith("bench-flowschemas.yaml") for x in a):
        open(os.path.join(STATE, "bench_flowschemas"), "w").write(open(a[-1]).read())
        sys.exit(0)
    if a[:2] == ["get", "flowschemas.flowcontrol.apiserver.k8s.io"]:
        names = [n for n in fs_uid if bench_fs or not n.startswith("bench-")]
        print(json.dumps({"items": [{"metadata": {"name": n, "uid": fs_uid[n]},
                                     "spec": {"matchingPrecedence": prec[n],
                                              "priorityLevelConfiguration": {"name": pl_of[n]}}} for n in names]}))
        sys.exit(0)
    if a[:2] == ["get", "prioritylevelconfigurations.flowcontrol.apiserver.k8s.io"]:
        print(json.dumps({"items": [{"metadata": {"name": n, "uid": u}, "spec": {}} for n, u in pl_uid.items()]}))
        sys.exit(0)
    if a[:2] == ["get", "--raw"] and imp:
        who = imp[0].split("=", 1)[1]
        # FAKE_BENCH_FS_IGNORED_FOR=<identity>: that identity's bench schema never takes
        ign = os.environ.get("FAKE_BENCH_FS_IGNORED_FOR", "")
        if who.startswith("system:serviceaccount:krateo-system:"):
            fs = "bench-snowplow-sa" if bench_fs and ign != "snowplow_sa" else "service-accounts"
        elif who.startswith("system:serviceaccount:bench-loadgen:"):
            fs = "bench-drivers" if bench_fs and ign != "bench_drivers" else "service-accounts"
        else:
            fs = "bench-snowplow-users" if bench_fs and ign != "snowplow_users" else "global-default"
        print("I1027 00:00:00.000000 1 round_trippers.go:560] Response Status: 403 Forbidden in 3 milliseconds\\n"
              "I1027 00:00:00.000000 1 round_trippers.go:567] Response Headers:\\n"
              f"I1027 00:00:00.000000 1 round_trippers.go:570]     X-Kubernetes-Pf-Flowschema-Uid: {fs_uid[fs]}\\n"
              f"I1027 00:00:00.000000 1 round_trippers.go:570]     X-Kubernetes-Pf-Prioritylevel-Uid: {pl_uid[pl_of[fs]]}",
              file=sys.stderr)
        print('Error from server (Forbidden): configmaps is forbidden', file=sys.stderr)
        sys.exit(1)
    if a[:2] == ["get", "deploy/snowplow"]:
        print(json.dumps({"metadata": {"name": "snowplow", "namespace": ns},
                          "spec": {"template": {"spec": {"serviceAccountName": "snowplow"}}}}))
        sys.exit(0)
    if a[:2] == ["get", "--raw=/readyz"]:                       # run606's api_answers probe
        if os.path.exists(down_flag) or collect_down:
            print("Unable to connect to the server: dial tcp 10.0.0.2:443: i/o timeout", file=sys.stderr)
            sys.exit(1)
        print("ok"); sys.exit(0)
    drivers = ("load606", "k6load", "k6browser")
    # FAKE_ANSWERED_ERROR=job|pods: the API server ANSWERS that read with an
    # error (NotFound): a real answer, not an outage
    ans = os.environ.get("FAKE_ANSWERED_ERROR")
    if ans == "job" and a[:1] == ["get"] and any(x in (f"job/{d}" for d in drivers) for x in a):
        print('Error from server (NotFound): jobs.batch "k6load" not found', file=sys.stderr); sys.exit(1)
    if ans == "pods" and a[:2] == ["get", "pods"] and any(x in (f"job-name={d}" for d in drivers) for x in a) \
            and ("json" in a or "name" in a):
        print('Error from server (Forbidden): pods is forbidden: User "someone@example.invalid" cannot list resource "pods"',
              file=sys.stderr); sys.exit(1)
    def count(name, n_max):   # True for the first n_max calls counted under `name`
        p =os.path.join(STATE, name)
        n = int(open(p).read()) if os.path.exists(p) else 0
        if n < n_max:
            open(p, "w").write(str(n + 1))
            return True
        return False
    driver_job_read = a[:1] == ["get"] and any(x in (f"job/{d}" for d in drivers) for x in a)
    # FAKE_EDGE_READS=N (the #898 re-reviewer's): the last read of an outage — it
    # timed out while the API server came back, so /readyz right after it
    # ANSWERS (no down flag). Waited out, never a verdict.
    if driver_job_read and count("edge_reads", int(os.environ.get("FAKE_EDGE_READS", "0"))):
        print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr)
        sys.exit(1)
    # FAKE_EDGE_PODS=N / FAKE_EDGE_WAITS=N / FAKE_EDGE_COLLECT=N: the same edge
    # on the driver pod listings, the fresh606 wait, and collect's pod listings
    if (not os.path.exists(collect_started) and a[:2] == ["get", "pods"] and "json" in a
            and any(x in (f"job-name={d}" for d in drivers) for x in a)
            and count("edge_pods", int(os.environ.get("FAKE_EDGE_PODS", "0")))):
        print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr)
        sys.exit(1)
    if a[:1] == ["wait"] and "job/fresh606" in a and count("edge_waits", int(os.environ.get("FAKE_EDGE_WAITS", "0"))):
        print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr)
        sys.exit(1)
    if (os.path.exists(collect_started) and a[:2] == ["get", "pods"] and "json" in a
            and any(x.startswith("job-name=") for x in a)
            and count("edge_collect", int(os.environ.get("FAKE_EDGE_COLLECT", "0")))):
        print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr)
        sys.exit(1)
    # FAKE_LOCAL_ERROR=1: kubectl's own configuration fails every driver job
    # read (the context does not exist): no API server is reached
    if driver_job_read and os.environ.get("FAKE_LOCAL_ERROR") == "1":
        print('error: context "gke_bench" does not exist', file=sys.stderr); sys.exit(1)
    # FAKE_LOGGED_OUT_READS=N: the first N driver job reads: logged out
    # (the operator can log in again: waited out)
    if driver_job_read and count("logged_out", int(os.environ.get("FAKE_LOGGED_OUT_READS", "0"))):
        print("error: You must be logged in to the server (Unauthorized)", file=sys.stderr); sys.exit(1)
    # FAKE_5XX_READS=N: the first N driver job reads answered with a 5xx while
    # /readyz answers: an overloaded / restarting server, waited out
    if driver_job_read and count("5xx_reads", int(os.environ.get("FAKE_5XX_READS", "0"))):
        print("Error from server (InternalError): an error on the server has prevented the request",
              file=sys.stderr); sys.exit(1)
    # FAKE_ANSWERED_ONCE=1: the first driver job status read is answered
    # NotFound, the re-read right after it is fine: no verdict on one answer
    if driver_job_read and "jsonpath" in " ".join(a) and os.environ.get("FAKE_ANSWERED_ONCE") == "1" \
            and count("answered_once", 1):
        print('Error from server (NotFound): jobs.batch "k6load" not found', file=sys.stderr); sys.exit(1)
    if collect_down and a[:2] == ["get", "pods"] and any(x.startswith("job-name=") for x in a) and "json" in a:
        unanswered()
    # FAKE_EXIT_CODES_UNANSWERED=1: the driver pods' name listing (their exit
    # codes' read) goes unanswered, once
    if (os.environ.get("FAKE_EXIT_CODES_UNANSWERED") == "1" and a[:2] == ["get", "pods"]
            and any(x in (f"job-name={d}" for d in drivers) for x in a) and "name" in a
            and not os.path.exists(os.path.join(STATE, "exit_codes_unanswered"))):
        open(os.path.join(STATE, "exit_codes_unanswered"), "w").write("1")
        print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr); sys.exit(1)
    # FAKE_WAIT_ANSWERED=1: the fresh606 wait, and a get of the Job, ANSWERED
    # with an error while /readyz answers: a real answer, never waited out
    if os.environ.get("FAKE_WAIT_ANSWERED") == "1" and (
            (a[:1] == ["wait"] and "job/fresh606" in a) or a[:2] == ["get", "job/fresh606"]):
        print('Error from server (Forbidden): jobs.batch "fresh606" is forbidden', file=sys.stderr); sys.exit(1)
    if a[:1] == ["get"] and (any(x in (f"job/{d}" for d in drivers) for x in a)
                             or (any(x in (f"job-name={d}" for d in drivers) for x in a) and "json" in a)):
        api_blip("FAKE_API_DOWN_POLLS", "api_down_polls")
    if (a[:1] == ["wait"] and "job/fresh606" in a) or (a[:2] == ["get", "job/fresh606"] and "name" in a):
        api_blip("FAKE_API_DOWN_WAITS", "api_down_waits")
    # FAKE_API_EMPTY_PODS=N: the first N driver pod listings answer rc 0 with
    # an empty body (a proxy in between, a cut stream): proves nothing either
    if a[:2] == ["get", "pods"] and any(x in (f"job-name={d}" for d in drivers) for x in a) and "json" in a:
        n_max = int(os.environ.get("FAKE_API_EMPTY_PODS", "0"))
        cnt = os.path.join(STATE, "api_empty_pods")
        n = int(open(cnt).read()) if os.path.exists(cnt) else 0
        if n < n_max:
            open(cnt, "w").write(str(n + 1)); sys.exit(0)
    if a[:2] == ["get", "jobs"] and "name" in a:                  # collect's reachability probe
        api_blip("FAKE_API_DOWN_COLLECT", "api_down_collect")
        open(collect_started, "w").write("1")
    if a[:2] == ["config", "current-context"]:
        if resolved is None: sys.exit(1)
        print(resolved); sys.exit(0)
    if a[:1] == ["annotate"]:
        kv = a[-1]
        if "bench-app-01-01" in a and kv.startswith("snowplow-bench/fresh-marker="):
            open(os.path.join(STATE, "marker"), "w").write(kv.split("=", 1)[1])
        sys.exit(0)
    ready = os.environ.get("FAKE_INSTALLER_READY", "1") == "1"
    kog = os.environ.get("FAKE_KOG", "0") == "1"           # run-3 private-registry 401
    kog_reason = os.environ.get("FAKE_KOG_REASON", "response status code 401: unauthorized")
    extra = os.environ.get("FAKE_EXTRA_NOT_READY", "0") == "1"
    def cr(kind, name, ok, reason="Available", msg="", synced=True, smsg=""):
        return {"kind": kind, "metadata": {"name": name, "namespace": "krateo-system"},
                "status": {"conditions": [{"type": "Ready", "status": "True" if ok else "False",
                                           "reason": reason, "message": msg},
                                          {"type": "Synced", "status": "True" if synced else "False",
                                           "reason": "ReconcileSuccess" if synced else "ReconcileError",
                                           "message": smsg}]}}
    def items(xs):
        print(json.dumps({"items": xs})); sys.exit(0)
    if a[:2] == ["config", "view"]:                            # the teardown's endpoint check
        print(json.load(open(kc))["clusters"][0]["cluster"]["server"], end=""); sys.exit(0)
    if a[:2] == ["create", "--dry-run=server"]:                # the teardown guards' probes
        import yaml
        d = yaml.safe_load(sys.stdin.read()) or {}
        for kind, pol in (("Service", "bench-teardown-no-new-loadbalancers"),
                          ("PersistentVolumeClaim", "bench-teardown-no-new-claims")):
            if d.get("kind") == kind and os.path.exists(os.path.join(STATE, "vap-" + pol)):
                print(f"ValidatingAdmissionPolicy '{pol}' with binding '{pol}' denied request", file=sys.stderr)
                sys.exit(1)
        sys.exit(0)
    if a[:2] == ["get", "svc"] and "--all-namespaces" in a:    # #1026: the LB release at teardown
        dl = os.path.join(STATE, "svcs_deleted")
        gone = set(open(dl).read().split()) if os.path.exists(dl) else set()
        lbs = json.loads(os.environ.get("FAKE_LB_SVCS", "[]"))
        print(json.dumps({"items": [x for x in lbs if x["metadata"]["name"] not in gone]})); sys.exit(0)
    if a[:2] == ["delete", "svc"]:
        with open(os.path.join(STATE, "svcs_deleted"), "a") as fh:
            fh.write(a[2] + " ")
        sys.exit(0)
    if a[:2] == ["get", "pv"]:                                 # #895: the PV release at teardown
        gone = os.path.exists(os.path.join(STATE, "pvcs_deleted"))
        items([] if gone else json.loads(os.environ.get("FAKE_PVS", "[]")))
    if a[:2] == ["delete", "pvc"]:
        open(os.path.join(STATE, "pvcs_deleted"), "w").write("1"); sys.exit(0)
    f483 = os.environ.get("FAKE_483", "")          # "", pass, wide
    if a[:3] == ["get", "crd", "alerts.observability.krateo.io"]:
        sys.exit(0 if f483 in ("pass", "wide") else 1)
    if a[:3] == ["rollout", "status", "deploy/frontend"] and os.environ.get("FAKE_FRONTEND_ROLLOUT_FAIL") == "1":
        # run 26: the frontend's rollout never completes (a pod still Pending)
        print('error: timed out waiting for the condition', file=sys.stderr)
        sys.exit(1)
    if (a[:2] == ["get", "pods"] and "app.kubernetes.io/name=frontend" in a and "json" in a
            and os.environ.get("FAKE_FRONTEND_DIGEST")):
        # run 26: the frontend's pod, its imageID at FAKE_FRONTEND_DIGEST (FRONTEND_TAG runs)
        with open(os.path.join(STATE, "frontend_pod_reads"), "a") as fh:
            fh.write("1")
        print(json.dumps({"items": [{"metadata": {"name": "frontend-abc"}, "status": {"containerStatuses": [{
            "name": "frontend", "ready": True, "image": "ghcr.io/krateo-platformops/frontend:1.8.38",
            "imageID": "ghcr.io/krateo-platformops/frontend@" + os.environ["FAKE_FRONTEND_DIGEST"]}]}}]}))
        sys.exit(0)
    if a[:2] == ["get", "pods"] and "app.kubernetes.io/name=snowplow" in a and "json" in a:
        # snowplow's own pod: optionally OOM-restarted between the before/after
        # records (FAKE_RESTART_DURING_MEASURE=1). Before the installer's helm install
        # (run 24: run606's pre-install tracing reading) there is no snowplow pod.
        if not os.path.exists(os.path.join(STATE, "installer-installed")):
            print(json.dumps({"items": []})); sys.exit(0)
        cnt = os.path.join(STATE, "snowplow_pod_reads")
        n = int(open(cnt).read()) if os.path.exists(cnt) else 0
        open(cnt, "w").write(str(n + 1))
        # read 0 = the digest gate, 1 = the before record, 2+ = after
        restarted = os.environ.get("FAKE_RESTART_DURING_MEASURE") == "1" and n >= 2
        cs = {"name": "snowplow", "ready": True, "restartCount": 1 if restarted else 0,
              "image": "ghcr.io/krateo-platformops/snowplow:1.12.50",
              "imageID": "ghcr.io/krateo-platformops/snowplow@" + os.environ["FAKE_IMAGE_DIGEST"],
              "lastState": ({"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                            "finishedAt": "2026-10-14T15:04:53Z"}}
                            if restarted else {})}
        # #922 F3: started a second ago, Ready now (the boot's end)
        now = time.time()
        cs["state"] = {"running": {"startedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 1))}}
        # run 23: its env as the chart renders it (envFrom its ConfigMap; HOST_IP from the
        # downward API; the endpoint entry that expands it), on node n1
        pods = [{"metadata": {"name": "snowplow-abc"},
                 "spec": {"nodeName": "n1", "containers": [{
                     "name": "snowplow", "resources": {"limits": {"memory": "8Gi"}},
                     "envFrom": [{"configMapRef": {"name": "snowplow"}}],
                     "env": [{"name": "HOST_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}},
                             {"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://$(HOST_IP):4318"}]}]},
                 "status": {"hostIP": "10.0.0.5", "containerStatuses": [cs],
                            "conditions": [{"type": "Ready", "status": "True", "lastTransitionTime":
                                            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))}]}}]
        # FAKE_TERMINATING_READS=N: the first N reads also list the old
        # ReplicaSet's Terminating pod (old digest) FIRST; -1 = it never goes.
        tr = int(os.environ.get("FAKE_TERMINATING_READS", "0"))
        if tr < 0 or n < tr:
            old = {"name": "snowplow", "ready": False, "restartCount": 0,
                   "imageID": "ghcr.io/krateo-platformops/snowplow@sha256:" + "ee" * 32}
            pods.insert(0, {"metadata": {"name": "snowplow-old",
                                         "deletionTimestamp": "2026-10-19T08:00:00Z"},
                            "spec": {"containers": [{}]},
                            "status": {"containerStatuses": [old]}})
        print(json.dumps({"items": pods}))
        sys.exit(0)
    if a[:2] == ["top", "pod"]:
        print("snowplow-abc 120m 650Mi"); sys.exit(0)
    if a[:1] == ["cp"] and any("mem606" in x for x in a):
        dest = a[-1]
        os.makedirs(dest, exist_ok=True)
        open(os.path.join(dest, "heap-01-150000-ready.pb.gz"), "wb").write(b"\\x1f\\x8bfake")
        recs = [{"kind": "mem_start", "t": 0.5},
                {"kind": "mem", "t": 1.0, "readyz": 200, "vars_status": 200,
                 "heap_inuse_mb": 1392.8, "sys_mb": 1551.9},
                {"kind": "heap_profile", "t": 1.0, "profile": "heap-01-150000-ready.pb.gz",
                 "bytes": 152190, "at": "ready"},
                {"kind": "mem", "t": 16.0, "readyz": 200, "vars_status": 200,
                 "heap_inuse_mb": 3789.4, "sys_mb": 6741.5},
                {"kind": "mem_end", "t": 17.0, "samples": 2, "profiles": 1, "stopped": True},
                {"kind": "mem_exit", "rc": 0, "t": 17}]
        if os.environ.get("FAKE_MEM_STREAM") == "cut":           # copied mid-stream
            recs = recs[:-2]
        with open(os.path.join(dest, "mem.jsonl"), "w") as fh:      # /out/mem.jsonl (#817)
            for r in recs:
                fh.write(json.dumps(r) + "\\n")
        sys.exit(0)
    if a[:1] == ["exec"] and a[-3:] == ["test", "-f", "/out/exited"] \\
            and os.environ.get("FAKE_MEM_NEVER_EXITS") == "1":
        time.sleep(float(os.environ.get("FAKE_EXEC_SLOW", "0")))     # a slow exec
        sys.exit(1)
    if a[:1] == ["exec"] and a[-2:] == ["cat", "/out/done"]:
        print(os.environ.get("FAKE_DRIVE_RC", "0")); sys.exit(0)   # drive's exit code
    if a[:1] == ["cp"] and any(x.startswith(("k6load", "k6browser")) for x in a):
        # #830: the k6 / browser pod's /out. FAKE_K6_OUT: whole | dropped
        # (the burst could not start one iteration) | nobrowser | viewercut
        # (the viewer sidecar saw no feed_end) | nosse (no live-update stream)
        import gzip
        name = "k6" if any(x.startswith("k6load") for x in a) else "browser"
        mode = os.environ.get("FAKE_K6_OUT", "whole")
        if name == "browser" and mode == "nobrowser":
            print("error: unable to upgrade connection", file=sys.stderr); sys.exit(1)
        dest = a[-1]
        os.makedirs(dest, exist_ok=True)
        def pt(m, v, **tags):
            # the browser's points carry their time (2026-10-21T10:00:00Z): its page loads are placed
            # against the CPU windows (the starvation check); k6's stay untimed
            data = {"value": v, "tags": tags}
            if name == "browser":
                data["time"] = "2026-10-21T10:00:00Z"
            return json.dumps({"metric": m, "type": "Point", "data": data})
        lines = [pt("bench_frame", 1, frame="start")]
        if name == "k6":
            for i in range(7):
                t = ({"kind": "first_load", "route": "/dashboard", "visit": "first"} if i < 3
                     else {"kind": "route", "route": "/dashboard", "visit": "warm"})
                lines += [pt("page_load_ms", 80 + i, **t), pt("page_loads", 1, **t)]
            cut = mode == "viewercut"
            vrecs = ([{"kind": "viewer_start", "t": 1}]
                     + [{"kind": "viewer_armed", "user": "u%d" % u, "nav": "navigate:%d" % u,
                         "t": 2, "arm_lag_ms": 5.0} for u in range(3)]
                     + [{"kind": "viewer_stats", "t": 3, "streams_opened": 3},
                        {"kind": "viewer_end", "t": 4, "rc": 3 if cut else 0,
                         "ended_by": "k6 exited without feed_end: the feed is cut" if cut
                         else "feed_end", "navs": 7}])
            with open(os.path.join(dest, "viewer.jsonl"), "w") as fh:
                fh.write("".join(json.dumps(r) + "\\n" for r in vrecs))
            with open(os.path.join(dest, "viewer-exit.json"), "w") as fh:
                json.dump({"kind": "viewer_exit", "rc": 3 if cut else 0}, fh)
            m = {"page_loads": {"count": 7}, "http_req_failed": {"rate": 0},
                 "route_table_mismatch": {"count": 0}}
            for sc, n in (("navigate", 3), ("composite_burst", 1)):
                m["iterations{scenario:%s}" % sc] = {"count": n}
                m["dropped_iterations{scenario:%s}" % sc] = {"count": 0}
            # "dropped": one of the 2 burst users' page loads never completed
            m["page_loads{scenario:composite_burst}"] = {"count": 1 if mode == "dropped" else 2}
            m = {"metrics": m, "config": {"burstUsers": 2}}
        else:
            for s in ("s1", "s2", "s3"):
                lines.append(pt("browser_logins", 1, session=s))
                for stream in ("refreshes", "notifications"):
                    lines.append(pt("browser_sse_opened", 0 if mode == "nosse" else 1,
                                    session=s, stream=stream))
                for i in range(20):
                    t = {"session": s, "route": "/r%d" % (i % 4),
                         "visit": "first" if i < 4 else "warm", "method": "click"}
                    # starved / saturated: loads over the warm bar that the stall alone can explain
                    ms = (600 if mode in ("starved", "saturated") else 300) + i
                    # each load's own start (pageload.recordOps' t0), inside the sampled windows
                    t["t0"] = str(1792576800000 - 3000 + 40 * (20 * int(s[1:] or 0) + i))
                    lines += [pt("browser_page_load_ms", ms, **t),
                              pt("browser_page_loads", 1, **t)]
            m = {"metrics": {"browser_page_loads": {"count": 60}}}
        lines.append(pt("bench_frame", 1, frame="end"))
        with gzip.open(os.path.join(dest, name + ".json.gz"), "wt") as fh:
            fh.write("\\n".join(lines) + "\\n")
        with open(os.path.join(dest, name + "-summary.json"), "w") as fh:
            json.dump(m, fh)
        # the wrappers' CPU samples (k6_job._cpu_sampler), anchored so the windows cover the
        # browser's page loads (2026-10-21T10:00:00Z = 1792576800). "starved": Chrome's cgroup
        # stalled every whole window; "saturated": the node had no idle tick; "busy": Chrome far
        # over its request with no stall; "nopressure": no cpu.pressure to read
        for c, req in ((("k6", 4000), ("viewer", 1000)) if name == "k6" else (("browser", 3000),)):
            cores = 2.9 if (c == "browser" and mode in ("starved", "busy")) else 1.0
            stall = 5e6 if (c == "browser" and mode == "starved") else 0
            idle = 0 if (c == "browser" and mode == "saturated") else 400
            recs = [{"kind": "cpu_start", "container": c, "request_m": req, "limit_m": None,
                     "node_cpus": 16, "epoch": 1792576800 - 15, "uptime": 100.0, "uptime_lo": 99.98}]
            recs += [{"kind": "cpu", "t": 100.0 + 5 * i, "usage_usec": int(cores * 5e6 * i),
                      "throttled_usec": 0,
                      "pressure_some_usec": None if mode == "nopressure" else int(stall * i),
                      "pressure_full_usec": None if mode == "nopressure" else 0,
                      "node_busy_ticks": 1000 * i, "node_idle_ticks": idle * i} for i in range(6)]
            recs.append({"kind": "cpu_end", "epoch": 1792576800 - 15 + 26, "uptime": 126.0, "uptime_lo": 125.98})
            with open(os.path.join(dest, c + "-cpu.jsonl"), "w") as fh:
                fh.write("".join(json.dumps(r) + "\\n" for r in recs))
        with open(os.path.join(dest, name + "-exit.json"), "w") as fh:
            json.dump({"kind": name + "_exit", "rc": 0}, fh)
        sys.exit(0)
    if a[:1] == ["cp"] and any(x.startswith("load606") for x in a):
        # The driver's /out/nav.jsonl (#817). FAKE_DRIVE_STREAM: whole (a framed
        # stream) | headcut (run 9: only the tail after log rotation) | nofile.
        slow_read(float(os.environ.get("FAKE_SLOW_LOGS", "0")))     # a slow / hung read
        mode = os.environ.get("FAKE_DRIVE_STREAM", "whole")
        if mode == "nofile":
            print("tar: /out/nav.jsonl: Cannot stat", file=sys.stderr); sys.exit(1)
        navs = [{"kind": "nav", "user": "scaleuser-0001", "page": "p", "visit": v,
                 "wall_ms": 50, "calls": 3, "errors": 0, "sched_lag_ms": 1} for v in (1, 2)]
        recs = ([{"kind": "drive_start", "t": 1.0}] + navs
                + [{"kind": "drive_end", "t": 2.0, "ok": True, "rc": 0, "reason": None,
                    "navs": 2}, {"kind": "drive_exit", "rc": 0, "t": 2}])
        if mode == "headcut":
            recs = [{"kind": "push_refetch", "t_event": 1.5, "status": 200},
                    {"kind": "vars", "t": 1.6}]
        with open(a[-1], "w") as fh:
            fh.writelines(json.dumps(r) + "\\n" for r in recs)
        sys.exit(0)
    if a[:1] == ["get"] and "json" in a:
        what = a[1]
        if what == "crd":
            kinds = [("installers", "Installer"), ("authns", "Authn"), ("snowplows", "Snowplow"),
                     ("frontends", "Frontend"), ("portals", "Portal")]
            if f483:
                kinds += [("alertprovidercrds", "AlertProviderCrds"),
                          ("krateoobservabilities", "KrateoObservability")]
            if f483 == "wide":
                kinds.append(("alertproviders", "AlertProvider"))
            items([{"spec": {"group": "composition.krateo.io",
                             "names": {"plural": p, "kind": k}}} for p, k in kinds])
        if what.startswith("alertprovidercrds."):
            items([cr("AlertProviderCrds", "alert-provider-crd", True)])
        if what.startswith("krateoobservabilities."):
            items([cr("KrateoObservability", "krateo-observability", True)])
        if what.startswith("alertproviders."):
            items([cr("AlertProvider", "alert-provider", True)])
        if what.startswith("alerts.observability"):
            items([{"metadata": {"name": "sre-pod-crashloop", "namespace": "krateo-system"}}])
        if what.startswith("installers."):
            kids = []
            if not ready:
                kids.append("authns/krateo-system/authn")
            if kog:
                kids.append("compositiondefinitions/krateo-system/github-provider-kog")
            if extra:
                kids.append("compositiondefinitions/krateo-system/oasgen-provider")
            msg = (f"{len(kids)} of 41 managed children are not ready: " + ", ".join(kids)) if kids else ""
            # FAKE_INSTALLER_SMSG: the Installer's Synced message (#1006 R3-2: it may name the pull account)
            inst = cr("Installer", "installer", not kids, "Creating" if kids else "Available", msg,
                      smsg=os.environ.get("FAKE_INSTALLER_SMSG", ""))
            # spec.components as the 0.3.459 Installer CR carries it: what it WILL
            # create (a feature-off entry is not created; github-provider-kog is
            # declared, its CR never appears while its definition 401s)
            decl = [("authn", "Authn", None), ("snowplow", "Snowplow", None),
                    ("frontend", "Frontend", None), ("portal", "Portal", None),
                    ("agentgateway", "Agentgateway", "agentGateway")]
            if f483:
                decl += [("alert-provider-crd", "AlertProviderCrds", None),
                         ("krateo-observability", "KrateoObservability", None)]
            if f483 == "wide":
                decl.append(("alert-provider", "AlertProvider", None))
            if kog:
                decl.append(("github-provider-kog", "GithubProviderKog", None))
            if os.environ.get("FAKE_DECLARED_LATER") == "1":   # run 17: deps on portal, not created yet
                decl.append(("dashboard-page", "DashboardPage", None))
            ra = os.path.join(STATE, "helm-registry-auth.json")
            inst["spec"] = {"features": {"portal": True, "agentGateway": False},
                            "components": [{"name": n, "kind": k, "feature": f or "portal",
                                            **({"deps": ["portal"]} if n == "dashboard-page" else {})}
                                           for n, k, f in decl]}
            if os.path.exists(ra):
                inst["spec"]["registryAuth"] = json.load(open(ra))
            items([inst])
        if what.startswith("authns."):
            items([cr("Authn", "authn", ready, "ReconcileError",
                      "helm install failed: token=" + os.environ.get("FAKE_TOKVAL", ""))])
        if what.startswith("snowplows."):
            items([cr("Snowplow", "snowplow", ready)])
        if what.startswith("frontends.") and "-A" not in a:
            # what the installer RENDERED into the Frontend composition: FAKE_RENDERED = same (default)
            # | split | absent | unread
            mode = os.environ.get("FAKE_RENDERED", "same")
            if mode == "unread":
                print("Error from server (Forbidden): frontends is forbidden", file=sys.stderr); sys.exit(1)
            host = "http://10.0.0.9" if mode == "same" else "http://localhost:808"
            cfg = ({"SNOWPLOW_API_BASE_URL": host + "/content", "AUTHN_API_BASE_URL": host + "/auth",
                    "EVENTS_API_BASE_URL": host + "/events", "EVENTS_PUSH_API_BASE_URL": host + "/events"}
                   if mode == "same" else
                   {"SNOWPLOW_API_BASE_URL": "http://localhost:8081", "AUTHN_API_BASE_URL": "http://localhost:8082",
                    "EVENTS_API_BASE_URL": "http://localhost:8083", "EVENTS_PUSH_API_BASE_URL": "http://localhost:8083"})
            print(json.dumps({"items": [] if mode == "absent" else [{"spec": {"config": cfg}}]}))
            sys.exit(0)
        if what.startswith("frontends."):
            items([cr("Frontend", "frontend", True)])
        if what.startswith("portals."):
            items([cr("Portal", "portal", True)])
        if what.startswith("restdefinitions."):
            if os.environ.get("FAKE_RDS_FORBIDDEN") == "1":
                sys.stderr.write('Error from server (Forbidden): restdefinitions.ogen.krateo.io is forbidden: '
                                 'User "scaleuser-0001" cannot list resource "restdefinitions"\\n')
                sys.exit(1)
            items(json.loads(os.environ.get("FAKE_RDS", "[]")))
        if what.startswith("compositiondefinitions."):
            cds = [cr("CompositionDefinition", "authn", True)]
            if f483:
                cds.append(cr("CompositionDefinition", "alert-provider-crd", True))
            if f483 == "wide":
                cds.append(cr("CompositionDefinition", "alert-provider", True))
            if kog:
                cds.append(cr("CompositionDefinition", "github-provider-kog", False, "Creating", "",
                              synced=False, smsg=(
                                  "observe failed: error getting chart info: failed to get chart "
                                  "after 5 attempts: failed to get chart: failed to get "
                                  "oci://ghcr.io/krateo-blueprints/charts/github-provider-kog: failed "
                                  "to resolve reference ghcr.io/krateo-blueprints/charts/"
                                  "github-provider-kog:0.3.2: HEAD \\"https://ghcr.io/v2/krateo-"
                                  "blueprints/charts/github-provider-kog/manifests/0.3.2\\": GET "
                                  "\\"https://ghcr.io/token?scope=repository%3Akrateo-blueprints%2F"
                                  "charts%2Fgithub-provider-kog%3Apull&service=ghcr.io\\": "
                                  + kog_reason)))
            if extra:
                cds.append(cr("CompositionDefinition", "oasgen-provider", False, "Creating",
                              "chart fetch timeout"))
            items(cds)
        if what == "pods" and any(x.startswith("job-name=") for x in a):
            # an in-cluster Job's pod (#817): lingering, or OOMKilled when
            # FAKE_DRIVER_POD_DEATH=1 and it is the driver's
            job = next(x for x in a if x.startswith("job-name=")).split("=", 1)[1]
            if os.environ.get("FAKE_DRIVER_STUCK") == job:
                # a native sidecar whose startupProbe never passes: Init, restarting
                items([{"metadata": {"name": f"{job}-0", "creationTimestamp": "2026-10-22T09:45:18Z"},
                        "status": {"phase": "Pending",
                                   "initContainerStatuses": [{"name": "forward", "ready": False,
                                       "restartCount": 3, "started": False,
                                       "state": {"running": {}},
                                       "lastState": {"terminated": {"reason": "Error",
                                                                    "exitCode": 137}}}],
                                   "containerStatuses": [{"name": "browser", "ready": False,
                                       "restartCount": 0,
                                       "state": {"waiting": {"reason": "PodInitializing"}}}]}}])
            dead = job == "load606" and os.environ.get("FAKE_DRIVER_POD_DEATH") == "1"
            state = ({"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                     "finishedAt": "2026-10-21T06:40:00Z"}}
                     if dead else {"running": {}})
            items([{"metadata": {"name": f"{job}-0"},
                    "status": {"phase": "Failed" if dead else "Running",
                               "containerStatuses": [{"name": "c", "ready": not dead,
                                                      "restartCount": 0, "state": state}]}}])
        if what == "pods" and any(x.startswith("app.kubernetes.io/name in (") for x in a):
            # the two controllers (diag606 definitions); their images on a registry nothing serves
            items([{"metadata": {"name": n, "namespace": "krateo-system", "labels": {"app.kubernetes.io/name": c}},
                    "spec": {"containers": [{"name": c}]},
                    "status": {"containerStatuses": [{"name": c, "restartCount": r,
                                                      "image": f"127.0.0.1:1/krateo/{c}:x",
                                                      "imageID": f"127.0.0.1:1/krateo/{c}@sha256:" + "0" * 64}]}}
                   for n, c, r in (("oasgen-provider-0", "oasgen-provider", 0),
                                   ("installer-core-provider-0", "core-provider", 1))])
        if what == "pods" and "--field-selector=spec.nodeName=n1" in a:
            # run 23: the pods on snowplow's node; FAKE_OTEL_AGENT = ready (default) | not_ready | none:
            # the collector agent DaemonSet's pod with hostPort 4318
            mode = os.environ.get("FAKE_OTEL_AGENT", "ready")
            agent = {"metadata": {"name": "otel-collector-daemonset-agent-x1", "namespace": "krateo-system",
                                  "ownerReferences": [{"kind": "DaemonSet",
                                                       "name": "otel-collector-daemonset-agent"}]},
                     "spec": {"nodeName": "n1", "containers": [{"name": "otc", "ports": [
                         {"containerPort": 4318, "hostPort": 4318, "protocol": "TCP"}]}]},
                     "status": {"phase": "Running", "conditions": [{"type": "Ready",
                                                                    "status": "True" if mode == "ready" else "False"}]}}
            items([{"metadata": {"name": "snowplow-abc", "namespace": "krateo-system"}, "spec": {"nodeName": "n1"},
                    "status": {"phase": "Running"}}] + ([] if mode == "none" else [agent]))
        if what == "pods":
            pull = []
            if os.environ.get("FAKE_IMAGE_PULL"):
                pull = [{"metadata": {"name": "agentgateway-policies-ui-token-hop-7d9f-x2", "namespace": "krateo-system"},
                         "spec": {"nodeName": "n1", "imagePullSecrets": [{"name": "krateo-registry-image-pull"}],
                                  "containers": [{"name": "hop", "image": "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18"}]},
                         "status": {"phase": "Pending", "containerStatuses": [{"name": "hop", "ready": False,
                             "restartCount": 0, "image": "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18",
                             "state": {"waiting": {"reason": "ImagePullBackOff",
                                 "message": 'Back-off pulling image "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18"'}}}]}}]
            items(pull + [{"metadata": {"name": "ok-0", "namespace": "krateo-system"},
                    "spec": {"nodeName": "n1"}, "status": {"phase": "Running",
                    "containerStatuses": [{"name": "c", "ready": True, "restartCount": 0, "state": {"running": {}}}]}},
                   {"metadata": {"name": "crash-0", "namespace": "krateo-system"},
                    "spec": {"nodeName": "n1"}, "status": {"phase": "Running",
                    "containerStatuses": [{"name": "app", "ready": False, "restartCount": 3,
                        "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                        "lastState": {"terminated": {"reason": "Error", "exitCode": 1}}}]}}])
        if what == "events" and "reason=FailedToRetrieveImagePullSecret" in a:
            items([{"reason": "FailedToRetrieveImagePullSecret", "type": "Warning",
                    "involvedObject": {"kind": "Pod", "namespace": "krateo-system",
                                       "name": "agentgateway-policies-ui-token-hop-7d9f-x2"},
                    "message": "Unable to retrieve some image pull secrets (krateo-registry-image-pull); "
                               "attempting to pull the image without them"}]
                  if os.environ.get("FAKE_IMAGE_PULL") == "missing_secret" else [])
        if what == "events":
            ev = [{"lastTimestamp": "2026-10-14T08:10:00Z", "type": "Warning",
                   "reason": "BackOff", "involvedObject": {"kind": "Pod", "namespace": "krateo-system", "name": "crash-0"},
                   "message": "Back-off restarting failed container"}]
            if os.environ.get("FAKE_SVC") == "pending":
                ev.append({"lastTimestamp": "2026-10-14T11:17:53Z", "type": "Warning",
                           "reason": "SyncLoadBalancerFailed",
                           "involvedObject": {"kind": "Service", "namespace": "krateo-system", "name": "snowplow"},
                           "message": "Error syncing load balancer: googleapi: Error 403: QUOTA_EXCEEDED - "
                                      "Quota 'IN_USE_ADDRESSES' exceeded.  Limit: 14.0 globally."})
            items(ev)
        if what in ("pvc", "nodes"):
            items([])
    if a[:1] == ["logs"] and ("oasgen-provider-0" in a or "installer-core-provider-0" in a):
        # a controller's log: long, with the line after a failure, and one naming a user
        n = int(os.environ.get("FAKE_CONTROLLER_LOG_LINES", "3000"))
        for i in range(n):
            print(f"2026-10-28T09:4{i % 10}:00Z line {i}")
        print('2026-10-28T09:50:00Z E create failed: User "scaleuser-0001" cannot create restdefinitions')
        print("2026-10-28T09:50:01Z I the line after the failure")
        sys.exit(0)
    if a[:1] == ["logs"] and "crash-0" in a:
        print("starting app")
        print("Authorization: Bearer " + os.environ.get("FAKE_BEARER", ""))
        print("jwt=" + os.environ.get("FAKE_JWT", ""))
        print("login with " + os.environ.get("FAKE_PW", ""))
        print("panic: cannot reach ghcr.io")
        sys.exit(0)
    if a[:1] == ["get"]:
        what = a[1] if len(a) > 1 else ""
        if what == "ns":
            print("bench-ns-01   Active   1m\\nbench-ns-02   Active   1m"); sys.exit(0)
        if what == "crd":
            sys.exit(1)
        if what.startswith("compositiondefinitions"):
            print("True"); sys.exit(0)
        if what.startswith("githubscaffoldingwithcompositionpages"):
            print("\\n".join(f"bench-ns-01 bench-app-01-0{i} 1m" for i in range(1, 5)))
            sys.exit(0)
        if what == "svc" and "krateo-agent-gateway-platform" in a:
            # the platform gateway (#1005): FAKE_GATEWAY = address (default) | none (GCP still
            # provisioning) | absent (NotFound) | unread (an API error) | flap (address, then none on
            # every 2nd read) | blip (address; read 2 is one API error; address again: #1006 R2-1)
            mode = os.environ.get("FAKE_GATEWAY", "address")
            if mode == "blip":
                cnt = os.path.join(STATE, "gateway_reads")
                n = int(open(cnt).read()) if os.path.exists(cnt) else 0
                open(cnt, "w").write(str(n + 1))
                mode = "unread" if n == 1 else "address"
            if mode == "flap":
                cnt = os.path.join(STATE, "gateway_reads")
                n = int(open(cnt).read()) if os.path.exists(cnt) else 0
                open(cnt, "w").write(str(n + 1))
                mode = "address" if n % 2 == 0 else "none"
            if mode == "absent":
                print('Error from server (NotFound): services "krateo-agent-gateway-platform" not found',
                      file=sys.stderr); sys.exit(1)
            if mode == "unread":
                print("Unable to connect to the server: net/http: TLS handshake timeout", file=sys.stderr)
                sys.exit(1)
            ing = [{"ip": "10.0.0.9"}] if mode == "address" else []
            print(json.dumps({"metadata": {"name": "krateo-agent-gateway-platform"},
                              "status": {"loadBalancer": {"ingress": ing}}}))
            sys.exit(0)
        if what in ("deploy", "deployments") and "json" in a and len([x for x in a[2:] if not x.startswith("-")
                                                                     and x != "json"]) == 0:
            # the installer cdc, by FAKE_CDC_RESYNC=<source>:<interval>: args | env | configmap | none
            src, _, iv = os.environ.get("FAKE_CDC_RESYNC", "configmap:60s").partition(":")
            c = {"name": "installers-v0-3-422-controller",
                 "args": ["-group=composition.krateo.io", "-resource=installers"]
                         + ([f"-resync-interval={iv}"] if src == "args" else []),
                 "env": [{"name": "COMPOSITION_CONTROLLER_RESYNC_INTERVAL", "value": iv}] if src == "env" else [],
                 "envFrom": [{"configMapRef": {"name": "installers-v0-3-422-configmap"}}]}
            avail = os.environ.get("FAKE_CDC_AVAILABLE", "True")
            d = {"metadata": {"name": "installers-v0-3-422-controller"},
                 "spec": {"template": {"spec": {"containers": [c]}}},
                 "status": {"conditions": [{"type": "Available", "status": avail}]}}
            items = [] if src == "none" else [d]
            if os.environ.get("FAKE_CDC_COUNT") == "2":
                items.append(dict(d, metadata={"name": "installers-v0-3-421-controller"}))
            print(json.dumps({"items": items}))
            sys.exit(0)
        if what in ("configmap", "configmaps") and "json" in a and "frontend-config-vars" not in a:
            src, _, iv = os.environ.get("FAKE_CDC_RESYNC", "configmap:60s").partition(":")
            data = {"COMPOSITION_CONTROLLER_RESYNC_INTERVAL": iv} if src == "configmap" else {"OTHER": "x"}
            # #1004: snowplow's own ConfigMap, its env through envFrom (FAKE_FW_CM: its data)
            fw_cm = json.loads(os.environ.get("FAKE_FW_CM", '{"DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS": "120"}'))
            # run 23: the chart's OTEL keys in it (FAKE_SNOWPLOW_OTEL: what it holds), last
            # written long before the snowplow container started
            otel = json.loads(os.environ.get("FAKE_SNOWPLOW_OTEL", '{"OTEL_ENABLED": "true", '
                                             '"OTEL_TRACES_SAMPLER": "parentbased_always_on"}'))
            print(json.dumps({"items": [{"metadata": {"name": "installers-v0-3-422-configmap"}, "data": data},
                                        {"metadata": {"name": "snowplow", "creationTimestamp": "2026-01-01T00:00:00Z",
                                                      "managedFields": [{"manager": "helm",
                                                                         "time": "2026-01-01T00:00:00Z"}]},
                                         "data": {**fw_cm, **otel}}]}))
            sys.exit(0)
        if what == "svc":
            mode = os.environ.get("FAKE_SVC", "lb")     # lb | clusterip | pending
            ports = {"snowplow": 8081, "authn": 8082, "frontend": 8080}
            def svc(n):
                typ = "ClusterIP" if mode == "clusterip" else "LoadBalancer"
                ing = [{"ip": "127.0.0.1"}] if mode == "lb" else []
                return {"metadata": {"name": n, "namespace": "krateo-system"},
                        "spec": {"type": typ, "ports": [{"port": ports[n]}]},
                        "status": {"loadBalancer": {"ingress": ing}}}
            print(json.dumps({"items": [svc("snowplow"), svc("authn"), svc("frontend")]}))
            sys.exit(0)
        if what == "pods" and "job-name=load606" in a:
            print("pod/load606-0"); sys.exit(0)
        if what == "pods" and ("job-name=k6load" in a or "job-name=k6browser" in a):
            print("pod/" + next(x for x in a if x.startswith("job-name=")).split("=")[1] + "-0")
            sys.exit(0)
        if what in ("job/k6load", "job/k6browser"):
            busy = (os.environ.get("FAKE_DRIVER_STUCK"), os.environ.get("FAKE_DRIVER_RUNNING"))
            print("0//1" if what[4:] in busy else "1//1")
            sys.exit(0)
        if what == "pods" and "job-name=fresh606" in a:
            print("pod/fresh606-0"); sys.exit(0)
        if what == "pods" and "job-name=mem606" in a:
            print("pod/mem606-0"); sys.exit(0)
        if what == "configmap" and "frontend-config-vars" in a:
            init = os.environ.get("FAKE_INIT", "/call?resource=layouts&apiVersion="
                                  "widgets.templates.krateo.io/v1beta1&name=app-shell"
                                  "&namespace=krateo-system")
            # the installer's path-mode URLs (one origin, 057's paths); FAKE_PORTAL=split:
            # the localhost defaults a config keeps until the gateway has an address;
            # FAKE_PORTAL=after:N: split for the first N reads, then same-origin (#1005)
            portal = os.environ.get("FAKE_PORTAL", "")
            if portal.startswith("after:"):
                cnt = os.path.join(STATE, "portal_reads")
                n = int(open(cnt).read()) if os.path.exists(cnt) else 0
                open(cnt, "w").write(str(n + 1))
                portal = "split" if n < int(portal.split(":")[1]) else ""
            if portal == "split":
                urls = {"SNOWPLOW_API_BASE_URL": "http://localhost:8081",
                        "AUTHN_API_BASE_URL": "http://localhost:8082",
                        "EVENTS_API_BASE_URL": "http://localhost:8083",
                        "EVENTS_PUSH_API_BASE_URL": "http://localhost:8083"}
            else:
                urls = {"SNOWPLOW_API_BASE_URL": "http://10.0.0.9/content",
                        "AUTHN_API_BASE_URL": "http://10.0.0.9/auth",
                        "EVENTS_API_BASE_URL": "http://10.0.0.9/events",
                        "EVENTS_PUSH_API_BASE_URL": "http://10.0.0.9/events"}
            print(json.dumps({"api": {"INIT": init, "BUILDERS_NAMESPACE": "krateo-system", **urls},
                              "params": {"FRONTEND_NAMESPACE": "krateo-system"}}))
            sys.exit(0)
        if (what == "pods" and "app.kubernetes.io/name=snowplow" in a
                and any(x.startswith("jsonpath=") for x in a)):
            # the log stream's live-pod lookup: FAKE_SNOWPLOW_PODS="p1,p2" =
            # p1 for the first lookups, then p2 (a replaced pod)
            names = [n for n in os.environ.get("FAKE_SNOWPLOW_PODS", "").split(",") if n]
            if names:
                cnt = os.path.join(STATE, "live_pod_reads")
                n = int(open(cnt).read()) if os.path.exists(cnt) else 0
                open(cnt, "w").write(str(n + 1))
                print(f"2026-10-21T05:00:00Z {names[min(n // 2, len(names) - 1)]} ")
            sys.exit(0)
        if what == "job/load606":                       # ready/failed/completions
            print("/1/1" if os.environ.get("FAKE_DRIVER_FAILED") == "1" else "1//1")
            sys.exit(0)
        if what == "job" and "psi606" in a:
            # #1042's pre-drive PSI check: answers once applied (else NotFound), and completes
            jf = os.path.join(STATE, "applied-jobs")
            if "psi606" not in (open(jf).read().split() if os.path.exists(jf) else []):
                print('Error from server (NotFound): jobs.batch "psi606" not found', file=sys.stderr)
                sys.exit(1)
            # FAKE_PSI=pending: the Job never finishes (the step's deadline must hold)
            done = os.environ.get("FAKE_PSI") != "pending"
            print("1" if done and any(x.endswith("{.status.succeeded}") for x in a) else ""); sys.exit(0)
        if what == "job" and any(x.startswith("preflight606") for x in a):   # + each menu class's
            # a preflight Job answers only once the run APPLIED it (#999 review B2): else NotFound
            job = next(x for x in a if x.startswith("preflight606"))
            jf = os.path.join(STATE, "applied-jobs")
            if job not in (open(jf).read().split() if os.path.exists(jf) else []):
                print(f'Error from server (NotFound): jobs.batch "{job}" not found', file=sys.stderr)
                sys.exit(1)
            # FAKE_PREFLIGHT=refused: the run's preflight refuses; FAKE_PREFLIGHT_REFUSE_JOB: that Job alone
            refused = (os.environ.get("FAKE_PREFLIGHT", "ok") == "refused" and job == "preflight606"
                       or os.environ.get("FAKE_PREFLIGHT_REFUSE_JOB") == job)
            # FAKE_PREFLIGHT=pending: the run's preflight Job never finishes (its pod is never scheduled)
            if os.environ.get("FAKE_PREFLIGHT") == "pending" and job == "preflight606":
                print(""); sys.exit(0)
            if any(x.endswith("{.status.succeeded}") for x in a):
                print("" if refused else "1"); sys.exit(0)
            if any(x.endswith("{.status.failed}") for x in a):
                print("1" if refused else ""); sys.exit(0)
        sys.exit(0)
    if a[:1] == ["logs"] and "job/psi606" in a:
        # FAKE_PSI: ok | nocgroup (no cpu.pressure) | nonode (no /proc/stat) | sharedns (/proc/self/cgroup
        # names a path: not a private cgroup namespace)
        mode = os.environ.get("FAKE_PSI", "ok")
        for which, f, line in (("cgroup", "/sys/fs/cgroup/cpu.pressure", "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234"),
                               ("node", "/proc/stat", "cpu  100 2 30 400 5 6 7 8 0 0"),
                               ("cgroupns", "/proc/self/cgroup",
                                "0::/kubepods/burstable/pod1/c1" if mode == "sharedns" else "0::/")):
            gone = (mode == "nocgroup" and which == "cgroup") or (mode == "nonode" and which == "node")
            print(json.dumps({"kind": "psi_file", "which": which, "file": f, "some": None if gone else line}))
        sys.exit(0)
    if a[:1] == ["logs"] and any("preflight606" in x for x in a):
        job = next(x for x in a if "preflight606" in x).split("/", 1)[-1]
        # each Job its OWN table: a class's Job adds a route only it has (#999 review B2)
        own = [] if job == "preflight606" else ["/only-" + job.rsplit("-", 1)[1]]
        if (os.environ.get("FAKE_PREFLIGHT", "ok") == "refused" and job == "preflight606"
                or os.environ.get("FAKE_PREFLIGHT_REFUSE_JOB") == job):
            print(json.dumps({"kind": "preflight", "ok": False, "init_status": 200,
                              "routes": [], "refused": [{"path": "/dashboard", "status": 500,
                              "body": "no matches for widgets.templates.krateo.io/v1beta1, Resource=pages"}],
                              "product_errors": []}))
            print("preflight REFUSED: /dashboard -> 500", file=sys.stderr)
        else:
            print(json.dumps({"kind": "preflight", "ok": True, "init_status": 200,
                              "routes": [{"path": p, "status": 200, "endpoint":
                                          "/call?resource=flexes&name=page" + p.replace("/", "-")}
                                         for p in ("/dashboard", "/observability", "/compositions", *own)],
                              "refused": [], "product_errors": [
                                  {"path": "/agents/new", "status": 500,
                                   "body": "unexpected end of JSON input"}],
                              "composite_routes": ([] if os.environ.get("FAKE_PREFLIGHT") == "nocomposite"
                                                   else ["/observability", "/compositions"]),
                              "full_walk_report_only": {"calls": 590, "bytes": 8429613,
                                                        "wall_ms": 17644}}))
        sys.exit(0)
    if a[:1] == ["logs"] and any("fresh606" in x for x in a):
        print("fresh: starting")                       # stderr-ish noise, skipped
        for rate in os.environ.get("FAKE_FRESH_RATES", "1 10 50").split():
            for i in range(2):
                print(json.dumps({"kind": "fresh", "label": f"list_dep@{rate}",
                                  "converged": True, "latency_ms": 120 + i,
                                  "polls": 2, "poll_ms": 100}))
        sys.exit(0)
    if a[:1] == ["logs"] and any("load606" in x for x in a):
        print("[login] baseline"); sys.exit(0)        # diagnostics only (#817)
    if a[:1] == ["wait"] and "job/fresh606" in a:
        time.sleep(float(os.environ.get("FAKE_SLOW_WAIT", "0")))   # measure in progress
    if a[:1] == ["wait"] and "job/fresh606" in a and os.environ.get("FAKE_WAIT_FAIL") == "1":
        print("error: timed out waiting for the condition on jobs/fresh606", file=sys.stderr)
        sys.exit(1)
    snow = next((x for x in a if x == "deploy/snowplow" or x.startswith("pod/snowplow")), None)
    if a[:1] == ["logs"] and snow and any(x.startswith("--since-time=") for x in a) and os.environ.get("FAKE_FW_LOG"):
        # #1004: the stage's read of snowplow's log since its start (FAKE_FW_LOG: its lines)
        for ln in open(os.environ["FAKE_FW_LOG"]).read().splitlines():
            print(ln, flush=True)
        sys.exit(0)
    if a[:1] == ["logs"] and snow and "--previous" not in a:
        ts = "2026-10-21T06:00:00.5Z " if "--timestamps" in a else ""
        if "-f" not in a and snow.startswith("pod/") and os.environ.get("FAKE_SNOWPLOW_PODS"):
            # the replaced pod's tail, read once (#823 review)
            print(ts + json.dumps({"time": "2026-10-21T06:00:00Z", "level": "INFO",
                                   "msg": "old pod tail"}), flush=True)
            sys.exit(0)
        # identity-bearing fields on purpose: the collected copy must not keep them (#822)
        print(ts + json.dumps({"time": "2026-10-21T06:00:00Z", "level": "INFO",
                               "msg": "refreshes: subscribed", "subsystem": "cache",
                               "user": "scaleuser-0001", "name": "scaleuser-0001-clientconfig",
                               "armed_keys": 3, "-f": "-f" in a}), flush=True)
        print(ts + "snowplow plain line", flush=True)
        if "-f" in a and os.environ.get("FAKE_FOLLOW_FOREVER") == "1":
            # a follower that never ends on its own, like a live `kubectl logs -f`
            with open(os.path.join(STATE, "followers"), "a") as fh:
                fh.write(f"{os.getpid()}\\n")
            i = 0
            while True:
                i += 1
                print(f"2026-10-21T06:00:{i % 60:02d}.{i:09d}Z " + json.dumps(
                    {"time": "2026-10-21T06:00:01Z", "msg": "tick"}), flush=True)
                time.sleep(0.2)
        sys.exit(0)
    sys.exit(0)   # apply / wait / rollout / version / scale / top / logs
    """)

FAKE_HELM = _COMMON + textwrap.dedent("""\
    a = sys.argv[1:]
    kc = a[a.index("--kubeconfig") + 1] if "--kubeconfig" in a else os.environ.get("KUBECONFIG")
    ctx = a[a.index("--kube-context") + 1] if "--kube-context" in a else None
    record("helm", kc, ctx)
    ver = a[a.index("--version") + 1] if "--version" in a else "0"
    if a[:2] == ["show", "chart"]:
        if os.environ.get("FAKE_CHART_MISSING") == "1":
            print("Error: not found", file=sys.stderr); sys.exit(1)
        print(f"apiVersion: v2\\nname: installer\\nversion: {ver}"); sys.exit(0)
    CRD_CHARTS = os.environ["FAKE_CRD_CHARTS"]    # the pinned CRD charts, vendored (tests/fixtures/crd_charts)
    def crd_chart(name, dest):
        # a CRD chart as the registry serves it; FAKE_CRD_REQUIRE="Kind:field,..." serves a
        # CRD whose spec requires one more field (what authn-crds did to the fresh classes)
        import shutil, yaml
        shutil.copytree(os.path.join(CRD_CHARTS, name), dest)
        req = [x.split(":", 1) for x in os.environ.get("FAKE_CRD_REQUIRE", "").split(",") if x]
        tdir = os.path.join(dest, "templates")
        for f in sorted(os.listdir(tdir)) if req else []:
            crd = yaml.safe_load(open(os.path.join(tdir, f)))
            for kind, field in req:
                if crd["spec"]["names"]["kind"] == kind:
                    for v in crd["spec"]["versions"]:
                        spec = v["schema"]["openAPIV3Schema"]["properties"]["spec"]
                        spec["required"] = list(spec.get("required") or []) + [field]
                        spec.setdefault("properties", {})[field] = {"type": "string"}
            yaml.safe_dump(crd, open(os.path.join(tdir, f), "w"))
    # installer#614: the jetstack cert-manager chart, the REAL archive (tests/fixtures), so its
    # pinned sha256 verifies; FAKE_CERT_MANAGER_TAMPER=1 serves it with one byte flipped
    if a[:1] == ["pull"] and a[1] == "oci://quay.io/jetstack/charts/cert-manager":
        data = open(os.environ["FAKE_CERT_MANAGER_TGZ"], "rb").read()
        if os.environ.get("FAKE_CERT_MANAGER_TAMPER") == "1":
            data = data[:-1] + bytes([data[-1] ^ 1])
        open(os.path.join(a[a.index("-d") + 1], f"cert-manager-{ver}.tgz"), "wb").write(data)
        sys.exit(0)
    if a[:1] == ["pull"] and not a[1].endswith("/installer"):
        chart = a[1].rsplit("/", 1)[-1]
        if not os.path.isdir(os.path.join(CRD_CHARTS, chart)):
            print(f"Error: {a[1]}:{ver}: not found", file=sys.stderr); sys.exit(1)
        crd_chart(chart, os.path.join(a[a.index("-d") + 1], chart))
        sys.exit(0)
    if a[:1] == ["template"]:
        os.execv(sys.executable, [sys.executable, os.environ["FAKE_HELM_TEMPLATE_BIN"], *a])
    if a[:1] == ["pull"]:
        d = os.path.join(a[a.index("-d") + 1], "installer")
        os.makedirs(os.path.join(d, "files"), exist_ok=True)
        os.makedirs(os.path.join(d, "charts"), exist_ok=True)
        # a chart helm loads (#1395: the installer chart is rendered, its private-image list read)
        import io, tarfile
        cp = json.dumps({"apiVersion": "v2", "name": "core-provider", "version": "2.13.24"}).encode()
        with tarfile.open(os.path.join(d, "charts", "core-provider-2.13.24.tgz"), "w:gz") as tf:
            ti = tarfile.TarInfo("core-provider/Chart.yaml"); ti.size = len(cp)
            tf.addfile(ti, io.BytesIO(cp))
        crd_chart("core-provider-crds", os.path.join(d, "charts", "core-provider-crds"))
        open(os.path.join(d, "values.yaml"), "w").write(
            "ociRepo: oci://ghcr.io/krateo-platformops/charts" + chr(10)
            + "features: {portal: true, agentGateway: false, specialistAgents: false}" + chr(10)
            # as 0.3.468: the installer turns the dashboard's ui-token-hop on through componentValues
            + "componentValues: {agentgateway-policies: {uiTokenHop: {enabled: true}}}" + chr(10))
        os.makedirs(os.path.join(d, "templates"), exist_ok=True)
        core = os.environ.get("FAKE_CORE_FEATURES", "")
        if core == "dynamic":
            core = "{{ .Values.coreFeatures }}"
        # inst.privateImageComponents as at 0.3.468, rendered by the preflight (#1395). FAKE_PRIVATE_IMAGES:
        # none = no such define; empty = one that lists nothing; commented = installer 0.3.616's form, a
        # template comment inside the list (run 26f); dynamic = one entry from a rule over the values
        # (alert-provider's only while the portal feature is on, which the installer's values turn on)
        mode = os.environ.get("FAKE_PRIVATE_IMAGES", "")
        entries = ["- name: core-provider-agent", "  path: [mcpServers, chartGate, imagePullSecrets]",
                   "- name: agentgateway-policies", "  path: [certReplayHop, image, pullSecrets]",
                   "- name: agentgateway-policies", "  path: [uiTokenHop, image, pullSecrets]"]
        last = {"commented": ["{{/* a comment, as installer 0.3.616 has one inside its list */}}",
                              "- name: alert-provider", "  path: [imagePullSecrets]"],
                "dynamic": ["{{- if .Values.features.portal }}", "- name: alert-provider",
                            "  path: [imagePullSecrets]", "{{- end }}"]}.get(
            mode, ["- name: alert-provider", "  path: [imagePullSecrets]"])
        priv = {"none": "", "empty": '{{- define "inst.privateImageComponents" -}}components: []{{- end -}}'
                }.get(mode, chr(10).join(['{{- define "inst.privateImageComponents" -}}', "components:", *entries,
                                          *last, "{{- end -}}"]))
        open(os.path.join(d, "templates", "_helpers.tpl"), "w").write(
            '{{- define "inst.coreFeatures" -}}' + core + '{{- end -}}' + chr(10) + priv + chr(10))
        open(os.path.join(d, "Chart.yaml"), "w").write(json.dumps({
            "apiVersion": "v2", "name": "installer", "version": ver,
            "dependencies": [{"name": "core-provider", "version": "2.13.24"},
                             {"name": "core-provider-crds", "version": "2.13.40"}]}))
        good = os.environ.get("FAKE_PINS", "good") == "good"
        open(os.path.join(d, "files", "component-pins.yaml"), "w").write(json.dumps({
            "components": [
                {"name": "authn-crd", "kind": "AuthnCrds", "chart": "authn-crds", "version": "0.27.3"},
                {"name": "snowplow-crd", "kind": "SnowplowCrds", "chart": "snowplow-crds", "version": "1.12.56"},
                {"name": "alert-provider-crd", "feature": "portal" if good else "specialistAgents"},
                # as at 0.3.468: on through portal, no repo of its own (the installer's ociRepo), a
                # private-image component (#1041 review F1 / N1)
                {"name": "alert-provider", "version": "0.2.53", "feature": "portal"},
                {"name": "krateo-observability", "feature": "portal",
                 "deps": ["clickhouse-operator"] + (["alert-provider-crd"] if good else [])},
                # PRIVATE (krateo-agentiko), as at 0.3.468: the ghcr pull token resolves them
                {"name": "agentgateway-controller", "version": os.environ.get("FAKE_AGW_VERSION", "0.2.0"),
                 "repo": "oci://ghcr.io/krateo-agentiko/charts", "feature": "agentGateway"},
                {"name": "agentgateway-policies", "version": os.environ.get("FAKE_POLICIES_VERSION", "0.2.18"),
                 "repo": "oci://ghcr.io/krateo-agentiko/charts", "feature": "agentGateway",
                 "deps": ["agentgateway-controller"]},
                # public repo overrides, as at 0.3.468 (one featureless: always on)
                {"name": "krateo-marketplace-catalog", "version": "0.1.2",
                 "repo": "oci://ghcr.io/krateo-blueprints/charts", "feature": "portal"},
                {"name": "github-provider-kog", "version": "0.6.0", "repo": "oci://ghcr.io/krateo-blueprints/charts"},
                # a private pin of a feature that is off unless the chart makes it a core feature,
                # at a version the fake registry has not published: preflighted only when on
                {"name": "incident-agent", "version": "9.9.9", "repo": "oci://ghcr.io/krateo-agentiko/charts",
                 "feature": "specialistAgents"}]
                # run 26: the frontend, pinned as 0.3.545 pins it (no repo of its own) at
                # FAKE_FRONTEND_PIN (0.3.545's own is 1.8.34); by default one that carries the login
                # form contract (bench/login_contract.py, run 26d); "none": no frontend pin
                + ([{"name": "frontend", "kind": "Frontend", "chart": "frontend",
                     "version": os.environ.get("FAKE_FRONTEND_PIN", "1.8.40"), "feature": "portal"}]
                   if os.environ.get("FAKE_FRONTEND_PIN") != "none" else [])}))
        sys.exit(0)
    # the subcommand's own args (run606's H=(helm --kubeconfig .. --kube-context ..) comes first)
    sa = [x for i, x in enumerate(a) if x not in ("--kubeconfig", "--kube-context")
          and (i == 0 or a[i - 1] not in ("--kubeconfig", "--kube-context"))]
    if sa[:2] == ["install", "cert-manager"]:
        open(os.path.join(STATE, "cert-manager-installed"), "w").write(json.dumps(sa))
    if sa[:1] == ["list"] and "cert-manager" in sa:
        # the release as helm reads it back (FAKE_CERT_MANAGER_CHART: another chart was installed)
        inst = os.path.exists(os.path.join(STATE, "cert-manager-installed"))
        print(json.dumps([{"name": "cert-manager", "namespace": "cert-manager", "status": "deployed",
                           "chart": os.environ.get("FAKE_CERT_MANAGER_CHART", "cert-manager-v1.21.1"),
                           "app_version": "v1.21.1"}] if inst else []))
        sys.exit(0)
    if sa[:3] == ["get", "values", "cert-manager"]:
        print(json.dumps({"crds": {"enabled": True}})); sys.exit(0)
    if "install" in a:
        import yaml
        if sa[:2] == ["install", "installer"]:
            # run 24 (#1276): the snowplow pod exists only from the install on
            open(os.path.join(STATE, "installer-installed"), "w").close()
        merged = {}
        for i, x in enumerate(a):
            if x == "-f" and i + 1 < len(a):
                f = a[i + 1]
                doc = yaml.safe_load(sys.stdin.read() if f == "-" else open(f).read()) or {}
                merged.update(doc.get("registryAuth") or {})
        if merged:
            with open(os.path.join(STATE, "helm-registry-auth.json"), "w") as fh:
                json.dump(merged, fh)
    if "list" in a:
        print(json.dumps([{"name": "installer", "namespace": "krateo-system",
                           "status": "deployed", "chart": "installer-0.3.422"}]))
    sys.exit(0)
    """)


def _fake_server(state: Path):
    class H(http.server.BaseHTTPRequestHandler):
        def _send(self, obj):
            body = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(u.query)
            if u.path == "/basic/login":
                return self._send({"accessToken": "fake-jwt"})
            if u.path == "/call" and q.get("name") == ["bench-fresh-listdep"]:
                m = state / "marker"
                marker = m.read_text() if m.exists() else ""
                return self._send({"status": [{"name": "bench-app-01-01",
                                               "marker": marker}]})
            return self._send({})

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


CRD_CHARTS = Path(__file__).resolve().parent / "fixtures" / "crd_charts"
# installer#614: the real jetstack cert-manager v1.21.1 archive (helm pull, 2026-11-04)
CERT_MANAGER_TGZ = Path(__file__).resolve().parent / "fixtures" / "cert-manager-v1.21.1.tgz"


def fake_registry(tmp_path: Path) -> dict:
    """The env a DRY run606 needs: its registry reads (the installer chart and
    the CRD charts it pins) answered by the harness's fake helm."""
    fakebin = tmp_path / "registry-bin"
    state = tmp_path / "registry-state"
    fakebin.mkdir(exist_ok=True)
    state.mkdir(exist_ok=True)
    _write_exe(fakebin / "helm", FAKE_HELM)
    return {"PATH": f"{fakebin}{os.pathsep}{os.environ['PATH']}", "FAKE_STATE": str(state),
            "FAKE_CRD_CHARTS": str(CRD_CHARTS), "FAKE_CERT_MANAGER_TGZ": str(CERT_MANAGER_TGZ)}


def _write_exe(path: Path, body: str):
    path.write_text(f"#!{sys.executable} -I\n" + body)
    path.chmod(0o755)


@pytest.fixture
def harness(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    _write_exe(fakebin / "gcloud", FAKE_GCLOUD)
    _write_exe(fakebin / "kubectl", FAKE_KUBECTL)
    _write_exe(fakebin / "helm", FAKE_HELM)
    ambient = tmp_path / "ambient.kubeconfig"
    ambient.write_text(json.dumps({
        "apiVersion": "v1", "kind": "Config", "current-context": AMBIENT_CTX,
        "contexts": [{"name": AMBIENT_CTX, "context": {"cluster": AMBIENT_CTX}}],
        "clusters": [], "users": []}))
    srv = _fake_server(state)
    from tests import fake_cloudquotas
    quotas_url, quotas_stop = fake_cloudquotas.serve(state / "cluster_limit")
    env = dict(os.environ)
    for k in ("BENCH_ALLOW_NON_GKE", "BENCH_GKE_CONTEXT", "BENCH_606_DRY_RUN",
              "BENCH_606_FAIL_AT", "SNOWPLOW_URL", "AUTHN_URL", "FRONTEND_URL"):
        env.pop(k, None)
    env.update({
        "PATH": f"{fakebin}{os.pathsep}{env['PATH']}",
        "HOME": str(tmp_path / "home"),
        "KUBECONFIG": str(ambient),          # the WRONG ambient context
        "FAKE_STATE": str(state),
        "FAKE_PORT": str(srv.server_address[1]),
        "PYTHONPATH": str(BENCH_DIR),
        "RUN_NAME": RUN_NAME, "INSTALLER_VERSION": "0.3.422",
        "SNOWPLOW_TAG": "1.12.46", "OUT": str(tmp_path / "out"),
        "EXPECTED_IMAGE_DIGEST": FAKE_DIGEST, "FAKE_IMAGE_DIGEST": FAKE_DIGEST,
        "SCALE": "4", "NAMESPACES": "2", "USERS": "2", "LOAD_DRIVER": "load606",
        "THINK_S": "0", "DRIVE_S": "1", "CHURN_RATES": "1",
        "FRESH_SAMPLES": "2", "FRESH_GAP_S": "0", "HOLD_PROBE_WAIT_S": "0",
        "PLATFORM_TIMEOUT_S": "1", "PLATFORM_POLL_S": "0.2",
        "BENCH_NO_COLOR": "1", "FAKE_PW": SCALEUSER_PASSWORD,
        "FAKE_USER_CRD": str(CRD_CHARTS / "authn-crds" / "templates" / "basic.authn.krateo.io_users.yaml"),
        "FAKE_CRD_CHARTS": str(CRD_CHARTS), "FAKE_CERT_MANAGER_TGZ": str(CERT_MANAGER_TGZ),
        # #985: GKE's per-location cluster limit, read from the Cloud Quotas fake
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_CLOUDQUOTAS": quotas_url,
    })
    from datetime import datetime, timezone
    env["BENCH_606_GO"] = datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + RUN_NAME
    (tmp_path / "home").mkdir()
    yield env, state, ambient, tmp_path
    # #1297: a cluster a test left standing (its gke-down failed, by design) still holds its
    # in-cluster Jobs' pods, which this fake runs as processes: they end with the test
    import glob
    for pf in glob.glob(str(state / "pods" / "*" / "pid")):
        pid = int(open(pf).read())
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(pid, sig)
            except OSError:
                break
            for _ in range(50):
                try:
                    os.killpg(pid, 0)
                except OSError:
                    break
                time.sleep(0.1)
    srv.shutdown()
    quotas_stop()


def _calls(state: Path):
    p = state / "calls.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []


def test_real_sequence_under_wrong_ambient_context(harness):
    env, state, ambient, tmp = harness
    before = hashlib.sha256(ambient.read_bytes()).hexdigest()
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-4000:]
    assert "context guard FAIL" not in out
    assert "=== step evaluate ===" in p.stderr and "run complete" in p.stderr

    calls = _calls(state)
    kcfg = str(tmp / "home" / ".kube" / "snowplow-bench" / f"{RUN_NAME}.kubeconfig")
    kube_calls = [c for c in calls if c["tool"] in ("kubectl", "helm")]
    assert kube_calls, "no kubectl/helm call recorded"
    for c in kube_calls:
        assert c["kubeconfig"] == kcfg, c
        assert c["context"] in (KCTX, None), c
        assert c["context"] != AMBIENT_CTX
    # every kubectl call resolved to the bench context (explicit or via the
    # dedicated kubeconfig's current-context)
    assert all(c["context"] == KCTX for c in calls if c["tool"] == "kubectl")
    # no gcloud call ever ran against the ambient kubeconfig
    assert all(c["kubeconfig"] != str(ambient) for c in calls)
    # the ambient kubeconfig is untouched
    assert hashlib.sha256(ambient.read_bytes()).hexdigest() == before

    # the harness steps really ran (data606, both in-cluster Jobs, verdict)
    proof = json.loads((tmp / "out" / "data-proof.json").read_text())
    assert proof["compositions"] == 4
    assert proof["logins"] == "deferred-to-in-cluster-driver"
    fresh = [json.loads(l) for l in
             (tmp / "out" / "fresh-fresh606-0.jsonl").read_text().splitlines()
             if l.startswith("{")]
    assert {r["label"] for r in fresh} == {"list_dep@1", "list_dep@10", "list_dep@50"}
    recs = [json.loads(l) for l in
            (tmp / "out" / "nav-load606-0.jsonl").read_text().splitlines()]
    assert [n["visit"] for n in recs if n["kind"] == "nav"] == [1, 2]   # copied from /out
    assert recs[0]["kind"] == "drive_start" and recs[-1]["kind"] == "drive_exit"
    verdict = json.loads((tmp / "out" / "verdict.json").read_text())
    assert verdict["report"]["navs"] == 2
    framing = [c for c in verdict["validity"] if c["check"].startswith("driver.stream")]
    assert [(c["check"], c["result"]) for c in framing] == [
        ("driver.stream_complete[nav-load606-0.jsonl]", "PASS")]
    gated = [c["check"] for c in verdict["checks"] if c["check"].startswith("fresh_p95")]
    assert gated == ["fresh_p95_ms[list_dep@10]"]          # ruling: gate at 10/s
    assert set(verdict["report"]["fresh_curve_report_only"]) == {"list_dep@1", "list_dep@50"}

    # teardown went through the label-checked path and deleted THIS cluster,
    # waited for it to be gone, then ran the exact disk backstop for it (#895)
    gcl = [c["argv"] for c in calls if c["tool"] == "gcloud"]
    assert ["container", "clusters", "describe", RUN_NAME] == gcl[-4][:4]
    assert gcl[-3][:4] == ["container", "clusters", "delete", RUN_NAME]
    assert [g[:3] for g in gcl[-3:]] == TEARDOWN_TAIL
    # #954: by the cluster's id (the describe's id, base32), never by its name label
    from bench import gke as _gke
    cid = _gke.cluster_id_b32({"id": "d8cac43bc0ac45d28fe8bdd1ccbe101a68fc42887613486d883c5066a9fc7796"})
    assert f"labels.cluster-id-gke-io={cid}" in " ".join(gcl[-1])
    assert "goog-k8s-cluster-name" not in " ".join(gcl[-1])
    assert "krateo-057" not in json.dumps(calls)

    # #1297: data606 and the samplers ran as the in-cluster Jobs (their pods stood in for by this
    # fake, running the containers' own commands), on the loadgen pool with the rendered requests;
    # data606's pod was gone, its Job, ServiceAccount and RBAC deleted, before any driver was applied
    o = tmp / "out"
    recs = [json.loads(ln) for ln in (o / "incluster-placement.jsonl").read_text().splitlines()]
    placed = [r for r in recs if r["kind"] == "incluster_placement"]
    rooms = [r for r in recs if r["kind"] == "loadgen_room"]
    assert [(p["job"], p["ok"], p["node_pool"]) for p in placed] == [("observe606", True, "loadgen"),
                                                                      ("data606", True, "loadgen")]
    # the room, read at env (the observer still to come) and at data (it is there), recorded
    assert [(r["phase"], r["ok"], r["observer_counted"], r["cpu_m"]) for r in rooms] == [
        ("env", True, "to come", 14390), ("data", True, "scheduled", 14390)]
    assert rooms[1]["floor_cpu_m"] == 2667 and rooms[1]["floor_memory_mi"] == 8192
    # review M2: the observer ran to collect, and the verdict says so
    obs = [json.loads(ln) for ln in (o / "observer-pod.jsonl").read_text().splitlines()]
    assert [(r["kind"], r["pod"], r["phase"]) for r in obs] == [("observer_pod", "observe606-0", "Running")]
    # review M1 / run 24b: the live node's room (allocatable 15890m / 59825Mi minus the DaemonSets' 1000m /
    # 4096Mi minus the observer's 500m / 1Gi, its 100m / 128Mi init container under its main one) holds
    # data606's need, and data606 requests that need (run 23's share), never the room
    # runs 24d / 25b: rendered 8192Mi, answered by the API server in canonical form (8Gi), the same
    # value: the placement is OK (it compared the strings, and failed every arm)
    assert placed[1]["requests"] == {"data606": {"cpu": "2667m", "memory": "8Gi"}}
    assert placed[1]["ok"] is True
    assert (rooms[1]["slack_cpu_m"], rooms[1]["slack_memory_mi"]) == (14390 - 2667, 54705 - 8192)
    deleted = [json.loads(ln) for ln in (state / "deleted.jsonl").read_text().splitlines()]
    gone = next(d for d in deleted if "job/data606" in d["names"])
    assert "serviceaccount/bench-data606" in gone["names"]
    assert any(d["names"] == ["clusterrolebinding/bench-data606", "clusterrole/bench-data606"] for d in deleted)
    assert any(d["ns"] == "krateo-system" and "role/bench-observe606" in d["names"] for d in deleted)
    applied = [json.loads(ln) for ln in (state / "calls.jsonl").read_text().splitlines()]
    i_gone = next(i for i, c in enumerate(applied) if "job/data606" in c["argv"] and "delete" in c["argv"])
    i_drivers = next(i for i, c in enumerate(applied) if any(x.endswith("loadgen-job.yaml") for x in c["argv"]))
    assert i_gone < i_drivers
    assert "data606: no pod left on the loadgen node" in (o / "run.log").read_text()
    # the samplers' files, under the names evaluate reads, from the observer's /out
    for f in ("apf.jsonl.gz", "top.jsonl", "top-boot.jsonl", "snowplow-stream.log", "observe606.json"):
        assert (o / f).exists(), f
    segs = {s["segment"]: s for s in json.loads((o / "observe606.json").read_text())["segments"]}
    assert all(segs[s]["started"] and segs[s]["stopped"] for s in ("apf", "stream", "top_boot", "top"))
    assert segs["top_boot"]["stopped"] <= segs["top"]["started"] + 1      # top_boot hands over to top
    import gzip as _gz
    apf = [json.loads(ln) for ln in _gz.open(o / "apf.jsonl.gz", "rt").read().splitlines()]
    assert apf and all(r["kind"] == "apf" and not r.get("failed") for r in apf)
    # the host's own load (#1297), recorded and said with the verdict
    assert verdict["report"]["host_load_report_only"]["state"] == "read"
    assert [ln.split(":")[0] for ln in verdict["report"]["incluster_report_only"]["lines"]] == [
        "INFO loadgen room (env)", "INFO loadgen room (data)", "INFO in-cluster Job observe606",
        "INFO in-cluster Job data606", "INFO observer pod"]
    assert verdict["report"]["incluster_report_only"]["lines"][-1] == "INFO observer pod: observe606-0 Running: ran to collect"

    # the synthetic users' password never reaches a log
    assert SCALEUSER_PASSWORD not in out
    assert SCALEUSER_PASSWORD not in (tmp / "out" / "run.log").read_text()
    for f in ("observe-job.yaml", "data606-job.yaml"):
        assert SCALEUSER_PASSWORD not in (o / f).read_text()


def test_gke_entrypoint_never_imports_the_context_guard():
    """`python -m bench.gke` must not import bench.cluster (its import-time
    guard reads the ambient context)."""
    code = ("import sys, bench.gke; "
            "print('bench.cluster' in sys.modules, 'bench.cli' in sys.modules)")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True,
                       text=True, env=dict(os.environ, PYTHONPATH=str(BENCH_DIR)))
    assert p.stdout.split() == ["False", "False"], p.stderr


def test_dry_run_log_redacts_password(tmp_path):
    env = dict(os.environ, **fake_registry(tmp_path))
    env.update(HOME=str(tmp_path), OUT=str(tmp_path / "out"),
               RUN_NAME=RUN_NAME, INSTALLER_VERSION="0.3.422",
               SNOWPLOW_TAG="1.12.46", THINK_S="0", BENCH_606_DRY_RUN="1",
               EXPECTED_IMAGE_DIGEST=FAKE_DIGEST, PYTHONPATH=str(BENCH_DIR))
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr
    # The password is only ever written into the in-cluster Job manifest (a
    # file), never into a logged command; any log line that did carry it would
    # be redacted by log(). The property that matters: it is in NO log.
    assert SCALEUSER_PASSWORD not in p.stderr + p.stdout
    assert SCALEUSER_PASSWORD not in (tmp_path / "out" / "run.log").read_text()


def test_failure_path_writes_redacted_diagnostics_before_teardown(harness):
    """Installer never goes Ready (shakedown #2's failure): the platform step
    must print WHERE it is stuck, time out, snapshot the cluster BEFORE the
    delete, read no Secret, and redact every token-looking value."""
    env, state, ambient, tmp = harness
    env["FAKE_INSTALLER_READY"] = "0"
    # Planted credentials, built at runtime (no credential literal in source).
    jwt, bear, tokval = _planted.jwt(), _planted.bearer(), _planted.opaque()
    env.update(FAKE_JWT=jwt, FAKE_BEARER=bear, FAKE_TOKVAL=tokval)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    # progress shows the stuck component and its reason
    assert "[platform] installer Ready=False" in out
    assert "NOT READY: authns/krateo-system/authn(ReconcileError" in out
    assert "TIMEOUT" in out

    diag = tmp / "out" / "diagnostics"
    for f in ("installer-cr.json", "composition-crs.json", "compositiondefinitions.json",
              "pods.json", "pods-failing.json", "events.json", "pvcs.json",
              "nodes.json", "helm-list.json", "progress.txt"):
        assert (diag / f).exists(), f
    failing = json.loads((diag / "pods-failing.json").read_text())
    assert [x["name"] for x in failing] == ["crash-0"]
    assert failing[0]["containers"][0]["waiting"] == "CrashLoopBackOff"
    assert failing[0]["containers"][0]["last_reason"] == "Error"
    cur = diag / "logs" / "krateo-system__crash-0__app__current.log"
    prev = diag / "logs" / "krateo-system__crash-0__app__previous.log"
    assert cur.exists() and prev.exists()
    assert "panic: cannot reach ghcr.io" in cur.read_text()
    comps = json.loads((diag / "composition-crs.json").read_text())
    assert any(c["name"] == "authn" and c["Ready"]["reason"] == "ReconcileError"
               for c in comps["components"])

    bundle = "\n".join(f.read_text() for f in diag.rglob("*") if f.is_file())
    for leaked in (SCALEUSER_PASSWORD, bear, jwt, jwt.split(".")[2], tokval):
        assert leaked not in bundle, leaked
    assert "<redacted" in bundle

    calls = _calls(state)

    def verb(c):
        a = [x for x in c["argv"] if not x.startswith(("--kubeconfig=", "--context="))]
        return a[2:] if a[:1] == ["-n"] else a
    # no Secret is ever READ (the two writes: the ghcr pull Secret and the image-pull Secret,
    # created at install from stdin)
    assert not any(a in ("secret", "secrets") for c in calls
                   if c["tool"] == "kubectl" and verb(c)[:1] != ["create"] for a in verb(c)[:3])
    assert [verb(c)[:4] for c in calls if c["tool"] == "kubectl" and verb(c)[:1] == ["create"]
            and "secret" in verb(c)] == [["create", "secret", "generic", "krateo-agentiko-pull"],
                                         ["create", "secret", "generic", "bench-ghcr-image-pull"]]
    # diagnostics were taken BEFORE the delete, and the delete still happened
    idx_logs = max(i for i, c in enumerate(calls)
                   if c["tool"] == "kubectl" and verb(c)[:1] == ["logs"])
    idx_delete = max(i for i, c in enumerate(calls)
                     if c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "delete"])
    assert idx_logs < idx_delete
    assert all(c["context"] in (KCTX, None) for c in calls if c["tool"] in ("kubectl", "helm"))


def test_a_portal_on_several_origins_stops_the_run_at_the_platform_step(harness):
    """Run 19: the browser must load the portal from ONE origin (057's
    gateway). A config.json that keeps the installer's localhost defaults
    (four origins: the gateway has no address yet, or path mode is off)
    stops the run at the platform step, before data and load are paid for,
    with what config.json said; the cluster is still torn down."""
    env, state, ambient, tmp = harness
    env["FAKE_PORTAL"] = "split"
    env["FAKE_CDC_RESYNC"] = "args:1s"    # the gate's window: 2 x the cdc's resync (#1005)
    env["FAKE_RENDERED"] = "split"        # the installer renders the localhost defaults: its defect
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "portal origin FAILED" in out and "3 origins" in out and "INSTALLER DEFECT" in out
    assert "=== step preflight ===" not in out and "=== step data ===" not in out
    fails = [json.loads(ln) for ln in (tmp / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert any(f["step"] == "platform" and "portal origin FAILED" in f["what"] for f in fails)
    calls = _calls(state)
    assert any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "delete"] for c in calls)


def test_the_real_sequence_reads_the_portal_origin_from_config(harness):
    env, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    assert "portal origin (config.json): http://10.0.0.9; snowplow at http://10.0.0.9/content" in p.stderr
    vals = (tmp / "out" / "installer-values.yaml").read_text()
    assert "type: Gateway" in vals and "gatewayMode: path" in vals and "agentGateway: true" in vals
