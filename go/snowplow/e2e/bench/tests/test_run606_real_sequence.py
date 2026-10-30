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
import subprocess
import sys
import textwrap
import threading
import urllib.parse
from pathlib import Path

import pytest

from bench import gke
from bench.load606 import SCALEUSER_PASSWORD
from tests import _planted

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

FAKE_GCLOUD = _COMMON + textwrap.dedent("""\
    a = sys.argv[1:]
    record("gcloud", os.environ.get("KUBECONFIG"))
    def opt(name):
        return a[a.index(name) + 1] if name in a else None
    if a[:3] == ["auth", "print-access-token"]:                 # #985: the Cloud Quotas read
        print("fake-access-token"); sys.exit(0)
    if a[:3] == ["container", "clusters", "list"]:
        print(os.environ.get("FAKE_CLUSTERS", "[]")); sys.exit(0)
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
    if a[:3] == ["compute", "regions", "describe"]:            # #895: the SSD quota read
        print(json.dumps({"name": a[3], "quotas": [
            {"metric": "SSD_TOTAL_GB", "limit": 500.0,
             "usage": float(os.environ.get("FAKE_SSD_USAGE", "333"))}]}))
        sys.exit(0)
    if a[:3] == ["compute", "disks", "list"]:                  # #895: the disk backstop
        if os.environ.get("FAKE_DISKS_FAIL") == "1":
            sys.exit(1)
        print(os.environ.get("FAKE_DISKS", "[]")); sys.exit(0)
    if a[:3] == ["container", "clusters", "describe"] and os.path.exists(os.path.join(STATE, "deleted")):
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
                          "locations": ["europe-west4-a"], "currentNodeCount": 5,
                          "nodePools": [{"name": n, "config": {"machineType": m}, "initialNodeCount": c,
                                         "locations": ["europe-west4-a"]}
                                        for n, m, c in (("default-pool", "n2-standard-8", 3),
                                                        ("snowplow", "n2-standard-8", 1),
                                                        ("loadgen", "n2-standard-8", 1))],
                          "resourceLabels": labels}))
        sys.exit(0)
    if a[:3] == ["container", "clusters", "delete"]:
        time.sleep(float(os.environ.get("FAKE_SLOW_DOWN", "0")))   # a delete in flight
        if os.environ.get("FAKE_DOWN_FAIL") == "1":
            sys.exit(1)
        open(os.path.join(STATE, "deleted"), "w").write("1")
        sys.exit(0)
    if a[:3] == ["container", "clusters", "create"]:
        time.sleep(float(os.environ.get("FAKE_SLOW_UP", "0")))   # a hung create
    sys.exit(0)   # create / node-pools create / delete
    """)

FAKE_KUBECTL = _COMMON + textwrap.dedent("""\
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
    port = os.environ["FAKE_PORT"]
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
        with open(os.path.join(STATE, "applied-kinds.jsonl"), "a") as fh, \\
                open(os.path.join(STATE, "applied-jobs"), "a") as jobs:
            for d in yaml.safe_load_all(text):
                if isinstance(d, dict):
                    fh.write(json.dumps([d.get("apiVersion"), d.get("kind")]) + "\\n")
                    if d.get("kind") == "Job":
                        jobs.write((d.get("metadata") or {}).get("name", "") + "\\n")
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
    if a[:2] == ["get", "pv"]:                                 # #895: the PV release at teardown
        gone = os.path.exists(os.path.join(STATE, "pvcs_deleted"))
        items([] if gone else json.loads(os.environ.get("FAKE_PVS", "[]")))
    if a[:2] == ["delete", "pvc"]:
        open(os.path.join(STATE, "pvcs_deleted"), "w").write("1"); sys.exit(0)
    f483 = os.environ.get("FAKE_483", "")          # "", pass, wide
    if a[:3] == ["get", "crd", "alerts.observability.krateo.io"]:
        sys.exit(0 if f483 in ("pass", "wide") else 1)
    if a[:2] == ["get", "pods"] and "app.kubernetes.io/name=snowplow" in a and "json" in a:
        # snowplow's own pod: optionally OOM-restarted between the before/after
        # records (FAKE_RESTART_DURING_MEASURE=1).
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
        pods = [{"metadata": {"name": "snowplow-abc"},
                 "spec": {"containers": [{"resources": {"limits": {"memory": "8Gi"}}}]},
                 "status": {"containerStatuses": [cs],
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
            return json.dumps({"metric": m, "type": "Point", "data": {"value": v, "tags": tags}})
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
                    lines += [pt("browser_page_load_ms", 300 + i, **t),
                              pt("browser_page_loads", 1, **t)]
            m = {"metrics": {"browser_page_loads": {"count": 60}}}
        lines.append(pt("bench_frame", 1, frame="end"))
        with gzip.open(os.path.join(dest, name + ".json.gz"), "wt") as fh:
            fh.write("\\n".join(lines) + "\\n")
        with open(os.path.join(dest, name + "-summary.json"), "w") as fh:
            json.dump(m, fh)
        # the wrappers' CPU samples (k6_job._cpu_sampler); "starved": Chrome at
        # its request the whole run
        for c, req in ((("k6", 4000), ("viewer", 1000)) if name == "k6" else (("browser", 3000),)):
            cores = 2.9 if (c == "browser" and mode == "starved") else 1.0
            recs = [{"kind": "cpu_start", "container": c, "request_m": req, "limit_m": None}]
            recs += [{"kind": "cpu", "t": 100.0 + 5 * i, "usage_usec": int(cores * 5e6 * i),
                      "throttled_usec": 0} for i in range(6)]
            with open(os.path.join(dest, c + "-cpu.jsonl"), "w") as fh:
                fh.write("".join(json.dumps(r) + "\\n" for r in recs))
        with open(os.path.join(dest, name + "-exit.json"), "w") as fh:
            json.dump({"kind": name + "_exit", "rc": 0}, fh)
        sys.exit(0)
    if a[:1] == ["cp"] and any(x.startswith("load606") for x in a):
        # The driver's /out/nav.jsonl (#817). FAKE_DRIVE_STREAM: whole (a framed
        # stream) | headcut (run 9: only the tail after log rotation) | nofile.
        time.sleep(float(os.environ.get("FAKE_SLOW_LOGS", "0")))   # a slow / hung read
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
            inst = cr("Installer", "installer", not kids, "Creating" if kids else "Available", msg)
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
            # every 2nd read)
            mode = os.environ.get("FAKE_GATEWAY", "address")
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
            print(json.dumps({"items": [{"metadata": {"name": "installers-v0-3-422-configmap"}, "data": data}]}))
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
            if any(x.endswith("{.status.succeeded}") for x in a):
                print("" if refused else "1"); sys.exit(0)
            if any(x.endswith("{.status.failed}") for x in a):
                print("1" if refused else ""); sys.exit(0)
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
    if a[:1] == ["pull"] and not a[1].endswith("/installer"):
        chart = a[1].rsplit("/", 1)[-1]
        if not os.path.isdir(os.path.join(CRD_CHARTS, chart)):
            print(f"Error: {a[1]}:{ver}: not found", file=sys.stderr); sys.exit(1)
        crd_chart(chart, os.path.join(a[a.index("-d") + 1], chart))
        sys.exit(0)
    if a[:1] == ["pull"]:
        d = os.path.join(a[a.index("-d") + 1], "installer")
        os.makedirs(os.path.join(d, "files"), exist_ok=True)
        os.makedirs(os.path.join(d, "charts"), exist_ok=True)
        open(os.path.join(d, "charts", "core-provider-2.13.24.tgz"), "w").write("x")
        crd_chart("core-provider-crds", os.path.join(d, "charts", "core-provider-crds"))
        open(os.path.join(d, "values.yaml"), "w").write(
            "ociRepo: oci://ghcr.io/krateo-platformops/charts" + chr(10)
            + "features: {portal: true, agentGateway: false, specialistAgents: false}" + chr(10))
        os.makedirs(os.path.join(d, "templates"), exist_ok=True)
        core = os.environ.get("FAKE_CORE_FEATURES", "")
        if core == "dynamic":
            core = "{{ .Values.coreFeatures }}"
        open(os.path.join(d, "templates", "_helpers.tpl"), "w").write(
            '{{- define "inst.coreFeatures" -}}' + core + '{{- end -}}' + chr(10))
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
                {"name": "alert-provider", "feature": "specialistAgents"},
                {"name": "krateo-observability", "feature": "portal",
                 "deps": ["clickhouse-operator"] + (["alert-provider-crd"] if good else [])},
                # PRIVATE (krateo-agentiko), as at 0.3.468: the ghcr pull token resolves them
                {"name": "agentgateway-controller", "version": os.environ.get("FAKE_AGW_VERSION", "0.2.0"),
                 "repo": "oci://ghcr.io/krateo-agentiko/charts", "feature": "agentGateway"},
                {"name": "agentgateway-policies", "version": "0.2.18",
                 "repo": "oci://ghcr.io/krateo-agentiko/charts", "feature": "agentGateway",
                 "deps": ["agentgateway-controller"]},
                # public repo overrides, as at 0.3.468 (one featureless: always on)
                {"name": "krateo-marketplace-catalog", "version": "0.1.2",
                 "repo": "oci://ghcr.io/krateo-blueprints/charts", "feature": "portal"},
                {"name": "github-provider-kog", "version": "0.6.0", "repo": "oci://ghcr.io/krateo-blueprints/charts"},
                # a private pin of a feature that is off unless the chart makes it a core feature,
                # at a version the fake registry has not published: preflighted only when on
                {"name": "incident-agent", "version": "9.9.9", "repo": "oci://ghcr.io/krateo-agentiko/charts",
                 "feature": "specialistAgents"}]}))
        sys.exit(0)
    if "install" in a:
        import yaml
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


def fake_registry(tmp_path: Path) -> dict:
    """The env a DRY run606 needs: its registry reads (the installer chart and
    the CRD charts it pins) answered by the harness's fake helm."""
    fakebin = tmp_path / "registry-bin"
    state = tmp_path / "registry-state"
    fakebin.mkdir(exist_ok=True)
    state.mkdir(exist_ok=True)
    _write_exe(fakebin / "helm", FAKE_HELM)
    return {"PATH": f"{fakebin}{os.pathsep}{os.environ['PATH']}", "FAKE_STATE": str(state),
            "FAKE_CRD_CHARTS": str(CRD_CHARTS)}


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
        "FAKE_CRD_CHARTS": str(CRD_CHARTS),
        # #985: GKE's per-location cluster limit, read from the Cloud Quotas fake
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_CLOUDQUOTAS": quotas_url,
    })
    from datetime import datetime, timezone
    env["BENCH_606_GO"] = datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + RUN_NAME
    (tmp_path / "home").mkdir()
    yield env, state, ambient, tmp_path
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

    # the synthetic users' password never reaches a log
    assert SCALEUSER_PASSWORD not in out
    assert SCALEUSER_PASSWORD not in (tmp / "out" / "run.log").read_text()


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
