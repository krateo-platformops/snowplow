#!/usr/bin/env bash
# #1297 kind parity arm: data606 run IN-CLUSTER (run606.sh's own s_data: the data606 Job on the loadgen
# node, bench/incluster.py) creates exactly the objects data606 run from this machine created.
#
# A MANUAL arm, not part of the bench suite: the suite is hermetic (no cluster; .github/workflows/
# bench-tests.yaml), this one needs Docker, kind, kubectl and the internet (the data606 pod pulls
# python:3.12-slim and fetches its pinned kubectl from dl.k8s.io). Its pure parts (snapshot, diff,
# the run606.sh extraction) are tested in the suite: tests/test_1297_data606_parity.py.
#
#   go/snowplow/e2e/bench$ tests/kind_1297_data606_parity.sh            # exit 0 = PASS
#
# Two fresh kind clusters, one after the other, built the same way (cluster `krateo-<pid>`, deleted at
# the end of each arm and on any exit; no other cluster is touched): a control plane and one worker
# labelled bench.krateo.io/pool=loadgen and tainted dedicated=loadgen:NoSchedule (the loadgen node);
# the CRDs the installer pins (tests/fixtures/crd_charts: authn-crds, core-provider-crds,
# snowplow-crds), a permissive CRD for the composition kind (githubscaffoldingwithcompositionpages.
# composition.krateo.io v1-2-2: core-provider would generate it from the CompositionDefinition), the
# namespace krateo-system; and, during data606, a stand-in loop (no core-provider here) that creates
# the composition controller Deployment in bench-ns-01 once that namespace exists, and patches the
# CompositionDefinition's status to Ready=True: identical in both arms.
#   arm A  BENCH_GKE_CONTEXT=kind-<name> KUBECONFIG=<scratch> python3 -m bench data606 ...  (this machine)
#   arm B  run606.sh's s_data, its functions and defaults read from the script itself (bench/
#          data606_parity.py functions), K=(kubectl --kubeconfig=<scratch> --context=kind-<name>), DRY=0;
#          then run606's data606_gone (no data606 pod left on the loadgen node).
# Each arm: a baseline snapshot before data606, a snapshot after (bench/data606_parity.py: every object
# of a kind data606 writes that the baseline lacks, server-set fields stripped, sha256 per object,
# Secret values only as their sha256). PASS = an empty diff and equal counts per kind, with arm B's
# placement on the loadgen pool, its requests as rendered, and its pod gone.
#
# The kubeconfig is a scratch file (WORK/kubeconfig): ~/.kube/config is never read or written.
#
# Arguments of both arms (as dev-1297 specified for #1297): --scale 500 --users 50 --namespaces 5
# --hold-probe-wait-s 0 --no-login --fresh-modes annotation status (run606's FRESH_MODES default: the
# status-mode RESTAction fixture is in the compared set). Arm B runs with LOAD_DRIVER=load606, which
# only skips fresh_classes_create (s_data's next action, objects of the #935 classes, not data606's).
#
# data606's requests: arm B's s_data derives them from the LIVE loadgen worker, as a paid run does
# (review M1, bench/incluster.py loadgen_room: allocatable minus the pods already there minus the
# observer's), and refuses with the numbers below run 23's share. The result records that room.
#
# Environment: PARITY_WORK (default: a new temporary directory) holds every file of the run (the
# snapshots, the diff, arm B's OUT, result.json); KEEP_CLUSTER=1 keeps the last arm's cluster.
set -Eeuo pipefail

BENCH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="krateo-$$"
CTX="kind-$NAME"
WORK="${PARITY_WORK:-$(mktemp -d "${TMPDIR:-/tmp}/kind-1297-parity.XXXXXX")}"
mkdir -p "$WORK"
KCFG="$WORK/kubeconfig"
export KUBECONFIG="$KCFG"           # kind and every kubectl here: the scratch file, never ~/.kube/config
unset BENCH_ALLOW_NON_GKE           # the bench's context pin applies, as in run606.sh
K=(kubectl "--kubeconfig=$KCFG" "--context=$CTX")
SCALE=500 USERS=50 NAMESPACES=5 HOLD_PROBE_WAIT_S=0
FRESH="annotation status"
CRD_CHARTS="$BENCH/tests/fixtures/crd_charts"
CONTROLLER=githubscaffoldingwithcompositionpages-v1-2-2-controller
STANDIN_PID=""

say() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$WORK/parity.log" >&2; }
die() { say "FAIL: $*"; exit 1; }

stop_standin() {
  [[ -n "$STANDIN_PID" ]] || return 0
  kill "$STANDIN_PID" 2>/dev/null || true
  wait "$STANDIN_PID" 2>/dev/null || true
  STANDIN_PID=""
}
delete_cluster() {
  # only this run's own cluster, by its name
  if kind get clusters 2>/dev/null | grep -qx "$NAME"; then
    kind delete cluster --name "$NAME" --kubeconfig "$KCFG" >/dev/null 2>&1 || say "WARNING: kind delete $NAME failed"
  fi
}
on_exit() {
  local rc=$?
  stop_standin
  [[ "${KEEP_CLUSTER:-0}" == "1" ]] || delete_cluster
  say "work dir: $WORK (exit $rc)"
}
trap on_exit EXIT

cat > "$WORK/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
  labels:
    bench.krateo.io/pool: loadgen
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      taints:
      - key: dedicated
        value: loadgen
        effect: NoSchedule
EOF

cat > "$WORK/composition-crd.yaml" <<'EOF'
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: githubscaffoldingwithcompositionpages.composition.krateo.io
spec:
  group: composition.krateo.io
  names:
    kind: GithubScaffoldingWithCompositionPage
    listKind: GithubScaffoldingWithCompositionPageList
    plural: githubscaffoldingwithcompositionpages
    singular: githubscaffoldingwithcompositionpage
  scope: Namespaced
  versions:
  - name: v1-2-2
    served: true
    storage: true
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        x-kubernetes-preserve-unknown-fields: true
EOF

cat > "$WORK/controller.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $CONTROLLER
  namespace: bench-ns-01
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $CONTROLLER
  template:
    metadata:
      labels:
        app: $CONTROLLER
    spec:
      containers:
      - name: controller
        image: registry.k8s.io/pause:3.10
EOF

make_cluster() {
  delete_cluster
  say "kind create cluster $NAME (control plane + loadgen worker)"
  kind create cluster --name "$NAME" --kubeconfig "$KCFG" --config "$WORK/kind.yaml" --wait 180s >>"$WORK/parity.log" 2>&1 \
    || die "kind create cluster $NAME"
  "${K[@]}" wait --for=condition=Ready node --all --timeout=300s >>"$WORK/parity.log" 2>&1 || die "nodes not Ready"
  local workers
  workers="$("${K[@]}" get nodes -l bench.krateo.io/pool=loadgen \
               -o jsonpath='{range .items[*]}{.metadata.name}:{range .spec.taints[*]} {.key}={.value}:{.effect}{end}{"\n"}{end}')"
  [[ "$workers" == *" dedicated=loadgen:NoSchedule"* && "$(wc -l <<<"$workers")" -eq 1 ]] \
    || die "the loadgen worker is not labelled and tainted as specified: $workers"
  say "loadgen worker: $workers"
  local c
  for c in authn-crds core-provider-crds snowplow-crds; do
    "${K[@]}" apply --server-side -f "$CRD_CHARTS/$c/templates/" >>"$WORK/parity.log" 2>&1 || die "apply $c"
  done
  "${K[@]}" apply --server-side -f "$WORK/composition-crd.yaml" >>"$WORK/parity.log" 2>&1 || die "apply the composition CRD"
  "${K[@]}" wait --for=condition=Established --timeout=120s crd --all >>"$WORK/parity.log" 2>&1 || die "CRDs not Established"
  "${K[@]}" create namespace krateo-system >>"$WORK/parity.log" 2>&1 || die "create namespace krateo-system"
}

# the stand-in for core-provider and the composition controller (the same loop in both arms)
start_standin() {
  ( set +e
    while true; do
      if "${K[@]}" get namespace bench-ns-01 >/dev/null 2>&1; then
        "${K[@]}" -n bench-ns-01 get deployment "$CONTROLLER" >/dev/null 2>&1 \
          || "${K[@]}" -n bench-ns-01 create -f "$WORK/controller.yaml" >/dev/null 2>&1
        for cd in $("${K[@]}" -n bench-ns-01 get compositiondefinitions.core.krateo.io -o name 2>/dev/null); do
          ready="$("${K[@]}" -n bench-ns-01 get "$cd" -o "jsonpath={.status.conditions[?(@.type=='Ready')].status}" 2>/dev/null)"
          [[ "$ready" == "True" ]] || "${K[@]}" -n bench-ns-01 patch "$cd" --subresource=status --type=merge \
            -p "{\"status\":{\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\",\"reason\":\"StandIn\",\"message\":\"kind parity arm stand-in\",\"lastTransitionTime\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}]}}" >/dev/null 2>&1
        done
      fi
      sleep 2
    done ) &
  STANDIN_PID=$!
}

snapshot() {  # snapshot <out> [baseline]
  (cd "$BENCH" && python3 -m bench.data606_parity snapshot --kubeconfig "$KCFG" --context "$CTX" --out "$1" \
       ${2:+--baseline "$2"}) | tee -a "$WORK/parity.log" >&2 || die "snapshot $1"
}

# ─── arm A: data606 from this machine ────────────────────────────────────────────────────────────
make_cluster
snapshot "$WORK/A-baseline.json"
start_standin
say "arm A: python3 -m bench data606 (this machine)"
# shellcheck disable=SC2086
(cd "$BENCH" && BENCH_GKE_CONTEXT="$CTX" python3 -m bench data606 --scale "$SCALE" --users "$USERS" \
   --namespaces "$NAMESPACES" --hold-probe-wait-s "$HOLD_PROBE_WAIT_S" --no-login --fresh-modes $FRESH \
   --out "$WORK/A.json") >"$WORK/A-data606.log" 2>&1 || die "arm A data606 exited non-zero (see $WORK/A-data606.log)"
stop_standin
snapshot "$WORK/A-snapshot.json" "$WORK/A-baseline.json"
delete_cluster

# ─── arm B: run606.sh's s_data on a fresh cluster ────────────────────────────────────────────────
make_cluster
snapshot "$WORK/B-baseline.json"

start_standin
OUTB="$WORK/B"
mkdir -p "$OUTB"
{
  echo 'set -Eeuo pipefail'
  printf 'OUT=%q; HERE=%q; DRY=0; KCFG=%q; KCTX=%q\n' "$OUTB" "$BENCH" "$KCFG" "$CTX"
  printf 'K=(kubectl %q %q)\n' "--kubeconfig=$KCFG" "--context=$CTX"
  echo 'export KUBECONFIG="$KCFG" BENCH_GKE_CONTEXT="$KCTX"; unset BENCH_ALLOW_NON_GKE'
  echo 'touch "$OUT/command-failures.jsonl"'
  # bench/data606_parity.py ARM_B_VARS / ARM_B_FUNCTIONS: refused when run606.sh's s_data path reads a
  # variable or calls a function the lists do not carry (arm_b_coverage; at f7aa9df3 INC_POLL_S was not
  # carried and arm B died on it)
  (cd "$BENCH" && python3 -m bench.data606_parity functions --script run606.sh --arm-b) \
    || die "extracting run606.sh's functions"
  echo 'KERR="$OUT/.kubectl-last-error"'
  echo 'CUR_STEP=data'
  # fail closed, and leave nothing behind: if s_data's path aborts the script (an unbound variable,
  # set -e), the EXIT trap says so and deletes data606's Job, ServiceAccount and RBAC, so the leftover
  # check below judges the arm, not the abort
  echo 'DONE=0'
  echo 'on_exit() { local rc=$?; [[ "$DONE" == "1" ]] && return; set +eu; echo "S_DATA_ABORTED exit=$rc"; incluster_delete data606 || true; exit 1; }'
  echo 'trap on_exit EXIT'
  echo 'rc=0; s_data || rc=$?'
  echo 'gone=0; CUR_STEP=measure; data606_gone || gone=$?'
  echo 'DONE=1'
  echo 'echo "S_DATA_RC=$rc DATA606_GONE_RC=$gone"'
  echo 'exit $(( rc != 0 || gone != 0 ))'
} > "$WORK/armB.sh"
say "arm B: run606.sh's s_data (the data606 Job on the loadgen node, its requests from the live worker)"
B_RC=0
(cd "$BENCH" && SCALE="$SCALE" USERS="$USERS" NAMESPACES="$NAMESPACES" HOLD_PROBE_WAIT_S="$HOLD_PROBE_WAIT_S" \
   LOAD_DRIVER=load606 FRESH_MODES="$FRESH" \
   bash "$WORK/armB.sh") >"$WORK/B-s_data.log" 2>&1 || B_RC=$?
stop_standin
say "arm B: $(grep -E '^(S_DATA_RC=|S_DATA_ABORTED)' "$WORK/B-s_data.log" || echo "no result line (exit $B_RC)")"
LEFT_PODS="$("${K[@]}" -n bench-loadgen get pods -l job-name=data606 -o name 2>&1 || true)"
LEFT_LOADGEN="$("${K[@]}" -n bench-loadgen get jobs,pods,serviceaccounts,configmaps,secrets -o name 2>&1 | tr '\n' ' ' || true)"
snapshot "$WORK/B-snapshot.json" "$WORK/B-baseline.json"

# ─── the verdict ─────────────────────────────────────────────────────────────────────────────────
DIFF_RC=0
(cd "$BENCH" && python3 -m bench.data606_parity diff "$WORK/A-snapshot.json" "$WORK/B-snapshot.json" \
   --out "$WORK/diff.json") | tee -a "$WORK/parity.log" >&2 || DIFF_RC=$?
python3 - "$WORK" "$B_RC" "$DIFF_RC" "$LEFT_PODS" "$LEFT_LOADGEN" <<'PY' | tee "$WORK/result.json"
import json, os, sys
work, b_rc, diff_rc, left_pods, left_loadgen = sys.argv[1:]
d = json.load(open(os.path.join(work, "diff.json")))
recs = [json.loads(l) for l in open(os.path.join(work, "B", "incluster-placement.jsonl"))] \
    if os.path.exists(os.path.join(work, "B", "incluster-placement.jsonl")) else []
placement = [r for r in recs if r.get("kind") == "incluster_placement"]
room = [r for r in recs if r.get("kind") == "loadgen_room"]
proof = {}
for arm, path in (("A", "A.json"), ("B", "B/data-proof.json")):
    try:
        p = json.load(open(os.path.join(work, path)))
        p.pop("elapsed_s", None)
        proof[arm] = p
    except (OSError, ValueError):
        proof[arm] = None
gone = left_pods.strip() in ("", "No resources found in bench-loadgen namespace.")
# bench-loadgen is not in the diff (bench/data606_parity.py): after s_data it holds only what Kubernetes
# puts in every namespace; anything else (the Job, its pod, its ServiceAccount, the bench-src ConfigMap,
# which run606 deletes with each in-cluster Job) fails the arm
expected_left = {"serviceaccount/default", "configmap/kube-root-ca.crt"}
unexpected_left = sorted(set(left_loadgen.split()) - expected_left)
ok = (d["verdict"] == "PASS" and int(b_rc) == 0 and gone and not unexpected_left
      and placement and all(p.get("ok") for p in placement) and room and all(r.get("ok") for r in room))
print(json.dumps({
    "kind": "kind_1297_data606_parity", "verdict": "PASS" if ok else "FAIL",
    "diff": {k: d[k] for k in ("verdict", "objects", "counts", "counts_equal", "controller")},
    "only_in_a": d["only_in_a"][:20], "only_in_b": d["only_in_b"][:20], "different": dict(list(d["different"].items())[:20]),
    "arm_b_exit": int(b_rc), "arm_b_aborted": "S_DATA_ABORTED" in open(os.path.join(work, "B-s_data.log")).read(),
    "placement": placement,
    "loadgen_room": room,
    "cpu_request_note": "data606's requests are the kind worker's live room (its Docker VM's CPUs); a paid run derives them from its own loadgen node the same way",
    "data606_pod_gone": gone, "data606_pods_left": left_pods.strip(),
    "bench_loadgen_left": left_loadgen.strip(), "bench_loadgen_unexpected": unexpected_left,
    "proofs_equal_but_elapsed": proof["A"] == proof["B"], "proof_a": proof["A"], "proof_b": proof["B"],
}, indent=1))
sys.exit(0 if ok else 1)
PY
