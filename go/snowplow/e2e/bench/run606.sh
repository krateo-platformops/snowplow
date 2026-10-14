#!/usr/bin/env bash
# #606 — production-scale bench (1000 users x 50K compositions) on a
# TEMPORARY dedicated GKE cluster, torn down afterwards. NEVER on krateo-057.
#
# Creating the cluster spends cloud money and needs Diego's explicit go PER RUN:
#   BENCH_606_GO=<YYYYMMDD>-<RUN_NAME>   (today's UTC date + this run's name)
# A go from a previous day, or for a different run name, refuses to start.
#
# Teardown layers (any one deletes the cluster):
#   1. `trap teardown EXIT` below (+ INT/TERM/HUP → exit → EXIT trap);
#   2. the in-cluster ttl-self-destruct CronJob applied by `gke-up`;
#   3. `python -m bench.gke gke-reap --yes` (first step of every run; also by hand);
#   4. the ttl-expires label, which 2 and 3 read.
#
# BENCH_606_DRY_RUN=1 prints every command and runs none (no cloud calls);
# BENCH_606_FAIL_AT=<step> makes that step fail — the teardown test uses both.
#
# Kube context: the AMBIENT kubeconfig / current-context is never read. The
# cluster lifecycle goes through `python -m bench.gke` (GKE by project + name,
# no kube context, its own name/label guards). Every in-cluster step uses this
# run's DEDICATED kubeconfig ($KCFG, written by get-credentials and holding
# only the bench cluster) and its context ($KCTX), exported below as
# KUBECONFIG and BENCH_GKE_CONTEXT, so the harness's context guard pins to the
# bench cluster — never to the operator's context, never to 057.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

DRY="${BENCH_606_DRY_RUN:-0}"
FAIL_AT="${BENCH_606_FAIL_AT:-}"
RUN_NAME="${RUN_NAME:?set RUN_NAME=snowplow-bench-<mmdd-hhmm>}"
case "$RUN_NAME" in
  snowplow-bench-*) ;;
  *) echo "RUN_NAME must start with snowplow-bench-" >&2; exit 2 ;;
esac
PROJECT="${PROJECT:-operations-dev-krateo-io}"
ZONE="${ZONE:-europe-west4-a}"
TTL_HOURS="${TTL_HOURS:-8}"
INSTALLER_VERSION="${INSTALLER_VERSION:?set INSTALLER_VERSION (read the live 057 helm release, do not copy it from notes)}"
INSTALLER_CHART="${INSTALLER_CHART:-oci://ghcr.io/krateo-platformops/charts/installer}"
SNOWPLOW_TAG="${SNOWPLOW_TAG:?set SNOWPLOW_TAG (the image under test)}"
SCALE="${SCALE:-50000}"
USERS="${USERS:-1000}"
NAMESPACES="${NAMESPACES:-50}"
THINK_S="${THINK_S:-60}"
DRIVE_S="${DRIVE_S:-1800}"
CHURN_RATES="${CHURN_RATES:-1 10 50}"
FRESH_SAMPLES="${FRESH_SAMPLES:-60}"
FRESH_GAP_S="${FRESH_GAP_S:-5}"
HOLD_PROBE_WAIT_S="${HOLD_PROBE_WAIT_S:-60}"
PLATFORM_TIMEOUT_S="${PLATFORM_TIMEOUT_S:-2700}"
PLATFORM_POLL_S="${PLATFORM_POLL_S:-60}"
OUT="${OUT:-/tmp/snowplow-runs/606/${RUN_NAME}}"

expected_go="$(date -u +%Y%m%d)-${RUN_NAME}"
if [[ "$DRY" != "1" && "${BENCH_606_GO:-}" != "$expected_go" ]]; then
  echo "refusing: BENCH_606_GO must be '${expected_go}' (Diego's explicit go for THIS run)" >&2
  exit 2
fi

YES="--yes"; [[ "$DRY" == "1" ]] && YES=""
KCFG="$HOME/.kube/snowplow-bench/${RUN_NAME}.kubeconfig"
KCTX="gke_${PROJECT}_${ZONE}_${RUN_NAME}"
K=(kubectl "--kubeconfig=${KCFG}" "--context=${KCTX}")
H=(helm --kubeconfig "${KCFG}" --kube-context "${KCTX}")

# Pin everything to the bench cluster and drop the kind/minikube escape hatch,
# so the harness guard is ENFORCED — against $KCTX, not the operator's context.
mkdir -p "$(dirname "$KCFG")"
export KUBECONFIG="$KCFG" BENCH_GKE_CONTEXT="$KCTX"
unset BENCH_ALLOW_NON_GKE

# The synthetic users' fixed TEST password (single definition in load606.py,
# a stdlib-only import). Redacted from every log line.
PW="$(python3 -c 'from bench.load606 import SCALEUSER_PASSWORD as p; print(p)')"

mkdir -p "$OUT"
# Logs go to STDERR (and the run log) so a step's stdout stays pipeable.
log() {
  local msg="$*"
  msg="${msg//"$PW"/<redacted>}"
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$msg" | tee -a "$OUT/run.log" >&2
}
run() {
  if [[ "$DRY" == "1" ]]; then log "DRY: $*"; return 0; fi
  log "RUN: $*"; "$@"
}
step() {
  local name="$1"; shift
  log "=== step ${name} ==="
  if [[ "$FAIL_AT" == "$name" ]]; then log "injected failure at ${name}"; return 1; fi
  "$@"
}

CLUSTER_CREATED=0
teardown() {
  local rc=$?
  trap - EXIT
  if [[ "$CLUSTER_CREATED" == "1" ]]; then
    # On ANY failure, snapshot the cluster BEFORE it is deleted (shakedown #2
    # tore down with nothing captured). Best effort: it never blocks teardown.
    if [[ "$rc" != "0" ]]; then
      if [[ "$DRY" == "1" ]]; then
        log "DRY: python3 -m bench.diag606 collect --out $OUT/diagnostics"
      elif [[ -s "$KCFG" ]]; then
        log "=== diagnostics before teardown -> $OUT/diagnostics ==="
        python3 -m bench.diag606 collect --kubeconfig "$KCFG" --context "$KCTX" \
            --out "$OUT/diagnostics" --redact "$PW" \
          || log "diagnostics collection failed (continuing to teardown)"
      else
        log "no kubeconfig yet; no diagnostics to collect"
      fi
    fi
    log "=== teardown ${RUN_NAME} (exit ${rc}) ==="
    if ! python3 -m bench.gke gke-down --name "$RUN_NAME" --project "$PROJECT" --zone "$ZONE" $YES; then
      log "TEARDOWN FAILED — delete by hand: gcloud container clusters delete ${RUN_NAME} --project ${PROJECT} --zone ${ZONE} --quiet"
      rc=$(( rc == 0 ? 1 : rc ))
    fi
  fi
  exit "$rc"
}
trap teardown EXIT
trap 'exit 130' INT TERM HUP

s_reap()   { run python3 -m bench.gke gke-reap --project "$PROJECT" $YES; }
s_up() {
  CLUSTER_CREATED=1   # set BEFORE create: a half-created cluster still bills
  run python3 -m bench.gke gke-up --name "$RUN_NAME" --project "$PROJECT" \
      --zone "$ZONE" --ttl-hours "$TTL_HOURS" $YES
  run "${K[@]}" version
}
s_install() {
  cat > "$OUT/installer-values.yaml" <<EOF
bootstrap:
  coreProvider:
    enabled: true
exposure:
  type: LoadBalancer
features:
  coreAgents: false
  specialistAgents: false
vertexAI:
  enabled: false
# Installer 0.3.422 schema: top-level additionalProperties=false and the
# engine subchart key is core-provider (the fresh-install runbook's
# krateo-core-provider key is rejected by helm schema validation).
core-provider:
  resources:
    limits:
      memory: 4Gi
componentValues:
  snowplow:
    image:
      tag: "${SNOWPLOW_TAG}"
    nodeSelector:
      bench.krateo.io/pool: snowplow
    tolerations:
    - key: dedicated
      operator: Equal
      value: snowplow
      effect: NoSchedule
EOF
  run "${H[@]}" install installer "$INSTALLER_CHART" --version "$INSTALLER_VERSION" \
      -n krateo-system --create-namespace -f "$OUT/installer-values.yaml" \
      --wait --timeout 15m
}
s_wait_platform() {
  # Polled, not a blind wait: prints which components are Ready (and why the
  # others are not) every PLATFORM_POLL_S, so a stall shows WHERE.
  run python3 -m bench.diag606 wait-installer --kubeconfig "$KCFG" \
      --context "$KCTX" --timeout-s "$PLATFORM_TIMEOUT_S" \
      --interval-s "$PLATFORM_POLL_S"
  run "${K[@]}" -n krateo-system rollout status deploy/snowplow --timeout=30m
  # Record WHERE snowplow runs: a pod outside the dedicated pool shares CPU.
  run "${K[@]}" -n krateo-system get pods -l app.kubernetes.io/name=snowplow \
      -o wide | tee -a "$OUT/snowplow-placement.txt" || true
}
s_env() {
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.gke gke-env --name $RUN_NAME > $OUT/env.sh"
    printf 'export SNOWPLOW_URL=http://dry:8081 AUTHN_URL=http://dry:8082 FRONTEND_URL=http://dry:8080\n' > "$OUT/env.sh"
  else
    python3 -m bench.gke gke-env --name "$RUN_NAME" --project "$PROJECT" --zone "$ZONE" > "$OUT/env.sh"
  fi
  # shellcheck disable=SC1091
  source "$OUT/env.sh"
  # gke-env re-exports these; re-assert the pin regardless of what it printed.
  export KUBECONFIG="$KCFG" BENCH_GKE_CONTEXT="$KCTX" EXPECTED_IMAGE_TAG="$SNOWPLOW_TAG"
}
s_data() {
  run python3 -m bench data606 --scale "$SCALE" --users "$USERS" \
      --namespaces "$NAMESPACES" --hold-probe-wait-s "$HOLD_PROBE_WAIT_S" \
      --out "$OUT/data-proof.json"
}
s_reboot() {
  # Cold start at 50K: restart once the data exists, time Ready (#594/#637).
  run "${K[@]}" -n krateo-system rollout restart deploy/snowplow
  run "${K[@]}" -n krateo-system rollout status deploy/snowplow --timeout=45m
}
s_measure() {
  # Load: in-cluster on the tainted loadgen pool, one Indexed-Job pod per
  # shard. Each pod streams its JSONL to stdout (per nav, flushed) and the
  # collect step reads it back with `kubectl logs`. The page roots are the
  # harness's known page widgets; drive's preflight refuses if any is not 200
  # on this portal version.
  python3 -m bench.load606_job --users "$USERS" --shards 1 \
      --think-s "$THINK_S" --duration-s "$DRIVE_S" \
      --snowplow-url "$SNOWPLOW_URL" --authn-url "$AUTHN_URL" --password "$PW" \
      --page "dashboard=3=/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1&resource=pages&name=dashboard-page&namespace=krateo-system" \
      --page "compositions=2=/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1&resource=pages&name=compositions-page&namespace=krateo-system" \
      > "$OUT/loadgen-job.yaml"
  run "${K[@]}" apply -f "$OUT/loadgen-job.yaml"
  # Let every user's FIRST navigation land before freshness is sampled.
  run sleep "$THINK_S"
  # Freshness under load, one churn step at a time, from this machine.
  local r
  for r in $CHURN_RATES; do
    run python3 -m bench.load606 fresh --snowplow-url "$SNOWPLOW_URL" \
        --authn-url "$AUTHN_URL" --user scaleuser-0001 --password "$PW" \
        --kubeconfig "$KCFG" --context "$KCTX" --version v1-2-2 \
        --plural githubscaffoldingwithcompositionpages \
        --probe-name bench-app-01-01 --churn-rate "$r" \
        --samples "$FRESH_SAMPLES" --gap-s "$FRESH_GAP_S" \
        --churn-namespaces "$NAMESPACES" \
        --out "$OUT/fresh-churn-${r}.jsonl"
  done
  run "${K[@]}" -n bench-loadgen wait job/load606 --for=condition=Complete \
      --timeout="$(( DRIVE_S + 1800 ))s"
}
s_collect() {
  local p
  if [[ "$DRY" == "1" ]]; then
    log "DRY: kubectl logs <each load606 pod> > $OUT/nav-<pod>.jsonl"
    return 0
  fi
  for p in $("${K[@]}" -n bench-loadgen get pods -l job-name=load606 -o name); do
    "${K[@]}" -n bench-loadgen logs "$p" > "$OUT/nav-${p#pod/}.jsonl"
  done
  "${K[@]}" -n krateo-system logs deploy/snowplow --tail=-1 > "$OUT/snowplow.log" || true
  "${K[@]}" top nodes > "$OUT/top-nodes.txt" || true
}
s_evaluate() {
  shopt -s nullglob
  local files=("$OUT"/nav-*.jsonl "$OUT"/fresh-churn-*.jsonl)
  if [[ "$DRY" == "1" ]]; then log "DRY: python3 -m bench.load606 evaluate ${files[*]:-<none>}"; return 0; fi
  if [[ ${#files[@]} -eq 0 ]]; then log "no result files to evaluate"; return 1; fi
  # The verdict is a RESULT, not a tooling failure: record it, do not abort.
  local vrc=0
  python3 -m bench.load606 evaluate "${files[@]}" > "$OUT/verdict.json" || vrc=$?
  log "verdict exit=${vrc} (0 PASS, 1 FAIL, 3 INVALID) -> $OUT/verdict.json"
}

step reap      s_reap
step up        s_up
step install   s_install
step platform  s_wait_platform
step env       s_env
step data      s_data
step reboot    s_reboot
step measure   s_measure
step collect   s_collect
step evaluate  s_evaluate
log "run complete: $OUT"
