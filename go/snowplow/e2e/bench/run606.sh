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
# The cluster's TTL (the self-destruct CronJob and gke-reap read it). The
# whole run's worst case must fit inside it, or the run is refused before any
# cloud call (see the budget below). 11 h holds run 9's default plan (two
# churn modes x 3 rates x 60 samples) with every bounded wait at its maximum.
TTL_HOURS="${TTL_HOURS:-11}"
INSTALLER_CHART="${INSTALLER_CHART:-oci://ghcr.io/krateo-platformops/charts/installer}"
# Installer source: ONLY the published OCI chart (INSTALLER_VERSION).
# Local-chart mode cannot work for this installer (run 4): its self-CD pulls
# its own chart from the registry at .Chart.Version (self-bootstrap.yaml:215;
# the cdc SA and CR version derive from it too: self-bootstrap.yaml:71-72,
# ordered-teardown.yaml:30), so a locally stamped version never converges.
if [[ -n "${INSTALLER_GIT_REF:-}" || -n "${INSTALLER_CHART_DIR:-}" ]]; then
  echo "refusing: local-chart installs cannot work for this installer (self-bootstrap.yaml:215 pulls its own chart at .Chart.Version from the registry). Release it and set INSTALLER_VERSION." >&2
  exit 2
fi
INSTALLER_VERSION="${INSTALLER_VERSION:?set INSTALLER_VERSION (read the live 057 helm release, or the released version under test; do not copy it from notes)}"
# VERIFY483=1: the registry preflight also requires the #483 pins, and the
# verify483 step runs after the platform step.
VERIFY483="${VERIFY483:-0}"
SNOWPLOW_TAG="${SNOWPLOW_TAG:?set SNOWPLOW_TAG (the image under test)}"
# The registry digest of SNOWPLOW_TAG. After the reboot and BEFORE any load,
# every live snowplow pod's status.containerStatuses[].imageID must carry
# exactly this digest, or the run is VOID (aborted, torn down). Read it from
# the registry for the tag under test; never copy it from notes.
EXPECTED_IMAGE_DIGEST="${EXPECTED_IMAGE_DIGEST:?set EXPECTED_IMAGE_DIGEST=sha256:<64 hex> (the registry digest of SNOWPLOW_TAG)}"
if [[ ! "$EXPECTED_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "refusing: EXPECTED_IMAGE_DIGEST must be sha256:<64 lowercase hex>, got '${EXPECTED_IMAGE_DIGEST}'" >&2
  exit 2
fi
# Bound on waiting for the old ReplicaSet's Terminating pod to go before the
# digest gate judges (10x the default 30 s termination grace period).
DIGEST_SETTLE_S="${DIGEST_SETTLE_S:-300}"
DIGEST_POLL_S="${DIGEST_POLL_S:-5}"
# Bound on each kubectl read in the collect step (it also runs from the
# teardown trap after a failed measure, #763).
COLLECT_TIMEOUT_S="${COLLECT_TIMEOUT_S:-300}"
DRIVE_POLL_S="${DRIVE_POLL_S:-30}"     # measure: is every driver pod Ready (drive over)?
LOG_STREAM_RETRY_S="${LOG_STREAM_RETRY_S:-5}"   # snowplow log stream: reconnect pause
MEM_STOP_WAIT_S="${MEM_STOP_WAIT_S:-180}"       # collect: mem606 writes its final stream
MEASURE_STARTED=0
COLLECTED=0
SCALE="${SCALE:-50000}"
USERS="${USERS:-1000}"
# #825: the driver as N pods x USERS/N users (Indexed Job), each its own
# GIL-bound process and framed /out stream. Run 10 (one pod, 1000 users)
# missed its schedule in the first-load arrival.
DRIVER_SHARDS="${DRIVER_SHARDS:-4}"
# Per driver pod requests (load606_job refuses a plan the loadgen node cannot
# hold): one process uses ~1 core at most, so 1 CPU each at 4 shards; 8
# shards fit at DRIVER_CPU=600m.
DRIVER_CPU="${DRIVER_CPU:-1}"
DRIVER_MEMORY="${DRIVER_MEMORY:-1Gi}"
NAMESPACES="${NAMESPACES:-50}"
THINK_S="${THINK_S:-60}"
DRIVE_S="${DRIVE_S:-1800}"
CHURN_RATES="${CHURN_RATES:-1 10 50}"
FRESH_SAMPLES="${FRESH_SAMPLES:-60}"
FRESH_GAP_S="${FRESH_GAP_S:-5}"
HOLD_PROBE_WAIT_S="${HOLD_PROBE_WAIT_S:-60}"
ENDPOINT_MODE="${ENDPOINT_MODE:-incluster}"
ENDPOINT_WAIT_S="${ENDPOINT_WAIT_S:-600}"
ENDPOINT_POLL_S="${ENDPOINT_POLL_S:-15}"
REQUIRED_DEPLOYMENTS="${REQUIRED_DEPLOYMENTS:-snowplow frontend authn}"
PLATFORM_TIMEOUT_S="${PLATFORM_TIMEOUT_S:-2700}"
FRONTEND_CONFIGMAP="${FRONTEND_CONFIGMAP:-frontend-config-vars}"
PREFLIGHT_TIMEOUT_S="${PREFLIGHT_TIMEOUT_S:-300}"
PREFLIGHT_POLL_S="${PREFLIGHT_POLL_S:-3}"
SNOWPLOW_POD_SELECTOR="${SNOWPLOW_POD_SELECTOR:-app.kubernetes.io/name=snowplow}"
TOP_POLL_S="${TOP_POLL_S:-15}"
# Extra /debug/vars key prefixes mem606 records verbatim every sample.
# Default = load606.RUN8_VARS_PREFIXES (single source): the #710 PR-0 fill and
# admission counters, the nested gate, the stale serve/push/debt family, the
# #740 retry engine, the resolved cache, customer in-flight, dispatch shape and
# build_info. Space-separated. With RUN8_REPORT=1 a value that misses any
# run-8 key is refused below, before any cloud call.
MEM_VARS_PREFIXES="${MEM_VARS_PREFIXES-$(python3 -m bench.load606 prefixes)}"
# Run 8 (#710 issuecomment-6187374111): a COLD COMPOSITE-PAGE BURST — the last
# COMPOSITE_BURST_USERS users stay idle, then all arrive together at
# COMPOSITE_BURST_AT_S, each deep-linking to a composite page (nested widget
# tree) the preflight derived from the live menu. 0 disables.
COMPOSITE_BURST_USERS="${COMPOSITE_BURST_USERS:-200}"
COMPOSITE_BURST_AT_S="${COMPOSITE_BURST_AT_S:-$(( DRIVE_S / 2 ))}"
RUN8_REPORT="${RUN8_REPORT:-1}"
# #764: every virtual user keeps ONE /refreshes stream armed over the widgets
# of its shell + current page (the SPA's per-tab stream), and refetches a
# widget on its `refresh` frame; fresh606 arms one on the probe cell. Without
# viewers the #710 stale-then-push and #740 viewer paths never run (run 8).
# Sizing: USERS concurrent streams (1000 = one tab per user), one 512 KiB-stack
# reader thread each in the driver pod. VIEWERS=0 turns them off (then the
# run-8 report marks those checks NOT EXERCISED).
VIEWERS="${VIEWERS:-1}"
# Run 9 (#765 spec): fresh606 churn MODES as separate labelled phases at every
# CHURN_RATES step: "annotation" (run 8's churn) and "status" (a condition in
# .status.conditions written through the status subresource). 10/s gates in
# each mode. FRESH_MODES=annotation reproduces run 8's probe exactly.
FRESH_MODES="${FRESH_MODES:-annotation status}"
for m in $FRESH_MODES; do
  case "$m" in annotation|status) ;; *) echo "refusing: FRESH_MODES has unknown mode '$m'" >&2; exit 2 ;; esac
done
# The measure wait is DERIVED from the work it waits for, and fresh606 is
# itself BOUNDED to that work (reviewer-small (d)): each (mode, rate) phase
# gets samples x (30 s budget + gap) x 1.25 of wall clock and stops there,
# recorded (load606.fresh_phase_budget_s). Run 8 waited a fixed DRIVE_S+3600
# = 5400 s while fresh606 needed up to 3 x 60 x 35 s ~ 6300 s: the wait
# expired at list_dep@50 30/60.
FRESH_BUDGET_S=30                 # load606.FRESH_BUDGET_S
FRESH_LOAD_WAIT_MAX_S=1800        # fresh --load-max-wait-s default (start gate)
FRESH_LAST_SAMPLE_OVERRUN_S=60    # the phase's last sample: mutate + last poll overrun
DRIVER_LOGIN_BUDGET_S=900         # adaptive logins before the driver's DRIVE_S
n_modes=$(echo $FRESH_MODES | wc -w | tr -d ' ')
n_rates=$(echo $CHURN_RATES | wc -w | tr -d ' ')
FRESH_PHASE_S=$(( FRESH_SAMPLES * (FRESH_BUDGET_S + ${FRESH_GAP_S%.*}) * 5 / 4 ))
FRESH_EST_S=$(( ${THINK_S%.*} + FRESH_LOAD_WAIT_MAX_S
                + n_modes * n_rates * (FRESH_PHASE_S + FRESH_LAST_SAMPLE_OVERRUN_S) ))
DRIVER_EST_S=$(( ${DRIVE_S%.*} + DRIVER_LOGIN_BUDGET_S ))
MEASURE_WAIT_S="${MEASURE_WAIT_S:-$(( (FRESH_EST_S > DRIVER_EST_S ? FRESH_EST_S : DRIVER_EST_S) + 600 ))}"
# Every other bounded step (seconds). Each one is ENFORCED where it runs.
UP_BUDGET_S="${UP_BUDGET_S:-1800}"            # reap + gke-up (+ node pools)
INSTALL_TIMEOUT_S="${INSTALL_TIMEOUT_S:-900}"  # helm install --wait --timeout
DEPLOY_WAIT_S="${DEPLOY_WAIT_S:-1800}"        # snowplow/frontend/authn Available
DATA_TIMEOUT_S="${DATA_TIMEOUT_S:-2400}"      # data606 (run 8: 1041 s)
ROLLOUT_TIMEOUT_S="${ROLLOUT_TIMEOUT_S:-2700}" # reboot: rollout status
COLLECT_CAP_S="${COLLECT_CAP_S:-1800}"        # collect + evaluate, all reads together
DIAG_CAP_S="${DIAG_CAP_S:-900}"               # diag606 snapshot on failure
DOWN_BUDGET_S="${DOWN_BUDGET_S:-900}"         # gke-down
TTL_MARGIN_S="${TTL_MARGIN_S:-1800}"
if [[ "$RUN8_REPORT" == "1" ]]; then
  # shellcheck disable=SC2086
  python3 -m bench.load606 check-prefixes $MEM_VARS_PREFIXES \
    || { echo "refusing: MEM_VARS_PREFIXES does not capture every run-8 key" >&2; exit 2; }
fi
# TTL budget (reviewer-small (d)): the whole run's worst case, every bounded
# step at its maximum, must fit inside the cluster's TTL minus a margin, or
# the TTL reaper could delete the cluster mid-measure (or a hung step could
# outlive it). Refused here, before any cloud call.
RUN_BUDGET_S=$(( UP_BUDGET_S + INSTALL_TIMEOUT_S + PLATFORM_TIMEOUT_S + DEPLOY_WAIT_S
                 + ENDPOINT_WAIT_S + PREFLIGHT_TIMEOUT_S + DATA_TIMEOUT_S
                 + ROLLOUT_TIMEOUT_S + DIGEST_SETTLE_S + MEASURE_WAIT_S
                 + COLLECT_CAP_S + DIAG_CAP_S + DOWN_BUDGET_S ))
TTL_S=$(( ${TTL_HOURS%.*} * 3600 ))
budget_line="up ${UP_BUDGET_S} + install ${INSTALL_TIMEOUT_S} + platform ${PLATFORM_TIMEOUT_S} + deploy ${DEPLOY_WAIT_S} + env ${ENDPOINT_WAIT_S} + preflight ${PREFLIGHT_TIMEOUT_S} + data ${DATA_TIMEOUT_S} + reboot ${ROLLOUT_TIMEOUT_S} + digest ${DIGEST_SETTLE_S} + measure ${MEASURE_WAIT_S} (fresh ${FRESH_EST_S}, driver ${DRIVER_EST_S}) + collect ${COLLECT_CAP_S} + diag ${DIAG_CAP_S} + down ${DOWN_BUDGET_S} = ${RUN_BUDGET_S} s"
if (( RUN_BUDGET_S > TTL_S - TTL_MARGIN_S )); then
  echo "refusing: worst-case run ${RUN_BUDGET_S} s exceeds TTL_HOURS=${TTL_HOURS} (${TTL_S} s) minus margin ${TTL_MARGIN_S} s" >&2
  echo "  budget: ${budget_line}" >&2
  echo "  needs TTL_HOURS >= $(( (RUN_BUDGET_S + TTL_MARGIN_S + 3599) / 3600 )), or fewer FRESH_SAMPLES / CHURN_RATES / FRESH_MODES" >&2
  exit 2
fi
COMPOSITE_ROUTES=()
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

# bounded <seconds> cmd...: run cmd, TERM it after <seconds> (portable: macOS
# has no `timeout`). Output and exit status are cmd's own (143 if killed).
bounded() {
  local secs="$1"; shift
  "$@" &
  local pid=$!
  ( sleep "$secs"; kill -TERM "$pid" 2>/dev/null ) >/dev/null 2>&1 &
  local dog=$!
  local rc=0
  wait "$pid" || rc=$?
  kill "$dog" 2>/dev/null; wait "$dog" 2>/dev/null || true
  return "$rc"
}
# capped <seconds> <label> cmd...: run cmd in the background under ONE
# wall-clock cap; stop early on the cap or when teardown was signalled.
TD_SIGNALED=0
CAPPED_PID=""
capped() {
  local secs="$1" label="$2"; shift 2
  ( set +e; "$@" ) &
  CAPPED_PID=$!
  local waited=0
  while kill -0 "$CAPPED_PID" 2>/dev/null; do
    if (( waited >= secs )) || [[ "$TD_SIGNALED" == "1" ]]; then
      log "${label}: stopped after ${waited}s ($([[ "$TD_SIGNALED" == "1" ]] && echo 'signal during teardown' || echo "cap ${secs}s reached")); continuing to teardown"
      pkill -TERM -P "$CAPPED_PID" 2>/dev/null || true
      kill -TERM "$CAPPED_PID" 2>/dev/null || true
      break
    fi
    sleep 1 || true      # a process-group SIGINT kills sleep (130): never fatal here
    waited=$(( waited + 1 ))
  done
  wait "$CAPPED_PID" 2>/dev/null || true
  CAPPED_PID=""
}
on_teardown_signal() {
  # A signal DURING teardown must never skip gke-down (reviewer-small (a)):
  # it only cuts the collect/diagnostics short.
  TD_SIGNALED=1
  log "signal during teardown: collect/diagnostics cut short; proceeding to gke-down"
  if [[ -n "$CAPPED_PID" ]]; then
    pkill -TERM -P "$CAPPED_PID" 2>/dev/null || true
    kill -TERM "$CAPPED_PID" 2>/dev/null || true
  fi
}

CLUSTER_CREATED=0
IN_TEARDOWN=0
teardown() {
  local rc=$?
  IN_TEARDOWN=1     # from here a signal only cuts collect/diagnostics short (on_signal)
  # FIRST: no signal may abort teardown from here on (the run's INT/TERM/HUP
  # trap is `exit 130`, which inside teardown would skip gke-down).
  trap on_teardown_signal INT TERM HUP
  set +e   # nothing inside teardown may abort it (a Ctrl-C reaches every child: set -e would exit on their 130)
  trap - EXIT
  declare -F stop_top_loop >/dev/null && stop_top_loop
  declare -F stop_log_stream >/dev/null && stop_log_stream
  if [[ "$CLUSTER_CREATED" == "1" ]]; then
    # On ANY failure, snapshot the cluster BEFORE it is deleted (shakedown #2
    # tore down with nothing captured). Best effort: it never blocks teardown.
    if [[ "$rc" != "0" ]]; then
      # #763: once measure has started, ANY failure first collects the run's
      # own results (job logs — running Jobs too —, heap profiles, the
      # after-record, snowplow logs) and computes a verdict over whatever was
      # collected. Run 8 lost the driver log, the heap profiles and the
      # snowplow logs this way.
      if [[ "${MEASURE_STARTED:-0}" == "1" && "${COLLECTED:-0}" != "1" ]]; then
        log "measure FAILED or interrupted (exit ${rc}): collecting partial results before teardown"
        capped "$COLLECT_CAP_S" "partial collect" collect_and_evaluate
      fi
      if [[ "$DRY" == "1" ]]; then
        log "DRY: python3 -m bench.diag606 collect --out $OUT/diagnostics"
      elif [[ "$TD_SIGNALED" == "1" ]]; then
        log "signalled: skipping diagnostics, deleting the cluster now"
      elif [[ -s "$KCFG" ]]; then
        log "=== diagnostics before teardown -> $OUT/diagnostics ==="
        capped "$DIAG_CAP_S" "diagnostics" python3 -m bench.diag606 collect \
            --kubeconfig "$KCFG" --context "$KCTX" --out "$OUT/diagnostics" --redact "$PW"
      else
        log "no kubeconfig yet; no diagnostics to collect"
      fi
    fi
    # Test hook (like BENCH_606_FAIL_AT): a command that fails inside teardown,
    # as one killed by a group signal would. It must not stop the delete.
    [[ "${BENCH_606_TEARDOWN_FAULT:-}" == "1" ]] && false
    log "=== teardown ${RUN_NAME} (exit ${rc}) ==="
    # shielded: a process-group Ctrl-C must not interrupt the delete itself
    if ! ( trap "" INT TERM HUP; exec python3 -m bench.gke gke-down --name "$RUN_NAME" --project "$PROJECT" --zone "$ZONE" $YES ); then
      log "TEARDOWN FAILED — delete by hand: gcloud container clusters delete ${RUN_NAME} --project ${PROJECT} --zone ${ZONE} --quiet"
      rc=$(( rc == 0 ? 1 : rc ))
    else
      # printed ONLY after gke-down succeeded
      if [[ "$DRY" == "1" ]]; then log "DRY: gke-down only printed; nothing was deleted"; else log "cluster ${RUN_NAME} deleted (gke-down OK)"; fi
    fi
  fi
  exit "$rc"
}
trap teardown EXIT
# Before teardown a signal ends the run (130) and the EXIT trap tears down;
# INSIDE teardown the same handler only cuts collect/diagnostics short. One
# handler for both closes the window between the EXIT trap firing and
# teardown replacing the traps.
on_signal() {
  if [[ "$IN_TEARDOWN" == "1" ]]; then on_teardown_signal; else exit 130; fi
}
trap on_signal INT TERM HUP

s_source() {
  # Registry preflight, BEFORE any cluster exists (read-only, ghcr only):
  # the chart must resolve at INSTALLER_VERSION and, with VERIFY483=1, its
  # pins must carry the #483 gate. Either failure refuses the run for free.
  local pins=(--no-pins)
  [[ "$VERIFY483" == "1" ]] && pins=()
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.installer_src oci-preflight --chart $INSTALLER_CHART --version $INSTALLER_VERSION ${pins[*]:-}"
    return 0
  fi
  rm -rf "$OUT/installer-pulled"
  python3 -m bench.installer_src oci-preflight --chart "$INSTALLER_CHART" \
      --version "$INSTALLER_VERSION" --workdir "$OUT/installer-pulled" \
      --out "$OUT/installer-preflight.json" ${pins[@]+"${pins[@]}"}
  log "installer source: $INSTALLER_CHART --version $INSTALLER_VERSION (registry preflight OK$([[ "$VERIFY483" == "1" ]] && echo ', #483 pins present'))"
}
s_reap()   { run python3 -m bench.gke gke-reap --project "$PROJECT" $YES; }
s_up() {
  CLUSTER_CREATED=1   # set BEFORE create: a half-created cluster still bills
  run bounded "$UP_BUDGET_S" python3 -m bench.gke gke-up --name "$RUN_NAME" --project "$PROJECT" \
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
      --wait --timeout "${INSTALL_TIMEOUT_S}s"
}
s_verify483() {
  # installer#483 acceptance. Items 1-4 gate (a FAIL aborts the run with
  # diagnostics before teardown); item 5 is recorded only.
  run python3 -m bench.diag606 verify483 --kubeconfig "$KCFG" --context "$KCTX" \
      --out "$OUT/verify483.json"
}
s_wait_platform() {
  # Polled, not a blind wait: prints which components are Ready (and why the
  # others are not) every PLATFORM_POLL_S, so a stall shows WHERE.
  run python3 -m bench.diag606 wait-installer --kubeconfig "$KCFG" \
      --context "$KCTX" --timeout-s "$PLATFORM_TIMEOUT_S" \
      --interval-s "$PLATFORM_POLL_S"
  # NEVER wait on the umbrella Installer being Ready: on a credential-free
  # bench it never is (github-provider-kog stays 401, installer#484). Wait on
  # the specific components the bench uses: their Deployments Available and
  # their Services present (checked by the env step).
  run "${K[@]}" -n krateo-system wait --for=condition=Available \
      $(printf 'deploy/%s ' $REQUIRED_DEPLOYMENTS) --timeout="${DEPLOY_WAIT_S}s"
  # Record WHERE snowplow runs: a pod outside the dedicated pool shares CPU.
  run "${K[@]}" -n krateo-system get pods -l app.kubernetes.io/name=snowplow \
      -o wide | tee -a "$OUT/snowplow-placement.txt" || true
}
s_env() {
  # ENDPOINT_MODE=incluster (default): Service DNS for the in-cluster Jobs —
  # every step that talks HTTP to snowplow/authn runs in-cluster, so no
  # external IP is needed (run 5: LBs pending, IN_USE_ADDRESSES quota).
  # ENDPOINT_MODE=external: LoadBalancer IPs with a bounded, reporting wait.
  log "endpoint mode: $ENDPOINT_MODE"
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.gke gke-env --name $RUN_NAME --mode $ENDPOINT_MODE > $OUT/env.sh"
    printf 'export SNOWPLOW_URL=http://snowplow.krateo-system.svc:8081 AUTHN_URL=http://authn.krateo-system.svc:8082\n' > "$OUT/env.sh"
  else
    python3 -m bench.gke gke-env --name "$RUN_NAME" --project "$PROJECT" --zone "$ZONE" \
        --mode "$ENDPOINT_MODE" --wait-s "$ENDPOINT_WAIT_S" \
        --interval-s "$ENDPOINT_POLL_S" > "$OUT/env.sh"
  fi
  # shellcheck disable=SC1091
  source "$OUT/env.sh"
  # gke-env re-exports these; re-assert the pin regardless of what it printed.
  export KUBECONFIG="$KCFG" BENCH_GKE_CONTEXT="$KCTX" EXPECTED_IMAGE_TAG="$SNOWPLOW_TAG"
  # The platform is up: snowplow's log is streamed from here to collect.
  start_log_stream
}
s_preflight() {
  # SECONDS-scale, right after env and BEFORE stamping or any login burst
  # (run 6 discovered stale page roots 54 minutes in). The boot root is the
  # frontend's own config.json INIT — never a hard-coded page name.
  if [[ "$DRY" == "1" ]]; then
    log "DRY: read frontend-config-vars config.json -> PORTAL_INIT; preflight606 Job"
    export PORTAL_INIT="/call?resource=layouts&apiVersion=widgets.templates.krateo.io/v1beta1&name=app-shell&namespace=krateo-system"
    export PORTAL_FRONTEND_NS=krateo-system PORTAL_BUILDERS_NS=krateo-system
    return 0
  fi
  "${K[@]}" -n krateo-system get configmap "$FRONTEND_CONFIGMAP" \
      -o 'jsonpath={.data.config\.json}' > "$OUT/frontend-config.json"
  python3 -m bench.load606 portal-config < "$OUT/frontend-config.json" > "$OUT/portal.env"
  # shellcheck disable=SC1091
  source "$OUT/portal.env"
  log "portal boot root (config.json INIT): $PORTAL_INIT"
  run python3 -m bench data606-preflight-user >&2
  python3 -m bench.load606_job --preflight-only --snowplow-url "$SNOWPLOW_URL" \
      --authn-url "$AUTHN_URL" --password "$PW" --init "$PORTAL_INIT" \
      --frontend-ns "$PORTAL_FRONTEND_NS" --builders-ns "$PORTAL_BUILDERS_NS" \
      > "$OUT/preflight-job.yaml"
  run "${K[@]}" apply -f "$OUT/preflight-job.yaml"
  local deadline=$(( $(date +%s) + PREFLIGHT_TIMEOUT_S )) ok="" failed=""
  while (( $(date +%s) < deadline )); do
    ok="$("${K[@]}" -n bench-loadgen get job preflight606 -o 'jsonpath={.status.succeeded}' 2>/dev/null || true)"
    failed="$("${K[@]}" -n bench-loadgen get job preflight606 -o 'jsonpath={.status.failed}' 2>/dev/null || true)"
    [[ -n "$ok" && "$ok" != "0" ]] && break
    [[ -n "$failed" && "$failed" != "0" ]] && break
    sleep "$PREFLIGHT_POLL_S"
  done
  "${K[@]}" -n bench-loadgen logs job/preflight606 > "$OUT/preflight.jsonl" 2>&1 || true
  python3 -m bench.load606 preflight-summary < "$OUT/preflight.jsonl" >&2 || true
  if [[ -n "$failed" && "$failed" != "0" ]]; then
    log "PREFLIGHT REFUSED (config error: unserved type / 404) — see $OUT/preflight.jsonl"
    return 1
  fi
  if [[ -z "$ok" || "$ok" == "0" ]]; then
    log "PREFLIGHT did not finish within ${PREFLIGHT_TIMEOUT_S}s"
    return 1
  fi
  log "preflight OK (product errors, if any, are recorded, not fatal)"
  local r
  while IFS= read -r r; do [[ -n "$r" ]] && COMPOSITE_ROUTES+=("$r"); done \
    < <(python3 -m bench.load606 preflight-composite < "$OUT/preflight.jsonl")
  if (( COMPOSITE_BURST_USERS > 0 )) && (( ${#COMPOSITE_ROUTES[@]} == 0 )); then
    log "WARNING: no composite page found in the live menu: the composite burst will record FAILED navigations"
  fi
  log "composite pages for the cold burst: ${COMPOSITE_ROUTES[*]:-NONE}"
}
s_data() {
  # Data stamping is kubectl-only. User LOGINS need authn over HTTP: in
  # in-cluster mode they are deferred to the in-cluster driver.
  local login=()
  [[ "$ENDPOINT_MODE" == "incluster" ]] && login=(--no-login)
  run bounded "$DATA_TIMEOUT_S" python3 -m bench data606 --scale "$SCALE" --users "$USERS" \
      --namespaces "$NAMESPACES" --hold-probe-wait-s "$HOLD_PROBE_WAIT_S" \
      --out "$OUT/data-proof.json" ${login[@]+"${login[@]}"} \
      $([[ "$FRESH_MODES" == "annotation" ]] || printf -- '--fresh-modes %s' "$FRESH_MODES")
}
s_reboot() {
  # Cold start at 50K: restart once the data exists, time Ready (#594/#637).
  run "${K[@]}" -n krateo-system rollout restart deploy/snowplow
  run "${K[@]}" -n krateo-system rollout status deploy/snowplow --timeout="${ROLLOUT_TIMEOUT_S}s"
}
s_digest() {
  # VOID gate (run 8): after the reboot, before any load. Reads what the
  # kubelet PULLED (status.containerStatuses[].imageID) for every live
  # snowplow pod; the record (expected, every pod's imageID and digest) is
  # kept in $OUT/image-digest.jsonl and fed to the verdict.
  if [[ "$DRY" == "1" ]]; then
    log "DRY: snowplow pods imageID == $EXPECTED_IMAGE_DIGEST (else VOID)"
    return 0
  fi
  # `rollout status` returns once the old ReplicaSet reports 0 replicas, while
  # its pod may still be Terminating: wait (bounded) until no snowplow pod has
  # a deletionTimestamp, so neither this gate nor the before-record can read
  # a leftover. A leftover that never clears fails the step, by name.
  local rc deadline=$(( $(date +%s) + DIGEST_SETTLE_S ))
  while true; do
    rc=0
    "${K[@]}" -n krateo-system get pods -l "$SNOWPLOW_POD_SELECTOR" -o json \
      | python3 -m bench.load606 digest-gate --expected "$EXPECTED_IMAGE_DIGEST" \
          --require-settled > "$OUT/image-digest.jsonl" || rc=$?
    (( rc == 4 )) || break
    if (( $(date +%s) >= deadline )); then
      log "digest gate: a snowplow pod is still Terminating after ${DIGEST_SETTLE_S}s: $(cut -c1-600 "$OUT/image-digest.jsonl")"
      log "FAILED: rollout leftovers did not clear; no load is applied"
      return 1
    fi
    log "digest gate: waiting for Terminating snowplow pod(s) to go ($(python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)["terminating"]))' < "$OUT/image-digest.jsonl"))"
    sleep "$DIGEST_POLL_S"
  done
  log "image digest gate: $(cut -c1-600 "$OUT/image-digest.jsonl" 2>/dev/null)"
  if (( rc != 0 )); then
    log "VOID: a snowplow pod does not run ${EXPECTED_IMAGE_DIGEST} (gate exit ${rc}); no load is applied"
    return 1
  fi
  log "image digest MATCH: every live snowplow pod runs ${EXPECTED_IMAGE_DIGEST}"
}
s_measure() {
  # Load: in-cluster on the tainted loadgen pool, one Indexed-Job pod per
  # shard. Each pod writes its JSONL to /out/nav.jsonl (per nav, flushed),
  # turns Ready when the drive is over, and stays up so the collect step can
  # `kubectl cp` the file (#817: run 9 read it from stdout with `kubectl
  # logs`, which after log rotation returned only the last ~10 minutes and
  # none of the navigations). The page roots are the
  # harness's known page widgets; drive's preflight refuses if any is not 200
  # on this portal version.
  # Both Jobs run IN-CLUSTER on the loadgen pool and reach snowplow/authn by
  # the endpoints the env step resolved (Service DNS in incluster mode):
  #   load606   the 1000-user driver;
  #   fresh606  the list_dep freshness probe, stepping CHURN_RATES (labels
  #             list_dep@<rate>; the verdict gates at 10/s, reports the rest),
  #             mutating through the Kubernetes API with its own SA.
  # shellcheck disable=SC2086
  python3 -m bench.load606_job --users "$USERS" --shards "$DRIVER_SHARDS" \
      --driver-cpu "$DRIVER_CPU" --driver-memory "$DRIVER_MEMORY" \
      --think-s "$THINK_S" --duration-s "$DRIVE_S" \
      --linger-s "$(( MEASURE_WAIT_S + COLLECT_CAP_S ))" \
      --snowplow-url "$SNOWPLOW_URL" --authn-url "$AUTHN_URL" --password "$PW" \
      --init "$PORTAL_INIT" --frontend-ns "$PORTAL_FRONTEND_NS" \
      --builders-ns "$PORTAL_BUILDERS_NS" \
      --fresh-rates $CHURN_RATES --fresh-samples "$FRESH_SAMPLES" \
      --fresh-gap-s "$FRESH_GAP_S" --fresh-delay-s "$THINK_S" \
      --namespaces "$NAMESPACES" \
      --mem-duration-s "$MEASURE_WAIT_S" \
      $([[ "$FRESH_MODES" == "annotation" ]] || printf -- '--fresh-modes %s' "$FRESH_MODES") \
      $(for x in $MEM_VARS_PREFIXES; do printf -- '--mem-vars-prefix %s ' "$x"; done) \
      --burst-users "$COMPOSITE_BURST_USERS" --burst-at-s "$COMPOSITE_BURST_AT_S" \
      ${COMPOSITE_ROUTES[@]+$(printf -- '--composite-route %q ' "${COMPOSITE_ROUTES[@]}")} \
      $([[ "$VIEWERS" == "1" ]] || printf -- '--no-viewers') \
      > "$OUT/loadgen-job.yaml"
  # snowplow's restart state BEFORE measure (the verdict compares after/before:
  # any restart or OOM during measure makes the run INVALID, with its reason).
  snowplow_pod_record before
  # Standard memory capture (#710): kubectl top every TOP_POLL_S from here,
  # plus the in-cluster mem606 Job (memstats + heap profiles, metadata only).
  start_top_loop
  MEASURE_STARTED=1
  run "${K[@]}" apply -f "$OUT/loadgen-job.yaml"
  local deadline=$(( $(date +%s) + MEASURE_WAIT_S ))
  # The teardown trap collects what exists before the cluster goes (#763).
  wait_drive_done "$deadline" || return 1
  if ! run "${K[@]}" -n bench-loadgen wait job/fresh606 --for=condition=Complete \
      --timeout="$(( deadline > $(date +%s) ? deadline - $(date +%s) : 1 ))s"; then
    log "measure FAILED: fresh606 not Complete within ${MEASURE_WAIT_S}s"
    return 1
  fi
}
# wait_drive_done <deadline>: every load606 pod is Ready, i.e. its drive has
# exited and /out/nav.jsonl is final (load606_job: readinessProbe on
# /out/done). The pods then stay up for collect, so the Job is NOT Complete
# here. A failed driver pod fails the step at once, not at the deadline.
wait_drive_done() {
  local deadline="$1" st ready failed want
  if [[ "$DRY" == "1" ]]; then log "DRY: wait until every load606 pod is Ready (drive over)"; return 0; fi
  log "waiting for the driver: load606 pods Ready = drive exited, results final"
  while true; do
    st="$("${K[@]}" --request-timeout=60s -n bench-loadgen get job/load606 \
          -o jsonpath='{.status.ready}/{.status.failed}/{.spec.completions}' 2>/dev/null || true)"
    IFS=/ read -r ready failed want <<< "$st"
    if [[ -n "$want" && "${ready:-0}" -ge "$want" ]]; then
      driver_exit_codes "${ready}/${want}"
      return 0
    fi
    if [[ "${failed:-0}" -ge 1 ]]; then
      log "measure FAILED: a load606 driver pod failed before its results were final"
      return 1
    fi
    if (( $(date +%s) >= deadline )); then
      log "measure FAILED: load606 driver not done within ${MEASURE_WAIT_S}s (${ready:-0}/${want:-?} Ready)"
      return 1
    fi
    sleep "$DRIVE_POLL_S"
  done
}
# driver_exit_codes <ready>: Ready only says the drive is OVER; each pod's
# /out/done holds drive's exit code (load606_job), and the log says which it
# was. A non-zero exit is not a measure failure (fresh606 still runs and its
# samples stand); the verdict makes the run INVALID with the driver's reason.
driver_exit_codes() {
  local p rc bad=""
  for p in $("${K[@]}" --request-timeout=60s -n bench-loadgen get pods -l job-name=load606 -o name 2>/dev/null); do
    rc="$(bounded 60 "${K[@]}" --request-timeout=60s -n bench-loadgen exec "${p#pod/}" -- cat /out/done 2>/dev/null | tr -d '[:space:]')"
    [[ "$rc" == "0" ]] || bad+=" ${p#pod/}=rc${rc:-?}"
  done
  if [[ -z "$bad" ]]; then
    log "driver done: $1 load606 pod(s) Ready, every drive exited 0"
  else
    log "driver FAILED:${bad} (drive over, results kept; verdict.json says why)"
  fi
}
snowplow_pod_record() {
  local phase="$1"
  if [[ "$DRY" == "1" ]]; then log "DRY: snowplow pod record ($phase)"; return 0; fi
  bounded "$COLLECT_TIMEOUT_S" "${K[@]}" --request-timeout="${COLLECT_TIMEOUT_S}s" \
      -n krateo-system get pods -l "$SNOWPLOW_POD_SELECTOR" -o json \
    | python3 -m bench.load606 pod-record --phase "$phase" > "$OUT/pod-$phase.jsonl" \
    || log "could not record the snowplow pod ($phase); the verdict will say restart state unknown"
  log "snowplow pod ($phase): $(cat "$OUT/pod-$phase.jsonl" 2>/dev/null | cut -c1-240)"
}
# safe_pid <value>: a PID that may be signalled — a positive integer above 1.
# `kill 0` signals the caller's whole process group and `kill -1` every
# process it may signal (#824): never from a corrupt pidfile or variable.
safe_pid() { [[ "${1:-}" =~ ^[0-9]+$ ]] && (( 10#$1 > 1 )); }
# take_pid <pidfile>: print the pidfile's PID and remove the file FIRST, so no
# later caller (teardown after collect's subshell) signals a reused PID.
take_pid() {
  local f="$1" pid
  [[ -n "$f" && -f "$f" ]] || return 1
  pid="$(cat "$f")"
  rm -f "$f"
  safe_pid "$pid" || return 1
  echo "$pid"
}
TOP_PIDFILE=""
start_top_loop() {
  if [[ "$DRY" == "1" ]]; then log "DRY: kubectl top loop -> $OUT/top.log"; return 0; fi
  ( while true; do
      line="$("${K[@]}" -n krateo-system top pod -l "$SNOWPLOW_POD_SELECTOR" --no-headers 2>&1 | tr -s ' ' | tr '\n' ' ' | tr -d '"\\')"
      printf '{"kind":"top","t":%s,"raw":"%s"}\n' "$(date +%s)" "$line"
      sleep "$TOP_POLL_S"
    done ) >> "$OUT/top.jsonl" 2>&1 &
  TOP_PIDFILE="$OUT/.top-loop.pid"
  echo "$!" > "$TOP_PIDFILE"
}
stop_top_loop() {
  local pid
  pid="$(take_pid "${TOP_PIDFILE:-}")" || return 0
  kill "$pid" 2>/dev/null || true
}
# #822: snowplow's log, streamed for the WHOLE run (env → collect) into
# $OUT/snowplow-stream.log. A one-shot `kubectl logs` at collect returns only
# the container's current file (run 9: the last ~30 s). The stream follows
# across dropped connections (resuming at the newest kubelet timestamp seen;
# `--since-time` is inclusive, and bench.logscrub drops the resent lines) and
# across a REPLACED pod (it follows the new pod by name, and first reads the
# old pod's tail while that is still readable; a tail it could not read is
# marked). Every line passes bench.logscrub: metadata only (no body, username
# or token reaches $OUT).
# The loop's PID lives in a pidfile, not only in a variable: stop_log_stream
# also runs inside collect's `capped` subshell, and a stale variable in the
# parent would make teardown kill a PID that may have been reused (#823 review).
BENCH_USER_PREFIX="${BENCH_USER_PREFIX:-scaleuser-}"   # load606 drive --user-prefix
LOG_STREAM_PIDFILE=""
scrub() { python3 -m bench.logscrub --deny "$PW" --deny "$BENCH_USER_PREFIX" "$@"; }
# snowplow_live_pod: the newest snowplow pod not being deleted (empty if none).
snowplow_live_pod() {
  "${K[@]}" --request-timeout=30s -n krateo-system get pods -l "$SNOWPLOW_POD_SELECTOR" \
      -o jsonpath='{range .items[*]}{.metadata.creationTimestamp}{" "}{.metadata.name}{" "}{.metadata.deletionTimestamp}{"\n"}{end}' \
      2>/dev/null | awk 'NF == 2' | sort | tail -1 | cut -d' ' -f2
}
start_log_stream() {
  if [[ "$DRY" == "1" ]]; then log "DRY: stream snowplow's log -> $OUT/snowplow-stream.log"; return 0; fi
  local S="$OUT/.log-stream"
  rm -f "$S-stop" "$S-state" "$S-last"
  ( set +e
    pod=""
    while [[ ! -e "$S-stop" ]]; do
      cur="$(snowplow_live_pod)"
      if [[ -n "$pod" && -n "$cur" && "$cur" != "$pod" ]]; then
        # The pod was replaced: its lines after the last one streamed are on
        # the OLD pod. Read them while it is readable, then start the new
        # pod from its beginning (its own timestamps, its own dedup state).
        since="$(cat "$S-last" 2>/dev/null || true)"
        if "${K[@]}" --request-timeout=60s -n krateo-system logs "pod/$pod" --timestamps \
              ${since:+"--since-time=$since"} 2>>"$OUT/snowplow-stream.err" \
            | scrub --kubelet-timestamps --state-file "$S-state" --last-time-file "$S-last" \
              >> "$OUT/snowplow-stream.log"; then
          printf '{"bench_stream":"pod_replaced","tail":"read","t":%s}\n' "$(date +%s)" >> "$OUT/snowplow-stream.log"
        else
          printf '{"bench_stream":"pod_replaced","tail":"lost","t":%s}\n' "$(date +%s)" >> "$OUT/snowplow-stream.log"
        fi
        rm -f "$S-state" "$S-last"
      fi
      target="deploy/snowplow"
      if [[ -n "$cur" ]]; then pod="$cur"; target="pod/$cur"; fi
      since="$(cat "$S-last" 2>/dev/null || true)"
      "${K[@]}" -n krateo-system logs -f "$target" --timestamps \
          ${since:+"--since-time=$since"} 2>>"$OUT/snowplow-stream.err" \
        | scrub --kubelet-timestamps --state-file "$S-state" --last-time-file "$S-last" \
          >> "$OUT/snowplow-stream.log"
      [[ -e "$S-stop" ]] && break
      printf '{"bench_stream":"reconnect","t":%s}\n' "$(date +%s)" >> "$OUT/snowplow-stream.log"
      sleep "$LOG_STREAM_RETRY_S"
    done ) >/dev/null 2>&1 &
  LOG_STREAM_PIDFILE="$S.pid"
  echo "$!" > "$LOG_STREAM_PIDFILE"
  log "streaming snowplow's log (scrubbed) -> $OUT/snowplow-stream.log"
}
stop_log_stream() {
  local pid
  [[ -n "${LOG_STREAM_PIDFILE:-}" && -f "$LOG_STREAM_PIDFILE" ]] || return 0
  touch "$OUT/.log-stream-stop"    # the loop ends on its own even if no PID is usable
  pid="$(take_pid "$LOG_STREAM_PIDFILE")" || return 0
  pkill -TERM -P "$pid" 2>/dev/null || true
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}
s_collect() {
  local p
  if [[ "$DRY" == "1" ]]; then
    log "DRY: kubectl cp <each load606 pod>:/out/nav.jsonl $OUT/nav-<pod>.jsonl"
    return 0
  fi
  # Every piece is best effort and bounded: this also runs from the teardown
  # trap after a failed measure (#763), where Jobs may still be Running
  # (`kubectl logs` / `cp` work on a running pod) and any one read may fail.
  # Each read: --request-timeout AND an outer wall-clock bound, because
  # --request-timeout does not bound an open exec stream (`kubectl cp`). The
  # whole collect + evaluate runs under ONE cap (COLLECT_CAP_S, see `capped`).
  local KR=(bounded "$COLLECT_TIMEOUT_S" "${K[@]}" --request-timeout="${COLLECT_TIMEOUT_S}s")
  # The driver's results are the FILE on its /out (#817), never its container
  # log: the kubelet rotates that, and `kubectl logs` returns only the current
  # file. The log keeps the driver's diagnostics.
  for p in $("${KR[@]}" -n bench-loadgen get pods -l job-name=load606 -o name); do
    "${KR[@]}" -n bench-loadgen cp "${p#pod/}:/out/nav.jsonl" "$OUT/nav-${p#pod/}.jsonl" >/dev/null 2>&1 \
      || log "could not copy the driver's results (/out/nav.jsonl) from ${p#pod/}: the verdict will say the stream is missing or incomplete"
    "${KR[@]}" -n bench-loadgen logs "$p" > "$OUT/driver-${p#pod/}.log" \
      || log "could not read the driver's diagnostics log from ${p#pod/}"
  done
  for p in $("${KR[@]}" -n bench-loadgen get pods -l job-name=fresh606 -o name); do
    "${KR[@]}" -n bench-loadgen logs "$p" > "$OUT/fresh-${p#pod/}.jsonl" \
      || log "could not read the fresh606 log from ${p#pod/}"
  done
  snowplow_pod_record after
  declare -F stop_top_loop >/dev/null && stop_top_loop
  # How every in-cluster Job pod ended, for the verdict (#817 review): a pod
  # that died (OOMKilled, evicted, a crash), never ran (Pending / Unknown) or
  # has no pod at all makes the run INVALID with its reason in verdict.json.
  # Before mem606's bounded stop, so nothing slow can come between.
  : > "$OUT/job-pods.jsonl"
  for p in load606 fresh606 mem606; do
    "${KR[@]}" -n bench-loadgen get pods -l "job-name=$p" -o json 2>/dev/null \
      | python3 -m bench.load606 job-pod-record --job "$p" >> "$OUT/job-pods.jsonl" \
      || log "could not record how the $p pod(s) ended"
  done
  # mem606: its memstats trajectory and heap profiles, both files in /out
  # (#817: its stdout log was cut at the head too in run 9). Ask it to end
  # its stream first (/out/stop → mem_end, then the wrapper's mem_exit and
  # /out/exited), so the copy is the final, framed stream; a sampler that
  # does not answer in MEM_STOP_WAIT_S is copied as is and the verdict says
  # the tail is missing.
  for p in $("${KR[@]}" -n bench-loadgen get pods -l job-name=mem606 -o name); do
    mem_stop "${p#pod/}"
    mkdir -p "$OUT/heap"
    "${KR[@]}" -n bench-loadgen cp "${p#pod/}:/out" "$OUT/heap" >/dev/null 2>&1 \
      || log "could not copy heap profiles from ${p#pod/}"
    if [[ -f "$OUT/heap/mem.jsonl" ]]; then
      mv "$OUT/heap/mem.jsonl" "$OUT/mem-${p#pod/}.jsonl"
    else
      log "could not copy the mem606 trajectory (/out/mem.jsonl) from ${p#pod/}"
    fi
  done
  # #822: the streamed log is the run's snowplow log; the one-shot reads below
  # only cover a stream gap at the very end. All of it scrubbed.
  declare -F stop_log_stream >/dev/null && stop_log_stream
  "${KR[@]}" -n krateo-system logs deploy/snowplow --tail=-1 --timestamps \
    | scrub --kubelet-timestamps > "$OUT/snowplow.log" \
    || log "could not read the snowplow log"
  "${KR[@]}" -n krateo-system logs deploy/snowplow --previous --tail=-1 --timestamps 2>/dev/null \
    | scrub --kubelet-timestamps > "$OUT/snowplow-previous.log"
  [[ -s "$OUT/snowplow-previous.log" ]] || rm -f "$OUT/snowplow-previous.log"
  "${KR[@]}" top nodes > "$OUT/top-nodes.txt" || true
  COLLECTED=1
  log "collected: $(cd "$OUT" && ls nav-*.jsonl fresh-*.jsonl mem-*.jsonl pod-after.jsonl snowplow.log 2>/dev/null | tr '\n' ' ')heap=$(ls "$OUT/heap" 2>/dev/null | wc -l | tr -d ' ')"
}
# mem_stop <pod>: ask mem606 for its final stream and wait until its wrapper
# has written the exit code — against a WALL-CLOCK deadline of
# MEM_STOP_WAIT_S, every exec included and each one bounded by what is left
# (#823 review: counting only the sleeps let 60 s execs run ~39 min, past
# COLLECT_CAP_S, killing collect before job-pods and the snowplow log).
mem_stop() {
  local pod="$1" deadline left
  deadline=$(( $(date +%s) + MEM_STOP_WAIT_S ))
  left=$(( MEM_STOP_WAIT_S < 60 ? MEM_STOP_WAIT_S : 60 ))
  bounded "$left" "${K[@]}" --request-timeout="${left}s" -n bench-loadgen exec "$pod" -- \
      touch /out/stop >/dev/null 2>&1 || { log "could not ask $pod to end its stream"; return 0; }
  while true; do
    left=$(( deadline - $(date +%s) ))
    if (( left <= 0 )); then
      log "$pod did not end its stream within ${MEM_STOP_WAIT_S}s: copying it as is"
      return 0
    fi
    (( left > 60 )) && left=60
    bounded "$left" "${K[@]}" --request-timeout="${left}s" -n bench-loadgen exec "$pod" -- \
        test -f /out/exited >/dev/null 2>&1 && return 0
    left=$(( deadline - $(date +%s) ))
    (( left > 0 )) && sleep "$(( left < 5 ? left : 5 ))"
  done
}
collect_and_evaluate() { s_collect; s_evaluate; }
s_evaluate() {
  shopt -s nullglob
  local files=("$OUT"/nav-*.jsonl "$OUT"/fresh-*.jsonl "$OUT"/preflight.jsonl
               "$OUT"/pod-*.jsonl "$OUT"/mem-*.jsonl "$OUT"/top.jsonl
               "$OUT"/image-digest.jsonl "$OUT"/job-pods.jsonl)
  if [[ "$DRY" == "1" ]]; then log "DRY: python3 -m bench.load606 evaluate ${files[*]:-<none>}"; return 0; fi
  if [[ ${#files[@]} -eq 0 ]]; then log "no result files to evaluate"; return 1; fi
  # The verdict is a RESULT, not a tooling failure: record it, do not abort.
  local vrc=0
  local r8=()
  [[ "$RUN8_REPORT" == "1" ]] && r8=(--run8)
  python3 -m bench.load606 evaluate "${files[@]}" ${r8[@]+"${r8[@]}"} \
      --expect-digest "$EXPECTED_IMAGE_DIGEST" --expect-driver-streams "$DRIVER_SHARDS" \
      --expect-users "$USERS" > "$OUT/verdict.json" || vrc=$?
  log "verdict exit=${vrc} (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE) -> $OUT/verdict.json"
  # Every INVALID reason in the run log, not only in verdict.json (#817: run
  # 9's zero navigations surfaced only as `warm_navs 0`).
  local r
  while IFS= read -r r; do
    [[ -n "$r" ]] && log "INVALID: $r"
  done < <(python3 -c 'import json, sys; print("\n".join(json.load(open(sys.argv[1])).get("invalid_reasons") or []))' \
             "$OUT/verdict.json" 2>/dev/null || true)
}

log "run budget: ${budget_line}; fits TTL_HOURS=${TTL_HOURS} (${TTL_S} s) minus ${TTL_MARGIN_S} s"
step source    s_source
step reap      s_reap
step up        s_up
step install   s_install
step platform  s_wait_platform
if [[ "$VERIFY483" == "1" ]]; then step verify483 s_verify483; fi
step env       s_env
step preflight s_preflight
step data      s_data
step reboot    s_reboot
step digest    s_digest
step measure   s_measure
step collect   capped "$COLLECT_CAP_S" "collect" s_collect
step evaluate  s_evaluate
log "run complete: $OUT"
