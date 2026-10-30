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
ZONE="${ZONE:-europe-west4-a}"      # every node's zone
# A ZONE, never a region: REGION is derived from it below, and a region here
# would put the nodes in "europe-west4" and the control plane in "europe" (#899
# F2). The same pattern as bench/gke.py ZONE_PATTERN; refused before anything.
if ! [[ "$ZONE" =~ ^[a-z]+-[a-z]+[0-9]+-[a-z]$ ]]; then
  echo "ZONE must be a zone (e.g. europe-west4-a), got '$ZONE': every node goes in it and the control plane in its region" >&2
  exit 2
fi
# The control plane is REGIONAL (3 replicas; run 16 attempt 2 lost B's zonal
# one for 5+ minutes): gcloud and the kube context name the region.
REGION="${ZONE%-*}"
# The cluster's TTL (the self-destruct CronJob and gke-reap read it), in
# hours. Unset (the default): DERIVED from the plan itself, its worst case +
# the margin rounded up to the hour (see the budget below), so the bound
# follows the plan's terms with no number to re-tune. Set by hand: it must fit
# that same plan, or the run is refused before any cloud call, with the
# numbers; a hand-set TTL is never raised.
TTL_HOURS="${TTL_HOURS:-}"
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
DRIVER_PENDING_S="${DRIVER_PENDING_S:-$(python3 -c 'from bench import load606; print(load606.DRIVER_PENDING_S)')}"  # measure: a driver pod still Pending/Init this long never started
LOG_STREAM_RETRY_S="${LOG_STREAM_RETRY_S:-5}"   # snowplow log stream: reconnect pause
MEM_STOP_WAIT_S="${MEM_STOP_WAIT_S:-180}"       # collect: mem606 writes its final stream
MEASURE_STARTED=0
COLLECTED=0
SCALE="${SCALE:-50000}"
USERS="${USERS:-1000}"
NAMESPACES="${NAMESPACES:-50}"
THINK_S="${THINK_S:-60}"
# K6_SPAN=churn (paired runs, #865): DRIVE_S is derived below; set by hand it
# would fight the derivation.
DRIVE_S_GIVEN="${DRIVE_S:+1}"
DRIVE_S="${DRIVE_S:-1800}"
# #830 (run 11): the load = k6 (1,000 users, arrival-rate) + real Chrome
# sessions whose page loads the north-star bars judge. load606 = the Python
# driver of runs 1-10, kept for comparison.
LOAD_DRIVER="${LOAD_DRIVER:-k6}"
case "$LOAD_DRIVER" in k6|load606) ;; *) echo "refusing: LOAD_DRIVER must be k6 or load606" >&2; exit 2 ;; esac
BROWSER_SESSIONS="${BROWSER_SESSIONS:-5}"
# k6 + Chrome need ~8.35 CPU of requests on the load node: only an
# n2-standard-16 fits them (bench/k6_job.loadgen_fit refuses the render otherwise).
if [[ "$LOAD_DRIVER" == "k6" ]]; then LOADGEN_MACHINE_TYPE="${LOADGEN_MACHINE_TYPE:-n2-standard-16}"
else LOADGEN_MACHINE_TYPE="${LOADGEN_MACHINE_TYPE:-n2-standard-8}"; fi
BROWSER_THINK_S="${BROWSER_THINK_S:-15}"
# The browser loads the portal from ITS OWN origin, as krateo-057's users do
# (run 19 ruling: same-origin like 057's gateway): the installer routes the
# portal stack through the platform gateway (s_install: exposure.type=Gateway,
# gatewayMode=path) and the frontend's live config.json names that origin
# (wait_portal_origin). No localhost forwards: they gave the SPA four origins,
# and sse-proxy 401s the preflight of a cross-origin Bearer fetch, so run 17
# attempt 5 opened no /notifications stream.
CHURN_RATES="${CHURN_RATES:-1 10 50}"
FRESH_SAMPLES="${FRESH_SAMPLES:-60}"
FRESH_GAP_S="${FRESH_GAP_S:-5}"
HOLD_PROBE_WAIT_S="${HOLD_PROBE_WAIT_S:-60}"
ENDPOINT_MODE="${ENDPOINT_MODE:-incluster}"
# external = the per-component LoadBalancer IPs; the portal now rides ONE
# gateway origin (s_install: exposure Gateway, path mode), whose Services are
# ClusterIP: no such IP ever exists. Refused here, before any cloud call.
if [[ "$ENDPOINT_MODE" != "incluster" ]]; then
  echo "refusing: ENDPOINT_MODE=${ENDPOINT_MODE}: the portal is exposed through one gateway origin (path mode, as on 057); its components have no LoadBalancer IP of their own. Use ENDPOINT_MODE=incluster" >&2
  exit 2
fi
# The read-only ghcr pull token (bench/ghcr_pull.py): the agentgateway charts
# the portal's gateway needs are PRIVATE (krateo-agentiko/*; run 19 died on
# their 401 at the platform step). The bench installs with registryAuth
# exactly as 057 does, from this operator-written file (mode 0600,
# GHCR_PULL_USERNAME + GHCR_PULL_TOKEN). Without it: refused here, before any
# cloud call; never a run without it.
# exported: bench.diag606 reads the same file (never argv) to redact the pull
# account from every bundle, whether the path is the default or an override
export GHCR_PULL_ENV_FILE="${GHCR_PULL_ENV_FILE:-$HOME/.config/snowplow-bench/ghcr-pull.env}"
python3 -m bench.ghcr_pull check --env-file "$GHCR_PULL_ENV_FILE" || exit 2
ENDPOINT_WAIT_S="${ENDPOINT_WAIT_S:-600}"
ENDPOINT_POLL_S="${ENDPOINT_POLL_S:-15}"
REQUIRED_DEPLOYMENTS="${REQUIRED_DEPLOYMENTS:-snowplow frontend authn}"
PLATFORM_TIMEOUT_S="${PLATFORM_TIMEOUT_S:-2700}"
FRONTEND_CONFIGMAP="${FRONTEND_CONFIGMAP:-frontend-config-vars}"
PREFLIGHT_TIMEOUT_S="${PREFLIGHT_TIMEOUT_S:-300}"
PREFLIGHT_POLL_S="${PREFLIGHT_POLL_S:-3}"
SNOWPLOW_POD_SELECTOR="${SNOWPLOW_POD_SELECTOR:-app.kubernetes.io/name=snowplow}"
# The period of run606's cluster samples (kubectl top, the API server's APF
# metrics; bench/sampler.py) and each sample's bound: a sample still
# unanswered when the next one is due has failed, recorded as such (#919).
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
# K6_SPAN (Diego 2026-10-23, paired runs for #865): "drive" (default) = k6's
# customers run DRIVE_S, as in runs 11-14, which ended inside fresh606's
# first (1/s) step. "churn" = they stay on through EVERY churn step: DRIVE_S
# is the longest fresh606 can run once its start gate opened — THINK_S plus
# every (mode, rate) phase's own bound — so the storm and burst phases stay
# where they were (COMPOSITE_BURST_AT_S keeps its 1800 / 2 default) and the
# churn steps all have customers. custlat.pair checks it (k6's last call
# after the last step ended), never assumes it.
K6_SPAN="${K6_SPAN:-drive}"
case "$K6_SPAN" in
  drive) ;;
  churn)
    [[ "$LOAD_DRIVER" == "k6" ]] || { echo "refusing: K6_SPAN=churn keeps k6's customers on; LOAD_DRIVER=${LOAD_DRIVER}" >&2; exit 2; }
    [[ -z "$DRIVE_S_GIVEN" ]] || { echo "refusing: K6_SPAN=churn derives DRIVE_S; do not set it" >&2; exit 2; }
    DRIVE_S=$(( ${THINK_S%.*} + n_modes * n_rates * (FRESH_PHASE_S + FRESH_LAST_SAMPLE_OVERRUN_S) )) ;;
  *) echo "refusing: K6_SPAN must be drive or churn" >&2; exit 2 ;;
esac
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
# diag606 definitions, once at the platform step's end and once at teardown:
# two lists, the two controllers' logs (each read bounded at 120 s) and their
# images' build info (two registry reads, reused at teardown). Its own cap, so
# neither the platform step nor the teardown is held by it; a cap hit is said
# and recorded (platform-definitions.jsonl, the verdict's platform section).
DEFS_CAP_S="${DEFS_CAP_S:-300}"
DOWN_BUDGET_S="${DOWN_BUDGET_S:-900}"         # gke-down
# evaluate runs after collect while the cluster is still up: run 15 measured
# it at 809 s (A) and 828 s (B) ("=== step evaluate ===" -> "verdict exit=" in
# snowplow-bench-run15{a,b}-983b17d8/run.log); the larger is its budget term
EVALUATE_EST_S="${EVALUATE_EST_S:-828}"
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
# the menu classes k6 drives, each one more preflight (s_classes)
CLASS_COUNT=0
if [[ "$LOAD_DRIVER" == "k6" ]]; then
  CLASS_COUNT="$(python3 -m bench.load606_job --menu-classes --users "$USERS" --namespaces "$NAMESPACES" \
      --browser-sessions "$BROWSER_SESSIONS" --burst-users "$COMPOSITE_BURST_USERS" --snowplow-url - \
      --authn-url - --password - --init - --driver k6 | grep -c .)" \
    || { echo "refusing: the menu classes k6 drives could not be listed (bench.load606_job --menu-classes)" >&2; exit 2; }
fi
# the portal-origin gate's own window (#1005): ENDPOINT_WAIT_S for the platform
# gateway's address (the one LoadBalancer the bench waits for; in-cluster
# endpoints need none), then 2 x the installer cdc's resync + one poll. The
# resync is read from the cluster at the gate; the budget takes the cdc's
# compiled default (bench/origin_gate.py), and the gate logs a read interval
# that exceeds it.
ORIGIN_RESYNC_BUDGET_S="$(python3 -c 'import math; from bench import origin_gate; print(math.ceil(origin_gate.COMPILED_DEFAULT_S))')"
ORIGIN_GATE_S="$(python3 -c 'import math, sys; print(math.ceil(float(sys.argv[1]) + 2 * float(sys.argv[2]) + float(sys.argv[3])))' \
    "$ENDPOINT_WAIT_S" "$ORIGIN_RESYNC_BUDGET_S" "${PLATFORM_POLL_S:-60}")"
RUN_BUDGET_S=$(( UP_BUDGET_S + INSTALL_TIMEOUT_S + PLATFORM_TIMEOUT_S + DEPLOY_WAIT_S
                 + ORIGIN_GATE_S + (1 + CLASS_COUNT) * PREFLIGHT_TIMEOUT_S + DATA_TIMEOUT_S
                 + ROLLOUT_TIMEOUT_S + DIGEST_SETTLE_S + MEASURE_WAIT_S
                 + COLLECT_CAP_S + EVALUATE_EST_S + DIAG_CAP_S + 2 * DEFS_CAP_S + DOWN_BUDGET_S ))
TTL_NEEDED_H=$(( (RUN_BUDGET_S + TTL_MARGIN_S + 3599) / 3600 ))
if [[ -z "$TTL_HOURS" ]]; then
  TTL_HOURS="$TTL_NEEDED_H"
  TTL_HOW="derived: the plan's ${RUN_BUDGET_S} s + margin ${TTL_MARGIN_S} s, rounded up to the hour"
else
  TTL_HOW="set by hand"
fi
TTL_S=$(( ${TTL_HOURS%.*} * 3600 ))
# what the TTL holds past the plan's worst case and the margin: the room a
# portal-origin window longer than its budget term may take (#1006 F8)
TTL_SLACK_S=$(( TTL_S - TTL_MARGIN_S - RUN_BUDGET_S ))
budget_line="up ${UP_BUDGET_S} + install ${INSTALL_TIMEOUT_S} + platform ${PLATFORM_TIMEOUT_S} + deploy ${DEPLOY_WAIT_S} + origin gate ${ORIGIN_GATE_S} (gateway ${ENDPOINT_WAIT_S} + 2x${ORIGIN_RESYNC_BUDGET_S} resync + ${PLATFORM_POLL_S:-60}) + preflight ${PREFLIGHT_TIMEOUT_S} + classes ${CLASS_COUNT}x${PREFLIGHT_TIMEOUT_S} + data ${DATA_TIMEOUT_S} + reboot ${ROLLOUT_TIMEOUT_S} + digest ${DIGEST_SETTLE_S} + measure ${MEASURE_WAIT_S} (fresh ${FRESH_EST_S}, driver ${DRIVER_EST_S}) + collect ${COLLECT_CAP_S} + evaluate ${EVALUATE_EST_S} + diag ${DIAG_CAP_S} + definitions 2x${DEFS_CAP_S} + down ${DOWN_BUDGET_S} = ${RUN_BUDGET_S} s"
if (( RUN_BUDGET_S > TTL_S - TTL_MARGIN_S )); then
  echo "refusing: worst-case run ${RUN_BUDGET_S} s exceeds TTL_HOURS=${TTL_HOURS} (${TTL_S} s) minus margin ${TTL_MARGIN_S} s" >&2
  echo "  budget: ${budget_line}" >&2
  echo "  needs TTL_HOURS >= ${TTL_NEEDED_H} (or leave it unset: it is derived from the plan), or fewer FRESH_SAMPLES / CHURN_RATES / FRESH_MODES" >&2
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
KCTX="gke_${PROJECT}_${REGION}_${RUN_NAME}"
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
# The record of every failing bench command (record_failure): present from
# the start, so an empty file reads "none failed" and a missing one is a
# missing record, never zero (evaluate reads it like every other stream).
touch "$OUT/command-failures.jsonl"
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
# kapply <file>: EVERY manifest this script applies goes through here.
# Server-side apply: client-side apply copies the whole object into the
# kubectl.kubernetes.io/last-applied-configuration annotation, and the API
# caps an object's annotations at 262144 bytes — the load606 ConfigMap ships
# the bench code (~250 KB), so run 15's preflight apply was refused ("The
# ConfigMap \"load606\" is invalid: metadata.annotations: Too long"). Server-
# side apply records field ownership in managedFields instead, no copy.
# (tests/test_apply_size.py: every object this script applies fits the API.)
# Once applied (or refused), the manifest keeps no password on disk: the
# Jobs' command lines, the browser config and #935's user Secrets carry the
# bench users' password, and the run directory outlives the cluster (#939
# review); redact_pw replaces it in place, success or not.
kapply() { local rc=0; run "${K[@]}" apply --server-side --force-conflicts --field-manager=bench606 -f "$1" || rc=$?; redact_pw "$1"; return "$rc"; }
redact_pw() {
  python3 -c 'import sys; p, s = sys.argv[1], sys.argv[2]; t = open(p).read(); open(p, "w").write(t.replace(s, "<redacted>"))' \
      "$1" "$PW"
}
step() {
  local name="$1"; shift
  CUR_STEP="$name"
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
CAPPED_HIT=0         # 1 when the last capped call was stopped (cap or signal)
capped() {
  local secs="$1" label="$2"; shift 2
  CAPPED_HIT=0
  ( set +e; "$@" ) &
  CAPPED_PID=$!
  local waited=0
  while kill -0 "$CAPPED_PID" 2>/dev/null; do
    if (( waited >= secs )) || [[ "$TD_SIGNALED" == "1" ]]; then
      CAPPED_HIT=1
      log "${label}: stopped after ${waited}s ($([[ "$TD_SIGNALED" == "1" ]] && echo 'signal during teardown' || echo "cap ${secs}s reached")); continuing"
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
UP_OK=0   # #985: 1 once gke-up returned 0 (the cluster exists)
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
  declare -F stop_apf_loop >/dev/null && stop_apf_loop
  declare -F stop_sampler >/dev/null && stop_sampler top_boot
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
            --kubeconfig "$KCFG" --context "$KCTX" --out "$OUT/diagnostics" --redact "$PW" \
            --installer-chart "$OUT/installer-pulled"
      else
        log "no kubeconfig yet; no diagnostics to collect"
      fi
    fi
    # every teardown, the run's outcome whatever it is: the definitions and the
    # controller logs once more, before the cluster goes
    if [[ "$TD_SIGNALED" != "1" ]] && { [[ "$DRY" == "1" ]] || [[ -s "$KCFG" ]]; }; then
      capped_definitions teardown
    fi
    # Test hook (like BENCH_606_FAIL_AT): a command that fails inside teardown,
    # as one killed by a group signal would. It must not stop the delete.
    [[ "${BENCH_606_TEARDOWN_FAULT:-}" == "1" ]] && false
    log "=== teardown ${RUN_NAME} (exit ${rc}) ==="
    # shielded: a process-group Ctrl-C must not interrupt the delete itself
    down_rc=0
    ( trap "" INT TERM HUP; exec python3 -m bench.gke gke-down --name "$RUN_NAME" --project "$PROJECT" --zone "$ZONE" --budget-s "$DOWN_BUDGET_S" $YES ) || down_rc=$?
    if [[ "$down_rc" == "5" && "${UP_OK:-0}" != "1" ]]; then
      # #985: gke-up did not complete and the exact-name describe finds no
      # cluster: the create never made one
      log "no cluster ${RUN_NAME} in ${REGION} (gke-down's exact-name describe: NotFound; gke-up did not complete): nothing to delete"
    elif [[ "$down_rc" == "5" ]]; then
      # gke-up made it, and its region no longer has it: not ours to explain away
      log "TEARDOWN FAILED — the cluster gke-up made is not in ${REGION} (exact-name describe: NotFound); check by hand: gcloud container clusters list --project ${PROJECT}"
      rc=$(( rc == 0 ? 1 : rc ))
    elif [[ "$down_rc" == "4" ]]; then
      # #895: the cluster IS deleted, but its PVC disks could not be checked or deleted
      # #954: its disks are found by the cluster's ID label (cluster-id-gke-io), not its name
      # label (a disk provisioned late carries the id alone); gke-down's stderr names each disk
      # left and the exact list command
      log "cluster ${RUN_NAME} deleted, but DISK BACKSTOP FAILED (gke-down's stderr says why and names the disks left, with the gcloud command that lists them by the cluster's id): its disks may be left"
      rc=$(( rc == 0 ? 1 : rc ))
    elif [[ "$down_rc" != "0" ]]; then
      log "TEARDOWN FAILED — delete by hand: gcloud container clusters delete ${RUN_NAME} --project ${PROJECT} --region ${REGION} --quiet"
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
# A PAIR STOP (SIGUSR1, run606_pair.sh when the partner run failed as tooling)
# is not an operator's interrupt: the measurement stops, and the teardown —
# collect, diagnostics, the platform definitions, gke-down and its disk
# backstop — runs IN FULL. Received during the teardown it changes nothing
# (run 17 attempt 4: the pair's stop reached B's teardown as a TERM).
on_pair_stop() {
  if [[ "$IN_TEARDOWN" == "1" ]]; then
    log "pair stop received during teardown: ignored, the teardown finishes in full"
    return 0
  fi
  log "pair stop: the partner run failed as tooling; this run's measurement stops here, its teardown runs in full"
  exit 143
}
trap on_pair_stop USR1

s_source() {
  # Registry preflight, BEFORE any cluster exists (read-only, ghcr only):
  # the chart must resolve at INSTALLER_VERSION and, with VERIFY483=1, its
  # pins must carry the #483 gate. Either failure refuses the run for free.
  # It runs in the dry pass too: s_schema validates against what it pulls.
  local pins=(--no-pins)
  [[ "$VERIFY483" == "1" ]] && pins=()
  rm -rf "$OUT/installer-pulled"
  python3 -m bench.installer_src oci-preflight --chart "$INSTALLER_CHART" \
      --version "$INSTALLER_VERSION" --workdir "$OUT/installer-pulled" \
      --out "$OUT/installer-preflight.json" ${pins[@]+"${pins[@]}"}
  log "installer source: $INSTALLER_CHART --version $INSTALLER_VERSION (registry preflight OK$([[ "$VERIFY483" == "1" ]] && echo ', #483 pins present'))"
}
# s_schema: every manifest the run applies, rendered offline and validated
# against the CRDs of the components INSTALLER_VERSION pins (bench/crd_schema.py),
# BEFORE any cluster exists, in the dry pass too. Run 17 attempt 4: all 670
# fresh-class Users lacked spec.avatarURL, which authn-crds 0.27.3 requires;
# two paid clusters ran ~40 min to learn it. Every line it prints is logged
# (kinds, counts, field paths, chart versions: no identity); what it could not
# validate faithfully it names.
s_schema() {
  local args=(--installer-pulled "$OUT/installer-pulled" --workdir "$OUT/crd-charts"
              --out "$OUT/crd-check.json" --users "$USERS" --namespaces "$NAMESPACES"
              --fresh-modes $FRESH_MODES --user-prefix "$BENCH_USER_PREFIX")
  [[ "$LOAD_DRIVER" == "k6" ]] && args+=(--fresh-classes --sessions "$BROWSER_SESSIONS"
                                         --browser-think-s "$BROWSER_THINK_S" --duration-s "$DRIVE_S")
  local rc=0 ln
  python3 -m bench.crd_schema check "${args[@]}" >/dev/null 2>"$KERR" || rc=$?
  while IFS= read -r ln; do
    if [[ -n "$ln" ]]; then log "$ln"; fi
  done < "$KERR"
  (( rc == 0 )) && return 0
  # the record keeps the refusals (every line when it failed otherwise, e.g. a traceback)
  if grep -F "crd check FAILED" "$KERR" > "$KERR.refusals"; then mv "$KERR.refusals" "$KERR"; else rm -f "$KERR.refusals"; fi
  record_failure "schema FAILED: the manifests of this run do not satisfy the CRDs installer ${INSTALLER_VERSION} pins (exit ${rc}, $OUT/crd-check.json)"
}
s_reap()   { run python3 -m bench.gke gke-reap --project "$PROJECT" $YES; }
s_up() {
  log "control plane: regional (${REGION}); every node in ${ZONE}; context ${KCTX}"
  CLUSTER_CREATED=1   # set BEFORE create: a half-created cluster still bills
  # #985: gke-up's stderr (its refusals and a failed create's reason, redacted
  # by gke-up) goes through record_failure like every other step's; its
  # progress is on stdout. Run 18 lost both arms with only the RUN line logged.
  local up_rc=0
  run bounded "$UP_BUDGET_S" python3 -m bench.gke gke-up --name "$RUN_NAME" --project "$PROJECT" \
      --zone "$ZONE" --ttl-hours "$TTL_HOURS" --loadgen-machine-type "$LOADGEN_MACHINE_TYPE" $YES \
      2>"$KERR" || up_rc=$?
  if [[ -s "$KERR" ]]; then cat "$KERR" >&2; fi
  (( up_rc == 0 )) || { record_failure "up FAILED: gke-up exited ${up_rc}"; return 1; }
  UP_OK=1             # gke-up made the cluster: a teardown that cannot find it is a failure
  run "${K[@]}" version
  # the run's record of the cluster's shape (runs 14-16: zonal control plane;
  # 17+: regional), READ BACK from GKE after create (#899 F1): where the control
  # plane is, where the nodes are and how many — not what the spec asked for.
  # A describe that does not answer records the shape as unknown (never the
  # spec); a dry run records the spec, marked as such.
  local shape_msg
  shape_msg="$(python3 -m bench.gke gke-shape --name "$RUN_NAME" --project "$PROJECT" --zone "$ZONE" \
      --loadgen-machine-type "$LOADGEN_MACHINE_TYPE" $YES 2>&1 >"$OUT/cluster-shape.jsonl")"
  log "$shape_msg"
}
# write_installer_values: the installer's values for this run (the registry
# preflight reads their features; s_install applies them). registryAuth is
# 057's (installer-platform values, read-only 2026-10-30): enabled, global
# credentials (registries: []), passwordRef krateo-agentiko-pull/token in
# krateo-system, TLS verified. Its username comes from the pull-token file
# at install time, piped into helm: no identity is kept in the run's record
# (and diag606 redacts it from the Installer CR the installer copies it into).
# The IMAGE-pull Secret is the bench's own (imagePullSecretName
# bench-ghcr-image-pull), the installer's documented opt-out: 0.3.468
# renders its derived Secret only when coreAgents or specialistAgents is on,
# yet injects its name into agentgateway-policies' private hop pods (run 19
# attempt 2: ImagePullBackOff for 43 min). Fixed in the installer's PR 599;
# once that ships the derived path ("") works too, and going back is a later
# decision.
write_installer_values() {
  cat > "$OUT/installer-values.yaml" <<EOF
bootstrap:
  coreProvider:
    enabled: true
# The portal stack behind ONE origin, as krateo-057's installer-platform
# release has it (helm get values, read-only 2026-10-29): exposure Gateway,
# path mode (/ frontend, /auth authn, /content snowplow, /events sse-proxy,
# each prefix rewritten to /), the two-gateway split, the agentgateway. 057
# also sets tls (its own hostname and certificate): the bench has neither, so
# its origin is http://<platform gateway ip>:<platformPort>. 057's
# exposure.port: 80 is left out: inert in Gateway mode (installer 0.3.468
# values.schema.json: "Ignored when type=Gateway"; compositions.yaml reads it
# only for LoadBalancer/NodePort and outside the path-mode branch).
# Comparability: from run 19 on, the browser reaches the portal through the
# agentgateway (runs <= 17 went direct, through a localhost forwarder), so
# browser figures are not comparable across that line; A/B inside one run is
# unaffected, and k6's users still reach snowplow by its Service DNS.
exposure:
  type: Gateway
  gatewayMode: path
  gatewayRef:
    name: krateo-agent-gateway
  internalDataPlane: true
  platformPort: 80
features:
  agentGateway: true
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
registryAuth:
  enabled: true
  # username: set at install from the pull-token file, piped into helm's -f - (not kept)
  passwordRef:
    name: krateo-agentiko-pull
    namespace: krateo-system
    key: token
  imagePullSecretName: bench-ghcr-image-pull
  insecureSkipVerifyTLS: false
  registries: []
EOF
}
# s_registry: BEFORE any cluster exists, the pull token must resolve every
# repo-override chart the installer turns on, at its pinned version, AND every
# ghcr.io image those charts' own values name (read from the chart itself):
# a bad or under-scoped token costs nothing (bench/ghcr_pull.py preflight;
# $OUT/ghcr-pull-preflight.json names charts, images and versions, never the
# token).
s_registry() {
  write_installer_values
  local rc=0 ln
  python3 -m bench.ghcr_pull preflight --env-file "$GHCR_PULL_ENV_FILE" --installer-pulled "$OUT/installer-pulled" \
      --values "$OUT/installer-values.yaml" --out "$OUT/ghcr-pull-preflight.json" 2>"$KERR" || rc=$?
  while IFS= read -r ln; do
    if [[ -n "$ln" ]]; then log "$ln"; fi
  done < "$KERR"
  (( rc == 0 )) || { record_failure "registry FAILED: the ghcr pull token does not resolve this install's private charts and images"; return 1; }
}
s_install() {
  [[ -s "$OUT/installer-values.yaml" ]] || write_installer_values
  # registryAuth's Secret and the image-pull Secret, both created BEFORE the
  # install renders. The token (and the dockerconfigjson, built in-process)
  # goes through a pipe into kubectl's stdin: never argv, a variable, a log, a
  # file or the run dir.
  if [[ "$DRY" == "1" ]]; then
    log "DRY: kubectl create namespace krateo-system; bench.ghcr_pull token | kubectl -n krateo-system create secret generic krateo-agentiko-pull --from-file=token=/dev/stdin"
    log "DRY: bench.ghcr_pull dockerconfigjson | kubectl -n krateo-system create secret generic bench-ghcr-image-pull --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson=/dev/stdin"
  else
    run "${K[@]}" create namespace krateo-system
    log "RUN: bench.ghcr_pull token | kubectl -n krateo-system create secret generic krateo-agentiko-pull --from-file=token=/dev/stdin"
    python3 -m bench.ghcr_pull token --env-file "$GHCR_PULL_ENV_FILE" \
      | "${K[@]}" -n krateo-system create secret generic krateo-agentiko-pull --from-file=token=/dev/stdin \
          >/dev/null 2>"$KERR" \
      || { record_failure "install FAILED: creating the ghcr pull Secret"; return 1; }
    log "RUN: bench.ghcr_pull dockerconfigjson | kubectl -n krateo-system create secret generic bench-ghcr-image-pull --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson=/dev/stdin"
    python3 -m bench.ghcr_pull dockerconfigjson --env-file "$GHCR_PULL_ENV_FILE" \
      | "${K[@]}" -n krateo-system create secret generic bench-ghcr-image-pull \
          --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson=/dev/stdin \
          >/dev/null 2>"$KERR" \
      || { record_failure "install FAILED: creating the image-pull Secret"; return 1; }
  fi
  # registryAuth.username through a pipe into helm's `-f -` (helm reads that
  # values document from stdin): no file holds it, so no exit path can leave
  # one behind (#1007 review F2)
  if [[ "$DRY" == "1" ]]; then
    run "${H[@]}" install installer "$INSTALLER_CHART" --version "$INSTALLER_VERSION" \
        -n krateo-system --create-namespace -f "$OUT/installer-values.yaml" -f - \
        --wait --timeout "${INSTALL_TIMEOUT_S}s"
    return 0
  fi
  python3 -m bench.ghcr_pull username --env-file "$GHCR_PULL_ENV_FILE" \
    | run "${H[@]}" install installer "$INSTALLER_CHART" --version "$INSTALLER_VERSION" \
        -n krateo-system --create-namespace -f "$OUT/installer-values.yaml" -f - \
        --wait --timeout "${INSTALL_TIMEOUT_S}s"
}
s_verify483() {
  # installer#483 acceptance. Items 1-4 gate (a FAIL aborts the run with
  # diagnostics before teardown); item 5 is recorded only.
  run python3 -m bench.diag606 verify483 --kubeconfig "$KCFG" --context "$KCTX" \
      --installer-chart "$OUT/installer-pulled" --out "$OUT/verify483.json"
}
# platform_definitions <platform_end|teardown>: every RestDefinition and
# CompositionDefinition (its create markers, Ready/Synced, what it manages;
# WEDGED = a pending create past provider-runtime's recovery window) and the
# oasgen-provider / core-provider logs IN FULL, appended to
# platform-definitions.jsonl (evaluate's platform section) and kept under
# platform-logs/<at>/. Evidence: it never fails the run.
platform_definitions() {
  local at="$1"
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.diag606 definitions --at $at --out $OUT/platform-definitions.jsonl --logs-dir $OUT/platform-logs/$at (capped at ${DEFS_CAP_S}s)"
    return 0
  fi
  python3 -m bench.diag606 definitions --kubeconfig "$KCFG" --context "$KCTX" --at "$at" \
      --out "$OUT/platform-definitions.jsonl" --logs-dir "$OUT/platform-logs/$at" --redact "$PW" >&2 \
    || log "platform definitions at ${at}: not recorded (exit $?)"
}
# capped_definitions <at>: platform_definitions under DEFS_CAP_S; a cap hit is
# said loudly and recorded, so an incomplete record never reads as a whole one
capped_definitions() {
  local at="$1"
  capped "$DEFS_CAP_S" "platform definitions at ${at}" platform_definitions "$at"
  if [[ "$CAPPED_HIT" == "1" ]]; then
    log "PLATFORM DEFINITIONS CAP HIT at ${at}: stopped after ${DEFS_CAP_S}s; its record and the controller logs may be incomplete or missing"
    printf '{"kind": "platform_definitions_cap", "at": "%s", "cap_s": %s, "t": %s}\n' \
        "$at" "$DEFS_CAP_S" "$(date +%s)" >> "$OUT/platform-definitions.jsonl"
  fi
}
# wait_portal_origin (#1005): the frontend's live config.json must name ONE
# origin for snowplow, authn and sse-proxy, at 057's paths (load606
# portal-origin). Two hops: the installer's cdc renders the Frontend
# composition's spec.config from a lookup of the platform gateway's address
# on every resync, then the frontend's cdc renders frontend-config-vars. The
# gate's OWN window (bench/origin_gate.py), never what the platform's earlier
# waits left: up to ENDPOINT_WAIT_S for the gateway's address, then 2 x the
# installer cdc's EFFECTIVE resync (read from the cluster by the cdc's own
# semantics) + one poll, from the latest reading with the address. A resync
# that cannot be honoured (an ambiguous cdc, one that cannot start, or a
# window past what the TTL holds) stops it at once. Every poll records three
# read-only readings (the gateway's address, what the installer RENDERED into
# the Frontend composition, the cdc's availability), and a gate that runs out
# names its failure from the last one.
PORTAL_GATEWAY_SVC="krateo-agent-gateway-platform"   # internalDataPlane: <gatewayRef>-platform carries the portal (installer compositions.yaml)
wait_portal_origin() {
  local why="" t0 now t_addr="" end resync_s resync_src refuse v rc_d rc_c rc_g rc_f g="$OUT/.gate"
  if [[ "$DRY" == "1" ]]; then
    log "DRY: read the installer cdc's resync interval; read $FRONTEND_CONFIGMAP config.json until load606 portal-origin passes (gateway $PORTAL_GATEWAY_SVC address, then 2 x resync) -> $OUT/portal-origin.env"
    printf 'export PORTAL_ORIGIN=http://dry-run-gateway PORTAL_SNOWPLOW_URL=http://dry-run-gateway/content\n' \
        > "$OUT/portal-origin.env"
    # shellcheck disable=SC1091
    source "$OUT/portal-origin.env"
    return 0
  fi
  mkdir -p "$g"; rm -f "$g/clock"
  t0="$(python3 -m bench.origin_gate now --state "$g/clock")"
  rc_d=0; rc_c=0
  "${K[@]}" -n krateo-system get deploy -o json > "$g/deploy.json" 2>"$g/err" || rc_d=$?
  "${K[@]}" -n krateo-system get configmap -o json > "$g/cm.json" 2>>"$g/err" || rc_c=$?
  python3 -m bench.origin_gate resync --deployments "$g/deploy.json" --deployments-rc "$rc_d" \
      --configmaps "$g/cm.json" --configmaps-rc "$rc_c" --errors "$g/err" > "$OUT/portal-origin-resync.json"
  rm -f "$g/cm.json"
  refuse="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["refuse"] or "")' "$OUT/portal-origin-resync.json")"
  if [[ -n "$refuse" ]]; then
    record_failure "portal origin FAILED: the installer cdc's resync cannot be honoured: $refuse" || true
    return 1
  fi
  resync_s="$(python3 -c 'import json, math, sys; print(math.ceil(json.load(open(sys.argv[1]))["seconds"]))' "$OUT/portal-origin-resync.json")"
  resync_src="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["source"])' "$OUT/portal-origin-resync.json")"
  log "portal origin gate: the installer cdc resyncs every ${resync_s}s (${resync_src}); window: up to ${ENDPOINT_WAIT_S}s for the gateway's address, then 2 x ${resync_s}s + one ${PLATFORM_POLL_S}s poll"
  # #1006 F8: a resync past the budget's term widens the window into the TTL's
  # slack (the derived TTL cannot know it: it is read after the cluster
  # exists). Within the slack: said; past it: the reaper could take the
  # cluster mid-run, so the run stops here.
  if (( resync_s > ORIGIN_RESYNC_BUDGET_S )); then
    if (( 2 * (resync_s - ORIGIN_RESYNC_BUDGET_S) > TTL_SLACK_S )); then
      record_failure "portal origin FAILED: the installer cdc resyncs every ${resync_s}s: its window exceeds the run budget's ${ORIGIN_RESYNC_BUDGET_S}s term by more than the TTL's ${TTL_SLACK_S}s slack" || true
      return 1
    fi
    log "WARNING: the cdc's resync (${resync_s}s) exceeds the run budget's ${ORIGIN_RESYNC_BUDGET_S}s: the gate's window runs into the TTL's ${TTL_SLACK_S}s slack"
  fi
  : > "$OUT/portal-origin-wait.jsonl"
  while :; do
    "${K[@]}" -n krateo-system get configmap "$FRONTEND_CONFIGMAP" \
        -o 'jsonpath={.data.config\.json}' > "$OUT/frontend-config.json" 2>"$KERR" || true
    if python3 -m bench.load606 portal-origin < "$OUT/frontend-config.json" \
        > "$OUT/portal-origin.env" 2>"$OUT/.portal-origin.err"; then
      # shellcheck disable=SC1091
      source "$OUT/portal-origin.env"
      log "portal origin (config.json): $PORTAL_ORIGIN; snowplow at $PORTAL_SNOWPLOW_URL"
      rm -rf "$g"
      return 0
    fi
    why="$(cut -c1-600 "$OUT/.portal-origin.err")"
    rc_g=0; rc_f=0; rc_d=0
    "${K[@]}" -n krateo-system get svc "$PORTAL_GATEWAY_SVC" -o json > "$g/svc.json" 2>"$g/svc.err" || rc_g=$?
    "${K[@]}" -n krateo-system get frontends.composition.krateo.io -o json > "$g/fe.json" 2>"$g/fe.err" || rc_f=$?
    "${K[@]}" -n krateo-system get deploy -o json > "$g/deploy.json" 2>/dev/null || rc_d=$?
    (( rc_d == 0 )) || : > "$g/deploy.json"
    now="$(python3 -m bench.origin_gate now --state "$g/clock")"
    python3 -m bench.origin_gate reading --t "$now" --gateway-json "$g/svc.json" --gateway-rc "$rc_g" \
        --gateway-err "$g/svc.err" --frontends-json "$g/fe.json" --frontends-rc "$rc_f" --frontends-err "$g/fe.err" \
        --deployments-json "$g/deploy.json" >> "$OUT/portal-origin-wait.jsonl"
    # the window counts from the latest reading with the address: one that disappears restarts it
    if python3 -c 'import json, sys; r = json.loads(open(sys.argv[1]).read().splitlines()[-1]); sys.exit(0 if r["gateway"] == "address" else 1)' "$OUT/portal-origin-wait.jsonl"; then
      [[ -n "$t_addr" ]] || t_addr="$now"
    else
      t_addr=""
    fi
    end="$(python3 -m bench.origin_gate deadline --t0 "$t0" --t-addr "$t_addr" --resync "$resync_s" \
        --poll "$PLATFORM_POLL_S" --lb-wait "$ENDPOINT_WAIT_S")"
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) < float(sys.argv[2]) else 1)' "$now" "$end" || break
    log "portal not same-origin yet: $why"
    sleep "$PLATFORM_POLL_S"
  done
  v="$(python3 -m bench.origin_gate verdict --readings "$OUT/portal-origin-wait.jsonl" \
      --resync "$OUT/portal-origin-resync.json" --why "${why:-config.json unreadable}")"
  rm -rf "$g"
  record_failure "portal origin FAILED: $(python3 -c 'import json, sys; v = json.loads(sys.argv[1]); print(v["kind"] + ": " + v["message"])' "$v")" || true
  return 1
}
s_wait_platform() {
  # Polled, not a blind wait: prints which components are Ready (and why the
  # others are not) every PLATFORM_POLL_S, so a stall shows WHERE.
  # The pulled chart (source step) gives inst.coreFeatures: what the
  # Installer CR declares is read with the chart's own feature rule.
  # Its failure is NAMED (platform-reason.txt): a pod whose image cannot be
  # pulled is reported with its image and the pull Secrets it references at
  # the poll it appears, and is the failure's reason, not "timeout".
  local prc=0
  rm -f "$OUT/platform-reason.txt"
  run python3 -m bench.diag606 wait-installer --kubeconfig "$KCFG" \
      --context "$KCTX" --timeout-s "$PLATFORM_TIMEOUT_S" \
      --interval-s "$PLATFORM_POLL_S" --installer-chart "$OUT/installer-pulled" \
      --reason-out "$OUT/platform-reason.txt" || prc=$?
  # the platform step's end, READY or not: a platform that never got ready is
  # the case the definitions and the controller logs explain
  capped_definitions platform_end
  if (( prc != 0 )); then
    record_failure "platform FAILED: $(cat "$OUT/platform-reason.txt" 2>/dev/null || echo "wait-installer exited $prc with no reason")" || true
    return "$prc"
  fi
  # NEVER wait on the umbrella Installer being Ready: on a credential-free
  # bench it never is (github-provider-kog stays 401, installer#484). Wait on
  # the specific components the bench uses: their Deployments Available and
  # their Services present (checked by the env step).
  run "${K[@]}" -n krateo-system wait --for=condition=Available \
      $(printf 'deploy/%s ' $REQUIRED_DEPLOYMENTS) --timeout="${DEPLOY_WAIT_S}s"
  wait_portal_origin || return 1
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
  # The platform is up: snowplow's log is streamed from here to collect, and
  # the API server's flow control sampled (#919).
  start_log_stream
  apf_preflight || return 1
  start_apf_loop
  flowcontrol_attribution || return 1
}
# flowcontrol_attribution (#922 F3, #924 review M2/M3): which APF flow schema
# serves snowplow's ServiceAccount, the requests it sends as a bench user, and
# the bench's own drivers — ASKED of the API server (one LIST as each, by
# kubectl impersonation; the response's X-Kubernetes-PF-* headers), never
# re-derived. Each then gets a FlowSchema of its own at the priority level it
# landed in (one precedence below the schema that took it: same level, same
# queuing), applied through kapply, and a second probe proves each lands in
# its own schema — so the figures under snowplow's schemas are snowplow's
# alone, and the bench's churn is apart. bench/flowcontrol.py; names only are
# recorded. Without it the run's #922 figures cannot be attributed: the step
# fails, before the data load is paid for.
flowcontrol_attribution() {
  local fc=(python3 -m bench.flowcontrol --users "$USERS" --namespaces "$NAMESPACES"
            --timeout-s "$COLLECT_TIMEOUT_S")
  if [[ "$DRY" == "1" ]]; then
    log "DRY: ${fc[*]} probe -> bench-flowschemas.yaml (kapply) -> verify -> $OUT/flowcontrol-config.jsonl"
    return 0
  fi
  "${fc[@]}" probe --out "$OUT/.flowcontrol-probe.json" -- "${K[@]}" 2>"$KERR" \
    || { record_failure "flow control FAILED: the API server's APF classification could not be read"; return 1; }
  "${fc[@]}" render --probe "$OUT/.flowcontrol-probe.json" > "$OUT/bench-flowschemas.yaml" 2>"$KERR" \
    || { record_failure "flow control FAILED: rendering the bench FlowSchemas"; return 1; }
  kapply "$OUT/bench-flowschemas.yaml"
  if ! "${fc[@]}" verify --probe "$OUT/.flowcontrol-probe.json" -- "${K[@]}" > "$OUT/flowcontrol-config.jsonl" 2>"$KERR"; then
    record_failure "flow control FAILED: an identity does not land in its bench FlowSchema: $(cut -c1-600 "$OUT/flowcontrol-config.jsonl")" || true
    return 1
  fi
  log "flow control: $(cut -c1-600 "$OUT/flowcontrol-config.jsonl")"
}
# run_preflight_job <job> <out.jsonl> <what>: wait (PREFLIGHT_TIMEOUT_S) for
# an applied preflight Job, keep its record, and refuse the run when it
# refused (naming what) or did not finish.
run_preflight_job() {
  local job="$1" out="$2" what="$3"
  [[ "$job" == preflight606 ]] || kapply "${out%.jsonl}-job.yaml"
  local deadline=$(( $(date +%s) + PREFLIGHT_TIMEOUT_S )) ok="" failed=""
  while (( $(date +%s) < deadline )); do
    ok="$("${K[@]}" -n bench-loadgen get job "$job" -o 'jsonpath={.status.succeeded}' 2>/dev/null || true)"
    failed="$("${K[@]}" -n bench-loadgen get job "$job" -o 'jsonpath={.status.failed}' 2>/dev/null || true)"
    [[ -n "$ok" && "$ok" != "0" ]] && break
    [[ -n "$failed" && "$failed" != "0" ]] && break
    sleep "$PREFLIGHT_POLL_S"
  done
  "${K[@]}" -n bench-loadgen logs "job/$job" > "$out" 2>&1 || true
  local summary why
  summary="$(python3 -m bench.load606 preflight-summary < "$out" 2>&1 || true)"
  printf '%s\n' "$summary" >&2
  if [[ -n "$failed" && "$failed" != "0" ]]; then
    # name WHAT was refused (run 17: "(sidebar menu) -> None: the shell
    # rendered no sidebar Menu with routes" was logged as an unserved type / 404)
    why="$(printf '%s\n' "$summary" | sed -n 's/^\[preflight\] REFUSED //p' | awk 'NR<=3' | tr '\n' ';')"
    log "PREFLIGHT REFUSED: ${why:-no refusal line (the preflight Job failed without a result)} (${what}) — see $out"
    return 1
  fi
  if [[ -z "$ok" || "$ok" == "0" ]]; then
    log "PREFLIGHT did not finish within ${PREFLIGHT_TIMEOUT_S}s (${what})"
    return 1
  fi
  log "preflight OK (${what}; product errors, if any, are recorded, not fatal)"
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
  kapply "$OUT/preflight-job.yaml"
  run_preflight_job preflight606 "$OUT/preflight.jsonl" "the run" || return 1
  local r
  while IFS= read -r r; do [[ -n "$r" ]] && COMPOSITE_ROUTES+=("$r"); done \
    < <(python3 -m bench.load606 preflight-composite < "$OUT/preflight.jsonl")
  if (( COMPOSITE_BURST_USERS > 0 )) && (( ${#COMPOSITE_ROUTES[@]} == 0 )); then
    # refused HERE, before data and reboot: the k6 driver cannot render a
    # composite burst without a composite route (bench/k6_job.py k6_config
    # raises at the measure step), and load606's would record only FAILED
    # navigations
    log "PREFLIGHT REFUSED: COMPOSITE_BURST_USERS=${COMPOSITE_BURST_USERS} but the live menu has no composite page (bench.load606 composite_routes found none) — see $OUT/preflight.jsonl"
    return 1
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
  if [[ "$LOAD_DRIVER" == "k6" ]]; then fresh_classes_create || return 1; fi
}
# fresh_classes_create (#935): the fresh identity classes each Chrome session
# switches to through the run (bench/fresh_classes.py: a new user per class,
# never logged in, and a RoleBinding naming it alone), created before the
# reboot — nothing warms a class before its own login. One class per two
# route cycles: cycle_s = 2 x len(routes) x BROWSER_THINK_S; per session
# ceil(DRIVE_S / cycle_s) classes, its initial user's included. Applied with
# kapply, its output discarded: it names the users; an error line goes
# through bench/identity.py (kerr_line).
fresh_classes_create() {
  local pwf="$OUT/.bench-password"
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.fresh_classes plan --sessions $BROWSER_SESSIONS --browser-think-s $BROWSER_THINK_S --duration-s $DRIVE_S -> $OUT/fresh-classes.json; kapply $OUT/fresh-classes.yaml"
    return 0
  fi
  (umask 077; printf '%s' "$PW" > "$pwf")
  local rc=0
  python3 -m bench.fresh_classes plan --preflight-file "$OUT/preflight.jsonl" --sessions "$BROWSER_SESSIONS" \
      --browser-think-s "$BROWSER_THINK_S" --duration-s "$DRIVE_S" --namespaces "$NAMESPACES" \
      --user-prefix "$BENCH_USER_PREFIX" --password-file "$pwf" --out "$OUT/fresh-classes.json" \
      --manifest "$OUT/fresh-classes.yaml" 2>"$KERR" || rc=$?
  rm -f "$pwf"
  (( rc == 0 )) || { record_failure "fresh classes FAILED: the plan could not be made"; return 1; }
  log "$(kerr_line)"
  if [[ ! -s "$OUT/fresh-classes.yaml" ]]; then
    log "no fresh identity class: the browser's run is shorter than one class cycle"
    return 0
  fi
  kapply "$OUT/fresh-classes.yaml" >/dev/null 2>"$KERR" || record_failure "fresh classes FAILED: applying their manifest"
}
# fresh_classes_cleanup (#935): every object fresh_classes_create made, and
# each fresh user's clientconfig Secret authn wrote at its login; bounded,
# best effort (the cluster's deletion is the backstop), output discarded.
fresh_classes_cleanup() {
  [[ -f "$OUT/fresh-classes.json" ]] || return 0
  local args
  while read -r -a args; do
    bounded "$COLLECT_TIMEOUT_S" "${K[@]}" --request-timeout="${COLLECT_TIMEOUT_S}s" "${args[@]}" \
        >/dev/null 2>"$KERR" || log "fresh classes cleanup: $(kerr_line)"
  done < <(python3 -m bench.fresh_classes cleanup --plan "$OUT/fresh-classes.json")
  log "fresh classes cleaned up"
}
# s_classes (run 17 attempt 5): the portal filters its menu by RBAC, so each
# menu class k6 drives (bench/rbac_model.menu_class: a user's groups less
# those whose grants the bench's RBAC confines to its data namespaces) gets
# its own route table: the preflight606 walk, signed in as a DEDICATED
# identity per class (bench/menu_classes: exactly the class's kept groups;
# no k6 user shares its identity class, so the walk teaches snowplow no
# class the run measures; #999 review N2). After the data step (its RBAC
# exists); the identities are deleted once their walks are done, and again
# at collect (the cluster's deletion is the backstop). Each table is one
# --class-preflight LABEL=FILE of the measure step.
CLASS_PREFLIGHTS=()
s_classes() {
  local label user k=0 f pwf="$OUT/.bench-password" rc=0
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.menu_classes plan -> $OUT/menu-classes.json; kapply $OUT/menu-classes.yaml; a preflight606-cN Job per class"
  fi
  (umask 077; printf '%s' "$PW" > "$pwf")
  python3 -m bench.menu_classes plan --users "$USERS" --namespaces "$NAMESPACES" \
      --browser-sessions "$BROWSER_SESSIONS" --burst-users "$COMPOSITE_BURST_USERS" \
      $([[ "$DRY" == "1" ]] && printf -- "--dry-run") --user-prefix "$BENCH_USER_PREFIX" \
      --password-file "$pwf" --out "$OUT/menu-classes.json" --manifest "$OUT/menu-classes.yaml" 2>"$KERR" || rc=$?
  rm -f "$pwf"
  (( rc == 0 )) || { record_failure "menu classes FAILED: the plan could not be made"; return 1; }
  if [[ "$DRY" != "1" ]]; then
    kapply "$OUT/menu-classes.yaml" >/dev/null 2>"$KERR" \
      || { record_failure "menu classes FAILED: applying their identities"; return 1; }
  fi
  while IFS=$'\t' read -r label user; do
    [[ -n "$label" ]] || continue
    k=$(( k + 1 ))
    f="$OUT/class-preflight-${k}.jsonl"
    log "menu class ${label}: preflight as its own identity"
    if [[ "$DRY" == "1" ]]; then
      log "DRY: preflight606-c${k} Job -> $f"
      continue
    fi
    python3 -m bench.load606_job --preflight-only --preflight-job-name "preflight606-c${k}" \
        --preflight-user "$user" --snowplow-url "$SNOWPLOW_URL" --authn-url "$AUTHN_URL" \
        --password "$PW" --init "$PORTAL_INIT" --frontend-ns "$PORTAL_FRONTEND_NS" \
        --builders-ns "$PORTAL_BUILDERS_NS" > "$OUT/class-preflight-${k}-job.yaml"
    run_preflight_job "preflight606-c${k}" "$f" "menu class ${label}" || { menu_classes_cleanup; return 1; }
    CLASS_PREFLIGHTS+=("${label}=${f}")
  done < <(python3 -m bench.menu_classes list --plan "$OUT/menu-classes.json")
  menu_classes_cleanup
  (( k > 0 )) || { log "no menu class: k6 drives no user"; return 1; }
}
# menu_classes_cleanup: the class identities s_classes made, and each one's
# clientconfig Secret authn wrote at its login; bounded, best effort,
# output discarded (it names the users).
menu_classes_cleanup() {
  [[ "$DRY" != "1" && -f "$OUT/menu-classes.json" ]] || return 0
  local args
  while read -r -a args; do
    bounded "$COLLECT_TIMEOUT_S" "${K[@]}" --request-timeout="${COLLECT_TIMEOUT_S}s" "${args[@]}" \
        >/dev/null 2>"$KERR" || log "menu classes cleanup: $(kerr_line)"
  done < <(python3 -m bench.menu_classes cleanup --plan "$OUT/menu-classes.json")
}
s_reboot() {
  # Cold start at 50K: restart once the data exists, time Ready (#594/#637).
  # #922 F3: boot is its own phase — its restart time here (boot.jsonl; the
  # new pod's start and Ready times come from its before-record), and kubectl
  # top through it (kind top_boot, apart from the measure's samples, stopped
  # when the measure starts its own).
  start_sampler top_boot "$OUT/top-boot.jsonl" -n krateo-system top pod -l "$SNOWPLOW_POD_SELECTOR" --no-headers
  [[ "$DRY" == "1" ]] || python3 -c 'import json, time; print(json.dumps({"kind": "boot", "t_restart": time.time()}))' \
      > "$OUT/boot.jsonl"
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
  python3 -m bench.load606_job --users "$USERS" --shards 1 \
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
      --driver "$LOAD_DRIVER" --preflight-file "$OUT/preflight.jsonl" $([[ "$DRY" == "1" ]] && printf -- "--dry-run") \
      --browser-sessions "$BROWSER_SESSIONS" --browser-think-s "$BROWSER_THINK_S" \
      $([[ -f "$OUT/fresh-classes.json" ]] && printf -- "--fresh-classes %s" "$OUT/fresh-classes.json") \
      --loadgen-machine-type "$LOADGEN_MACHINE_TYPE" \
      ${CLASS_PREFLIGHTS[@]+$(printf -- '--class-preflight %q ' "${CLASS_PREFLIGHTS[@]}")} \
      --browser-origin "$PORTAL_ORIGIN" --browser-snowplow-url "$PORTAL_SNOWPLOW_URL" \
      > "$OUT/loadgen-job.yaml"
  # snowplow's restart state BEFORE measure (the verdict compares after/before:
  # any restart or OOM during measure makes the run INVALID, with its reason).
  snowplow_pod_record before
  # Standard memory capture (#710): kubectl top every TOP_POLL_S from here
  # (each sample bounded by it, #919), plus the in-cluster mem606 Job
  # (memstats + heap profiles, metadata only). The boot's own samples end here.
  stop_sampler top_boot
  start_top_loop
  MEASURE_STARTED=1
  kapply "$OUT/loadgen-job.yaml"
  local deadline=$(( $(date +%s) + MEASURE_WAIT_S ))
  # The teardown trap collects what exists before the cluster goes (#763).
  wait_drive_done "$deadline" || return 1
  # The wait's own "timed out waiting for the condition" is the verdict: the
  # watch ran (the API answered) for its whole --timeout, which is what was
  # left to the deadline. Otherwise a failed wait is a verdict only when the
  # API server ANSWERED it with an error, twice (read_verdict); anything else
  # (a connection lost, a TLS / dial timeout, a 5xx) is retried to the same
  # deadline (run 16 attempt 2).
  local rv
  while poll_sent && ! kwait "${K[@]}" -n bench-loadgen wait job/fresh606 --for=condition=Complete \
      --timeout="$(( deadline > $(date +%s) ? deadline - $(date +%s) : 1 ))s"; do
    if grep -q 'timed out waiting for the condition' "$KERR" 2>/dev/null; then
      api_seen ok
      log "measure FAILED: fresh606 not Complete within ${MEASURE_WAIT_S}s"
      return 1
    fi
    rv=0
    read_verdict "${K[@]}" --request-timeout=60s -n bench-loadgen get job/fresh606 -o name || rv=$?
    case "$rv" in
      0|4) api_seen ok
           record_failure "measure FAILED: waiting for fresh606 failed $(verdict_why "$rv")" || true
           return 1 ;;
      2) api_seen ok ;;
      *) api_seen down "wait job/fresh606" ;;
    esac
    if (( $(date +%s) >= deadline )); then
      record_failure "measure FAILED: fresh606 not seen Complete within ${MEASURE_WAIT_S}s (last error)" || true
      return 1
    fi
    sleep "$DRIVE_POLL_S"
  done
  api_seen ok
}
# kwait <kubectl wait ...>: like run, with kubectl's stderr kept in $KERR (the
# failed wait is classified from it) and its first line logged.
kwait() {
  if [[ "$DRY" == "1" ]]; then log "DRY: $*"; return 0; fi
  log "RUN: $*"
  "$@" 2>"$KERR" && return 0
  local rc=$?
  log "  wait failed (rc=$rc): $(kerr_line)"
  return "$rc"
}
# api_answers: does the bench cluster's API server answer at all? (/readyz on
# the same bench kubeconfig and context.)
api_answers() { "${K[@]}" --request-timeout=30s get --raw=/readyz >/dev/null 2>&1; }
KERR="$OUT/.kubectl-last-error"
# kerr_line: kubectl's first error line, for the log and collect's record —
# every identity redacted first by bench/identity.py, the bench's ONE identity
# redaction (bench.gke's error summary uses it too): 'User "…"', 'users "…"',
# whatever follows "authenticated as" (an e-mail, a numeric id,
# system:serviceaccount:…), any e-mail address. A redactor that does not run
# withholds the line: it is never logged unredacted.
kerr_line() {
  # EVERY line the command wrote, not the first: run606's own "RUN: ..." line shares
  # the captured stderr and came first (run 17 attempt 4 logged only that, and lost
  # its 670 identical errors); identical lines counted once
  [[ -s "$KERR" ]] || return 0
  local out
  if out="$(python3 "${HERE:-.}/bench/identity.py" --summary < "$KERR" 2>/dev/null)"; then
    printf '%s\n' "$out"
  else
    echo "<kubectl's error withheld: the identity redactor did not run>"
  fi
}
# record_failure <message>: a failing bench command's evidence, said in the
# log ("<message>: <its stderr>") AND kept for the verdict
# (command-failures.jsonl, evaluate's report.command_failures): the message,
# the step, and the command's stderr summarized and redacted (kerr_line).
# Returns 1, so `cmd || record_failure "x FAILED: ..."` fails the step.
record_failure() {
  local what="$1" parts
  log "${what}: $(kerr_line)"
  parts="$(python3 "${HERE:-.}/bench/identity.py" --summary-json < "$KERR" 2>/dev/null || echo '["<withheld: the identity redactor did not run>"]')"
  python3 -c 'import json, sys, time
print(json.dumps({"kind": "command_failure", "step": sys.argv[1], "what": sys.argv[2], "t": time.time(),
                  "stderr": json.loads(sys.argv[3] or "[]")}))' "${CUR_STEP:-}" "$what" "$parts" \
      >> "$OUT/command-failures.jsonl" \
    || log "could not record this failure in $OUT/command-failures.jsonl: the verdict will not carry it"
  return 1
}
# local_error: kubectl failed BEFORE reaching any API server — its own
# client-side configuration: an unknown flag or command, a kubeconfig that
# cannot be loaded, a context that does not exist, no server configured (the
# localhost:8080 default). Deterministic: a verdict at once, with the reason,
# never waited out as "API server unreachable". "You must be logged in
# (Unauthorized)" is NOT one: the operator can re-authenticate, so it waits.
local_error() {
  grep -qE '^error: (unknown (shorthand )?flag|unknown command|error loading config file|context "[^"]*" does not exist|no context exists|invalid configuration)|The connection to the server localhost:8080 was refused' "$KERR" 2>/dev/null
}
# answered_error: kubectl's error (in $KERR) is the API server's own ANSWER —
# the request reached it and it refused: NotFound, Forbidden, Unauthorized,
# BadRequest, Invalid. Anything else — "Unable to connect", a TLS / dial /
# client timeout, EOF, a 5xx, a 429 — is waited out, never a verdict: the
# read that spans an outage's END times out (up to ~75 s) while the server
# comes back, so /readyz right after it answers (#898 re-review: run 16
# attempt 2 again). The error itself decides, not a probe taken after it.
answered_error() {
  # self-contained (a reviewer's classifier copies this function alone): the
  # server's own refusals, and — a verdict too, said at once by read_verdict —
  # local_error's patterns, the same text (a test pins that the two agree)
  grep -qE 'Error from server \((NotFound|Forbidden|Unauthorized|BadRequest|Invalid)\)|^error: (unknown (shorthand )?flag|unknown command|error loading config file|context "[^"]*" does not exist|no context exists|invalid configuration)|The connection to the server localhost:8080 was refused' "$KERR" 2>/dev/null
}
# read_verdict <re-read cmd...>: a read just failed (its stderr in $KERR).
#   4  a verdict at once: kubectl's own local configuration (local_error);
#   0  a verdict: it was ANSWERED with an error, /readyz answers, and the one
#      re-read (<re-read cmd...>) was answered with an error again;
#   2  the re-read succeeded: the API answers, no verdict — poll again;
#   1  waited out: not an answered error, or /readyz or the re-read did not
#      answer.
read_verdict() {
  local_error && return 4
  answered_error || return 1
  api_answers || return 1
  if "$@" >/dev/null 2>"$KERR"; then return 2; fi
  answered_error && return 0
  return 1
}
# verdict_why <rv>: how a read_verdict verdict (0 or 4) is said in the log
verdict_why() {
  if [[ "$1" == "4" ]]; then echo "before reaching any API server (kubectl's local configuration)"
  else echo "while the API server answers"; fi
}
# err_kind: what kubectl's last error (in $KERR) says of the API server
err_kind() {
  if local_error; then echo "kubectl's local configuration"
  elif answered_error; then echo "the API answered"
  else echo "API did not answer"; fi
}
# poll_sent: stamp a poll's SEND time, just before the polled kubectl call
# (#917 review F1). A poll's outcome is known only when the call returns — an
# unanswered one up to its --request-timeout later — but what it says is about
# the time it was sent from on: api_seen records send times, never the moment
# kubectl gave up. Every call whose outcome api_seen records stamps first.
POLL_SENT=""
poll_sent() { POLL_SENT=$(date +%s); }
# api_seen <ok|down> [what]: the API server's reachability as the measure /
# collect loops see it, from the poll just made (sent at POLL_SENT). Logged on a
# change of state only (start and end of an outage, each with its time). Each
# outage is recorded in api-outages.jsonl with its window bounded by run606's
# own polls, as wide as they can bound it: t0 = the SEND time of the last poll
# the API answered (it answered at some moment after that), first_unanswered =
# the SEND time of the first poll it did not answer, t1 = the moment the first
# poll it answered again RETURNED. The paired verdict judges the whole [t0, t1]
# (an A outage is excluded from both runs; of B's, the part an A outage covers
# is the vantage's, the rest is judged by B's k6 requests).
API_DOWN_SINCE="" API_DOWN_AT="" API_DOWN_WHAT="" API_LAST_OK=""
api_seen() {
  local now sent
  now=$(date +%s)
  sent=${POLL_SENT:-$now}
  if [[ "$1" == "down" ]]; then
    if [[ -z "$API_DOWN_SINCE" ]]; then
      API_DOWN_SINCE=$sent API_DOWN_AT="$(date -u -r "$sent" +%H:%M:%S 2>/dev/null || date -u -d "@$sent" +%H:%M:%S)"
      API_DOWN_WHAT="${2:-}"
      log "API server unreachable since ${API_DOWN_AT} (${API_DOWN_WHAT}): waiting it out, not a verdict"
    fi
  else
    if [[ -n "$API_DOWN_SINCE" ]]; then
      log "API server reachable again at $(date -u +%H:%M:%S), after $(( now - API_DOWN_SINCE )) s (unreachable since ${API_DOWN_AT})"
      api_record "$now" false
    fi
    API_LAST_OK=$sent
  fi
}
# api_record <t1> <open>: one outage into the run's record. t0 = the send time
# of the last poll the API answered before it (else of the first that went
# unanswered), t1 = when the first it answered after returned (or now, still
# open).
api_record() {
  printf '{"kind":"api_unreachable","t0":%s,"t1":%s,"seconds":%s,"first_unanswered":%s,"seen_by":"%s","open":%s,"source":"run606 polls: the last answered poll before to the first answered poll after"}\n' \
    "${API_LAST_OK:-$API_DOWN_SINCE}" "$1" "$(( $1 - ${API_LAST_OK:-$API_DOWN_SINCE} ))" "$API_DOWN_SINCE" \
    "$API_DOWN_WHAT" "$2" >> "$OUT/api-outages.jsonl"
  API_DOWN_SINCE=""
}
# api_reachable_for_collect: before collect reads anything, wait out an API
# outage within half of COLLECT_CAP_S (collect's own cap; the other half is
# for the reads), so results are not declared missing over a blip. An outage
# still open then is recorded as open.
api_reachable_for_collect() {
  local until=$(( $(date +%s) + COLLECT_CAP_S / 2 ))
  while poll_sent && ! bounded "$COLLECT_TIMEOUT_S" "${K[@]}" --request-timeout="${COLLECT_TIMEOUT_S}s" \
        -n bench-loadgen get jobs -o name >/dev/null 2>&1; do
    api_seen down "collect"
    if (( $(date +%s) >= until )); then
      log "collect: the API server still unreachable after $(( COLLECT_CAP_S / 2 )) s; collecting what answers"
      api_record "$(date +%s)" true
      return 0
    fi
    sleep "$DRIVE_POLL_S"
  done
  api_seen ok
}
# driver_jobs: the in-cluster Jobs that drive the load. load606: the Python
# driver; k6 (#830, run 11): the k6 1,000-user load and the real-Chrome
# sessions. Each one's pods turn Ready when their run is over and their
# results under /out are final; they then stay up for collect.
driver_jobs() { if [[ "$LOAD_DRIVER" == "k6" ]]; then echo "k6load k6browser"; else echo "load606"; fi; }
# wait_drive_done <deadline>: every driver Job's pods are Ready (readinessProbe
# on /out/done), so the Jobs are NOT Complete here. A failed driver pod fails
# the step at once, not at the deadline; so does one that never starts (#834
# review: a pod held in Init, e.g. by an init container that keeps failing),
# checked on EVERY driver Job at each poll (the browser pod's would
# otherwise wait behind k6load's whole run).
wait_drive_done() {
  local deadline="$1" st ready failed want j k stuck pods src rv
  if [[ "$DRY" == "1" ]]; then log "DRY: wait until every $(driver_jobs) pod is Ready (run over)"; return 0; fi
  for j in $(driver_jobs); do
    log "waiting for the driver: $j pods Ready = run over, results final"
    while true; do
      # A read the API server did not answer proves nothing: it is waited out
      # (to the measure deadline), never concluded (run 16 attempt 2). Only a
      # read it ANSWERED with an error, twice (read_verdict), fails the step.
      poll_sent
      if st="$("${K[@]}" --request-timeout=60s -n bench-loadgen get "job/$j" \
               -o jsonpath='{.status.ready}/{.status.failed}/{.spec.completions}' 2>"$KERR")"; then
        api_seen ok
      else
        st="" rv=0
        read_verdict "${K[@]}" --request-timeout=60s -n bench-loadgen get "job/$j" -o name || rv=$?
        case "$rv" in
          0|4) api_seen ok
               record_failure "measure FAILED: reading job/$j failed $(verdict_why "$rv")" || true
               return 1 ;;
          2) api_seen ok ;;
          *) api_seen down "get job/$j" ;;
        esac
      fi
      IFS=/ read -r ready failed want <<< "$st"
      want="${want:-1}"
      if [[ -n "$st" && "${ready:-0}" -ge "$want" ]]; then
        driver_exit_codes "$j" "${ready}/${want}"
        break
      fi
      if [[ "${failed:-0}" -ge 1 ]]; then
        log "measure FAILED: a $j driver pod failed before its results were final"
        return 1
      fi
      for k in $(driver_jobs); do
        # only positive evidence fails the step: a pod seen Pending past
        # DRIVER_PENDING_S (its own creationTimestamp) or a restarting init step
        poll_sent
        if ! pods="$("${K[@]}" --request-timeout=60s -n bench-loadgen get pods -l "job-name=$k" -o json 2>"$KERR")"; then
          rv=0
          read_verdict "${K[@]}" --request-timeout=60s -n bench-loadgen get pods -l "job-name=$k" -o name || rv=$?
          case "$rv" in
            0|4) api_seen ok
                 record_failure "measure FAILED: listing the $k driver pods failed $(verdict_why "$rv")" || true
                 return 1 ;;
            2) api_seen ok ;;
            *) api_seen down "get pods job-name=$k" ;;
          esac
          continue
        fi
        api_seen ok
        src=0
        stuck="$(printf '%s' "$pods" | python3 -m bench.load606 driver-stuck --pending-s "$DRIVER_PENDING_S")" || src=$?
        if [[ "$src" == "1" ]]; then
          log "measure FAILED: the $k driver never started: $stuck"
          return 1
        fi
      done
      if (( $(date +%s) >= deadline )); then
        log "measure FAILED: $j not done within ${MEASURE_WAIT_S}s (${ready:-0}/${want} Ready)"
        return 1
      fi
      sleep "$DRIVE_POLL_S"
    done
  done
}
# driver_exit_codes <job> <ready>: Ready only says the run is OVER; each
# pod's /out/done holds its exit code, and the log says which it was. A
# non-zero exit is not a measure failure (fresh606 still runs and its samples
# stand); the verdict makes the run INVALID with the reason. A listing the API
# server did not answer says nothing: "unknown", never "every run exited 0".
driver_exit_codes() {
  local j="$1" p rc bad="" names
  if ! names="$("${K[@]}" --request-timeout=60s -n bench-loadgen get pods -l "job-name=$j" -o name 2>"$KERR")" \
      || [[ -z "$names" ]]; then
    log "driver $j exit codes unknown ($(err_kind): $(kerr_line)); the collected results say how it ended"
    return 0
  fi
  for p in $names; do
    rc="$(bounded 60 "${K[@]}" --request-timeout=60s -n bench-loadgen exec "${p#pod/}" -- cat /out/done 2>/dev/null | tr -d '[:space:]')"
    [[ "$rc" == "0" ]] || bad+=" ${p#pod/}=rc${rc:-?}"
  done
  if [[ -z "$bad" ]]; then
    log "driver done: $2 $j pod(s) Ready, every run exited 0"
  else
    log "driver FAILED:${bad} (run over, results kept; verdict.json says why)"
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
# start_sampler <kind> <file> <kubectl args...> (#919): every TOP_POLL_S, one
# kubectl read of the bench cluster, one record in <file> (bench/sampler.py).
# Each sample is bounded by its period — kubectl's own --request-timeout and
# the sampler's wall clock (it kills the call's process group) — so a sample
# still unanswered when the next one is due is recorded as a FAILED sample
# (rc, timed_out, kubectl's error line redacted by bench/identity.py) and the
# loop carries on. Run 16 B: one hung `kubectl top` with no request timeout
# ended CPU sampling for the last 3.5 h, the stall included. The PID lives in
# a pidfile: stop_sampler also runs inside collect's `capped` subshell.
start_sampler() {
  local kind="$1" file="$2"; shift 2
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.sampler --kind $kind --every-s $TOP_POLL_S --out $file -- ${K[*]} --request-timeout=${TOP_POLL_S}s $*"
    return 0
  fi
  python3 -m bench.sampler --kind "$kind" --every-s "$TOP_POLL_S" --out "$file" -- \
      "${K[@]}" --request-timeout="${TOP_POLL_S}s" "$@" 2>>"$OUT/sampler-$kind.err" &
  echo "$!" > "$OUT/.sampler-$kind.pid"
  log "sampling $kind every ${TOP_POLL_S}s (each sample bounded by it) -> $file"
}
stop_sampler() {
  local f="$OUT/.sampler-$1.pid" pid
  [[ -f "$f" ]] || return 0
  pid="$(cat "$f")"
  rm -f "$f"       # first: no later caller (teardown) kills this PID again
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}
# Standard memory capture (#710): snowplow's pods, kubectl top.
start_top_loop() {
  start_sampler top "$OUT/top.jsonl" -n krateo-system top pod -l "$SNOWPLOW_POD_SELECTOR" --no-headers
}
stop_top_loop() { stop_sampler top; }
# #919: the API server's flow control (APF), for the whole run (env ->
# collect): every apiserver_flowcontrol_* series of its /metrics (request wait,
# rejections, in-queue / executing per priority level and flow schema), and
# process_start_time_seconds, which says which replica of the regional control
# plane answered. evaluate reports it per phase (report only).
start_apf_loop() { start_sampler apf "$OUT/apf.jsonl.gz" get --raw=/metrics; }
stop_apf_loop() { stop_sampler apf; }
# apf_preflight: one bounded scrape before the loop. The paired run judges
# #920 by API-server load, so a /metrics the API server REFUSES (Forbidden,
# Unauthorized: deterministic) fails the step now, before the data load and
# the measure are paid for; any other failure is the loop's to record.
apf_preflight() {
  if [[ "$DRY" == "1" ]]; then log "DRY: ${K[*]} --request-timeout=${TOP_POLL_S}s get --raw=/metrics (APF preflight)"; return 0; fi
  local m="$OUT/.apf-preflight-metrics"
  if bounded "$TOP_POLL_S" "${K[@]}" --request-timeout="${TOP_POLL_S}s" get --raw=/metrics \
        >"$m" 2>"$KERR" && grep -q '^apiserver_flowcontrol_' "$m"; then
    rm -f "$m"
    log "APF preflight: the API server's /metrics has apiserver_flowcontrol_* series"
    return 0
  fi
  rm -f "$m"
  if answered_error; then
    record_failure "APF preflight FAILED: the API server refused /metrics (the run would have no flow-control evidence)" || true
    return 1
  fi
  log "WARNING: APF preflight read no apiserver_flowcontrol_* series ($(err_kind): $(kerr_line)); the sampler records every sample"
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
  pid="$(cat "$LOG_STREAM_PIDFILE")"
  rm -f "$LOG_STREAM_PIDFILE"      # first: no later caller (teardown) kills this PID again
  touch "$OUT/.log-stream-stop"
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
  COLLECT_DEADLINE=$(( $(date +%s) + COLLECT_CAP_S ))     # collect's own cap (capped)
  # an outage still open from measure, or starting now, is waited out first
  api_reachable_for_collect
  # The driver's results are the FILE on its /out (#817), never its container
  # log: the kubelet rotates that, and `kubectl logs` returns only the current
  # file. The log keeps the driver's diagnostics.
  for p in $("${KR[@]}" -n bench-loadgen get pods -l job-name=load606 -o name); do
    "${KR[@]}" -n bench-loadgen cp "${p#pod/}:/out/nav.jsonl" "$OUT/nav-${p#pod/}.jsonl" >/dev/null 2>&1 \
      || log "could not copy the driver's results (/out/nav.jsonl) from ${p#pod/}: the verdict will say the stream is missing or incomplete"
    "${KR[@]}" -n bench-loadgen logs "$p" > "$OUT/driver-${p#pod/}.log" \
      || log "could not read the driver's diagnostics log from ${p#pod/}"
  done
  # #830: the k6 pod's and the browser pod's whole /out (metric stream,
  # summary, exit record) — never their container logs.
  local j dir
  for j in k6load k6browser; do
    dir="$OUT/$([[ "$j" == "k6load" ]] && echo k6 || echo browser)"
    for p in $("${KR[@]}" -n bench-loadgen get pods -l "job-name=$j" -o name); do
      mkdir -p "$dir"
      "${KR[@]}" -n bench-loadgen cp "${p#pod/}:/out" "$dir" >/dev/null 2>&1 \
        || log "could not copy $j's results (/out) from ${p#pod/}: the verdict will say the stream is missing or incomplete"
      "${KR[@]}" -n bench-loadgen logs "$p" --all-containers > "$OUT/driver-${p#pod/}.log" \
        || log "could not read $j's diagnostics log from ${p#pod/}"
    done
  done
  for p in $("${KR[@]}" -n bench-loadgen get pods -l job-name=fresh606 -o name); do
    "${KR[@]}" -n bench-loadgen logs "$p" > "$OUT/fresh-${p#pod/}.jsonl" \
      || log "could not read the fresh606 log from ${p#pod/}"
  done
  snowplow_pod_record after
  declare -F stop_top_loop >/dev/null && stop_top_loop
  declare -F stop_apf_loop >/dev/null && stop_apf_loop
  # How every in-cluster Job pod ended, for the verdict (#817 review): a pod
  # that died (OOMKilled, evicted, a crash), never ran (Pending / Unknown) or
  # has no pod at all makes the run INVALID with its reason in verdict.json.
  # Before mem606's bounded stop, so nothing slow can come between.
  : > "$OUT/job-pods.jsonl"
  local jp rv
  for p in $(driver_jobs) fresh606 mem606; do
    # kubectl's own exit decides: an unanswered read is retried within
    # collect's cap; one still unanswered is recorded "unreadable", never as
    # a pod that is missing ("never started": run 16's bug, at collect)
    # a verdict only when the API ANSWERED with an error, twice (read_verdict)
    while poll_sent && ! jp="$("${KR[@]}" -n bench-loadgen get pods -l "job-name=$p" -o json 2>"$KERR")"; do
      rv=0
      read_verdict "${KR[@]}" -n bench-loadgen get pods -l "job-name=$p" -o name || rv=$?
      case "$rv" in
        1) api_seen down "collect: $p pods" ;;
        0|2) api_seen ok ;;
      esac
      if [[ "$rv" == "0" || "$rv" == "4" ]] || (( $(date +%s) + COLLECT_TIMEOUT_S >= COLLECT_DEADLINE )); then
        log "could not read how the $p pod(s) ended: $(kerr_line)"
        python3 -m bench.load606 job-pod-record --job "$p" \
            --unreadable "$(case "$rv" in
                              1) echo "API did not answer" ;;
                              4) echo "kubectl's local configuration: $(kerr_line)" ;;
                              *) echo "the API answered: $(kerr_line)" ;;
                            esac)" \
            >> "$OUT/job-pods.jsonl"
        continue 2
      fi
      sleep "$DRIVE_POLL_S"
    done
    api_seen ok
    printf '%s' "$jp" | python3 -m bench.load606 job-pod-record --job "$p" >> "$OUT/job-pods.jsonl" \
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
  # #935: the fresh identity classes, once the browser is done with them;
  # the menu classes' identities again (s_classes deleted them already)
  fresh_classes_cleanup
  menu_classes_cleanup
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
               "$OUT"/top-boot.jsonl "$OUT"/boot.jsonl "$OUT"/flowcontrol-config.jsonl
               "$OUT"/image-digest.jsonl "$OUT"/job-pods.jsonl "$OUT"/cluster-shape*.jsonl
               "$OUT"/api-outages*.jsonl "$OUT"/platform-definitions.jsonl "$OUT"/command-failures.jsonl)
  # #919: the API server's flow-control scrapes, read by evaluate as a stream
  # (a file that is not there is reported missing)
  local r8=() k6args=() apfargs=(--apf "$OUT/apf.jsonl.gz")
  [[ "$RUN8_REPORT" == "1" ]] && r8=(--run8)
  # #830: the page loads are the real browsers'; k6 is the load
  [[ "$LOAD_DRIVER" == "k6" ]] && k6args=(--k6-out "$OUT/k6" --browser-out "$OUT/browser"
                                          --browser-sessions "$BROWSER_SESSIONS")
  [[ "$LOAD_DRIVER" == "k6" && "$VIEWERS" != "1" ]] && k6args+=(--no-viewers)
  if [[ "$DRY" == "1" ]]; then
    log "DRY: python3 -m bench.load606 evaluate ${files[*]:-<none>} ${r8[*]:-} --profile-dir $OUT/heap ${k6args[*]:-} ${apfargs[*]}"; return 0
  fi
  if [[ ${#files[@]} -eq 0 ]]; then log "no result files to evaluate"; return 1; fi
  # The verdict is a RESULT, not a tooling failure: record it, do not abort.
  local vrc=0
  # #871: --profile-dir attributes mem606's CPU profiles by the snowplow_work label
  python3 -m bench.load606 evaluate "${files[@]}" ${r8[@]+"${r8[@]}"} \
      --expect-digest "$EXPECTED_IMAGE_DIGEST" --profile-dir "$OUT/heap" \
      ${k6args[@]+"${k6args[@]}"} "${apfargs[@]}" > "$OUT/verdict.json" || vrc=$?
  log "verdict exit=${vrc} (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE) -> $OUT/verdict.json"
  # Every INVALID reason in the run log, not only in verdict.json (#817: run
  # 9's zero navigations surfaced only as `warm_navs 0`).
  local r
  while IFS= read -r r; do
    [[ -n "$r" ]] && log "INVALID: $r"
  done < <(python3 -c 'import json, sys; print("\n".join(json.load(open(sys.argv[1])).get("invalid_reasons") or []))' \
             "$OUT/verdict.json" 2>/dev/null || true)
  # report-only warnings (e.g. the #765 B shadow absent: missing, not zero)
  while IFS= read -r r; do
    [[ -n "$r" ]] && log "WARNING: $r"
  done < <(python3 -c 'import json, sys; print("\n".join((json.load(open(sys.argv[1])).get("report") or {}).get("report_warnings") or []))' \
             "$OUT/verdict.json" 2>/dev/null || true)
  # A verdict is a result, not a tooling failure: the step succeeds whatever
  # it is. Without this the loop's last `[[ -n "" ]]` (no warnings) was the
  # step's status: under set -e runs 12 and 14 exited 1 here, re-collected and
  # re-evaluated in teardown, and never logged "run complete".
  return 0
}

log "run budget: ${budget_line}; fits TTL_HOURS=${TTL_HOURS} (${TTL_S} s; ${TTL_HOW}) minus ${TTL_MARGIN_S} s"
log "k6 customers: K6_SPAN=${K6_SPAN}, DRIVE_S=${DRIVE_S} s, composite burst at ${COMPOSITE_BURST_AT_S} s"
step source    s_source
step schema    s_schema
step registry  s_registry
step reap      s_reap
step up        s_up
step install   s_install
step platform  s_wait_platform
if [[ "$VERIFY483" == "1" ]]; then step verify483 s_verify483; fi
step env       s_env
step preflight s_preflight
step data      s_data
if [[ "$LOAD_DRIVER" == "k6" ]]; then step classes s_classes; fi
step reboot    s_reboot
step digest    s_digest
step measure   s_measure
step collect   capped "$COLLECT_CAP_S" "collect" s_collect
step evaluate  s_evaluate
log "run complete: $OUT"
