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
SCALE="${SCALE:-50000}"
USERS="${USERS:-1000}"
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
if [[ "$RUN8_REPORT" == "1" ]]; then
  # shellcheck disable=SC2086
  python3 -m bench.load606 check-prefixes $MEM_VARS_PREFIXES \
    || { echo "refusing: MEM_VARS_PREFIXES does not capture every run-8 key" >&2; exit 2; }
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

CLUSTER_CREATED=0
teardown() {
  local rc=$?
  trap - EXIT
  declare -F stop_top_loop >/dev/null && stop_top_loop
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
      $(printf 'deploy/%s ' $REQUIRED_DEPLOYMENTS) --timeout=30m
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
  run python3 -m bench data606 --scale "$SCALE" --users "$USERS" \
      --namespaces "$NAMESPACES" --hold-probe-wait-s "$HOLD_PROBE_WAIT_S" \
      --out "$OUT/data-proof.json" ${login[@]+"${login[@]}"}
}
s_reboot() {
  # Cold start at 50K: restart once the data exists, time Ready (#594/#637).
  run "${K[@]}" -n krateo-system rollout restart deploy/snowplow
  run "${K[@]}" -n krateo-system rollout status deploy/snowplow --timeout=45m
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
  local rc=0
  "${K[@]}" -n krateo-system get pods -l "$SNOWPLOW_POD_SELECTOR" -o json \
    | python3 -m bench.load606 digest-gate --expected "$EXPECTED_IMAGE_DIGEST" \
    > "$OUT/image-digest.jsonl" || rc=$?
  log "image digest gate: $(cut -c1-600 "$OUT/image-digest.jsonl" 2>/dev/null)"
  if (( rc != 0 )); then
    log "VOID: a snowplow pod does not run ${EXPECTED_IMAGE_DIGEST} (gate exit ${rc}); no load is applied"
    return 1
  fi
  log "image digest MATCH: every live snowplow pod runs ${EXPECTED_IMAGE_DIGEST}"
}
s_measure() {
  # Load: in-cluster on the tainted loadgen pool, one Indexed-Job pod per
  # shard. Each pod streams its JSONL to stdout (per nav, flushed) and the
  # collect step reads it back with `kubectl logs`. The page roots are the
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
      --snowplow-url "$SNOWPLOW_URL" --authn-url "$AUTHN_URL" --password "$PW" \
      --init "$PORTAL_INIT" --frontend-ns "$PORTAL_FRONTEND_NS" \
      --builders-ns "$PORTAL_BUILDERS_NS" \
      --fresh-rates $CHURN_RATES --fresh-samples "$FRESH_SAMPLES" \
      --fresh-gap-s "$FRESH_GAP_S" --fresh-delay-s "$THINK_S" \
      --namespaces "$NAMESPACES" \
      --mem-duration-s "$(( DRIVE_S + 3600 ))" \
      $(for x in $MEM_VARS_PREFIXES; do printf -- '--mem-vars-prefix %s ' "$x"; done) \
      --burst-users "$COMPOSITE_BURST_USERS" --burst-at-s "$COMPOSITE_BURST_AT_S" \
      ${COMPOSITE_ROUTES[@]+$(printf -- '--composite-route %q ' "${COMPOSITE_ROUTES[@]}")} \
      > "$OUT/loadgen-job.yaml"
  # snowplow's restart state BEFORE measure (the verdict compares after/before:
  # any restart or OOM during measure makes the run INVALID, with its reason).
  snowplow_pod_record before
  # Standard memory capture (#710): kubectl top every TOP_POLL_S from here,
  # plus the in-cluster mem606 Job (memstats + heap profiles, metadata only).
  start_top_loop
  run "${K[@]}" apply -f "$OUT/loadgen-job.yaml"
  run "${K[@]}" -n bench-loadgen wait job/load606 job/fresh606 --for=condition=Complete \
      --timeout="$(( DRIVE_S + 3600 ))s"
}
snowplow_pod_record() {
  local phase="$1"
  if [[ "$DRY" == "1" ]]; then log "DRY: snowplow pod record ($phase)"; return 0; fi
  "${K[@]}" -n krateo-system get pods -l "$SNOWPLOW_POD_SELECTOR" -o json \
    | python3 -m bench.load606 pod-record --phase "$phase" > "$OUT/pod-$phase.jsonl" \
    || log "could not record the snowplow pod ($phase); the verdict will say restart state unknown"
  log "snowplow pod ($phase): $(cat "$OUT/pod-$phase.jsonl" 2>/dev/null | cut -c1-240)"
}
TOP_PID=""
start_top_loop() {
  if [[ "$DRY" == "1" ]]; then log "DRY: kubectl top loop -> $OUT/top.log"; return 0; fi
  ( while true; do
      line="$("${K[@]}" -n krateo-system top pod -l "$SNOWPLOW_POD_SELECTOR" --no-headers 2>&1 | tr -s ' ' | tr '\n' ' ' | tr -d '"\\')"
      printf '{"kind":"top","t":%s,"raw":"%s"}\n' "$(date +%s)" "$line"
      sleep "$TOP_POLL_S"
    done ) >> "$OUT/top.jsonl" 2>&1 &
  TOP_PID=$!
}
stop_top_loop() {
  [[ -n "${TOP_PID:-}" ]] && kill "$TOP_PID" 2>/dev/null || true
  TOP_PID=""
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
  for p in $("${K[@]}" -n bench-loadgen get pods -l job-name=fresh606 -o name); do
    "${K[@]}" -n bench-loadgen logs "$p" > "$OUT/fresh-${p#pod/}.jsonl"
  done
  snowplow_pod_record after
  declare -F stop_top_loop >/dev/null && stop_top_loop
  # mem606: its memstats trajectory (logs) and heap profiles (files in /out).
  for p in $("${K[@]}" -n bench-loadgen get pods -l job-name=mem606 -o name); do
    "${K[@]}" -n bench-loadgen logs "$p" > "$OUT/mem-${p#pod/}.jsonl" || true
    mkdir -p "$OUT/heap"
    "${K[@]}" -n bench-loadgen cp "${p#pod/}:/out" "$OUT/heap" >/dev/null 2>&1 \
      || log "could not copy heap profiles from ${p#pod/}"
  done
  "${K[@]}" -n krateo-system logs deploy/snowplow --tail=-1 > "$OUT/snowplow.log" || true
  "${K[@]}" top nodes > "$OUT/top-nodes.txt" || true
}
s_evaluate() {
  shopt -s nullglob
  local files=("$OUT"/nav-*.jsonl "$OUT"/fresh-*.jsonl "$OUT"/preflight.jsonl
               "$OUT"/pod-*.jsonl "$OUT"/mem-*.jsonl "$OUT"/top.jsonl
               "$OUT"/image-digest.jsonl)
  if [[ "$DRY" == "1" ]]; then log "DRY: python3 -m bench.load606 evaluate ${files[*]:-<none>}"; return 0; fi
  if [[ ${#files[@]} -eq 0 ]]; then log "no result files to evaluate"; return 1; fi
  # The verdict is a RESULT, not a tooling failure: record it, do not abort.
  local vrc=0
  local r8=()
  [[ "$RUN8_REPORT" == "1" ]] && r8=(--run8)
  python3 -m bench.load606 evaluate "${files[@]}" ${r8[@]+"${r8[@]}"} \
      --expect-digest "$EXPECTED_IMAGE_DIGEST" > "$OUT/verdict.json" || vrc=$?
  log "verdict exit=${vrc} (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE) -> $OUT/verdict.json"
}

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
step collect   s_collect
step evaluate  s_evaluate
log "run complete: $OUT"
