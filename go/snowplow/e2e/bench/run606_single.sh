#!/usr/bin/env bash
# ONE measuring arm (Diego 2026-11-17, "Add a single-arm VM mode first"): one run606.sh run on its own
# TEMPORARY cluster, with everything run606_pair.sh does for each of its arms, and nothing it does for a
# comparison. It is what bench/vm606.py launches on its GCE VM for a one-cluster rehearsal, so that run is
# off the laptop: one arm, measured in full (load, verdict), its evidence copied off the VM.
#
# Started like the pair launcher, DETACHED (python3 -m bench.detach --log <LOG> --pidfile <PIDFILE> -- bash
# run606_single.sh); a BENCH_606_DRY_RUN=1 pass needs no detaching, and stops after its checks.
#
# RUNS: exactly ONE label ([A-Z][A-Z0-9]*). Its variables are the pair launcher's per-run ones, for that label:
#   RUN_NAME_L, SNOWPLOW_TAG_L, EXPECTED_IMAGE_DIGEST_L, BENCH_606_GO_L (Diego's go, <YYYYMMDD>-<RUN_NAME>),
#   optional OUT_L, FRONTEND_TAG_L + EXPECTED_FRONTEND_DIGEST_L (both or neither), SNOWPLOW_ENV_L,
#   BENCH_1004_GO_L (#1004's full-widen stage).
# The arm runs a pair arm's schedule (K6_SPAN=churn, LOAD_DRIVER=k6: run606_arm.sh's runenv), so what it
# measures reads as one arm of a launch would. Refused: two or more RUNS (that is run606_pair.sh), PAIRS or
# REPLICA_PAIR set (a single arm compares nothing: a launch that names a pair is not guessed into one arm),
# the run606.sh names themselves (RUN_NAME, ...: per run), DRIVE_S, a K6_SPAN other than churn, a LOAD_DRIVER
# other than k6, and BENCH_606_REHEARSAL (run606.sh's no-load test hook: this launch measures).
#
# The arm's steps, as in a pair (run606_arm.sh holds the ones both launchers share):
#   - its dry pass (BENCH_606_DRY_RUN=1, OUT=$SINGLE_OUT/dry-L: the layout a pair's dry pass leaves, so
#     vm606 reads the arm's TTL and planned k6 seconds from dry-L/run.log as it does a pair's), its derived
#     cluster footprint read against the region's quotas (bench.gke gke-quotas), the host-load gate; nothing
#     is created before all of them passed. BENCH_606_DRY_RUN=1 stops here.
#   - the launch's record, single.json (bench commit, the arm's name / tag / digest / OUT), published
#     (bench.evidence) and copied to the evidence bucket (gs://<bucket>/runs/<launch id>/plan/,
#     bench.durable) before the run starts: a launch whose record cannot be copied spends nothing.
#   - the run (run606.sh, which publishes its own evidence before its cluster goes); a TERM / INT / HUP to
#     this launcher is forwarded as TERM to it (run606.sh's trap tears its cluster down).
#   - #1390: once it exits, its run directory copied to the bucket and verified (durable_copy), then read back
#     from it, a fresh download verified against its manifest (durable_fetch, as each arm of a pair before its
#     statistics): the evidence has left the VM, and the copy is known readable.
#   - all of $SINGLE_OUT published at its final stage, and the run's own evidence verified (bench.evidence).
# Exit: the arm's verdict (verdict.json: 0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE; the pair launcher's
# codes for one verdict); the run's own non-zero exit when it has no verdict; 2 = refused before anything was
# created; 5 = the evidence is not safe (not copied, not read back, not published or not verified), whatever
# the verdict: the launch is NOT finished.
# Out: $SINGLE_OUT (default: bench.evidence run-dir single-<RUN_NAME_L>): single.json, single.log,
# dry-L.console, dry-L/, L.console, host-load.jsonl; the arm's own OUT as run606.sh leaves it.
set -Eeuo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
DRY="${BENCH_606_DRY_RUN:-0}"
RUN606="$HERE/run606.sh"

refuse() { echo "refusing: $*" >&2; exit 2; }
source "$HERE/run606_arm.sh"
read -r -a LABELS <<< "${RUNS:-}"
(( ${#LABELS[@]} == 1 )) \
  || refuse "RUNS names ${#LABELS[@]} run(s): a single-arm launch runs exactly one (two or more, compared: run606_pair.sh)"
L="${LABELS[0]}"
[[ "$L" =~ ^[A-Z][A-Z0-9]*$ ]] || refuse "RUNS: '$L' is not a label ([A-Z][A-Z0-9]*: it names the run's variables, ${L}_...)"
[[ -z "${PAIRS:-}" ]] \
  || refuse "PAIRS is set (${PAIRS}): a single arm compares nothing, and a launch that names a pair is not run as one arm: unset it, or launch the pair (run606_pair.sh)"
[[ -z "${REPLICA_PAIR:-}" ]] || refuse "REPLICA_PAIR is set (${REPLICA_PAIR}): a single arm has no pair: unset it"
# a launch measures: the one-cluster rehearsal mode of run606.sh (BENCH_606_REHEARSAL: no load, no verdict) is a
# test hook, never a launch's
[[ -z "${BENCH_606_REHEARSAL:-}" ]] || refuse "BENCH_606_REHEARSAL is set (${BENCH_606_REHEARSAL}): a launch never carries it (this one measures its arm in full): unset it"
for v in RUN_NAME SNOWPLOW_TAG EXPECTED_IMAGE_DIGEST; do
  w="${v}_$L"; [[ -n "${!w:-}" ]] || refuse "$w is required (the run's)"
done
for v in RUN_NAME SNOWPLOW_TAG EXPECTED_IMAGE_DIGEST BENCH_606_GO BENCH_1004_GO OUT FRONTEND_TAG EXPECTED_FRONTEND_DIGEST SNOWPLOW_ENV; do
  [[ -z "${!v:-}" ]] || refuse "$v is per run (set ${v}_$L): unset it"
done
ft="FRONTEND_TAG_$L" fd="EXPECTED_FRONTEND_DIGEST_$L"
if [[ -n "${!ft:-}" && -z "${!fd:-}" ]] || [[ -z "${!ft:-}" && -n "${!fd:-}" ]]; then
  refuse "$ft and $fd are set together or not at all (run $L)"
fi
# the run's declared snowplow env (bench/snowplow_env.py), checked as a pair checks each of its arms'
se="SNOWPLOW_ENV_$L"
env_msg="$(python3 -m bench.snowplow_env same-keys "${L}=${!se:-}" 2>&1)" || refuse "${env_msg#refusing: }"
[[ -z "${DRIVE_S:-}" ]] || refuse "DRIVE_S is derived by K6_SPAN=churn: unset it"
[[ "${K6_SPAN:-churn}" == "churn" ]] || refuse "the arm runs a pair arm's schedule: K6_SPAN=churn (customers on through every churn step)"
[[ "${LOAD_DRIVER:-k6}" == "k6" ]] || refuse "the arm runs a pair arm's schedule: LOAD_DRIVER=k6"
nm="RUN_NAME_$L" go="BENCH_606_GO_$L" fwg="BENCH_1004_GO_$L"
if [[ "$DRY" != "1" ]]; then
  today="$(date -u +%Y%m%d)"
  [[ "${!go:-}" == "${today}-${!nm}" ]] || refuse "$go must be '${today}-${!nm}' (Diego's go for run $L)"
  if [[ -n "${!fwg:-}" ]]; then
    [[ "${!fwg}" == "${today}-${!nm}" ]] || refuse "$fwg must be '${today}-${!nm}' (Diego's go for the full-widen stage on run $L)"
  fi
fi
# one commit, nothing uncommitted under the bench: the run executes it
BENCH_SHA="$(git -C "$HERE" rev-parse HEAD)" || refuse "the bench is not a git checkout"
dirty="$(git -C "$HERE" status --porcelain -- . | head -5)"
if [[ -n "$dirty" ]]; then
  [[ "$DRY" == "1" ]] || refuse "uncommitted changes under the bench: the run must be one commit (${dirty//$'\n'/, })"
fi
SINGLE_NAME="single-${!nm}"
SINGLE_OUT="${SINGLE_OUT:-$(python3 -m bench.evidence run-dir "$SINGLE_NAME")}"
out="OUT_$L"
[[ -n "${!out:-}" ]] || printf -v "$out" '%s' "$(python3 -m bench.evidence run-dir "${!nm}")"   # run606.sh's own default OUT
SINGLE_EVIDENCE="$(python3 -m bench.evidence dest "$SINGLE_NAME")"
LAUNCH_ID="${SINGLE_NAME}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
# #1390: the evidence bucket the run's directory is copied to (bench/durable.py; vm606 up ensures it)
EVIDENCE_BUCKET="$(python3 -m bench.durable bucket ${PROJECT:+"$PROJECT"})"
if [[ "$DRY" != "1" ]]; then
  for f in verdict.json run.log; do
    [[ ! -e "${!out}/$f" ]] || refuse "OUT_$L (${!out}) already holds a run ($f): use a new RUN_NAME_$L or OUT_$L"
  done
  [[ ! -e "$SINGLE_OUT/single.json" ]] \
    || refuse "SINGLE_OUT ($SINGLE_OUT) already holds a launch (single.json): it is never written over; use a new run name or SINGLE_OUT"
fi
python3 -m bench.evidence preflight --dest "$SINGLE_EVIDENCE" || exit 2
mkdir -p "$SINGLE_OUT"
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$SINGLE_OUT/single.log" >&2; }

# publish_single <stage> [--require F]...: all of SINGLE_OUT, copied and verified (bench/evidence.py)
publish_single() {
  local stage="$1"; shift
  python3 -m bench.evidence publish --src "$SINGLE_OUT" --dest "$SINGLE_EVIDENCE" --run-id "$LAUNCH_ID" \
      --stage "$stage" --require single.json "$@"
}

tg="SNOWPLOW_TAG_$L"
log "run: ${L}=${!nm} (${!tg}${!se:+, snowplow env ${!se}}${!ft:+, frontend ${!ft} ${!fd}}); one arm, no pair; bench ${BENCH_SHA}, ${BASH} ${BASH_VERSION}"
runenv "$L"
rm -f "$SINGLE_OUT/dry-$L/footprint.json"   # #1171: only THIS dry pass's derivation counts
if ! env "${RUNENV[@]}" BENCH_606_DRY_RUN=1 OUT="$SINGLE_OUT/dry-$L" "$BASH" "$RUN606" \
     > "$SINGLE_OUT/dry-$L.console" 2>&1; then
  tail -5 "$SINGLE_OUT/dry-$L.console" >&2
  refuse "run $L's dry pass was refused (${SINGLE_OUT}/dry-$L.console): nothing was created"
fi
log "run $L: dry pass OK"
# the cluster's footprint, derived by its dry pass, against every regional quota it consumes (#1171), as a pair
# reads its arms' together: reads only, so the dry pass runs it too
quota_args=()
[[ -n "${PROJECT:-}" ]] && quota_args+=(--project "$PROJECT")
[[ -n "${ZONE:-}" ]] && quota_args+=(--zone "$ZONE")
[[ -s "$SINGLE_OUT/dry-$L/footprint.json" ]] \
  || refuse "run $L's dry pass did not derive its LoadBalancer Services and cluster footprint ($SINGLE_OUT/dry-$L/footprint.json): whether the run fits the region's quotas is unknown; nothing was created"
quota_args+=(--footprint "$SINGLE_OUT/dry-$L/footprint.json")
if ! quota_msg="$(python3 -m bench.gke gke-quotas "${quota_args[@]}" 2>&1)"; then
  refuse "${quota_msg//$'\n'/$'\n'refusing: }"
fi
while IFS= read -r ln; do
  if [[ -n "$ln" ]]; then log "$ln"; fi
done <<< "$quota_msg"
# #1297: the orchestrator's load (run606.sh checks it again, and records it every 15 s)
if ! hl_msg="$(python3 -m bench.hostload check --out "$SINGLE_OUT/host-load.jsonl" $([[ "$DRY" == "1" ]] && printf -- --dry) 2>&1)"; then
  refuse "${hl_msg#refusing: }: nothing was created"
fi
log "host load: ${hl_msg}"
if [[ "$DRY" == "1" ]]; then
  log "DRY: run $L would start now; the launch's record (single.json) and, once the run exits, its run directory would be copied to gs://${EVIDENCE_BUCKET}/runs/${LAUNCH_ID}/, verified and read back (python3 -m bench.durable upload / download)"
  exit 0
fi

dg="EXPECTED_IMAGE_DIGEST_$L"
python3 -c 'import json, sys, time
out, sha, label, name, tag, digest, run_out, ft, fd = sys.argv[1:10]
run = {"label": label, "run_name": name, "tag": tag, "digest": digest, "out": run_out}
if ft != "-":                         # run 26: the run overrode its frontend (FRONTEND_TAG_<L>)
    run["frontend_tag"], run["frontend_digest"] = ft, fd
json.dump({"kind": "single", "bench_sha": sha, "started": time.time(), "run": run}, open(out, "w"), indent=1)' \
  "$SINGLE_OUT/single.json" "$BENCH_SHA" "$L" "${!nm}" "${!tg}" "${!dg}" "${!out}" "${!ft:--}" "${!fd:--}"
# #1194: a launch that cannot publish its own record spends nothing
if ! msg="$(publish_single started 2>&1)"; then
  refuse "the launch's evidence could not be published: ${msg}: nothing was created"
fi
log "$msg"
# #1390 item 1: the evidence leaves the VM: first the launch's record, before the run starts, then the run's
# directory once it exits, each copy verified. A launch whose record cannot be copied (no bucket, no grant)
# spends nothing.
mkdir -p "$SINGLE_OUT/plan"
cp "$SINGLE_OUT/single.json" "$SINGLE_OUT/plan/single.json"
if ! msg="$(python3 -m bench.durable upload --bucket "$EVIDENCE_BUCKET" --launch "$LAUNCH_ID" --src "$SINGLE_OUT/plan" 2>&1)"; then
  refuse "the launch's record could not be copied to the evidence bucket: ${msg}: nothing was created"
fi
log "$msg"
DURABLE_FAILED=0
PID=""
# a signal stops the run, not the record: the run publishes its own in its teardown
trap 'log "signal: stopping run $L (it tears its own cluster down)"; [[ -z "$PID" ]] || kill -TERM "$PID" 2>/dev/null || true
      wait; msg="$(publish_single interrupted 2>&1)" || log "EVIDENCE NOT SAFE (launch): ${msg}"; exit 130' INT TERM HUP
# ONE simple command, backgrounded: env execs bash, so $! is run606's shell (its trap tears the cluster down)
env "${RUNENV[@]}" "$BASH" "$RUN606" > "$SINGLE_OUT/$L.console" 2>&1 &
PID="$!"
log "run $L pid $PID"
# bash 3.2 (macOS) has no `wait -n`, and a trapped signal interrupts a `wait`: poll, then reap
while kill -0 "$PID" 2>/dev/null; do sleep "${SINGLE_POLL_S:-5}"; done
set +e; wait "$PID"; RC="$?"; set -e
trap - INT TERM HUP
if grep -q "run complete: " "${!out}/run.log" 2>/dev/null; then done_note="its evaluate step ran"; else done_note="it did not complete"; fi
log "run $L exited ${RC} (${done_note})"
durable_copy "$L"
durable_fetch "$L" "${SINGLE_OUT}.durable"
# the arm's verdict, its exit (the pair launcher's codes for one verdict); none: the run's own exit
vrc="$(python3 -c 'import json, sys
try:
    v = json.load(open(sys.argv[1])).get("verdict")
except (OSError, ValueError):
    v = None
print({"PASS": 0, "FAIL": 1, "INVALID": 3, "INCONCLUSIVE": 4}.get(v, ""))' "${!out}/verdict.json")"
if [[ -z "$vrc" ]]; then
  vrc="$RC"; [[ "$vrc" != "0" ]] || vrc=1          # a run that exited 0 with no verdict decided nothing
  log "run $L has no verdict (verdict.json): exit ${vrc}"
else
  log "run $L verdict exit=${vrc}"
fi
# #1194: the launch is finished when its evidence is safe, whatever the verdict
safe=1
if msg="$(publish_single final 2>&1)"; then
  log "$msg"
else
  safe=0
  log "EVIDENCE NOT SAFE (launch): ${msg}"
fi
if [[ -e "${!out}/.evidence-due" ]]; then
  if msg="$(python3 -m bench.evidence verify --dest "$(python3 -m bench.evidence dest "${!nm}")" --stage final 2>&1)"; then
    log "run $L: $msg"
  else
    safe=0
    log "EVIDENCE NOT SAFE (run $L): ${msg}"
  fi
fi
if [[ "$DURABLE_FAILED" == "1" ]]; then
  safe=0
  log "EVIDENCE NOT DURABLE: the run's directory could not be copied to gs://${EVIDENCE_BUCKET}/runs/${LAUNCH_ID}/ and read back (above)"
fi
if [[ "$safe" != "1" ]]; then
  log "the launch is NOT finished: its evidence is not safe (exit 5); the run's verdict, exit ${vrc}, stands in ${!out}"
  exit 5
fi
exit "$vrc"
