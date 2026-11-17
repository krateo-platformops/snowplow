# shellcheck shell=bash disable=SC2034  # sourced: its variables are its launchers'
# run606_arm.sh — one measuring arm's steps, the SAME for every launcher that runs arms (sourced, never run):
# run606_pair.sh (two or more arms and their pairs) and run606_single.sh (one arm, no pair: the one-cluster
# rehearsal off the laptop, Diego 2026-11-17). Each launcher sets, before calling these:
#   log <text>        its own log line
#   EVIDENCE_BUCKET   the evidence bucket (python3 -m bench.durable bucket)
#   LAUNCH_ID         the launch's id: its prefix in the bucket (gs://$EVIDENCE_BUCKET/runs/$LAUNCH_ID/)
#   OUT_<L>, RUN_NAME_<L>, ...  the arm's own variables

# runenv <L>: RUNENV = that run's per-run variables, for `env`
runenv() {
  local name="RUN_NAME_$1" tag="SNOWPLOW_TAG_$1" dig="EXPECTED_IMAGE_DIGEST_$1" go="BENCH_606_GO_$1"
  local out="OUT_$1" fw="BENCH_1004_GO_$1"
  RUNENV=(RUN_NAME="${!name}" SNOWPLOW_TAG="${!tag}" EXPECTED_IMAGE_DIGEST="${!dig}"
          BENCH_606_GO="${!go:-}" BENCH_1004_GO="${!fw:-}" K6_SPAN=churn LOAD_DRIVER=k6 OUT="${!out}"
          FRONTEND_TAG_VAR="FRONTEND_TAG_$1")   # run 26d: its login-contract refusal names this run's variable
  # the run's declared snowplow env and its label (a Go runtime key no run declared must read the same on
  # both arms of a pair: compared after the measure, from each run's record, by custlat pair and replica)
  local se="SNOWPLOW_ENV_$1"
  RUNENV+=(SNOWPLOW_ENV="${!se:-}" BENCH_ARM="$1")
  # run 26: the run's frontend override, only when it has one
  local ft="FRONTEND_TAG_$1" fd="EXPECTED_FRONTEND_DIGEST_$1"
  if [[ -n "${!ft:-}" ]]; then
    RUNENV+=(FRONTEND_TAG="${!ft}" EXPECTED_FRONTEND_DIGEST="${!fd}")
  fi
}

# #1390 item 1: the evidence leaves the orchestrator's machine, each arm's run directory as the arm exits.
# durable_copy <L>: the run's directory to the bucket, verified (DURABLE_<L> = 1), or said loudly (DURABLE_FAILED = 1)
durable_copy() {
  local out="OUT_$1" msg
  if msg="$(python3 -m bench.durable upload --bucket "$EVIDENCE_BUCKET" --launch "$LAUNCH_ID" --src "${!out}" 2>&1)"; then
    printf -v "DURABLE_$1" '%s' 1
    log "run $1: ${msg}"
  else
    printf -v "DURABLE_$1" '%s' 0
    DURABLE_FAILED=1
    log "EVIDENCE NOT DURABLE (run $1): ${msg}: its run directory is only on this machine"
  fi
}

# #1390 item 2: durable_fetch <L> <dir>: a fresh download of the arm's durable copy into <dir>, verified against its
# manifest: SRC_<L> = <dir>/<its run dir>, or "" (and DURABLE_FAILED = 1) when the copy cannot be read back. An arm
# with no verified copy (DURABLE_<L> != 1) is skipped: it said so when it was copied
durable_fetch() {
  local out="OUT_$1" dur="DURABLE_$1" msg
  printf -v "SRC_$1" '%s' ""
  [[ "${!dur:-0}" == "1" ]] || return 0
  if msg="$(python3 -m bench.durable download --bucket "$EVIDENCE_BUCKET" --launch "$LAUNCH_ID" \
              --name "${!out##*/}" --dest "$2" 2>&1)"; then
    printf -v "SRC_$1" '%s' "$2/${!out##*/}"
    log "run $1: ${msg}"
  else
    DURABLE_FAILED=1
    log "EVIDENCE NOT DURABLE (run $1): its durable copy could not be downloaded and verified: ${msg}"
  fi
}
