#!/usr/bin/env bash
# #606 / #865 — PAIRED runs (Diego 2026-10-23): A (the baseline image) and B
# (the candidate) run in parallel, each on its own TEMPORARY cluster (two
# run606.sh, each with its own teardown layers), from ONE bench checkout at
# one commit, on one schedule with k6's customers on through every churn step
# (K6_SPAN=churn). Then the paired customer-latency verdict (bench/custlat.py
# pair: B against A per phase, the spread from A's own data).
#
# Per run (required):  RUN_NAME_A / _B, SNOWPLOW_TAG_A / _B,
#                      EXPECTED_IMAGE_DIGEST_A / _B, BENCH_606_GO_A / _B
#                      (Diego's go PER RUN, as run606.sh: <YYYYMMDD>-<RUN_NAME>)
# Everything else in the environment is shared by both runs, so the two
# schedules are the same by construction (and custlat pair checks it from the
# data). The per-run names themselves (RUN_NAME, SNOWPLOW_TAG, ...) and K6_SPAN
# other than churn are refused: they would apply to both runs or break the pair.
#
# Nothing is created before BOTH runs passed run606.sh's own refusals: a dry
# pass of each (BENCH_606_DRY_RUN=1) runs first. BENCH_606_DRY_RUN=1 here
# stops after it. If one run fails (run606 exits non-zero: a tooling failure,
# never a verdict), the other is stopped (TERM: its trap tears its cluster
# down) — half a pair cannot be compared.
#
# OUT_A / OUT_B: each run's OUT (default: run606.sh's, per RUN_NAME).
# Out: $PAIR_OUT/pair.json (bench commit, both runs' names / tags / digests /
# OUTs), pair.log, <A|B>.console, paired-verdict.json. Exit: the paired
# verdict's (0 PASS, 1 FAIL, 3 INVALID); 2 = refused before any cloud call.
set -Eeuo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
DRY="${BENCH_606_DRY_RUN:-0}"
RUN606="$HERE/run606.sh"
REPLICA="$HERE/customer_latency_replica_runs12_13.json"

refuse() { echo "refusing: $*" >&2; exit 2; }
for v in RUN_NAME_A RUN_NAME_B SNOWPLOW_TAG_A SNOWPLOW_TAG_B EXPECTED_IMAGE_DIGEST_A EXPECTED_IMAGE_DIGEST_B; do
  [[ -n "${!v:-}" ]] || refuse "$v is required (one per run of the pair)"
done
for v in RUN_NAME SNOWPLOW_TAG EXPECTED_IMAGE_DIGEST BENCH_606_GO OUT; do
  [[ -z "${!v:-}" ]] || refuse "$v is per run (set ${v}_A / ${v}_B): unset it"
done
[[ -z "${DRIVE_S:-}" ]] || refuse "DRIVE_S is derived by K6_SPAN=churn: unset it"
[[ "${K6_SPAN:-churn}" == "churn" ]] || refuse "a pair runs K6_SPAN=churn (customers on through every churn step)"
[[ "${LOAD_DRIVER:-k6}" == "k6" ]] || refuse "a pair compares k6's customers: LOAD_DRIVER=k6"
[[ "$RUN_NAME_A" != "$RUN_NAME_B" ]] || refuse "RUN_NAME_A and RUN_NAME_B are the same: two clusters need two names"
[[ "$EXPECTED_IMAGE_DIGEST_A" != "$EXPECTED_IMAGE_DIGEST_B" ]] || refuse "the two digests are the same image: nothing to compare"
if [[ "$DRY" != "1" ]]; then
  today="$(date -u +%Y%m%d)"
  [[ "${BENCH_606_GO_A:-}" == "${today}-${RUN_NAME_A}" ]] || refuse "BENCH_606_GO_A must be '${today}-${RUN_NAME_A}' (Diego's go for run A)"
  [[ "${BENCH_606_GO_B:-}" == "${today}-${RUN_NAME_B}" ]] || refuse "BENCH_606_GO_B must be '${today}-${RUN_NAME_B}' (Diego's go for run B)"
fi
# one commit, nothing uncommitted under the bench: both runs execute it
BENCH_SHA="$(git -C "$HERE" rev-parse HEAD)" || refuse "the bench is not a git checkout"
dirty="$(git -C "$HERE" status --porcelain -- . | head -5)"
if [[ -n "$dirty" ]]; then
  [[ "$DRY" == "1" ]] || refuse "uncommitted changes under the bench: the two runs must be one commit (${dirty//$'\n'/, })"
fi
PAIR_OUT="${PAIR_OUT:-/tmp/snowplow-runs/606/pair-${RUN_NAME_A}-${RUN_NAME_B}}"
OUT_A="${OUT_A:-/tmp/snowplow-runs/606/${RUN_NAME_A}}"    # run606.sh's own default OUT
OUT_B="${OUT_B:-/tmp/snowplow-runs/606/${RUN_NAME_B}}"
[[ "$OUT_A" != "$OUT_B" ]] || refuse "OUT_A and OUT_B are the same directory"
mkdir -p "$PAIR_OUT"
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$PAIR_OUT/pair.log" >&2; }

# run <A|B> <extra env...> -- run606.sh with that run's names
run() {
  local x="$1"; shift
  local name tag dig go out
  name="RUN_NAME_$x"; tag="SNOWPLOW_TAG_$x"; dig="EXPECTED_IMAGE_DIGEST_$x"; go="BENCH_606_GO_$x"
  out="OUT_$x"
  env RUN_NAME="${!name}" SNOWPLOW_TAG="${!tag}" EXPECTED_IMAGE_DIGEST="${!dig}" \
      BENCH_606_GO="${!go:-}" K6_SPAN=churn LOAD_DRIVER=k6 OUT="${!out}" "$@" bash "$RUN606"
}

log "pair: A=${RUN_NAME_A} (${SNOWPLOW_TAG_A}) B=${RUN_NAME_B} (${SNOWPLOW_TAG_B}), bench ${BENCH_SHA}"
for x in A B; do
  if ! run "$x" BENCH_606_DRY_RUN=1 OUT="$PAIR_OUT/dry-$x" > "$PAIR_OUT/dry-$x.console" 2>&1; then
    tail -5 "$PAIR_OUT/dry-$x.console" >&2
    refuse "run $x's dry pass was refused (${PAIR_OUT}/dry-$x.console): nothing was created"
  fi
  log "run $x: dry pass OK"
done
eval_cmd=(python3 -m bench.custlat pair --a "$OUT_A/verdict.json" --b "$OUT_B/verdict.json"
          --replica "$REPLICA" --expect-a "$EXPECTED_IMAGE_DIGEST_A" --expect-b "$EXPECTED_IMAGE_DIGEST_B")
if [[ "$DRY" == "1" ]]; then
  log "DRY: both runs would start now; then ${eval_cmd[*]} > $PAIR_OUT/paired-verdict.json"
  exit 0
fi

python3 -c 'import json, sys, time
keys = ("run_name", "tag", "digest", "out")
json.dump({"kind": "pair", "bench_sha": sys.argv[2], "started": time.time(),
           "a": dict(zip(keys, sys.argv[3:7])), "b": dict(zip(keys, sys.argv[7:11]))},
          open(sys.argv[1], "w"), indent=1)' \
  "$PAIR_OUT/pair.json" "$BENCH_SHA" "$RUN_NAME_A" "$SNOWPLOW_TAG_A" "$EXPECTED_IMAGE_DIGEST_A" "$OUT_A" \
  "$RUN_NAME_B" "$SNOWPLOW_TAG_B" "$EXPECTED_IMAGE_DIGEST_B" "$OUT_B"
run A > "$PAIR_OUT/A.console" 2>&1 & PID_A=$!
run B > "$PAIR_OUT/B.console" 2>&1 & PID_B=$!
log "run A pid ${PID_A}, run B pid ${PID_B}"
stop_both() { kill -TERM "$PID_A" "$PID_B" 2>/dev/null || true; }
trap 'log "signal: stopping both runs (each tears its own cluster down)"; stop_both; wait; exit 130' INT TERM HUP
# bash 3.2 (macOS) has no `wait -n`: poll; the first run to FAIL stops the other
RC_A="" RC_B=""
while [[ -z "$RC_A" || -z "$RC_B" ]]; do
  for x in A B; do
    pid="PID_$x" rc="RC_$x"
    if [[ -z "${!rc}" ]] && ! kill -0 "${!pid}" 2>/dev/null; then
      set +e; wait "${!pid}"; printf -v "$rc" '%s' "$?"; set -e
      log "run $x exited ${!rc}"
      if [[ "${!rc}" != "0" ]]; then
        other=$([[ "$x" == "A" ]] && echo B || echo A); opid="PID_$other"
        log "run $x failed: stopping run $other (half a pair cannot be compared)"
        kill -TERM "${!opid}" 2>/dev/null || true
      fi
    fi
  done
  [[ -n "$RC_A" && -n "$RC_B" ]] || sleep "${PAIR_POLL_S:-5}"
done
trap - INT TERM HUP
vrc=0
"${eval_cmd[@]}" > "$PAIR_OUT/paired-verdict.json" || vrc=$?
log "paired verdict exit=${vrc} (0 PASS, 1 FAIL, 3 INVALID) -> $PAIR_OUT/paired-verdict.json"
while IFS= read -r r; do
  [[ -n "$r" ]] && log "$r"
done < <(python3 -c 'import json, sys; v = json.load(open(sys.argv[1]))
print("\n".join(["INVALID: " + r for r in v["invalid_reasons"]] + ["%s %s ratio=%s bar: %s" % (c["result"], c["check"], c["value"]["ratio"], c["bar"]) for c in v["checks"]]))' \
           "$PAIR_OUT/paired-verdict.json" 2>/dev/null || true)
exit "$vrc"
