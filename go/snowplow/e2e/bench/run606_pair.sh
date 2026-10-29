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
# stops after it.
#
# Stopping. run606.sh exits 0 once its evaluate step ran, whatever the verdict
# (a verdict is a result); it logs "run complete: <OUT>" then. A run that
# exits non-zero WITHOUT that line failed as tooling or infrastructure: the
# other run is stopped (TERM), since half a pair cannot be compared. A run
# that completed and then exited non-zero (e.g. its gke-down failed) is a
# measurement; its partner keeps running. Each run is ONE background simple
# command ending in exec (env -> bash run606.sh), so its PID is run606's own
# shell under bash 3.2 as under 5: a TERM reaches run606's trap, which tears
# its cluster down. Background jobs of a non-interactive shell ignore SIGINT,
# so a Ctrl-C (INT) or a TERM to this script is forwarded as TERM to both.
#
# OUT_A / OUT_B: each run's OUT (default: run606.sh's, per RUN_NAME); an OUT
# that already holds a run (verdict.json or run.log) is refused.
# Out: $PAIR_OUT/pair.json (bench commit, both runs' names / tags / digests /
# OUTs), pair.log, <A|B>.console, paired-verdict.json. Exit: the paired
# verdict's (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE); 2 = refused before
# anything was created (#895: the SSD quota read refuses too). A restart of B
# FAILs the pair; a restart of A is recorded and
# its not-Ready time is removed from both runs (bench/custlat.py's module doc):
# the pair reads each run's k6 stream from its OUT for that.
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
if [[ "$DRY" != "1" ]]; then
  for x in A B; do
    out="OUT_$x"
    for f in verdict.json run.log; do
      [[ ! -e "${!out}/$f" ]] || refuse "OUT_$x (${!out}) already holds a run ($f): the pair would grade it; use a new RUN_NAME_$x or OUT_$x"
    done
  done
fi
mkdir -p "$PAIR_OUT"
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$PAIR_OUT/pair.log" >&2; }

# runenv <A|B>: RUNENV = that run's per-run variables, for `env`
runenv() {
  local name="RUN_NAME_$1" tag="SNOWPLOW_TAG_$1" dig="EXPECTED_IMAGE_DIGEST_$1" go="BENCH_606_GO_$1"
  local out="OUT_$1"
  RUNENV=(RUN_NAME="${!name}" SNOWPLOW_TAG="${!tag}" EXPECTED_IMAGE_DIGEST="${!dig}"
          BENCH_606_GO="${!go:-}" K6_SPAN=churn LOAD_DRIVER=k6 OUT="${!out}")
}

log "pair: A=${RUN_NAME_A} (${SNOWPLOW_TAG_A}) B=${RUN_NAME_B} (${SNOWPLOW_TAG_B}), bench ${BENCH_SHA}, ${BASH} ${BASH_VERSION}"
for x in A B; do
  runenv "$x"
  if ! env "${RUNENV[@]}" BENCH_606_DRY_RUN=1 OUT="$PAIR_OUT/dry-$x" "$BASH" "$RUN606" \
       > "$PAIR_OUT/dry-$x.console" 2>&1; then
    tail -5 "$PAIR_OUT/dry-$x.console" >&2
    refuse "run $x's dry pass was refused (${PAIR_OUT}/dry-$x.console): nothing was created"
  fi
  log "run $x: dry pass OK"
done
eval_cmd=(python3 -m bench.custlat pair --a "$OUT_A/verdict.json" --b "$OUT_B/verdict.json"
          --a-dir "$OUT_A" --b-dir "$OUT_B"
          --replica "$REPLICA" --expect-a "$EXPECTED_IMAGE_DIGEST_A" --expect-b "$EXPECTED_IMAGE_DIGEST_B")
if [[ "$DRY" == "1" ]]; then
  log "DRY: both runs would start now; then ${eval_cmd[*]} > $PAIR_OUT/paired-verdict.json"
  exit 0
fi
# #895: both clusters' PVC disks must fit the region's SSD_TOTAL_GB together
# (each gke-up alone would see room for itself); a read, nothing is created
quota_args=()
[[ -n "${PROJECT:-}" ]] && quota_args+=(--project "$PROJECT")
[[ -n "${ZONE:-}" ]] && quota_args+=(--zone "$ZONE")
if ! quota_msg="$(python3 -m bench.gke gke-ssd-quota ${quota_args[@]+"${quota_args[@]}"} --runs 2 2>&1)"; then
  refuse "${quota_msg#refusing: }"
fi
log "$quota_msg"

python3 -c 'import json, sys, time
keys = ("run_name", "tag", "digest", "out")
json.dump({"kind": "pair", "bench_sha": sys.argv[2], "started": time.time(),
           "a": dict(zip(keys, sys.argv[3:7])), "b": dict(zip(keys, sys.argv[7:11]))},
          open(sys.argv[1], "w"), indent=1)' \
  "$PAIR_OUT/pair.json" "$BENCH_SHA" "$RUN_NAME_A" "$SNOWPLOW_TAG_A" "$EXPECTED_IMAGE_DIGEST_A" "$OUT_A" \
  "$RUN_NAME_B" "$SNOWPLOW_TAG_B" "$EXPECTED_IMAGE_DIGEST_B" "$OUT_B"
PID_A="" PID_B=""
stop_both() { kill -TERM ${PID_A:+"$PID_A"} ${PID_B:+"$PID_B"} 2>/dev/null || true; }
trap 'log "signal: stopping both runs (each tears its own cluster down)"; stop_both; wait; exit 130' INT TERM HUP
# ONE simple command each, backgrounded: env execs bash, so $! is run606's shell
runenv A
env "${RUNENV[@]}" "$BASH" "$RUN606" > "$PAIR_OUT/A.console" 2>&1 &
PID_A=$!
runenv B
env "${RUNENV[@]}" "$BASH" "$RUN606" > "$PAIR_OUT/B.console" 2>&1 &
PID_B=$!
log "run A pid ${PID_A}, run B pid ${PID_B}"
completed() { local out="OUT_$1"; grep -q "run complete: " "${!out}/run.log" 2>/dev/null; }
# bash 3.2 (macOS) has no `wait -n`: poll; a run that FAILED as tooling stops the other
RC_A="" RC_B="" DONE_A=0 DONE_B=0
while [[ -z "$RC_A" || -z "$RC_B" ]]; do
  for x in A B; do
    pid="PID_$x" rc="RC_$x"
    if [[ -z "${!rc}" ]] && ! kill -0 "${!pid}" 2>/dev/null; then
      set +e; wait "${!pid}"; printf -v "$rc" '%s' "$?"; set -e
      if completed "$x"; then printf -v "DONE_$x" '%s' 1; fi
      done_x="DONE_$x"
      log "run $x exited ${!rc} ($([[ "${!done_x}" == "1" ]] && echo 'its evaluate step ran' || echo 'it did not complete'))"
      if [[ "${!rc}" != "0" && "${!done_x}" != "1" ]]; then
        other=$([[ "$x" == "A" ]] && echo B || echo A); opid="PID_$other"; orc="RC_$other"
        if [[ -z "${!orc}" ]]; then
          # a PAIR STOP (USR1), not a TERM: its measurement stops and its teardown, diagnostics
          # and disk backstop run in full (run 17 attempt 4: the TERM reached B's teardown)
          log "run $x failed as tooling: stopping run $other (half a pair cannot be compared): its measurement stops, its teardown runs in full"
          kill -USR1 "${!opid}" 2>/dev/null || true
        fi
      fi
    fi
  done
  [[ -n "$RC_A" && -n "$RC_B" ]] || sleep "${PAIR_POLL_S:-5}"
done
trap - INT TERM HUP
vrc=0
"${eval_cmd[@]}" --completed-a "$DONE_A" --completed-b "$DONE_B" > "$PAIR_OUT/paired-verdict.json" || vrc=$?
log "paired verdict exit=${vrc} (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE) -> $PAIR_OUT/paired-verdict.json"
while IFS= read -r r; do
  [[ -n "$r" ]] && log "$r"
done < <(python3 -c 'import json, sys; v = json.load(open(sys.argv[1]))
ex = (v.get("exclusion") or {}).get("items") or []
print("\n".join(["FAIL: " + r for r in v.get("fail_reasons") or [] if ".candidate_" in r]
                + ["INVALID: " + r for r in v["invalid_reasons"]] + ["NOTE: " + n for n in v.get("notes") or []]
                + ["EXCLUDED from both runs: %s (A %.0f s, B %.0f s): %s" % (i["what"], sum(b - a for a, b in i["a"]),
                   sum(b - a for a, b in i["b"]), i["why"]) for i in ex]
                + [c["line"] for c in v["cells"]] + (v.get("a_healthy_lines") or [])
                + [r["line"] for r in v.get("b_inside_a_outage") or []]))' \
           "$PAIR_OUT/paired-verdict.json" 2>/dev/null || true)
exit "$vrc"
