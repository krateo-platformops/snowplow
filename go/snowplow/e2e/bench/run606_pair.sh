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
# #1004's full-widen stage (after each run's evaluate step): BENCH_1004_GO_A
# and _B, Diego's go for it PER RUN in the same form; both or neither (half a
# stage cannot be compared), and its one switch.
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
# that already holds a run (verdict.json or run.log) is refused, and so is a
# PAIR_OUT that holds a pair (pair.json or paired-verdict.json).
# Out: $PAIR_OUT/pair.json (bench commit, both runs' names / tags / digests /
# OUTs), pair.log, <A|B>.console, paired-verdict.json. Exit: the paired
# verdict's (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE); 2 = refused before
# anything was created (#1171: both runs' footprints, together, against every
# regional quota they consume: bench/gke.py RUN_QUOTAS); 5 = the evidence is not
# safe (#1194): whatever the verdict says, the pair is NOT finished. Each run
# publishes its own before its cluster goes (run606.sh). The pair publishes
# pair.json before either run starts, and all of PAIR_OUT once the verdict
# exists, which is after both clusters went: it is computed from files both
# runs already published, so it can be computed again. It then verifies both
# runs' published copies; a run that never reached a cluster owes none.
# A restart of B FAILs the pair; a restart of A is recorded and
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
for v in RUN_NAME SNOWPLOW_TAG EXPECTED_IMAGE_DIGEST BENCH_606_GO BENCH_1004_GO OUT; do
  [[ -z "${!v:-}" ]] || refuse "$v is per run (set ${v}_A / ${v}_B): unset it"
done
[[ -z "${DRIVE_S:-}" ]] || refuse "DRIVE_S is derived by K6_SPAN=churn: unset it"
[[ "${K6_SPAN:-churn}" == "churn" ]] || refuse "a pair runs K6_SPAN=churn (customers on through every churn step)"
[[ "${LOAD_DRIVER:-k6}" == "k6" ]] || refuse "a pair compares k6's customers: LOAD_DRIVER=k6"
[[ "$RUN_NAME_A" != "$RUN_NAME_B" ]] || refuse "RUN_NAME_A and RUN_NAME_B are the same: two clusters need two names"
[[ "$EXPECTED_IMAGE_DIGEST_A" != "$EXPECTED_IMAGE_DIGEST_B" ]] || refuse "the two digests are the same image: nothing to compare"
# #1004: the full-widen stage runs on both arms or on neither
if [[ -n "${BENCH_1004_GO_A:-}" && -z "${BENCH_1004_GO_B:-}" ]] || [[ -z "${BENCH_1004_GO_A:-}" && -n "${BENCH_1004_GO_B:-}" ]]; then
  refuse "BENCH_1004_GO_A and BENCH_1004_GO_B: set both (the full-widen stage on both arms) or neither"
fi
if [[ "$DRY" != "1" ]]; then
  today="$(date -u +%Y%m%d)"
  [[ "${BENCH_606_GO_A:-}" == "${today}-${RUN_NAME_A}" ]] || refuse "BENCH_606_GO_A must be '${today}-${RUN_NAME_A}' (Diego's go for run A)"
  [[ "${BENCH_606_GO_B:-}" == "${today}-${RUN_NAME_B}" ]] || refuse "BENCH_606_GO_B must be '${today}-${RUN_NAME_B}' (Diego's go for run B)"
  if [[ -n "${BENCH_1004_GO_A:-}" ]]; then
    [[ "$BENCH_1004_GO_A" == "${today}-${RUN_NAME_A}" ]] || refuse "BENCH_1004_GO_A must be '${today}-${RUN_NAME_A}' (Diego's go for the full-widen stage on run A)"
    [[ "$BENCH_1004_GO_B" == "${today}-${RUN_NAME_B}" ]] || refuse "BENCH_1004_GO_B must be '${today}-${RUN_NAME_B}' (Diego's go for the full-widen stage on run B)"
  fi
fi
# one commit, nothing uncommitted under the bench: both runs execute it
BENCH_SHA="$(git -C "$HERE" rev-parse HEAD)" || refuse "the bench is not a git checkout"
dirty="$(git -C "$HERE" status --porcelain -- . | head -5)"
if [[ -n "$dirty" ]]; then
  [[ "$DRY" == "1" ]] || refuse "uncommitted changes under the bench: the two runs must be one commit (${dirty//$'\n'/, })"
fi
PAIR_NAME="pair-${RUN_NAME_A}-${RUN_NAME_B}"
PAIR_OUT="${PAIR_OUT:-$(python3 -m bench.evidence run-dir "$PAIR_NAME")}"
OUT_A="${OUT_A:-$(python3 -m bench.evidence run-dir "$RUN_NAME_A")}"    # run606.sh's own default OUT
OUT_B="${OUT_B:-$(python3 -m bench.evidence run-dir "$RUN_NAME_B")}"
PAIR_EVIDENCE="$(python3 -m bench.evidence dest "$PAIR_NAME")"
PAIR_RUN_ID="${PAIR_NAME}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
[[ "$OUT_A" != "$OUT_B" ]] || refuse "OUT_A and OUT_B are the same directory"
if [[ "$DRY" != "1" ]]; then
  for x in A B; do
    out="OUT_$x"
    for f in verdict.json run.log; do
      [[ ! -e "${!out}/$f" ]] || refuse "OUT_$x (${!out}) already holds a run ($f): the pair would grade it; use a new RUN_NAME_$x or OUT_$x"
    done
  done
  for f in pair.json paired-verdict.json; do
    [[ ! -e "$PAIR_OUT/$f" ]] || refuse "PAIR_OUT ($PAIR_OUT) already holds a pair ($f): it is never written over; use new run names or a new PAIR_OUT"
  done
fi
python3 -m bench.evidence preflight --dest "$PAIR_EVIDENCE" || exit 2
mkdir -p "$PAIR_OUT"
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$PAIR_OUT/pair.log" >&2; }

# publish_pair <stage> [--require F]...: all of PAIR_OUT, copied and verified (bench/evidence.py)
publish_pair() {
  local stage="$1"; shift
  python3 -m bench.evidence publish --src "$PAIR_OUT" --dest "$PAIR_EVIDENCE" --run-id "$PAIR_RUN_ID" \
      --stage "$stage" --require pair.json "$@"
}

# runenv <A|B>: RUNENV = that run's per-run variables, for `env`
runenv() {
  local name="RUN_NAME_$1" tag="SNOWPLOW_TAG_$1" dig="EXPECTED_IMAGE_DIGEST_$1" go="BENCH_606_GO_$1"
  local out="OUT_$1" fw="BENCH_1004_GO_$1"
  RUNENV=(RUN_NAME="${!name}" SNOWPLOW_TAG="${!tag}" EXPECTED_IMAGE_DIGEST="${!dig}"
          BENCH_606_GO="${!go:-}" BENCH_1004_GO="${!fw:-}" K6_SPAN=churn LOAD_DRIVER=k6 OUT="${!out}")
}

log "pair: A=${RUN_NAME_A} (${SNOWPLOW_TAG_A}) B=${RUN_NAME_B} (${SNOWPLOW_TAG_B}), bench ${BENCH_SHA}, ${BASH} ${BASH_VERSION}"
for x in A B; do
  runenv "$x"
  rm -f "$PAIR_OUT/dry-$x/footprint.json"   # #1171: only THIS dry pass's derivation counts
  if ! env "${RUNENV[@]}" BENCH_606_DRY_RUN=1 OUT="$PAIR_OUT/dry-$x" "$BASH" "$RUN606" \
       > "$PAIR_OUT/dry-$x.console" 2>&1; then
    tail -5 "$PAIR_OUT/dry-$x.console" >&2
    refuse "run $x's dry pass was refused (${PAIR_OUT}/dry-$x.console): nothing was created"
  fi
  log "run $x: dry pass OK"
done
# What both clusters need, together (each gke-up alone would see room for
# itself): reads only, nothing is created, so the DRY pass runs them too and
# refuses a pair that cannot start. #1171: each run's dry pass derived its
# footprint from the spec its gke-up creates and the LoadBalancer Services its
# install creates ($OUT/footprint.json); both together must fit every regional
# quota they consume (bench/gke.py RUN_QUOTAS): the PVC disks (#895), GKE's
# per-location cluster limit (#985, read), the static addresses (#1026), the
# in-use addresses every node and LoadBalancer holds (run 21 attempt 2 failed
# 30 min in on IN_USE_ADDRESSES), vCPUs, instances, disks, forwarding rules.
quota_args=()
[[ -n "${PROJECT:-}" ]] && quota_args+=(--project "$PROJECT")
[[ -n "${ZONE:-}" ]] && quota_args+=(--zone "$ZONE")
for x in A B; do
  [[ -s "$PAIR_OUT/dry-$x/footprint.json" ]] \
    || refuse "run $x's dry pass did not derive its LoadBalancer Services and cluster footprint ($PAIR_OUT/dry-$x/footprint.json): whether the pair fits the region's quotas is unknown; nothing was created"
  quota_args+=(--footprint "$PAIR_OUT/dry-$x/footprint.json")
done
if ! quota_msg="$(python3 -m bench.gke gke-quotas "${quota_args[@]}" 2>&1)"; then
  refuse "${quota_msg//$'\n'/$'\n'refusing: }"
fi
while IFS= read -r ln; do
  if [[ -n "$ln" ]]; then log "$ln"; fi
done <<< "$quota_msg"
eval_cmd=(python3 -m bench.custlat pair --a "$OUT_A/verdict.json" --b "$OUT_B/verdict.json"
          --a-dir "$OUT_A" --b-dir "$OUT_B"
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
# #1194: a pair that cannot publish its own record spends nothing
if ! msg="$(publish_pair started 2>&1)"; then
  refuse "the pair's evidence could not be published: ${msg}: nothing was created"
fi
log "$msg"
PID_A="" PID_B=""
stop_both() { kill -TERM ${PID_A:+"$PID_A"} ${PID_B:+"$PID_B"} 2>/dev/null || true; }
# a signal stops the runs, not the record: each run publishes its own in its teardown
trap 'log "signal: stopping both runs (each tears its own cluster down)"; stop_both; wait
      msg="$(publish_pair interrupted 2>&1)" || log "EVIDENCE NOT SAFE (pair): ${msg}"; exit 130' INT TERM HUP
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
# #1194: the pair is finished when its evidence is safe, whatever the verdict
safe=1
if msg="$(publish_pair final --require paired-verdict.json 2>&1)"; then
  log "$msg"
else
  safe=0
  log "EVIDENCE NOT SAFE (pair): ${msg}"
fi
for x in A B; do
  out="OUT_$x" name="RUN_NAME_$x"
  [[ -e "${!out}/.evidence-due" ]] || continue
  if msg="$(python3 -m bench.evidence verify --dest "$(python3 -m bench.evidence dest "${!name}")" --stage final 2>&1)"; then
    log "run $x: $msg"
  else
    safe=0
    log "EVIDENCE NOT SAFE (run $x): ${msg}"
  fi
done
if [[ "$safe" != "1" ]]; then
  log "the pair is NOT finished: its evidence is not safe (exit 5); the paired verdict, exit ${vrc}, stands in $PAIR_OUT/paired-verdict.json"
  exit 5
fi
exit "$vrc"
