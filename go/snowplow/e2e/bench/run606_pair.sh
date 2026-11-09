#!/usr/bin/env bash
# #606 / #865 — PAIRED runs (Diego 2026-10-23), generalised to N runs in ONE
# window (run 22, Diego 2026-11-07): every run in parallel, each on its own
# TEMPORARY cluster (one run606.sh each, with its own teardown layers), from
# ONE bench checkout at one commit, on one schedule with k6's customers on
# through every churn step (K6_SPAN=churn). Then one paired customer-latency
# verdict per PAIR (bench/custlat.py pair: the candidate against the baseline
# per phase, the spread from the baseline's own data).
#
# LAUNCH (run 23 on): a paid launch is started DETACHED, never as a child of
# the session that types it. Run 22's launcher was killed when the Claude Code
# session restarted (its SIGTERM reached the process tree) and its teardown did
# not complete; macOS has no setsid(1). From this directory:
#   <every variable below> python3 -m bench.detach --log <LOG> --pidfile <PIDFILE> \
#       -- bash run606_pair.sh
# It prints the pid and returns at once; the launcher runs in a session and a
# process group of its own, re-parented away from the session, SIGHUP ignored,
# stdin /dev/null, stdout and stderr appended to <LOG> (bench/detach.py). Keep
# <LOG> and <PIDFILE> outside every run's OUT and PAIR_OUT. Watch it with
# `tail -f <LOG>` (and PAIR_OUT/pair.log); stop it with
# `kill -TERM "$(cat <PIDFILE>)"`, which stops every run below, each tearing
# its own cluster down. A BENCH_606_DRY_RUN=1 pass needs no detaching.
#
# RUNS  (default "A B"): the runs' labels, each [A-Z][A-Z0-9]*, two or more.
# PAIRS (default "A:B"): the comparisons, each BASELINE:CANDIDATE, both labels
#       in RUNS. A run may sit in several pairs (run 22: RUNS="R N C",
#       PAIRS="R:C N:C", C shared): every pair reads its runs' collected files.
#       Every run must be in at least one pair: a cluster that no comparison
#       reads is refused. Two runs and one pair is the case PAIRS="A:B" always was.
# REPLICA_PAIR (run 24, Diego 2026-11-09; default: none): one pair of PAIRS
#       declared the launch's A/A replica, e.g. RUNS="N1 N2 N2B" PAIRS="N1:N2
#       N2:N2B" REPLICA_PAIR="N2:N2B": its two runs run the SAME image on two
#       clusters (EXPECTED_IMAGE_DIGEST equal, required; refused otherwise), and it
#       gets no paired verdict of its own: its replica (`custlat replica --in-run`,
#       the same ratios as customer_latency_replica_runs12_13.json, measured on this
#       schedule, steady included) is $PAIR_OUT/replica-<L1>-<L2>.json, and it is
#       the between-run term of EVERY other pair, in place of the committed file.
#       A REPLICA_PAIR not in PAIRS, or with nothing to calibrate (no other pair), is
#       refused. Without REPLICA_PAIR the committed file is the replica, as before.
#       The replica's arms stay up only while a pair it calibrates can be compared.
#
# Per run L (required):  RUN_NAME_L, SNOWPLOW_TAG_L, EXPECTED_IMAGE_DIGEST_L,
#                        BENCH_606_GO_L (Diego's go PER RUN, as run606.sh:
#                        <YYYYMMDD>-<RUN_NAME>); OUT_L optional.
# #1004's full-widen stage (after each run's evaluate step): BENCH_1004_GO_L,
# Diego's go for it PER RUN in the same form; on every run or on none (part of
# a stage cannot be compared), and its one switch.
# Everything else in the environment is shared by every run, so the schedules
# are the same by construction (and custlat pair checks it from the data). The
# per-run names themselves (RUN_NAME, SNOWPLOW_TAG, ...) and K6_SPAN other than
# churn are refused: they would apply to every run or break the pairs. Run
# names are unique; the two runs of a pair run two different images.
#
# Nothing is created before EVERY run passed run606.sh's own refusals: a dry
# pass of each (BENCH_606_DRY_RUN=1) runs first, then ONE quota read for every
# cluster together. BENCH_606_DRY_RUN=1 here stops after it.
#
# Stopping. run606.sh exits 0 once its evaluate step ran, whatever the verdict
# (a verdict is a result); it logs "run complete: <OUT>" then. A run that
# exits non-zero WITHOUT that line failed as tooling or infrastructure: every
# pair it is in can no longer be compared. A run still going is stopped when
# EVERY pair it is in has lost a run, since its cluster then feeds no
# comparison; while one of its pairs can still be compared, it runs on (run
# 22: N failing leaves R:C, so R and C go on; C failing ends both pairs). A
# run that completed and then exited non-zero (e.g. its gke-down failed) is a
# measurement; nothing is stopped. Each run is ONE background simple command
# ending in exec (env -> bash run606.sh), so its PID is run606's own shell
# under bash 3.2 as under 5: a signal reaches run606's trap, which tears its
# cluster down. Background jobs of a non-interactive shell ignore SIGINT, so a
# Ctrl-C (INT) or a TERM to this script is forwarded as TERM to every run.
#
# OUT_L: each run's OUT (default: run606.sh's, per RUN_NAME); an OUT that
# already holds a run (verdict.json or run.log) is refused, and so is a
# PAIR_OUT that holds a pair (pair.json, any paired-verdict-*.json or replica-*.json).
# Out: $PAIR_OUT/pair.json (bench commit, every run's name / tag / digest /
# OUT, the pairs, the replica pair), pair.log, <L>.console, per pair
# paired-verdict-<BASELINE>-<CANDIDATE>.json, and with REPLICA_PAIR
# replica-<L1>-<L2>.json (no verdict of its own). Exit: the pairs' verdicts
# together: 1 when any pair FAILs (a pair's FAIL is that comparison's result,
# whatever another pair says), else 3 when any is INVALID, else 4 when any is
# INCONCLUSIVE, else 0 (every pair PASS); one pair's exit is its own verdict's.
# 2 = refused before anything was created (#1171: every run's footprint,
# together, against every regional quota they consume: bench/gke.py
# RUN_QUOTAS); 5 = the evidence is not safe (#1194): whatever the verdicts
# say, the launch is NOT finished. Each run publishes its own before its
# cluster goes (run606.sh). The launcher publishes pair.json before any run
# starts, and all of PAIR_OUT once the verdicts exist, which is after every
# cluster went: they are computed from files the runs already published, so
# they can be computed again. It then verifies every run's published copy; a
# run that never reached a cluster owes none.
# A restart of a pair's candidate FAILs that pair; a restart of its baseline
# is recorded and its not-Ready time is removed from both runs of the pair
# (bench/custlat.py's module doc): the pair reads each run's k6 stream from
# its OUT for that.
set -Eeuo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
DRY="${BENCH_606_DRY_RUN:-0}"
RUN606="$HERE/run606.sh"
REPLICA="$HERE/customer_latency_replica_runs12_13.json"

refuse() { echo "refusing: $*" >&2; exit 2; }
read -r -a LABELS <<< "${RUNS:-A B}"
read -r -a PAIR_LIST <<< "${PAIRS:-A:B}"
(( ${#LABELS[@]} >= 2 )) || refuse "RUNS names ${#LABELS[@]} run(s): a comparison needs two or more"
(( ${#PAIR_LIST[@]} >= 1 )) || refuse "PAIRS names no comparison"
is_label() { local x; for x in "${LABELS[@]}"; do [[ "$x" == "$1" ]] && return 0; done; return 1; }
seen=" "
for x in "${LABELS[@]}"; do
  [[ "$x" =~ ^[A-Z][A-Z0-9]*$ ]] || refuse "RUNS: '$x' is not a label ([A-Z][A-Z0-9]*: it names the run's variables, ${x}_...)"
  [[ "$seen" != *" $x "* ]] || refuse "RUNS names $x twice"
  seen="$seen$x "
done
seen=" "
for p in "${PAIR_LIST[@]}"; do
  [[ "$p" =~ ^([A-Z][A-Z0-9]*):([A-Z][A-Z0-9]*)$ ]] || refuse "PAIRS: '$p' is not BASELINE:CANDIDATE"
  is_label "${BASH_REMATCH[1]}" && is_label "${BASH_REMATCH[2]}" \
    || refuse "PAIRS: $p names a run that is not in RUNS (${LABELS[*]})"
  [[ "${BASH_REMATCH[1]}" != "${BASH_REMATCH[2]}" ]] || refuse "PAIRS: $p compares a run with itself"
  [[ "$seen" != *" $p "* ]] || refuse "PAIRS names $p twice"
  seen="$seen$p "
done
# in_pair <label> <pair>: the run is one of the pair's two
in_pair() { [[ "${2%%:*}" == "$1" || "${2##*:}" == "$1" ]]; }
for x in "${LABELS[@]}"; do
  n=0; for p in "${PAIR_LIST[@]}"; do in_pair "$x" "$p" && n=$(( n + 1 )); done
  (( n > 0 )) || refuse "run $x is in no pair (PAIRS: ${PAIR_LIST[*]}): its cluster would feed no comparison"
done
for x in "${LABELS[@]}"; do
  for v in RUN_NAME SNOWPLOW_TAG EXPECTED_IMAGE_DIGEST; do
    w="${v}_$x"; [[ -n "${!w:-}" ]] || refuse "$w is required (one per run)"
  done
done
for v in RUN_NAME SNOWPLOW_TAG EXPECTED_IMAGE_DIGEST BENCH_606_GO BENCH_1004_GO OUT; do
  [[ -z "${!v:-}" ]] || refuse "$v is per run (set ${v}_<run>, one per run of ${LABELS[*]}): unset it"
done
[[ -z "${DRIVE_S:-}" ]] || refuse "DRIVE_S is derived by K6_SPAN=churn: unset it"
[[ "${K6_SPAN:-churn}" == "churn" ]] || refuse "a pair runs K6_SPAN=churn (customers on through every churn step)"
[[ "${LOAD_DRIVER:-k6}" == "k6" ]] || refuse "a pair compares k6's customers: LOAD_DRIVER=k6"
for x in "${LABELS[@]}"; do
  for y in "${LABELS[@]}"; do
    [[ "$x" < "$y" ]] || continue
    nx="RUN_NAME_$x" ny="RUN_NAME_$y"
    [[ "${!nx}" != "${!ny}" ]] || refuse "RUN_NAME_$x and RUN_NAME_$y are the same: every cluster needs its own name"
  done
done
REPLICA_PAIR="${REPLICA_PAIR:-}"
if [[ -n "$REPLICA_PAIR" ]]; then
  [[ " ${PAIR_LIST[*]} " == *" $REPLICA_PAIR "* ]] \
    || refuse "REPLICA_PAIR $REPLICA_PAIR is not in PAIRS (${PAIR_LIST[*]}): the A/A replica must be one of the launch's pairs"
  (( ${#PAIR_LIST[@]} >= 2 )) \
    || refuse "REPLICA_PAIR $REPLICA_PAIR is the only pair: an A/A replica calibrates other pairs, and there is none"
  da="EXPECTED_IMAGE_DIGEST_${REPLICA_PAIR%%:*}" db="EXPECTED_IMAGE_DIGEST_${REPLICA_PAIR##*:}"
  [[ "${!da}" == "${!db}" ]] \
    || refuse "REPLICA_PAIR $REPLICA_PAIR: an A/A replica runs ONE image on both arms; ${da} (${!da}) and ${db} (${!db}) differ"
fi
# is_replica <pair>: the pair is the launch's declared A/A replica
is_replica() { [[ -n "$REPLICA_PAIR" && "$1" == "$REPLICA_PAIR" ]]; }
for p in "${PAIR_LIST[@]}"; do
  is_replica "$p" && continue
  da="EXPECTED_IMAGE_DIGEST_${p%%:*}" db="EXPECTED_IMAGE_DIGEST_${p##*:}"
  [[ "${!da}" != "${!db}" ]] || refuse "pair $p: the two digests are the same image: nothing to compare (an A/A replica is declared by REPLICA_PAIR)"
done
# #1004: the full-widen stage runs on every run or on none
fw=0; for x in "${LABELS[@]}"; do v="BENCH_1004_GO_$x"; [[ -n "${!v:-}" ]] && fw=$(( fw + 1 )); done
(( fw == 0 || fw == ${#LABELS[@]} )) \
  || refuse "BENCH_1004_GO_<run>: set it on every run of ${LABELS[*]} (the full-widen stage on every arm) or on none (${fw} set)"
if [[ "$DRY" != "1" ]]; then
  today="$(date -u +%Y%m%d)"
  for x in "${LABELS[@]}"; do
    go="BENCH_606_GO_$x" fwg="BENCH_1004_GO_$x" nm="RUN_NAME_$x"
    [[ "${!go:-}" == "${today}-${!nm}" ]] || refuse "$go must be '${today}-${!nm}' (Diego's go for run $x)"
    if (( fw > 0 )); then
      [[ "${!fwg}" == "${today}-${!nm}" ]] || refuse "$fwg must be '${today}-${!nm}' (Diego's go for the full-widen stage on run $x)"
    fi
  done
fi
# one commit, nothing uncommitted under the bench: every run executes it
BENCH_SHA="$(git -C "$HERE" rev-parse HEAD)" || refuse "the bench is not a git checkout"
dirty="$(git -C "$HERE" status --porcelain -- . | head -5)"
if [[ -n "$dirty" ]]; then
  [[ "$DRY" == "1" ]] || refuse "uncommitted changes under the bench: every run must be one commit (${dirty//$'\n'/, })"
fi
PAIR_NAME="pair"
for x in "${LABELS[@]}"; do nm="RUN_NAME_$x"; PAIR_NAME="${PAIR_NAME}-${!nm}"; done
PAIR_OUT="${PAIR_OUT:-$(python3 -m bench.evidence run-dir "$PAIR_NAME")}"
for x in "${LABELS[@]}"; do
  out="OUT_$x" nm="RUN_NAME_$x"
  [[ -n "${!out:-}" ]] || printf -v "$out" '%s' "$(python3 -m bench.evidence run-dir "${!nm}")"   # run606.sh's own default OUT
done
for x in "${LABELS[@]}"; do
  for y in "${LABELS[@]}"; do
    [[ "$x" < "$y" ]] || continue
    ox="OUT_$x" oy="OUT_$y"
    [[ "${!ox}" != "${!oy}" ]] || refuse "OUT_$x and OUT_$y are the same directory"
  done
done
REPLICA_FILE=""
if [[ -n "$REPLICA_PAIR" ]]; then
  REPLICA_FILE="$PAIR_OUT/replica-${REPLICA_PAIR%%:*}-${REPLICA_PAIR##*:}.json"
  REPLICA="$REPLICA_FILE"           # every other pair's between-run term: this launch's A/A pair
fi
PAIR_EVIDENCE="$(python3 -m bench.evidence dest "$PAIR_NAME")"
PAIR_RUN_ID="${PAIR_NAME}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
if [[ "$DRY" != "1" ]]; then
  for x in "${LABELS[@]}"; do
    out="OUT_$x"
    for f in verdict.json run.log; do
      [[ ! -e "${!out}/$f" ]] || refuse "OUT_$x (${!out}) already holds a run ($f): a pair would grade it; use a new RUN_NAME_$x or OUT_$x"
    done
  done
  for f in "$PAIR_OUT/pair.json" "$PAIR_OUT"/paired-verdict*.json "$PAIR_OUT"/replica-*.json; do
    [[ ! -e "$f" ]] || refuse "PAIR_OUT ($PAIR_OUT) already holds a pair (${f##*/}): it is never written over; use new run names or a new PAIR_OUT"
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

# runenv <L>: RUNENV = that run's per-run variables, for `env`
runenv() {
  local name="RUN_NAME_$1" tag="SNOWPLOW_TAG_$1" dig="EXPECTED_IMAGE_DIGEST_$1" go="BENCH_606_GO_$1"
  local out="OUT_$1" fw="BENCH_1004_GO_$1"
  RUNENV=(RUN_NAME="${!name}" SNOWPLOW_TAG="${!tag}" EXPECTED_IMAGE_DIGEST="${!dig}"
          BENCH_606_GO="${!go:-}" BENCH_1004_GO="${!fw:-}" K6_SPAN=churn LOAD_DRIVER=k6 OUT="${!out}")
}
# verdict_file <pair>: that pair's paired verdict
verdict_file() { echo "$PAIR_OUT/paired-verdict-${1%%:*}-${1##*:}.json"; }
# pair_cmd <pair>: PAIR_CMD = its custlat pair, the baseline as A, the candidate as B
pair_cmd() {
  local a="${1%%:*}" b="${1##*:}"
  local oa="OUT_$a" ob="OUT_$b" da="EXPECTED_IMAGE_DIGEST_$a" db="EXPECTED_IMAGE_DIGEST_$b"
  PAIR_CMD=(python3 -m bench.custlat pair --a "${!oa}/verdict.json" --b "${!ob}/verdict.json"
            --a-dir "${!oa}" --b-dir "${!ob}" --replica "$REPLICA" --expect-a "${!da}" --expect-b "${!db}"
            --a-label "$a" --b-label "$b")
}
# replica_cmd: REPLICA_CMD = the declared A/A pair's in-run replica (custlat replica --in-run)
replica_cmd() {
  local a="${REPLICA_PAIR%%:*}" b="${REPLICA_PAIR##*:}"
  local oa="OUT_$a" ob="OUT_$b" da="EXPECTED_IMAGE_DIGEST_$a"
  REPLICA_CMD=(python3 -m bench.custlat replica "${!oa}" "${!ob}" --in-run "$REPLICA_PAIR" --expect-digest "${!da}")
}

runs_line=""
for x in "${LABELS[@]}"; do nm="RUN_NAME_$x" tg="SNOWPLOW_TAG_$x"; runs_line="${runs_line} ${x}=${!nm} (${!tg})"; done
log "runs:${runs_line}; pairs (baseline:candidate) ${PAIR_LIST[*]}; bench ${BENCH_SHA}, ${BASH} ${BASH_VERSION}"
for x in "${LABELS[@]}"; do
  runenv "$x"
  rm -f "$PAIR_OUT/dry-$x/footprint.json"   # #1171: only THIS dry pass's derivation counts
  if ! env "${RUNENV[@]}" BENCH_606_DRY_RUN=1 OUT="$PAIR_OUT/dry-$x" "$BASH" "$RUN606" \
       > "$PAIR_OUT/dry-$x.console" 2>&1; then
    tail -5 "$PAIR_OUT/dry-$x.console" >&2
    refuse "run $x's dry pass was refused (${PAIR_OUT}/dry-$x.console): nothing was created"
  fi
  log "run $x: dry pass OK"
done
# What every cluster needs, together (each gke-up alone would see room for
# itself): ONE read for all of them, reads only, nothing is created, so the DRY
# pass runs it too and refuses a launch that cannot start. #1171: each run's
# dry pass derived its footprint from the spec its gke-up creates and the
# LoadBalancer Services its install creates ($OUT/footprint.json); all of them
# together must fit every regional quota they consume (bench/gke.py
# RUN_QUOTAS): the PVC disks (#895), GKE's per-location cluster limit (#985,
# read), the static addresses (#1026), the in-use addresses every node and
# LoadBalancer holds (run 21 attempt 2 failed 30 min in on IN_USE_ADDRESSES),
# vCPUs, instances, disks, forwarding rules.
quota_args=()
[[ -n "${PROJECT:-}" ]] && quota_args+=(--project "$PROJECT")
[[ -n "${ZONE:-}" ]] && quota_args+=(--zone "$ZONE")
for x in "${LABELS[@]}"; do
  [[ -s "$PAIR_OUT/dry-$x/footprint.json" ]] \
    || refuse "run $x's dry pass did not derive its LoadBalancer Services and cluster footprint ($PAIR_OUT/dry-$x/footprint.json): whether the runs fit the region's quotas is unknown; nothing was created"
  quota_args+=(--footprint "$PAIR_OUT/dry-$x/footprint.json")
done
if ! quota_msg="$(python3 -m bench.gke gke-quotas "${quota_args[@]}" 2>&1)"; then
  refuse "${quota_msg//$'\n'/$'\n'refusing: }"
fi
while IFS= read -r ln; do
  if [[ -n "$ln" ]]; then log "$ln"; fi
done <<< "$quota_msg"
if [[ "$DRY" == "1" ]]; then
  log "DRY: all ${#LABELS[@]} runs would start now; then, per pair:"
  if [[ -n "$REPLICA_PAIR" ]]; then
    replica_cmd; log "DRY:   ${REPLICA_CMD[*]} --completed <0|1> <0|1> > $REPLICA_FILE (the A/A replica, first)"
  fi
  for p in "${PAIR_LIST[@]}"; do
    is_replica "$p" && continue
    pair_cmd "$p"; log "DRY:   ${PAIR_CMD[*]} > $(verdict_file "$p")"
  done
  exit 0
fi

pj_args=("$PAIR_OUT/pair.json" "$BENCH_SHA")
for x in "${LABELS[@]}"; do
  nm="RUN_NAME_$x" tg="SNOWPLOW_TAG_$x" dg="EXPECTED_IMAGE_DIGEST_$x" out="OUT_$x"
  pj_args+=("$x" "${!nm}" "${!tg}" "${!dg}" "${!out}")
done
pj_args+=(-- "${REPLICA_PAIR:--}" "${PAIR_LIST[@]}")
python3 -c 'import json, sys, time
out, sha, rest = sys.argv[1], sys.argv[2], sys.argv[3:]
cut = rest.index("--")
fields, replica, pairs = rest[:cut], rest[cut + 1], rest[cut + 2:]
keys = ("run_name", "tag", "digest", "out")
runs = {fields[i]: dict(zip(keys, fields[i + 1:i + 5])) for i in range(0, len(fields), 5)}
doc = {"kind": "pair", "bench_sha": sha, "started": time.time(), "runs": runs,
       "pairs": [dict(zip(("baseline", "candidate"), p.split(":"))) for p in pairs]}
if replica != "-":
    doc["replica_pair"] = dict(zip(("first", "second"), replica.split(":")))
json.dump(doc, open(out, "w"), indent=1)' "${pj_args[@]}"
# #1194: a launch that cannot publish its own record spends nothing
if ! msg="$(publish_pair started 2>&1)"; then
  refuse "the pair's evidence could not be published: ${msg}: nothing was created"
fi
log "$msg"
for x in "${LABELS[@]}"; do printf -v "PID_$x" '%s' ""; printf -v "RC_$x" '%s' ""; printf -v "DONE_$x" '%s' 0
                           printf -v "FAILED_$x" '%s' 0; printf -v "STOPPED_$x" '%s' 0; done
stop_all() {
  local x pid
  for x in "${LABELS[@]}"; do pid="PID_$x"; [[ -z "${!pid}" ]] || kill -TERM "${!pid}" 2>/dev/null || true; done
}
# a signal stops the runs, not the record: each run publishes its own in its teardown
trap 'log "signal: stopping every run (each tears its own cluster down)"; stop_all; wait
      msg="$(publish_pair interrupted 2>&1)" || log "EVIDENCE NOT SAFE (pair): ${msg}"; exit 130' INT TERM HUP
# ONE simple command each, backgrounded: env execs bash, so $! is run606's shell
for x in "${LABELS[@]}"; do
  runenv "$x"
  env "${RUNENV[@]}" "$BASH" "$RUN606" > "$PAIR_OUT/$x.console" 2>&1 &
  printf -v "PID_$x" '%s' "$!"
  pid="PID_$x"; log "run $x pid ${!pid}"
done
completed() { local out="OUT_$1"; grep -q "run complete: " "${!out}/run.log" 2>/dev/null; }
# intact <pair>: neither of its runs failed as tooling
intact() { local fa="FAILED_${1%%:*}" fb="FAILED_${1##*:}"; [[ "${!fa}" != "1" && "${!fb}" != "1" ]]; }
# compared <label>: some pair the run is in has lost no run (its cluster still feeds a comparison);
# the A/A replica feeds one only while a pair it calibrates is intact
compared() {
  local p q
  for p in "${PAIR_LIST[@]}"; do
    in_pair "$1" "$p" || continue
    intact "$p" || continue
    is_replica "$p" || return 0
    for q in "${PAIR_LIST[@]}"; do
      if ! is_replica "$q" && intact "$q"; then return 0; fi
    done
  done
  return 1
}
all_exited() { local x rc; for x in "${LABELS[@]}"; do rc="RC_$x"; [[ -n "${!rc}" ]] || return 1; done; return 0; }
# bash 3.2 (macOS) has no `wait -n`: poll; a run that FAILED as tooling stops each run left in no comparable pair
while ! all_exited; do
  for x in "${LABELS[@]}"; do
    pid="PID_$x" rc="RC_$x"
    if [[ -z "${!rc}" ]] && ! kill -0 "${!pid}" 2>/dev/null; then
      set +e; wait "${!pid}"; printf -v "$rc" '%s' "$?"; set -e
      if completed "$x"; then printf -v "DONE_$x" '%s' 1; fi
      done_x="DONE_$x"
      log "run $x exited ${!rc} ($([[ "${!done_x}" == "1" ]] && echo 'its evaluate step ran' || echo 'it did not complete'))"
      if [[ "${!rc}" != "0" && "${!done_x}" != "1" ]]; then
        printf -v "FAILED_$x" '%s' 1
        for y in "${LABELS[@]}"; do
          orc="RC_$y" ost="STOPPED_$y" opid="PID_$y"
          [[ -z "${!orc}" && "${!ost}" != "1" ]] || continue
          compared "$y" && continue
          # a PAIR STOP (USR1), not a TERM: its measurement stops and its teardown, diagnostics
          # and disk backstop run in full (run 17 attempt 4: the TERM reached B's teardown)
          log "run $x failed as tooling: stopping run $y (every pair it is in has lost a run, so its cluster feeds no comparison): its measurement stops, its teardown runs in full"
          printf -v "$ost" '%s' 1
          kill -USR1 "${!opid}" 2>/dev/null || true
        done
      fi
    fi
  done
  all_exited || sleep "${PAIR_POLL_S:-5}"
done
trap - INT TERM HUP
# one paired verdict per pair; the launch's exit ranks them (header): FAIL, INVALID, INCONCLUSIVE, PASS
rank() { case "$1" in 1) echo 4 ;; 4) echo 2 ;; 0) echo 1 ;; *) echo 3 ;; esac; }
vrc=0
required=()
# the A/A replica first: every other pair is judged against it (REPLICA_PAIR)
if [[ -n "$REPLICA_PAIR" ]]; then
  ra="${REPLICA_PAIR%%:*}" rb="${REPLICA_PAIR##*:}"
  replica_cmd
  da="DONE_$ra" db="DONE_$rb"
  oa="OUT_$ra" ob="OUT_$rb"
  if "${REPLICA_CMD[@]}" --completed "${!da}" "${!db}" > "$REPLICA_FILE"; then
    required+=(--require "${REPLICA_FILE##*/}")
  else
    # a crash of the replica's own tool: the file is removed, and each pair's `custlat pair
    # --replica` then cannot read it, so every other pair is INVALID with that reason
    log "the A/A replica ${REPLICA_PAIR} could not be computed (exit $?): every other pair will be INVALID"
    rm -f "$REPLICA_FILE"
  fi
  while IFS= read -r r; do
    [[ -n "$r" ]] && log "replica ${REPLICA_PAIR}: $r"
  done < <(python3 -c 'import json, sys
from bench import custlat
try:
    ir = json.load(open(sys.argv[1])).get("in_run") or {}
except (OSError, ValueError):
    ir = {}
out = [ir.get("line") or "", *("NOT MEASURED: " + w for w in ir.get("why") or []),
       *("NOTE: " + n for n in ir.get("notes") or [])]
for lab, d in ((sys.argv[2], sys.argv[3]), (sys.argv[4], sys.argv[5])):
    try:
        rv = json.load(open(d + "/verdict.json"))
    except (OSError, ValueError):
        rv = None
    for sec, missing in custlat.INFO_SECTIONS:
        out += custlat.arm_info_lines(rv, sec, missing, lab + " (replica arm)")[1]
print("\n".join(out))' "$REPLICA_FILE" "$ra" "${!oa}" "$rb" "${!ob}" 2>/dev/null || true)
fi
for p in "${PAIR_LIST[@]}"; do
  is_replica "$p" && continue
  a="${p%%:*}" b="${p##*:}" f="$(verdict_file "$p")"
  pair_cmd "$p"
  da="DONE_$a" db="DONE_$b"
  prc=0
  "${PAIR_CMD[@]}" --completed-a "${!da}" --completed-b "${!db}" > "$f" || prc=$?
  required+=(--require "${f##*/}")
  log "paired verdict ${a} vs ${b} exit=${prc} (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE) -> $f"
  while IFS= read -r r; do
    [[ -n "$r" ]] && log "${a} vs ${b}: $r"
  done < <(python3 -c 'import json, sys; v = json.load(open(sys.argv[1]))
ex = (v.get("exclusion") or {}).get("items") or []
print("\n".join(["FAIL: " + r for r in v.get("fail_reasons") or [] if ".candidate_" in r]
                + ["INVALID: " + r for r in v["invalid_reasons"]]
                + [((v.get("replica_source") or {}).get("line") or "INFO replica: not said (a verdict before run 24)")]
                + ["NOTE: " + n for n in v.get("notes") or []]
                + ["EXCLUDED from both runs: %s (A %.0f s, B %.0f s): %s" % (i["what"], sum(b - a for a, b in i["a"]),
                   sum(b - a for a, b in i["b"]), i["why"]) for i in ex]
                + [c["line"] for c in v["cells"]] + (v.get("a_healthy_lines") or [])
                + [r["line"] for r in v.get("b_inside_a_outage") or []]
                + ((v.get("refresher_tiers_report_only") or {}).get("lines") or [])
                + [ln for k in ("tracing_report_only", "egress_connections_report_only", "nodes_report_only",
                                "go_runtime_report_only", "user_buckets_report_only")
                   for ln in (v.get(k) or {}).get("lines") or []]))' \
             "$f" 2>/dev/null || true)
  (( $(rank "$prc") <= $(rank "$vrc") )) || vrc=$(( prc == 1 || prc == 4 || prc == 0 ? prc : 3 ))
done
# #1194: the launch is finished when its evidence is safe, whatever the verdicts
safe=1
if msg="$(publish_pair final "${required[@]}" 2>&1)"; then
  log "$msg"
else
  safe=0
  log "EVIDENCE NOT SAFE (pair): ${msg}"
fi
for x in "${LABELS[@]}"; do
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
  log "the launch is NOT finished: its evidence is not safe (exit 5); the paired verdicts, exit ${vrc} together, stand in $PAIR_OUT"
  exit 5
fi
exit "$vrc"
