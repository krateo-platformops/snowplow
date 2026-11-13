#!/usr/bin/env bash
# run606_stats.sh: #1390, the pair statistics of one launch: the in-run A/A replica first (REPLICA_PAIR), then one
# paired verdict per other pair, each through `python3 -m bench.custlat` exactly as before, each a stat_step
# (its start, progress and end lines in the pair log, its duration in $PAIR_OUT/statistics.jsonl).
#
# Every input is a run directory read from the launch's DURABLE copy (bench/durable.py: a fresh download from the
# evidence bucket, verified file by file against its manifest), never the VM's own disk: run 26e's VM deleted
# itself, with every run directory, while its replica still ran.
#
# Two callers, one code path:
#   - run606_pair.sh sources this file (its helpers, for the dry plan too) and calls stats_main once every run
#     has exited and each one's durable copy is downloaded;
#   - `python3 -m bench.durable rejudge` runs it on its own, offline, from a fresh download of the same copy:
#     a lost VM loses no verdict.
#
# In: PAIR_OUT, PAIR_RUN_ID, REPLICA_PAIR (may be empty), REPLICA (the committed replica file, used when there
# is no REPLICA_PAIR), the labels (LABELS array, or STATS_LABELS="L1 L2 ..."), the pairs (PAIR_LIST array, or
# PAIRS="A:B ..."), and per label SRC_<L> (its verified durable copy; empty when it has none),
# EXPECTED_IMAGE_DIGEST_<L> and DONE_<L> (1 when its evaluate step ran).
# Out: replica-<L1>-<L2>.json and paired-verdict-<A>-<B>.json in PAIR_OUT; STATS_VRC, the verdicts ranked (FAIL 1,
# INVALID 3, INCONCLUSIVE 4, PASS 0); STATS_REQUIRED, the files the pair's evidence must hold. On its own it exits
# STATS_VRC.

# verdict_file <pair>: that pair's paired verdict
verdict_file() { echo "$PAIR_OUT/paired-verdict-${1%%:*}-${1##*:}.json"; }
# is_replica <pair>: the pair is the launch's declared A/A replica
is_replica() { [[ -n "${REPLICA_PAIR:-}" && "$1" == "$REPLICA_PAIR" ]]; }
# pair_cmd <pair>: PAIR_CMD = its custlat pair, the baseline as A, the candidate as B, read from their durable copies
pair_cmd() {
  local a="${1%%:*}" b="${1##*:}"
  local sa="SRC_$a" sb="SRC_$b" da="EXPECTED_IMAGE_DIGEST_$a" db="EXPECTED_IMAGE_DIGEST_$b"
  PAIR_CMD=(python3 -m bench.custlat pair --a "${!sa}/verdict.json" --b "${!sb}/verdict.json"
            --a-dir "${!sa}" --b-dir "${!sb}" --replica "$REPLICA" --expect-a "${!da}" --expect-b "${!db}"
            --a-label "$a" --b-label "$b" --launch "$PAIR_RUN_ID")
}
# replica_cmd: REPLICA_CMD = the declared A/A pair's in-run replica (custlat replica --in-run), from the durable
# copies, recording this launch's id: a pair says "measured in this launch" only on its own id
replica_cmd() {
  local a="${REPLICA_PAIR%%:*}" b="${REPLICA_PAIR##*:}"
  local sa="SRC_$a" sb="SRC_$b" da="EXPECTED_IMAGE_DIGEST_$a"
  REPLICA_CMD=(python3 -m bench.custlat replica "${!sa}" "${!sb}" --in-run "$REPLICA_PAIR" --expect-digest "${!da}"
               --launch "$PAIR_RUN_ID")
}
# stat_step <step> <out> <command> <dir 1> <dir 2> -- <cmd...>: one statistics step. Its start and end lines in
# the pair log, with the input it reads; the command's own progress lines (custlat.progress, its stderr) between
# them, each prefixed with the step; its stdout to <out>; its duration and input recorded in
# $PAIR_OUT/statistics.jsonl (bench/statstime.py: what a later launch's TTL budget is measured from). Returns the
# command's exit code.
stat_step() {
  local step="$1" out="$2" cmd="$3" d1="$4" d2="$5" t0 t1 rc bytes
  shift 6
  bytes="$(python3 -m bench.statstime input-bytes "$d1" "$d2" 2>/dev/null)" || bytes="unknown"
  t0="$(date +%s)"
  log "statistics: ${step} started (${bytes} bytes of k6 and browser streams)"
  set +e
  "$@" 2>&1 >"$out" | while IFS= read -r ln; do log "statistics: ${step}: ${ln}"; done
  rc="${PIPESTATUS[0]}"
  set -e
  t1="$(date +%s)"
  log "statistics: ${step} ended exit=${rc} after $(( t1 - t0 )) s"
  python3 -m bench.statstime record --file "$PAIR_OUT/statistics.jsonl" --step "$step" --command "$cmd" \
      --started "$t0" --ended "$t1" --exit "$rc" "$d1" "$d2" >/dev/null \
    || log "statistics: ${step}: its duration could not be recorded in $PAIR_OUT/statistics.jsonl"
  return "$rc"
}
# not_durable <pair> <label>...: the pair's verdict, INVALID, when a run of it has no verified durable copy
not_durable() {
  local f="$1"; shift
  python3 -c 'import json, sys
print(json.dumps({"kind": "customer_latency_pair", "verdict": "INVALID", "checks": [], "cells": [],
                  "invalid_reasons": ["run %s has no verified durable copy (#1390): its statistics are not computed "
                                      "from a run directory that exists only on the orchestrator VM" % r
                                      for r in sys.argv[1:]]}, indent=1))' "$@" > "$f"
}
# the verdicts ranked into the launch's exit (header of run606_pair.sh): FAIL, INVALID, INCONCLUSIVE, PASS
rank() { case "$1" in 1) echo 4 ;; 4) echo 2 ;; 0) echo 1 ;; *) echo 3 ;; esac; }

stats_main() {
  local ra rb da db sa sb p a b f prc missing r
  STATS_VRC=0
  STATS_REQUIRED=()
  # the A/A replica first: every other pair is judged against it (REPLICA_PAIR)
  if [[ -n "${REPLICA_PAIR:-}" ]]; then
    ra="${REPLICA_PAIR%%:*}" rb="${REPLICA_PAIR##*:}"
    replica_cmd
    da="DONE_$ra" db="DONE_$rb"
    sa="SRC_$ra" sb="SRC_$rb"
    if [[ -z "${!sa:-}" || -z "${!sb:-}" ]]; then
      log "the A/A replica ${REPLICA_PAIR} is not computed: a run of it has no verified durable copy; every other pair will be INVALID"
      rm -f "$REPLICA_FILE"
    elif stat_step "replica ${REPLICA_PAIR}" "$REPLICA_FILE" replica "${!sa}" "${!sb}" -- \
         "${REPLICA_CMD[@]}" --completed "${!da}" "${!db}"; then
      STATS_REQUIRED+=(--require "${REPLICA_FILE##*/}")
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
print("\n".join(out))' "$REPLICA_FILE" "$ra" "${!sa:-/nonexistent}" "$rb" "${!sb:-/nonexistent}" 2>/dev/null || true)
  fi
  for p in "${PAIR_LIST[@]}"; do
    is_replica "$p" && continue
    a="${p%%:*}" b="${p##*:}" f="$(verdict_file "$p")"
    pair_cmd "$p"
    da="DONE_$a" db="DONE_$b"
    sa="SRC_$a" sb="SRC_$b"
    prc=0
    missing=()
    [[ -n "${!sa:-}" ]] || missing+=("$a")
    [[ -n "${!sb:-}" ]] || missing+=("$b")
    if (( ${#missing[@]} > 0 )); then
      not_durable "$f" "${missing[@]}"
      prc=3
    else
      stat_step "pair ${a}:${b}" "$f" pair "${!sa}" "${!sb}" -- \
          "${PAIR_CMD[@]}" --completed-a "${!da}" --completed-b "${!db}" || prc=$?
    fi
    STATS_REQUIRED+=(--require "${f##*/}")
    log "paired verdict ${a} vs ${b} exit=${prc} (0 PASS, 1 FAIL, 3 INVALID, 4 INCONCLUSIVE) -> $f"
    while IFS= read -r r; do
      [[ -n "$r" ]] && log "${a} vs ${b}: $r"
    done < <(python3 -c 'import json, sys; v = json.load(open(sys.argv[1]))
ex = (v.get("exclusion") or {}).get("items") or []
print("\n".join(["FAIL: " + r for r in v.get("fail_reasons") or [] if ".candidate_" in r]
                + ["INVALID: " + r for r in v["invalid_reasons"]]
                + [((v.get("replica_source") or {}).get("line") or "INFO replica: not said (a verdict before run 24)")]
                + ([v["frontend"]["line"]] if v.get("frontend") else [])
                + ["NOTE: " + n for n in v.get("notes") or []]
                + ["EXCLUDED from both runs: %s (A %.0f s, B %.0f s): %s" % (i["what"], sum(b - a for a, b in i["a"]),
                   sum(b - a for a, b in i["b"]), i["why"]) for i in ex]
                + [c["line"] for c in v["cells"]] + (v.get("a_healthy_lines") or [])
                + [r["line"] for r in v.get("b_inside_a_outage") or []]
                + ((v.get("refresher_tiers_report_only") or {}).get("lines") or [])
                + [ln for k in ("tracing_report_only", "egress_connections_report_only", "nodes_report_only",
                                "go_runtime_report_only", "user_buckets_report_only", "resolve_memo_report_only")
                   for ln in (v.get(k) or {}).get("lines") or []]))' \
               "$f" 2>/dev/null || true)
    (( $(rank "$prc") <= $(rank "$STATS_VRC") )) || STATS_VRC=$(( prc == 1 || prc == 4 || prc == 0 ? prc : 3 ))
  done
}

# on its own (bench.durable rejudge): the arrays from their strings, the pair log, then the statistics
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -Eeuo pipefail
  HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  cd "$HERE"
  : "${PAIR_OUT:?}" "${PAIR_RUN_ID:?}"
  read -r -a LABELS <<< "${STATS_LABELS:?}"
  read -r -a PAIR_LIST <<< "${PAIRS:?}"
  REPLICA_PAIR="${REPLICA_PAIR:-}"
  REPLICA="${REPLICA:-$HERE/customer_latency_replica_runs12_13.json}"
  REPLICA_FILE=""
  if [[ -n "$REPLICA_PAIR" ]]; then
    REPLICA_FILE="$PAIR_OUT/replica-${REPLICA_PAIR%%:*}-${REPLICA_PAIR##*:}.json"
    REPLICA="$REPLICA_FILE"
  fi
  mkdir -p "$PAIR_OUT"
  log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$PAIR_OUT/pair.log" >&2; }
  log "statistics of launch ${PAIR_RUN_ID}, from its durable copy: runs ${LABELS[*]}; pairs ${PAIR_LIST[*]}${REPLICA_PAIR:+; replica $REPLICA_PAIR}"
  stats_main
  exit "$STATS_VRC"
fi
