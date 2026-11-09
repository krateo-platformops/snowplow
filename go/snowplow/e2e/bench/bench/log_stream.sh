#!/usr/bin/env bash
# #822: snowplow's log, streamed for the WHOLE run (env -> collect) into $OUT/snowplow-stream.log.
# A one-shot `kubectl logs` at collect returns only the container's current file (run 9: the last
# ~30 s). The stream follows across dropped connections (resuming at the newest kubelet timestamp
# seen; `--since-time` is inclusive, and bench.logscrub drops the resent lines) and across a REPLACED
# pod (it follows the new pod by name, and first reads the old pod's tail while that is still
# readable; a tail it could not read is marked). Every line passes bench.logscrub: metadata only (no
# body, username or token reaches $OUT).
#
# #1297: it runs IN-CLUSTER, in the observer pod on the loadgen node (bench/incluster.py), no longer
# on the operator's laptop: the same loop run606.sh ran, moved here unchanged. Its environment:
#   OUT                     where it writes (the observer's /out)
#   PW, BENCH_USER_PREFIX   what the scrubber denies (the bench users' password, their name prefix)
#   SNOWPLOW_POD_SELECTOR   snowplow's pods
#   LOG_STREAM_RETRY_S      the pause before a reconnect
#   KUBECTL                 the kubectl command (default: kubectl, the pod's own ServiceAccount)
# It stops when $OUT/.log-stream-stop exists; the observer creates that file, then stops the loop's
# whole process group (bench/incluster.py Segment.halt), so no command of the loop outlives it.
set +e
: "${OUT:?}" "${PW:?}" "${BENCH_USER_PREFIX:?}" "${SNOWPLOW_POD_SELECTOR:?}" "${LOG_STREAM_RETRY_S:=5}"
read -r -a K <<< "${KUBECTL:-kubectl}"
S="$OUT/.log-stream"
rm -f "$S-stop" "$S-state" "$S-last"
scrub() { python3 -m bench.logscrub --deny "$PW" --deny "$BENCH_USER_PREFIX" "$@"; }
# snowplow_live_pod: the newest snowplow pod not being deleted (empty if none).
snowplow_live_pod() {
  "${K[@]}" --request-timeout=30s -n krateo-system get pods -l "$SNOWPLOW_POD_SELECTOR" \
      -o jsonpath='{range .items[*]}{.metadata.creationTimestamp}{" "}{.metadata.name}{" "}{.metadata.deletionTimestamp}{"\n"}{end}' \
      2>/dev/null | awk 'NF == 2' | sort | tail -1 | cut -d' ' -f2
}
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
done
