#!/bin/bash
# check_rawjson_readers.sh — #854 CI guard: every direct read of
# ResolvedEntry.RawJSON is a known, classified site.
#
# THE DEFECT CLASS. An incremental list content cell (internal/cache/
# list_cell.go) stores NO RawJSON: its envelope is built on demand from the
# indexer's own per-object bytes. A reader that takes entry.RawJSON directly from
# such an entry silently sees an empty body. Readers that can meet an apistage
# content entry must go through entry.Raw() / entry.RawLen(). A NEW direct read
# is therefore a decision, not a default: this guard fails CI on any read beyond
# the classified sites below, so whoever adds one stops and classifies it.
#
# THE ALLOWLIST (file → number of direct `.RawJSON` reads, writes excluded):
#   - internal/cache/list_cell.go                      2  Raw / RawLen themselves (plain branch)
#   - internal/cache/resolved.go                       2  entryBytes, plain branch (incremental handled first)
#   - internal/cache/resolved_entry_items.go           2  EnsureItems parse fallback (incremental handled first)
#   - internal/cache/ra_full_list_layers.go            1  raFullList class (never incremental)
#   - internal/resolvers/restactions/api/apistage.go   1  content HIT: plain entry (incremental view handled next)
#   - internal/resolvers/widgets/apiref/ra_full_list.go 1 raFullList class
#   - internal/handlers/dispatchers/restactions.go     2  restactions class (never incremental)
#   - internal/handlers/dispatchers/widgets.go         3  widgets class (never incremental)
#
# Run it locally before pushing:
#
#   bash scripts/check_rawjson_readers.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

declare -A allow=(
  [internal/cache/list_cell.go]=2
  [internal/cache/resolved.go]=2
  [internal/cache/resolved_entry_items.go]=2
  [internal/cache/ra_full_list_layers.go]=1
  [internal/resolvers/restactions/api/apistage.go]=1
  [internal/resolvers/widgets/apiref/ra_full_list.go]=1
  [internal/handlers/dispatchers/restactions.go]=2
  [internal/handlers/dispatchers/widgets.go]=3
)

# Direct reads: `.RawJSON` as a whole word, in non-test Go files, not in a line
# comment, and not a write (`RawJSON:` in a literal, `.RawJSON =`).
reads=$(grep -rnE '\.RawJSON\b' --include='*.go' internal \
  | grep -v '_test\.go:' \
  | grep -vE '^[^:]*:[0-9]+:[[:space:]]*//' \
  | grep -vE '\.RawJSON[[:space:]]*=[^=]' \
  | awk -F: '{print $1}' | sort | uniq -c || true)

fail=0
while read -r count file; do
  [ -z "${file:-}" ] && continue
  want=${allow[$file]:-0}
  if [ "$count" -gt "$want" ]; then
    echo "ERROR: $file reads ResolvedEntry.RawJSON directly $count time(s); $want classified." >&2
    fail=1
  fi
done <<< "$reads"

if [ "$fail" -ne 0 ]; then
  cat >&2 <<'EOF'

       An incremental list content cell (#854) stores no RawJSON. Read the body
       through entry.Raw() (or its size through entry.RawLen()), or — for a cell
       class that can never be incremental — classify the site in this script's
       allowlist with the reason.
EOF
  exit 1
fi
echo "OK: every direct ResolvedEntry.RawJSON read is classified (#854)."
