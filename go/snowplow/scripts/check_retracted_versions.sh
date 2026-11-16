#!/bin/bash
# check_retracted_versions.sh — the repo root's retracted-versions.yaml is well
# formed, names only this repo's charts, and points at published releases.
#
# installer's CI reads that file and refuses a pin on a listed version; a
# malformed entry matches nothing there and the pin goes through, so the file is
# checked here, where it is written (scripts/checkretracted has the rules).
#
# Two steps, in this order, because a guard that refuses nothing looks exactly
# like a clean file:
#   1. --self-test: every malformed control is refused, and an unpublished
#      version reads as unpublished;
#   2. the file itself, with every retracted version and every `use` resolved
#      anonymously in the chart registry.
#
# Run it locally before pushing:
#
#   bash scripts/check_retracted_versions.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

go run ./scripts/checkretracted --self-test ../..
go run ./scripts/checkretracted ../..
