#!/bin/bash
# check_rbac_publish_slot.sh — #1206 CI guard: the RBAC snapshot is only ever
# published through the writer slot.
#
# EvaluateRBAC, the identity class in every cache key and the #424 re-check after
# a resolve all rely on the snapshot only moving forward. A Store outside the slot
# can land after a rebuild that read later, so older content goes live and a
# revoke is undone until the next rebuild (#687, #1206).
#
# The AST checker in scripts/checkrbacpublish walks the writer chain and fails on
# any reference that bypasses it. See its package doc for the rules.
#
# Two steps, in this order, because a guard that has stopped seeing anything looks
# exactly like a clean tree:
#   1. --self-test: the known-good chain passes and every defect is caught;
#   2. the scan, which also requires the chain to be present.
#
# Run it locally before pushing:
#
#   bash scripts/check_rbac_publish_slot.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

go run ./scripts/checkrbacpublish --self-test
go run ./scripts/checkrbacpublish .
