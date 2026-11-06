#!/bin/bash
# check_request_log_context.sh — #1184 CI guard: a WARN or ERROR keeps the
# context of the work that emitted it.
#
# THE DEFECT CLASS. traceCorrelationHandler stamps trace_id/span_id from the
# record's context, and a plain log.Warn / log.Error reaches it with
# context.Background(). Before #1184 that was every WARN and ERROR snowplow
# wrote: 0 of 96,315 live rows had a trace_id, with the stamping itself working.
#
# The typed checker in scripts/checkrequestlogctx enforces, over every non-test
# Go file outside scripts/, e2e/ and hack/:
#
#   - a site with a context in scope passes it (a context.Context, an
#     *http.Request, or a struct holding a context), and a literal
#     context.Background() or context.TODO() does not count as passing it;
#   - a site with no context in scope either threads one or carries a
#     classified `// log-ctx-waiver: <class>: <reason>` on the line above.
#     A new plain site is therefore a decision, not a default, and a waiver with
#     no site, or on a site that has a context, fails too.
#
# Run it locally before pushing:
#
#   bash scripts/check_request_log_context.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

go run ./scripts/checkrequestlogctx .
