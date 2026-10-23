# #854 evidence bundle

Branch under test: `feat/854-incremental-list-cell` @ `240d658340e58dd4476941168eb61596429fc96a`
(tree `ec58d98465c894fa1d8110a5c92b21df366eaff9`). The gate, arms and mutations ran on
`42aeb8a1ca9c42c940d5a1406397f6f3bbfe6d98`, which has the SAME tree. Only the commit message
changed afterwards, to carry the ABAB numbers below. Base: `main` @ `525b4ddf6f0aacdc55820b30b5215a0fdb77374a`.

Fetch: `git fetch origin feat/854-incremental-list-cell && git checkout FETCH_HEAD`

## abab/ — the binding paired arm (RUN)
- `bin-branch` was built from the branch tree with `go test -c -tags paired854` in
  `internal/resolvers/restactions/api`.
- `bin-main` was built from 525b4ddf plus the same `paired_854_test.go`
  (byte-identical, checked with cmp) and a stub `paired_854_stats_test.go` that returns nil.
- `abab_sampled.sh` → `abab.sh`: for each of 1, 10 and 50 events/s, 3 rounds of (main, branch);
  50K objects, 30 s per run.
- `runs/*.json`: per-run metrics. `runs/*.cpu.pb.gz`: CPU profiles with pprof labels
  (`apistage_fill_gvr`, `dep`). `runs/load.log`: CPU of other processes on the host,
  sampled every 10 s.
- `analysis.txt` (`analyze.py`): absolute metrics and paired ratios.
  `percompletion.txt` (`percompletion.py`): CPU and allocation per dependent completion, per round,
  with the background load of each run.
- `runs-v1-54a9bb38/` and `analysis-v1-54a9bb38.txt`: the first ABAB, on the pre-fix squash.
  It is superseded and kept for comparison.

## gate/ (RUN)
- `gate854.sh`: `go test ./...`, then the CI invocation
  `go test -race -tags=unit,integration -p 1 ./...`, then every `scripts/check_*.sh`.
  Summaries: `g854-all.summary.txt`, `g854-race-ci.summary.txt`, `g854-checks.log`.
  `g854-head.log` records the SHA and a clean status.
- `arms854.sh` / `arms854.log`: every #854 arm, x3, under -race with the CI tags.
- `mut854-final.log`: every logic-only mutation, run on the final tree
  (`mutate854.py`; `mut_shape.sh` is the earlier standalone run of M24b-M27).
