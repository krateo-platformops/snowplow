# #854 evidence bundle

Branch under test: `feat/854-incremental-list-cell` @ `d0b9059572b94728ed0d0fa21420c0e666ffcdb0`,
rebased onto `main` @ `5575c592e4685e6e3dc9005c0136a2642a732bdd`.

Fetch: `git fetch origin feat/854-incremental-list-cell && git checkout FETCH_HEAD`

## gate/ (RUN on d0b90595)
- `gate854.sh`: `go test ./...`, then the CI invocation
  `go test -race -tags=unit,integration -p 1 ./...`, then every `scripts/check_*.sh`.
  Summaries: `g854-all.summary.txt`, `g854-race-ci.summary.txt`, `g854-checks.log`.
  `g854-head.log` records the SHA and a clean status.
- `arms854.sh` / `arms854.log`: every #854 arm (30) x3, under -race with the CI tags.
- `mut854-final.log`: all 28 logic-only mutations, rerun on d0b90595
  (`mutate854.py`; `mut_shape.sh` is the earlier standalone run of M24b-M27).

## customer/: the customer arm, the BINDING question (RUN)
Same harness as the throughput arm (`paired_854_test.go`, committed, build tag `paired854`), extended with:
- **Customers:** customer ctx, closed loop, 50 ms think time.
  - 2 heavy customers read the dependents' 50K list and run the dashboard jq.
  - 4 light customers read an UNRELATED widget: a 200-object list in another namespace that no event touches, plus the same jq.
- **dirty_to_fresh:** per dependent, measured from the mark to the completed re-resolve.
- **The refresher's customer-priority yield, modelled as production runs it** (`refresher.go` `yieldToCustomer`, installed unconditionally in `main.go`):
  - a worker parks after its dequeue while any customer request is in flight;
  - it re-checks every 25 ms, for at most 5 s;
  - a re-resolve already running is not interrupted.

Setup:
- GOMAXPROCS=4: the chart's CPU limit (helm/snowplow/values.yaml), which Go turns into GOMAXPROCS, and equal to the refresher's 4 workers. The live 057 limit was NOT read.
- 50K objects, 1/10/50 events/s, 3 rounds of (main, branch, main2), 30 s per run.
- The main/main spread is main2/main.

Files:
- `bin-main` is 5575c592 plus the byte-identical harness; `bin-branch` is d0b90595 (`build.sh`).
- `runs/`: per-run JSON, CPU profiles with labels (`dep`, `cust`, `apistage_fill_*`), `load.log`.
- `cust-v2-analysis.txt` (`cust_analyze.py`) and `cust-v2-yield.txt` (`cust_yield.py`: parked worker-seconds and 5 s cap hits).
- `noyield-runs/` and `noyield-v1-analysis.txt` (`noyield.sh`): SUPPLEMENTARY.
  - The same arm with the yield removed (scratch variant `paired_854_test.go.noyield`, never committed), at 10 ev/s, 3 rounds.
  - It shows what the existing yield is doing for customers on this branch.

## abab/: the throughput arm (RUN on 42aeb8a1, before the rebase; base 525b4ddf)
- Per-run JSON, CPU profiles, load log, `analysis.txt`, `percompletion.txt`.
- `runs-v1-54a9bb38/` is the first ABAB, kept for comparison.
