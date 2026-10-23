# #854 evidence bundle

Branch under test: `feat/854-incremental-list-cell` @ `aec5f1fb4273da4b580dc9199733012e94621420`,
rebased onto `main` @ `f7c4578cf87f13da0018e9a376d03e33db26b769`. The #865 review fixes are four commits
on top of the reviewed commit (now `b8ccc903` after the rebase).

Fetch: `git fetch origin feat/854-incremental-list-cell && git checkout FETCH_HEAD`

## customer865/ — the binding customer arm on the REAL refresher (#865 review, RUN)

**Harness:** `internal/handlers/dispatchers/paired_865_test.go` (committed, build tag `paired854`; byte-identical on both trees; main gets the stub `p865_stats_main_stub.go.txt`). Everything a request touches is production code:
- customers send `GET /call` through `restActionHandler.ServeHTTP`;
- the refresher is `cache.StartRefresher` with production settings and main.go's `SetCustomerInflightHook(CustomerInFlight)`, so the yield is `refresher.yieldToCustomer` itself;
- the refresh function is the production closure, `resolveAndPopulateL1`.

**Setup:**
- 50K-object list; 20 dependent RESTActions plus 1 unrelated one;
- GOMAXPROCS=4: the chart's CPU limit, and equal to the refresher's 4 workers;
- 5 rounds of (main, branch, main2), 30 s each.

**Provenance:**
- The binaries were built from branch `29251913` (the harness commit, before the rebase) and main `5575c592`.
- The rebase onto `f7c4578c` brought in only the widget 424 dependency-status path (#853/#868), which this arm (RESTAction `/call` only) does not execute.

**Arms:**
- `A/`: normal load. 1/10/50 ev/s; 2 customers on dependents plus 4 on the unrelated RESTAction; 50 ms think time. Analysis: `cust865-A-analysis.txt`; GC, allocation and refreshes per leg: `gc-A.txt`.
- `B/`: sustained load. 10/50 ev/s; 8 + 4 customers; 0 ms think time, so a request is always in flight and the yield runs to its 5 s cap. Analysis: `cust865-B-analysis.txt`, `gc-B.txt`.
- `controls/`: at 1 ev/s, the arm-A load plus two controls.
  - `hit_bench`: 3,000 sequential L1 hits right after warm-up, with nothing else running.
  - `probe`: a fixed CPU-only computation, with the customers' think time, that touches no snowplow code.
  - Analysis: `probe865-analysis.txt`.

**Other files:**
- `comparator-and-read-cost-50k.txt`: what a 50K-object cell's comparator pass and its reads cost (`internal/cache/list_cell_854_cost_test.go`, run with `LISTCELL854_COST=1`).

## gate/ (RUN on aec5f1fb)
- `gate854.sh`: `go test ./...`, then the CI invocation
  `go test -race -tags=unit,integration -p 1 ./...`, then every `scripts/check_*.sh`.
  Summaries: `g854-all.summary.txt`, `g854-race-ci.summary.txt`, `g854-checks.log`.
  `g854-head.log` records the SHA and a clean status.
- `arms854.sh` / `arms854.log`: every #854 arm (37) x3, under -race with the CI tags.
- `mut854-final.log`: all 35 logic-only mutations, run on aec5f1fb
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
