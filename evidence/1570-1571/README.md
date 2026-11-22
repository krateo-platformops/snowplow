# Evidence: #1570 (rate floor = the cell's own cost) and #1571 (admission by cores)

Every result here is marked RUN or NOT RUN. Paths are written as `$SCRATCH` (the session scratchpad) and `$HOME`.

## Trees measured

| Arm | What | SHA |
|---|---|---|
| main | origin/main | `1c788e2844407c624494e50d77976dfa80ab755d` |
| floor | `refresher/1570-floor-own-cost` | `9e6427ce37e1223f0810c2de68a88909049fb22e` |
| yield | `refresher/1571-yield-core-admission` | `0b1d3f435de5a0fce054a92c5a4d16ad32cc9639` |
| combined | `evidence/1570-1571-combined` (A+B merge) | `11d45b49d475aa5bf3bba9b2dc207cb5ad47a626` |

- **How each tree was built:** each is a `git archive` of its SHA (`tools/prep_tree.py`).
- **The main tree carries the harness files** (`paired_fresh_1570_test.go` and the shim-calling `paired_865_test.go`) plus `tools/wire_main.go.txt` as its `paired_wire_test.go`. That shim is main.go's own wiring in that tree: `cache.SetCustomerInflightHook(CustomerInFlight)`.

## Falsifiers — RUN

Each mutant was a fresh archive with one exact edit, run once (`tools/falsify.py`).

| File | Unmutated arms | Mutants | Result |
|---|---|---|---|
| `falsifiers/falsify-b.out.json` (ref 9e6427ce's parent c3de267a, same code) | PASS | floor ignored; fixed 2 s floor; unknown cost given a fixed floor; cost not recorded | all KILLED by the arms' assertions (each kill's failing line is in the file) |
| `falsifiers/falsify-a.out.json` (ref 1347abd6, same code as 0b1d3f43 minus the harness commit) | PASS | any customer treated as every core; yield bypassed; wake removed; poll added beside the wake; viewed classified as normal; cap removed | all KILLED |

The new arms passed ×3 under `-race` on both branches (RUN, recorded in the PR bodies).

## Freshness harness — RUN, 2 of 6 rounds

- **Setup:** `TestPairedFresh1570`, tags `paired854,pairedfresh`, GOMAXPROCS=4, nice 10, alternated with rotation (`tools/run_paired.py`).
- **Load gate:** a run starts only when the 1-minute load is < 8. Each record in `fresh-gated/runs.jsonl` carries the load at start, the load during the run (mean and max; these include the run itself) and the wait for the gate.
- **Completed:** 2 rounds × 4 arms. Starts were at load 7.5–7.8. Summary: `fresh-gated/summary.json` (`tools/analyze_fresh.py`).
- **Excluded:** `fresh-excluded-load-over-8/` holds 2 runs started at load 29.5 and 43.6, before the gate existed. They are EXCLUDED from every summary.
- **Stopped:** rounds 3–6 were stopped for the coordinator's quiet hold before any of them started.

## #865 paired-throughput gate — RUN on the build VM, alone

- **Setup:** snowplow-build-1 (32 vCPU) in a VM-exclusive window, `tools/rb_paired.py` with `tools/arms-865.json`. One rbuild job at a time: a fresh checkout of the pushed SHA (main + `overlays/main`), GOTOOLCHAIN=go1.27.3, GOMAXPROCS=4, `PAIRED865_PROBE=2`, `PAIRED865_HITBENCH=2000`.
- **"Alone" proof:** every record lists the rjob jobs present before and after the test. All 30 runs saw only their own label.
- **Runs:** `vm-865-r10/` and `vm-865-r50/`. Each holds 5 arms (main, mainB, floor, yield, combined) × 3 rounds, alternated, plus `summary.json` (`tools/analyze_865.py`: ratios against the same round's main, judged against the mainB/main spread).
- **Result, A (yield):** neutral. Customer p50/p95 0.84–1.03, req/s 0.99–1.01, completions/s 1.06–1.07, CPU unchanged.
- **Result, B (floor) and combined:**
  - customer p50 0.68–0.89 and p95 0.86–0.92, i.e. better;
  - dep staleness p95 −16 to −19%;
  - closed-loop req/s 0.97–0.98: FAIL against the spread;
  - completions/s 0.80× at 10 events/s and 0.44× at 50, with refresher CPU +22–25% (2.7 → 3.4 cores): FAIL;
  - `floored` 0, against main's 3–11K.
- `vm-probe/` is the plumbing check: one main run, not evidence.

## Freshness ×6 on the VM — RUN, alone

- **Setup:** `vm-fresh/`, `tools/arms-fresh.json`, 6 alternated rounds, 24 runs, every one alone.
- **Excluded round:** round 3's combined run failed after 7221 s when the ssh link to the VM dropped (`RUN-FAILED`, rc 255). Round 3 is EXCLUDED for every arm: rounds are complete or excluded.
- **Pooled:** 5 rounds, 100 probe samples per arm and rate (`vm-fresh/summary.json`).

| Arm | Probe push p95 at 1 / 10 / 50 events/s | Viewed waits ≤ 100 ms | Customer p95 vs main |
|---|---|---|---|
| main | 1219 / 204 / 1305 ms | 0.9 | — |
| floor | 250 / 178 / 153 ms | 0.8 / 0.9 / 1.0 | 0.95–1.05 |
| yield | 1006 / 4.7 / 1006 ms | 1.0 | 0.95–1.03 |
| combined | 14 / 3.7 / 3.2 ms | 1.0 | 0.94–0.98 |

- Customer rps is 1.00 for every arm.
- The 2–3× customer p95 seen in 2 laptop rounds does NOT reproduce on the VM.
- The freshness harness does not measure process CPU, so its CPU is reported only by the #865 runs.
- Earlier laptop rounds (`fresh-gated/`) are a different environment and are not pooled with these.

## Completions split by class (#865, 50 events/s, main / floor / combined ×3) — RUN, alone

- **Harness change:** an overlay of `paired_865_test.go` (`overlays/split-main`, `overlays/split-branch`) adds the accepted Puts by class|tier|content, completions without a Put, and the wait and duration of refreshes by tier (`vm-865-split-r50/`).
- **Expensive dependents (restactions, ~275 ms each):** accepted Puts go from main 8.6–8.7/s to 10.4–11.1/s with B. That is MORE expensive re-resolves, about one per dependent every 1.9 s instead of every 2.3 s.
- **Completions without a Put** (the cheap refreshes: the incremental list cell's snapshot and declines) go from main ~21/s to 0.6/s. These are the "lost" completions.
- **Normal-tier wait** goes from main mean 55–67 ms (99% ≤ 1 s) to B mean 1.71–1.82 s (4% ≤ 1 s).
- **CPU:** 2.8 → 3.4 cores. **Dependents' staleness p95:** 2.43 → 2.04–2.09 s.

## NOT RUN (at this commit)

- the full package suite, `scripts/check_*.sh`, `gh pr checks`.
