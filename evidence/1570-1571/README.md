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

## NOT RUN (at this commit)

- freshness rounds 3–6;
- the #865 paired-throughput gate at 10 and 50 events/s (main/main, floor, yield, combined, ×3);
- the full package suite, `scripts/check_*.sh`, `gh pr checks`.
