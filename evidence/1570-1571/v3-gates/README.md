# #1570 v3 — the floored wait follows the live backlog (reviewer blocker B1)

All runs are on snowplow-build-1 (the build VM), dated 2026-11-23 (UTC). They are correctness gates, not CPU measurements. The VM-exclusive re-measure (#865 split + freshness ×6) is NOT RUN yet: it waits for the window.

| File | What | SHA | Result |
|---|---|---|---|
| arms.txt, falsifier.txt | first attempt: an arm bug (the SETUP read the floored set; the gate channel was closed twice) | fc72c786 + v3-first-commit.patch | RUN, discarded (harness bug, fixed in arms2) |
| arms2.txt | new arms ×3: S=1 behind 40 and 120 keys; S=3 behind 1,600 keys with a 275 ms cell | fc72c786 + the 98f53da6 patch | RUN, PASS 3/3 |
| falsifier2.txt | the same arms ×3 with the frozen `q.AddAfter(key, floor-elapsed)` restored | same + sed | RUN, FAIL 3/3 on every arm (4.08 s and 12.03–12.07 s after the drain; the burst arm times out with a frozen floor of ~145 s) |
| suite.txt | full `-race -tags=unit -p 8 ./...` | 98f53da6 | RUN: 51 ok, 1 FAIL (F6c, mechanism-bound; fixed in 1c21573b) |
| checks.txt, race.txt | `scripts/check_*.sh`; floor/admission arms under `-race` ×3 | 98f53da6 | RUN, PASS |
| suite2.txt | full `-race -tags=unit -p 8 ./...` | 1c21573b | RUN: 52 ok, rc 0 |
| f6c2.txt / f6c2-mut.txt | F6c ×3 / F6c ×3 with the floor reading the wall clock (dequeue and set) | 1c21573b | RUN: PASS 3/3 / FAIL 3/3 ("F6c RED") |
| f6c.txt / f6c-mutant.txt | the same runs, with output truncated by `head` | 1c21573b | RUN; superseded by f6c2 |
| arms-final.txt | every `TestRefresher1570_` arm under `-race` ×3 | 1c21573b | RUN, PASS |
| checks-final.txt | all 11 `scripts/check_*.sh` | 1c21573b | RUN, 0 failed |

Re-measure specs: `tools/arms-v3-865.json` (main / v3 / mainB with the split-branch overlay) and `tools/arms-v3-fresh.json` (main / v3 with `overlays/fresh-d2f`, which adds dirty_to_fresh per phase). Both overlays were vetted on both SHAs (`go vet -tags paired854,pairedfresh`): RUN, clean.
