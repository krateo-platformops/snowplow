# PM gate — L1 key multiplicity (RBACSubGen fold removal, v7)

PM: cache-pm · 2026-09-23 · design under gate:
`scratchpad/design-key-multiplicity.md` (1,041 lines) · worktree `gate-118cv2` @ `4c66c29`

## VERDICT: **SHIP-WITH-CONDITIONS** (7 conditions, each tied to a named gap)

The analysis is sound and the fix shape is right. The three TL rulings are upheld —
ruling 3 is upheld **only** with C3 attached, and I say below exactly why an
obligation on its own is not sufficient. Everything else here is gap-closing, not
re-litigation.

I verified the load-bearing code myself rather than taking the design's word.
What follows separates **confirmed**, **corrected**, and **missing**.

---

## A. Confirmed by my own read

| design claim | my check | result |
|---|---|---|
| Fold at `resolved.go:844-847`, inside `if in.CacheEntryClass != CacheEntryClassWidgetContent` (`:828`) | read 820-870 | **CONFIRMED**, byte-for-byte as described |
| `resolvedKeyVersion = "v6"` at `resolved.go:518` | read 495-525 | **CONFIRMED** |
| One non-test write site for `RBACSubGen` — `helpers.go:275` | `grep -rn "RBACSubGen:"` | **CONFIRMED** |
| `RepresentativeUsername`/`Groups` already carried, one write site `helpers.go:283` | grep | **CONFIRMED** |
| `cache.HashExtras` exists and routes through the *same* `canonicaliseExtras` `ComputeKey` folds | `resolved.go:895`, `:913` | **CONFIRMED** (§5 cites 868-880; actual 887-908 — cosmetic) |
| `declineWidgetUAFPut` gates all three `widgets` Put sites | grep | **CONFIRMED** (`widgets.go:408`, `phase1_pip_seed.go:1461`, `resolve_populate.go:385`) |
| `uaf_shortttl.go` schedules the decline helpers for deletion | read 265-285 | **CONFIRMED** — and see C3, the comment names **v7** |

## B. Corrected — two errors in the design's evidence, neither fatal, both material

### B1. §2.2's four-row table is really a **one-row** control. Two rows are vacuous.

The design offers `widgetContent` (0 excess), `apistage` (0 excess) and `raFullList`
(0 excess) as corroboration that the identity fold is the driver. But:

- `apistage` inputs are built by `contentKeyInputs`
  (`internal/resolvers/restactions/api/apistage.go:65-74`) — it sets **no**
  `BindingUID` and **no** `RBACSubGen`. Its own comment: *"Username/Groups are left
  zero."*
- `raFullList` inputs come from `RAFullListKeyInputs(...)`
  (`apiref/ra_full_list.go:170`) — `BindingUID` yes, `RBACSubGen` **never set**.

Both therefore fold a **constant zero** sub-gen. They **cannot** exhibit sub-gen
excess under any hypothesis, so their zeros corroborate nothing. The class
discriminator reduces to `widgetContent` alone — **which is the corpus the design
itself flags as its weakest link (§2.2 caveat, §8)**.

Consequence: "three independent cuts" is really **one weak cut + two strong ones**
(§2.3 cross-plural uniformity and §2.4 wave signature, both of which I accept as
stated). This does **not** overturn the conclusion — it makes **TL ruling 1
(instrument alone, first) load-bearing rather than merely prudent**. Endorsed
without reservation.

Second-order finding worth recording: **#118 (c)'s sub-gen protection never applied
to `apistage` or `raFullList` at all** — they have been folding 0 since v5. That is
either a gap in #118 (c) or evidence those classes never needed it. Not this ship's
problem; file it.

### B2. The decline-site enumeration is short by one.

Design names three `restactions` sites. `grep -rn "declineUAFPut"` returns **four**
production call sites:

```
restactions.go:399 · resolve_populate.go:387 · phase1_pip_seed.go:1040
phase1_pip_seed.go:1194   ← NOT in the design's set
```

`:1194` is a real gate (inside `if reason := uafDeclineReason(...); reason != ""`,
the POST-RESOLVE decline for a seeded RA that *nests* a UAF it does not declare).
The safety property still holds — it is gated — but per
`feedback_enumerate_all_write_sites_not_the_designs_named_set` the named set was
trusted instead of grepped. The widgets set of three **is** complete; I checked.

---

## C. Conditions

### C1 — BLOCKING · §6.2's "by construction" is NOT independent of §6.3's mitigation

This is the gate's central finding on TL question 2.

Deleting a key component merges exactly those keys that differed **only** in that
component. So the 14% stays separated **iff no pair of cells with genuinely
different bodies differs only in `RBACSubGen`**. §6.2 argues this from
"`PerPage`/`Page`/`Extras` are untouched" — but that only covers bodies that differ
*because of* pagination or extras. It says nothing about a body that differs
**because of the requester**, which is precisely what a per-subject key component
would have been separating.

The design does close that hole — but in **§6.3 limb 2**, not §6.2: an
identity-dependent body cannot be resident because `declineWidgetUAFPut` keeps
refilter-touched envelopes out of L1. So:

> **"The 14% is preserved by construction" and "dropping the fold is safe" are the
> SAME argument, and both rest entirely on the UAF Put-decline.**

§6.2 must be rewritten to say so explicitly and to stop reading as an independent
guarantee. Two arguments that share a single point of failure must not be presented
as two.

Note what this also means, in the design's favour: because identity-dependent bodies
cannot be resident, two cells differing only in sub-gen differ only **temporally**
(an old body vs a newer one of the same lineage). Merging those is **correct** —
last Put wins. That is the right disposition of the `portal-builder-page-header`
2456-vs-1568 split in §9, and the design never states it. State it.

### C2 — BLOCKING · Hermetic key-parity golden, RED today, alongside F1

F1 is a live-log observation. It cannot run until the instrument ships (which is
ruling 1, correct), it depends on catching a real bump in a capture window, and it
leaves **no permanent guard** — a future edit re-adding the fold passes F1 forever,
because nobody re-runs a live capture in CI.

Required addition, per `feedback_key_parity_golden_real_inputs_prehash_diff` and
`feedback_hermetic_keyrotation_is_not_endtoend_fix` (hermetic is not end-to-end —
so this is **as well as** F1, never instead of it):

A golden over **real `ComputeKey` on both sides**, pre-hash inputs constructed
independently, asserting the conjunction:

1. `ComputeKey(in{subgen:5}) == ComputeKey(in{subgen:7})`, all else equal.
   **RED on today's code**, and its RED is caused by exactly the four lines under
   deletion. This is the arm that satisfies the TL's "RED caused by the thing it
   claims to test".
2. Differing `PerPage` → different key. Differing `Page` → different key.
3. **Differing `Extras` VALUE at identical cardinality → different key.** This
   closes the `Extras` weak link (§8, B1 above) *hermetically*, independent of the
   `widgetContent` corpus. Green before and after — a guard, not a falsifier.
4. Differing `BindingUID` → different key. (v7 must not collapse identity.)

Plus `TestBootSeedCoverage_*` as F5 already states
(`feedback_seed_falsifier_required_on_l1_key_changes`).

### C3 — BLOCKING · The obligation is not sufficient. Make it executable.

**TL question 3, answered on its merits and not echoed.**

I uphold **ship-now**, but §6.4 as written fails for a reason the design does not
consider: **the obligation's anchor is inside the file the 1.13.0 change deletes.**
§6.4 instructs that the text be transcribed to `resolvedKeyVersion` and to the
1.13.0 brief, and points at `uaf_shortttl.go:273-275` — but that comment block and
those helpers are *the thing being removed*. A guard that vanishes with the change
it guards is not a guard. A doc obligation also cannot survive a fresh author, and
`feedback_recurring_regression_pattern` records four reverts of exactly that shape.

And a second, sharper problem I found in the code: **`uaf_shortttl.go:273-275`
already names the UAF-digest key version as `v7`** —

> *"the mechanism is instead cleanly REMOVABLE — 1.13.0 deletes these helpers and
> their call sites when the UAF-scope digest lands in the key (v7)."*

This ship **spends v7 on the sub-gen removal**. The 1.13.0 digest then becomes v8,
and the one in-code record of the 1.13.0 plan is silently wrong from the day v7
lands — on the exact line §6.4 nominates as its anchor.

**Condition (all three, they are cheap):**

- **(a)** An **executable guard**, in the manner of the existing AST-guard prior art
  (`feedback_leak_needs_acceptance_arm_per_carrier`): a test asserting the
  conjunction *"`ComputeKey` does not fold `RBACSubGen`"* **AND** *"every
  `widgets`/`restactions` L1 Put site is gated by a UAF decline"* — enumerated by
  AST/grep, **not** a hard-coded list of four file:line pairs that rots on the next
  refactor. Deleting a decline site turns this **RED** and it cannot be made green
  without either restoring a separator in the key or editing the guard — which
  forces the re-derivation **into the diff**, where a reviewer sees it.
- **(b)** The v7 ship **must** update `uaf_shortttl.go:273-275` to stop saying v7,
  and carry the §6.4 obligation text **at that site** as well as at
  `resolvedKeyVersion`.
- **(c)** §6.4's owner line stays, and #248 is cross-linked from it (the design's
  §9.1 already establishes the coupling: the digest either dissolves the
  decline-freeze class or entrenches it — both answers are owed).

With (a) in place, ship-now is sound and I do not send this to 1.13.0. **Without
(a) I would rule HOLD**, because the standing 40% tax is real but a silently
re-opened #118 (c) is a cross-tenant leak, and the asymmetry does not favour a
comment.

### C4 — BLOCKING · Counters: the sharp form, and the sentence test

The design ships **two log fields and zero counters**, while claiming a bound
(*"an RBAC change now invalidates a cell in place"*) whose failure is silent.
`feedback_measurement_use_expvar_not_log_tails` applies.

**For every bound, what is non-zero if the bound is absent:**

| bound the design states | absent ⇒ non-zero where? | verdict |
|---|---|---|
| key no longer multiplies on a bump | excess cells > 0 in the F2 capture | covered by F2 |
| RBAC change invalidates in place | **nothing.** No counter exists. Revalidation could be unwired and every number reads healthy | **GAP → new counter** |
| no requester ping-pong (Option B) | F4's hit ratio — but **only if the window spans a real bump** | **GAP → see C5** |
| the 14% stays separated | F3's `bodySha256` enumeration | covered by F3 |

**Required counters**, split so no zero is ambiguous:

- `l1_subgen_revalidate_checked_total` — reads where the cell **carries** a
  representative identity (the check can fire)
- `l1_subgen_revalidate_skipped_unstamped_total` — reads where it **cannot**
  (`widgetContent` / `apistage` / `raFullList` / the prewarm construction sites,
  which set no `Representative*` and no `RBACSubGen`)
- `l1_subgen_revalidate_mismatch_total` — the invalidations actually taken

The split is not decoration. **Without it this fix introduces the seventh
two-regimes-one-number of this investigation**: `mismatch_total == 0` reads
identically for *"RBAC was quiet"*, *"these cells structurally cannot revalidate"*,
and *"the revalidation branch was never wired into the serve path."* With the split,
`checked > 0 ∧ publish_seq advancing ∧ mismatch == 0` is unambiguously a **DEFECT**.

**Sentence test, written first and read back as a ship decision:**

- Two log fields (`rbac_subgen`, `extras_hash`):
  > *"They name which `ComputeKey` input differs between two key computations for
  > the same object. They say nothing about whether the two bodies differ, and are
  > blind to a duplicate whose sub-gen happens to coincide. A clean read means the
  > duplicates were not minted by sub-gen — never that the cache holds none."*

  Read back: **SHIPS.** It is an attribution instrument for an attribution
  question, and its two regimes genuinely differ. **TL ruling 1 APPROVED.**

- `mismatch_total` alone:
  > *"It reads 0 when RBAC is quiet, 0 when the cell cannot revalidate, and 0 when
  > the revalidation path is not wired in."*

  Read back: **DOES NOT SHIP ALONE.** It ships only with the `checked` /
  `skipped_unstamped` denominators above. The sentence test does the work here.

**Scope sentence that must ship with the counters:** revalidation can only fire for
cells written through `helpers.go:275`. For every other construction site the stamp
is 0 and the recomputation is 0, so the check is a permanent no-op there — **which
is no regression** (v6 folds a constant 0 for those classes today, per B1) but is
**not** what "an RBAC change invalidates the cell in place" promises. Say so, in the
counter's `desc:`.

### C5 — BLOCKING · Acceptance criteria: fix-invariant denominator, stated as a conjunction

F2's `excess / cells` carries the defect the TL named from #231: **the denominator
moves with the fix** (`cells` falls from 4,433 as the fix works). Use the
denominator the fix **cannot** move — the count of distinct **logical cells**,
`(class, gvr, ns, name, bindingUID, perPage, page, extrasHash)`. The fix neither
creates nor destroys logical cells; it only stops minting extra keys per logical
cell.

**Acceptance = the conjunction of all six, on one post-soak capture:**

1. `resident_widgets_keys / distinct_logical_cells ≤ 1.05` — today **1.77**
   (4,433 / 2,509).
2. Among **fresh** cells (`ageSeconds ≤ 300`): logical cells holding >1 resident key
   with the **same `bodySha256`** = **0**. (Today 423 of 491 excess = 86.2%.)
3. **Separation, fresh-restricted:** every coordinate that held ≥2 distinct
   `bodySha256` in the **pre-fix fresh** capture still holds ≥2 distinct keys
   post-fix. **Zero** coordinates may go ≥2 → 1.
   *Fresh-restriction is mandatory*: on the full resident set, temporal variants
   inflate the distinct-body count, so an unrestricted form of this criterion would
   **fail a successful fix** — the exact defect retracted from #231.
   F3's *"`tables/incident-auditrecords` must hold exactly 2 keys"* is a scorecard
   row on a coordinate that may not survive the v7 cold break; restate it as the
   class above, with the specific coordinate as an illustration only.
4. `widgetContent` / `apistage` / `raFullList` excess stays **0** — carrying B1's
   qualifier that two of those three are vacuous.
5. `widgets` L1 hit ratio (`dispatch_l1_lookups`) post ≥ pre, over a window
   containing **≥2 `rbac_publish_seq` advances**. Without the ≥2, a green is
   vacuous — the failure mode only exists after a bump
   (`feedback_falsifier_must_drive_real_boundary_not_install_crossed_state`).
6. Comparison is taken at **equal distinct-logical-cell count**, not equal
   wall-clock. The v7 break cold-starts the store; a 1h-old store against a 34h
   steady state confounds every ratio above.

### C6 — REQUIRED · North-star row, and the v7 cold break

This is a key-version bump: **every v6 cell stops being a hit at the rolling
restart.** `feedback_zero_cold_navigations_hard_requirement` and
`project_readyz_gates_on_prewarm_complete` both bind.

- `/readyz` must not admit traffic until prewarm completes at v7. A cold navigation
  after the upgrade is a **ship defect**, not a warm-up.
- Seed-hit acceptance via **Chrome `l1:HIT` + `key_hash`** — curl is inadmissible
  (`feedback_curl_probes_inadmissible_for_seed_hit_acceptance`).
- **North-star ledger row is the artifact**: Chrome through the portal ingress,
  Dashboard piechart **and** Compositions datagrid, mix 0.95 cyberjoker / 0.05
  admin, cold / warm / fresh. A ship without its row is not done.
- L1 hit invariant **100%**, COLD == WARM
  (`feedback_l1_hit_invariant_is_100_percent`).

The expected north-star delta here is **neutral-to-positive** (headroom and
refresher work, not per-request latency). Say that in the row rather than claiming a
page-load win the mechanism does not predict
(`feedback_bench_metrics_must_predict_customer_wall_clock`).

### C7 — REQUIRED · Symptom hygiene: #237 and the #239 fan-out claim

**On TL question 1 — which symptom.** The design's §9 is explicit, correct and
well-evidenced: the fix recovers ~40% of L1 and does **NOT** fix #237's staleness.
Two residues still invite the inheritance:

- **§6.1 is titled "What makes the symptom disappear" and never names the symptom.**
  Rename it: *"Recovers the key-space fan-out (the 40% waste). Does not address
  #237's staleness — see §9."* A doc that kills the link in §9 and leaves an
  unqualified promise in §6.1 will be read at §6.1.
- **Issue #247's BODY still carries "Why it also produces staleness."** The
  retraction is a **comment**. Bodies outlive comments in every downstream read, and
  the body's opening line still reads *"This is the root cause behind #237."*
  **Strike it through in the body** with a pointer to the correction comment.

**On the #239 claim.** *"~40% of the dirty-mark fan-out"* appears in the brief, the
issue and the design, and **no falsifier arm measures it** — F1-F5 cover key, store
excess, separation, hit ratio, version break. Per
`feedback_pre_ship_pprof_must_validate_projection`, a stated gain needs an artifact
showing where it comes from. Either attach a counter (dep-edge count per logical
cell, or refresher `completed_total` per `publish_seq`) or **strike the fan-out
number from acceptance** and carry it as an untested expectation. Do not let it into
the ledger row unmeasured.

---

## D. THE SINGLE THING MOST LIKELY TO BITE

**The refresher's re-stamp, at `resolve_populate.go:160-175`.**

The design disposes of it in a parenthesis — *"the refresher gets the same check for
free at `resolve_populate.go:116` (re-stamp on every re-Put)"* — with no file:line
and no dedicated arm. It is the most dangerous line in the change.

The refresher re-Puts from the **stored** `Inputs`. If the implementer does not
re-stamp, then after the first bump **every stamped cell mismatches on every read,
forever**: read → mismatch → MISS → full re-resolve. L1 hit rate collapses. That is
**strictly worse than today's defect** — today the duplicate at least serves.

And the natural implementation is the wrong one. `resolve_populate.go:166-175`
already computes a refresh identity:

```go
refreshUser := inputs.RepresentativeUsername
refreshGroups := inputs.RepresentativeGroups
if isIdentityFreeClass(inputs.CacheEntryClass) {
    if saEP != nil {
        if saUser, ok := phase1SAUsername(saEP.Token); ok {
            refreshUser = saUser
            refreshGroups = nil
```

`refreshUser` is **sitting right there** and is the obvious thing to reach for — but
for identity-free classes it has been **swapped to the SA identity**. Stamping
`RBACSubGenForSubject(refreshUser, refreshGroups)` while the read side recomputes
from `Inputs.RepresentativeUsername` (`""` for those classes) yields a **permanent
mismatch on every `widgetContent` / `apistage` read**. Silent, total, and it will
present as an unexplained L1 hit-rate collapse — the Option A failure mode arriving
through a completely different door.

**Requirement:** the re-stamp must use **`inputs.Representative*`**, the identical
expression the read-side revalidation uses, derived at **one** shared helper called
by both sides — never two call sites that happen to agree today. C4's
`skipped_unstamped` counter is what makes the identity-free case visible instead of
silent, and C5.5 (≥2 publishes in the window) is what makes F4's green mean
anything.

---

## E. Rulings — disposition

| TL ruling | PM |
|---|---|
| 1 · Instrument alone and first | **APPROVED**, and B1 makes it *necessary*: two of §2.2's four rows are vacuous, so the class discriminator rests on the one corpus the design itself calls weak. Instrument sentence passes the sentence test. |
| 2 · Option B, cell's representative | **APPROVED.** Option A's ping-pong is correctly diagnosed and correctly refused. Section D is the *implementation* trap inside B, not a defect in the choice. |
| 3 · Ship at v7 now | **UPHELD, conditional on C3(a).** The obligation alone is insufficient — its anchor is deleted by the change it guards, and `uaf_shortttl.go:273-275` already claims v7 for the digest. With the executable guard, ship now. Without it, HOLD. |

## F. Journals (`feedback_maintain_feature_journal`)

`project_feature_journal.md` on ship — expected / test / actual / delta.
No `project_regression_journal.md` row: this is a standing defect being fixed, not a
regression introduced by us. If C5's ratios miss, that row gets written then.

---

# AMENDMENT 1 — 2026-09-23, after the developer ACK

TL reports C5.1's denominator is not computable on any surface that exists today
(`perPage`, `page`, `extrasHash` absent from `ResolvedEntryMeta`), and has ruled
four fields onto the metadata surface rather than the two log fields originally
specced. Accepted, and the criterion is unchanged. Below: the dependency stated
explicitly, a **second** computability gap on a different criterion that the ACK
did not reach, and the consequences for the arms.

## G1 — C5.1's dependency, stated (amends C5)

**C5 is not evaluable until the instrument deliverable is live.** That is a
sequencing fact, not a defect, and it is the correct order: ruling 1 already put the
instrument first. Three consequences that must be written down now rather than
discovered at acceptance:

**(a) The pre-fix baseline must be RE-TAKEN on the instrument build.**
`preroll-237/apistage.json` cannot serve as the C5 baseline — it lacks all three
fields. Per `feedback_anchor_is_cluster_state_dependent`, re-baseline on the build
that carries the instrument, on the same pod, before the key change.

**(b) "today = 1.77" is an UPPER bound and will move.** It was computed on the
4-tuple `(resource, ns, name, bindingUID)`. The 8-tuple denominator is necessarily
**≥** 2,509, so the true ratio is **≤** 1.77. The same applies to the headline
**40.7% excess** and to §4's 71.6% / 86.2% yield figures — all are computed on a
tuple that collapses `perPage`/`page`/`extras` into the coordinate, so all are upper
bounds on sub-gen-attributable waste. The C5.1 target (**≤ 1.05**) is absolute and
unaffected; only the reference point moves.

**(c) Pre-registered falsification threshold for ruling 1.** The instrument exists
to let the conclusion be falsified before it is acted on, so the threshold belongs
in the gate rather than being settled after the data arrives:

> On the first instrument-build capture, compute excess attributable to sub-gen =
> keys sharing a logical cell (**equal** `perPage`, `page`, `extrasHash`) that
> differ **only** in `rbac_subgen`. **If that is below half the claimed yield**
> (i.e. < ~20% of resident `widgets` keys, against the claimed 40.7%), the ship
> decision is **re-put to TL** before the key change is written. A key version is
> being spent on this; the yield is the entire justification.

A downward revision is a **legitimate outcome of ruling 1, not a failure of it.**

## G2 — SECOND computability gap: `bodySha256` is not available on the full walk

The ACK found the gap on C5.1. There is another, on a different criterion, and it
hits the highest-consequence arm in this gate — **F3 / C5.2 / C5.3, the arm that
guards the 14%.**

`ResolvedEntryMeta.BodySHA256` (`resolved.go:1347-1357`) is populated **ONLY** for a
single-key lookup:

> *"Populated ONLY for a single-key lookup (`/debug/apistage?key_hash=...`), never
> for the full walk — hashing every body under the store mutex would stall serving
> at 100K entries."*

So C5.2 ("logical cells holding >1 key with the **same** `bodySha256` = 0") and C5.3
(coordinates that held ≥2 distinct `bodySha256`) cannot be evaluated from one
capture. **The four new metadata fields do not fix this** — they solve C5.1's
denominator only. Do not let the two gaps be read as one.

**Resolution — a three-pass enumeration, not a full-walk hash.** Do NOT add
`bodySha256` to the walk; that comment is right, and at 50K it would stall serving
(`feedback_customer_priority_over_refresher`).

1. **Full walk.** `keyHash` + the 8-tuple + `rawJSONBytes` — all present on the walk
   today. Group into logical cells. Cells holding one key are done.
2. **Byte-length pre-filter.** Within a multi-key logical cell, keys with
   **different** `rawJSONBytes` hold **definitely distinct** bodies — no hash
   needed. Only **length-equal** candidates can be byte-identical duplicates.
   §4 already establishes length equality ≠ body equality, so the pre-filter is
   sound in exactly one direction and that is the direction needed.
3. **Single-key `bodySha256`** for the length-equal candidates only.

Today that is ~1,924 lookups worst case before the pre-filter and materially fewer
after; fresh-restricted (C5.2/C5.3) it is a few hundred. Tractable. A naive
per-resident-key sweep would be 4,823 today and 50K at scale, each taking the store
mutex.

**Sequencing condition:** the pass-3 sweep must **not** run concurrently with the
north-star Chrome measurement (C6). It takes the store mutex per lookup and would
perturb the very wall-clock being scored.

## G3 — The RED arm moves with the instrument (amends C2 / F1)

F1 was specced as two **log lines** for one widget spanning a bump. With the
instrument on `ResolvedEntryMeta`, that arm no longer exists as written. Restate it
on the new vantage — where it is **stronger**, not weaker:

> **RED (pre-fix):** two **resident cells**, same logical cell (identical
> `bindingUID`, `perPage`, `page`, `extrasHash`), **different** `rbac_subgen`,
> **different** `keyHash`.
> **GREEN (post-fix):** no such pair exists — that combination can no longer be
> resident, because the key no longer separates on it.

This observes the **residue itself** rather than a transient pair of computations,
it needs no bump to be caught inside a capture window, and it is the same surface
the 40.7% was measured on. C2's hermetic golden is unchanged and still required —
`feedback_hermetic_keyrotation_is_not_endtoend_fix` cuts both ways.

**And the metadata vantage cures the blindness the developer found by construction:**
`ResolvedEntryMeta` is emitted per **resident entry**, uniformly for every class,
independent of which key path constructed it. The log site was per **emit site**, so
a class with no emit site was invisible. That is why the instrument is now able to
cover `widgetContent` — the one class that constitutes the evidence (B1) — and it is
a property of the surface, not a fix that could regress.

## G4 — The sentence test now states its vantage (adopted)

Adopted into this gate, and it belongs in the template:

> **Every instrument's limitation sentence must name the vantage it is read from,
> and that vantage must be one that exists in production.**

As applied until now the test asked what a field *means* and never whether anyone
can *see* it — so it passed an instrument emitting `log.Info` beneath a live
`LOG_LEVEL=warn` floor. I approved that instrument in C4 on its meaning alone. The
test as amended fails it on the second clause, before the first is even reached.

Re-running C4's two sentences on the corrected vantage:

- **Four metadata fields, read via `/debug/apistage`:** *"Read from the resident-cell
  walk, they name which `ComputeKey` input differs between two keys of the same
  logical cell. They cannot see a cell that was never Put, they are blind to a
  duplicate whose sub-gen coincides, and `bodySha256` is not among them on the walk
  (G2)."* → **SHIPS**, with G2's three-pass enumeration attached.
- **`mismatch_total` alone:** unchanged — **DOES NOT SHIP ALONE** (C4).

The counters in C4 must be checked against this too: they are **expvar**, read from
`/debug/vars`, which is live and readable on 057 — vantage confirmed, not assumed
(`feedback_measurement_use_expvar_not_log_tails` is the same lesson one layer up).

## G5 — Disposition

C5's denominator **stands unamended**; G1 attaches its dependency and its baseline
obligation. C2/F1 are **restated** on the metadata vantage (G3). **G2 is a new
BLOCKING condition** — F3/C5.2/C5.3 are not evaluable without it, and F3 is the arm
guarding the 14%. Verdict unchanged: **SHIP-WITH-CONDITIONS**, now 8.

---

# DIFF REVIEW — #247 instrument · 2026-09-23

Request: dev-keymult, pre-commit, branch `feat/247-key-multiplicity-instrument`
@ `5c444db`. Gate: acceptance + falsifier coverage.

## VERDICT: **APPROVED TO COMMIT**, subject to H1–H3 (all mechanical; no design change)

The instrument is sound and the falsifier work is the best on this workstream. It
closes C5.1, honours C4's scope sentence in behaviour, honours G2 and G4, and the
one production defect I found in review had already been fixed in the tree before I
reached it. H1–H3 are artifact-integrity items, not substance.

## H0 — What I verified independently (not taken from the request)

- **`lruItem` write sites enumerated by my own grep**: exactly three —
  `resolved.go:1117` (`old.entry`), `:1118` (`old.bytes`), `:1138` (construction).
  All three carry `extrasHash`. No fourth site. This is the Section-D shape
  ("derived field that tracks the entry everywhere except the re-Put") and it is
  closed at every site, not at the named ones.
- **The off-lock claim holds**: `extrasHashForEntry` is called at `:1053`,
  `c.mu.Lock()` at `:1063`. Ten lines earlier, as the comment says.
- **`metaForItemLocked` is O(1)** — reads `item.extrasHash`, hashes nothing.
- **C5.1 is now computable from one row**: `cacheEntryClass`, `group`, `version`,
  `resource`, `namespace`, `name`, `bindingUID`, `perPage`, `page`, `extrasHash`.
  The 8-tuple closes.
- **G2 honoured**: `BodySHA256` deliberately left single-key-only, and
  `rawJSONBytes` remains on the walk — so the three-pass enumeration (walk →
  byte-length pre-filter → per-key sha) is implementable exactly as specified.
- **G4 honoured**: the instrument is the `/debug/apistage` surface, readable at
  `LOG_LEVEL=warn`; the two log fields ship with a code comment stating they are
  bench/kind-only and are **not** the deliverable's evidence.

The **blindness probe** (`01-blindness-before-5c444db.log`) is the right artifact
and the right shape: four genuinely distinct `ComputeKey` values for one logical
cell projecting byte-identical metadata on every attributable field, captured
**before** implementation, naming C5.1 explicitly. That is "the surface the 40.7%
was measured on could not say why the keys differed" in executable form.

**Control 6 deserves specific credit.** Mutating to hash-per-walk and requiring the
arms to stay **GREEN** proves the arms test behaviour rather than pinning the
implementation, and puts the stash decision on cost where it belongs. Few RED
matrices include the control that must not fire.

## H1 — BLOCKING · The artifact under review is not the tree

`247-production.diff` — sha256 `c896ad0f…`, 251 lines — does **not** match the
working tree, whose `git diff` is 236 lines, sha256 `9f184380…`. Two deltas:

1. **`RBACSubGen` lost `,omitempty`**, with eight lines of new doc.
2. `subgenOf` moved below `emitDispatchCacheKeyDiag` (code motion only).

Delta 1 is the **one production defect I had flagged to raise** — a `uint64` with
`omitempty` omits its zero, so a zero sub-gen would have been *absent* from the
wire, not readable as a value. That would have made the field's own doc describe a
reading that never appears, made field-coverage counting (the §2.1 method used to
exclude `Stage`) unable to distinguish "this build lacks the field" from "all these
cells are unstamped", and turned a two-regime value into a three-regime absence. It
is fixed in the tree, the new doc gives the right reason, and the contrast drawn
against `ExtrasHash`'s honest `omitempty` is correct.

So the tree is **better** than the diff — but a gate binds to an artifact.
**Regenerate the diff; my sign-off binds to the regenerated sha256 and line count**
(`feedback_rework_priority_and_sha_confirmations`).

Process note for TL, not for dev: this worktree changed branch and content under me
mid-review — it was `feat/237-b` @ `4c66c29` when I read `resolved.go` earlier this
session. Gating from a live multi-writer tree is what
`feedback_freeze_to_committed_ref_for_multiagent_gating` warns about. It cost
nothing here because the drift was an improvement, and it is exactly how a drift
that was not an improvement would also have arrived.

## H2 — BLOCKING · The RED matrix certifies a tree that no longer exists

`02-red-control-matrix.log` ran at **16:03**. `resolved.go` was modified at
**16:10** (the H1 fix). The matrix therefore certifies a different file than the one
being committed (`feedback_one_sha_one_clean_tested_tree` — touch → re-run).

**Re-run on the final tree, and add one control**, because the matrix currently has
no control for the only defect this review surfaced:

> **CONTROL 7: `RBACSubGen` re-acquires `,omitempty`** → expect
> `TestDebugApistage_ZeroSubGenStillSerialises` **RED**.

That arm already exists and is written at the right vantage — it unmarshals to
`map[string]any` and asserts key **presence** (`_, present := row["rbacSubGen"]`),
which is what catches an encoding-level regression that a struct-level assertion
cannot see. It is currently unproven; the control proves it.

Note this also fixes a structural gap: **the matrix mutates only
`internal/cache/resolved.go` and runs only `./internal/cache/`**, so the two
wire-shape arms — the *vantage* arms, the entire point of G4 — have no control
today. Control 7 must run `./internal/handlers/`.

## H3 — REQUIRED · `run_arm` infers GREEN from an exit code

`go test -run <pattern-that-matches-nothing>` **exits 0**. `run_arm` maps `rc == 0`
to GREEN, so Controls 0, 6 and "restored" — every GREEN row — would pass vacuously
under a test rename or a typo'd pattern.

**Not a live defect in this run**: Controls 2/3/4 go RED under specific arm names
that the `TestResolvedMeta_` prefix necessarily matches, so non-vacuity is provable
by composition. But the harness is not self-proving, and Control 6 is where that
bites hardest — it is the row carrying "the arms do not pin the implementation", a
claim a vacuous GREEN would silently fabricate.

Fix: require `≥ N` `--- PASS` lines (`-v`, count them) rather than trusting the exit
code — `feedback_falsifier_must_actually_run_under_gate_tag_env`,
`feedback_wrapper_exit_code_is_not_the_works`.

## H4 — Non-blocking · Source the `~50 Puts/s` figure

`lruItem.extrasHash`'s doc justifies the Put-side cost with *"which runs at ~50
Puts/s in production"*. Unsourced, and it looks low: refresher
`completed_total = 36,599,293` over ~34 h is ≈ **300/s** of completions.

The **conclusion is unaffected** — a `json.Marshal` of a small map plus a SHA-256 is
single-digit microseconds, so it is negligible at 300/s just as at 50/s, and
hash-at-Put over hash-per-walk stands. But it is a capacity number in a permanent
code comment that future readers will rely on, and this programme has seen 180×
errors in exactly that position (`feedback_empirical_capacity_caps`). Cite the
counter it came from, or restate it as a bound.

## H5 — Bounds I consider guarded, and the one I do not

Every bound in the request's table is guarded by a named arm that the matrix drives
RED, with the mutation-applied check in place. I add nothing to that table.

The disclosure I want kept is the request's item 3 — **the arming path
(`refresh_subscription.go:242`, `:263`) derives a stamped key and Puts nothing, so
no resident cell exists and neither form of the instrument can observe it.** Held
correctly here. Carrying it forward: that makes `refresh_subscription` a **fourth
consumer of the stamped key** whose behaviour changes at v7, and the v7 gate must
check it — a subscription armed on a key whose sub-gen differs from the resident
cell's arms against a cell that does not exist. **Not this deliverable's problem,
and not a reopening of §9's kill** (which is empirical — cells demonstrably do get
refreshed). Logged so v7 inherits it as an open item rather than as settled.

Items 1, 2 and 4 of the request's "what this does NOT do" are accurate and match
this gate's own record. Item 4 especially: this is not a #237 detector and must
never be reported as one.

## H6 — Journals

Instrument ship → `project_feature_journal.md` (expected / test / actual / delta).
No north-star ledger row: no key change, no serve-path change, no latency claim —
state that explicitly in the journal entry so the absence reads as a decision.

---

# DIFF REVIEW — AMENDMENT · #247 instrument, revision 2 · 2026-09-23

## VERDICT: **APPROVED — COMMIT**, with H7 fixed in this commit and H2b/H3 as a follow-up on the same branch before the PR merges.

## H1 — CLEARED

`247-production.diff` and the working tree are now **byte-identical**:
sha256 **`fa81da7b690aa79683a95617e7a955d42d97ae832f83ed124ac4c1122b2e5080`**,
**281 lines**. My sign-off binds to that hash.

The `omitempty` fix is in, and the architect's reason is better than mine: under the
hypothesis this instrument exists to test, **the baseline cell carries sub-gen 0 and
its duplicates carry >0**, so the zero rows are *half of every comparison* — not an
edge case. `extrasHash` correctly keeps `omitempty`, where `""` means "no
key-inputs record". The asymmetry between the two tags is now principled and
documented.

## H2 — HALF CLEARED

**Re-run on the final tree: done** (matrix mtime 16:15, `resolved.go` 16:10). 11/11.

**H2b — still open.** The re-run is the same eleven rows as before. **Control 7 —
re-add `,omitempty` to `RBACSubGen`, expect `TestDebugApistage_ZeroSubGenStillSerialises`
RED — was not added**, so the arm guarding the one production defect this review
surfaced is still unproven. The structural gap behind it also stands: the matrix
mutates only `internal/cache/resolved.go` and runs only `./internal/cache/`, so
**neither wire-shape arm has any control**. Control 7 must run `./internal/handlers/`.

The same control covers `PerPage`/`Page`: they are non-`omitempty` for the same
reason, and nothing would currently RED if that changed.

## H3 — still open

`run_arm` maps exit code 0 to GREEN; `go test -run <no-match>` exits 0. Unchanged
(`red-control-matrix.sh` mtime 16:00). Count `--- PASS` lines.

## H7 — NEW, fix in this commit · the `extras_hash` cut is right, its stated magnitude is not

The cut itself is **correct and I endorse it**. `slog` evaluates arguments before
`log.Info` checks level, so the hash would be computed and discarded at
`LOG_LEVEL=warn`. That reasoning is magnitude-independent: *it buys nothing the
metadata surface does not already carry.* That is the whole argument and it is
sufficient.

The permanent code comment then overstates the magnitude, and the comment will
outlive the ruling:

- **`HashExtras` short-circuits**: `if len(m) == 0 { return "e0" }`
  (`resolved.go:925`) — no marshal, no SHA-256.
- The **restactions** seed site (`phase1_pip_seed.go:906`) passes **`nil`** extras.
  It would have cost nothing.
- `phase1_bindingset_seed_resolves_total` is bumped at **two** sites —
  `phase1_pip_seed.go:1220` (restactions) and `:1485` (widgets) — so **the counter
  citation is right**; it covers both. But the actual hash count is bounded by the
  **widgets** share carrying non-empty extras, not by 279,588.

"Hundreds of thousands of discarded hashes on the readiness path" is therefore an
upper bound presented as a count. **One clause fixes it**: cite the `len(m)==0`
short-circuit and say the cut rests on *buys nothing*, not on magnitude.
`feedback_bench_metric_name_disambiguation` — trace the metric to file:line.

**The same short-circuit is good news for the Put side, and it sharpens H4.** The
Put-side `extrasHashForEntry` is also paid per boot-seed Put, on the same
`/readyz`-gating path — the exact regime the TL's ruling identified as material. It
is bounded by the same short-circuit, so the exposure is small. But
`lruItem.extrasHash`'s doc justifies its cost with *"~50 Puts/s in production"*,
which describes **steady state** — the wrong regime. Boot is where the cost lands
and where the readiness gate is.

**H4 is therefore no longer merely unsourced; it cites the wrong regime.** Still
non-blocking — the conclusion survives at any of these numbers — but fix both
comments in this commit while the question is open, rather than leaving two
permanent cost claims that a future reader will inherit.

## What I additionally verified this round

- Boot emit sites confirmed at `phase1_pip_seed.go:906` (restactions, `nil` extras)
  and `:1302` (widgets, `seedKeyExtras`).
- `TestResolvedMeta_NilInputsZerosAreDiscriminatedByClass` is the right shape:
  it asserts the two rows are **numerically identical** on all three fields (the
  ambiguity, stated as a precondition) and separable **only** by
  `cacheEntryClass`. The discriminator is a tested property, not a comment.
- The struct/wire pairing is complete: that arm covers discrimination at the struct,
  and `TestDebugApistage_ZeroSubGenStillSerialises` covers **presence on the wire**
  for `rbacSubGen`, `perPage` and `page` by decoding into `map[string]any`. Struct
  assertions cannot see an encoding regression; this pair can.

## Disposition

Commit. H7 (two comment clauses) in this commit. H2b and H3 as a follow-up on the
same branch before the PR merges — they are falsifier-harness robustness, and the
matrix must be trustworthy **before the v7 change**, which is when it becomes the
thing standing between us and a key-space regression. Do not hold the instrument
for them (`feedback_rework_priority_over_enhancements`).

---

# SIGN-OFF RE-BIND · #247 instrument · 2026-09-23

## The binding hash — supersedes every earlier one in this file

> **`d49525575ddaf22673891a6e1ad1a4a0e5df5c22ae173351cf041fdfec2e01bf` · 302 lines
> · commit `b242785` ("feat(cache): attribute L1 key multiplicity on the surface it
> was measured on (#247 instrument)"), parent `5c444db`.**

Verified by my own regeneration:
`git diff 5c444db b242785 -- internal/cache/resolved.go internal/handlers/debug_apistage.go internal/handlers/dispatchers/helpers.go`
hashes to `d4952557…` at 302 lines.

**Delta against the `fa81da7b…`/281 artifact is comment-only** — I checked rather
than accepted it: differencing the two diffs and filtering comment, `index` and
hunk-header lines leaves **zero** remaining lines. The added blocks are the H4
Put-rate sourcing and the `omitempty` asymmetry paragraph.

`fa81da7b…` was accurate when I asserted it and is now superseded. The point stands
on its own terms: **a sign-off must name what shipped**, and a hash for a tree that
was never committed is the artifact-without-a-run failure in miniature. Raised by
the developer, correctly.

## H2b and H3 — CLOSED, verified

The eleven-row log I read was the 16:15 file, overwritten at 16:19. Current matrix:
**20 rows, `19 OK, 1 VACUOUS (expected self-test), 0 mismatches — MATRIX: PASS`**,
`resolved.go` sha identical before and after.

- **H3 closed and self-proving.** `run_arm` no longer reads the exit code; every row
  now reports `(ran=N pass=N fail=N)` and reports VACUOUS at zero. **Control 0b** is
  a deliberate non-matching pattern that MUST report VACUOUS — the harness
  demonstrates its own non-vacuity instead of asserting it. That is a better answer
  than the `--- PASS` count I asked for.
- **H2b closed.** Control 7 present; Controls 2/3/4/7 now run `./internal/handlers/`
  as well as `./internal/cache/`.

**Control 7's result is the most valuable single row in this deliverable, and it is
a measurement rather than an argument:** re-adding `,omitempty` REDs
`TestDebugApistage_ZeroSubGenStillSerialises` (`ran=1 fail=1`) while **all eight**
`TestResolvedMeta_` arms stay GREEN (`ran=8 pass=8`). Eight struct-level arms are
structurally blind to a JSON encoding defect. Without Control 7 those eight greens
would have read as coverage of exactly the defect they cannot see — which is why
the control was worth insisting on, and it is the vantage clause (G4) earning its
keep in executable form.

## H7 — accepted, and the follow-up-commit ruling is right

All three source claims were verified at file:line by the developer rather than
taken from me: the `len(m)==0` short-circuit, `nil` extras on the restactions seed
path, and the counter bumped at both sites.

I asked for H7 "in this commit". **TL overrode to a follow-up commit, and that is
the better call**: `b242785` has a full `-race` suite running against it, and an
amend would orphan that result (`feedback_one_sha_one_clean_tested_tree`). Comment-
only still means touch → re-run, and the follow-up carries its own suite and its own
matrix run. Recorded as an accepted deviation, not an open item.

## Gate status

**#247 instrument: APPROVED and COMMITTED at `b242785`.** Nothing outstanding.

Carried forward to the v7 ship, unchanged: C1–C7, G1–G5, and the Section-D
re-stamp trap. The v7 change is what the matrix now exists to protect.

---

# G1 RE-REGISTRATION + C3 RE-PUT · 2026-09-24

## J1 — Corrections accepted

**Stage.** Accepted, and the direction of the bias is as TL states — an omitted fold
component collapses legitimately-distinct keys into one logical cell and inflates
measured excess, i.e. **biases the denominator toward the conclusion the gate
exists to test**. Register the tuple as **nine elements** (eleven JSON keys):

`cacheEntryClass`, `group`, `version`, `resource`, `namespace`, `name`,
`bindingUID`, `perPage`, `page`, `extrasHash`, **`stage`**.

Precision worth recording so the correction is not over-read: §2.1 measured `stage`
**absent from all 4,823 entries** of the original capture by field-coverage count,
so **the historical 40.7% is not in fact inflated by Stage**. The rule must still
carry it — it will be applied to captures that do contain apistage cells, and a
registered rule that happens to be harmless on one dataset is still wrong.

**Threshold 0.217.** Accepted as TL ruling — it is my own (b), it is the internally
consistent reading, and it is a tightening of the 0.20 I registered. No tilde.
Pinned against the **original** claim per (d).

**The (c) refinement is better than my (c), and I am registering it as the primary
arm rather than a refinement.** Counting `n − 1` only where a cell's distinct
`rbacSubGen` count equals `n` converts an assumption into a detector: a cell whose
keys share the full tuple **and** the same `rbacSubGen` yet differ in `keyHash`
proves an **unenumerated fold component**, which falsifies the whole attribution.
That is precisely the risk §8 named as the analysis's largest and left without an
arm.

Note what this means for the Stage miss: had the refinement been in place, the
missing `stage` would have **surfaced empirically as loud residual cells**, not been
caught by re-reading `ComputeKey`. It is self-correcting for exactly the class of
error that produced it.

## J2 — Admissibility precondition (new, and G1 does not function without it)

Two captures have now refused with all cells at `rbacSubGen = 0`. That is a **third
reading of the zero**, beyond the two in C4's scope sentence: *"no bump has occurred
since this pod booted"* — a statement about **pod uptime**, not about key
multiplicity.

Without a precondition, such a capture computes **0% sub-gen-attributable excess**,
trips a `< 21.7%` threshold, and re-puts a ship decision on the strength of an
instrument that was not yet measuring anything. That is
`feedback_negative_evidence_needs_its_scope` in its most expensive form.

**Registered precondition — G1's trip point may not be evaluated until BOTH hold:**

1. A non-trivial share of resident `widgets` cells carry `rbacSubGen > 0` (TL to set
   the share; it must be stated as a number before the capture, not after).
2. `rbac_subgen_bumps_total` has advanced by **≥ 2** over the capture window, so at
   least two distinct sub-gen epochs can be resident
   (`feedback_falsifier_must_drive_real_boundary_not_install_crossed_state`).

A capture failing either is **INADMISSIBLE — not a trip**. TL's use of the counters
to gate admissibility rather than to substitute for the capture is the correct
reading of (e) and closes it.

## J3 — Does the no-op-rotation finding change what G1 is a threshold ON? **YES.**

Verified at source before answering: `bindings_by_gvr_delta.go:155-179`,
`recordPendingSubGenBumps` is called unconditionally for OLD subjects and again for
NEW subjects, with **no comparison between them** — no `DeepEqual`, no
resourceVersion skip, no subject-set diff. The dedup the code comments cite is
within a pending flush, not between old and new. Combined with client-go
dispatching `Sync`/`Replaced` as `OnUpdate` for every object already in the store, a
relist rotates every subject of all 687 bindings with zero RBAC change. **TRACED.**

### What it changes

G1 measures **total** sub-gen-attributable excess and asks *"is the yield large
enough to justify spending a key version?"* That question silently assumes **v7 is
the only way to obtain the yield.** If the bumps are overwhelmingly relist no-ops,
that assumption is false, because a second and much cheaper fix exists:

> **Compare old against new in `onBindingUpdate` and skip the bump when the subject
> set and roleRef are unchanged.** No key version. No cold break. No exposure to
> C1/C3's UAF-decline dependency at all.

And the residue is **rate-proportional, not cumulative**: §2.3's own eviction
figures (`evict_ttl_total = 7,262`, `evict_max_age_total = 2,125`, no resident cell
older than 1,226 min) show orphans do age out, so steady-state residue ≈
bump-rate × objects × cell-lifetime. Cutting bumps from ~360/h to tens/day cuts
residue by two to three orders of magnitude.

**Therefore the quantity a v7 ship decision turns on is the MARGINAL yield of v7
over the bump-source fix — not the total.** G1 as registered would green-light v7 on
a benefit v7 does not uniquely deliver. Re-register it as marginal, measured after
the bump-source fix is in, or it is measuring the wrong thing with great precision.

### What it does NOT change — stated so this does not over-swing

The bump-source fix does not make v7 wrong or unnecessary:

- Folding a monotonic counter into key identity remains the anti-pattern §5's
  prior-art check identified — it turns an invalidation into an allocation.
- A **genuine** RBAC change still mints a new key and orphans the old under
  DELETE-only invalidation. Lower rate, identical defect shape.

What changes is **urgency**, and therefore whether v7 is worth a key version **now**
versus bundled at 1.13.0.

### J4 — I am re-putting my own C3 ruling

C3 upheld ship-now on this reasoning, quoted from above:

> *"the standing 40% tax is real but a silently re-opened #118 (c) is a cross-tenant
> leak, and the asymmetry does not favour a comment."*

**If the tax is mostly relist noise and is removable without touching the key, that
asymmetry flips.** Holding v7 for 1.13.0 then costs little and dissolves C3's
central problem — the self-erasing guard — because the fold removal and the
UAF-scope digest would land in **one** key version, which is what
`uaf_shortttl.go:273-275` planned before this ship proposed spending v7 first.

Per `feedback_verify_the_facts_inside_an_option_before_putting_it_to_the_decider`
— a decision taken on a premise since shown false is VOID and must be re-put — **I
am not defending C3. It is re-put to TL**, to be decided after
`noop_updates_total` reports. TL's public commitment that no v7 key change is
written until then is the right sequencing and I endorse it.

**If the no-op finding holds:** fix the bump source first, re-measure, and decide v7
on its marginal yield — with holding for 1.13.0 the likely right answer.
**If it does not hold:** C3 stands as written, unchanged, with its executable guard.

### J5 — Gate note on the bump-source fix, for whoever designs it

Not a ruling; the trap is foreseeable and cheap to state now:

- Compare the **subject set + roleRef**, not the whole object (label/annotation
  churn would defeat it) and not `resourceVersion` (a relist can carry a new RV with
  identical content).
- **Bump when in doubt.** A missed bump is a stale RBAC scope — a correctness
  defect. A spurious bump is only waste. The asymmetry is total and the default must
  follow it.
- The falsifier must drive a **real relist** (≥2), not an installed crossed state,
  and must include a **genuine** subject-set edit that still bumps. An arm that only
  proves no-ops are skipped cannot see a real change being skipped too.

---

# G1 RE-REGISTRATION, MARGINAL FORM · 2026-09-24

## K0 — My RV claim was wrong; the advice stands on the other ground only

I wrote *"never `resourceVersion` (a relist carries a new RV with identical
content)"*. **False for the per-item `metadata.resourceVersion`**, which is the etcd
modRevision and advances only when that object is written; it is the **collection**
RV in `ListMeta` that moves on a relist. TL's two-LIST capture on 057 settles it.
Same-RV is therefore a clean detector of relist fan-out.

The recommendation — do not key the *fix* on RV — survives, but only on the second
ground I gave: label/annotation churn rewrites an object without touching RBAC, so
RV-inequality would bump on metadata-only rewrites. TL's split is better than either
of our positions: `noop_updates_total` (same RV) and `semantic_noop_updates_total`
(same subject set + roleRef, order-insensitive), with **their difference** — genuine
rewrites touching nothing RBAC-relevant — as the quantity that decides whether the
fix keys on RV or on semantics. The reordered-subject-list case TL added is the one
most likely to be got wrong and belongs in the arm.

## K1 — DO NOT retire the total-form G1. It answers a different question.

TL: *"0.217 was set against a total that is now the wrong denominator."* Correct for
the **ship** question, and **not** correct for the **attribution** question. These
are two gates, and collapsing them would retire a falsification arm before it has
reported — the failure mode ruling 1 exists to prevent.

**G1-A (ATTRIBUTION) — unchanged, still live, still 0.217.**
Measured on an admissible capture **before** the bump-source fix.
Question: *was `RBACSubGen` ever the driver of the 40.7%?*
If sub-gen-attributable excess comes in below 21.7% of resident `widgets` keys,
then the #247 attribution is wrong — and note this would also remove the
justification for the **bump-source fix itself**, since that fix is premised on the
same mechanism. **G1-A gates both fixes, not just v7.** It is not superseded by the
HOLD and must still report.

**G1-B (MARGINAL) — new, registered below.** Measured **after** the bump-source fix.
Question: *does the bump-source fix alone recover enough that the fold can wait for
1.13.0?*

## K2 — G1-B, registered

**The instrument is C5.1's own ratio.** It is already fix-agnostic: it does not care
*which* fix drove it, and its denominator — distinct logical cells on the nine-element
tuple — is moved by neither fix. Re-using it avoids inventing a second measurement
that could disagree with the acceptance criterion.

> **M = (resident_widgets_keys / distinct_logical_cells) − 1**
>
> measured on an admissible capture taken after the bump-source fix has been live
> for **at least the maximum cell lifetime observed in the pre-fix capture**
> (1,226 min ≈ 21 h on the reference pod — stated as the observed maximum, not a
> hard-coded constant, so it self-adapts to TTL/maxEntryAge config).
> The soak requirement is not ceremony: by the rate-proportional argument, pre-fix
> residue must **age out** before M measures the new steady state rather than the
> old one's remains.
>
> Today **M = 0.77** (1.77 − 1).
>
> - **M ≤ 0.05** → C5.1 is already satisfied without touching the key. v7's marginal
>   yield is negligible. **HOLD for 1.13.0 is CONFIRMED** and needs no further
>   defence.
> - **M > 0.217** → the tax survives the bump-source fix at a level comparable to
>   half the original claim. **The HOLD is re-put to TL** — bring v7 forward, or
>   accelerate 1.13.0.
> - **0.05 < M ≤ 0.217** → neither confirmed nor tripped. Report the number and
>   decide with the 1.13.0 schedule in hand; do not let silence default to HOLD.

**The 0.217 is carried, not re-derived.** It was ruled; what changed is the quantity
it is applied to, not the number. Re-deriving a ruled threshold against a new
baseline is how a pre-registration quietly becomes a post-hoc one.

**Note the trip has INVERTED, and this is the substantive part.** G1-A protected
against *shipping v7 on an overstated yield*. With v7 held, that risk is spent and
the live risk is its opposite: **holding v7 while a large tax persists.** A gate
that only ever fired in the ship direction would now be pointed at a decision
nobody is about to take. G1-B fires in the hold direction.

## K3 — Admissibility for G1-B

J2's preconditions carry over in full (`rbacSubGen > 0` share; `rbac_subgen_bumps_total`
advanced ≥ 2 across the window), plus one that is specific to the marginal form:

> **The bump-source fix must be shown to have taken.** Compare the
> `rbac_subgen_bumps_total` **rate** post-fix against pre-fix, over comparable
> windows. If the rate has not materially fallen, M is measuring a fix that did not
> land, and the capture is **INADMISSIBLE — not a confirmation**.

Without this, a fix that silently no-ops produces a **high** M, which reads as
"the tax survived, bring v7 forward" — tripping G1-B in the expensive direction on
the strength of a fix that never ran. It is J2's failure mode with the sign
reversed, and it deserves the same distinct printed verdict.

**TL's change making INADMISSIBLE a distinct printed verdict from BELOW TRIP POINT
is the right fix and should apply to all three verdicts** — CONFIRMED, RE-PUT and
INADMISSIBLE must be visually distinct in the transcript. The `--force` smoke run
that printed `0.0% excess · BELOW TRIP POINT · returns to TL` on an all-zero pod is
exactly the artifact a future reader would misread, and the reader will not have the
context that it was a code-path exercise.

## K4 — What is now registered, in one place

| gate | when | quantity | trip | decides |
|---|---|---|---|---|
| **G1-A** | before the bump-source fix | sub-gen-attributable excess / resident `widgets` keys | **< 0.217** → re-put | is the #247 attribution real *at all* — gates **both** fixes |
| **G1-B** | ≥1 max-cell-lifetime after the bump-source fix | `M = keys/cells − 1` on the 9-element tuple | **≤ 0.05** confirm HOLD · **> 0.217** re-put HOLD | may the fold wait for 1.13.0 |
| **C5.1** | after v7, whenever it ships | `keys / cells ≤ 1.05` | acceptance | did v7 do what it claimed |

G1-B and C5.1 are the **same ratio at different times**, deliberately: the criterion
that accepts v7 is the criterion that decides whether v7 is needed. One
instrument, no disagreement between them possible.

## K5 — Unchanged by any of this

The fold remains the anti-pattern (§5 prior art), a genuine RBAC change still
mints-and-orphans under DELETE-only invalidation, and the cross-tenant correctness
property sits exactly where it is until 1.13.0. **Urgency changed; the defect did
not.** C1, C2, C4–C7, G2–G5 and the Section-D re-stamp trap all carry forward to the
v7 ship whenever it lands, unchanged.

C3 is **resolved by the HOLD**: bundling the fold removal with the UAF-scope digest
in one key version dissolves the self-erasing-guard problem, because the decline
sites and the fold leave together. C3's executable-guard requirement therefore
lapses **only if** they ship in the same key version; if v7 is ever brought forward
under G1-B, **C3 revives in full**, including the AST guard.

---

# L — VANTAGE CORRECTION, CHAIN TIGHTENING, ANCHOR · 2026-09-24

## L1 — The global-dump correction: verified, and nothing of mine rested on it

Verified independently rather than accepted. `DebugApistage` calls
`store.RangeMetadata(...)` (`debug_apistage.go:98-102`) appending **every** row, and
`RangeMetadata` (`resolved.go:1535-1547`) walks `c.order` front-to-back under `c.mu`
with **no predicate of any kind**. It is a global residency dump. pm-g1-prereg is
right.

**Nothing I registered was built on the identity-scoped footing.** Checked by
search: the gate file contains no disjoint-subjects branch, no `s6-harness` scoping
claim, and J2's third reading of the zero is already worded as *"no bump has
occurred since this pod booted — a statement about **pod uptime**"*, not about one
identity. So J2 needs no retraction; the correction **strengthens** it, because
"no subject anywhere has been bumped since boot" is a stronger claim than the one
J2 was resting on. No re-read required.

## L2 — The three-link chain BREAKS my J2 precondition 2. Self-correction.

`publish_seq ≥ bumps_total ≥ minting events ≥ resident excess`. A bump mints nothing
until a `/call` from that subject computes a key afterwards.

**J2 precondition 2 as registered — `rbac_subgen_bumps_total` advanced ≥ 2 — is
insufficient**, and by exactly the mechanism the chain names. A capture can satisfy
it and still read **0% sub-gen-attributable excess** purely because no traffic
followed the bumps. That is link 2→3 failing, not link 3→4, and it would trip
**G1-A in the ship direction** — re-putting the attribution on evidence that the
attribution never had an opportunity to produce. It is the `publish_seq` error one
link down, arriving inside my own admissibility rule.

**Replacement, readable from the capture alone with no new counter:**

> **J2-2 (revised).** The resident set must carry **≥ 2 distinct `rbacSubGen`
> values** among `widgets` cells.
>
> This is the *opportunity* condition: multiplicity cannot exist unless cells were
> minted in at least two epochs. A resident set at a single sub-gen value — zero or
> non-zero — proves only that every cell was minted within one epoch, and a low
> reading from it is a statement about the window, not about `RBACSubGen`.
> `bumps_total ≥ 2` is retained as a **necessary** condition; it is no longer
> sufficient.

This is `feedback_falsifier_must_drive_real_boundary_not_install_crossed_state`
applied to a capture rather than a test: the boundary must have been crossed **in
the resident data**, not merely in a counter upstream of it.

## L3 — G1-B was delivered; it is not outstanding

The marginal re-registration is **K2 above**, sent in my previous message. Our
messages crossed. In brief, so it is not owed twice:

**M = (resident_widgets_keys / distinct_logical_cells) − 1** on the nine-element
tuple — C5.1's own ratio, reused deliberately so the criterion that accepts v7 is
the criterion that decides whether v7 is needed. Measured ≥ the observed maximum
pre-fix cell lifetime after the bump-source fix lands. Today **M = 0.77**.
**≤ 0.05** confirms the HOLD · **> 0.217** re-puts it · between, report and decide
against the 1.13.0 schedule. The trip is **inverted** — with v7 held, the live risk
is holding while a large tax persists, not shipping on an overstated one. Full text,
admissibility and the K4 table are in the file.

## L4 — #247 is closed and this gate's obligations are now anchored to a closed issue

That is C3's self-erasing-anchor problem one level up, and it needs an owner before
it bites. Every live obligation and where it must now travel:

| obligation | now lives with |
|---|---|
| **G1-A** (attribution, 0.217, pre-bump-fix) — gates **both** fixes | **#253** — it is the premise of the bump-source fix too |
| **G1-B** (marginal, M, post-bump-fix) + J2/L2 admissibility | **#253**, as the gate on its own sufficiency |
| **C5.1 / C5.2 / C5.3**, G2's three-pass enumeration | the **1.13.0 UAF-digest** ship |
| **C1, C2, C4, C6, C7**, Section-D re-stamp trap | the **1.13.0** ship |
| **C3** (lapsed while bundled; revives if v7 comes forward) | the **1.13.0** ship, with the revival condition stated |
| **#248** decline-freeze coupling (both answers owed) | the **1.13.0** ship |

**A closed issue is not a home for a live pre-registration.** Whichever of #253 or
the 1.13.0 brief is the durable one, the thresholds must be transcribed there —
`feedback_gate_evidence_must_live_on_a_pushed_ref` is the same principle for
evidence, and the reason is identical: an artifact that only exists somewhere
transient is an artifact that will not be read at the moment it matters.

---

# M — G1-B SECOND ARM: THE COST IS COLD NAVIGATIONS · 2026-09-24

## M1 — The reframing is right, and it is more serious than the issue's own headline

TL's mechanism is correct and I accept it without qualification: a bump rotates the
key, the resident cell becomes **unreachable** rather than wrong, and for an idle
subject that costs **no capacity and one cold navigation on their next visit**. A
relist touches every binding, so every subject, so **every relist costs every user
one cold navigation with no RBAC change anywhere.**

That is a `feedback_zero_cold_navigations_hard_requirement` violation and a
`feedback_l1_hit_invariant_is_100_percent` violation —
`feedback_phase6_validates_l1_always_hit` calls cold-after-mutation a DEFECT, and
this is **cold-after-nothing**, which is strictly worse.

**#247's headline framing — "40.7% of L1 is residue" — understated this by measuring
the wrong axis.** Capacity was never the customer's problem. Correct the framing in
the feature/regression journal and in anything inheriting the 40.7%.

**One aggravation neither of us has stated.** The refresher re-Puts from the
**stored** Inputs, carrying the **old** sub-gen (`resolve_populate.go:116`). So it
actively keeps the **unreachable** cell fresh while the user pays cold for the
reachable key. That is refresher budget spent maintaining a cell no request can
ever hit — an inversion of `feedback_customer_priority_over_refresher`, and a third
observable worth capturing: refresh work on keys no longer reachable.

## M2 — M cannot see this, and TL's diagnosis of why is right

`M` is a residency ratio. Cold-navigation cost and residue are **decoupled in both
directions**: short TTLs give cold navs at every relist with M near 1.0; a long-idle
population gives high M with no cold navs at all. M stays the correct gate for the
residue question and is the wrong instrument for whether the tax was real. **Add an
arm; do not change M** — the reason G1-B and C5.1 are the same ratio is that they
must not be able to disagree, and re-pointing M would break that.

## M3 — The arm as proposed inherits a MIXED counter. It must be a join.

`dispatch_l1_lookups` miss_total aggregates genuine cold-fills, evictions, TTL
expiry **and** sub-gen rotation. A miss spike at a relist is suggestive, not
attributable — and this programme has been bitten by exactly this shape before
(`feedback_measurement_use_expvar_not_log_tails`: *fallthrough_total is MIXED*).

**Register the arm as a conjunction, not a counter** — the #249 precedent in this
repo's own words, *"field plus join is a kill; the field alone would not have
been"*:

> **G1-B ARM 2 — relist-induced cold navigation.** Across a window containing
> **≥ 2 real relists** (`feedback_falsifier_must_drive_real_boundary_not_install_crossed_state`
> — observed relists, never an induced single one), assert the conjunction:
>
> 1. `dispatch_l1_lookups` miss rate for class `widgets` **spikes in temporal
>    alignment with each relist**, against a matched no-relist window; **and**
> 2. in that same window `semantic_noop_updates_total` advances while genuine
>    subject-set changes do **not** — i.e. nothing RBAC-relevant changed; **and**
> 3. the resident set shows ≥ 2 distinct `rbacSubGen` values at the affected
>    logical cells (L2's opportunity condition, read from the same capture).
>
> (1) alone is a mixed counter. (2) alone is upstream of residency — the
> three-link chain. **(1) ∧ (2) ∧ (3) attributes the misses to rotation** without a
> new hot-path counter and without an index on the miss path.

**Limitation sentence, stated first and read back as a ship decision** (G4):

> *"This arm shows that misses rose when bindings were rewritten without RBAC
> changing, at cells that had rotated. It is read from expvar on `/debug/vars` plus
> the `/debug/apistage` walk — both live at `LOG_LEVEL=warn`. It cannot separate a
> rotation miss from a co-timed eviction on any single request, and it measures a
> **rate**, not a user's wall-clock."*

Read back: **ships as a mechanism arm, and only as a mechanism arm.**

## M4 — Therefore it needs a wall-clock companion, or it repeats 0.30.221

A cold navigation is a **user-visible latency event**, and
`feedback_bench_metrics_must_predict_customer_wall_clock` is explicit that a
mechanism metric is not the customer's clock. A hit-rate arm that stands alone would
be the 0.30.221 mistake in a new place.

> **G1-B ARM 3 — the north-star form.** A **Chrome** navigation through the portal
> ingress (Dashboard piechart + Compositions datagrid, 0.95 cyberjoker / 0.05 admin)
> taken **immediately after an observed relist**, asserting `l1:HIT` + `key_hash` on
> the served cells. `feedback_curl_probes_inadmissible_for_seed_hit_acceptance`
> binds: curl is inadmissible for this.
>
> **A cold navigation here is the whole finding, in the only unit that counts.**

## M5 — Which way this cuts: it REINFORCES the HOLD. Do not read it the other way.

The reframing makes the defect more serious, so the reflex is "urgent — bring v7
forward". **That reflex is wrong, and the reason matters:**

- **#253 removes the cold navigations entirely, with no key change.** A relist that
  does not bump does not rotate; no rotation, no orphan, no cold nav.
- After #253, the surviving rotations are the ones driven by **genuine** RBAC
  changes — tens per day — and on a genuine RBAC change **a cold miss is CORRECT**.
  That is #118 (c) working exactly as designed: new key → cold miss → fresh UAF
  refilter.
- So v7's marginal benefit **on the cold-navigation axis is approximately zero**,
  and arguably negative in isolation: removing the fold means a genuine RBAC change
  no longer rotates the key, and the revalidate-on-read of §6.1/Option B is what
  must replace that protection. Removing the fold *without* that replacement working
  is a correctness regression, not a latency win.

**Registered reading of Arm 2 / Arm 3:** a high cold-navigation rate **pre-#253**
raises **#253's** priority — potentially above everything else in this gate, since
it is a live north-star violation on the customer path. It does **not** raise v7's.
G1-B's bands (`M ≤ 0.05` confirm · `M > 0.217` re-put) stand unchanged for the
residue question, and Arm 2/Arm 3 decide whether the tax was real — which is TL's
framing and is correct.

## M6 — One consequence for #253's own acceptance

If the cold navigation is the real cost, then **#253's acceptance criterion is not a
counter at all** — it is Arm 3. `noop_updates_total` going to zero proves the bump
stopped; only a Chrome navigation across a relist proves the **user** stopped paying
for it. Register Arm 3 as #253's acceptance, with the counters as its mechanism
evidence, not its proof.
