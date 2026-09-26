# v7 Step 2 — PM GATE VERDICT: PASS-WITH-CONDITIONS (2026-09-26)

Gate on the architect design (`s2-design.md` in this dir; also on the `design-records` branch). Every blocker/major from both adversarial reviews resolved (not partially); load-bearing code claims verified against origin/main (a6b9d348) and origin/feat/v7-access-domain-deriver. The name-fidelity leak is closed STRUCTURALLY (non-shareable classification), not merely detected. Does NOT authorize code beyond the diff-reviewed build order; each sub-step needs its own diff review + explicit ACK.

## Per-checklist: all PASS
1. Dark-safety airtight — recover() around BOTH the hook and the inline D-derivation (matches the existing rebuild-goroutine pattern rbac_snapshot.go:324-333); comma-ok ctx read; defer no-ops correctly on cache-off / nil-snap / error; latency vs correctness windows separated.
2. Parity unlock sound & FAIL-CLOSED — two-part (offline completeness over 057 corpus + runtime confirmation); any coordinate neither offline-verified nor runtime-exercised = digest-untrusted. Shadow-parity validates P not just R; name-specific-verb/name-free-class ⇒ non-shareable (leak impossible even on unexercised paths).
3. Falsifier coverage — F-D2div is the divergence arm (naive R allows where EvaluateRBAC denies → RED); F-2mutant the control matrix; F-C0 the routeRBSubjects behavior-preserving arm; every hot-path .go edit has an arm.
4. Invariants — debug surface emits hashed identity only (canonicalGroupsHash + sha256(username)), no raw identity/body; index ~4MB at 63K RBs, additive; write-site enumeration verified (authz maps written only in rebuildSubjectIndexes).
5. De-risks Step 3 — passing establishes R==EvaluateRBAC over the domain + D-coverage + name-faithful-or-non-shareable P; residuals fail-closed.
6. Build order — A(index+routeRBSubjects) → B(rulesForRef split+R+memo) → C(P+digest+shareable) → D(dark hook) → E(measure); each independently dark+additive, one SHA on a clean tested tree, explicit ACK.

## Six conditions the dev must honor
1. F-H5's RHS (`checks_total == served-path EvaluateRBAC count`) must be an INDEPENDENT instrument, else tautological.
2. Name the hook-enablement mechanism: an observability toggle, default-off, structurally unable to persist into a latency-acceptance window — NOT a magic env knob.
3. Measure the unconditional defer overhead with hook=nil on the ~160M-hit path (step E latency run, hook off); a regression triggers the explicit-call fallback (hooks at evaluate.go:249/:301 instead of a defer).
4. #265 (deriver) is a hard dependency for steps C/D/E; A/B can start first.
5. Gate the shadow subsystem on cache.Enabled() (skip wasted dark work at cache-off).
6. routeRBSubjects extraction spans evaluate.go:512-543 (not just :527-543); F-C0 guards it.

## NOT signed off (residuals carried to Step 3)
- No latency-neutrality number (step E deliverable; Step 3 promotion gated on it).
- BindingUID must NOT be derived from R in Step 3 (order-dependent first-match; R yields only the boolean).
- §2.4 conservatism: genuinely name-free cells (widget resourcesRefs via UserCan, Name="") are sidelined from sharing; per-origin NameThreaded refinement is a #265/Step-3 decision. Step 2 measures v7_cell_not_shareable_total{reason}.
- The "exactly 1 get-UAF on 057" data point is architect-attested, not PM-re-verified; F-proj stands synthetically regardless.

Bottom line: build it, in order, honoring the six conditions.
