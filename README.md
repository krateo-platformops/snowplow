# Design records — v7 key and #247/#253 gates

This orphan branch holds design and gate records that previously lived only in untracked or temporary scratchpads, at risk of being lost. Per the rule that gate evidence must live on a pushed ref, they are preserved here verbatim. Nothing on this branch is built, merged or deployed.

| file | what it is | where it lived | written |
|---|---|---|---|
| `design-keyv7-unified.md` | The only written design for the v7 key, including the UAF-scope digest that removes the 1.12.3 `declineUAFPut` (see #180, #254) | untracked `scratchpad/` in a worktree | 2026-09-18 |
| `design-key-multiplicity.md` | The #247 key-multiplicity design (RBACSubGen fold, C-conditions) | session `/private/tmp` scratchpad | 2026-09-23/24 |
| `gate-key-multiplicity-pm.md` | The PM gate record for #247/#254 (G1, C1–C7, C5.1–C5.3, G2, re-registrations) | session `/private/tmp` scratchpad | 2026-09-24/25 |

## Read these with the later corrections

These records predate findings made on 2026-09-25. Several of their premises have since been corrected on the issues:

- **#253 amendment 2** (https://github.com/krateo-platformops/snowplow/issues/253#issuecomment-5832256913): the Role/ClusterRole rotation path, no RBAC-triggered re-seed, orphan retention, and the corrected decision gate.
- **#254** (https://github.com/krateo-platformops/snowplow/issues/254#issuecomment-5833056717): measured on 057, the UAF decline keeps ~95% of portal `widgets` calls out of L1 for every user. On this evidence the UAF-scope digest, not the RBACSubGen fold, is the high-value half of v7.
- Owner facts (2026-09-25): namespace-scoped RBAC is the tenant isolation model; IdP group cardinality per user is unknown.

None of these records was written against those facts.
