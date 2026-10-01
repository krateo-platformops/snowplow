// shadow_parity_hook.go — v7 Step 2D: the DARK shadow-parity hook, wired onto
// the LIVE EvaluateRBAC path, running DARK.
//
// WHAT STEP 2D ADDS. Step 2A built the all-namespace reverse index; 2B the
// requester profile R + memo; 2C the pure projection P, digest, and shareable /
// name-ambiguity classification. NOTHING was wired to a serving path. Step 2D
// wires it — still DARK:
//
//   - dispatcher entry (restactions.go / widgets.go) installs a READ-ONLY shadow
//     context on the resolve ctx: the cell's identity-free access domain D, this
//     requester's identity, the projection digest, and the cell's shareable
//     classification. The D-derivation is recover-isolated.
//   - EvaluateRBAC's defer (evaluate.go) calls runShadowParityHook after every
//     served check where snap != nil && err == nil — memo-hit permits and walk
//     permit/deny. The hook builds R from the CHECK's own snapshot (no
//     generation skew), compares R.Permits(opts) to the live verdict, checks
//     D-coverage of the check's coordinate, and detects the name-ambiguous
//     projection leak — bumping counters only.
//
// DARK INVARIANT (load-bearing). The hook and the entry-side derivation change
// NO verdict, NO served byte, and NO cache key. cache.ComputeKey is untouched.
// Everything is gated on the observability toggle (rbac.ShadowParityEnabled,
// default-off) AND cache-on. Both carriers — the hook body and the inline
// D-derivation — are recover-isolated: a dark panic bumps dark_panic_total and
// is swallowed, never propagated. Identity never leaves this package raw: the
// counters are process-wide scalars (no labels, no identity), and the anomaly
// log carries only the non-identity coordinate {verb,group,resource,scope-kind}
// plus a HASHED identity (sha256(username) prefix + canonical groups hash).

package dispatchers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"expvar"
	"log/slog"
	"sync"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func init() {
	// Register the dark hook into package rbac. This is atomic.Pointer WIRING,
	// NOT an expvar surface — it must run in EVERY mode so a test (or production
	// after the toggle flips) has the hook available. It stays inert until the
	// toggle is on AND a served check carries a shadow context, and it no-ops
	// cache-off because EvaluateRBAC returns with snap==nil there. So it is
	// deliberately BEFORE the CFG-1 gate below.
	rbac.SetShadowHook(runShadowParityHook)

	// CFG-1: the shadow-parity expvar surface (snowplow_v7_shadow_parity) is a
	// CACHE-ONLY measurement — meaningless cache-off (no snapshot, no digest, no
	// served check to compare) — so its registration MUST NOT publish under
	// cache-off. Mirrors the gate every other cache-metric init uses (e.g.
	// l1_lookup_metrics.go); structurally asserted by TestCFG1_Structural.
	if cache.Disabled() {
		return
	}
	registerShadowParityMetrics()
}

// ─────────────────────────────────────────────────────────────────────────
// Counters. Process-wide scalars — NO labels, NO identity, NO body (the
// safest reading of the debug-surface rule + PM condition 6's bounded
// cardinality). The three anomaly counters also emit a hashed-identity Debug
// log (logShadowAnomaly) carrying the non-identity coordinate.
// ─────────────────────────────────────────────────────────────────────────

var (
	// shadowChecksTotal — the served-path denominator: EvaluateRBAC checks that
	// carried a shadow context (F-H5's LHS). Its independent RHS is
	// rbac.EvaluateRBACCallCount() in a hermetic served-only window.
	shadowChecksTotal atomic.Uint64
	// shadowChecksAllowTotal / shadowChecksDenyTotal — the dark allow/deny split of
	// checks_total (1.12.18). Partitioned on the hook's own `allowed` param (the
	// live EvaluateRBAC verdict): bumped once per served check, immediately after
	// checks_total, so allow+deny == checks_total holds unconditionally. Pure
	// instrument — no gate, no v7-key certification (that is Step 2E).
	shadowChecksAllowTotal atomic.Uint64
	shadowChecksDenyTotal  atomic.Uint64
	// shadowVerdictMismatchTotal — R.Permits(opts) != live allowed (F-verdict).
	shadowVerdictMismatchTotal atomic.Uint64
	// shadowCoverageMissTotal — the check's coordinate is covered by no class in
	// the cell's D (F-coverage; catches the ${._getpath} runtime blind spot).
	shadowCoverageMissTotal atomic.Uint64
	// shadowProjectionNameAmbiguousLeakTotal — a name-carrying real check is
	// covered ONLY by name-free classes in a cell the classification treated as
	// shareable (F-proj; the runtime proof §2.4 fires).
	shadowProjectionNameAmbiguousLeakTotal atomic.Uint64
	// shadowDarkPanicTotal — a panic in EITHER carrier (hook body or inline
	// D-derivation) was recovered (F-panic).
	shadowDarkPanicTotal atomic.Uint64

	// Step 2E (#275) — populate-side (boot-seed) instrument. installs counts the
	// seed resolves that successfully carried a shadow context (INVARIANT b: bumped
	// only after the shadowContext is on ctx). The four buckets partition those
	// installs exactly — b1+b2+b3+b4 == installs (the sum-integrity invariant, the
	// false-green guard): a silently-missed resolve makes the sum fall short.
	shadowPopulateSeedInstallsTotal     atomic.Uint64
	shadowPopulateAllowBearingTotal     atomic.Uint64 // b1: dGated & allowsInResolve≥1
	shadowPopulateRBACDeniedTotal       atomic.Uint64 // b2: dGated & checks≥1 & allows==0
	shadowPopulateRBACGatedNoCheckTotal atomic.Uint64 // b3: dGated & checksInResolve==0
	shadowPopulatePassthroughTotal      atomic.Uint64 // b4: NOT dGated
)

var shadowMetricsOnce sync.Once

func registerShadowParityMetrics() {
	shadowMetricsOnce.Do(func() {
		expvar.Publish("snowplow_v7_shadow_parity", expvar.Func(func() any {
			// Cache-only measurement counters (gated off cache-off). The toggle
			// STATE/SOURCE is published separately + UNGATED in
			// shadow_parity_toggle_metrics.go (#367) so a latency window can read it
			// regardless of CACHE_ENABLED.
			return map[string]uint64{
				"checks_total":                         shadowChecksTotal.Load(),
				"checks_allow_total":                   shadowChecksAllowTotal.Load(),
				"checks_deny_total":                    shadowChecksDenyTotal.Load(),
				"verdict_mismatch_total":               shadowVerdictMismatchTotal.Load(),
				"coverage_miss_total":                  shadowCoverageMissTotal.Load(),
				"projection_name_ambiguous_leak_total": shadowProjectionNameAmbiguousLeakTotal.Load(),
				"dark_panic_total":                     shadowDarkPanicTotal.Load(),
				"populate_seed_installs_total":         shadowPopulateSeedInstallsTotal.Load(),
				"populate_allow_bearing_total":         shadowPopulateAllowBearingTotal.Load(),
				"populate_rbac_denied_total":           shadowPopulateRBACDeniedTotal.Load(),
				"populate_rbac_gated_no_check_total":   shadowPopulateRBACGatedNoCheckTotal.Load(),
				"populate_passthrough_total":           shadowPopulatePassthroughTotal.Load(),
				// #368 — wildcard digest-collision probe. collision/observed are the
				// DETECTOR + denominator (also on OTLP, metrics.go); the gated counter
				// is an expvar-only diagnostic (expected non-zero until enumerate lands).
				"wildcard_digest_collision_total":       shadowWildcardDigestCollisionTotal.Load(),
				"wildcard_digest_observed_total":        shadowWildcardDigestObservedTotal.Load(),
				"wildcard_digest_evicted_total":         shadowWildcardDigestEvictedTotal.Load(),
				"wildcard_gated_digest_collision_total": shadowWildcardGatedDigestCollisionTotal.Load(),
			}
		}))
	})
}

// ─────────────────────────────────────────────────────────────────────────
// The shadow context. Carried on the resolve ctx by pointer so the hook can
// mark the resolve's digest untrusted in place.
// ─────────────────────────────────────────────────────────────────────────

type shadowCtxKeyType struct{}

// shadowCtxKey is a private type-tagged key: a foreign value under it can never
// collide (comma-ok on the concrete type returns cleanly — F-panic ctx arm).
var shadowCtxKey = shadowCtxKeyType{}

// shadowContext is the read-only cell state the hook consults, plus a mutable
// untrusted marker. domain / identity / digest / shareable are computed once at
// dispatcher entry; untrusted is set by the hook (or by a recovered panic).
type shadowContext struct {
	domain    AccessDomain
	identity  rbac.EvaluateOptions // Username + Groups only
	digest    string
	shareable bool
	// wildcardGated mirrors Projection.WildcardGated for this cell (#368): true iff
	// D's digest incorporated a gated ClassWildcard (AnswerWildcardGated). The
	// wildcard digest-collision probe routes gated cells to an expvar-only
	// diagnostic and the real detector counts only Shareable && !wildcardGated.
	wildcardGated bool
	untrusted     atomic.Bool

	// Step 2E (#275) — per-resolve accumulation for the populate-side classify.
	// Bumped by runShadowParityHook alongside the process-wide checks/allow/deny
	// counters, but scoped to THIS resolve's shadowContext; read ONCE at seedOne*
	// return by classifyShadowSeedResolve. Atomic ⇒ safe under the resolve's
	// errgroup inner-call fan-out (the goroutines share this pointer via the ctx).
	// Inert on the serve path: nobody classifies there and the fields die with the
	// ctx. dGated is stamped once at seed install (D has ≥1 non-escape class).
	checksInResolve atomic.Uint64
	allowsInResolve atomic.Uint64
	dGated          bool
}

// shadowProfileFor is the requester-profile accessor seam. Production points at
// the memoised rbac.RequesterProfileFor; the F-verdict RED arm swaps it for a
// deliberately divergent builder to prove the hook detects a mismatch.
var shadowProfileFor = rbac.RequesterProfileFor

// Test-only panic injectors (F-panic). Package-private; set by in-package tests.
var (
	shadowHookPanicForTest   atomic.Bool
	shadowDerivePanicForTest atomic.Bool
)

// ─────────────────────────────────────────────────────────────────────────
// Dispatcher-entry installation (carrier #2 — recover-isolated).
// ─────────────────────────────────────────────────────────────────────────

// installShadowParityRESTAction installs the dark shadow context for a
// RESTAction dispatch. No-op (returns ctx unchanged) when the toggle is off or
// cache is off (PM condition 5 — no wasted dark work at cache-off).
func installShadowParityRESTAction(ctx context.Context, ra *templatesv1.RESTAction) context.Context {
	if !rbac.ShadowParityEnabled() || cache.Disabled() {
		return ctx
	}
	return installShadowParity(ctx, func() AccessDomain {
		return DeriveRESTActionAccessDomain(ra, NilChainResolver)
	})
}

// installShadowParityWidget installs the dark shadow context for a widget
// dispatch. Same gates as the RESTAction twin.
func installShadowParityWidget(ctx context.Context, widget map[string]any) context.Context {
	if !rbac.ShadowParityEnabled() || cache.Disabled() {
		return ctx
	}
	return installShadowParity(ctx, func() AccessDomain {
		return DeriveWidgetAccessDomain(widget, NilChainResolver)
	})
}

// installShadowParity derives D (via the caller's closure), builds R for the
// requester, computes the digest, and returns a ctx carrying the shadow context.
//
// RECOVER-ISOLATED (carrier #2). The whole body — the inline D-derivation and
// the R build — is wrapped so a panic bumps dark_panic_total and returns the
// ORIGINAL ctx UNCHANGED (named return `out`, defaulted to ctx). A nil ctx must
// never escape: on any early return or panic the resolve proceeds exactly as
// shadow-off (no shadow context ⇒ the hook no-ops).
func installShadowParity(ctx context.Context, derive func() AccessDomain) (out context.Context) {
	out = ctx
	defer func() {
		if r := recover(); r != nil {
			shadowDarkPanicTotal.Add(1)
			out = ctx // never return nil / a partial ctx
		}
	}()

	if shadowDerivePanicForTest.Load() {
		panic("injected: shadow D-derivation")
	}

	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return ctx
	}
	id := rbac.EvaluateOptions{Username: ui.Username, Groups: ui.Groups}

	d := derive()

	rw := cache.Global()
	if rw == nil {
		return ctx
	}
	snap := rw.Snapshot()
	if snap == nil {
		return ctx
	}

	r := shadowProfileFor(snap, id)
	digest, proj := ComputeProjectionDigest(r, d)

	return context.WithValue(ctx, shadowCtxKey, &shadowContext{
		domain:        d,
		identity:      id,
		digest:        digest,
		shareable:     Shareable(d),
		wildcardGated: proj.WildcardGated, // #368 — route gated cells to the diagnostic
	})
}

// ─────────────────────────────────────────────────────────────────────────
// Seed-path installation + classification (Step 2E, #275 — the populate side).
//
// The #266 shadow context installs ONLY at the two SERVE entries, so the boot
// seed resolves are off-hook — a boot walk fires ZERO checks for the COHORT
// (populator) identities that KEY L1 cells (the v7-key #254 leak vector). 2E
// installs the same dark context on the seed resolve ctx (which already carries
// the cohort identity via withCohortSeedContext) and classifies each completed
// resolve into one of four buckets. Reuses the serve-path install VERBATIM; the
// only new logic is the install-count + dGated stamp and the classify switch.
// ─────────────────────────────────────────────────────────────────────────

// installShadowParitySeedRESTAction installs the dark shadow context for a
// boot-SEED RESTAction resolve, over the serve-path installShadowParityRESTAction.
func installShadowParitySeedRESTAction(ctx context.Context, ra *templatesv1.RESTAction) context.Context {
	return finishSeedInstall(installShadowParityRESTAction(ctx, ra))
}

// installShadowParitySeedWidget — the widget twin.
func installShadowParitySeedWidget(ctx context.Context, widget map[string]any) context.Context {
	return finishSeedInstall(installShadowParityWidget(ctx, widget))
}

// finishSeedInstall stamps dGated (D has ≥1 non-escape class) and bumps
// populate_seed_installs_total IFF the wrapped serve-path install actually put a
// shadowContext on out — INVARIANT b: only after a successful install, so a
// derive-panicked resolve stays OUT of installs AND all four buckets (its
// dark_panic_total>0 is the backstop; sum-integrity preserved). Off (toggle/cache)
// ⇒ no shadowContext ⇒ no bump ⇒ classify no-ops. Returns out unchanged.
func finishSeedInstall(out context.Context) context.Context {
	if sc, ok := out.Value(shadowCtxKey).(*shadowContext); ok && sc != nil {
		sc.dGated = sc.domain.HasClasses()
		shadowPopulateSeedInstallsTotal.Add(1)
	}
	return out
}

// classifyShadowSeedResolve buckets ONE completed seed resolve into exactly one
// of the four populate counters. It MUST run at seedOne* return, AFTER the resolve
// has fully returned (errgroup joined — INVARIANT a): reading the per-resolve
// atomics before the inner-call fan-out joins would misclassify. No shadow context
// (toggle/cache off, or a derive-panicked install) ⇒ no-op. Comma-ok read — never
// panics on a foreign ctx value.
func classifyShadowSeedResolve(ctx context.Context) {
	sc, ok := ctx.Value(shadowCtxKey).(*shadowContext)
	if !ok || sc == nil {
		return
	}
	allows := sc.allowsInResolve.Load()
	checks := sc.checksInResolve.Load()
	switch {
	case !sc.dGated:
		shadowPopulatePassthroughTotal.Add(1) // b4 — no gated class; nothing to certify.
	case allows >= 1:
		shadowPopulateAllowBearingTotal.Add(1) // b1 — a live-serve allow direction exists.
	case checks >= 1: // dGated & checks≥1 & allows==0
		shadowPopulateRBACDeniedTotal.Add(1) // b2 — every check denied for this cohort.
	default: // dGated & checks==0
		shadowPopulateRBACGatedNoCheckTotal.Add(1) // b3 — memo hit or iterator-empty.
	}
}

// ─────────────────────────────────────────────────────────────────────────
// The dark hook (carrier #1 — recover-isolated).
// ─────────────────────────────────────────────────────────────────────────

// runShadowParityHook is the registered rbac.ShadowHookFunc. It runs inside
// EvaluateRBAC's defer, after the live verdict is decided, on served checks that
// carry a shadow context. READ-ONLY: it never mutates any EvaluateRBAC return.
//
// RECOVER-ISOLATED (carrier #1). The whole body is wrapped: a panic bumps
// dark_panic_total, marks the resolve's digest untrusted (comma-ok read; never
// panics), and is swallowed. The live verdict already returned; the request is
// unaffected.
func runShadowParityHook(ctx context.Context, snap *cache.RBACSnapshot, opts rbac.EvaluateOptions, allowed bool) {
	defer func() {
		if r := recover(); r != nil {
			shadowDarkPanicTotal.Add(1)
			markShadowUntrusted(ctx)
		}
	}()

	sc, ok := ctx.Value(shadowCtxKey).(*shadowContext)
	if !ok || sc == nil {
		return // no shadow context on this check (e.g. the dispatch-gate check).
	}

	if shadowHookPanicForTest.Load() {
		panic("injected: shadow hook body")
	}

	// checks_total — the served-path denominator (F-H5). One bump per served
	// EvaluateRBAC check that carried a shadow context.
	shadowChecksTotal.Add(1)

	// Dark allow/deny split (1.12.18). Attribute this check to the live verdict
	// (`allowed` — the exact bool compared against R.Permits below). Placed
	// immediately after checks_total with nothing between, so the partition
	// invariant allow+deny == checks_total can never diverge.
	if allowed {
		shadowChecksAllowTotal.Add(1)
	} else {
		shadowChecksDenyTotal.Add(1)
	}

	// Step 2E (#275) — per-resolve accumulation for the populate-side classify.
	// Same partition as the process-wide split, but scoped to THIS resolve's
	// shadowContext so classifyShadowSeedResolve can bucket the seed at seedOne*
	// return. Serve path: harmless — nothing classifies there and these die with
	// the ctx.
	sc.checksInResolve.Add(1)
	if allowed {
		sc.allowsInResolve.Add(1)
	}

	// R from the CHECK's own snapshot (design V10: the hook builds R from the
	// check's snap, so generation skew is impossible). Memoised.
	r := shadowProfileFor(snap, sc.identity)
	if r.Permits(opts) != allowed {
		shadowVerdictMismatchTotal.Add(1)
		sc.untrusted.Store(true)
		logShadowAnomaly(ctx, "verdict_mismatch", opts)
	}

	// D-coverage: is the check's coordinate anticipated by any class in the
	// cell's D? An uncovered check means the digest did not account for this
	// access — the ${._getpath} runtime blind spot (F-coverage).
	if !domainCoversCheck(sc.domain, opts) {
		shadowCoverageMissTotal.Add(1)
		sc.untrusted.Store(true)
		logShadowAnomaly(ctx, "coverage_miss", opts)
	}

	// Name-ambiguous projection leak: a name-carrying real check covered ONLY by
	// name-free classes, in a cell the classification treated as shareable. The
	// structural rule (§2.4) makes this impossible in the correct build (such a
	// cell is classified not-shareable); the counter is the runtime proof it
	// fires (F-proj).
	if projectionNameAmbiguousLeak(sc, opts) {
		shadowProjectionNameAmbiguousLeakTotal.Add(1)
		sc.untrusted.Store(true)
		logShadowAnomaly(ctx, "projection_name_ambiguous_leak", opts)
	}

	// #368 — the 4th dark detector: the cross-identity wildcard digest-collision
	// probe. Independent of the three above (it uses the EVALUATOR, not the
	// projection, for the access-hash) so it catches the wrong-wildcard-projection
	// leak the three are structurally blind to. Dark: counters only, inside this
	// function's recover, wildcard-bearing cells only.
	wildcardProbe.observe(snap, sc, opts)
}

// markShadowUntrusted marks this resolve's digest untrusted, if a shadow context
// is present. Comma-ok read — never panics (F-panic ctx arm).
func markShadowUntrusted(ctx context.Context) {
	if sc, ok := ctx.Value(shadowCtxKey).(*shadowContext); ok && sc != nil {
		sc.untrusted.Store(true)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Coverage + name-ambiguous-leak predicates. Pure functions of (D, opts).
// ─────────────────────────────────────────────────────────────────────────

// fieldMatch reports whether a class field (which may be the rule wildcard "*")
// covers a check's concrete value.
func fieldMatch(classVal, checkVal string) bool {
	return classVal == AccessWildcard || classVal == checkVal
}

// classCoversCheck reports whether class c anticipates the check's
// (verb, group, resource) coordinate. Coverage is coordinate-level (the digest
// records P per class at the (v,g,r) granularity); namespace/name refinement is
// what the projection's per-answer value captures, not coverage.
func classCoversCheck(c AccessClass, opts rbac.EvaluateOptions) bool {
	return fieldMatch(c.Verb, opts.Verb) &&
		fieldMatch(c.Group, opts.Group) &&
		fieldMatch(c.Resource, opts.Resource)
}

// domainCoversCheck reports whether any class in D covers the check's
// coordinate. Escapes are NOT coverage — a non-read escape step is dispatched
// with the requester token and makes no in-process EvaluateRBAC check.
func domainCoversCheck(d AccessDomain, opts rbac.EvaluateOptions) bool {
	for i := range d.Classes {
		if classCoversCheck(d.Classes[i], opts) {
			return true
		}
	}
	return false
}

// projectionNameAmbiguousLeak reports the runtime name-fidelity leak condition
// (design §2.5): a name-carrying real check (name-specific verb + real Name)
// whose covering classes are ALL name-free (nameAmbiguous), in a cell the
// classification treated as shareable. In the correct build such a cell is
// classified not-shareable (sc.shareable == false via Shareable, which flags any
// name-ambiguous class), so this never fires — it is the empirical guard that
// §2.4 actually closed the leak.
func projectionNameAmbiguousLeak(sc *shadowContext, opts rbac.EvaluateOptions) bool {
	if opts.Name == "" || !rbac.IsNameSpecificVerb(opts.Verb) {
		return false // not a name-carrying per-object check.
	}
	if !sc.shareable {
		return false // cell correctly flagged not-shareable ⇒ structurally safe.
	}
	covered := false
	for i := range sc.domain.Classes {
		c := sc.domain.Classes[i]
		if !classCoversCheck(c, opts) {
			continue
		}
		covered = true
		if !nameAmbiguous(c) {
			return false // a name-pinned class covers it ⇒ projection is faithful.
		}
	}
	// A shareable cell whose only covering classes are name-free, for a
	// name-carrying check ⇒ the digest under-reports a resourceNames grant.
	return covered
}

// ─────────────────────────────────────────────────────────────────────────
// Hashed-identity anomaly log (§4.4). Coordinate + hashed identity only.
// ─────────────────────────────────────────────────────────────────────────

func checkScopeKind(opts rbac.EvaluateOptions) string {
	switch {
	case opts.Name != "":
		return "name"
	case opts.Namespace != "":
		return "namespaced"
	default:
		return "cluster"
	}
}

// hashUsername returns a short sha256 hex prefix of the username. Never the raw
// username.
func hashUsername(username string) string {
	sum := sha256.Sum256([]byte(username))
	return hex.EncodeToString(sum[:8])
}

// logShadowAnomaly emits a Debug line for one anomaly. It carries ONLY the
// non-identity coordinate {verb,group,resource,scope-kind} and a HASHED identity
// (sha256(username) prefix + the canonical groups hash) — never raw
// username/groups/name, never a body (the debug-surface rule).
func logShadowAnomaly(ctx context.Context, reason string, opts rbac.EvaluateOptions) {
	log := xcontext.Logger(ctx)
	log.Debug("v7.shadow_parity anomaly (dark)",
		slog.String("reason", reason),
		slog.String("verb", opts.Verb),
		slog.String("group", opts.Group),
		slog.String("resource", opts.Resource),
		slog.String("scope_kind", checkScopeKind(opts)),
		slog.String("user_hash", hashUsername(opts.Username)),
		slog.Uint64("groups_hash", rbac.CanonicalGroupsHash(opts.Groups)),
	)
}
