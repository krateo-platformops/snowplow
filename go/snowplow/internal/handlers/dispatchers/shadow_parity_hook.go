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
	// Register the dark hook into package rbac. This runs whenever the
	// dispatchers package is imported (always, in production). It is DARK until
	// the toggle is turned on — EvaluateRBAC's defer no-ops on
	// !rbac.ShadowParityEnabled().
	rbac.SetShadowHook(runShadowParityHook)
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
)

var shadowMetricsOnce sync.Once

func registerShadowParityMetrics() {
	shadowMetricsOnce.Do(func() {
		expvar.Publish("snowplow_v7_shadow_parity", expvar.Func(func() any {
			return map[string]uint64{
				"checks_total":                         shadowChecksTotal.Load(),
				"verdict_mismatch_total":               shadowVerdictMismatchTotal.Load(),
				"coverage_miss_total":                  shadowCoverageMissTotal.Load(),
				"projection_name_ambiguous_leak_total": shadowProjectionNameAmbiguousLeakTotal.Load(),
				"dark_panic_total":                     shadowDarkPanicTotal.Load(),
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
	untrusted atomic.Bool
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
	digest, _ := ComputeProjectionDigest(r, d)

	return context.WithValue(ctx, shadowCtxKey, &shadowContext{
		domain:    d,
		identity:  id,
		digest:    digest,
		shareable: Shareable(d),
	})
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
