// learned_identity_seed.go — #262: seed LEARNED identity classes so a user
// whose binding set no representative shares is warm too.
//
// THE GAP (since #423). A resolved-output key folds the requester's FULL
// binding-set digest. The engine seeds one cell set per binding representative
// (cache.EnumeratePrewarmTargetsForGVR), so a user with one extra binding — a
// User-subject RoleBinding next to their group's — derives a key nothing ever
// seeds: cold first navigation after every restart and every rotation.
//
// THE CLASSES come from cache.LearnedIdentitiesSnapshot (authn's clientconfig
// certificates + live traffic; see cache/learned_identities.go).
//
// ONE WRAPPER over the target enumeration (enumeratePrewarmTargetsForGVRFn)
// covers every seed path — boot, keepwarm, gvr-discovered and the #258 reseed.
// For a GVR it emits a learned class only if it is
//
//   - AUTHORISED: a binding in that GVR's bucket names one of its subjects
//     (cache.IdentityAuthorisedForGVR), and
//   - DISTINCT: its (binding-set digest, RBACSubGen over the effective groups)
//     differs from every representative's in the bucket and from every learned
//     class already emitted. Equal (digest, sub-gen) means an equal key (the
//     first-match BindingUID is a function of the binding set), so a dropped
//     class is seeded by the identity it equals: LOSSLESS dedup.
//
// Both are computed LIVE on every call from the published snapshot and index;
// nothing about the dedup is memoised (the digest itself is the key's own
// per-snapshot memo, rbac.SubjectBindingSetDigest).
//
// THE BOUND (self-adapting, no env knob). Learned classes are seeded newest
// LastSeen first, until the first of two measured capacities is reached:
//
//   - ENGINE: every keepwarm cycle must re-resolve every seeded unit before the
//     next one, so Σ (W_c·t_widget + R_c·t_ra) ≤ keepwarmSweepInterval −
//     (W_base·t_widget + R_base·t_ra), with t_widget / t_ra the measured mean
//     wall cost of a resolved widget / RESTAction seed unit (kept apart: an RA
//     can cost far more than an informer-served widget) and W/R the class's (or
//     the base cohorts') units of each kind in this pass. Until a unit has been
//     measured no class is admitted (never over-commit on a guess); until an RA
//     has been measured t_ra borrows t_widget and the RA phase re-decides.
//   - MEMORY: the new classes' cells (their units · the store's measured mean
//     entry size) must fit the L1 store's own byte/entry headroom AND the
//     adaptive admission ceiling (cache.AdmissionCeiling, GOMEMLIMIT-relative).
//     A class admitted last time already holds its cells and costs no new memory.
//
// A class over capacity stays registered, unseeded, and counted
// (snowplow_learned_classes_unseeded_capacity).
//
// SEEDING runs under each class's OWN identity through withCohortSeedContext
// (the same seed primitives and terminal-Put guard as every cohort, so #424's
// identityClassDrift still re-derives the class at Put). Order at boot /
// gvr-discovered: the admitted classes whose nav units fit the time left before
// the readiness BACKSTOP (the first-nav latch's recorded PHASE1_TIMEOUT /
// pipGlobalTimeout deadline) seed their nav widgets right after the base nav
// widgets and BEFORE the latch fires (a learned user's first navigation is warm
// when the pod turns Ready); the remaining admitted classes seed theirs right
// after the latch, in the same order — a latch released by the backstop is a
// failed boot. Their RESTActions follow the base RA tail. Keepwarm re-seeds them
// after the base cohorts. A login or a membership change (a new class) enqueues
// the payload-free class-seed scope. The class-seed scope and the #258 reseed
// share ONE customer-yielding resident pass (enumerateResident).

package dispatchers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// enumerateBasePrewarmTargetsFn is the representative enumeration the learned
// wrapper extends (a seam so a falsifier can count base calls; production
// always uses cache.EnumeratePrewarmTargetsForGVR).
var enumerateBasePrewarmTargetsFn = cache.EnumeratePrewarmTargetsForGVR

// learnedClassSignature is the identity-class signature the DISTINCT rule
// compares: (binding-set digest, RBACSubGen over the effective groups) — the
// two identity dimensions of a resolved-output key besides the first-match
// BindingUID, which is a function of the binding set.
func learnedClassSignature(username string, groups []string) string {
	return rbac.SubjectBindingSetDigest(username, groups) + "\x1f" +
		strconv.FormatUint(cache.RBACSubGenForSubject(username, rbac.WithAuthenticatedGroup(groups)), 10)
}

// enumeratePrewarmTargetsWithLearned is THE wrapper: the representative targets
// for (gvr, verb), plus every learned class that is AUTHORISED for the GVR and
// DISTINCT from every representative and every learned class already emitted.
// Live on every call.
func enumeratePrewarmTargetsWithLearned(gvr schema.GroupVersionResource, verb string) []cache.PrewarmTarget {
	base := enumerateBasePrewarmTargetsFn(gvr, verb)
	classes := cache.LearnedIdentitiesSnapshot()
	if len(classes) == 0 {
		return base
	}
	seen := make(map[string]struct{}, len(base)+len(classes))
	for _, t := range base {
		seen[learnedClassSignature(t.Subject.Username, t.Subject.Groups)] = struct{}{}
	}
	out := base
	for _, c := range classes {
		if !cache.IdentityAuthorisedForGVR(gvr, c.Username, c.Groups) {
			continue
		}
		sig := learnedClassSignature(c.Username, c.Groups)
		if _, dup := seen[sig]; dup {
			continue
		}
		seen[sig] = struct{}{}
		out = append(out, cache.PrewarmTarget{
			Subject:           cache.SubjectIdentity{Username: c.Username, Groups: c.Groups},
			GVR:               gvr,
			Verb:              verb,
			CollapsedBindings: 1,
			Learned:           true,
		})
	}
	return out
}

// learnedKey is a seed target's class key.
func learnedKey(c seedTarget) string { return cache.LearnedClassKey(c.Username, c.Groups) }

// ── per-unit cost measurement (the engine bound's t_unit, per kind) ─────────
//
// A widget seed and a RESTAction seed cost very differently (a nav widget is
// mostly informer-served; an RA can run gojq over a large list), so the two
// means are kept apart and every class is weighted by its own unit mix. At boot
// only widgets have run when the pre-latch decision is taken; until an RA has
// been measured the RA mean falls back to the widget mean, and the post-latch
// RA decision is re-taken once the base RA tail has measured it.

type seedUnitCost struct {
	sumNs atomic.Int64
	count atomic.Int64
}

func (c *seedUnitCost) add(d time.Duration) {
	if d <= 0 {
		d = time.Nanosecond
	}
	c.sumNs.Add(int64(d))
	c.count.Add(1)
}

func (c *seedUnitCost) mean() (time.Duration, bool) {
	n := c.count.Load()
	if n <= 0 {
		return 0, false
	}
	return time.Duration(c.sumNs.Load() / n), true
}

var widgetUnitCost, raUnitCost seedUnitCost

// seedUnitMarkerKey carries a per-target flag that enterSeedUnit sets when the
// target really resolves (a fresh/age/liveness skip never reaches it).
type seedUnitMarkerKey struct{}

// measuredSeedUnit runs one seed target and, if it resolved, records its whole
// per-target wall cost (seed ctx build, key derivation, admission wait,
// resolve, terminal Put) under its kind — the cost a keepwarm cycle pays per
// seeded unit.
func measuredSeedUnit(ctx context.Context, isWidget bool, do func(context.Context) error) error {
	m := new(atomic.Bool)
	start := time.Now()
	err := do(context.WithValue(ctx, seedUnitMarkerKey{}, m))
	if m.Load() {
		if isWidget {
			widgetUnitCost.add(time.Since(start))
		} else {
			raUnitCost.add(time.Since(start))
		}
	}
	return err
}

// markSeedUnitResolved flags the enclosing measuredSeedUnit (if any).
func markSeedUnitResolved(ctx context.Context) {
	if m, ok := ctx.Value(seedUnitMarkerKey{}).(*atomic.Bool); ok {
		m.Store(true)
	}
}

// seedUnitCosts returns the per-kind costs the bound uses. ok=false until a
// unit of either kind was measured; an unmeasured kind borrows the other's
// mean (and says so in the measured flags).
func seedUnitCosts() (tWidget, tRA time.Duration, wMeasured, rMeasured, ok bool) {
	tWidget, wMeasured = widgetUnitCost.mean()
	tRA, rMeasured = raUnitCost.mean()
	switch {
	case wMeasured && rMeasured:
	case wMeasured:
		tRA = tWidget
	case rMeasured:
		tWidget = tRA
	default:
		return 0, 0, false, false, false
	}
	return tWidget, tRA, wMeasured, rMeasured, true
}

func resetSeedUnitCostForTest() {
	widgetUnitCost.sumNs.Store(0)
	widgetUnitCost.count.Store(0)
	raUnitCost.sumNs.Store(0)
	raUnitCost.count.Store(0)
}

// ── admission (the self-adapting bound) ──────────────────────────────────────

// learnedPlan is one pass's learned workload: every class with ≥1 distinct
// target (newest LastSeen first), its widget and RESTAction units, and the
// pass's base-cohort units of each kind.
type learnedPlan struct {
	order  []string
	wUnits map[string]int
	rUnits map[string]int
	baseW  int
	baseR  int
}

// newLearnedPlan orders the classes present in the unit maps by the registry's
// newest-LastSeen-first order. A class that left the registry since the targets
// were enumerated (Secret deleted / JWT expired) is not seeded.
func newLearnedPlan(wUnits, rUnits map[string]int, baseW, baseR int) learnedPlan {
	p := learnedPlan{wUnits: wUnits, rUnits: rUnits, baseW: baseW, baseR: baseR}
	if len(wUnits) == 0 && len(rUnits) == 0 {
		return p
	}
	for _, c := range cache.LearnedIdentitiesSnapshot() {
		if k := c.Key(); wUnits[k]+rUnits[k] > 0 {
			p.order = append(p.order, k)
		}
	}
	return p
}

// learnedAdmitted is the last admission decision (class keys), read by the
// #258 reseed path so a rotation reseeds only admitted classes.
var learnedAdmitted atomic.Pointer[map[string]struct{}]

func learnedClassAdmitted(key string) bool {
	m := learnedAdmitted.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[key]
	return ok
}

// learnedDecision is the outcome of one admission, with the measured inputs.
type learnedDecision struct {
	admitted map[string]struct{}
	order    []string // admitted, newest first
	// preLatch is the newest-first prefix of order whose NAV units fit the
	// readiness backstop budget (all of order when no backstop applies); the
	// rest of order seeds right after the latch fires, in the same order.
	preLatch      []string
	bound         string // "engine" | "memory" | "none"
	tWidget, tRA  time.Duration
	budget        time.Duration // keepwarm interval − the base cohorts' cost
	used          time.Duration
	memHeadroom   int64
	entryHeadroom int64
	avgEntryBytes int64
	memFit        int // the memory-feasible newest-first prefix length
}

// learnedMemoryInputs reads the memory side of the bound: the byte headroom
// (min of the L1 store's own budget and the adaptive admission ceiling), the
// entry headroom, and the store's measured mean entry size.
func learnedMemoryInputs() (memHeadroom, entryHeadroom, avgEntryBytes int64) {
	store := cache.ResolvedCache()
	maxEntries, maxBytes := store.Caps()
	length, bytes := int64(store.Len()), store.Bytes()
	memHeadroom = maxBytes - bytes
	if ceiling, unlimited := cache.AdmissionCeiling(); !unlimited && ceiling < memHeadroom {
		memHeadroom = ceiling
	}
	entryHeadroom = int64(maxEntries) - length
	if length > 0 {
		avgEntryBytes = bytes / length
	} else {
		// Nothing measured in the store yet: the seed gate's per-unit estimate
		// (calibrated, else its conservative-high code constant).
		ceiling, unlimited := cache.AdmissionCeiling()
		avgEntryBytes = currentEstUnit(ceiling, unlimited)
	}
	if avgEntryBytes < 1 {
		avgEntryBytes = 1
	}
	return memHeadroom, entryHeadroom, avgEntryBytes
}

// decideLearnedAdmission applies the bound to plan, newest first, stopping at
// the first class that does not fit (the admitted set is a newest-LastSeen
// prefix):
//
//   - ENGINE: Σ (W_c·t_widget + R_c·t_ra) ≤ keepwarmSweepInterval −
//     (W_base·t_widget + R_base·t_ra) — every keepwarm cycle re-resolves every
//     seeded unit; nothing is admitted until a unit has been measured.
//   - MEMORY: a new class's cells (W_c+R_c at the store's mean entry size) fit
//     the L1 byte/entry headroom and the adaptive admission ceiling.
//
// preLatchBudget (hasBackstop) is the time left before the readiness backstop:
// the pre-latch prefix is the admitted classes whose NAV units fit it at
// t_widget. Publishes the decision (learnedAdmitted + the expvar stats).
//
// memFit < 0 computes the memory side; memFit >= 0 reuses an earlier decision's
// memory-feasible prefix length from the SAME pass (the cells that decision's
// seeding just wrote are now in the store and must not be charged twice).
func decideLearnedAdmission(plan learnedPlan, preLatchBudget time.Duration, hasBackstop bool, memFit int) learnedDecision {
	d := learnedDecision{admitted: map[string]struct{}{}, bound: "none"}
	tW, tR, wMeasured, rMeasured, measured := seedUnitCosts()
	d.tWidget, d.tRA = tW, tR
	interval := keepwarmSweepInterval()
	if measured {
		d.budget = interval - time.Duration(plan.baseW)*tW - time.Duration(plan.baseR)*tR
	}
	d.memHeadroom, d.entryHeadroom, d.avgEntryBytes = learnedMemoryInputs()

	prev := learnedAdmitted.Load()
	resident := func(k string) bool {
		if prev == nil {
			return false
		}
		_, ok := (*prev)[k]
		return ok
	}
	// The two sides are newest-first prefixes, computed independently; the
	// admitted set is the shorter one (that side is the binding bound).
	engineFit := 0
	for _, k := range plan.order {
		cost := time.Duration(plan.wUnits[k])*tW + time.Duration(plan.rUnits[k])*tR
		if !measured || d.used+cost > d.budget {
			break
		}
		d.used += cost
		engineFit++
	}
	if memFit < 0 {
		memFit = 0
		var usedMem, usedEntries int64
		for _, k := range plan.order {
			var mem, entries int64
			if !resident(k) {
				u := int64(plan.wUnits[k] + plan.rUnits[k])
				mem, entries = u*d.avgEntryBytes, u
			}
			if usedMem+mem > d.memHeadroom || usedEntries+entries > d.entryHeadroom {
				break
			}
			usedMem += mem
			usedEntries += entries
			memFit++
		}
	}
	d.memFit = memFit
	n := engineFit
	switch {
	case memFit < engineFit:
		n, d.bound = memFit, "memory"
		d.used = 0
		for _, k := range plan.order[:n] {
			d.used += time.Duration(plan.wUnits[k])*tW + time.Duration(plan.rUnits[k])*tR
		}
	case engineFit < len(plan.order):
		d.bound = "engine"
	}
	for _, k := range plan.order[:n] {
		d.admitted[k] = struct{}{}
		d.order = append(d.order, k)
	}
	// The readiness backstop: only the nav units that fit the time left before
	// it seed pre-latch; a latch released by the backstop is a FAILED boot.
	var navUsed time.Duration
	for _, k := range d.order {
		nav := time.Duration(plan.wUnits[k]) * tW
		if hasBackstop && navUsed+nav > preLatchBudget {
			break
		}
		navUsed += nav
		d.preLatch = append(d.preLatch, k)
	}
	admitted := d.admitted
	learnedAdmitted.Store(&admitted)
	backstopUs := int64(-1)
	if hasBackstop {
		backstopUs = preLatchBudget.Microseconds()
	}
	cache.SetLearnedAdmissionStats(len(d.admitted), len(plan.order)-len(d.admitted), map[string]any{
		"bound":                 d.bound,
		"t_widget_us":           tW.Microseconds(),
		"t_ra_us":               tR.Microseconds(),
		"t_widget_measured":     wMeasured,
		"t_ra_measured":         rMeasured,
		"keepwarm_interval_us":  interval.Microseconds(),
		"base_widget_units":     plan.baseW,
		"base_ra_units":         plan.baseR,
		"budget_us":             d.budget.Microseconds(),
		"admitted_cost_us":      d.used.Microseconds(),
		"distinct_classes":      len(plan.order),
		"prelatch_budget_us":    backstopUs,
		"prelatch_classes":      len(d.preLatch),
		"prelatch_nav_cost_us":  navUsed.Microseconds(),
		"memory_headroom_bytes": d.memHeadroom,
		"entry_headroom":        d.entryHeadroom,
		"avg_entry_bytes":       d.avgEntryBytes,
	})
	if len(plan.order) == 0 {
		return d
	}
	slog.Info("prewarm.learned.admission",
		slog.String("subsystem", "cache"),
		slog.Int("distinct_classes", len(plan.order)),
		slog.Int("admitted", len(d.admitted)),
		slog.Int("prelatch", len(d.preLatch)),
		slog.String("bound", d.bound),
		slog.Int64("t_widget_us", tW.Microseconds()),
		slog.Int64("t_ra_us", tR.Microseconds()),
		slog.Bool("t_ra_measured", rMeasured),
		slog.Int64("budget_us", d.budget.Microseconds()),
		slog.Int64("admitted_cost_us", d.used.Microseconds()),
		slog.Int64("prelatch_budget_us", backstopUs),
	)
	return d
}

func resetLearnedAdmissionForTest() {
	learnedAdmitted.Store(nil)
}

// ── per-pass learned seeding (called from seedScopeYielding) ────────────────

// learnedPass collects one seedScopeYielding pass's learned workload, split out
// of the precomputed widget / restaction seeds so the base-cohort ranking, the
// NavOrder flat pass and the keepwarm prefix are unchanged.
type learnedPass struct {
	wUnits map[string]int
	rUnits map[string]int
	memFit int // -1 until the pass's first decision measured the memory side
}

func newLearnedPass() *learnedPass {
	return &learnedPass{wUnits: map[string]int{}, rUnits: map[string]int{}, memFit: -1}
}

// splitLearned returns targets without the learned ones, and the learned ones.
func splitLearned(targets []seedTarget) (base, learned []seedTarget) {
	for _, c := range targets {
		if c.Learned {
			learned = append(learned, c)
		} else {
			base = append(base, c)
		}
	}
	return base, learned
}

func (lp *learnedPass) note(learned []seedTarget, isWidget bool) {
	for _, c := range learned {
		if isWidget {
			lp.wUnits[learnedKey(c)]++
		} else {
			lp.rUnits[learnedKey(c)]++
		}
	}
}

// decide applies the bound to this pass (re-taken at each phase boundary, so
// the RA phase uses the RA cost the base RA tail just measured).
//
// The memory side is measured once per pass (the first decision); later
// decisions in the pass reuse it, so the cells the pass itself wrote are not
// charged again.
func (lp *learnedPass) decide(baseW, baseR int, preLatchBudget time.Duration, hasBackstop bool) learnedDecision {
	d := decideLearnedAdmission(newLearnedPlan(lp.wUnits, lp.rUnits, baseW, baseR), preLatchBudget, hasBackstop, lp.memFit)
	lp.memFit = d.memFit
	return d
}

// navCost is one class's nav units at the measured widget cost (#442 re-check).
func (lp *learnedPass) navCost(k string, tWidget time.Duration) time.Duration {
	return time.Duration(lp.wUnits[k]) * tWidget
}

// learnedPhaseObserver is a TEST-ONLY synchronous view of the classes each boot
// / keepwarm learned phase seeds ("prelatch", "ra", "keepwarm"); nil in
// production.
var learnedPhaseObserver func(phase string, classes []string)

func noteLearnedPhase(phase string, classes []string) {
	if learnedPhaseObserver != nil {
		learnedPhaseObserver(phase, append([]string(nil), classes...))
	}
}

// ── the resident pass (class-seed scope + #258 reseed) ──────────────────────

// registerEngineLearnedClassHook subscribes the engine to new learned classes.
// The callback only enqueues the payload-free scope (coalesced); the new class
// keys wait in the registry's pending set.
func registerEngineLearnedClassHook(e *prewarmEngine) {
	cache.RegisterLearnedClassHook(func() {
		e.enqueueScope(prewarmScope{kind: scopeKindLearnedClass})
	})
}

// residentUnit is one RESIDENT unit (harvester snapshot — no walk) with its
// full, unfiltered identity target set (base + learned), in cohort-label order.
type residentUnit struct {
	isWidget bool
	widget   navWidgetEntry
	ra       templatesv1.ObjectReference
	// harvested — the unit is in the harvester snapshot (the #258 reseed's
	// domain); a proactive-only RA (RBAC-reachable, never walked) is enumerated
	// only for the learned plan.
	harvested bool
	targets   []seedTarget
}

// enumerateResident walks the resident units ONCE — widgets in NavOrder, then
// RESTActions by ns/name — yielding to customers before every unit (it runs on
// every RBAC shift and every class-seed scope). withProactive adds the
// RBAC-reachable RESTActions the boot seed unions in (learned plan only). On a
// ctx cut it returns the prefix it reached.
func enumerateResident(ctx context.Context, deps rePrewarmDeps, withProactive bool) []residentUnit {
	widgetUnits := deps.navHarv.snapshot()
	sort.SliceStable(widgetUnits, func(i, j int) bool {
		if widgetUnits[i].NavOrder != widgetUnits[j].NavOrder {
			return widgetUnits[i].NavOrder < widgetUnits[j].NavOrder
		}
		return reseedWidgetNSName(widgetUnits[i]) < reseedWidgetNSName(widgetUnits[j])
	})
	raUnits := deps.harvester.snapshot()
	harvested := make(map[string]struct{}, len(raUnits))
	for _, r := range raUnits {
		harvested[r.Namespace+"/"+r.Name] = struct{}{}
	}
	if withProactive && ProactiveRASeedEnabled() && deps.rw != nil {
		raUnits = unionProactiveRARefs(raUnits, deps.rw, slog.Default())
	}
	sort.SliceStable(raUnits, func(i, j int) bool {
		return raUnits[i].Namespace+"/"+raUnits[i].Name < raUnits[j].Namespace+"/"+raUnits[j].Name
	})
	byLabel := func(ts []seedTarget) []seedTarget {
		sort.SliceStable(ts, func(i, j int) bool { return cohortLogLabel(ts[i]) < cohortLogLabel(ts[j]) })
		return ts
	}
	out := make([]residentUnit, 0, len(widgetUnits)+len(raUnits))
	for _, e := range widgetUnits {
		if ctx.Err() != nil {
			return out
		}
		engineYieldCheckpoint(ctx)
		out = append(out, residentUnit{isWidget: true, widget: e, harvested: true,
			targets: byLabel(reseedIdentityTargetsAll(e.GVR))})
	}
	for _, ref := range raUnits {
		if ctx.Err() != nil {
			return out
		}
		engineYieldCheckpoint(ctx)
		targetGVR, ok := restActionTargetGVRFn(ctx, ref)
		if !ok {
			continue
		}
		_, h := harvested[ref.Namespace+"/"+ref.Name]
		out = append(out, residentUnit{ra: ref, harvested: h, targets: byLabel(reseedIdentityTargetsAll(targetGVR))})
	}
	return out
}

func (u residentUnit) request(c seedTarget) reseedRequest {
	if u.isWidget {
		return reseedRequest{identity: c, isWidget: true, widget: u.widget}
	}
	return reseedRequest{identity: c, isWidget: false, ra: u.ra}
}

// residentLearnedPlan is the learned plan over the resident units plus the
// per-class reseed requests (widgets NavOrder first, then RESTActions).
type residentLearnedPlan struct {
	plan       learnedPlan
	widgetReqs map[string][]reseedRequest
	raReqs     map[string][]reseedRequest
}

func planFromResident(units []residentUnit) residentLearnedPlan {
	wUnits, rUnits := map[string]int{}, map[string]int{}
	baseW, baseR := 0, 0
	rp := residentLearnedPlan{widgetReqs: map[string][]reseedRequest{}, raReqs: map[string][]reseedRequest{}}
	for _, u := range units {
		for _, c := range u.targets {
			switch {
			case !c.Learned && u.isWidget:
				baseW++
			case !c.Learned:
				baseR++
			case u.isWidget:
				k := learnedKey(c)
				wUnits[k]++
				rp.widgetReqs[k] = append(rp.widgetReqs[k], u.request(c))
			default:
				k := learnedKey(c)
				rUnits[k]++
				rp.raReqs[k] = append(rp.raReqs[k], u.request(c))
			}
		}
	}
	rp.plan = newLearnedPlan(wUnits, rUnits, baseW, baseR)
	return rp
}

// rotatedReqsFromResident is the #258 Q1 TARGET-FILTER over a resident pass:
// the harvested (unit × identity) pairs whose folded subject rotated; a learned
// class only if the bound admits it. Order: widgets NavOrder, RESTActions by
// ns/name, identities by cohort label (unchanged from #258).
func rotatedReqsFromResident(units []residentUnit, rotated cache.RotatedSubjectSet) []reseedRequest {
	var reqs []reseedRequest
	for _, u := range units {
		if !u.harvested {
			continue
		}
		for _, c := range u.targets {
			if c.Learned && !learnedClassAdmitted(learnedKey(c)) {
				continue
			}
			if rotated.Rotated(c.Username, c.Groups) {
				reqs = append(reqs, u.request(c))
			}
		}
	}
	return reqs
}

// rePrewarmLearnedClasses is the scopeKindLearnedClass handler: seed the NEW
// classes (drained from the registry) across the resident units, NavOrder
// first, under their own identity, within the bound. Before the boot walk has
// harvested anything there is nothing resident; the boot pass reads the whole
// registry itself, so the drained classes are not lost.
func rePrewarmLearnedClasses(ctx context.Context, deps rePrewarmDeps) error {
	pending := cache.DrainPendingLearnedClasses()
	if len(pending) == 0 {
		return nil
	}
	units := enumerateResident(ctx, deps, true)
	if ctx.Err() != nil {
		cache.RemergePendingLearnedClasses(pending)
		return ctx.Err()
	}
	if len(units) == 0 {
		return nil
	}
	rp := planFromResident(units)
	d := decideLearnedAdmission(rp.plan, 0, false, -1)
	var reqs []reseedRequest
	for _, k := range d.order {
		if _, isNew := pending[k]; !isNew {
			continue
		}
		reqs = append(reqs, rp.widgetReqs[k]...)
		reqs = append(reqs, rp.raReqs[k]...)
	}
	start := time.Now()
	reEnqueue := reseedTargets(ctx, deps, reqs)
	if ctx.Err() != nil || len(reEnqueue) > 0 {
		cache.RemergePendingLearnedClasses(pending)
		if e := prewarmEngineSingleton(); e != nil {
			e.enqueueScope(prewarmScope{kind: scopeKindLearnedClass})
		}
	}
	slog.Info("prewarm.learned.class_seed.done",
		slog.String("subsystem", "cache"),
		slog.Int("new_classes", len(pending)),
		slog.Int("admitted", len(d.admitted)),
		slog.Int("targets", len(reqs)),
		slog.Int("processed", len(reqs)-len(reEnqueue)),
		slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
	)
	return nil
}

// reseedIdentityTargetsAll is reseedIdentityTargets WITHOUT the admission
// filter (the class-seed scope builds its own plan).
func reseedIdentityTargetsAll(gvr schema.GroupVersionResource) []seedTarget {
	raw := enumeratePrewarmTargetsForGVRFn(gvr, "list")
	out := make([]seedTarget, 0, len(raw))
	for _, t := range raw {
		out = append(out, seedTargetFromPrewarm(t))
	}
	return out
}

func seedTargetFromPrewarm(t cache.PrewarmTarget) seedTarget {
	return seedTarget{
		BindingUID:        t.BindingUID,
		Username:          t.Subject.Username,
		Groups:            append([]string(nil), t.Subject.Groups...),
		CollapsedBindings: t.CollapsedBindings,
		Learned:           t.Learned,
	}
}

// ── S2: live customer identities ─────────────────────────────────────────────

// observeLiveCaller records the /call's identity (S2) at the dispatcher
// ServeHTTP entry. Never called from the key-mint path the seeds share.
func observeLiveCaller(req *http.Request) {
	if cache.Disabled() || req == nil {
		return
	}
	ui, err := xcontext.UserInfo(req.Context())
	if err != nil || ui.Username == "" {
		return
	}
	cache.ObserveLiveIdentity(ui.Username, ui.Groups, bearerExpiry(req))
}

// bearerExpiry reads the `exp` claim of the request's bearer JWT (already
// verified by the auth middleware upstream; only the expiry is read here).
// Zero when absent or unreadable.
func bearerExpiry(req *http.Request) time.Time {
	h := req.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok {
		return time.Time{}
	}
	parts := strings.Split(strings.TrimSpace(tok), ".")
	if len(parts) != 3 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Exp float64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Exp <= 0 {
		return time.Time{}
	}
	return time.Unix(int64(claims.Exp), 0)
}
