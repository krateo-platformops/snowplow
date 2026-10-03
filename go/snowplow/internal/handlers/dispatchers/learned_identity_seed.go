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
//     next one, so Σ U_c · t_unit ≤ keepwarmSweepInterval − T_base, where
//     t_unit is the measured mean wall cost of a resolved seed unit
//     (enterSeedUnit), T_base = t_unit · (this pass's base-cohort units) and
//     U_c the class's units in this pass. Equivalently
//     Σ U_c ≤ interval/t_unit − N_base. Until a unit has been measured no
//     class is admitted (never over-commit on a guess).
//   - MEMORY: the new classes' cells (U_c · the store's measured mean entry
//     size) must fit the L1 store's own byte/entry headroom AND the adaptive
//     admission ceiling (cache.AdmissionCeiling, GOMEMLIMIT-relative). A class
//     admitted last time already holds its cells and costs no new memory.
//
// A class over capacity stays registered, unseeded, and counted
// (snowplow_learned_classes_unseeded_capacity).
//
// SEEDING runs under each class's OWN identity through withCohortSeedContext
// (the same seed primitives and terminal-Put guard as every cohort, so #424's
// identityClassDrift still re-derives the class at Put). Order: boot /
// gvr-discovered seed the admitted classes' nav widgets right after the base
// nav widgets and before the first-nav latch fires (a learned user's first
// navigation is warm when the pod turns Ready), then their RESTActions after
// the base RA tail; keepwarm re-seeds them after the base cohorts. A login or a
// membership change (a new class) enqueues the payload-free class-seed scope.

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
	"sync"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
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

// ── per-unit cost measurement (the engine bound's t_unit) ───────────────────

var (
	seedUnitCostSumNs atomic.Int64
	seedUnitCostCount atomic.Int64
)

// seedUnitMarkerKey carries a per-target flag that enterSeedUnit sets when the
// target really resolves (a fresh/age/liveness skip never reaches it).
type seedUnitMarkerKey struct{}

// measuredSeedUnit runs one seed target and, if it resolved, records its whole
// per-target wall cost (seed ctx build, key derivation, admission wait,
// resolve, terminal Put) — the t_unit a keepwarm cycle pays per seeded unit.
func measuredSeedUnit(ctx context.Context, do func(context.Context) error) error {
	m := new(atomic.Bool)
	start := time.Now()
	err := do(context.WithValue(ctx, seedUnitMarkerKey{}, m))
	if m.Load() {
		recordSeedUnitCost(time.Since(start))
	}
	return err
}

// markSeedUnitResolved flags the enclosing measuredSeedUnit (if any).
func markSeedUnitResolved(ctx context.Context) {
	if m, ok := ctx.Value(seedUnitMarkerKey{}).(*atomic.Bool); ok {
		m.Store(true)
	}
}

// recordSeedUnitCost records one resolved seed unit's wall cost.
func recordSeedUnitCost(d time.Duration) {
	if d <= 0 {
		d = time.Nanosecond
	}
	seedUnitCostSumNs.Add(int64(d))
	seedUnitCostCount.Add(1)
}

// seedUnitMeanCost is the measured mean wall cost of a resolved seed unit over
// the process lifetime; ok=false until one unit has been measured.
func seedUnitMeanCost() (time.Duration, bool) {
	n := seedUnitCostCount.Load()
	if n <= 0 {
		return 0, false
	}
	return time.Duration(seedUnitCostSumNs.Load() / n), true
}

func resetSeedUnitCostForTest() {
	seedUnitCostSumNs.Store(0)
	seedUnitCostCount.Store(0)
}

// ── admission (the self-adapting bound) ──────────────────────────────────────

// learnedPlan is one pass's learned workload: every class with ≥1 distinct
// target, newest LastSeen first, its units, and the pass's base-cohort units.
type learnedPlan struct {
	order []string
	units map[string]int
	nBase int
}

// newLearnedPlan orders the classes present in units by the registry's
// newest-LastSeen-first order.
func newLearnedPlan(units map[string]int, nBase int) learnedPlan {
	p := learnedPlan{units: units, nBase: nBase}
	if len(units) == 0 {
		return p
	}
	for _, c := range cache.LearnedIdentitiesSnapshot() {
		if units[c.Key()] > 0 {
			p.order = append(p.order, c.Key())
		}
	}
	// A class that left the registry since the targets were enumerated is not
	// seeded (its Secret was deleted / its JWT expired).
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
	admitted      map[string]struct{}
	bound         string // "engine" | "memory" | "none"
	tUnit         time.Duration
	measured      bool
	capUnits      int64
	usedUnits     int64
	memHeadroom   int64
	entryHeadroom int64
	avgEntryBytes int64
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
// prefix). Publishes the decision (learnedAdmitted + the expvar stats).
func decideLearnedAdmission(plan learnedPlan) learnedDecision {
	d := learnedDecision{admitted: map[string]struct{}{}, bound: "none"}
	d.tUnit, d.measured = seedUnitMeanCost()
	interval := keepwarmSweepInterval()
	if d.measured && d.tUnit > 0 {
		d.capUnits = int64(interval/d.tUnit) - int64(plan.nBase)
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
	var usedMem, usedEntries int64
	for _, k := range plan.order {
		u := int64(plan.units[k])
		if d.usedUnits+u > d.capUnits {
			d.bound = "engine"
			break
		}
		var mem, entries int64
		if !resident(k) {
			mem, entries = u*d.avgEntryBytes, u
		}
		if usedMem+mem > d.memHeadroom || usedEntries+entries > d.entryHeadroom {
			d.bound = "memory"
			break
		}
		d.usedUnits += u
		usedMem += mem
		usedEntries += entries
		d.admitted[k] = struct{}{}
	}
	admitted := d.admitted
	learnedAdmitted.Store(&admitted)
	cache.SetLearnedAdmissionStats(len(d.admitted), len(plan.order)-len(d.admitted), map[string]any{
		"bound":                 d.bound,
		"t_unit_measured":       d.measured,
		"t_unit_us":             d.tUnit.Microseconds(),
		"keepwarm_interval_s":   int64(interval / time.Second),
		"base_units":            plan.nBase,
		"capacity_units":        d.capUnits,
		"admitted_units":        d.usedUnits,
		"distinct_classes":      len(plan.order),
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
		slog.String("bound", d.bound),
		slog.Bool("t_unit_measured", d.measured),
		slog.Int64("t_unit_us", d.tUnit.Microseconds()),
		slog.Int("base_units", plan.nBase),
		slog.Int64("capacity_units", d.capUnits),
		slog.Int64("admitted_units", d.usedUnits),
	)
	return d
}

func resetLearnedAdmissionForTest() {
	learnedAdmitted.Store(nil)
}

// ── per-pass learned seeding (called from seedScopeYielding) ────────────────

// learnedPass holds one seedScopeYielding pass's learned targets, split out of
// the precomputed widget / restaction seeds so the base-cohort ranking, the
// NavOrder flat pass and the keepwarm prefix are unchanged.
type learnedPass struct {
	widgets [][]seedTarget // parallel to widgetSeeds
	ras     [][]seedTarget // parallel to restactionSeeds
	units   map[string]int

	once          sync.Once
	decision      learnedDecision
	admittedOrder []string
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

func (lp *learnedPass) note(learned []seedTarget) {
	for _, c := range learned {
		lp.units[learnedKey(c)]++
	}
}

// decide runs the admission once per pass, lazily — after the base nav units
// have been seeded, so t_unit is measured by the time it is read.
func (lp *learnedPass) decide(nBase int) learnedDecision {
	lp.once.Do(func() {
		plan := newLearnedPlan(lp.units, nBase)
		lp.decision = decideLearnedAdmission(plan)
		for _, k := range plan.order {
			if _, ok := lp.decision.admitted[k]; ok {
				lp.admittedOrder = append(lp.admittedOrder, k)
			}
		}
	})
	return lp.decision
}

// orderedAdmitted returns this pass's admitted classes, newest first.
func (lp *learnedPass) orderedAdmitted(nBase int) []string {
	lp.decide(nBase)
	return lp.admittedOrder
}

// ── the class-seed scope (a login / membership change) ──────────────────────

// registerEngineLearnedClassHook subscribes the engine to new learned classes.
// The callback only enqueues the payload-free scope (coalesced); the new class
// keys wait in the registry's pending set.
func registerEngineLearnedClassHook(e *prewarmEngine) {
	cache.RegisterLearnedClassHook(func() {
		e.enqueueScope(prewarmScope{kind: scopeKindLearnedClass})
	})
}

// residentLearnedPlan is the learned workload over the RESIDENT units (the
// harvester snapshots — no walk): the plan the bound is applied to, plus the
// per-class reseed requests (widgets NavOrder first, then RESTActions).
type residentLearnedPlan struct {
	plan       learnedPlan
	widgetReqs map[string][]reseedRequest
	raReqs     map[string][]reseedRequest
}

// planLearnedOverResident enumerates every resident unit once through the
// learned wrapper. ok=false when nothing is resident yet (before the boot walk
// harvested; the boot pass then reads the whole registry itself) or ctx ended.
func planLearnedOverResident(ctx context.Context, deps rePrewarmDeps) (residentLearnedPlan, bool) {
	widgetUnits := deps.navHarv.snapshot()
	sort.SliceStable(widgetUnits, func(i, j int) bool {
		if widgetUnits[i].NavOrder != widgetUnits[j].NavOrder {
			return widgetUnits[i].NavOrder < widgetUnits[j].NavOrder
		}
		return reseedWidgetNSName(widgetUnits[i]) < reseedWidgetNSName(widgetUnits[j])
	})
	raUnits := deps.harvester.snapshot()
	if ProactiveRASeedEnabled() && deps.rw != nil {
		raUnits = unionProactiveRARefs(raUnits, deps.rw, slog.Default())
	}
	sort.SliceStable(raUnits, func(i, j int) bool {
		return raUnits[i].Namespace+"/"+raUnits[i].Name < raUnits[j].Namespace+"/"+raUnits[j].Name
	})
	if len(widgetUnits) == 0 && len(raUnits) == 0 {
		return residentLearnedPlan{}, false
	}
	units := map[string]int{}
	nBase := 0
	rp := residentLearnedPlan{widgetReqs: map[string][]reseedRequest{}, raReqs: map[string][]reseedRequest{}}
	for _, e := range widgetUnits {
		if ctx.Err() != nil {
			return residentLearnedPlan{}, false
		}
		for _, c := range reseedIdentityTargetsAll(e.GVR) {
			if !c.Learned {
				nBase++
				continue
			}
			k := learnedKey(c)
			units[k]++
			rp.widgetReqs[k] = append(rp.widgetReqs[k], reseedRequest{identity: c, isWidget: true, widget: e})
		}
	}
	for _, ref := range raUnits {
		if ctx.Err() != nil {
			return residentLearnedPlan{}, false
		}
		targetGVR, ok := restActionTargetGVRFn(ctx, ref)
		if !ok {
			continue
		}
		for _, c := range reseedIdentityTargetsAll(targetGVR) {
			if !c.Learned {
				nBase++
				continue
			}
			k := learnedKey(c)
			units[k]++
			rp.raReqs[k] = append(rp.raReqs[k], reseedRequest{identity: c, isWidget: false, ra: ref})
		}
	}
	rp.plan = newLearnedPlan(units, nBase)
	return rp, true
}

// refreshLearnedAdmission re-applies the bound over the resident units, so a
// class that just BECAME distinct (a binding added on a group it presents — a
// rotation) is admitted before the #258 reseed filters on the admission. A
// no-op when no class is registered.
func refreshLearnedAdmission(ctx context.Context, deps rePrewarmDeps) {
	if r, _, _ := cache.LearnedClassCounts(); r == 0 {
		return
	}
	if rp, ok := planLearnedOverResident(ctx, deps); ok {
		decideLearnedAdmission(rp.plan)
	}
}

// rePrewarmLearnedClasses is the scopeKindLearnedClass handler: seed the NEW
// classes (drained from the registry) across the resident units, NavOrder
// first, under their own identity, within the bound.
func rePrewarmLearnedClasses(ctx context.Context, deps rePrewarmDeps) error {
	pending := cache.DrainPendingLearnedClasses()
	if len(pending) == 0 {
		return nil
	}
	rp, ok := planLearnedOverResident(ctx, deps)
	if !ok {
		if ctx.Err() != nil {
			cache.RemergePendingLearnedClasses(pending)
			return ctx.Err()
		}
		return nil // nothing resident yet: the boot pass seeds the whole registry
	}
	d := decideLearnedAdmission(rp.plan)
	var reqs []reseedRequest
	for _, k := range rp.plan.order {
		if _, isNew := pending[k]; !isNew {
			continue
		}
		if _, admitted := d.admitted[k]; !admitted {
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
