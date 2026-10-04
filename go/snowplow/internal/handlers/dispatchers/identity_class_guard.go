// identity_class_guard.go — #424: the Put-time re-check of an identity-bound
// key's RBAC class.
//
// An identity-bound key (restactions / widgets / raFullList) names an RBAC
// class: (first-match BindingUID, SubjectBindingSet digest, RBACSubGen) for
// the identity that minted it. The key is minted BEFORE the resolve and the
// body is written AFTER it. Two ways the body can stop belonging to the class
// its key names:
//
//  1. TOCTOU (every Put path). A grant/revoke lands between mint and Put. The
//     resolve then reads (some of) the requester's NEW rights while the key
//     still names the OLD class — and every other member of the old class
//     derives that key. Writing would serve the new rights to them.
//
//  2. Representative drift (the refresher). The refresher re-resolves under
//     the cell's recorded representative using the representative's CURRENT
//     RBAC, then re-Puts under the carried key. If the representative gained
//     or lost a binding since the cell was minted, the re-resolve is for a
//     different class than the one still deriving the key.
//
// The guard re-derives the identity dimensions for the identity the body was
// resolved under, AFTER the resolve, and declines the write when they differ
// from the key's. The body is still served to its own requester (it is correct
// for them); only the shared-cell write is skipped. RBACSubGen is monotone per
// subject, so a grant-then-revoke inside one resolve (ABA on the binding set)
// still differs and is caught.
//
// A change landing AFTER this check but before the Put is harmless: then the
// whole resolve ran under the minting class, so the body is that class's body;
// the next request derives the new key and misses.

package dispatchers

import (
	"context"
	"errors"
	"expvar"
	"slices"
	"sync"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// identityClassDrift reports why (username, groups) no longer belongs to the
// RBAC class inputs' key was minted for, or "" when it still does (or when the
// class is identity-free / inputs is nil — nothing to check).
func identityClassDrift(ctx context.Context, inputs *cache.ResolvedKeyInputs, username string, groups []string) string {
	if inputs == nil || cache.IsIdentityFreeClass(inputs.CacheEntryClass) {
		return ""
	}
	// The same derivation every identity-bound key was minted with (#449).
	now, minted := rbac.IdentityClassOf(jwtutil.UserInfo{Username: username, Groups: groups}), inputs.Class()
	if now.SubjectBindingSet != minted.SubjectBindingSet {
		return "binding_set"
	}
	// Every identity-bound class folds RBACSubGen — raFullList too since #435
	// (seedFullListRAKey), so one limb covers all three.
	if now.RBACSubGen != minted.RBACSubGen {
		return "rbac_subgen"
	}
	// No separate first-match BindingUID re-evaluation: BindingUID is a function
	// of (the binding set, the roles those bindings reference, the cell's fixed
	// coordinates). An unchanged binding set rules out the first; any rules change
	// on a referenced role bumps the per-subject RBACSubGen of every subject of the
	// referencing bindings (#257 onRoleRulesChanged) — rotating the key above.
	_ = ctx
	return ""
}

// identityClassDriftDeclined counts Put/re-Put declines by "<site>/<reason>"
// (/debug/vars snowplow_l1_identity_class_drift_declined_total). Non-zero is
// expected and benign — every grant/revoke that lands mid-resolve on a
// requester ticks it once — and it is the evidence the guard fires.
//
// CFG-1: the counters always exist (the guard can run in tests regardless of
// env); the expvar KEY is published only when the cache subsystem is on, from
// a Disabled()-gated init — under cache-off there is no L1 to write, so the key
// must be absent (e2e/bench/cfg1_probe).
var identityClassDriftDeclined sync.Map // "<site>/<reason>" -> *atomic.Int64

func init() {
	if cache.Disabled() {
		return
	}
	expvar.Publish("snowplow_l1_identity_class_drift_declined_total", expvar.Func(func() any {
		out := map[string]int64{}
		identityClassDriftDeclined.Range(func(k, v any) bool {
			out[k.(string)] = v.(*atomic.Int64).Load()
			return true
		})
		return out
	}))
}

// noteIdentityClassDrift records one declined write at site for reason.
func noteIdentityClassDrift(site, reason string) {
	v, _ := identityClassDriftDeclined.LoadOrStore(site+"/"+reason, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

// IdentityClassDriftSites and IdentityClassDriftReasons are the CLOSED
// attribute sets of snowplow_l1_identity_class_drift_declined_total on OTLP
// (#448, F8 attribute hygiene): the four noteIdentityClassDrift call sites and
// the three reasons identityClassDrift/identityClassDriftCtx return. Neither
// carries identity. TestIdentityClassDrift448_CallSitesUseTheClosedSets pins
// every literal at the call sites to these lists.
var (
	IdentityClassDriftSites   = []string{"restactions", "widgets", "seed", "refresher"}
	IdentityClassDriftReasons = []string{"binding_set", "rbac_subgen", "no_identity"}
)

// IdentityClassDriftCell is one {site, reason} decline count.
type IdentityClassDriftCell struct {
	Site, Reason string
	Count        int64
}

// IdentityClassDriftDeclinedCells returns every {site, reason} pair of the
// closed sets with its live count (0 when never ticked), so the OTLP series set
// is fixed by construction and never grows with traffic. Each count is its own
// monotonic atomic.
func IdentityClassDriftDeclinedCells() []IdentityClassDriftCell {
	out := make([]IdentityClassDriftCell, 0, len(IdentityClassDriftSites)*len(IdentityClassDriftReasons))
	for _, site := range IdentityClassDriftSites {
		for _, reason := range IdentityClassDriftReasons {
			var n int64
			if v, ok := identityClassDriftDeclined.Load(site + "/" + reason); ok {
				n = v.(*atomic.Int64).Load()
			}
			out = append(out, IdentityClassDriftCell{Site: site, Reason: reason, Count: n})
		}
	}
	return out
}

// NoteIdentityClassDriftForTest ticks one site/reason counter through the
// production recorder (the OTLP parity arms in internal/metrics).
func NoteIdentityClassDriftForTest(site, reason string) { noteIdentityClassDrift(site, reason) }

// ResetIdentityClassDriftForTest clears every site/reason counter.
func ResetIdentityClassDriftForTest() {
	identityClassDriftDeclined.Range(func(k, _ any) bool {
		identityClassDriftDeclined.Delete(k)
		return true
	})
}

// identityClassDriftDeclinedForTest reads one site/reason counter.
func identityClassDriftDeclinedForTest(site, reason string) int64 {
	if v, ok := identityClassDriftDeclined.Load(site + "/" + reason); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// identityClassDriftCtx is identityClassDrift for the identity on ctx (the
// customer and seed Put paths). A missing identity is drift: a key that was
// minted for an identity cannot be re-confirmed without one.
func identityClassDriftCtx(ctx context.Context, inputs *cache.ResolvedKeyInputs) string {
	if inputs == nil || cache.IsIdentityFreeClass(inputs.CacheEntryClass) {
		return ""
	}
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return "no_identity"
	}
	return identityClassDrift(ctx, inputs, ui.Username, ui.Groups)
}

// errRepresentativeDriftedMidRefresh is returned when the representative's RBAC
// class moved DURING a refresh re-resolve (#444). The body is not written; the
// error is retryable, so the refresher requeues the key and the next attempt
// re-picks an in-class representative (repickRepresentative) or evicts. A spent
// requeue budget drops → evicts at the drop point. Never a suppress-to-TTL.
var errRepresentativeDriftedMidRefresh = errors.New("representative's RBAC class moved during the re-resolve")

// Where the refresher's representative came from (#444).
const (
	repSourceRecorded = "recorded"
	repSourceGroup    = "group"
	repSourceHitter   = "hitter"
)

// repickRepresentative finds a replacement representative for an identity-bound
// cell whose recorded representative drifted out of the key's RBAC class (#444).
// Candidates, in order, each accepted only if identityClassDrift says it is in
// the class NOW (the sub-gen is monotone, so an identity that has left a class
// can never be promoted back into it):
//
//	(a) the canonical group representative ("", RepresentativeGroups ∪
//	    system:authenticated) — the seed's own representative shape
//	    (withCohortSeedContext); covers every group-only class with no per-user
//	    tracking;
//	(b) the cell's recent hitters (ResolvedEntry.RecentHitters, most recent
//	    first), skipping the drifted representative itself.
//
// ok=false when none is in the class: the caller evicts. The returned identity
// is never logged (#262 redaction rules).
func repickRepresentative(ctx context.Context, inputs *cache.ResolvedKeyInputs, prior *cache.ResolvedEntry) (username string, groups []string, source string, ok bool) {
	if inputs == nil {
		return "", nil, "", false
	}
	g := rbac.WithAuthenticatedGroup(inputs.RepresentativeGroups)
	if identityClassDrift(ctx, inputs, "", g) == "" {
		return "", g, repSourceGroup, true
	}
	for _, h := range prior.RecentHitters() {
		if h.Username == inputs.RepresentativeUsername && slices.Equal(h.Groups, inputs.RepresentativeGroups) {
			continue
		}
		if identityClassDrift(ctx, inputs, h.Username, h.Groups) == "" {
			return h.Username, slices.Clone(h.Groups), repSourceHitter, true
		}
	}
	return "", nil, "", false
}

// representativeRepick counts the refresher's #444 outcomes by "group" /
// "hitter" / "evicted" (/debug/vars snowplow_l1_representative_repick_total).
// CFG-1: the key is published only cache-on (no L1 to refresh otherwise).
var representativeRepick sync.Map // outcome -> *atomic.Int64

func init() {
	if cache.Disabled() {
		return
	}
	expvar.Publish("snowplow_l1_representative_repick_total", expvar.Func(func() any {
		out := map[string]int64{}
		representativeRepick.Range(func(k, v any) bool {
			out[k.(string)] = v.(*atomic.Int64).Load()
			return true
		})
		return out
	}))
}

func noteRepresentativeRepick(outcome string) {
	v, _ := representativeRepick.LoadOrStore(outcome, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

// representativeRepickForTest reads one outcome counter.
func representativeRepickForTest(outcome string) int64 {
	if v, ok := representativeRepick.Load(outcome); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// RepresentativeRepickOutcomes is the CLOSED `outcome` attribute set of
// snowplow_l1_representative_repick_total on OTLP (#448, F8): the two
// repickRepresentative sources and the eviction. No identity is carried.
// TestRepresentativeRepick448_CallSitesUseTheClosedSet pins it to the source.
var RepresentativeRepickOutcomes = []string{repSourceGroup, repSourceHitter, "evicted"}

// RepresentativeRepickCell is one outcome count.
type RepresentativeRepickCell struct {
	Outcome string
	Count   int64
}

// RepresentativeRepickCells returns every outcome of the closed set with its
// live count (0 when never ticked); each count is its own monotonic atomic.
func RepresentativeRepickCells() []RepresentativeRepickCell {
	out := make([]RepresentativeRepickCell, 0, len(RepresentativeRepickOutcomes))
	for _, o := range RepresentativeRepickOutcomes {
		var n int64
		if v, ok := representativeRepick.Load(o); ok {
			n = v.(*atomic.Int64).Load()
		}
		out = append(out, RepresentativeRepickCell{Outcome: o, Count: n})
	}
	return out
}

// NoteRepresentativeRepickForTest ticks one outcome through the production
// recorder (the OTLP parity arms in internal/metrics).
func NoteRepresentativeRepickForTest(outcome string) { noteRepresentativeRepick(outcome) }

// refreshLogUser is the representative as it may appear in a refresher log
// line: a promoted recent hitter is redacted (#444, #262 redaction rules).
func refreshLogUser(user, source string) string {
	if source == repSourceHitter {
		return "<recent-hitter>"
	}
	return user
}
