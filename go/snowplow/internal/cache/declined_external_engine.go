// declined_external_engine.go — #643 PR-B: ONE engine-wide declined-external
// set, and the facts each mark is taken under so it can retire itself.
//
// WHY ENGINE-WIDE. Until #643 the set was created per boot scope, installed only
// on the boot scope and torn down when boot completed, so keepwarm and
// gvr-discovered never had one: they re-dialled every external cell, for every
// cohort, on every pass (~19 ClickHouse queries/min from one 057 pod). The
// engine now holds ONE set for the life of the process and installs it on every
// scope kind. A user /call and the refresher still never carry it (C-F4B-3's
// request half, I-6).
//
// WHY A MARK RETIRES ITSELF (I-1, zero cold navigation). A mark says "resolving
// this key touched an external endpoint, so the cell is never Put". That can
// stop being true, and a stale mark would leave a now-cacheable cell cold. Every
// input that decides whether the resolve is external is recorded with the mark
// and re-checked before a skip; any doubt resolves toward resolving:
//
//	(a) Put-unmark — any L1 Put of the key proves it cacheable (except an
//	    ExternalTTL entry, which is an external result cached by opt-in, not
//	    proof). Hooked at putCoreLocked, the one chokepoint every Put variant
//	    reaches, so a new Put site cannot forget it.
//	(b) definition — the RESTAction's uid + resourceVersion and each static
//	    endpointRef Secret's resourceVersion, read informer-only by the caller.
//	(c) GONE — the widget or RESTAction was deleted (notifyObjectGone).
//	(d) data — external dispatch can be data-dependent (an iterator stage over
//	    an empty list makes no call), and a declined cell records no dep edge,
//	    so (a) could never fire for it. The mark keeps the deps its resolve
//	    captured and the resolve's entry stamp; any dep event since then retires
//	    it (the #411 seedMemoDepsMovedSince check). No TTL anywhere.

package cache

import (
	"context"
	"expvar"
	"sync"
)

// DeclinedExternalMark is the set of facts a declined-external mark was taken
// under. Zero-valued fields record nothing to re-check.
type DeclinedExternalMark struct {
	// Widget / RA are the coordinates the GONE rule matches. Widget is zero for
	// a RESTAction unit.
	Widget DepKey
	RA     DepKey
	// RAUID / RAResourceVersion are the RESTAction's identity and version as
	// read at mark time.
	RAUID             string
	RAResourceVersion string
	// Secrets are the static endpointRef Secrets the RESTAction dispatches
	// through, with the resourceVersion read at mark time (Secrets snapshot).
	Secrets []SecretVersion
	// Deps / Stamp are the resolve's captured deps and entry epoch (d4).
	Deps  []DepKey
	Stamp uint64
	// Served are the cached L1 cells the resolve read inputs from, with their
	// Put identity (served_cell.go). The deps they cover are judged by them.
	Served []ServedCell
}

// SecretVersion is one endpointRef Secret's name and resourceVersion.
type SecretVersion struct {
	Name            string
	ResourceVersion string
}

var (
	engineDeclinedExternalOnce sync.Once
	engineDeclinedExternal     *SeedDeclinedExternalSet
)

// EngineDeclinedExternalSet returns the process's one declined-external set.
func EngineDeclinedExternalSet() *SeedDeclinedExternalSet {
	engineDeclinedExternalOnce.Do(func() {
		engineDeclinedExternal = NewSeedDeclinedExternalSet()
	})
	return engineDeclinedExternal
}

// unmarkDeclinedExternalOnPut is retire rule (a), called from putCoreLocked for
// every accepted Put. An ExternalTTL entry is an external result cached under
// the opt-in annotation: the cell still touches external, so it does not retire
// the mark.
func unmarkDeclinedExternalOnPut(key string, entry *ResolvedEntry) {
	if entry != nil && entry.ExternalTTL {
		return
	}
	if EngineDeclinedExternalSet().Unmark(key) {
		RecordDeclinedExternalRetired(DeclinedExternalRetiredPutUnmark, 1)
	}
}

// The retire reasons — a CLOSED set, one per retire rule, every cell
// pre-populated so a 0 is a reading. After a roll, the way this change can fail
// silently is marks retiring every pass (skips stay low, dials stay high); this
// breakdown names which rule is doing it.
const (
	DeclinedExternalRetiredNilMark               = "nil_mark"
	DeclinedExternalRetiredServedCellChanged     = "served_cell_changed"
	DeclinedExternalRetiredDepsMoved             = "deps_moved"
	DeclinedExternalRetiredRAUnreadAtMark        = "ra_unread_at_mark"
	DeclinedExternalRetiredRAUnreadable          = "ra_unreadable"
	DeclinedExternalRetiredRAChanged             = "ra_changed"
	DeclinedExternalRetiredEndpointSecretChanged = "endpoint_secret_changed"
	DeclinedExternalRetiredPutUnmark             = "put_unmark"
	DeclinedExternalRetiredGone                  = "gone"
	DeclinedExternalRetiredConfigRedrive         = "config_redrive"
)

// DeclinedExternalRetiredReasons is the closed reason set, in emit order.
var DeclinedExternalRetiredReasons = []string{
	DeclinedExternalRetiredNilMark,
	DeclinedExternalRetiredServedCellChanged,
	DeclinedExternalRetiredDepsMoved,
	DeclinedExternalRetiredRAUnreadAtMark,
	DeclinedExternalRetiredRAUnreadable,
	DeclinedExternalRetiredRAChanged,
	DeclinedExternalRetiredEndpointSecretChanged,
	DeclinedExternalRetiredPutUnmark,
	DeclinedExternalRetiredGone,
	DeclinedExternalRetiredConfigRedrive,
}

var (
	declinedRetiredMu sync.Mutex
	declinedRetired   = newDeclinedRetired()
)

func newDeclinedRetired() map[string]uint64 {
	m := make(map[string]uint64, len(DeclinedExternalRetiredReasons))
	for _, r := range DeclinedExternalRetiredReasons {
		m[r] = 0
	}
	return m
}

// RecordDeclinedExternalRetired counts n marks retired for reason. A reason
// outside the closed set is dropped (the set cannot grow from data).
func RecordDeclinedExternalRetired(reason string, n int) {
	if n <= 0 {
		return
	}
	declinedRetiredMu.Lock()
	if _, ok := declinedRetired[reason]; ok {
		declinedRetired[reason] += uint64(n)
	}
	declinedRetiredMu.Unlock()
}

// DeclinedExternalRetiredCount is one {reason} cell.
type DeclinedExternalRetiredCount struct {
	Reason string
	Count  uint64
}

// DeclinedExternalRetiredSnapshot returns every reason cell, zeros included.
func DeclinedExternalRetiredSnapshot() []DeclinedExternalRetiredCount {
	declinedRetiredMu.Lock()
	defer declinedRetiredMu.Unlock()
	out := make([]DeclinedExternalRetiredCount, 0, len(DeclinedExternalRetiredReasons))
	for _, r := range DeclinedExternalRetiredReasons {
		out = append(out, DeclinedExternalRetiredCount{Reason: r, Count: declinedRetired[r]})
	}
	return out
}

// ResetDeclinedExternalRetiredForTest zeroes the breakdown. Tests only.
func ResetDeclinedExternalRetiredForTest() {
	declinedRetiredMu.Lock()
	declinedRetired = newDeclinedRetired()
	declinedRetiredMu.Unlock()
}

// DepsMovedSince reports whether any of deps saw a dep event after stamp —
// the #411 check the seed-resolve memo uses (retire rule d4).
func DepsMovedSince(deps []DepKey, stamp uint64) bool {
	return seedMemoDepsMovedSince(deps, stamp)
}

// DepGenDepsFromContext returns a copy of the deps the resolve under ctx has
// recorded so far and its entry stamp (the dep-gen sink's startSeq), or ok=false
// when ctx carries no sink. recordInternal appends every Record / RecordList to
// the sink at dispatch time, whether or not the resolve is later Put, so a
// declined resolve's deps are here too.
func DepGenDepsFromContext(ctx context.Context) (deps []DepKey, stamp uint64, ok bool) {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return nil, 0, false
	}
	s.mu.Lock()
	deps = append([]DepKey(nil), s.deps...)
	s.mu.Unlock()
	return deps, s.startSeq, true
}

// MarksForTest returns a copy of every held mark by key. Tests only.
func (s *SeedDeclinedExternalSet) MarksForTest() map[string]DeclinedExternalMark {
	out := map[string]DeclinedExternalMark{}
	if s == nil {
		return out
	}
	s.m.Range(func(k, v any) bool {
		if m, _ := v.(*DeclinedExternalMark); m != nil {
			out[k.(string)] = *m
		} else {
			out[k.(string)] = DeclinedExternalMark{}
		}
		return true
	})
	return out
}

// ResetEngineDeclinedExternalSetForTest empties the process set. Tests only.
func ResetEngineDeclinedExternalSetForTest() {
	EngineDeclinedExternalSet().Reset()
}

// CFG-1: published only with the cache on.
func init() {
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_prewarm_declined_external_marks", expvar.Func(func() any {
		return EngineDeclinedExternalSet().Len()
	}))
	expvar.Publish("snowplow_prewarm_declined_external_retired_total", expvar.Func(func() any {
		m := map[string]uint64{}
		for _, c := range DeclinedExternalRetiredSnapshot() {
			m[c.Reason] = c.Count
		}
		return m
	}))
}
