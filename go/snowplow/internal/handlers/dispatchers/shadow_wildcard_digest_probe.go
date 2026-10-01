// shadow_wildcard_digest_probe.go — v7 #368: the 4th DARK shadow detector, the
// cross-identity digest-collision probe that certifies the ClassWildcard class.
//
// THE BLIND SPOT IT CLOSES. The three existing dark detectors (verdict_mismatch,
// coverage_miss, name_ambiguous_leak) are STRUCTURALLY blind to a wrong
// collection-verb ClassWildcard projection encoding (see #290 / #368): an
// identity-INDEPENDENT wildcard digest reads clean on all three yet leaks at
// Step-3 share. This probe is the only runtime detector for that class after the
// wildcard projection ungates.
//
// HOW. For each wildcard-bearing cell (D contains a ClassWildcard) it compares, per
// (cell == D.Canonical(), digest), the EVALUATOR-TRUTH access of every identity
// observed on that (cell,digest). Two identities with DIFFERENT evaluator access
// that collapsed to ONE digest = the leak → collision.
//
// THE ACCESS-HASH IS INDEPENDENT OF THE PROJECTION UNDER TEST (the load-bearing
// correction, #368 TL BLOCKER). It is a fingerprint of the requester's permit
// answers from the EVALUATOR (rbac.RequesterProfile.Permits), NEVER from
// ComputeProjectionDigest/projectClass. A wrong enumerate encoding corrupts the
// DIGEST; it cannot corrupt the Permits-fingerprint, which re-runs the evaluator's
// rule matching over the identity's raw rules. So two identities that differ on ANY
// in-scope coordinate get different fingerprints even if the (buggy) projection
// collapsed their digests. (golden must model sources independently.)
//
// COORDINATE INCLUDES NAMESPACE. The fingerprint coordinate is
// (verb,group,resource,namespace) — namespace-complete by construction, so it
// catches the cross-namespace break (a RoleBinding-N1 identity vs a
// ClusterRoleBinding identity diverge on an N2 coordinate even if a namespace-blind
// projection collapsed their digests).
//
// UNION-DRIFT (fix b). The fingerprint is computed over the per-cell union of
// observed coordinates, which GROWS as more checks arrive. If a pre-hashed
// fingerprint were stored, identity A (fingerprinted early over a small union) and
// B (later over a larger union) would differ even with identical access → false
// collisions. Fix (b): keep the raw per-identity handle and RECOMPUTE every
// identity's fingerprint over the CURRENT union at comparison time. Identical-access
// identities therefore always match (converse arm); the namespace collision still
// fires because the union spans both identities' namespaces.
//
// DARK / BOUNDED / ISOLATED. Counters only — no verdict, served byte, or key fold.
// Runs inside runShadowParityHook's recover (dark-panic isolated) and only when a
// shadow context is present (shadowParityEnabled-gated). Memory is a bounded LRU
// over wildcard-bearing (cell,digest) entries with hard caps on coordinates and
// identities per entry; overflow DROPS observations (less coverage, never a false
// collision). Per-identity handles (username+groups) are held IN-PROCESS ONLY for
// the recompute and are NEVER exposed on any debug surface (the expvar surface
// publishes counts only; dedup is by a sha256 handle).
//
// COUNTER SEMANTICS (TL ruling — zero-means-safe). collision_total and
// observed_total count ONLY cells that would actually be SHARED: Shareable(D) AND
// !WildcardGated (the Step-3 share predicate). A WildcardGated cell is never shared
// and cannot leak, so its collisions go on a SEPARATE expvar-only DIAGNOSTIC
// (wildcard_gated_digest_collision_total), expected non-zero until the enumerate
// projection lands. Thus collision_total == 0 today (every ClassWildcard is gated)
// and a non-zero collision_total genuinely means a shareable cell leaked.

package dispatchers

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// #368 counters. The first two are the DETECTOR (zero-means-safe) and its
// denominator, on expvar AND hand-wired to OTLP (metrics.go, #311). The third is an
// expvar-ONLY diagnostic for the gated baseline.
var (
	shadowWildcardDigestCollisionTotal      atomic.Uint64
	shadowWildcardDigestObservedTotal       atomic.Uint64
	shadowWildcardGatedDigestCollisionTotal atomic.Uint64
)

// ShadowWildcardDigestCounts exposes the #368 DETECTOR counters for the OTLP
// hand-wire (metrics.go; #311 — no expvar→OTLP bridge, so a detector's zero is
// hand-carried). collision is the detector (zero-means-safe), observed its
// denominator. The gated-collision diagnostic stays expvar-only and is deliberately
// NOT exported (it is expected non-zero until enumerate lands, so it must not reach
// the alerting surface).
func ShadowWildcardDigestCounts() (collision, observed int64) {
	return int64(shadowWildcardDigestCollisionTotal.Load()), int64(shadowWildcardDigestObservedTotal.Load())
}

// Bounded-memory caps (hard). The probe scope is wildcard-bearing cells only, so
// the entry count is already small; these bound the worst case with no unbounded
// growth. Overflow drops observations (dark-safe for SERVING — the probe never
// affects a verdict/serve/key).
//
// CERTIFICATION CAVEAT (pm-1217 condition 2 — empirical-capacity-caps). An LRU
// eviction or a per-entry coord/identity cap drops an observation, which toward
// COUNTING is a FALSE-NEGATIVE: a (cell,digest) collision that would have fired
// could be missed, so collision_total==0 over a capped population does NOT fully
// certify. This is safe for the dark window (never a false POSITIVE / false alarm)
// but it WEAKENS the enumerate certification. These values are a DARK Step-2 bound,
// NOT yet validated against the live wildcard (cell,digest) population: the 057
// wildcard corpus (distinct wildcard-bearing cells × digests × identities-per-cell)
// must VALIDATE/RETUNE them — and any Step-3/enumerate certification that leans on
// collision_total==0 MUST account for possible cap-eviction false-negatives (or
// confirm the caps exceed the measured population) before relying on this probe as
// the sole runtime cert detector. Same discipline as requesterProfileMemoCap
// (profile.go): a DARK bound retuned from the corpus before any promotion. Not env
// knobs (no magic env vars).
const (
	maxWildcardDigestEntries    = 4096
	maxWildcardCoordsPerEntry   = 64
	maxWildcardIdentityPerEntry = 32
)

// wildcardCoord is one evaluator coordinate observed on a cell. Namespace is
// included (the freshness-audit cross-namespace dimension). Name is deliberately
// EXCLUDED: collection-verb wildcards are name-free, and a name would explode
// cardinality without adding a wildcard-discrimination signal.
type wildcardCoord struct {
	verb, group, resource, namespace string
}

// wildcardProbeEntry is the per-(cell,digest) accumulator. identities maps a sha256
// handle → the RAW identity (username+groups), kept in-process only for the fix-(b)
// recompute and NEVER exposed. counted* make each counter fire at most once per
// (cell,digest).
type wildcardProbeEntry struct {
	el               *list.Element // position in the LRU order (key is el.Value.(string))
	coordUnion       map[wildcardCoord]struct{}
	identities       map[string]rbac.EvaluateOptions
	countedObserved  bool
	countedCollision bool
}

type wildcardDigestProbe struct {
	mu      sync.Mutex
	entries map[string]*wildcardProbeEntry
	order   *list.List // front = most-recently-touched; back = LRU victim
}

var wildcardProbe = &wildcardDigestProbe{
	entries: map[string]*wildcardProbeEntry{},
	order:   list.New(),
}

// resetWildcardDigestProbeForTest clears the probe store and counters. TEST ONLY.
func resetWildcardDigestProbeForTest() {
	wildcardProbe.mu.Lock()
	wildcardProbe.entries = map[string]*wildcardProbeEntry{}
	wildcardProbe.order = list.New()
	wildcardProbe.mu.Unlock()
	shadowWildcardDigestCollisionTotal.Store(0)
	shadowWildcardDigestObservedTotal.Store(0)
	shadowWildcardGatedDigestCollisionTotal.Store(0)
}

// domainHasWildcard reports whether D carries at least one ClassWildcard — the
// probe's scope gate (keeps cardinality bounded).
func domainHasWildcard(d AccessDomain) bool {
	for _, c := range d.Classes {
		if c.Kind == ClassWildcard {
			return true
		}
	}
	return false
}

// wildcardIdentityHandle is the sha256 dedup handle for an identity. It is the ONLY
// identity value that ever leaves memory (and even that stays internal); the raw
// username/groups are never surfaced.
func wildcardIdentityHandle(id rbac.EvaluateOptions) string {
	h := sha256.New()
	h.Write([]byte(id.Username))
	h.Write([]byte{0x1f})
	h.Write([]byte(strconv.FormatUint(rbac.CanonicalGroupsHash(id.Groups), 16)))
	return hex.EncodeToString(h.Sum(nil))
}

// observeWildcardDigest is the per-check entry point, called from runShadowParityHook
// (inside its recover). It is DARK: counters only. snap is the check's own snapshot
// (same source the verdict detector trusts); opts is the concrete check coordinate.
func (p *wildcardDigestProbe) observe(snap *cache.RBACSnapshot, sc *shadowContext, opts rbac.EvaluateOptions) {
	if sc == nil || snap == nil || !domainHasWildcard(sc.domain) {
		return
	}
	cell := sc.domain.Canonical()
	key := cell + "\x1e" + sc.digest
	coord := wildcardCoord{verb: opts.Verb, group: opts.Group, resource: opts.Resource, namespace: opts.Namespace}
	idHandle := wildcardIdentityHandle(sc.identity)

	p.mu.Lock()
	defer p.mu.Unlock()

	e := p.entries[key]
	if e == nil {
		if len(p.entries) >= maxWildcardDigestEntries {
			p.evictLRULocked()
		}
		e = &wildcardProbeEntry{
			coordUnion: map[wildcardCoord]struct{}{},
			identities: map[string]rbac.EvaluateOptions{},
		}
		e.el = p.order.PushFront(key)
		p.entries[key] = e
	} else {
		p.order.MoveToFront(e.el)
	}

	if len(e.coordUnion) < maxWildcardCoordsPerEntry {
		e.coordUnion[coord] = struct{}{}
	}
	if _, seen := e.identities[idHandle]; !seen && len(e.identities) < maxWildcardIdentityPerEntry {
		// RAW identity retained in-process ONLY, for the fix-(b) recompute.
		e.identities[idHandle] = sc.identity
	}

	// Denominator (real path only): a (cell,digest) seen with ≥2 distinct identities.
	// Counts once; shareable+ungated only (TL ruling — zero-means-safe).
	if len(e.identities) >= 2 && !e.countedObserved && sc.shareable && !sc.wildcardGated {
		shadowWildcardDigestObservedTotal.Add(1)
		e.countedObserved = true
	}

	// Collision: fix (b) — recompute EVERY identity's evaluator-fingerprint over the
	// CURRENT union and count distinct. ≥2 distinct = two different-access identities
	// collapsed to one digest. Counts once per (cell,digest).
	if !e.countedCollision && len(e.identities) >= 2 {
		union := sortedCoords(e.coordUnion)
		seen := map[string]struct{}{}
		collided := false
		for _, id := range e.identities {
			seen[fingerprintOver(snap, id, union)] = struct{}{}
			if len(seen) >= 2 {
				collided = true
				break
			}
		}
		if collided {
			e.countedCollision = true
			switch {
			case sc.wildcardGated:
				// Gated cells are never shared → diagnostic only (expected non-zero
				// until the enumerate projection lands and ungates).
				shadowWildcardGatedDigestCollisionTotal.Add(1)
			case sc.shareable:
				// The DETECTOR: a shareable, ungated cell whose digest failed to
				// distinguish two different-access identities. Zero means safe.
				shadowWildcardDigestCollisionTotal.Add(1)
			}
		}
	}
}

// evictLRULocked drops the least-recently-touched entry. Caller holds p.mu.
func (p *wildcardDigestProbe) evictLRULocked() {
	back := p.order.Back()
	if back == nil {
		return
	}
	k := back.Value.(string)
	p.order.Remove(back)
	delete(p.entries, k)
}

// fingerprintOver is the INDEPENDENT access-hash: the requester's evaluator permit
// answers over the sorted coordinate union. Built from rbac.Permits, NEVER from the
// projection — this is what makes the probe catch a projection bug the digest hides.
func fingerprintOver(snap *cache.RBACSnapshot, id rbac.EvaluateOptions, union []wildcardCoord) string {
	r := shadowProfileFor(snap, id)
	h := sha256.New()
	for _, c := range union {
		permit := r.Permits(rbac.EvaluateOptions{
			Username:  id.Username,
			Groups:    id.Groups,
			Verb:      c.verb,
			Group:     c.group,
			Resource:  c.resource,
			Namespace: c.namespace,
		})
		if permit {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		h.Write([]byte(c.verb))
		h.Write([]byte{0x1f})
		h.Write([]byte(c.group))
		h.Write([]byte{0x1f})
		h.Write([]byte(c.resource))
		h.Write([]byte{0x1f})
		h.Write([]byte(c.namespace))
		h.Write([]byte{0x1e})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sortedCoords returns the union in a deterministic order so the fingerprint is
// order-independent.
func sortedCoords(m map[wildcardCoord]struct{}) []wildcardCoord {
	out := make([]wildcardCoord, 0, len(m))
	for c := range m {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].verb != out[j].verb {
			return out[i].verb < out[j].verb
		}
		if out[i].group != out[j].group {
			return out[i].group < out[j].group
		}
		if out[i].resource != out[j].resource {
			return out[i].resource < out[j].resource
		}
		return out[i].namespace < out[j].namespace
	})
	return out
}
