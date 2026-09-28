// shadow_parity.go — v7 Step 2C: the PURE, DARK projection P, its digest, and
// the shareable / name-ambiguity classification.
//
// WHAT THIS IS. Given the identity-free access domain D (access_domain.go, Step
// 2a) and a single requester's dark profile R (internal/rbac/profile.go, Step
// 2b), this file computes:
//
//   - Projection P: R's answer to every AccessClass in D, using the EXACT
//     evaluator predicate (rbac.RequesterProfile.Permits) verbatim — never a
//     re-implementation of rule matching (design s2-fulldesign §3).
//   - Digest: a canonical, deterministic sha256 over D.Canonical() concatenated
//     with P's per-class answers in D's sorted class order. PURE. In Step 2C the
//     digest is folded into NO cache key, stored on NO cache entry, and changes
//     NO verdict and NO served byte.
//   - shareable(D): the structural leak-closure predicate (design §2.4) — a cell
//     is NOT shareable if D carries any name-ambiguous class or any escape.
//
// DARK (Step 2C invariant): these are PURE functions. Nothing here is wired to
// dispatch / seed / resolve / serving; nothing computes on a request path; and
// NOTHING here emits a counter or a log. The shadow hook, the ctx shadow
// context, the dispatcher-entry D-derivation, and the helpers.go emit sites are
// Step 2D/2E — deliberately absent. The only callers of these functions in Step
// 2C are this package's unit tests.
//
// REUSE, NOT RE-IMPLEMENTATION (hard constraint): the projection consults
// rbac.RequesterProfile.Permits (the exported evaluator-faithful predicate) and
// the name-ambiguity rule consults rbac.IsNameSpecificVerb. No rule-matching or
// domain-derivation logic is re-implemented here.
//
// ⚠️ GATED — the ClassWildcard projection is NOT computed here. The design
// (s2-fulldesign §3, one clause) under-specifies it on two safety-critical
// points (match direction when the CLASS side carries "*", and the exact
// canonical atom encoding); guessing it could seed a false-share leak in Step 3.
// Per the task's explicit instruction, the wildcard projection is represented by
// an explicit gated sentinel (AnswerWildcardGated) rather than a fabricated
// value, and Projection.WildcardGated flags any cell whose digest incorporates
// it. See the doc on projectClass for the exact open questions. This gate does
// NOT affect the shareable classification (§2.4 is fully specified and is
// implemented exactly) nor the ClassExact / ClassNamespaceSet projections.

package dispatchers

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// ClassAnswerKind discriminates the projected answer shape for one AccessClass.
type ClassAnswerKind uint8

const (
	// AnswerBool is a single permit/deny bool — the projection of a ClassExact.
	AnswerBool ClassAnswerKind = iota
	// AnswerNamespaceSet is the projection of a ClassNamespaceSet: either ⊤
	// (ClusterPermit — R permits the (verb,group,resource) cluster-wide) or the
	// sorted SET of namespaces (drawn from R.NamespacedRules' keys) where R
	// permits the per-object check.
	AnswerNamespaceSet
	// AnswerWildcardGated is the placeholder for a ClassWildcard projection whose
	// canonical encoding is GATED pending a design decision (see projectClass). It
	// carries no fabricated permit value — it is an explicit sentinel so a digest
	// that incorporates it is deterministic (F-D7b) yet visibly incomplete
	// (Projection.WildcardGated == true).
	AnswerWildcardGated
)

// ClassAnswer is R's projected answer to one AccessClass. Its zero value is a
// deny bool. The active fields are selected by Kind.
type ClassAnswer struct {
	Kind ClassAnswerKind
	// Permit is meaningful for AnswerBool only.
	Permit bool
	// ClusterPermit is meaningful for AnswerNamespaceSet: true == ⊤ (permitted in
	// every namespace via a cluster-wide grant). When true, Namespaces is empty.
	ClusterPermit bool
	// Namespaces is meaningful for AnswerNamespaceSet when ClusterPermit is false:
	// the sorted set of namespaces where R permits the per-object check. Always
	// sorted and deduplicated (map keys are unique) so the encoding is
	// order-independent.
	Namespaces []string
}

// canonical is the delimiter-safe, deterministic encoding of one answer, folded
// into the digest. It uses the same US (0x1f) field delimiter discipline as
// AccessClass.canonical (access_domain.go); a kube namespace never contains US.
func (a ClassAnswer) canonical() string {
	switch a.Kind {
	case AnswerBool:
		if a.Permit {
			return "a:bool\x1f1"
		}
		return "a:bool\x1f0"
	case AnswerNamespaceSet:
		if a.ClusterPermit {
			return "a:nsset\x1fTOP"
		}
		// a.Namespaces is already sorted by projectClass.
		return "a:nsset\x1f" + strings.Join(a.Namespaces, "\x1f")
	case AnswerWildcardGated:
		return "a:wild\x1fGATED"
	default:
		return "a:unknown\x1fGATED"
	}
}

// Projection (P) is R projected onto every class of D, in D's sorted class
// order (Answers[i] is the answer for D.Classes[i]). It is a deterministic pure
// function of (R, D).
type Projection struct {
	// Answers is aligned index-for-index with the AccessDomain's sorted Classes.
	Answers []ClassAnswer
	// HasEscape mirrors "D.Escapes present → P records an unclassifiable marker"
	// (design §3): the cell is not purely shareable on D alone.
	HasEscape bool
	// WildcardGated is true iff D contains at least one ClassWildcard, whose
	// projection encoding is gated (AnswerWildcardGated). A digest with
	// WildcardGated == true is deterministic but INCOMPLETE.
	//
	// LOAD-BEARING SHARING CONTRACT: a cell may be shared for real use in Step 3
	// ONLY IF Shareable(D) AND !Projection.WildcardGated AND shadow-parity-
	// verified. Shareable(D) alone is NOT sufficient — a collection-verb-wildcard
	// cell can be Shareable(D)==true while its projection is wildcard-gated, so its
	// digest does not yet distinguish identities on the wildcard-covered access.
	// Use DigestTrustworthy(p) for the !WildcardGated half of the contract.
	WildcardGated bool
}

// DigestTrustworthy reports whether the projection's digest is COMPLETE enough to
// be trusted for identity discrimination — i.e. no class in D was wildcard-gated.
// It is the !WildcardGated half of the Step-3 sharing contract (see the
// WildcardGated field and Shareable): a cell is trustable-shareable only when
// Shareable(D) AND DigestTrustworthy(p) AND shadow-parity-verified. In Step 2C it
// gates nothing (pure); it exists so a later step cannot forget the wildcard gate.
func DigestTrustworthy(p Projection) bool { return !p.WildcardGated }

// ProjectRequesterProfile projects R onto D. PURE and deterministic. It calls
// rbac.RequesterProfile.Permits verbatim for the fully-specified class kinds and
// emits the gated sentinel for ClassWildcard.
func ProjectRequesterProfile(r *rbac.RequesterProfile, d AccessDomain) Projection {
	p := Projection{
		Answers:   make([]ClassAnswer, 0, len(d.Classes)),
		HasEscape: len(d.Escapes) > 0,
	}
	for _, c := range d.Classes {
		ans := projectClass(r, c)
		if ans.Kind == AnswerWildcardGated {
			p.WildcardGated = true
		}
		p.Answers = append(p.Answers, ans)
	}
	return p
}

// projectClass computes R's answer to one class, per design s2-fulldesign §3:
//
//   - ClassExact(v,g,r,ns,nm) → one bool: R.Permits({v,g,r,ns,nm}). Because R
//     stores the raw PolicyRules and Permits re-runs the evaluator's rulesPermit,
//     resourceNames / name-specific / wildcard matching is the served
//     evaluator's, bit for bit.
//
//   - ClassNamespaceSet(v,g,r) → (clusterPermit, {ns : R permits (v,g,r,ns)}).
//     clusterPermit is computed with an EMPTY Namespace: Permits then consults
//     ONLY the cluster rules (its namespaced term is guarded by
//     opts.Namespace != ""), so clusterPermit == rulesPermit(ClusterRules, opts_c)
//     exactly. When clusterPermit is false, the per-namespace answer for each ns
//     is Permits({v,g,r,ns}) == rulesPermit(ClusterRules,·) || rulesPermit(
//     NamespacedRules[ns],·) == rulesPermit(NamespacedRules[ns],·) (the cluster
//     term is already known false) — i.e. exactly the design's
//     {ns : rulesPermit(NamespacedRules[ns], opts_c)}. The namespace UNIVERSE is
//     R.NamespacedRules' keys (design §3 / task scope item 1). Reuses Permits
//     verbatim; adds no new rbac export.
//
//   - ClassWildcard(v,g,r|*) → GATED (AnswerWildcardGated). The design's one
//     clause — "the set of R rule atoms whose (verb,group,resource) match under
//     stringSliceMatches ... recorded as a wildcard-covered marker, never
//     collapsed to a false permit ... canonicalized + deduped" — is materially
//     ambiguous on:
//     (1) MATCH DIRECTION when the CLASS side carries "*" (a templated group /
//     resource). stringSliceMatches(allowed, want) treats only the `allowed`
//     side's "*" as wildcard, so the literal reading (rule as `allowed`,
//     class field as `want`) makes a class group="*" match ONLY R rules
//     whose APIGroups literally contain "*". That contradicts the design's
//     own gloss "the atoms a runtime-discovered concrete GVR could hit"
//     (which wants class "*" to match ALL of R's atoms). The two readings
//     give different atom sets and different digests. Under-counting (the
//     literal reading) is the UNSAFE direction for a shareable collection-
//     verb wildcard: two identities with different concrete group grants
//     would project to the same (empty) set and collide → a false-share
//     leak once Step 3 shares the cell.
//     (2) The exact canonical ATOM ENCODING (which PolicyRule fields — Verbs /
//     APIGroups / Resources / ResourceNames? — in what sorted, deduped
//     string form) is not pinned to a function the way AccessClass.canonical
//     is.
//     Per the task's explicit instruction ("if the design under-specifies the
//     wildcard projection ... STOP and report rather than guess"), the encoding
//     is NOT invented here; a gated sentinel is emitted so the rest of Step 2C is
//     complete and testable. The sentinel keeps the digest deterministic but
//     visibly incomplete (Projection.WildcardGated).
//
//     NOTE ON REACH: a ClassWildcard is name-ambiguous — and therefore the cell
//     is NOT shareable (§2.4) — for every name-specific verb and for verb "*".
//     Only a COLLECTION-verb wildcard (list / create / watch / deletecollection)
//     is both shareable AND needs a trusted projection value; that is the exact
//     population the gated encoding decision governs.
func projectClass(r *rbac.RequesterProfile, c AccessClass) ClassAnswer {
	switch c.Kind {
	case ClassExact:
		return ClassAnswer{
			Kind: AnswerBool,
			Permit: r.Permits(rbac.EvaluateOptions{
				Verb: c.Verb, Group: c.Group, Resource: c.Resource,
				Namespace: c.Namespace, Name: c.Name,
			}),
		}

	case ClassNamespaceSet:
		// clusterPermit: EMPTY Namespace ⇒ Permits consults only ClusterRules.
		if r.Permits(rbac.EvaluateOptions{Verb: c.Verb, Group: c.Group, Resource: c.Resource}) {
			return ClassAnswer{Kind: AnswerNamespaceSet, ClusterPermit: true}
		}
		// cluster term is false ⇒ Permits({...,ns}) == per-namespace rulesPermit.
		var nss []string
		if r != nil {
			for ns := range r.NamespacedRules {
				if ns == "" {
					continue // RoleBindings always carry a non-empty namespace.
				}
				if r.Permits(rbac.EvaluateOptions{
					Verb: c.Verb, Group: c.Group, Resource: c.Resource, Namespace: ns,
				}) {
					nss = append(nss, ns)
				}
			}
		}
		sort.Strings(nss)
		return ClassAnswer{Kind: AnswerNamespaceSet, Namespaces: nss}

	case ClassWildcard:
		return ClassAnswer{Kind: AnswerWildcardGated}

	default:
		return ClassAnswer{Kind: AnswerWildcardGated}
	}
}

// ComputeProjectionDigest returns the canonical sha256 digest of P over D, plus
// the Projection it hashed. The payload is D.Canonical() (access_domain.go:184 —
// sorted class then escape canonicals, RS-delimited), an RS, then each class's
// projected answer canonical in D's sorted class order, RS-delimited. PURE and
// deterministic: D.Classes are pre-sorted by the deriver's builder and every
// set-valued answer (namespace sets) is sorted, so the digest is independent of
// map-iteration order (F-D7b). Computed here, folded into NOTHING (Step 2C).
func ComputeProjectionDigest(r *rbac.RequesterProfile, d AccessDomain) (string, Projection) {
	p := ProjectRequesterProfile(r, d)
	h := sha256.New()
	// Escapes are already inside D.Canonical(), so they affect the digest.
	h.Write([]byte(d.Canonical()))
	h.Write([]byte("\x1e"))
	for i := range d.Classes {
		h.Write([]byte(p.Answers[i].canonical()))
		h.Write([]byte("\x1e"))
	}
	return hex.EncodeToString(h.Sum(nil)), p
}

// ─────────────────────────────────────────────────────────────────────────
// Shareable / name-ambiguity classification (design §2.4 — the structural leak
// closure; LOAD-BEARING). Fully specified; implemented exactly.
// ─────────────────────────────────────────────────────────────────────────

// nameAmbiguous reports whether class c would let two identities that differ
// ONLY in a resourceNames-scoped grant collapse to one digest — i.e. the class's
// verb is name-specific (get/update/patch/delete) OR the verb wildcard "*", AND
// the class does NOT pin a literal object name:
//
//	nameAmbiguous(c) := (IsNameSpecificVerb(c.Verb) ∨ c.Verb=="*") ∧
//	                    (c.Kind ∈ {ClassNamespaceSet, ClassWildcard} ∨
//	                     (c.Kind==ClassExact ∧ c.Name==""))
//
// A ClassExact with a literal Name != "" is name-pinned → P is faithful →
// shareable. A collection-verb class (list/watch/create/deletecollection) is
// never name-ambiguous: resourceNames cannot apply, so a name-free projection at
// Name=="" is exactly what the real collection check evaluates. A verb-"*"
// wildcard IS name-ambiguous (it subsumes get) — this closes R1-m1 with no
// atom-canonicalization change. IsNameSpecificVerb is reused verbatim from the
// evaluator (rbac.IsNameSpecificVerb).
func nameAmbiguous(c AccessClass) bool {
	if !rbac.IsNameSpecificVerb(c.Verb) && c.Verb != AccessWildcard {
		return false
	}
	switch c.Kind {
	case ClassNamespaceSet, ClassWildcard:
		return true
	case ClassExact:
		return c.Name == ""
	default:
		// Unknown kind: be conservative — treat as name-ambiguous (not shareable).
		return true
	}
}

// Shareable reports whether cell domain D is structurally safe to share across
// identities on D alone (design §2.4): NO escape and NO name-ambiguous class. In
// Step 2C this gates NOTHING (it is a pure predicate consumed only by tests and,
// in Step 2E, by a measurement counter). Because a name-ambiguous cell is
// structurally never shareable, two identities differing only in a resourceNames
// grant can never share a key — cluster-state-independently.
//
// ⚠️ NOT SUFFICIENT ALONE. Shareable(D) is one conjunct of the Step-3 sharing
// contract, not the whole of it: a cell may be shared for real use ONLY IF
// Shareable(D) AND DigestTrustworthy(p) (== !Projection.WildcardGated) AND
// shadow-parity-verified. A collection-verb ClassWildcard is name-free with a
// collection verb, so it is NOT name-ambiguous and Shareable(D) can be true —
// yet its projection is wildcard-gated (AnswerWildcardGated), so the digest does
// not yet distinguish identities on the wildcard-covered access. Sharing on
// Shareable(D) alone would leak. Always AND in DigestTrustworthy(p).
func Shareable(d AccessDomain) bool {
	if len(d.Escapes) > 0 {
		return false
	}
	for _, c := range d.Classes {
		if nameAmbiguous(c) {
			return false
		}
	}
	return true
}

// NotShareableTally is the per-reason breakdown of why a cell is not shareable —
// the shape Step 2E's `v7_cell_not_shareable_total{reason}` counter will read.
// A cell may hit BOTH reasons; each field counts the classes/escapes responsible
// (Escape == len(D.Escapes); NameAmbiguous == count of name-ambiguous classes).
// A fully-shareable cell tallies zero on both. Emits nothing in Step 2C.
type NotShareableTally struct {
	NameAmbiguous int
	Escape        int
}

// Any reports whether the tally records any not-shareable reason (equivalent to
// !Shareable(D) for the domain it was built from).
func (t NotShareableTally) Any() bool { return t.NameAmbiguous > 0 || t.Escape > 0 }

// NotShareableReasons tallies, per reason, why D is not shareable. Pure.
func NotShareableReasons(d AccessDomain) NotShareableTally {
	t := NotShareableTally{Escape: len(d.Escapes)}
	for _, c := range d.Classes {
		if nameAmbiguous(c) {
			t.NameAmbiguous++
		}
	}
	return t
}
