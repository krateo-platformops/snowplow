// identity_class.go — #449: the single derivation of a requester's RBAC class.
//
// Every identity-bound key (restactions / widgets / raFullList cells, the
// SeedResolveMemo key) and every re-check of one (#424 identityClassDrift,
// raKeyClassCurrent) derives the class here, so a new class dimension is added
// in one place and reaches every key at once. Before #449 the two dimensions
// were assembled by hand at five sites, and #435 was one of those copies
// falling behind.

package rbac

import (
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// IdentityClassOf returns ui's current RBAC class:
//
//   - SubjectBindingSet (#423): the digest of every binding whose subjects match
//     the requester. Equal digests ⇒ equal verdicts for every RBAC check at
//     one instant, so sharing a cell between them is sound.
//   - RBACSubGen (#118 (c)): the sub-generation over the EFFECTIVE groups
//     (system:authenticated included, #424): the number of the last RBAC state
//     change that touched the requester (#1439). It moves on any grant, revoke
//     or rules edit touching the subject — including a grant-then-revoke or a
//     Role edit that leaves the binding set unchanged (#435) — and one value is
//     one state, so equal classes at two different times are equal verdicts
//     too, PROVIDED every subject through which a binding matches the requester
//     is one the sub-generation folds and the binding is in the role index.
//     The first holds by construction since #1443 (the fold reads the subjects
//     the evaluator matches with, cache.SubjectMatcher). The one known
//     exception to the second (cache.RBACSubGenForSubject states it): until
//     #1458 lands, a binding that an index rebuild dropped because the
//     snapshot it read was older than the binding's already-applied event.
//
// Both read the published snapshot / counters; neither is ever logged.
func IdentityClassOf(ui jwtutil.UserInfo) cache.IdentityClass {
	return cache.IdentityClass{
		SubjectBindingSet: SubjectBindingSetDigest(ui.Username, ui.Groups),
		RBACSubGen:        cache.RBACSubGenForSubject(ui.Username, WithAuthenticatedGroup(ui.Username, ui.Groups)),
	}
}
