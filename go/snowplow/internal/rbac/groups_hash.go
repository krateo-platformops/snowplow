// groups_hash.go — Ship L2 (0.30.253).
//
// canonicalGroupsHash is the SINGLE SOURCE OF TRUTH for hashing an RBAC
// groups SET into a uint64. It exists so the snapshot authz memo
// (snapshot_authz_memo.go) can fold the unordered group membership into
// one comparable scalar for its key (snapshotAuthzKey.GroupsHash)
// WITHOUT a second, divergent hasher.
//
// B3 (PM gate) — the deleted `BindingSetHash` symbol (removed in
// 0.30.242 / a6ff9fd) left only historical comments behind; there is NO
// live canonical-set hasher to reuse. This file adds exactly one. Do NOT
// inline a second groups hasher anywhere — that is the 0.30.239 failure
// mode (two hashers drifting out of parity).
//
// COLLISION SAFETY (the 0.30.239 lesson): groups are an UNORDERED set,
// so the hash MUST be order-independent (we sort first) AND must make
// the concatenation boundary unambiguous so distinct sets cannot alias.
// The classic alias is ["a","bc"] vs ["ab","c"]: naive concatenation of
// either yields "abc". We defeat it with a per-element LENGTH PREFIX
// (uint64 length, little-endian, fed to the hash before the element
// bytes). Two different element partitions can never produce the same
// length-prefixed byte stream, so the alias is impossible. This is
// covered by a dedicated collision test (groups_hash_test in evaltest).
//
// GROUP-EQUIVALENCE PARITY WITH THE EVALUATOR (B3): the verdict that the
// memo caches is produced by anySubjectMatches / effectiveGroups
// (evaluate.go), which match Group subjects against opts.Groups plus —
// for a ServiceAccount identity — the two synthetic SA groups
// (system:serviceaccounts, system:serviceaccounts:<ns>) and, for any
// authenticated identity, system:authenticated. Those synthetic/implicit
// groups are derived deterministically FROM (Username, opts.Groups) by
// the evaluator itself; they are NOT free inputs. Therefore hashing the
// caller-supplied opts.Groups (this function's input) together with
// Username already in the memo key uniquely determines effectiveGroups,
// so two requests with the same (Username, GroupsHash) are guaranteed to
// expand to the same effective group set and thus the same verdict. We
// deliberately hash the RAW opts.Groups (not the expanded set) so the
// hash input is exactly what the caller controls; the expansion is a
// pure function of (Username, Groups) downstream.

package rbac

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

// canonicalGroupsHash returns an order-independent FNV-1a (64-bit) hash
// of the groups set. nil and empty slices hash to the same value (the
// empty-set hash). The input slice is NOT mutated — a copy is sorted.
//
// Algorithm (the single canonical form):
//  1. copy + sort.Strings (set canonicalisation — order independence)
//  2. for each element: write its byte length as a uint64 (little-
//     endian, 8 bytes) THEN the element bytes (length-prefix framing —
//     collision safety)
//  3. return the FNV-1a sum
//
// Duplicate group names in the input are intentionally NOT de-duplicated
// here: opts.Groups from the auth layer is already a set, and a repeated
// element would change the hash — but the evaluator's anySubjectMatches
// also iterates opts.Groups verbatim, so a duplicate is treated
// identically on both sides (it cannot change the verdict, and it cannot
// change the hash relative to another identical-duplicate request). The
// hash's only contract is: equal input group SLICES (post-sort) ->
// equal hash; unequal group SETS -> different hash. Both hold.
func canonicalGroupsHash(groups []string) uint64 {
	h := fnv.New64a()
	if len(groups) == 0 {
		// Hash the empty stream — a stable, distinct value for "no
		// groups" that cannot collide with any non-empty set (which
		// always writes at least one 8-byte length prefix).
		return h.Sum64()
	}
	sorted := make([]string, len(groups))
	copy(sorted, groups)
	sort.Strings(sorted)

	var lenbuf [8]byte
	for _, g := range sorted {
		binary.LittleEndian.PutUint64(lenbuf[:], uint64(len(g)))
		_, _ = h.Write(lenbuf[:])
		_, _ = h.Write([]byte(g))
	}
	return h.Sum64()
}

// CanonicalGroupsHash is the exported, production-safe view of
// canonicalGroupsHash — the SINGLE canonical groups hasher (do NOT inline a
// second one, per this file's header). v7 Step 2D uses it to fold a requester's
// groups SET into one order-independent scalar for the dark shadow-parity
// anomaly log, so the log carries a HASHED identity only (never the raw group
// names) — the debug-surface rule (metadata + hash, never per-identity content).
func CanonicalGroupsHash(groups []string) uint64 {
	return canonicalGroupsHash(groups)
}
