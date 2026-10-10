// Fixture: identity-key mint sites. Lines marked BAD must be flagged; lines
// marked GOOD must not. main_test.go pins them by line number.
package app

import (
	"example/internal/cache"
	"example/internal/rbac"
)

// GOOD: the dispatch shape — SetIdentity(IdentityClassOf).
func goodDispatch(ui rbac.UserInfo, uid string) string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "ra"}
	in.SetIdentity(uid, rbac.IdentityClassOf(ui))
	return cache.ComputeKey(in)
}

// GOOD: a builder that forwards the class, called with IdentityClassOf.
func goodRAFullList(ui rbac.UserInfo, uid string) string {
	in := cache.BoundKeyInputs("ra", uid, rbac.IdentityClassOf(ui))
	return cache.ComputeKey(in)
}

// GOOD: re-hash of carried inputs.
func goodRehash(in cache.ResolvedKeyInputs) string {
	return cache.ComputeKey(in)
}

// GOOD: an identity-free class never takes an identity.
func goodContent() string {
	return cache.ComputeKey(cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassApistage, Name: "cm"})
}

// GOOD: the reuse key folds the class.
func goodMemo(ctx any, ui rbac.UserInfo) string {
	memo := cache.SeedResolveMemoFromContext(ctx)
	return memo.Key("ns", "ra", ui.Username, ui.Groups, rbac.IdentityClassOf(ui).String())
}

// BAD (R1, the #435 shape): identity-bound inputs hashed with no class at all.
func badNoClass() string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "raFullList", Name: "ra"}
	return cache.ComputeKey(in) // BAD-R1
}

// BAD (R2): hand-assembled identity, the pre-#449 shape.
func badHandAssembled(ui rbac.UserInfo, uid string) string {
	in := cache.ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		BindingUID:      uid, // BAD-R2-literal
	}
	in.SetIdentity(uid, rbac.IdentityClassOf(ui))
	in.RBACSubGen = 0 // BAD-R2-assign
	return cache.ComputeKey(in)
}

// BAD (R3): a class that did not come from IdentityClassOf.
func badForgedClass(uid string) string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "ra"}
	in.SetIdentity(uid, cache.IdentityClass{SubjectBindingSet: "x"}) // BAD-R3
	return cache.ComputeKey(in)
}

// BAD (R3): the forwarding builder called with a forged class.
func badForwardForged(uid string) string {
	return cache.ComputeKey(cache.BoundKeyInputs("ra", uid, cache.IdentityClass{})) // BAD-R3-forward
}

// BAD (R3): production calling the identity test seam.
func badSeam() string {
	return cache.ComputeKey(cache.BoundKeyInputsForTest("ra", "uid")) // BAD-R3-seam
}

// BAD (R4, the #432 shape): the reuse key folds (username, groups) only.
func badMemo(ctx any, ui rbac.UserInfo) string {
	memo := cache.SeedResolveMemoFromContext(ctx)
	return memo.Key("ns", "ra", ui.Username, ui.Groups, "") // BAD-R4
}

// BAD (R1): an identity-free class given an identity.
func badFreeWithIdentity(ui rbac.UserInfo) string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassWidgetContent, Name: "w"}
	in.SetIdentity("uid", rbac.IdentityClassOf(ui))
	return cache.ComputeKey(in) // BAD-R1-free
}

// BAD (R2, review D4): the #435 shape as an IdentityClass field write between
// IdentityClassOf and SetIdentity.
func badEditedClass(ui rbac.UserInfo, uid string) string {
	c := rbac.IdentityClassOf(ui)
	c.RBACSubGen = 0 // BAD-R2-classedit
	in := cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "ra"}
	in.SetIdentity(uid, c)
	return cache.ComputeKey(in)
}

// BAD (R2, review D3): a pointer to an identity field, written after SetIdentity.
func badAddressOf(ui rbac.UserInfo, uid string) string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "ra"}
	in.SetIdentity(uid, rbac.IdentityClassOf(ui))
	p := &in.RBACSubGen // BAD-R2-addr
	*p = 0
	return cache.ComputeKey(in)
}

// BAD (R2): a pointer to an IdentityClass field.
func badClassAddressOf(ui rbac.UserInfo) *string {
	c := rbac.IdentityClassOf(ui)
	return &c.SubjectBindingSet // BAD-R2-classaddr
}

// BAD (R3): a second class derivation, the shape #435's lagging copy had.
func badOwnClass(ui rbac.UserInfo) string {
	return rbac.SubjectBindingSetDigest(ui.Username, ui.Groups) // BAD-R3-own
}

// BAD (S2): assigns a scope dimension directly; only SetScope may write it.
func badScopeDirect(name string) string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Name: name}
	in.UAFScopeDigest = "forged"
	return cache.ComputeKey(in)
}

// BAD (S2): sets a scope dimension in a ResolvedKeyInputs literal.
func badScopeLiteral(name string) string {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Name: name, UAFScopeDigest: "forged"}
	return cache.ComputeKey(in)
}

// BAD (S3): a second scope derivation; only ScopeClassOf may read the sources.
func badScopeDerivation(profile, domain any) string {
	return cache.ComputeProjectionDigest(profile, domain)
}
