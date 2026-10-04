// identity_class.go — #449: the single write path for a key's identity
// dimensions, and the single list of identity-free cache entry classes.
//
// THE CLASS OF BUG (#423, #432, #435). A cache or reuse key must fold every
// identity input that determines the cached content. It broke three times on
// three code paths because the identity dimensions were assembled by hand at
// each mint site: #423 folded only the first-match binding, #432's seed memo
// folded (username, groups) but not the RBAC class, #435's raFullList key
// folded the binding set but not the sub-generation. Each fix patched one
// site; nothing made the next site impossible to get wrong.
//
// THE STRUCTURE:
//   - IdentityClass is the requester's RBAC class: the binding-set digest and
//     the per-subject sub-generation. rbac.IdentityClassOf is its only producer
//     (package cache cannot import rbac).
//   - ResolvedKeyInputs.SetIdentity is the only writer of BindingUID,
//     SubjectBindingSet and RBACSubGen. It takes the binding and the class
//     together, so a mint site cannot set one dimension and forget another.
//   - identityFreeClasses is the only list of classes whose content is shared
//     across users and re-gated per requester at serve. ComputeKey, the
//     refresher's identity selection, the #424 drift guard and the /refreshes
//     subscription derivation all read it.
//
// scripts/checkidentitykeys enforces the first two by AST (nothing else writes
// the fields; every identity-bound hash site goes through SetIdentity; every
// SetIdentity call takes an rbac.IdentityClassOf class), and reads the third
// from this file. The dispatchers property test proves the class is sufficient
// (same class ⇒ same verdicts) and rotating (a verdict change ⇒ a new key).
package cache

import (
	"encoding/binary"
	"hash"
	"sort"
	"strconv"
)

// IdentityClass is the requester's RBAC class as the identity-bound keys fold
// it. Two identities with equal classes get the same verdict for every RBAC
// check (the binding-set digest), and an RBAC change that moves any of a
// subject's verdicts moves the class (the sub-generation, monotone per subject).
type IdentityClass struct {
	SubjectBindingSet string
	RBACSubGen        uint64
}

// String is the class as one reuse-key segment: "<binding-set digest>/<sub-gen>".
// The SeedResolveMemo key folds it (#432); the format is the one that memo has
// always used.
func (c IdentityClass) String() string {
	return c.SubjectBindingSet + "/" + strconv.FormatUint(c.RBACSubGen, 10)
}

// SetIdentity writes the identity dimensions of an identity-bound key: the
// first-match binding that authorised the cell's own GET, and the requester's
// class (rbac.IdentityClassOf). It is the only production writer of these
// fields; scripts/checkidentitykeys fails the build on any other.
func (in *ResolvedKeyInputs) SetIdentity(bindingUID string, class IdentityClass) {
	in.BindingUID = bindingUID
	in.SubjectBindingSet = class.SubjectBindingSet
	in.RBACSubGen = class.RBACSubGen
}

// Class returns the RBAC class the key was minted for.
func (in *ResolvedKeyInputs) Class() IdentityClass {
	return IdentityClass{SubjectBindingSet: in.SubjectBindingSet, RBACSubGen: in.RBACSubGen}
}

// identityFreeEncoding is how an identity-free class's key encodes the
// (absent) identity. Both encodings ignore every identity field of the inputs.
type identityFreeEncoding int

const (
	// omitIdentitySegment writes nothing (widgetContent, Ship G).
	omitIdentitySegment identityFreeEncoding = iota + 1
	// zeroIdentitySegment writes the identity segment with every dimension
	// zero — the bytes an apistage content key has always carried.
	zeroIdentitySegment
)

// identityFreeClasses is the ONLY list of identity-free cache entry classes.
//
//   - widgetContent (Ship G): the walker resolves the widget envelope under the
//     snowplow SA; gateWidgetEnvelope re-derives every resourcesRefs `allowed`
//     flag for the requester, and isRBACSensitiveApiRefWidget keeps widgets
//     whose widgetData is built from an apiRef out of the class.
//   - apistage (Ship F1): one raw, UN-gated apiserver envelope per K8s call,
//     filled only from the SA informer (WithApistageContentResolve);
//     gateContentEnvelope (or the UAF refilter) narrows it for the requester on
//     the hit AND miss paths.
//
// #449 resolved the disagreement over apistage in favour of identity-free. Its
// only mint site (contentKeyInputs) always minted it with zero identity, its
// content is the un-gated envelope, its refresher re-resolve runs under the SA,
// and it is re-gated at serve — every property of an identity-free class. Only
// ComputeKey's literal `!= widgetContent` test and DeriveSubscriptionKey treated
// it as identity-bound, and both did so for keys no mint site produces. Folding
// the zero segment keeps every existing apistage key byte-identical (the
// key-parity golden) while making a caller-set identity on apistage impossible
// to fold.
var identityFreeClasses = map[string]identityFreeEncoding{
	CacheEntryClassWidgetContent: omitIdentitySegment,
	CacheEntryClassApistage:      zeroIdentitySegment,
}

// IsIdentityFreeClass reports whether class is identity-free: shared content,
// re-gated per requester at serve, keyed without any identity.
func IsIdentityFreeClass(class string) bool {
	_, ok := identityFreeClasses[class]
	return ok
}

// IdentityFreeClasses returns the identity-free classes, sorted.
func IdentityFreeClasses() []string {
	out := make([]string, 0, len(identityFreeClasses))
	for c := range identityFreeClasses {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// writeIdentitySegment is the identity segment of ComputeKey's encoding.
//
//   - BindingUID, then 0xff;
//   - the binding-set digest (#423: fixed-width hex SHA-256, or "" without a
//     snapshot), then 0xfd so it cannot run into the sub-gen bytes;
//   - the sub-generation (#118 (c)) as uint64 LE, then 0xfe.
//
// The three terminators are distinct so no dimension can alias another.
func writeIdentitySegment(h hash.Hash, bindingUID, subjectBindingSet string, subGen uint64) {
	h.Write([]byte(bindingUID))
	h.Write([]byte{0xff})
	h.Write([]byte(subjectBindingSet))
	h.Write([]byte{0xfd})
	var sg [8]byte
	binary.LittleEndian.PutUint64(sg[:], subGen)
	h.Write(sg[:])
	h.Write([]byte{0xfe})
}
