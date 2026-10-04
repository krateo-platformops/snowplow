package apiref

import (
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// f6KeyInputs is cache.RAFullListKeyInputs PLUS the #423 SubjectBindingSet the
// production key derivation (seedFullListRAKey) folds. Tests that hand-build the
// raKey "the serve path derives" must model that dimension too, or they compute
// a key production never mints. The f6 fixture's identity per first-match
// binding is fixed (f6_cross_binding_isolation_test.go header):
//
//	C:crb-a-f6-uid → admin / [system:masters]  (ctxWithUser, f6 user A)
//	C:crb-b-f6-uid → dev-1 / [devs]            (f6 user B)
//	""             → no identity (the empty-identity key; no digest)
func f6KeyInputs(group, version, resource, namespace, name, bindingUID string, extras map[string]any) cache.ResolvedKeyInputs {
	in := cache.RAFullListKeyInputsForTest(group, version, resource, namespace, name, bindingUID, extras)
	switch bindingUID {
	case "C:crb-a-f6-uid":
		in.SubjectBindingSet = rbac.SubjectBindingSetDigest("admin", []string{"system:masters"})
	case "C:crb-b-f6-uid":
		in.SubjectBindingSet = rbac.SubjectBindingSetDigest("dev-1", []string{"devs"})
	}
	return in
}

// keyWithoutSBS423 recomputes a key with the #423 SubjectBindingSet blanked — the
// key the pre-#423 derivation would mint (arms' "would have shared" precondition).
func keyWithoutSBS423(in cache.ResolvedKeyInputs) string {
	in.SubjectBindingSet = ""
	return cache.ComputeKey(in)
}
