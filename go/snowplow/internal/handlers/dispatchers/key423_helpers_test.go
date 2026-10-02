package dispatchers

import (
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// e3286KeyInputs is cache.RAFullListKeyInputs PLUS the #423 SubjectBindingSet the
// production raKey derivation folds, for the #286 fixture's identity
// (admin / [system:masters], e3286Ctx). A hand-built "production raKey" that
// omits it names a cell production never mints.
func e3286KeyInputs(group, version, resource, namespace, name, bindingUID string, extras map[string]any) cache.ResolvedKeyInputs {
	in := cache.RAFullListKeyInputs(group, version, resource, namespace, name, bindingUID, extras)
	in.SubjectBindingSet = rbac.SubjectBindingSetDigest("admin", []string{"system:masters"})
	return in
}

// keyWithoutSBS423 recomputes a key with the #423 SubjectBindingSet blanked — the
// key the pre-#423 derivation would have minted for the same inputs. Used by arms
// whose precondition is "these two identities WOULD have shared one cell".
func keyWithoutSBS423(in cache.ResolvedKeyInputs) string {
	in.SubjectBindingSet = ""
	return cache.ComputeKey(in)
}

// sbsOf423 reads the #423 SubjectBindingSet (diagnostics in arm messages).
func sbsOf423(in cache.ResolvedKeyInputs) string { return in.SubjectBindingSet }
