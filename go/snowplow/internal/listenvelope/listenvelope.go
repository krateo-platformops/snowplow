// Package listenvelope is the single definition of the LIST envelope that a
// RESTAction stage receives for a Kubernetes collection LIST: exactly
// {apiVersion, kind, items}, with apiVersion/kind synthesized from the GVR and
// no list-level metadata (no resourceVersion, no continue). It holds on EVERY
// path. The informer, the content cache, the cluster-list collapse and
// branch C (internal-driver dispatch, informer-served or live) build with it.
// The httpcall.Do apiserver fall-through is normalised to it at
// jsonHandlerCore (restactions/api normaliseListEnvelope, #676 C2(a)). A
// list-level .metadata read is therefore never possible on any path.
//
// This is invariant E6 of the #603 design (§1). The jqinterest analysis is
// sound only under it: toItemReads maps `.[]` over the envelope onto items
// alone, and a whole or kind/shape read of the envelope onto item reads
// alone (internal/cache/jqinterest/interest.go). A list-level key that
// changes on every event (metadata.resourceVersion) would make an UPDATE of
// an UNREAD item field change the output while every recorded projection is
// unchanged: silent under-invalidation.
//
// The guards (#674) live in internal/resolvers/restactions/api:
//   - envelope_e6_test.go: a table test over every envelope constructor
//     there and in internal/cache, and an AST check that fails on an
//     unregistered builder;
//   - envelope_e6_chokepoint_test.go: a check on the envelope every stage
//     receives;
//   - envelope_e6_fallthrough_test.go: the apiserver fall-through arms.
//
// The jqinterest emulator
// builds its envelope with New too, so the differential fuzz runs on the
// same key set production serves.
package listenvelope

// The envelope keys.
const (
	APIVersion = "apiVersion"
	Kind       = "kind"
	Items      = "items"
)

// Keys returns the envelope's complete key set, sorted.
func Keys() []string { return []string{APIVersion, Items, Kind} }

// New builds the envelope as a decoded value. items must be non-nil for an
// empty list (`"items": []`, never null, so `.items[]` yields an empty
// stream, as the apiserver does).
func New(apiVersion, kind string, items []any) map[string]any {
	if items == nil {
		items = []any{}
	}
	return map[string]any{
		APIVersion: apiVersion,
		Kind:       kind,
		Items:      items,
	}
}

// For builds the envelope of a LIST of gvr with the apiVersion and kind
// synthesized from the GVR, so every path serves the same envelope for the
// same GVR whatever its items are (an empty list included).
func For(group, version, resource string, items []any) map[string]any {
	return New(APIVersionFor(group, version), KindFor(resource), items)
}

// APIVersionFor is the apiserver convention: "v" for the core group, "g/v"
// otherwise.
func APIVersionFor(group, version string) string {
	if group == "" {
		return version
	}
	return group + "/" + version
}

// KindFor is the LIST kind snowplow synthesizes from the resource: the
// resource with its first ASCII letter upper-cased, plus "List" (PM
// 2026-05-15, DEV-Q2), or "List" for an empty resource.
func KindFor(resource string) string {
	if resource == "" {
		return "List"
	}
	first := resource[0]
	if first >= 'a' && first <= 'z' {
		first -= 'a' - 'A'
	}
	return string(first) + resource[1:] + "List"
}
