// Package listenvelope is the single definition of the LIST envelope that
// snowplow serves to a RESTAction stage from its informers and from its
// content cache: exactly {apiVersion, kind, items}, with no list-level
// metadata (no resourceVersion, no continue).
//
// This is invariant E6 of the #603 design (§1). The jqinterest analysis is
// sound only under it: toItemReads maps `.[]` over the envelope onto items
// alone, and a whole or kind/shape read of the envelope onto item reads
// alone (internal/cache/jqinterest/interest.go). A list-level key that
// changes on every event (metadata.resourceVersion) would make an UPDATE of
// an UNREAD item field change the output while every recorded projection is
// unchanged: silent under-invalidation.
//
// The guard is internal/resolvers/restactions/api/envelope_e6_test.go
// (#674): a table test over every envelope constructor in that package, and
// an AST check that fails when a function there builds or decodes a list
// envelope without being registered in the table. The jqinterest emulator
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
	return map[string]any{
		APIVersion: apiVersion,
		Kind:       kind,
		Items:      items,
	}
}
