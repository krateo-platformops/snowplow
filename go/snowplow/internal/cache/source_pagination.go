// source_pagination.go — #874: "is there another page" is the DATA SOURCE's
// answer, never an inference from how many items a page returned.
//
// THE DEFECT. The widget resolver used to emit
// status.resourcesRefs.slice.continue = (len(items) >= perPage). A source that
// ignores the requested window returns the same >= perPage items on every page,
// so it looked infinitely paginated: on krateo-057 the sidebar-nav menu (its
// RESTAction emits one ref per page root and never reads .slice) was walked
// 12-16K pages to the drain deadline on 1.12.50/51 and 20,000 pages to the
// backstop on 1.12.54.
//
// THE CONTRACT. A RESTAction that pages says so in its OWN output, under the
// `slice` key its filter emits (the resolver already preserves an RA-emitted
// slice — widgets.injectSlice never overwrites it):
//
//	slice.continue (bool)   — the answer, as is: an RA that consumes
//	                          .slice.offset/.page, or projects a Kubernetes LIST's
//	                          metadata.continue, states it;
//	slice.cursor  (string)  — otherwise: more iff non-empty (the shape upstream
//	                          portal's *-panels RAs already emit). The cursor is
//	                          handed back to the RA on the next page as
//	                          .slice.cursor.
//	neither                 — the source does not paginate: ONE page, by
//	                          construction rather than by a cap.
package cache

// SourcePagination reads the pagination a data source declared in its resolved
// output (the RA filter's top-level map). declared is false when out carries no
// `slice` map, or one with neither a bool `continue` nor a string `cursor`; more
// and cursor are then zero. A declared bool `continue` wins over a cursor;
// cursor is returned whenever it is a non-empty string (so a source declaring
// both can still be handed its cursor).
func SourcePagination(out map[string]any) (more bool, cursor string, declared bool) {
	if out == nil {
		return false, "", false
	}
	s, ok := out["slice"].(map[string]any)
	if !ok {
		return false, "", false
	}
	if c, ok := s["cursor"].(string); ok {
		cursor = c
		declared = true
		more = c != ""
	}
	if b, ok := s["continue"].(bool); ok {
		more = b
		declared = true
	}
	return more, cursor, declared
}
