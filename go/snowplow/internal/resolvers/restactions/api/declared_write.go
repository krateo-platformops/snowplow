package api

import templates "github.com/krateo-platformops/snowplow/apis/templates/v1"

// declaredWriteStage returns the first write stage the RESTAction declares (in
// spec order), or "" if it declares none. It reads the spec only, so it is
// known before any stage runs and holds for a resolve that stops before running
// one. It is the ONE declared-write classification of a resolve: #903 F1 (never
// stored) and #905 (never served stale) both read it.
func declaredWriteStage(items []*templates.API) string {
	for _, a := range items {
		if a != nil && isWriteVerb(a.Verb) {
			return a.Name
		}
	}
	return ""
}
