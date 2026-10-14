package jqinterest

import (
	"github.com/krateo-platformops/snowplow/internal/apicall"
)

// The tracked stage's path can carry a query (#683). Since #628 a LIST
// stage's labelSelector is APPLIED in process: the served list holds only the
// objects the selector admits. An UPDATE that changes a selected label then
// moves an object INTO or OUT OF the list. That is an UPDATE at the GVR level,
// not an ADD or DELETE, so the "membership always marks" rule does not cover
// it. The cell's output depends on the label, and the read set must contain
// it.
//
// Which queries are applied in process is decided by internal/apicall
// PlanQuery, the SAME function the resolver serves by (restactions/api
// planListQuery delegates to it), on the path's shape from apicall.ParsePath
// (which cache.ParseAPIServerPathToDep delegates to). For a LITERAL path:
//   - not an apiserver resource path (ParsePath !ok, e.g. an endpoint's own
//     /?query=): not informer-served, so not a list the analysis tracks;
//     nothing to add;
//   - a query PlanQuery REFUSES (limit/continue, resourceVersion, watch, an
//     unknown or repeated param, an unparseable selector, a field other than
//     metadata.name / metadata.namespace, metadata.namespace on a cluster
//     path): the call falls through to the apiserver, which applies semantics
//     the informer does not reproduce (a selectable mutable field, a page
//     limit) and serves states the informer never delivered as transitions
//     (E7). The edge reads everything: All (why=query_unapplied <reason>).
//     This is decided here, statically, so PR-2 does not depend on the
//     runtime §2.4 fall-through rule for a literal path;
//   - an applied labelSelector: a WHOLE read of metadata.labels.<key> for
//     every key a requirement names. A whole read observes presence too, so
//     it covers =, ==, !=, in, notin, exists and !key alike;
//   - an applied fieldSelector (metadata.name / metadata.namespace): those
//     fields are immutable, so an UPDATE cannot move an object across it.
//     Nothing to add.
//
// A TEMPLATED path ("${ … }") is only known at run time. The whole
// metadata.labels map is read: any labelSelector a rendered path applies is
// covered, and every other query a render could produce either is an
// immutable field selector or falls through, which the runtime §2.4 rule
// (query_unapplied) makes All.
func (s *state) selectorReads(tracked *Stage) {
	p := tracked.Path
	if _, isTemplate := MaybeQuery(p); isTemplate {
		if !mutated(mutSelectorPathsDropped) {
			s.readItemLabels("")
		}
		return
	}
	_, ns, name, ok := apicall.ParsePath(p)
	if !ok {
		return
	}
	q := apicall.PlanQuery(p, name == "", ns != "")
	if !q.Applicable {
		if mutated(mutQueryUnappliedNarrowed) {
			return // analysed as if the call carried no query
		}
		top("query_unapplied %s", q.Reason)
	}
	if q.Label == nil || mutated(mutSelectorPathsDropped) {
		return
	}
	reqs, _ := q.Label.Requirements()
	for _, r := range reqs {
		s.readItemLabels(r.Key())
	}
}

// readItemLabels records a whole read of every item's metadata.labels.<key>,
// or of the whole labels map when key is "".
func (s *state) readItemLabels(key string) {
	path := Path{{Key: "items"}, {Any: true}, {Key: "metadata"}, {Key: "labels"}}
	if key != "" {
		path = path.with(Step{Key: key})
	}
	s.reads = append(s.reads, Read{Path: path, Mode: ModeWhole})
}
