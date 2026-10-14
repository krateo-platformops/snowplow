package jqinterest

import (
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
)

// The tracked stage's path can carry a query (#683). Since #628 a LIST
// stage's labelSelector is APPLIED in process (restactions/api list_query.go
// planListQuery): the served list holds only the objects the selector
// admits. An UPDATE that changes a selected label then moves an object INTO
// or OUT OF the list. That is an UPDATE at the GVR level, not an ADD or
// DELETE, so the "membership always marks" rule does not cover it. The
// cell's output depends on the label, and the read set must contain it.
//
// What planListQuery applies, and what this adds:
//   - labelSelector (labels.Parse, the apiserver's own parser): a WHOLE read
//     of metadata.labels.<key> for every key a requirement names. A whole read
//     observes presence too, so it covers =, ==, !=, in, notin, exists and
//     !key alike.
//   - fieldSelector on metadata.name / metadata.namespace: those fields are
//     immutable, so an UPDATE cannot move an object across them. Nothing to
//     add.
//   - every other param makes the whole call fall through to the apiserver
//     (query_unapplied, #603 §2.4's runtime fallback). Reads are added for
//     any labelSelector it carries anyway (over-approximation is sound).
//
// A TEMPLATED path ("${ … }") is only known at run time, so its selector keys
// cannot be read statically. The whole metadata.labels map is read instead:
// any key a run-time selector could name is covered. An unparseable query or
// selector cannot be applied in process (the apiserver answers it), and it is
// still read conservatively, as the whole labels map.
func (s *state) selectorReads(tracked *Stage) {
	if mutated(mutSelectorPathsDropped) {
		return
	}
	p := tracked.Path
	if _, isTemplate := MaybeQuery(p); isTemplate {
		s.readItemLabels("")
		return
	}
	i := strings.IndexByte(p, '?')
	if i < 0 {
		return
	}
	vals, err := url.ParseQuery(p[i+1:])
	if err != nil {
		s.readItemLabels("")
		return
	}
	for _, sel := range vals["labelSelector"] {
		parsed, err := labels.Parse(sel)
		if err != nil {
			s.readItemLabels("")
			continue
		}
		reqs, _ := parsed.Requirements()
		for _, r := range reqs {
			s.readItemLabels(r.Key())
		}
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
