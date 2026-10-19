// narrow.go — #603 PR-2 / #765 option B SHADOW, the record side.
//
// A RESTAction resolve records a LIST edge per inner GET (edge-3, resolve.go).
// This file decides, once the resolve is done, what that edge's interest is:
// the item fields the resolved cell can observe (cache/deps_list_interest.go).
//
// The edge is narrowed (CommitListInterest; a union with any read set it
// already had) only when ALL of these hold:
//
//	(a) no stage of this resolve recorded an error, and it was not truncated:
//	    an error can put raw or partial data into the output, and a #681
//	    denial-only Put can store it;
//	(b) every call of that stage on that edge was served from the informer
//	    (the "informer" dispatch) or from the informer-filled content cell
//	    ("apistage-content"). An apiserver fall-through may hold a state the
//	    informer never delivered as a transition (#603 E7, §2.4);
//	(c) jqinterest.Analyze on the RESTAction chain is not All, and does not
//	    read the whole item.
//
// Anything else WIDENS the edge to all, for good. Unknown is never narrowed.
//
// THE CHAIN IS THE RESTACTION'S ONLY (Widget == nil), even when the cell is a
// widget whose resolve runs this RESTAction in-process. That is sound: a
// widget's output is a function of its RESTAction's output and of cell
// constants (extras and slice are in its key), so a list change that leaves
// the RESTAction's output unchanged leaves the widget's unchanged. It is less
// precise than analysing the widget templates too, which is a follow-up.
//
// spec.filter comes from ResolveOptions.SpecFilter; a caller that does not
// pass it gets the analysis of the whole dict, which is a sound
// over-approximation (the real output is a function of the dict).
//
// Shadow only: nothing here changes what is recorded in the forward index,
// what an event marks, or what is served.
package api

import (
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/cache/jqinterest"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type narrowEdgeKey struct {
	stage string
	dk    cache.DepKey
}

type narrowEdgeStat struct {
	recorded int
	served   int
}

// narrowRun is the per-resolve record of the list edges this resolve recorded
// under its L1 key, and how their calls were served.
type narrowRun struct {
	l1Key string
	mu    sync.Mutex
	edges map[narrowEdgeKey]*narrowEdgeStat
	dirty atomic.Bool
}

// newNarrowRun returns nil (plain recording) when there is nothing to narrow:
// cache off, shadow off, an inert resolve, or no L1 key on the ctx.
func newNarrowRun(r *resolveRun) *narrowRun {
	if cache.Disabled() || !cache.ListNarrowShadowOn() || cache.Inert(r.ctx) {
		return nil
	}
	l1Key := cache.L1KeyFromContext(r.ctx)
	if l1Key == "" {
		return nil
	}
	return &narrowRun{l1Key: l1Key, edges: map[narrowEdgeKey]*narrowEdgeStat{}}
}

// recordListEdge is the edge-3 list record: narrowable when this resolve owns
// the key, plain otherwise (identical forward edge either way).
func (r *resolveRun) recordListEdge(stage, l1Key string, gvr schema.GroupVersionResource, ns string) {
	n := r.narrow
	if n == nil || l1Key != n.l1Key {
		cache.Deps().RecordList(r.ctx, l1Key, gvr, ns)
		return
	}
	cache.Deps().RecordListNarrowable(r.ctx, l1Key, gvr, ns)
	k := narrowEdgeKey{stage: stage, dk: cache.DepKey{GVR: gvr, Namespace: ns, Name: "*"}}
	n.mu.Lock()
	st := n.edges[k]
	if st == nil {
		st = &narrowEdgeStat{}
		n.edges[k] = st
	}
	st.recorded++
	n.mu.Unlock()
}

// noteNarrowServed records that one call of stage was served from the
// informer (directly or through the informer-filled content cell) and fed
// without error.
func (r *resolveRun) noteNarrowServed(stage, path string) {
	n := r.narrow
	if n == nil {
		return
	}
	gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path)
	if !ok || name != "" {
		return
	}
	k := narrowEdgeKey{stage: stage, dk: cache.DepKey{GVR: gvr, Namespace: ns, Name: "*"}}
	n.mu.Lock()
	if st := n.edges[k]; st != nil {
		st.served++
	}
	n.mu.Unlock()
}

// taintNarrow marks the resolve as not clean: every edge it recorded widens.
func (r *resolveRun) taintNarrow() {
	if r.narrow != nil {
		r.narrow.dirty.Store(true)
	}
}

// commitNarrow settles every edge this resolve recorded (see the file doc).
func (r *resolveRun) commitNarrow() {
	n := r.narrow
	// #443 — an inert (dry-run) resolve records no edge (newNarrowRun already
	// returns nil for it); checked here too, where the side effects are.
	if n == nil || cache.Inert(r.ctx) {
		return
	}
	n.mu.Lock()
	edges := make(map[narrowEdgeKey]narrowEdgeStat, len(n.edges))
	for k, v := range n.edges {
		edges[k] = *v
	}
	n.mu.Unlock()
	if len(edges) == 0 {
		return
	}
	deps := cache.Deps()
	dirty := n.dirty.Load()
	var ra *jqinterest.RESTAction
	for k, st := range edges {
		if dirty {
			deps.WidenListInterest(n.l1Key, k.dk, cache.CommitWhyStageError)
			continue
		}
		if st.recorded == 0 || st.served != st.recorded {
			deps.WidenListInterest(n.l1Key, k.dk, cache.CommitWhyNotInformer)
			continue
		}
		if ra == nil {
			ra = narrowChainRA(r.opts.Items, r.opts.SpecFilter)
		}
		in := analyzeNarrow(ra, k.stage)
		switch {
		case in.All:
			deps.WidenListInterest(n.l1Key, k.dk, cache.CommitWhyOfAnalysis(in.Why))
		case in.ReadsEverything():
			deps.WidenListInterest(n.l1Key, k.dk, cache.CommitWhyReadsWholeItem)
		default:
			deps.CommitListInterest(n.l1Key, k.dk, in.Reads)
		}
	}
}

// narrowChainRA maps the RESTAction spec onto the analyzer's chain model.
func narrowChainRA(items []*templates.API, specFilter *string) *jqinterest.RESTAction {
	ra := &jqinterest.RESTAction{Filter: specFilter}
	for _, a := range items {
		if a == nil {
			continue
		}
		st := jqinterest.Stage{
			Name:    a.Name,
			Path:    a.Path,
			Payload: ptr.Deref(a.Payload, ""),
			Headers: append([]string(nil), a.Headers...),
			Filter:  a.Filter,
		}
		if a.DependsOn != nil {
			st.DependsOn = a.DependsOn.Name
			st.Iterator = ptr.Deref(a.DependsOn.Iterator, "")
		}
		if u := a.UserAccessFilter; u != nil {
			st.UAF = &jqinterest.UAF{
				NamespaceFrom:    u.NamespaceFrom,
				NameFrom:         u.NameFrom,
				NameSpecificVerb: rbac.IsNameSpecificVerb(u.Verb),
				ResourcesFrom:    u.ResourcesFrom,
			}
		}
		ra.Stages = append(ra.Stages, st)
	}
	return ra
}

// The analysis is pure and deterministic in (chain, tracked stage): memoise it
// by the JSON of both. Bounded: past narrowMemoMax entries a miss is computed
// and not stored.
const narrowMemoMax = 4096

var (
	narrowMemo  sync.Map // string -> jqinterest.Interest
	narrowMemoN atomic.Int64
)

func analyzeNarrow(ra *jqinterest.RESTAction, tracked string) jqinterest.Interest {
	b, err := json.Marshal(struct {
		RA      *jqinterest.RESTAction
		Tracked string
	}{ra, tracked})
	if err != nil {
		return jqinterest.Interest{All: true, Why: "parse: unmarshalable chain"}
	}
	key := string(b)
	if v, ok := narrowMemo.Load(key); ok {
		return v.(jqinterest.Interest)
	}
	in := jqinterest.Analyze(jqinterest.Chain{RA: *ra, Tracked: tracked}, jqsupport.ModuleLoader())
	if narrowMemoN.Load() < narrowMemoMax {
		if _, loaded := narrowMemo.LoadOrStore(key, in); !loaded {
			narrowMemoN.Add(1)
		}
	}
	return in
}
