// narrow_765_test.go — #603 PR-2 / #765 option B SHADOW, record-side arms.
//
// The REAL api.Resolve stage loop over a real synced informer (the #375
// harness) decides each list edge's interest at the end of the resolve:
//
//	R1  a clean, informer-served resolve narrows the edge to its read set;
//	R2  a JQ the analysis cannot prove (now) leaves it all;
//	R3  a chain that reads the whole item leaves it all;
//	R4  spec.filter is part of the chain: it can narrow a stage that is
//	    otherwise whole;
//	R5  a call that was not informer-served (an apiserver fall-through) or a
//	    stage error widens the edge for good, and a later clean resolve
//	    cannot narrow it back.
package api

import (
	"context"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func dk765() cache.DepKey { return cache.DepKey{GVR: f1WidgetsGVR, Namespace: "", Name: "*"} }

func resolve765(t *testing.T, key string, specFilter *string, stages ...*templates.API) {
	t.Helper()
	ctx := xcontext.BuildContext(cache.WithL1KeyContext(context.Background(), key),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: u375}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	dict := Resolve(ctx, ResolveOptions{
		RC: &rest.Config{}, Items: stages, SpecFilter: specFilter,
		RESTActionNamespace: "default", RESTActionName: "c765",
	})
	if _, ok := dict["w"]; !ok {
		t.Fatalf("setup: stage w produced nothing: %v", dict)
	}
}

func stage765(filter string) *templates.API {
	st := stage375("w", f1WidgetsGVR)
	st.Filter = ptr.To(filter)
	return st
}

func TestNarrow765_R1_CleanInformerServedResolveNarrows(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r1"
	resolve765(t, key, nil, stage765(`[.w.items[] | {name: .metadata.name}]`))
	got := cache.ListInterestStateForTest(key, dk765())
	if !strings.HasPrefix(got, "narrowed:") || strings.Contains(got, "spec") {
		t.Fatalf("R1 RED: a clean informer-served resolve whose chain reads only metadata.name must narrow "+
			"its list edge, got %q", got)
	}
}

func TestNarrow765_R2_UnprovableJQStaysAll(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r2"
	resolve765(t, key, nil, stage765(`[.w.items[] | {name: .metadata.name, t: now}]`))
	if got := cache.ListInterestStateForTest(key, dk765()); got != "all" {
		t.Fatalf("R2 RED: an unprovable JQ (now) must leave the edge all, got %q", got)
	}
}

func TestNarrow765_R3_WholeItemStaysAll(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r3"
	resolve765(t, key, nil, stage375("w", f1WidgetsGVR)) // filter ".w.items": the whole items
	if got := cache.ListInterestStateForTest(key, dk765()); got != "all" {
		t.Fatalf("R3 RED: a chain that reads the whole item must leave the edge all, got %q", got)
	}
}

func TestNarrow765_R4_SpecFilterIsPartOfTheChain(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r4"
	resolve765(t, key, ptr.To(`[.w[] | .metadata.name]`), stage375("w", f1WidgetsGVR))
	got := cache.ListInterestStateForTest(key, dk765())
	if !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("R4 RED: spec.filter reads only metadata.name of a whole-items stage; the chain must narrow, got %q", got)
	}
}

// R5 drives commitNarrow directly: what decides is how each call was served
// and whether the resolve was clean, not the dispatch machinery.
func TestNarrow765_R5_NotInformerServedOrStageErrorWidensForGood(t *testing.T) {
	newWatcher375(t)
	st := stage765(`[.w.items[] | {name: .metadata.name}]`)
	run := func(key string, served, dirty bool) string {
		t.Helper()
		r := &resolveRun{ctx: cache.WithL1KeyContext(context.Background(), key), opts: ResolveOptions{Items: []*templates.API{st}}}
		r.narrow = newNarrowRun(r)
		if r.narrow == nil {
			t.Fatalf("setup: no narrow run (cache off?)")
		}
		r.recordListEdge("w", key, f1WidgetsGVR, "")
		if served {
			r.noteNarrowServed("w", st.Path)
		}
		if dirty {
			r.taintNarrow()
		}
		r.commitNarrow()
		return cache.ListInterestStateForTest(key, dk765())
	}
	if got := run("n765-r5-ok", true, false); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("R5 setup: a served clean resolve must narrow, got %q", got)
	}
	if got := run("n765-r5-fallthrough", false, false); got != "all" {
		t.Fatalf("R5 RED: a call that was not informer-served must leave the edge all, got %q", got)
	}
	if got := run("n765-r5-error", true, true); got != "all" {
		t.Fatalf("R5 RED: a resolve with a stage error must leave the edge all, got %q", got)
	}
	// Sticky: a later clean resolve of the same key never narrows it back.
	if got := run("n765-r5-fallthrough", true, false); got != "all" {
		t.Fatalf("R5 RED: a widened edge was narrowed again by a later resolve, got %q", got)
	}
	// A plain record (a replay) of a narrowed edge widens it.
	cache.Deps().ReplayEdges(context.Background(), "n765-r5-ok", []cache.DepKey{dk765()})
	if got := cache.ListInterestStateForTest("n765-r5-ok", dk765()); got != "all" {
		t.Fatalf("R5 RED: a replayed (plain) record must widen a narrowed edge, got %q", got)
	}
}
