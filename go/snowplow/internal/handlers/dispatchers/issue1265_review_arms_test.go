// issue1265_review_arms_test.go — #1274 review: the served-cold counter cannot
// be dodged by a caller and classifies refusals as refused. Same harness and
// oracle as issue1265_customer_serves_test.go (the live-resolve seam count).
//
//   - A loopback marker (nested depth, background hop, work class) excludes a
//     request ONLY when it arrives on the self host: from any other host the
//     call is a customer's own and is counted cold with its header. On the
//     self host the same marker still excludes, so the arm is not satisfied
//     by dropping the exclusion.
//   - A #1116 reserved-extras 400 and the widgets inline-object 400 are
//     refused, with no header: nothing was served or read.
//   - A LookupMiss nobody set is not a hit.
package dispatchers

import (
	"net/http"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

const (
	r1265SelfURL  = "http://snowplow.krateo-system.svc:8081"
	r1265SelfHost = "snowplow.krateo-system.svc:8081"
)

var r1265Markers = map[string]func(*http.Request){
	"nested_depth":   func(r *http.Request) { r.Header.Set(cache.NestedDepthHeader, "1") },
	"background_hop": func(r *http.Request) { r.Header.Set(cache.BackgroundHeader, "1") },
	"work_class":     func(r *http.Request) { r.Header.Set(workclass.Header, workclass.Refresh.String()) },
}

func TestIssue1265_LoopbackMarkersExcludeOnlyFromTheSelfHost(t *testing.T) {
	SetSelfLoopbackHost(r1265SelfURL)
	t.Cleanup(func() { SetSelfLoopbackHost("") })
	for _, tg := range s1265Targets {
		h := tg.name
		for name, set := range r1265Markers {
			t.Run(h+"/"+name+"/from_another_host_is_counted", func(t *testing.T) {
				e := s1265Setup(t, tg)
				before := s1265Snap(t)
				rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) {
					r.Host = "portal.example.com"
					set(r)
				})
				e.wantResolves(t, "an untrusted marker", 1)
				s1265Header(t, "an untrusted "+name, rec, "cold")
				s1265AssertDelta(t, "an untrusted "+name, before,
					map[string]uint64{h + "|cold": 1, "cold|" + h + "|" + coldCauseNoCell: 1})
			})
			t.Run(h+"/"+name+"/from_the_self_host_is_excluded", func(t *testing.T) {
				e := s1265Setup(t, tg)
				before := s1265Snap(t)
				rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) {
					r.Host = r1265SelfHost
					set(r)
				})
				e.wantResolves(t, "a trusted marker", 1)
				s1265Header(t, "a trusted "+name, rec, "")
				s1265AssertDelta(t, "a trusted "+name, before, map[string]uint64{})
			})
		}
	}
}

func TestIssue1265_RefusalsAreRefusedNotCold(t *testing.T) {
	for _, tg := range s1265Targets {
		h := tg.name
		// The resolver refuses extras naming a key it writes itself, before it
		// reads anything (#1116).
		t.Run(h+"/reserved_extras_400", func(t *testing.T) {
			e := s1265Setup(t, tg)
			e.resolveErr = &api.ReservedExtrasError{Keys: []api.ReservedKey{{Key: "slice", Owner: api.ReservedBySlice}}}
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("setup: a reserved-extras refusal must 400; got %d", rec.Code)
			}
			s1265Header(t, "a reserved-extras 400", rec, "")
			s1265AssertDelta(t, "a reserved-extras 400", before, map[string]uint64{h + "|refused": 1})
		})
	}

	// The widgets handler's backstop: an inline object is a RESTAction only.
	t.Run("widgets/inline_object_400", func(t *testing.T) {
		e := s1265Setup(t, s1265Targets[1])
		before := s1265Snap(t)
		ctx := util.WithInlineObject(h1ReqCtx(h1User), &unstructured.Unstructured{Object: map[string]any{}})
		rec := e.serve(ctx, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("setup: an inline object at the widgets handler must 400; got %d", rec.Code)
		}
		e.wantResolves(t, "an inline-object 400", 0)
		s1265Header(t, "an inline-object 400", rec, "")
		s1265AssertDelta(t, "an inline-object 400", before, map[string]uint64{"widgets|refused": 1})
	})
}

func TestIssue1265_AnUnsetLookupMissIsNotAHit(t *testing.T) {
	var unset cache.LookupMiss
	if unset == cache.LookupHit {
		t.Fatal("the zero LookupMiss reads as a hit: a default or a stub would serve as a hit")
	}
	if unset != cache.LookupAbsent {
		t.Fatalf("the zero LookupMiss = %d, want LookupAbsent", unset)
	}
}
