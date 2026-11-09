// issue1274_loopback_guard_published_test.go — #1274 review: #1274 switches
// the HTTP-edge recursion guard on for self-hops by the short Service name, so
// each stop must be visible on a live pod. Each stop, driven through the real
// ServeHTTP on a short-name self-hop, moves its published cell of
// snowplow_nested_loopback_guard_stops_total (read through /debug/vars, not
// the in-process accessor), and both cells exist at 0 before any stop.
package dispatchers

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"strconv"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func g1274Published(t *testing.T) map[string]uint64 {
	t.Helper()
	v := expvar.Get("snowplow_nested_loopback_guard_stops_total")
	if v == nil {
		t.Fatal("snowplow_nested_loopback_guard_stops_total is not published at /debug/vars")
	}
	var out map[string]uint64
	if err := json.Unmarshal([]byte(v.String()), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func TestIssue1274_LoopbackGuardStopsArePublishedFromZero(t *testing.T) {
	cache.RegisterNestedLoopbackGuardExpvarForTest()
	got := g1274Published(t)
	if len(got) != len(cache.NestedLoopbackGuardStopKinds) {
		t.Fatalf("published cells %v, want exactly %v", got, cache.NestedLoopbackGuardStopKinds)
	}
	for _, k := range cache.NestedLoopbackGuardStopKinds {
		if _, ok := got[k]; !ok {
			t.Fatalf("kind %q is absent, not 0: %v", k, got)
		}
	}
}

func TestIssue1274_EachLoopbackGuardStopMovesItsPublishedCell(t *testing.T) {
	cache.RegisterNestedLoopbackGuardExpvarForTest()
	SetSelfLoopbackHost(r1265ChartSelfURL)
	t.Cleanup(func() { SetSelfLoopbackHost("") })
	const shortSelf = "snowplow.krateo-system.svc:8081"
	for _, tg := range s1265Targets {
		t.Run(tg.name+"/cycle", func(t *testing.T) {
			e := s1265Setup(t, tg)
			self := nestedResolveNodeKey(tg.gvr.Resource, h1NS, tg.crName)
			anc := cache.AncestorsHeaderValue(cache.WithNestedResolveAncestor(context.Background(), self))
			before := g1274Published(t)
			rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) {
				r.Host = shortSelf
				r.Header.Set(cache.NestedDepthHeader, "1")
				r.Header.Set(cache.ResolveAncestorsHeader, anc)
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("a cycle-stop serves the raw CR (200); got %d", rec.Code)
			}
			e.wantResolves(t, "a cycle-stopped hop", 0)
			after := g1274Published(t)
			if after["cycle"]-before["cycle"] != 1 || after["depth"] != before["depth"] {
				t.Fatalf("published stops moved %v → %v, want cycle +1 only", before, after)
			}
		})
		t.Run(tg.name+"/depth", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := g1274Published(t)
			rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) {
				r.Host = shortSelf
				r.Header.Set(cache.NestedDepthHeader, strconv.Itoa(cache.NestedCallMaxDepth()))
			})
			if rec.Code != http.StatusLoopDetected {
				t.Fatalf("the depth backstop answers 508; got %d", rec.Code)
			}
			e.wantResolves(t, "a depth-stopped hop", 0)
			after := g1274Published(t)
			if after["depth"]-before["depth"] != 1 || after["cycle"] != before["cycle"] {
				t.Fatalf("published stops moved %v → %v, want depth +1 only", before, after)
			}
		})
	}
}
