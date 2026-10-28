// issue893_declined_put_releases_edges_test.go — #893 (with #890): a /call whose
// resolve records dep edges under its L1 key and then does NOT store the cell —
// a Put gate declines it, or PutIfGen refuses it — leaves no edge for that
// non-resident key. Driven through the real RESTAction handler and its real
// Put-gate chain (the #548 harness); the control branch stores the cell and keeps
// its edge.

package dispatchers

import (
	"context"
	"net/http/httptest"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var gvr893Backing = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "benchapps893"}

func TestIssue893_DeclinedCallLeavesNoEdgesForItsKey(t *testing.T) {
	branches := []struct {
		name   string
		during func(ctx context.Context, key string)
		stored bool
	}{
		{name: "control_stored", stored: true},
		{name: "stage_error", during: func(ctx context.Context, _ string) {
			cache.StageErrorSinkFromContext(ctx).Bump("apistage", "per-item boom")
		}},
		{name: "external_touch", during: func(ctx context.Context, _ string) {
			cache.ExternalTouchedSinkFromContext(ctx).Bump()
		}},
		{name: "sensitive_secret_read", during: func(ctx context.Context, _ string) {
			cache.SensitiveTouchedSinkFromContext(ctx).Bump()
		}},
		{name: "putifgen_refused", during: func(_ context.Context, key string) {
			c := cache.ResolvedCache()
			c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
			c.DeleteForTest(key)
		}},
	}
	for _, b := range branches {
		t.Run(b.name, func(t *testing.T) {
			h1BuildWatcher(t)
			reqCtx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: h1User, Groups: []string{"devs"}}))
			key, handle, _ := dispatchCacheLookupKey(reqCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
			if handle == nil || key == "" {
				t.Fatalf("PRECONDITION: a live cache handle and key (key=%q)", key)
			}
			if _, ok := handle.Get(key); ok {
				t.Fatal("PRECONDITION: the key must be cold")
			}

			resolved := &templatesv1.RESTAction{}
			resolved.SetName(h1RAName)
			resolved.SetNamespace(h1NS)
			restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
				func(ctx context.Context, _ restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
					// The resolve's own read: an inner call records under the L1 key.
					cache.Deps().Record(ctx, key, gvr893Backing, h1NS, "backing-893")
					if b.during != nil {
						b.during(ctx, key)
					}
					return resolved, nil
				})
			defer restore()

			req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
			RESTAction().ServeHTTP(httptest.NewRecorder(), req)

			_, resident := handle.Get(key)
			if resident != b.stored {
				t.Fatalf("harness [%s]: resident=%v, want %v — the branch did not take the path it names", b.name, resident, b.stored)
			}
			backing := cache.DepKey{GVR: gvr893Backing, Namespace: h1NS, Name: "backing-893"}
			hasBacking := false
			for _, dk := range cache.Deps().EdgesUnder(key) {
				if dk == backing {
					hasBacking = true
				}
			}
			switch {
			case b.stored && !hasBacking:
				t.Fatalf("[%s] the stored cell lost the edge its resolve recorded", b.name)
			case !b.stored && len(cache.Deps().EdgesUnder(key)) != 0:
				t.Fatalf("#893 RED [%s]: the /call did not store its cell, yet the tracker keeps %d edge(s) under the non-resident key: %v",
					b.name, len(cache.Deps().EdgesUnder(key)), cache.Deps().EdgesUnder(key))
			}
		})
	}
}
