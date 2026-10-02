// cache_off_call_429_test.go — #429: with the cache OFF, dispatchCacheLookupKey
// returns a nil cacheHandle interface, and the #189 generation capture
// (cacheHandle.CaptureGen) in restactions.go / widgets.go dereferenced it before
// the resolve, panicking every cache-off /call. The cache-off path must stay a
// transparent fallback: 200 with a freshly resolved body.
//
// RED on c20cc763: both arms panic (nil interface method call).

package dispatchers

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const c429Marker = "C429-FRESH-BODY"

func c429CacheOff(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "false")
	if !cache.Disabled() {
		t.Fatalf("PRE: cache must be disabled")
	}
	if _, h, _ := dispatchCacheLookupKey(h1ReqCtx(h1User), "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil); h != nil {
		t.Fatalf("PRE: cache-off must yield a nil cache handle (the #429 shape)")
	}
}

func c429Serve(t *testing.T, serve func(*httptest.ResponseRecorder)) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("#429: cache-off /call panicked: %v", r)
			}
		}()
		serve(rec)
	}()
	return rec
}

func TestCacheOff429_RESTActionCall_ServesFreshBody(t *testing.T) {
	c429CacheOff(t)
	resolves := 0
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			resolves++
			ra := &templatesv1.RESTAction{}
			ra.SetName(h1RAName)
			ra.SetNamespace(h1NS)
			ra.SetAnnotations(map[string]string{"marker": c429Marker})
			return ra, nil
		})
	defer restore()
	rec := c429Serve(t, func(rec *httptest.ResponseRecorder) {
		RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(h1ReqCtx(h1User)))
	})
	if rec.Code != 200 || resolves != 1 || !bytes.Contains(rec.Body.Bytes(), []byte(c429Marker)) {
		t.Fatalf("cache-off RESTAction /call must resolve once and serve 200 with the fresh body; code=%d resolves=%d body=%s",
			rec.Code, resolves, rec.Body.String())
	}
}

func TestCacheOff429_WidgetCall_ServesFreshBody(t *testing.T) {
	c429CacheOff(t)
	resolves := 0
	restore := installWidgetFakes(t, h1WidgetUnstructured(nil), func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			resolves++
			out := h1WidgetUnstructured(nil)
			_ = unstructured.SetNestedField(out.Object, c429Marker, "status", "widgetData", "marker")
			return out, nil
		})
	defer restore()
	rec := c429Serve(t, func(rec *httptest.ResponseRecorder) {
		Widgets().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(h1ReqCtx(h1User)))
	})
	if rec.Code != 200 || resolves != 1 || !bytes.Contains(rec.Body.Bytes(), []byte(c429Marker)) {
		t.Fatalf("cache-off widget /call must resolve once and serve 200 with the fresh body; code=%d resolves=%d body=%s",
			rec.Code, resolves, rec.Body.String())
	}
}
