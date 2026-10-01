// issue375_cr_selfdep_widget_test.go — #375 CR self-dep, WIDGET carrier: the same cold-
// key arm as the RESTAction one, driven through the real widgetsHandler.ServeHTTP. The
// widget CR is edited (real dyn UPDATE) while its resolve is in flight. NEUTER: revert to
// WithL1KeyContext at widgets.go → RED.

package dispatchers

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIssue375_CRSelfDep_Widget_ColdKey_EditInWindow_Remarks(t *testing.T) {
	dyn := h1BuildWatcherWithRA(t)
	if _, err := dyn.Resource(h1WidgetGVR).Namespace(h1NS).Create(context.Background(), h1WidgetUnstructured(nil), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create widget: %v", err)
	}
	_, syncCh := cache.Global().EnsureResourceType(h1WidgetGVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("widget informer did not sync")
	}
	var mu sync.Mutex
	byKey := map[string]int{}
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, _ string) { mu.Lock(); byKey[k]++; mu.Unlock() }))
	reqCtx := h1ReqCtx(h1User)
	cr := h1WidgetUnstructured(nil)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1,
		effectiveKeyExtras(reqCtx, cr.Object, nil))
	if handle == nil || key == "" {
		t.Fatalf("PRECONDITION: a live cacheable widget key is required")
	}
	restore := installWidgetFakes(t, cr, func() bool { return true },
		func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
			seq0 := cache.DepEventSeqForTest()
			u, err := dyn.Resource(h1WidgetGVR).Namespace(h1NS).Get(context.Background(), h1WName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get widget: %v", err)
			}
			u.SetLabels(map[string]string{"rev": "new"})
			if _, err := dyn.Resource(h1WidgetGVR).Namespace(h1NS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("update widget: %v", err)
			}
			for end := time.Now().Add(5 * time.Second); cache.DepEventSeqForTest() == seq0; time.Sleep(2 * time.Millisecond) {
				if time.Now().After(end) {
					t.Fatalf("the widget UPDATE never reached OnObjectEvent")
				}
			}
			out := h1WidgetUnstructured(nil)
			out.Object["status"] = map[string]any{"widgetData": map[string]any{"v": "old"}}
			return out, nil
		})
	defer restore()
	rec := httptest.NewRecorder()
	Widgets().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))
	if rec.Code != 200 {
		t.Fatalf("dispatch: want 200, got %d %s", rec.Code, rec.Body.String())
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("setup: the widget dispatch did not Put its cell")
	}
	mu.Lock()
	defer mu.Unlock()
	if byKey[key] != 1 {
		t.Fatalf("#375 CR self-dep RED (widget): the widget CR was edited during a COLD first-fill resolve but its "+
			"Put remarked %d times (want 1)", byKey[key])
	}
}
