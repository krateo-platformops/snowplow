package apiref

// issue912_declared_write_ra_full_list_test.go — #912 re-review F-B and #942: a
// paginated widget whose apiRef RESTAction sends a write, end to end through the
// REAL apiref.Resolve → restactions.Resolve.
//
//   P1  the widget's RESTAction declares the write itself. Two page /calls send
//       the POST exactly twice (once each) and leave no raFullList cell.
//   P2  (#942) the write is NESTED: the widget's RESTAction declares no write of
//       its own and resolves in-process (resolve:true) a RESTAction that does.
//       Two page /calls send the POST exactly twice and leave no raKey cell. A
//       check of the widget RESTAction's own spec cannot see this write; the
//       runtime declared-write mark does.
//   P3  (#912 delta-3 item 4) a write RESTAction whose (raKey × shape) is
//       ALREADY known sliceable: the repopulate branch runs the unpaginated
//       resolve, the full list PASSES the slice check, and the raFullList write
//       is reached with a declared_write verdict. One POST per /call, the page
//       is served from that one run, and the cell is never stored.
//
//   P4  the write is declared and never dialled (its iterator yields no item),
//       so declared_write is the verdict's ONLY reason to decline: no cell, on
//       first sight and on the known-sliceable repopulate branch.
//
// A dialled write also bumps the external-touch sink (every HTTP dial does), so
// P1–P3 hold on either permanent reason; P4 is the arm of the declared-write
// input alone.
//
// Mutations (logic-only, RUN):
//   - first sight back to "decline only after both runs" → P1 and P2 RED (4
//     POSTs for 2 /calls);
//   - drop the declared-write row of cache.PutGateVerdict → P4 RED (the
//     repopulate branch stores the cell); P1–P3 stay GREEN on external_touched;
//   - make raFullListPermanentDecline and raFullListDeclines ignore
//     declared_write → P1 and P2 RED (3 POSTs); P3 stays GREEN on the cell
//     because PutRAFullListIfGen's own Store() guard refuses it; additionally
//     drop that guard → P3 RED (the cell is stored, and page 2 is then served
//     from it without running the write: 1 POST).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// i912Harness is the shared set-up of P1–P3: a POST-counting upstream and the
// restactions informer seeded with the given RESTActions.
type i912Harness struct {
	t     *testing.T
	posts atomic.Int64
	up    *httptest.Server
}

func newI912Harness(t *testing.T, ras ...*unstructured.Unstructured) *i912Harness {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	h := &i912Harness{t: t}
	h.up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			h.posts.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(h.up.Close)
	objs := f6BuildFixture()
	for _, ra := range ras {
		objs = append(objs, ra)
	}
	edge3NewWatcher(t, objs...)
	added, syncCh := cache.Global().EnsureResourceType(gvr())
	if added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatal("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gvr(), 3*time.Second) {
		t.Fatal("restactions GVR never became servable")
	}
	return h
}

// call is one customer page /call of a widget over the RESTAction raName: its
// own L1 key, its own sinks, the real apiref.Resolve.
func (h *i912Harness) call(raName string, perPage, page int) (map[string]any, error) {
	userCtx := f6CtxWithUser(h.t, "admin", []string{"system:masters"})
	ctx := cache.WithL1KeyContext(userCtx, "L1_i912_widget_page")
	ctx, _ = cache.WithExternalTouchedSink(ctx)
	ctx, _ = cache.WithStageErrorSink(ctx)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: h.up.URL})
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: raName, Namespace: edge3BackingNS}, Resource: "restactions", APIVersion: "templates.krateo.io/v1",
	}
	return Resolve(ctx, ResolveOptions{ApiRef: ref, PerPage: perPage, Page: page, RC: &rest.Config{Host: h.up.URL}})
}

func (h *i912Harness) raKey(raName string) string {
	h.t.Helper()
	_, raKey, ok := seedFullListRAKey(f6CtxWithUser(h.t, "admin", []string{"system:masters"}), gvr(), edge3BackingNS, raName, nil)
	if !ok || raKey == "" {
		h.t.Fatalf("PRE: the raFullList key must derive (ok=%v key=%q)", ok, raKey)
	}
	return raKey
}

const i912Filter = `{ items: (.items.items // []) }`

// i912ListingRA is a RESTAction whose output is one list (the restactions in
// the namespace, informer-served) the widget pages over, plus extra stages.
func i912ListingRA(name string, extra ...any) *unstructured.Unstructured {
	api := append([]any{map[string]any{"name": "items", "verb": "GET",
		"path": "/apis/templates.krateo.io/v1/namespaces/" + edge3BackingNS + "/restactions"}}, extra...)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": edge3BackingNS},
		"spec":       map[string]any{"api": api, "filter": i912Filter},
	}}
}

// i912PostStage is a write stage, sent to the harness upstream.
func i912PostStage() map[string]any {
	return map[string]any{"name": "create", "verb": "POST", "path": "/create",
		"payload": `{"n":1}`, "continueOnError": true, "errorKey": "createErr",
		"dependsOn": map[string]any{"name": "items"}}
}

// registerI912NestedResolver wires the in-process nested resolve the way
// production does (main.go wires dispatchers.ResolveNestedCall, whose
// RESTAction branch fetches the CR under the caller's identity and runs
// restactions.Resolve on it under a depth+1 ctx).
func registerI912NestedResolver(t *testing.T, host string) {
	t.Helper()
	restactionsapi.RegisterNestedCallResolver(func(ctx context.Context, ref templatesv1.ObjectReference,
		perPage, page int, extras map[string]any) ([]byte, error) {
		got := objects.Get(ctx, ref)
		if got.Err != nil || got.Unstructured == nil {
			return nil, fmt.Errorf("nested fetch %s/%s failed", ref.Resource, ref.Name)
		}
		var cr templatesv1.RESTAction
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(got.Unstructured.Object, &cr); err != nil {
			return nil, err
		}
		inner := cache.WithNestedCallDepth(ctx, cache.NestedCallDepthFromContext(ctx)+1)
		res, err := restactions.Resolve(inner, restactions.ResolveOptions{
			In: &cr, SArc: &rest.Config{Host: host}, PerPage: perPage, Page: page, Extras: extras,
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	})
	t.Cleanup(func() { restactionsapi.RegisterNestedCallResolver(nil) })
}

func TestIssue912_P1_PaginatedWidgetOverAWriteRA_OnePOSTPerCall_NoRAFullListCell(t *testing.T) {
	const raName = "i912-write-ra"
	h := newI912Harness(t, i912ListingRA(raName, i912PostStage()))
	raKey := h.raKey(raName)
	for page := 1; page <= 2; page++ {
		if _, err := h.call(raName, 5, page); err != nil {
			t.Fatalf("apiref.Resolve(page %d): %v", page, err)
		}
	}
	if n := h.posts.Load(); n != 2 {
		t.Errorf("#912 F-B RED: two page /calls of a widget over a write RESTAction sent %d POST(s), want exactly 2", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(raKey); stored {
		t.Error("#912 F-B RED: the raFullList cell of a RESTAction that declares a write was stored")
	}
}

func TestIssue942_P2_PaginatedWidgetOverANestedWrite_OnePOSTPerCall_NoRAFullListCell(t *testing.T) {
	const outer, inner = "i942-outer-ra", "i942-inner-write-ra"
	// The inner RESTAction declares the write; the outer one declares none and
	// resolves the inner one in-process.
	innerRA := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": inner, "namespace": edge3BackingNS},
		"spec": map[string]any{
			"api": []any{map[string]any{"name": "create", "verb": "POST", "path": "/create",
				"payload": `{"n":1}`, "continueOnError": true, "errorKey": "createErr"}},
		},
	}}
	outerRA := i912ListingRA(outer, map[string]any{"name": "inner", "verb": "GET",
		"path":    "/apis/templates.krateo.io/v1/namespaces/" + edge3BackingNS + "/restactions/" + inner,
		"resolve": true, "continueOnError": true, "errorKey": "innerErr",
		"dependsOn": map[string]any{"name": "items"}})
	h := newI912Harness(t, outerRA, innerRA)
	registerI912NestedResolver(t, h.up.URL)
	raKey := h.raKey(outer)

	for page := 1; page <= 2; page++ {
		if _, err := h.call(outer, 1, page); err != nil {
			t.Fatalf("apiref.Resolve(page %d): %v", page, err)
		}
	}
	if n := h.posts.Load(); n != 2 {
		t.Errorf("#942 RED: two page /calls of a widget whose RESTAction resolves a write RESTAction in-process sent %d POST(s), want exactly 2", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(raKey); stored {
		t.Error("#942 RED: the raFullList cell of a RESTAction that resolves a write was stored")
	}
}

func TestIssue912_P3_KnownSliceableWriteRA_RepopulatePassesSliceCheck_OnePOSTPerCall_NoCell(t *testing.T) {
	const raName = "i912-p3-write-ra"
	h := newI912Harness(t, i912ListingRA(raName, i912PostStage()), i912ListingRA("i912-p3-other-ra"))
	raKey := h.raKey(raName)
	// The (raKey × shape) verdict is already sliceable: what a first sight that
	// saw no write records (a write reached only through a stage whose input was
	// empty then). Primed through the serve path's own shape derivation.
	shape := seedFullListShape(gvr(), edge3BackingNS, raName,
		&templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{Filter: ptr.To(i912Filter)}})
	cache.RecordSliceabilityClassified(raKey, shape, true, false, cache.SliceabilityLabels{})
	if sliceable, known := cache.SliceabilityLookupCtx(context.Background(), raKey, shape); !known || !sliceable {
		t.Fatalf("PRE: the primed verdict must read known+sliceable (known=%v sliceable=%v)", known, sliceable)
	}
	refusedBefore := RAFullListWriteNotRerunForTest()

	for page := 1; page <= 2; page++ {
		out, err := h.call(raName, 1, page)
		if err != nil {
			t.Fatalf("apiref.Resolve(page %d): %v", page, err)
		}
		// The page comes from the one unpaginated run: the slice check passed.
		if items, _ := out["items"].([]any); len(items) != 1 {
			t.Fatalf("PRE: page %d must be a one-item Go-slice of the full list, got %#v", page, out)
		}
	}
	if n := h.posts.Load(); n != 2 {
		t.Errorf("#912 P3 RED: two page /calls on the repopulate branch sent %d POST(s), want exactly 2", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(raKey); stored {
		t.Error("#912 P3 RED: the raFullList write stored the full list of a RESTAction that declares a write")
	}
	if n := RAFullListWriteNotRerunForTest() - refusedBefore; n != 0 {
		t.Errorf("PRE: the slice check passed, so no run may have been refused a fall-back (got %d)", n)
	}
}

// P4 isolates the declared-write input of the verdict. A dialled write also
// reads as an external touch (every HTTP dial bumps that sink), so P1–P3 hold
// on either reason. Here the write is declared and never dialled (its iterator
// yields no item): declared_write is the ONLY reason not to store, and the
// sliceable full list of this RESTAction must still never be stored (#903 F1),
// on first sight and on the known-sliceable repopulate branch.
func TestIssue912_P4_UndialledDeclaredWrite_IsTheOnlyReason_NoRAFullListCell(t *testing.T) {
	const raName = "i912-p4-undialled-write-ra"
	undialled := i912PostStage()
	undialled["dependsOn"] = map[string]any{"name": "items", "iterator": ".items.items | map(select(false))"}
	h := newI912Harness(t, i912ListingRA(raName, undialled), i912ListingRA("i912-p4-other-ra"))
	raKey := h.raKey(raName)
	shape := seedFullListShape(gvr(), edge3BackingNS, raName,
		&templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{Filter: ptr.To(i912Filter)}})

	for page := 1; page <= 2; page++ {
		if _, err := h.call(raName, 1, page); err != nil {
			t.Fatalf("first sight, apiref.Resolve(page %d): %v", page, err)
		}
	}
	if n := h.posts.Load(); n != 0 {
		t.Fatalf("PRE: the write's iterator yields no item, so nothing may be dialled (got %d POSTs)", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(raKey); stored {
		t.Error("#912 P4 RED (first sight): the raFullList cell of a RESTAction that declares a write was stored")
	}

	// The repopulate branch: the verdict already known sliceable.
	cache.RecordSliceabilityClassified(raKey, shape, true, false, cache.SliceabilityLabels{})
	for page := 1; page <= 2; page++ {
		out, err := h.call(raName, 1, page)
		if err != nil {
			t.Fatalf("repopulate, apiref.Resolve(page %d): %v", page, err)
		}
		if items, _ := out["items"].([]any); len(items) != 1 {
			t.Fatalf("PRE: page %d must be a one-item Go-slice of the full list, got %#v", page, out)
		}
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(raKey); stored {
		t.Error("#912 P4 RED (repopulate): the raFullList write stored the full list of a RESTAction that declares a write")
	}
}
