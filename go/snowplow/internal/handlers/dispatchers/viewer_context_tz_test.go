// viewer_context_tz_test.go - the viewer's time zone (tz) as a viewer-context
// extra, through the REAL Widgets().ServeHTTP.
//
//	undeclared  -> stripped before resolution; the Put happens; the key is the
//	               no-tz key; the revisit hits.
//	declared    -> reaches the resolve; partitions the key; a forged value
//	               reaches only its own cell.
//	invalid     -> (not an IANA zone name) dropped before the key and the
//	               resolve, as if it had not been sent.
//	unchanged   -> an undeclared NON-viewer-context key still quarantines (F6-6).
//
// The apiRef widget's resolver is a fake that renders what widgets.ApiRefExtras
// says the apiRef RESTAction would receive (the extras widgets.Resolve hands to
// apiref.Resolve): a real apiRef resolve needs an apiserver, and the hop being
// tested is what extras the RESTAction gets, which that accessor derives with
// the same function Resolve uses.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/viewercontext"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type tzResult struct {
	served, persisted []byte
	resolves          int
	declineDelta      uint64
	droppedDelta      uint64
}

// tzApiRefSpec is an apiRef widget (an extras-reading surface, so an undeclared
// extra WOULD decline the Put) declaring the given keyExtras.
func tzApiRefSpec(keyExtras ...string) map[string]any {
	spec := map[string]any{
		"widgetData": map[string]any{},
		"apiRef":     map[string]any{"name": "ra-tz", "namespace": h1NS},
	}
	if len(keyExtras) > 0 {
		decl := make([]any, len(keyExtras))
		for i, k := range keyExtras {
			decl[i] = k
		}
		spec["keyExtras"] = decl
	}
	return spec
}

// tzServe drives one /call. lookup is the extras the test expects the cell to be
// keyed by (the "no-tz" request, or the same tz): the persisted bytes are read
// back from THAT cell, so a key that moved shows up as a missing Put.
func tzServe(t *testing.T, spec map[string]any, extras, lookup map[string]any, fresh bool) tzResult {
	t.Helper()
	if fresh {
		cache.ResetResolvedCacheForTest()
	}
	var res tzResult
	cr := h1WidgetUnstructured(spec)
	_, hasApiRef := spec["apiRef"]
	restore := installWidgetFakes(t, cr, func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			res.resolves++
			if hasApiRef {
				// What the apiRef RESTAction would be handed (see file header).
				raExtras := widgets.ApiRefExtras(ctx, cr.Object, opts.Extras)
				out := h1WidgetUnstructured(spec)
				_ = unstructured.SetNestedField(out.Object, map[string]any{"raExtras": raExtras}, "status", "widgetData")
				return out, nil
			}
			out, _ := widgets.Resolve(ctx, opts)
			if out == nil {
				out = &unstructured.Unstructured{Object: map[string]any{}}
			}
			return out, nil
		})
	defer restore()

	reqCtx := h1ReqCtx(h1User)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1,
		effectiveKeyExtras(reqCtx, cr.Object, lookup))
	if handle == nil || key == "" {
		t.Fatalf("PRECONDITION: expected a live cache handle and key (key=%q)", key)
	}

	target := "/call"
	if len(extras) > 0 {
		b, err := json.Marshal(extras)
		if err != nil {
			t.Fatalf("marshal extras: %v", err)
		}
		target += "?extras=" + url.QueryEscape(string(b))
	}
	dec0, drop0 := widgetSkippedUndeclaredExtrasPutTotal.Load(), viewercontext.DroppedTotal()
	rec := httptest.NewRecorder()
	Widgets().ServeHTTP(rec, httptest.NewRequest("GET", target, nil).WithContext(reqCtx))
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	res.declineDelta = widgetSkippedUndeclaredExtrasPutTotal.Load() - dec0
	res.droppedDelta = viewercontext.DroppedTotal() - drop0
	res.served = rec.Body.Bytes()
	if entry, hit := handle.Get(key); hit {
		res.persisted = entry.RawJSON
	}
	return res
}

func tzKey(t *testing.T, spec map[string]any, extras map[string]any) string {
	t.Helper()
	cr := h1WidgetUnstructured(spec)
	ctx := h1ReqCtx(h1User)
	key, _, _ := dispatchCacheLookupKey(ctx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1,
		effectiveKeyExtras(ctx, cr.Object, extras))
	return key
}

// CONTROL for the arm below: with no extras at all the apiRef widget caches and
// the revisit hits. Passes before and after the change.
func TestViewerContextTZ_Control_NoExtrasCachesAndRevisitHits(t *testing.T) {
	h1BuildWatcher(t)
	a := tzServe(t, tzApiRefSpec(), nil, nil, true)
	if a.persisted == nil {
		t.Fatal("CONTROL: an apiRef widget with no extras must be cached")
	}
	b := tzServe(t, tzApiRefSpec(), nil, nil, false)
	if b.resolves != 0 {
		t.Fatalf("CONTROL: the revisit resolved (%d) instead of hitting", b.resolves)
	}
}

// Undeclared tz on an apiRef widget: the RESTAction is never handed it, the Put
// happens (it used to decline), the key is the no-tz key, the revisit hits - even
// under a different tz.
func TestViewerContextTZ_Undeclared_StrippedPutsKeyUnchangedRevisitHits(t *testing.T) {
	h1BuildWatcher(t)
	spec := tzApiRefSpec("projects")
	noTzKey := tzKey(t, spec, nil)

	a := tzServe(t, spec, map[string]any{"tz": "Europe/Rome"}, nil, true)
	if bytes.Contains(a.served, []byte("Europe/Rome")) || bytes.Contains(a.persisted, []byte("Europe/Rome")) {
		t.Fatalf("an undeclared tz reached the apiRef RESTAction input; served=%s persisted=%s", a.served, a.persisted)
	}
	if a.persisted == nil || a.declineDelta != 0 {
		t.Fatalf("an undeclared tz declined the Put (stored=%v decline +%d): the data widgets would stop caching the day tz is sent", a.persisted != nil, a.declineDelta)
	}
	if got := tzKey(t, spec, map[string]any{"tz": "Europe/Rome"}); got != noTzKey {
		t.Fatalf("an undeclared tz moved the key: %q vs no-tz %q", got, noTzKey)
	}
	b := tzServe(t, spec, map[string]any{"tz": "UTC"}, nil, false)
	if b.resolves != 0 {
		t.Fatalf("the revisit under another tz resolved (%d); it must hit the one shared cell", b.resolves)
	}
	c := tzServe(t, spec, nil, nil, false)
	if c.resolves != 0 {
		t.Fatalf("a request without tz resolved (%d); it must hit the same cell", c.resolves)
	}
}

// F6-6 `foo` stays quarantined: an undeclared NON-viewer-context key on an apiRef
// widget still declines the Put. The tz exemption must not widen into "any key".
func TestViewerContextTZ_UndeclaredNonViewerContextKeyStillQuarantines(t *testing.T) {
	h1BuildWatcher(t)
	r := tzServe(t, tzApiRefSpec("projects"), map[string]any{"foo": "evil", "tz": "Europe/Rome"}, nil, true)
	if r.persisted != nil || r.declineDelta != 1 {
		t.Fatalf("F6-6 regression: an undeclared foo must decline the Put (stored=%v decline +%d)", r.persisted != nil, r.declineDelta)
	}
}

// Declared tz: the RESTAction is handed it, the key differs by tz, Europe/Rome
// and UTC are different cells, and the same tz hits.
func TestViewerContextTZ_Declared_ReachesRAPartitionsKeySameTzHits(t *testing.T) {
	h1BuildWatcher(t)
	spec := tzApiRefSpec("projects", "tz")
	rome := map[string]any{"tz": "Europe/Rome"}
	utc := map[string]any{"tz": "UTC"}

	if tzKey(t, spec, rome) == tzKey(t, spec, utc) || tzKey(t, spec, rome) == tzKey(t, spec, nil) {
		t.Fatal("a declared tz must partition the key: Rome, UTC and no-tz must be three cells")
	}

	a := tzServe(t, spec, rome, rome, true)
	if !bytes.Contains(a.served, []byte("Europe/Rome")) || !bytes.Contains(a.persisted, []byte("Europe/Rome")) {
		t.Fatalf("a declared tz did not reach the apiRef RESTAction; served=%s", a.served)
	}
	if a.persisted == nil || a.declineDelta != 0 {
		t.Fatalf("a declared tz must be cached (stored=%v decline +%d)", a.persisted != nil, a.declineDelta)
	}
	b := tzServe(t, spec, utc, utc, false)
	if b.resolves != 1 || !bytes.Contains(b.persisted, []byte("UTC")) || bytes.Contains(b.persisted, []byte("Rome")) {
		t.Fatalf("UTC must resolve its own cell, not reuse Rome's (resolves=%d persisted=%s)", b.resolves, b.persisted)
	}
	c := tzServe(t, spec, rome, rome, false)
	if c.resolves != 0 || !bytes.Contains(c.served, []byte("Europe/Rome")) {
		t.Fatalf("the same tz must hit its cell (resolves=%d served=%s)", c.resolves, c.served)
	}
}

// A forged (valid-looking) tz on a declared widget lands only in that tz's
// cell: a viewer who sends another tz, or none, never reads it.
func TestViewerContextTZ_Declared_ForgedValueLandsOnlyInItsOwnCell(t *testing.T) {
	h1BuildWatcher(t)
	spec := tzApiRefSpec("tz")
	forged := map[string]any{"tz": "Pacific/Kiritimati"}
	tzServe(t, spec, forged, forged, true)

	for name, ex := range map[string]map[string]any{"none": nil, "UTC": {"tz": "UTC"}, "Rome": {"tz": "Europe/Rome"}} {
		r := tzServe(t, spec, ex, ex, false)
		if r.resolves != 1 {
			t.Fatalf("%s: a viewer without the forged tz read the forged cell (resolves=%d, want a miss)", name, r.resolves)
		}
		if bytes.Contains(r.served, []byte("Kiritimati")) {
			t.Fatalf("%s: the forged tz leaked into another viewer's body: %s", name, r.served)
		}
	}
}

// Declared tz on a widget without an apiRef (the template reads .tz), through
// the real Resolve: the body uses it and Rome / UTC are different cells.
func TestViewerContextTZ_Declared_RealResolveBodyUsesTz(t *testing.T) {
	h1BuildWatcher(t)
	spec := a3EchoSpec(nil, []string{"tz"})
	servedRome, persistedRome, _ := a3Serve(t, spec, map[string]any{"tz": "Europe/Rome"})
	if !bytes.Contains(servedRome, []byte("Europe/Rome")) || len(persistedRome) == 0 {
		t.Fatalf("a declared tz must shape the body and be cached; served=%s", servedRome)
	}
	_, persistedUTC, _ := a3Serve(t, spec, map[string]any{"tz": "UTC"})
	if bytes.Contains(persistedUTC, []byte("Europe/Rome")) || !bytes.Contains(persistedUTC, []byte("UTC")) {
		t.Fatalf("UTC read Rome's cell or was not cached: %s", persistedUTC)
	}
}

// Values that are not an IANA zone name are dropped before the key and before
// the resolve, as if not sent: the widget still caches, the body carries
// nothing, and the cell is the no-tz cell.
func TestViewerContextTZ_InvalidValueIsDroppedAsIfNotSent(t *testing.T) {
	h1BuildWatcher(t)
	bad := map[string]any{
		"sql":       "UTC'); DROP TABLE otel_logs; --",
		"quote":     "Europe/Rome'",
		"newline":   "Europe/Rome\n",
		"overlong":  "America/" + string(bytes.Repeat([]byte("a"), 80)),
		"Local":     "Local",
		"empty":     "",
		"number":    float64(7),
		"traversal": "../../etc/passwd",
	}
	for _, spec := range []struct {
		name string
		s    map[string]any
	}{{"declared", tzApiRefSpec("tz")}, {"undeclared", tzApiRefSpec("projects")}} {
		for name, v := range bad {
			t.Run(spec.name+"/"+name, func(t *testing.T) {
				r := tzServe(t, spec.s, map[string]any{"tz": v}, nil, true)
				if r.persisted == nil || r.declineDelta != 0 {
					t.Fatalf("an invalid tz made the widget uncacheable (stored=%v decline +%d)", r.persisted != nil, r.declineDelta)
				}
				if r.droppedDelta != 1 {
					t.Fatalf("the drop was not counted (+%d)", r.droppedDelta)
				}
				if s, ok := v.(string); ok && s != "" && (bytes.Contains(r.served, []byte(s)) || bytes.Contains(r.persisted, []byte("DROP"))) {
					t.Fatalf("an invalid tz reached the resolve: %s", r.served)
				}
				if bytes.Contains(r.persisted, []byte(`"tz":`)) {
					t.Fatalf("a tz key reached the resolve input: %s", r.persisted)
				}
				// The cell is the no-tz cell: a request with no tz hits it.
				if again := tzServe(t, spec.s, nil, nil, false); again.resolves != 0 {
					t.Fatalf("the cell is not the no-tz cell: a request without tz resolved (%d)", again.resolves)
				}
			})
		}
	}
}

// An undeclared tz on an inline-embedding parent keeps the old behaviour (the
// Put declines): embedded children receive the raw request extras and may
// declare tz themselves while this cell is not partitioned by it.
func TestViewerContextTZ_UndeclaredOnInlineParentStillDeclines(t *testing.T) {
	cr := h1WidgetUnstructured(tzApiRefSpec("projects"))
	if requestExtrasFullyDeclared(cr.Object, map[string]any{"tz": "Europe/Rome"}) != true {
		t.Fatal("CONTROL: a plain apiRef widget treats an undeclared tz as safe")
	}
	spec := tzApiRefSpec("projects")
	spec["resourcesRefs"] = map[string]any{"items": []any{map[string]any{
		"id": "child", "apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
		"resource": h1WidgetGVR.Resource, "namespace": h1NS, "name": "child", "verb": "GET", "inline": true,
	}}}
	if !hasInlineGETRef(h1WidgetUnstructured(spec).Object) {
		t.Fatal("fixture does not build an inline-embedding parent; hasInlineGETRef is false")
	}
	if requestExtrasFullyDeclared(h1WidgetUnstructured(spec).Object, map[string]any{"tz": "Europe/Rome"}) {
		t.Fatal("an inline-embedding parent must not exempt an undeclared tz: its children get the raw extras")
	}
	if !requestExtrasFullyDeclared(h1WidgetUnstructured(spec).Object, map[string]any{"username": "u"}) {
		t.Fatal("identity keys stay exempt on an inline parent (the per-user inline marker covers them)")
	}
}
