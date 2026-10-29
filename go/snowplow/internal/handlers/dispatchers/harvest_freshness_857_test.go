package dispatchers

// harvest_freshness_857_test.go — #857: a keep-warm sweep must resolve a
// widget CR's CURRENT spec, never the boot-time harvested copy.
//
// TestS857_KeepwarmResolvesTheEditedWidgetSpec — the 057 shape: a nav widget
// is harvested at boot with its apiRef on RESTAction A; the CR is then edited
// (a portal upgrade moves the apiRef to RESTAction B, new resourceVersion).
// The keep-warm sweep re-walks the nav roots from live reads, so the walk
// harvests the edited CR again (the same harvestNavWidget call walk() makes);
// the sweep's seed must then resolve the edited spec and Put a cell that
// reflects B, under the key a real user (a group-only member) is served from.
// Before #857 the harvest was first-write-wins: the sweep resolved A every
// sweep for the pod lifetime.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func s857Widget(rv, apiRef string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
		"kind":       "Panel",
		"metadata": map[string]any{"name": "s857-w", "namespace": h1NS,
			"uid": "uid-s857-w", "resourceVersion": rv},
		"spec": map[string]any{"apiRef": map[string]any{"name": apiRef, "namespace": h1NS}},
	}}
}

func TestS857_KeepwarmResolvesTheEditedWidgetSpec(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "1") // keep-warm age-skip threshold = TTL/4
	_ = l262Setup(t, l262Opts{secrets: []*corev1.Secret{
		// erin holds only the group's bindings: the group representative's
		// cells are hers (a real requester, so the representative is seeded).
		l262ClientconfigSecret(t, "erin-clientconfig", "erin", []string{l262Group}, time.Now().Add(-time.Hour), "1"),
	}})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 1)

	var mu sync.Mutex
	var resolvedApiRefs []string
	inner := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, o widgets.ResolveOptions) (*widgets.Widget, error) {
		name := ""
		if ref, err := widgets.GetApiRef(o.In.Object); err == nil {
			name = ref.Name
		}
		mu.Lock()
		resolvedApiRefs = append(resolvedApiRefs, name)
		mu.Unlock()
		if _, err := inner(ctx, o); err != nil {
			return nil, err
		}
		// The body carries the spec it was resolved from.
		return h1WidgetUnstructured(map[string]any{"resolvedFrom": name}), nil
	}
	t.Cleanup(func() { widgetsResolveFn = inner })

	// Boot: the widget as harvested at boot, apiRef → A.
	navHarv := newNavWidgetHarvester()
	navHarv.harvestNavWidget(s857Widget("1", "s857-ra-a"), h1WidgetGVR, unpaginatedPrewarm)
	if err := seedScopeYielding(context.Background(), nil, navHarv.snapshot(),
		endpoints.Endpoint{}, nil, l262AuthnNS, seedModeBoot); err != nil {
		t.Fatal(err)
	}
	erinCtx := l262CustomerCtx("erin", []string{l262Group})
	ws := navHarv.snapshot()
	key, handle, _ := dispatchCacheLookupKey(erinCtx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version,
		h1WidgetGVR.Resource, h1NS, "s857-w", -1, -1, effectiveKeyExtras(erinCtx, ws[0].W.Object, nil))
	if e, ok := handle.GetNoTouch(key); !ok || !strings.Contains(string(e.RawJSON), "s857-ra-a") {
		t.Fatalf("PRECONDITION: the boot seed Put erin's cell from spec A (ok=%v)", ok)
	}

	// The edit (new resourceVersion, apiRef → B), seen by the keep-warm
	// sweep's re-walk exactly as walk() harvests it.
	navHarv.harvestNavWidget(s857Widget("2", "s857-ra-b"), h1WidgetGVR, unpaginatedPrewarm)
	time.Sleep(300 * time.Millisecond) // the cell is older than the age-skip threshold
	mu.Lock()
	resolvedApiRefs = nil
	mu.Unlock()
	if err := seedScopeYielding(context.Background(), nil, navHarv.snapshot(),
		endpoints.Endpoint{}, nil, l262AuthnNS, seedModeKeepwarm); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]string(nil), resolvedApiRefs...)
	mu.Unlock()
	if len(got) == 0 {
		t.Fatal("NON-VACUITY: the keep-warm sweep resolved nothing (age-skip still held?)")
	}
	for _, r := range got {
		if r != "s857-ra-b" {
			t.Fatalf("#857 RED: the keep-warm sweep resolved the widget with apiRef %q — the boot-time spec; "+
				"the CR was edited to %q (resolved: %v)", r, "s857-ra-b", got)
		}
	}
	e, ok := handle.GetNoTouch(key)
	if !ok {
		t.Fatal("the keep-warm sweep left erin's cell absent")
	}
	if !strings.Contains(string(e.RawJSON), "s857-ra-b") {
		t.Fatalf("#857 RED: erin's live cell holds a body resolved from the boot-time spec: %s", e.RawJSON)
	}
}

// TestS857_SameObjectVersionDoesNotChurnTheHarvest — a re-walk (or an informer
// resync) that sees the SAME object version (UID and resourceVersion unchanged)
// keeps the stored copy: no replacement, no counter tick, the entry's stamps
// untouched. A changed UID alone (delete + re-create under the same name, which
// can reuse nothing else) does replace it.
func TestS857_SameObjectVersionDoesNotChurnTheHarvest(t *testing.T) {
	h := newNavWidgetHarvester()
	h.harvestNavWidget(s857Widget("7", "s857-ra-a"), h1WidgetGVR, unpaginatedPrewarm)
	h.harvestNavWidget(s857Widget("1", "s857-other"), h1WidgetGVR, newPrewarmPagination(5, 1)) // a second key: NavOrder 1
	byName := func() map[string]navWidgetEntry {
		out := map[string]navWidgetEntry{}
		for _, e := range h.snapshot() {
			out[e.W.GetResourceVersion()+"/"+strconvItoa(e.Pagination.Page)] = e
		}
		return out
	}
	before := byName()["7/-1"]
	refreshed := navWidgetHarvestRefreshedTotal.Load()

	for i := 0; i < 3; i++ { // resyncs: same UID, same resourceVersion
		h.harvestNavWidget(s857Widget("7", "s857-ra-a"), h1WidgetGVR, unpaginatedPrewarm)
	}
	after, ok := byName()["7/-1"]
	if !ok || after.W != before.W {
		t.Fatalf("#857 RED: a sighting of the SAME object version replaced the stored copy (churn)")
	}
	if after.NavOrder != before.NavOrder || after.RootIndex != before.RootIndex {
		t.Fatalf("the stamps moved: NavOrder %d→%d RootIndex %d→%d",
			before.NavOrder, after.NavOrder, before.RootIndex, after.RootIndex)
	}
	if d := navWidgetHarvestRefreshedTotal.Load() - refreshed; d != 0 {
		t.Fatalf("#857 RED: %d refresh(es) counted for an unchanged object version", d)
	}

	// Same name, same resourceVersion string, NEW UID (deleted and re-created):
	// a different object, so the copy is replaced and the stamps stay.
	recreated := s857Widget("7", "s857-ra-a")
	recreated.SetUID("uid-s857-w-recreated")
	h.harvestNavWidget(recreated, h1WidgetGVR, unpaginatedPrewarm)
	got := byName()["7/-1"]
	if got.W == before.W || got.W.GetUID() != "uid-s857-w-recreated" {
		t.Fatalf("#857 RED: a new UID under the same name kept the old object's copy")
	}
	if got.NavOrder != before.NavOrder {
		t.Fatalf("a replacement moved NavOrder %d→%d", before.NavOrder, got.NavOrder)
	}
	if d := navWidgetHarvestRefreshedTotal.Load() - refreshed; d != 1 {
		t.Fatalf("refresh counter moved by %d, want 1", d)
	}
}

func strconvItoa(i int) string { return fmt.Sprint(i) }
