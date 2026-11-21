package widgets

// resourcesrefs_failed_1564_test.go — #1564, the agentiko-sandbox shape end to
// end through widgets.Resolve. A staged page root names four children whose
// TYPES the staged discovery does not serve. Before #1564 all four were
// dropped: status.resourcesRefs had 0 items, no error, no log, and the portal
// rendered four dangling children. Now status.resourcesRefs.items holds one
// explicit failure entry per child (id, verb, allowed:false, NO path, and the
// TypeNotInstalled `error`), and each is a WARN: never an absence, and never a
// path the portal would fetch as if the child worked.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

type rf1564Handler struct {
	mu    *sync.Mutex
	warns *[]map[string]string
}

func (h rf1564Handler) Enabled(context.Context, slog.Level) bool { return true }
func (h rf1564Handler) Handle(_ context.Context, r slog.Record) error {
	if r.Level != slog.LevelWarn || !strings.HasPrefix(r.Message, "resource ref failed") {
		return nil
	}
	a := map[string]string{}
	r.Attrs(func(x slog.Attr) bool { a[x.Key] = x.Value.String(); return true })
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.warns = append(*h.warns, a)
	return nil
}
func (h rf1564Handler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h rf1564Handler) WithGroup(string) slog.Handler      { return h }

func TestIssue1564_StagedPageRootWhoseChildTypesDiscoveryLacks(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	stubApiRef(t, nil, nil)
	stubValidate(t, nil)

	// The staged discovery: the widgets group/version is served, but its
	// document lists only panels, none of the four child kinds.
	const gv = "widgets.templates.krateo.io/v1beta1"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/apis/"+gv {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
			return
		}
		_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"` + gv + `","resources":[{"name":"panels","singularName":"panel","namespaced":true,"kind":"Panel","verbs":["get","list"]}]}`))
	}))
	t.Cleanup(srv.Close)

	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	warns := &[]map[string]string{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(rf1564Handler{mu: &sync.Mutex{}, warns: warns})),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL}))
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: srv.URL})

	children := []string{"incidentfeeds1564", "alertlists1564", "incidentcharts1564", "alerttables1564"}
	var items []any
	for _, res := range children {
		items = append(items, map[string]any{
			"id": res, "apiVersion": gv, "resource": res, "name": res, "namespace": "demo-system", "verb": "GET",
		})
	}
	w := baseWidget()
	w.Object["spec"].(map[string]any)["resourcesRefs"] = map[string]any{"items": items}

	_, err := Resolve(ctx, ResolveOptions{In: w})
	require.NoError(t, err, "a failed child is a failure entry, not a widget failure")

	got, ok, e := unstructured.NestedSlice(w.Object, "status", "resourcesRefs", "items")
	require.NoError(t, e)
	require.True(t, ok, "#1564: status.resourcesRefs.items must be present (before: 0 items, the silent drop)")
	require.Len(t, got, len(children), "one failure entry per child, never an absence")

	byID := map[string]map[string]any{}
	for _, it := range got {
		m, _ := it.(map[string]any)
		byID[m["id"].(string)] = m
	}
	for _, res := range children {
		m, ok := byID[res]
		require.True(t, ok, "child %q must have an entry the portal can look up by id", res)
		_, hasPath := m["path"]
		require.False(t, hasPath, "child %q: a failure entry carries no path, so the portal never fetches it: %v", res, m)
		require.Equal(t, false, m["allowed"], "child %q: allowed:false", res)
		require.Equal(t, "GET", m["verb"])
		errObj, ok := m["error"].(map[string]any)
		require.True(t, ok, "child %q: the failure entry carries `error`: %v", res, m)
		require.EqualValues(t, 424, errObj["code"])
		require.Equal(t, "FailedDependency", errObj["reason"])
		b, _ := json.Marshal(errObj["details"])
		require.Contains(t, string(b), `"reason":"TypeNotInstalled"`)
		require.Contains(t, string(b), `"kind":"`+res+`"`)
	}

	require.Len(t, *warns, len(children), "one WARN per failed child")
	for _, a := range *warns {
		require.Equal(t, "type_not_installed", a["reason"])
		require.NotEmpty(t, a["err"])
	}
}
