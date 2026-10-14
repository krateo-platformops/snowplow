//go:build unit
// +build unit

package api

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/listenvelope"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// The httpcall.Do apiserver FALL-THROUGH (reviewer-603's #676 re-gate,
// C2(a)): a real apiserver List carries list-level metadata
// (resourceVersion, continue). jsonHandlerCore now normalises every GET
// collection LIST to E6, so the stage value carries no metadata and is
// byte-identical to the informer path's envelope for the same items. A
// list-level .metadata read through a fall-through is therefore no longer
// possible; the informer path never offered it.

var e6FTGVR = schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1", Resource: "widgets"}

const e6FTPath = "/apis/widgets.krateo.io/v1/namespaces/ns/widgets"

func e6FTItems() []any {
	return []any{
		map[string]any{"apiVersion": "widgets.krateo.io/v1", "kind": "Widget",
			"metadata": map[string]any{"name": "w1", "namespace": "ns", "resourceVersion": "11"},
			"spec":     map[string]any{"title": "one"}},
		map[string]any{"apiVersion": "widgets.krateo.io/v1", "kind": "Widget",
			"metadata": map[string]any{"name": "w2", "namespace": "ns", "resourceVersion": "12"},
			"spec":     map[string]any{"title": "two"}},
	}
}

// e6FallThroughStage resolves one GET collection LIST stage against an
// httptest apiserver that answers with a real List (resourceVersion 4242),
// through the plain httpcall.Do path (no informer, no internal rest
// config), and returns the stage value plus the apiserver hit count.
func e6FallThroughStage(t *testing.T) (any, int64) {
	t.Helper()
	sa271FailFast(t)
	installSATokenFile(t)
	var hits atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != e6FTPath {
			http.NotFound(w, r)
			return
		}
		hits.Add(1)
		b, _ := json.Marshal(map[string]any{
			"apiVersion": "widgets.krateo.io/v1", "kind": "WidgetList",
			"metadata": map[string]any{"resourceVersion": "4242", "continue": ""},
			"items":    e6FTItems(),
		})
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(b))
	}))
	t.Cleanup(srv.Close)
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "e6-fallthrough"}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca})

	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{{Name: "widgets", Path: e6FTPath}},
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "e6-fallthrough",
	})
	return dict["widgets"], hits.Load()
}

// TestE6_FallThroughStageValueCarriesNoListMetadata — reviewer-603's probe
// as an arm: the apiserver's List carries metadata.resourceVersion=4242; the
// stage value must not.
func TestE6_FallThroughStageValueCarriesNoListMetadata(t *testing.T) {
	v, hits := e6FallThroughStage(t)
	if hits == 0 {
		t.Fatalf("premise: the stage never reached the httptest apiserver (no fall-through ran); value=%v", v)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("stage value is %T (%v), want the list envelope", v, v)
	}
	if md, has := m["metadata"]; has {
		t.Fatalf("E6 (#676 C2(a)): the fall-through stage value still carries list metadata %v", md)
	}
	if got := e6Keys(t, m); fmt.Sprint(got) != fmt.Sprint(listenvelope.Keys()) {
		t.Fatalf("fall-through stage value keys %v, want exactly %v", got, listenvelope.Keys())
	}
}

// TestE6_FallThroughIsByteIdenticalToTheInformerPath: the fall-through stage
// value and the informer path's envelope (listEnvelopeValue, what a content
// hit and the informer serve hand jsonHandlerValue) for the same items are
// byte-identical.
func TestE6_FallThroughIsByteIdenticalToTheInformerPath(t *testing.T) {
	v, hits := e6FallThroughStage(t)
	if hits == 0 {
		t.Fatalf("premise: no fall-through ran")
	}
	var items []*unstructured.Unstructured
	for _, it := range e6FTItems() {
		items = append(items, &unstructured.Unstructured{Object: it.(map[string]any)})
	}
	informer := listEnvelopeValue(apiVersionForGVR(e6FTGVR), listKindForResource(e6FTGVR.Resource), items)
	got, _ := json.Marshal(v)
	want, _ := json.Marshal(informer)
	if string(got) != string(want) {
		t.Fatalf("fall-through vs informer path differ:\n fall-through: %s\n informer:     %s", got, want)
	}
}
