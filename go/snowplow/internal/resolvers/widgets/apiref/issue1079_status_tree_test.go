// issue1079_status_tree_test.go — #1079 through the REAL apiref.Resolve: the
// body handed to the widget is the RESTAction's filter output turned into a
// tree WITHOUT re-parsing the Status bytes, whenever that tree is exactly
// their decode. The ORACLE is the pre-#1079 path, run here: jqutil.Eval of
// the filter (restactions.Resolve's Status bytes), then
// rawExtensionToMapEncoded (the decode). Every filter, exact or not, must give
// the same body and the same error as that oracle.

package apiref

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// filters1079 cover the fast path (exact) and every reason to decode instead:
// a top level the map decode rejects, a stream, numbers the round trip
// re-parses (a big integer, an infinity).
var filters1079 = []struct{ name, filter string }{
	{"rich", richFilter883},
	{"empty", `empty`},
	{"null", `null`},
	{"nan", `{"x": nan}`},
	{"array-top", `[1, 2]`},
	{"string-top", `"s"`},
	{"stream", `{"a": 1}, {"b": 2}`},
	{"big-int", `{"b": 100000000000000000000000000001}`},
	{"infinite", `{"x": infinite, "y": -infinite}`},
}

func oracle1079(t *testing.T, filter string) (map[string]any, error) {
	t.Helper()
	// restactions.Resolve before #1079: jqutil.Eval of the filter over the
	// (stage-less, so empty) dict, "" read as JSON null (#709).
	s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: filter, Data: map[string]any{}, ModuleLoader: jqsupport.ModuleLoader()})
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	if s == "" {
		s = "null"
	}
	m, _, err := rawExtensionToMapEncoded(&runtime.RawExtension{Raw: []byte(s)})
	if err != nil {
		return map[string]any{}, err
	}
	return m, nil
}

func TestIssue1079_ApirefBodyIsTheDecode(t *testing.T) {
	cacheOn(t)
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	objs := f6BuildFixture()
	for i, fc := range filters1079 {
		objs = append(objs, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "templates.krateo.io/v1",
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": fmt.Sprintf("ra-1079-%d", i), "namespace": edge3BackingNS},
			"spec":       map[string]any{"filter": fc.filter},
		}})
	}
	edge3NewWatcher(t, objs...)
	if added, syncCh := cache.Global().EnsureResourceType(gvr()); added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gvr(), 3*time.Second) {
		t.Fatalf("restactions GVR never became servable")
	}
	base := f6CtxWithUser(t, "admin", []string{"system:masters"})

	for i, fc := range filters1079 {
		t.Run(fc.name, func(t *testing.T) {
			want, wantErr := oracle1079(t, fc.filter)
			got, gotErr := Resolve(cache.WithL1KeyContext(base, fmt.Sprintf("w-1079-%d", i)), ResolveOptions{ApiRef: templatesv1.ObjectReference{
				Reference:  templatesv1.Reference{Name: fmt.Sprintf("ra-1079-%d", i), Namespace: edge3BackingNS},
				Resource:   "restactions",
				APIVersion: "templates.krateo.io/v1",
			}})
			if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) {
				t.Fatalf("#1079 RED: error %v, want the decode's %v", gotErr, wantErr)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("#1079 RED: body is not the decode:\n  decode: %#v\n  body:   %#v", want, got)
			}
			if fc.name == "rich" {
				if _, isMap := got["empty"].(map[string]any); !isMap {
					t.Fatalf("harness: the RESTAction did not produce the rich body: %#v", got)
				}
			}
		})
	}
}
