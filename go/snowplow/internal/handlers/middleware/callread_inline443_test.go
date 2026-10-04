// callread_inline443_test.go — #443 part 2, TestS443_Inline/g (Validation400)
// for the body-shape cases, plus the minting rule: a VALID inline object is
// stashed and marks the request inert; a request without one is not inert.
package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
)

const inlineQ443 = "/call/read?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo&name=draft"

func inlineRA443(mut func(m map[string]any)) map[string]any {
	m := map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": "draft", "namespace": "demo"},
		"spec":       map[string]any{"api": []any{map[string]any{"name": "ns", "path": "/api/v1/namespaces"}}},
	}
	if mut != nil {
		mut(m)
	}
	return m
}

// serveInline443 runs BodyExtrasDecode and reports whether next was reached,
// and with which ctx flags.
func serveInline443(t *testing.T, target string, body []byte) (code int, reached, inert, hasObj bool) {
	t.Helper()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		inert = cache.Inert(r.Context())
		_, hasObj = util.InlineObject(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	BodyExtrasDecode(next).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body)))
	return rec.Code, reached, inert, hasObj
}

func mustJSON443(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestS443_InlineValidation(t *testing.T) {
	t.Run("valid_object_mints_inert", func(t *testing.T) {
		code, reached, inert, hasObj := serveInline443(t, inlineQ443, mustJSON443(t, map[string]any{"extras": map[string]any{}, "object": inlineRA443(nil)}))
		if code != http.StatusOK || !reached || !inert || !hasObj {
			t.Fatalf("valid inline object: code=%d reached=%v inert=%v hasObj=%v, want 200/true/true/true", code, reached, inert, hasObj)
		}
	})
	t.Run("no_object_is_not_inert", func(t *testing.T) {
		code, reached, inert, hasObj := serveInline443(t, inlineQ443, mustJSON443(t, map[string]any{"extras": map[string]any{"a": 1}}))
		if code != http.StatusOK || !reached || inert || hasObj {
			t.Fatalf("extras-only body: code=%d reached=%v inert=%v hasObj=%v, want 200/true/false/false", code, reached, inert, hasObj)
		}
	})

	big := strings.Repeat("x", CallReadMaxBodyBytes+16)
	cases := []struct {
		name   string
		target string
		body   []byte
	}{
		{"body_over_1MiB", inlineQ443, mustJSON443(t, map[string]any{"object": inlineRA443(func(m map[string]any) {
			m["spec"].(map[string]any)["filter"] = big
		})})},
		{"kind_mismatch", inlineQ443, mustJSON443(t, map[string]any{"object": inlineRA443(func(m map[string]any) { m["kind"] = "Panel" })})},
		{"apiVersion_mismatch", inlineQ443, mustJSON443(t, map[string]any{"object": inlineRA443(func(m map[string]any) { m["apiVersion"] = "templates.krateo.io/v2" })})},
		{"name_mismatch", inlineQ443, mustJSON443(t, map[string]any{"object": inlineRA443(func(m map[string]any) {
			m["metadata"].(map[string]any)["name"] = "other"
		})})},
		{"namespace_mismatch", inlineQ443, mustJSON443(t, map[string]any{"object": inlineRA443(func(m map[string]any) {
			m["metadata"].(map[string]any)["namespace"] = "other"
		})})},
		{"does_not_convert", inlineQ443, mustJSON443(t, map[string]any{"object": inlineRA443(func(m map[string]any) { m["spec"] = "not-an-object" })})},
		{"object_on_widgets", "/call/read?apiVersion=widgets.templates.krateo.io/v1beta1&resource=tables&namespace=demo&name=draft",
			mustJSON443(t, map[string]any{"object": inlineRA443(nil)})},
		{"object_on_fallthrough_gvr", "/call/read?apiVersion=v1&resource=configmaps&namespace=demo&name=draft",
			mustJSON443(t, map[string]any{"object": inlineRA443(nil)})},
		{"object_on_restactions_other_group", "/call/read?apiVersion=example.com/v1&resource=restactions&namespace=demo&name=draft",
			mustJSON443(t, map[string]any{"object": inlineRA443(nil)})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, reached, _, _ := serveInline443(t, tc.target, tc.body)
			if code != http.StatusBadRequest || reached {
				t.Fatalf("%s: code=%d reached-next=%v, want 400 and never reaching the dispatcher", tc.name, code, reached)
			}
		})
	}
}
