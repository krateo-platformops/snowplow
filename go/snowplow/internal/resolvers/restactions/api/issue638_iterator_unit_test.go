//go:build unit
// +build unit

package api

// issue638_iterator_unit_test.go — #638 iterator plan, through the REAL
// dispatch-plan builder (createRequestOptionsChecked, the call the stage loop
// makes). The element semantics are EXACTLY origin/main's (one array →
// elements; null or no value → empty plan); the only change is that a real
// iterator error (main: logged, nothing dispatched, cacheable) is returned so
// the stage fails. Rows include reviewer-628's main-vs-PR probe idioms.

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func TestIssue638_IteratorPlan_MainParity(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	plan := func(iter string, dict map[string]any) ([]string, error) {
		opts, err := createRequestOptionsChecked(context.Background(), log, &templates.API{
			Name:      "t",
			Path:      `${ "/t/" + (. | tostring) }`,
			Verb:      ptr.To(http.MethodGet),
			DependsOn: &templates.Dependency{Name: "up", Iterator: ptr.To(iter)},
		}, dict)
		var paths []string
		for _, o := range opts {
			paths = append(paths, o.Path)
		}
		return paths, err
	}
	ab := []any{"a", "b"}
	cases := []struct {
		name    string
		iter    string
		dict    map[string]any
		want    []string
		wantErr bool
	}{
		{"array form", `.x`, map[string]any{"x": ab}, []string{"/t/a", "/t/b"}, false},
		{"`// empty` (reviewer probe)", `.x // empty`, map[string]any{"x": ab}, []string{"/t/a", "/t/b"}, false},
		{"`else empty` (reviewer probe)", `if .x then .x else empty end`, map[string]any{"x": ab}, []string{"/t/a", "/t/b"}, false},
		{"def call returning the array (reviewer probe)", `def f: .x; f`, map[string]any{"x": ab}, []string{"/t/a", "/t/b"}, false},
		{"empty upstream behind select → empty plan (reviewer probe)", `.up | select(. != null)`, map[string]any{}, nil, false},
		{"null upstream → empty plan", `.up`, map[string]any{}, nil, false},
		{"`.[]` over a single-key dict (one array value)", `.[]`, map[string]any{"ns": ab}, []string{"/t/a", "/t/b"}, false},
		{"stream form, 2 values → stage error (main: logged + empty)", `.x[]`, map[string]any{"x": ab}, nil, true},
		{"def call streaming → stage error", `def f: .x[]; f`, map[string]any{"x": ab}, nil, true},
		{"object → stage error", `.x`, map[string]any{"x": map[string]any{"k": "v"}}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := plan(c.iter, c.dict)
			if (err != nil) != c.wantErr {
				t.Fatalf("plan(%q) err = %v, wantErr %v", c.iter, err, c.wantErr)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("plan(%q) = %v, want %v", c.iter, got, c.want)
			}
		})
	}
}
