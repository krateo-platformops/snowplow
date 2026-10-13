package jq

// safe_error_638_test.go — #638 arms for the iterator contract (IterateValues:
// EXACT main parity on elements; a real iterator error is an error the stage
// fails with) and the runtime-error sanitizer (SanitizeError).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/jqutil"
)

// s638Sentinel is a Secret-shaped value: what a stage response that fails a jq
// expression can carry. It fits whole inside gojq's ~24-byte value preview, so
// an unsanitized error text would carry it verbatim.
const s638Sentinel = "withheld-sentinel-638-zz"

func s638Data() map[string]any {
	return map[string]any{
		"secret": map[string]any{
			"kind": "Secret",
			"data": map[string]any{"password": s638Sentinel},
		},
	}
}

// TestIssue638_IterateValues_MainParity runs every iterator idiom through BOTH
// the pre-#638 contract (plumbing jqutil.ForEach, the call setup.go made) and
// IterateValues, and requires the SAME plan:
//
//   - ForEach succeeded → IterateValues yields exactly the same elements;
//   - ForEach failed on a null or no-value result (the stage dispatched
//     nothing) → IterateValues yields no elements and ErrIteratorNoValue, which
//     a RESTAction stage treats as its empty plan;
//   - ForEach failed on anything else (the stage dispatched nothing, the error
//     was only logged) → IterateValues returns an error, which the stage now
//     fails with (#638) instead of silently dispatching nothing.
//
// The rows include reviewer-628's main-vs-PR probe idioms (`// empty`,
// `else empty`, def calls, mixed branches, an empty upstream behind select).
func TestIssue638_IterateValues_MainParity(t *testing.T) {
	ab := []any{"a", "b"}
	cases := []struct {
		name  string
		query string
		data  map[string]any
		want  string // "elems" (same as main) | "empty" | "error"
	}{
		{"array path", `.x`, map[string]any{"x": ab}, "elems"},
		{"// empty, present", `.x // empty`, map[string]any{"x": ab}, "elems"},
		{"// empty, absent", `.x // empty`, map[string]any{}, "empty"},
		{"if/else empty, present", `if .x then .x else empty end`, map[string]any{"x": ab}, "elems"},
		{"if/else empty, absent", `if .x then .x else empty end`, map[string]any{}, "empty"},
		{"def call returning the array", `def f: .x; f`, map[string]any{"x": ab}, "elems"},
		{"mixed branches, array branch", `if .flag then .x else .x[] end`, map[string]any{"flag": true, "x": ab}, "elems"},
		{"empty upstream behind select", `.up | select(. != null)`, map[string]any{}, "empty"},
		{"select keeps the array", `.up | select(. != null)`, map[string]any{"up": ab}, "elems"},
		{"null upstream", `.up`, map[string]any{}, "empty"},
		{"empty array", `.x`, map[string]any{"x": []any{}}, "elems"},
		{"array construction", `[.x[] | ascii_upcase]`, map[string]any{"x": ab}, "elems"},
		{".[] over a single-key dict (one array value)", `.[]`, map[string]any{"ns": ab}, "elems"},
		{"one-array stream", `.x[]`, map[string]any{"x": []any{ab}}, "elems"},
		{"def call streaming", `def f: .x[]; f`, map[string]any{"x": ab}, "error"},
		{"mixed branches, stream branch", `if .flag then .x else .x[] end`, map[string]any{"flag": false, "x": ab}, "error"},
		{"stream form, 2 values", `.x[]`, map[string]any{"x": ab}, "error"},
		{"stream form, 1 object", `.x[]`, map[string]any{"x": []any{map[string]any{"k": "v"}}}, "error"},
		{"object", `.x`, map[string]any{"x": map[string]any{"k": "v"}}, "error"},
		{"scalar", `.x`, map[string]any{"x": "s"}, "error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var mainElems []any
			mainErr := jqutil.ForEach(context.Background(), jqutil.EvalOptions{Query: c.query, Unquote: true, Data: c.data},
				func(v any) error { mainElems = append(mainElems, v); return nil })
			got, err := IterateValues(context.Background(), c.query, c.data, nil)

			switch c.want {
			case "elems":
				if mainErr != nil {
					t.Fatalf("SETUP: main (jqutil.ForEach) failed on %q: %v", c.query, mainErr)
				}
				if err != nil {
					t.Fatalf("IterateValues(%q) error %v; main built a plan of %d elements", c.query, err, len(mainElems))
				}
				if jsonOf(got) != jsonOf(mainElems) {
					t.Fatalf("IterateValues(%q) = %s, main = %s — the plan changed", c.query, jsonOf(got), jsonOf(mainElems))
				}
			case "empty":
				if mainErr == nil && len(mainElems) != 0 {
					t.Fatalf("SETUP: main built %d elements for %q", len(mainElems), c.query)
				}
				if !errors.Is(err, ErrIteratorNoValue) || len(got) != 0 {
					t.Fatalf("IterateValues(%q) = (%v, %v), want ErrIteratorNoValue and no elements (main dispatched nothing)", c.query, got, err)
				}
			case "error":
				if mainErr == nil {
					t.Fatalf("SETUP: main succeeded on %q with %v; an error row must be a main failure", c.query, mainElems)
				}
				if err == nil {
					t.Fatalf("IterateValues(%q) = %v with no error; main failed (%v) and dispatched nothing — this must be a stage error, not a new plan", c.query, got, mainErr)
				}
			}
		})
	}
}

func jsonOf(v []any) string {
	if len(v) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// TestIssue638_RuntimeErrors_CarryNoValue drives REAL gojq runtime errors over
// a Secret-shaped value and asserts the sanitized text keeps the class and
// never the value.
func TestIssue638_RuntimeErrors_CarryNoValue(t *testing.T) {
	cases := []struct {
		query     string
		wantClass string
	}{
		{`.secret.data.password | keys`, "func0TypeError in keys (got string)"},
		{`.secret.data.password[]`, "iteratorError (got string)"},
		{`.secret.data.password + 1`, "binopTypeError in add"},
		{`.secret.data.password | error`, "exitCodeError"},
		{`.secret.data | error(.password)`, "exitCodeError"},
		{`.secret.data.password | .x`, "expectedObjectError (got string)"},
		{`.secret.data | test(.password)`, "func2TypeError in test"},
		{`.secret.data.password | fromjson`, ""},
	}
	for _, c := range cases {
		_, err := IterateValues(context.Background(), c.query, s638Data(), nil)
		if err == nil {
			t.Fatalf("%q: no runtime error — the arm cannot see a leak", c.query)
		}
		raw := rawGojqError(t, c.query)
		if !strings.Contains(raw, s638Sentinel) {
			t.Logf("%q: raw gojq text does not preview the value (%q) — not a leak witness, still asserted", c.query, raw)
		}
		if strings.Contains(err.Error(), s638Sentinel) {
			t.Errorf("%q: sanitized error carries the value: %v", c.query, err)
		}
		if c.wantClass != "" && !strings.Contains(err.Error(), c.wantClass) {
			t.Errorf("%q: sanitized error %q lost its class, want it to contain %q", c.query, err, c.wantClass)
		}
		if s := SanitizeError(rawErr(t, c.query)); strings.Contains(s.Error(), s638Sentinel) {
			t.Errorf("%q: SanitizeError(raw) carries the value: %v", c.query, s)
		}
	}
}

// TestIssue638_QueryDerivedErrors_PassThrough — parse errors keep their jq
// position, compile errors their name; nil iteration keeps the benign marker.
func TestIssue638_QueryDerivedErrors_PassThrough(t *testing.T) {
	_, err := IterateValues(context.Background(), `.a | (`, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid jq query") || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("parse error = %v, want the query-derived parse error", err)
	}
	_, err = IterateValues(context.Background(), `nosuchfn638`, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "nosuchfn638") {
		t.Errorf("compile error = %v, want the function name", err)
	}
	_, err = IterateValues(context.Background(), `.missing[]`, map[string]any{}, nil)
	if !IsBenignNilIteration(err) {
		t.Errorf("nil iteration = %v, want IsBenignNilIteration", err)
	}
}

func rawErr(t *testing.T, q string) error {
	t.Helper()
	_, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{Query: q, Data: s638Data()})
	if err == nil {
		t.Fatalf("%q: jqutil.Eval did not error", q)
	}
	return err
}

func rawGojqError(t *testing.T, q string) string { return rawErr(t, q).Error() }
