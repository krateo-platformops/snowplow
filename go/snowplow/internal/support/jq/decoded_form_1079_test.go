// decoded_form_1079_test.go — #1079: DecodedObjectOf must be the decode it
// replaces. The ORACLE is the pre-#1079 path itself, run in the test: encode
// the output values with jqutil (the RESTAction filter's encoder) and
// json.Unmarshal the text into a map[string]any, exactly as apiref's
// rawExtensionToMapEncoded did. reflect.DeepEqual compares dynamic types, so an
// int that should have become float64, a nil that should be an empty slice, or
// a NaN that should be nil all fail here.

package jq

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/jqutil"
)

// decodeOracle1079 is the pre-#1079 path: jqutil's text for the output stream,
// "" read as JSON null (restactions #709), decoded into a map.
func decodeOracle1079(t *testing.T, outputs []any) (map[string]any, error) {
	t.Helper()
	var sb strings.Builder
	for _, v := range outputs {
		s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: ".", Data: v})
		if err != nil {
			t.Fatalf("oracle encode: %v", err)
		}
		sb.WriteString(s)
	}
	text := sb.String()
	if text == "" {
		text = "null"
	}
	var m map[string]any
	err := json.Unmarshal([]byte(text), &m)
	return m, err
}

// assertIsTheDecode1079 checks the soundness half of the contract: whenever
// DecodedObjectOf claims exactness, its tree is the oracle's (and the oracle
// decode succeeded). wantExact also pins the completeness half for inputs the
// fast path is meant to cover.
func assertIsTheDecode1079(t *testing.T, label string, outputs []any, wantExact bool) {
	t.Helper()
	got, exact := DecodedObjectOf(outputs)
	if exact != wantExact {
		t.Fatalf("%s: exact=%v, want %v", label, exact, wantExact)
	}
	if !exact {
		return
	}
	want, err := decodeOracle1079(t, outputs)
	if err != nil {
		t.Fatalf("%s: claimed exact, but the decode it replaces fails: %v", label, err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s: not the decode:\n  decode: %#v\n  tree:   %#v", label, want, got)
	}
	// The same value under jq: the encoding of each must match byte for byte.
	a, _ := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: ".", Data: want})
	b, _ := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: ".", Data: got})
	if a != b {
		t.Fatalf("%s: encodings differ:\n  decode: %s\n  tree:   %s", label, a, b)
	}
}

func TestIssue1079_DecodedObjectOf_ExactValueClasses(t *testing.T) {
	var nilSlice []any
	var nilMap map[string]any
	cases := map[string]any{
		"null":          nil,
		"true":          true,
		"false":         false,
		"empty-string":  "",
		"escapes":       "<a&b>    \" \\ \b\f\n\r\t \x00\x1f\x7f é ✓ �",
		"int-zero":      0,
		"int":           42,
		"int-neg":       -7,
		"int-max":       math.MaxInt64, // rounds to 2^63 on both sides
		"int-min":       math.MinInt64,
		"int-2^53+1":    int(1<<53 + 1), // not representable: nearest-even on both sides
		"int64":         int64(1791534491122175009),
		"int64-neg":     int64(-9007199254740993),
		"int32":         int32(-5),
		"float":         0.1,
		"third":         1.0 / 3.0,
		"neg-zero":      math.Copysign(0, -1),
		"huge":          1e300,
		"exp-boundary":  1e21,
		"tiny":          1e-7,
		"subnormal":     5e-324,
		"max-float":     math.MaxFloat64,
		"nan":           math.NaN(), // written as null
		"nil-slice":     nilSlice,   // written as [] → an EMPTY slice
		"empty-slice":   []any{},
		"nil-map":       nilMap, // written as {} → an EMPTY map
		"empty-map":     map[string]any{},
		"nested":        map[string]any{"a": []any{1, []any{2.5, map[string]any{"k": []any{int64(3), nil}}}, "x"}},
		"key-escapes":   map[string]any{"<k&>": 1, "é\n": 2, "": 3},
		"array-of-many": []any{nil, true, 1, int64(2), 3.5, "s", []any{}, map[string]any{}},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			assertIsTheDecode1079(t, name, []any{map[string]any{"v": v}}, true)
		})
	}
}

func TestIssue1079_DecodedObjectOf_TopLevel(t *testing.T) {
	assertIsTheDecode1079(t, "no output (jq empty → null)", nil, true)
	assertIsTheDecode1079(t, "null", []any{nil}, true)
	assertIsTheDecode1079(t, "object", []any{map[string]any{"a": 1}}, true)
	assertIsTheDecode1079(t, "empty object", []any{map[string]any{}}, true)
	if m, ok := DecodedObjectOf(nil); !ok || m != nil {
		t.Fatalf("no output must be the decode of null: a nil map, got %#v ok=%v", m, ok)
	}
	if m, ok := DecodedObjectOf([]any{map[string]any(nil)}); !ok || m == nil {
		t.Fatalf("a nil map output is written as {}: an EMPTY map, got %#v ok=%v", m, ok)
	}
	// Top levels the decode into a map rejects (or that are a stream): the
	// caller must decode, and the oracle confirms the decode errors.
	for name, outputs := range map[string][]any{
		"array":          {[]any{1, 2}},
		"string":         {"s"},
		"number":         {1},
		"bool":           {true},
		"two objects":    {map[string]any{}, map[string]any{}},
		"object then nl": {map[string]any{"a": 1}, nil},
	} {
		assertIsTheDecode1079(t, name, outputs, false)
		if _, err := decodeOracle1079(t, outputs); err == nil {
			t.Fatalf("%s: oracle unexpectedly decodes", name)
		}
	}
}

// Values the round trip changes in ways this does not reproduce: the caller
// must decode them (exact=false), never get a guessed tree.
func TestIssue1079_DecodedObjectOf_DeclinesWhatItCannotReproduce(t *testing.T) {
	big1, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	cases := map[string]any{
		"+inf":                math.Inf(1),
		"-inf":                math.Inf(-1),
		"big.Int":             big1,
		"json.Number":         json.Number("1.50"),
		"invalid-utf8-string": "a\xffb",
		"invalid-utf8-nested": []any{map[string]any{"x": "\xc3"}},
		"unknown-type":        struct{}{},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := DecodedObjectOf([]any{map[string]any{"v": v}}); ok {
				t.Fatalf("%s: claimed exact", name)
			}
		})
	}
	t.Run("invalid-utf8-key", func(t *testing.T) {
		// Both keys encode as "�": the decode keeps ONE of them.
		if _, ok := DecodedObjectOf([]any{map[string]any{"\xff": 1, "\xfe": 2}}); ok {
			t.Fatal("claimed exact for keys that collide once decoded")
		}
	})
}

func TestIssue1079_DecodedObjectOf_NestingBound(t *testing.T) {
	nest := func(depth int) map[string]any {
		var v any = "leaf"
		for i := 1; i < depth; i++ {
			v = []any{v}
		}
		return map[string]any{"v": v}
	}
	// 9999 containers deep (the object plus 9998 arrays): within encoding/json's
	// limit, reproduced exactly.
	assertIsTheDecode1079(t, "depth 9999", []any{nest(9999)}, true)
	// At the limit it declines, so the decode decides (it may error).
	if _, ok := DecodedObjectOf([]any{nest(10000)}); ok {
		t.Fatal("depth 10000: claimed exact at encoding/json's nesting limit")
	}
}

// TestIssue1079_DecodedObjectOf_SharesNoContainer — the outputs may alias the
// RESTAction's stage data (jq's `.foo` yields input sub-trees by reference), and
// the caller hands the tree to code that writes into it. The decode it replaces
// built fresh containers; so must this.
func TestIssue1079_DecodedObjectOf_SharesNoContainer(t *testing.T) {
	inner := map[string]any{"k": "v"}
	arr := []any{inner, []any{"x"}}
	out := map[string]any{"arr": arr, "obj": inner}
	before, _ := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: ".", Data: out})

	tree, ok := DecodedObjectOf([]any{out})
	if !ok {
		t.Fatal("not exact")
	}
	var scribble func(v any)
	scribble = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, c := range x {
				scribble(c)
				x[k] = "scribbled"
			}
			x["added"] = true
		case []any:
			for i, c := range x {
				scribble(c)
				x[i] = "scribbled"
			}
		}
	}
	scribble(tree)
	after, _ := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: ".", Data: out})
	if before != after {
		t.Fatalf("#1079 RED: writing into the tree changed the outputs it was built from:\n  before: %s\n  after:  %s", before, after)
	}
}

// TestIssue1079_DecodedObjectOf_RandomTreesAreTheDecode — a seeded differential
// over random trees of every class gojq and the stage data hand the filter.
// Soundness: whenever exact, the tree IS the decode. Completeness: a tree built
// only from the classes the fast path covers is always exact.
func TestIssue1079_DecodedObjectOf_RandomTreesAreTheDecode(t *testing.T) {
	r := rand.New(rand.NewSource(1079))
	strs := []string{"", "a", "é✓", "<&>", " ", "line\nfeed", "\x7f", "q\"uote", "back\\slash"}
	var gen func(depth int, withDeclined bool) any
	gen = func(depth int, withDeclined bool) any {
		n := 10
		if withDeclined {
			n = 13
		}
		if depth > 5 {
			n = 7 // leaves only
		}
		switch r.Intn(n) {
		case 0:
			return nil
		case 1:
			return r.Intn(2) == 0
		case 2:
			return r.Int() - r.Int()
		case 3:
			return int64(r.Uint64())
		case 4:
			return r.NormFloat64() * math.Pow(10, float64(r.Intn(40)-20))
		case 5:
			return strs[r.Intn(len(strs))] + fmt.Sprint(r.Intn(100))
		case 6:
			return []any{math.NaN(), int32(r.Int31()), math.Copysign(0, -1)}[r.Intn(3)]
		case 7, 8:
			m := map[string]any{}
			for i := r.Intn(5); i > 0; i-- {
				m[strs[r.Intn(len(strs))]+fmt.Sprint(i)] = gen(depth+1, withDeclined)
			}
			return m
		case 9:
			a := make([]any, r.Intn(5))
			for i := range a {
				a[i] = gen(depth+1, withDeclined)
			}
			return a
		case 10:
			return math.Inf(1)
		case 11:
			return big.NewInt(r.Int63())
		default:
			return "bad\xff" + fmt.Sprint(r.Intn(10))
		}
	}
	for i := 0; i < 3000; i++ {
		withDeclined := i%2 == 1
		top := map[string]any{"root": gen(0, withDeclined), "n": i}
		if !withDeclined {
			assertIsTheDecode1079(t, fmt.Sprintf("tree %d", i), []any{top}, true)
			continue
		}
		if _, exact := DecodedObjectOf([]any{top}); exact {
			assertIsTheDecode1079(t, fmt.Sprintf("tree %d", i), []any{top}, true)
		}
	}
}
