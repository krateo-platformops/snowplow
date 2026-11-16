package jq

import (
	"math"
	"unicode/utf8"
)

// DecodedObjectOf returns the Go tree that
//
//	var m map[string]any
//	err := json.Unmarshal([]byte(jqutil.Eval(...)), &m)
//
// produces for a jq program whose output values are outputs, computed from the
// values themselves instead of from their encoding (#1079). ok is true only
// when that decode would succeed AND the returned tree is exactly what it
// would hold: the same Go types, the same values, the same nil-vs-empty
// containers. When ok is false the caller must decode the encoding, which is
// always correct; false therefore costs speed, never a result.
//
// What the encode→decode round trip does to a value, and so what this does:
//
//   - every number becomes float64 (encoding/json without UseNumber). int,
//     int32 and int64 convert with Go's integer→float conversion, which rounds
//     to nearest-even exactly as strconv.ParseFloat rounds their decimal text.
//     A float64 is kept: jqutil writes the shortest text that parses back to
//     the same float64, -0 included. NaN is written as null, so it becomes nil.
//   - objects become fresh map[string]any and arrays fresh []any (a nil slice
//     or map is written as [] or {}, so it becomes an EMPTY one, never nil).
//     The returned tree therefore shares no container with outputs, which may
//     alias the RESTAction's stage data.
//   - a valid UTF-8 string decodes to the same bytes, so the string is shared.
//
// Everything this does not reproduce exactly answers ok=false, so the caller
// decodes: zero or several outputs (no object at the top level), a top level
// that is not an object or null, an infinity (clamped to ±MaxFloat64 by the
// encoder), a *big.Int or json.Number (their decimal text, re-parsed), a
// string or key that is not valid UTF-8 (each bad byte becomes U+FFFD, and two
// keys may then collide), nesting deeper than encoding/json accepts, and any
// type jqutil's encoder does not know.
//
// No output is ever written to: outputs is read-only here.
func DecodedObjectOf(outputs []any) (map[string]any, bool) {
	switch len(outputs) {
	case 0:
		// The caller encodes "no value" as JSON null (restactions #709), which
		// decodes into a nil map.
		return nil, true
	case 1:
	default:
		return nil, false
	}
	switch top := outputs[0].(type) {
	case nil:
		return nil, true
	case map[string]any:
		m, ok := decodedObject(top, 1)
		return m, ok
	default:
		return nil, false
	}
}

// maxDecodedDepth is the deepest container nesting this reproduces. encoding/json
// rejects input nested deeper than 10000; staying below it keeps that error on
// the decode path, where it is produced exactly.
const maxDecodedDepth = 10000

func decodedObject(in map[string]any, depth int) (map[string]any, bool) {
	if depth >= maxDecodedDepth {
		return nil, false
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if !utf8.ValidString(k) {
			return nil, false
		}
		dv, ok := decodedValue(v, depth)
		if !ok {
			return nil, false
		}
		out[k] = dv
	}
	return out, true
}

func decodedArray(in []any, depth int) ([]any, bool) {
	if depth >= maxDecodedDepth {
		return nil, false
	}
	out := make([]any, len(in))
	for i, v := range in {
		dv, ok := decodedValue(v, depth)
		if !ok {
			return nil, false
		}
		out[i] = dv
	}
	return out, true
}

// decodedValue is one value's decoded form; depth is its parent's nesting.
func decodedValue(v any, depth int) (any, bool) {
	switch x := v.(type) {
	case nil:
		return nil, true
	case string:
		if !utf8.ValidString(x) {
			return nil, false
		}
		return v, true
	case bool:
		return v, true
	case float64:
		if math.IsNaN(x) {
			return nil, true
		}
		if math.IsInf(x, 0) {
			return nil, false
		}
		return v, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case map[string]any:
		return decodedObject(x, depth+1)
	case []any:
		return decodedArray(x, depth+1)
	default:
		return nil, false
	}
}
