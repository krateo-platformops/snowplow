//go:build js && wasm

package fakeapiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// maxDepth is the deepest nesting of a snapshot file that is read: containers (objects and arrays
// alike) counted from the file's top level, the top-level one at depth 1. A file nested to maxDepth
// is read; one level more is refused. The bound is held here and nowhere else; agentiko-sandbox's
// two parsers hold the same number.
const maxDepth = 1000

// strictJSON is the one reading of a snapshot file. It parses data once and returns it as plain
// JSON values (map[string]any, []any, string, json.Number, bool, nil), refusing what would make two
// readers of the file disagree:
//
//   - an object that holds a key twice, anywhere in the file: encoding/json and JSON.parse keep the
//     last, a reader that stops at the first does not;
//   - a key that encoding/json would take for a field of t only by ignoring case ("Resources", or
//     "reſources" with a long s, for `resources`): JSON.parse, and agentiko-sandbox's validator
//     with it, read keys exactly. The fold is encoding/json's own: simple, one code point for one;
//   - an unpaired surrogate escape in any string, key or value ("\ud800" alone, "\udc00" alone):
//     encoding/json reads it as U+FFFD where JavaScript keeps it, so two different strings would
//     be one here and two there (unpairedSurrogate). A raw byte that is not UTF-8 is read as U+FFFD
//     on both sides, by encoding/json here and by the sandbox's UTF-8 decoder there;
//   - nesting deeper than maxDepth;
//   - a number that would not be read as written (checkNumber): an integer literal that names no
//     double (integerLiteralOk), a number that is not finite as a double, and, where t holds a Go
//     integer, a fraction or an exponent, or an integer that does not fit it.
//
// t is the Go type the caller then decodes the same bytes into with encoding/json; after strictJSON
// that decode matches every key exactly and has no duplicate to choose from, so the values returned
// here and the typed ones are the same reading. A key that is no field of t is left alone: the
// format is open (snapshot.json holds keys the fake does not read).
//
// encoding/json (v1) cannot match keys exactly. encoding/json/v2 can, and refuses a key twice, but
// it would read a key in another case as one it does not know, silently, where this refuses it; the
// snapshot's types hold Kubernetes API types written for v1; and the rule needs the plain values to
// name what is wrong with a list (v1 or v2 would report a Go type instead). So the check is this
// token walk beside t, and the decode into t stays v1's.
func strictJSON(data []byte, t reflect.Type) (any, error) {
	if at, esc, found := unpairedSurrogate(data); found {
		return nil, fmt.Errorf("the JSON holds an unpaired surrogate, %s, at byte %d", esc, at)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := strictValue(dec, t, "", 0)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("data after the JSON value")
	}
	return v, nil
}

// checkNumber refuses a number token that would not be read as written. What a number is, is
// decided by the form of its literal, as agentiko-sandbox's parsers decide it:
//
//   - an integer has no fraction and no exponent (an optional minus and digits): it is read only if
//     it names one double, exactly or as that double's own shortest spelling (integerLiteralOk);
//   - any other literal is a float, and only has to be finite as an IEEE double: 1e20,
//     9007199254740993.0 and 1.5e300 are read, 1e400 is not;
//   - where t is a Go integer, a float, and an integer that does not fit t (9223372036854776000 in
//     an int64), are refused here, by name, and not by the decoder after.
func checkNumber(n json.Number, t reflect.Type, at string) error {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	lit := n.String()
	if strings.ContainsAny(lit, ".eE") {
		if f, _ := strconv.ParseFloat(lit, 64); math.IsInf(f, 0) {
			return fmt.Errorf("%s is the number %s, which is not finite as a double", at, lit)
		}
		if t != nil && t.Kind() >= reflect.Int && t.Kind() <= reflect.Uintptr {
			return fmt.Errorf("%s is the number %s, where an integer is read", at, lit)
		}
		return nil
	}
	i, ok := new(big.Int).SetString(lit, 10)
	if !ok || !integerLiteralOk(lit, i) {
		return fmt.Errorf("%s is the integer %s, which names no double: it is neither one exactly nor one's shortest spelling", at, lit)
	}
	if t != nil && t.Kind() >= reflect.Int && t.Kind() <= reflect.Uintptr {
		signed := t.Kind() <= reflect.Int64
		if (signed && (!i.IsInt64() || reflect.Zero(t).OverflowInt(i.Int64()))) || (!signed && (!i.IsUint64() || reflect.Zero(t).OverflowUint(i.Uint64()))) {
			return fmt.Errorf("%s is the integer %s, which does not fit the %s read there", at, lit, t.Kind())
		}
	}
	return nil
}

// integerLiteralOk is the integer rule agentiko-sandbox's parsers keep too: the double f nearest to
// the integer literal lit (i, its value) is finite, and lit names f, either exactly (lit is f's
// value) or as f's own shortest spelling, the one ECMAScript's String(f) writes. JSON is read and
// written as doubles on both sides: JavaScript holds every number as one, encoding/json into an
// untyped value too, and the sandbox's canonical writer (RFC 8785) writes each as String(f). So:
//
//   - every literal read names exactly one double, the one it is then held as, and nothing is read
//     as a value other than the one it names: 9007199254740993, 9223372036854775807 (both would be
//     rounded, silently) and a 400-digit integer (an infinity) are refused;
//   - the rule is closed under that writer: what it writes is read back as the same double. The
//     exact arm alone is not: -2^63 is written -9223372036854776000, which is 2^63+192 exactly, not
//     a double. The shortest-spelling arm reads it, and 9223372036854776000, kube-apiserver's
//     spelling of an int64 maximum in a CRD schema, with it;
//   - String(f) writes an integer f in exponent form at 1e21 and above, so there the shortest-
//     spelling arm does not apply: 2^70 is read written out in full (exactly), and
//     1180591620717411300000, which no writer produces, is not. Below 1e21, String(f) is
//     strconv.FormatFloat(f, 'f', -1, 64), the spelling encoding/json writes too
//     (TestStrictJSON_ShortestSpellingIsECMAScripts checks it against Node's String).
//
// i is never negative zero: "-0" is 0 exactly, and read.
func integerLiteralOk(lit string, i *big.Int) bool {
	f, _ := new(big.Float).SetInt(i).Float64() // the nearest double, ties to even; ±Inf beyond the largest
	if math.IsInf(f, 0) {
		return false
	}
	if back, _ := new(big.Float).SetFloat64(f).Int(nil); back.Cmp(i) == 0 {
		return true
	}
	return math.Abs(f) < 1e21 && strconv.FormatFloat(f, 'f', -1, 64) == lit
}

func strictValue(dec *json.Decoder, t reflect.Type, path string, depth int) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	at := func() string {
		if path == "" {
			return "the top level"
		}
		return strings.TrimPrefix(path, ".")
	}
	if d, ok := tok.(json.Delim); ok && (d == '{' || d == '[') {
		if depth++; depth > maxDepth {
			return nil, fmt.Errorf("the JSON is nested deeper than %d containers", maxDepth)
		}
	}
	if n, ok := tok.(json.Number); ok {
		if err := checkNumber(n, t, at()); err != nil {
			return nil, err
		}
	}
	switch tok {
	case json.Delim('{'):
		fields, elem := jsonFields(t)
		m := map[string]any{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, _ := kt.(string)
			if _, twice := m[key]; twice {
				return nil, fmt.Errorf("%s holds the key %s twice", at(), strconv.Quote(key))
			}
			child, exact := fields[key]
			if !exact {
				child = elem
				for _, name := range sortedKeys(fields) {
					if strings.EqualFold(name, key) { // encoding/json's own fold
						return nil, fmt.Errorf("%s holds the key %s, which is %s only by ignoring case", at(), strconv.Quote(key), strconv.Quote(name))
					}
				}
			}
			if m[key], err = strictValue(dec, child, path+"."+key, depth); err != nil {
				return nil, err
			}
		}
		_, err := dec.Token()
		return m, err
	case json.Delim('['):
		_, elem := jsonFields(t)
		a := []any{}
		for i := 0; dec.More(); i++ {
			v, err := strictValue(dec, elem, path+"["+strconv.Itoa(i)+"]", depth)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		_, err := dec.Token()
		return a, err
	}
	return tok, nil
}

// unpairedSurrogate finds the first \u escape inside a JSON string that names a surrogate not in a
// pair: a high one (D800 to DBFF) not followed at once by an escaped low one (DC00 to DFFF), or a low
// one not preceded by a high one. Escapes are read in order, so "\\ud800" (an escaped backslash,
// then text) is none. A malformed escape is the tokenizer's to refuse.
func unpairedSurrogate(data []byte) (at int, esc string, found bool) {
	hex4 := func(i int) (rune, bool) {
		if i+6 > len(data) || data[i] != '\\' || data[i+1] != 'u' {
			return 0, false
		}
		v, err := strconv.ParseUint(string(data[i+2:i+6]), 16, 32)
		return rune(v), err == nil
	}
	inString := false
	for i := 0; i < len(data); i++ {
		switch c := data[i]; {
		case !inString:
			inString = c == '"'
		case c == '"':
			inString = false
		case c == '\\':
			r, ok := hex4(i)
			switch {
			case ok && r >= 0xD800 && r <= 0xDBFF:
				if r2, ok2 := hex4(i + 6); ok2 && r2 >= 0xDC00 && r2 <= 0xDFFF {
					i += 11
					continue
				}
				return i, string(data[i : i+6]), true
			case ok && r >= 0xDC00 && r <= 0xDFFF:
				return i, string(data[i : i+6]), true
			case ok:
				i += 5
			default:
				i++ // the escaped character
			}
		}
	}
	return 0, "", false
}

var jsonUnmarshaler = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()

// jsonFields is how encoding/json decodes into t: the fields of a struct by their JSON names (an
// embedded struct's are its own), and the element type of a map, slice or array. A type that
// decodes itself (json.RawMessage, a time) and an interface have neither: what is under them is
// free-form.
func jsonFields(t reflect.Type) (fields map[string]reflect.Type, elem reflect.Type) {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Implements(jsonUnmarshaler) || reflect.PointerTo(t).Implements(jsonUnmarshaler) {
		return nil, nil
	}
	switch t.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		return nil, t.Elem()
	case reflect.Struct:
		fields = map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			switch {
			case name == "-" || (!f.IsExported() && !f.Anonymous):
			case name == "" && f.Anonymous:
				inner, _ := jsonFields(f.Type)
				for k, v := range inner {
					if _, outer := fields[k]; !outer {
						fields[k] = v
					}
				}
			case name == "":
				fields[f.Name] = f.Type
			default:
				fields[name] = f.Type
			}
		}
	}
	return fields, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
