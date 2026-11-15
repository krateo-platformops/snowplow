//go:build js && wasm

package fakeapiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// The two bounds of a snapshot file, held here and nowhere else. agentiko-sandbox's two parsers
// hold the same numbers.
const (
	// maxDepth is the deepest nesting read: containers (objects and arrays alike) counted from the
	// file's top level, the top-level one at depth 1. A file nested to maxDepth is read; one level
	// more is refused.
	maxDepth = 1000
	// maxSafeInteger is the largest magnitude of an integer read, 2^53: up to it every integer is
	// exact as an IEEE double, which is how JavaScript, and encoding/json into an untyped value,
	// hold a number; beyond it they round, silently.
	maxSafeInteger = 1 << 53
)

// strictJSON is the one reading of a snapshot file. It parses data once and returns it as plain
// JSON values (map[string]any, []any, string, json.Number, bool, nil), refusing what would make two
// readers of the file disagree:
//
//   - an object that holds a key twice, anywhere in the file: encoding/json and JSON.parse keep the
//     last, a reader that stops at the first does not;
//   - a key that encoding/json would take for a field of t only by ignoring case ("Resources", or
//     "reſources" with a long s, for `resources`): JSON.parse, and agentiko-sandbox's validator
//     with it, read keys exactly. The fold is encoding/json's own: simple, one code point for one;
//   - nesting deeper than maxDepth;
//   - a number that would not be read as written (checkNumber): an integer beyond maxSafeInteger in
//     magnitude, a number that is not finite as a double, and a fraction or an exponent where t
//     holds an integer.
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
//   - an integer has no fraction and no exponent (an optional minus and digits): it is refused
//     beyond maxSafeInteger in magnitude (9007199254740992 is read, 9007199254740993 is not);
//   - any other literal is a float, and only has to be finite as an IEEE double: 1e20,
//     9007199254740993.0 and 1.5e300 are read, 1e400 is not;
//   - where t is a Go integer, a float is refused here, by name, and not by the decoder after.
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
	if m, err := strconv.ParseUint(strings.TrimPrefix(lit, "-"), 10, 64); err != nil || m > maxSafeInteger {
		return fmt.Errorf("%s is the integer %s, beyond %d in magnitude", at, lit, uint64(maxSafeInteger))
	}
	return nil
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
