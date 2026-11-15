//go:build js && wasm

package fakeapiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// strictJSON is the one reading of a snapshot file. It parses data once and returns it as plain
// JSON values (map[string]any, []any, string, json.Number, bool, nil), refusing what would make two
// readers of the file disagree:
//
//   - an object that holds a key twice, anywhere in the file: encoding/json and JSON.parse keep the
//     last, a reader that stops at the first does not;
//   - a key that encoding/json would take for a field of t only by ignoring case ("Resources", or
//     "reſources" with a long s, for `resources`): JSON.parse, and agentiko-sandbox's validator
//     with it, read keys exactly.
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
	v, err := strictValue(dec, t, "")
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("data after the JSON value")
	}
	return v, nil
}

func strictValue(dec *json.Decoder, t reflect.Type, path string) (any, error) {
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
			if m[key], err = strictValue(dec, child, path+"."+key); err != nil {
				return nil, err
			}
		}
		_, err := dec.Token()
		return m, err
	case json.Delim('['):
		_, elem := jsonFields(t)
		a := []any{}
		for i := 0; dec.More(); i++ {
			v, err := strictValue(dec, elem, path+"["+strconv.Itoa(i)+"]")
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
