package jq

import (
	"errors"
	"reflect"
	"strings"

	"github.com/itchyny/gojq"
)

// RuntimeError is a jq RUNTIME error with the evaluated data removed (#638).
//
// gojq renders most runtime errors with a preview of the value they failed on —
// `keys cannot be applied to: string ("<first bytes of a Secret value>")`,
// `cannot iterate over: object ({"data":{...)`, `error(<any value>)`. Those
// values are the RESPONSE being filtered: per-identity bodies, ConfigMap and
// Secret data. A resolve-path jq error is logged and written to the stage's
// errorKey, so the raw gojq text would carry response bytes into both.
//
// RuntimeError keeps only what the query author needs and nothing the data
// supplied: the gojq error CLASS (its Go type, e.g. "iteratorError"), the jq
// function or operator name when the class has one (that comes from the query
// text), and the jq TYPE of each offending value (object/array/string/...),
// never the value. A null value carries no data, so the nil-iteration case keeps
// gojq's exact "cannot iterate over: null" wording (IsBenignNilIteration keys on
// it).
//
// Parse and compile errors are NOT runtime errors: they are derived from the
// query text alone (the parse error carries its byte offset — the jq position)
// and are returned unchanged by SanitizeError.
type RuntimeError struct {
	Class string   // gojq error class, e.g. "func0TypeError"
	Name  string   // jq function/operator name, when the class carries one
	Types []string // jq types of the offending value(s), never the values
}

func (e *RuntimeError) Error() string {
	if e.Class == "iteratorError" && len(e.Types) == 1 && e.Types[0] == "null" {
		return "jq runtime error: " + gojqNilIterationMarker
	}
	var b strings.Builder
	b.WriteString("jq runtime error: ")
	b.WriteString(e.Class)
	if e.Name != "" {
		b.WriteString(" in ")
		b.WriteString(e.Name)
	}
	if len(e.Types) > 0 {
		b.WriteString(" (got ")
		b.WriteString(strings.Join(e.Types, ", "))
		b.WriteString(")")
	}
	b.WriteString(" [value withheld]")
	return b.String()
}

// SanitizeError returns err with every evaluated value removed. A parse or
// compile error (query-derived) is returned unchanged; anything else is a
// runtime error and becomes a *RuntimeError. nil stays nil; an already
// sanitized error is returned as-is.
func SanitizeError(err error) error {
	if err == nil {
		return nil
	}
	var re *RuntimeError
	if errors.As(err, &re) {
		return err
	}
	if isQueryDerived(err) {
		return err
	}
	return toRuntimeError(err)
}

// isQueryDerived reports whether err was produced from the query text alone:
// a gojq *ParseError, or the parse/compile wrappers both jqutil.Eval and the
// resolver's CompileJQ put around them (compile runs before any data exists).
func isQueryDerived(err error) bool {
	var pe *gojq.ParseError
	if errors.As(err, &pe) {
		return true
	}
	msg := err.Error()
	return strings.HasPrefix(msg, "invalid jq query ") || strings.HasPrefix(msg, "unable to compile jq query ")
}

func toRuntimeError(err error) *RuntimeError {
	// The class is the innermost gojq error's type.
	inner := err
	for {
		u := errors.Unwrap(inner)
		if u == nil {
			break
		}
		inner = u
	}
	t := reflect.TypeOf(inner)
	class := t.String()
	if i := strings.LastIndex(class, "."); i >= 0 {
		class = class[i+1:]
	}
	re := &RuntimeError{Class: class}
	v := reflect.ValueOf(inner)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return re
	}
	if f := v.FieldByName("name"); f.IsValid() && f.Kind() == reflect.String {
		re.Name = f.String()
	}
	// The offending value(s): gojq names them v (and w/x for arguments), l/r
	// for binary operators. Only their jq TYPE is read — reflect Kind, never
	// the content.
	for _, fn := range []string{"v", "l", "r"} {
		if f := v.FieldByName(fn); f.IsValid() {
			re.Types = append(re.Types, jqTypeOf(f))
		}
	}
	return re
}

// jqTypeOf maps a reflected value to its jq type name without reading it.
func jqTypeOf(f reflect.Value) string {
	for f.Kind() == reflect.Interface {
		if f.IsNil() {
			return "null"
		}
		f = f.Elem()
	}
	switch f.Kind() {
	case reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Pointer:
		if f.IsNil() {
			return "null"
		}
		return "number" // *big.Int is gojq's only pointer value type
	case reflect.Invalid:
		return "null"
	}
	return "value"
}
