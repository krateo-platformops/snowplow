package jq

import (
	"context"
	"errors"
	"fmt"

	"github.com/itchyny/gojq"
)

// IterateValues runs a resolve-path ITERATOR query and returns the elements to
// fan out over (#638). The element semantics are EXACTLY the pre-#638 contract
// (plumbing jqutil.ForEach), so every iterator keeps its meaning:
//
//   - the query yields ONE array → its elements;
//   - it yields one null, or no value at all → ErrIteratorNoValue with no
//     elements. jqutil.ForEach errored here too; a RESTAction stage treats it
//     as the C-3 empty plan (the upstream produced nothing — what it
//     dispatched before), the widget resourcesRefsTemplate returns it as an
//     error exactly as before;
//   - anything else — one non-array value, or more than one value (jq's
//     stream form, `.items[]`) — is an ERROR. Pre-#638 that error was logged
//     and the stage dispatched nothing; the caller now fails the stage with it
//     instead. Supporting the stream form is a separate feature.
//
// What changes against jqutil.ForEach is only the ERROR TEXT: a runtime error is
// returned SANITIZED (*RuntimeError — gojq's value preview removed), and a
// parse/compile error is returned as-is (query-derived).
func IterateValues(ctx context.Context, query string, data any, ml gojq.ModuleLoader) ([]any, error) {
	q, err := gojq.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("invalid jq query %q: %w", query, err)
	}
	var opts []gojq.CompilerOption
	if ml != nil {
		opts = append(opts, gojq.WithModuleLoader(ml))
	}
	code, err := gojq.Compile(q, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to compile jq query %q: %w", query, err)
	}
	var vals []any
	iter := code.RunWithContext(ctx, data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if e, isErr := v.(error); isErr {
			return nil, toRuntimeError(e)
		}
		vals = append(vals, v)
	}
	switch len(vals) {
	case 0:
		return nil, ErrIteratorNoValue
	case 1:
		switch one := vals[0].(type) {
		case nil:
			return nil, ErrIteratorNoValue
		case []any:
			return one, nil
		}
		return nil, fmt.Errorf("query %q must return a JSON array; it returned %s", query, gojq.TypeOf(vals[0]))
	}
	return nil, fmt.Errorf("query %q must return a single JSON array; it yielded %d values (the jq stream form is not supported by iterators)", query, len(vals))
}

// ErrIteratorNoValue reports an iterator that yielded a single null or no value
// at all: the upstream data it walks is absent. Data-free by construction.
var ErrIteratorNoValue = errors.New("iterator yielded no array (null or no value)")
