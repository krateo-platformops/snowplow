// jqvalue.go — Ship A (resolver-path rebuild, 0.30.137).
//
// EvalValue is a snowplow-local thin wrapper over github.com/itchyny/gojq
// that returns gojq's result as a Go `any` value DIRECTLY — no
// encode-to-string, no json.Unmarshal-back. It eliminates the D3+D4 jq
// string round-trip identified in docs/ship-a-jq-roundtrip-design.md §0.
//
// It deliberately does NOT patch github.com/krateo-platformops/plumbing/jqutil
// (that is an upstream dependency). It REPLICATES jqutil.Eval's control flow
// (parse / compile / runtime-error / zero-yield / single / multi-yield)
// exactly as the design's §3.4.1 specifies, but separates the five outcomes
// onto the (value, ok, err) channels so each call site can route them.
//
// Ship A scope note: compile-caching of gojq.Parse/gojq.Compile is OUT of
// scope (deferred to Ship A.2) — EvalValue calls Parse + Compile inline on
// every call, exactly as jqutil.Eval does today.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/itchyny/gojq"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// jqCompileCount counts CompileJQ invocations (every gojq.Parse+Compile pair
// snowplow's refilter path performs). It is the falsifier seam for #121 1b's
// C3 "compile-once-per-filter" assertion: a test resets it, drives an N-item
// refilterSlice through ONE NamespaceFrom filter, and asserts the delta == 1
// (NOT N). A RED run against the pre-1b per-item-recompile code would show
// delta == N. Prod-inert: a single monotonic atomic add per compile, never
// read outside the test hook.
var jqCompileCount atomic.Int64

// JQCompileCountForTest returns the current CompileJQ invocation count. Test
// seam only (see jqCompileCount) — used by the #121 1b C3 falsifier to prove
// compile-count == 1 across N items.
func JQCompileCountForTest() int64 { return jqCompileCount.Load() }

// ErrMultiYield is the package-level sentinel returned by EvalValue when a
// jq query yields more than one value. It is matchable via errors.Is.
//
// The wrapper collapses zero-yield and multi-yield onto the ok/err channels;
// it does NOT decide stage-fail vs skip — that is the call site's job
// (design §3.4). Zero-yield is (nil, false, nil); multi-yield is
// (nil, false, ErrMultiYield); parse/compile/runtime errors are
// (nil, false, err) with err != ErrMultiYield.
var ErrMultiYield = errors.New("jq query yielded more than one value, expected exactly one")

// EvalValue runs query against data and returns gojq's result VALUE
// directly — no encode-to-string, no json.Unmarshal-back.
//
// Outcome contract (design §1 — locks the §3.4 truth tables):
//
//	gojq outcome                  EvalValue returns
//	-----------------------------------------------------------------
//	parse error                   (nil,  false, err)
//	compile error                 (nil,  false, err)
//	runtime error-value yielded    (nil,  false, err)   matches jqutil.go:46-48
//	zero values (jq `empty`)       (nil,  false, nil)   ok=false, NO error
//	exactly one value              (v,    true,  nil)
//	more than one value            (nil,  false, ErrMultiYield)
//
// The returned value is whatever gojq yields — documented gojq result types:
// nil, bool, int, float64, *big.Int, string, []any, map[string]any.
//
// ALIASING (design §4 / AC-A.6): the returned value CAN alias sub-trees of
// `data` — jq's identity filter `.` and field access `.foo` yield input
// sub-objects by reference. EvalValue performs no defensive copy. Callers
// MUST treat the result as read-only and MUST NOT place it in a shared cache
// unless `data` is already per-call-private. All three Ship A migrated
// callers satisfy this (see the AC-A.6 proof in jqvalue_test.go).
func EvalValue(ctx context.Context, query string, data any, ml gojq.ModuleLoader) (value any, ok bool, err error) {
	code, cerr := CompileJQ(query, ml)
	if cerr != nil {
		return nil, false, cerr
	}
	return EvalValueCompiled(ctx, code, data)
}

// CompileJQ parses + compiles `query` into a reusable *gojq.Code, returning
// the SAME wrapped parse/compile errors EvalValue historically returned
// inline (design §1 — the parse/compile rows of the outcome contract). It is
// the compile HALF of EvalValue split out so a caller that evaluates ONE
// fixed query against MANY inputs (the refilter per-item RBAC NamespaceFrom
// loop, #121 1b) compiles ONCE and reuses the code across items, instead of
// re-Parse+Compiling per item.
//
// The compiled *gojq.Code is safe for concurrent RunWithContext calls (gojq
// contract: Code is immutable after Compile; the mutable state lives in the
// per-Run iterator). ModuleLoader option is appended only when non-nil,
// byte-identical to the pre-split inline path.
func CompileJQ(query string, ml gojq.ModuleLoader) (*gojq.Code, error) {
	jqCompileCount.Add(1) // #121 1b C3 falsifier seam (see jqCompileCount).
	// Parse — the same call jqutil makes at jqutil.go:23.
	q, perr := gojq.Parse(query)
	if perr != nil {
		return nil, fmt.Errorf("invalid jq query %q: %w", query, perr)
	}

	// Compile — jqutil.go:28-38. ModuleLoader option only when non-nil,
	// matching jqutil's conditional CompilerOption append.
	comopts := []gojq.CompilerOption{}
	if ml != nil {
		comopts = append(comopts, gojq.WithModuleLoader(ml))
	}
	code, cerr := gojq.Compile(q, comopts...)
	if cerr != nil {
		return nil, fmt.Errorf("unable to compile jq query %q: %w", query, cerr)
	}
	return code, nil
}

// EvalValueCompiled is the RUN half of EvalValue: it runs an ALREADY-compiled
// *gojq.Code against `data` and applies the identical five-outcome contract
// (zero / single / multi / runtime-error) EvalValue documents. Split out for
// #121 1b so a fixed-query loop compiles once (CompileJQ) and calls this per
// item — the parse+compile cost is paid once, not O(items).
//
// Behaviour is byte-identical to the inline body EvalValue used before the
// split: the same aliasing caveat (§4 / AC-A.6) applies — the returned value
// can alias sub-trees of `data`; callers must treat it read-only.
func EvalValueCompiled(ctx context.Context, code *gojq.Code, data any) (value any, ok bool, err error) {
	// Run + drain the iterator. jqutil.go:40-52 stops on the first
	// error-typed yielded value and returns it as err; EvalValue does the
	// same, then additionally distinguishes zero / single / multi yield.
	iter := code.RunWithContext(ctx, data)
	var first any
	count := 0
	for {
		v, more := iter.Next()
		if !more {
			break
		}
		// A yielded error-value is a jq runtime error — matches
		// jqutil.go:46-48 (return "", err).
		//
		// #638 — SANITIZED: gojq renders a preview of the failing VALUE (the
		// response being filtered) into most runtime errors; every caller logs
		// or surfaces this error (stage filter → errorKey, UAF refilter Warn).
		// Only the error class and the jq types survive.
		if rerr, isErr := v.(error); isErr {
			return nil, false, jqsupport.SanitizeError(rerr)
		}
		if count == 0 {
			first = v
		}
		count++
		if count > 1 {
			// Multi-yield: stop draining — the cardinality is already
			// known and the surplus values are discarded, exactly as a
			// caller routing on ErrMultiYield would discard them.
			return nil, false, ErrMultiYield
		}
	}

	if count == 0 {
		// Zero-yield (jq `empty`): ok=false, NO error.
		return nil, false, nil
	}
	// Exactly one value.
	return first, true, nil
}

// encodeValueCompact returns the compact JSON serialisation of a single gojq
// result value. It is used ONLY by evalJQString's cold non-string-single-value
// branch (design §3.4.4) to stay byte-identical with the pre-Ship-A
// trimJSONString path, which returned the JSON serialisation of a non-string
// result verbatim.
//
// This is a cold, rare branch — evalJQString callers expect identity-string
// expressions (.metadata.name, .). Correctness over speed: encoding/json is
// fine here. The §3.1 number caveat (gojq int vs json.Unmarshal float64) does
// not apply on the encode direction — json.Marshal of int(5) and float64(5)
// both yield "5".
func encodeValueCompact(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		// json.Marshal of a gojq result value (nil/bool/number/string/
		// []any/map[string]any) does not fail in practice; if it somehow
		// does, fall back to Go's default rendering rather than panic.
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
