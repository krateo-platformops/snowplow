package api

import (
	"context"

	"github.com/itchyny/gojq"
)

// uafExpr is a userAccessFilter per-item expression (NamespaceFrom /
// NameFrom), compiled once per refilter (#121 1b) and evaluated once per
// item.
//
// When the expression is a plain field path — "." or ".a.b.c" with only
// bare identifiers — path holds its keys and eval reads the item directly,
// without running gojq. During a login storm the per-item gojq run was 15.9%
// of all CPU (bench run 12), and each run allocates a fresh gojq env, so the
// direct read takes both the compute and the allocations off the 50K-item
// loop. The portal's expressions (.metadata.namespace, .metadata.name) are
// all plain paths.
//
// The direct read answers only where its result provably equals gojq's:
// walking a map or a null, ending on a string or a null. Every other case
// (a non-map on the way, a non-string leaf, a cancelled context) runs the
// compiled gojq code, so errors, number rendering and fail-closed outcomes
// stay exactly gojq's. A nil *uafExpr is a compile failure; the callers
// deny on it, as they did on a nil *gojq.Code.
type uafExpr struct {
	code  *gojq.Code
	plain bool     // the expression is a plain field path
	path  []string // its keys; empty for "."
}

// compileUAFExpr compiles expr once and recognises a plain field path.
// A compile error returns nil (fail-closed at the caller).
func compileUAFExpr(expr string, ml gojq.ModuleLoader) *uafExpr {
	code, err := CompileJQ(expr, ml)
	if err != nil {
		return nil
	}
	path, plain := plainFieldPath(expr)
	return &uafExpr{code: code, plain: plain, path: path}
}

// eval returns the expression's value on item as evalJQStringCompiled does:
// the same string, the same error.
func (e *uafExpr) eval(ctx context.Context, item any) (string, error) {
	// gojq answers a cancelled context with ctx.Err(); the direct read
	// leaves that case to it.
	if e.plain && ctx.Err() == nil {
		if s, ok := plainPathString(e.path, item); ok {
			return s, nil
		}
	}
	return evalJQStringCompiled(ctx, e.code, item)
}

// plainPathString reads path from v with gojq's semantics for .key on the
// two receivers it handles: an object yields the key's value (null when
// absent) and null yields null. It answers ok only when the result is a
// string or null, which jqValueToString maps to the string and to "".
// Anything else returns ok=false and the caller runs gojq: indexing a
// non-object is a gojq error, and a non-string leaf is rendered by gojq's
// value path.
func plainPathString(path []string, v any) (string, bool) {
	for _, k := range path {
		switch m := v.(type) {
		case map[string]any:
			v = m[k]
		case nil:
			// .k on null is null.
		default:
			return "", false
		}
	}
	switch s := v.(type) {
	case string:
		return s, true
	case nil:
		return "", true
	}
	return "", false
}

// plainFieldPath recognises "." and ".a.b.c" (each key an ASCII identifier,
// no spaces, quotes, brackets, '?', pipes or functions) and returns the
// keys. It is deliberately narrow: the text must match that grammar AND
// gojq must parse it to exactly that shape — the identity, or one index
// term followed only by plain index suffixes with the same keys. Anything
// else is not plain and keeps the gojq path.
func plainFieldPath(expr string) ([]string, bool) {
	keys, ok := plainPathKeys(expr)
	if !ok {
		return nil, false
	}
	q, err := gojq.Parse(expr)
	if err != nil || !parsedAsPlainPath(q, keys) {
		return nil, false
	}
	return keys, true
}

// plainPathKeys is the textual half of plainFieldPath.
func plainPathKeys(expr string) ([]string, bool) {
	if expr == "." {
		return []string{}, true
	}
	if len(expr) < 2 || expr[0] != '.' {
		return nil, false
	}
	var keys []string
	start := 1
	for i := 1; i <= len(expr); i++ {
		if i < len(expr) && expr[i] != '.' {
			continue
		}
		key := expr[start:i]
		if !isASCIIIdent(key) {
			return nil, false
		}
		keys = append(keys, key)
		start = i + 1
	}
	return keys, true
}

func isASCIIIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_', 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z':
		case i > 0 && '0' <= c && c <= '9':
		default:
			return false
		}
	}
	return true
}

// parsedAsPlainPath is the structural half of plainFieldPath: q is a single
// term with no operators, definitions or imports, and that term is the
// identity (keys empty) or .keys[0] followed by .keys[1:] as plain suffixes.
func parsedAsPlainPath(q *gojq.Query, keys []string) bool {
	if q == nil || q.Meta != nil || len(q.Imports) > 0 || len(q.FuncDefs) > 0 ||
		q.Left != nil || q.Right != nil || len(q.Patterns) > 0 || q.Op != 0 || q.Term == nil {
		return false
	}
	t := q.Term
	if t.Func != nil || t.Object != nil || t.Array != nil || t.Number != "" || t.Unary != nil ||
		t.Format != "" || t.Str != nil || t.If != nil || t.Try != nil || t.Reduce != nil ||
		t.Foreach != nil || t.Label != nil || t.Break != "" || t.Query != nil {
		return false
	}
	if len(keys) == 0 {
		return t.Type == gojq.TermTypeIdentity && t.Index == nil && len(t.SuffixList) == 0
	}
	if t.Type != gojq.TermTypeIndex || !isPlainIndex(t.Index, keys[0]) || len(t.SuffixList) != len(keys)-1 {
		return false
	}
	for i, s := range t.SuffixList {
		if s == nil || s.Iter || s.Optional || !isPlainIndex(s.Index, keys[i+1]) {
			return false
		}
	}
	return true
}

func isPlainIndex(ix *gojq.Index, key string) bool {
	return ix != nil && ix.Name == key && ix.Str == nil && ix.Start == nil && ix.End == nil && !ix.IsSlice
}
