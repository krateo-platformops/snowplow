package jqinterest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// Step is one navigation step: an object key, or Any — every element of an
// array / every value of an object (the analysis does not track positions).
type Step struct {
	Key string
	Any bool
}

func (s Step) String() string {
	if s.Any {
		return "[]"
	}
	return "." + strconv.Quote(s.Key)
}

// Path is a sequence of steps from a root (the list item, in an Interest).
type Path []Step

func (p Path) String() string {
	if len(p) == 0 {
		return "."
	}
	var b strings.Builder
	for _, s := range p {
		b.WriteString(s.String())
	}
	return b.String()
}

func (p Path) with(s Step) Path {
	out := make(Path, len(p), len(p)+1)
	copy(out, p)
	return append(out, s)
}

// hasPrefix reports whether q is a prefix of p (Any matches only Any).
func (p Path) hasPrefix(q Path) bool {
	if len(q) > len(p) {
		return false
	}
	for i := range q {
		if p[i] != q[i] {
			return false
		}
	}
	return true
}

// covers reports whether every concrete location p can denote is under some
// location q can denote: q is a prefix of p where an Any step in q matches any
// step of p. Used for exclusions (del(.metadata.managedFields) under a whole
// read) and normalisation.
func (q Path) covers(p Path) bool {
	if len(q) > len(p) {
		return false
	}
	for i := range q {
		if q[i].Any {
			continue
		}
		if p[i].Any || p[i].Key != q[i].Key {
			return false
		}
	}
	return true
}

// Mode is how much of the value at a read path the program can observe.
type Mode uint8

const (
	// ModeKind observes, at every step along the path and at its end, whether
	// the key is present and the JSON kind (null, false, true, number, string,
	// array, object). Enough to decide navigation, `?` suppression, type
	// errors, truthiness and `//` fallbacks.
	ModeKind Mode = iota + 1
	// ModeShape is ModeKind plus, at the end of the path: an array's length,
	// an object's key set, a scalar's value. Enough for length, keys, has,
	// iteration counts.
	ModeShape
	// ModeWhole is ModeKind along the path plus the entire value at its end
	// (minus Excl subtrees).
	ModeWhole
)

func (m Mode) String() string {
	switch m {
	case ModeKind:
		return "kind"
	case ModeShape:
		return "shape"
	case ModeWhole:
		return "whole"
	}
	return "?"
}

// Read is one entry of a read set.
type Read struct {
	Path Path
	Mode Mode
	// Excl are subtrees (relative to Path) removed before observing a Shape or
	// Whole read — del(...) results.
	Excl []Path
}

func (r Read) String() string {
	s := r.Mode.String() + " " + r.Path.String()
	if len(r.Excl) > 0 {
		ex := make([]string, len(r.Excl))
		for i, e := range r.Excl {
			ex[i] = e.String()
		}
		s += " minus{" + strings.Join(ex, ",") + "}"
	}
	return s
}

func (r Read) key() string { return r.String() }

// normalizeReads sorts, de-duplicates and drops entries subsumed by an
// unexcluded whole read of a covering path. Dropping is sound: a whole read of
// q observes everything an entry at or under q observes (its kind walk covers
// the steps under q, the whole value covers shape and kind at the end).
func normalizeReads(in []Read) []Read {
	seen := map[string]bool{}
	var wholes []Path
	for _, r := range in {
		if r.Mode == ModeWhole && len(r.Excl) == 0 {
			wholes = append(wholes, r.Path)
		}
	}
	var out []Read
	for _, r := range in {
		subsumed := false
		for _, w := range wholes {
			if w.covers(r.Path) && !(r.Mode == ModeWhole && len(r.Excl) == 0 && len(w) == len(r.Path) && w.String() == r.Path.String()) {
				subsumed = true
				break
			}
		}
		if subsumed {
			continue
		}
		k := r.key()
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// Project returns the canonical observation of one list item restricted to
// reads. Two items with equal projections drive every analysed program
// through the same navigation, the same errors and the same outputs: that is
// the property the analyzer's soundness rests on and the differential fuzz
// checks.
func Project(item any, reads []Read) []byte {
	var b bytes.Buffer
	for _, r := range reads {
		b.WriteString(r.key())
		b.WriteByte('=')
		projectWalk(&b, item, true, r.Path, r)
		b.WriteByte(';')
	}
	return b.Bytes()
}

func projectWalk(b *bytes.Buffer, v any, present bool, rest Path, r Read) {
	b.WriteString(kindOf(v, present))
	if len(rest) == 0 {
		switch r.Mode {
		case ModeShape:
			b.WriteByte(':')
			writeShape(b, v, r.Excl)
		case ModeWhole:
			b.WriteByte(':')
			writeCanonical(b, applyExcl(v, r.Excl))
		}
		return
	}
	s := rest[0]
	switch t := v.(type) {
	case map[string]any:
		if s.Any {
			keys := sortedKeys(t)
			b.WriteString("{")
			b.WriteString(strconv.Itoa(len(keys)))
			for _, k := range keys {
				b.WriteByte('|')
				b.WriteString(strconv.Quote(k))
				b.WriteByte('>')
				projectWalk(b, t[k], true, rest[1:], r)
			}
			b.WriteString("}")
			return
		}
		cv, ok := t[s.Key]
		b.WriteByte('>')
		projectWalk(b, cv, ok, rest[1:], r)
	case []any:
		if s.Any {
			b.WriteString("[")
			b.WriteString(strconv.Itoa(len(t)))
			for _, e := range t {
				b.WriteByte('|')
				projectWalk(b, e, true, rest[1:], r)
			}
			b.WriteString("]")
		}
		// A key step on an array: the kind already says it errors.
	default:
		// Scalars / null / absent: navigation stops; the kind says what it does.
	}
}

func kindOf(v any, present bool) string {
	if !present {
		return "absent"
	}
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number, *big.Int:
		return "number"
	}
	return fmt.Sprintf("other(%T)", v)
}

func writeShape(b *bytes.Buffer, v any, excl []Path) {
	switch t := applyExcl(v, excl).(type) {
	case map[string]any:
		keys := sortedKeys(t)
		b.WriteString(strconv.Itoa(len(keys)))
		for _, k := range keys {
			b.WriteByte(',')
			b.WriteString(strconv.Quote(k))
		}
	case []any:
		b.WriteString(strconv.Itoa(len(t)))
	default:
		writeCanonical(b, t)
	}
}

// applyExcl returns v with every excluded subtree removed (a copy; v is never
// mutated). An Any step removes the subtree under every element / value.
func applyExcl(v any, excl []Path) any {
	for _, e := range excl {
		v = removePath(v, e)
	}
	return v
}

func removePath(v any, p Path) any {
	if len(p) == 0 {
		return nil
	}
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = x
		}
		if p[0].Any {
			for k, x := range out {
				if len(p) == 1 {
					delete(out, k)
				} else {
					out[k] = removePath(x, p[1:])
				}
			}
			return out
		}
		x, ok := out[p[0].Key]
		if !ok {
			return out
		}
		if len(p) == 1 {
			delete(out, p[0].Key)
		} else {
			out[p[0].Key] = removePath(x, p[1:])
		}
		return out
	case []any:
		if !p[0].Any {
			return t
		}
		if len(p) == 1 {
			return []any{}
		}
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = removePath(x, p[1:])
		}
		return out
	}
	return v
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// writeCanonical writes a deterministic encoding that distinguishes every
// value gojq distinguishes (numbers compare by numeric value, so they are
// normalised through big.Float text).
func writeCanonical(b *bytes.Buffer, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		b.WriteString(strconv.Quote(t))
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		b.WriteByte('{')
		for i, k := range sortedKeys(t) {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(k))
			b.WriteByte(':')
			writeCanonical(b, t[k])
		}
		b.WriteByte('}')
	default:
		// Numbers: the exact Go type matters to gojq output (int vs float
		// formatting), so keep the type in the encoding.
		b.WriteString(fmt.Sprintf("%T(%v)", t, t))
	}
}
