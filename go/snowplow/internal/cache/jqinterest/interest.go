// Package jqinterest derives, from the JQ a cell is computed with, WHICH PARTS
// of a listed object the cell can observe (#603).
//
// A list dependency today says only "this cell reads the incidents list in
// krateo-system", so every event on every incident marks it. The analysis here
// adds two facts, both computed from the JQ the author wrote — no JQ
// expression is ever forbidden:
//
//  1. Reads — the read set: the item paths whose presence, kind, shape or
//     value the cell's output can depend on. An UPDATE that leaves the
//     projection of the item on the read set unchanged cannot change the
//     cell (Project). Every transition that changes it must mark.
//  2. Membership — when every use of the list goes through
//     select(.<path> == <constant>) before anything else observes it, only
//     the objects whose key equals the constant can affect the cell.
//
// Anything the analysis cannot prove yields All ("reads everything"), which
// is exactly today's marking. The package is pure: it is not wired into
// marking (#603 PR-1); PR-2 consumes it in shadow mode.
//
// SOUNDNESS rests on: the abstract interpretation rules (analyze.go,
// builtins.go), the shape-carrying projection (path.go), the chain model
// (chain.go, which mirrors the resolver glue it cites), axiom A1 (an
// informer-served list item is a Kubernetes object: a JSON object whose
// metadata is an object and whose labels/annotations are string maps), and
// the runtime fallbacks the design (§2.4) puts in PR-2. Each of these degrades
// the edge to All:
//   - a runtime JQ error: a stage filter (raw passthrough), a widget
//     widgetDataTemplate, or a resourcesRefsTemplate ITERATOR. The iterator
//     error is a hard widget error whose status.error can carry value
//     previews, so the fallback must cover that path too;
//   - an apiserver fall-through;
//   - a replayed edge.
//
// Request extras whose key equals a stage name are modelled in the analysis
// itself (chain.go: dict[stage] pre-seeded, then accumulated), so they need
// no runtime rule. The differential fuzz (fuzz_test.go) checks the theorem on
// the portal corpus.
package jqinterest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/itchyny/gojq"
)

// Interest is the analysis result for one list edge of one cell.
type Interest struct {
	// All: the analysis could not prove a read set. The edge must keep
	// today's behaviour (interest 0).
	All bool
	// Why says what made it All (empty otherwise).
	Why string
	// Reads is the item-relative read set (normalised, sorted). Meaningful
	// only when !All. ADD and DELETE are always membership changes and always
	// mark: Reads narrows UPDATEs only.
	Reads []Read
	// Membership, when non-nil, means only objects whose key paths equal the
	// recorded constants can affect the cell.
	Membership *Membership
	// MembershipBlockers lists why no membership key was claimed, when a
	// guard was seen but something else observed non-members (diagnostic,
	// bounded).
	MembershipBlockers []string

	// sites: every key guard that carries an extras source, with the AST
	// node of the constant the program compares with. Tests use it to check
	// the source against the value the program really compares with
	// (TestExtrasSource_EvaluatesToTheComparedValue). Not part of the result.
	sites []extrasSite
}

// extrasSite ties an extras source to the constant's AST node.
type extrasSite struct {
	q   *gojq.Query
	src ExtrasSource
}

// Membership is the proven membership-key shape, in disjunctive normal form:
// an object can affect the cell only if, for SOME disjunct, it satisfies
// EVERY guard in it. One select contributes one disjunct (the AND of its
// leading key conjuncts). Two separate selects contribute two disjuncts: an
// object that passes either one is a member.
type Membership struct {
	Disjuncts [][]KeyGuard
}

// Paths lists every key path in any disjunct (deduplicated, sorted). An
// object whose values on all of these paths are unchanged keeps its
// membership.
func (m *Membership) Paths() []Path {
	seen := map[string]bool{}
	var out []Path
	for _, d := range m.Disjuncts {
		for _, k := range d {
			if s := k.Path.String(); !seen[s] {
				seen[s] = true
				out = append(out, k.Path)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// KeyGuard is one `select(.path == constant)` that admits only the cell's own
// objects.
type KeyGuard struct {
	Path Path
	// ConstClass: literal | extras (already in the cell key) | other_edge
	// (another stage's data; it re-records when that edge fires).
	ConstClass string
	// ConstSource is a printable description of the constant.
	ConstSource string
	// Values, when non-nil, is the complete set of values the constant can
	// take. It is set when every alternative folds to a literal: the stream
	// ("a", "b"), or the object {"app": "x"}. The guard then admits an object
	// whose key equals any of them.
	//
	// When Values is nil in a claimed Membership, Extras is set: the constant
	// is one navigation chain from the request extras (already in the cell
	// key), and PR-2 evaluates Extras on the cell's real root at record time.
	// interest() enforces this: a guard that is neither literal-folded nor a
	// structured extras source (an array literal, another stage's data, a
	// union or an object mixing extras with anything, a value computed from
	// the extras) makes the whole membership unknown. The claim is then
	// dropped and recorded in MembershipBlockers, never narrowed. See
	// Decidable.
	Values []any
	// Extras, when non-nil, is how PR-2 computes an extras constant: the
	// constant is exactly the value (or, for a stream, every value) at
	// Extras.Path of the root Extras.Root (#633). It is set only when the
	// constant is ONE navigation chain from that root. Anything else that
	// involves the extras (a union such as ("zzz", $n), an object such as
	// {app: $n}, a value derived from them such as $n + "-x" or
	// .extras.name // "") leaves it nil, and the guard is not Decidable.
	Extras *ExtrasSource
}

// Decidable reports whether PR-2 can decide this guard for an object: by
// Values, or by evaluating Extras against the cell's real root. Every guard
// of a claimed Membership is Decidable (interest() enforces it, and
// TestKeyGuard_EveryClaimedGuardIsDecidable pins it).
func (k KeyGuard) Decidable() bool {
	return k.Values != nil || k.ConstClass == ccExtras.String()
}

// ExtrasRoot names the value an extras source is navigated from. A source
// never starts at a stage's output, only at the request extras or the slice,
// so each root is a per-cell constant as far as a source can reach.
type ExtrasRoot string

const (
	// RootStageInput is the tracked stage filter's input
	// {"extras": <extras>, <stage>: <response>, "slice": <slice>}
	// (handler.go jsonHandlerCore). A source from it starts at .extras or
	// .slice.
	RootStageInput ExtrasRoot = "stage_input"
	// RootRADict is the resolver dict that spec.filter and the other stages
	// read (resolve.go newResolveRun): the request extras and the slice at top
	// level, plus every stage's output under its name. A source from it never
	// starts at a stage name.
	RootRADict ExtrasRoot = "ra_dict"
	// RootWidgetDS is the widget data source (widgets/resolve.go): the
	// RESTAction output, then the slice and the extras for the keys it does
	// not have. A source from it never starts at a key the output has.
	RootWidgetDS ExtrasRoot = "widget_ds"
)

// ExtrasSource is a structured extras constant: navigate Root along Path.
// Path holds object keys and, when Stream is set, at least one [] step (every
// element of an array, every value of an object).
type ExtrasSource struct {
	Root   ExtrasRoot
	Path   Path
	Stream bool
}

func (x ExtrasSource) String() string { return string(x.Root) + ":" + x.Path.String() }

// Eval computes the constant on the cell's real root, with jq's semantics:
// .k on null is null, a missing key is null, [] yields every array element
// or object value. The result is every value the program compares the key
// with (exactly one for a non-stream source).
//
// An error means jq raises on this root: the program either fails or, under
// a try, compares with a prefix of the stream. PR-2 must then treat the
// guard as undecidable for this cell (the edge reads everything).
func (x ExtrasSource) Eval(root any) ([]any, error) {
	cur := []any{root}
	for _, st := range x.Path {
		var next []any
		for _, v := range cur {
			if st.Any {
				switch t := v.(type) {
				case []any:
					next = append(next, t...)
				case map[string]any:
					keys := make([]string, 0, len(t))
					for k := range t {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					for _, k := range keys {
						next = append(next, t[k])
					}
				default:
					return nil, fmt.Errorf("cannot iterate over %T", v)
				}
				continue
			}
			switch t := v.(type) {
			case nil:
				next = append(next, nil)
			case map[string]any:
				next = append(next, t[st.Key])
			default:
				return nil, fmt.Errorf("cannot index %T with %q", v, st.Key)
			}
		}
		cur = next
	}
	return cur, nil
}

func (k KeyGuard) String() string {
	if k.Values != nil {
		return fmt.Sprintf("%s in %s", k.Path, valuesString(k.Values))
	}
	return fmt.Sprintf("%s == <%s:%s>", k.Path, k.ConstClass, k.ConstSource)
}

func valuesString(vs []any) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		b, _ := json.Marshal(v)
		parts[i] = string(b)
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ", ") + "}"
}

func (in Interest) String() string {
	if in.All {
		return "ALL (" + in.Why + ")"
	}
	parts := make([]string, len(in.Reads))
	for i, r := range in.Reads {
		parts[i] = r.String()
	}
	s := "reads{" + strings.Join(parts, "; ") + "}"
	if in.Membership != nil {
		s += " member{" + in.Membership.String() + "}"
	}
	return s
}

// ReadsEverything reports whether the read set observes the whole item.
func (in Interest) ReadsEverything() bool {
	if in.All {
		return true
	}
	for _, r := range in.Reads {
		if len(r.Path) == 0 && r.Mode == ModeWhole && len(r.Excl) == 0 {
			return true
		}
	}
	return false
}

// run executes an analysis body, turning an unprovable construct into All.
func run(s *state, body func()) (interest Interest) {
	defer func() {
		if r := recover(); r != nil {
			var t topErr
			if err, ok := r.(error); ok && errors.As(err, &t) {
				interest = Interest{All: true, Why: t.why}
				return
			}
			if t, ok := r.(topErr); ok {
				interest = Interest{All: true, Why: t.why}
				return
			}
			panic(r)
		}
	}()
	body()
	return s.interest()
}

// interest converts the envelope-relative reads into item-relative reads.
func (s *state) interest() Interest {
	var items []Read
	for _, r := range s.reads {
		conv, ok := toItemReads(r)
		if !ok {
			return Interest{All: true, Why: "list_level_read " + r.String()}
		}
		items = append(items, conv...)
	}
	out := Interest{Reads: normalizeReads(items)}
	if len(s.guards) > 0 {
		var undecidable []string
		for _, d := range s.guards {
			for _, k := range d {
				if !k.Decidable() {
					undecidable = append(undecidable, "undecidable key constant: "+k.String())
				}
			}
		}
		switch {
		case len(s.violations) > 0:
			out.MembershipBlockers = append([]string(nil), s.violations...)
		case len(undecidable) > 0:
			// One undecidable disjunct makes membership unknown: an
			// object it admits could not be told apart from a non-member.
			out.MembershipBlockers = undecidable
		default:
			out.Membership = &Membership{Disjuncts: dedupDisjuncts(s.guards)}
		}
	}
	return out
}

func (m *Membership) String() string {
	ds := make([]string, len(m.Disjuncts))
	for i, d := range m.Disjuncts {
		ks := make([]string, len(d))
		for j, k := range d {
			ks[j] = k.String()
		}
		ds[i] = "(" + strings.Join(ks, " AND ") + ")"
	}
	return strings.Join(ds, " OR ")
}

// dedupDisjuncts normalises each disjunct and drops repeats (the same select
// used twice is one disjunct). Under mutKeyFlatAND every guard of every
// select is merged into ONE conjunction. That is the over-narrowing that
// finding 4 of the #603 PR-1 review describes.
func dedupDisjuncts(ds [][]KeyGuard) [][]KeyGuard {
	if mutated(mutKeyFlatAND) {
		var flat []KeyGuard
		for _, d := range ds {
			flat = append(flat, d...)
		}
		ds = [][]KeyGuard{flat}
	}
	seen := map[string]bool{}
	var out [][]KeyGuard
	var keys []string
	for _, d := range ds {
		g := dedupGuards(d)
		parts := make([]string, len(g))
		for i, k := range g {
			parts[i] = k.String()
		}
		key := strings.Join(parts, " AND ")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, g)
		keys = append(keys, key)
	}
	idx := make([]int, len(out))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(i, j int) bool { return keys[idx[i]] < keys[idx[j]] })
	sorted := make([][]KeyGuard, len(out))
	for i, j := range idx {
		sorted[i] = out[j]
	}
	return sorted
}

func dedupGuards(gs []KeyGuard) []KeyGuard {
	seen := map[string]bool{}
	var out []KeyGuard
	for _, g := range gs {
		k := g.String()
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// toItemReads maps one read of the LIST ENVELOPE ({apiVersion, kind, items};
// E6: nothing else is in an informer-served envelope) onto item reads. The
// envelope's apiVersion and kind are constant for a GVR; the items array's
// kind and length are membership (ADD/DELETE always mark).
func toItemReads(r Read) ([]Read, bool) {
	p := r.Path
	if len(p) == 0 {
		if r.Mode != ModeWhole {
			return nil, true
		}
		return []Read{{Path: Path{}, Mode: ModeWhole, Excl: itemExcl(r.Excl, true)}}, true
	}
	first := p[0]
	if first.Any {
		// `.[]` over the envelope: its values are apiVersion, kind (constant)
		// and items.
		rr := r
		rr.Path = append(Path{{Key: "items"}}, p[1:]...)
		return toItemReads(rr)
	}
	switch first.Key {
	case "apiVersion", "kind":
		return nil, true
	case "items":
	default:
		return nil, false
	}
	if len(p) == 1 {
		if r.Mode != ModeWhole {
			return nil, true
		}
		return []Read{{Path: Path{}, Mode: ModeWhole, Excl: itemExcl(r.Excl, false)}}, true
	}
	if !p[1].Any {
		// `.items.foo` is an error on an array: only the array's kind matters.
		return nil, true
	}
	return []Read{{Path: append(Path{}, p[2:]...), Mode: r.Mode, Excl: r.Excl}}, true
}

// itemExcl keeps the exclusions that apply to every item.
func itemExcl(excl []Path, fromEnvelope bool) []Path {
	var out []Path
	for _, e := range excl {
		q := e
		if fromEnvelope {
			if len(q) == 0 || q[0].Any || q[0].Key != "items" {
				continue
			}
			q = q[1:]
		}
		if len(q) >= 2 && q[0].Any {
			out = append(out, q[1:])
		}
	}
	return out
}

// AnalyzeProgram analyses ONE jq program whose input is the list envelope
// itself (the shape a stage filter sees under its stage key, unwrapped). It
// is the unit the tests drive; Analyze handles a whole resolve chain.
func AnalyzeProgram(program string, loader gojq.ModuleLoader) Interest {
	s := newState(loader)
	return run(s, func() {
		q := mustParse(program)
		out := s.eval(q, &abs{k: aRef, path: Path{}}, newEnv(nil))
		s.consumeWhole(out, opOther)
		s.nonTotal("output", out)
	})
}

func newState(loader gojq.ModuleLoader) *state {
	return &state{listPath: Path{{Key: "items"}}, loader: loader}
}

// testParseHook, when set (tests only), sees every program the analysis
// parses, with the AST it analyses.
var testParseHook func(src string, q *gojq.Query)

func mustParse(src string) *gojq.Query {
	q, err := gojq.Parse(src)
	if err != nil {
		top("parse error: %v", err)
	}
	if testParseHook != nil {
		testParseHook(src, q)
	}
	return q
}
