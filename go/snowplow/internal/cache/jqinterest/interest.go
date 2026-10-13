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
// the runtime fallbacks the design (§2.4) puts in PR-2: a runtime JQ error, an
// apiserver fall-through and a replayed edge all degrade the edge to All. The
// differential fuzz (fuzz_test.go) checks the theorem on the portal corpus.
package jqinterest

import (
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
}

// Membership is the proven membership-key shape.
type Membership struct {
	Keys []KeyGuard
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
}

func (k KeyGuard) String() string {
	return fmt.Sprintf("%s == <%s:%s>", k.Path, k.ConstClass, k.ConstSource)
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
		ks := make([]string, len(in.Membership.Keys))
		for i, k := range in.Membership.Keys {
			ks[i] = k.String()
		}
		s += " member{" + strings.Join(ks, " AND ") + "}"
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
		if len(s.violations) == 0 {
			out.Membership = &Membership{Keys: dedupGuards(s.guards)}
		} else {
			out.MembershipBlockers = append([]string(nil), s.violations...)
		}
	}
	return out
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

func mustParse(src string) *gojq.Query {
	q, err := gojq.Parse(src)
	if err != nil {
		top("parse error: %v", err)
	}
	return q
}
