package jqinterest

import (
	"strings"

	"github.com/itchyny/gojq"
)

// The chain model. Each step mirrors resolver code; the citations are to
// go/snowplow at the commit this package was written against.

// UAF is a stage's userAccessFilter, as far as it reads the item.
type UAF struct {
	NamespaceFrom string // "" = ".metadata.namespace" (refilter.go compileNamespaceFrom)
	NameFrom      string // "" = ".metadata.name"      (refilter.go compileNameFrom)
	// NameSpecificVerb: NameFrom is evaluated only for get/update/patch/delete
	// (refilter.go evalSingleMemo, rbac.IsNameSpecificVerb).
	NameSpecificVerb bool
	// ResourcesFrom is evaluated against the resolver dict (refilter.go
	// resolveUAFResources).
	ResourcesFrom string
}

// Stage is one RESTAction spec.api entry.
type Stage struct {
	Name      string
	Path      string   // template: "${ jq }" or literal (setup.go evalJQE)
	Payload   string   // template
	Headers   []string // templates
	DependsOn string
	Iterator  string // plain jq (setup.go jqutil.ForEach, no module loader)
	Filter    *string
	UAF       *UAF
}

// RESTAction is the part of a RESTAction the chain reads.
type RESTAction struct {
	Stages []Stage
	Filter *string // spec.filter (restactions.go)
}

// ResourcesRef is one resourcesRefsTemplate entry.
type ResourcesRef struct {
	Iterator string   // template
	Fields   []string // id, apiVersion, name, namespace, resource templates
}

// Widget is the part of a widget the chain reads.
type Widget struct {
	DataTemplates []string // widgetDataTemplate[].expression
	ResourcesRefs []ResourcesRef
}

// Chain is one list edge: the LIST call made by stage Tracked of RA, consumed
// by the RESTAction cell (Widget == nil) or by a widget cell.
type Chain struct {
	RA      RESTAction
	Tracked string
	Widget  *Widget
}

// MaybeQuery mirrors plumbing jqutil.MaybeQuery: the jq inside the FIRST
// "${ … }" of s (brace-balanced), or ok=false when s is not a template.
func MaybeQuery(s string) (string, bool) {
	start := strings.Index(s, "${")
	if start == -1 {
		return s, false
	}
	start += len("${")
	depth := 1
	for end := start; end < len(s); end++ {
		switch s[end] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return strings.TrimSpace(s[start:end]), true
			}
		}
	}
	return s, false
}

// testRootsHook, when set (tests only), sees the abstract extras roots of a
// widget chain: the stage input, the RESTAction dict, the data source.
var testRootsHook func(pig, dict, ds *abs)

// Analyze derives the interest of one list edge.
func Analyze(c Chain, loader gojq.ModuleLoader) Interest {
	s := newState(loader)
	return run(s, func() { s.chain(c) })
}

func (s *state) chain(c Chain) {
	var tracked *Stage
	others := map[string]bool{}
	for i := range c.RA.Stages {
		if c.RA.Stages[i].Name == c.Tracked {
			tracked = &c.RA.Stages[i]
		} else {
			others[c.RA.Stages[i].Name] = true
		}
	}
	if tracked == nil {
		top("tracked stage %q not in the RESTAction", c.Tracked)
	}
	// #683: the path's applied labelSelector decides membership on labels.
	s.selectorReads(tracked)
	root := &abs{k: aRef, path: Path{}}
	e := newEnv(nil)

	// UAF runs on the RAW envelope before the stage filter (handler.go
	// applyUserAccessFilterOnPig). Per item: the item's type (map/string are
	// evaluated, anything else denied), namespaceFrom and, for name-specific
	// verbs, nameFrom — each stringified whole (jqValueToString); an error
	// denies the item. Survivors keep their values: the list stays a
	// sub-sequence of the raw list, decided by these reads alone (RBAC is a
	// per-cell constant).
	if u := tracked.UAF; u != nil && !mutated(mutUAFExcluded) {
		s.uaf++
		item := &abs{k: aRef, path: Path{{Key: "items"}, {Any: true}}}
		s.consumeKind(item, opType)
		ns := u.NamespaceFrom
		if ns == "" {
			ns = ".metadata.namespace"
		}
		s.consumeWhole(s.eval(mustParse(ns), item, e), opEq)
		if u.NameSpecificVerb && !mutated(mutUAFNameFromExcluded) {
			nm := u.NameFrom
			if nm == "" {
				nm = ".metadata.name"
			}
			s.consumeWhole(s.eval(mustParse(nm), item, e), opEq)
		}
		s.uaf--
	}

	// The stage filter's input (handler.go jsonHandlerCore) is built in this
	// order: pig["extras"] (when there are extras), then pig[<stage>] =
	// envelope, then pig["slice"] (when the dict has a slice). So a stage
	// named "extras" sees its OWN response under .extras, and a stage named
	// "slice" sees the response OR the slice, depending on whether the
	// request is paginated.
	pig := &abs{k: aObj, fields: map[string]*abs{
		"extras": extrasAt(RootStageInput, "extras", Step{Key: "extras"}),
		"slice":  extrasAt(RootStageInput, "slice", Step{Key: "slice"}),
	}}
	pig.fields[tracked.Name] = root
	if tracked.Name == "slice" {
		pig.fields["slice"] = union(root, constOf(ccExtras, "slice"))
	}
	if mutated(mutPigConstWins) && (tracked.Name == "extras" || tracked.Name == "slice") {
		pig.fields[tracked.Name] = constOf(ccExtras, tracked.Name)
	}
	out := root
	if tracked.Filter != nil {
		out = s.eval(mustParse(*tracked.Filter), pig, e)
	}
	// Accumulation into dict[stage] (handler.go jsonHandlerCore merge):
	// - when dict[stage] is unset, the call stores its value;
	// - when dict[stage] is set, the value is appended, and a filter-produced
	//   array is spliced in.
	// dict[stage] can already be set for two reasons:
	// - an iterator fan-out's earlier calls (other edges: constant here);
	// - the request extras, or the slice. resolve.go newResolveRun seeds the
	//   dict with the extras at TOP level, and the extras keys are chosen by
	//   the client, so ANY stage name can collide. The seed is already in the
	//   cell key, so it is a constant.
	// Because of the second reason, the accumulated shape is always possible.
	seed := constOf(ccExtras, "extras-seed")
	if tracked.DependsOn != "" {
		seed = union(seed, constOf(ccOther, "other-calls"))
	}
	acc := &abs{k: aArr, elem: union(seed, out)}
	if tracked.Filter != nil {
		sp := s.iterate(out, true)
		acc.elem = union(acc.elem, stripCU(sp))
		acc.elemCU = acc.elemCU || anyCU(sp) || out.k == aArr && out.elemCU
	}
	dictS := union(out, acc)
	if mutated(mutNoExtrasSeed) && tracked.DependsOn == "" {
		dictS = out
	}
	// The resolver dict (resolve.go newResolveRun): request extras + slice +
	// every stage's output under its name.
	// It IS the RootRADict root: a key that is no stage's name reads the
	// root's own value (navKey), a structured extras source.
	dict := &abs{k: aObj, fields: map[string]*abs{tracked.Name: dictS}, rest: constOf(ccExtras, "dict"),
		nav: &ExtrasSource{Root: RootRADict}}
	for name := range others {
		dict.fields[name] = constOf(ccOther, name)
	}

	// Other stages read the dict in their iterator, path, payload, headers and
	// resourcesFrom; their requests (so their responses) depend on what they
	// read. Their error text can surface as a stage error record, so they are
	// analysed strictly.
	for _, st := range c.RA.Stages {
		if st.Name == tracked.Name || mutated(mutOtherStagesSkip) {
			continue
		}
		in := &abs{k: aObj, fields: dict.fields, rest: dict.rest, nav: dict.nav}
		var tin *abs = in
		if st.DependsOn != "" && st.Iterator != "" {
			it := s.eval(mustParse(st.Iterator), in, e)
			s.consumeWhole(it, opOther) // the iterator's whole output decides the calls
			s.nonTotal("iterator", it)
			tin = s.iterate(it, true)
		}
		s.strict++
		for _, tmpl := range append([]string{st.Path, st.Payload}, st.Headers...) {
			if q, ok := MaybeQuery(tmpl); ok {
				v := s.eval(mustParse(q), tin, e)
				s.consumeWhole(v, opOther)
				s.nonTotal("request template", v)
			}
		}
		if st.UAF != nil && st.UAF.ResourcesFrom != "" {
			v := s.eval(mustParse(st.UAF.ResourcesFrom), in, e)
			s.consumeWhole(v, opOther)
			s.nonTotal("resourcesFrom", v)
		}
		s.strict--
	}

	// spec.filter over the dict (restactions.go); no filter = the dict itself.
	raOut := dict
	if c.RA.Filter != nil {
		raOut = s.eval(mustParse(*c.RA.Filter), dict, e)
	}
	if c.Widget == nil {
		s.consumeWhole(raOut, opOther)
		s.nonTotal("RESTAction output", raOut)
		return
	}

	// Widget data source (widgets/resolve.go): the RA output object, then
	// injectSlice and mergeExtras fill keys the output does not have.
	var ds *abs
	switch {
	case raOut.k == aObj:
		rest := constOf(ccExtras, "extras")
		if raOut.rest != nil {
			rest = union(raOut.rest, rest)
		}
		ds = &abs{k: aObj, fields: raOut.fields, rest: rest, cu: raOut.cu, nav: &ExtrasSource{Root: RootWidgetDS}}
	case raOut.isConst():
		ds = extrasAt(RootWidgetDS, "ds")
	default:
		// Not a known object: observe it whole; the data source is opaque.
		s.consumeWhole(raOut, opOther)
		s.nonTotal("widget data source", raOut)
		ds = absOpaque
	}
	if testRootsHook != nil {
		testRootsHook(pig, dict, ds)
	}
	for _, expr := range c.Widget.DataTemplates {
		if q, ok := MaybeQuery(expr); ok {
			v := s.eval(mustParse(q), ds, e)
			s.consumeWhole(v, opOther)
			s.nonTotal("widgetDataTemplate output", v)
		}
	}
	// resourcesRefsTemplate (resourcesrefstemplate/resolve.go): the iterator
	// output's elements feed the field templates; a field's error MESSAGE is
	// written into the ref (evalJQ), so fields are analysed strictly.
	for _, rr := range c.Widget.ResourcesRefs {
		tin := ds
		if q, ok := MaybeQuery(rr.Iterator); ok && q != "" {
			it := s.eval(mustParse(q), ds, e)
			s.nonTotal("resourcesRefs iterator", it)
			s.consumeShape(it, opOther)
			tin = s.iterate(it, true)
		}
		inc := 1
		if mutated(mutRRTNotStrict) {
			inc = 0
		}
		s.strict += inc
		for _, f := range rr.Fields {
			if q, ok := MaybeQuery(f); ok {
				v := s.eval(mustParse(q), tin, e)
				s.consumeWhole(v, opOther)
				s.nonTotal("resourcesRefs field", v)
			}
		}
		s.strict -= inc
	}
}
