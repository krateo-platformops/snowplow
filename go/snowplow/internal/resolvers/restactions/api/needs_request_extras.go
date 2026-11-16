package api

// needs_request_extras.go — #1477 (generalises #113): does a resolve of this
// RESTAction, carrying these extras, need request extras it does not carry?
//
// A stage's endpointRef.name may be a jq template (#113, hub-spoke): the
// resolver renders it against the resolve dict (evalEndpointRef). The dict is
// the extras the resolve carries plus what the resolver writes itself (each
// stage's output under its name, each errorKey, the pagination block:
// ReservedDictKeys, which no extras may name, #1116). A template that needs a
// request extra the resolve does not carry renders a name no customer's request
// would select: the endpoint read misses and the stage truncates. No customer
// key can match the cell such a resolve produces, because a customer request
// that renders the template carries the key, and the cache keys fold the
// extras.
//
// So a background resolve (a seed, the discovery walk, the content prewarm)
// that would render such a template is useless work, and possibly a truncated
// body: its caller skips it. The decision reads only the declared templates and
// the extras the resolve carries; never a resource, RESTAction or widget name.

import (
	"github.com/krateo-platformops/plumbing/jqutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// NeedsRequestExtras reports whether some api step of items has a templated
// endpointRef.name that needs a request extra the resolve does not carry.
//
// A dict key the template reads (jqsupport.RootKeys; a read under `//` counts,
// see root_keys.go) is satisfied when it is:
//   - carried (a key of carried, the extras the resolve will carry);
//   - a stage's name, and that stage's OUTPUT needs no uncarried extra
//     (#1477 review B2): the stage's own dict reads — its endpointRef name,
//     its iterator, its userAccessFilter resourcesFrom, and, for a stage
//     without an iterator, its path, payload and headers (an iterator stage
//     renders those from its elements, which its iterator yields) — are
//     satisfied by the same rule, transitively, and its filter reads no
//     uncarried extra (its input holds the extras under `extras`, beside the
//     stage's response and the pagination block, handler.go jsonHandlerCore:
//     a read `.extras.k` needs k carried; a use of `.extras` the analysis
//     cannot narrow to constant keys needs extras). A stage read through a
//     cycle is not satisfied;
//   - an errorKey, and EVERY stage writing it needs no uncarried extra (what
//     lands there depends on whether those stages fail, B2');
//   - the pagination block.
//
// Any other key is a request extra the resolve does not carry. A template whose
// reads cannot be determined (RootKeys ok=false: it does not parse, or reads
// its input as a whole or through a function) needs extras: rendered with
// none, it is what #113 skips.
//
// A nil step and a nil endpointRef (the stage dials the caller's own
// clientconfig or the ServiceAccount) need nothing; nor does a literal name,
// nor a template that reads no key at all.
func NeedsRequestExtras(items []*templates.API, carried map[string]any) bool {
	n := needsAnalysis{items: items, carried: carried}
	for _, step := range items {
		if step == nil || step.EndpointRef == nil {
			continue
		}
		if n.templateNeeds(step.EndpointRef.Name, true) {
			return true
		}
	}
	return false
}

type needsAnalysis struct {
	items    []*templates.API
	carried  map[string]any
	reserved map[string]ReservedKey
	stages   map[string]*templates.API
	// errorKeyStages: each effective errorKey and the stages that write it.
	errorKeyStages map[string][]string
	// state per stage name: 1 = being decided (a cycle reads as needing), 2 =
	// needs nothing, 3 = needs an uncarried extra.
	state map[string]int
}

func (n *needsAnalysis) init() {
	if n.reserved != nil {
		return
	}
	n.reserved = ReservedDictKeys(n.items)
	n.stages = map[string]*templates.API{}
	n.errorKeyStages = map[string][]string{}
	for _, it := range n.items {
		if it != nil {
			n.stages[it.Name] = it
			ek := defaultErrorKey
			if it.ErrorKey != nil {
				ek = *it.ErrorKey
			}
			n.errorKeyStages[ek] = append(n.errorKeyStages[ek], it.Name)
		}
	}
	n.state = map[string]int{}
}

// templateNeeds reports whether the template s needs an uncarried extra. A
// `${...}` template is analysed through its query; when wrapped is false, s
// is a bare jq query (an iterator, a resourcesFrom). A string that is not a
// template reads nothing.
func (n *needsAnalysis) templateNeeds(s string, wrapped bool) bool {
	q := s
	if wrapped {
		var isTemplate bool
		if q, isTemplate = jqutil.MaybeQuery(s); !isTemplate {
			return false
		}
	} else if inner, isTemplate := jqutil.MaybeQuery(s); isTemplate {
		q = inner
	}
	if q == "" {
		return false
	}
	keys, ok := jqsupport.RootKeys(q)
	if !ok {
		return true
	}
	n.init()
	for _, k := range keys {
		if n.keyNeeds(k) {
			return true
		}
	}
	return false
}

// keyNeeds reports whether the dict key k, read by a template, depends on an
// uncarried extra (NeedsRequestExtras lists the rule).
func (n *needsAnalysis) keyNeeds(k string) bool {
	if _, has := n.carried[k]; has {
		return false
	}
	if _, isStage := n.stages[k]; isStage {
		return n.stageNeeds(k)
	}
	if r, written := n.reserved[k]; written {
		if r.Owner != ReservedByErrorKey {
			return false // the pagination block
		}
		for _, st := range n.errorKeyStages[k] {
			if n.stageNeeds(st) {
				return true
			}
		}
		return false
	}
	return true
}

// filterNeeds reports whether the stage filter f reads an uncarried extra. Its
// input is {extras: the extras, <stage>: the response, slice}; a stage named
// "extras" keeps its own response under that key (jsonHandlerCore).
func (n *needsAnalysis) filterNeeds(stage, f string) bool {
	q := f
	if inner, isTemplate := jqutil.MaybeQuery(f); isTemplate {
		q = inner
	}
	if q == "" {
		return false
	}
	keys, sub, subOK, ok := jqsupport.RootKeysUnder(q, "extras")
	if !ok {
		return true
	}
	if stage == "extras" {
		return false // `.extras` is the stage's own response
	}
	readsExtras := false
	for _, k := range keys {
		if k == "extras" {
			readsExtras = true
		}
	}
	if !readsExtras {
		return false
	}
	if !subOK {
		return true
	}
	for _, k := range sub {
		if _, has := n.carried[k]; !has {
			return true
		}
	}
	return false
}

// stageNeeds reports whether the output of the stage named name depends on an
// uncarried extra, through its own dict reads.
func (n *needsAnalysis) stageNeeds(name string) bool {
	switch n.state[name] {
	case 1, 3:
		return true
	case 2:
		return false
	}
	n.state[name] = 1
	st := n.stages[name]
	needs := false
	if st.EndpointRef != nil && n.templateNeeds(st.EndpointRef.Name, true) {
		needs = true
	}
	iterator := ""
	if st.DependsOn != nil && st.DependsOn.Iterator != nil {
		iterator = *st.DependsOn.Iterator
	}
	if !needs && iterator != "" && n.templateNeeds(iterator, false) {
		needs = true
	}
	if !needs && st.UserAccessFilter != nil && st.UserAccessFilter.ResourcesFrom != "" &&
		n.templateNeeds(st.UserAccessFilter.ResourcesFrom, false) {
		needs = true
	}
	if !needs && st.Filter != nil && n.filterNeeds(st.Name, *st.Filter) {
		needs = true
	}
	if !needs && iterator == "" {
		if n.templateNeeds(st.Path, true) || st.Payload != nil && n.templateNeeds(*st.Payload, true) {
			needs = true
		}
		for _, h := range st.Headers {
			if !needs && n.templateNeeds(h, true) {
				needs = true
			}
		}
	}
	if needs {
		n.state[name] = 3
	} else {
		n.state[name] = 2
	}
	return needs
}
