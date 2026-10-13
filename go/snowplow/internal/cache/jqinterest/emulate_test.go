package jqinterest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/itchyny/gojq"
	"github.com/krateo-platformops/plumbing/jqutil"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// The emulator runs a Chain CONCRETELY — real gojq / real plumbing jqutil on
// real data — through the same glue the resolver applies (each step cites the
// resolver code it mirrors). The differential fuzz compares its output for two
// lists the analyzer calls equivalent.

// emuEnv is everything about one cell that is not the tracked list.
type emuEnv struct {
	extras   map[string]any
	perPage  int
	page     int
	identity uint32 // RBAC oracle seed: one identity's narrowed view
	// fanOutFirst: for an iterator-driven tracked stage, whether the tracked
	// call's output is accumulated before or after another call's.
	fanOutFirst bool
}

// emuOutcome is the cell's observable result.
type emuOutcome struct {
	// fallback: a runtime path that degrades the edge to "reads everything"
	// at runtime (design §2.4: stage filter error → raw passthrough). A cell
	// computed this way is never narrowed, so it is not compared.
	fallback string
	// err: the resolve failed (the cell would not be cached with this body).
	err  string
	body []byte
}

func (o emuOutcome) String() string {
	switch {
	case o.fallback != "":
		return "FALLBACK(" + o.fallback + ")"
	case o.err != "":
		return "ERROR(" + o.err + ")"
	}
	return string(o.body)
}

var (
	codeMu    sync.Mutex
	codeCache = map[string]*gojq.Code{}
)

func compileCached(q string, withLoader bool) (*gojq.Code, error) {
	key := fmt.Sprintf("%v|%s", withLoader, q)
	codeMu.Lock()
	defer codeMu.Unlock()
	if c, ok := codeCache[key]; ok {
		return c, nil
	}
	pq, err := gojq.Parse(q)
	if err != nil {
		return nil, err
	}
	var opts []gojq.CompilerOption
	if withLoader {
		opts = append(opts, gojq.WithModuleLoader(jqsupport.ModuleLoader()))
	}
	c, err := gojq.Compile(pq, opts...)
	if err != nil {
		return nil, err
	}
	codeCache[key] = c
	return c, nil
}

var errMulti = errors.New("multi-yield")

// evalValue mirrors restactions/api/jqvalue.go EvalValue (+ CompileJQ).
func evalValue(q string, data any) (any, bool, error) {
	code, err := compileCached(q, true)
	if err != nil {
		return nil, false, err
	}
	iter := code.RunWithContext(context.Background(), data)
	var first any
	n := 0
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if e, isErr := v.(error); isErr {
			return nil, false, e
		}
		if n == 0 {
			first = v
		}
		n++
		if n > 1 {
			return nil, false, errMulti
		}
	}
	if n == 0 {
		return nil, false, nil
	}
	return first, true, nil
}

// jqString mirrors refilter.go jqValueToString.
func jqString(q string, data any) (string, error) {
	v, ok, err := evalValue(q, data)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	switch s := v.(type) {
	case string:
		return s, nil
	case nil:
		return "", nil
	}
	b, _ := json.Marshal(v)
	return string(b), nil
}

func deepCopy(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out any
	d := json.NewDecoder(bytes.NewReader(b))
	if err := d.Decode(&out); err != nil {
		panic(err)
	}
	return out
}

// rbacAllows is the identity's RBAC oracle (refilter.go EvaluateRBAC stands
// for a pure function of the identity and (namespace, name)).
func rbacAllows(identity uint32, ns, name string) bool {
	h := fnv.New32a()
	fmt.Fprintf(h, "%d|%s|%s", identity, ns, name)
	return h.Sum32()%4 != 0
}

// synthResponse is another stage's response: a deterministic function of the
// request it was asked, so a request that changes changes the response.
func synthResponse(path, payload string) map[string]any {
	h := fnv.New32a()
	fmt.Fprintf(h, "%s|%s", path, payload)
	tag := fmt.Sprintf("r%08x", h.Sum32())
	return map[string]any{
		"apiVersion": "v1", "kind": "List",
		"items": []any{map[string]any{
			"apiVersion": "v1", "kind": "Thing",
			"metadata": map[string]any{"name": tag, "namespace": "ns1"},
			"status":   map[string]any{"state": tag, "phase": "Ready"},
		}},
		"metadata": map[string]any{"name": tag},
		"status":   map[string]any{"state": tag},
	}
}

// accumulate mirrors handler.go jsonHandlerCore's merge into the dict.
func accumulate(dict map[string]any, key string, tmp any, filtered bool) {
	got, ok := dict[key]
	if !ok {
		dict[key] = tmp
		return
	}
	switch existing := got.(type) {
	case []any:
		if arr, isArr := tmp.([]any); isArr {
			if filtered && len(arr) > 0 {
				dict[key] = append(existing, arr...)
			} else if !filtered {
				dict[key] = append(existing, tmp)
			}
		} else {
			dict[key] = append(existing, tmp)
		}
	default:
		if arr, isArr := tmp.([]any); isArr && filtered {
			dict[key] = append([]any{got}, arr...)
		} else {
			dict[key] = []any{got, tmp}
		}
	}
}

// runStageCall mirrors jsonHandlerCore for one call: UAF on the raw envelope,
// then the stage filter over {extras?, <stage>: envelope, slice?}.
func runStageCall(st *Stage, envelope map[string]any, env emuEnv, dict map[string]any, tracked bool) (any, string) {
	pig := map[string]any{}
	if len(env.extras) > 0 {
		pig["extras"] = env.extras
	}
	pig[st.Name] = envelope
	if sl, ok := dict["slice"]; ok {
		pig["slice"] = sl
	}
	if st.UAF != nil && tracked {
		items, _ := envelope["items"].([]any)
		kept := []any{}
		ns := st.UAF.NamespaceFrom
		if ns == "" {
			ns = ".metadata.namespace"
		}
		nm := st.UAF.NameFrom
		if nm == "" {
			nm = ".metadata.name"
		}
		for _, it := range items {
			switch it.(type) {
			case map[string]any, string:
			default:
				continue
			}
			n, err := jqString(ns, it)
			if err != nil {
				continue
			}
			name := ""
			if st.UAF.NameSpecificVerb {
				name, err = jqString(nm, it)
				if err != nil {
					continue
				}
			}
			if rbacAllows(env.identity, n, name) {
				kept = append(kept, it)
			}
		}
		envelope["items"] = kept
	}
	if st.Filter == nil {
		return envelope, ""
	}
	v, ok, err := evalValue(*st.Filter, pig)
	switch {
	case errors.Is(err, errMulti):
		return nil, "stage filter multi-yield"
	case err != nil:
		if tracked {
			return nil, "FALLBACK stage filter error"
		}
		return envelope, ""
	case !ok:
		return nil, "stage filter empty"
	}
	return v, ""
}

func renderTemplate(tmpl string, data any) (string, error) {
	q, ok := MaybeQuery(tmpl)
	if !ok {
		return q, nil
	}
	return jqutil.Eval(context.Background(), jqutil.EvalOptions{
		Query: q, Unquote: true, Data: data, ModuleLoader: jqsupport.ModuleLoader(),
	})
}

// emulate runs the chain on the tracked list items.
func emulate(c Chain, items []any, env emuEnv) emuOutcome {
	dict := map[string]any{}
	for k, v := range env.extras {
		dict[k] = deepCopy(v)
	}
	if env.perPage > 0 && env.page > 0 {
		dict["slice"] = map[string]any{"page": env.page, "perPage": env.perPage, "offset": (env.page - 1) * env.perPage}
	}
	var tracked *Stage
	for i := range c.RA.Stages {
		if c.RA.Stages[i].Name == c.Tracked {
			tracked = &c.RA.Stages[i]
		}
	}
	envelope := map[string]any{"apiVersion": "v1", "kind": "List", "items": deepCopy(items)}
	trackedCall := func() string {
		v, e := runStageCall(tracked, envelope, env, dict, true)
		if e != "" {
			return e
		}
		accumulate(dict, tracked.Name, v, tracked.Filter != nil)
		return ""
	}
	otherCall := func(st *Stage, path, payload string) string {
		v, e := runStageCall(st, synthResponse(path, payload), env, dict, false)
		if e != "" {
			return e
		}
		accumulate(dict, st.Name, v, st.Filter != nil)
		return ""
	}
	// The tracked stage first (worst case: every other stage sees it), with
	// one other call around it when it is iterator-driven.
	if tracked.DependsOn != "" && !env.fanOutFirst {
		if e := otherCall(tracked, "/other-call", ""); e != "" {
			return failOutcome(e)
		}
	}
	if e := trackedCall(); e != "" {
		return failOutcome(e)
	}
	if tracked.DependsOn != "" && env.fanOutFirst {
		if e := otherCall(tracked, "/other-call", ""); e != "" {
			return failOutcome(e)
		}
	}
	// Other stages, in spec order, each seeing the dict so far.
	var errRecords []string
	for i := range c.RA.Stages {
		st := &c.RA.Stages[i]
		if st.Name == tracked.Name {
			continue
		}
		elems := []any{dict}
		if st.DependsOn != "" && st.Iterator != "" {
			elems = nil
			err := jqutil.ForEach(context.Background(), jqutil.EvalOptions{Query: st.Iterator, Unquote: true, Data: dict},
				func(a any) error { elems = append(elems, a); return nil })
			if err != nil {
				errRecords = append(errRecords, "iterator:"+err.Error())
			}
		}
		for _, el := range elems {
			path, err := renderTemplate(st.Path, el)
			if err != nil {
				errRecords = append(errRecords, "path:"+err.Error())
				continue
			}
			payload, err := renderTemplate(st.Payload, el)
			if err != nil {
				errRecords = append(errRecords, "payload:"+err.Error())
				continue
			}
			for _, h := range st.Headers {
				hv, err := renderTemplate(h, el)
				if err != nil {
					errRecords = append(errRecords, "header:"+err.Error())
				}
				payload += "|" + hv
			}
			if st.UAF != nil && st.UAF.ResourcesFrom != "" {
				v, _, err := evalValue(st.UAF.ResourcesFrom, dict)
				b, _ := json.Marshal(v)
				payload += "|res:" + string(b)
				if err != nil {
					payload += "|reserr:" + err.Error()
				}
			}
			if e := otherCall(st, path, payload); e != "" {
				errRecords = append(errRecords, e)
			}
		}
	}
	if len(errRecords) > 0 {
		dict["__stage_errors"] = strings.Join(errRecords, "\n")
	}
	var raw string
	if c.RA.Filter != nil {
		s, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{Query: *c.RA.Filter, Data: dict, ModuleLoader: jqsupport.ModuleLoader()})
		if err != nil {
			return emuOutcome{err: "ra filter: " + err.Error()}
		}
		raw = s
	} else {
		b, err := json.Marshal(dict)
		if err != nil {
			return emuOutcome{err: err.Error()}
		}
		raw = string(b)
	}
	if c.Widget == nil {
		return emuOutcome{body: []byte(raw)}
	}
	var ds map[string]any
	if err := json.Unmarshal([]byte(raw), &ds); err != nil || ds == nil {
		return emuOutcome{err: "widget data source is not an object"}
	}
	if env.perPage > 0 && env.page > 0 {
		if _, ok := ds["slice"]; !ok {
			ds["slice"] = map[string]any{"page": env.page, "perPage": env.perPage, "offset": (env.page - 1) * env.perPage}
		}
	}
	for k, v := range env.extras {
		if _, ok := ds[k]; !ok {
			ds[k] = deepCopy(v)
		}
	}
	out := map[string]any{}
	var data []any
	for _, expr := range c.Widget.DataTemplates {
		q, ok := MaybeQuery(expr)
		if !ok {
			data = append(data, expr)
			continue
		}
		s, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{Query: q, Data: ds, ModuleLoader: jqsupport.ModuleLoader()})
		if err != nil {
			return emuOutcome{err: "widgetDataTemplate: " + err.Error()}
		}
		data = append(data, jqutil.InferType(s))
	}
	out["data"] = data
	var refs []any
	for _, rr := range c.Widget.ResourcesRefs {
		var elems []any
		if q, ok := MaybeQuery(rr.Iterator); ok && q != "" {
			// An iterator error is a HARD widget error. The widget's
			// status.error can carry value previews, so the cell depends on
			// whatever the error message embeds. PR-2's runtime rule
			// "widget JQ error → interest 0" covers it. A body built this way
			// is never narrowed, so it is a fallback here and is not compared
			// (it was swallowed before the #603 PR-1 review).
			if err := jqutil.ForEach(context.Background(), jqutil.EvalOptions{Query: q, Unquote: true, Data: ds},
				func(a any) error { elems = append(elems, a); return nil }); err != nil {
				return emuOutcome{fallback: "resourcesRefsTemplate iterator error"}
			}
		} else {
			elems = []any{ds}
		}
		for _, el := range elems {
			var ref []string
			for _, f := range rr.Fields {
				q, ok := MaybeQuery(f)
				if !ok {
					ref = append(ref, q)
					continue
				}
				s, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{Query: q, Unquote: true, Data: el, ModuleLoader: jqsupport.ModuleLoader()})
				if err != nil {
					s = err.Error()
				}
				ref = append(ref, s)
			}
			refs = append(refs, ref)
		}
	}
	out["refs"] = refs
	var b bytes.Buffer
	writeCanonical(&b, normalizeNumbersDeep(out))
	return emuOutcome{body: b.Bytes()}
}

func failOutcome(e string) emuOutcome {
	if strings.HasPrefix(e, "FALLBACK ") {
		return emuOutcome{fallback: strings.TrimPrefix(e, "FALLBACK ")}
	}
	return emuOutcome{err: e}
}

// normalizeNumbersDeep makes json.Number / int / float comparable in the
// canonical encoding of the widget outcome (InferType yields mixed types).
func normalizeNumbersDeep(v any) any {
	b, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}
