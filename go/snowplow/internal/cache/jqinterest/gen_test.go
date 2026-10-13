package jqinterest

import (
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"
)

// Random Kubernetes-shaped data biased towards what a chain's programs touch:
// the field names its JQ navigates, the string literals it compares with, and
// the names/namespaces the extras carry.

var (
	reChain = regexp.MustCompile(`(?:\.[A-Za-z_][A-Za-z0-9_]*|\["[^"\\]+"\])+`)
	reKey   = regexp.MustCompile(`\.([A-Za-z_][A-Za-z0-9_]*)|\["([^"\\]+)"\]`)
	reStr   = regexp.MustCompile(`"((?:[^"\\]|\\.){0,40})"`)
)

type vocab struct {
	chains [][]string
	keys   []string
	strs   []string
}

func chainVocab(c Chain) vocab {
	var srcs []string
	add := func(s string) {
		if s != "" {
			srcs = append(srcs, s)
		}
	}
	for _, st := range c.RA.Stages {
		if st.Filter != nil {
			add(*st.Filter)
		}
		add(st.Iterator)
		add(st.Path)
		add(st.Payload)
		if st.UAF != nil {
			add(st.UAF.NamespaceFrom)
			add(st.UAF.NameFrom)
		}
	}
	if c.RA.Filter != nil {
		add(*c.RA.Filter)
	}
	if c.Widget != nil {
		for _, d := range c.Widget.DataTemplates {
			add(d)
		}
		for _, r := range c.Widget.ResourcesRefs {
			add(r.Iterator)
			for _, f := range r.Fields {
				add(f)
			}
		}
	}
	seenC, seenK, seenS := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var v vocab
	for _, s := range srcs {
		for _, m := range reChain.FindAllString(s, -1) {
			var ch []string
			for _, km := range reKey.FindAllStringSubmatch(m, -1) {
				k := km[1]
				if k == "" {
					k = km[2]
				}
				ch = append(ch, k)
				if !seenK[k] {
					seenK[k] = true
					v.keys = append(v.keys, k)
				}
			}
			key := strings.Join(ch, ".")
			if len(ch) > 0 && !seenC[key] {
				seenC[key] = true
				v.chains = append(v.chains, ch)
			}
		}
		for _, m := range reStr.FindAllStringSubmatch(s, -1) {
			if !seenS[m[1]] && !strings.Contains(m[1], `\`) {
				seenS[m[1]] = true
				v.strs = append(v.strs, m[1])
			}
		}
	}
	sort.Strings(v.keys)
	sort.Strings(v.strs)
	return v
}

var (
	poolNames = []string{"a", "b", "c", "comp-1"}
	poolNS    = []string{"ns1", "ns2", "krateo-system"}
	poolMisc  = []string{"", "Ready", "True", "False", "open", "resolved", "Error", "v1", "apps/v1", "x/y"}
)

type gen struct {
	r     *rand.Rand
	v     vocab
	fresh int
}

func (g *gen) pick(xs []string) string {
	if len(xs) == 0 {
		return "z"
	}
	return xs[g.r.Intn(len(xs))]
}

func (g *gen) str() string {
	if len(g.v.strs) > 0 && g.r.Intn(3) == 0 {
		return g.pick(g.v.strs)
	}
	switch g.r.Intn(4) {
	case 0:
		return g.pick(poolNames)
	case 1:
		return g.pick(poolNS)
	case 2:
		return g.pick(poolMisc)
	}
	if len(g.v.strs) > 0 {
		return g.pick(g.v.strs)
	}
	return "s"
}

func (g *gen) value(depth int) any {
	n := 9
	if depth > 2 {
		n = 6
	}
	switch g.r.Intn(n) {
	case 0, 1:
		return g.str()
	case 2:
		return float64(g.r.Intn(5) - 1)
	case 3:
		return g.r.Intn(2) == 0
	case 4:
		return nil
	case 5:
		return fmt.Sprintf("%d", g.r.Intn(3))
	case 6, 7:
		m := map[string]any{}
		for i := g.r.Intn(4); i > 0; i-- {
			m[g.pick(g.v.keys)] = g.value(depth + 1)
		}
		return m
	default:
		var a []any
		for i := g.r.Intn(4); i > 0; i-- {
			if g.r.Intn(2) == 0 {
				m := map[string]any{}
				for j := 1 + g.r.Intn(3); j > 0; j-- {
					m[g.pick(g.v.keys)] = g.value(depth + 2)
				}
				a = append(a, m)
			} else {
				a = append(a, g.value(depth+2))
			}
		}
		if a == nil {
			a = []any{}
		}
		return a
	}
}

func (g *gen) item() map[string]any {
	it := map[string]any{
		"apiVersion": "v1",
		"kind":       "Thing",
		"metadata": map[string]any{
			"name":      g.pick(poolNames),
			"namespace": g.pick(poolNS),
			"labels":    map[string]any{"app": g.pick(poolNames)},
		},
	}
	for _, ch := range g.v.chains {
		if g.r.Intn(3) == 0 {
			continue
		}
		start := g.r.Intn(len(ch))
		setPath(it, ch[start:], g.value(len(ch)-start))
	}
	g.fixA1(it)
	return it
}

// fixA1 restores axiom A1 — what every informer-served object satisfies: a
// JSON object whose metadata is an object with string name/namespace and
// string-map labels/annotations. Generated data and mutations must stay
// inside the population the analysis is a theorem about.
func (g *gen) fixA1(it map[string]any) {
	md, ok := it["metadata"].(map[string]any)
	if !ok {
		md = map[string]any{}
		it["metadata"] = md
	}
	for _, k := range []string{"name", "namespace"} {
		if _, ok := md[k].(string); !ok {
			if k == "name" {
				md[k] = g.pick(poolNames)
			} else {
				md[k] = g.pick(poolNS)
			}
		}
	}
	for _, k := range []string{"labels", "annotations"} {
		v, present := md[k]
		if !present {
			continue
		}
		m, ok := v.(map[string]any)
		if !ok {
			delete(md, k)
			continue
		}
		for kk, vv := range m {
			if _, ok := vv.(string); !ok {
				m[kk] = g.str()
			}
		}
	}
}

// setPath writes v at keys, creating objects (and replacing non-objects).
func setPath(m map[string]any, keys []string, v any) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		m[keys[0]] = v
		return
	}
	next, ok := m[keys[0]].(map[string]any)
	if !ok {
		next = map[string]any{}
		m[keys[0]] = next
	}
	setPath(next, keys[1:], v)
}

func deletePath(m map[string]any, keys []string) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		delete(m, keys[0])
		return
	}
	if next, ok := m[keys[0]].(map[string]any); ok {
		deletePath(next, keys[1:])
	}
}

// leafPaths lists object-key paths present in v (arrays are not entered).
func leafPaths(v any, prefix []string, out *[][]string) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	for k, x := range m {
		p := append(append([]string{}, prefix...), k)
		*out = append(*out, p)
		leafPaths(x, p, out)
	}
}

// mutate returns a mutated deep copy of item. preferReads biases the target
// towards the read set (so equal-projection and changed-projection trials are
// both frequent).
func (g *gen) mutate(item map[string]any, reads []Read) map[string]any {
	m := deepCopy(item).(map[string]any)
	if g.r.Intn(3) == 0 {
		// A random descent through the ACTUAL data, into arrays too: reaches
		// places no read path names (which is what an over-narrowing hides).
		var p Path
		var cur any = m
		for depth := 0; depth < 6; depth++ {
			switch c := cur.(type) {
			case map[string]any:
				if len(c) == 0 {
					depth = 6
					continue
				}
				ks := make([]string, 0, len(c))
				for k := range c {
					ks = append(ks, k)
				}
				sort.Strings(ks)
				k := ks[g.r.Intn(len(ks))]
				p = append(p, Step{Key: k})
				cur = c[k]
			case []any:
				if len(c) == 0 {
					depth = 6
					continue
				}
				p = append(p, Step{Any: true})
				cur = c[g.r.Intn(len(c))]
			default:
				depth = 6
				continue
			}
			if g.r.Intn(3) == 0 {
				break
			}
		}
		if len(p) > 0 && g.mutateAt(m, p) {
			g.fixA1(m)
			return m
		}
	}
	if len(reads) > 0 && g.r.Intn(2) == 0 {
		// Walk a read path through the actual data (into arrays too) and
		// tweak what is there, often keeping its kind: the changes a
		// kind-only or shape-only observation cannot see.
		r := reads[g.r.Intn(len(reads))]
		if g.mutateAt(m, r.Path) {
			g.fixA1(m)
			return m
		}
	}
	var target []string
	switch g.r.Intn(3) {
	case 0:
		var ps [][]string
		leafPaths(m, nil, &ps)
		sort.Slice(ps, func(i, j int) bool { return strings.Join(ps[i], ".") < strings.Join(ps[j], ".") })
		if len(ps) > 0 {
			target = ps[g.r.Intn(len(ps))]
		}
	case 1:
		if len(reads) > 0 {
			r := reads[g.r.Intn(len(reads))]
			for _, s := range r.Path {
				if s.Any {
					break
				}
				target = append(target, s.Key)
			}
			if len(target) > 0 && g.r.Intn(3) == 0 {
				target = append(target, g.pick(g.v.keys))
			}
		}
	}
	if len(target) == 0 && len(g.v.chains) > 0 {
		ch := g.v.chains[g.r.Intn(len(g.v.chains))]
		target = ch[g.r.Intn(len(ch)):]
	}
	if len(target) == 0 {
		target = []string{"status", "x"}
	}
	if g.r.Intn(4) == 0 {
		deletePath(m, target)
	} else {
		setPath(m, target, g.value(len(target)))
	}
	g.fixA1(m)
	return m
}

// mutateAt follows path in v (choosing a random element / value at Any
// steps, possibly stopping early at a prefix) and changes the value there.
// Returns false when nothing could be changed.
func (g *gen) mutateAt(v any, path Path) bool {
	stop := len(path)
	if stop > 0 && g.r.Intn(4) == 0 {
		stop = g.r.Intn(stop + 1)
	}
	cur := v
	for i := 0; i < stop; i++ {
		s := path[i]
		last := i == stop-1
		switch c := cur.(type) {
		case map[string]any:
			key := s.Key
			if s.Any {
				if len(c) == 0 {
					return false
				}
				ks := make([]string, 0, len(c))
				for k := range c {
					ks = append(ks, k)
				}
				sort.Strings(ks)
				key = ks[g.r.Intn(len(ks))]
			}
			if last {
				c[key] = g.tweak(c[key])
				return true
			}
			next, ok := c[key]
			if !ok {
				c[key] = g.tweak(nil)
				return true
			}
			cur = next
		case []any:
			if len(c) == 0 {
				return false
			}
			if !s.Any {
				return false
			}
			j := g.r.Intn(len(c))
			if last {
				switch g.r.Intn(6) {
				case 0: // append
					c = append(c, g.value(2))
					return g.replaceArr(v, path[:i], c)
				default:
					c[j] = g.tweak(c[j])
				}
				return true
			}
			cur = c[j]
		default:
			return false
		}
	}
	return false
}

// replaceArr is a no-op helper kept for clarity: appending to a slice held in
// a map needs the parent; mutateAt only appends when the parent is reachable.
func (g *gen) replaceArr(root any, prefix Path, arr []any) bool {
	cur := root
	for i, s := range prefix {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		if i == len(prefix)-1 && !s.Any {
			m[s.Key] = arr
			return true
		}
		if s.Any {
			return false
		}
		cur = m[s.Key]
	}
	return false
}

// tweak returns a changed value: half the time of the SAME kind.
func (g *gen) tweak(v any) any {
	if g.r.Intn(2) == 0 {
		switch x := v.(type) {
		case string:
			for i := 0; i < 4; i++ {
				if s := g.str(); s != x {
					return s
				}
			}
			return x + "'"
		case float64:
			return x + float64(1+g.r.Intn(3))
		case bool:
			return !x
		case []any:
			if len(x) > 0 {
				y := deepCopy(x).([]any)
				y[g.r.Intn(len(y))] = g.tweak(y[g.r.Intn(len(y))])
				return y
			}
		case map[string]any:
			y := deepCopy(x).(map[string]any)
			y[g.pick(g.v.keys)] = g.value(2)
			return y
		}
	}
	return g.value(2)
}

func (g *gen) freshString() string {
	g.fresh++
	return fmt.Sprintf("nonmember-%d-%d", g.fresh, g.r.Intn(1000000))
}
