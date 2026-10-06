// path_parse.go — Ship 0.5 / 0.30.223 — pure-string parser for apiserver
// paths. Relocated from the pre-v6 CRD-watch file (deleted under v6).
//
// The functions here have NO informer side-effects and NO apiserver
// hops; they live in cache because that is the only package where they
// are consumed (phase1_walk.go's lazy-register hook + the new
// discovery_lookup.go + the #279 C3 empty-fan skeleton dep record).

package cache

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ExtractAPIServerGroupFromTemplatedPath extracts the static apiserver
// GROUP from a (possibly JQ-templated) apiserver path. Unlike
// ParseAPIServerPathToGVR — which rejects any path containing `${` —
// this tolerates templated version/namespace/resource segments because
// the GROUP segment is always static:
//
//	/apis/<group>/<version>/...        -> <group>   (named group)
//	/apis/<group>/${.v}/namespaces/... -> <group>   (templated version OK)
//	/api/v1/...                        -> ""        (core group, ignored)
//
// Returns ("", false) for core-group paths, external endpoints, paths
// whose group segment itself is templated (`/apis/${...}/...`), or any
// non-apiserver shape. A templated GROUP segment is deliberately
// rejected — we cannot know the group statically, and admitting a
// `${...}` literal as a group would corrupt the navigation-discovered
// group set.
func ExtractAPIServerGroupFromTemplatedPath(path string) (string, bool) {
	// Strip a leading `${ "..." }` wrapper if the whole path is one JQ
	// string expression — take the first quoted apiserver-looking
	// fragment. We only need the /apis/<group> prefix to survive.
	if i := strings.Index(path, "/apis/"); i >= 0 {
		rest := path[i+len("/apis/"):]
		// The group is everything up to the next '/'.
		slash := strings.IndexByte(rest, '/')
		if slash <= 0 {
			return "", false
		}
		group := rest[:slash]
		// Reject a templated group segment — cannot key the set on it.
		if group == "" || strings.Contains(group, "${") || strings.Contains(group, "\"") {
			return "", false
		}
		return group, true
	}
	return "", false
}

// IsTemplatedAPIServerPath reports whether path is a templated
// apiserver path (contains a `${...}` segment). Convenience predicate
// for callers that want to distinguish "static-resolvable GVR" from
// "templated, need group-only extraction".
func IsTemplatedAPIServerPath(path string) bool {
	return strings.Contains(path, "${")
}

// skeletonSentinel marks a path segment that was JQ-TEMPLATED (a variable
// value). It contains no '/' (so it stays a single segment when the path is
// split) and no '$'/'"' (so it is never mistaken for a live template or a
// leftover JQ quote by the static-segment check).
const skeletonSentinel = "\x00tpl\x00"

// ParseAPIServerListDepSkeleton reconstructs the STATIC apiserver LIST-dep
// coordinate from a (JQ-templated) stage path — the #279 C3 empty-fan fix.
//
// WHY (TRACED to resolve.go collapseOrFanoutPlan): when an iterator stage fans
// over ZERO items, createRequestOptions returns no request options, so the
// per-call edge-3 recorder (resolve.go's `for i := range tmp` loop, which parses
// each RESOLVED call.Path via ParseAPIServerPathToDep) never runs and the fanned
// GVR's (gvr, ns, "*") LIST edge is never recorded. The warm cell is then
// invalidated by nothing on a later CR ADD of that GVR — a stale-negative. But
// the TARGET GVR is STATIC in the path TEMPLATE even with zero items, so the
// LIST edge can still be recorded from the skeleton.
//
// STATIC REQUIREMENT (pm-freshness #279 condition 1 — honest residual): GROUP is
// always static; #288 shows VERSION and/or RESOURCE can ALSO be templated
// (`/apis/<group>/${.v}/...` or a fully-interpolated resource). This requires
// BOTH version AND resource static; a templated version OR resource returns
// ok=false — deliberately DECLINING rather than recording a wrong/underdetermined
// coordinate. That residual (LARGER than "only a templated GVR segment") falls
// back to the #285 seed re-walk fence.
//
// namespace: a static namespace segment is returned verbatim; a TEMPLATED
// namespace on a NAMESPACED resource DECLINES (ok=false → #285 fence) rather than
// guess a cluster-wide (gvr,"","*") super-scope (#279 ns-scope hardening — the
// wrong scope would feed the #239 dirty-mark fan-out at 50K×1000). ns="" is
// returned ONLY for a genuinely cluster-scoped path (no /namespaces/ segment),
// where "" is its TRUE scope. name is never returned — always a LIST-scope edge.
//
// SCOPE (#279 condition 3): this cures the empty fan for stages whose fan is over
// apiCall.Path's OWN GVR (the dominant list-then-get-each). A CROSS-GVR iterator
// (iterate widgets → GET secrets per widget) is NOT cured by THIS edge — the
// invalidation that matters there is the SOURCE stage's own LIST edge.
//
// GVR-PARITY (#279 condition 2): on a concrete instance of the same path the
// (group,version,resource) returned here MUST equal ParseAPIServerPathToDep's —
// asserted by the #279 GVR-parity golden.
func ParseAPIServerListDepSkeleton(path string) (gvr schema.GroupVersionResource, namespace string, ok bool) {
	skel := skeletonizeTemplatedPath(path)
	if skel == "" {
		return schema.GroupVersionResource{}, "", false
	}
	if i := strings.IndexByte(skel, '?'); i >= 0 {
		skel = skel[:i]
	}
	skel = strings.TrimRight(skel, "/")

	// A segment is "templated" (non-static) if it is the sentinel, empty, or
	// still carries live JQ syntax (a leftover `${` or `"` from an unhandled
	// shape) — in all cases we cannot treat it as a static coordinate.
	templated := func(seg string) bool {
		return seg == "" || strings.Contains(seg, skeletonSentinel) ||
			strings.Contains(seg, "${") || strings.Contains(seg, "\"")
	}

	switch {
	case strings.HasPrefix(skel, "/apis/"):
		parts := strings.Split(strings.TrimPrefix(skel, "/apis/"), "/")
		if len(parts) < 3 {
			return schema.GroupVersionResource{}, "", false
		}
		group, version := parts[0], parts[1]
		if templated(group) || templated(version) {
			return schema.GroupVersionResource{}, "", false
		}
		if parts[2] == "namespaces" {
			// namespaced: <g>/<v>/namespaces/<ns>/<resource>[/...]. A MISSING
			// resource segment means a trailing templated resource was dropped by
			// skeletonization → DECLINE (never misread "namespaces" as a resource).
			if len(parts) < 5 {
				return schema.GroupVersionResource{}, "", false
			}
			resource := parts[4]
			if templated(resource) {
				return schema.GroupVersionResource{}, "", false
			}
			ns := parts[3]
			if templated(ns) {
				// #279 ns-scope hardening: a TEMPLATED namespace on a NAMESPACED
				// resource is UNDETERMINED. DECLINE (→ #285 seed re-walk fence)
				// rather than guess a cluster-wide (gvr, "", "*") super-scope — the
				// wrong scope would feed the #239 dirty-mark fan-out at 50K×1000.
				return schema.GroupVersionResource{}, "", false
			}
			return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, ns, true
		}
		resource := parts[2]
		if templated(resource) {
			return schema.GroupVersionResource{}, "", false
		}
		return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, "", true

	case strings.HasPrefix(skel, "/api/"):
		// Core group (Group=="").
		parts := strings.Split(strings.TrimPrefix(skel, "/api/"), "/")
		if len(parts) < 2 {
			return schema.GroupVersionResource{}, "", false
		}
		version := parts[0]
		if templated(version) {
			return schema.GroupVersionResource{}, "", false
		}
		if parts[1] == "namespaces" {
			if len(parts) < 4 {
				return schema.GroupVersionResource{}, "", false
			}
			resource := parts[3]
			if templated(resource) {
				return schema.GroupVersionResource{}, "", false
			}
			ns := parts[2]
			if templated(ns) {
				// #279 ns-scope hardening (symmetric with the grouped branch): a
				// templated namespace on a core namespaced resource DECLINES.
				return schema.GroupVersionResource{}, "", false
			}
			return schema.GroupVersionResource{Version: version, Resource: resource}, ns, true
		}
		resource := parts[1]
		if templated(resource) {
			return schema.GroupVersionResource{}, "", false
		}
		return schema.GroupVersionResource{Version: version, Resource: resource}, "", true
	}
	return schema.GroupVersionResource{}, "", false
}

// skeletonizeTemplatedPath renders a (JQ-templated) path to a static skeleton in
// which every templated sub-expression is replaced by skeletonSentinel, so a
// plain '/' split distinguishes static segments from templated ones. Two shapes:
//
//   - fully-wrapped `${ "lit" + .v + "lit" ... }` (the RESTAction author idiom):
//     join the double-quoted string literals with the sentinel — a (variable)
//     sub-expression sat between each consecutive literal pair.
//   - embedded `/apis/g/v/namespaces/${ .ns }/r`: replace each `${...}` run.
//
// Returns "" when there is nothing parseable (no literals in a wrapped expr, or
// an unterminated template).
func skeletonizeTemplatedPath(path string) string {
	p := strings.TrimSpace(path)
	if p == "" {
		return ""
	}
	if strings.HasPrefix(p, "${") && strings.HasSuffix(p, "}") {
		lits := extractStringLiterals(p[2 : len(p)-1])
		if len(lits) == 0 {
			return ""
		}
		return strings.Join(lits, skeletonSentinel)
	}
	return replaceTemplateRuns(p)
}

// extractStringLiterals returns the contents of each double-quoted string
// literal in s, in order (handling backslash-escaped characters).
func extractStringLiterals(s string) []string {
	var out []string
	var b strings.Builder
	inStr := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inStr {
			if c == '\\' && i+1 < len(s) {
				b.WriteByte(s[i+1])
				i++
				continue
			}
			if c == '"' {
				out = append(out, b.String())
				b.Reset()
				inStr = false
				continue
			}
			b.WriteByte(c)
			continue
		}
		if c == '"' {
			inStr = true
		}
	}
	return out
}

// replaceTemplateRuns replaces each `${...}` run in p with skeletonSentinel.
// An unterminated `${` yields "" (caller declines).
func replaceTemplateRuns(p string) string {
	var b strings.Builder
	for {
		i := strings.Index(p, "${")
		if i < 0 {
			b.WriteString(p)
			return b.String()
		}
		b.WriteString(p[:i])
		b.WriteString(skeletonSentinel)
		j := strings.IndexByte(p[i:], '}')
		if j < 0 {
			return "" // unterminated template — nothing safely parseable
		}
		p = p[i+j+1:]
	}
}

// ReadSetSkeleton is the READ-SET sibling of ParseAPIServerListDepSkeleton:
// same skeletonization, DIFFERENT namespace policy, because the two consumers
// are answering different questions about the same path (#504).
//
// ParseAPIServerListDepSkeleton records a dep EDGE, and for that consumer a
// templated namespace must DECLINE: guessing a cluster-wide (gvr,"","*")
// super-scope would feed the #239 dirty-mark fan-out at 50K×1000, so a wrong
// scope is far worse than no edge (#279 ns-scope hardening).
//
// A READ-SET answers "what will resolving this RESTAction read", for
// core-provider to pre-generate RBAC BEFORE the first /call — at which point no
// composition instance exists, so a templated namespace is not an anomaly, it is
// the NORMAL case. Declining there is what made GET /rbac answer 422 for every
// CompositionDefinition carrying spec.apiRef (#504). So this returns the GVR with
// nsUnknown set, and lets the caller decide what to grant, rather than guessing
// or refusing.
//
// WHAT IT STILL REFUSES, unchanged from its sibling and deliberately: GROUP,
// VERSION and RESOURCE must be STATIC in the TEMPLATE. A templated resource is
// not determinable from the path at all, and the read-set's own rule is that a
// partial set under-grants, so it must fail loud rather than enumerate a guess.
// This is also what distinguishes a genuine bare-discovery stage (/apis/<g>/<v>,
// two segments, no resource) from a stage whose RESOURCE segment collapsed away
// (/apis/<g>/<v>/ — the render looks like discovery; the template was not).
//
// NAME is returned three-state, because the verb depends on it and the rendered
// path cannot carry the distinction (#504 ruling 2):
//   - nameKind=nameAbsent    — no name segment: a collection read -> list
//   - nameKind=nameStatic    — a literal name: a single-object read -> get
//   - nameKind=nameTemplated — an interpolated name: still a single-object read
//     -> get. Rendering it would collapse to "" and the
//     verb would come out `list`, which does NOT
//     authorize a by-name GET (200 + a minted Role +
//     403 at the first /call — an under-grant).
//
// ReadSetCoord is what a read-set needs to know about one stage path.
//
// OK=false has two importantly different causes, and the caller must be able to
// tell them apart (#504 ruling 3): a genuine BARE DISCOVERY template
// (/apis/<g>/<v> with no resource segment at all) is legitimate and contributes
// zero rows, while a template whose RESOURCE segment was interpolated and
// collapsed away renders IDENTICALLY and must fail loud. ResourceTemplated is
// that discriminator — and it can only be answered from the template, which is
// why the rendered path is not consulted anywhere in here.
type ReadSetCoord struct {
	GVR               schema.GroupVersionResource
	Namespace         string
	NamespaceUnknown  bool
	Name              NameKind
	OK                bool
	ResourceTemplated bool
}

func ReadSetSkeleton(path string) ReadSetCoord {
	// #515 gate F1 — ResourceTemplated MUST NOT fire on a literal path.
	// skeletonizeTemplatedPath leaves a literal unchanged, and the templated()
	// predicate below counts an EMPTY segment as non-static — so a literal
	// trailing slash ("/apis/apps/v1/", no jq anywhere) skeletonized to the same
	// string as `${ "/apis/apps/v1/" + .kind }` and looked templated. Both are
	// OK=false, which is right, but flagging the literal one as a COLLAPSED
	// RESOURCE turned a 200 into a 422 for a path that contains no template at
	// all. The template and the render genuinely cannot be told apart from the
	// skeleton alone here; what distinguishes them is whether the author wrote a
	// template, which only the raw path can say.
	hasTemplate := strings.Contains(path, "${")
	skel := skeletonizeTemplatedPath(path)
	if skel == "" {
		return ReadSetCoord{}
	}
	if i := strings.IndexByte(skel, '?'); i >= 0 {
		skel = skel[:i]
	}
	// NOT TrimRight'd: a trailing empty segment is the signature of a collapsed
	// trailing NAME, and trimming it is exactly how the rendered path loses the
	// get-vs-list distinction. The sibling trims because it never returns a name.
	templated := func(seg string) bool {
		return seg == "" || strings.Contains(seg, skeletonSentinel) ||
			strings.Contains(seg, "${") || strings.Contains(seg, "\"")
	}
	// nameAt classifies the name segment at index i of parts.
	nameAt := func(parts []string, i int) NameKind {
		if i >= len(parts) {
			return nameAbsent
		}
		if templated(parts[i]) {
			return nameTemplated
		}
		return nameStatic
	}

	switch {
	case strings.HasPrefix(skel, "/apis/"):
		parts := strings.Split(strings.TrimPrefix(skel, "/apis/"), "/")
		if len(parts) < 3 {
			return ReadSetCoord{}
		}
		group, version := parts[0], parts[1]
		if templated(group) || templated(version) {
			return ReadSetCoord{}
		}
		if parts[2] == "namespaces" {
			if len(parts) < 5 {
				return ReadSetCoord{}
			}
			resource := parts[4]
			if templated(resource) {
				return ReadSetCoord{ResourceTemplated: hasTemplate}
			}
			ns := parts[3]
			g := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
			if templated(ns) {
				return ReadSetCoord{GVR: g, NamespaceUnknown: true, Name: nameAt(parts, 5), OK: true}
			}
			return ReadSetCoord{GVR: g, Namespace: ns, Name: nameAt(parts, 5), OK: true}
		}
		resource := parts[2]
		if templated(resource) {
			return ReadSetCoord{ResourceTemplated: hasTemplate}
		}
		return ReadSetCoord{GVR: schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, Name: nameAt(parts, 3), OK: true}

	case strings.HasPrefix(skel, "/api/"):
		parts := strings.Split(strings.TrimPrefix(skel, "/api/"), "/")
		if len(parts) < 2 {
			return ReadSetCoord{}
		}
		version := parts[0]
		if templated(version) {
			return ReadSetCoord{}
		}
		if parts[1] == "namespaces" {
			// `namespaces` is BOTH the namespace-scoping marker and a core
			// resource in its own right, and which one it is depends on how many
			// segments follow:
			//   /api/v1/namespaces                 -> LIST the namespaces resource
			//   /api/v1/namespaces/<name>          -> GET ONE namespace object
			//   /api/v1/namespaces/<ns>/<resource> -> a namespaced read
			// Treating the short forms as a namespaced read with a missing
			// resource makes a by-name namespace read UNRESOLVABLE — which is how
			// the real fixture `${ "/api/v1/namespaces/" + .nsName }` would have
			// 422'd. (This is the same divergence the apiserver's own
			// RequestInfoFactory has, where /api/v1/namespaces/<ns> reads back as
			// {Resource: namespaces, Name: <ns>}.) So the short forms resolve to
			// the namespaces RESOURCE at cluster scope, with the trailing segment
			// classified as a NAME.
			if len(parts) < 4 {
				return ReadSetCoord{GVR: schema.GroupVersionResource{Version: version, Resource: "namespaces"}, Name: nameAt(parts, 2), OK: true}
			}
			resource := parts[3]
			if templated(resource) {
				return ReadSetCoord{ResourceTemplated: hasTemplate}
			}
			ns := parts[2]
			g := schema.GroupVersionResource{Version: version, Resource: resource}
			if templated(ns) {
				return ReadSetCoord{GVR: g, NamespaceUnknown: true, Name: nameAt(parts, 4), OK: true}
			}
			return ReadSetCoord{GVR: g, Namespace: ns, Name: nameAt(parts, 4), OK: true}
		}
		resource := parts[1]
		if templated(resource) {
			return ReadSetCoord{ResourceTemplated: hasTemplate}
		}
		return ReadSetCoord{GVR: schema.GroupVersionResource{Version: version, Resource: resource}, Name: nameAt(parts, 2), OK: true}
	}
	return ReadSetCoord{}
}

// NameKind is the three-state name a read-set needs: the VERB depends on whether
// a name segment exists, and a rendered path cannot distinguish "no name" from
// "a name that interpolated to empty" (#504 ruling 2).
type NameKind int

const (
	nameAbsent    NameKind = iota // no name segment: a collection read
	nameStatic                    // a literal name in the template
	nameTemplated                 // an interpolated name: still a single-object read
)

// IsSingleObject reports whether the path addresses ONE object, i.e. whether the
// RBAC verb must be `get` rather than `list`.
func (k NameKind) IsSingleObject() bool { return k == nameStatic || k == nameTemplated }

// String names the kind for logs and test failures.
func (k NameKind) String() string {
	switch k {
	case nameStatic:
		return "static"
	case nameTemplated:
		return "templated"
	default:
		return "absent"
	}
}
