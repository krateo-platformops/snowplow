//go:build js && wasm

package fakeapiserver

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
)

// review answers the authorization.k8s.io reviews from the RBAC evaluator. Bodies come JSON or
// protobuf (a typed clientset sends protobuf) and are decoded with client-go's scheme; answers are JSON.
func (s *Server) review(c *reqCtx) error {
	body, err := readBody(c.r)
	if err != nil {
		return err
	}
	mt := mediaType(c.r)
	if mt != "" && mt != "application/json" && mt != "application/vnd.kubernetes.protobuf" {
		return unsupportedMediaType(s.bodyTypes(c))
	}
	obj, bodyKind, err := scheme.Codecs.UniversalDeserializer().Decode(body, nil, nil)
	if err != nil {
		return transformDecodeError(c, bodyKind, body, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out runtime.Object
	switch r := obj.(type) {
	case *authorizationv1.SelfSubjectAccessReview:
		if c.resource != "selfsubjectaccessreviews" {
			return kindMismatch(c, r.Kind)
		}
		r.Status = s.accessStatus(c.id, r.Spec.ResourceAttributes, r.Spec.NonResourceAttributes)
		out = r
	case *authorizationv1.SubjectAccessReview:
		if c.resource != "subjectaccessreviews" {
			return kindMismatch(c, r.Kind)
		}
		who := identity{User: r.Spec.User, Groups: r.Spec.Groups}
		r.Status = s.accessStatus(who, r.Spec.ResourceAttributes, r.Spec.NonResourceAttributes)
		out = r
	case *authorizationv1.LocalSubjectAccessReview:
		if c.resource != "localsubjectaccessreviews" {
			return kindMismatch(c, r.Kind)
		}
		if r.Spec.ResourceAttributes == nil || r.Spec.ResourceAttributes.Namespace != c.ns {
			return apierrors.NewBadRequest(fmt.Sprintf("spec.resourceAttributes.namespace must match namespace: %v", c.ns))
		}
		who := identity{User: r.Spec.User, Groups: r.Spec.Groups}
		r.Status = s.accessStatus(who, r.Spec.ResourceAttributes, nil)
		out = r
	case *authorizationv1.SelfSubjectRulesReview:
		if c.resource != "selfsubjectrulesreviews" {
			return kindMismatch(c, r.Kind)
		}
		if r.Spec.Namespace == "" { // k8s.io/kubernetes v1.36.2 pkg/registry/authorization/selfsubjectrulesreview/rest.go:70-73
			return apierrors.NewBadRequest("no namespace on request")
		}
		r.Status = s.rulesStatus(c.id, r.Spec.Namespace)
		out = r
	default:
		return s.unimplemented(fmt.Sprintf("authorization.k8s.io %s with a %T body", c.resource, obj))
	}
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(out)
	if err != nil {
		return apierrors.NewInternalError(err)
	}
	gvk := authorizationv1.SchemeGroupVersion.WithKind(reflect.TypeOf(out).Elem().Name())
	u["apiVersion"], u["kind"] = gvk.GroupVersion().String(), gvk.Kind
	writeJSON(c.w, http.StatusCreated, "application/json", u)
	c.line["status"] = http.StatusCreated
	return nil
}

func kindMismatch(c *reqCtx, kind string) error {
	return apierrors.NewBadRequest(fmt.Sprintf("a %s body was posted to %s", kind, c.resource))
}

func (s *Server) accessStatus(who identity, ra *authorizationv1.ResourceAttributes, nra *authorizationv1.NonResourceAttributes) authorizationv1.SubjectAccessReviewStatus {
	var at attributes
	switch {
	case ra != nil:
		at = attributes{Verb: ra.Verb, Group: ra.Group, Resource: ra.Resource, Subresource: ra.Subresource, Namespace: ra.Namespace, Name: ra.Name}
	case nra != nil:
		at = attributes{Verb: nra.Verb, Path: nra.Path, NonResource: true}
	default:
		return authorizationv1.SubjectAccessReviewStatus{EvaluationError: "exactly one of nonResourceAttributes or resourceAttributes must be specified"}
	}
	if ok, why := s.authz.authorize(who, at); ok {
		return authorizationv1.SubjectAccessReviewStatus{Allowed: true, Reason: why}
	}
	return authorizationv1.SubjectAccessReviewStatus{Allowed: false}
}

// rulesStatus is the caller's RBAC rules in a namespace. Whether the answer is complete is the
// reference cluster's, as captured (snapshot.json capturedFrom.selfSubjectRulesReview, §4): on a
// cluster with a webhook authorizer after RBAC, the rules are RBAC's and the answer is incomplete.
func (s *Server) rulesStatus(who identity, ns string) authorizationv1.SubjectRulesReviewStatus {
	st := authorizationv1.SubjectRulesReviewStatus{ResourceRules: []authorizationv1.ResourceRule{}, NonResourceRules: []authorizationv1.NonResourceRule{}}
	// k8s.io/kubernetes v1.36.2 plugin/pkg/auth/authorizer/rbac/rbac.go:129-150: a rule is a
	// resource rule only if it names resources, and a non-resource rule only if it names URLs.
	for _, r := range s.authz.rulesFor(who, ns) {
		if len(r.Resources) > 0 {
			st.ResourceRules = append(st.ResourceRules, authorizationv1.ResourceRule{Verbs: r.Verbs, APIGroups: r.APIGroups, Resources: r.Resources, ResourceNames: r.ResourceNames})
		}
		if len(r.NonResourceURLs) > 0 {
			st.NonResourceRules = append(st.NonResourceRules, authorizationv1.NonResourceRule{Verbs: r.Verbs, NonResourceURLs: r.NonResourceURLs})
		}
	}
	if rr := s.snap.CapturedFrom.SelfSubjectRulesReview; rr != nil {
		st.Incomplete, st.EvaluationError = rr.Incomplete, rr.EvaluationError
	}
	return st
}

// ------------------------------------------------------------------ applyStatus (§6)

// contains is §6 on.match: objects recursively, scalars by equality, arrays as whole values, null = absent.
func contains(o, m any) bool {
	mm, isObj := m.(map[string]any)
	if !isObj {
		return reflect.DeepEqual(normalizeScalar(o), normalizeScalar(m))
	}
	om, ok := o.(map[string]any)
	if !ok {
		return false
	}
	for k, v := range mm {
		x, has := om[k]
		if v == nil {
			if has {
				return false
			}
			continue
		}
		if !has || !contains(x, v) {
			return false
		}
	}
	return true
}

// normalizeScalar compares numbers by value whether they were decoded as json.Number or float64.
func normalizeScalar(v any) any {
	if n, err := normalize(map[string]any{"v": v}); err == nil {
		return n["v"]
	}
	return v
}

// fireRules schedules every rule a write matches. Holds s.mu.
func (s *Server) fireRules(c *reqCtx, verb string, stored map[string]any) {
	if c.dryRun {
		return // a dry run commits nothing, so no controller reacts to it
	}
	name, _ := meta(stored)["name"].(string)
	for _, rule := range s.rules {
		on := rule.On
		if on.Verb != verb || on.Group != c.group || on.Resource != c.resource || on.Namespace != c.ns {
			continue
		}
		if on.Name != nil && *on.Name != name {
			continue
		}
		if on.Match != nil && !contains(stored, map[string]any(on.Match)) {
			continue
		}
		t, err := rule.target()
		if err != nil {
			s.o.Log(map[string]any{"event": "applyStatus.failed", "id": rule.ID, "pointer": "", "reason": err.Error()})
			continue
		}
		if t.Trigger {
			uid, _ := meta(stored)["uid"].(string)
			t = target{Group: c.group, Resource: c.resource, Namespace: c.ns, Name: name, UID: uid}
		}
		rule := rule
		s.o.AfterFunc(time.Duration(rule.AfterMs)*time.Millisecond, func() { s.applyStatus(rule, t) })
	}
}

// applyStatus is a controller's status write: as the ServiceAccount, through the /status path's
// rules, one MODIFIED event to every matching watch.
func (s *Server) applyStatus(rule Rule, t target) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fail := func(pointer, reason string) {
		s.o.Log(map[string]any{"event": "applyStatus.failed", "id": rule.ID, "pointer": pointer, "reason": reason})
	}
	gr := t.Group + "/" + t.Resource
	prev := s.bucket(gr)[t.Namespace+"/"+t.Name]
	if t.UID != "" && (prev == nil || meta(prev)["uid"] != t.UID) {
		// The object the rule fired on was deleted before the delay ran out (and its name may be
		// another object's now). A controller does not write the status of an object that is gone.
		return
	}
	if prev == nil {
		fail("", fmt.Sprintf("target %s %s/%s does not exist", t.Resource, t.Namespace, t.Name))
		return
	}
	loc, ok := s.locate(prev)
	if !ok {
		fail("", "the target's kind is not in discovery")
		return
	}
	next := deepCopy(prev).(map[string]any)
	status, _ := prev["status"].(map[string]any)
	merged, err := mergeStatus(status, rule.Status)
	if err != nil {
		fail("", err.Error())
		return
	}
	next["status"] = merged
	at := s.now()
	for _, p := range rule.Stamp {
		toks, reason := pointerTokens(p)
		if reason == "" {
			reason = stampAt(next, toks, at)
		}
		if reason != "" {
			fail(p, reason)
			return
		}
	}
	gv := loc.group + "/" + loc.version
	if loc.group == "" {
		gv = loc.version
	}
	c := &reqCtx{group: t.Group, version: loc.version, gv: gv, resource: t.Resource, subresource: "status", ns: t.Namespace, name: t.Name, gr: gr, api: loc.api}
	// The scripted write's request line is a plain one: the ServiceAccount's patch of /status.
	line := requestLine("")
	line["verb"], line["group"], line["version"], line["resource"], line["namespace"], line["name"], line["status"], line["identity"] = "patch", t.Group, loc.version, t.Resource+"/status", t.Namespace, t.Name, 200, "sa"
	if !s.authz.allowed(s.sa, attributes{Verb: "patch", Group: t.Group, Resource: t.Resource, Subresource: "status", Namespace: t.Namespace, Name: t.Name}) {
		line["status"] = 403
		s.o.Log(line)
		fail("", fmt.Sprintf("the ServiceAccount may not patch %s/status", t.Resource))
		return
	}
	coerced, _, err := s.coerceCustomResource(c, next, "Ignore")
	if err != nil {
		line["status"] = 400
		s.o.Log(line)
		fail("", err.Error())
		return
	}
	admitted := s.restrict(c, prev, coerced)
	if err := s.validateCustomResource(c, admitted, prev); err != nil {
		line["status"] = 422
		s.o.Log(line)
		fail("", err.Error())
		return
	}
	if _, err := s.write(c, prev, admitted); err != nil {
		line["status"] = 500
		s.o.Log(line)
		fail("", err.Error())
		return
	}
	s.o.Log(line)
}

// stampAt sets the value at an RFC 6901 pointer's location in doc: through objects by key and arrays
// by index (decimal, no leading zero, in range), the last token naming an object member (set or
// added) or an existing array element (replaced). "-", the element past an array's end, is refused:
// a stamp sets a field, it never appends. It returns why it could not, or "".
func stampAt(doc any, toks []string, v any) string {
	node := doc
	for i, tok := range toks {
		last := i == len(toks)-1
		switch n := node.(type) {
		case map[string]any:
			if last {
				n[tok] = v
				return ""
			}
			node = n[tok]
		case []any:
			if tok == "-" {
				return "\"-\" names no element: a stamp sets an existing array element, it never appends"
			}
			idx, ok := arrayIndex(tok, len(n))
			if !ok {
				return fmt.Sprintf("%q is not an index of an array of %d after the status merge", tok, len(n))
			}
			if last {
				n[idx] = v
				return ""
			}
			node = n[idx]
		default:
			return "parent is not an object or an array after the status merge"
		}
	}
	return "a stamp names a field, not the whole document"
}

// arrayIndex is RFC 6901's array-index: "0", or a digit 1-9 followed by digits; and below n.
func arrayIndex(tok string, n int) (int, bool) {
	if tok == "" || (len(tok) > 1 && tok[0] == '0') {
		return 0, false
	}
	idx := 0
	for _, c := range tok {
		if c < '0' || c > '9' {
			return 0, false
		}
		idx = idx*10 + int(c-'0')
		if idx >= n {
			return 0, false
		}
	}
	return idx, true
}

// pointerTokens splits an RFC 6901 pointer into its reference tokens, unescaped: "~1" is "/" and
// "~0" is "~", in that order, so "~01" is the token "~1". A pointer is "" (the whole document, no
// tokens) or "/" followed by tokens; a "~" not followed by "0" or "1" is not one. It returns why p
// is not a pointer, or "".
func pointerTokens(p string) ([]string, string) {
	if p == "" {
		return nil, ""
	}
	if p[0] != '/' {
		return nil, fmt.Sprintf("%q is not an RFC 6901 pointer: it does not start with \"/\"", p)
	}
	parts := strings.Split(p[1:], "/")
	for i, t := range parts {
		for j := 0; j < len(t); j++ {
			if t[j] == '~' && (j+1 == len(t) || (t[j+1] != '0' && t[j+1] != '1')) {
				return nil, fmt.Sprintf("%q is not an RFC 6901 pointer: \"~\" must be followed by \"0\" or \"1\"", p)
			}
		}
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(t, "~1", "/"), "~0", "~")
	}
	return parts, ""
}

// mergeStatus is an RFC 7386 merge of the rule's status into the stored one.
func mergeStatus(cur map[string]any, patch map[string]any) (map[string]any, error) {
	out, err := normalize(map[string]any{"s": mergeValue(deepCopy(cur), patch)})
	if err != nil {
		return nil, err
	}
	m, _ := out["s"].(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

func mergeValue(cur any, patch any) any {
	pm, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	cm, ok := cur.(map[string]any)
	if !ok {
		cm = map[string]any{}
	}
	for k, v := range pm {
		if v == nil {
			delete(cm, k)
			continue
		}
		cm[k] = mergeValue(cm[k], v)
	}
	return cm
}
