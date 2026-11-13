//go:build js && wasm

package fakeapiserver

// The behaviours #1372's review (c86e3083) found untested or wrong: dry run (H1), the first write
// to a loaded object (M1), each surviving mutation (M3), L1-L10, and per-kind field selectors.

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

const sa = "system:serviceaccount:krateo-system:snowplow"

func rule(groups, resources, verbs []string, names ...string) map[string]any {
	r := map[string]any{"apiGroups": toAny(groups), "resources": toAny(resources), "verbs": toAny(verbs)}
	if len(names) > 0 {
		r["resourceNames"] = toAny(names)
	}
	return r
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// grant is a ClusterRole and a binding (cluster-wide, or a RoleBinding in ns) to subject.
func grant(name, ns string, subject map[string]any, rules ...map[string]any) []map[string]any {
	rs := make([]any, len(rules))
	for i, r := range rules {
		rs[i] = r
	}
	role := map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": name}, "rules": rs}
	ref := map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name}
	if ns == "" {
		return []map[string]any{role, {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": name}, "roleRef": ref, "subjects": []any{subject}}}
	}
	return []map[string]any{role, {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": map[string]any{"name": name, "namespace": ns}, "roleRef": ref, "subjects": []any{subject}}}
}

var saSubject = map[string]any{"kind": "ServiceAccount", "name": "snowplow", "namespace": "krateo-system"}

func concat(gs ...[]map[string]any) []map[string]any {
	var out []map[string]any
	for _, g := range gs {
		out = append(out, g...)
	}
	return out
}

// withResources adds resources to the snapshot's discovery for one group version.
func withResources(gv string, res ...map[string]any) func(map[string]any) {
	return func(s map[string]any) {
		d := s["discovery"].(map[string]any)["resources"].(map[string]any)
		l, ok := d[gv].(map[string]any)
		if !ok {
			l = map[string]any{"kind": "APIResourceList", "groupVersion": gv, "resources": []any{}}
			d[gv] = l
		}
		for _, r := range res {
			l["resources"] = append(l["resources"].([]any), r)
		}
	}
}

func apires(name, kind string, namespaced bool) map[string]any {
	return map[string]any{"name": name, "kind": kind, "namespaced": namespaced, "verbs": []any{"get", "list", "watch", "create", "update", "patch", "delete"}}
}

func rv(r resp) string { return fmt.Sprint(get(r.body, "metadata", "resourceVersion")) }

func incidentRV(h *harness) string { return rv(h.do("GET", inc+"/"+incident, req{as: "sa"})) }

// ------------------------------------------------------------------ H1: dry run

func TestDryRun_PatchCommitsNothing(t *testing.T) {
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true", req{as: "sa"})
	defer w.cancel()
	for w.next()["type"] != "BOOKMARK" {
	}
	before := incidentRV(h)
	r := h.do("PATCH", inc+"/"+incident+"?dryRun=All", req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
	eq(t, r.code, 200, "code")
	eq(t, get(r.body, "spec", "applied"), true, "the would-be object")
	eq(t, rv(r), before, "the answer keeps the stored RV")
	eq(t, incidentRV(h), before, "nothing stored")
	eq(t, get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "spec", "applied"), false, "spec unchanged")
	h.advance(5 * time.Second)
	if logged(h, func(l map[string]any) bool { return l["applyStatus"] != nil }) {
		t.Fatal("a dry run fired a rule")
	}
	w.quiet()
	// The real write after it behaves as if the dry run never happened.
	eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}}).code, 200, "real write")
	eq(t, w.next()["type"], "MODIFIED", "the real write's event")
}

func TestDryRun_CreateUpdateDelete(t *testing.T) {
	g := grant("t-cm", "", saSubject, rule([]string{""}, []string{"configmaps"}, []string{"create", "update", "delete", "get"}))
	h := newHarness(t, opts{extra: g})
	cm := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "dry"}, "data": map[string]any{"a": "1"}}
	const cms = "/api/v1/namespaces/krateo-system/configmaps"
	c := h.do("POST", cms+"?dryRun=All", req{as: "sa", ct: "application/json", body: cm})
	eq(t, c.code, 201, "dry-run create")
	eq(t, get(c.body, "metadata", "resourceVersion"), nil, "a dry-run create has no resourceVersion")
	eq(t, h.do("GET", cms+"/dry", req{as: "sa"}).code, 404, "nothing stored")
	real := h.do("POST", cms, req{as: "sa", ct: "application/json", body: cm})
	eq(t, real.code, 201, "the real create is not a 409")
	upd := real.body
	upd["data"] = map[string]any{"a": "2"}
	u := h.do("PUT", cms+"/dry?dryRun=All", req{as: "sa", ct: "application/json", body: upd})
	eq(t, u.code, 200, "dry-run update")
	eq(t, get(u.body, "data", "a"), "2", "the would-be object")
	eq(t, get(h.do("GET", cms+"/dry", req{as: "sa"}).body, "data", "a"), "1", "update not stored")
	eq(t, h.do("DELETE", cms+"/dry?dryRun=All", req{as: "sa"}).code, 200, "dry-run delete")
	eq(t, h.do("GET", cms+"/dry", req{as: "sa"}).code, 200, "delete not stored")
	bad := h.do("DELETE", cms+"/dry?dryRun=Yes", req{as: "sa"})
	eq(t, bad.code, 422, "an unsupported dryRun value")
	eq(t, bad.body["message"], `DeleteOptions.meta.k8s.io "" is invalid: dryRun: Unsupported value: "Yes": supported values: "All"`, "message")
}

// ------------------------------------------------------------------ M1: the first write to a loaded object

func TestNoOpFirstWriteToALoadedObject(t *testing.T) {
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&resourceVersion=100", req{as: "sa"})
	defer w.cancel()
	before := incidentRV(h)
	current := get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "spec")
	r := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": current}})
	eq(t, r.code, 200, "code")
	eq(t, rv(r), before, "a no-op keeps the RV")
	w.quiet()
}

// ------------------------------------------------------------------ M3: the surviving mutations

var impersonateAll = grant("t-imp-all", "", saSubject, rule([]string{""}, []string{"users", "groups"}, []string{"impersonate"}))

func TestImpersonatedUserWithoutGroupsIsAuthenticated(t *testing.T) { // M1
	h := newHarness(t, opts{extra: impersonateAll})
	eq(t, h.do("GET", "/api", req{as: "sa", headers: map[string]string{"Impersonate-User": "someone"}}).code, 200, "system:authenticated reaches system:discovery")
}

func TestModifiedLeavingTheSelectorIsDeleted(t *testing.T) { // M4
	h := newHarness(t, opts{})
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"labels": map[string]any{"x": "y"}}}})
	w := h.watch(inc+"?watch=1&resourceVersion=0&labelSelector=x%3Dy", req{as: "sa"})
	defer w.cancel()
	eq(t, w.next()["type"], "ADDED", "initial")
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"labels": map[string]any{"x": "z"}}}})
	eq(t, w.next()["type"], "DELETED", "leaving the selector")
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"labels": map[string]any{"x": "y"}}}})
	eq(t, w.next()["type"], "ADDED", "entering it again")
}

func TestPutStaleResourceVersionIsAConflict(t *testing.T) { // M5
	g := grant("t-put", "", map[string]any{"kind": "User", "name": "sandbox-visitor"}, rule([]string{"observability.krateo.io"}, []string{"incidents"}, []string{"update"}))
	h := newHarness(t, opts{extra: g})
	cur := h.do("GET", inc+"/"+incident, req{as: "visitor"}).body
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"note": "moved"}}})
	cur["spec"].(map[string]any)["note"] = "stale"
	r := h.do("PUT", inc+"/"+incident, req{as: "visitor", ct: "application/json", body: cur})
	eq(t, r.code, 409, "stale PUT")
	eq(t, r.body["reason"], "Conflict", "reason")
	fresh := h.do("GET", inc+"/"+incident, req{as: "visitor"}).body
	fresh["spec"].(map[string]any)["note"] = "fresh"
	eq(t, h.do("PUT", inc+"/"+incident, req{as: "visitor", ct: "application/json", body: fresh}).code, 200, "current PUT")
}

func TestWritesKeepServerSetMetadata(t *testing.T) { // M6
	h := newHarness(t, opts{})
	before := h.do("GET", inc+"/"+incident, req{as: "sa"}).body
	r := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"uid": "forged", "creationTimestamp": "2000-01-01T00:00:00Z"}, "spec": map[string]any{"n": 1}}})
	eq(t, get(r.body, "metadata", "uid"), get(before, "metadata", "uid"), "uid")
	eq(t, get(r.body, "metadata", "creationTimestamp"), get(before, "metadata", "creationTimestamp"), "creationTimestamp")
}

func TestResourceNamesLimitAGrant(t *testing.T) { // M7
	g := grant("t-names", "", map[string]any{"kind": "User", "name": "sandbox-visitor"}, rule([]string{""}, []string{"secrets"}, []string{"get"}, "only-this"))
	h := newHarness(t, opts{extra: g})
	const secs = "/api/v1/namespaces/krateo-system/secrets/"
	eq(t, h.do("GET", secs+"only-this", req{as: "visitor"}).code, 404, "the named one is allowed (and absent)")
	eq(t, h.do("GET", secs+"another", req{as: "visitor"}).code, 403, "another name is not")
}

func TestBuiltinCreateIsStoredInItsTypedForm(t *testing.T) { // M8
	g := grant("t-pods", "", saSubject, rule([]string{""}, []string{"pods"}, []string{"create", "get"}))
	h := newHarness(t, opts{extra: g, edit: withResources("v1", apires("pods", "Pod", true))})
	pod := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "p"},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x", "resources": map[string]any{"limits": map[string]any{"memory": "8192Mi"}}}}}}
	r := h.do("POST", "/api/v1/namespaces/krateo-system/pods", req{as: "sa", ct: "application/json", body: pod})
	eq(t, r.code, 201, "create")
	c0 := get(r.body, "spec", "containers").([]any)[0]
	eq(t, get(c0, "resources", "limits", "memory"), "8Gi", "the canonical quantity")
}

func TestGenerationBumpsOnSpecOnly(t *testing.T) { // M9, L7
	h := newHarness(t, opts{})
	g0 := get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "metadata", "generation")
	g1 := get(h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"n": 1}}}).body, "metadata", "generation")
	eq(t, fmt.Sprint(g1), fmt.Sprint(toInt(g0)+1), "a spec change bumps")
	g2 := get(h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"labels": map[string]any{"a": "b"}}}}).body, "metadata", "generation")
	eq(t, g2, g1, "a metadata change does not")
	cm := grant("t-cm2", "", saSubject, rule([]string{""}, []string{"configmaps"}, []string{"create"}))
	k := newHarness(t, opts{extra: cm})
	r := k.do("POST", "/api/v1/namespaces/krateo-system/configmaps", req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "g", "generation": 7}}})
	eq(t, get(r.body, "metadata", "generation"), nil, "a ConfigMap has no generation")
}

func toInt(v any) int {
	var i int
	_, _ = fmt.Sscan(fmt.Sprint(v), &i)
	return i
}

func TestSubjectAccessReviewIsForSpecUser(t *testing.T) { // M10, L3
	g := grant("t-sar", "", saSubject, rule([]string{"authorization.k8s.io"}, []string{"subjectaccessreviews"}, []string{"create"}))
	h := newHarness(t, opts{extra: g})
	sar := func(user string, groups ...string) map[string]any {
		return h.do("POST", "/apis/authorization.k8s.io/v1/subjectaccessreviews", req{as: "sa", ct: "application/json", body: map[string]any{
			"apiVersion": "authorization.k8s.io/v1", "kind": "SubjectAccessReview",
			"spec": map[string]any{"user": user, "groups": toAny(groups), "resourceAttributes": map[string]any{"verb": "get", "resource": "secrets", "namespace": "krateo-system"}}}}).body
	}
	eq(t, get(sar("nobody"), "status", "allowed"), false, "the caller may, spec.user may not")
	yes := sar(sa, "system:serviceaccounts", "system:serviceaccounts:krateo-system", "system:authenticated")
	eq(t, get(yes, "status", "allowed"), true, "spec.user may")
	eq(t, get(yes, "status", "reason"), `RBAC: allowed by RoleBinding "sandbox-test-snowplow-clientconfig/krateo-system" of Role "sandbox-test-snowplow-clientconfig" to ServiceAccount "snowplow/krateo-system"`, "the allow reason")
}

func TestFieldSelectorNameIsTheAuthzName(t *testing.T) { // M11
	g := grant("t-list-one", "", map[string]any{"kind": "User", "name": "sandbox-visitor"}, rule([]string{""}, []string{"secrets"}, []string{"list"}, "one"))
	h := newHarness(t, opts{extra: g})
	const secs = "/api/v1/namespaces/krateo-system/secrets"
	eq(t, h.do("GET", secs+"?fieldSelector=metadata.name%3Done", req{as: "visitor"}).code, 200, "pinned to the granted name")
	eq(t, h.do("GET", secs, req{as: "visitor"}).code, 403, "unpinned")
}

func TestServiceAccountTokenIsAuthenticated(t *testing.T) { // M12
	h := newHarness(t, opts{})
	eq(t, h.do("GET", "/version", req{as: "sa"}).code, 200, "system:discovery through system:authenticated")
}

func TestDeleteWithFinalizers(t *testing.T) { // M13, L7, L8
	g := grant("t-del", "", map[string]any{"kind": "User", "name": "sandbox-visitor"}, rule([]string{"observability.krateo.io"}, []string{"incidents"}, []string{"delete", "patch", "get"}))
	h := newHarness(t, opts{extra: g})
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"finalizers": []any{"x/y"}}}})
	gen := toInt(get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "metadata", "generation"))
	w := h.watch(inc+"?watch=1&resourceVersion="+incidentRV(h), req{as: "sa"})
	defer w.cancel()
	d := h.do("DELETE", inc+"/"+incident, req{as: "visitor"})
	eq(t, d.code, 200, "delete")
	if get(d.body, "metadata", "deletionTimestamp") == nil {
		t.Fatal("no deletionTimestamp")
	}
	eq(t, fmt.Sprint(get(d.body, "metadata", "generation")), fmt.Sprint(gen+1), "the first deletion bumps generation")
	eq(t, w.next()["type"], "MODIFIED", "marked")
	eq(t, h.do("GET", inc+"/"+incident, req{as: "sa"}).code, 200, "still there while a finalizer holds it")
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"metadata": map[string]any{"finalizers": nil}}})
	eq(t, w.next()["type"], "DELETED", "the last finalizer removed deletes it")
	eq(t, h.do("GET", inc+"/"+incident, req{as: "sa"}).code, 404, "gone")
}

func TestWatchCacheShrinks(t *testing.T) { // M14
	c := newWatchCache(0)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rvN := uint64(0)
	add := func(t0 time.Time) { rvN++; c.add(event{Type: "MODIFIED", RV: rvN, Time: t0}) }
	for i := 0; i < 150; i++ { // fresh events: the cache grows past the lower bound
		add(at)
	}
	if c.capacity != 200 {
		t.Fatalf("capacity %d after a fresh burst, want 200", c.capacity)
	}
	for i := 0; i < 50; i++ {
		add(at)
	}
	add(at.Add(10 * time.Minute)) // full, and the event a quarter from the end is stale: halve
	if c.capacity != 100 {
		t.Fatalf("capacity %d after the burst went stale, want 100", c.capacity)
	}
	if len(c.events) != 100 {
		t.Fatalf("%d events kept, want 100", len(c.events))
	}
}

// ------------------------------------------------------------------ L1-L10

func TestServiceAccountSubjectTakesTheRoleBindingsNamespace(t *testing.T) { // L1
	g := grant("t-sa-nons", "krateo-system", map[string]any{"kind": "ServiceAccount", "name": "snowplow"}, rule([]string{""}, []string{"configmaps"}, []string{"create"}))
	h := newHarness(t, opts{extra: g})
	r := h.do("POST", "/api/v1/namespaces/krateo-system/configmaps", req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "l1"}}})
	eq(t, r.code, 201, "the subject's namespace is the binding's")
}

func TestImpersonationGroups(t *testing.T) { // L2
	unauth := grant("t-unauth", "", map[string]any{"kind": "Group", "name": "system:unauthenticated"}, map[string]any{"nonResourceURLs": []any{"/version"}, "verbs": []any{"get"}})
	h := newHarness(t, opts{extra: concat(impersonateAll, unauth)})
	eq(t, h.do("GET", "/version", req{as: "sa", headers: map[string]string{"Impersonate-User": "system:anonymous"}}).code, 200, "anonymous gains system:unauthenticated")
	eq(t, h.do("GET", "/api", req{as: "sa", headers: map[string]string{"Impersonate-User": "someone", "Impersonate-Group": "system:unauthenticated"}}).code, 403, "an explicit system:unauthenticated adds no system:authenticated")
	eq(t, h.do("GET", "/api", req{as: "sa", headers: map[string]string{"Impersonate-Group": "g"}}).code, 400, "groups without a user")
}

func TestRulesReviewNeedsANamespaceAndSplitsRules(t *testing.T) { // L3
	h := newHarness(t, opts{})
	r := h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews", req{as: "visitor", ct: "application/json", body: map[string]any{"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectRulesReview", "spec": map[string]any{}}})
	eq(t, r.code, 400, "no namespace")
	eq(t, r.body["message"], "no namespace on request", "message")
	ok := h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews", req{as: "visitor", ct: "application/json", body: map[string]any{"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectRulesReview", "spec": map[string]any{"namespace": "krateo-system"}}})
	for _, rr := range get(ok.body, "status", "resourceRules").([]any) {
		if res, _ := get(rr, "resources").([]any); len(res) == 0 {
			t.Fatalf("a resource rule with no resources: %v", rr)
		}
	}
}

func TestBadFieldSelectorAfterAuthorization(t *testing.T) { // L4
	h := newHarness(t, opts{})
	eq(t, h.do("GET", inc+"?fieldSelector=spec.x%3Dy", req{}).code, 403, "anonymous: 403, not 400")
	eq(t, h.do("GET", inc+"?fieldSelector=spec.x%3Dy", req{as: "visitor"}).code, 400, "an allowed caller: 400")
}

func TestContinueWithoutLimitReturnsTheRest(t *testing.T) { // L5
	h := newHarness(t, opts{})
	const crs = "/apis/rbac.authorization.k8s.io/v1/clusterroles"
	all := len(items(h.do("GET", crs, req{as: "sa"})))
	p1 := h.do("GET", crs+"?limit=1", req{as: "sa"})
	rest := h.do("GET", crs+"?continue="+fmt.Sprint(get(p1.body, "metadata", "continue")), req{as: "sa"})
	eq(t, len(items(rest)), all-1, "the rest")
	eq(t, get(rest.body, "metadata", "continue"), nil, "no further token")
	h.advance(6 * time.Minute)
	h.do("GET", crs, req{as: "sa"})
	h.s.mu.Lock()
	n := len(h.s.conts)
	h.s.mu.Unlock()
	eq(t, n, 0, "expired tokens are pruned")
}

func TestWatchLeavesNoTimerArmed(t *testing.T) { // L6
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&resourceVersion=100&allowWatchBookmarks=true&timeoutSeconds=120", req{as: "sa"})
	time.Sleep(50 * time.Millisecond)
	h.advance(time.Minute)
	eq(t, w.next()["type"], "BOOKMARK", "1 min")
	h.advance(time.Minute) // the bookmark and the deadline at the same instant
	for ev := w.next(); ev != nil; ev = w.next() {
	}
	time.Sleep(50 * time.Millisecond)
	h.mu.Lock()
	armed := 0
	for _, ft := range h.timers {
		if !ft.done {
			armed++
		}
	}
	h.mu.Unlock()
	eq(t, armed, 0, "timers left armed after the watch ended")
}

func TestUnregisteredBuiltinKindsDecodeAsUnstructured(t *testing.T) { // L9
	g := grant("t-crd", "", saSubject, rule([]string{"apiextensions.k8s.io"}, []string{"customresourcedefinitions"}, []string{"create"}))
	h := newHarness(t, opts{extra: g})
	crd := map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "widgets.example.invalid"},
		"spec": map[string]any{"group": "example.invalid", "names": map[string]any{"plural": "widgets", "kind": "Widget"}, "scope": "Namespaced",
			"versions": []any{map[string]any{"name": "v1", "served": true, "storage": true, "schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object"}}}}}}
	eq(t, h.do("POST", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions", req{as: "sa", ct: "application/json", body: crd}).code, 201, "a CRD create")
}

func TestErrorBodiesMatchUpstream(t *testing.T) { // L10
	h := newHarness(t, opts{})
	r415 := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "text/plain", body: "{}"})
	if _, ok := r415.body["details"]; ok {
		t.Fatalf("415 carries details: %v", r415.body)
	}
	r406 := h.do("GET", inc, req{as: "visitor", accept: "application/vnd.kubernetes.protobuf"})
	if _, ok := r406.body["details"]; ok {
		t.Fatalf("406 carries details: %v", r406.body)
	}
	eq(t, r406.body["message"], "only the following media types are accepted: application/json, application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1", "406 message")
	// A non-resource 403 is apierrors.NewForbidden with an empty GroupResource, exactly as
	// responsewriters.ForbiddenStatusError builds it: its Details is set, so upstream's body has
	// "details":{} (k8s.io/apimachinery v0.36.5 pkg/api/errors/errors.go NewForbidden).
	r403 := h.do("GET", "/metrics", req{as: "visitor"})
	b, _ := json.Marshal(r403.body)
	eq(t, string(b), `{"apiVersion":"v1","code":403,"details":{},"kind":"Status","message":"forbidden: User \"sandbox-visitor\" cannot get path \"/metrics\"","metadata":{},"reason":"Forbidden","status":"Failure"}`, "non-resource 403")
}

// ------------------------------------------------------------------ Q7: per-kind field selectors

func TestFieldSelectorsPerKind(t *testing.T) {
	g := grant("t-pods-list", "", saSubject, rule([]string{""}, []string{"pods"}, []string{"list", "create"}))
	h := newHarness(t, opts{extra: g, edit: withResources("v1", apires("pods", "Pod", true))})
	const pods = "/api/v1/namespaces/krateo-system/pods"
	for _, p := range []map[string]any{
		{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "a"}, "spec": map[string]any{"nodeName": "n1", "containers": []any{map[string]any{"name": "c", "image": "x"}}}, "status": map[string]any{"phase": "Running"}},
		{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "b"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}}, "status": map[string]any{"phase": "Pending"}},
	} {
		eq(t, h.do("POST", pods, req{as: "sa", ct: "application/json", body: p}).code, 201, "create")
	}
	names := func(q string) string {
		r := h.do("GET", pods+"?fieldSelector="+q, req{as: "sa"})
		if r.code != 200 {
			return fmt.Sprint(r.code, " ", r.body["message"])
		}
		var ns []string
		for _, it := range items(r) {
			ns = append(ns, fmt.Sprint(get(it, "metadata", "name")))
		}
		return strings.Join(ns, ",")
	}
	eq(t, names("status.phase%3DRunning"), "a", "status.phase")
	eq(t, names("spec.nodeName%3Dn1"), "a", "spec.nodeName")
	eq(t, names("spec.host%3Dn1"), "a", "spec.host, converted to spec.nodeName")
	eq(t, names("spec.hostNetwork%3Dfalse"), "a,b", "spec.hostNetwork defaults to false")
	eq(t, names("spec.nodeName%21%3Dn1"), "b", "!=")
	eq(t, names("spec.image%3Dx"), "400 field label not supported: spec.image", "an unsupported label")
	eq(t, names("metadata.name%3Db"), "b", "metadata.name")
}

func TestFieldSelectorsForACustomResource(t *testing.T) {
	h := newHarness(t, opts{})
	eq(t, h.do("GET", inc+"?fieldSelector=spec.applied%3Dtrue", req{as: "visitor"}).code, 400, "no selectableFields: 400")
	j := newHarness(t, opts{pageEdit: func(objs []map[string]any) {
		for _, o := range objs {
			if o["kind"] == "CustomResourceDefinition" && get(o, "metadata", "name") == "incidents.observability.krateo.io" {
				for _, v := range get(o, "spec", "versions").([]any) {
					v.(map[string]any)["selectableFields"] = []any{map[string]any{"jsonPath": ".spec.applied"}}
				}
			}
		}
	}})
	r := j.do("GET", inc+"?fieldSelector=spec.applied%3Dfalse", req{as: "visitor"})
	eq(t, r.code, 200, "a CRD selectable field")
	eq(t, len(items(r)), 1, "matches")
	eq(t, len(items(j.do("GET", inc+"?fieldSelector=spec.applied%3Dtrue", req{as: "visitor"}))), 0, "and filters")
}
