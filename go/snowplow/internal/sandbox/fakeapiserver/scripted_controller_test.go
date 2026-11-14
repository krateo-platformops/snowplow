//go:build js && wasm

package fakeapiserver

// A scripted status write is the simulated controller's (agentiko-sandbox #56). On a snapshot built
// from a real capture snowplow's ServiceAccount holds the reference cluster's rights exactly:
// read-only. The write is therefore not the ServiceAccount's: it is authorised as nobody, bound to
// the rule's target and its status subresource, and its request line says identity "controller",
// which no request can carry.

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	alertName         = "demo-volume-attach"
	alerts            = "/apis/observability.krateo.io/v1alpha1/namespaces/krateo-system/alerts"
	visitorSecretPath = "/api/v1/namespaces/krateo-system/secrets/sandbox-visitor-clientconfig"
)

// requireReadOnlyServiceAccount is the tests' premise, checked: the ServiceAccount's rules, cluster
// wide and in the story's namespace, hold no verb but get, list and watch, apart from the two a
// real capture also carries (create on the access reviews, impersonate). So no rule gives it a
// write on anything, a /status among them.
func requireReadOnlyServiceAccount(t *testing.T, h *harness) {
	t.Helper()
	n := 0
	for _, ns := range []string{"", "krateo-system"} {
		for _, r := range h.s.authz.rulesFor(h.s.sa, ns) {
			n++
			for _, v := range r.Verbs {
				switch {
				case v == "get" || v == "list" || v == "watch":
				case v == "impersonate":
				case v == "create" && len(r.APIGroups) == 1 && r.APIGroups[0] == "authorization.k8s.io":
				default:
					t.Fatalf("the premise does not hold: the ServiceAccount has %q on %v %v", v, r.APIGroups, r.Resources)
				}
			}
		}
	}
	if n == 0 {
		t.Fatal("the premise was not checked: the ServiceAccount has no rule at all")
	}
}

// controllerLines are the request lines whose identity is the controller's.
func controllerLines(lines []map[string]any) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["identity"] == "controller" {
			out = append(out, l)
		}
	}
	return out
}

func failures(lines []map[string]any) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["event"] == "applyStatus.failed" {
			out = append(out, l)
		}
	}
	return out
}

// synced watches path from the current state and returns once the initial events are through.
func synced(h *harness, path string) *stream {
	w := h.watch(path+"?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true", req{as: "sa"})
	for {
		e := w.next()
		if e == nil {
			h.t.Fatal("the watch ended before its bookmark")
		}
		if e["type"] == "BOOKMARK" {
			return w
		}
	}
}

// Both rule kinds fire while the ServiceAccount is read-only, as on a real capture: the status is
// written, one MODIFIED event reaches the watch, and the write's request line is the controller's
// patch of <resource>/status, 200.
func TestScriptedWrite_FiresWithAReadOnlyServiceAccount(t *testing.T) {
	t.Run("resolve-on-applied, a patch rule on incidents", func(t *testing.T) {
		h := newHarness(t, opts{})
		requireReadOnlyServiceAccount(t, h)
		w := synced(h, inc)
		defer w.cancel()
		eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}}).code, 200, "the visitor's write")
		eq(t, w.next()["type"], "MODIFIED", "the visitor's event")
		before := len(h.log())
		h.advance(1500 * time.Millisecond)
		mod := w.next()
		eq(t, mod["type"], "MODIFIED", "the controller's event")
		eq(t, get(mod, "object", "status"), map[string]any{"state": "Resolved", "resolution": map[string]any{"by": "verify", "at": "2026-11-12T10:00:01Z"}}, "the status written")
		w.quiet() // one event, not two
		lines := h.log()[before:]
		if len(lines) != 1 {
			t.Fatalf("the rule wrote %d lines, want its one request line: %v", len(lines), lines)
		}
		eq(t, lines[0], map[string]any{"verb": "patch", "group": "observability.krateo.io", "version": "v1alpha1", "resource": "incidents/status", "namespace": "krateo-system", "name": incident, "query": "", "status": 200, "items": 0, "identity": "controller"}, "the write's request line")
		eq(t, get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "status", "state"), "Resolved", "stored")
	})
	t.Run("file-raised-request, a create rule on zendeskrequests", func(t *testing.T) {
		h, uid := zrHarness(t)
		requireReadOnlyServiceAccount(t, h)
		w := synced(h, zrPath)
		defer w.cancel()
		name := "zr-sandbox-visitor-1794600056"
		eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(name, uid)}).code, 201, "the visitor's create")
		eq(t, w.next()["type"], "ADDED", "the visitor's event")
		before := len(h.log())
		h.advance(zrFileWait)
		mod := w.next()
		eq(t, mod["type"], "MODIFIED", "the controller's event")
		eq(t, get(mod, "object", "metadata", "name"), name, "the object filed")
		eq(t, cardState(mod["object"].(map[string]any)), "filed", "the status written")
		w.quiet()
		lines := h.log()[before:]
		if len(lines) != 1 {
			t.Fatalf("the rule wrote %d lines, want its one request line: %v", len(lines), lines)
		}
		eq(t, lines[0], map[string]any{"verb": "patch", "group": zrGroup, "version": "v2", "resource": "zendeskrequests/status", "namespace": "krateo-system", "name": name, "query": "", "status": 200, "items": 0, "identity": "controller"}, "the write's request line")
		eq(t, cardState(h.do("GET", zrPath+"/"+name, req{as: "sa"}).body), "filed", "stored")
	})
}

// The change grants nobody anything: the ServiceAccount and the visitor are each refused the patch
// of <resource>/status over HTTP, before a rule fired and after, and a self access review says so.
func TestScriptedWrite_GrantsNobodyTheStatusPatch(t *testing.T) {
	ssar := func(t *testing.T, h *harness, as, group, resource string) any {
		t.Helper()
		r := h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", req{as: as, ct: "application/json", body: map[string]any{
			"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
			"spec": map[string]any{"resourceAttributes": map[string]any{"verb": "patch", "group": group, "resource": resource, "subresource": "status", "namespace": "krateo-system"}}}})
		eq(t, r.code, 201, "the self access review as "+as)
		return get(r.body, "status", "allowed")
	}
	refused := func(t *testing.T, h *harness, path, group, resource, when string) {
		t.Helper()
		body := map[string]any{"status": map[string]any{"state": "Resolved", "status": "solved"}}
		for _, as := range []string{"sa", "visitor"} {
			before := h.do("GET", path, req{as: "sa"})
			eq(t, before.code, 200, "the object is there")
			r := h.do("PATCH", path+"/status", req{as: as, ct: merge, body: body})
			eq(t, r.code, 403, when+": "+as+" patches "+resource+"/status")
			if !strings.Contains(msg(r), `cannot patch resource "`+resource+`/status"`) {
				t.Fatalf("%s: %s is refused for another reason: %s", when, as, msg(r))
			}
			eq(t, h.do("PUT", path+"/status", req{as: as, ct: "application/json", body: before.body}).code, 403, when+": "+as+" updates "+resource+"/status")
			eq(t, h.do("GET", path, req{as: "sa"}).body, before.body, when+": nothing was written")
			eq(t, ssar(t, h, as, group, resource), false, when+": "+as+"'s self access review for patch "+resource+"/status")
		}
		eq(t, h.do("PATCH", path+"/status", req{ct: merge, body: body}).code, 403, when+": anonymous")
		eq(t, h.do("PATCH", path+"/status", req{as: "controller", ct: merge, body: body}).code, 401, when+": a token of no identity")
	}
	t.Run("incidents", func(t *testing.T) {
		h := newHarness(t, opts{})
		refused(t, h, inc+"/"+incident, "observability.krateo.io", "incidents", "before the rule")
		h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
		h.advance(2 * time.Second)
		if len(controllerLines(h.log())) != 1 {
			t.Fatalf("the rule did not fire once: %v", h.log())
		}
		refused(t, h, inc+"/"+incident, "observability.krateo.io", "incidents", "after the rule")
		if n := len(controllerLines(h.log())); n != 1 {
			t.Fatalf("%d controller lines, want the rule's one: a refused request wrote one", n)
		}
	})
	t.Run("zendeskrequests", func(t *testing.T) {
		h, uid := zrHarness(t)
		name := "zr-sandbox-visitor-1794600057"
		eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(name, uid)}).code, 201, "the visitor's create")
		refused(t, h, zrPath+"/"+name, zrGroup, "zendeskrequests", "before the rule")
		h.advance(zrFileWait)
		if len(controllerLines(h.log())) != 1 {
			t.Fatalf("the rule did not fire once: %v", h.log())
		}
		refused(t, h, zrPath+"/"+name, zrGroup, "zendeskrequests", "after the rule")
		if n := len(controllerLines(h.log())); n != 1 {
			t.Fatalf("%d controller lines, want the rule's one: a refused request wrote one", n)
		}
	})
}

// credentials is every way a request could try to be the controller: tokens, schemes,
// impersonation of users, groups, ServiceAccounts and a uid, and headers and query parameters that
// other systems read an identity from.
func credentials(saUser, visitorUser string) []struct {
	what    string
	headers map[string]string
	query   string
} {
	type c = struct {
		what    string
		headers map[string]string
		query   string
	}
	out := []c{
		{what: "no credential"},
		{what: "the ServiceAccount's token", headers: map[string]string{"Authorization": "Bearer sa-token"}},
		{what: "the visitor's token", headers: map[string]string{"Authorization": "Bearer visitor-token"}},
		{what: "an identity header", headers: map[string]string{"X-Remote-User": "controller", "X-Remote-Group": "controller", "X-Identity": "controller", "X-Forwarded-User": "controller", "Identity": "controller", "From": "controller", "User-Agent": "controller"}},
		{what: "an identity header beside the ServiceAccount's token", headers: map[string]string{"Authorization": "Bearer sa-token", "X-Remote-User": "controller", "X-Identity": "controller", "Identity": "controller", "User-Agent": "controller"}},
		{what: "an identity query parameter", query: "identity=controller&as=controller&user=controller"},
		{what: "an identity query parameter beside the visitor's token", headers: map[string]string{"Authorization": "Bearer visitor-token"}, query: "identity=controller&as=controller&user=controller"},
	}
	for _, tok := range []string{"controller", "controller-token", "Controller", "sa-token controller", "sa-token,controller", "system:controller", "", "sa", "visitor", "other", "none"} {
		for _, scheme := range []string{"Bearer ", "bearer ", "Basic ", "Controller ", ""} {
			out = append(out, c{what: fmt.Sprintf("Authorization %q", scheme+tok), headers: map[string]string{"Authorization": scheme + tok}})
		}
	}
	out = append(out, c{what: "Basic controller:controller", headers: map[string]string{"Authorization": "Basic Y29udHJvbGxlcjpjb250cm9sbGVy"}})
	for _, as := range []string{"sa", "visitor"} {
		for i, user := range []string{"controller", "system:controller", "system:kube-controller-manager", "system:serviceaccount:krateo-system:controller", "system:serviceaccount:kube-system:controller", "system:anonymous", saUser, visitorUser} {
			for _, extra := range []map[string]string{{}, {"Impersonate-Group": "controller"}, {"Impersonate-Uid": "controller"}, {"Impersonate-Group": "system:masters"}} {
				hd := map[string]string{"Authorization": "Bearer " + as + "-token", "Impersonate-User": user}
				for k, v := range extra {
					hd[k] = v
				}
				out = append(out, c{what: fmt.Sprintf("%s impersonating user %d of the list with %v", as, i, extra), headers: hd})
			}
		}
		out = append(out, c{what: as + " impersonating a group alone", headers: map[string]string{"Authorization": "Bearer " + as + "-token", "Impersonate-Group": "controller"}})
	}
	return out
}

// No request line is the controller's: whatever credential a request carries, and whatever it may
// impersonate, the authentication path names it sa, visitor, other or none, and its line says so.
// The ServiceAccount here may do anything (crSAAll: every verb on everything, impersonation
// included), the widest a credential can be.
func TestScriptedWrite_NoRequestIsTheControllers(t *testing.T) {
	h := newHarness(t, opts{extra: crSAAll})
	requests := []struct{ method, path, ct, body string }{
		{"GET", "/api", "", ""},
		{"GET", inc + "/" + incident, "", ""},
		{"PATCH", inc + "/" + incident + "/status", merge, `{"status":{"state":"Resolved"}}`},
		{"PATCH", inc + "/" + incident, merge, `{"spec":{"seen":true}}`},
	}
	request := func(method, path, query, ct, body string, headers map[string]string) *http.Request {
		if query != "" {
			path += "?" + query
		}
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if ct != "" {
			r.Header.Set("Content-Type", ct)
		}
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return r
	}
	ofARequest := map[string]bool{"sa": true, "visitor": true, "other": true, "none": true}
	seen := map[string]bool{}
	for _, c := range credentials(h.s.sa.User, h.s.visitor.User) {
		// The authentication path itself: what it names the caller.
		r := request("GET", "/api", c.query, "", "", c.headers)
		id, err := h.s.authenticate(r)
		if err == nil {
			if !ofARequest[id.Label] {
				t.Errorf("%s: authenticated as %q", c.what, id.Label)
			}
			if id, err = h.s.impersonate(r, id); err == nil && !ofARequest[id.Label] {
				t.Errorf("%s: impersonation made it %q", c.what, id.Label)
			}
		}
		// And every line a request with it writes.
		for _, q := range requests {
			before := len(h.log())
			rec := httptest.NewRecorder()
			h.s.ServeHTTP(rec, request(q.method, q.path, c.query, q.ct, q.body, c.headers))
			lines := h.log()[before:]
			if len(lines) == 0 {
				t.Errorf("%s: %s %s wrote no line", c.what, q.method, q.path)
			}
			for _, l := range lines {
				if _, event := l["event"]; event {
					continue
				}
				label, _ := l["identity"].(string)
				seen[label] = true
				if !ofARequest[label] {
					t.Errorf("%s: %s %s is logged with identity %q (status %v)", c.what, q.method, q.path, label, l["status"])
				}
			}
		}
	}
	// The run covered the four a request can be, so the check above could have said otherwise.
	for label := range ofARequest {
		if !seen[label] {
			t.Errorf("no request in the run was logged as %q", label)
		}
	}
	// No rule fired in this run's requests' own time; the log can hold a controller line, and does
	// once one fires.
	if n := len(controllerLines(h.log())); n != 0 {
		t.Fatalf("%d controller lines before any rule's delay ran out", n)
	}
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
	h.advance(2 * time.Second)
	if len(controllerLines(h.log())) == 0 {
		t.Fatal("the instrument is blind: a fired rule's line is not the controller's")
	}
	for _, l := range controllerLines(h.log()) {
		if l["verb"] != "patch" || l["resource"] != "incidents/status" {
			t.Errorf("a controller line that is not a rule's status write: %v", l)
		}
	}
}

// The controller's identity is named in one place and set in one place: the constant, and
// applyStatus, which only a fired rule's timer calls. Nothing that reads a request (authenticate,
// impersonate, ServeHTTP, the handlers) refers to it, by the constant or by its value, so no
// credential of any shape can be given it. Read from the package's source.
func TestScriptedWrite_OnlyApplyStatusNamesTheController(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var constUses, applyUses, constValues, files int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files++
		for _, d := range f.Decls {
			where := "a declaration"
			if fd, ok := d.(*ast.FuncDecl); ok {
				where = "func " + fd.Name.Name
			}
			ast.Inspect(d, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.ValueSpec:
					for i, name := range x.Names {
						if name.Name != "labelController" {
							continue
						}
						if lit, ok := x.Values[i].(*ast.BasicLit); !ok || lit.Value != `"controller"` {
							t.Errorf("%s: labelController is not the string \"controller\"", fset.Position(x.Pos()))
						}
						constValues++
					}
				case *ast.BasicLit:
					if x.Kind != token.STRING {
						break
					}
					if v, err := strconv.Unquote(x.Value); err == nil && strings.EqualFold(strings.TrimSpace(v), "controller") && where != "a declaration" {
						t.Errorf("%s: %s spells the controller's identity itself", fset.Position(x.Pos()), where)
					}
				case *ast.Ident:
					if x.Name == "labelController" && where != "a declaration" {
						constUses++
						if where != "func applyStatus" {
							t.Errorf("%s: %s uses labelController; only applyStatus may", fset.Position(x.Pos()), where)
						}
					}
				case *ast.SelectorExpr:
					if x.Sel.Name == "applyStatus" {
						applyUses++
						if where != "func fireRules" {
							t.Errorf("%s: %s calls applyStatus; only a fired rule's timer (fireRules) may", fset.Position(x.Pos()), where)
						}
					}
				}
				return true
			})
		}
	}
	if files == 0 || constValues != 1 || constUses != 1 || applyUses != 1 {
		t.Fatalf("read %d files: labelController declared %d times, used %d times, applyStatus called %d times; want one of each", files, constValues, constUses, applyUses)
	}
}

// storeDigest is a digest of every stored object, by resource and key. Digests, not bodies: the
// store holds the visitor's Secret, which a failure message must not print.
func storeDigest(h *harness) map[string]string {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	out := map[string]string{}
	for gr, bucket := range h.s.store {
		for key, obj := range bucket {
			b, _ := json.Marshal(obj)
			out[gr+" "+key] = fmt.Sprintf("%x", sha256.Sum256(b))
		}
	}
	return out
}

// changed names the objects whose digest differs between two store digests.
func changed(before, after map[string]string) []string {
	out := []string{}
	for k, v := range after {
		if before[k] != v {
			out = append(out, k)
		}
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			out = append(out, k+" (gone)")
		}
	}
	sort.Strings(out)
	return out
}

// withRule replaces the fixture's rules with one, fired by the visitor's patch of the incident.
func withRule(t *testing.T, target any, status map[string]any, stamp ...any) *harness {
	return newHarness(t, opts{edit: func(s map[string]any) {
		s["scripted"] = map[string]any{"applyStatus": []any{map[string]any{
			"id":      "reach",
			"on":      map[string]any{"verb": "patch", "group": "observability.krateo.io", "resource": "incidents", "namespace": "krateo-system", "name": incident},
			"afterMs": 100, "target": target, "status": status, "stamp": stamp,
		}}}
	}})
}

func fire(h *harness) []map[string]any {
	h.t.Helper()
	eq(h.t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"poked": true}}}).code, 200, "the visitor's write")
	before := len(h.log())
	h.advance(time.Second)
	return h.log()[before:]
}

// A rule's write reaches its declared target's status and nothing else, however the rule is
// written: not a field outside .status, not another object, not a kind that has no status
// subresource (a Namespace, the visitor's Secret).
func TestScriptedWrite_TouchesOnlyItsTargetsStatus(t *testing.T) {
	const incKey = "observability.krateo.io/incidents krateo-system/" + incident
	const alertKey = "observability.krateo.io/alerts krateo-system/" + alertName

	t.Run("a stamp under /status is written, and only the status changes", func(t *testing.T) {
		h := withRule(t, "trigger", map[string]any{"state": "Resolved"}, "/status/at")
		before := h.do("GET", inc+"/"+incident, req{as: "sa"}).body
		if get(before, "status", "state") == "Resolved" {
			t.Fatal("the fixture's incident is already Resolved")
		}
		eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"poked": true}}}).code, 200, "the visitor's write")
		poked := h.do("GET", inc+"/"+incident, req{as: "sa"}).body
		store := storeDigest(h)
		mark := len(h.log())
		h.advance(time.Second)
		lines := h.log()[mark:]
		if f := failures(lines); len(f) != 0 {
			t.Fatalf("the write failed: %v", f)
		}
		if c := controllerLines(lines); len(c) != 1 || c[0]["status"] != 200 {
			t.Fatalf("want the write's one 200 line: %v", lines)
		}
		eq(t, changed(store, storeDigest(h)), []string{incKey}, "the objects the write changed")
		after := h.do("GET", inc+"/"+incident, req{as: "sa"}).body
		eq(t, get(after, "status", "state"), "Resolved", "the status was written")
		eq(t, get(after, "status", "at"), "2026-11-12T10:00:00Z", "the stamp under /status was set")
		// Outside .status and the resourceVersion a write bumps, the object is the one the visitor left.
		strip := func(o map[string]any) map[string]any {
			c := deepCopy(o).(map[string]any)
			delete(c, "status")
			delete(meta(c), "resourceVersion")
			return c
		}
		eq(t, strip(after), strip(poked), "everything outside .status")
	})

	// A stamp that names no field under /status is a defective rule: the write fails, loudly, with
	// the pointer named, and nothing at all is written: not the rule's status either, no event, no
	// request line. One rule for every pointer, whatever its parent holds: the incident's spec,
	// metadata and metadata.annotations all exist, so each of these stamps could otherwise be set.
	for _, c := range []struct {
		name    string
		stamps  []any
		pointer string
	}{
		{"into spec", []any{"/spec/x"}, "/spec/x"},
		{"into metadata.annotations", []any{"/metadata/annotations/x"}, "/metadata/annotations/x"},
		{"into metadata", []any{"/metadata/name"}, "/metadata/name"},
		{"the whole status, not a field of it", []any{"/status"}, "/status"},
		{"a member that only starts with status", []any{"/statusx/at"}, "/statusx/at"},
		{"the whole document", []any{""}, ""},
		{"after a stamp under /status: no partial write", []any{"/status/at", "/spec/x"}, "/spec/x"},
		{"before a stamp under /status: no partial write", []any{"/metadata/annotations/x", "/status/at"}, "/metadata/annotations/x"},
	} {
		t.Run("a stamp outside /status fails the write: "+c.name, func(t *testing.T) {
			h := newHarness(t, opts{
				pageEdit: func(objs []map[string]any) {
					for _, o := range objs {
						if o["kind"] == "Incident" {
							meta(o)["annotations"] = map[string]any{"seeded": "yes"}
						}
					}
				},
				edit: func(s map[string]any) {
					s["scripted"] = map[string]any{"applyStatus": []any{map[string]any{
						"id":      "reach",
						"on":      map[string]any{"verb": "patch", "group": "observability.krateo.io", "resource": "incidents", "namespace": "krateo-system", "name": incident},
						"afterMs": 100, "target": "trigger", "status": map[string]any{"state": "Resolved"}, "stamp": c.stamps,
					}}}
				}})
			seeded := h.do("GET", inc+"/"+incident, req{as: "sa"}).body
			eq(t, get(seeded, "metadata", "annotations", "seeded"), "yes", "the incident has annotations, so a stamp into them has a parent")
			if _, ok := seeded["spec"].(map[string]any); !ok {
				t.Fatal("the incident has no spec, so a stamp into it has no parent")
			}
			w := synced(h, inc)
			defer w.cancel()
			eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"poked": true}}}).code, 200, "the visitor's write")
			eq(t, w.next()["type"], "MODIFIED", "the visitor's event")
			store := storeDigest(h)
			mark := len(h.log())
			h.advance(time.Second)
			lines := h.log()[mark:]
			eq(t, lines, []map[string]any{{"event": "applyStatus.failed", "id": "reach", "pointer": c.pointer, "reason": "a stamp names a field under /status: a rule writes its target's status and nothing else"}}, "the lines the rule wrote: its one failure, naming the pointer, and no request line")
			eq(t, changed(store, storeDigest(h)), []string{}, "the objects the write changed")
			w.quiet() // no event
		})
	}

	t.Run("an explicit target is the only object written", func(t *testing.T) {
		h := withRule(t, map[string]any{"group": "observability.krateo.io", "resource": "alerts", "namespace": "krateo-system", "name": alertName}, map[string]any{"state": "Cleared"})
		eq(t, h.do("GET", alerts+"/"+alertName, req{as: "sa"}).code, 200, "the alert is there")
		h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"poked": true}}})
		store := storeDigest(h)
		mark := len(h.log())
		h.advance(time.Second)
		lines := h.log()[mark:]
		if f := failures(lines); len(f) != 0 {
			t.Fatalf("the write failed, so this arm shows nothing: %v", f)
		}
		c := controllerLines(lines)
		if len(c) != 1 {
			t.Fatalf("want the write's one line: %v", lines)
		}
		eq(t, c[0], map[string]any{"verb": "patch", "group": "observability.krateo.io", "version": "v1alpha1", "resource": "alerts/status", "namespace": "krateo-system", "name": alertName, "query": "", "status": 200, "items": 0, "identity": "controller"}, "the write's request line")
		eq(t, changed(store, storeDigest(h)), []string{alertKey}, "the objects the write changed: the alert, not the incident the rule fired on")
		eq(t, get(h.do("GET", alerts+"/"+alertName, req{as: "sa"}).body, "status", "state"), "Cleared", "the alert's status")
	})

	for name, c := range map[string]struct {
		target   map[string]any
		resource string
		get      string
	}{
		"a Namespace has no status subresource here": {map[string]any{"group": "", "resource": "namespaces", "namespace": "", "name": "krateo-system"}, "namespaces", "/api/v1/namespaces/krateo-system"},
		"the visitor's Secret":                       {map[string]any{"group": "", "resource": "secrets", "namespace": "krateo-system", "name": "sandbox-visitor-clientconfig"}, "secrets", visitorSecretPath},
	} {
		t.Run("refused: "+name, func(t *testing.T) {
			// The status and its stamp would land in the object's own fields were the write let through.
			h := withRule(t, c.target, map[string]any{"phase": "Terminating"}, "/status/at")
			// The target is stored (a missing one is refused for another reason): the ServiceAccount reads it.
			eq(t, h.do("GET", c.get, req{as: "sa"}).code, 200, "the target is there")
			h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"poked": true}}})
			store := storeDigest(h)
			mark := len(h.log())
			h.advance(time.Second)
			lines := h.log()[mark:]
			eq(t, changed(store, storeDigest(h)), []string{}, "the objects the write changed")
			cl := controllerLines(lines)
			if len(cl) != 1 {
				t.Fatalf("want the refused write's one request line: %v", lines)
			}
			eq(t, cl[0]["status"], 404, "the refused write's status: the path a rule writes does not exist for this kind")
			eq(t, cl[0]["resource"], c.resource+"/status", "the refused write's resource")
			f := failures(lines)
			if len(f) != 1 {
				t.Fatalf("want one applyStatus.failed: %v", lines)
			}
			eq(t, f[0], map[string]any{"event": "applyStatus.failed", "id": "reach", "pointer": "", "reason": c.resource + " has no status subresource: a rule writes its target's status and nothing else"}, "the failure line")
		})
	}
}

// A schema refusal is still the /status path's: 422 on the controller's line, applyStatus.failed,
// nothing written. (A failed stamp and a missing target: TestApplyStatusRuntimeFailure,
// TestRequestLog_EveryLineIsARequestLineOrAListedEvent.)
func TestScriptedWrite_SchemaRefusalIsStillA422(t *testing.T) {
	h := newHarness(t, opts{pageEdit: crUpdateSchema, edit: func(s map[string]any) {
		rule := get(s, "scripted").(map[string]any)["applyStatus"].([]any)[0].(map[string]any)
		rule["status"] = map[string]any{"state": "bogus"}
		rule["stamp"] = []any{}
	}})
	requireReadOnlyServiceAccount(t, h)
	before := h.do("GET", inc+"/"+incident, req{as: "sa"}).body
	eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}}).code, 200, "the visitor's write")
	mark := len(h.log())
	h.advance(2 * time.Second)
	lines := h.log()[mark:]
	c := controllerLines(lines)
	if len(c) != 1 {
		t.Fatalf("want the write's one request line: %v", lines)
	}
	eq(t, c[0]["status"], 422, "the refused write's status")
	f := failures(lines)
	if len(f) != 1 || !strings.Contains(fmt.Sprint(f[0]["reason"]), `status.state: Unsupported value: "bogus"`) {
		t.Fatalf("want one applyStatus.failed naming the schema's refusal: %v", lines)
	}
	eq(t, get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "status"), get(before, "status"), "nothing written")
}
