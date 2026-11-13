//go:build js && wasm

package fakeapiserver

// The request log's contract (sandbox SNAPSHOT.md §4, read by capture's §11.2 step 6.1): every line
// the fake writes is either a request line with exactly the 10 keys verb, group, version, resource,
// namespace, name, query, status, items and identity, or one of the two event lines with exactly its
// own keys. The run below drives every kind of request the fake answers.

import (
	"sort"
	"strings"
	"testing"
	"time"
)

var requestLineKeys = []string{"group", "identity", "items", "name", "namespace", "query", "resource", "status", "verb", "version"}

// eventLineKeys are the fake's event lines and the exact keys of each.
var eventLineKeys = map[string][]string{
	"applyStatus.failed": {"event", "id", "pointer", "reason"},
	"fake.unimplemented": {"event", "what"},
}

func keysOf(l map[string]any) []string {
	var ks []string
	for k := range l {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// lineShapeErrors names every line that is neither shape.
func lineShapeErrors(lines []map[string]any) []string {
	var bad []string
	for _, l := range lines {
		if ev, ok := l["event"].(string); ok {
			want, known := eventLineKeys[ev]
			if !known {
				bad = append(bad, "an event line of no listed kind: "+strings.Join(keysOf(l), ","))
			} else if strings.Join(keysOf(l), ",") != strings.Join(want, ",") {
				bad = append(bad, ev+" with keys "+strings.Join(keysOf(l), ",")+", want "+strings.Join(want, ","))
			}
			continue
		}
		if strings.Join(keysOf(l), ",") != strings.Join(requestLineKeys, ",") {
			bad = append(bad, "a request line with keys "+strings.Join(keysOf(l), ","))
			continue
		}
		if _, ok := l["status"].(int); !ok {
			bad = append(bad, "a request line whose status is not an int")
		}
		if _, ok := l["items"].(int); !ok {
			bad = append(bad, "a request line whose items is not an int")
		}
	}
	return bad
}

func TestRequestLog_EveryLineIsARequestLineOrAListedEvent(t *testing.T) {
	h := newHarness(t, opts{extra: append(append([]map[string]any{}, crSAAll...), nsPatchGrant...)})
	cm := "/api/v1/namespaces/krateo-system/configmaps"
	// discovery
	h.do("GET", "/api", req{as: "sa"})
	h.do("GET", "/apis", req{as: "sa"})
	h.do("GET", "/apis/observability.krateo.io/v1alpha1", req{as: "sa"})
	h.do("GET", "/version", req{as: "sa"})
	// reads
	h.do("GET", inc+"/"+incident, req{as: "sa"})
	h.do("GET", inc, req{as: "sa"})
	w := h.watch(inc+"?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true", req{as: "sa"})
	for w.next()["type"] != "BOOKMARK" {
	}
	w.cancel()
	// writes, a dry run among them
	h.do("POST", cm, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}})
	h.do("POST", cm+"?dryRun=All", req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}}})
	cur := h.do("GET", cm+"/a", req{as: "sa"})
	h.do("PUT", cm+"/a", req{as: "sa", ct: "application/json", body: cur.body})
	h.do("PATCH", cm+"/a", req{as: "sa", ct: merge, body: map[string]any{"data": map[string]any{"k": "v"}}})
	h.do("DELETE", cm+"/a", req{as: "sa"})
	// a review
	h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", req{as: "visitor", ct: "application/json", body: map[string]any{"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview", "spec": map[string]any{"resourceAttributes": map[string]any{"verb": "get", "resource": "secrets", "namespace": "krateo-system"}}}})
	// errors: 401 (a bad token), 403, 404, 415, 422, 400, and an unimplemented request (500 and its event line)
	h.do("GET", inc, req{as: "nope"})
	h.do("GET", "/api/v1/namespaces/krateo-system/secrets/x", req{as: "visitor"})
	h.do("GET", inc+"/missing", req{as: "sa"})
	h.do("POST", cm, req{as: "sa", ct: "text/plain", body: "x"})
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "application/apply-patch+yaml", body: "{}"})
	h.do("GET", inc+"?fieldSelector=spec.x%3Dy", req{as: "sa"})
	h.do("PATCH", "/api/v1/namespaces/krateo-system", req{as: "visitor", ct: "application/strategic-merge-patch+json", body: map[string]any{"metadata": map[string]any{"labels": map[string]any{"a": "b"}}}})
	// the scripted status write, applied
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
	h.advance(2 * time.Second)

	// the scripted status write, failed (applyStatus.failed)
	f := newHarness(t, opts{edit: func(s map[string]any) {
		rule := get(s, "scripted").(map[string]any)["applyStatus"].([]any)[0].(map[string]any)
		rule["stamp"] = []any{"/status/state/at"}
	}})
	f.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
	f.advance(2 * time.Second)

	lines := append(h.log(), f.log()...)
	for _, b := range lineShapeErrors(lines) {
		t.Error(b)
	}
	// The run covered what it claims: every verb, the error statuses and both event lines.
	seenVerb, seenStatus, seenEvent := map[string]bool{}, map[int]bool{}, map[string]bool{}
	for _, l := range lines {
		if ev, ok := l["event"].(string); ok {
			seenEvent[ev] = true
			continue
		}
		seenVerb[l["verb"].(string)] = true
		if s, ok := l["status"].(int); ok {
			seenStatus[s] = true
		}
	}
	for _, v := range []string{"get", "list", "watch", "create", "update", "patch", "delete", "review"} {
		if !seenVerb[v] {
			t.Errorf("the run made no %s", v)
		}
	}
	for _, s := range []int{200, 201, 400, 401, 403, 404, 415, 422, 500} {
		if !seenStatus[s] {
			t.Errorf("the run saw no %d", s)
		}
	}
	for ev := range eventLineKeys {
		if !seenEvent[ev] {
			t.Errorf("the run wrote no %s line", ev)
		}
	}
	if !logged(h, statusWrite) {
		t.Error("the applied status write has no request line")
	}
	// The network seam's egress lines are the bridge's, on their own channel: never the fake's.
	if logged(h, func(l map[string]any) bool { return l["event"] == "egress" }) {
		t.Error("an egress line in the fake's request log")
	}
}

// The check bites: an extra key, a missing items, an unknown event and an event with an extra key
// are each refused.
func TestRequestLog_ShapeCheckRefusesEachDeviation(t *testing.T) {
	good := requestLine("")
	good["status"] = 200
	for name, l := range map[string]map[string]any{
		"an extra key":            func() map[string]any { m := requestLine(""); m["status"] = 200; m["contentType"] = "x"; return m }(),
		"no items":                func() map[string]any { m := requestLine(""); m["status"] = 200; delete(m, "items"); return m }(),
		"an unlisted event":       {"event": "applyStatus.applied", "id": "r", "pointer": ""},
		"an event with extra key": {"event": "fake.unimplemented", "what": "x", "status": 500},
	} {
		if len(lineShapeErrors([]map[string]any{l})) == 0 {
			t.Errorf("%s passed the shape check", name)
		}
	}
	if b := lineShapeErrors([]map[string]any{good, {"event": "fake.unimplemented", "what": "x"}}); len(b) != 0 {
		t.Errorf("good lines refused: %v", b)
	}
}
