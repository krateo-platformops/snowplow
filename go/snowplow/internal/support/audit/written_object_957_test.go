package audit

// written_object_957_test.go — #957 (krateo-acmp#59 Option B): a successful
// Kubernetes write records the written object's uid, generation and
// resourceVersion as the apiserver RETURNED them.
//
//   R  RecordWrittenObject reads the response: per verb, the three fields equal
//      the response's metadata; a DELETE keeps only the uid (of the returned
//      object, or details.uid of a Status); a failure, a read and a body that
//      is not one JSON object record none.
//   K  the emitter writes them under the shared contract's keys
//      (github.com/krateo-platformops/audit v0.1.0: k8s.object.uid,
//      k8s.object.generation as an int64, k8s.object.resource_version), and
//      omits each one that is unset.
//   D  docs parity: docs/architecture/audit-coverage.md names every attribute
//      the emitter adds on a path an emit site cannot see (a delegated actor,
//      a gap, a method semconv does not name, ...). The attribute set is read
//      back from the records, not from a list here. Each emit site's own
//      record shapes are held to the same page by the #1291 parity arms that
//      drive the real sites (handlers, the RESTAction stage).

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/logtest"
)

const (
	object957 = `{"apiVersion":"composition.krateo.io/v1alpha1","kind":"FireworksApp",` +
		`"metadata":{"name":"demo","namespace":"team-a","uid":"uid-resp-957","generation":7,"resourceVersion":"502"},` +
		`"details":"a top-level field of the object, not a Status's details","spec":{"replicas":2}}`
	deleting957 = `{"apiVersion":"composition.krateo.io/v1alpha1","kind":"FireworksApp",` +
		`"metadata":{"name":"demo","uid":"uid-resp-957","generation":8,"resourceVersion":"503",` +
		`"deletionTimestamp":"2026-10-28T10:00:00Z","finalizers":["composition.krateo.io/finalizer"]}}`
	deletedStatus957 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Success",` +
		`"details":{"name":"demo","group":"composition.krateo.io","kind":"fireworksapps","uid":"uid-gone-957"}}`
	forbidden957 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"Forbidden","code":403,` +
		`"details":{"name":"demo","kind":"fireworksapps","uid":"uid-should-not-957"}}`
)

type identity957 struct {
	uid string
	gen int64
	rv  string
}

func identityOf957(ev Event) identity957 {
	return identity957{ev.ObjectUID, ev.ObjectGeneration, ev.ObjectResourceVersion}
}

func TestWrittenObject957_R_ReadsTheResponse(t *testing.T) {
	full := identity957{"uid-resp-957", 7, "502"}
	cases := []struct {
		name, verb, outcome, body string
		want                      identity957
	}{
		{"create", http.MethodPost, "success", object957, full},
		{"update", http.MethodPut, "success", object957, full},
		{"patch", http.MethodPatch, "success", object957, full},
		{"delete pending: the returned object's uid only", http.MethodDelete, "success", deleting957, identity957{uid: "uid-resp-957"}},
		{"delete done: the Status's details.uid only", http.MethodDelete, "success", deletedStatus957, identity957{uid: "uid-gone-957"}},
		{"failure carrying an object", http.MethodPut, "failure", object957, identity957{}},
		{"failed delete carrying a Status with a uid", http.MethodDelete, "failure", forbidden957, identity957{}},
		{"denied", http.MethodPatch, "denied", object957, identity957{}},
		{"a read", http.MethodGet, "success", object957, identity957{}},
		{"no body (204)", http.MethodPost, "success", "", identity957{}},
		{"null body", http.MethodPost, "success", "null", identity957{}},
		{"not an object", http.MethodPost, "success", `[{"metadata":{"uid":"x"}}]`, identity957{}},
		{"not JSON", http.MethodPost, "success", `<html>ok</html>`, identity957{}},
		{"an object without metadata", http.MethodPost, "success", `{"ok":true}`, identity957{}},
		{"a Status's details.uid on a non-DELETE", http.MethodPost, "success", deletedStatus957, identity957{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := Event{HTTPMethod: c.verb, Outcome: c.outcome}
			RecordWrittenObject(&ev, []byte(c.body))
			if got := identityOf957(ev); got != c.want {
				t.Fatalf("identity = %+v, want %+v", got, c.want)
			}
		})
	}
	// A dry run: the same successful answers record nothing. A dry-run update
	// answers with the real uid and the generation the change would reach.
	for _, verb := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
		ev := Event{HTTPMethod: verb, Outcome: "success", DryRun: true}
		RecordWrittenObject(&ev, []byte(object957))
		if got := identityOf957(ev); got != (identity957{}) {
			t.Errorf("dry-run %s: identity = %+v, want none", verb, got)
		}
	}
	for _, body := range []string{deleting957, deletedStatus957} {
		ev := Event{HTTPMethod: http.MethodDelete, Verb: "delete", Outcome: "success", DryRun: true}
		RecordWrittenObject(&ev, []byte(body))
		if got := identityOf957(ev); got != (identity957{}) {
			t.Errorf("dry-run DELETE: identity = %+v, want none", got)
		}
	}
}

// The dry-run mark: krateo.action carries DryRunSuffix; a write that is not a
// dry run keeps its action; an absent action stays absent (and missing).
func TestWrittenObject957_DryRunMarksTheAction(t *testing.T) {
	cases := []struct {
		action string
		dry    bool
		want   string
	}{
		{"call", true, "call.dryRun"},
		{"restaction", true, "restaction.dryRun"},
		{"restaction", false, "restaction"},
		{"", true, ""},
	}
	for _, c := range cases {
		rec := logtest.NewRecorder()
		New(rec.Logger("test-957")).Emit(actorCtx(), Event{
			Action: c.action, DryRun: c.dry, HTTPMethod: http.MethodPatch, Verb: "patch", Resource: "fireworksapps", Outcome: "success",
		})
		got := attrsOf957(t, rec)
		v, has := got["krateo.action"]
		if c.want == "" {
			if has {
				t.Errorf("action %q dry=%t: krateo.action = %v, want absent", c.action, c.dry, v)
			}
			if got["krateo.audit.incomplete"].AsString() != IncompleteAction {
				t.Errorf("action %q dry=%t: incomplete = %v, want %q", c.action, c.dry, got["krateo.audit.incomplete"], IncompleteAction)
			}
			continue
		}
		if v.AsString() != c.want {
			t.Errorf("action %q dry=%t: krateo.action = %q, want %q", c.action, c.dry, v.AsString(), c.want)
		}
	}
}

// attrsOf957 is the attribute map of the only record rec holds, values typed.
func attrsOf957(t *testing.T, rec *logtest.Recorder) map[string]attribute.Value {
	t.Helper()
	recs := flattenRecords(rec.Result())
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	out := map[string]attribute.Value{}
	for _, kv := range recs[0].Attributes {
		out[string(kv.Key)] = kv.Value
	}
	return out
}

func TestWrittenObject957_K_SharedContractKeys(t *testing.T) {
	rec := logtest.NewRecorder()
	e := New(rec.Logger("test-957"))
	e.Emit(actorCtx(), Event{
		Action: "call", HTTPMethod: http.MethodPut, Verb: "update", Resource: "fireworksapps", Outcome: "success",
		ObjectUID: "uid-resp-957", ObjectGeneration: 7, ObjectResourceVersion: "502",
	})
	got := attrsOf957(t, rec)
	if v := got["k8s.object.uid"]; v.Type() != attribute.STRING || v.AsString() != "uid-resp-957" {
		t.Errorf("k8s.object.uid = %v (%v), want the string uid-resp-957", v, v.Type())
	}
	if v := got["k8s.object.generation"]; v.Type() != attribute.INT64 || v.AsInt64() != 7 {
		t.Errorf("k8s.object.generation = %v (%v), want the int64 7 (the shared emitter writes an Int64)", v, v.Type())
	}
	if v := got["k8s.object.resource_version"]; v.Type() != attribute.STRING || v.AsString() != "502" {
		t.Errorf("k8s.object.resource_version = %v (%v), want the string 502", v, v.Type())
	}
	if v, ok := got["krateo.audit.incomplete"]; ok {
		t.Errorf("a complete record is marked incomplete=%v", v)
	}

	// Unset: none of the three keys, and the record is not incomplete for it.
	rec2 := logtest.NewRecorder()
	New(rec2.Logger("test-957")).Emit(actorCtx(), Event{
		Action: "call", HTTPMethod: http.MethodPut, Verb: "update", Resource: "fireworksapps", Outcome: "failure", StatusCode: 403,
	})
	got2 := attrsOf957(t, rec2)
	for _, k := range []string{"k8s.object.uid", "k8s.object.generation", "k8s.object.resource_version", "krateo.audit.incomplete"} {
		if v, ok := got2[k]; ok {
			t.Errorf("a record with no written object carries %s=%v", k, v)
		}
	}
}

// docKeys957 is every attribute key of the records rec holds.
func docKeys957(rec *logtest.Recorder) map[string]bool {
	keys := map[string]bool{}
	for _, r := range flattenRecords(rec.Result()) {
		for _, kv := range r.Attributes {
			keys[string(kv.Key)] = true
		}
	}
	return keys
}

// AssertDocNamesKeys fails unless docs/architecture/audit-coverage.md names
// (in backticks) every key in keys.
func assertDocNamesKeys957(t *testing.T, keys map[string]bool) {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "architecture", "audit-coverage.md"))
	if err != nil {
		t.Fatalf("read audit-coverage.md: %v", err)
	}
	var missing []string
	for k := range keys {
		if !strings.Contains(string(doc), "`"+k+"`") {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("docs/architecture/audit-coverage.md does not name (in backticks) the attribute(s) %v that the emitter writes", missing)
	}
}

// D — what the emitter adds to an emit site's record on every path an emit
// site cannot see: a delegated actor (on_behalf_of, the act chain), the
// request facts and session id, a missing key (krateo.audit.incomplete), no
// trace context, a method semconv does not name (method_original), a preview
// flag. The emit sites' own record shapes are held to the same page by the
// parity arms that drive them (handlers and the RESTAction stage, #1291).
func TestWrittenObject957_D_AuditCoverageDocNamesEveryAttribute(t *testing.T) {
	rec := logtest.NewRecorder()
	ev := Event{
		Action: "call", HTTPMethod: http.MethodPut, Verb: "update", Outcome: "failure", StatusCode: 403, Message: "m",
		Group: "g.example", Version: "v1", Resource: "things", Subresource: "status", ObjectName: "n", Namespace: "ns",
		ObjectUID: "u", ObjectGeneration: 1, ObjectResourceVersion: "1", DryRun: true,
		RESTActionNamespace: "rns", RESTActionName: "ra", RESTActionStage: "s",
	}
	req := httptest.NewRequest(http.MethodPost, "/call?name=x", nil)
	req.Header.Set("User-Agent", "ua-957")
	ctx := withReceivedRequest(WithSessionID(delegatedCtx1291(), "s-957"), req)
	e := New(rec.Logger("test-957"))
	e.Emit(ctx, ev)
	ev.Verb, ev.HTTPMethod = "", "mkcol"
	e.Emit(ctx, ev)
	ev.Resource, ev.Namespace, ev.ServerAddress = "", "", "render.example"
	e.Emit(ctx, ev)

	keys := docKeys957(rec)
	for _, must := range []string{"k8s.object.uid", "k8s.object.generation", "k8s.object.resource_version", "krateo.audit.incomplete",
		AttrSourceIPs, AttrUserAgent, AttrRequestURI, "krateo.audit.on_behalf_of.user.name", "krateo.audit.delegation",
		"krateo.audit.preview", "http.request.method_original", "server.address", "krateo.audit.dry_run"} {
		if !keys[must] {
			t.Fatalf("precondition: the records do not carry %s (got %v)", must, keys)
		}
	}
	assertDocNamesKeys957(t, keys)
}

// The package-level Emit is what every site calls; it must carry the fields too.
func TestWrittenObject957_K_PackageEmitCarriesThem(t *testing.T) {
	rec := logtest.NewRecorder()
	SetDefault(New(rec.Logger("test-957")))
	t.Cleanup(func() { SetDefault(nil) })
	ev := Event{Action: "call", HTTPMethod: http.MethodPost, Verb: "create", Resource: "fireworksapps", Outcome: "success"}
	RecordWrittenObject(&ev, []byte(object957))
	Emit(actorCtx(), ev)
	got := attrsOf957(t, rec)
	if got["k8s.object.uid"].AsString() != "uid-resp-957" || got["k8s.object.generation"].AsInt64() != 7 ||
		got["k8s.object.resource_version"].AsString() != "502" {
		t.Fatalf("record = %v, want the response's uid/generation/resourceVersion", got)
	}
}
