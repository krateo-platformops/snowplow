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
//      the emitter can write. The attribute set is read back from a record
//      with EVERY Event field set (by reflection), not from a list here, so a
//      new field that reaches the record and not the page turns this red.

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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
			ev := Event{Verb: c.verb, Outcome: c.outcome}
			ev.RecordWrittenObject([]byte(c.body))
			if got := identityOf957(ev); got != c.want {
				t.Fatalf("identity = %+v, want %+v", got, c.want)
			}
		})
	}
	// A dry run: the same successful answers record nothing. A dry-run update
	// answers with the real uid and the generation the change would reach.
	for _, verb := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
		ev := Event{Verb: verb, Outcome: "success", DryRun: true}
		ev.RecordWrittenObject([]byte(object957))
		if got := identityOf957(ev); got != (identity957{}) {
			t.Errorf("dry-run %s: identity = %+v, want none", verb, got)
		}
	}
	for _, body := range []string{deleting957, deletedStatus957} {
		ev := Event{Verb: http.MethodDelete, Outcome: "success", DryRun: true}
		ev.RecordWrittenObject([]byte(body))
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
			Action: c.action, DryRun: c.dry, Verb: http.MethodPatch, Resource: "fireworksapps", Outcome: "success",
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
		Action: "call", Verb: http.MethodPut, Resource: "fireworksapps", Outcome: "success",
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
		Action: "call", Verb: http.MethodPut, Resource: "fireworksapps", Outcome: "failure", Code: 403,
	})
	got2 := attrsOf957(t, rec2)
	for _, k := range []string{"k8s.object.uid", "k8s.object.generation", "k8s.object.resource_version", "krateo.audit.incomplete"} {
		if v, ok := got2[k]; ok {
			t.Errorf("a record with no written object carries %s=%v", k, v)
		}
	}
}

// fillEveryField sets every exported field of ev to a non-zero value, so a
// record emitted from it carries every attribute an Event can produce.
func fillEveryField(t *testing.T, ev *Event) {
	t.Helper()
	v := reflect.ValueOf(ev).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString("x-" + v.Type().Field(i).Name)
		case reflect.Int, reflect.Int64:
			f.SetInt(1)
		case reflect.Bool:
			f.SetBool(true)
		default:
			t.Fatalf("Event.%s has kind %v: teach fillEveryField to set it, so docs parity covers it", v.Type().Field(i).Name, f.Kind())
		}
	}
}

func TestWrittenObject957_D_AuditCoverageDocNamesEveryAttribute(t *testing.T) {
	rec := logtest.NewRecorder()
	var ev Event
	fillEveryField(t, &ev)
	ev.Outcome = "failure" // outcome is a closed set; any value reaches the record
	// The context carries what the emitter reads besides the Event: the actor
	// and groups (actorCtx), the session id. It has no span, so the
	// no-trace-context marker is written too. krateo.audit.incomplete needs a
	// missing field: emit a second record without a verb.
	ctx := WithSessionID(actorCtx(), "s-957")
	New(rec.Logger("test-957")).Emit(ctx, ev)
	ev.Verb = ""
	New(rec.Logger("test-957")).Emit(ctx, ev)

	keys := map[string]bool{}
	for _, r := range flattenRecords(rec.Result()) {
		for _, kv := range r.Attributes {
			keys[string(kv.Key)] = true
		}
	}
	for _, must := range []string{"k8s.object.uid", "k8s.object.generation", "k8s.object.resource_version", "krateo.audit.incomplete"} {
		if !keys[must] {
			t.Fatalf("precondition: the fully-set records do not carry %s (got %v)", must, keys)
		}
	}

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

// The package-level Emit is what every site calls; it must carry the fields too.
func TestWrittenObject957_K_PackageEmitCarriesThem(t *testing.T) {
	rec := logtest.NewRecorder()
	SetDefault(New(rec.Logger("test-957")))
	t.Cleanup(func() { SetDefault(nil) })
	ev := Event{Action: "call", Verb: http.MethodPost, Resource: "fireworksapps", Outcome: "success"}
	ev.RecordWrittenObject([]byte(object957))
	Emit(context.Background(), ev) // no actor: refused
	if n := len(flattenRecords(rec.Result())); n != 0 {
		t.Fatalf("an actorless record was emitted (%d)", n)
	}
	Emit(actorCtx(), ev)
	got := attrsOf957(t, rec)
	if got["k8s.object.uid"].AsString() != "uid-resp-957" || got["k8s.object.generation"].AsInt64() != 7 ||
		got["k8s.object.resource_version"].AsString() != "502" {
		t.Fatalf("record = %v, want the response's uid/generation/resourceVersion", got)
	}
}
