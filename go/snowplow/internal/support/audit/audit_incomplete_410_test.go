package audit

// audit_incomplete_410_test.go — AUDIT-4 (krateo-platformops/installer#410)
// criterion 5: "no record is emitted without an actor; prefer no record to a
// recordless actor". The live trail on krateo-057 held one record carrying only
// event.name, outcome and session.id (no method, no resource, no actor). It was
// not written by snowplow (ServiceName "audit-filter-probe"), but nothing in the
// emitter would have refused it either.
//
// The contract, enforced in Emitter.Emit:
//   - no actor → no record, counted under "actor";
//   - an actor but a missing action/method/target → the record IS emitted,
//     marked krateo.audit.incomplete=<fields>, and each field is counted.

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/log/logtest"
)

// complete410 is an answerable record: actor (from ctx), action, method, target.
func complete410() Event {
	return Event{
		Action: "restaction", Verb: "POST", Group: "composition.krateo.io", Version: "v1alpha1",
		Resource: "fireworksapps", Name: "demo", Namespace: "team-a", Outcome: "success", Code: 200,
	}
}

// emit410 emits ev under ctx and returns each record's attributes.
func emit410(t *testing.T, ctx context.Context, ev Event) []map[string]string {
	t.Helper()
	rec := logtest.NewRecorder()
	New(rec.Logger("test")).Emit(ctx, ev)
	var out []map[string]string
	for _, r := range flattenRecords(rec.Result()) {
		m := map[string]string{}
		for _, kv := range r.Attributes {
			m[string(kv.Key)] = kv.Value.String()
		}
		out = append(out, m)
	}
	return out
}

// TestAudit410_RecordlessActorShape_IsNotEmitted is the 057 shape exactly: no
// actor on ctx, only outcome and a session id. It must produce no record, and the
// refusal must be counted under "actor".
func TestAudit410_RecordlessActorShape_IsNotEmitted(t *testing.T) {
	before := IncompleteCount(IncompleteActor)
	ctx := WithSessionID(context.Background(), "probe-1")
	if got := emit410(t, ctx, Event{Outcome: "success"}); len(got) != 0 {
		t.Fatalf("a record with only outcome + session.id was emitted (%v); it must not be", got)
	}
	if d := IncompleteCount(IncompleteActor) - before; d != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", d)
	}
}

// TestAudit410_NoActor_RefusesEvenAnOtherwiseCompleteRecord: the actor is the
// one field whose absence refuses the record.
func TestAudit410_NoActor_RefusesEvenAnOtherwiseCompleteRecord(t *testing.T) {
	before := IncompleteCount(IncompleteActor)
	if got := emit410(t, context.Background(), complete410()); len(got) != 0 {
		t.Fatalf("a complete record with no actor was emitted (%v); it must not be", got)
	}
	if d := IncompleteCount(IncompleteActor) - before; d != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", d)
	}
}

// TestAudit410_MissingNonActorField_EmitsMarkedIncomplete removes action,
// method or target from a record that has an actor. Each is emitted, marked
// with the missing field(s), and counted; the complete control carries no mark.
func TestAudit410_MissingNonActorField_EmitsMarkedIncomplete(t *testing.T) {
	control := emit410(t, actorCtx(), complete410())
	if len(control) != 1 {
		t.Fatalf("control: a complete record emitted %d records, want 1", len(control))
	}
	if v, ok := control[0]["krateo.audit.incomplete"]; ok {
		t.Fatalf("control: a complete record is marked incomplete=%q", v)
	}

	cases := []struct {
		name   string
		mutate func(*Event)
		mark   string
		counts []string
	}{
		{"action", func(e *Event) { e.Action = "" }, "action", []string{IncompleteAction}},
		{"method", func(e *Event) { e.Verb = "" }, "method", []string{IncompleteMethod}},
		{"target", func(e *Event) { e.Resource = "" }, "target", []string{IncompleteTarget}},
		{"method+target", func(e *Event) { e.Verb, e.Resource = "", "" }, "method,target", []string{IncompleteMethod, IncompleteTarget}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := complete410()
			c.mutate(&ev)
			before := map[string]int64{}
			for _, f := range c.counts {
				before[f] = IncompleteCount(f)
			}
			actorBefore := IncompleteCount(IncompleteActor)

			got := emit410(t, actorCtx(), ev)
			if len(got) != 1 {
				t.Fatalf("missing %s with an actor: %d records, want 1 (emitted, marked)", c.name, len(got))
			}
			if m := got[0]["krateo.audit.incomplete"]; m != c.mark {
				t.Errorf("krateo.audit.incomplete = %q, want %q", m, c.mark)
			}
			if got[0]["enduser.id"] != "alice" {
				t.Errorf("enduser.id = %q, want alice", got[0]["enduser.id"])
			}
			for _, f := range c.counts {
				if d := IncompleteCount(f) - before[f]; d != 1 {
					t.Errorf("incomplete{%s} moved by %d, want 1", f, d)
				}
			}
			if d := IncompleteCount(IncompleteActor) - actorBefore; d != 0 {
				t.Errorf("incomplete{actor} moved by %d for a record that has an actor", d)
			}
		})
	}
}

// TestAudit410_ServerAddressIsATarget: a RESTAction stage that writes to an
// external API has no Kubernetes resource; the host it was sent to is its
// target, and the record carries the stage's RESTAction attributes and no path.
func TestAudit410_ServerAddressIsATarget(t *testing.T) {
	got := emit410(t, actorCtx(), Event{
		Action: "restaction", Verb: "POST", ServerAddress: "render.example:8080",
		RESTActionNamespace: "krateo-system", RESTActionName: "blueprint-render", Stage: "render",
		Outcome: "success", Code: 201,
	})
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	want := map[string]string{
		"server.address":              "render.example:8080",
		"krateo.restaction.namespace": "krateo-system",
		"krateo.restaction.name":      "blueprint-render",
		"krateo.restaction.stage":     "render",
		"enduser.id":                  "alice",
		"user.name":                   "alice",
		"http.request.method":         "POST",
		"http.response.status_code":   "201",
	}
	for k, v := range want {
		if got[0][k] != v {
			t.Errorf("%s = %q, want %q", k, got[0][k], v)
		}
	}
	for _, k := range []string{"k8s.resource.resource", "url.path", "krateo.audit.incomplete"} {
		if v, ok := got[0][k]; ok {
			t.Errorf("%s must be absent, got %q", k, v)
		}
	}
}
