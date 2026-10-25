package audit

// audit_incomplete_410_test.go — AUDIT-4 (krateo-platformops/installer#410)
// criterion 5: "no record is emitted without an actor; prefer no record to a
// recordless actor". The live trail on krateo-057 held one record carrying only
// event.name, outcome and session.id (no method, no resource, no actor). It was
// not written by snowplow (ServiceName "audit-filter-probe"), but nothing in the
// emitter would have refused it either: these arms make that shape impossible
// from snowplow, at the one choke point every emit site passes.

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

func emitCount410(t *testing.T, ctx context.Context, ev Event) int {
	t.Helper()
	rec := logtest.NewRecorder()
	New(rec.Logger("test")).Emit(ctx, ev)
	return len(flattenRecords(rec.Result()))
}

// TestAudit410_RecordlessActorShape_IsNotEmitted is the 057 shape exactly: no
// actor on ctx, only outcome and a session id. It must produce no record, and the
// refusal must be counted under "actor".
func TestAudit410_RecordlessActorShape_IsNotEmitted(t *testing.T) {
	before := IncompleteCount(IncompleteActor)
	ctx := WithSessionID(context.Background(), "probe-1")
	if n := emitCount410(t, ctx, Event{Outcome: "success"}); n != 0 {
		t.Fatalf("a record with only outcome + session.id was emitted (%d records); it must not be", n)
	}
	if got := IncompleteCount(IncompleteActor) - before; got != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", got)
	}
}

// TestAudit410_EachMissingField_RefusesTheRecord removes one field at a time
// from a complete record. Each must be refused and counted under its own reason;
// the complete control must be emitted (so a refusal is not just a dead emitter).
func TestAudit410_EachMissingField_RefusesTheRecord(t *testing.T) {
	if n := emitCount410(t, actorCtx(), complete410()); n != 1 {
		t.Fatalf("control: a complete record emitted %d records, want 1", n)
	}

	cases := []struct {
		reason string
		ctx    context.Context
		mutate func(*Event)
	}{
		{IncompleteActor, context.Background(), func(*Event) {}},
		{IncompleteAction, actorCtx(), func(e *Event) { e.Action = "" }},
		{IncompleteMethod, actorCtx(), func(e *Event) { e.Verb = "" }},
		{IncompleteTarget, actorCtx(), func(e *Event) { e.Resource = "" }},
	}
	for _, c := range cases {
		t.Run(c.reason, func(t *testing.T) {
			ev := complete410()
			c.mutate(&ev)
			before := IncompleteCount(c.reason)
			if n := emitCount410(t, c.ctx, ev); n != 0 {
				t.Fatalf("missing %s: %d records emitted, want 0", c.reason, n)
			}
			if got := IncompleteCount(c.reason) - before; got != 1 {
				t.Fatalf("incomplete{%s} moved by %d, want 1", c.reason, got)
			}
		})
	}
}

// TestAudit410_ServerAddressIsATarget: a RESTAction stage that writes to an
// external API has no Kubernetes resource; the host it was sent to is its target,
// and the record carries the stage's RESTAction attributes.
func TestAudit410_ServerAddressIsATarget(t *testing.T) {
	rec := logtest.NewRecorder()
	New(rec.Logger("test")).Emit(actorCtx(), Event{
		Action: "restaction", Verb: "POST", ServerAddress: "render.example:8080", URLPath: "/render",
		RESTActionNamespace: "krateo-system", RESTActionName: "blueprint-render", Stage: "render",
		Outcome: "success", Code: 200,
	})
	got := flattenRecords(rec.Result())
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	attrs := map[string]string{}
	for _, kv := range got[0].Attributes {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	want := map[string]string{
		"server.address":              "render.example:8080",
		"url.path":                    "/render",
		"krateo.restaction.namespace": "krateo-system",
		"krateo.restaction.name":      "blueprint-render",
		"krateo.restaction.stage":     "render",
		"enduser.id":                  "alice",
		"user.name":                   "alice",
		"http.request.method":         "POST",
	}
	for k, v := range want {
		if attrs[k] != v {
			t.Errorf("%s = %q, want %q", k, attrs[k], v)
		}
	}
	if _, ok := attrs["k8s.resource.resource"]; ok {
		t.Errorf("an external target must not carry k8s.resource.resource, got %q", attrs["k8s.resource.resource"])
	}
}
