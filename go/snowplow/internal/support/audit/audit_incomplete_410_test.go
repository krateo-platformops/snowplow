package audit

// audit_incomplete_410_test.go — AUDIT-4 (krateo-platformops/installer#410)
// criterion 5, as the shared schema settles it (#1291, audit#7): a record is
// never dropped. The live trail on krateo-057 held one record carrying only
// event.name, outcome and session.id (no method, no resource, no actor). It was
// not written by snowplow (ServiceName "audit-filter-probe"). Until #1291,
// snowplow refused a record with no actor; the shared writer emits it instead,
// marked, so the gap is in the trail and in the count, never hidden:
//
//   - no actor → the record IS emitted, marked krateo.audit.incomplete=actor
//     (and actor_class: no actor, no class), and counted under "actor";
//   - an actor but a missing action/verb/method/target → emitted, marked with
//     each missing marker, each counted.
//
// The count is krateo.audit.records.incomplete{marker}, on /debug/vars
// (IncompleteCount) and OTLP (audit_1291_test.go, M arm).

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/log/logtest"
)

// complete410 is an answerable record: actor (from ctx), action, verb, method,
// target.
func complete410() Event {
	return Event{
		Action: "restaction", HTTPMethod: "POST", Verb: "create", Group: "composition.krateo.io", Version: "v1alpha1",
		Resource: "fireworksapps", ObjectName: "demo", Namespace: "team-a", Outcome: "success", StatusCode: 200,
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

// hasMarker reports whether a krateo.audit.incomplete value lists marker.
func hasMarker(list, marker string) bool {
	for _, m := range strings.Split(list, ",") {
		if m == marker {
			return true
		}
	}
	return false
}

// TestAudit410_RecordlessActorShape_IsEmittedMarked is the 057 shape exactly:
// no actor on ctx, only outcome and a session id. It is emitted, marked with
// every gap it has (the actor first), and counted under "actor".
func TestAudit410_RecordlessActorShape_IsEmittedMarked(t *testing.T) {
	before := IncompleteCount(IncompleteActor)
	ctx := WithSessionID(context.Background(), "probe-1")
	got := emit410(t, ctx, Event{Outcome: "success"})
	if len(got) != 1 {
		t.Fatalf("the 057 shape emitted %d records, want 1 (emitted, marked; a record is never dropped)", len(got))
	}
	if m := got[0]["krateo.audit.incomplete"]; m != "actor,actor_class,action,verb,method,target" {
		t.Fatalf("krateo.audit.incomplete = %q, want every gap of the shape in the schema's order", m)
	}
	if _, ok := got[0]["user.name"]; ok {
		t.Fatalf("an actorless record carries user.name=%q", got[0]["user.name"])
	}
	if d := IncompleteCount(IncompleteActor) - before; d != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", d)
	}
}

// TestAudit410_NoActor_EmitsAnOtherwiseCompleteRecordMarkedActor: the actor's
// absence is a gap like any other, not a reason to drop the record.
func TestAudit410_NoActor_EmitsAnOtherwiseCompleteRecordMarkedActor(t *testing.T) {
	before := IncompleteCount(IncompleteActor)
	got := emit410(t, WithSessionID(context.Background(), "s-test"), complete410())
	if len(got) != 1 {
		t.Fatalf("a complete record with no actor emitted %d records, want 1", len(got))
	}
	if m := got[0]["krateo.audit.incomplete"]; m != "actor,actor_class" {
		t.Fatalf("krateo.audit.incomplete = %q, want actor,actor_class", m)
	}
	for _, k := range []string{"user.name", "enduser.id", "user.roles", "krateo.audit.actor_class"} {
		if v, ok := got[0][k]; ok {
			t.Errorf("an actorless record carries %s=%q", k, v)
		}
	}
	if d := IncompleteCount(IncompleteActor) - before; d != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", d)
	}
}

// TestAudit410_MissingNonActorField_EmitsMarkedIncomplete removes action,
// verb, method or target from a record that has an actor. Each is emitted,
// marked with the missing marker(s), and counted; the complete control carries
// no mark.
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
		{"verb", func(e *Event) { e.Verb = "" }, "verb", []string{IncompleteVerb}},
		{"method", func(e *Event) { e.HTTPMethod = "" }, "method", []string{IncompleteMethod}},
		// No Kubernetes object left: no target, and no preview flag to read.
		{"target", func(e *Event) { e.Group, e.Version, e.Resource, e.ObjectName, e.Namespace = "", "", "", "", "" }, "target", []string{IncompleteTarget}},
		{"method+target", func(e *Event) {
			e.HTTPMethod = ""
			e.Group, e.Version, e.Resource, e.ObjectName, e.Namespace = "", "", "", "", ""
		}, "method,target", []string{IncompleteMethod, IncompleteTarget}},
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
		Action: "restaction", HTTPMethod: "POST", Verb: "post", ServerAddress: "render.example:8080",
		RESTActionNamespace: "krateo-system", RESTActionName: "blueprint-render", RESTActionStage: "render",
		Outcome: "success", StatusCode: 201,
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
		"krateo.audit.verb":           "post",
		"http.response.status_code":   "201",
	}
	for k, v := range want {
		if got[0][k] != v {
			t.Errorf("%s = %q, want %q", k, got[0][k], v)
		}
	}
	for _, k := range []string{"k8s.resource.resource", "url.path", "krateo.audit.incomplete", "krateo.audit.preview"} {
		if v, ok := got[0][k]; ok {
			t.Errorf("%s must be absent, got %q", k, v)
		}
	}
}
