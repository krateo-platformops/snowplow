package audit

// schema_version_test.go — every record snowplow's emitter writes carries
// krateo.audit.schema_version = "1".
//
// Every snowplow audit row goes through Emit (the package function, or an
// Emitter's), which hands it to the shared writer krateo-platformops/audit:
// that library stamps the key on every record it writes (audit.go attributes:
// add(KeySchemaVersion, SchemaVersion), SchemaVersion = "1" at v0.13.0). This
// arm reads the records as the LoggerProvider receives them, for each way a row
// is made here: a request row with its preview flag and its client provenance,
// a refusal (denied) and a failure row, the route's own record (EmitRoute), and
// OpenRecord's fallback rows (a 4xx refusal, an unrecorded answer, a panic).
// The value is checked as the literal "1", not the library's constant, so a
// library that stopped writing it, or wrote another, fails here.

import (
	"context"
	"net/http"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

func assertSchemaV1(t *testing.T, label string, recs []sdklog.Record) {
	t.Helper()
	if len(recs) == 0 {
		t.Fatalf("%s: PRECONDITION: no record was emitted, so the arm proves nothing", label)
	}
	for i, r := range recs {
		var got []string
		for _, kv := range audittest.Attributes(r) {
			if string(kv.Key) == krateoaudit.KeySchemaVersion {
				got = append(got, kv.Value.Emit())
			}
		}
		if len(got) != 1 || got[0] != "1" {
			t.Errorf("%s: record %d: krateo.audit.schema_version = %v, want exactly [\"1\"]", label, i, got)
		}
	}
}

func TestSchemaVersion_EveryRecordCarriesV1(t *testing.T) {
	t.Run("request, refusal and failure rows, with preview and provenance", func(t *testing.T) {
		rec := routeRecorder(t)
		ctx := actorCtx()
		h := http.Header{}
		h.Set(ProvenanceHeader, "not-a-valid-provenance") // a rejected provenance is still recorded
		Emit(ctx, Event{Action: "call", HTTPMethod: http.MethodPost, Verb: "create", Resource: "configmaps",
			Namespace: "team-a", Outcome: OutcomeSuccess, StatusCode: http.StatusCreated, Client: ProvenanceOf(h)})
		Emit(ctx, Event{Action: "call", HTTPMethod: http.MethodDelete, Verb: "delete", Resource: "configmaps",
			Namespace: "team-a", Outcome: OutcomeDenied, DenialReason: DenialInvalidRequest, Message: MessageRequestRefused})
		Emit(ctx, Event{Action: "call", HTTPMethod: http.MethodPut, Verb: "update", ServerAddress: "example.test",
			Outcome: OutcomeFailure, Message: MessageUnrecordedAnswer})
		assertSchemaV1(t, "Emit", rec.Records())
	})
	t.Run("the route's own record (EmitRoute)", func(t *testing.T) {
		_, recs := serveRouteStatus(t, func(w http.ResponseWriter, r *http.Request) {
			EmitRoute(r.Context(), Event{Action: "route", HTTPMethod: http.MethodPost, Verb: "post",
				ServerAddress: "example.test", Outcome: OutcomeSuccess, StatusCode: http.StatusOK})
			w.WriteHeader(http.StatusOK)
		})
		assertSchemaV1(t, "EmitRoute", recs)
	})
	t.Run("OpenRecord fallback: a 4xx refusal", func(t *testing.T) {
		_, recs := serveRouteStatus(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) })
		assertSchemaV1(t, "fallback 4xx", recs)
	})
	t.Run("OpenRecord fallback: an unrecorded answer", func(t *testing.T) {
		_, recs := serveRouteStatus(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		assertSchemaV1(t, "fallback unrecorded", recs)
	})
	t.Run("OpenRecord fallback: a panic", func(t *testing.T) {
		rec := routeRecorder(t)
		func() {
			defer func() { _ = recover() }()
			serveRoute(t, func(http.ResponseWriter, *http.Request) { panic("x") })
		}()
		assertSchemaV1(t, "fallback panic", rec.Records())
	})
	t.Run("an Emitter used directly", func(t *testing.T) {
		rec := audittest.NewRecorder(t)
		New(rec.Logger(Scope)).Emit(context.Background(), Event{Action: "call", HTTPMethod: http.MethodGet, Verb: "get",
			ServerAddress: "example.test", Outcome: OutcomeSuccess, StatusCode: http.StatusOK})
		assertSchemaV1(t, "Emitter.Emit", rec.Records())
	})
}
