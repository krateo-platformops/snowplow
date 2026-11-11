package handlers

// call_provenance_1333_test.go — #1333: a /call write or dry run reads the
// portal's X-Krateo-Provenance header into its audit record, driven through
// the REAL /call handlers behind audit.Middleware, as the pod serves them.
//
//   W  every write verb and the dry run: the record carries exactly the
//      asserted krateo.audit.client.* keys, conforms to the shared schema, and
//      the library's falsifiers bite on it.
//   R  every rejection reason: the write still runs and answers as it would,
//      and the record carries krateo.audit.client.rejected alone, with the
//      identity, verb, target and outcome snowplow established.
//   N  no header: no client key.
//   S  the header never leaves snowplow (not sent to the apiserver), never
//      reaches a log line (debug wire dump on) and never a span, and no
//      identity is on the span.
//   D  docs/architecture/audit-coverage.md names every client key.

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	session1333 = "agentiko-sentinel-1333"
	confirm1333 = "0b8f8a52-61d4-4c43-9a7e-2f1d0c9b8a76"
	setSHA1333  = "0a08bb91090b111f15a045b6061bccc4d0b8488d85f99ba57d8632e17061c277"
	prov1333    = `{"source":"portal","agentSessionId":"` + session1333 + `","confirmationId":"` + confirm1333 + `",` +
		`"confirmedSet":{"count":3,"irreversible":true,"sha256":"` + setSHA1333 + `"},"op":1,` +
		`"requestedAt":"2026-11-11T12:20:30.456+02:00"}`
)

func header1333(js string) string {
	return "v1." + base64.RawURLEncoding.EncodeToString([]byte(js))
}

// want1333 is what prov1333 asserts, as the record writes it (requestedAt in
// UTC, milliseconds).
var want1333 = map[string]string{
	krateoaudit.KeyClientSource:                   "portal",
	krateoaudit.KeyClientAgentSessionID:           session1333,
	krateoaudit.KeyClientConfirmationID:           confirm1333,
	krateoaudit.KeyClientConfirmedSetCount:        "3",
	krateoaudit.KeyClientConfirmedSetIrreversible: "true",
	krateoaudit.KeyClientConfirmedSetSHA256:       setSHA1333,
	krateoaudit.KeyClientConfirmedSetOp:           "1",
	krateoaudit.KeyClientRequestedAt:              "2026-11-11T10:20:30.456Z",
}

// sent1333 is one request the fake apiserver received.
type sent1333 struct {
	method string
	header http.Header
	query  string
}

// api1333 is a fake apiserver that answers every request with status/body and
// keeps what it received.
func api1333(t *testing.T, status int, body string) (*[]sent1333, endpoints.Endpoint) {
	t.Helper()
	var mu sync.Mutex
	got := &[]sent1333{}
	ep := rawAPI1298(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		*got = append(*got, sent1333{method: r.Method, header: r.Header.Clone(), query: r.URL.RawQuery})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	return got, ep
}

type route1333 struct {
	name, method, target, body, action string
	h                                  func() http.Handler
	status                             int
	resp                               string
}

var routes1333 = []route1333{
	{"POST /call", http.MethodPost, "/call?" + cmQuery443, cmRequest957, "call", Call, http.StatusCreated, cmResponse957},
	{"PUT /call", http.MethodPut, "/call?" + cmQuery443, cmRequest957, "call", Call, http.StatusOK, cmResponse957},
	{"PATCH /call", http.MethodPatch, "/call?" + cmQuery443, `{"data":{"k":"w"}}`, "call", Call, http.StatusOK, cmResponse957},
	{"DELETE /call", http.MethodDelete, "/call?" + cmQuery443, "", "call", Call, http.StatusOK, cmDeleted957},
	{"POST /call/dry-run", http.MethodPost, "/call/dry-run?" + cmQuery443, cmRequest957, "call.dryRun", CallDryRun, http.StatusCreated, cmResponse957},
	{"PUT /call/dry-run", http.MethodPut, "/call/dry-run?" + cmQuery443, cmRequest957, "call.dryRun", CallDryRun, http.StatusOK, cmResponse957},
}

// serve1333 serves r through audit.Middleware with the given provenance
// header values (none: no header), as alice, and returns the caller's
// response, the one record, and what the apiserver received.
func serve1333(t *testing.T, r route1333, h http.Handler, prov ...string) (*httptest.ResponseRecorder, sdklog.Record, []sent1333) {
	t.Helper()
	got, ep := api1333(t, r.status, r.resp)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	var rdr io.Reader
	if r.body != "" {
		rdr = strings.NewReader(r.body)
	}
	req := withCaller443(httptest.NewRequest(r.method, r.target, rdr), ep)
	if r.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, v := range prov {
		req.Header.Add(audit.ProvenanceHeader, v)
	}
	w := httptest.NewRecorder()
	audit.Middleware()(h).ServeHTTP(w, req)
	return w, rec.Only(t), *got
}

// clientKeys1333 is the record's krateo.audit.client.* keys.
func clientKeys1333(r sdklog.Record) map[string]string {
	out := map[string]string{}
	for k, v := range asStrings1291(r) {
		if strings.HasPrefix(k, "krateo.audit.client.") {
			out[k] = v
		}
	}
	return out
}

func TestCall1333_W_EveryWriteAndDryRunCarriesTheProvenance(t *testing.T) {
	docKeys := map[string]bool{}
	for _, r := range routes1333 {
		t.Run(r.name, func(t *testing.T) {
			w, rec, sent := serve1333(t, r, r.h(), header1333(prov1333))
			if w.Code != http.StatusOK || len(sent) != 1 {
				t.Fatalf("SETUP: caller %d, apiserver received %d requests", w.Code, len(sent))
			}
			attrs := asStrings1291(rec)
			if attrs["krateo.action"] != r.action || attrs[krateoaudit.KeyUserName] != "alice" {
				t.Fatalf("SETUP: record %v is not alice's %s record", attrs, r.action)
			}
			if got := clientKeys1333(rec); !maps.Equal(got, want1333) {
				t.Errorf("RED #1333: client keys\n got %v\nwant %v", got, want1333)
			}
			audittest.AssertConformant(t, audit.Writer, rec)
			audittest.AssertRequiredKeysDetected(t, audit.Writer, rec)
			audittest.AssertEnumsDetected(t, audit.Writer, rec)
			for k := range attrs {
				docKeys[k] = true
			}
		})
	}
	// D — and the rejected key (R below).
	docKeys[krateoaudit.KeyClientRejected] = true
	assertCoverageDocNames1291(t, docKeys)
}

func TestCall1333_R_ARejectedHeaderStillWritesAndAssertsNothing(t *testing.T) {
	cases := []struct {
		name, reason string
		values       []string
	}{
		// #1333's contract, 2 KiB, written out: never derived from the
		// constant under test.
		{"oversize", "oversize", []string{"v1." + strings.Repeat("A", 2048-len("v1.")+1)}},
		{"version", "version", []string{"v2." + strings.TrimPrefix(header1333(prov1333), "v1.")}},
		{"encoding", "encoding", []string{header1333(prov1333) + "="}},
		{"encoding: two field lines", "encoding", []string{header1333(prov1333), header1333(prov1333)}},
		// The header names an identity and a target: never taken, the whole
		// header is refused, and the record keeps what snowplow established.
		{"schema", "schema", []string{header1333(`{"source":"portal","user":"mallory","namespace":"kube-system","outcome":"denied"}`)}},
	}
	for _, r := range []route1333{routes1333[0], routes1333[3], routes1333[4]} {
		for _, c := range cases {
			t.Run(r.name+"/"+c.name, func(t *testing.T) {
				plain, _, plainSent := serve1333(t, r, r.h())
				w, rec, sent := serve1333(t, r, r.h(), c.values...)
				if w.Code != plain.Code || w.Body.String() != plain.Body.String() {
					t.Errorf("RED #1333: with a rejected header the caller got %d %q, without one %d %q",
						w.Code, w.Body.String(), plain.Code, plain.Body.String())
				}
				if len(sent) != 1 || len(plainSent) != 1 || sent[0].method != plainSent[0].method || sent[0].query != plainSent[0].query {
					t.Errorf("RED #1333: the write was not sent as without the header: %+v vs %+v", sent, plainSent)
				}
				if got := clientKeys1333(rec); !maps.Equal(got, map[string]string{krateoaudit.KeyClientRejected: c.reason}) {
					t.Errorf("RED #1333: client keys %v, want rejected=%s alone", got, c.reason)
				}
				attrs := asStrings1291(rec)
				if attrs[krateoaudit.KeyUserName] != "alice" || attrs["k8s.namespace.name"] != "default" || attrs["krateo.audit.outcome"] != "success" {
					t.Errorf("RED #1333: the header changed what snowplow established: %v", attrs)
				}
				audittest.AssertConformant(t, audit.Writer, rec)
			})
		}
	}
}

func TestCall1333_N_NoHeaderNoClientKey(t *testing.T) {
	for _, r := range routes1333 {
		t.Run(r.name, func(t *testing.T) {
			_, rec, _ := serve1333(t, r, r.h())
			if got := clientKeys1333(rec); len(got) != 0 {
				t.Errorf("no header, but the record carries %v", got)
			}
		})
	}
}

// syncBuffer1333 is a log sink safe across the handler's goroutines.
type syncBuffer1333 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer1333) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer1333) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestCall1333_S_TheHeaderStaysInTheRecord(t *testing.T) {
	value := header1333(prov1333)
	secrets := []string{value, strings.TrimPrefix(value, "v1."), session1333, confirm1333, setSHA1333}
	for _, r := range []route1333{routes1333[0], routes1333[4]} {
		t.Run(r.name, func(t *testing.T) {
			logs := &syncBuffer1333{}
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
			t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

			// The debug wire dump on: every outbound header is logged.
			ch := r.h().(*callHandler)
			ch.verbose = true
			h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				ctx, span := tp.Tracer("t").Start(req.Context(), r.method+" /call", trace.WithSpanKind(trace.SpanKindServer))
				ctx = xcontext.BuildContext(ctx, xcontext.WithLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
				ch.ServeHTTP(w, req.WithContext(ctx))
				span.End()
			})
			_, rec, sent := serve1333(t, r, h, value)
			if got := clientKeys1333(rec); !maps.Equal(got, want1333) {
				t.Fatalf("SETUP: the record does not carry the provenance: %v", got)
			}
			if logs.String() == "" {
				t.Fatal("SETUP: the handler logged nothing: the log capture is not wired")
			}

			for _, s := range sent {
				if v := s.header.Values(audit.ProvenanceHeader); len(v) != 0 {
					t.Errorf("RED #1333: the apiserver received %s", audit.ProvenanceHeader)
				}
			}
			for _, s := range secrets {
				if strings.Contains(logs.String(), s) {
					t.Errorf("RED #1333: a log line carries the header's content %q", s)
				}
			}
			spans := exp.GetSpans()
			if len(spans) == 0 {
				t.Fatal("SETUP: no span exported")
			}
			for _, sp := range spans {
				for _, kv := range sp.Attributes {
					k, v := string(kv.Key), kv.Value.Emit()
					if strings.HasPrefix(k, "krateo.audit.client.") || strings.HasPrefix(k, "user.") || k == "enduser.id" ||
						strings.Contains(v, "alice") || slices.ContainsFunc(secrets, func(s string) bool { return strings.Contains(v, s) }) {
						t.Errorf("RED #1333: span %q carries %s=%q", sp.Name, k, v)
					}
				}
			}
		})
	}
}
