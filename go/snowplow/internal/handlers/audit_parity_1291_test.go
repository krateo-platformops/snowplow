package handlers

// audit_parity_1291_test.go — #1291: the /call, /call/dry-run and /export
// records, written by the shared library (github.com/krateo-platformops/audit
// v0.6.0) since #1291, keep every key and value snowplow wrote before it.
//
//   P  parity, per record shape: each shape is served through the REAL
//      handler and audit.Middleware, and every key of the record snowplow's
//      own writer emitted for the same request (testdata/audit_parity_1291/
//      before.json, captured by this file's driver at main 6a18a554, before
//      the move) is on the new record with the same value. What the record
//      gains is the schema's (krateo.audit.*), listed per shape in the log.
//   C  each shape conforms to the shared schema for snowplow's writer class
//      (audit.Check through audittest), and the library's falsifiers bite on
//      it: every key Check requires, and every closed set, is detected when
//      it goes.
//   V  krateo.audit.verb is the apiserver's verb of what was dialled: create,
//      update, patch, delete for /call; get for an export of one object;
//      dry_run is true only on /call/dry-run and absent on an export (a read).
//   D  docs/architecture/audit-coverage.md names every key of every shape.

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

type parityShape1291 struct {
	name           string
	h              func() http.Handler
	method, target string
	body           string
	status         int
	resp           string
	// verb and dryRun are what the new record must say ("" dryRun = absent).
	verb, dryRun string
}

var parityShapes1291 = []parityShape1291{
	{"call.create", Call, http.MethodPost, "/call?" + cmQuery443, cmRequest957, http.StatusCreated, cmResponse957, "create", "false"},
	{"call.update", Call, http.MethodPut, "/call?" + cmQuery443, cmRequest957, http.StatusOK, cmResponse957, "update", "false"},
	{"call.patch", Call, http.MethodPatch, "/call?" + cmQuery443, `{"data":{"k":"w"}}`, http.StatusOK, cmResponse957, "patch", "false"},
	{"call.delete", Call, http.MethodDelete, "/call?" + cmQuery443, "", http.StatusOK, cmDeleted957, "delete", "false"},
	{"call.subresource", Call, http.MethodPut, "/call?" + cmQuery443 + "&subresource=status", cmRequest957, http.StatusOK, cmResponse957, "update", "false"},
	{"call.failure", Call, http.MethodPost, "/call?" + cmQuery443, cmRequest957, http.StatusForbidden, cmForbidden957, "create", "false"},
	{"call.dryRun", CallDryRun, http.MethodPost, "/call/dry-run?" + cmQuery443, cmRequest957, http.StatusCreated, cmResponse957, "create", "true"},
	{"export.get", func() http.Handler { return Export(Call()) }, http.MethodGet, "/export?format=json&" + target1055, "", http.StatusOK, body1055, "get", ""},
	{"export.post", ExportPOST, http.MethodPost, "/export?format=json&" + target1055, `{"extras":{}}`, http.StatusOK, body1055, "get", ""},
}

// serveParity1291 serves one shape through audit.Middleware, as the pod does,
// with a fixed peer, User-Agent and session id, and returns its one record.
func serveParity1291(t *testing.T, s parityShape1291) sdklog.Record {
	t.Helper()
	api, ep := newFakeAPI443(t)
	api.respond(s.status, s.resp)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	var rdr io.Reader
	if s.body != "" {
		rdr = bytes.NewReader([]byte(s.body))
	}
	req := withCaller443(httptest.NewRequest(s.method, s.target, rdr), ep)
	if s.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.RemoteAddr = "10.0.0.9:41000"
	req.Header.Set("User-Agent", "parity-ua-1291")
	req = req.WithContext(audit.WithSessionID(req.Context(), "parity-1291"))
	audit.Middleware()(s.h()).ServeHTTP(httptest.NewRecorder(), req)
	return rec.Only(t)
}

// asStrings1291 is a record's attributes as the before capture wrote them.
func asStrings1291(r sdklog.Record) map[string]string {
	out := map[string]string{}
	for _, kv := range audittest.Attributes(r) {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

// before1291 is the record each shape got from snowplow's own writer.
func before1291(t *testing.T) map[string]map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "audit_parity_1291", "before.json"))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]map[string]string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// assertParity1291 fails for every key of before that after lacks or holds
// with another value.
func assertParity1291(t *testing.T, shape string, before, after map[string]string) {
	t.Helper()
	for _, k := range slices.Sorted(maps.Keys(before)) {
		v, ok := after[k]
		switch {
		case !ok:
			t.Errorf("%s: key %s, written before #1291, is gone", shape, k)
		case v != before[k]:
			t.Errorf("%s: key %s changed value across #1291", shape, k)
		}
	}
}

func TestAudit1291_Handlers_P_C_V_EveryShapeKeepsItsKeysAndConforms(t *testing.T) {
	before := before1291(t)
	if got, want := slices.Sorted(maps.Keys(before)), func() []string {
		var n []string
		for _, s := range parityShapes1291 {
			n = append(n, s.name)
		}
		return slices.Sorted(slices.Values(n))
	}(); !slices.Equal(got, want) {
		t.Fatalf("precondition: the before capture holds shapes %v, the driver serves %v", got, want)
	}
	docKeys := map[string]bool{}
	for _, s := range parityShapes1291 {
		t.Run(s.name, func(t *testing.T) {
			r := serveParity1291(t, s)
			after := asStrings1291(r)
			assertParity1291(t, s.name, before[s.name], after)

			audittest.AssertConformant(t, audit.Writer, r)
			audittest.AssertRequiredKeysDetected(t, audit.Writer, r)
			audittest.AssertEnumsDetected(t, audit.Writer, r)

			if after[krateoaudit.KeyVerb] != s.verb {
				t.Errorf("krateo.audit.verb = %q, want %q", after[krateoaudit.KeyVerb], s.verb)
			}
			if v, ok := after[krateoaudit.KeyDryRun]; v != s.dryRun || (s.dryRun == "" && ok) {
				t.Errorf("krateo.audit.dry_run = %q (present %t), want %q", v, ok, s.dryRun)
			}
			var added []string
			for k := range after {
				docKeys[k] = true
				if _, was := before[s.name][k]; !was {
					added = append(added, k)
				}
			}
			slices.Sort(added)
			t.Logf("%s: +%s", s.name, strings.Join(added, " +"))
		})
	}
	assertCoverageDocNames1291(t, docKeys)
}

// assertCoverageDocNames1291 fails unless docs/architecture/audit-coverage.md
// names (in backticks) every key in keys.
func assertCoverageDocNames1291(t *testing.T, keys map[string]bool) {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "architecture", "audit-coverage.md"))
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for k := range keys {
		if !strings.Contains(string(doc), "`"+k+"`") {
			missing = append(missing, k)
		}
	}
	slices.Sort(missing)
	if len(missing) > 0 {
		t.Errorf("docs/architecture/audit-coverage.md does not name %v", missing)
	}
}
