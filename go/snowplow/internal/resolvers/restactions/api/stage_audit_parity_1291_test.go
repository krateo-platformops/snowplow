package api

// stage_audit_parity_1291_test.go — #1291: the RESTAction stage records,
// written by the shared library (github.com/krateo-platformops/audit v0.6.0)
// since #1291, keep every key and value snowplow wrote before it, but for the
// two changes schema v1 requires, each named here per shape.
//
//   P  parity, per record shape: each shape is resolved through the REAL
//      api.Resolve on a customer request served through audit.Middleware, and
//      every key of the record snowplow's own writer emitted for the same
//      request (testdata/audit_parity_1291/before.json, captured by this
//      file's driver at main 6a18a554, before the move) is on the new record
//      with the same value, except:
//        - server.address on a stage that wrote a Kubernetes object (no
//          endpointRef): it was the host of the caller's own clientconfig, the
//          apiserver's, beside the object it wrote. Schema v1 gives a request
//          record exactly one target ("two-targets" otherwise), and the object
//          is it;
//        - outcome on a write snowplow's destination policy refused: it was
//          failure; snowplow itself refused, which is the schema's denied.
//   C  each shape conforms to the shared schema for snowplow's writer class,
//      and the library's falsifiers bite on it.
//   V  krateo.audit.verb: the apiserver's verb for a Kubernetes object, the
//      lowercase method for a named endpoint; dry_run on every write, true
//      only for ?dryRun=All.
//   D  docs/architecture/audit-coverage.md names every key of every shape.

import (
	"context"
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
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const parityBase1291 = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps"

// parityAPI1291 is an apiserver-shaped upstream that answers a write with an
// object carrying a uid, generation and resourceVersion, a DELETE with the
// Status of a gone object, and anything under team-b with a 403.
func parityAPI1291(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/team-b/"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`)
		case r.Method == http.MethodDelete:
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Success","details":{"name":"demo","uid":"uid-gone-1291"}}`)
		default:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"apiVersion":"composition.krateo.io/v1alpha1","kind":"FireworksApp",`+
				`"metadata":{"name":"demo","namespace":"team-a","uid":"uid-1291","generation":3,"resourceVersion":"91"}}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

type stageParityShape1291 struct {
	name  string
	ext   func(t *testing.T) string // the named endpoint's URL
	stage func() *templates.API
	// verb and dryRun are what the new record must say.
	verb, dryRun string
	// gone and changed are the schema's changes to the before record: keys it
	// no longer carries, keys whose value it changes (to the value given).
	gone    []string
	changed map[string]string
}

func goneURL1291(t *testing.T) string {
	gone := httptest.NewServer(http.NotFoundHandler())
	u := gone.URL
	gone.Close()
	return u
}

// oneTarget1291 is the change of a stage that wrote a Kubernetes object.
var oneTarget1291 = []string{"server.address"}

var stageParityShapes1291 = []stageParityShape1291{
	{"stage.create", nil, func() *templates.API { return stage410("s", http.MethodPost, parityBase1291) }, "create", "false", oneTarget1291, nil},
	{"stage.update", nil, func() *templates.API { return stage410("s", http.MethodPut, parityBase1291+"/demo") }, "update", "false", oneTarget1291, nil},
	{"stage.patch", nil, func() *templates.API { return stage410("s", http.MethodPatch, parityBase1291+"/demo") }, "patch", "false", oneTarget1291, nil},
	{"stage.delete", nil, func() *templates.API { return stage410("s", http.MethodDelete, parityBase1291+"/demo") }, "delete", "false", oneTarget1291, nil},
	{"stage.subresource", nil, func() *templates.API { return stage410("s", http.MethodPut, parityBase1291+"/demo/status") }, "update", "false", oneTarget1291, nil},
	{"stage.dryRun", nil, func() *templates.API { return stage410("s", http.MethodPost, parityBase1291+"?dryRun=All") }, "create", "true", oneTarget1291, nil},
	{"stage.failure", nil, func() *templates.API { return stage410("s", http.MethodPost, denyPath410) }, "create", "false", oneTarget1291, nil},
	{"stage.endpointRef", nil, func() *templates.API { return extStage410("s", http.MethodPost, "/render") }, "post", "false", nil, nil},
	{"stage.endpointRef.noResponse", goneURL1291, func() *templates.API { return extStage410("s", http.MethodPost, "/render") }, "post", "false", nil, nil},
	{"stage.endpointRef.refused", func(*testing.T) string { return "http://169.254.169.254" },
		func() *templates.API { return extStage410("s", http.MethodPost, "/render") }, "post", "false", nil,
		map[string]string{"outcome": krateoaudit.OutcomeDenied}},
}

// unlabelledNamespaces1291 is a cluster in which every Namespace exists and
// none is in the preview sandbox.
type unlabelledNamespaces1291 struct{}

func (unlabelledNamespaces1291) NamespaceLabels(context.Context, string) (map[string]string, bool, error) {
	return map[string]string{}, true, nil
}

// Every record this package's tests emit reads its preview flag (false) from
// unlabelledNamespaces1291; the audit package's own arms cover the others.
func init() { audit.SetNamespaceReader(unlabelledNamespaces1291{}) }

// serveStageParity1291 resolves one shape on a customer request served through
// audit.Middleware, as the pod does, and returns its one record. A
// server.address of a test server is normalised to "<test-server>".
func serveStageParity1291(t *testing.T, s stageParityShape1291) (sdklog.Record, map[string]string) {
	t.Helper()
	api := parityAPI1291(t)
	ext := api.URL
	if s.ext != nil {
		ext = s.ext(t)
	}
	env410(t, api.URL, ext)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	h := audit.Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		resolve410(withUserOn1291(r.Context(), user410, group410), api.URL, s.stage())
	}))
	req := httptest.NewRequest(http.MethodGet, "/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=krateo-system&name=ra410", nil)
	req.RemoteAddr = "10.0.0.9:41000"
	req.Header.Set("User-Agent", "parity-ua-1291")
	req = req.WithContext(audit.WithSessionID(req.Context(), "parity-1291"))
	h.ServeHTTP(httptest.NewRecorder(), req)
	r := rec.Only(t)
	out := map[string]string{}
	for _, kv := range audittest.Attributes(r) {
		out[string(kv.Key)] = kv.Value.String()
	}
	if v, ok := out["server.address"]; ok && strings.HasPrefix(v, "127.0.0.1:") {
		out["server.address"] = "<test-server>"
	}
	return r, out
}

// withUserOn1291 attaches the requester to a request context the way the
// UserConfig middleware does, keeping its baggage and what it was received
// with. The token carries authn's principal claim.
func withUserOn1291(ctx context.Context, username string, groups ...string) context.Context {
	return xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: username, Groups: groups,
		Principal: jwtutil.PrincipalHuman}))
}

func TestAudit1291_Stage_P_C_V_EveryShapeKeepsItsKeysAndConforms(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "audit_parity_1291", "before.json"))
	if err != nil {
		t.Fatal(err)
	}
	var before map[string]map[string]string
	if err := json.Unmarshal(raw, &before); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range stageParityShapes1291 {
		names = append(names, s.name)
	}
	if got := slices.Sorted(maps.Keys(before)); !slices.Equal(got, slices.Sorted(slices.Values(names))) {
		t.Fatalf("precondition: the before capture holds shapes %v, the driver resolves %v", got, names)
	}
	docKeys := map[string]bool{}
	for _, s := range stageParityShapes1291 {
		t.Run(s.name, func(t *testing.T) {
			r, after := serveStageParity1291(t, s)
			was := before[s.name]
			for _, k := range slices.Sorted(maps.Keys(was)) {
				v, ok := after[k]
				switch want, changes := s.changed[k]; {
				case slices.Contains(s.gone, k):
					if ok {
						t.Errorf("%s: %s is still written beside the Kubernetes object (two targets)", s.name, k)
					}
				case !ok:
					t.Errorf("%s: key %s, written before #1291, is gone", s.name, k)
				case changes && v != want:
					t.Errorf("%s: %s = %q, want the schema's %q", s.name, k, v, want)
				case !changes && v != was[k]:
					t.Errorf("%s: key %s changed value across #1291", s.name, k)
				}
			}
			for _, k := range s.gone {
				if _, ok := was[k]; !ok {
					t.Errorf("precondition: %s did not carry %s before; the named change is not one", s.name, k)
				}
			}

			audittest.AssertConformant(t, audit.Writer, r)
			audittest.AssertRequiredKeysDetected(t, audit.Writer, r)
			audittest.AssertEnumsDetected(t, audit.Writer, r)

			if after[krateoaudit.KeyVerb] != s.verb {
				t.Errorf("krateo.audit.verb = %q, want %q", after[krateoaudit.KeyVerb], s.verb)
			}
			if after[krateoaudit.KeyDryRun] != s.dryRun {
				t.Errorf("krateo.audit.dry_run = %q, want %q", after[krateoaudit.KeyDryRun], s.dryRun)
			}
			if after[krateoaudit.KeyOutcome] != after["outcome"] {
				t.Errorf("krateo.audit.outcome %q disagrees with outcome %q", after[krateoaudit.KeyOutcome], after["outcome"])
			}
			var added []string
			for k := range after {
				docKeys[k] = true
				if _, was := was[k]; !was {
					added = append(added, k)
				}
			}
			slices.Sort(added)
			t.Logf("%s: +%s", s.name, strings.Join(added, " +"))
		})
	}

	doc, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "docs", "architecture", "audit-coverage.md"))
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for k := range docKeys {
		if !strings.Contains(string(doc), "`"+k+"`") {
			missing = append(missing, k)
		}
	}
	slices.Sort(missing)
	if len(missing) > 0 {
		t.Errorf("docs/architecture/audit-coverage.md does not name %v", missing)
	}
}
