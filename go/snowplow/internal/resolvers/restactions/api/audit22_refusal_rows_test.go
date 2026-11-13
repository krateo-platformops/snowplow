package api

// audit22_refusal_rows_test.go — audit v0.12.0 (audit#22, #34), AUDIT-4
// installer#410: the RESTAction write-stage row is conformant.
//
//   E  #1283: a write stage dialled through a named endpointRef records which
//      one (krateo.audit.endpoint_ref.namespace and .name) beside the host it
//      went to; a stage with no endpointRef records neither.
//   R  every cause snowplow refuses a write stage for, on a request somebody
//      asked for, is one denied row with its reason and no status code:
//      per_user_credential_ref, endpoint_ref_malformed, dependency_failed,
//      uaf_on_write_verb, invalid_path, template_error, dry_run_not_executed,
//      write_refused (an export). A target not resolved is marked target.
//   F  a write stage whose endpoint Secret read failed is a failure row with
//      the code the apiserver answered that read with.
//   N  no row: background work (no requester), and a refused READ stage.
//   S  a per_user_credential_ref row never carries the Secret's name, which
//      embeds the username: no attribute and no body holds "-clientconfig".
//   C  every row conforms for snowplow's writer class (the target marker
//      accepted where the row says the target is unresolved), and the
//      library's falsifiers bite on it.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	brokenRef22 = "broken-endpoint" // a Secret with no server-url
	absentRef22 = "absent-endpoint" // a Secret the apiserver answers 404 for
	base22      = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps"
)

// upstream22 is the apiserver and the named endpoint in one: a Secret read is
// a 404 Status, a path under team-b a 403, and any other write is created.
func upstream22(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/secrets/"+brokenRef22):
			// The Secret exists and is not a usable endpoint: no server-url.
			_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"`+brokenRef22+`","namespace":"`+raNS410+`"},"data":{"token":"ZXh0LXRvaw=="}}`)
		case strings.Contains(r.URL.Path, "/secrets/"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		case strings.Contains(r.URL.Path, "/team-b/"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`)
		default:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"apiVersion":"composition.krateo.io/v1alpha1","kind":"FireworksApp",`+
				`"metadata":{"name":"demo","namespace":"team-a","uid":"uid-22","generation":1,"resourceVersion":"1"}}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// env22 is env410 with the broken endpoint Secret beside the two it publishes.
func env22(t *testing.T, url string) {
	t.Helper()
	env410(t, url, url)
	cache.ResetSecretsSnapshotForTest()
	own := kubeutil.MakeDNS1123Compatible(user410) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		own: {ObjectMeta: authnMeta(raNS410, own, user410), // #1382: as authn writes it
			Data: map[string][]byte{"server-url": []byte(url), "token": []byte("tok")}},
		extRef410: {ObjectMeta: metav1.ObjectMeta{Namespace: raNS410, Name: extRef410},
			Data: map[string][]byte{"server-url": []byte(url), "token": []byte("ext-tok")}},
		brokenRef22: {ObjectMeta: metav1.ObjectMeta{Namespace: raNS410, Name: brokenRef22},
			Data: map[string][]byte{"token": []byte("ext-tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest(raNS410)
}

// resolve22 resolves stages on a request of user410 carrying a session id, as
// audit.Middleware leaves it, with mark applied (a background or export
// marker), and returns every record.
func resolve22(t *testing.T, mark func(context.Context) context.Context, stages ...*templates.API) []sdklog.Record {
	t.Helper()
	api := upstream22(t)
	env22(t, api.URL)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	// Served through audit.Middleware, as the pod serves a /call: the record
	// carries what the request was received with and its session id.
	h := audit.Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		ctx := withUserOn1291(r.Context(), user410, group410)
		if mark != nil {
			ctx = mark(ctx)
		}
		resolve410(ctx, api.URL, stages...)
	}))
	req := httptest.NewRequest(http.MethodGet, "/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=krateo-system&name=ra410", nil)
	req.RemoteAddr = "10.0.0.9:41000"
	h.ServeHTTP(httptest.NewRecorder(), req)
	return rec.Records()
}

func refStage22(id, verb, path, ref string) *templates.API {
	s := stage410(id, verb, path)
	s.EndpointRef = &templates.Reference{Name: ref, Namespace: raNS410}
	return s
}

func attrs22(r sdklog.Record) map[string]string {
	out := map[string]string{}
	for _, kv := range audittest.Attributes(r) {
		out[string(kv.Key)] = kv.Value.Emit()
	}
	return out
}

func conforms22(t *testing.T, r sdklog.Record, opts ...audittest.Option) {
	t.Helper()
	audittest.AssertConformant(t, audit.Writer, r, opts...)
	audittest.AssertRequiredKeysDetected(t, audit.Writer, r, opts...)
	audittest.AssertEnumsDetected(t, audit.Writer, r, opts...)
}

func TestAudit22_E_C_ANamedEndpointWriteRecordsItsEndpointRef(t *testing.T) {
	recs := resolve22(t, nil, extStage410("render", http.MethodPost, "/render"), stage410("create", http.MethodPost, base22))
	if len(recs) != 2 {
		t.Fatalf("%d records, want 2", len(recs))
	}
	by := map[string]map[string]string{}
	for _, r := range recs {
		a := attrs22(r)
		by[a[krateoaudit.KeyRESTActionStage]] = a
		conforms22(t, r)
	}
	ext, own := by["render"], by["create"]
	if ext[krateoaudit.KeyEndpointRefNamespace] != raNS410 || ext[krateoaudit.KeyEndpointRefName] != extRef410 {
		t.Errorf("endpoint_ref = %q/%q, want %s/%s", ext[krateoaudit.KeyEndpointRefNamespace], ext[krateoaudit.KeyEndpointRefName], raNS410, extRef410)
	}
	if ext[krateoaudit.KeyServerAddress] == "" {
		t.Errorf("the endpointRef row names no server.address")
	}
	for _, k := range []string{krateoaudit.KeyEndpointRefNamespace, krateoaudit.KeyEndpointRefName} {
		if v, ok := own[k]; ok {
			t.Errorf("a stage with no endpointRef carries %s=%q", k, v)
		}
	}
}

func TestAudit22_R_F_S_C_EveryRefusalOfAWriteStageIsOneRow(t *testing.T) {
	own := kubeutil.MakeDNS1123Compatible(user410) + "-clientconfig"
	uaf := stage410("uaf", http.MethodPost, base22)
	uaf.UserAccessFilter = &templates.UserAccessFilterSpec{Verb: "get", Resource: "fireworksapps"}
	dependent := stage410("after", http.MethodPost, base22)
	dependent.ContinueOnError = ptr.To(false)
	dependent.DependsOn = &templates.Dependency{Name: "first"}
	for _, c := range []struct {
		name    string
		mark    func(context.Context) context.Context
		stages  []*templates.API
		stage   string // the refused stage's row
		outcome string
		reason  string
		code    string
		target  bool // the row names a target
	}{
		{"per_user_credential_ref", nil, []*templates.API{refStage22("s", http.MethodPost, "/render", own)},
			"s", krateoaudit.OutcomeDenied, audit.DenialPerUserCredentialRef, "", false},
		{"endpoint_ref_malformed", nil, []*templates.API{refStage22("s", http.MethodPost, "/render", brokenRef22)},
			"s", krateoaudit.OutcomeDenied, audit.DenialEndpointRefMalformed, "", false},
		{"the Secret read failed", nil, []*templates.API{refStage22("s", http.MethodPost, "/render", absentRef22)},
			"s", krateoaudit.OutcomeFailure, "", "404", false},
		{"dependency_failed", nil, []*templates.API{stage410("first", http.MethodPost, denyPath410), dependent},
			"after", krateoaudit.OutcomeDenied, audit.DenialDependencyFailed, "", false},
		{"uaf_on_write_verb", nil, []*templates.API{uaf},
			"uaf", krateoaudit.OutcomeDenied, audit.DenialUAFOnWriteVerb, "", false},
		{"invalid_path", nil, []*templates.API{stage410("s", http.MethodPost, base22+"/a%2Fb")},
			"s", krateoaudit.OutcomeDenied, audit.DenialInvalidPath, "", false},
		{"template_error", nil, []*templates.API{stage410("s", http.MethodPost, `${ error("x22") }`)},
			"s", krateoaudit.OutcomeDenied, audit.DenialTemplateError, "", false},
		{"dry_run_not_executed", cache.WithInert, []*templates.API{stage410("s", http.MethodPost, base22)},
			"s", krateoaudit.OutcomeDenied, audit.DenialDryRunNotExecuted, "", true},
		{"dry_run_not_executed, named endpoint", cache.WithInert, []*templates.API{extStage410("s", http.MethodPost, "/render")},
			"s", krateoaudit.OutcomeDenied, audit.DenialDryRunNotExecuted, "", true},
		{"write_refused (an export)", cache.WithExportRead, []*templates.API{stage410("s", http.MethodPost, base22)},
			"s", krateoaudit.OutcomeDenied, audit.DenialWriteRefused, "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var row *sdklog.Record
			for _, r := range resolve22(t, c.mark, c.stages...) {
				if attrs22(r)[krateoaudit.KeyRESTActionStage] == c.stage {
					if row != nil {
						t.Fatalf("stage %q has more than one row", c.stage)
					}
					row = &r
				}
			}
			if row == nil {
				t.Fatalf("stage %q has no row", c.stage)
			}
			a := attrs22(*row)
			if a[krateoaudit.KeyOutcome] != c.outcome || a[krateoaudit.KeyDenialReason] != c.reason {
				t.Errorf("outcome %q reason %q, want %q %q", a[krateoaudit.KeyOutcome], a[krateoaudit.KeyDenialReason], c.outcome, c.reason)
			}
			if code, ok := a[krateoaudit.KeyHTTPStatusCode]; code != c.code || (c.code == "" && ok) {
				t.Errorf("status code %q (present %t), want %q", code, ok, c.code)
			}
			_, obj := a[krateoaudit.KeyObjectResource]
			_, host := a[krateoaudit.KeyServerAddress]
			if (obj || host) != c.target {
				t.Errorf("names a target: %t, want %t", obj || host, c.target)
			}
			if a[krateoaudit.KeyAction] != "restaction" || a[krateoaudit.KeyHTTPMethod] != http.MethodPost {
				t.Errorf("action %q method %q", a[krateoaudit.KeyAction], a[krateoaudit.KeyHTTPMethod])
			}
			// S — no attribute and no body names a per-user credential Secret.
			for k, v := range a {
				if strings.Contains(v, "-clientconfig") {
					t.Errorf("%s names a per-user credential Secret", k)
				}
			}
			if strings.Contains(row.Body().AsString(), "-clientconfig") {
				t.Errorf("the body names a per-user credential Secret")
			}
			var opts []audittest.Option
			if !c.target {
				opts = append(opts, audittest.AllowIncomplete(krateoaudit.RuleTarget))
			}
			conforms22(t, *row, opts...)
		})
	}
}

func TestAudit22_N_NoRowWithoutARequesterOrForARead(t *testing.T) {
	own := kubeutil.MakeDNS1123Compatible(user410) + "-clientconfig"
	for _, c := range []struct {
		name   string
		mark   func(context.Context) context.Context
		stages []*templates.API
	}{
		{"background work's write", cache.WithBackgroundResolve, []*templates.API{stage410("s", http.MethodPost, base22)}},
		{"a refused read stage", nil, []*templates.API{refStage22("s", http.MethodGet, "/render", own)}},
		{"a read stage whose path is refused", nil, []*templates.API{stage410("s", http.MethodGet, base22+"/a%2Fb")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if recs := resolve22(t, c.mark, c.stages...); len(recs) != 0 {
				t.Errorf("%d rows, want 0", len(recs))
			}
		})
	}
}
