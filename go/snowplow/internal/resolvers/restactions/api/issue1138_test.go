package api

// issue1138_test.go — #1138, the #1120 review's residuals, at the resolver:
//
//   A  the nameless PUT/PATCH refusal is own-cluster only: on a named endpoint
//      a nameless PUT/PATCH to an apiserver-SHAPED path (no trailing slash) is
//      sent; the same PUT on an own-cluster dial is refused (InvalidPath).
//   B  a credential in a stage header with a deliberately odd name (one
//      nothing in the stack strips by name) never follows a redirect to
//      another origin: 0 requests at the other host. The control shows the
//      header does ride a same-origin hop, so the zero is the origin rule, not
//      a header strip.
//   C  the #288 skip's DEBUG line logs the rendered path with its query values
//      replaced, never the value itself.

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/redact"
)

func TestIssue1138_A_NamelessPutPatchOnANamedRefIsSent(t *testing.T) {
	for _, tc := range []struct{ verb, path string }{
		{http.MethodPut, `${ "/api/v2/tickets" }`},
		{http.MethodPatch, `${ "/api/v2/tickets" }`},
		{http.MethodPut, `${ "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps" }`},
		{http.MethodPatch, `${ "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps" }`},
	} {
		t.Run(tc.verb+"_"+tc.path, func(t *testing.T) {
			ep := newX1106Srv(t, false, x1106OK)
			x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
			o := x1106Resolve(t, map[string]any{"text": x1106Body}, x1106Write("w", tc.verb, tc.path))
			if n := len(ep.all()); n != 1 || o.outcomes != "[]" {
				t.Fatalf("RED: a named ref's nameless %s %s must be sent: sent %d, header %s", tc.verb, tc.path, n, o.outcomes)
			}
		})
	}
	// own-cluster control: the same nameless PUT/PATCH is refused (InvalidPath)
	for _, verb := range []string{http.MethodPut, http.MethodPatch} {
		api := newX1106Srv(t, false, x1106OK)
		x1106Env(t, api.srv.URL, map[string]map[string]string{})
		o := x1106Resolve(t, map[string]any{"text": x1106Body},
			x1106Own("w", verb, `${ "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps" }`))
		if n := len(api.all()); n != 0 {
			t.Fatalf("control: an own-cluster nameless %s was dialled %d time(s)", verb, n)
		}
		x1106WantReason(t, o.outcomes, "w", StageReasonInvalidPath)
	}
}

// x1138Cargo is a header name nothing in net/http, plumbing or snowplow
// strips by name (Go strips only Authorization, WWW-Authenticate,
// Proxy-Authorization and the cookie headers on a cross-host hop; the SA dial
// drops only Authorization and Impersonate-*).
const (
	x1138Cargo   = "X-Zq1138-Cargo-Hold"
	x1138Carried = "cargo-canary-zq"
)

// x1138Recorder is a fake host that records the odd header of every request.
type x1138Recorder struct {
	mu    sync.Mutex
	cargo []string
}

func (r *x1138Recorder) record(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cargo = append(r.cargo, req.Header.Get(x1138Cargo))
}

func (r *x1138Recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.cargo...)
}

func TestIssue1138_B_OddNamedHeaderCredentialNeverLeavesTheOrigin(t *testing.T) {
	for _, verb := range []string{http.MethodGet, http.MethodPut} {
		for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
			for _, target := range []string{"other_host", "same_hostname_other_port"} {
				name := verb + "_" + strconv.Itoa(code) + "_" + target
				t.Run(name, func(t *testing.T) {
					otherRec, epRec := &x1138Recorder{}, &x1138Recorder{}
					other := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
						otherRec.record(r)
						x1106OK(w, r)
					})
					to := x1106OtherURL(other.srv)
					if target == "same_hostname_other_port" {
						to = other.srv.URL
					}
					ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
						epRec.record(r)
						http.Redirect(w, r, to+"/stolen", code)
					})
					// the endpoint has no credential of its own: the odd header is
					// the only one the dial carries
					x1106Env(t, "", map[string]map[string]string{x1106Ref: {"server-url": ep.srv.URL}})
					s := x1106Write("w", verb, "/zd/tickets/42")
					s.Headers = append(s.Headers, x1138Cargo+": "+x1138Carried)
					o := x1106Resolve(t, x1106Extras, s)

					if got := epRec.all(); len(got) != 1 || got[0] != x1138Carried {
						t.Fatalf("PRE: the endpoint gets the one request carrying %s, got %q", x1138Cargo, got)
					}
					if n := len(other.all()); n != 0 {
						t.Fatalf("RED: %d request(s) reached the other origin (%s carried=%v)",
							n, x1138Cargo, len(otherRec.all()) > 0 && otherRec.all()[0] == x1138Carried)
					}
					x1106WantReason(t, o.outcomes, "w", StageReasonError)
					x1106NoSecretIn(t, name, o, x1138Carried)
				})
			}
		}
	}
	// control: on a same-origin hop the odd header IS carried, so nothing
	// strips it by name and the zero above is the origin rule.
	rec := &x1138Recorder{}
	ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		if r.URL.Path == "/zd/tickets/42" {
			http.Redirect(w, r, "/zd/tickets/42/moved", http.StatusTemporaryRedirect)
			return
		}
		x1106OK(w, r)
	})
	x1106Env(t, "", map[string]map[string]string{x1106Ref: {"server-url": ep.srv.URL}})
	s := x1106Write("w", http.MethodPut, "/zd/tickets/42")
	s.Headers = append(s.Headers, x1138Cargo+": "+x1138Carried)
	o := x1106Resolve(t, x1106Extras, s)
	if got := rec.all(); len(got) != 2 || got[1] != x1138Carried || o.outcomes != "[]" {
		t.Fatalf("control: the same-origin hop must carry %s (got %q, header %s)", x1138Cargo, got, o.outcomes)
	}
}

func TestIssue1138_C_MalformedDialSkipDebugPathIsRedacted(t *testing.T) {
	const canary = "qval-canary-zq"
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	s := x1106Own("r", http.MethodGet, `${ "/api/v1/namespaces/" + .ns + "/configmaps/cm?token=" + .tok }`)
	s.Payload, s.Headers = nil, nil
	o := x1106Resolve(t, map[string]any{"ns": "", "tok": canary}, s)
	if n := len(api.all()); n != 0 {
		t.Fatalf("PRE: the collapsed namespace must not be dialled, %d sent", n)
	}
	var line map[string]any
	for _, l := range strings.Split(o.logs, "\n") {
		if strings.Contains(l, `"msg":"skipping api call: collapsed empty namespace segment`) {
			if err := json.Unmarshal([]byte(l), &line); err != nil {
				t.Fatalf("PRE: the skip line is JSON: %v", err)
			}
		}
	}
	if line == nil {
		t.Fatalf("PRE: the #288 skip's DEBUG line is logged; logs: %s", o.logs)
	}
	if want := "/api/v1/namespaces//configmaps/cm?token=" + redact.URLRedactedValue; line["path"] != want {
		t.Fatalf("RED: the skip line's path = %v, want %q", line["path"], want)
	}
	if strings.Contains(o.logs, canary) {
		t.Fatalf("RED: the query value is in the logs: %s", o.logs)
	}
}
