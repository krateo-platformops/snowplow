package api

// issue1106_1112_helpers_test.go — the harness of the #1106 and #1112 arms,
// taken from the investigation that reproduced them (a fake third-party
// endpoint reached through a named endpointRef, resolved the way a customer
// /call resolves it, with the cache on).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	x1106NS    = "krateo-system"
	x1106Ref   = "ticketing-ep"
	x1106Token = "EPTOKEN-canary-7f3a9c"
	x1106User  = "u-x1106-tenant"
	// x1106Body is a marker carried by every write payload and every answer,
	// so a dump of either is found by substring.
	x1106Body = "BODY-canary-41d2"
)

type x1106Hit struct {
	Method, Path, Auth, Cookie string
	Body                       string
	PeerCerts                  int
}

// x1106Srv is one fake host: every request it receives is recorded.
type x1106Srv struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits []x1106Hit
}

func newX1106Srv(t *testing.T, tlsServer bool, fn http.HandlerFunc) *x1106Srv {
	t.Helper()
	z := &x1106Srv{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		hit := x1106Hit{Method: r.Method, Path: r.URL.Path, Auth: r.Header.Get("Authorization"),
			Cookie: r.Header.Get("Cookie"), Body: string(b)}
		if r.TLS != nil {
			hit.PeerCerts = len(r.TLS.PeerCertificates)
		}
		z.mu.Lock()
		z.hits = append(z.hits, hit)
		z.mu.Unlock()
		fn(w, r)
	})
	if tlsServer {
		z.srv = httptest.NewTLSServer(h)
	} else {
		z.srv = httptest.NewServer(h)
	}
	t.Cleanup(z.srv.Close)
	return z
}

func (z *x1106Srv) all() []x1106Hit {
	z.mu.Lock()
	defer z.mu.Unlock()
	return append([]x1106Hit(nil), z.hits...)
}

// x1106OK answers a JSON object carrying the body marker.
func x1106OK(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"ok":true,"echo":"`+x1106Body+`"}`)
}

// x1106OtherURL is s's URL under the hostname "localhost": another host AND
// another port than an endpoint served on 127.0.0.1.
func x1106OtherURL(s *httptest.Server) string {
	return strings.Replace(s.URL, "127.0.0.1", "localhost", 1)
}

type x1106Writer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *x1106Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *x1106Writer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// x1106Env publishes the endpointRef Secrets the way production finds them
// with the cache on: the SA-backed Secrets snapshot of the namespace. own, when
// set, is the requester's own clientconfig server (a stage with no endpointRef
// dials it: a held, own-cluster dial).
func x1106Env(t *testing.T, own string, secrets map[string]map[string]string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	by := map[string]*corev1.Secret{}
	if own != "" {
		secrets[kubeutil.MakeDNS1123Compatible(x1106User)+"-clientconfig"] = map[string]string{"server-url": own, "token": "own-tok"}
	}
	for name, data := range secrets {
		d := map[string][]byte{}
		for k, v := range data {
			d[k] = []byte(v)
		}
		by[name] = authnOwned(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: x1106NS, Name: name}, Data: d}, x1106User)
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: by})
	cache.ForceSecretsCacheReadyForTest(x1106NS)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
}

// x1106Secret is a bearer-token endpoint Secret for url, plus extra keys.
func x1106Secret(url string, extra ...string) map[string]string {
	m := map[string]string{"server-url": url, "token": x1106Token, "insecure": "true"}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i]] = extra[i+1]
	}
	return m
}

type x1106Out struct {
	dict     map[string]any
	dictJSON string
	failure  *StageFailure
	outcomes string
	logs     string
}

type x1106Opts struct {
	verbose bool
}

func x1106Resolve(t *testing.T, extras map[string]any, stages ...*templates.API) x1106Out {
	t.Helper()
	return x1106ResolveWith(t, x1106Opts{}, extras, stages...)
}

func x1106ResolveWith(t *testing.T, o x1106Opts, extras map[string]any, stages ...*templates.API) x1106Out {
	t.Helper()
	w := &x1106Writer{}
	lg := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prev := slog.Default()
	slog.SetDefault(lg)
	t.Cleanup(func() { slog.SetDefault(prev) })
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: x1106User, Groups: []string{"tenants"}}),
		xcontext.WithLogger(lg))
	ctx, oc := WithStageOutcomes(ctx)
	dict, f := ResolveWithFailure(ctx, ResolveOptions{
		RC: &rest.Config{Host: "http://127.0.0.1:1"}, AuthnNS: x1106NS, Items: stages, Extras: extras,
		RESTActionNamespace: x1106NS, RESTActionName: "ticket-actions", Verbose: o.verbose,
	})
	out := x1106Out{dict: dict, failure: f, outcomes: oc.HeaderValue(), logs: w.String()}
	b, _ := json.Marshal(dict)
	out.dictJSON = string(b)
	return out
}

// x1106Stage is a named-endpointRef stage without continueOnError.
func x1106Stage(name, verb, path string) *templates.API {
	return &templates.API{Name: name, Verb: ptr.To(verb), Path: path,
		EndpointRef: &templates.Reference{Name: x1106Ref, Namespace: x1106NS}, ErrorKey: ptr.To(name + "Err")}
}

// x1106Write is x1106Stage with a JSON payload carrying the body marker.
func x1106Write(name, verb, path string) *templates.API {
	s := x1106Stage(name, verb, path)
	s.Payload = ptr.To(`${ {ticket:{comment:{body: .text}}} | tojson }`)
	s.Headers = []string{"Content-Type: application/json"}
	return s
}

// x1106Own is a stage with no endpointRef: it dials the requester's own
// clientconfig (this cluster's apiserver, a held credential).
func x1106Own(name, verb, path string) *templates.API {
	s := x1106Write(name, verb, path)
	s.EndpointRef = nil
	return s
}

var x1106Extras = map[string]any{"ticketId": float64(42), "text": x1106Body}

type x1106Row struct {
	Name, Reason, DependsOn string
	OK                      bool
}

func x1106Rows(t *testing.T, header string) []x1106Row {
	t.Helper()
	var rows []x1106Row
	if err := json.Unmarshal([]byte(header), &rows); err != nil {
		t.Fatalf("X-Snowplow-Stage-Outcomes is not a JSON array: %q (%v)", header, err)
	}
	return rows
}

// x1106WantReason asserts stage name is listed as failed with reason.
func x1106WantReason(t *testing.T, header, name, reason string) {
	t.Helper()
	for _, r := range x1106Rows(t, header) {
		if r.Name == name {
			if r.OK || r.Reason != reason {
				t.Errorf("stage %q: outcome %+v, want ok=false reason=%s (header %s)", name, r, reason, header)
			}
			return
		}
	}
	t.Errorf("stage %q is not listed as failed (want reason %s): header %s", name, reason, header)
}

// x1106NoSecretIn fails when the endpoint token or the body marker is in any
// of the resolve's outputs.
func x1106NoSecretIn(t *testing.T, label string, o x1106Out, secrets ...string) {
	t.Helper()
	fs := ""
	if o.failure != nil {
		fs = fmt.Sprintf("%+v", *o.failure)
	}
	for name, s := range map[string]string{"dict": o.dictJSON, "failure": fs, "outcomes": o.outcomes, "logs": o.logs} {
		for _, sec := range secrets {
			if strings.Contains(s, sec) {
				t.Errorf("%s: %q found in the %s", label, sec, name)
			}
		}
	}
}
