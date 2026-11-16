package api

// plan_before_endpoint_1477_test.go — #1477 (a): runStage plans a stage's
// requests BEFORE it resolves the stage's endpointRef, and resolves the
// endpoint only when the plan has a request.
//
// The shape is portal#450's clusters page: a `target` stage lists what the
// request names, and `spokeCrds` iterates over it through a NAMED endpointRef.
// When `target` yields nothing, spokeCrds plans zero requests; before #1477 it
// still read its endpointRef Secret first, and a missing Secret stopped the
// whole resolve with an ERROR line.
//
// The fake apiserver is the requester's clientconfig endpoint AND the
// apiserver the named Secret is read from (the snapshot does not hold it, so
// the read falls through to a GET .../secrets/<name>, answered 404). Every
// Secret read is counted on the wire: "no Secret read" is a count of 0, not an
// absence of an error.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/client-go/rest"
)

const absentRef1477 = "absent-spoke-endpoint"

// rec1477 records every log record a resolve writes.
type rec1477 struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *rec1477) Enabled(context.Context, slog.Level) bool { return true }
func (h *rec1477) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *rec1477) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *rec1477) WithGroup(string) slog.Handler      { return h }

func (h *rec1477) atOrAbove(l slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.recs {
		if r.Level >= l {
			out = append(out, r.Level.String()+" "+r.Message)
		}
	}
	return out
}

func (h *rec1477) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r.Message == msg {
			return true
		}
	}
	return false
}

// resolve1477 resolves target → spokeCrds with `targetItems` as the target
// list's items, and returns the dict, the failure, the Secret reads seen on
// the wire, and the log.
func resolve1477(t *testing.T, targetItems string) (map[string]any, *StageFailure, int64, *rec1477) {
	t.Helper()
	var secretReads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/secrets/"):
			secretReads.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		default:
			_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"List","items":`+targetItems+`}`)
		}
	}))
	t.Cleanup(srv.Close)
	env410(t, srv.URL, srv.URL)

	target := &templates.API{Name: "target", Path: "/apis/clusters.example.io/v1/namespaces/krateo-system/clusters",
		Verb: ptr.To(http.MethodGet)}
	spoke := &templates.API{Name: "spokeCrds", Path: `${ "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + .name }`,
		Verb:        ptr.To(http.MethodGet),
		EndpointRef: &templates.Reference{Name: absentRef1477, Namespace: raNS410},
		DependsOn:   &templates.Dependency{Name: "target", Iterator: ptr.To(".target.items")},
	}

	rec := &rec1477{}
	ctx := xcontext.BuildContext(ctxWithUser(user410, group410), xcontext.WithLogger(slog.New(rec)))
	dict, failure := ResolveWithFailure(ctx, ResolveOptions{RC: &rest.Config{Host: srv.URL}, AuthnNS: raNS410,
		Items: []*templates.API{target, spoke}, RESTActionNamespace: raNS410, RESTActionName: "cluster-compositions-1477"})
	return dict, failure, secretReads.Load(), rec
}

// A stage whose iterator yields nothing resolves to an empty result: its
// endpointRef Secret is never read, nothing is logged above DEBUG, and the
// resolve does not fail.
func TestIssue1477_ZeroItemStage_MissingEndpointSecret_NotRead(t *testing.T) {
	dict, failure, reads, rec := resolve1477(t, `[]`)
	if reads != 0 {
		t.Fatalf("#1477 RED: a stage that plans no request read its endpointRef Secret %d time(s)", reads)
	}
	if _, ok := dict["target"]; !ok {
		t.Fatalf("precondition: the target stage did not run (dict keys %v)", keys1477(dict))
	}
	if _, ok := dict["spokeCrds"]; ok {
		t.Fatalf("a zero-request stage wrote a result: %v", dict["spokeCrds"])
	}
	if failure != nil {
		t.Fatalf("#1477 RED: the resolve failed on a stage that sends nothing: %+v", failure)
	}
	if got := rec.atOrAbove(slog.LevelWarn); len(got) != 0 {
		t.Fatalf("#1477 RED: a zero-request stage logged above DEBUG: %v", got)
	}
	if rec.has("unable to resolve api endpoint reference") {
		t.Fatal("#1477 RED: the endpoint of a zero-request stage was resolved")
	}
}

// A stage with one item still reads its endpointRef Secret and fails exactly
// as before: the read misses, the ERROR line is written, the stage produces
// nothing, and the resolve reports the stage failure.
func TestIssue1477_OneItemStage_MissingEndpointSecret_StillFails(t *testing.T) {
	dict, failure, reads, rec := resolve1477(t, `[{"name":"widgets.example.io"}]`)
	if reads == 0 {
		t.Fatal("a stage that plans a request did not read its endpointRef Secret")
	}
	if !rec.has("unable to resolve api endpoint reference") {
		t.Fatalf("the missing Secret was not logged as before; log at WARN+ = %v", rec.atOrAbove(slog.LevelWarn))
	}
	if _, ok := dict["spokeCrds"]; ok {
		t.Fatalf("a stage whose endpoint could not be resolved wrote a result: %v", dict["spokeCrds"])
	}
	if failure == nil || failure.Stage != "spokeCrds" {
		t.Fatalf("the resolve must report the spokeCrds stage failure; got %+v", failure)
	}
}

func keys1477(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
