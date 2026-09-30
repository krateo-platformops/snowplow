// resolve_prewarm_loglevel_test.go — #214: a resourceRef RBAC denial during a
// PREWARM walk must not WARN (546 lines/12h on 057). It is downgraded to Debug +
// a bounded counter ONLY on the prewarm path; a SERVE-path denial still WARNs
// (so we never hide a real serve-side access problem). The resolved output is
// byte-identical between the two — only the log LEVEL differs (the load-bearing
// safety proof: this is a log-only change, it must not perturb el.Allowed or the
// returned []ResourceRefResult).
//
// Hermetic: drives the real resolveOne over a BUILTIN GVR (configmaps →
// ConfigMap resolves through cache.KindForGVR's builtin arm, no discovery hop),
// with the RBAC denial coming from rbac.UserCan's fail-closed path (no UserInfo
// on ctx). The unrelated "unable to extract UserInfo" record is filtered out —
// we assert only on the "resource ref action not allowed" record.
//
// RED-first: written before the deps.go marker / counter and the resolve.go
// gate exist, so it does not compile (cache.WithPrewarmPath /
// cache.PrewarmRefDeniedTotal undefined) — the RED-for-the-right-reason signal.

package resourcesrefs

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

type capRec struct {
	level slog.Level
	msg   string
}

type capHandler struct {
	mu   *sync.Mutex
	recs *[]capRec
}

func (h capHandler) Enabled(context.Context, slog.Level) bool { return true } // capture Debug too
func (h capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, capRec{r.Level, r.Message})
	return nil
}
func (h capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h capHandler) WithGroup(string) slog.Handler      { return h }

func ctxWithCapture(t *testing.T, prewarm bool) (context.Context, *[]capRec) {
	t.Helper()
	recs := &[]capRec{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(capHandler{mu: &sync.Mutex{}, recs: recs})))
	if prewarm {
		ctx = cache.WithPrewarmPath(ctx)
	}
	return ctx, recs
}

// denialRecords returns the level and count of the "resource ref action not
// allowed" records, ignoring every other log line.
func denialRecords(recs *[]capRec) (slog.Level, int) {
	var lvl slog.Level
	n := 0
	for _, r := range *recs {
		if r.msg == "resource ref action not allowed" {
			lvl = r.level
			n++
		}
	}
	return lvl, n
}

func TestResolveOne_PrewarmDenial_DebugCountedServeStillWARN_ByteParity(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // rbac.UserCan → EvaluateRBAC path; no UserInfo → deny (fail-closed), no network

	// A create (write) ref on a builtin GVR → KindForGVR builtin arm (no
	// discovery), verb "create", denied by UserCan.
	ref := &templatesv1.ResourceRef{
		ID: "r1", APIVersion: "v1", Resource: "configmaps",
		Name: "cm1", Namespace: "ns1", Verb: "POST",
	}

	// SERVE (unmarked) → the denial STILL WARNs.
	serveCtx, serveRecs := ctxWithCapture(t, false)
	serveOut, err := resolveOne(serveCtx, &rest.Config{}, ref)
	if err != nil {
		t.Fatalf("serve resolveOne errored: %v", err)
	}
	if lvl, n := denialRecords(serveRecs); n != 1 || lvl != slog.LevelWarn {
		t.Fatalf("#214 FAIL: serve-path denial recorded n=%d level=%v, want exactly 1 at WARN "+
			"(a serve denial must stay visible)", n, lvl)
	}
	// Premise (non-vacuous): the ref was actually DENIED.
	if len(serveOut) != 1 || serveOut[0].Allowed {
		t.Fatalf("premise: expected exactly one DENIED resource-ref result, got %+v", serveOut)
	}

	// PREWARM (marked) → the denial is downgraded to Debug + counter+1.
	before := cache.PrewarmRefDeniedTotal()
	pwCtx, pwRecs := ctxWithCapture(t, true)
	pwOut, err := resolveOne(pwCtx, &rest.Config{}, ref)
	if err != nil {
		t.Fatalf("prewarm resolveOne errored: %v", err)
	}
	if lvl, n := denialRecords(pwRecs); n != 1 || lvl != slog.LevelDebug {
		t.Fatalf("#214 FAIL: prewarm-path denial recorded n=%d level=%v, want exactly 1 at DEBUG "+
			"(prewarm denials are expected — de-WARN them, don't hide them)", n, lvl)
	}
	if got := cache.PrewarmRefDeniedTotal(); got != before+1 {
		t.Fatalf("#214 FAIL: prewarm_ref_denied_total moved by %d, want 1 (the counter preserves observability)",
			got-before)
	}

	// BYTE-PARITY (load-bearing): the resolved output is IDENTICAL between the
	// two paths — the change is log-level only, never el.Allowed or the bytes.
	if !reflect.DeepEqual(serveOut, pwOut) {
		t.Fatalf("#214 FAIL: byte-parity broken — prewarm output != serve output.\nserve=%+v\nprewarm=%+v",
			serveOut, pwOut)
	}
}
