// extras_body_channel_186_test.go — #186 cross-channel key-parity + HIT at the
// real dispatch level. The load-bearing constraint (arch/pm C2a): a read whose
// `extras` arrives in the POST body must derive the IDENTICAL L1 cell a
// query-supplied read derives, and HIT it — no key divergence, no cold-nav for
// the same logical request.
//
// This drives BOTH real channels end to end over the REAL RESTAction dispatch:
//  1. query channel — GET /call?extras=<J> cold-resolves and Puts a cell;
//  2. body channel  — the REAL middleware.BodyExtrasDecode decodes
//     {"extras":<J>} onto the context, and the ctx-first util.ParseExtras
//     feeds the SAME map into the SAME key derivation → a HIT.
//
// The HIT is asserted by the resolve COUNTER (feedback_l1_hit_miss_via_metrics
// _not_timing): the body channel must resolve ZERO additional times, and serve
// bytes identical to the query-populated cell.
package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
)

func TestExtrasBodyChannel_CrossChannel_KeyParity_And_Hit(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)

	// A realistic, non-trivial extras payload (nested + numeric) — the class of
	// input that 431s in the URL today.
	extras := map[string]any{
		"chart":    map[string]any{"values.yaml": "replicaCount: 3\n"},
		"replicas": float64(3),
	}
	rawExtras, err := json.Marshal(extras)
	if err != nil {
		t.Fatalf("marshal extras: %v", err)
	}

	var resolveCalls int
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			resolveCalls++
			// Sanity: the resolver must receive the SAME extras on both channels.
			if opts.Extras["replicas"] != float64(3) {
				t.Fatalf("resolver got unexpected extras: %#v", opts.Extras)
			}
			return &templatesv1.RESTAction{}, nil
		})
	defer restore()

	// --- Channel 1: query (GET ?extras=) COLD → resolve once + Put. ---
	recQ := httptest.NewRecorder()
	reqQ := httptest.NewRequest("GET", "/call?extras="+url.QueryEscape(string(rawExtras)), nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(recQ, reqQ)
	if recQ.Code != 200 {
		t.Fatalf("query cold serve: got %d body=%s", recQ.Code, recQ.Body.String())
	}
	if resolveCalls != 1 {
		t.Fatalf("query cold serve must resolve exactly once; resolveCalls=%d", resolveCalls)
	}
	queryBody := append([]byte(nil), recQ.Body.Bytes()...)

	// --- Channel 2: body (POST /call/read {"extras":<J>}) via the REAL
	// BodyExtrasDecode middleware → context → same key → HIT. ---
	envelope, err := json.Marshal(map[string]any{"extras": extras})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// r now carries the body-decoded extras on the SAME identity ctx.
		RESTAction().ServeHTTP(w, r)
	})
	h := middleware.BodyExtrasDecode(inner)
	recB := httptest.NewRecorder()
	reqB := httptest.NewRequest("POST", "/call/read", bytes.NewReader(envelope)).WithContext(reqCtx)
	h.ServeHTTP(recB, reqB)
	if recB.Code != 200 {
		t.Fatalf("body-channel serve: got %d body=%s", recB.Code, recB.Body.String())
	}

	// KEY PARITY + HIT: the body channel resolved ZERO additional times (it hit
	// the cell the query channel populated) and served identical bytes.
	if resolveCalls != 1 {
		t.Fatalf("CROSS-CHANNEL MISS: the body channel re-resolved (resolveCalls=%d, was 1 after the query populate) — body-supplied extras derived a DIFFERENT key than query-supplied, i.e. a cold-nav for the same logical request", resolveCalls)
	}
	if !bytes.Equal(recB.Body.Bytes(), queryBody) {
		t.Fatalf("body-channel served bytes differ from the query-populated cell:\n body=%q\nquery=%q", recB.Body.Bytes(), queryBody)
	}
}
