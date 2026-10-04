// inline443_secret_test.go — #443 part 2, arm (f): an inline draft whose stage
// reads a core v1 Secret (#398 contract, design §2).
//
// The Secret is read LIVE from the apiserver AS THE CALLER — never with
// snowplow's ServiceAccount, never from an informer — and it is neither
// cached nor informed. It is NOT refused (owner ruling: UAF stays uniform; the
// #398 fix lives where Secret data is held): an allowed caller is served the
// data, a denied caller gets the apiserver's own 403 as the stage error.
//
//	f1 SecretGETReadLiveAsCaller — alice (get secrets in x) is served the
//	   Secret; every Secret request the apiserver saw carried alice's token;
//	   a repeat read reaches the apiserver again; no informer, no L1 cell, no
//	   refresh-key header; the stage-outcomes header shows the stage as
//	   executed (ok), not StageNotExecuted.
//	f2 SecretGETDeniedCallerGetsApiserver403 — bob (no secrets grant) reaches
//	   the apiserver as himself and gets its 403 (outcome Forbidden): snowplow
//	   does not refuse the stage, and nothing of alice's read is served.
//	f3 UAFSecretStageDialsCaller — a userAccessFilter stage LISTing secrets is
//	   dialed as the caller, never the ServiceAccount (stored UAF semantics).
//
// The ServiceAccount endpoint seam points at the same fake with a distinct
// token ("tok-sa"), and the handler's saRC carries none, so any non-caller
// Secret request is visible as a user other than the caller.
package dispatchers

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// in443SecretArm: target core v1/secrets x/y, alice granted get/list on
// secrets in x, bob not; never pre-registered (#398 steady state).
var in443SecretArm = psArm{name: "inline443-secret", target: psSecretsGVR, userBound: true, watchShape: true, noPrereg: true}

const (
	in443SecretPath     = "/api/v1/namespaces/" + psTargetNS + "/secrets/" + psTargetObj
	in443SecretListPath = "/api/v1/namespaces/" + psTargetNS + "/secrets"
)

func in443SecretSetup(t *testing.T) *in443API {
	t.Helper()
	f := in443SetupFor(t, in443SecretArm)
	t.Cleanup(api.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: f.srv.URL, Token: "tok-sa"}, nil
	}))
	return f
}

// secretReqUsers returns the identity of every request the fake apiserver
// received on path (a token label, never a Secret name).
func secretReqUsers(f *in443API, path string) []string {
	var users []string
	for _, r := range f.requests() {
		if r.path == path {
			users = append(users, r.user)
		}
	}
	return users
}

// assertSecretHeldNowhere: no v1/secrets informer and no L1 cell (any class)
// carries the Secret's data.
func assertSecretHeldNowhere(t *testing.T, what string) {
	t.Helper()
	if rw := cache.Global(); rw != nil && rw.IsRegistered(psSecretsGVR) {
		t.Errorf("%s: an inline draft registered a v1/secrets informer (a cluster-wide informer now holds every Secret)", what)
	}
	c := cache.ResolvedCache()
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && psHasSentinel(e.RawJSON) {
			class := ""
			if e.Inputs != nil {
				class = e.Inputs.CacheEntryClass
			}
			t.Errorf("%s: an L1 cell (class %q) holds the Secret's data", what, class)
		}
	}
}

func assertNoRefreshKey(t *testing.T, rec interface{ Header() http.Header }, what string) {
	t.Helper()
	if v := rec.Header().Get(refreshKeyHeader); v != "" {
		t.Errorf("%s: reply carries %s (a subscription key for a cell that must not exist)", what, refreshKeyHeader)
	}
	if v := rec.Header().Get(refreshClassHeader); v != "" {
		t.Errorf("%s: reply carries %s=%q", what, refreshClassHeader, v)
	}
}

func TestS443_InlineSecret(t *testing.T) {
	t.Run("f1_SecretGETReadLiveAsCaller", func(t *testing.T) {
		f := in443SecretSetup(t)
		alice := in443Ctx(f, psAlice)
		obj := in443RA("draft-f1", "", map[string]any{"name": "secret", "continueOnError": true, "errorKey": "secretErr",
			"path": in443SecretPath})
		before := takeIn443State(t)
		f.reset()
		rec := serveInline(t, f, alice, obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("the inline Secret read was refused: code=%d body=%s", rec.Code, psTrunc(rec.Body.String(), 300))
		}
		if !psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("alice (get secrets in x) was not served the Secret live; body=%s", psTrunc(rec.Body.String(), 400))
		}
		users := secretReqUsers(f, in443SecretPath)
		if len(users) == 0 {
			t.Fatalf("the Secret was never read from the apiserver (served from an informer or a cache); reqs=%d", len(f.requests()))
		}
		for _, u := range users {
			if u != psAlice {
				t.Errorf("the Secret was read from the apiserver as %q, want the caller (%s) — never snowplow's ServiceAccount", u, psAlice)
			}
		}
		assertInlineEcho(t, rec, "f1")
		assertNoRefreshKey(t, rec, "f1")
		out := parseOutcomes443(t, rec.Header().Get("X-Snowplow-Stage-Outcomes"))
		if row, ok := out["secret"]; !ok || !row.OK || row.Reason == api.StageReasonNotExecuted {
			t.Errorf("f1: the Secret stage must be EXECUTED (ok, not %s); outcome=%+v", api.StageReasonNotExecuted, row)
		}
		time.Sleep(50 * time.Millisecond) // a registration would be synchronous; settle any async edge
		assertSecretHeldNowhere(t, "f1")
		diffIn443State(t, "f1 inline Secret read", before, takeIn443State(t))

		// Live, every time: a repeat read reaches the apiserver again as alice.
		n := len(users)
		rec2 := serveInline(t, f, alice, obj)
		if rec2.Code != http.StatusOK || !psHasSentinel(rec2.Body.Bytes()) {
			t.Fatalf("repeat read: code=%d", rec2.Code)
		}
		users2 := secretReqUsers(f, in443SecretPath)
		if len(users2) <= n {
			t.Errorf("a repeat inline Secret read did not reach the apiserver (served from a cache)")
		}
		for _, u := range users2[n:] {
			if u != psAlice {
				t.Errorf("repeat read: the Secret was read as %q, want the caller", u)
			}
		}
		assertNoRefreshKey(t, rec2, "f1 repeat")
		assertSecretHeldNowhere(t, "f1 repeat")
	})

	t.Run("f2_SecretGETDeniedCallerGetsApiserver403", func(t *testing.T) {
		f := in443SecretSetup(t)
		obj := in443RA("draft-f2", "", map[string]any{"name": "secret", "continueOnError": true, "errorKey": "secretErr",
			"path": in443SecretPath})
		// alice reads first: nothing of her read may reach bob.
		if rec := serveInline(t, f, in443Ctx(f, psAlice), obj); !psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("SETUP: alice must be served the Secret; code=%d", rec.Code)
		}
		f.reset()
		rec := serveInline(t, f, in443Ctx(f, psBob), obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("bob's draft was refused by snowplow (want a 200 carrying the apiserver's 403 stage error): code=%d", rec.Code)
		}
		if psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("bob (no secrets grant) was served the Secret's data; body=%s", psTrunc(rec.Body.String(), 300))
		}
		users := secretReqUsers(f, in443SecretPath)
		if len(users) == 0 {
			t.Fatalf("bob's Secret stage never reached the apiserver (refused or cached before egress)")
		}
		for _, u := range users {
			if u != psBob {
				t.Errorf("bob's Secret stage was read as %q, want the caller (%s)", u, psBob)
			}
		}
		if !strings.Contains(rec.Body.String(), `"code":403`) {
			t.Errorf("bob must see the apiserver's own 403 under secretErr; body=%s", psTrunc(rec.Body.String(), 400))
		}
		out := parseOutcomes443(t, rec.Header().Get("X-Snowplow-Stage-Outcomes"))
		if row := out["secret"]; row.OK || row.Reason != api.StageReasonForbidden {
			t.Errorf("f2: bob's Secret stage outcome=%+v, want ok=false reason=%s (the apiserver's, not %s)",
				row, api.StageReasonForbidden, api.StageReasonNotExecuted)
		}
		assertNoRefreshKey(t, rec, "f2")
		assertSecretHeldNowhere(t, "f2")
	})

	t.Run("f3_UAFSecretStageDialsCaller", func(t *testing.T) {
		f := in443SecretSetup(t)
		obj := in443RA("draft-f3", "", map[string]any{"name": "secrets", "continueOnError": true, "errorKey": "secretsErr",
			"path":             in443SecretListPath,
			"userAccessFilter": map[string]any{"verb": "get", "resource": "secrets", "group": ""}})
		before := takeIn443State(t)
		f.reset()
		rec := serveInline(t, f, in443Ctx(f, psAlice), obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("the inline UAF Secret stage was refused: code=%d", rec.Code)
		}
		users := secretReqUsers(f, in443SecretListPath)
		if len(users) == 0 {
			t.Fatalf("the UAF Secret stage never dialed the apiserver; body=%s", psTrunc(rec.Body.String(), 400))
		}
		for _, u := range users {
			if u != psAlice {
				t.Errorf("the UAF Secret stage was dialed as %q, want the caller (%s) — a caller-supplied draft never reads Secrets with snowplow's ServiceAccount", u, psAlice)
			}
		}
		if !psHasSentinel(rec.Body.Bytes()) {
			t.Errorf("alice (list+get secrets in x) was not served her Secret; body=%s", psTrunc(rec.Body.String(), 400))
		}
		out := parseOutcomes443(t, rec.Header().Get("X-Snowplow-Stage-Outcomes"))
		if row, ok := out["secrets"]; !ok || !row.OK {
			t.Errorf("f3: the UAF Secret stage must be EXECUTED (ok); outcome=%+v", row)
		}
		assertNoRefreshKey(t, rec, "f3")
		time.Sleep(50 * time.Millisecond)
		assertSecretHeldNowhere(t, "f3")
		diffIn443State(t, "f3 inline UAF Secret stage", before, takeIn443State(t))
	})
}
