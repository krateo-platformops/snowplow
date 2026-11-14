// plumbing_v172_key_set_time_test.go — the one behaviour change of the plumbing
// v1.68.0 → v1.74.0 bump that these middlewares depend on (plumbing#169,
// v1.72.0): a fetched key set is timestamped when its fetch STARTED, not when
// it completed.
//
// jwtutil decides between two answers for a token whose key id is absent from
// authn's key set, and both middlewares map them:
//
//   - ErrKeyNotPublished → 401 KeyNotPublished: the key was retired; no retry
//     makes the token verify, and the client signs in again;
//   - ErrKeyUnavailable  → 503: the set may predate the key; the client
//     retries, and a later fetch finds it.
//
// The decision is "was the token issued before the set was fetched (by more
// than 5 s of leeway)". With the set's time at the fetch's END, a token issued
// shortly before or during a slow fetch read as issued before the set, so a
// key authn published while the round trip was on the wire was called retired:
// a wrong, permanent 401. With the set's time at the fetch's START, the same
// request is a 503.
//
// THE ARM. authn's JWKS answer takes several seconds and does not hold the
// token's key. The token was issued 2 s before the request, inside the leeway
// of the fetch's start and more than the leeway before its end. Through the
// REAL middlewares and the REAL JWKSKeySource:
//
//	plumbing v1.68.0 (measured on main before the bump): 401 KeyNotPublished
//	plumbing v1.74.0 (this test):                        503, no cause
//
// A token issued well before the fetch started stays 401 KeyNotPublished: that
// is predispatch_401_keynotpublished_test.go, unchanged by the bump.
package middleware_test

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

func TestPlumbingV172_ATokenIssuedAroundASlowKeySetFetchIs503NotKeyNotPublished(t *testing.T) {
	if testing.Short() {
		t.Skip("holds a JWKS response for several seconds")
	}
	t.Setenv("CACHE_ENABLED", "false")
	// The fetch takes longer than jwtutil's 5 s leeway minus the token's age
	// (2 to 3 s once truncated to the second), with margin on both sides.
	const fetchTakes = 4500 * time.Millisecond

	for _, mw := range knpMiddlewares {
		t.Run(mw.name, func(t *testing.T) {
			t.Parallel()
			published, signing := knpKey(t), knpKey(t)
			authn := &knpJWKS{}
			authn.publish(map[string]*rsa.PublicKey{"key-2": &published.PublicKey})
			inner := authn.serve(t)
			// authn, slow: the same key set, answered after fetchTakes.
			slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(fetchTakes)
				resp, err := http.Get(inner)
				if err != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				defer resp.Body.Close()
				var set map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&set)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(set)
			}))
			t.Cleanup(slow.Close)
			keys := jwtutil.NewJWKSKeySource(slow.URL)

			// Signed with a key authn does not publish (yet), issued 2 s ago.
			issued := time.Now().Add(-2 * time.Second).Truncate(time.Second)
			tok := knpMint(t, signing, "key-3", &issued)

			start := time.Now()
			rec, reached := p1076Serve(mw.mk(keys), knpRequest(tok))
			took := time.Since(start)
			t.Logf("answer after %s: code=%d cause=%q body=%s", took.Round(100*time.Millisecond), rec.Code, knpCause(rec.Body.Bytes()), strings.TrimSpace(rec.Body.String()))

			// PRECONDITIONS that make this the v1.72.0 case: the fetch really was
			// slow, so its end is more than the leeway after the token's issue
			// time while its start is not.
			if took < fetchTakes {
				t.Fatalf("precondition: the key-set fetch took %s, want at least %s", took, fetchTakes)
			}
			if !issued.Add(5 * time.Second).Before(start.Add(took)) {
				t.Fatalf("precondition: the token (issued %s) must be more than 5s older than the fetch's end (%s)", issued, start.Add(took))
			}
			if issued.Add(5 * time.Second).Before(start) {
				t.Fatalf("precondition: the token (issued %s) must be within 5s of the fetch's start (%s)", issued, start)
			}

			if reached {
				t.Fatalf("the request reached the handler with a token whose key is not published")
			}
			if rec.Code == http.StatusUnauthorized {
				t.Fatalf("401 %s: a key the set may predate was called retired (plumbing < v1.72.0 answers this); body=%s",
					knpCause(rec.Body.Bytes()), rec.Body.String())
			}
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("want 503; code=%d body=%s", rec.Code, rec.Body.String())
			}
			// The 503 is plumbing's, byte for byte: no cause, no challenge.
			var st response.Status
			_ = json.Unmarshal(rec.Body.Bytes(), &st)
			plumbing := httptest.NewRecorder()
			_ = response.ServiceUnavailable(plumbing, errors.New(st.Message))
			if rec.Body.String() != plumbing.Body.String() || strings.Contains(rec.Body.String(), "causes") {
				t.Fatalf("the 503 must stay plumbing's, byte for byte:\n got  %s\n want %s", rec.Body.String(), plumbing.Body.String())
			}
			if got := rec.Header().Values("WWW-Authenticate"); len(got) != 0 {
				t.Fatalf("a 503 sends no challenge; got %q", got)
			}
		})
	}
}
