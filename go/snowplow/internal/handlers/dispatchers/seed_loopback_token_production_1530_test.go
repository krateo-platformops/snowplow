package dispatchers

// #1530, what changes in production, where the projected authn-audience token file exists and authn answers: only
// WHEN the token is fetched. The real authn.Client against a stand-in authn: building Phase 1 SA contexts exchanges
// nothing; the first bearer-carrying stage that asks triggers the one exchange; every later ask, from that context or
// from another pass's, is served from authn.Client's own cache (until shortly before the JWT's exp), the same token.

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/authn"
	"github.com/krateo-platformops/snowplow/internal/loopbacktoken"
)

func TestSeedTokenSource_ProductionTimingOnly_OneExchangeAtTheFirstNeed(t *testing.T) {
	var exchanges atomic.Int64
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())))
	jwt := "eyJhbGciOiJSUzI1NiJ9." + payload + ".sig"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/serviceaccount/login" || r.Header.Get("Authorization") != "Bearer projected-sa" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		exchanges.Add(1)
		_, _ = fmt.Fprintf(w, `{"accessToken":%q}`, jwt)
	}))
	defer srv.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-sa"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New(srv.URL, tokenFile).Token)

	pass1 := withPhase1SAContext(context.Background(), testSAEndpoint(), nil)
	pass2 := withPhase1SAContext(context.Background(), testSAEndpoint(), nil)
	if n := exchanges.Load(); n != 0 {
		t.Fatalf("building two Phase 1 SA contexts exchanged %d time(s): the exchange must wait for a stage's need", n)
	}
	for i, ctx := range []context.Context{pass1, pass1, pass2, pass1} {
		tok, err := loopbacktoken.SourceFrom(ctx)(ctx)
		if err != nil || tok != jwt {
			t.Fatalf("ask %d: got (%q, %v), want authn's token", i+1, tok, err)
		}
		if n := exchanges.Load(); n != 1 {
			t.Fatalf("after ask %d: %d exchange(s), want exactly 1 (the first need; then authn.Client's cache)", i+1, n)
		}
	}
}
