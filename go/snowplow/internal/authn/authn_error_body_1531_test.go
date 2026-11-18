package authn

// #1531 review N2: authn's error answer reaches the prewarm WARN through the token acquisition's error. At base it
// was never logged; now it is, so it carries fixed text for the status class plus the body's length and a sha256
// prefix, and NEVER any part of the body (it may echo a credential).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func errorFor(t *testing.T, status int, saToken, body string) error {
	t.Helper()
	c := New("http://authn.test:8082", writeTokenFile(t, saToken)).WithHTTPClient(&http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		}),
	})
	_, err := c.Token(context.Background())
	if err == nil {
		t.Fatalf("a %d from authn must surface as an error", status)
	}
	return err
}

func TestToken_AnErrorCarriesTheStatusClassNeverTheBody(t *testing.T) {
	jwt := makeJWT(1894000000)
	for _, tc := range []struct {
		status int
		class  string
	}{
		{http.StatusUnauthorized, "refused the service account (not authenticated, or not on authn's allowlist)"},
		{http.StatusForbidden, "refused the service account (not authenticated, or not on authn's allowlist)"},
		{http.StatusNotFound, "has no /serviceaccount/login (not found)"},
		{http.StatusServiceUnavailable, "unavailable"},
		{http.StatusTeapot, "answered an unexpected status"},
	} {
		saToken := fmt.Sprintf("projected-sa-token-%d", tc.status)
		body := "no allowlist CR for " + saToken + "; issued " + jwt
		msg := errorFor(t, tc.status, saToken, body).Error()
		if strings.Contains(msg, saToken) || strings.Contains(msg, jwt) || strings.Contains(msg, "allowlist CR") {
			t.Fatalf("%d: the error carries the body (mutation: the body logged): %q", tc.status, msg)
		}
		sum := sha256.Sum256([]byte(body))
		want := fmt.Sprintf("authn returned %d: %s (body %d bytes, sha256 %s)", tc.status, tc.class, len(body),
			hex.EncodeToString(sum[:])[:12])
		if msg != want {
			t.Fatalf("%d: error %q, want %q", tc.status, msg, want)
		}
	}
}
