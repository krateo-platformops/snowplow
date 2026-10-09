//go:build unit || integration

package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// coordsJSON builds n subscription coordinates shaped like the real ones, so the
// byte sizes below are representative rather than invented.
func coordsJSON(n int) []byte {
	type coord struct {
		Class     string `json:"class"`
		Group     string `json:"group"`
		Version   string `json:"version"`
		Resource  string `json:"resource"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
	out := make([]coord, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, coord{
			Class:     "widgets",
			Group:     "widgets.templates.krateo.io",
			Version:   "v1beta1",
			Resource:  "tables",
			Namespace: "krateo-system",
			Name:      fmt.Sprintf("composition-architecture-card-%02d", i),
		})
	}
	b, _ := json.Marshal(out)
	return b
}

// TestIssue560_PostCarriesWhatTheQueryStringCannot is the reason the POST
// transport exists, stated in bytes.
//
// The GET form base64s the set into `?sub=`, so the REQUEST LINE grows with page
// density and the ingress answers 431 above ~22 widgets (8 KiB request line) —
// before snowplow sees the request at all. The POST form carries the identical
// JSON in the body, where no request-line limit applies and no base64 inflation
// happens.
//
// This asserts the arithmetic that makes the change worth making: that a
// realistic dense-page subscription produces a GET request line over the common
// ingress limit while the same set is an ordinary small body.
func TestIssue560_PostCarriesWhatTheQueryStringCannot(t *testing.T) {
	const ingressRequestLineLimit = 8 * 1024 // nginx large_client_header_buffers default

	body := coordsJSON(50)
	encoded := base64.StdEncoding.EncodeToString(body)
	getRequestLine := len("GET /refreshes?sub=") + len(encoded) + len(" HTTP/1.1")

	if getRequestLine <= ingressRequestLineLimit {
		t.Fatalf("a 50-widget GET request line is %d bytes, which is UNDER the %d-byte "+
			"ingress limit — either the coordinate shape in this test has drifted from the "+
			"real one or #560's premise no longer holds; re-measure before trusting it",
			getRequestLine, ingressRequestLineLimit)
	}
	if len(body) > refreshSubParamMaxBytes {
		t.Fatalf("the same 50 coords are %d bytes of body, over snowplow's own %d-byte cap; "+
			"POST would not help and the fix shape is wrong", len(body), refreshSubParamMaxBytes)
	}
	// base64 inflation is real and is part of why GET loses.
	if len(encoded) <= len(body) {
		t.Fatalf("base64 did not inflate: %d encoded vs %d raw", len(encoded), len(body))
	}
	t.Logf("50 coords: body %d bytes, base64 %d bytes, GET request line %d bytes (ingress limit %d)",
		len(body), len(encoded), getRequestLine, ingressRequestLineLimit)
}

// TestIssue560_BothTransportsYieldTheSamePayload — the POST path must be a pure
// transport change. If the two forms could disagree about the coordinate set,
// the subscription a browser gets would depend on how it asked, which is a
// correctness difference rather than a delivery fix.
func TestIssue560_BothTransportsYieldTheSamePayload(t *testing.T) {
	body := coordsJSON(7)

	get := httptest.NewRequest(http.MethodGet,
		"/refreshes?sub="+base64.StdEncoding.EncodeToString(body), nil)
	post := httptest.NewRequest(http.MethodPost, "/refreshes", strings.NewReader(string(body)))

	gotGet, err := readSubscriptionPayload(get)
	if err != nil {
		t.Fatalf("GET payload: %v", err)
	}
	gotPost, err := readSubscriptionPayload(post)
	if err != nil {
		t.Fatalf("POST payload: %v", err)
	}
	if string(gotGet) != string(gotPost) {
		t.Fatalf("transports disagree:\n GET  %s\n POST %s", gotGet, gotPost)
	}
	if string(gotGet) != string(body) {
		t.Fatalf("payload drifted from the input:\n want %s\n  got %s", body, gotGet)
	}
}

// TestIssue560_GetTransportIsUnchanged pins backward compatibility. The server
// must be able to land ahead of the client, so every GET behaviour — including
// each rejection — has to be exactly what it was.
func TestIssue560_GetTransportIsUnchanged(t *testing.T) {
	body := coordsJSON(2)

	cases := []struct {
		name    string
		url     string
		wantErr string
	}{
		{"std base64 accepted", "/refreshes?sub=" + base64.StdEncoding.EncodeToString(body), ""},
		{"url-safe base64 accepted", "/refreshes?sub=" + base64.RawURLEncoding.EncodeToString(body), ""},
		{"missing sub", "/refreshes", "missing 'sub' query parameter"},
		{"empty sub", "/refreshes?sub=", "missing 'sub' query parameter"},
		{"not base64", "/refreshes?sub=!!!not-base64!!!", "invalid 'sub' encoding: not base64"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readSubscriptionPayload(httptest.NewRequest(http.MethodGet, tc.url, nil))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				if string(got) != string(body) {
					t.Fatalf("payload drifted: %s", got)
				}
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("want error %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// TestIssue560_PostRejectsRatherThanTruncates — an oversized body must be
// REFUSED, never silently shortened.
//
// A truncated set would arm some widgets and quietly drop the rest: they render,
// they look live, and they never refresh. That is precisely the failure mode the
// frontend's own MAX_SUB_BYTES cap produces (and warns about), and reproducing it
// server-side would replace a loud 431 with a silent partial subscription —
// strictly worse, because nothing would report it.
func TestIssue560_PostRejectsRatherThanTruncates(t *testing.T) {
	oversized := make([]byte, refreshSubParamMaxBytes+1)
	for i := range oversized {
		oversized[i] = 'x'
	}
	req := httptest.NewRequest(http.MethodPost, "/refreshes", strings.NewReader(string(oversized)))
	got, err := readSubscriptionPayload(req)
	if err == nil {
		t.Fatalf("an oversized body must be refused; got %d bytes back", len(got))
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("the refusal must name the size problem, got %v", err)
	}

	// Exactly at the cap is accepted — the boundary must not be off by one, or
	// the cap silently becomes stricter than it is documented to be.
	atCap := make([]byte, refreshSubParamMaxBytes)
	for i := range atCap {
		atCap[i] = 'y'
	}
	if _, err := readSubscriptionPayload(
		httptest.NewRequest(http.MethodPost, "/refreshes", strings.NewReader(string(atCap)))); err != nil {
		t.Fatalf("a body exactly at the %d-byte cap must be accepted, got %v",
			refreshSubParamMaxBytes, err)
	}

	// And an empty body is a distinct, named error rather than an obscure JSON
	// failure further down.
	if _, err := readSubscriptionPayload(
		httptest.NewRequest(http.MethodPost, "/refreshes", strings.NewReader(""))); err == nil ||
		!strings.Contains(err.Error(), "empty subscription body") {
		t.Fatalf("an empty POST body must be named as such, got %v", err)
	}
}
