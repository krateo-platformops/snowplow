//go:build falsifier_268_267

// sa_token_reload_falsifier_268_267_test.go — Arm F (#267): dynamic.
// ServiceAccountEndpoint reads the projected SA token file ONCE (sa_client.go:113)
// and caches the *Endpoint with a STATIC Token for the process lifetime
// (:132). The projected token rotates (expirationSeconds: 3607), so the cached
// copy goes stale → the branch-E dial presents an expired bearer → 401 (the #267
// production symptom, and the accident that masks #268 branch E).
//
// RED (a6b9d348): after the token file rotates, ServiceAccountEndpoint() still
// returns the OLD token (the cached singleton). GREEN post-#267-fix: the current
// token is presented (whether the fix re-reads per acquisition or threads the
// token FILE through the dial — this arm asserts at the ServiceAccountEndpoint
// boundary, the unambiguous mechanism site; the wire-bearer assertion belongs at
// the owned dial site and is added with the chosen fix variant — see the hand-back
// note).
//
// Contrast (control): ServiceAccountRESTConfig() rides rest.InClusterConfig, whose
// transport re-reads BearerTokenFile per request and never goes stale — the
// asymmetry #267 reports. (Not driven here: inClusterConfigFn is out-of-cluster in
// unit tests; the file-vs-static asymmetry is the point, asserted on the static
// side.)
package dynamic

import (
	"os"
	"path/filepath"
	"testing"
)

// plantRotatableSAToken writes an initial SA token + CA into a temp dir, points
// ServiceAccountEndpoint at them, and returns the token file path so the test can
// rotate it (simulating kubelet's projected-token refresh). Restores production
// paths + clears the singleton on cleanup.
func plantRotatableSAToken(t *testing.T, initialToken string) (tokenFile string) {
	t.Helper()
	dir := t.TempDir()
	rawCA := realShapedSACA(t) // reused from sa_credential_real_test.go (package dynamic)

	tokenFile = filepath.Join(dir, "token")
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(tokenFile, []byte(initialToken), 0o600); err != nil {
		t.Fatalf("write SA token file: %v", err)
	}
	if err := os.WriteFile(caFile, []byte(rawCA), 0o600); err != nil {
		t.Fatalf("write SA ca.crt file: %v", err)
	}

	origToken, origCA := saTokenPath, saCAPath
	saTokenPath = tokenFile
	saCAPath = caFile
	resetSAEndpointForTest()
	t.Cleanup(func() {
		saTokenPath = origToken
		saCAPath = origCA
		resetSAEndpointForTest()
	})
	return tokenFile
}

// TestLeak267_ArmF_ServiceAccountEndpoint_StaleAfterRotation — the token file
// rotates AFTER the first acquisition; ServiceAccountEndpoint() must present the
// CURRENT token. RED against a6b9d348 (the cached singleton still carries the old
// token).
func TestLeak267_ArmF_ServiceAccountEndpoint_StaleAfterRotation(t *testing.T) {
	const oldToken = "sa-token-BOOT-eyJhbGciOiJSUzI1NiJ9.old.sig"
	const newToken = "sa-token-ROTATED-eyJhbGciOiJSUzI1NiJ9.new.sig"

	tokenFile := plantRotatableSAToken(t, oldToken)

	// First acquisition (boot): caches the singleton with the boot token.
	ep1, err := ServiceAccountEndpoint()
	if err != nil {
		t.Fatalf("ServiceAccountEndpoint (boot): %v", err)
	}
	if ep1.Token != oldToken {
		t.Fatalf("precondition: boot endpoint must carry the boot token; got %q", ep1.Token)
	}

	// Kubelet rotates the projected token file (expirationSeconds: 3607).
	if err := os.WriteFile(tokenFile, []byte(newToken), 0o600); err != nil {
		t.Fatalf("rotate SA token file: %v", err)
	}

	// Second acquisition (post-rotation): must present the CURRENT token.
	ep2, err := ServiceAccountEndpoint()
	if err != nil {
		t.Fatalf("ServiceAccountEndpoint (post-rotation): %v", err)
	}

	fileNow, _ := os.ReadFile(tokenFile)
	t.Logf("Arm F: token_file_now=%q endpoint_token=%q (want endpoint_token == token_file_now)", string(fileNow), ep2.Token)

	// FIXED EXPECTATION: the endpoint presents the CURRENT file token.
	if ep2.Token != newToken {
		t.Fatalf("LEAK/#267 — RED: after the projected token rotated to %q, ServiceAccountEndpoint() still returns the STALE cached token %q. "+
			"The branch-E dial would present an expired bearer → 401 (the #267 symptom, and the accident masking #268 branch E). "+
			"The SA endpoint's credential must track the token file.", newToken, ep2.Token)
	}
	t.Logf("Arm F GREEN: ServiceAccountEndpoint() tracks the rotated token file.")
}
