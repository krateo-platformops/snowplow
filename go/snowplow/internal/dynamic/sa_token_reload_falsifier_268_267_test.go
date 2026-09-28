//go:build falsifier_268_267

// sa_token_reload_falsifier_268_267_test.go — Arm F (#267), dynamic-package half.
//
// STEP 2 REWORK: #267's root cause is dynamic.ServiceAccountEndpoint reading the
// projected token once and caching a static Token. The DECIDED robust fix (arch-268
// Option 1) threads the token FILE at the dial site (httpClientForEndpoint →
// transport.Config.BearerTokenFile → client-go NewCachedFileTokenSource) for a
// dispatch marked by PROVENANCE (cache.WithServiceAccountDial), and deliberately
// LEAVES ServiceAccountEndpoint's caching intact — so the step-1 "ServiceAccountEndpoint
// re-reads after rotation" assertion would stay RED forever and has been REMOVED.
//
// The #267 GREEN criterion (the DIAL presents the rotated token on the wire), the
// provenance GUARD (token-auth clientconfig keeps its own token), and the non-reg
// all live in the api package (sa_dial_tokenfile_falsifier_268_267_test.go), because
// httpClientForEndpoint is unexported in api and dynamic cannot import api. There is
// deliberately NO shape predicate here — a token-auth clientconfig is shape-identical
// to the SA endpoint, so recognition is provenance, not shape.
//
// THIS file asserts the one dynamic-package export the robust fix adds:
// ServiceAccountTokenFile() returns the projected token path the dial threads into
// transport.Config.BearerTokenFile.
package dynamic

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLeak267_ArmF_ServiceAccountTokenFileAccessor(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("tok"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	restore := SetServiceAccountTokenPathForTest(tokenFile)
	defer restore()

	if got := ServiceAccountTokenFile(); got != tokenFile {
		t.Fatalf("ServiceAccountTokenFile() = %q; want the projected token path %q "+
			"(the dial threads this into transport.Config.BearerTokenFile)", got, tokenFile)
	}
	t.Logf("Arm F/accessor GREEN: ServiceAccountTokenFile() reports the projected token path.")
}
